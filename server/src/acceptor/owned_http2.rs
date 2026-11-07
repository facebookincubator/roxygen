//! Serves an accepted HTTP/2 connection on the experimental owned driver.

use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::error::HttpError;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::WorkerCleanupTask;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::owned_h2::OwnedH2Handle;
use roxygen_session::owned_h2::OwnedH2Options;
use roxygen_session::owned_h2::OwnedH2Session;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexTransport;

use super::handler_application::HandlerApplication;
use super::handler_application::failure;

type RunFuture = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>>;

/// One owned HTTP/2 connection and the worker cleanup that retires it.
pub(crate) struct OwnedHttp2Session {
    // Declared first, so an unrun session releases its handler context before `cleanup`
    // activates the owner's finalization.
    session: OwnedH2Session<HandlerApplication>,
    cleanup: Cleanup,
}

/// A worker cleanup task reserved at construction. Activated when the run ends, or when the
/// session or its run future is dropped, it finishes an abandoned run (after cancelling it),
/// then finalizes the handler contexts' liveness owner once every context is released.
struct Cleanup {
    registration: WorkerCleanupTaskRegistration,
    abandoned: Rc<Cell<Option<RunFuture>>>,
    handle: OwnedH2Handle,
    finished: Rc<Cell<bool>>,
}

impl OwnedHttp2Session {
    pub(crate) fn new(
        context: &WorkerContext,
        transport: Rc<dyn UniplexTransport>,
        handler: HttpHandlerRef,
        connection: &ConnectionContext,
        io: UniplexConfig,
        http2: Http2SessionConfig,
    ) -> Result<(Self, OwnedH2Handle, Rc<Cell<bool>>), HttpError> {
        let mut owner = KeepAliveOwner::new_for_worker((), context.handle())
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let base = HandlerSessionContext::acquire_with_peer_identity(
            &owner,
            connection.peer_identity().map(Rc::from),
        )
        .map_err(|error| failure(error.to_string().as_bytes()))?;
        let abandoned: Rc<Cell<Option<RunFuture>>> = Rc::default();
        let finished = Rc::new(Cell::new(false));
        let registration = context
            .register_cleanup_task({
                let abandoned = Rc::clone(&abandoned);
                let finished = Rc::clone(&finished);
                async move {
                    if let Some(mut run) = abandoned.take() {
                        // The run was cancelled when it was abandoned. Nothing awaits its
                        // outcome any more, so its error, or a callback panic the driver
                        // resumes as it completes, is discarded.
                        std::future::poll_fn(|context| {
                            match catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(context))) {
                                Ok(poll) => poll.map(drop),
                                Err(panic) => {
                                    discard_panic(panic);
                                    Poll::Ready(())
                                }
                            }
                        })
                        .await;
                        finished.set(true);
                    }
                    owner.finalize().await;
                }
            })
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let (session, handle) = OwnedH2Session::new(
            HandlerApplication::new(handler, base),
            OwnedH2Options {
                context: context.clone(),
                transport,
                io,
                http2,
            },
        )?;
        Ok((
            Self {
                session,
                cleanup: Cleanup {
                    registration,
                    abandoned,
                    handle: handle.clone(),
                    finished: Rc::clone(&finished),
                },
            },
            handle,
            finished,
        ))
    }

    /// Runs the connection, then waits for its handler contexts to be released. Dropping
    /// the returned future before it completes cancels the connection, whose run then
    /// finishes in the worker cleanup task.
    pub(crate) fn run(self) -> OwnedHttp2Run {
        OwnedHttp2Run {
            run: Some(Box::pin(self.session.run())),
            cleanup: Some(self.cleanup),
            finalizing: None,
            result: None,
        }
    }
}

pub(crate) struct OwnedHttp2Run {
    run: Option<RunFuture>,
    cleanup: Option<Cleanup>,
    finalizing: Option<WorkerCleanupTask>,
    result: Option<Result<UniplexRunSummary, HttpError>>,
}

impl Future for OwnedHttp2Run {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(run) = this.run.as_mut() {
            let result = match catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(context))) {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(result)) => result,
                Err(panic) => {
                    // The driver rethrows a callback panic only after its cleanup joined, so
                    // the run is finished; dropping the registration finalizes the owner.
                    this.run = None;
                    if let Some(cleanup) = this.cleanup.take() {
                        cleanup.finished.set(true);
                    }
                    resume_unwind(panic);
                }
            };
            this.run = None;
            let cleanup = this
                .cleanup
                .take()
                .expect("a running session owns its cleanup");
            cleanup.finished.set(true);
            match cleanup.registration.activate() {
                Ok(task) => {
                    this.result = Some(result);
                    this.finalizing = Some(task);
                }
                Err(error) => return Poll::Ready(Err(failure(error.to_string().as_bytes()))),
            }
        }
        let task = this
            .finalizing
            .as_mut()
            .expect("owned HTTP/2 run polled after completion");
        std::task::ready!(Pin::new(task).poll(context));
        this.finalizing = None;
        Poll::Ready(this.result.take().expect("a finished run keeps its result"))
    }
}

impl Drop for OwnedHttp2Run {
    fn drop(&mut self) {
        if let (Some(run), Some(cleanup)) = (self.run.take(), self.cleanup.as_ref()) {
            cleanup.abandoned.set(Some(run));
            cleanup.handle.cancel();
        }
        // Dropping the registration activates the cleanup task.
        self.cleanup.take();
    }
}
