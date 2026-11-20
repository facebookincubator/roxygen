/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves an accepted HTTP/1.1 connection on the experimental owned driver.

use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
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
use roxygen_runtime::worker::ContainedPanicScope;
use roxygen_runtime::worker::WorkerCleanupTask;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h1::OwnedH1Session;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexTransport;

use super::handler_application::HandlerApplication;
use super::handler_application::failure;

type RunFuture = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>>;

/// One owned HTTP/1.1 connection and the worker cleanup that retires it.
pub(crate) struct OwnedHttp1Session<A: OwnedApplication> {
    // Declared first, so an unrun session releases its application before `cleanup` activates
    // the application's finalization.
    session: OwnedH1Session<A>,
    cleanup: Cleanup,
}

/// A worker cleanup task reserved at construction, activated when the run ends or when the
/// session or its run future is dropped. It finishes an abandoned (already cancelled) run,
/// then runs the application's finalization.
struct Cleanup {
    registration: WorkerCleanupTaskRegistration,
    abandoned: Rc<Cell<Option<RunFuture>>>,
    handle: OwnedH1Handle,
    finished: Rc<Cell<bool>>,
    context: WorkerContext,
}

impl OwnedHttp1Session<HandlerApplication> {
    /// A session that serves `handler`, whose handler contexts the cleanup finalizes once every
    /// one is released.
    pub(crate) fn new(
        context: &WorkerContext,
        transport: Rc<dyn UniplexTransport>,
        handler: HttpHandlerRef,
        connection: &ConnectionContext,
        io: UniplexConfig,
        http1: Http1DriverConfig,
    ) -> Result<(Self, OwnedH1Handle, Rc<Cell<bool>>), HttpError> {
        let mut owner = KeepAliveOwner::new_for_worker((), context.handle())
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let base = HandlerSessionContext::acquire_with_peer_identity(
            &owner,
            connection.peer_identity().map(Rc::from),
        )
        .map_err(|error| failure(error.to_string().as_bytes()))?;
        Self::new_with(
            context,
            OwnedH1Options {
                context: context.clone(),
                transport,
                io,
                http1,
            },
            async move { owner.finalize().await },
            |_| HandlerApplication::new(handler, base),
        )
    }
}

impl<A: OwnedApplication> OwnedHttp1Session<A> {
    /// A session whose application `make` builds from its handle. The cleanup runs `finalize`
    /// once the run has finished.
    pub(crate) fn new_with(
        context: &WorkerContext,
        options: OwnedH1Options,
        finalize: impl Future<Output = ()> + 'static,
        make: impl FnOnce(&OwnedH1Handle) -> A,
    ) -> Result<(Self, OwnedH1Handle, Rc<Cell<bool>>), HttpError> {
        let abandoned: Rc<Cell<Option<RunFuture>>> = Rc::default();
        let finished = Rc::new(Cell::new(false));
        let registration = context
            .register_cleanup_task({
                let abandoned = Rc::clone(&abandoned);
                let finished = Rc::clone(&finished);
                async move {
                    if let Some(mut run) = abandoned.take() {
                        // The run was cancelled when it was abandoned. Its outcome is
                        // unobserved, so its error, or a callback panic that the driver
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
                    finalize.await;
                }
            })
            .map_err(|error| failure(error.to_string().as_bytes()))?;
        let (session, handle) = OwnedH1Session::new_with(options, make)?;
        Ok((
            Self {
                session,
                cleanup: Cleanup {
                    registration,
                    abandoned,
                    handle: handle.clone(),
                    finished: Rc::clone(&finished),
                    context: context.clone(),
                },
            },
            handle,
            finished,
        ))
    }

    /// Runs the connection, then waits for its cleanup's finalization. Dropping the returned
    /// future before it completes cancels the connection, whose run then finishes in the
    /// worker cleanup task.
    pub(crate) fn run(self) -> OwnedHttp1Run {
        OwnedHttp1Run {
            run: Some(Box::pin(self.session.run())),
            cleanup: Some(self.cleanup),
            finalizing: None,
            result: None,
        }
    }
}

pub(crate) struct OwnedHttp1Run {
    run: Option<RunFuture>,
    cleanup: Option<Cleanup>,
    finalizing: Option<WorkerCleanupTask>,
    result: Option<Result<UniplexRunSummary, HttpError>>,
}

impl Future for OwnedHttp1Run {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if let Some(run) = this.run.as_mut() {
            let result = match catch_unwind(AssertUnwindSafe(|| run.as_mut().poll(context))) {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(result)) => result,
                Err(panic) => {
                    // The driver resumes a callback panic only after its cleanup has joined,
                    // so the run has finished. The panic fails only this connection.
                    if let Some(cleanup) = this.cleanup.as_ref() {
                        cleanup.context.record_contained_panic(
                            ContainedPanicScope::Connection,
                            "owned_http1_run",
                            None,
                            &*panic,
                        );
                    }
                    discard_panic(panic);
                    Err(failure(b"owned connection panicked"))
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
            .expect("owned HTTP/1.1 run polled after completion");
        std::task::ready!(Pin::new(task).poll(context));
        this.finalizing = None;
        Poll::Ready(this.result.take().expect("a finished run keeps its result"))
    }
}

impl Drop for OwnedHttp1Run {
    fn drop(&mut self) {
        if let (Some(run), Some(cleanup)) = (self.run.take(), self.cleanup.as_ref()) {
            cleanup.abandoned.set(Some(run));
            cleanup.handle.cancel();
        }
        // Dropping the registration activates the cleanup task.
        self.cleanup.take();
    }
}
