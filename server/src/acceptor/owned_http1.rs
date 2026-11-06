//! Serves an accepted HTTP/1.1 connection on the experimental owned driver.
//!
//! [`HandlerApplication`] adapts the server's `HttpHandler` to `OwnedApplication`: each
//! exchange invokes the handler when its request head arrives and polls the handler future
//! and the response source inline from the driver's poll, with no task per request.

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
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::get_error_response;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::hybrid_source::HttpHybridSource;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source_holder::HolderHeaderRead;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::WorkerCleanupTask;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h1::OwnedH1Session;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexTransport;

type BodyRead = Pin<Box<dyn Future<Output = Result<HttpBodyEvent, HttpError>> + 'static>>;
type RunFuture = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>>;

/// One owned HTTP/1.1 connection and the worker cleanup that retires it.
pub(crate) struct OwnedHttp1Session {
    // Declared first, so an unrun session releases its handler context before `cleanup`
    // activates the owner's finalization.
    session: OwnedH1Session<HandlerApplication>,
    cleanup: Cleanup,
}

/// A worker cleanup task reserved at construction. Activated when the run ends, or when the
/// session or its run future is dropped, it finishes an abandoned run (after cancelling it),
/// then finalizes the handler contexts' liveness owner once every context is released.
struct Cleanup {
    registration: WorkerCleanupTaskRegistration,
    abandoned: Rc<Cell<Option<RunFuture>>>,
    handle: OwnedH1Handle,
    finished: Rc<Cell<bool>>,
}

impl OwnedHttp1Session {
    pub(crate) fn new(
        context: &WorkerContext,
        transport: Rc<dyn UniplexTransport>,
        handler: HttpHandlerRef,
        connection: &ConnectionContext,
        io: UniplexConfig,
    ) -> Result<(Self, OwnedH1Handle, Rc<Cell<bool>>), HttpError> {
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
        let (session, handle) = OwnedH1Session::new(
            HandlerApplication { handler, base },
            OwnedH1Options {
                context: context.clone(),
                transport,
                io,
                http1: Http1DriverConfig::default(),
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

/// As `UniplexSession`, a handler failure becomes an error response that closes the
/// connection: `408` for a read timeout, `500` otherwise.
fn handler_response(
    result: Result<HttpSourceHolder, HttpError>,
) -> Result<HttpSourceHolder, HttpError> {
    let (status, body): (u16, &[u8]) = match &result {
        Ok(response) if response.readable() => return result,
        Ok(_) => (500, b"handler returned no response"),
        Err(error) if error.code() == HttpErrorCode::READ_TIMEOUT => (408, b""),
        Err(error) => (500, error.message()),
    };
    get_error_response(status, body)
        .map(|source| HttpSourceHolder::new(Box::new(source)))
        .map_err(|error| failure(error.to_string().as_bytes()))
}

fn handler_panic_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"request handler panicked")
}

fn failure(message: &[u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message)
}

/// Invokes the server's handler for each exchange of one connection.
pub(crate) struct HandlerApplication {
    handler: HttpHandlerRef,
    base: HandlerSessionContext,
}

impl OwnedApplication for HandlerApplication {
    type Exchange = HandlerExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> HandlerExchange {
        HandlerExchange {
            handler: Rc::clone(&self.handler),
            context: Some(self.base.clone()),
            state: ExchangeState::AwaitingRequest,
        }
    }
}

enum ExchangeState {
    AwaitingRequest,
    Handling(LocalHandlerFuture),
    Responding {
        response: HttpSourceHolder,
        header_read: Option<HolderHeaderRead>,
        body_read: Option<BodyRead>,
    },
    Retired,
}

pub(crate) struct HandlerExchange {
    handler: HttpHandlerRef,
    context: Option<HandlerSessionContext>,
    state: ExchangeState,
}

impl HandlerExchange {
    /// Polls the handler future until it returns the response source.
    fn poll_handler(&mut self, context: &mut Context<'_>) -> Poll<Result<(), HttpError>> {
        loop {
            match &mut self.state {
                ExchangeState::Handling(future) => {
                    let result =
                        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                            Ok(Poll::Pending) => return Poll::Pending,
                            Ok(Poll::Ready(result)) => result,
                            Err(panic) => {
                                discard_panic(panic);
                                Err(handler_panic_error())
                            }
                        };
                    let handler = std::mem::replace(&mut self.state, ExchangeState::Retired);
                    let drop_result = catch_unwind(AssertUnwindSafe(|| drop(handler)));
                    let drop_panicked = drop_result.is_err();
                    if let Err(panic) = drop_result {
                        discard_panic(panic);
                    }
                    let result = match result {
                        Ok(response) if drop_panicked => {
                            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(response))) {
                                discard_panic(panic);
                            }
                            Err(handler_panic_error())
                        }
                        result => result,
                    };
                    self.state = ExchangeState::Responding {
                        response: handler_response(result)?,
                        header_read: None,
                        body_read: None,
                    };
                }
                ExchangeState::Responding { .. } => return Poll::Ready(Ok(())),
                ExchangeState::AwaitingRequest | ExchangeState::Retired => {
                    return Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"owned HTTP/1.1 exchange polled without a request",
                    )));
                }
            }
        }
    }

    fn retire(&mut self, error: Option<HttpErrorCode>) {
        match std::mem::replace(&mut self.state, ExchangeState::Retired) {
            ExchangeState::Responding {
                mut response,
                header_read,
                body_read,
            } => {
                drop(header_read);
                drop(body_read);
                response.clear(error);
            }
            ExchangeState::Handling(future) => {
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
                    discard_panic(panic);
                }
            }
            ExchangeState::AwaitingRequest | ExchangeState::Retired => {}
        }
        self.context.take();
    }
}

impl OwnedExchange for HandlerExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        let context = self
            .context
            .take()
            .expect("an exchange receives one request");
        let future = match HttpHeaderEvent::new(*head, eom) {
            Ok(head) => {
                let request = HttpSourceHolder::new(Box::new(HttpHybridSource::from_header_event(
                    head,
                    body.into_source_holder(),
                )));
                catch_unwind(AssertUnwindSafe(|| {
                    self.handler.handle_request(context, request)
                }))
                .unwrap_or_else(|panic| {
                    discard_panic(panic);
                    Box::pin(std::future::ready(Err(handler_panic_error())))
                })
            }
            Err(error) => Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                error.to_string().as_bytes(),
            )))),
        };
        self.state = ExchangeState::Handling(future);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        std::task::ready!(self.poll_handler(context))?;
        let ExchangeState::Responding {
            response,
            header_read,
            ..
        } = &mut self.state
        else {
            unreachable!("a polled handler leaves a response");
        };
        let read = header_read.get_or_insert_with(|| response.read_header_event_unboxed());
        let result = std::task::ready!(Pin::new(read).poll(context));
        *header_read = None;
        Poll::Ready(result)
    }

    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let ExchangeState::Responding {
            response,
            body_read,
            ..
        } = &mut self.state
        else {
            return Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned HTTP/1.1 body polled before its head",
            )));
        };
        // A pending read keeps the allowance it started with: the exchange is serial, so
        // nothing is serialized while it waits and the driver's allowance can only grow.
        let read =
            body_read.get_or_insert_with(|| Box::pin(response.read_body_event_unboxed(maximum)));
        let result = std::task::ready!(read.as_mut().poll(context));
        *body_read = None;
        Poll::Ready(result)
    }

    fn cancel(&mut self, error: &HttpError, _peer_reset: bool) {
        self.retire(Some(error.code()));
    }

    fn response_complete(&mut self) {}

    fn complete(mut self) {
        self.retire(None);
    }
}
