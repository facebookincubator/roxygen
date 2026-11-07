//! Adapts the server's `HttpHandler` to the owned drivers' `OwnedApplication`.
//!
//! Each exchange invokes the handler when its request head arrives and polls the
//! handler future and the response source inline from the driver's poll, with no
//! task per request. The owned HTTP/1.1 and HTTP/2 session wrappers share it.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;

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
use roxygen_runtime::panic::discard_panic;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;

type BodyRead = Pin<Box<dyn Future<Output = Result<HttpBodyEvent, HttpError>> + 'static>>;

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

pub(crate) fn failure(message: &[u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message)
}

/// Invokes the server's handler for each exchange of one connection.
pub(crate) struct HandlerApplication {
    handler: HttpHandlerRef,
    base: HandlerSessionContext,
}

impl HandlerApplication {
    pub(crate) fn new(handler: HttpHandlerRef, base: HandlerSessionContext) -> Self {
        Self { handler, base }
    }
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
                        b"owned exchange polled without a request",
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
                b"owned body polled before its head",
            )));
        };
        // A pending read keeps the allowance it started with: while it waits the
        // driver serializes nothing more for this exchange, so its allowance can
        // only grow.
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
