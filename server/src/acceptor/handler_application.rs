/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HolderBodyRead;
use roxygen_core::source_holder::HolderHeaderRead;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::panic::discard_panic;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;

/// Turns a handler failure into an error response that closes the connection, as
/// `UniplexSession` does: `408` for a read timeout, `500` otherwise.
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

    fn exchange(&self) -> HandlerExchange {
        HandlerExchange {
            handler: Rc::clone(&self.handler),
            context: Some(self.base.clone()),
            state: ExchangeState::AwaitingRequest,
        }
    }
}

impl OwnedApplication for HandlerApplication {
    type Exchange = HandlerExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> HandlerExchange {
        self.exchange()
    }

    fn new_exchange_with(
        &self,
        _stream: StreamId,
        _ingress: impl FnOnce() -> IngressCompletion,
    ) -> HandlerExchange {
        self.exchange()
    }
}

/// A started response body read.
enum BodyRead {
    /// A read from a source that polls its body events in place. It is polled through the
    /// exclusively owned holder with the allowance it started with.
    InPlace(u32),
    /// A read through the source's read future, held inline.
    Future(HolderBodyRead),
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
        let future = match HttpHeaderEvent::from_boxed_headers(head, eom) {
            Ok(head) => {
                let source = if body.source().is_some() {
                    HttpHybridSource::from_header_event(head, body.into_source_holder())
                } else {
                    HttpHybridSource::from_header_event_only(head)
                };
                let request = HttpSourceHolder::new(Box::new(source));
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
        // A pending read keeps the allowance it started with. While it waits, the
        // driver serializes no more output for this exchange, so the allowance can
        // only grow.
        let read = body_read.get_or_insert_with(|| {
            if response.polls_body_events() {
                BodyRead::InPlace(maximum)
            } else {
                BodyRead::Future(response.read_body_event_unboxed(maximum))
            }
        });
        let result = std::task::ready!(match read {
            BodyRead::InPlace(maximum) => response.poll_body_event_exclusive(context, *maximum),
            BodyRead::Future(read) => Pin::new(read).poll(context),
        });
        *body_read = None;
        Poll::Ready(result)
    }

    fn handler_active(&self) -> bool {
        matches!(self.state, ExchangeState::Handling(_))
    }

    fn cancel(&mut self, error: &HttpError, _peer_reset: bool) {
        self.retire(Some(error.code()));
    }

    fn response_complete(&mut self) {}

    fn complete(mut self) {
        self.retire(None);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;

    use bytes::Bytes;
    use roxygen_core::buf_queue::BufQueue;
    use roxygen_core::handler::HttpHandler;
    use roxygen_core::source::LocalSourceFuture;

    use super::*;

    const AVAILABLE: &[u8] = b"0123456789";

    /// Returns `Pending` once for each body read, then at most `maximum` bytes of
    /// `AVAILABLE`, through `poll_body_event` when `polling`, or else through the
    /// read future, which keeps the `maximum` its read started with.
    struct OnePendingBody {
        polling: bool,
        primed: Rc<Cell<bool>>,
        maxima: Rc<RefCell<Vec<u32>>>,
    }

    fn poll_once_pending(
        primed: &Cell<bool>,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if !primed.replace(false) {
            primed.set(true);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        let size = AVAILABLE.len().min(maximum as usize);
        let body = BufQueue::from_bytes(Bytes::from_static(&AVAILABLE[..size]), size)
            .expect("the body fits its queue");
        Poll::Ready(Ok(HttpBodyEvent::body(body, true)))
    }

    impl HttpSource for OnePendingBody {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            unreachable!("the exchange is already responding")
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            assert!(
                !self.polling,
                "a polling source is never read through a future"
            );
            self.maxima.borrow_mut().push(maximum);
            let primed = Rc::clone(&self.primed);
            Box::pin(std::future::poll_fn(move |context| {
                poll_once_pending(&primed, context, maximum)
            }))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

        fn polls_body_events(&self) -> bool {
            self.polling
        }

        fn poll_body_event(
            &mut self,
            context: &mut Context<'_>,
            maximum: u32,
        ) -> Poll<Result<HttpBodyEvent, HttpError>> {
            self.maxima.borrow_mut().push(maximum);
            poll_once_pending(&self.primed, context, maximum)
        }
    }

    struct NoHandler;

    impl HttpHandler for NoHandler {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            _request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            unreachable!("the exchange is already responding")
        }
    }

    /// A body read pending when the driver's allowance grows still returns at
    /// most the allowance it started with, polled in place or read through a
    /// future.
    #[test]
    fn a_pending_body_read_keeps_the_allowance_it_started_with() {
        for polling in [false, true] {
            let maxima = Rc::new(RefCell::new(Vec::new()));
            let source = OnePendingBody {
                polling,
                primed: Rc::new(Cell::new(false)),
                maxima: Rc::clone(&maxima),
            };
            let mut exchange = HandlerExchange {
                handler: Rc::new(NoHandler),
                context: None,
                state: ExchangeState::Responding {
                    response: HttpSourceHolder::new(Box::new(source)),
                    header_read: None,
                    body_read: None,
                },
            };
            let mut context = Context::from_waker(std::task::Waker::noop());
            assert!(exchange.poll_body(&mut context, 3).is_pending());
            let Poll::Ready(Ok(event)) = exchange.poll_body(&mut context, 8) else {
                panic!("polling {polling}: the pending read completes");
            };
            assert_eq!(
                event.as_body().expect("a body event").chain_length(),
                3,
                "polling {polling}: the read's starting allowance"
            );
            let expected: &[u32] = if polling { &[3, 3] } else { &[3] };
            assert_eq!(*maxima.borrow(), expected, "polling {polling}");
            let Poll::Ready(Err(error)) = exchange.poll_body(&mut context, 8) else {
                panic!("polling {polling}: the consumed response fails the next read");
            };
            assert_eq!(error.code(), HttpErrorCode::DROPPED, "polling {polling}");
            exchange.complete();
        }
    }
}
