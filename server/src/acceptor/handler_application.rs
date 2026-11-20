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
//! task per request. A request without a body is first offered to
//! `HttpHandler::respond_now`, whose whole response the exchange holds inline. The
//! owned HTTP/1.1 and HTTP/2 session wrappers share it.
//!
//! The exchanges of one connection share its handler and base context, and an
//! exchange clones the context only for a request it passes to `handle_request`.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;

use roxygen_core::buf_queue::BufQueue;
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

fn failed_handler(error: HttpError) -> LocalHandlerFuture {
    Box::pin(std::future::ready(Err(error)))
}

/// The handler and base context that the exchanges of one connection share.
struct Shared {
    handler: HttpHandlerRef,
    base: HandlerSessionContext,
}

/// Invokes the server's handler for each exchange of one connection.
pub(crate) struct HandlerApplication {
    shared: Rc<Shared>,
}

impl HandlerApplication {
    pub(crate) fn new(handler: HttpHandlerRef, base: HandlerSessionContext) -> Self {
        Self {
            shared: Rc::new(Shared { handler, base }),
        }
    }

    fn exchange(&self) -> HandlerExchange {
        HandlerExchange {
            shared: Rc::clone(&self.shared),
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
    /// A response from `respond_now`, read as `HttpFixedSource` reads. The head
    /// is taken by the header poll, and the body by the poll that ends it.
    Fixed {
        head: Option<Box<HttpMessage>>,
        body: Option<BufQueue>,
    },
    Retired,
}

pub(crate) struct HandlerExchange {
    shared: Rc<Shared>,
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
                ExchangeState::AwaitingRequest
                | ExchangeState::Fixed { .. }
                | ExchangeState::Retired => {
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
            ExchangeState::Fixed { .. }
            | ExchangeState::AwaitingRequest
            | ExchangeState::Retired => {}
        }
    }

    /// Offers a request without a body to `respond_now`, under the same unwind
    /// boundary as `handle_request`. Returns the head when `respond_now`
    /// returns `None`.
    fn try_respond_now(&mut self, mut head: Box<HttpMessage>) -> Option<Box<HttpMessage>> {
        let handler = &self.shared.handler;
        let response = match catch_unwind(AssertUnwindSafe(|| handler.respond_now(&head))) {
            Ok(None) => return Some(head),
            Ok(Some(response)) => response,
            Err(panic) => {
                discard_panic(panic);
                Err(handler_panic_error())
            }
        };
        self.state = match response {
            Ok(response) => {
                let (response, body) = response.into_parts();
                *head = response;
                ExchangeState::Fixed {
                    head: Some(head),
                    body: Some(body),
                }
            }
            Err(error) => ExchangeState::Handling(failed_handler(error)),
        };
        None
    }
}

fn fixed_header(
    head: Option<Box<HttpMessage>>,
    body: &Option<BufQueue>,
) -> Result<HttpHeaderEvent, HttpError> {
    let head = head.ok_or_else(|| {
        HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"response header was already read",
        )
    })?;
    HttpHeaderEvent::from_boxed_headers(head, body.as_ref().is_none_or(BufQueue::is_empty))
        .map_err(|error| failure(error.to_string().as_bytes()))
}

fn fixed_body(body: &mut Option<BufQueue>, maximum: u32) -> Result<HttpBodyEvent, HttpError> {
    let rest = body.as_mut().ok_or_else(|| {
        HttpError::with_message(HttpErrorCode::DROPPED, b"HTTP source is no longer readable")
    })?;
    let data = rest.split_at_most(maximum as usize);
    let eom = rest.is_empty();
    if eom {
        *body = None;
    }
    Ok(HttpBodyEvent::body(data, eom))
}

impl OwnedExchange for HandlerExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool) {
        assert!(
            matches!(self.state, ExchangeState::AwaitingRequest),
            "an exchange receives one request"
        );
        let head = if eom {
            match self.try_respond_now(head) {
                Some(head) => head,
                None => return,
            }
        } else {
            head
        };
        let future = match HttpHeaderEvent::from_boxed_headers(head, eom) {
            Ok(head) => {
                let source = if body.source().is_some() {
                    HttpHybridSource::from_header_event(head, body.into_source_holder())
                } else {
                    HttpHybridSource::from_header_event_only(head)
                };
                let request = HttpSourceHolder::new(Box::new(source));
                let Shared { handler, base } = &*self.shared;
                catch_unwind(AssertUnwindSafe(|| {
                    handler.handle_request(base.clone(), request)
                }))
                .unwrap_or_else(|panic| {
                    discard_panic(panic);
                    failed_handler(handler_panic_error())
                })
            }
            Err(error) => failed_handler(failure(error.to_string().as_bytes())),
        };
        self.state = ExchangeState::Handling(future);
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if let ExchangeState::Fixed { head, body } = &mut self.state {
            return Poll::Ready(fixed_header(head.take(), body));
        }
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
        let (response, body_read) = match &mut self.state {
            ExchangeState::Responding {
                response,
                body_read,
                ..
            } => (response, body_read),
            ExchangeState::Fixed { body, .. } => return Poll::Ready(fixed_body(body, maximum)),
            _ => {
                return Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"owned body polled before its head",
                )));
            }
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
    use roxygen_core::handler::FixedResponse;
    use roxygen_core::handler::HttpHandler;
    use roxygen_core::source::LocalSourceFuture;
    use roxygen_core::stream_source::HttpStreamSource;
    use roxygen_runtime::keep_alive::KeepAliveOwner;

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

    /// Finalizes `owner`, which no reference may still hold.
    fn finalize(mut owner: KeepAliveOwner<()>) {
        let mut context = Context::from_waker(std::task::Waker::noop());
        let finalized = std::pin::pin!(owner.finalize()).poll(&mut context);
        assert!(finalized.is_ready(), "no reference remains");
    }

    /// The references on `owner` besides its own.
    fn references(owner: &KeepAliveOwner<()>) -> u64 {
        owner.num_keep_alives() - 1
    }

    /// Builds an exchange in `state` whose base context holds a reference on
    /// `owner`.
    fn exchange_in(owner: &KeepAliveOwner<()>, state: ExchangeState) -> HandlerExchange {
        HandlerExchange {
            shared: Rc::new(Shared {
                handler: Rc::new(NoHandler),
                base: HandlerSessionContext::acquire(owner).expect("the owner accepts work"),
            }),
            state,
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
            let owner = KeepAliveOwner::new(());
            let mut exchange = exchange_in(
                &owner,
                ExchangeState::Responding {
                    response: HttpSourceHolder::new(Box::new(source)),
                    header_read: None,
                    body_read: None,
                },
            );
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
            finalize(owner);
        }
    }

    fn fixed_exchange(owner: &KeepAliveOwner<()>, body: &'static [u8]) -> HandlerExchange {
        let mut head = HttpMessage::new();
        head.set_status_code(200).expect("a response head");
        exchange_in(
            owner,
            ExchangeState::Fixed {
                head: Some(Box::new(head)),
                body: Some(
                    BufQueue::from_bytes(Bytes::from_static(body), body.len())
                        .expect("the body fits its queue"),
                ),
            },
        )
    }

    /// A fixed response splits its body by each poll's allowance and ends the
    /// message with its last byte, or with its head when the body is empty. A
    /// poll after the end fails as a consumed response source does.
    #[test]
    fn a_fixed_response_splits_its_body_by_the_allowance() {
        let owner = KeepAliveOwner::new(());
        let mut context = Context::from_waker(std::task::Waker::noop());
        let mut exchange = fixed_exchange(&owner, AVAILABLE);
        let Poll::Ready(Ok(head)) = exchange.poll_headers(&mut context) else {
            panic!("the head is ready");
        };
        assert!(!head.eom());
        assert_eq!(head.headers().response_status(), Some(200));
        let mut events = Vec::new();
        for maximum in [4, 0, 4, 100] {
            let Poll::Ready(Ok(event)) = exchange.poll_body(&mut context, maximum) else {
                panic!("a body event is ready");
            };
            let data = event.as_body().expect("a body event");
            events.push((
                data.coalesce(usize::MAX).expect("data").to_vec(),
                event.eom(),
            ));
        }
        assert_eq!(
            events,
            [
                (b"0123".to_vec(), false),
                (Vec::new(), false),
                (b"4567".to_vec(), false),
                (b"89".to_vec(), true),
            ]
        );
        let Poll::Ready(Err(error)) = exchange.poll_body(&mut context, 8) else {
            panic!("the consumed response fails the next read");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        let Poll::Ready(Err(error)) = exchange.poll_headers(&mut context) else {
            panic!("the head is read once");
        };
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        assert!(!exchange.handler_active());
        exchange.complete();

        let mut exchange = fixed_exchange(&owner, b"");
        let Poll::Ready(Ok(head)) = exchange.poll_headers(&mut context) else {
            panic!("the head is ready");
        };
        assert!(head.eom(), "an empty body ends the message with its head");
        exchange.cancel(
            &HttpError::with_message(HttpErrorCode::CANCEL, b"reset"),
            true,
        );
        assert!(matches!(exchange.state, ExchangeState::Retired));
        drop(exchange);
        finalize(owner);
    }

    /// Answers from `respond_now` while `answer` is set, and otherwise keeps
    /// each context that `handle_request` receives.
    #[derive(Default)]
    struct AnswerOrKeep {
        answer: Cell<bool>,
        kept: RefCell<Vec<HandlerSessionContext>>,
    }

    impl HttpHandler for AnswerOrKeep {
        fn respond_now(&self, _request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
            self.answer.get().then(|| {
                let mut head = HttpMessage::new();
                head.set_status_code(204).expect("a response head");
                Ok(FixedResponse::new(head, BufQueue::new(0)))
            })
        }

        fn handle_request(
            &self,
            context: HandlerSessionContext,
            _request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            self.kept.borrow_mut().push(context);
            failed_handler(failure(b"kept"))
        }
    }

    fn request_without_body(exchange: &mut HandlerExchange) {
        let mut head = HttpMessage::new();
        head.set_method(roxygen_core::http::HttpMethod::Get)
            .expect("a request head");
        let body =
            HttpStreamSource::new_async(Some(StreamId::new(1)), None, 8).expect("a body source");
        exchange.request(Box::new(head), HttpStreamSourceOwner::new(body), true);
    }

    /// A request answered by `respond_now` takes no keep-alive reference, and
    /// `handle_request` receives a clone of the connection's base context that
    /// keeps the owner referenced until the handler drops it. The base itself is
    /// released with the application and the last of its exchanges.
    #[test]
    fn only_a_handled_request_clones_the_base_context() {
        let owner = KeepAliveOwner::new(());
        let handler = Rc::new(AnswerOrKeep::default());
        let base =
            HandlerSessionContext::acquire_with_peer_identity(&owner, Some(Rc::from(&b"peer"[..])))
                .expect("the owner accepts work");
        let application = HandlerApplication::new(handler.clone(), base);
        assert_eq!(references(&owner), 1);

        handler.answer.set(true);
        let mut answered = application.exchange();
        request_without_body(&mut answered);
        assert_eq!(references(&owner), 1);
        let mut context = Context::from_waker(std::task::Waker::noop());
        let Poll::Ready(Ok(head)) = answered.poll_headers(&mut context) else {
            panic!("the answer's head is ready");
        };
        assert_eq!(head.headers().response_status(), Some(204));
        assert!(head.eom());
        answered.complete();

        handler.answer.set(false);
        let mut handled = application.exchange();
        request_without_body(&mut handled);
        assert_eq!(references(&owner), 2);
        assert_eq!(handler.kept.borrow()[0].peer_identity(), Some(&b"peer"[..]));
        assert!(handled.handler_active());
        handled.complete();
        assert_eq!(references(&owner), 2, "the handler keeps its context");
        handler.kept.borrow_mut().clear();
        assert_eq!(references(&owner), 1);

        let unrequested = application.exchange();
        drop(application);
        assert_eq!(references(&owner), 1, "an exchange keeps the base");
        drop(unrequested);
        assert_eq!(references(&owner), 0);
        finalize(owner);
    }
}
