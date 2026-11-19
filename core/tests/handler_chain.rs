/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod handler_chain {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use roxygen_runtime::keep_alive::KeepAliveOwner;

    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::events::HttpBodyEvent;
    use crate::events::HttpHeaderEvent;
    use crate::handler::HandlerFuture;
    use crate::handler::HandlerInvocation;
    use crate::handler::HandlerSessionContext;
    use crate::handler::LocalHandlerFuture;
    use crate::handler::ResponseCounters;
    use crate::handler_chain::HttpChainHandler;
    use crate::handler_chain::HttpChainHandlerHooks;
    use crate::handler_chain::HttpChainHandlerRef;
    use crate::handler_chain::HttpHandlerChain;
    use crate::handler_chain::HttpHandlerRef;
    use crate::source::HttpSource;
    use crate::source::LocalSourceFuture;
    use crate::source_holder::HttpSourceHolder;

    struct EmptyHandler;

    impl HttpChainHandlerHooks for EmptyHandler {
        fn handle_request(
            &self,
            _next_handler: Option<HttpHandlerRef>,
            _context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move {
                drop(request_source);
                Ok(HttpSourceHolder::empty())
            })
        }
    }

    fn handler() -> HttpChainHandlerRef {
        Rc::new(HttpChainHandler::new(EmptyHandler))
    }

    fn as_handler(handler: &HttpChainHandlerRef) -> HttpHandlerRef {
        handler.clone() as HttpHandlerRef
    }

    fn assert_same(actual: Option<HttpChainHandlerRef>, expected: &HttpChainHandlerRef) {
        let actual = actual.expect("expected a handler");
        assert!(Rc::ptr_eq(&actual, expected));
    }

    fn assert_next(actual: &HttpChainHandlerRef, expected: &HttpChainHandlerRef) {
        let actual = actual.get_next_handler().expect("expected a next handler");
        assert!(Rc::ptr_eq(&actual, &as_handler(expected)));
    }

    #[test]
    fn next_empty() {
        assert!(handler().get_next_handler().is_none());
    }

    #[test]
    fn set_next() {
        let handler_a = handler();
        let handler_b = handler();

        handler_a.set_next_handler(Some(as_handler(&handler_b)));
        assert_next(&handler_a, &handler_b);

        handler_a.set_next_handler(None);
        assert!(handler_a.get_next_handler().is_none());
    }

    #[test]
    fn front_empty() {
        assert!(HttpHandlerChain::new().get_front().is_none());
    }

    #[test]
    fn back_empty() {
        assert!(HttpHandlerChain::new().get_back().is_none());
    }

    #[test]
    fn front_non_empty() {
        let mut chain_front = HttpHandlerChain::new();
        chain_front.insert_front(Some(handler()));
        assert!(chain_front.get_front().is_some());

        let mut chain_back = HttpHandlerChain::new();
        chain_back.insert_back(Some(handler()));
        assert!(chain_back.get_front().is_some());
    }

    #[test]
    fn back_non_empty() {
        let mut chain_front = HttpHandlerChain::new();
        chain_front.insert_front(Some(handler()));
        assert!(chain_front.get_back().is_some());

        let mut chain_back = HttpHandlerChain::new();
        chain_back.insert_back(Some(handler()));
        assert!(chain_back.get_back().is_some());
    }

    #[test]
    fn insert_front_preserves_order() {
        let mut chain = HttpHandlerChain::new();
        let handler_a = handler();
        let handler_b = handler();
        let handler_c = handler();

        chain.insert_front(Some(Rc::clone(&handler_a)));
        chain.insert_front(Some(Rc::clone(&handler_b)));
        chain.insert_front(Some(Rc::clone(&handler_c)));

        assert_same(chain.get_front(), &handler_c);
        assert_same(chain.get_back(), &handler_a);
        assert_next(&handler_c, &handler_b);
        assert_next(&handler_b, &handler_a);
        assert!(handler_a.get_next_handler().is_none());
    }

    #[test]
    fn insert_back_preserves_order() {
        let mut chain = HttpHandlerChain::new();
        let handler_a = handler();
        let handler_b = handler();
        let handler_c = handler();

        chain.insert_back(Some(Rc::clone(&handler_a)));
        chain.insert_back(Some(Rc::clone(&handler_b)));
        chain.insert_back(Some(Rc::clone(&handler_c)));

        assert_same(chain.get_front(), &handler_a);
        assert_same(chain.get_back(), &handler_c);
        assert_next(&handler_a, &handler_b);
        assert_next(&handler_b, &handler_c);
        assert!(handler_c.get_next_handler().is_none());
    }

    #[test]
    fn mixed_insert_preserves_order() {
        let mut chain = HttpHandlerChain::new();
        let handler_a = handler();
        let handler_b = handler();
        let handler_c = handler();
        let handler_d = handler();

        chain.insert_front(Some(Rc::clone(&handler_a)));
        chain.insert_back(Some(Rc::clone(&handler_b)));
        chain.insert_front(Some(Rc::clone(&handler_c)));
        chain.insert_back(Some(Rc::clone(&handler_d)));

        assert_same(chain.get_front(), &handler_c);
        assert_same(chain.get_back(), &handler_d);
        assert_next(&handler_c, &handler_a);
        assert_next(&handler_a, &handler_b);
        assert_next(&handler_b, &handler_d);
        assert!(handler_d.get_next_handler().is_none());
    }

    #[test]
    fn insert_front_none_is_noop() {
        let mut chain = HttpHandlerChain::new();
        let only = handler();
        chain.insert_front(Some(Rc::clone(&only)));
        chain.insert_front(None);

        assert_same(chain.get_front(), &only);
        assert_same(chain.get_back(), &only);
        assert!(only.get_next_handler().is_none());
    }

    #[test]
    fn insert_back_none_is_noop() {
        let mut chain = HttpHandlerChain::new();
        let only = handler();
        chain.insert_back(Some(Rc::clone(&only)));
        chain.insert_back(None);

        assert_same(chain.get_front(), &only);
        assert_same(chain.get_back(), &only);
        assert!(only.get_next_handler().is_none());
    }

    #[derive(Default)]
    struct SourceCounts {
        stops: Cell<usize>,
        drops: Cell<usize>,
    }

    struct ObservedSource {
        counts: Rc<SourceCounts>,
    }

    impl HttpSource for ObservedSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"test source has no header event",
            ))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"test source has no body event",
            ))))
        }

        fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
            self.counts.stops.set(self.counts.stops.get() + 1);
        }
    }

    impl Drop for ObservedSource {
        fn drop(&mut self) {
            self.counts.drops.set(self.counts.drops.get() + 1);
        }
    }

    struct ReadyWithSource {
        source: Option<HttpSourceHolder>,
        future_dropped: Rc<Cell<bool>>,
    }

    impl Future for ReadyWithSource {
        type Output = Result<HttpSourceHolder, HttpError>;

        fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(self.source.take().expect("ready future polled twice")))
        }
    }

    impl Drop for ReadyWithSource {
        fn drop(&mut self) {
            self.future_dropped.set(true);
        }
    }

    struct PendingWithSource {
        _source: HttpSourceHolder,
        future_dropped: Rc<Cell<bool>>,
    }

    impl Future for PendingWithSource {
        type Output = Result<HttpSourceHolder, HttpError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingWithSource {
        fn drop(&mut self) {
            self.future_dropped.set(true);
        }
    }

    enum OwnershipMode {
        Ready,
        Pending,
    }

    struct OwnershipHandler {
        future_dropped: Rc<Cell<bool>>,
        mode: OwnershipMode,
    }

    impl HttpChainHandlerHooks for OwnershipHandler {
        fn handle_request(
            &self,
            _next_handler: Option<HttpHandlerRef>,
            _context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            match self.mode {
                OwnershipMode::Ready => Box::pin(ReadyWithSource {
                    source: Some(request_source),
                    future_dropped: Rc::clone(&self.future_dropped),
                }),
                OwnershipMode::Pending => Box::pin(PendingWithSource {
                    _source: request_source,
                    future_dropped: Rc::clone(&self.future_dropped),
                }),
            }
        }
    }

    fn context(owner: &KeepAliveOwner<()>) -> HandlerSessionContext {
        HandlerSessionContext::acquire(owner).unwrap()
    }

    fn poll_handler(future: &mut HandlerFuture) -> Poll<Result<HttpSourceHolder, HttpError>> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Future::poll(Pin::new(future), &mut context)
    }

    fn poll_future<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Future::poll(future, &mut context)
    }

    #[test]
    fn handler_transfers_request_to_response_and_releases_context_on_completion() {
        let mut owner = KeepAliveOwner::new(());
        let future_dropped = Rc::new(Cell::new(false));
        let source_counts = Rc::new(SourceCounts::default());
        let handler = HttpChainHandler::new(OwnershipHandler {
            future_dropped: Rc::clone(&future_dropped),
            mode: OwnershipMode::Ready,
        });
        let request_source = HttpSourceHolder::new(Box::new(ObservedSource {
            counts: Rc::clone(&source_counts),
        }));
        let mut invocation = handler.handle_request(context(&owner), request_source);
        let mut finalize = Box::pin(owner.finalize());
        assert!(poll_future(finalize.as_mut()).is_pending());

        let response_source = match poll_handler(&mut invocation) {
            Poll::Ready(Ok(response_source)) => response_source,
            Poll::Ready(Err(error)) => panic!("unexpected handler error: {error}"),
            Poll::Pending => panic!("handler response unexpectedly pending"),
        };
        assert!(future_dropped.get());
        assert!(poll_future(finalize.as_mut()).is_ready());
        assert_eq!(source_counts.stops.get(), 0);
        assert_eq!(source_counts.drops.get(), 0);

        drop(response_source);
        assert_eq!(source_counts.stops.get(), 1);
        assert_eq!(source_counts.drops.get(), 1);
        drop(finalize);
        drop(owner);
    }

    struct ForwardingHandler {
        name: &'static str,
        effects: Rc<RefCell<Vec<&'static str>>>,
        counters: Option<Arc<ResponseCounters>>,
    }

    impl HttpChainHandlerHooks for ForwardingHandler {
        fn handle_request(
            &self,
            next_handler: Option<HttpHandlerRef>,
            context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let name = self.name;
            let effects = Rc::clone(&self.effects);
            let counters = self.counters.clone();
            Box::pin(async move {
                if let Some(counters) = counters {
                    assert!(context.response_proposer().unwrap().propose(counters, 201));
                }
                effects.borrow_mut().push(match name {
                    "front" => "front-enter",
                    "back" => "back-enter",
                    _ => "handler-enter",
                });
                let result = match next_handler {
                    Some(next_handler) => {
                        next_handler.handle_request(context, request_source).await
                    }
                    None => Ok(request_source),
                };
                effects.borrow_mut().push(match name {
                    "front" => "front-exit",
                    "back" => "back-exit",
                    _ => "handler-exit",
                });
                result
            })
        }
    }

    #[test]
    fn stored_successor_is_invoked_in_chain_order() {
        let mut owner = KeepAliveOwner::new(());
        let effects = Rc::new(RefCell::new(Vec::new()));
        let counters = Arc::new(ResponseCounters::default());
        let owner_invocation = Rc::new(HandlerInvocation::new());
        let invocation_lifetime = Rc::downgrade(&owner_invocation);
        let proposer = owner_invocation.response_proposer();
        let front = Rc::new(HttpChainHandler::new(ForwardingHandler {
            name: "front",
            effects: Rc::clone(&effects),
            counters: None,
        }));
        let back = Rc::new(HttpChainHandler::new(ForwardingHandler {
            name: "back",
            effects: Rc::clone(&effects),
            counters: Some(Arc::clone(&counters)),
        }));
        let mut chain = HttpHandlerChain::new();
        chain.insert_back(Some(Rc::clone(&front)));
        chain.insert_back(Some(Rc::clone(&back)));
        let chain_front = chain.get_front().unwrap();
        let context = context(&owner)
            .with_response_proposer(proposer.clone())
            .unwrap();
        let other = Rc::new(HandlerInvocation::new());
        assert!(
            context
                .clone()
                .with_response_proposer(other.response_proposer())
                .is_err()
        );
        let mut invocation = chain_front.handle_request(context, HttpSourceHolder::empty());
        let mut finalize = Box::pin(owner.finalize());
        assert!(poll_future(finalize.as_mut()).is_pending());

        let response = match poll_handler(&mut invocation) {
            Poll::Ready(Ok(response)) => response,
            Poll::Ready(Err(error)) => panic!("unexpected handler error: {error}"),
            Poll::Pending => panic!("handler chain unexpectedly pending"),
        };
        assert_eq!(
            &*effects.borrow(),
            &["front-enter", "back-enter", "back-exit", "front-exit"]
        );
        assert!(poll_future(finalize.as_mut()).is_ready());
        assert!(!proposer.propose(Arc::clone(&counters), 500));
        owner_invocation.seal_response_proposal().unwrap().accept();
        assert_eq!(counters.snapshot(), [1, 0, 1, 0, 0, 0]);
        assert!(!proposer.propose(Arc::clone(&counters), 500));
        drop(owner_invocation);
        assert!(invocation_lifetime.upgrade().is_none());
        assert!(!proposer.propose(Arc::clone(&counters), 500));
        drop(response);
        drop(invocation);
        drop(finalize);
        drop(owner);
    }

    struct ErrorHandler;

    impl HttpChainHandlerHooks for ErrorHandler {
        fn handle_request(
            &self,
            _next_handler: Option<HttpHandlerRef>,
            _context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move {
                drop(request_source);
                Err(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"handler deadline",
                ))
            })
        }
    }

    #[test]
    fn error_completion_preserves_identity_and_releases_context() {
        let mut owner = KeepAliveOwner::new(());
        let handler = HttpChainHandler::new(ErrorHandler);
        let mut invocation = handler.handle_request(context(&owner), HttpSourceHolder::empty());
        let mut finalize = Box::pin(owner.finalize());
        assert!(poll_future(finalize.as_mut()).is_pending());

        let error = match poll_handler(&mut invocation) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("failing handler returned a response source"),
            Poll::Pending => panic!("failing handler unexpectedly pending"),
        };
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"handler deadline");
        assert!(poll_future(finalize.as_mut()).is_ready());
        drop(invocation);
        drop(finalize);
        drop(owner);
    }

    #[test]
    fn dropping_pending_handler_destroys_request_before_context() {
        let mut owner = KeepAliveOwner::new(());
        let future_dropped = Rc::new(Cell::new(false));
        let source_counts = Rc::new(SourceCounts::default());
        let handler = HttpChainHandler::new(OwnershipHandler {
            future_dropped: Rc::clone(&future_dropped),
            mode: OwnershipMode::Pending,
        });
        let request_source = HttpSourceHolder::new(Box::new(ObservedSource {
            counts: Rc::clone(&source_counts),
        }));
        let mut invocation = handler.handle_request(context(&owner), request_source);
        assert!(matches!(poll_handler(&mut invocation), Poll::Pending));
        let mut finalize = Box::pin(owner.finalize());
        assert!(poll_future(finalize.as_mut()).is_pending());

        drop(invocation);
        assert!(future_dropped.get());
        assert_eq!(source_counts.stops.get(), 1);
        assert_eq!(source_counts.drops.get(), 1);
        assert!(poll_future(finalize.as_mut()).is_ready());
        drop(finalize);
        drop(owner);
    }

    struct DropHandler {
        drops: Rc<Cell<usize>>,
    }

    impl HttpChainHandlerHooks for DropHandler {
        fn handle_request(
            &self,
            _next_handler: Option<HttpHandlerRef>,
            _context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move { Ok(request_source) })
        }
    }

    impl Drop for DropHandler {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn links_retain_successors_after_chain_drop() {
        let drops = Rc::new(Cell::new(0));
        let front = Rc::new(HttpChainHandler::new(DropHandler {
            drops: Rc::clone(&drops),
        }));
        let back = Rc::new(HttpChainHandler::new(DropHandler {
            drops: Rc::clone(&drops),
        }));
        let mut chain = HttpHandlerChain::new();
        chain.insert_back(Some(Rc::clone(&front)));
        chain.insert_back(Some(Rc::clone(&back)));
        drop(back);

        drop(chain);
        assert_eq!(drops.get(), 0);
        assert!(front.get_next_handler().is_some());

        drop(front);
        assert_eq!(drops.get(), 2);
    }
}
