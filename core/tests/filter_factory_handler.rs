/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod filter_factory {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use roxygen_runtime::keep_alive::KeepAliveOwner;

    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::events::HttpBodyEvent;
    use crate::events::HttpHeaderEvent;
    use crate::filter_factory::FilterFactory;
    use crate::filter_factory::FilterPair;
    use crate::filter_factory::HttpFilterFactoryHandler;
    use crate::fixed_source::HttpFixedSource;
    use crate::handler::HandlerFuture;
    use crate::handler::HandlerSessionContext;
    use crate::handler::HttpHandler;
    use crate::handler::LocalHandlerFuture;
    use crate::handler_chain::HttpChainHandler;
    use crate::handler_chain::HttpHandlerRef;
    use crate::source::HttpSource;
    use crate::source::LocalSourceFuture;
    use crate::source_filter::HttpSourceFilter;
    use crate::source_filter::HttpSourceFilterHooks;
    use crate::source_filter::MutateFilter;
    use crate::source_holder::HttpSourceHolder;

    #[derive(Clone, Copy)]
    enum HandlerOutcome {
        Response,
        Empty,
        Error,
    }

    struct RecordingHandler {
        expected_request_filter: &'static [u8],
        outcome: HandlerOutcome,
        calls: Rc<Cell<usize>>,
    }

    impl HttpHandler for RecordingHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            mut request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let expected_request_filter = self.expected_request_filter;
            let outcome = self.outcome;
            let calls = Rc::clone(&self.calls);
            Box::pin(async move {
                let _context = context;
                calls.set(calls.get() + 1);
                let request = request_source.read_header_event().await?;
                assert_eq!(
                    request
                        .headers()
                        .headers()
                        .get_single_or_empty(b"req-filter"),
                    expected_request_filter
                );
                drop(request_source);
                match outcome {
                    HandlerOutcome::Response => Ok(HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_fixed_response(200, None)
                            .expect("the fixed 200 response is valid"),
                    ))),
                    HandlerOutcome::Empty => Ok(HttpSourceHolder::empty()),
                    HandlerOutcome::Error => Err(HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"handler deadline",
                    )),
                }
            })
        }
    }

    struct AppendFilterNameFactory {
        request: Option<&'static [u8]>,
        response: Option<&'static [u8]>,
    }

    impl FilterFactory for AppendFilterNameFactory {
        fn make_filters(&self) -> FilterPair {
            (
                self.request
                    .map(|suffix| append_filter(b"req-filter", suffix)),
                self.response
                    .map(|suffix| append_filter(b"res-filter", suffix)),
            )
        }
    }

    fn append_filter(name: &'static [u8], suffix: &'static [u8]) -> HttpSourceFilter {
        MutateFilter::new(
            Some(Box::new(move |event| {
                let headers = event.headers_mut().headers_mut();
                let mut value = headers.get_single_or_empty(name).to_vec();
                value.extend_from_slice(suffix);
                headers.set(name, value);
            })),
            None,
        )
        .into_filter()
    }

    fn append_factory(
        request: Option<&'static [u8]>,
        response: Option<&'static [u8]>,
    ) -> Rc<dyn FilterFactory> {
        Rc::new(AppendFilterNameFactory { request, response })
    }

    struct FactoryCreationProbe {
        name: &'static str,
        created: Rc<RefCell<Vec<&'static str>>>,
        inner: AppendFilterNameFactory,
    }

    impl FilterFactory for FactoryCreationProbe {
        fn make_filters(&self) -> FilterPair {
            self.created.borrow_mut().push(self.name);
            self.inner.make_filters()
        }
    }

    fn recorded_append_factory(
        name: &'static str,
        created: &Rc<RefCell<Vec<&'static str>>>,
        request: Option<&'static [u8]>,
        response: Option<&'static [u8]>,
    ) -> Rc<dyn FilterFactory> {
        Rc::new(FactoryCreationProbe {
            name,
            created: Rc::clone(created),
            inner: AppendFilterNameFactory { request, response },
        })
    }

    fn configured_handler(
        factories: impl IntoIterator<Item = Rc<dyn FilterFactory>>,
        next: HttpHandlerRef,
    ) -> HttpChainHandler {
        let mut hooks = HttpFilterFactoryHandler::new();
        hooks.add_filter_factory(None);
        for factory in factories {
            hooks.add_filter_factory(Some(factory));
        }
        let handler = hooks.into_chain_handler();
        handler.set_next_handler(Some(next));
        handler
    }

    fn fixed_request() -> HttpSourceHolder {
        HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_get_request(b"/", None).expect("the fixed GET request is valid"),
        ))
    }

    fn invoke(
        handler: &HttpChainHandler,
        request: HttpSourceHolder,
    ) -> Result<HttpSourceHolder, HttpError> {
        let mut owner = KeepAliveOwner::new(());
        let context = HandlerSessionContext::acquire(&owner).expect("the owner is accepting work");
        let mut invocation = handler.handle_request(context, request);
        let result = ready_handler(&mut invocation);
        drop(invocation);

        let mut finalize = Box::pin(owner.finalize());
        assert!(poll_future(finalize.as_mut()).is_ready());
        drop(finalize);
        drop(owner);
        result
    }

    fn ready_handler(future: &mut HandlerFuture) -> Result<HttpSourceHolder, HttpError> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match Future::poll(Pin::new(future), &mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("the fixture handler unexpectedly suspended"),
        }
    }

    fn ready_source<T>(future: LocalSourceFuture<T>) -> T {
        let mut future = future;
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("the fixture source unexpectedly suspended"),
        }
    }

    fn poll_future<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Future::poll(future, &mut context)
    }

    fn response_message(response: &mut HttpSourceHolder) -> crate::message::HttpMessage {
        match ready_source(response.read_header_event()) {
            Ok(event) => event.into_headers(),
            Err(error) => panic!("expected response headers, got {error}"),
        }
    }

    #[test]
    fn empty_chain() {
        let calls = Rc::new(Cell::new(0));
        let next: HttpHandlerRef = Rc::new(RecordingHandler {
            expected_request_filter: b"",
            outcome: HandlerOutcome::Response,
            calls: Rc::clone(&calls),
        });
        let handler = configured_handler([], next);

        let mut response = invoke(&handler, fixed_request()).expect("handler succeeds");
        let mut message = response_message(&mut response);
        assert_eq!(message.status_code(), Ok(200));
        assert!(!message.headers().exists(b"res-filter"));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn request_forward_response_reverse_order() {
        let calls = Rc::new(Cell::new(0));
        let created = Rc::new(RefCell::new(Vec::new()));
        let next: HttpHandlerRef = Rc::new(RecordingHandler {
            expected_request_filter: b"reqFilterA,reqFilterB,",
            outcome: HandlerOutcome::Response,
            calls: Rc::clone(&calls),
        });
        let handler = configured_handler(
            [
                recorded_append_factory("A", &created, Some(b"reqFilterA,"), Some(b"resFilterA,")),
                recorded_append_factory("B", &created, Some(b"reqFilterB,"), None),
                recorded_append_factory("C", &created, None, Some(b"resFilterC,")),
            ],
            next,
        );

        let mut response = invoke(&handler, fixed_request()).expect("handler succeeds");
        assert_eq!(&*created.borrow(), &["A", "B", "C"]);
        let message = response_message(&mut response);
        assert_eq!(
            message.headers().get_single_or_empty(b"res-filter"),
            b"resFilterC,resFilterA,"
        );
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn null_source_becomes_500_before_response_filters() {
        let calls = Rc::new(Cell::new(0));
        let next: HttpHandlerRef = Rc::new(RecordingHandler {
            expected_request_filter: b"reqFilterA,reqFilterB,",
            outcome: HandlerOutcome::Empty,
            calls: Rc::clone(&calls),
        });
        let handler = configured_handler(
            [
                append_factory(Some(b"reqFilterA,"), Some(b"resFilterA,")),
                append_factory(Some(b"reqFilterB,"), None),
                append_factory(None, Some(b"resFilterC,")),
            ],
            next,
        );

        let mut response = invoke(&handler, fixed_request()).expect("fallback succeeds");
        let mut message = response_message(&mut response);
        assert_eq!(message.status_code(), Ok(500));
        assert!(!message.wants_keepalive());
        assert_eq!(
            message.headers().get_single_or_empty(b"res-filter"),
            b"resFilterC,resFilterA,"
        );
        assert_eq!(calls.get(), 1);
    }

    struct ErrorObserverHooks {
        name: &'static str,
        effects: Rc<RefCell<Vec<&'static str>>>,
    }

    impl HttpSourceFilterHooks for ErrorObserverHooks {
        fn filter_name(&self) -> &'static str {
            self.name
        }

        fn read_header_event(
            &mut self,
            downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
        ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            let name = self.name;
            let effects = Rc::clone(&self.effects);
            Box::pin(async move {
                let result = downstream.await;
                effects.borrow_mut().push(name);
                result
            })
        }
    }

    struct ErrorObserverFactory {
        name: &'static str,
        effects: Rc<RefCell<Vec<&'static str>>>,
    }

    impl FilterFactory for ErrorObserverFactory {
        fn make_filters(&self) -> FilterPair {
            (
                None,
                Some(HttpSourceFilter::new(ErrorObserverHooks {
                    name: self.name,
                    effects: Rc::clone(&self.effects),
                })),
            )
        }
    }

    #[test]
    fn handler_error_is_wrapped_before_response_filters() {
        let calls = Rc::new(Cell::new(0));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let next: HttpHandlerRef = Rc::new(RecordingHandler {
            expected_request_filter: b"",
            outcome: HandlerOutcome::Error,
            calls: Rc::clone(&calls),
        });
        let factories: [Rc<dyn FilterFactory>; 2] = [
            Rc::new(ErrorObserverFactory {
                name: "A",
                effects: Rc::clone(&effects),
            }),
            Rc::new(ErrorObserverFactory {
                name: "C",
                effects: Rc::clone(&effects),
            }),
        ];
        let handler = configured_handler(factories, next);

        let mut response = invoke(&handler, fixed_request())
            .expect("handler failures become readable error sources");
        let error = match ready_source(response.read_header_event()) {
            Err(error) => error,
            Ok(_) => panic!("handler failure became successful headers"),
        };
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"handler deadline");
        assert_eq!(&*effects.borrow(), &["C", "A"]);
        assert_eq!(calls.get(), 1);
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
                b"the cleanup fixture is not read",
            ))))
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"the cleanup fixture is not read",
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

    struct EmptyIgnoringHandler;

    impl HttpHandler for EmptyIgnoringHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move {
                let _context = context;
                drop(request_source);
                Ok(HttpSourceHolder::empty())
            })
        }
    }

    struct DropHooks {
        name: &'static str,
        drops: Rc<Cell<usize>>,
    }

    impl HttpSourceFilterHooks for DropHooks {
        fn filter_name(&self) -> &'static str {
            self.name
        }
    }

    impl Drop for DropHooks {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct DropFactory {
        request_drops: Rc<Cell<usize>>,
        response_drops: Rc<Cell<usize>>,
    }

    impl FilterFactory for DropFactory {
        fn make_filters(&self) -> FilterPair {
            (
                Some(HttpSourceFilter::new(DropHooks {
                    name: "RequestDrop",
                    drops: Rc::clone(&self.request_drops),
                })),
                Some(HttpSourceFilter::new(DropHooks {
                    name: "ResponseDrop",
                    drops: Rc::clone(&self.response_drops),
                })),
            )
        }
    }

    #[test]
    fn fallback_and_filters_cleanup_once() {
        let source_counts = Rc::new(SourceCounts::default());
        let request_drops = Rc::new(Cell::new(0));
        let response_drops = Rc::new(Cell::new(0));
        let factory: Rc<dyn FilterFactory> = Rc::new(DropFactory {
            request_drops: Rc::clone(&request_drops),
            response_drops: Rc::clone(&response_drops),
        });
        let next: HttpHandlerRef = Rc::new(EmptyIgnoringHandler);
        let handler = configured_handler([factory], next);
        let request = HttpSourceHolder::new(Box::new(ObservedSource {
            counts: Rc::clone(&source_counts),
        }));

        let response = invoke(&handler, request).expect("fallback succeeds");
        assert_eq!(source_counts.stops.get(), 1);
        assert_eq!(source_counts.drops.get(), 1);
        assert_eq!(request_drops.get(), 1);
        assert_eq!(response_drops.get(), 0);

        drop(response);
        assert_eq!(response_drops.get(), 1);
        assert_eq!(source_counts.stops.get(), 1);
        assert_eq!(source_counts.drops.get(), 1);
        drop(handler);
        assert_eq!(request_drops.get(), 1);
        assert_eq!(response_drops.get(), 1);
    }
}
