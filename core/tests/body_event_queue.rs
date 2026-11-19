/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod body_event_queue {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::future::poll_fn;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use bytes::Bytes;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::host::Host;
    use tokio::runtime::Builder;
    use tokio::time;

    use crate::body_event_queue::BodyEventQueueCallback;
    use crate::body_event_queue::BodyEventQueueMutationError;
    use crate::body_event_queue::HttpBodyEventQueue;
    use crate::body_event_queue::HttpBodySource;
    use crate::body_event_queue::HttpBodyTransfer;
    use crate::buf_queue::BufQueue;
    use crate::byte_event::ByteEventType;
    use crate::byte_event::HttpByteEvent;
    use crate::byte_event::HttpByteEventCallback;
    use crate::byte_event::HttpByteEventRegistration;
    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::events::HttpBodyEvent;
    use crate::events::HttpBodyEventKind;
    use crate::events::HttpHeaderEvent;
    use crate::events::ResumeStatus;
    use crate::headers::HttpHeaderCode;
    use crate::ids::StreamId;
    use crate::message::HttpMessage;
    use crate::source::HttpSource;
    use crate::source::LocalSourceFuture;
    use crate::source_holder::HttpSourceHolder;
    use crate::stream_source::HttpIngressBody;
    use crate::stream_source::HttpStreamSource;
    use crate::stream_source::HttpStreamSourceCallback;
    use crate::stream_source::HttpStreamSourceOwner;

    #[derive(Default)]
    struct QueueObservations {
        buffered_deltas: RefCell<Vec<i64>>,
        mismatches: RefCell<Vec<(u64, u64)>>,
    }

    impl BodyEventQueueCallback for QueueObservations {
        fn on_egress_bytes_buffered(&self, bytes: i64) {
            self.buffered_deltas.borrow_mut().push(bytes);
        }

        fn on_content_length_mismatch(&self, expected: u64, observed: u64) {
            self.mismatches.borrow_mut().push((expected, observed));
        }
    }

    #[derive(Default)]
    struct SourceObservations {
        stops: Cell<usize>,
        drops: Cell<usize>,
        stop_errors: RefCell<Vec<Option<HttpErrorCode>>>,
        body_maximums: RefCell<Vec<u32>>,
    }

    struct ScriptedSource {
        observations: Rc<SourceObservations>,
        headers: VecDeque<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
        bodies: VecDeque<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
    }

    impl ScriptedSource {
        fn new(observations: Rc<SourceObservations>) -> Self {
            Self {
                observations,
                headers: VecDeque::new(),
                bodies: VecDeque::new(),
            }
        }

        fn with_header(mut self, event: Result<HttpHeaderEvent, HttpError>) -> Self {
            self.headers.push_back(Box::pin(std::future::ready(event)));
            self
        }

        fn with_header_future(
            mut self,
            event: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
        ) -> Self {
            self.headers.push_back(event);
            self
        }

        fn with_body(mut self, event: Result<HttpBodyEvent, HttpError>) -> Self {
            self.bodies.push_back(Box::pin(std::future::ready(event)));
            self
        }

        fn with_body_future(
            mut self,
            event: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
        ) -> Self {
            self.bodies.push_back(event);
            self
        }
    }

    impl HttpSource for ScriptedSource {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.headers.pop_front().unwrap_or_else(|| {
                Box::pin(std::future::ready(Err(HttpError::with_message(
                    HttpErrorCode::DROPPED,
                    b"header script exhausted",
                ))))
            })
        }

        fn read_body_event(
            &mut self,
            maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.observations.body_maximums.borrow_mut().push(maximum);
            self.bodies.pop_front().unwrap_or_else(|| {
                Box::pin(std::future::ready(Err(HttpError::with_message(
                    HttpErrorCode::DROPPED,
                    b"body script exhausted",
                ))))
            })
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.observations
                .stops
                .set(self.observations.stops.get() + 1);
            self.observations.stop_errors.borrow_mut().push(error);
        }
    }

    impl Drop for ScriptedSource {
        fn drop(&mut self) {
            self.observations
                .drops
                .set(self.observations.drops.get() + 1);
        }
    }

    struct ManualCompletion<T> {
        result: Rc<RefCell<Option<T>>>,
    }

    impl<T> ManualCompletion<T> {
        fn complete(&self, result: T) {
            assert!(self.result.borrow_mut().replace(result).is_none());
        }
    }

    struct ManualFuture<T> {
        result: Rc<RefCell<Option<T>>>,
    }

    impl<T> Future for ManualFuture<T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.result
                .borrow_mut()
                .take()
                .map_or(Poll::Pending, Poll::Ready)
        }
    }

    fn manual_future<T: 'static>() -> (ManualCompletion<T>, LocalSourceFuture<T>) {
        let result = Rc::new(RefCell::new(None));
        (
            ManualCompletion {
                result: Rc::clone(&result),
            },
            Box::pin(ManualFuture { result }),
        )
    }

    fn body(bytes: &[u8], eom: bool) -> HttpBodyEvent {
        HttpBodyEvent::body(
            BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap(),
            eom,
        )
    }

    fn response(status: u16, content_length: Option<&[u8]>, eom: bool) -> HttpHeaderEvent {
        let mut message = HttpMessage::new();
        message.set_status_code(status).unwrap();
        if let Some(content_length) = content_length {
            message
                .headers_mut()
                .add(HttpHeaderCode::ContentLength, content_length);
        }
        HttpHeaderEvent::new(message, eom).unwrap()
    }

    fn queue(
        source: ScriptedSource,
        callback: &Rc<QueueObservations>,
        limit: usize,
        timeout: Duration,
    ) -> HttpBodyEventQueue {
        HttpBodyEventQueue::from_source_with_configuration(
            HttpSourceHolder::new(Box::new(source)),
            StreamId::new(1),
            callback,
            limit,
            timeout,
            Host::tokio(),
        )
    }

    fn ready<T>(future: LocalSourceFuture<T>) -> T {
        let mut future = future;
        match poll_once(future.as_mut()) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly remained pending"),
        }
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    fn run_paused(test: impl Future<Output = ()>) {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                time::pause();
                test.await;
            });
    }

    async fn wait_for_capacity_commit(
        queue: &HttpBodyEventQueue,
        mut future: Pin<
            &mut (
                     dyn Future<Output = Result<crate::body_event_queue::ReadBodyResult, HttpError>>
                         + 'static
                 ),
        >,
    ) {
        poll_fn(|context| {
            assert!(
                future.as_mut().poll(context).is_pending(),
                "timeout must commit before the queue read resumes"
            );
            if queue.capacity_wait_ready_for_test() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    #[test]
    fn uncancelled_header_read_succeeds_and_drop_stops_source() {
        let source_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(Rc::clone(&source_observations))
            .with_header(Ok(response(200, None, false)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        assert!(ready(queue.read_header_event()).is_ok());
        drop(queue);
        assert_eq!(source_observations.stops.get(), 1);
        assert_eq!(source_observations.drops.get(), 1);
    }

    #[test]
    fn dropping_an_unpolled_header_read_does_not_consume_the_header() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(201, None, false)))
            .with_header(Ok(response(202, None, false)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        let unpolled = queue.read_header_event();
        drop(unpolled);

        let mut event = ready(queue.read_header_event()).unwrap();
        assert_eq!(event.headers_mut().status_code().unwrap(), 201);
    }

    #[test]
    fn adopting_a_holder_preserves_its_pending_read() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_body_future(pending)
            .with_body(Ok(body(b"queued", true)));
        let mut holder = HttpSourceHolder::new(Box::new(source));
        let mut pending = holder.read_body_event(100);
        assert!(poll_once(pending.as_mut()).is_pending());

        let mut queue = HttpBodyEventQueue::from_source_with_configuration(
            holder,
            StreamId::new(1),
            &callback,
            100,
            Duration::ZERO,
            Host::tokio(),
        );
        queue.skip_content_length_validation();
        let error = ready(queue.read_body_event(100, CancellationToken::never()))
            .err()
            .expect("the adopted holder still has an active read");
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 0);

        completion.complete(Ok(body(b"pending", false)));
        let event = ready(pending).unwrap();
        assert_eq!(
            event.as_body().unwrap().coalesce(7).unwrap(),
            b"pending"[..]
        );
        assert!(queue.empty());
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        let event = queue.dequeue_body_event(100).unwrap();
        assert_eq!(event.as_body().unwrap().coalesce(6).unwrap(), b"queued"[..]);
        drop(queue);
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);
    }

    #[test]
    fn transfer_adoption_preserves_an_already_pending_holder_read() {
        let observations = Rc::new(SourceObservations::default());
        let (completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_body_future(pending)
            .with_body(Ok(body(b"transferred", true)));
        let mut holder = HttpSourceHolder::new(Box::new(source));
        let mut pending = holder.read_body_event(100);
        assert!(poll_once(pending.as_mut()).is_pending());
        let mut transfer = HttpBodyTransfer::new(holder, StreamId::new(1), 100);
        let mut context = Context::from_waker(Waker::noop());
        let cancellation = CancellationToken::never();
        let Poll::Ready(Err(error)) =
            transfer.poll_read_body_event(&mut context, 100, &cancellation)
        else {
            panic!("transfer adoption must preserve holder admission");
        };
        assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 0);
        assert_eq!(observations.body_maximums.borrow().as_slice(), &[100]);

        completion.complete(Ok(body(b"pending", false)));
        let event = ready(pending).unwrap();
        assert_eq!(
            event.as_body().unwrap().coalesce(7).unwrap(),
            b"pending"[..]
        );
        let Poll::Ready(Ok(event)) =
            transfer.poll_read_body_event(&mut context, 100, &cancellation)
        else {
            panic!("the next event transfers directly after holder admission is released");
        };
        assert!(event.eom());
        assert_eq!(
            event.as_body().unwrap().coalesce(11).unwrap(),
            b"transferred"[..]
        );
        drop(transfer);
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);
    }

    #[test]
    fn concrete_body_transfer_preserves_lazy_admission_and_terminal_errors() {
        for convert in [false, true] {
            for (trailers, error) in [
                (false, None),
                (true, None),
                (false, Some(HttpErrorCode::READ_TIMEOUT)),
            ] {
                let source = HttpStreamSource::new_async(Some(StreamId::new(1)), None, 8).unwrap();
                let producer = source.producer();
                let receipt = source.ingress_completion().unwrap();
                let mut head = HttpMessage::new();
                head.set_status_code(200).unwrap();
                assert!(source.initialize_body_only(&mut head, false));
                let mut initial = source.read_body_event_unboxed(2, CancellationToken::never());
                let mut capacity_probe =
                    source.read_body_event_unboxed(2, CancellationToken::never());
                let input = HttpBodySource::from(HttpStreamSourceOwner::new(source));
                assert!(input.readable());
                let input = if convert {
                    input.into_source_holder().into()
                } else {
                    input
                };
                assert!(receipt.snapshot().is_none());
                let mut transfer = HttpBodyTransfer::new(input, StreamId::new(1), 2);
                let mut context = Context::from_waker(Waker::noop());
                let cancellation = CancellationToken::never();

                producer.body(BufQueue::from_vec(b"ab".to_vec(), 2).unwrap(), 0, false);
                let Poll::Ready(Ok(event)) = poll_once(Pin::new(&mut initial)) else {
                    panic!("constructing a transfer must not admit its body read");
                };
                assert_eq!(
                    event.as_body().unwrap().coalesce(2).unwrap().as_ref(),
                    b"ab"
                );
                drop(event);
                drop(initial);
                producer.body(BufQueue::from_vec(b"cdef".to_vec(), 4).unwrap(), 0, false);
                let Poll::Ready(Ok(transferred)) =
                    transfer.poll_read_body_event(&mut context, 8, &cancellation)
                else {
                    panic!("the available event transfers with the configured read bound");
                };
                assert!(!transferred.eom());
                assert_eq!(producer.body_bytes_buffered(), Some(2));
                let Poll::Ready(Ok(event)) = poll_once(Pin::new(&mut capacity_probe)) else {
                    panic!("returning an event must not eagerly reserve another source read");
                };
                assert_eq!(
                    event.as_body().unwrap().coalesce(2).unwrap().as_ref(),
                    b"ef"
                );
                drop(event);
                drop(capacity_probe);
                assert_eq!(
                    transferred.as_body().unwrap().coalesce(2).unwrap().as_ref(),
                    b"cd"
                );
                drop(transferred);

                let Poll::Ready(Ok(result)) =
                    transfer.poll_read_body_event(&mut context, 2, &cancellation)
                else {
                    panic!("an empty stream first yields Suspend");
                };
                let HttpBodyEventKind::Suspend(resume) = result.into_kind() else {
                    panic!("initial empty input returns the original Suspend event");
                };
                drop(resume);
                assert!(
                    transfer
                        .poll_read_body_event(&mut context, 2, &cancellation)
                        .is_pending()
                );
                if let Some(error) = error {
                    assert!(producer.abort(error, b"upload failed"));
                } else if trailers {
                    let mut fields = crate::headers::HttpHeaders::new();
                    fields.add("x-last", "yes");
                    assert!(producer.trailers(fields));
                } else {
                    producer.body(BufQueue::from_vec(b"g".to_vec(), 1).unwrap(), 0, true);
                }
                match transfer.poll_read_body_event(&mut context, 2, &cancellation) {
                    Poll::Ready(Err(actual)) => assert_eq!(Some(actual.code()), error),
                    Poll::Ready(Ok(event)) => {
                        assert!(error.is_none());
                        assert!(event.eom());
                        if trailers {
                            let HttpBodyEventKind::Trailers(fields) = event.kind() else {
                                panic!("terminal trailers remain intact")
                            };
                            assert_eq!(fields.get_single_or_empty("x-last"), b"yes");
                        } else {
                            assert_eq!(
                                event.as_body().unwrap().coalesce(1).unwrap().as_ref(),
                                b"g"
                            );
                        }
                    }
                    Poll::Pending => panic!("producer completion wakes the admitted read"),
                }
                assert!(
                    !producer.is_live(),
                    "terminal ownership retires before returning"
                );
                assert_eq!(
                    receipt.snapshot().unwrap().err().map(|error| error.code()),
                    error
                );
                let Poll::Ready(Err(error)) =
                    transfer.poll_read_body_event(&mut context, 2, &cancellation)
                else {
                    panic!("retired input remains a readable error result");
                };
                assert_eq!(error.code(), HttpErrorCode::DROPPED);
                assert_eq!(error.message(), b"HTTP source is no longer readable");
            }
        }

        let source = HttpStreamSource::new_async(None, None, 1).unwrap();
        let mut owner = HttpStreamSourceOwner::new(source);
        owner.clear(None);
        for input in [
            HttpBodySource::from(owner),
            HttpSourceHolder::empty().into(),
        ] {
            assert!(!input.readable());
            let mut transfer = HttpBodyTransfer::new(input, StreamId::new(1), 1);
            let mut context = Context::from_waker(Waker::noop());
            let Poll::Ready(Err(error)) =
                transfer.poll_read_body_event(&mut context, 1, &CancellationToken::never())
            else {
                panic!("initially empty input reports an ordinary source error");
            };
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(error.message(), b"HTTP source is no longer readable");
        }
    }

    #[test]
    fn concrete_body_transfer_preserves_cancellation_and_unwind_order() {
        struct Payload(Arc<std::sync::Mutex<Vec<&'static str>>>);
        impl AsRef<[u8]> for Payload {
            fn as_ref(&self) -> &[u8] {
                b"x"
            }
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push("body.drop");
            }
        }
        struct Callback {
            log: Arc<std::sync::Mutex<Vec<&'static str>>>,
            producer: crate::stream_source::HttpStreamSourceProducer,
            panic_on_credit: bool,
            credit_panicked: bool,
            first: Arc<()>,
            cleanup: Arc<()>,
        }
        impl HttpStreamSourceCallback for Callback {
            fn bytes_processed(&mut self, _: StreamId, _: usize, _: usize) {
                self.log.lock().unwrap().push("credit");
                if self.panic_on_credit {
                    assert!(
                        self.producer
                            .stop_reading(Some(HttpErrorCode::READ_TIMEOUT))
                    );
                    self.credit_panicked = true;
                    panic_any(Arc::clone(&self.first));
                }
            }
            fn source_complete(&mut self, _: StreamId, _error: Option<HttpError>) {
                self.log.lock().unwrap().push("complete");
                if self.credit_panicked {
                    panic_any(Arc::clone(&self.cleanup));
                }
            }
        }
        impl Drop for Callback {
            fn drop(&mut self) {
                self.log
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push("callback.drop");
            }
        }

        for (token_cancel, panic_on_credit) in [(false, false), (true, false), (false, true)] {
            let source = HttpStreamSource::new_async(Some(StreamId::new(1)), None, 4).unwrap();
            let producer = source.producer();
            let receipt = source.ingress_completion().unwrap();
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let first = Arc::new(());
            source.set_callback(Some(Box::new(Callback {
                log: Arc::clone(&log),
                producer: producer.clone(),
                panic_on_credit,
                credit_panicked: false,
                first: Arc::clone(&first),
                cleanup: Arc::new(()),
            })));
            let mut head = HttpMessage::new();
            head.set_status_code(200).unwrap();
            assert!(source.initialize_body_only(&mut head, false));
            let mut transfer =
                HttpBodyTransfer::new(HttpStreamSourceOwner::new(source), StreamId::new(1), 2);
            let cancellation = CancellationSource::new();
            let mut context = Context::from_waker(Waker::noop());
            let mut pending_event = None;
            if !panic_on_credit {
                producer.body(
                    BufQueue::from_bytes(Bytes::from_owner(Payload(Arc::clone(&log))), 1).unwrap(),
                    0,
                    false,
                );
                let Poll::Ready(Ok(event)) =
                    transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                else {
                    panic!("the ready body event transfers to its caller");
                };
                assert!(!event.eom());
                pending_event = Some(event);
                assert_eq!(*log.lock().unwrap(), ["credit"]);
                log.lock().unwrap().clear();
            }
            let Poll::Ready(Ok(result)) =
                transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
            else {
                panic!("initial empty input returns Suspend");
            };
            let HttpBodyEventKind::Suspend(resume) = result.into_kind() else {
                panic!("initial empty input returns the original Suspend event");
            };
            drop(resume);
            assert!(
                transfer
                    .poll_read_body_event(&mut context, 1, &cancellation.token())
                    .is_pending()
            );

            if panic_on_credit {
                producer.body(
                    BufQueue::from_bytes(Bytes::from_owner(Payload(Arc::clone(&log))), 1).unwrap(),
                    0,
                    false,
                );
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = transfer.poll_read_body_event(&mut context, 1, &cancellation.token());
                }))
                .expect_err("the original credit callback panic must escape");
                assert!(Arc::ptr_eq(
                    panic.downcast_ref::<Arc<()>>().unwrap(),
                    &first
                ));
                assert_eq!(*log.lock().unwrap(), ["credit", "body.drop", "complete"]);
                assert!(producer.is_live());
                let Poll::Ready(Err(error)) =
                    transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                else {
                    panic!("retained transfer releases admission before retrying the source error");
                };
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert!(!producer.is_live());
                assert_eq!(
                    *log.lock().unwrap(),
                    ["credit", "body.drop", "complete", "callback.drop"]
                );
            } else {
                if token_cancel {
                    cancellation.request_cancellation();
                    let Poll::Ready(Err(error)) =
                        transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                    else {
                        panic!("Pending cancellation must release its read");
                    };
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert!(receipt.snapshot().is_none());
                    assert!(producer.is_live());
                }
                let retirement =
                    transfer.prepare_stop_and_clear(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                assert!(log.lock().unwrap().is_empty());
                assert!(receipt.snapshot().is_none());
                assert!(retirement.retire().is_ok());
                if token_cancel {
                    assert_eq!(*log.lock().unwrap(), ["complete", "callback.drop"]);
                    assert!(!producer.is_live());
                } else {
                    assert!(log.lock().unwrap().is_empty());
                    assert!(
                        producer.is_live(),
                        "the admitted read still owns source completion"
                    );
                }
                transfer.drop_pending_read();
                assert!(!producer.is_live());
                assert_eq!(*log.lock().unwrap(), ["complete", "callback.drop"]);
                drop(pending_event.take());
                assert_eq!(
                    *log.lock().unwrap(),
                    ["complete", "callback.drop", "body.drop"]
                );
            }
            assert_eq!(
                receipt.snapshot().unwrap().unwrap_err().code(),
                HttpErrorCode::READ_TIMEOUT
            );
            drop(transfer);
            let log = log.lock().unwrap();
            for event in ["body.drop", "complete", "callback.drop"] {
                assert_eq!(log.iter().filter(|actual| **actual == event).count(), 1);
            }
        }
    }

    #[test]
    fn fresh_body_transfer_preserves_wait_phases_bounds_and_terminal_errors() {
        struct Completion(Rc<Cell<usize>>);
        impl HttpStreamSourceCallback for Completion {
            fn source_complete(&mut self, _: StreamId, _: Option<HttpError>) {
                self.0.set(self.0.get() + 1);
            }
        }

        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                time::pause();
                for terminal in [
                    None,
                    Some(HttpErrorCode::READ_TIMEOUT),
                    Some(HttpErrorCode::MESSAGE_ERROR),
                ] {
                    let completed = Rc::new(Cell::new(0));
                    let source = HttpIngressBody::new(
                        StreamId::new(1),
                        Completion(Rc::clone(&completed)),
                        8,
                        Host::tokio(),
                    )
                    .unwrap();
                    let producer = source.producer();
                    let receipt = source.ingress_completion().unwrap();
                    let mut head = HttpMessage::new();
                    head.set_status_code(200).unwrap();
                    assert!(source.initialize_body_only(&mut head, false));
                    assert!(producer.set_read_timeout(Duration::from_millis(5)));
                    let mut transfer = HttpBodyTransfer::new(source, StreamId::new(1), 4);
                    let cancellation = CancellationSource::new();
                    let mut context = Context::from_waker(Waker::noop());
                    assert!(
                        transfer
                            .poll_read_body_event(&mut context, 2, &cancellation.token())
                            .is_pending()
                    );
                    time::advance(Duration::from_millis(5)).await;
                    assert_eq!(
                        poll_fn(|cx| transfer.poll_resume(cx)).await,
                        Some(ResumeStatus::TimedOut)
                    );
                    assert!(
                        receipt.snapshot().is_none(),
                        "a suspended timeout is not a source error"
                    );
                    assert_eq!(completed.get(), 0);
                    assert!(
                        transfer
                            .poll_read_body_event(&mut context, 2, &cancellation.token())
                            .is_pending()
                    );
                    assert!(matches!(
                        transfer.poll_resume(&mut context),
                        Poll::Ready(None)
                    ));

                    if let Some(expected) = terminal {
                        if expected == HttpErrorCode::READ_TIMEOUT {
                            time::advance(Duration::from_millis(5)).await;
                        } else {
                            assert!(producer.abort(expected, b"original ingress failure"));
                            cancellation.request_cancellation();
                        }
                        let error = poll_fn(|cx| {
                            transfer.poll_read_body_event(cx, 8, &cancellation.token())
                        })
                        .await
                        .err()
                        .expect("ordinary timeout or ingress error reaches the receiver");
                        assert_eq!(
                            error.code(),
                            expected,
                            "a ready source error precedes token cancellation"
                        );
                        assert_eq!(receipt.snapshot().unwrap().unwrap_err().code(), expected);
                    } else {
                        let bytes = Bytes::from_static(b"abcd");
                        let pointer = bytes.as_ptr();
                        producer.body(BufQueue::from_bytes(bytes, 4).unwrap(), 0, true);
                        let Poll::Ready(Ok(event)) =
                            transfer.poll_read_body_event(&mut context, 8, &cancellation.token())
                        else {
                            panic!("signalled ordinary read returns its original maximum");
                        };
                        assert!(!event.eom());
                        assert_eq!(producer.body_bytes_buffered(), Some(2));
                        assert_eq!(completed.get(), 0);
                        assert_eq!(
                            event.as_body().unwrap().coalesce(2).unwrap().as_ref(),
                            b"ab"
                        );
                        assert_eq!(
                            event.as_body().unwrap().segments().next().unwrap().as_ptr(),
                            pointer
                        );
                        drop(event);
                        let Poll::Ready(Ok(event)) =
                            transfer.poll_read_body_event(&mut context, 8, &cancellation.token())
                        else {
                            panic!("the terminal event transfers directly from its source");
                        };
                        assert!(
                            !producer.is_live(),
                            "terminal source ownership retires before returning the event"
                        );
                        assert!(event.eom());
                        assert_eq!(
                            event.as_body().unwrap().coalesce(2).unwrap().as_ref(),
                            b"cd"
                        );
                        assert_eq!(
                            event.as_body().unwrap().segments().next().unwrap().as_ptr(),
                            pointer.wrapping_add(2)
                        );
                        assert!(receipt.snapshot().unwrap().is_ok());
                    }
                    assert_eq!(completed.get(), 1);
                    assert!(!producer.is_live());
                    let Poll::Ready(Err(error)) =
                        transfer.poll_read_body_event(&mut context, 2, &CancellationToken::never())
                    else {
                        panic!("retired fresh input reports the existing unavailable-source error");
                    };
                    assert_eq!(error.code(), HttpErrorCode::DROPPED);
                    assert_eq!(error.message(), b"HTTP source is no longer readable");
                    drop(transfer);
                    assert_eq!(completed.get(), 1);
                }
            });
    }

    #[test]
    fn fresh_body_transfer_preserves_cancellation_and_retained_panic_order() {
        struct Payload(Arc<std::sync::Mutex<Vec<&'static str>>>);
        impl AsRef<[u8]> for Payload {
            fn as_ref(&self) -> &[u8] {
                b"x"
            }
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push("body.drop");
            }
        }
        struct Callback {
            log: Arc<std::sync::Mutex<Vec<&'static str>>>,
            producer: Rc<RefCell<Option<crate::stream_source::HttpStreamSourceProducer>>>,
            panic_on_credit: bool,
            credit_panicked: bool,
            first: Arc<()>,
        }
        impl HttpStreamSourceCallback for Callback {
            fn bytes_processed(&mut self, _: StreamId, _: usize, _: usize) {
                self.log.lock().unwrap().push("credit");
                if self.panic_on_credit {
                    self.producer
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .stop_reading(Some(HttpErrorCode::READ_TIMEOUT));
                    self.credit_panicked = true;
                    panic_any(Arc::clone(&self.first));
                }
            }
            fn source_complete(&mut self, _: StreamId, _: Option<HttpError>) {
                self.log.lock().unwrap().push("complete");
                if self.credit_panicked {
                    panic_any("secondary source completion");
                }
            }
        }
        impl Drop for Callback {
            fn drop(&mut self) {
                self.log
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push("callback.drop");
            }
        }

        for (token_cancel, suspended, panic_on_credit) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let first = Arc::new(());
            let callback_producer = Rc::new(RefCell::new(None));
            let source = HttpIngressBody::new(
                StreamId::new(1),
                Callback {
                    log: Arc::clone(&log),
                    producer: Rc::clone(&callback_producer),
                    panic_on_credit,
                    credit_panicked: false,
                    first: Arc::clone(&first),
                },
                4,
                Host::tokio(),
            )
            .unwrap();
            let producer = source.producer();
            *callback_producer.borrow_mut() = Some(producer.clone());
            let receipt = source.ingress_completion().unwrap();
            let mut head = HttpMessage::new();
            head.set_status_code(200).unwrap();
            assert!(source.initialize_body_only(&mut head, false));
            let mut transfer = HttpBodyTransfer::new(source, StreamId::new(1), 2);
            let cancellation = CancellationSource::new();
            let mut context = Context::from_waker(Waker::noop());
            let mut pending_event = None;
            if !panic_on_credit {
                producer.body(
                    BufQueue::from_bytes(Bytes::from_owner(Payload(Arc::clone(&log))), 1).unwrap(),
                    0,
                    false,
                );
                let Poll::Ready(Ok(event)) =
                    transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                else {
                    panic!("the ready body event transfers to its caller");
                };
                assert!(!event.eom());
                pending_event = Some(event);
                assert_eq!(*log.lock().unwrap(), ["credit"]);
                log.lock().unwrap().clear();
            }
            assert!(
                transfer
                    .poll_read_body_event(&mut context, 1, &cancellation.token())
                    .is_pending()
            );
            if !suspended {
                transfer.drop_pending_read();
                assert!(
                    transfer
                        .poll_read_body_event(&mut context, 1, &cancellation.token())
                        .is_pending()
                );
                assert!(matches!(
                    transfer.poll_resume(&mut context),
                    Poll::Ready(None)
                ));
            }
            if panic_on_credit {
                producer.body(
                    BufQueue::from_bytes(Bytes::from_owner(Payload(Arc::clone(&log))), 1).unwrap(),
                    0,
                    false,
                );
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _ = transfer.poll_read_body_event(&mut context, 1, &cancellation.token());
                }))
                .expect_err("the original credit panic escapes a retained transfer");
                assert!(Arc::ptr_eq(
                    panic.downcast_ref::<Arc<()>>().unwrap(),
                    &first
                ));
                assert_eq!(*log.lock().unwrap(), ["credit", "body.drop", "complete"]);
                assert!(producer.is_live());
                let Poll::Ready(Err(error)) =
                    transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                else {
                    panic!(
                        "the retained receiver releases its failed read before the source-error retry"
                    );
                };
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert!(!producer.is_live());
                assert_eq!(
                    *log.lock().unwrap(),
                    ["credit", "body.drop", "complete", "callback.drop"]
                );
            } else {
                if token_cancel {
                    cancellation.request_cancellation();
                    let Poll::Ready(Err(error)) =
                        transfer.poll_read_body_event(&mut context, 1, &cancellation.token())
                    else {
                        panic!("token cancellation retires the pending operation");
                    };
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    assert!(receipt.snapshot().is_none());
                    assert!(producer.is_live());
                }
                let retirement =
                    transfer.prepare_stop_and_clear(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                assert!(log.lock().unwrap().is_empty());
                assert!(receipt.snapshot().is_none());
                assert!(retirement.retire().is_ok());
                if token_cancel {
                    assert_eq!(*log.lock().unwrap(), ["complete", "callback.drop"]);
                    assert!(!producer.is_live());
                } else {
                    let expected: &[&str] = if suspended { &["complete"] } else { &[] };
                    assert_eq!(log.lock().unwrap().as_slice(), expected);
                    assert!(
                        producer.is_live(),
                        "the retained wait owns callback state until retirement"
                    );
                }
                transfer.drop_pending_read();
                assert!(!producer.is_live());
                assert_eq!(*log.lock().unwrap(), ["complete", "callback.drop"]);
                drop(pending_event.take());
                assert_eq!(
                    *log.lock().unwrap(),
                    ["complete", "callback.drop", "body.drop"]
                );
            }
            assert_eq!(
                receipt.snapshot().unwrap().unwrap_err().code(),
                HttpErrorCode::READ_TIMEOUT
            );
            drop(transfer);
            let log = log.lock().unwrap();
            for event in ["body.drop", "complete", "callback.drop"] {
                assert_eq!(log.iter().filter(|actual| **actual == event).count(), 1);
            }
        }
    }

    #[test]
    fn matching_egress_content_length() {
        let source_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(source_observations)
            .with_header(Ok(response(200, Some(b"50"), false)))
            .with_body(Ok(body(&[b'a'; 25], false)))
            .with_body(Ok(body(&[b'b'; 25], true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        assert!(
            !ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert_eq!(queue.observed_body_length(), 50);
        assert!(callback.mismatches.borrow().is_empty());
        assert_eq!(&*callback.buffered_deltas.borrow(), &[25, 25]);
    }

    #[test]
    fn egress_content_length_overrun_reported_once() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(200, Some(b"50"), false)))
            .with_body(Ok(body(&[0; 75], true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        let result = ready(queue.read_body_event(100, CancellationToken::never()));
        assert!(result.unwrap().eom);
        assert_eq!(&*callback.mismatches.borrow(), &[(50, 75)]);
    }

    #[test]
    fn egress_content_length_short_eom_reported_once() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(200, Some(b"50"), false)))
            .with_body(Ok(body(&[0; 25], true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert_eq!(&*callback.mismatches.borrow(), &[(50, 25)]);
    }

    #[test]
    fn response_304_skips_egress_length_check() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source =
            ScriptedSource::new(observations).with_header(Ok(response(304, Some(b"10"), true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        assert!(ready(queue.read_header_event()).unwrap().eom());
        assert!(callback.mismatches.borrow().is_empty());
    }

    #[test]
    fn malformed_egress_length_disables_check() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(200, Some(b"invalid"), false)))
            .with_body(Ok(body(&[0; 25], true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert!(callback.mismatches.borrow().is_empty());
    }

    #[test]
    fn header_source_error_preserved_during_cancel() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let expected = HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"");
        let source = ScriptedSource::new(observations).with_header(Err(expected));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        let error = match ready(queue.read_header_event()) {
            Ok(_) => panic!("the source cancellation error was lost"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        assert_eq!(error.message(), b"");
    }

    #[test]
    fn cancel_inflight_body_read() {
        #[derive(Default)]
        struct WakeFlag(AtomicBool);

        impl Wake for WakeFlag {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (_completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&observations)).with_body_future(pending);
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        let cancellation = CancellationSource::new();
        let mut read = queue.read_body_event(100, cancellation.token());
        let wake = Arc::new(WakeFlag::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);

        assert!(read.as_mut().poll(&mut context).is_pending());
        assert!(!wake.0.load(Ordering::SeqCst));
        assert!(cancellation.request_cancellation());
        assert!(
            wake.0.load(Ordering::SeqCst),
            "cancellation must wake a read whose source remains pending"
        );
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("a cancelled body read returned an event"),
            Poll::Pending => panic!("cancellation did not complete the body read"),
        };
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        drop(read);
        drop(queue);
        assert_eq!(observations.stops.get(), 1);
        assert_eq!(observations.drops.get(), 1);
    }

    #[test]
    fn source_cancel_error_is_preserved() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (completion, pending) = manual_future();
        let source = ScriptedSource::new(observations).with_body_future(pending);
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        let cancellation = CancellationSource::new();
        let mut read = queue.read_body_event(100, cancellation.token());
        assert!(poll_once(read.as_mut()).is_pending());

        completion.complete(Err(HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"source cancellation",
        )));
        assert!(cancellation.request_cancellation());
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("the source error was lost"),
            Poll::Pending => panic!("the completed source remained pending"),
        };
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        assert_eq!(error.message(), b"source cancellation");
    }

    #[test]
    fn cancel_while_waiting_for_capacity() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source =
            ScriptedSource::new(Rc::clone(&observations)).with_body(Ok(body(&[0; 100], false)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();
        assert!(
            !ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );

        let cancellation = CancellationSource::new();
        let mut blocked = queue.read_body_event(u32::MAX, cancellation.token());
        assert!(poll_once(blocked.as_mut()).is_pending());
        assert!(cancellation.request_cancellation());
        let error = match poll_once(blocked.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("a cancelled capacity wait succeeded"),
            Poll::Pending => panic!("capacity cancellation did not complete"),
        };
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
        assert_eq!(error.message(), b"cancelled waiting for buffer space");
        drop(blocked);
        drop(queue);
        assert_eq!(observations.stops.get(), 1);
    }

    #[test]
    fn egress_mismatch_never_surfaces_from_read_body_event() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(200, Some(b"1"), false)))
            .with_body(Ok(body(b"too long", true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        assert!(ready(queue.read_body_event(100, CancellationToken::never())).is_ok());
        assert_eq!(&*callback.mismatches.borrow(), &[(1, 8)]);
    }

    #[test]
    fn egress_mismatch_callback_fires_once() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations)
            .with_header(Ok(response(200, Some(b"1"), false)))
            .with_body(Ok(body(b"ab", false)))
            .with_body(Ok(body(b"c", true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);

        ready(queue.read_header_event()).unwrap();
        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        queue.validate_content_length(true);
        assert_eq!(&*callback.mismatches.borrow(), &[(1, 2)]);
    }

    #[derive(Default)]
    struct ByteObservations {
        cancellations: RefCell<Vec<(Option<StreamId>, ByteEventType, HttpErrorCode)>>,
    }

    impl HttpByteEventCallback for ByteObservations {
        fn on_byte_event(&self, _event: HttpByteEvent) {}

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancellations
                .borrow_mut()
                .push((event.stream_id, event.event_type, error.code()));
        }
    }

    #[test]
    fn zero_body_limit_preserves_empty_event_order_and_registrations() {
        for registered in [false, true] {
            let source_observations = Rc::new(SourceObservations::default());
            let callback = Rc::new(QueueObservations::default());
            let byte_callback = Rc::new(ByteObservations::default());
            let mut empty = body(b"", false);
            if registered {
                empty
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        &byte_callback,
                    ));
            }
            let mut source =
                ScriptedSource::new(Rc::clone(&source_observations)).with_body(Ok(empty));
            if !registered {
                source = source.with_body(Ok(HttpBodyEvent::padding(9, false)));
            }
            let mut trailers = crate::headers::HttpHeaders::new();
            trailers.add("x-order", "last");
            source = source
                .with_body(Ok(body(b"x", false)))
                .with_body(Ok(HttpBodyEvent::trailers(trailers)));
            let mut queue = queue(source, &callback, 10, Duration::ZERO);
            queue.skip_content_length_validation();
            assert!(!queue.can_dequeue_body_event(0));
            assert!(!queue.can_dequeue_body_event(1));
            assert_eq!(queue.buffered_event_count(), 0);
            let events = if registered { 3 } else { 4 };
            for index in 0..events {
                let result = ready(queue.read_body_event(10, CancellationToken::never())).unwrap();
                assert_eq!(result.eom, index == events - 1);
            }
            assert_eq!(source_observations.drops.get(), 1);
            assert_eq!(queue.buffered_body_bytes(), 1);
            assert_eq!(queue.buffered_event_count(), events);
            assert!(queue.can_dequeue_body_event(0));
            let mut empty = queue.dequeue_body_event(0).unwrap();
            assert_eq!(empty.as_body().unwrap().chain_length(), 0);
            assert!(!empty.eom());
            assert_eq!(
                empty.byte_event_registrations().len(),
                usize::from(registered)
            );
            if registered {
                empty.byte_event_registrations_mut()[0]
                    .cancel(&HttpError::new(HttpErrorCode::CANCEL), None);
            }
            drop(empty);
            if !registered {
                assert!(queue.can_dequeue_body_event(0));
                let padding = queue.dequeue_body_event(0).unwrap();
                assert!(matches!(padding.kind(), HttpBodyEventKind::Padding(9)));
                assert!(!padding.eom());
            }
            assert!(!queue.can_dequeue_body_event(0));
            assert!(queue.can_dequeue_body_event(1));
            assert_eq!(queue.buffered_event_count(), 2);
            let probe = queue.dequeue_body_event(0).unwrap();
            assert_eq!(probe.as_body().unwrap().chain_length(), 0);
            assert!(!probe.eom());
            assert!(probe.byte_event_registrations().is_empty());
            assert_eq!(queue.buffered_body_bytes(), 1);
            assert_eq!(queue.buffered_event_count(), 2);
            assert!(!queue.can_dequeue_body_event(0));
            let data = queue.dequeue_body_event(1).unwrap();
            assert_eq!(data.as_body().unwrap().front().unwrap().as_ref(), b"x");
            assert!(!data.eom());
            assert!(queue.can_dequeue_body_event(0));
            let trailers = queue.dequeue_body_event(0).unwrap();
            let HttpBodyEventKind::Trailers(fields) = trailers.kind() else {
                panic!("trailers follow empty event, padding and DATA")
            };
            assert_eq!(fields.get_single_or_empty("x-order"), b"last");
            assert!(trailers.eom());
            assert_eq!(queue.buffered_body_bytes(), 0);
            assert_eq!(queue.buffered_event_count(), 0);
            assert!(!queue.can_dequeue_body_event(0));
            assert!(!queue.can_dequeue_body_event(1));
            assert_eq!(
                *byte_callback.cancellations.borrow(),
                if registered {
                    vec![(
                        Some(StreamId::new(1)),
                        ByteEventType::KernelWrite,
                        HttpErrorCode::CANCEL,
                    )]
                } else {
                    Vec::new()
                }
            );
        }
    }

    #[test]
    fn partial_dequeue_keeps_eom_and_registrations_on_the_remainder() {
        let source_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let byte_callback = Rc::new(ByteObservations::default());
        let mut event = body(b"abcdefghij", true);
        event
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                None,
                ByteEventType::KernelWrite.bit(),
                &byte_callback,
            ));
        let source = ScriptedSource::new(source_observations).with_body(Ok(event));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );

        let first = queue.dequeue_body_event(4).unwrap();
        assert_eq!(first.as_body().unwrap().chain_length(), 4);
        assert!(!first.eom());
        assert!(first.byte_event_registrations().is_empty());
        assert_eq!(queue.buffered_body_bytes(), 6);

        let mut remainder = queue.dequeue_body_event(6).unwrap();
        assert_eq!(remainder.as_body().unwrap().chain_length(), 6);
        assert!(remainder.eom());
        assert_eq!(remainder.byte_event_registrations().len(), 1);
        remainder.byte_event_registrations_mut()[0]
            .cancel(&HttpError::new(HttpErrorCode::CANCEL), None);
        assert_eq!(
            &*byte_callback.cancellations.borrow(),
            &[(
                Some(StreamId::new(1)),
                ByteEventType::KernelWrite,
                HttpErrorCode::CANCEL,
            )]
        );
    }

    #[test]
    fn coalescing_fills_only_missing_registration_stream_ids() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let byte_callback = Rc::new(ByteObservations::default());
        let first = body(b"abc", false);
        let mut second = body(b"de", true);
        second
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                None,
                ByteEventType::TransportWrite.bit(),
                &byte_callback,
            ));
        second
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                Some(StreamId::new(77)),
                ByteEventType::KernelWrite.bit(),
                &byte_callback,
            ));
        let source = ScriptedSource::new(observations)
            .with_body(Ok(first))
            .with_body(Ok(second));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();

        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        let mut combined = queue.dequeue_body_event(100).unwrap();
        assert!(queue.empty());
        assert_eq!(combined.as_body().unwrap().chain_length(), 5);
        assert_eq!(combined.as_body().unwrap().segment_count(), 2);
        assert!(combined.eom());
        assert_eq!(
            combined.byte_event_registrations()[0].stream_id(),
            Some(StreamId::new(1))
        );
        assert_eq!(
            combined.byte_event_registrations()[1].stream_id(),
            Some(StreamId::new(77))
        );
        for registration in combined.byte_event_registrations_mut() {
            registration.cancel(&HttpError::new(HttpErrorCode::CANCEL), None);
        }
    }

    #[test]
    fn coalesced_backing_is_released_before_reentrant_mismatch_returns() {
        struct Payload {
            bytes: Vec<u8>,
            name: &'static str,
            drops: Arc<Mutex<Vec<&'static str>>>,
        }
        impl AsRef<[u8]> for Payload {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.drops
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(self.name);
            }
        }
        struct ClearOnMismatch {
            queue: RefCell<Weak<RefCell<HttpBodyEventQueue>>>,
            observations: QueueObservations,
            drops: Arc<Mutex<Vec<&'static str>>>,
            drops_before_return: RefCell<Vec<&'static str>>,
            bytes_before_clear: Cell<usize>,
        }
        impl BodyEventQueueCallback for ClearOnMismatch {
            fn on_egress_bytes_buffered(&self, bytes: i64) {
                self.observations.on_egress_bytes_buffered(bytes);
            }
            fn on_content_length_mismatch(&self, expected: u64, observed: u64) {
                self.observations
                    .on_content_length_mismatch(expected, observed);
                let queue = self.queue.borrow().upgrade().unwrap();
                self.bytes_before_clear
                    .set(queue.borrow().buffered_body_bytes());
                queue
                    .borrow_mut()
                    .clear(&HttpError::new(HttpErrorCode::CANCEL));
                *self.drops_before_return.borrow_mut() = self.drops.lock().unwrap().clone();
            }
        }

        let mut callback_lifetimes = Vec::new();
        for (tail_length, incoming_length) in [(3, 2), (32_768, 32_768)] {
            let total = tail_length + incoming_length;
            let drops = Arc::new(Mutex::new(Vec::new()));
            let callback = Rc::new(ClearOnMismatch {
                queue: RefCell::new(Weak::new()),
                observations: QueueObservations::default(),
                drops: Arc::clone(&drops),
                drops_before_return: RefCell::new(Vec::new()),
                bytes_before_clear: Cell::new(0),
            });
            let tracked_body = |length, byte, name, eom| {
                HttpBodyEvent::body(
                    BufQueue::from_bytes(
                        Bytes::from_owner(Payload {
                            bytes: vec![byte; length],
                            name,
                            drops: Arc::clone(&drops),
                        }),
                        length,
                    )
                    .unwrap(),
                    eom,
                )
            };
            let first = tracked_body(tail_length, b'a', "tail", false);
            let mut second = tracked_body(incoming_length, b'b', "incoming", true);
            let byte_callback = Rc::new(ByteObservations::default());
            for (stream_id, event_type) in [
                (None, ByteEventType::TransportWrite),
                (Some(StreamId::new(77)), ByteEventType::KernelWrite),
            ] {
                second
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        stream_id,
                        event_type.bit(),
                        &byte_callback,
                    ));
            }
            let source = ScriptedSource::new(Rc::new(SourceObservations::default()))
                .with_header(Ok(response(
                    200,
                    Some(tail_length.to_string().as_bytes()),
                    false,
                )))
                .with_body(Ok(first))
                .with_body(Ok(second));
            let queue = Rc::new(RefCell::new(
                HttpBodyEventQueue::from_source_with_configuration(
                    HttpSourceHolder::new(Box::new(source)),
                    StreamId::new(1),
                    &callback,
                    total,
                    Duration::ZERO,
                    Host::tokio(),
                ),
            ));
            *callback.queue.borrow_mut() = Rc::downgrade(&queue);
            let headers = queue.borrow_mut().read_header_event();
            ready(headers).unwrap();
            let first_read = queue
                .borrow_mut()
                .read_body_event(u32::MAX, CancellationToken::never());
            assert!(!ready(first_read).unwrap().eom);
            assert!(drops.lock().unwrap().is_empty());
            let second_read = queue
                .borrow_mut()
                .read_body_event(u32::MAX, CancellationToken::never());
            assert!(ready(second_read).unwrap().eom);
            queue.borrow_mut().validate_content_length(true);
            assert_eq!(queue.borrow().observed_body_length(), total as u64);
            assert!(queue.borrow().empty());
            assert_eq!(queue.borrow().buffered_body_bytes(), 0);
            assert_eq!(callback.bytes_before_clear.get(), total);
            assert_eq!(
                &*callback.observations.mismatches.borrow(),
                &[(tail_length as u64, total as u64)]
            );
            assert_eq!(
                &*callback.observations.buffered_deltas.borrow(),
                &[tail_length as i64, -(total as i64), incoming_length as i64]
            );
            let snapshot = callback.drops_before_return.borrow();
            callback_lifetimes.push((
                total,
                snapshot.contains(&"tail"),
                snapshot.contains(&"incoming"),
            ));
            drop(queue);
            assert_eq!(
                &*byte_callback.cancellations.borrow(),
                &[
                    (
                        Some(StreamId::new(1)),
                        ByteEventType::TransportWrite,
                        HttpErrorCode::CANCEL
                    ),
                    (
                        Some(StreamId::new(77)),
                        ByteEventType::KernelWrite,
                        HttpErrorCode::CANCEL
                    ),
                ]
            );
            let drops = drops.lock().unwrap();
            assert_eq!(drops.len(), 2);
            assert!(drops.contains(&"tail"));
            assert!(drops.contains(&"incoming"));
        }
        assert_eq!(callback_lifetimes, [(5, true, true), (65_536, true, true)]);
    }

    #[test]
    fn clear_cancels_all_registrations_and_reports_removed_body_bytes() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let byte_callback = Rc::new(ByteObservations::default());
        let mut first = body(b"abc", false);
        first
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                None,
                ByteEventType::TransportWrite.bit(),
                &byte_callback,
            ));
        let mut second = body(b"de", false);
        second
            .byte_event_registrations_mut()
            .push(HttpByteEventRegistration::new(
                None,
                ByteEventType::KernelWrite.bit(),
                &byte_callback,
            ));
        let source = ScriptedSource::new(observations)
            .with_body(Ok(first))
            .with_body(Ok(second))
            .with_body(Ok(body(b"", true)));
        let mut queue = queue(source, &callback, 5, Duration::ZERO);
        queue.skip_content_length_validation();
        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        let mut pending = queue.read_body_event(100, CancellationToken::never());
        assert!(poll_once(pending.as_mut()).is_pending());

        queue.clear(&HttpError::with_message(
            HttpErrorCode::FLOW_CONTROL_ERROR,
            b"stream reset",
        ));
        assert!(queue.empty());
        assert_eq!(queue.buffered_body_bytes(), 0);
        assert_eq!(&*callback.buffered_deltas.borrow(), &[3, 2, -5]);
        assert_eq!(byte_callback.cancellations.borrow().len(), 2);
        assert!(
            byte_callback
                .cancellations
                .borrow()
                .iter()
                .all(|(_, _, code)| *code == HttpErrorCode::FLOW_CONTROL_ERROR)
        );
        assert!(matches!(
            poll_once(pending.as_mut()),
            Poll::Ready(Ok(result)) if result.eom
        ));
    }

    #[test]
    fn dequeue_reopens_capacity_and_caps_the_next_source_read() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_body(Ok(body(b"abcde", false)))
            .with_body(Ok(body(b"fg", true)));
        let mut queue = queue(source, &callback, 5, Duration::ZERO);
        queue.skip_content_length_validation();
        ready(queue.read_body_event(u32::MAX, CancellationToken::never())).unwrap();

        let mut blocked = queue.read_body_event(u32::MAX, CancellationToken::never());
        assert!(poll_once(blocked.as_mut()).is_pending());
        assert_eq!(queue.buffered_event_count(), 1);
        let prefix = queue.dequeue_body_event(2).unwrap();
        assert_eq!(prefix.as_body().unwrap().chain_length(), 2);
        assert!(matches!(poll_once(blocked.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(&*observations.body_maximums.borrow(), &[5, 2]);
        assert_eq!(queue.buffered_body_bytes(), 5);
        assert_eq!(queue.buffered_event_count(), 1);
    }

    #[test]
    fn stopped_capacity_wait_does_not_block_a_replacement_read() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations).with_body(Ok(body(b"a", false)));
        let mut queue = queue(source, &callback, 1, Duration::ZERO);
        queue.skip_content_length_validation();
        ready(queue.read_body_event(1, CancellationToken::never())).unwrap();

        let mut stale = queue.read_body_event(1, CancellationToken::never());
        assert!(poll_once(stale.as_mut()).is_pending());
        queue.stop_reading(HttpErrorCode::CANCEL);

        let replacement_observations = Rc::new(SourceObservations::default());
        let replacement =
            ScriptedSource::new(replacement_observations).with_body(Ok(body(b"b", true)));
        assert!(queue.set_source(Box::new(replacement)).is_ok());
        let mut replacement_read = queue.read_body_event(1, CancellationToken::never());
        assert!(poll_once(replacement_read.as_mut()).is_pending());

        queue.dequeue_body_event(1).unwrap();
        assert!(matches!(
            poll_once(replacement_read.as_mut()),
            Poll::Ready(Ok(_))
        ));
        let error = match poll_once(stale.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("the stopped capacity wait unexpectedly succeeded"),
            Poll::Pending => panic!("the stopped capacity wait remained pending"),
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
    }

    #[test]
    fn capacity_wait_times_out_deterministically() {
        run_paused(async {
            let observations = Rc::new(SourceObservations::default());
            let callback = Rc::new(QueueObservations::default());
            let source = ScriptedSource::new(observations).with_body(Ok(body(b"x", false)));
            let mut queue = queue(source, &callback, 1, Duration::from_secs(2));
            queue.skip_content_length_validation();
            ready(queue.read_body_event(1, CancellationToken::never())).unwrap();

            let mut blocked = queue.read_body_event(1, CancellationToken::never());
            assert!(poll_once(blocked.as_mut()).is_pending());
            time::advance(Duration::from_secs(2)).await;
            wait_for_capacity_commit(&queue, blocked.as_mut()).await;
            let error = match poll_once(blocked.as_mut()) {
                Poll::Ready(Err(error)) => error,
                Poll::Ready(Ok(_)) => panic!("a timed-out capacity wait succeeded"),
                Poll::Pending => panic!("timeout did not commit on the second repoll"),
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"timed out waiting for buffer space");
        });
    }

    #[test]
    fn suspend_is_returned_without_queueing_or_accounting() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source = ScriptedSource::new(observations).with_body(Ok(HttpBodyEvent::suspend(
            Box::pin(std::future::ready(ResumeStatus::Signalled)),
        )));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();

        let mut result = ready(queue.read_body_event(100, CancellationToken::never())).unwrap();
        assert!(!result.eom);
        assert_eq!(
            ready(result.resume.take().unwrap()),
            ResumeStatus::Signalled
        );
        assert!(queue.empty());
        assert_eq!(queue.observed_body_length(), 0);
        assert!(callback.buffered_deltas.borrow().is_empty());
    }

    #[test]
    fn dropped_read_can_be_retried_and_active_replacement_is_rejected() {
        let first_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (_completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&first_observations))
            .with_body_future(pending)
            .with_body(Ok(body(b"retry", true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();
        let mut first = queue.read_body_event(100, CancellationToken::never());
        assert!(poll_once(first.as_mut()).is_pending());

        let replacement_observations = Rc::new(SourceObservations::default());
        let replacement = ScriptedSource::new(Rc::clone(&replacement_observations));
        let rejected = match queue.set_source(Box::new(replacement)) {
            Ok(()) => panic!("active read replacement must be rejected"),
            Err(rejected) => rejected,
        };
        assert_eq!(
            rejected.reason(),
            BodyEventQueueMutationError::ReadInProgress
        );
        assert_eq!(replacement_observations.drops.get(), 0);
        drop(rejected.into_source());
        assert_eq!(replacement_observations.drops.get(), 1);

        drop(first);
        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert_eq!(queue.observed_body_length(), 5);
    }

    #[test]
    fn unpolled_body_read_does_not_block_replacement_or_retry() {
        let first_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let first =
            ScriptedSource::new(Rc::clone(&first_observations)).with_body(Ok(body(b"old", true)));
        let mut queue = queue(first, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();

        let unpolled = queue.read_body_event(100, CancellationToken::never());
        let replacement_observations = Rc::new(SourceObservations::default());
        let replacement =
            ScriptedSource::new(replacement_observations).with_body(Ok(body(b"replacement", true)));
        assert!(queue.set_source(Box::new(replacement)).is_ok());
        drop(unpolled);

        assert!(
            ready(queue.read_body_event(100, CancellationToken::never()))
                .unwrap()
                .eom
        );
        let event = queue.dequeue_body_event(100).unwrap();
        assert_eq!(
            event.as_body().unwrap().coalesce(11).unwrap(),
            b"replacement"[..]
        );
        assert_eq!(first_observations.stops.get(), 1);
        assert_eq!(first_observations.drops.get(), 1);
    }

    #[test]
    fn stopped_pending_read_cannot_queue_a_stale_event() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (completion, pending) = manual_future();
        let source = ScriptedSource::new(Rc::clone(&observations)).with_body_future(pending);
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();
        let mut read = queue.read_body_event(100, CancellationToken::never());
        assert!(poll_once(read.as_mut()).is_pending());

        queue.stop_reading(HttpErrorCode::CANCEL);
        completion.complete(Ok(body(b"stale", true)));
        let error = match poll_once(read.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("a stopped read queued a stale event"),
            Poll::Pending => panic!("the completed stale read remained pending"),
        };
        assert_eq!(error.code(), HttpErrorCode::CANCEL);
        assert!(queue.empty());
        assert_eq!(queue.observed_body_length(), 0);
        assert_eq!(observations.stops.get(), 1);
    }

    #[test]
    fn stale_body_read_does_not_release_pending_replacement_lease() {
        for stale_result in [
            Some(Ok(body(b"stale", true))),
            Some(Err(HttpError::with_message(
                HttpErrorCode::TRANSPORT_READ_ERROR,
                b"stale body error",
            ))),
            None,
        ] {
            let old_observations = Rc::new(SourceObservations::default());
            let new_observations = Rc::new(SourceObservations::default());
            let callback = Rc::new(QueueObservations::default());
            let (old_completion, old_pending) = manual_future();
            let (new_completion, new_pending) = manual_future();
            let source =
                ScriptedSource::new(Rc::clone(&old_observations)).with_body_future(old_pending);
            let mut queue = queue(source, &callback, 100, Duration::ZERO);
            queue.skip_content_length_validation();
            let mut old_read = queue.read_body_event_unboxed(100, CancellationToken::never());
            assert!(poll_once(Pin::new(&mut old_read)).is_pending());

            queue.stop_reading(HttpErrorCode::CANCEL);
            let replacement = ScriptedSource::new(Rc::clone(&new_observations))
                .with_body_future(new_pending)
                .with_body(Ok(body(b"tail", true)));
            assert!(queue.set_source(Box::new(replacement)).is_ok());
            let mut new_read = queue.read_body_event_unboxed(100, CancellationToken::never());
            assert!(poll_once(Pin::new(&mut new_read)).is_pending());

            if let Some(result) = stale_result {
                old_completion.complete(result);
                let Poll::Ready(Err(error)) = poll_once(Pin::new(&mut old_read)) else {
                    panic!("a completed stale body read must be rejected");
                };
                assert_eq!(error.code(), HttpErrorCode::DROPPED);
            }
            drop(old_read);
            assert!(queue.empty());
            assert_eq!(queue.observed_body_length(), 0);
            assert!(poll_once(Pin::new(&mut new_read)).is_pending());

            let mut third = queue.read_body_event_unboxed(100, CancellationToken::never());
            let Poll::Ready(Err(error)) = poll_once(Pin::new(&mut third)) else {
                panic!("the stale read released the replacement queue lease");
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(
                error.message(),
                b"HTTP body event queue already has an active read"
            );
            drop(third);
            assert_eq!(new_observations.drops.get(), 0);

            new_completion.complete(Ok(body(b"replacement", false)));
            assert!(matches!(
                poll_once(Pin::new(&mut new_read)),
                Poll::Ready(Ok(result)) if !result.eom
            ));
            drop(new_read);
            let event = queue.dequeue_body_event(100).unwrap();
            assert_eq!(
                event.as_body().unwrap().coalesce(11).unwrap(),
                b"replacement"[..]
            );

            let mut next = queue.read_body_event_unboxed(100, CancellationToken::never());
            assert!(matches!(
                poll_once(Pin::new(&mut next)),
                Poll::Ready(Ok(result)) if result.eom
            ));
            let event = queue.dequeue_body_event(100).unwrap();
            assert_eq!(event.as_body().unwrap().coalesce(4).unwrap(), b"tail"[..]);
            assert!(event.eom());
            assert_eq!(queue.observed_body_length(), 15);
            assert_eq!(old_observations.stops.get(), 1);
            assert_eq!(old_observations.drops.get(), 1);
            assert_eq!(new_observations.stops.get(), 0);
            assert_eq!(new_observations.drops.get(), 1);
        }
    }

    /// A source whose read panics is consumed as by an error, and the failed
    /// read's admission is released before that read is dropped.
    #[test]
    fn source_poll_panic_retires_the_source_and_releases_admission_before_failed_future_drop() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (completion, pending) = manual_future::<()>();
        let panicking: LocalSourceFuture<Result<HttpBodyEvent, HttpError>> = Box::pin(async move {
            pending.await;
            panic_any("body source poll");
        });
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_body_future(panicking)
            .with_body(Ok(body(b"retry", true)));
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        queue.skip_content_length_validation();
        let mut failed = queue.read_body_event_unboxed(100, CancellationToken::never());
        assert!(poll_once(Pin::new(&mut failed)).is_pending());
        completion.complete(());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = poll_once(Pin::new(&mut failed));
        }))
        .expect_err("the source poll must preserve its panic");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"body source poll"));
        assert!(queue.empty());
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);

        let mut retry = queue.read_body_event_unboxed(100, CancellationToken::never());
        let Poll::Ready(Err(error)) = poll_once(Pin::new(&mut retry)) else {
            panic!("the retry finds the source retired, not a read still admitted");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        assert_eq!(error.message(), b"HTTP source is no longer readable");
        assert!(queue.empty());
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);
        drop(failed);
        assert_eq!(observations.drops.get(), 1);
    }

    #[test]
    fn retained_transfer_retires_the_holder_source_after_a_source_poll_panic() {
        let observations = Rc::new(SourceObservations::default());
        let (completion, pending) = manual_future::<()>();
        let panicking: LocalSourceFuture<Result<HttpBodyEvent, HttpError>> = Box::pin(async move {
            pending.await;
            panic_any("transfer body source poll");
        });
        let source = ScriptedSource::new(Rc::clone(&observations))
            .with_body_future(panicking)
            .with_body(Ok(body(b"retry", true)));
        let mut transfer = HttpBodyTransfer::new(
            HttpSourceHolder::new(Box::new(source)),
            StreamId::new(1),
            100,
        );
        let mut context = Context::from_waker(Waker::noop());
        let cancellation = CancellationToken::never();
        assert!(
            transfer
                .poll_read_body_event(&mut context, 100, &cancellation)
                .is_pending()
        );
        completion.complete(());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = transfer.poll_read_body_event(&mut context, 100, &cancellation);
        }))
        .expect_err("the transfer must preserve the source panic");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"transfer body source poll")
        );
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);
        let Poll::Ready(Err(error)) =
            transfer.poll_read_body_event(&mut context, 100, &cancellation)
        else {
            panic!("the retry finds the source retired, not a read still admitted");
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        assert_eq!(error.message(), b"HTTP source is no longer readable");
        assert_eq!(observations.stops.get(), 0);
        assert_eq!(observations.drops.get(), 1);
        drop(transfer);
        assert_eq!(observations.drops.get(), 1);
    }

    #[test]
    fn unpolled_body_read_resumes_after_stop_and_capacity_release() {
        let old_observations = Rc::new(SourceObservations::default());
        let new_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source =
            ScriptedSource::new(Rc::clone(&old_observations)).with_body(Ok(body(b"a", false)));
        let mut queue = queue(source, &callback, 1, Duration::ZERO);
        queue.skip_content_length_validation();
        let mut fill = queue.read_body_event_unboxed(1, CancellationToken::never());
        assert!(matches!(
            poll_once(Pin::new(&mut fill)),
            Poll::Ready(Ok(result)) if !result.eom
        ));
        drop(fill);
        assert_eq!(queue.buffered_body_bytes(), 1);

        let mut read = queue.read_body_event_unboxed(1, CancellationToken::never());
        queue.stop_reading(HttpErrorCode::CANCEL);
        let replacement =
            ScriptedSource::new(Rc::clone(&new_observations)).with_body(Ok(body(b"b", true)));
        assert!(queue.set_source(Box::new(replacement)).is_ok());
        assert_eq!(queue.buffered_body_bytes(), 1);
        assert!(poll_once(Pin::new(&mut read)).is_pending());
        assert!(new_observations.body_maximums.borrow().is_empty());

        let old = queue.dequeue_body_event(1).unwrap();
        assert_eq!(old.as_body().unwrap().coalesce(1).unwrap(), b"a"[..]);
        assert!(!old.eom());
        assert!(matches!(
            poll_once(Pin::new(&mut read)),
            Poll::Ready(Ok(result)) if result.eom
        ));
        let event = queue.dequeue_body_event(1).unwrap();
        assert_eq!(event.as_body().unwrap().coalesce(1).unwrap(), b"b"[..]);
        assert!(event.eom());
        assert!(queue.empty());
        assert_eq!(queue.observed_body_length(), 2);
        assert_eq!(&*new_observations.body_maximums.borrow(), &[1]);
        assert_eq!(old_observations.stops.get(), 1);
        assert_eq!(old_observations.drops.get(), 1);
        assert_eq!(new_observations.stops.get(), 0);
        assert_eq!(new_observations.drops.get(), 1);
    }

    #[test]
    fn stale_source_error_after_replacement_does_not_escape() {
        let first_observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let (completion, pending) = manual_future();
        let source =
            ScriptedSource::new(Rc::clone(&first_observations)).with_header_future(pending);
        let mut queue = queue(source, &callback, 100, Duration::ZERO);
        let mut stale = queue.read_header_event();
        assert!(poll_once(stale.as_mut()).is_pending());

        queue.stop_reading(HttpErrorCode::CANCEL);
        let replacement_observations = Rc::new(SourceObservations::default());
        let replacement = ScriptedSource::new(replacement_observations)
            .with_header(Ok(response(201, None, false)));
        assert!(queue.set_source(Box::new(replacement)).is_ok());

        completion.complete(Err(HttpError::with_message(
            HttpErrorCode::TRANSPORT_READ_ERROR,
            b"stale source error",
        )));
        let error = match poll_once(stale.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("a stale source read returned an event"),
            Poll::Pending => panic!("the completed stale source read remained pending"),
        };
        assert_eq!(error.code(), HttpErrorCode::DROPPED);
        assert_ne!(error.message(), b"stale source error");
        drop(stale);

        let mut event = ready(queue.read_header_event()).unwrap();
        assert_eq!(event.headers_mut().status_code().unwrap(), 201);
        assert_eq!(first_observations.stops.get(), 1);
        assert_eq!(first_observations.drops.get(), 1);
    }

    #[test]
    fn folly_unsigned_content_length_whitespace_and_overflow() {
        let callback = Rc::new(QueueObservations::default());
        let mut queue = HttpBodyEventQueue::new(StreamId::new(1), &callback);
        queue.set_expected_egress_content_length(b"\x0b5\x0c", false);
        queue.validate_content_length(true);
        assert_eq!(&*callback.mismatches.borrow(), &[(5, 0)]);

        let callback = Rc::new(QueueObservations::default());
        let mut queue = HttpBodyEventQueue::new(StreamId::new(1), &callback);
        queue.set_expected_egress_content_length(b"18446744073709551616", true);
        assert!(callback.mismatches.borrow().is_empty());
    }

    #[test]
    fn non_body_event_is_queued_intact_without_consuming_capacity() {
        let observations = Rc::new(SourceObservations::default());
        let callback = Rc::new(QueueObservations::default());
        let source =
            ScriptedSource::new(observations).with_body(Ok(HttpBodyEvent::padding(9, true)));
        let mut queue = queue(source, &callback, 1, Duration::ZERO);
        queue.skip_content_length_validation();

        assert!(
            ready(queue.read_body_event(1, CancellationToken::never()))
                .unwrap()
                .eom
        );
        assert_eq!(queue.buffered_body_bytes(), 0);
        let event = queue.dequeue_body_event(0).unwrap();
        assert!(event.eom());
        assert!(matches!(event.kind(), HttpBodyEventKind::Padding(9)));
    }
}

#[test]
fn prepared_retirement_releases_borrows_before_foreign_callbacks() {
    prepared_retirement_fixture::borrows();
}

#[test]
fn prepared_retirement_preserves_first_panic_and_finishes_all_items() {
    prepared_retirement_fixture::panics();
}

#[test]
fn prepared_retirement_does_not_consume_replacement_generation() {
    prepared_retirement_fixture::replacement();
}

mod prepared_retirement_fixture {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use bytes::Bytes;
    use roxygen_runtime::cancellation::CancellationToken;
    use roxygen_runtime::host::Host;

    use crate::body_event_queue::BodyEventQueueCallback;
    use crate::body_event_queue::HttpBodyEventQueue;
    use crate::body_event_queue::HttpBodyQueueRetirement;
    use crate::body_event_queue::ReadBodyResult;
    use crate::buf_queue::BufQueue;
    use crate::byte_event::ByteEventType;
    use crate::byte_event::HttpByteEvent;
    use crate::byte_event::HttpByteEventCallback;
    use crate::byte_event::HttpByteEventRegistration;
    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::events::HttpBodyEvent;
    use crate::events::HttpHeaderEvent;
    use crate::ids::StreamId;
    use crate::message::HttpMessage;
    use crate::source::HttpSource;
    use crate::source::LocalSourceFuture;
    use crate::source_holder::HttpSourceHolder;

    #[derive(Debug)]
    struct Marker;

    struct Secondary {
        drops: Arc<AtomicUsize>,
        nested: Arc<AtomicUsize>,
    }

    struct Nested(Arc<AtomicUsize>);

    impl Drop for Secondary {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(Nested(Arc::clone(&self.nested)));
        }
    }

    impl Drop for Nested {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("a nested retirement panic must not be recursively destroyed");
        }
    }

    struct Observations {
        queue: RefCell<Weak<RefCell<HttpBodyEventQueue>>>,
        events: RefCell<Vec<(&'static str, bool)>>,
        deltas: RefCell<Vec<i64>>,
        errors: RefCell<Vec<HttpErrorCode>>,
        faults: Cell<bool>,
        first: Arc<Marker>,
        secondary_drops: Arc<AtomicUsize>,
        nested_drops: Arc<AtomicUsize>,
        read_drops: Cell<usize>,
    }

    impl Observations {
        fn new() -> Self {
            Self {
                queue: RefCell::new(Weak::new()),
                events: RefCell::new(Vec::new()),
                deltas: RefCell::new(Vec::new()),
                errors: RefCell::new(Vec::new()),
                faults: Cell::new(false),
                first: Arc::new(Marker),
                secondary_drops: Arc::new(AtomicUsize::new(0)),
                nested_drops: Arc::new(AtomicUsize::new(0)),
                read_drops: Cell::new(0),
            }
        }

        fn record(&self, label: &'static str) {
            let available = self.queue.borrow().upgrade().is_some_and(|queue| {
                queue
                    .try_borrow_mut()
                    .is_ok_and(|queue| queue.retirement_borrows_available_for_test())
            });
            self.events.borrow_mut().push((label, available));
        }

        fn secondary(&self) -> Secondary {
            Secondary {
                drops: Arc::clone(&self.secondary_drops),
                nested: Arc::clone(&self.nested_drops),
            }
        }

        fn count(&self, label: &str) -> usize {
            self.events
                .borrow()
                .iter()
                .filter(|(event, _)| *event == label)
                .count()
        }
    }

    impl BodyEventQueueCallback for Observations {
        fn on_egress_bytes_buffered(&self, bytes: i64) {
            self.deltas.borrow_mut().push(bytes);
            self.record("accounting");
        }
    }

    thread_local! {
        static CAPACITY_OBSERVER: RefCell<Option<Rc<Observations>>> = const { RefCell::new(None) };
    }

    struct CapacityWake;

    impl Wake for CapacityWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            CAPACITY_OBSERVER
                .with_borrow(|observer| observer.as_ref().unwrap().record("capacity wake"));
        }
    }

    struct ByteProbe(Rc<Observations>);

    impl HttpByteEventCallback for ByteProbe {
        fn on_byte_event(&self, _event: HttpByteEvent) {
            self.0.record("unexpected byte success");
        }

        fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
            self.0.record("byte cancellation");
            self.0.errors.borrow_mut().push(error.code());
            if self.0.faults.get() {
                std::panic::panic_any(self.0.secondary());
            }
        }
    }

    type Completion<T> = Rc<RefCell<Option<Result<T, HttpError>>>>;

    struct PendingRead<T> {
        completion: Completion<T>,
        observations: Rc<Observations>,
    }

    impl<T> Future for PendingRead<T> {
        type Output = Result<T, HttpError>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.completion
                .borrow_mut()
                .take()
                .map_or(Poll::Pending, Poll::Ready)
        }
    }

    impl<T> Drop for PendingRead<T> {
        fn drop(&mut self) {
            self.observations
                .read_drops
                .set(self.observations.read_drops.get() + 1);
            self.observations.record("read future drop");
        }
    }

    struct Source {
        observations: Rc<Observations>,
        header: Option<LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>>,
        bodies: VecDeque<LocalSourceFuture<Result<HttpBodyEvent, HttpError>>>,
        stop: &'static str,
        dropped: &'static str,
        faulty: bool,
    }

    impl HttpSource for Source {
        fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
            self.header.take().expect("prepared header read")
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
            self.bodies.pop_front().expect("prepared body read")
        }

        fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
            self.observations.record(self.stop);
            if let Some(error) = error {
                self.observations.errors.borrow_mut().push(error);
            }
            if self.faulty && self.observations.faults.get() {
                std::panic::panic_any(Arc::clone(&self.observations.first));
            }
        }
    }

    impl Drop for Source {
        fn drop(&mut self) {
            self.observations.record(self.dropped);
            if self.faulty && self.observations.faults.get() {
                std::panic::panic_any(self.observations.secondary());
            }
        }
    }

    fn body(bytes: &'static [u8], eom: bool) -> HttpBodyEvent {
        HttpBodyEvent::body(
            BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).unwrap(),
            eom,
        )
    }

    fn header() -> HttpHeaderEvent {
        let mut message = HttpMessage::new();
        message.set_status_code(200).unwrap();
        HttpHeaderEvent::new(message, false).unwrap()
    }

    fn poll<T>(future: Pin<&mut dyn Future<Output = T>>) -> Poll<T> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    fn ready<T>(mut future: LocalSourceFuture<T>) -> T {
        match poll(future.as_mut()) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("scripted preparation must be ready"),
        }
    }

    enum Read {
        Header(LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>),
        Body(LocalSourceFuture<Result<ReadBodyResult, HttpError>>),
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Header,
        Body,
        Capacity,
    }

    struct Fixture {
        queue: Rc<RefCell<HttpBodyEventQueue>>,
        observations: Rc<Observations>,
        _byte_probe: Rc<ByteProbe>,
        read: Option<Read>,
        header: Completion<HttpHeaderEvent>,
        body: Completion<HttpBodyEvent>,
    }

    impl Fixture {
        fn new(mode: Mode) -> Self {
            let observations = Rc::new(Observations::new());
            let byte_probe = Rc::new(ByteProbe(Rc::clone(&observations)));
            let header_completion = Rc::new(RefCell::new(None));
            let body_completion = Rc::new(RefCell::new(None));
            let nested = Source {
                observations: Rc::clone(&observations),
                header: None,
                bodies: VecDeque::new(),
                stop: "nested stop",
                dropped: "nested drop",
                faulty: false,
            };
            let push =
                HttpBodyEvent::push_promise(HttpMessage::new(), Some(Box::new(nested)), false);
            let mut data = body(b"abc", false);
            data.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::KernelWrite.bit(),
                    &byte_probe,
                ));
            let mut source = Source {
                observations: Rc::clone(&observations),
                header: None,
                bodies: VecDeque::from([
                    Box::pin(std::future::ready(Ok(push))) as LocalSourceFuture<_>,
                    Box::pin(std::future::ready(Ok(data))) as LocalSourceFuture<_>,
                ]),
                stop: "source stop",
                dropped: "source drop",
                faulty: true,
            };
            if mode == Mode::Header {
                source.header = Some(Box::pin(PendingRead {
                    completion: Rc::clone(&header_completion),
                    observations: Rc::clone(&observations),
                }));
            } else if mode == Mode::Body {
                source.bodies.push_back(Box::pin(PendingRead {
                    completion: Rc::clone(&body_completion),
                    observations: Rc::clone(&observations),
                }));
            }
            let queue = Rc::new(RefCell::new(
                HttpBodyEventQueue::from_source_with_configuration(
                    HttpSourceHolder::new(Box::new(source)),
                    StreamId::new(1),
                    &observations,
                    if mode == Mode::Capacity { 3 } else { 100 },
                    Duration::ZERO,
                    Host::tokio(),
                ),
            ));
            *observations.queue.borrow_mut() = Rc::downgrade(&queue);
            queue.borrow_mut().skip_content_length_validation();
            for _ in 0..2 {
                let read = queue
                    .borrow_mut()
                    .read_body_event(100, CancellationToken::never());
                ready(read).unwrap();
            }
            CAPACITY_OBSERVER.set(Some(Rc::clone(&observations)));
            let read = if mode == Mode::Header {
                let mut read = queue.borrow_mut().read_header_event();
                assert!(poll(read.as_mut()).is_pending());
                Read::Header(read)
            } else {
                let mut read = queue
                    .borrow_mut()
                    .read_body_event(100, CancellationToken::never());
                let waker = Waker::from(Arc::new(CapacityWake));
                assert!(
                    read.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                Read::Body(read)
            };
            Self {
                queue,
                observations,
                _byte_probe: byte_probe,
                read: Some(read),
                header: header_completion,
                body: body_completion,
            }
        }

        fn prepare(&self) -> HttpBodyQueueRetirement {
            let before = self.observations.events.borrow().len();
            let retirement =
                self.queue
                    .borrow_mut()
                    .prepare_stop_and_clear(HttpError::with_message(
                        HttpErrorCode::CANCEL,
                        b"prepared queue retirement",
                    ));
            assert_eq!(self.observations.events.borrow().len(), before);
            assert_eq!(self.queue.borrow().buffered_body_bytes(), 0);
            assert!(self.queue.borrow().empty());
            retirement
        }

        fn finish_read(&mut self, expected: HttpErrorCode) {
            *self.header.borrow_mut() = Some(Ok(header()));
            *self.body.borrow_mut() = Some(Ok(body(b"stale", true)));
            let code = match self.read.take().unwrap() {
                Read::Header(mut read) => match poll(read.as_mut()) {
                    Poll::Ready(Err(error)) => error.code(),
                    _ => panic!("stopped header lease must fail"),
                },
                Read::Body(mut read) => match poll(read.as_mut()) {
                    Poll::Ready(Err(error)) => error.code(),
                    _ => panic!("stopped body lease must fail"),
                },
            };
            assert_eq!(code, expected);
        }

        fn assert_retired(&self) {
            for label in [
                "source stop",
                "source drop",
                "nested stop",
                "nested drop",
                "byte cancellation",
            ] {
                assert_eq!(self.observations.count(label), 1, "{label}");
            }
            assert_eq!(self.observations.count("unexpected byte success"), 0);
            assert!(
                self.observations
                    .events
                    .borrow()
                    .iter()
                    .all(|(_, borrowed)| *borrowed),
                "{:?}",
                self.observations.events.borrow()
            );
            assert_eq!(self.observations.deltas.borrow().as_slice(), &[3, -3]);
            assert_eq!(
                self.observations.errors.borrow().as_slice(),
                &[HttpErrorCode::CANCEL, HttpErrorCode::CANCEL]
            );
        }
    }

    fn discard(payload: Box<dyn std::any::Any + Send>) {
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    }

    fn successful(result: std::thread::Result<()>) {
        if let Err(payload) = result {
            discard(payload);
            panic!("healthy prepared retirement panicked");
        }
    }

    fn identity(payload: Box<dyn std::any::Any + Send>, expected: &Arc<Marker>) {
        let marker = match payload.downcast::<Arc<Marker>>() {
            Ok(marker) => marker,
            Err(payload) => {
                discard(payload);
                panic!("first retirement panic type was replaced");
            }
        };
        assert!(Arc::ptr_eq(&marker, expected));
    }

    pub(super) fn borrows() {
        for mode in [Mode::Header, Mode::Body, Mode::Capacity] {
            let mut fixture = Fixture::new(mode);
            let retirement = fixture.prepare();
            assert_eq!(fixture.observations.read_drops.get(), 0);
            successful(retirement.retire());
            assert_eq!(fixture.observations.read_drops.get(), 0);
            fixture.finish_read(HttpErrorCode::CANCEL);
            fixture.assert_retired();
            assert_eq!(
                fixture.observations.read_drops.get(),
                usize::from(mode != Mode::Capacity)
            );
            assert_eq!(
                fixture.observations.count("capacity wake"),
                usize::from(mode == Mode::Capacity)
            );
        }
        CAPACITY_OBSERVER.take();
    }

    pub(super) fn panics() {
        for unwinding in [false, true] {
            let mut fault = Fixture::new(Mode::Body);
            let mut healthy = Fixture::new(Mode::Body);
            let records = vec![fault.prepare(), healthy.prepare()];
            fault.observations.faults.set(true);
            if unwinding {
                let original = Arc::new(Marker);
                let payload = catch_unwind(AssertUnwindSafe(|| {
                    let _records = records;
                    std::panic::panic_any(Arc::clone(&original));
                }))
                .expect_err("outer unwind must survive retirement");
                identity(payload, &original);
            } else {
                let mut records = records.into_iter();
                let first = records
                    .next()
                    .unwrap()
                    .retire()
                    .expect_err("source stop must panic");
                successful(records.next().unwrap().retire());
                identity(first, &fault.observations.first);
            }
            fault.finish_read(HttpErrorCode::CANCEL);
            healthy.finish_read(HttpErrorCode::CANCEL);
            fault.assert_retired();
            healthy.assert_retired();
            assert_eq!(fault.observations.secondary_drops.load(Ordering::SeqCst), 2);
            assert_eq!(fault.observations.nested_drops.load(Ordering::SeqCst), 2);
            assert_eq!(fault.observations.read_drops.get(), 1);
            assert_eq!(healthy.observations.read_drops.get(), 1);
        }
        CAPACITY_OBSERVER.take();
    }

    pub(super) fn replacement() {
        for drop_record in [false, true] {
            let mut fixture = Fixture::new(Mode::Body);
            let old = fixture.prepare();
            let replacement = Source {
                observations: Rc::clone(&fixture.observations),
                header: None,
                bodies: VecDeque::from([
                    Box::pin(std::future::ready(Ok(body(b"new", true)))) as LocalSourceFuture<_>
                ]),
                stop: "replacement stop",
                dropped: "replacement drop",
                faulty: false,
            };
            assert!(
                fixture
                    .queue
                    .borrow_mut()
                    .set_source(Box::new(replacement))
                    .is_ok()
            );
            let read = fixture
                .queue
                .borrow_mut()
                .read_body_event(100, CancellationToken::never());
            assert!(ready(read).unwrap().eom);
            if drop_record {
                successful(catch_unwind(AssertUnwindSafe(|| drop(old))));
            } else {
                successful(old.retire());
            }
            fixture.finish_read(HttpErrorCode::DROPPED);
            assert_eq!(fixture.queue.borrow().buffered_body_bytes(), 3);
            assert_eq!(fixture.observations.deltas.borrow().as_slice(), &[3, 3, -3]);
            assert_eq!(fixture.observations.count("replacement stop"), 0);
            assert_eq!(fixture.observations.count("replacement drop"), 1);
            assert_eq!(fixture.observations.count("source stop"), 1);
            assert_eq!(fixture.observations.count("source drop"), 1);
            assert_eq!(fixture.observations.count("nested stop"), 1);
            assert_eq!(fixture.observations.count("nested drop"), 1);
            assert_eq!(fixture.observations.count("byte cancellation"), 1);
            assert_eq!(
                fixture.observations.errors.borrow().as_slice(),
                &[HttpErrorCode::CANCEL, HttpErrorCode::CANCEL]
            );
            assert!(
                fixture
                    .observations
                    .events
                    .borrow()
                    .iter()
                    .all(|(_, borrowed)| *borrowed)
            );
            let event = fixture.queue.borrow_mut().dequeue_body_event(100).unwrap();
            assert_eq!(event.as_body().unwrap().coalesce(3).unwrap(), b"new"[..]);
            assert!(fixture.queue.borrow().empty());
            assert_eq!(fixture.queue.borrow().buffered_body_bytes(), 0);
            assert_eq!(fixture.observations.read_drops.get(), 1);
            assert_eq!(fixture.observations.deltas.borrow().as_slice(), &[3, 3, -3]);
            CAPACITY_OBSERVER.take();
        }
    }
}
