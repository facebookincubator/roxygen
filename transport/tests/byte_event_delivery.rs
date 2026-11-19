/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod byte_event_delivery {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;

    use roxygen_core::byte_event::ByteEventType;
    use roxygen_core::byte_event::FieldSectionInfo;
    use roxygen_core::byte_event::FieldSectionType;
    use roxygen_core::byte_event::HttpByteEvent;
    use roxygen_core::byte_event::HttpByteEventCallback;
    use roxygen_core::byte_event::HttpByteEventRegistration;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::http::HttpHeaderSize;
    use roxygen_core::ids::StreamId;
    use roxygen_runtime::driver::WorkerDriver;
    use roxygen_runtime::wait::WaitStatus;
    use roxygen_runtime::worker::WorkerContext;
    use roxygen_runtime::worker::WorkerHandle;
    use roxygen_runtime::worker::WorkerState;
    use tokio::runtime::Builder;
    use tokio::task::LocalSet;
    use tokio::time;

    use crate::byte_event::AsyncSocketByteEventObserver;
    use crate::byte_event::ByteEventOffsets;
    use crate::byte_event::ByteEventWriteFlags;
    use crate::byte_event::PendingByteEvent;
    use crate::byte_event::SocketByteEventType;
    use crate::byte_event::TxAckEvent;

    #[derive(Default)]
    struct EventLog {
        fired: RefCell<Vec<(HttpByteEvent, ThreadId)>>,
        cancelled: RefCell<Vec<(HttpByteEvent, HttpErrorCode, ThreadId)>>,
        transport_destroyed: Option<Rc<Cell<bool>>>,
        transport_destroyed_at_cancel: RefCell<Vec<bool>>,
        worker: Option<WorkerHandle>,
        callback_states: RefCell<Vec<WorkerState>>,
    }

    impl HttpByteEventCallback for EventLog {
        fn on_byte_event(&self, event: HttpByteEvent) {
            self.fired
                .borrow_mut()
                .push((event, std::thread::current().id()));
        }

        fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
            self.cancelled
                .borrow_mut()
                .push((event, error.code(), std::thread::current().id()));
            if let Some(destroyed) = &self.transport_destroyed {
                self.transport_destroyed_at_cancel
                    .borrow_mut()
                    .push(destroyed.get());
            }
            if let Some(worker) = &self.worker {
                self.callback_states.borrow_mut().push(worker.state());
            }
        }
    }

    std::thread_local! {
        static ESCAPED_OBSERVER: RefCell<Option<(AsyncSocketByteEventObserver, Rc<EventLog>)>> =
            const { RefCell::new(None) };
        static ESCAPED_TX_ACK: RefCell<
            Option<(AsyncSocketByteEventObserver, TxAckEvent, Rc<EventLog>)>,
        > = const { RefCell::new(None) };
    }

    #[derive(Default)]
    struct WakeSignal {
        woken: AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl WakeSignal {
        fn record(&self) {
            self.woken.store(true, Ordering::Release);
            self.notify.notify_one();
        }

        fn take(&self) -> bool {
            self.woken.swap(false, Ordering::AcqRel)
        }

        async fn wait(&self) {
            while !self.take() {
                self.notify.notified().await;
            }
        }
    }

    impl Wake for WakeSignal {
        fn wake(self: Arc<Self>) {
            self.record();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.record();
        }
    }

    struct TransportDropProbe(Rc<Cell<bool>>);

    impl Drop for TransportDropProbe {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    fn run_worker<F, Fut>(test: F)
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(LocalSet::new().run_until(async move {
                time::pause();
                let (worker, driver) = WorkerDriver::new(32).unwrap();
                let call = worker.call_local(test).unwrap();
                let driver_task = tokio::task::spawn_local(driver);
                call.await.unwrap();
                worker.begin_shutdown();
                driver_task.await.unwrap();
            }));
    }

    fn registration(
        stream_id: Option<u64>,
        events: u8,
        callback: &Rc<EventLog>,
    ) -> HttpByteEventRegistration {
        HttpByteEventRegistration::new(stream_id.map(StreamId::new), events, callback)
    }

    fn offsets(
        stream_id: u64,
        session_byte_offset: u64,
        session_bytes_scheduled: u64,
        stream_offset: u64,
        body_offset: u64,
    ) -> ByteEventOffsets {
        ByteEventOffsets {
            stream_id: StreamId::new(stream_id),
            session_byte_offset,
            session_bytes_scheduled,
            stream_offset,
            body_offset,
        }
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    mod byte_event {
        use roxygen_runtime::context::RuntimeContext;
        use roxygen_runtime::wait::TimedWait;

        use super::*;
        use crate::byte_event::observer_cancellation_drains_for_test;
        use crate::byte_event::observer_cleanup_registrations_for_test;
        use crate::byte_event::timeout_queue_takes_for_test;

        const CANCELLED: &[u8] = b"ByteEvent Cancelled";
        const DROPPED: &[u8] = b"byte-event observer was dropped";
        const CLOSED: &[u8] = b"byte-event observer is closed";
        const FOREIGN: &[u8] = b"byte-event observer requires its exact worker context";
        const REGISTRATION_CANCELLED: &[u8] = b"ByteEvent registration cancelled";
        const STREAM_RESET: &[u8] = b"stream reset";

        type CancelledByteEvent = (ByteEventType, HttpErrorCode, Vec<u8>, ThreadId);

        #[derive(Default)]
        struct MessageLog {
            fired: RefCell<Vec<ByteEventType>>,
            cancelled: RefCell<Vec<CancelledByteEvent>>,
            worker: Option<WorkerHandle>,
            callback_states: RefCell<Vec<WorkerState>>,
            callback_contexts: RefCell<Vec<Option<RuntimeContext>>>,
        }

        impl MessageLog {
            fn on_worker(worker: &WorkerHandle) -> Rc<Self> {
                Rc::new(Self {
                    worker: Some(worker.clone()),
                    ..Self::default()
                })
            }
        }

        impl HttpByteEventCallback for MessageLog {
            fn on_byte_event(&self, event: HttpByteEvent) {
                self.fired.borrow_mut().push(event.event_type);
            }

            fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
                self.cancelled.borrow_mut().push((
                    event.event_type,
                    error.code(),
                    error.message().to_vec(),
                    std::thread::current().id(),
                ));
                if let Some(worker) = &self.worker {
                    self.callback_states.borrow_mut().push(worker.state());
                }
                self.callback_contexts
                    .borrow_mut()
                    .push(RuntimeContext::current());
            }
        }

        fn cancellation(
            event_type: ByteEventType,
            code: HttpErrorCode,
            message: &[u8],
        ) -> CancelledByteEvent {
            (
                event_type,
                code,
                message.to_vec(),
                std::thread::current().id(),
            )
        }

        fn live(events: u8, log: &Rc<MessageLog>) -> HttpByteEventRegistration {
            HttpByteEventRegistration::new(None, events, log)
        }

        fn assert_error(result: Result<usize, HttpError>, message: &[u8]) {
            let error = result.unwrap_err();
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(error.message(), message);
        }

        /// The observer's six byte-event containers.
        #[derive(Clone, Copy, Debug)]
        enum Container {
            TransportWrite,
            KernelWrite,
            TxAck,
            InFlightTxAck,
            Tx,
            Ack,
        }

        const CONTAINERS: [Container; 6] = [
            Container::TransportWrite,
            Container::KernelWrite,
            Container::TxAck,
            Container::InFlightTxAck,
            Container::Tx,
            Container::Ack,
        ];

        impl Container {
            fn event_type(self) -> ByteEventType {
                match self {
                    Self::TransportWrite => ByteEventType::TransportWrite,
                    Self::KernelWrite => ByteEventType::KernelWrite,
                    Self::TxAck | Self::InFlightTxAck | Self::Tx => ByteEventType::NicTx,
                    Self::Ack => ByteEventType::CumulativeAck,
                }
            }

            fn has_timeout(self) -> bool {
                matches!(self, Self::Tx | Self::Ack)
            }

            /// Queues one event in this container and returns the in-flight
            /// token that keeps an in-flight boundary in the observer.
            fn queue(
                self,
                observer: &AsyncSocketByteEventObserver,
                context: &WorkerContext,
                log: &Rc<MessageLog>,
            ) -> Option<TxAckEvent> {
                observer.set_tx_ack_events_enabled(context, true).unwrap();
                let scheduled = if self.has_timeout() {
                    observer
                        .socket_byte_event(context, SocketByteEventType::Write, 10)
                        .unwrap();
                    10
                } else {
                    0
                };
                observer
                    .register_byte_events(
                        context,
                        offsets(1, 10, scheduled, 10, 0),
                        None,
                        vec![live(self.event_type().bit(), log)],
                        false,
                    )
                    .unwrap();
                matches!(self, Self::InFlightTxAck)
                    .then(|| observer.next_tx_ack_event(context).unwrap().unwrap())
            }

            /// Queues one event, in this container only.
            fn populate(
                self,
                observer: &AsyncSocketByteEventObserver,
                context: &WorkerContext,
                log: &Rc<MessageLog>,
            ) -> Option<TxAckEvent> {
                let tx_ack = self.queue(observer, context, log);
                assert_eq!(observer.pending_events(context).unwrap(), 1, "{self:?}");
                assert_eq!(
                    observer.pending_timeout_refs(context).unwrap(),
                    usize::from(self.has_timeout()),
                    "{self:?}",
                );
                tx_ack
            }
        }

        struct EscapedObserver {
            _tx_ack: Option<TxAckEvent>,
            observer: AsyncSocketByteEventObserver,
            other: Option<AsyncSocketByteEventObserver>,
            log: Rc<MessageLog>,
            zero_refs: Option<Pin<Box<TimedWait>>>,
            request: Option<RuntimeContext>,
        }

        impl EscapedObserver {
            fn new(observer: AsyncSocketByteEventObserver, log: Rc<MessageLog>) -> Self {
                Self {
                    _tx_ack: None,
                    observer,
                    other: None,
                    log,
                    zero_refs: None,
                    request: None,
                }
            }
        }

        std::thread_local! {
            static ESCAPED: RefCell<Option<EscapedObserver>> = const { RefCell::new(None) };
        }

        /// Runs `setup` in a worker task whose observer outlives the task, shuts
        /// the worker down, and runs `check` once its driver has finished.
        fn shut_down_with_escaped_observer<Setup, Check>(setup: Setup, check: Check)
        where
            Setup: FnOnce(WorkerContext) -> EscapedObserver + Send + 'static,
            Check: FnOnce(EscapedObserver),
        {
            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    worker
                        .spawn_local(move |context| async move {
                            let escaped = setup(context);
                            ESCAPED.with(|slot| assert!(slot.replace(Some(escaped)).is_none()));
                            started_tx.send(()).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    started_rx.await.unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    let escaped = ESCAPED
                        .with(|slot| slot.borrow_mut().take())
                        .expect("the observer escaped its task");
                    check(escaped);
                }));
        }

        /// Arms one TX or ACK event and then cancels its registration in
        /// place, as a stream reset leaves it. Also leaves a zero-count
        /// in-flight boundary after a socket cancellation.
        fn populate_stale(
            observer: &AsyncSocketByteEventObserver,
            context: &WorkerContext,
            log: &Rc<MessageLog>,
        ) -> TxAckEvent {
            let error = HttpError::with_message(HttpErrorCode::CANCEL, STREAM_RESET);
            observer.set_tx_ack_events_enabled(context, true).unwrap();
            observer
                .socket_byte_event(context, SocketByteEventType::Write, 10)
                .unwrap();
            for (stream, event_type) in
                [(1, ByteEventType::NicTx), (3, ByteEventType::CumulativeAck)]
            {
                observer
                    .register_byte_events(
                        context,
                        offsets(stream, 10, 10, 10, 0),
                        None,
                        vec![live(event_type.bit(), log)],
                        false,
                    )
                    .unwrap();
            }
            observer.cancel_queued_socket_registration_for_test(SocketByteEventType::Tx, &error);
            observer.cancel_queued_socket_registration_for_test(SocketByteEventType::Ack, &error);
            observer
                .register_byte_events(
                    context,
                    offsets(5, 20, 10, 20, 0),
                    None,
                    vec![live(ByteEventType::NicTx.bit(), log)],
                    false,
                )
                .unwrap();
            let tx_ack = observer.next_tx_ack_event(context).unwrap().unwrap();
            assert_eq!(
                observer
                    .cancel_socket_byte_event(context, SocketByteEventType::Tx, 20, &error)
                    .unwrap(),
                1,
            );
            assert_eq!(observer.pending_events(context).unwrap(), 2);
            assert_eq!(observer.pending_timeout_refs(context).unwrap(), 2);
            assert_eq!(
                *log.cancelled.borrow(),
                vec![
                    cancellation(ByteEventType::NicTx, HttpErrorCode::CANCEL, STREAM_RESET),
                    cancellation(
                        ByteEventType::CumulativeAck,
                        HttpErrorCode::CANCEL,
                        STREAM_RESET,
                    ),
                    cancellation(ByteEventType::NicTx, HttpErrorCode::CANCEL, STREAM_RESET),
                ],
            );
            tx_ack
        }

        #[test]
        fn fresh_or_cancelled_observer_drop_runs_no_callback() {
            run_worker(|context| async move {
                let fresh = AsyncSocketByteEventObserver::new(&context).unwrap();
                let mut zero_refs = Box::pin(fresh.zero_refs(&context).unwrap());
                assert_eq!(
                    poll_once(zero_refs.as_mut(), Waker::noop()),
                    Poll::Ready(WaitStatus::Signalled),
                );
                drop(zero_refs);
                drop(fresh);

                let cancelled = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(MessageLog::default());
                let tx_ack: Vec<_> = CONTAINERS
                    .into_iter()
                    .filter_map(|container| container.queue(&cancelled, &context, &log))
                    .collect();
                assert_eq!(cancelled.pending_events(&context).unwrap(), 6);
                assert_eq!(cancelled.pending_timeout_refs(&context).unwrap(), 2);
                cancelled
                    .cancel_events(&context, &HttpError::new(HttpErrorCode::INTERNAL_ERROR))
                    .unwrap();
                assert_eq!(log.cancelled.borrow().len(), 6);
                assert!(
                    log.cancelled
                        .borrow()
                        .iter()
                        .all(|(_, code, _, _)| *code == HttpErrorCode::INTERNAL_ERROR)
                );
                let mut zero_refs = Box::pin(cancelled.zero_refs(&context).unwrap());
                assert_eq!(
                    poll_once(zero_refs.as_mut(), Waker::noop()),
                    Poll::Ready(WaitStatus::Signalled),
                );
                drop(zero_refs);
                drop(cancelled);
                drop(tx_ack);
                assert_eq!(log.cancelled.borrow().len(), 6, "no callback on drop");
                assert!(log.fired.borrow().is_empty());
            });
        }

        #[test]
        fn observer_drop_cancels_each_container_once() {
            run_worker(|context| async move {
                for container in CONTAINERS {
                    let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let log = Rc::new(MessageLog::default());
                    let tx_ack = container.populate(&observer, &context, &log);
                    let signal = Arc::new(WakeSignal::default());
                    let waker = Waker::from(Arc::clone(&signal));
                    let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                    let pending = poll_once(zero_refs.as_mut(), &waker).is_pending();
                    assert_eq!(pending, container.has_timeout(), "{container:?}");
                    drop(observer);
                    assert_eq!(
                        *log.cancelled.borrow(),
                        vec![cancellation(
                            container.event_type(),
                            HttpErrorCode::CANCEL,
                            CANCELLED,
                        )],
                        "{container:?}",
                    );
                    if pending {
                        assert!(
                            signal.take(),
                            "{container:?}: the zero_refs waiter is woken"
                        );
                    }
                    drop(zero_refs);
                    drop(tx_ack);
                    assert_eq!(log.cancelled.borrow().len(), 1, "{container:?}");
                    assert!(log.fired.borrow().is_empty(), "{container:?}");
                }
            });
        }

        #[test]
        fn worker_shutdown_cancels_each_container_with_dropped() {
            for container in CONTAINERS {
                shut_down_with_escaped_observer(
                    move |context| {
                        let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                        let log = MessageLog::on_worker(context.handle());
                        let tx_ack = container.populate(&observer, &context, &log);
                        let zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                        EscapedObserver {
                            _tx_ack: tx_ack,
                            zero_refs: Some(zero_refs),
                            ..EscapedObserver::new(observer, log)
                        }
                    },
                    move |mut escaped| {
                        assert_eq!(
                            *escaped.log.cancelled.borrow(),
                            vec![cancellation(
                                container.event_type(),
                                HttpErrorCode::DROPPED,
                                DROPPED,
                            )],
                            "{container:?}",
                        );
                        assert_eq!(
                            *escaped.log.callback_states.borrow(),
                            vec![WorkerState::Closing],
                            "{container:?}",
                        );
                        let mut zero_refs = escaped.zero_refs.take().unwrap();
                        assert_eq!(
                            poll_once(zero_refs.as_mut(), Waker::noop()),
                            Poll::Ready(WaitStatus::Signalled),
                            "{container:?}",
                        );
                        drop(zero_refs);
                        let log = Rc::clone(&escaped.log);
                        drop(escaped);
                        assert_eq!(log.cancelled.borrow().len(), 1, "{container:?}");
                        assert!(log.fired.borrow().is_empty(), "{container:?}");
                    },
                );
            }
        }

        #[test]
        fn observer_close_releases_stale_entries_and_zero_count_boundaries() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(MessageLog::default());
                let tx_ack = populate_stale(&observer, &context, &log);
                let drains = observer_cancellation_drains_for_test();
                drop(observer);
                assert_eq!(
                    observer_cancellation_drains_for_test(),
                    drains + 1,
                    "stale entries and a zero-count boundary take the cancellation path",
                );
                drop(tx_ack);
                assert_eq!(log.cancelled.borrow().len(), 3);
                assert!(log.fired.borrow().is_empty());
            });

            shut_down_with_escaped_observer(
                |context| {
                    let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let log = MessageLog::on_worker(context.handle());
                    let tx_ack = populate_stale(&observer, &context, &log);
                    let zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                    EscapedObserver {
                        _tx_ack: Some(tx_ack),
                        zero_refs: Some(zero_refs),
                        ..EscapedObserver::new(observer, log)
                    }
                },
                |mut escaped| {
                    let mut zero_refs = escaped.zero_refs.take().unwrap();
                    assert_eq!(
                        poll_once(zero_refs.as_mut(), Waker::noop()),
                        Poll::Ready(WaitStatus::Signalled),
                        "worker-shutdown close releases the stale entries' timeout references",
                    );
                    drop(zero_refs);
                    assert_eq!(escaped.log.cancelled.borrow().len(), 3);
                    let log = Rc::clone(&escaped.log);
                    drop(escaped);
                    assert_eq!(log.cancelled.borrow().len(), 3);
                    assert!(log.fired.borrow().is_empty());
                },
            );
        }

        #[test]
        fn empty_observer_poll_timeouts_keeps_its_result_and_errors() {
            type Escaped = (
                WorkerContext,
                WorkerContext,
                AsyncSocketByteEventObserver,
                AsyncSocketByteEventObserver,
                Rc<MessageLog>,
            );
            std::thread_local! {
                static FOREIGN_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
                static OBSERVERS: RefCell<Option<Escaped>> = const { RefCell::new(None) };
            }

            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    time::pause();
                    let (worker, driver) = WorkerDriver::new(32).unwrap();
                    let (foreign_worker, foreign_driver) = WorkerDriver::new(32).unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    let foreign_task = tokio::task::spawn_local(foreign_driver);
                    foreign_worker
                        .call_local(|foreign| async move {
                            FOREIGN_CONTEXT.with(|slot| *slot.borrow_mut() = Some(foreign));
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    worker
                        .call_local(|context| async move {
                            let foreign = FOREIGN_CONTEXT
                                .with(|slot| slot.borrow_mut().take())
                                .expect("foreign worker published its context");
                            let empty = AsyncSocketByteEventObserver::new(&context).unwrap();
                            let armed = AsyncSocketByteEventObserver::new(&context).unwrap();
                            let log = MessageLog::on_worker(context.handle());
                            Container::Tx.populate(&armed, &context, &log);
                            let signal = Arc::new(WakeSignal::default());
                            let waker = Waker::from(Arc::clone(&signal));
                            let mut task_context = Context::from_waker(&waker);
                            assert_eq!(
                                empty.poll_timeouts(&context, &mut task_context).unwrap(),
                                0
                            );
                            assert_eq!(
                                empty.poll_timeouts(&context, &mut task_context).unwrap(),
                                0
                            );
                            assert_eq!(
                                armed.poll_timeouts(&context, &mut task_context).unwrap(),
                                0
                            );
                            assert!(!signal.take());
                            for observer in [&empty, &armed] {
                                assert_error(
                                    observer.poll_timeouts(&foreign, &mut task_context),
                                    FOREIGN,
                                );
                            }

                            // Stay in this poll so driver shutdown cannot close the observer first.
                            assert!(context.handle().begin_shutdown());
                            for observer in [&empty, &armed] {
                                assert_error(
                                    observer.poll_timeouts(&context, &mut task_context),
                                    CLOSED,
                                );
                                assert_error(
                                    observer.poll_timeouts(&foreign, &mut task_context),
                                    FOREIGN,
                                );
                            }
                            OBSERVERS.with(|slot| {
                                *slot.borrow_mut() = Some((context, foreign, empty, armed, log));
                            });
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    driver_task.await.unwrap();
                    let (context, foreign, empty, armed, log) = OBSERVERS
                        .with(|slot| slot.borrow_mut().take())
                        .expect("observers escaped the command");
                    assert_eq!(
                        *log.cancelled.borrow(),
                        vec![cancellation(
                            ByteEventType::NicTx,
                            HttpErrorCode::DROPPED,
                            DROPPED
                        )],
                    );
                    let mut task_context = Context::from_waker(Waker::noop());
                    for observer in [&empty, &armed] {
                        assert_error(observer.poll_timeouts(&context, &mut task_context), CLOSED);
                    }
                    drop((empty, armed, context, foreign));
                    assert_eq!(log.cancelled.borrow().len(), 1);
                    foreign_worker.begin_shutdown();
                    foreign_task.await.unwrap();
                }));
        }

        #[test]
        fn single_armed_tx_event_expires_with_unchanged_count_and_timing() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let timeout = Duration::from_millis(10);
                observer.set_byte_event_timeout(&context, timeout).unwrap();
                let log = Rc::new(MessageLog::default());
                let armed_at = time::Instant::now();
                Container::Tx.populate(&observer, &context, &log);
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                time::advance(timeout - Duration::from_millis(1)).await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                assert!(!signal.take());
                assert!(log.cancelled.borrow().is_empty());

                signal.wait().await;
                let elapsed = time::Instant::now() - armed_at;
                assert!(
                    elapsed >= timeout && elapsed <= timeout + Duration::from_millis(1),
                    "the event times out at the timer tick of its arming plus its timeout, \
                     not {elapsed:?} after it",
                );
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(
                    *log.cancelled.borrow(),
                    vec![cancellation(
                        ByteEventType::NicTx,
                        HttpErrorCode::READ_TIMEOUT,
                        b"ByteEvent Timeout",
                    )],
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                assert_eq!(
                    poll_once(zero_refs.as_mut(), Waker::noop()),
                    Poll::Ready(WaitStatus::Signalled),
                );
                drop(zero_refs);
                drop(observer);
                assert_eq!(log.cancelled.borrow().len(), 1);
            });
        }

        #[test]
        fn poll_timeouts_discards_a_stale_entry_left_in_one_queue() {
            run_worker(|context| async move {
                for (container, socket_type) in [
                    (Container::Ack, SocketByteEventType::Ack),
                    (Container::Tx, SocketByteEventType::Tx),
                ] {
                    let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let log = Rc::new(MessageLog::default());
                    container.populate(&observer, &context, &log);
                    observer.cancel_queued_socket_registration_for_test(
                        socket_type,
                        &HttpError::with_message(HttpErrorCode::CANCEL, STREAM_RESET),
                    );
                    assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);
                    let signal = Arc::new(WakeSignal::default());
                    let waker = Waker::from(Arc::clone(&signal));
                    let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                    assert!(poll_once(zero_refs.as_mut(), &waker).is_pending());

                    let mut task_context = Context::from_waker(Waker::noop());
                    assert_eq!(
                        observer.poll_timeouts(&context, &mut task_context).unwrap(),
                        0
                    );
                    assert_eq!(
                        observer.pending_timeout_refs(&context).unwrap(),
                        0,
                        "{container:?}"
                    );
                    assert_eq!(
                        observer.pending_events(&context).unwrap(),
                        0,
                        "{container:?}"
                    );
                    assert!(signal.take(), "{container:?}");
                    assert_eq!(
                        poll_once(zero_refs.as_mut(), &waker),
                        Poll::Ready(WaitStatus::Signalled),
                        "{container:?}",
                    );
                    drop(zero_refs);
                    assert_eq!(
                        *log.cancelled.borrow(),
                        vec![cancellation(
                            container.event_type(),
                            HttpErrorCode::CANCEL,
                            STREAM_RESET,
                        )],
                        "{container:?}",
                    );
                    drop(observer);
                    assert_eq!(log.cancelled.borrow().len(), 1, "{container:?}");
                }
            });
        }

        #[test]
        fn event_armed_after_a_drained_queue_still_expires() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let timeout = Duration::from_millis(10);
                observer.set_byte_event_timeout(&context, timeout).unwrap();
                let log = Rc::new(MessageLog::default());
                Container::Tx.populate(&observer, &context, &log);
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, 10)
                        .unwrap(),
                    1,
                );
                assert_eq!(*log.fired.borrow(), vec![ByteEventType::NicTx]);
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );

                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 20)
                    .unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(3, 20, 20, 20, 0),
                        None,
                        vec![live(ByteEventType::NicTx.bit(), &log)],
                        false,
                    )
                    .unwrap();
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                time::advance(timeout).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(
                    *log.cancelled.borrow(),
                    vec![cancellation(
                        ByteEventType::NicTx,
                        HttpErrorCode::READ_TIMEOUT,
                        b"ByteEvent Timeout",
                    )],
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
            });
        }

        #[test]
        fn registered_observer_cancels_with_dropped_in_its_cleanup_envelope() {
            shut_down_with_escaped_observer(
                |context| {
                    let request = RuntimeContext::new();
                    let log = MessageLog::on_worker(context.handle());
                    let observer = {
                        let _entered = request.enter();
                        let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                        observer
                            .register_byte_events(
                                &context,
                                offsets(1, 10, 0, 10, 0),
                                None,
                                vec![live(ByteEventType::TransportWrite.bit(), &log)],
                                false,
                            )
                            .unwrap();
                        observer
                    };
                    assert_ne!(RuntimeContext::current(), Some(request.clone()));
                    EscapedObserver {
                        request: Some(request),
                        ..EscapedObserver::new(observer, log)
                    }
                },
                |escaped| {
                    assert_eq!(
                        *escaped.log.cancelled.borrow(),
                        vec![cancellation(
                            ByteEventType::TransportWrite,
                            HttpErrorCode::DROPPED,
                            DROPPED,
                        )],
                    );
                    assert_eq!(
                        *escaped.log.callback_states.borrow(),
                        vec![WorkerState::Closing],
                    );
                    assert_eq!(
                        *escaped.log.callback_contexts.borrow(),
                        vec![escaped.request.clone()],
                        "the cleanup runs in the envelope that its registration captured",
                    );
                    assert!(escaped.request.is_some());
                },
            );
        }

        #[test]
        fn observer_without_live_events_holds_no_cleanup_registration() {
            run_worker(|context| async move {
                let registrations = observer_cleanup_registrations_for_test();
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                assert!(!observer.has_cleanup_registration_for_test());
                let dead = Rc::new(MessageLog::default());
                let dead_registration = live(ByteEventType::TransportWrite.bit(), &dead);
                drop(dead);
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![dead_registration, HttpByteEventRegistration::default()],
                        false,
                    )
                    .unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        Vec::new(),
                        false,
                    )
                    .unwrap();
                assert_eq!(observer.transport_write(&context, 10).unwrap(), 0);
                let mut task_context = Context::from_waker(Waker::noop());
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
                assert!(!observer.has_cleanup_registration_for_test());
                assert_eq!(observer_cleanup_registrations_for_test(), registrations);
                drop(observer);
                assert_eq!(observer_cleanup_registrations_for_test(), registrations);
            });
        }

        #[test]
        fn first_live_registration_registers_the_cleanup_once() {
            run_worker(|context| async move {
                let registrations = observer_cleanup_registrations_for_test();
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                assert!(!observer.has_cleanup_registration_for_test());
                assert_eq!(observer_cleanup_registrations_for_test(), registrations);
                let log = Rc::new(MessageLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![live(ByteEventType::TransportWrite.bit(), &log)],
                        false,
                    )
                    .unwrap();
                assert!(observer.has_cleanup_registration_for_test());
                assert_eq!(observer_cleanup_registrations_for_test(), registrations + 1);
                for event_type in [ByteEventType::KernelWrite, ByteEventType::NicTx] {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, 10, 0, 10, 0),
                            None,
                            vec![live(event_type.bit(), &log)],
                            false,
                        )
                        .unwrap();
                }
                assert_eq!(observer.transport_write(&context, 10).unwrap(), 1);
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 10, None)
                        .unwrap(),
                    1,
                );
                assert!(observer.has_cleanup_registration_for_test());
                assert_eq!(
                    observer_cleanup_registrations_for_test(),
                    registrations + 1,
                    "later batches register no cleanup",
                );
                assert_eq!(
                    *log.cancelled.borrow(),
                    vec![cancellation(
                        ByteEventType::NicTx,
                        HttpErrorCode::CANCEL,
                        b"TX/ACK Events disabled",
                    )],
                );
                drop(observer);
                assert_eq!(log.cancelled.borrow().len(), 1);
            });
        }

        #[test]
        fn registration_after_shutdown_begins_cancels_without_dropped() {
            shut_down_with_escaped_observer(
                |context| {
                    let registrations = observer_cleanup_registrations_for_test();
                    let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let log = MessageLog::on_worker(context.handle());
                    assert!(context.handle().begin_shutdown());
                    let error = observer
                        .register_byte_events_after_context_check_for_test(
                            &context,
                            offsets(1, 10, 0, 10, 0),
                            None,
                            vec![
                                live(
                                    ByteEventType::TransportWrite.bit()
                                        | ByteEventType::KernelWrite.bit(),
                                    &log,
                                ),
                                HttpByteEventRegistration::new(
                                    Some(StreamId::new(3)),
                                    ByteEventType::NicTx.bit(),
                                    &log,
                                ),
                            ],
                            false,
                        )
                        .unwrap_err();
                    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                    assert_eq!(error.message(), CLOSED);
                    assert_eq!(
                        *log.cancelled.borrow(),
                        vec![
                            cancellation(
                                ByteEventType::TransportWrite,
                                HttpErrorCode::CANCEL,
                                REGISTRATION_CANCELLED,
                            ),
                            cancellation(
                                ByteEventType::KernelWrite,
                                HttpErrorCode::CANCEL,
                                REGISTRATION_CANCELLED,
                            ),
                            cancellation(
                                ByteEventType::NicTx,
                                HttpErrorCode::CANCEL,
                                REGISTRATION_CANCELLED,
                            ),
                        ],
                    );
                    assert_eq!(*log.callback_states.borrow(), vec![WorkerState::Closing; 3],);
                    assert!(!observer.has_cleanup_registration_for_test());
                    assert_eq!(observer_cleanup_registrations_for_test(), registrations);
                    EscapedObserver::new(observer, log)
                },
                |escaped| {
                    assert_eq!(escaped.log.cancelled.borrow().len(), 3);
                    assert!(
                        escaped
                            .log
                            .cancelled
                            .borrow()
                            .iter()
                            .all(|(_, code, _, _)| *code == HttpErrorCode::CANCEL),
                        "no registration is cancelled with DROPPED",
                    );
                    let log = Rc::clone(&escaped.log);
                    drop(escaped);
                    assert_eq!(log.cancelled.borrow().len(), 3);
                },
            );
        }

        #[test]
        fn eventless_observer_is_unregistered_once_the_worker_leaves_open() {
            shut_down_with_escaped_observer(
                |context| {
                    let eventless = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let registered = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let log = MessageLog::on_worker(context.handle());
                    registered
                        .register_byte_events(
                            &context,
                            offsets(1, 10, 0, 10, 0),
                            None,
                            vec![live(ByteEventType::TransportWrite.bit(), &log)],
                            false,
                        )
                        .unwrap();
                    assert!(eventless.is_registered());
                    assert!(registered.is_registered());
                    assert!(context.handle().begin_shutdown());
                    assert!(
                        !eventless.is_registered(),
                        "an observer without events is unregistered once the worker leaves Open",
                    );
                    assert!(
                        registered.is_registered(),
                        "an observer with events stays registered until its cleanup closes it",
                    );
                    EscapedObserver {
                        other: Some(eventless),
                        ..EscapedObserver::new(registered, log)
                    }
                },
                |escaped| {
                    assert!(!escaped.observer.is_registered());
                    assert!(!escaped.other.as_ref().unwrap().is_registered());
                    assert_eq!(
                        *escaped.log.cancelled.borrow(),
                        vec![cancellation(
                            ByteEventType::TransportWrite,
                            HttpErrorCode::DROPPED,
                            DROPPED,
                        )],
                    );
                },
            );
        }

        #[test]
        fn empty_observer_close_skips_the_cancellation_drain() {
            run_worker(|context| async move {
                let drains = observer_cancellation_drains_for_test;
                let before = drains();
                drop(AsyncSocketByteEventObserver::new(&context).unwrap());
                assert_eq!(drains(), before, "a fresh observer's close skips the drain");

                let fired = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(MessageLog::default());
                fired
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![live(ByteEventType::TransportWrite.bit(), &log)],
                        false,
                    )
                    .unwrap();
                assert_eq!(fired.transport_write(&context, 10).unwrap(), 1);
                drop(fired);
                assert_eq!(
                    drains(),
                    before,
                    "an observer whose events fired skips the drain"
                );

                let cancelled = AsyncSocketByteEventObserver::new(&context).unwrap();
                let tx_ack = Container::InFlightTxAck.populate(&cancelled, &context, &log);
                cancelled
                    .cancel_events(&context, &HttpError::new(HttpErrorCode::CANCEL))
                    .unwrap();
                drop(cancelled);
                drop(tx_ack);
                assert_eq!(drains(), before, "a cancelled observer skips the drain");

                for (index, container) in CONTAINERS.into_iter().enumerate() {
                    let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                    let tx_ack = container.populate(&observer, &context, &log);
                    drop(observer);
                    assert_eq!(drains(), before + index + 1, "{container:?}");
                    drop(tx_ack);
                }
            });
        }

        #[test]
        fn poll_timeouts_without_timeout_events_takes_no_queue() {
            run_worker(|context| async move {
                let takes = timeout_queue_takes_for_test;
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let timeout = Duration::from_millis(10);
                observer.set_byte_event_timeout(&context, timeout).unwrap();
                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                let before = takes();
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                assert_eq!(takes(), before);

                let log = Rc::new(MessageLog::default());
                for container in [Container::TransportWrite, Container::TxAck] {
                    let other = AsyncSocketByteEventObserver::new(&context).unwrap();
                    container.populate(&other, &context, &log);
                    assert_eq!(other.poll_timeouts(&context, &mut task_context).unwrap(), 0);
                    assert_eq!(takes(), before, "{container:?} holds no timeout event");
                }

                Container::Ack.populate(&observer, &context, &log);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                assert_eq!(takes(), before + 2);
                time::advance(timeout).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(takes(), before + 4);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                assert_eq!(takes(), before + 4, "both timeout queues are empty again");
            });
        }

        #[test]
        pub(super) fn timeout_then_late_transport_callback_is_ignored() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .set_byte_event_timeout(&context, Duration::from_millis(10))
                    .unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 0, 100, 0, 0),
                        None,
                        vec![registration(None, ByteEventType::NicTx.bit(), &log)],
                        false,
                    )
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 100)
                    .unwrap();
                let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                observer
                    .transport_write_complete(&context, 100, Some(tx_ack))
                    .unwrap();
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);

                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                time::advance(Duration::from_millis(10)).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                assert_eq!(log.cancelled.borrow().len(), 1);
                assert_eq!(log.cancelled.borrow()[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::READ_TIMEOUT);
                assert_eq!(log.cancelled.borrow()[0].2, owner);
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, 100)
                        .unwrap(),
                    0,
                );
                assert!(log.fired.borrow().is_empty());
                assert_eq!(log.cancelled.borrow().len(), 1);
                let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                assert!(poll_once(zero_refs.as_mut(), Waker::noop()).is_ready());
                drop(zero_refs);
                drop(observer);
            });
        }

        #[test]
        fn pending_event_fires_or_cancels_each_kind_once() {
            let log = Rc::new(EventLog::default());
            let first = HttpByteEvent {
                stream_id: Some(StreamId::new(7)),
                field_section_info: None,
                body_offset: 2,
                transport_offset: 9,
                stream_offset: 4,
                eom: false,
                event_type: ByteEventType::TransportWrite,
            };
            let mut second = first.clone();
            second.event_type = ByteEventType::KernelWrite;
            let mut pending = VecDeque::from([
                PendingByteEvent::new(
                    9,
                    first.clone(),
                    registration(None, ByteEventType::TransportWrite.bit(), &log),
                ),
                PendingByteEvent::new(
                    10,
                    second.clone(),
                    registration(None, ByteEventType::KernelWrite.bit(), &log),
                ),
            ]);
            assert_eq!(PendingByteEvent::fire_events(&mut pending, 8), 0);
            assert_eq!(PendingByteEvent::fire_events(&mut pending, 9), 1);
            assert_eq!(PendingByteEvent::fire_events(&mut pending, 10), 1);
            assert_eq!(
                log.fired.borrow().as_slice(),
                &[
                    (first, std::thread::current().id()),
                    (second, std::thread::current().id()),
                ],
            );
            assert!(log.cancelled.borrow().is_empty());

            let event = HttpByteEvent::new(ByteEventType::KernelWrite);
            let pending = PendingByteEvent::new(
                10,
                event,
                registration(None, ByteEventType::KernelWrite.bit(), &log),
            );
            drop(pending);
            assert_eq!(log.cancelled.borrow().len(), 1);
            assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::CANCEL);

            let event = HttpByteEvent::new(ByteEventType::NicTx);
            let mut pending = VecDeque::from([PendingByteEvent::new(
                11,
                event,
                registration(None, ByteEventType::NicTx.bit(), &log),
            )]);
            PendingByteEvent::cancel_events(
                &mut pending,
                &HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR),
            );
            assert!(pending.is_empty());
            assert_eq!(log.cancelled.borrow().len(), 2);
            assert_eq!(
                log.cancelled.borrow()[1].1,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
            );
        }

        #[test]
        fn write_progress_without_ready_events_keeps_order_and_context_checks() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                assert_eq!(observer.transport_write(&context, 10).unwrap(), 0);
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 10, None)
                        .unwrap(),
                    0
                );
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Write, 10)
                        .unwrap(),
                    0
                );
                let log = Rc::new(EventLog::default());
                let event_bits =
                    ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 20, 10, 20, 0),
                        None,
                        vec![registration(None, event_bits, &log)],
                        false,
                    )
                    .unwrap();
                assert_eq!(observer.transport_write(&context, 19).unwrap(), 0);
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 19, None)
                        .unwrap(),
                    0
                );
                assert!(log.fired.borrow().is_empty());
                assert_eq!(observer.transport_write(&context, 20).unwrap(), 1);
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 20, None)
                        .unwrap(),
                    1
                );
                let fired: Vec<_> = log
                    .fired
                    .borrow()
                    .iter()
                    .map(|(event, _)| event.event_type)
                    .collect();
                assert_eq!(
                    fired,
                    [ByteEventType::TransportWrite, ByteEventType::KernelWrite]
                );
                assert_eq!(observer.pending_events(&context).unwrap(), 0);

                assert!(context.handle().begin_shutdown());
                for result in [
                    observer.transport_write(&context, 30),
                    observer.transport_write_complete(&context, 30, None),
                    observer.socket_byte_event(&context, SocketByteEventType::Write, 30),
                ] {
                    assert_eq!(
                        result.unwrap_err().code(),
                        HttpErrorCode::INVALID_STATE_TRANSITION
                    );
                }
            });
        }

        #[test]
        fn socket_cancellation_is_exact_by_kind_and_session_boundary() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .set_byte_event_timeout(&context, Duration::from_millis(10))
                    .unwrap();
                let log = Rc::new(EventLog::default());
                let events = ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit();
                for (session_offset, raw_offset) in [(10, 100), (20, 200)] {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, session_offset, 0, session_offset, 0),
                            None,
                            vec![registration(None, events, &log)],
                            false,
                        )
                        .unwrap();
                    let boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Write, raw_offset)
                        .unwrap();
                    observer
                        .transport_write_complete(&context, session_offset, Some(boundary))
                        .unwrap();
                }
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 4);

                let error = HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(
                    observer
                        .cancel_socket_byte_event(&context, SocketByteEventType::Tx, 10, &error,)
                        .unwrap(),
                    1,
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 3);
                assert_eq!(log.cancelled.borrow().len(), 1);
                assert_eq!(log.cancelled.borrow()[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(log.cancelled.borrow()[0].0.transport_offset, 10);

                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Ack, 100)
                        .unwrap(),
                    1,
                );
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, 200)
                        .unwrap(),
                    1,
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);

                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0,
                );
                time::advance(Duration::from_millis(10)).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1,
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                assert_eq!(log.fired.borrow().len(), 2);
                assert_eq!(
                    log.fired.borrow()[0].0.event_type,
                    ByteEventType::CumulativeAck
                );
                assert_eq!(log.fired.borrow()[0].0.transport_offset, 10);
                assert_eq!(log.fired.borrow()[1].0.event_type, ByteEventType::NicTx);
                assert_eq!(log.fired.borrow()[1].0.transport_offset, 20);
                assert_eq!(log.cancelled.borrow().len(), 2);
                assert_eq!(
                    log.cancelled.borrow()[1].0.event_type,
                    ByteEventType::CumulativeAck,
                );
                assert_eq!(log.cancelled.borrow()[1].0.transport_offset, 20);
                assert_eq!(log.cancelled.borrow()[1].1, HttpErrorCode::READ_TIMEOUT);
                assert_eq!(
                    observer
                        .cancel_socket_byte_event(&context, SocketByteEventType::Tx, 10, &error,)
                        .unwrap(),
                    0,
                );
            });
        }

        #[test]
        fn closing_worker_can_cancel_byte_events_and_wait_for_timeout_refs() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                assert!(context.is_owner_thread());
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .set_byte_event_timeout(&context, Duration::from_secs(10))
                    .unwrap();
                let log = Rc::new(EventLog {
                    worker: Some(context.handle().clone()),
                    ..EventLog::default()
                });
                let event_bits = ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit();
                for (stream, offset) in [(1, 10), (3, 20)] {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(stream, offset, offset - 10, offset, 0),
                            None,
                            vec![registration(None, event_bits, &log)],
                            false,
                        )
                        .unwrap();
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Write, offset)
                        .unwrap();
                    let boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                    observer
                        .transport_write_complete(&context, offset, Some(boundary))
                        .unwrap();
                }
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 4);
                assert!(log.fired.borrow().is_empty());
                assert!(log.cancelled.borrow().is_empty());

                // Stay in this poll so driver shutdown cannot close the observer first.
                assert!(context.handle().begin_shutdown());
                assert_eq!(context.handle().state(), WorkerState::Closing);
                let rejected_log = Rc::new(EventLog::default());
                assert_eq!(
                    observer
                        .register_byte_events(
                            &context,
                            offsets(5, 30, 20, 30, 0),
                            None,
                            vec![registration(None, event_bits, &rejected_log)],
                            false,
                        )
                        .unwrap_err()
                        .code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                );
                assert!(rejected_log.fired.borrow().is_empty());

                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                assert!(poll_once(zero_refs.as_mut(), &waker).is_pending());
                let error = HttpError::new(HttpErrorCode::CORO_CANCELLED);
                assert_eq!(
                    observer
                        .cancel_stream_events(&context, StreamId::new(1), &error)
                        .unwrap(),
                    2,
                );
                assert_eq!(log.cancelled.borrow().len(), 2);
                assert!(
                    log.cancelled
                        .borrow()
                        .iter()
                        .all(|(event, _, _)| { event.stream_id == Some(StreamId::new(1)) })
                );
                assert!(!signal.take());
                assert!(poll_once(zero_refs.as_mut(), &waker).is_pending());

                observer.cancel_events(&context, &error).unwrap();
                assert!(signal.take());
                assert_eq!(
                    poll_once(zero_refs.as_mut(), &waker),
                    Poll::Ready(WaitStatus::Signalled),
                );
                drop(zero_refs);
                let mut fresh_zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                assert_eq!(
                    poll_once(fresh_zero_refs.as_mut(), Waker::noop()),
                    Poll::Ready(WaitStatus::Signalled),
                );
                drop(fresh_zero_refs);
                assert_eq!(
                    observer
                        .cancel_stream_events(&context, StreamId::new(1), &error)
                        .unwrap(),
                    0,
                );
                observer.cancel_events(&context, &error).unwrap();
                drop(observer);

                assert!(log.fired.borrow().is_empty());
                let cancelled = log.cancelled.borrow();
                assert_eq!(cancelled.len(), 4);
                for stream in [1, 3] {
                    for event_type in [ByteEventType::NicTx, ByteEventType::CumulativeAck] {
                        assert_eq!(
                            cancelled
                                .iter()
                                .filter(|(event, code, thread)| {
                                    event.stream_id == Some(StreamId::new(stream))
                                        && event.event_type == event_type
                                        && *code == HttpErrorCode::CORO_CANCELLED
                                        && *thread == owner
                                })
                                .count(),
                            1,
                        );
                    }
                }
                assert_eq!(
                    log.callback_states.borrow().as_slice(),
                    &[WorkerState::Closing; 4],
                );
            });
        }

        #[test]
        fn foreign_context_and_closed_observer_errors_are_exact() {
            std::thread_local! {
                static FOREIGN_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
            }
            fn assert_error<T: std::fmt::Debug>(result: Result<T, HttpError>, message: &[u8]) {
                let error = result.unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                assert_eq!(error.message(), message);
            }
            const FOREIGN: &[u8] = b"byte-event observer requires its exact worker context";
            const CLOSED: &[u8] = b"byte-event observer is closed";

            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    time::pause();
                    let (worker, driver) = WorkerDriver::new(32).unwrap();
                    let (foreign_worker, foreign_driver) = WorkerDriver::new(32).unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    let foreign_task = tokio::task::spawn_local(foreign_driver);
                    foreign_worker
                        .call_local(|foreign| async move {
                            FOREIGN_CONTEXT.with(|slot| *slot.borrow_mut() = Some(foreign));
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    worker
                        .call_local(|context| async move {
                            let foreign = FOREIGN_CONTEXT
                                .with(|slot| slot.borrow_mut().take())
                                .expect("foreign worker published its context");
                            assert!(foreign.is_owner_thread());
                            assert!(!foreign.handle().same_worker(context.handle()));
                            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();

                            assert_error(observer.transport_write(&foreign, 10), FOREIGN);
                            assert_error(observer.pending_events(&foreign), FOREIGN);
                            assert_error(
                                observer.socket_byte_event(
                                    &foreign,
                                    SocketByteEventType::Write,
                                    10,
                                ),
                                FOREIGN,
                            );
                            let error = HttpError::new(HttpErrorCode::CANCEL);
                            assert_error(observer.cancel_events(&foreign, &error), FOREIGN);
                            assert_error(
                                observer.cancel_stream_events(&foreign, StreamId::new(1), &error),
                                FOREIGN,
                            );
                            assert_eq!(observer.transport_write(&context, 10).unwrap(), 0);

                            // Stay in this poll so driver shutdown cannot close the observer first.
                            assert!(context.handle().begin_shutdown());
                            assert_error(observer.transport_write(&context, 20), CLOSED);
                            assert_error(observer.pending_events(&context), CLOSED);
                            assert_error(observer.transport_write(&foreign, 20), FOREIGN);
                            observer.cancel_events(&context, &error).unwrap();
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    worker.begin_shutdown();
                    driver_task.await.unwrap();
                    foreign_worker.begin_shutdown();
                    foreign_task.await.unwrap();
                }));
        }

        #[test]
        fn stream_cancellation_preserves_sibling_events_across_observer_states() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let reset_log = Rc::new(EventLog::default());
                let sibling_log = Rc::new(EventLog::default());
                let event_bits = ByteEventType::TransportWrite.bit()
                    | ByteEventType::KernelWrite.bit()
                    | ByteEventType::NicTx.bit()
                    | ByteEventType::CumulativeAck.bit();
                for (stream, offset, log) in [(1, 10, &reset_log), (3, 20, &sibling_log)] {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(stream, offset, 0, offset, 0),
                            None,
                            vec![registration(None, event_bits, log)],
                            false,
                        )
                        .unwrap();
                }

                let reset_boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                let error = HttpError::new(HttpErrorCode::INTERNAL_ERROR);
                assert_eq!(
                    observer
                        .cancel_stream_events(&context, StreamId::new(1), &error)
                        .unwrap(),
                    4,
                );
                assert_eq!(reset_log.cancelled.borrow().len(), 4);
                assert!(
                    reset_log
                        .cancelled
                        .borrow()
                        .iter()
                        .all(|(event, code, _)| event.stream_id == Some(StreamId::new(1))
                            && *code == HttpErrorCode::INTERNAL_ERROR)
                );
                assert!(sibling_log.cancelled.borrow().is_empty());
                assert_eq!(reset_boundary.write_flags(), ByteEventWriteFlags::default());

                assert_eq!(observer.transport_write(&context, 20).unwrap(), 1);
                let sibling_boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 20)
                    .unwrap();
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 20, Some(sibling_boundary))
                        .unwrap(),
                    1,
                );
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, 20)
                        .unwrap(),
                    1,
                );
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Ack, 20)
                        .unwrap(),
                    1,
                );
                assert_eq!(sibling_log.fired.borrow().len(), 4);
                assert!(
                    sibling_log
                        .fired
                        .borrow()
                        .iter()
                        .all(|(event, _)| event.stream_id == Some(StreamId::new(3)))
                );
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
            });
        }

        #[test]
        fn in_flight_socket_cancellation_is_panic_isolated() {
            struct PanicOnCancel(Cell<usize>);

            impl HttpByteEventCallback for PanicOnCancel {
                fn on_byte_event(&self, _event: HttpByteEvent) {}

                fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
                    self.0.set(self.0.get() + 1);
                    panic!("expected byte-event cancellation panic");
                }
            }

            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let panic_log = Rc::new(PanicOnCancel(Cell::new(0)));
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![
                            HttpByteEventRegistration::new(
                                None,
                                ByteEventType::NicTx.bit(),
                                &panic_log,
                            ),
                            registration(
                                None,
                                ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                                &log,
                            ),
                        ],
                        false,
                    )
                    .unwrap();
                let boundary = observer.next_tx_ack_event(&context).unwrap().unwrap();
                let error = HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        let _ = observer.cancel_socket_byte_event(
                            &context,
                            SocketByteEventType::Tx,
                            10,
                            &error,
                        );
                    }))
                    .is_err()
                );
                assert_eq!(panic_log.0.get(), 1);
                assert_eq!(log.cancelled.borrow().len(), 1);
                assert_eq!(log.cancelled.borrow()[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(
                    boundary.write_flags(),
                    ByteEventWriteFlags {
                        timestamp_write: true,
                        timestamp_tx: false,
                        timestamp_ack: true,
                    },
                );
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 100)
                    .unwrap();
                observer
                    .transport_write_complete(&context, 10, Some(boundary))
                    .unwrap();
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Ack, 100)
                        .unwrap(),
                    1,
                );
                assert_eq!(log.fired.borrow().len(), 1);
                assert_eq!(
                    log.fired.borrow()[0].0.event_type,
                    ByteEventType::CumulativeAck,
                );
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
            });
        }
    }

    mod h1 {
        use super::*;

        #[test]
        fn httpdownstream_session_test_simple_byte_events() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let log = Rc::new(EventLog::default());
                let header_bits = ByteEventType::TransportWrite.bit()
                    | ByteEventType::KernelWrite.bit()
                    | ByteEventType::NicTx.bit()
                    | ByteEventType::CumulativeAck.bit();
                let header_size = HttpHeaderSize {
                    compressed: 20,
                    uncompressed: 40,
                    compressed_block: 16,
                };
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 20, 0, 20, 0),
                        Some(FieldSectionInfo {
                            section_type: FieldSectionType::Headers,
                            final_headers: true,
                            size: header_size,
                        }),
                        vec![registration(None, header_bits, &log)],
                        false,
                    )
                    .unwrap();
                assert_eq!(observer.transport_write(&context, 20).unwrap(), 1);
                let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                assert_eq!(
                    tx_ack.write_flags(),
                    ByteEventWriteFlags {
                        timestamp_write: true,
                        timestamp_tx: true,
                        timestamp_ack: true,
                    },
                );
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 24)
                    .unwrap();
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 20, Some(tx_ack))
                        .unwrap(),
                    1,
                );
                observer
                    .socket_byte_event(&context, SocketByteEventType::Tx, 24)
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Ack, 24)
                    .unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 124, 20, 124, 100),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                            &log,
                        )],
                        true,
                    )
                    .unwrap();
                observer.transport_write(&context, 124).unwrap();
                observer
                    .transport_write_complete(&context, 124, None)
                    .unwrap();

                let fired = log.fired.borrow();
                assert_eq!(fired.len(), 6);
                assert_eq!(
                    fired
                        .iter()
                        .map(|(event, _)| event.event_type)
                        .collect::<Vec<_>>(),
                    vec![
                        ByteEventType::TransportWrite,
                        ByteEventType::KernelWrite,
                        ByteEventType::NicTx,
                        ByteEventType::CumulativeAck,
                        ByteEventType::TransportWrite,
                        ByteEventType::KernelWrite,
                    ],
                );
                assert!(fired.iter().all(|(_, thread)| *thread == owner));
                assert_eq!(fired[0].0.field_section_info.unwrap().size, header_size);
                assert_eq!(fired[0].0.stream_offset, 20);
                assert_eq!(fired[0].0.transport_offset, 20);
                assert_eq!(fired[5].0.body_offset, 100);
                assert!(fired[5].0.eom);
            });
        }

        #[test]
        fn httpdownstream_session_test_trailer_byte_events() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 120, 0, 120, 100),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit(),
                            &log,
                        )],
                        false,
                    )
                    .unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 140, 0, 140, 100),
                        Some(FieldSectionInfo {
                            section_type: FieldSectionType::Trailers,
                            final_headers: true,
                            size: HttpHeaderSize {
                                compressed: 20,
                                uncompressed: 30,
                                compressed_block: 16,
                            },
                        }),
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit(),
                            &log,
                        )],
                        true,
                    )
                    .unwrap();
                assert_eq!(observer.transport_write(&context, 140).unwrap(), 2);
                let fired = log.fired.borrow();
                assert_eq!(fired[0].0.body_offset, 100);
                assert!(!fired[0].0.eom);
                assert_eq!(fired[1].0.body_offset, 100);
                assert!(fired[1].0.eom);
                assert_eq!(
                    fired[1].0.field_section_info.unwrap().section_type,
                    FieldSectionType::Trailers,
                );
            });
        }

        #[test]
        fn httpdownstream_session_test_byte_event_errors() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let fired = Rc::new(Cell::new(0));
                struct CountCallback(Rc<Cell<usize>>);
                impl HttpByteEventCallback for CountCallback {
                    fn on_byte_event(&self, _event: HttpByteEvent) {
                        self.0.set(self.0.get() + 1);
                    }
                    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
                        self.0.set(self.0.get() + 1);
                    }
                }
                let callback = Rc::new(CountCallback(Rc::clone(&fired)));
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![HttpByteEventRegistration::new(
                            None,
                            ByteEventType::TransportWrite.bit(),
                            &callback,
                        )],
                        false,
                    )
                    .unwrap();
                drop(callback);
                assert_eq!(observer.transport_write(&context, 10).unwrap(), 1);
                assert_eq!(fired.get(), 0);
            });
        }

        #[test]
        fn mixed_transport_write_survives_disabled_and_capped_tx_rejection() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let disabled_log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit() | ByteEventType::NicTx.bit(),
                            &disabled_log,
                        )],
                        false,
                    )
                    .unwrap();
                assert_eq!(disabled_log.cancelled.borrow().len(), 1);
                assert_eq!(
                    disabled_log.cancelled.borrow()[0].0.event_type,
                    ByteEventType::NicTx,
                );
                assert_eq!(observer.transport_write(&context, 10).unwrap(), 1);
                assert_eq!(disabled_log.fired.borrow().len(), 1);
                assert_eq!(
                    disabled_log.fired.borrow()[0].0.event_type,
                    ByteEventType::TransportWrite,
                );

                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let filler_log = Rc::new(EventLog::default());
                for offset in 20..116 {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, offset, 0, offset, offset),
                            None,
                            vec![registration(None, ByteEventType::NicTx.bit(), &filler_log)],
                            false,
                        )
                        .unwrap();
                }
                let capped_log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 200, 0, 200, 0),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit() | ByteEventType::NicTx.bit(),
                            &capped_log,
                        )],
                        false,
                    )
                    .unwrap();
                assert_eq!(capped_log.cancelled.borrow().len(), 1);
                assert_eq!(
                    capped_log.cancelled.borrow()[0].0.event_type,
                    ByteEventType::NicTx,
                );
                assert_eq!(observer.transport_write(&context, 200).unwrap(), 1);
                assert_eq!(capped_log.fired.borrow().len(), 1);
                assert_eq!(
                    capped_log.fired.borrow()[0].0.event_type,
                    ByteEventType::TransportWrite,
                );
            });
        }

        #[test]
        fn httpdownstream_session_test_byte_events_cancel_on_write_error() {
            run_worker(|context| async move {
                let owner = std::thread::current().id();
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 20, 0, 20, 0),
                        None,
                        vec![registration(None, ByteEventType::KernelWrite.bit(), &log)],
                        false,
                    )
                    .unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 120, 0, 120, 100),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                            &log,
                        )],
                        true,
                    )
                    .unwrap();
                assert_eq!(observer.transport_write(&context, 120).unwrap(), 1);
                observer
                    .cancel_events(
                        &context,
                        &HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                    )
                    .unwrap();
                assert_eq!(
                    log.fired.borrow()[0].0.event_type,
                    ByteEventType::TransportWrite
                );
                let cancelled = log.cancelled.borrow();
                assert_eq!(cancelled.len(), 2);
                assert!(cancelled.iter().all(|(event, code, thread)| {
                    event.event_type == ByteEventType::KernelWrite
                        && *code == HttpErrorCode::TRANSPORT_WRITE_ERROR
                        && *thread == owner
                }));
            });
        }

        #[test]
        fn h1_downstream_session_test_zero_byte_eomack_event_immediate() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 101)
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Ack, 101)
                    .unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 101, 101, 120, 1),
                        None,
                        vec![registration(None, ByteEventType::CumulativeAck.bit(), &log)],
                        true,
                    )
                    .unwrap();
                assert_eq!(log.fired.borrow().len(), 1);
                assert!(log.fired.borrow()[0].0.eom);
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
            });
        }

        #[test]
        fn h1_downstream_session_test_zero_byte_eomack_event_delayed() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 101)
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Ack, 100)
                    .unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 101, 101, 120, 1),
                        None,
                        vec![registration(None, ByteEventType::CumulativeAck.bit(), &log)],
                        true,
                    )
                    .unwrap();
                assert!(log.fired.borrow().is_empty());
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);
                let mut zero_refs = Box::pin(observer.zero_refs(&context).unwrap());
                assert!(poll_once(zero_refs.as_mut(), Waker::noop()).is_pending());
                observer
                    .socket_byte_event(&context, SocketByteEventType::Ack, 101)
                    .unwrap();
                assert_eq!(log.fired.borrow().len(), 1);
                assert!(log.fired.borrow()[0].0.eom);
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                assert!(poll_once(zero_refs.as_mut(), Waker::noop()).is_ready());
            });
        }

        #[test]
        fn staggered_tx_ack_completion_and_timeout_are_independent() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .set_byte_event_timeout(&context, Duration::from_millis(10))
                    .unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 10, 0, 10, 0),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                            &log,
                        )],
                        false,
                    )
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 10)
                    .unwrap();
                let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                observer
                    .transport_write_complete(&context, 10, Some(tx_ack))
                    .unwrap();
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 2);
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, 10)
                        .unwrap(),
                    1,
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 1);

                let signal = Arc::new(WakeSignal::default());
                let waker = Waker::from(Arc::clone(&signal));
                let mut task_context = Context::from_waker(&waker);
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                time::advance(Duration::from_millis(10)).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(observer.pending_timeout_refs(&context).unwrap(), 0);
                assert_eq!(log.fired.borrow().len(), 1);
                assert_eq!(log.fired.borrow()[0].0.event_type, ByteEventType::NicTx);
                assert_eq!(log.cancelled.borrow().len(), 1);
                assert_eq!(
                    log.cancelled.borrow()[0].0.event_type,
                    ByteEventType::CumulativeAck,
                );
                assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::READ_TIMEOUT);

                let inverse_log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 20, 10, 20, 0),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                            &inverse_log,
                        )],
                        false,
                    )
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 20)
                    .unwrap();
                let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                observer
                    .transport_write_complete(&context, 20, Some(tx_ack))
                    .unwrap();
                assert_eq!(
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Ack, 20)
                        .unwrap(),
                    1,
                );
                assert_eq!(inverse_log.fired.borrow().len(), 1);
                assert_eq!(
                    inverse_log.fired.borrow()[0].0.event_type,
                    ByteEventType::CumulativeAck,
                );
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    0
                );
                time::advance(Duration::from_millis(10)).await;
                signal.wait().await;
                assert_eq!(
                    observer.poll_timeouts(&context, &mut task_context).unwrap(),
                    1
                );
                assert_eq!(inverse_log.cancelled.borrow().len(), 1);
                assert_eq!(
                    inverse_log.cancelled.borrow()[0].0.event_type,
                    ByteEventType::NicTx,
                );
                assert_eq!(
                    inverse_log.cancelled.borrow()[0].1,
                    HttpErrorCode::READ_TIMEOUT,
                );
            });
        }

        #[test]
        fn h12_downstream_session_test_byte_event_timeout() {
            super::byte_event::timeout_then_late_transport_callback_is_ignored();
        }

        #[test]
        fn h12_downstream_session_test_too_many_byte_events() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let log = Rc::new(EventLog::default());
                for offset in 1..=100 {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, offset, 0, offset, offset),
                            None,
                            vec![registration(
                                None,
                                ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                                &log,
                            )],
                            false,
                        )
                        .unwrap();
                }
                assert_eq!(log.cancelled.borrow().len(), 104);
                while let Some(tx_ack) = observer.next_tx_ack_event(&context).unwrap() {
                    let offset = tx_ack.session_byte_offset();
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Write, offset)
                        .unwrap();
                    observer
                        .transport_write_complete(&context, offset, Some(tx_ack))
                        .unwrap();
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Tx, offset)
                        .unwrap();
                    observer
                        .socket_byte_event(&context, SocketByteEventType::Ack, offset)
                        .unwrap();
                }
                assert_eq!(log.fired.borrow().len(), 96);
                assert_eq!(log.cancelled.borrow().len(), 104);
            });
        }

        #[test]
        fn in_flight_tx_ack_events_remain_subject_to_the_admission_limit() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let log = Rc::new(EventLog::default());
                let bits = ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit();
                for offset in 1..=48 {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, offset, 0, offset, offset),
                            None,
                            vec![registration(None, bits, &log)],
                            false,
                        )
                        .unwrap();
                }

                let mut in_flight = Vec::new();
                while let Some(tx_ack) = observer.next_tx_ack_event(&context).unwrap() {
                    in_flight.push(tx_ack);
                }
                assert_eq!(in_flight.len(), 48);
                assert_eq!(observer.pending_events(&context).unwrap(), 96);

                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 49, 0, 49, 49),
                        None,
                        vec![registration(None, bits, &log)],
                        false,
                    )
                    .unwrap();
                assert_eq!(observer.pending_events(&context).unwrap(), 96);
                assert_eq!(log.cancelled.borrow().len(), 2);
                assert!(
                    log.cancelled
                        .borrow()
                        .iter()
                        .all(|(_, error, _)| *error == HttpErrorCode::CANCEL)
                );

                drop(in_flight);
            });
        }

        #[test]
        fn h12_downstream_session_test_cancel_pending_byte_events() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                let log = Rc::new(EventLog::default());
                let bits = ByteEventType::KernelWrite.bit()
                    | ByteEventType::NicTx.bit()
                    | ByteEventType::CumulativeAck.bit();
                for offset in [50, 100] {
                    observer
                        .register_byte_events(
                            &context,
                            offsets(1, offset, 0, offset, offset),
                            None,
                            vec![registration(None, bits, &log)],
                            false,
                        )
                        .unwrap();
                }
                observer
                    .cancel_events(
                        &context,
                        &HttpError::new(HttpErrorCode::TRANSPORT_WRITE_ERROR),
                    )
                    .unwrap();
                assert_eq!(log.cancelled.borrow().len(), 6);
                assert!(
                    log.cancelled
                        .borrow()
                        .iter()
                        .all(|(_, code, _)| *code == HttpErrorCode::TRANSPORT_WRITE_ERROR)
                );
                assert_eq!(observer.pending_events(&context).unwrap(), 0);
            });
        }

        #[test]
        fn h12_downstream_session_test_byte_event_observer_outlives_transport() {
            run_worker(|context| async move {
                struct ObserverOutlivesTransport {
                    _transport: TransportDropProbe,
                    _observer: AsyncSocketByteEventObserver,
                }

                let destroyed = Rc::new(Cell::new(false));
                let log = Rc::new(EventLog {
                    transport_destroyed: Some(Rc::clone(&destroyed)),
                    ..EventLog::default()
                });
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 1, 0, 0, 0),
                        None,
                        vec![registration(None, ByteEventType::NicTx.bit(), &log)],
                        false,
                    )
                    .unwrap();
                let harness = ObserverOutlivesTransport {
                    _transport: TransportDropProbe(Rc::clone(&destroyed)),
                    _observer: observer,
                };
                drop(harness);
                assert_eq!(
                    log.transport_destroyed_at_cancel.borrow().as_slice(),
                    &[true],
                );
                assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::CANCEL);
            });
        }

        #[test]
        fn escaped_observer_cancels_on_owner_while_worker_is_closing() {
            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    let owner = std::thread::current().id();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let callback_worker = worker.clone();
                    worker
                        .spawn_local(move |context| async move {
                            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                            let log = Rc::new(EventLog {
                                worker: Some(callback_worker),
                                ..EventLog::default()
                            });
                            observer
                                .register_byte_events(
                                    &context,
                                    offsets(1, 10, 0, 10, 0),
                                    None,
                                    vec![registration(
                                        None,
                                        ByteEventType::TransportWrite.bit(),
                                        &log,
                                    )],
                                    false,
                                )
                                .unwrap();
                            ESCAPED_OBSERVER.with(|escaped| {
                                assert!(escaped.replace(Some((observer, log))).is_none());
                            });
                            started_tx.send(()).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    started_rx.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    let log = ESCAPED_OBSERVER.with(|escaped| {
                        let escaped = escaped.borrow();
                        let (_, log) = escaped.as_ref().unwrap();
                        assert_eq!(log.cancelled.borrow().len(), 1);
                        assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::DROPPED);
                        assert_eq!(log.cancelled.borrow()[0].2, owner);
                        assert_eq!(
                            log.callback_states.borrow().as_slice(),
                            &[WorkerState::Closing],
                        );
                        Rc::clone(log)
                    });
                    ESCAPED_OBSERVER.with(|escaped| drop(escaped.replace(None)));
                    assert_eq!(log.cancelled.borrow().len(), 1);
                }));
        }

        #[test]
        fn escaped_tx_ack_token_is_cancelled_before_worker_closes() {
            Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("current-thread runtime")
                .block_on(LocalSet::new().run_until(async move {
                    let owner = std::thread::current().id();
                    let (worker, driver) = WorkerDriver::new(4).unwrap();
                    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                    let callback_worker = worker.clone();
                    worker
                        .spawn_local(move |context| async move {
                            let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                            observer.set_tx_ack_events_enabled(&context, true).unwrap();
                            let log = Rc::new(EventLog {
                                worker: Some(callback_worker),
                                ..EventLog::default()
                            });
                            observer
                                .register_byte_events(
                                    &context,
                                    offsets(1, 10, 0, 10, 0),
                                    None,
                                    vec![registration(None, ByteEventType::NicTx.bit(), &log)],
                                    false,
                                )
                                .unwrap();
                            let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                            ESCAPED_TX_ACK.with(|escaped| {
                                assert!(escaped.replace(Some((observer, tx_ack, log))).is_none());
                            });
                            started_tx.send(()).unwrap();
                        })
                        .unwrap();
                    let driver_task = tokio::task::spawn_local(driver);
                    started_rx.await.unwrap();
                    assert!(worker.begin_shutdown());
                    driver_task.await.unwrap();
                    assert_eq!(worker.state(), WorkerState::Closed);
                    let log = ESCAPED_TX_ACK.with(|escaped| {
                        let escaped = escaped.borrow();
                        let (_, _, log) = escaped.as_ref().unwrap();
                        assert_eq!(log.cancelled.borrow().len(), 1);
                        assert_eq!(log.cancelled.borrow()[0].0.event_type, ByteEventType::NicTx);
                        assert_eq!(log.cancelled.borrow()[0].1, HttpErrorCode::DROPPED);
                        assert_eq!(log.cancelled.borrow()[0].2, owner);
                        assert_eq!(
                            log.callback_states.borrow().as_slice(),
                            &[WorkerState::Closing],
                        );
                        Rc::clone(log)
                    });
                    ESCAPED_TX_ACK.with(|escaped| drop(escaped.replace(None)));
                    assert_eq!(log.cancelled.borrow().len(), 1);
                }));
        }

        #[test]
        fn h12_downstream_session_test_tx_ack_before_schedule_byte_events() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                observer.set_tx_ack_events_enabled(&context, true).unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Write, 100)
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Tx, 100)
                    .unwrap();
                observer
                    .socket_byte_event(&context, SocketByteEventType::Ack, 100)
                    .unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(1, 20, 0, 20, 0),
                        None,
                        vec![registration(
                            None,
                            ByteEventType::NicTx.bit() | ByteEventType::CumulativeAck.bit(),
                            &log,
                        )],
                        false,
                    )
                    .unwrap();
                let tx_ack = observer.next_tx_ack_event(&context).unwrap().unwrap();
                assert_eq!(
                    observer
                        .transport_write_complete(&context, 20, Some(tx_ack))
                        .unwrap(),
                    2,
                );
                assert_eq!(
                    log.fired
                        .borrow()
                        .iter()
                        .map(|(event, _)| event.event_type)
                        .collect::<Vec<_>>(),
                    vec![ByteEventType::NicTx, ByteEventType::CumulativeAck],
                );
            });
        }
    }

    mod h2 {
        use super::*;

        #[test]
        fn session_offsets_remain_distinct_from_transport_offsets() {
            run_worker(|context| async move {
                let observer = AsyncSocketByteEventObserver::new(&context).unwrap();
                let log = Rc::new(EventLog::default());
                observer
                    .register_byte_events(
                        &context,
                        offsets(7, 900, 800, 63, 17),
                        None,
                        vec![registration(
                            Some(11),
                            ByteEventType::TransportWrite.bit(),
                            &log,
                        )],
                        true,
                    )
                    .unwrap();
                observer.transport_write(&context, 900).unwrap();
                let fired = log.fired.borrow();
                let event = &fired[0].0;
                assert_eq!(event.stream_id, Some(StreamId::new(11)));
                assert_eq!(event.transport_offset, 900);
                assert_eq!(event.stream_offset, 63);
                assert_eq!(event.body_offset, 17);
                assert!(event.eom);
            });
        }
    }
}
