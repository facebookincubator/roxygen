/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod observability {
    use std::future::Future;
    use std::future::pending;
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::thread::ThreadId;
    use std::time::Duration;

    use futures::task::ArcWake;
    use futures::task::noop_waker;
    use futures::task::waker;
    use thiserror::Error;
    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    use crate::context::ContextEnvelope;
    use crate::context::RuntimeContext;
    use crate::context::RuntimeContextGuard;
    use crate::observability::MetricDescriptor;
    use crate::observability::MetricError;
    use crate::observability::MetricLabel;
    use crate::observability::MetricLabelSpec;
    use crate::observability::MetricRegistry;
    use crate::observability::MetricUnit;
    use crate::observability::OperationKind;
    use crate::observability::OperationSpan;
    use crate::observability::SpanObserver;
    use crate::observability::TerminalOutcome;
    use crate::source_proxy::LocalBodyRead;
    use crate::source_proxy::LocalHttpSource;
    use crate::source_proxy::LocalSourceFuture;
    use crate::source_proxy::RemoteHttpSource;
    use crate::source_proxy::RemoteSourceEvent;
    use crate::task_group::TaskGroup;
    use crate::tokio_host::CallerDrivenGuard;
    use crate::tokio_host::ManagedWorker;

    macro_rules! assert_not_send {
        ($type:ty) => {
            const _: fn() = || {
                trait AmbiguousIfSend<Marker> {
                    fn assert_not_send() {}
                }
                impl<T: ?Sized> AmbiguousIfSend<()> for T {}
                struct IsSend;
                impl<T: ?Sized + Send> AmbiguousIfSend<IsSend> for T {}
                let _ = <$type as AmbiguousIfSend<_>>::assert_not_send;
            };
        };
    }

    assert_not_send!(RuntimeContextGuard);

    fn runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    struct ContextWake {
        notified: Mutex<Option<std::sync::mpsc::Sender<Option<RuntimeContext>>>>,
    }

    impl ArcWake for ContextWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            if let Some(notified) = arc_self
                .notified
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
            {
                notified
                    .send(RuntimeContext::current())
                    .expect("report response wake context");
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
    #[error("bridge error")]
    struct BridgeError;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct BridgeEvent;

    impl RemoteSourceEvent for BridgeEvent {
        fn is_terminal(&self) -> bool {
            false
        }
    }

    type ContextAction = (&'static str, Option<RuntimeContext>, ThreadId);

    fn record_context(actions: &Arc<Mutex<Vec<ContextAction>>>, name: &'static str) {
        actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((name, RuntimeContext::current(), std::thread::current().id()));
    }

    struct BridgeReadFuture {
        gate: oneshot::Receiver<()>,
        actions: Arc<Mutex<Vec<ContextAction>>>,
    }

    impl Future for BridgeReadFuture {
        type Output = Result<BridgeEvent, BridgeError>;

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Self::Output> {
            match std::pin::Pin::new(&mut self.gate).poll(context) {
                Poll::Ready(_) => {
                    record_context(&self.actions, "read_ready");
                    Poll::Ready(Ok(BridgeEvent))
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Drop for BridgeReadFuture {
        fn drop(&mut self) {
            record_context(&self.actions, "read_drop");
        }
    }

    struct ContextSource {
        gate: Option<oneshot::Receiver<()>>,
        started: Option<std::sync::mpsc::Sender<()>>,
        control_applied: Option<std::sync::mpsc::Sender<()>>,
        dropped: Option<std::sync::mpsc::Sender<()>>,
        actions: Arc<Mutex<Vec<ContextAction>>>,
    }

    impl LocalHttpSource for ContextSource {
        type HeaderEvent = BridgeEvent;
        type BodyEvent = BridgeEvent;
        type Error = BridgeError;

        fn read_header_event(&mut self) -> LocalSourceFuture<Result<BridgeEvent, BridgeError>> {
            record_context(&self.actions, "read_start");
            self.started
                .take()
                .expect("read starts once")
                .send(())
                .expect("announce read start");
            Box::pin(BridgeReadFuture {
                gate: self.gate.take().expect("read gate exists"),
                actions: Arc::clone(&self.actions),
            })
        }

        fn read_body_event(
            &mut self,
            _maximum: u32,
        ) -> LocalSourceFuture<Result<LocalBodyRead<BridgeEvent>, BridgeError>> {
            Box::pin(pending())
        }

        fn stop_reading(&mut self, _error: Option<BridgeError>) {
            record_context(&self.actions, "stop");
        }

        fn set_read_timeout(&mut self, _timeout: Duration) {
            record_context(&self.actions, "control");
            self.control_applied
                .take()
                .expect("control applies once")
                .send(())
                .expect("announce control application");
        }
    }

    impl Drop for ContextSource {
        fn drop(&mut self) {
            record_context(&self.actions, "source_drop");
            if let Some(dropped) = self.dropped.take() {
                dropped.send(()).expect("announce source drop");
            }
        }
    }

    fn source_bridge_context_round_trip(observer: &SpanObserver) {
        let worker = ManagedWorker::new(8).expect("source worker starts");
        let owner = worker.owner_thread();
        let actions = Arc::new(Mutex::new(Vec::new()));
        let (gate_tx, gate_rx) = oneshot::channel();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (control_tx, control_rx) = std::sync::mpsc::channel();
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let source_actions = Arc::clone(&actions);
        let mut remote = RemoteHttpSource::new(&worker.handle(), 4, move |_| ContextSource {
            gate: Some(gate_rx),
            started: Some(started_tx),
            control_applied: Some(control_tx),
            dropped: Some(dropped_tx),
            actions: source_actions,
        })
        .expect("source bridge starts");
        let control = remote.control_handle();
        let abort = remote.abort_handle();
        let read_context = RuntimeContext::new();
        let control_context = RuntimeContext::new();
        let stop_context = RuntimeContext::new();
        let read_envelope = {
            let _read = read_context.enter();
            ContextEnvelope::capture().with_span_observer(observer.clone())
        };
        let mut read = Box::pin(read_envelope.wrap(remote.read_header_event()));
        let waker = noop_waker();
        let mut task = Context::from_waker(&waker);
        assert!(read.as_mut().poll(&mut task).is_pending());
        started_rx.recv().expect("owner starts read");

        {
            let _control = control_context.enter();
            ContextEnvelope::capture()
                .with_span_observer(observer.clone())
                .run(|| {
                    control
                        .set_read_timeout(Duration::from_secs(3))
                        .expect("control command admitted");
                });
        }
        control_rx.recv().expect("owner applies control");
        {
            let _stop = stop_context.enter();
            ContextEnvelope::capture()
                .with_span_observer(observer.clone())
                .run(|| assert!(abort.stop_reading(None)));
        }
        assert!(runtime().block_on(read).is_err());
        dropped_rx.recv().expect("source destroyed on owner");
        drop(gate_tx);
        drop(remote);
        worker.shutdown().expect("source worker joins");

        let actions = actions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (name, expected) in [
            ("read_start", &read_context),
            ("read_drop", &read_context),
            ("control", &control_context),
            ("stop", &stop_context),
            ("source_drop", &stop_context),
        ] {
            assert!(actions.iter().any(|(actual, context, thread)| {
                *actual == name && context.as_ref() == Some(expected) && *thread == owner
            }));
        }
        let spans = observer.snapshots();
        assert_eq!(observer.active_count(), 0);
        for (kind, outcome) in [
            (OperationKind::Stream, TerminalOutcome::Cancelled),
            (OperationKind::Command, TerminalOutcome::Success),
            (OperationKind::Cancellation, TerminalOutcome::Success),
        ] {
            assert!(
                spans
                    .iter()
                    .any(|span| span.kind == kind && span.outcome == Some(outcome))
            );
        }
    }

    #[test]
    fn context_crosses_command_and_callback() {
        let managed = ManagedWorker::new(4).expect("managed worker starts");
        let managed_owner = managed.owner_thread();
        let origin = RuntimeContext::new();
        let ambient = RuntimeContext::new();
        let (release_tx, release_rx) = oneshot::channel();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut call = {
            let _origin = origin.enter();
            Box::pin(
                managed
                    .handle()
                    .call_local(move |worker| async move {
                        started_tx.send(()).expect("announce managed callback");
                        release_rx.await.expect("release managed callback");
                        (
                            RuntimeContext::current(),
                            std::thread::current().id(),
                            worker.worker_id(),
                        )
                    })
                    .expect("contextual call admitted"),
            )
        };

        let _ambient = ambient.enter();
        let (wake_tx, wake_rx) = std::sync::mpsc::channel();
        let response_waker = waker(Arc::new(ContextWake {
            notified: Mutex::new(Some(wake_tx)),
        }));
        let mut response_task = Context::from_waker(&response_waker);
        assert!(call.as_mut().poll(&mut response_task).is_pending());
        assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
        started_rx.recv().expect("managed callback starts");
        release_tx.send(()).expect("release managed callback");
        assert_eq!(
            wake_rx.recv().expect("response callback wakes"),
            Some(origin.clone())
        );
        let owner_observation = runtime().block_on(call).expect("managed call completes");
        assert_eq!(owner_observation.0, Some(origin.clone()));
        assert_eq!(owner_observation.1, managed_owner);
        assert_eq!(owner_observation.2, managed.handle().id());
        assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
        managed.shutdown().expect("managed worker joins");

        runtime().block_on(async {
            let mut caller = CallerDrivenGuard::new(4).expect("caller-driven worker starts");
            let owner = std::thread::current().id();
            let (call, response_context) = {
                let _origin = origin.enter();
                let response_context = ContextEnvelope::capture();
                let call = caller
                    .handle()
                    .call(|worker| {
                        (
                            RuntimeContext::current(),
                            std::thread::current().id(),
                            worker.worker_id(),
                        )
                    })
                    .expect("caller-driven contextual call admitted");
                (call, response_context)
            };
            let result = caller
                .run_until(response_context.wrap(async move {
                    let result = call.await.expect("caller-driven call completes");
                    (result, RuntimeContext::current())
                }))
                .await
                .expect("caller-driven poll stays on owner");
            assert_eq!(result.0.0, Some(origin.clone()));
            assert_eq!(result.0.1, owner);
            assert_eq!(result.1, Some(origin.clone()));
            assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
            caller.shutdown().await.expect("caller-driven worker joins");
        });

        source_bridge_context_round_trip(&SpanObserver::default());
    }

    struct PendingDropProbe {
        observed: Arc<Mutex<Option<Option<RuntimeContext>>>>,
    }

    impl Future for PendingDropProbe {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingDropProbe {
        fn drop(&mut self) {
            *self
                .observed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(RuntimeContext::current());
        }
    }

    struct ContextMutationFuture {
        polls: u8,
        original: RuntimeContext,
        changed: RuntimeContext,
    }

    impl Future for ContextMutationFuture {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Self::Output> {
            match self.polls {
                0 => {
                    assert_eq!(RuntimeContext::current(), Some(self.original.clone()));
                    RuntimeContext::set_current(Some(self.changed.clone()));
                    self.polls = 1;
                    Poll::Pending
                }
                1 => {
                    assert_eq!(RuntimeContext::current(), Some(self.changed.clone()));
                    self.polls = 2;
                    Poll::Ready(())
                }
                _ => panic!("context mutation future polled after completion"),
            }
        }
    }

    struct ContextDropMutationFuture {
        actions: Arc<Mutex<Vec<ContextAction>>>,
        poll_context: RuntimeContext,
        drop_context: RuntimeContext,
        panic_on_poll: bool,
        panic_on_drop: bool,
    }

    impl Future for ContextDropMutationFuture {
        type Output = u8;

        fn poll(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            record_context(&self.actions, "poll");
            RuntimeContext::set_current(Some(self.poll_context.clone()));
            assert!(!self.panic_on_poll, "context poll panic");
            Poll::Ready(7)
        }
    }

    impl Drop for ContextDropMutationFuture {
        fn drop(&mut self) {
            record_context(&self.actions, "drop");
            RuntimeContext::set_current(Some(self.drop_context.clone()));
            assert!(!self.panic_on_drop, "context drop panic");
        }
    }

    #[test]
    fn context_ready_destruction_and_panics_preserve_capture_order() {
        for (panic_on_poll, panic_on_drop) in [(false, false), (true, false), (false, true)] {
            let original = RuntimeContext::new();
            let polled = RuntimeContext::new();
            let destroyed = RuntimeContext::new();
            let ambient = RuntimeContext::new();
            let owner = std::thread::current().id();
            let envelope = {
                let _original = original.enter();
                ContextEnvelope::capture()
            };
            let actions = Arc::new(Mutex::new(Vec::new()));
            let mut future = Box::pin(envelope.wrap(ContextDropMutationFuture {
                actions: Arc::clone(&actions),
                poll_context: polled.clone(),
                drop_context: destroyed.clone(),
                panic_on_poll,
                panic_on_drop,
            }));
            let _ambient = ambient.enter();
            let waker = noop_waker();
            let mut task = Context::from_waker(&waker);

            let result =
                std::panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut task)));
            assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
            if panic_on_poll || panic_on_drop {
                let panic = result.expect_err("context mutation panics");
                let message = if panic_on_poll {
                    "context poll panic"
                } else {
                    "context drop panic"
                };
                assert_eq!(panic.downcast_ref::<&str>(), Some(&message));
                assert_eq!(future.envelope().request_context(), Some(original.clone()));
            } else {
                assert_eq!(result.expect("context mutation completes"), Poll::Ready(7));
                assert_eq!(future.envelope().request_context(), Some(destroyed));
            }
            assert_eq!(envelope.request_context(), Some(original.clone()));
            let mut expected = vec![("poll", Some(original.clone()), owner)];
            if !panic_on_poll {
                expected.push(("drop", Some(polled), owner));
            }
            assert_eq!(
                *actions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                expected,
            );

            drop(future);
            if panic_on_poll {
                expected.push(("drop", Some(original), owner));
            }
            assert_eq!(
                *actions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                expected,
            );
            assert_eq!(RuntimeContext::current(), Some(ambient));
        }
    }

    #[test]
    fn sibling_context_wrappers_isolate_poll_mutations() {
        let original = RuntimeContext::new();
        let changed = RuntimeContext::new();
        let ambient = RuntimeContext::new();
        let envelope = {
            let _original = original.enter();
            ContextEnvelope::capture()
        };
        let mut first = Box::pin(envelope.wrap(ContextMutationFuture {
            polls: 0,
            original: original.clone(),
            changed: changed.clone(),
        }));
        let mut sibling = Box::pin(envelope.wrap(async { RuntimeContext::current() }));
        let _ambient = ambient.enter();
        let waker = noop_waker();
        let mut task = Context::from_waker(&waker);

        assert!(first.as_mut().poll(&mut task).is_pending());
        assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
        assert_eq!(
            sibling.as_mut().poll(&mut task),
            Poll::Ready(Some(original)),
        );
        assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
        assert!(first.as_mut().poll(&mut task).is_ready());
        assert_eq!(RuntimeContext::current(), Some(ambient));
    }

    #[test]
    fn context_is_restored_without_bleed() {
        let runtime = runtime();
        runtime.block_on(async {
            let mut caller = CallerDrivenGuard::new(8).expect("caller-driven worker starts");
            let first = RuntimeContext::new();
            let second = RuntimeContext::new();
            let ambient = RuntimeContext::new();
            let (first_started_tx, first_started_rx) = oneshot::channel();
            let (second_started_tx, second_started_rx) = oneshot::channel();
            let (release_first_tx, release_first_rx) = oneshot::channel();
            let (release_second_tx, release_second_rx) = oneshot::channel();

            let first_call = {
                let _first = first.enter();
                let expected = first.clone();
                caller
                    .handle()
                    .call_local(move |_| async move {
                        assert_eq!(RuntimeContext::current(), Some(expected.clone()));
                        first_started_tx.send(()).expect("announce first poll");
                        release_first_rx.await.expect("release first poll");
                        RuntimeContext::current()
                    })
                    .expect("first call admitted")
            };
            let second_call = {
                let _second = second.enter();
                let expected = second.clone();
                caller
                    .handle()
                    .call_local(move |_| async move {
                        assert_eq!(RuntimeContext::current(), Some(expected.clone()));
                        second_started_tx.send(()).expect("announce second poll");
                        release_second_rx.await.expect("release second poll");
                        RuntimeContext::current()
                    })
                    .expect("second call admitted")
            };

            let _ambient = ambient.enter();
            caller
                .run_until(async {
                    first_started_rx.await.expect("first command starts");
                    assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
                    second_started_rx.await.expect("second command starts");
                    assert_eq!(RuntimeContext::current(), Some(ambient.clone()));

                    release_second_tx.send(()).expect("release second command");
                    assert_eq!(
                        second_call.await.expect("second command completes"),
                        Some(second.clone()),
                    );
                    assert_eq!(RuntimeContext::current(), Some(ambient.clone()));

                    release_first_tx.send(()).expect("release first command");
                    assert_eq!(
                        first_call.await.expect("first command completes"),
                        Some(first.clone()),
                    );
                    assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
                })
                .await
                .expect("interleaved calls remain on owner");
            caller.shutdown().await.expect("caller-driven worker joins");

            let captured = {
                let _first = first.enter();
                ContextEnvelope::capture()
            };
            let panic_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                captured.run(|| {
                    assert_eq!(RuntimeContext::current(), Some(first.clone()));
                    panic!("deterministic context unwind");
                });
            }));
            assert!(panic_result.is_err());
            assert_eq!(RuntimeContext::current(), Some(ambient.clone()));

            let dropped_under = Arc::new(Mutex::new(None));
            let mut future = Box::pin(captured.wrap(PendingDropProbe {
                observed: Arc::clone(&dropped_under),
            }));
            let waker = noop_waker();
            let mut task = Context::from_waker(&waker);
            assert!(future.as_mut().poll(&mut task).is_pending());
            assert_eq!(RuntimeContext::current(), Some(ambient.clone()));
            drop(future);
            assert_eq!(
                *dropped_under
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                Some(Some(first)),
            );
            assert_eq!(RuntimeContext::current(), Some(ambient));

            let original = RuntimeContext::new();
            let changed = RuntimeContext::new();
            let envelope = {
                let _original = original.enter();
                ContextEnvelope::capture()
            };
            let outside = RuntimeContext::new();
            let _outside = outside.enter();
            let mut future = Box::pin(envelope.wrap(ContextMutationFuture {
                polls: 0,
                original,
                changed,
            }));
            assert!(future.as_mut().poll(&mut task).is_pending());
            assert_eq!(RuntimeContext::current(), Some(outside.clone()));
            assert!(future.as_mut().poll(&mut task).is_ready());
            assert_eq!(RuntimeContext::current(), Some(outside));

            let queued_context = RuntimeContext::new();
            let queued_drop = Arc::new(Mutex::new(None));
            let mut queued_guard = CallerDrivenGuard::new(1).expect("queued-drop worker starts");
            {
                let _queued = queued_context.enter();
                queued_guard
                    .handle()
                    .spawn_local({
                        let probe = PendingDropProbe {
                            observed: Arc::clone(&queued_drop),
                        };
                        move |_| async move {
                            probe.await;
                        }
                    })
                    .expect("queued command admitted");
            }
            queued_guard
                .shutdown()
                .await
                .expect("queued command discarded during shutdown");
            assert_eq!(
                *queued_drop
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                Some(Some(queued_context)),
            );

            let active_context = RuntimeContext::new();
            let active_drop = Arc::new(Mutex::new(None));
            let mut active_guard = CallerDrivenGuard::new(1).expect("active-drop worker starts");
            let (active_tx, active_rx) = oneshot::channel();
            {
                let _active = active_context.enter();
                active_guard
                    .handle()
                    .spawn_local({
                        let observed = Arc::clone(&active_drop);
                        move |_| async move {
                            let _probe = PendingDropProbe { observed };
                            active_tx.send(()).expect("announce active future");
                            pending::<()>().await;
                        }
                    })
                    .expect("active command admitted");
            }
            active_guard
                .run_until(active_rx)
                .await
                .expect("active worker stays on owner")
                .expect("active future starts");
            active_guard
                .shutdown()
                .await
                .expect("active future discarded during shutdown");
            assert_eq!(
                *active_drop
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                Some(Some(active_context)),
            );
        });
    }

    #[test]
    fn metrics_have_declared_units_and_bounded_labels() {
        const OPERATIONS: &[&str] = &["command", "stream"];
        const OUTCOMES: &[&str] = &["success", "failure", "cancelled"];
        const LABELS: &[MetricLabelSpec] = &[
            MetricLabelSpec::new("operation", OPERATIONS),
            MetricLabelSpec::new("outcome", OUTCOMES),
        ];
        const NO_LABELS: &[MetricLabelSpec] = &[];
        const IDENTIFIER_VALUE: &[&str] = &["present"];
        const REQUEST_ID: &[MetricLabelSpec] =
            &[MetricLabelSpec::new("http_request_id", IDENTIFIER_VALUE)];
        const SESSION_ID: &[MetricLabelSpec] =
            &[MetricLabelSpec::new("session-id", IDENTIFIER_VALUE)];
        const STREAM_ID: &[MetricLabelSpec] =
            &[MetricLabelSpec::new("stream.id", IDENTIFIER_VALUE)];

        let duration = MetricDescriptor::new(
            "roxygen.operation.duration",
            MetricUnit::Microseconds,
            LABELS,
        )
        .expect("bounded descriptor");
        let bytes = MetricDescriptor::new("roxygen.stream.bytes", MetricUnit::Bytes, NO_LABELS)
            .expect("unit-only descriptor");
        let registry = MetricRegistry::default();
        let success = [
            MetricLabel::new("outcome", "success"),
            MetricLabel::new("operation", "command"),
        ];
        registry
            .record(&duration, &success, 7)
            .expect("bounded sample accepted");
        registry
            .record(&duration, &success, 5)
            .expect("matching dimensions accumulate");
        registry
            .record(&bytes, &[], 128)
            .expect("byte sample accepted");

        assert_eq!(
            registry.record(
                &duration,
                &[
                    MetricLabel::new("operation", "request-72831"),
                    MetricLabel::new("outcome", "success"),
                ],
                1,
            ),
            Err(MetricError::LabelValueOutOfRange {
                key: "operation",
                value: "request-72831",
            }),
        );
        assert_eq!(
            registry.record(&duration, &[MetricLabel::new("operation", "command")], 1,),
            Err(MetricError::LabelSetMismatch),
        );
        assert_eq!(
            MetricDescriptor::new("bad.request", MetricUnit::Count, REQUEST_ID),
            Err(MetricError::IdentifierLabel("http_request_id")),
        );
        assert_eq!(
            MetricDescriptor::new("bad.session", MetricUnit::Count, SESSION_ID),
            Err(MetricError::IdentifierLabel("session-id")),
        );
        assert_eq!(
            MetricDescriptor::new("bad.stream", MetricUnit::Count, STREAM_ID),
            Err(MetricError::IdentifierLabel("stream.id")),
        );

        let samples = registry.snapshot();
        assert_eq!(samples.len(), 2);
        let duration_sample = samples
            .iter()
            .find(|sample| sample.name == duration.name())
            .expect("duration sample present");
        assert_eq!(duration_sample.unit, MetricUnit::Microseconds);
        assert_eq!(duration_sample.value, 12);
        let byte_sample = samples
            .iter()
            .find(|sample| sample.name == bytes.name())
            .expect("byte sample present");
        assert_eq!(byte_sample.unit, MetricUnit::Bytes);
        assert_eq!(byte_sample.value, 128);
    }

    #[test]
    fn terminal_spans_close_for_all_outcomes() {
        let observer = SpanObserver::new(128).expect("bounded span observer");
        let observed = ContextEnvelope::capture().with_span_observer(observer.clone());

        runtime().block_on(async {
            let mut caller = CallerDrivenGuard::new(1).expect("caller-driven worker starts");
            let handle = caller.handle();
            let (ran_tx, ran_rx) = oneshot::channel();
            observed
                .run(|| handle.submit(move |_| ran_tx.send(()).expect("announce command")))
                .expect("first command admitted");
            assert_eq!(
                observed.run(|| handle.submit(|_| {})),
                Err(crate::worker::SubmitError::Overloaded),
            );
            caller
                .run_until(ran_rx)
                .await
                .expect("driver remains on owner")
                .expect("admitted command runs");
            observed
                .wrap(caller.shutdown())
                .await
                .expect("caller-driven shutdown joins");
        });

        let managed = ManagedWorker::new(4).expect("managed worker starts");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        observed
            .run(|| {
                managed.handle().spawn_local(move |_| async move {
                    started_tx.send(()).expect("announce pending task");
                    pending::<()>().await;
                })
            })
            .expect("pending command admitted");
        started_rx.recv().expect("pending command starts");
        observed
            .run(|| managed.shutdown())
            .expect("managed shutdown cancels and joins");

        let task_runtime = runtime();
        let local = LocalSet::new();
        local.block_on(
            &task_runtime,
            observed.wrap(async {
                let mut tasks = TaskGroup::new();
                tasks
                    .spawn_local(|_| async move {
                        panic!("deterministic task failure");
                    })
                    .expect("panicking task admitted");
                assert_eq!(
                    tasks.join().await,
                    Err(crate::task_group::TaskGroupError::TaskPanicked),
                );
            }),
        );

        runtime().block_on(async {
            tokio::time::pause();
            let timeout = observed.run(|| {
                OperationSpan::start(OperationKind::Queue, &ContextEnvelope::capture())
                    .instrument_timeout(pending::<()>(), Duration::from_secs(5), |_| {
                        TerminalOutcome::Success
                    })
            });
            let mut timeout = Box::pin(timeout);
            futures::future::poll_fn(|context| {
                assert!(timeout.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            tokio::time::advance(Duration::from_secs(5)).await;
            assert_eq!(timeout.await, Err(crate::observability::OperationTimeout));
        });

        let snapshots = observer.snapshots();
        assert_eq!(observer.active_count(), 0);
        assert!(snapshots.iter().all(|span| span.close_count == 1));
        for (kind, outcome) in [
            (OperationKind::Command, TerminalOutcome::Success),
            (OperationKind::Command, TerminalOutcome::Failure),
            (OperationKind::Command, TerminalOutcome::Cancelled),
            (OperationKind::Overload, TerminalOutcome::Overloaded),
            (OperationKind::Queue, TerminalOutcome::TimedOut),
            (OperationKind::Shutdown, TerminalOutcome::Shutdown),
            (OperationKind::Cancellation, TerminalOutcome::Success),
        ] {
            assert!(
                snapshots
                    .iter()
                    .any(|span| span.kind == kind && span.outcome == Some(outcome))
            );
        }
        assert!(snapshots.iter().any(|command| {
            command.kind == OperationKind::Command
                && snapshots.iter().any(|queue| {
                    queue.kind == OperationKind::Queue && command.parent == Some(queue.id)
                })
        }));

        let bounded = SpanObserver::new(2).expect("small bounded observer");
        let context = ContextEnvelope::capture().with_span_observer(bounded.clone());
        for _ in 0..3 {
            let mut span = OperationSpan::start(OperationKind::Command, &context);
            span.complete(TerminalOutcome::Success);
        }
        assert_eq!(bounded.snapshots().len(), 2);
        assert_eq!(bounded.dropped_records(), 0);
        assert_eq!(bounded.drain_completed().len(), 2);
        assert!(bounded.snapshots().is_empty());

        let saturated = SpanObserver::new(1).expect("active span bound");
        let context = ContextEnvelope::capture().with_span_observer(saturated.clone());
        let mut first = OperationSpan::start(OperationKind::Command, &context);
        let mut unrecorded = OperationSpan::start(OperationKind::Stream, &context);
        assert_eq!(saturated.snapshots().len(), 1);
        assert_eq!(saturated.dropped_records(), 1);
        unrecorded.complete(TerminalOutcome::Success);
        first.complete(TerminalOutcome::Success);
        assert_eq!(saturated.drain_completed().len(), 1);
    }

    /// What the thread has installed, read without capturing an envelope.
    fn installed() -> (
        Option<RuntimeContext>,
        Option<crate::context::SpanLink>,
        bool,
    ) {
        (
            RuntimeContext::current(),
            crate::context::current_span_link(),
            crate::context::current_span_observer().is_some(),
        )
    }

    /// An envelope with its own request context, span link, and span
    /// observer, run from a thread that has a different request context and
    /// neither of the others.
    fn run_fixture() -> (RuntimeContext, ContextEnvelope, RuntimeContextGuard) {
        let outer = RuntimeContext::new();
        let entered = outer.enter();
        let observer = SpanObserver::new(4).expect("span observer");
        let span = observer.start(OperationKind::Command, &ContextEnvelope::capture());
        let mut envelope = span.context().clone();
        envelope.set_request_context(Some(RuntimeContext::new()));
        (outer, envelope, entered)
    }

    #[test]
    fn context_run_restores_after_return_and_unwind() {
        let (outer, envelope, _entered) = run_fixture();
        let before = (Some(outer.clone()), None, false);
        assert_eq!(installed(), before);
        let inner = (envelope.request_context(), envelope.linked_span(), true);
        assert!(inner.1.is_some());

        let value = envelope.run(|| {
            assert_eq!(installed(), inner);
            RuntimeContext::set_current(Some(RuntimeContext::new()));
            let nested = ContextEnvelope::capture();
            nested.run(|| assert_eq!(installed().1, inner.1));
            assert_eq!(
                installed().1,
                inner.1,
                "a nested run restores the outer install"
            );
            7
        });
        assert_eq!(value, 7);
        assert_eq!(
            installed(),
            before,
            "a normal return restores the prior context"
        );

        let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
            envelope.run(|| {
                assert_eq!(installed(), inner);
                RuntimeContext::set_current(None);
                std::panic::panic_any("context run callback panic")
            })
        }));
        let payload = unwound.expect_err("the callback panics");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"context run callback panic")
        );
        assert_eq!(
            installed(),
            before,
            "an unwinding callback restores the prior context"
        );
    }

    #[test]
    fn context_run_returns_without_the_cold_restore() {
        let (_outer, envelope, _entered) = run_fixture();
        let restores = crate::context::cold_context_restores();
        assert_eq!(envelope.run(|| 3), 3);
        envelope.run(|| ContextEnvelope::capture().run(|| ()));
        assert_eq!(
            crate::context::cold_context_restores(),
            restores,
            "a normal return restores inline"
        );
        let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
            envelope.run(|| std::panic::panic_any("context run callback panic"))
        }));
        assert!(unwound.is_err());
        assert_eq!(
            crate::context::cold_context_restores(),
            restores + 1,
            "an unwinding callback restores through the guard once"
        );
    }
}
