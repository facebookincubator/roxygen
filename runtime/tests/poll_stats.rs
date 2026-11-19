/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod poll_stats {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    #[cfg(feature = "observability")]
    use std::fmt::Write as _;
    use std::future::Future;
    use std::future::pending;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    #[cfg(feature = "observability")]
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Instant;

    use futures::task::noop_waker;
    use tokio::runtime::Builder;

    use crate::driver::WorkerDriver;
    use crate::host::Host;
    use crate::host::LocalFuture;
    use crate::host::LocalHost;
    use crate::panic::panic_message;
    use crate::panic::truncated;
    use crate::tokio_host::ManagedWorker;
    use crate::worker::ContainedPanicScope;
    use crate::worker::WorkerCleanupTask;
    use crate::worker::WorkerCleanupTaskRegistration;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerHandle;
    use crate::worker::WorkerPollStats;

    /// A cleanup that counts its polls, keeps its latest waker, and finishes
    /// once `ready` is set.
    #[derive(Clone, Default)]
    struct Probe {
        polls: Rc<Cell<u64>>,
        ready: Rc<Cell<bool>>,
        waker: Rc<RefCell<Option<Waker>>>,
    }

    impl Probe {
        fn future(&self) -> ProbeFuture {
            ProbeFuture(self.clone())
        }

        fn reserve(&self, context: &WorkerContext) -> WorkerCleanupTaskRegistration {
            context
                .register_cleanup_task(self.future())
                .expect("cleanup reserved on the owner while open")
        }

        fn activate(&self, context: &WorkerContext) -> WorkerCleanupTask {
            self.reserve(context)
                .activate()
                .expect("cleanup activates while open")
        }

        fn finish(&self) {
            self.ready.set(true);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    struct ProbeFuture(Probe);

    impl Future for ProbeFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            self.0.polls.set(self.0.polls.get() + 1);
            if self.0.ready.get() {
                return Poll::Ready(());
            }
            *self.0.waker.borrow_mut() = Some(context.waker().clone());
            Poll::Pending
        }
    }

    /// A host that runs its spawned tasks only when the test runs them.
    #[derive(Clone, Default)]
    struct ManualHost {
        tasks: Rc<RefCell<VecDeque<LocalFuture>>>,
    }

    impl ManualHost {
        fn run(&self) {
            let tasks: Vec<_> = self.tasks.borrow_mut().drain(..).collect();
            let waker = noop_waker();
            let pending: Vec<_> = tasks
                .into_iter()
                .filter_map(|mut task| {
                    task.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                        .then_some(task)
                })
                .collect();
            self.tasks.borrow_mut().extend(pending);
        }
    }

    impl LocalHost for ManualHost {
        fn spawn_local(&self, future: LocalFuture) {
            self.tasks.borrow_mut().push_back(future);
        }

        fn now(&self) -> Instant {
            Instant::now()
        }

        fn sleep_until(&self, _deadline: Instant) -> LocalFuture {
            Box::pin(pending())
        }

        fn yield_to_io(&self) -> LocalFuture {
            Box::pin(std::future::ready(()))
        }
    }

    fn turn(driver: &mut WorkerDriver) -> Poll<()> {
        Pin::new(driver).poll(&mut Context::from_waker(&noop_waker()))
    }

    fn histogram(entries: &[(usize, u64)]) -> [u64; 8] {
        let mut buckets = [0; 8];
        for &(bucket, count) in entries {
            buckets[bucket] = count;
        }
        buckets
    }

    /// The counters of `stats`, without its gauges and maxima.
    fn counts(stats: &WorkerPollStats) -> (u64, u64, u64, [u64; 8]) {
        (
            stats.driver_polls,
            stats.command_polls,
            stats.cleanup_polls,
            stats.cleanup_polls_per_driver_poll,
        )
    }

    fn gauges(handle: &WorkerHandle) -> (u64, u64) {
        let stats = handle.poll_stats();
        (stats.cleanups_registered, stats.cleanups_active)
    }

    #[test]
    fn inline_driver_counts_each_driver_command_and_cleanup_poll() {
        let (handle, mut driver) = WorkerDriver::new(4).expect("worker starts");
        let context = driver.context().clone();
        assert_eq!(handle.poll_stats(), WorkerPollStats::default());

        let start = handle.poll_stats();
        assert!(turn(&mut driver).is_pending());
        let delta = handle.poll_stats().since(&start);
        assert_eq!(counts(&delta), (1, 0, 0, histogram(&[(0, 1)])));

        let active = Probe::default();
        let reserved = Probe::default();
        let _active_task = active.activate(&context);
        let registration = reserved.reserve(&context);
        assert_eq!(gauges(&handle), (2, 1));

        let start = handle.poll_stats();
        handle.submit(|_| {}).expect("command admitted");
        assert!(turn(&mut driver).is_pending());
        let delta = handle.poll_stats().since(&start);
        assert_eq!(counts(&delta), (1, 1, 1, histogram(&[(1, 1)])));
        assert!(turn(&mut driver).is_pending());
        let delta = handle.poll_stats().since(&start);
        assert_eq!(counts(&delta), (2, 1, 1, histogram(&[(0, 1), (1, 1)])));
        assert_eq!((active.polls.get(), reserved.polls.get()), (1, 0));

        assert_eq!(registration.disarm(), Ok(true));
        assert_eq!(gauges(&handle), (1, 1));
        active.finish();
        assert!(turn(&mut driver).is_pending());
        assert_eq!(gauges(&handle), (0, 0));

        assert!(handle.begin_shutdown());
        assert!(turn(&mut driver).is_ready());
        let total = handle.poll_stats();
        assert_eq!(counts(&total), (5, 1, 2, histogram(&[(0, 3), (1, 2)])));
        assert_eq!(total.max_cleanup_polls_per_driver_poll, 1);
        assert_eq!(total.budget_exhausted_passes, 0);
        assert_eq!(handle.driver_poll_count(), 5);

        assert!(turn(&mut driver).is_ready());
        assert_eq!(
            handle.poll_stats(),
            total,
            "a completed driver counts no more polls"
        );
    }

    #[test]
    fn hosted_driver_counts_cleanup_task_polls_without_a_histogram() {
        let host = ManualHost::default();
        let (handle, mut driver) =
            WorkerDriver::with_host(4, Host::new(Rc::new(host.clone()))).expect("worker starts");
        let context = driver.context().clone();
        assert!(turn(&mut driver).is_pending());

        let active = Probe::default();
        let _active_task = active.activate(&context);
        let reserved = Probe::default();
        let _registration = reserved.reserve(&context);
        assert_eq!(gauges(&handle), (2, 1));
        host.run();
        handle.submit(|_| {}).expect("command admitted");
        assert!(turn(&mut driver).is_pending());
        // The manual host polls every live task on each run.
        host.run();
        active.finish();
        host.run();
        assert_eq!(active.polls.get(), 3);
        assert_eq!(gauges(&handle), (1, 0));
        assert!(turn(&mut driver).is_pending());

        assert!(handle.begin_shutdown());
        reserved.ready.set(true);
        assert!(turn(&mut driver).is_pending());
        host.run();
        assert_eq!(reserved.polls.get(), 1);
        assert!(turn(&mut driver).is_ready());
        let total = handle.poll_stats();
        assert_eq!(counts(&total), (5, 0, 4, [0; 8]));
        assert_eq!(total.max_cleanup_polls_per_driver_poll, 0);
        assert_eq!(gauges(&handle), (0, 0));
    }

    #[test]
    fn histogram_buckets_follow_powers_of_two() {
        for (polls, bucket) in [(0, 0), (1, 1), (3, 2), (4, 3), (63, 6), (64, 7), (1000, 7)] {
            let (handle, mut driver) = WorkerDriver::new(4).expect("worker starts");
            let context = driver.context().clone();
            let ready = Rc::new(Cell::new(false));
            let tasks: Vec<_> = (0..polls)
                .map(|_| {
                    let probe = Probe {
                        ready: Rc::clone(&ready),
                        ..Probe::default()
                    };
                    probe.activate(&context)
                })
                .collect();
            let start = handle.poll_stats();
            assert!(turn(&mut driver).is_pending());
            let delta = handle.poll_stats().since(&start);
            assert_eq!(delta.cleanup_polls, polls, "{polls}");
            assert_eq!(
                delta.cleanup_polls_per_driver_poll,
                histogram(&[(bucket, 1)]),
                "{polls}"
            );
            assert_eq!(delta.max_cleanup_polls_per_driver_poll, polls, "{polls}");

            ready.set(true);
            assert!(handle.begin_shutdown());
            assert!(turn(&mut driver).is_ready());
            drop(tasks);
        }
    }

    #[test]
    fn since_keeps_later_gauges_and_maxima_and_merge_sums() {
        let earlier = WorkerPollStats {
            driver_polls: 10,
            command_polls: 4,
            cleanup_polls: 20,
            ready_entries: 18,
            stale_ready_entries: 1,
            budget_exhausted_passes: 1,
            cleanup_polls_per_driver_poll: [1, 2, 3, 4, 0, 0, 0, 0],
            max_cleanup_polls_per_driver_poll: 9,
            max_ready_batch: 8,
            cleanups_registered: 7,
            cleanups_active: 5,
            contained_exchange_panics: 3,
            contained_connection_panics: 2,
            contained_driver_panics: 1,
        };
        let later = WorkerPollStats {
            driver_polls: 15,
            command_polls: 6,
            cleanup_polls: 29,
            ready_entries: 27,
            stale_ready_entries: 2,
            budget_exhausted_passes: 3,
            cleanup_polls_per_driver_poll: [2, 4, 3, 5, 1, 0, 0, 0],
            max_cleanup_polls_per_driver_poll: 12,
            max_ready_batch: 10,
            cleanups_registered: 2,
            cleanups_active: 1,
            contained_exchange_panics: 8,
            contained_connection_panics: 6,
            contained_driver_panics: 2,
        };
        assert_eq!(
            later.since(&earlier),
            WorkerPollStats {
                driver_polls: 5,
                command_polls: 2,
                cleanup_polls: 9,
                ready_entries: 9,
                stale_ready_entries: 1,
                budget_exhausted_passes: 2,
                cleanup_polls_per_driver_poll: [1, 2, 0, 1, 1, 0, 0, 0],
                max_cleanup_polls_per_driver_poll: 12,
                max_ready_batch: 10,
                cleanups_registered: 2,
                cleanups_active: 1,
                contained_exchange_panics: 5,
                contained_connection_panics: 4,
                contained_driver_panics: 1,
            }
        );

        let mut merged = earlier;
        merged.merge(&later);
        assert_eq!(
            merged,
            WorkerPollStats {
                driver_polls: 25,
                command_polls: 10,
                cleanup_polls: 49,
                ready_entries: 45,
                stale_ready_entries: 3,
                budget_exhausted_passes: 4,
                cleanup_polls_per_driver_poll: [3, 6, 6, 9, 1, 0, 0, 0],
                max_cleanup_polls_per_driver_poll: 12,
                max_ready_batch: 10,
                cleanups_registered: 9,
                cleanups_active: 6,
                contained_exchange_panics: 11,
                contained_connection_panics: 8,
                contained_driver_panics: 3,
            }
        );
        let hosted = WorkerPollStats {
            cleanup_polls: 1,
            ..WorkerPollStats::default()
        };
        merged.merge(&hosted);
        assert_eq!(merged.cleanup_polls, 50);
    }

    #[test]
    fn contained_panics_are_counted_by_scope_and_the_message_is_read() {
        let (handle, driver) = WorkerDriver::new(4).expect("worker starts");
        let context = driver.context().clone();
        #[cfg(feature = "observability")]
        let (recorder, _subscriber) = EventRecorder::install(false);
        let long = "x".repeat(300);
        let start = handle.poll_stats();
        context.record_contained_panic(
            ContainedPanicScope::Exchange,
            "exchange_callback",
            Some(3),
            &"exchange panicked",
        );
        context.record_contained_panic(
            ContainedPanicScope::Exchange,
            "exchange_callback",
            Some(5),
            &long,
        );
        context.record_contained_panic(ContainedPanicScope::Driver, "driver_turn", None, &7_i32);
        let delta = handle.poll_stats().since(&start);
        assert_eq!(
            (
                delta.contained_exchange_panics,
                delta.contained_connection_panics,
                delta.contained_driver_panics,
            ),
            (2, 0, 1)
        );

        let literal = catch_unwind(|| panic!("literal")).unwrap_err();
        let formatted = catch_unwind(|| panic_any(String::from("formatted"))).unwrap_err();
        let other = catch_unwind(|| panic_any(7_i32)).unwrap_err();
        assert_eq!(panic_message(&*literal), Some("literal"));
        assert_eq!(panic_message(&*formatted), Some("formatted"));
        assert_eq!(panic_message(&*other), None);
        // Byte 128 falls inside a three-byte character, so the prefix ends at 127.
        let euros = format!("a{}", "\u{20ac}".repeat(100));
        assert_eq!(truncated(&euros, 128), &euros[..127]);
        assert_eq!(truncated(&long, 128).len(), 128);
        assert_eq!(truncated("short", 128), "short");

        #[cfg(feature = "observability")]
        assert_eq!(
            recorder.take(),
            [
                "ERROR roxygen::contained_panic scope=exchange site=exchange_callback stream=3 \
                 message=exchange panicked"
                    .to_owned(),
                format!(
                    "ERROR roxygen::contained_panic scope=exchange site=exchange_callback \
                     stream=5 message={}",
                    "x".repeat(128)
                ),
                "ERROR roxygen::contained_panic scope=driver site=driver_turn".to_owned(),
            ]
        );
        drop(driver);
    }

    #[cfg(feature = "observability")]
    #[test]
    fn a_panicking_subscriber_is_discarded() {
        let (handle, driver) = WorkerDriver::new(4).expect("worker starts");
        let context = driver.context().clone();
        let (recorder, _subscriber) = EventRecorder::install(true);
        let start = handle.poll_stats();
        context.record_contained_panic(
            ContainedPanicScope::Connection,
            "lifecycle_observer",
            None,
            &"observer panicked",
        );
        assert!(
            !recorder.panic_next.load(Ordering::Relaxed),
            "the subscriber saw the event"
        );
        assert!(recorder.take().is_empty());
        assert_eq!(
            handle
                .poll_stats()
                .since(&start)
                .contained_connection_panics,
            1
        );
        // The subscriber's panic left this thread's dispatcher usable.
        context.record_contained_panic(
            ContainedPanicScope::Connection,
            "lifecycle_observer",
            Some(1),
            &"observer panicked again",
        );
        assert_eq!(
            recorder.take(),
            [
                "ERROR roxygen::contained_panic scope=connection site=lifecycle_observer \
                 stream=1 message=observer panicked again"
            ]
        );
        assert_eq!(
            handle
                .poll_stats()
                .since(&start)
                .contained_connection_panics,
            2
        );
        drop(driver);
    }

    /// Records each `roxygen::contained_panic` event as one line, on the thread where it
    /// is the default subscriber. While `panic_next` is set, it panics on the next event
    /// instead.
    #[cfg(feature = "observability")]
    #[derive(Clone, Default)]
    struct EventRecorder {
        lines: Arc<Mutex<Vec<String>>>,
        panic_next: Arc<AtomicBool>,
    }

    #[cfg(feature = "observability")]
    impl EventRecorder {
        fn install(panic_next: bool) -> (Self, tracing::subscriber::DefaultGuard) {
            let recorder = Self::default();
            recorder.panic_next.store(panic_next, Ordering::Relaxed);
            let guard = tracing::subscriber::set_default(recorder.clone());
            (recorder, guard)
        }

        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.lines.lock().expect("recorder lock"))
        }
    }

    #[cfg(feature = "observability")]
    struct Line<'a>(&'a mut String);

    #[cfg(feature = "observability")]
    impl tracing::field::Visit for Line<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            let _ = write!(self.0, " {}={value}", field.name());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    #[cfg(feature = "observability")]
    impl tracing::Subscriber for EventRecorder {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "roxygen::contained_panic"
        }

        fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if self.panic_next.swap(false, Ordering::Relaxed) {
                panic!("subscriber panicked");
            }
            let metadata = event.metadata();
            let mut line = format!("{} {}", metadata.level(), metadata.target());
            event.record(&mut Line(&mut line));
            self.lines.lock().expect("recorder lock").push(line);
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[test]
    fn woken_driver_counts_only_woken_cleanup_polls_exactly() {
        let (handle, mut driver) = WorkerDriver::new(4).expect("worker starts");
        let context = driver.context().clone();
        assert!(turn(&mut driver).is_pending());

        let first = Probe::default();
        let second = Probe::default();
        let _tasks = (first.activate(&context), second.activate(&context));
        let reserved = Probe::default();
        let _registration = reserved.reserve(&context);
        assert_eq!(gauges(&handle), (3, 2));

        let start = handle.poll_stats();
        handle.submit(|_| {}).expect("command admitted");
        assert!(turn(&mut driver).is_pending());
        let delta = handle.poll_stats().since(&start);
        assert_eq!(counts(&delta), (1, 1, 2, histogram(&[(2, 1)])));
        assert_eq!((delta.ready_entries, delta.stale_ready_entries), (2, 0));

        let start = handle.poll_stats();
        assert!(turn(&mut driver).is_pending());
        first.waker.borrow().as_ref().expect("polled").wake_by_ref();
        assert!(turn(&mut driver).is_pending());
        let delta = handle.poll_stats().since(&start);
        assert_eq!(counts(&delta), (2, 0, 1, histogram(&[(0, 1), (1, 1)])));
        assert_eq!((delta.ready_entries, delta.stale_ready_entries), (1, 0));
        assert_eq!((first.polls.get(), second.polls.get()), (2, 1));

        first.finish();
        second.finish();
        assert!(turn(&mut driver).is_pending());
        assert_eq!(gauges(&handle), (1, 0));
        assert!(handle.begin_shutdown());
        reserved.ready.set(true);
        assert!(turn(&mut driver).is_ready());
        let total = handle.poll_stats();
        assert_eq!(
            counts(&total),
            (6, 1, 6, histogram(&[(0, 2), (1, 2), (2, 2)]))
        );
        assert_eq!((total.ready_entries, total.max_ready_batch), (5, 2));
        assert_eq!(gauges(&handle), (0, 0));
    }

    #[test]
    fn stats_are_read_from_another_thread_while_the_worker_runs() {
        const CALLS: u64 = 200;
        let worker = ManagedWorker::new(4).expect("worker starts");
        let handle = worker.handle();
        let stop = Arc::new(AtomicBool::new(false));
        let (started, ready) = mpsc::channel();
        let reader = std::thread::spawn({
            let handle = handle.clone();
            let stop = Arc::clone(&stop);
            move || {
                let mut previous = handle.poll_stats();
                let mut reads = 0_u64;
                while !stop.load(Ordering::Acquire) {
                    let current = handle.poll_stats();
                    assert!(current.driver_polls >= previous.driver_polls);
                    assert!(current.command_polls >= previous.command_polls);
                    previous = current;
                    reads += 1;
                    if reads == 1 {
                        started.send(()).expect("reader startup observed");
                    }
                }
                reads
            }
        });
        ready.recv().expect("reader has sampled the running worker");
        let runtime = Builder::new_current_thread().build().expect("test runtime");
        for value in 0..CALLS {
            let call = handle.call(move |_| value).expect("call admitted");
            assert_eq!(runtime.block_on(call), Ok(value));
        }
        worker.shutdown().expect("worker shuts down");
        let stats = handle.poll_stats();
        stop.store(true, Ordering::Release);
        assert!(reader.join().expect("reader joins") > 0);
        assert!(stats.driver_polls >= CALLS, "{stats:?}");
        assert!(stats.command_polls >= CALLS, "{stats:?}");
        assert_eq!(stats.driver_polls as usize, handle.driver_poll_count());
    }

    fn assert_gauges_follow_registration_activation_and_finish(
        handle: &WorkerHandle,
        driver: &mut WorkerDriver,
    ) {
        let context = driver.context().clone();
        assert!(turn(driver).is_pending());
        let first = Probe::default();
        let second = Probe::default();
        let third = Probe::default();
        let disarmed = first.reserve(&context);
        let refinished = second.reserve(&context);
        let dropped = third.reserve(&context);
        assert_eq!(gauges(handle), (3, 0));

        assert_eq!(disarmed.disarm(), Ok(true));
        assert_eq!(gauges(handle), (2, 0));
        let finish_again = refinished.finish_hook();
        assert_eq!(refinished.disarm(), Ok(true));
        assert_eq!(gauges(handle), (1, 0));
        finish_again();
        assert_eq!(gauges(handle), (1, 0), "a second finish counts nothing");

        drop(dropped);
        assert_eq!(
            gauges(handle),
            (1, 1),
            "dropping a registration activates it"
        );
        let active = Probe::default();
        let _task = active.activate(&context);
        let _reserved = Probe::default().reserve(&context);
        assert_eq!(gauges(handle), (3, 2));
    }

    #[test]
    fn gauges_follow_disarm_double_finish_activation_and_abort() {
        let (handle, mut driver) = WorkerDriver::new(4).expect("worker starts");
        assert_gauges_follow_registration_activation_and_finish(&handle, &mut driver);
        drop(driver);
        assert_eq!(gauges(&handle), (0, 0), "abort finishes every cleanup");

        let host = ManualHost::default();
        let (handle, mut driver) =
            WorkerDriver::with_host(4, Host::new(Rc::new(host.clone()))).expect("worker starts");
        assert_gauges_follow_registration_activation_and_finish(&handle, &mut driver);
        drop(driver);
        assert_eq!(
            gauges(&handle),
            (0, 0),
            "hosted abort finishes every cleanup"
        );
    }

    #[test]
    fn shutdown_force_activation_is_not_counted_as_activation() {
        let host = ManualHost::default();
        let (handle, mut driver) =
            WorkerDriver::with_host(4, Host::new(Rc::new(host.clone()))).expect("worker starts");
        let context = driver.context().clone();
        assert!(turn(&mut driver).is_pending());
        let reserved = Probe::default();
        let _registration = reserved.reserve(&context);
        assert!(handle.begin_shutdown());
        assert!(turn(&mut driver).is_pending());
        assert_eq!(gauges(&handle), (1, 0));
        reserved.ready.set(true);
        host.run();
        assert!(turn(&mut driver).is_ready());
        assert_eq!(gauges(&handle), (0, 0));
    }
}
