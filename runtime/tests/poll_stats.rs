mod poll_stats {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::future::pending;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
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
    use crate::tokio_host::ManagedWorker;
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

    /// A host whose spawned tasks run only when the test runs them.
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
        assert_eq!(counts(&delta), (2, 1, 2, histogram(&[(1, 2)])));
        assert_eq!((active.polls.get(), reserved.polls.get()), (2, 0));

        assert_eq!(registration.disarm(), Ok(true));
        assert_eq!(gauges(&handle), (1, 1));
        active.finish();
        assert!(turn(&mut driver).is_pending());
        assert_eq!(gauges(&handle), (0, 0));

        assert!(handle.begin_shutdown());
        assert!(turn(&mut driver).is_ready());
        let total = handle.poll_stats();
        assert_eq!(counts(&total), (5, 1, 3, histogram(&[(0, 2), (1, 3)])));
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
            budget_exhausted_passes: 1,
            cleanup_polls_per_driver_poll: [1, 2, 3, 4, 0, 0, 0, 0],
            max_cleanup_polls_per_driver_poll: 9,
            cleanups_registered: 7,
            cleanups_active: 5,
        };
        let later = WorkerPollStats {
            driver_polls: 15,
            command_polls: 6,
            cleanup_polls: 29,
            budget_exhausted_passes: 3,
            cleanup_polls_per_driver_poll: [2, 4, 3, 5, 1, 0, 0, 0],
            max_cleanup_polls_per_driver_poll: 12,
            cleanups_registered: 2,
            cleanups_active: 1,
        };
        assert_eq!(
            later.since(&earlier),
            WorkerPollStats {
                driver_polls: 5,
                command_polls: 2,
                cleanup_polls: 9,
                budget_exhausted_passes: 2,
                cleanup_polls_per_driver_poll: [1, 2, 0, 1, 1, 0, 0, 0],
                max_cleanup_polls_per_driver_poll: 12,
                cleanups_registered: 2,
                cleanups_active: 1,
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
                budget_exhausted_passes: 4,
                cleanup_polls_per_driver_poll: [3, 6, 6, 9, 1, 0, 0, 0],
                max_cleanup_polls_per_driver_poll: 12,
                cleanups_registered: 9,
                cleanups_active: 6,
            }
        );
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
