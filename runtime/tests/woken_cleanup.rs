/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod woken_cleanup {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use futures::channel::oneshot;
    use tokio::runtime::Builder;
    use tokio::task::LocalSet;

    use crate::driver::WorkerDriver;
    use crate::tokio_host::ManagedWorker;
    use crate::worker::WorkerCleanupObserver;
    use crate::worker::WorkerCleanupTask;
    use crate::worker::WorkerCleanupTaskRegistration;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerHandle;
    use crate::worker::WorkerPollStats;
    use crate::worker::WorkerState;

    const TIMEOUT: Duration = Duration::from_secs(10);

    /// A driver waker that counts its wakes.
    #[derive(Default)]
    struct DriverWake {
        wakes: AtomicUsize,
    }

    impl Wake for DriverWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A non-hosted driver that the test thread polls with a counting waker.
    struct Worker {
        handle: WorkerHandle,
        driver: WorkerDriver,
        context: WorkerContext,
        wake: Arc<DriverWake>,
        waker: Waker,
    }

    impl Worker {
        fn start() -> Self {
            let (handle, driver) = WorkerDriver::new(4).expect("worker starts");
            let context = driver.context().clone();
            let wake = Arc::new(DriverWake::default());
            let waker = Waker::from(Arc::clone(&wake));
            let mut worker = Self {
                handle,
                driver,
                context,
                wake,
                waker,
            };
            assert!(worker.turn().is_pending());
            worker
        }

        fn turn(&mut self) -> Poll<()> {
            Pin::new(&mut self.driver).poll(&mut Context::from_waker(&self.waker))
        }

        fn wakes(&self) -> usize {
            self.wake.wakes.load(Ordering::SeqCst)
        }

        fn stats(&self) -> WorkerPollStats {
            self.handle.poll_stats()
        }

        /// Turns once and returns what that driver poll counted.
        fn counted_turn(&mut self) -> WorkerPollStats {
            let before = self.stats();
            assert!(self.turn().is_pending());
            self.stats().since(&before)
        }

        /// Starts shutdown and turns until the driver completes.
        fn shut_down(mut self) {
            assert!(self.handle.begin_shutdown());
            for _ in 0..16 {
                if self.turn().is_ready() {
                    assert_eq!(self.handle.state(), WorkerState::Closed);
                    return;
                }
            }
            panic!("shutdown did not complete");
        }
    }

    type Hook = Box<dyn FnMut(&Waker)>;

    /// An owner-local cleanup that counts its polls, logs its id, keeps its
    /// latest waker, and finishes once `ready` is set or the worker cancels
    /// when `until_cancelled` is set.
    #[derive(Clone)]
    struct Probe {
        id: usize,
        log: Rc<RefCell<Vec<usize>>>,
        polls: Rc<Cell<usize>>,
        ready: Rc<Cell<bool>>,
        until_cancelled: Option<WorkerHandle>,
        waker: Rc<RefCell<Option<Waker>>>,
        wakers: Rc<RefCell<Vec<Waker>>>,
        clones: Rc<Cell<usize>>,
        hook: Rc<RefCell<Option<Hook>>>,
    }

    impl Probe {
        fn new(id: usize, log: &Rc<RefCell<Vec<usize>>>) -> Self {
            Self {
                id,
                log: Rc::clone(log),
                polls: Rc::default(),
                ready: Rc::default(),
                until_cancelled: None,
                waker: Rc::default(),
                wakers: Rc::default(),
                clones: Rc::default(),
                hook: Rc::default(),
            }
        }

        fn alone() -> Self {
            Self::new(0, &Rc::default())
        }

        fn until_cancelled(mut self, handle: &WorkerHandle) -> Self {
            self.until_cancelled = Some(handle.clone());
            self
        }

        fn polls(&self) -> usize {
            self.polls.get()
        }

        fn set_hook(&self, hook: impl FnMut(&Waker) + 'static) {
            *self.hook.borrow_mut() = Some(Box::new(hook));
        }

        fn reserve(&self, context: &WorkerContext) -> WorkerCleanupTaskRegistration {
            context
                .register_cleanup_task(ProbeFuture(self.clone()))
                .expect("cleanup reserved on the owner while open")
        }

        fn activate(&self, context: &WorkerContext) -> WorkerCleanupTask {
            self.reserve(context)
                .activate()
                .expect("cleanup activates while open")
        }

        /// Wakes the latest waker that the cleanup kept.
        fn wake(&self) {
            self.waker
                .borrow()
                .as_ref()
                .expect("the cleanup was polled")
                .wake_by_ref();
        }

        fn finish(&self) {
            self.ready.set(true);
            if let Some(waker) = self.waker.borrow().as_ref() {
                waker.wake_by_ref();
            }
        }
    }

    struct ProbeFuture(Probe);

    impl Future for ProbeFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            let probe = &self.0;
            probe.polls.set(probe.polls.get() + 1);
            probe.log.borrow_mut().push(probe.id);
            probe.wakers.borrow_mut().push(context.waker().clone());
            {
                let mut waker = probe.waker.borrow_mut();
                if waker
                    .as_ref()
                    .is_none_or(|kept| !kept.will_wake(context.waker()))
                {
                    probe.clones.set(probe.clones.get() + 1);
                    *waker = Some(context.waker().clone());
                }
            }
            if let Some(hook) = probe.hook.borrow_mut().as_mut() {
                hook(context.waker());
            }
            let cancelled = probe
                .until_cancelled
                .as_ref()
                .is_some_and(WorkerHandle::is_cancelled);
            if probe.ready.get() || cancelled {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    fn bucket(index: usize) -> [u64; 8] {
        let mut buckets = [0; 8];
        buckets[index] = 1;
        buckets
    }

    fn observed(observer: &WorkerCleanupObserver) -> bool {
        Box::pin(observer.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            == Poll::Ready(Ok(()))
    }

    #[test]
    fn woken_polls_only_the_cleanup_whose_waker_fired() {
        let mut worker = Worker::start();
        let first = Probe::alone();
        let second = Probe::alone();
        let _tasks = (
            first.activate(&worker.context),
            second.activate(&worker.context),
        );
        let activation = worker.counted_turn();
        assert_eq!((first.polls(), second.polls()), (1, 1));
        assert_eq!(activation.cleanup_polls, 2);

        first.wake();
        let delta = worker.counted_turn();
        assert_eq!(delta.driver_polls, 1);
        assert_eq!(delta.stale_ready_entries, 0);
        assert_eq!((first.polls(), second.polls()), (2, 1));
        assert_eq!(activation.ready_entries, 2);
        assert_eq!(delta.cleanup_polls, 1);
        assert_eq!(delta.ready_entries, 1);
        assert_eq!(delta.cleanup_polls_per_driver_poll, bucket(1));
        first.finish();
        second.finish();
        assert!(worker.turn().is_pending());
        assert_eq!(worker.stats().cleanups_registered, 0);
        worker.shut_down();
    }

    #[test]
    fn a_cross_thread_wake_during_the_poll_is_not_lost() {
        let mut worker = Worker::start();
        let probe = Probe::alone();
        probe.set_hook(|waker| {
            let waker = waker.clone();
            std::thread::spawn(move || waker.wake())
                .join()
                .expect("waking thread joins");
        });
        let task = probe.activate(&worker.context);
        let wakes = worker.wakes();
        assert!(worker.turn().is_pending());
        assert_eq!(probe.polls(), 1);
        assert!(worker.wakes() > wakes, "the wake reached the driver");
        *probe.hook.borrow_mut() = None;
        probe.ready.set(true);
        assert!(worker.turn().is_pending());
        assert_eq!(probe.polls(), 2);
        assert!(task.is_finished());
        worker.shut_down();
    }

    thread_local! {
        static INTAKE_WAKE: RefCell<Option<Waker>> = const { RefCell::new(None) };
    }

    /// Wakes the kept cleanup waker from another thread, before the pass
    /// takes its commands.
    fn wake_from_another_thread(_: &WorkerHandle) {
        if let Some(waker) = INTAKE_WAKE.take() {
            std::thread::spawn(move || waker.wake())
                .join()
                .expect("waking thread joins");
        }
    }

    #[test]
    fn a_wake_before_the_drain_is_polled_in_the_same_pass() {
        let mut worker = Worker::start();
        let probe = Probe::alone();
        let _task = probe.activate(&worker.context);
        assert!(worker.turn().is_pending());
        assert_eq!(probe.polls(), 1);
        INTAKE_WAKE.set(probe.waker.borrow().clone());
        let previous = crate::worker::set_driver_intake_hook(Some(wake_from_another_thread));
        let delta = worker.counted_turn();
        crate::worker::set_driver_intake_hook(previous);
        assert_eq!(probe.polls(), 2);
        assert_eq!(delta.cleanup_polls, 1);
        probe.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    /// A cleanup that finishes once it has seen `target` from `sent`, and
    /// reports its finish.
    struct CountingCleanup {
        sent: Arc<AtomicU64>,
        target: u64,
        waker: Arc<Mutex<Option<Waker>>>,
        done: Option<mpsc::Sender<()>>,
        _not_send: Rc<()>,
    }

    impl Future for CountingCleanup {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            {
                let mut waker = self.waker.lock().expect("waker slot");
                if waker
                    .as_ref()
                    .is_none_or(|kept| !kept.will_wake(context.waker()))
                {
                    *waker = Some(context.waker().clone());
                }
            }
            if self.sent.load(Ordering::Acquire) < self.target {
                return Poll::Pending;
            }
            if let Some(done) = self.done.take() {
                let _ = done.send(());
            }
            Poll::Ready(())
        }
    }

    #[test]
    fn ten_thousand_cross_thread_wakes_never_strand_the_cleanup() {
        const WAKES: u64 = 10_000;
        let worker = ManagedWorker::new(4).expect("worker starts");
        let sent = Arc::new(AtomicU64::new(0));
        let waker = Arc::new(Mutex::new(None::<Waker>));
        let (done_tx, done_rx) = mpsc::channel();
        worker
            .handle()
            .submit({
                let sent = Arc::clone(&sent);
                let waker = Arc::clone(&waker);
                move |context| {
                    context
                        .register_cleanup_task(CountingCleanup {
                            sent,
                            target: WAKES,
                            waker,
                            done: Some(done_tx),
                            _not_send: Rc::new(()),
                        })
                        .expect("cleanup reserved")
                        .activate()
                        .expect("cleanup activated")
                        .detach();
                }
            })
            .expect("command admitted");
        let waking = std::thread::spawn(move || {
            let mut random = 0x2545_f491_4f6c_dd1d_u64;
            for value in 1..=WAKES {
                sent.store(value, Ordering::Release);
                let kept = waker.lock().expect("waker slot").clone();
                if let Some(kept) = kept {
                    kept.wake();
                }
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                match random % 8 {
                    0 => std::thread::yield_now(),
                    1 => (0..random % 64).for_each(|_| std::hint::spin_loop()),
                    _ => {}
                }
            }
        });
        waking.join().expect("waking thread joins");
        done_rx
            .recv_timeout(TIMEOUT)
            .unwrap_or_else(|_| panic!("the last wake was lost"));
        worker.shutdown().expect("worker shuts down");
    }

    #[test]
    fn a_wake_of_a_cleanup_later_in_the_batch_coalesces() {
        let mut worker = Worker::start();
        let waker = Probe::alone();
        let woken = Probe::alone();
        let _tasks = (
            waker.activate(&worker.context),
            woken.activate(&worker.context),
        );
        assert!(worker.turn().is_pending());
        let armed = Rc::new(Cell::new(true));
        waker.set_hook({
            let woken = woken.clone();
            let armed = Rc::clone(&armed);
            move |_| {
                if armed.replace(false) {
                    woken.wake();
                }
            }
        });

        waker.wake();
        woken.wake();
        let wakes = worker.wakes();
        let delta = worker.counted_turn();
        assert_eq!((waker.polls(), woken.polls()), (2, 2));
        assert_eq!(delta.ready_entries, 2);
        assert_eq!(worker.wakes(), wakes, "the coalesced wake queued nothing");
        let delta = worker.counted_turn();
        assert_eq!((waker.polls(), woken.polls()), (2, 2));
        assert_eq!(delta.ready_entries, 0);
        waker.finish();
        woken.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    #[test]
    fn a_wake_of_a_cleanup_earlier_in_the_batch_polls_it_next_pass() {
        let mut worker = Worker::start();
        let waker = Probe::alone();
        let woken = Probe::alone();
        let _tasks = (
            waker.activate(&worker.context),
            woken.activate(&worker.context),
        );
        assert!(worker.turn().is_pending());
        let armed = Rc::new(Cell::new(true));
        waker.set_hook({
            let woken = woken.clone();
            let armed = Rc::clone(&armed);
            move |_| {
                if armed.replace(false) {
                    woken.wake();
                }
            }
        });

        woken.wake();
        waker.wake();
        let wakes = worker.wakes();
        let delta = worker.counted_turn();
        assert_eq!((waker.polls(), woken.polls()), (2, 2));
        assert_eq!(delta.ready_entries, 2);
        assert!(
            worker.wakes() > wakes,
            "the wake after its poll woke the driver"
        );
        let delta = worker.counted_turn();
        assert_eq!((waker.polls(), woken.polls()), (2, 3));
        assert_eq!(delta.ready_entries, 1);
        assert_eq!(delta.cleanup_polls, 1);
        let delta = worker.counted_turn();
        assert_eq!(delta.cleanup_polls, 0);
        waker.finish();
        woken.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    #[test]
    fn a_self_waking_cleanup_cannot_starve_a_sibling() {
        let mut worker = Worker::start();
        let spinning = Probe::alone();
        spinning.set_hook(Waker::wake_by_ref);
        let sibling = Probe::alone();
        let _tasks = (
            spinning.activate(&worker.context),
            sibling.activate(&worker.context),
        );
        assert!(worker.turn().is_pending());
        for pass in 2..=6 {
            let wakes = worker.wakes();
            let sibling_polls = sibling.polls();
            if pass == 4 {
                sibling.wake();
            }
            assert!(worker.turn().is_pending());
            assert_eq!(spinning.polls(), pass, "once per pass");
            assert!(worker.wakes() > wakes, "the self-wake rewakes");
            let expected = sibling_polls + usize::from(pass == 4);
            assert_eq!(sibling.polls(), expected, "pass {pass}");
        }
        *spinning.hook.borrow_mut() = None;
        spinning.finish();
        sibling.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    #[test]
    fn a_wake_after_completion_queues_nothing() {
        let mut worker = Worker::start();
        let probe = Probe::alone();
        let task = probe.activate(&worker.context);
        assert!(worker.turn().is_pending());
        probe.finish();
        assert!(worker.turn().is_pending());
        assert!(task.is_finished());
        let wakes = worker.wakes();
        let kept = probe
            .waker
            .borrow()
            .clone()
            .expect("the cleanup kept a waker");
        kept.wake_by_ref();
        std::thread::spawn(move || kept.wake())
            .join()
            .expect("waking thread joins");
        assert_eq!(
            worker.wakes(),
            wakes,
            "a finished cleanup's wake is latched"
        );
        let delta = worker.counted_turn();
        assert_eq!(
            (delta.ready_entries, delta.stale_ready_entries),
            (0, 0),
            "{delta:?}"
        );
        worker.shut_down();
    }

    #[test]
    fn a_self_wake_in_the_final_poll_leaves_one_stale_entry() {
        let mut worker = Worker::start();
        let probe = Probe::alone();
        probe.set_hook(Waker::wake_by_ref);
        probe.ready.set(true);
        let task = probe.activate(&worker.context);
        assert!(worker.turn().is_pending());
        assert!(task.is_finished());
        let delta = worker.counted_turn();
        assert_eq!(
            (
                delta.ready_entries,
                delta.stale_ready_entries,
                delta.cleanup_polls
            ),
            (1, 1, 0)
        );
        assert_eq!(probe.polls(), 1);
        worker.shut_down();
    }

    #[test]
    fn a_stale_entry_never_polls_the_cleanup_that_reused_its_slot() {
        let mut worker = Worker::start();
        let finished = Probe::alone();
        finished.set_hook(Waker::wake_by_ref);
        finished.ready.set(true);
        let _finished_task = finished.activate(&worker.context);
        assert!(worker.turn().is_pending());
        assert_eq!(worker.context.stored_hosted_cleanups(), 1);
        assert_eq!(worker.context.armed_hosted_cleanups(), 0);

        let reusing = Probe::alone();
        let _reusing_task = reusing.activate(&worker.context);
        assert_eq!(
            worker.context.stored_hosted_cleanups(),
            1,
            "the new cleanup reused the finished cleanup's slot"
        );
        let delta = worker.counted_turn();
        assert_eq!(reusing.polls(), 1, "the stale entry did not poll it");
        assert_eq!((delta.ready_entries, delta.stale_ready_entries), (2, 1));
        reusing.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    fn assert_send_sync<Value: Send + Sync>(_: &Value) {}

    #[test]
    fn each_cleanup_keeps_one_stable_waker() {
        let mut worker = Worker::start();
        let first = Probe::alone();
        let second = Probe::alone();
        let _tasks = (
            first.activate(&worker.context),
            second.activate(&worker.context),
        );
        assert!(worker.turn().is_pending());
        for _ in 0..4 {
            first.wake();
            second.wake();
            assert!(worker.turn().is_pending());
        }
        let first_wakers = first.wakers.borrow().clone();
        let second_wakers = second.wakers.borrow().clone();
        assert_eq!((first_wakers.len(), second_wakers.len()), (5, 5));
        assert!(
            first_wakers
                .iter()
                .all(|waker| first_wakers[0].will_wake(waker))
        );
        assert!(
            second_wakers
                .iter()
                .all(|waker| second_wakers[0].will_wake(waker))
        );
        assert!(!first_wakers[0].will_wake(&second_wakers[0]));
        assert!(!first_wakers[0].will_wake(&worker.waker));
        assert_eq!(
            (first.clones.get(), second.clones.get()),
            (1, 1),
            "a cache that checks will_wake clones each waker once"
        );

        let remote = second_wakers[0].clone();
        assert_send_sync(&remote);
        std::thread::spawn(move || remote.wake())
            .join()
            .expect("waking thread joins");
        assert!(worker.turn().is_pending());
        assert_eq!((first.polls(), second.polls()), (5, 6));
        first.finish();
        second.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    #[test]
    fn a_cleanup_activated_by_a_command_is_polled_in_the_same_pass() {
        let mut worker = Worker::start();
        let polls = Arc::new(AtomicUsize::new(0));
        worker
            .handle
            .submit({
                let polls = Arc::clone(&polls);
                move |context| {
                    let handle = context.handle().clone();
                    context
                        .register_cleanup_task(std::future::poll_fn(move |_| {
                            polls.fetch_add(1, Ordering::SeqCst);
                            if handle.is_cancelled() {
                                Poll::Ready(())
                            } else {
                                Poll::Pending
                            }
                        }))
                        .expect("cleanup reserved")
                        .activate()
                        .expect("cleanup activated")
                        .detach();
                }
            })
            .expect("command admitted");
        let delta = worker.counted_turn();
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!((delta.command_polls, delta.cleanup_polls), (1, 1));
        worker.shut_down();
    }

    #[test]
    fn a_cleanup_activated_by_a_cleanup_is_polled_once_in_the_next_pass() {
        let mut worker = Worker::start();
        let activating = Probe::alone();
        let activated = Probe::alone();
        let registration = Rc::new(RefCell::new(Some(activated.reserve(&worker.context))));
        let task = Rc::new(RefCell::new(None));
        activating.set_hook({
            let registration = Rc::clone(&registration);
            let task = Rc::clone(&task);
            move |_| {
                if let Some(registration) = registration.borrow_mut().take() {
                    *task.borrow_mut() = Some(registration.activate().expect("cleanup activates"));
                }
            }
        });
        let _activating_task = activating.activate(&worker.context);
        let wakes = worker.wakes();
        assert!(worker.turn().is_pending());
        assert_eq!((activating.polls(), activated.polls()), (1, 0));
        assert!(worker.wakes() > wakes, "the activation woke the driver");
        assert!(worker.turn().is_pending());
        assert_eq!((activating.polls(), activated.polls()), (1, 1));
        assert!(worker.turn().is_pending());
        assert_eq!(activated.polls(), 1);
        activating.finish();
        activated.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }

    #[test]
    fn a_cleanup_activated_by_a_foreign_local_task_wakes_the_driver() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        LocalSet::new().block_on(&runtime, async {
            let (handle, driver) = WorkerDriver::new(4).expect("worker starts");
            let probe = Probe::alone();
            probe.ready.set(true);
            let registration = probe.reserve(driver.context());
            let observer = registration.observe();
            let driver = tokio::task::spawn_local(driver);
            tokio::task::yield_now().await;
            let foreign = tokio::task::spawn_local(async move {
                tokio::task::yield_now().await;
                registration.activate().expect("cleanup activates").detach();
            });
            tokio::time::timeout(TIMEOUT, observer.wait())
                .await
                .expect("the driver polled the activated cleanup")
                .expect("cleanup retired");
            assert_eq!(probe.polls(), 1);
            foreign.await.expect("foreign task joins");
            assert!(handle.begin_shutdown());
            driver.await.expect("driver completes");
        });
    }

    /// A cleanup that panics in its first poll.
    struct PanicCleanup;

    impl Future for PanicCleanup {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic!("woken cleanup poll panic");
        }
    }

    /// A cleanup that finishes once its worker cancels, logging its id.
    struct CancelledCleanup {
        id: usize,
        handle: WorkerHandle,
        finished: Arc<Mutex<Vec<usize>>>,
    }

    impl Future for CancelledCleanup {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.handle.is_cancelled() {
                self.finished.lock().expect("finish log").push(self.id);
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    #[test]
    fn a_cleanup_panic_shuts_down_and_finishes_its_siblings_in_order() {
        let worker = ManagedWorker::new(4).expect("worker starts");
        let finished = Arc::new(Mutex::new(Vec::new()));
        let (started_tx, started_rx) = mpsc::channel();
        worker
            .handle()
            .submit({
                let finished = Arc::clone(&finished);
                move |context| {
                    for id in [1, 2] {
                        context
                            .register_cleanup_task(CancelledCleanup {
                                id,
                                handle: context.handle().clone(),
                                finished: Arc::clone(&finished),
                            })
                            .expect("cleanup reserved")
                            .activate()
                            .expect("cleanup activated")
                            .detach();
                    }
                    context
                        .register_cleanup_task(PanicCleanup)
                        .expect("cleanup reserved")
                        .activate()
                        .expect("cleanup activated")
                        .detach();
                    started_tx.send(()).expect("test observes the start");
                }
            })
            .expect("command admitted");
        started_rx.recv_timeout(TIMEOUT).expect("command ran");
        let handle = worker.handle();
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async { tokio::time::timeout(TIMEOUT, handle.closed()).await })
            .expect("the cleanup panic closes the worker by itself");
        let result = worker.shutdown();
        assert_eq!(handle.state(), WorkerState::Closed);
        let stats = handle.poll_stats();
        assert_eq!((stats.cleanups_registered, stats.cleanups_active), (0, 0),);
        assert!(result.is_err(), "{result:?}");
        assert_eq!(*finished.lock().expect("finish log"), [1, 2]);
    }

    #[test]
    fn shutdown_polls_every_cleanup_in_registration_order_despite_queued_wakes() {
        let mut worker = Worker::start();
        let log = Rc::new(RefCell::new(Vec::new()));
        let probes: Vec<_> = (0..4)
            .map(|id| Probe::new(id, &log).until_cancelled(&worker.handle))
            .collect();
        let mut reserved = Vec::new();
        let mut active = Vec::new();
        for (id, probe) in probes.iter().enumerate() {
            if id % 2 == 0 {
                reserved.push(probe.reserve(&worker.context));
            } else {
                active.push(probe.activate(&worker.context));
            }
        }
        assert!(worker.turn().is_pending());
        assert_eq!(*log.borrow(), [1, 3]);
        assert!(
            Arc::strong_count(&worker.wake) > 2,
            "an open worker keeps the driver waker"
        );
        probes[3].wake();
        probes[1].wake();

        log.borrow_mut().clear();
        assert!(worker.handle.begin_shutdown());
        assert!(worker.turn().is_ready());
        assert_eq!(*log.borrow(), [0, 1, 2, 3]);
        assert_eq!(worker.handle.state(), WorkerState::Closed);
        assert!(active.iter().all(WorkerCleanupTask::is_finished));
        drop(reserved);
        assert_eq!(
            Arc::strong_count(&worker.wake),
            2,
            "the closed worker released every driver waker clone"
        );
        let wakes = worker.wakes();
        for probe in &probes {
            probe.wake();
        }
        assert_eq!(worker.wakes(), wakes, "wakes after Closed are no-ops");
        assert_eq!(worker.context.queued_cleanup_wakes(), Some((0, 0)));
    }

    /// A cleanup that ignores cancellation and finishes when `receiver` does.
    struct ReceiverCleanup {
        receiver: oneshot::Receiver<()>,
        polls: Rc<Cell<usize>>,
        waker: Rc<RefCell<Option<Waker>>>,
    }

    impl Future for ReceiverCleanup {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            self.polls.set(self.polls.get() + 1);
            *self.waker.borrow_mut() = Some(context.waker().clone());
            Pin::new(&mut self.receiver).poll(context).map(|_| ())
        }
    }

    #[test]
    fn a_wake_during_closing_polls_the_cleanup_again() {
        let mut worker = Worker::start();
        let (sender, receiver) = oneshot::channel();
        let polls = Rc::new(Cell::new(0));
        let waker = Rc::new(RefCell::new(None::<Waker>));
        let task = worker
            .context
            .register_cleanup_task(ReceiverCleanup {
                receiver,
                polls: Rc::clone(&polls),
                waker: Rc::clone(&waker),
            })
            .expect("cleanup reserved")
            .activate()
            .expect("cleanup activated");
        assert!(worker.turn().is_pending());
        assert_eq!(polls.get(), 1);
        // This wake's entry is queued when shutdown discards the list.
        waker
            .borrow()
            .as_ref()
            .expect("the cleanup kept a waker")
            .wake_by_ref();

        assert!(worker.handle.begin_shutdown());
        assert!(worker.turn().is_pending());
        assert_eq!(polls.get(), 2);
        let wakes = worker.wakes();
        std::thread::spawn(move || sender.send(()).expect("receiver is alive"))
            .join()
            .expect("sending thread joins");
        assert!(
            worker.wakes() > wakes,
            "the closing wake reached the driver"
        );
        assert!(worker.turn().is_ready());
        assert_eq!(polls.get(), 3);
        assert!(task.is_finished());
    }

    #[test]
    fn abort_drops_woken_cleanups_in_registration_order() {
        let mut worker = Worker::start();
        let dropped = Rc::new(RefCell::new(Vec::new()));
        struct DropLog(usize, Rc<RefCell<Vec<usize>>>);
        impl Drop for DropLog {
            fn drop(&mut self) {
                self.1.borrow_mut().push(self.0);
            }
        }
        let mut registrations = Vec::new();
        let mut tasks = Vec::new();
        for id in 0..5 {
            let log = DropLog(id, Rc::clone(&dropped));
            let registration = worker
                .context
                .register_cleanup_task(async move {
                    let _log = log;
                    std::future::pending::<()>().await;
                })
                .expect("cleanup reserved");
            if id % 2 == 1 {
                tasks.push(registration.activate().expect("cleanup activated"));
            } else {
                registrations.push(registration);
            }
        }
        assert!(worker.turn().is_pending());
        let observers: Vec<_> = registrations
            .iter()
            .map(WorkerCleanupTaskRegistration::observe)
            .chain(tasks.iter().map(WorkerCleanupTask::observe))
            .collect();
        assert_eq!(registrations.remove(1).disarm(), Ok(true));
        let reusing_log = DropLog(5, Rc::clone(&dropped));
        let _reusing = worker
            .context
            .register_cleanup_task(async move {
                let _log = reusing_log;
            })
            .expect("cleanup reserved");
        let handle = worker.handle.clone();
        drop(worker);
        assert_eq!(*dropped.borrow(), [2, 0, 1, 3, 4, 5]);
        assert!(observers.iter().all(observed));
        assert!(tasks.iter().all(WorkerCleanupTask::is_finished));
        let stats = handle.poll_stats();
        assert_eq!((stats.cleanups_registered, stats.cleanups_active), (0, 0));
    }

    #[test]
    fn the_ready_list_stays_bounded_by_the_activated_cleanups() {
        const CLEANUPS: usize = 1000;
        let mut worker = Worker::start();
        let probes: Vec<_> = (0..CLEANUPS).map(|_| Probe::alone()).collect();
        let tasks: Vec<_> = probes
            .iter()
            .map(|probe| probe.activate(&worker.context))
            .collect();
        let (queued, _) = worker
            .context
            .queued_cleanup_wakes()
            .expect("woken polling");
        assert_eq!(queued, CLEANUPS);
        assert!(worker.turn().is_pending());
        let mut capacities = Vec::new();
        for round in 0..4 {
            for _ in 0..100 {
                for probe in &probes {
                    probe.wake();
                }
                let (queued, _) = worker
                    .context
                    .queued_cleanup_wakes()
                    .expect("woken polling");
                assert!(queued <= CLEANUPS, "round {round}: {queued} entries");
            }
            let (queued, capacity) = worker
                .context
                .queued_cleanup_wakes()
                .expect("woken polling");
            assert_eq!(queued, CLEANUPS, "round {round}");
            capacities.push(capacity);
            let delta = worker.counted_turn();
            assert_eq!(delta.ready_entries, CLEANUPS as u64, "round {round}");
            assert_eq!(delta.cleanup_polls, CLEANUPS as u64, "round {round}");
        }
        assert!(capacities[1] < 2 * CLEANUPS, "{capacities:?}");
        assert!(
            capacities[1..]
                .iter()
                .all(|&capacity| capacity == capacities[1]),
            "the list keeps its peak capacity: {capacities:?}"
        );
        assert_eq!(worker.stats().max_ready_batch, CLEANUPS as u64);
        for probe in &probes {
            probe.finish();
        }
        assert!(worker.turn().is_pending());
        assert!(tasks.iter().all(WorkerCleanupTask::is_finished));
        worker.shut_down();
    }

    /// A cleanup that finishes once a command sets `released`, which wakes
    /// nothing.
    struct UnwokenCleanup {
        released: Arc<AtomicBool>,
    }

    impl Future for UnwokenCleanup {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.released.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    #[test]
    fn an_unwoken_ready_cleanup_waits_for_the_shutdown_sweep() {
        let mut worker = Worker::start();
        let released = Arc::new(AtomicBool::new(false));
        let task = worker
            .context
            .register_cleanup_task(UnwokenCleanup {
                released: Arc::clone(&released),
            })
            .expect("cleanup reserved")
            .activate()
            .expect("cleanup activated");
        assert!(worker.turn().is_pending());
        worker
            .handle
            .submit(move |_| released.store(true, Ordering::SeqCst))
            .expect("command admitted");
        let delta = worker.counted_turn();
        assert_eq!(delta.cleanup_polls, 0);
        assert!(!task.is_finished(), "no wake, no poll");
        worker.shut_down();
        assert!(task.is_finished(), "the shutdown sweep polls it");
    }

    #[test]
    fn a_cleanup_woken_by_a_command_does_not_poll_an_idle_sibling() {
        let mut worker = Worker::start();
        let (sender, receiver) = oneshot::channel();
        let polls = Rc::new(Cell::new(0));
        let woken = worker
            .context
            .register_cleanup_task(ReceiverCleanup {
                receiver,
                polls: Rc::clone(&polls),
                waker: Rc::default(),
            })
            .expect("cleanup reserved")
            .activate()
            .expect("cleanup activated");
        let idle = Probe::alone();
        let _idle_task = idle.activate(&worker.context);
        assert!(worker.turn().is_pending());
        worker
            .handle
            .submit(move |_| sender.send(()).expect("receiver is alive"))
            .expect("command admitted");
        let delta = worker.counted_turn();
        assert!(woken.is_finished());
        assert_eq!(
            polls.get(),
            2,
            "the receiver is polled once for activation and once for its wake"
        );
        assert_eq!(delta.cleanup_polls, 1, "{delta:?}");
        assert_eq!(idle.polls(), 1);
        idle.finish();
        assert!(worker.turn().is_pending());
        worker.shut_down();
    }
}
