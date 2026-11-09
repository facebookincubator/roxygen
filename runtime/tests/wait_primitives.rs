mod wait {
    use std::cell::Cell;
    use std::future::Future;
    use std::future::poll_fn;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::pin::pin;
    use std::ptr;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use tokio::runtime::Builder;
    use tokio::runtime::Runtime;
    use tokio::task::LocalSet;
    use tokio::time;

    use crate::cancellation::CancellationRegistration;
    use crate::cancellation::CancellationSource;
    use crate::cancellation::CancellationToken;
    use crate::driver::WorkerDriver;
    use crate::host::Host;
    use crate::host::timer::WheelTimer;
    use crate::host::timer::tests::count_allocations;
    use crate::tokio_host::wheel_driving_tests::run_with_worker_wheel;
    use crate::wait::CancellableBaton;
    use crate::wait::DetachableCancellableBaton;
    use crate::wait::TimedBaton;
    use crate::wait::TimedCancellableBaton;
    use crate::wait::WaitStatus;

    fn paused_runtime() -> Runtime {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread test runtime")
    }

    /// Runs `test` under a paused clock on both hosts: a bare runtime, where
    /// the Tokio host has no wheel timer and a timed wait keeps a host sleep,
    /// and a wheel host, where a worker supervisor drives a wheel timer for
    /// this thread as a hosted worker's owner thread does.
    fn run_paused<Test, Work>(test: Test)
    where
        Test: Fn() -> Work,
        Work: Future<Output = ()>,
    {
        paused_runtime().block_on(async {
            time::pause();
            test().await;
        });
        run_on_wheel_host(test());
    }

    /// Runs `test` under a paused clock on a wheel host only.
    fn run_on_wheel_host(test: impl Future<Output = ()>) {
        LocalSet::new().block_on(&paused_runtime(), async {
            time::pause();
            run_with_worker_wheel(test).await;
        });
    }

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(17_u8);
        }
    }

    struct CountingWake(Arc<AtomicUsize>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    thread_local! {
        static WAIT_WAKER_CLONE_COUNTDOWN: Cell<usize> = const { Cell::new(0) };
        static WAIT_WAKER_DROP_ARMED: Cell<bool> = const { Cell::new(false) };
        static WAIT_WAKER_WAKES: Cell<usize> = const { Cell::new(0) };
        static WAIT_WAKER_CANCEL_ON_DROP: Cell<Option<CancellationSource>> = const { Cell::new(None) };
    }

    fn faulting_wait_waker() -> Waker {
        fn clone(_data: *const ()) -> RawWaker {
            let remaining = WAIT_WAKER_CLONE_COUNTDOWN.get();
            if remaining != 0 {
                WAIT_WAKER_CLONE_COUNTDOWN.set(remaining - 1);
                if remaining == 1 {
                    panic_any("wait waker clone");
                }
            }
            raw()
        }
        fn wake(_data: *const ()) {
            WAIT_WAKER_WAKES.set(WAIT_WAKER_WAKES.get() + 1);
        }
        fn drop_waker(_data: *const ()) {
            if let Some(source) = WAIT_WAKER_CANCEL_ON_DROP.take() {
                source.request_cancellation();
            }
            if WAIT_WAKER_DROP_ARMED.replace(false) {
                panic_any("wait waker drop");
            }
        }
        fn raw() -> RawWaker {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, drop_waker);
            RawWaker::new(ptr::null(), &VTABLE)
        }
        // SAFETY: The static vtable never dereferences its null data and owns no
        // resources. Every clone is valid independently; callbacks use only TLS.
        unsafe { Waker::from_raw(raw()) }
    }

    #[test]
    fn cancellation_wake_registration_tracks_independent_current_targets() {
        let faulting = faulting_wait_waker();
        let mut never = CancellationToken::never().wake_registration();
        let cancelled = CancellationSource::new();
        assert!(cancelled.request_cancellation());
        let mut late = cancelled.token().wake_registration();
        WAIT_WAKER_CLONE_COUNTDOWN.set(1);
        assert!(!never.update_waker(&faulting));
        assert!(late.update_waker(&faulting));
        assert_eq!(WAIT_WAKER_CLONE_COUNTDOWN.replace(0), 1);

        let source = CancellationSource::new();
        let token = source.token();
        let mut first = token.wake_registration();
        let mut removed = token.wake_registration();
        let old_wakes = Arc::new(AtomicUsize::new(0));
        let current_wakes = Arc::new(AtomicUsize::new(0));
        let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
        let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
        let equivalent = current.clone();
        assert_eq!(token.registration_count(), 0);
        for waker in [&old, &old, &current, &equivalent] {
            assert!(!first.update_waker(waker));
            assert_eq!(token.registration_count(), 1);
        }
        assert!(!removed.update_waker(&current));
        assert_eq!(token.registration_count(), 2);
        drop(removed);
        assert_eq!(token.registration_count(), 1);
        assert!(source.request_cancellation());
        assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(token.registration_count(), 0);
        assert!(first.update_waker(&equivalent));
        assert!(first.update_waker(&old));
        assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(current_wakes.load(Ordering::SeqCst), 1);

        WAIT_WAKER_WAKES.set(0);
        let source = CancellationSource::new();
        let mut registration = source.token().wake_registration();
        assert!(!registration.update_waker(&faulting));
        WAIT_WAKER_CLONE_COUNTDOWN.set(1);
        assert!(!registration.update_waker(&faulting));
        assert_eq!(WAIT_WAKER_CLONE_COUNTDOWN.replace(0), 1);
        WAIT_WAKER_CANCEL_ON_DROP.set(Some(source));
        assert!(registration.update_waker(&current));
        assert_eq!(WAIT_WAKER_WAKES.get(), 1);
        assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancellation_wake_registration_retries_after_waker_panics() {
        struct LaterPanickingWake;

        impl Wake for LaterPanickingWake {
            fn wake(self: Arc<Self>) {
                panic_any(23_u8);
            }
        }

        for clone_fault in [1, 2] {
            for retry_current in [Some(false), Some(true), None] {
                WAIT_WAKER_WAKES.set(0);
                let source = CancellationSource::new();
                let token = source.token();
                let mut registration = token.wake_registration();
                let old_wakes = Arc::new(AtomicUsize::new(0));
                let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
                let current = faulting_wait_waker();
                assert!(!registration.update_waker(&old));
                let survivor_wakes = Arc::new(AtomicUsize::new(0));
                let survivor_waker =
                    Waker::from(Arc::new(CountingWake(Arc::clone(&survivor_wakes))));
                let mut survivor = token.wake_registration();
                assert!(!survivor.update_waker(&survivor_waker));

                WAIT_WAKER_CLONE_COUNTDOWN.set(clone_fault);
                let update = catch_unwind(AssertUnwindSafe(|| registration.update_waker(&current)));
                WAIT_WAKER_CLONE_COUNTDOWN.set(0);
                let panic = update.expect_err("registry or cache clone must fail");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
                if let Some(retry_current) = retry_current {
                    assert!(!registration.update_waker(if retry_current {
                        &current
                    } else {
                        &old
                    }));
                    assert_eq!(token.registration_count(), 2);
                } else {
                    drop(registration);
                    assert_eq!(token.registration_count(), 1);
                }
                assert!(source.request_cancellation());
                assert_eq!(
                    old_wakes.load(Ordering::SeqCst),
                    usize::from(retry_current == Some(false))
                );
                assert_eq!(
                    WAIT_WAKER_WAKES.get(),
                    usize::from(retry_current == Some(true))
                );
                assert_eq!(survivor_wakes.load(Ordering::SeqCst), 1);
                assert_eq!(token.registration_count(), 0);
            }
        }

        for registry_drop in [false, true] {
            for retry_current in [false, true] {
                WAIT_WAKER_WAKES.set(0);
                let source = CancellationSource::new();
                let token = source.token();
                let mut registration = token.wake_registration();
                let old = faulting_wait_waker();
                let current_wakes = Arc::new(AtomicUsize::new(0));
                let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
                if registry_drop {
                    WAIT_WAKER_CLONE_COUNTDOWN.set(2);
                    let update = catch_unwind(AssertUnwindSafe(|| registration.update_waker(&old)));
                    WAIT_WAKER_CLONE_COUNTDOWN.set(0);
                    assert!(
                        update.is_err(),
                        "leave the listener installed without a cache"
                    );
                } else {
                    assert!(!registration.update_waker(&old));
                }
                WAIT_WAKER_DROP_ARMED.set(true);
                let update = catch_unwind(AssertUnwindSafe(|| registration.update_waker(&current)));
                WAIT_WAKER_DROP_ARMED.set(false);
                let panic =
                    update.expect_err("cache or registry replacement must reach the drop fault");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));
                assert!(!registration.update_waker(if retry_current { &current } else { &old }));
                assert!(source.request_cancellation());
                assert_eq!(WAIT_WAKER_WAKES.get(), usize::from(!retry_current));
                assert_eq!(
                    current_wakes.load(Ordering::SeqCst),
                    usize::from(retry_current)
                );
                assert_eq!(token.registration_count(), 0);
            }
        }

        WAIT_WAKER_WAKES.set(0);
        let source = CancellationSource::new();
        let token = source.token();
        let mut registration = token.wake_registration();
        let faulting = faulting_wait_waker();
        assert!(!registration.update_waker(&faulting));
        let survivor_wakes = Arc::new(AtomicUsize::new(0));
        let survivor_waker = Waker::from(Arc::new(CountingWake(Arc::clone(&survivor_wakes))));
        let mut survivor = token.wake_registration();
        assert!(!survivor.update_waker(&survivor_waker));
        WAIT_WAKER_DROP_ARMED.set(true);
        let retirement = catch_unwind(AssertUnwindSafe(|| drop(registration)));
        WAIT_WAKER_DROP_ARMED.set(false);
        let panic = retirement.expect_err("unregister must reach the drop fault");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));
        assert_eq!(token.registration_count(), 1);
        assert!(source.request_cancellation());
        assert_eq!(WAIT_WAKER_WAKES.get(), 0);
        assert_eq!(survivor_wakes.load(Ordering::SeqCst), 1);

        let source = CancellationSource::new();
        let mut registration = source.token().wake_registration();
        assert!(!registration.update_waker(&faulting));
        assert!(source.request_cancellation());
        WAIT_WAKER_DROP_ARMED.set(true);
        let cleanup = catch_unwind(AssertUnwindSafe(|| registration.update_waker(&faulting)));
        WAIT_WAKER_DROP_ARMED.set(false);
        let panic = cleanup.expect_err("cancelled cleanup must reach the cache drop fault");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));
        WAIT_WAKER_CLONE_COUNTDOWN.set(1);
        assert!(registration.update_waker(&faulting));
        assert_eq!(WAIT_WAKER_CLONE_COUNTDOWN.replace(0), 1);
        assert_eq!(source.token().registration_count(), 0);
        assert_eq!(WAIT_WAKER_WAKES.get(), 1);

        let source = CancellationSource::new();
        let token = source.token();
        let mut first = token.wake_registration();
        let mut second = token.wake_registration();
        let mut last = token.wake_registration();
        assert!(!first.update_waker(&Waker::from(Arc::new(PanickingWake))));
        assert!(!second.update_waker(&Waker::from(Arc::new(LaterPanickingWake))));
        let wakes = Arc::new(AtomicUsize::new(0));
        assert!(!last.update_waker(&Waker::from(Arc::new(CountingWake(Arc::clone(&wakes))))));
        let panic = catch_unwind(AssertUnwindSafe(|| source.request_cancellation()))
            .expect_err("the first wake panic is rethrown after all listeners");
        assert_eq!(panic.downcast_ref::<u8>(), Some(&17));
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_eq!(token.registration_count(), 0);
        assert!(first.update_waker(Waker::noop()));
        assert!(second.update_waker(Waker::noop()));
        assert!(last.update_waker(Waker::noop()));
        assert!(!source.request_cancellation());
    }

    #[test]
    fn cancellation_wake_registration_allows_late_wake_after_drop() {
        use std::sync::Mutex;
        use std::sync::mpsc;

        struct BlockingWake {
            entered: mpsc::SyncSender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }

        impl Wake for BlockingWake {
            fn wake(self: Arc<Self>) {
                let _ = self.entered.try_send(());
                let _ = self.release.lock().unwrap().recv();
            }
        }

        struct ReleaseOnDrop(mpsc::SyncSender<()>);

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                let _ = self.0.try_send(());
            }
        }

        for retire_by_drop in [false, true] {
            let source = CancellationSource::new();
            let token = source.token();
            let mut first = token.wake_registration();
            let mut second = token.wake_registration();
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(1);
            let first_waker = Waker::from(Arc::new(BlockingWake {
                entered: entered_tx,
                release: Mutex::new(release_rx),
            }));
            let old_wakes = Arc::new(AtomicUsize::new(0));
            let current_wakes = Arc::new(AtomicUsize::new(0));
            let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
            let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
            assert!(!first.update_waker(&first_waker));
            assert!(!second.update_waker(&old));
            assert_eq!(token.registration_count(), 2);

            std::thread::scope(|scope| {
                let requester = scope.spawn(|| source.request_cancellation());
                let release = ReleaseOnDrop(release_tx);
                entered_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("fanout extracted both listeners");
                assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
                assert_eq!(token.registration_count(), 0);
                if retire_by_drop {
                    drop(second);
                } else {
                    assert!(second.update_waker(&current));
                    assert!(second.update_waker(&current));
                }
                let mut late = token.wake_registration();
                assert!(late.update_waker(&current));
                assert_eq!(current_wakes.load(Ordering::SeqCst), 0);
                assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
                drop(release);
                assert!(requester.join().expect("foreign cancellation joins"));
            });
            assert_eq!(old_wakes.load(Ordering::SeqCst), 1);
            assert_eq!(current_wakes.load(Ordering::SeqCst), 0);
            assert_eq!(token.registration_count(), 0);
            assert!(first.update_waker(Waker::noop()));
        }
    }

    async fn wait_for_pending_commit<F>(mut future: Pin<&mut F>, is_committed: impl Fn() -> bool)
    where
        F: Future,
    {
        poll_fn(|context| {
            assert!(
                future.as_mut().poll(context).is_pending(),
                "timeout callback must commit before the waiter resumes"
            );
            if is_committed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    #[test]
    fn cancellable_signal() {
        run_paused(|| async {
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            baton.signal();
            assert_eq!(wait.await, WaitStatus::Signalled);
        });
    }

    #[test]
    fn signal_before_wait() {
        run_paused(|| async {
            let baton = CancellableBaton::new();
            baton.signal();
            assert!(baton.ready());

            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
            baton.reset();
            assert!(!baton.ready());
        });
    }

    #[test]
    fn cross_thread_cancel_resumes_on_owner() {
        run_paused(|| async {
            let owner = std::thread::current().id();
            let source = CancellationSource::new();
            let token = source.token();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(token));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            assert!(baton.ready());
            assert_eq!(wait.await, WaitStatus::Cancelled);
            assert_eq!(std::thread::current().id(), owner);
        });
    }

    #[test]
    fn cancellable_wait_updates_cancellation_waker() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(source.token()));
            let old_wakes = Arc::new(AtomicUsize::new(0));
            let current_wakes = Arc::new(AtomicUsize::new(0));
            let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
            let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
            let equivalent = current.clone();
            for waker in [&old, &old, &current, &equivalent] {
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(waker))
                        .is_pending()
                );
            }

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));
            assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
            assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
            assert!(baton.ready());
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&equivalent)),
                Poll::Ready(WaitStatus::Cancelled)
            );
        });
    }

    #[test]
    fn cancellable_wait_retries_panicking_waker_update() {
        run_paused(|| async {
            for clone_fault in [1, 2] {
                WAIT_WAKER_WAKES.set(0);
                let source = CancellationSource::new();
                let baton = CancellableBaton::new();
                let mut wait = Box::pin(baton.wait(source.token()));
                let old_wakes = Arc::new(AtomicUsize::new(0));
                let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
                let current = faulting_wait_waker();
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&old))
                        .is_pending()
                );

                // The second clone fails after the baton has adopted the new waker.
                WAIT_WAKER_CLONE_COUNTDOWN.set(clone_fault);
                let update = catch_unwind(AssertUnwindSafe(|| {
                    wait.as_mut().poll(&mut Context::from_waker(&current))
                }));
                WAIT_WAKER_CLONE_COUNTDOWN.set(0);
                let panic = update.expect_err("waker replacement must reach the clone fault");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&current))
                        .is_pending()
                );
                assert!(source.request_cancellation());
                assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
                assert_eq!(WAIT_WAKER_WAKES.get(), 1);
                assert_eq!(
                    wait.as_mut().poll(&mut Context::from_waker(&current)),
                    Poll::Ready(WaitStatus::Cancelled)
                );
            }

            WAIT_WAKER_WAKES.set(0);
            let source = CancellationSource::new();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(source.token()));
            let old = faulting_wait_waker();
            let current_wakes = Arc::new(AtomicUsize::new(0));
            let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&old))
                    .is_pending()
            );
            WAIT_WAKER_DROP_ARMED.set(true);
            let update = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&current))
            }));
            WAIT_WAKER_DROP_ARMED.set(false);
            let panic = update.expect_err("waker replacement must reach the drop fault");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&current))
                    .is_pending()
            );
            assert!(source.request_cancellation());
            assert_eq!(WAIT_WAKER_WAKES.get(), 0);
            assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&current)),
                Poll::Ready(WaitStatus::Cancelled)
            );
        });
    }

    #[test]
    fn cancellable_wait_retries_panicking_unregister() {
        run_paused(|| async {
            WAIT_WAKER_WAKES.set(0);
            let source = CancellationSource::new();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(source.token()));
            let waker = faulting_wait_waker();
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            baton.signal();
            assert_eq!(WAIT_WAKER_WAKES.get(), 1);

            WAIT_WAKER_DROP_ARMED.set(true);
            let finish = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&waker))
            }));
            WAIT_WAKER_DROP_ARMED.set(false);
            let panic = finish.expect_err("unregister must reach the drop fault");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));

            // Teardown removed the cancellation entry, even though the baton waker stayed.
            WAIT_WAKER_CLONE_COUNTDOWN.set(1);
            let retry = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&waker))
            }));
            WAIT_WAKER_CLONE_COUNTDOWN.set(0);
            let panic = retry.expect_err("retained retry must resynchronize cancellation");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Ready(WaitStatus::Signalled)
            );
            drop(wait);
            assert!(source.request_cancellation());
            assert_eq!(WAIT_WAKER_WAKES.get(), 1);

            baton.reset();
            let mut next = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(next.as_mut()).is_pending());
            baton.signal();
            assert_eq!(next.await, WaitStatus::Signalled);
        });
    }

    #[test]
    fn cancellation_preserves_wake_order_after_listener_removal_and_reuse() {
        struct OrderedWake {
            ordinal: usize,
            calls: Arc<std::sync::Mutex<Vec<usize>>>,
        }

        impl Wake for OrderedWake {
            fn wake(self: Arc<Self>) {
                self.calls.lock().unwrap().push(self.ordinal);
            }
        }

        let source = CancellationSource::new();
        let first_baton = CancellableBaton::new();
        let second_baton = CancellableBaton::new();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let wakers: [Waker; 4] = std::array::from_fn(|ordinal| {
            Waker::from(Arc::new(OrderedWake {
                ordinal,
                calls: Arc::clone(&calls),
            }))
        });
        let mut removed = Box::pin(first_baton.wait(source.token()));
        assert!(
            removed
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[0]))
                .is_pending()
        );
        drop(removed);

        let mut earlier = Box::pin(first_baton.wait(source.token()));
        let mut survivor = Box::pin(second_baton.wait(source.token()));
        for (wait, waker) in [(&mut earlier, &wakers[0]), (&mut survivor, &wakers[1])] {
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_pending()
            );
        }
        drop(earlier);
        let mut replacement = Box::pin(first_baton.wait(source.token()));
        for (wait, waker) in [(&mut replacement, &wakers[2]), (&mut survivor, &wakers[3])] {
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_pending()
            );
        }

        assert!(source.request_cancellation());
        assert_eq!(calls.lock().unwrap().as_slice(), &[3, 2]);
        assert!(!source.request_cancellation());
        for wait in [&mut survivor, &mut replacement] {
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(WaitStatus::Cancelled)
            );
        }
        assert_eq!(calls.lock().unwrap().as_slice(), &[3, 2]);
    }

    #[test]
    fn cancellation_preserves_other_listener_after_registration_clone_panic() {
        WAIT_WAKER_WAKES.set(0);
        let source = CancellationSource::new();
        let first_baton = CancellableBaton::new();
        let second_baton = CancellableBaton::new();
        let mut first_wait = Box::pin(first_baton.wait(source.token()));
        let mut second_wait = Box::pin(second_baton.wait(source.token()));
        let first_wakes = Arc::new(AtomicUsize::new(0));
        let first_waker = Waker::from(Arc::new(CountingWake(Arc::clone(&first_wakes))));
        let second_waker = faulting_wait_waker();
        assert!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );

        WAIT_WAKER_CLONE_COUNTDOWN.set(2);
        let registration = catch_unwind(AssertUnwindSafe(|| {
            second_wait
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
        }));
        WAIT_WAKER_CLONE_COUNTDOWN.set(0);
        let panic = registration.expect_err("second listener clone must fail");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
        assert_eq!(first_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(WAIT_WAKER_WAKES.get(), 0);
        assert!(
            second_wait
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );

        assert!(source.request_cancellation());
        assert_eq!(first_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(WAIT_WAKER_WAKES.get(), 1);
        assert!(!source.request_cancellation());
        for wait in [&mut first_wait, &mut second_wait] {
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(WaitStatus::Cancelled)
            );
        }
    }

    #[test]
    fn repeated_cancellation_and_late_wait_finish_while_first_wake_is_blocked() {
        use std::sync::Mutex;
        use std::sync::mpsc;

        struct BlockingWake {
            count: Arc<AtomicUsize>,
            entered: mpsc::SyncSender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }

        impl Wake for BlockingWake {
            fn wake(self: Arc<Self>) {
                self.count.fetch_add(1, Ordering::SeqCst);
                let _ = self.entered.try_send(());
                let _ = self.release.lock().unwrap().recv();
            }
        }

        struct ReleaseOnDrop(mpsc::SyncSender<()>);

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                let _ = self.0.try_send(());
            }
        }

        let source = CancellationSource::new();
        let token = source.token();
        let first_baton = CancellableBaton::new();
        let second_baton = CancellableBaton::new();
        let mut first_wait = Box::pin(first_baton.wait(token.clone()));
        let mut second_wait = Box::pin(second_baton.wait(token.clone()));
        let first_count = Arc::new(AtomicUsize::new(0));
        let second_count = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let first_waker = Waker::from(Arc::new(BlockingWake {
            count: Arc::clone(&first_count),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }));
        let second_waker = Waker::from(Arc::new(CountingWake(Arc::clone(&second_count))));
        assert!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            second_wait
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );
        assert_eq!(token.registration_count(), 2);

        std::thread::scope(|scope| {
            let first = scope.spawn(|| source.request_cancellation());
            let release = ReleaseOnDrop(release_tx);
            let timeout = Duration::from_secs(5);
            entered_rx
                .recv_timeout(timeout)
                .expect("first wake entered");

            let (repeat_tx, repeat_rx) = mpsc::sync_channel(1);
            let repeat_source = &source;
            let repeated = scope.spawn(move || {
                let _ = repeat_tx.send(repeat_source.request_cancellation());
            });
            assert!(
                !repeat_rx
                    .recv_timeout(timeout)
                    .expect("repeat finishes while first wake remains blocked")
            );
            assert_eq!(first_count.load(Ordering::SeqCst), 1);
            assert_eq!(second_count.load(Ordering::SeqCst), 0);

            let late_baton = CancellableBaton::new();
            let mut late_wait = Box::pin(late_baton.wait(token.clone()));
            assert_eq!(
                late_wait
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(WaitStatus::Cancelled),
            );
            assert_eq!(token.registration_count(), 0);

            drop(release);
            assert!(first.join().expect("first cancellation requester"));
            repeated.join().expect("repeated cancellation requester");
        });

        assert_eq!(first_count.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.load(Ordering::SeqCst), 1);
        assert_eq!(token.registration_count(), 0);
        for wait in [&mut first_wait, &mut second_wait] {
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(WaitStatus::Cancelled),
            );
        }
    }

    #[test]
    fn cancellation_wakes_every_waiter_before_rethrowing_first_panic() {
        let source = CancellationSource::new();
        let token = source.token();
        let first_baton = CancellableBaton::new();
        let second_baton = CancellableBaton::new();
        let mut first_wait = Box::pin(first_baton.wait(token.clone()));
        let mut second_wait = Box::pin(second_baton.wait(token.clone()));

        let panicking_waker = Waker::from(Arc::new(PanickingWake));
        let mut panicking_context = Context::from_waker(&panicking_waker);
        assert!(
            first_wait
                .as_mut()
                .poll(&mut panicking_context)
                .is_pending()
        );

        let wake_count = Arc::new(AtomicUsize::new(0));
        let counting_waker = Waker::from(Arc::new(CountingWake(Arc::clone(&wake_count))));
        let mut counting_context = Context::from_waker(&counting_waker);
        assert!(
            second_wait
                .as_mut()
                .poll(&mut counting_context)
                .is_pending()
        );

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            source.request_cancellation()
        }))
        .expect_err("the first waker panic is rethrown after all wakes");
        assert_eq!(panic.downcast_ref::<u8>(), Some(&17));
        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        assert!(token.is_cancellation_requested());
        assert_eq!(token.registration_count(), 0);
        assert!(!source.request_cancellation());
        assert_eq!(
            first_wait.as_mut().poll(&mut panicking_context),
            Poll::Ready(WaitStatus::Cancelled),
        );
        assert_eq!(
            second_wait.as_mut().poll(&mut counting_context),
            Poll::Ready(WaitStatus::Cancelled),
        );
    }

    #[test]
    fn cancellation_secondary_panic_preserves_first_and_wakes_all() {
        struct Marker;

        struct Observations {
            calls: std::sync::Mutex<Vec<usize>>,
            unlocked: AtomicUsize,
            secondary_drops: Arc<AtomicUsize>,
            nested_drops: Arc<AtomicUsize>,
        }

        struct SecondaryPayload(Arc<Observations>);

        struct NestedPayload(Arc<AtomicUsize>);

        impl Drop for SecondaryPayload {
            fn drop(&mut self) {
                self.0.secondary_drops.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(NestedPayload(Arc::clone(&self.0.nested_drops)));
            }
        }

        impl Drop for NestedPayload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("nested disposal panic");
            }
        }

        struct FanoutWake {
            ordinal: usize,
            token: CancellationToken,
            first: Arc<Marker>,
            observations: Arc<Observations>,
        }

        impl Wake for FanoutWake {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.observations.calls.lock().unwrap().push(self.ordinal);
                if self.token.is_cancellation_requested() && self.token.registration_count() == 0 {
                    self.observations.unlocked.fetch_add(1, Ordering::SeqCst);
                }
                match self.ordinal {
                    0 => std::panic::panic_any(Arc::clone(&self.first)),
                    1 => std::panic::panic_any(SecondaryPayload(Arc::clone(&self.observations))),
                    _ => {}
                }
            }
        }

        let source = CancellationSource::new();
        let token = source.token();
        let first = Arc::new(Marker);
        let observations = Arc::new(Observations {
            calls: std::sync::Mutex::new(Vec::new()),
            unlocked: AtomicUsize::new(0),
            secondary_drops: Arc::new(AtomicUsize::new(0)),
            nested_drops: Arc::new(AtomicUsize::new(0)),
        });
        let batons = [
            CancellableBaton::new(),
            CancellableBaton::new(),
            CancellableBaton::new(),
        ];
        let wakers = (0..3)
            .map(|ordinal| {
                Waker::from(Arc::new(FanoutWake {
                    ordinal,
                    token: token.clone(),
                    first: Arc::clone(&first),
                    observations: Arc::clone(&observations),
                }))
            })
            .collect::<Vec<_>>();
        let mut waits = batons
            .iter()
            .map(|baton| Box::pin(baton.wait(token.clone())))
            .collect::<Vec<_>>();
        for (wait, waker) in waits.iter_mut().zip(&wakers) {
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_pending()
            );
        }
        assert_eq!(token.registration_count(), 3);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            source.request_cancellation()
        }));
        let original = outcome
            .as_ref()
            .err()
            .and_then(|payload| payload.downcast_ref::<Arc<Marker>>())
            .is_some_and(|marker| Arc::ptr_eq(marker, &first));
        if let Err(payload) = outcome
            && let Err(secondary) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
        {
            crate::panic::discard_panic(secondary);
        }
        let repeated = source.request_cancellation();
        let statuses = waits
            .iter_mut()
            .map(|wait| wait.as_mut().poll(&mut Context::from_waker(Waker::noop())))
            .collect::<Vec<_>>();
        drop(waits);
        drop(batons);
        assert!(
            original,
            "the first typed panic must survive secondary disposal"
        );
        assert_eq!(observations.calls.lock().unwrap().as_slice(), &[0, 1, 2]);
        assert_eq!(observations.unlocked.load(Ordering::SeqCst), 3);
        assert_eq!(observations.secondary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(observations.nested_drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&observations.nested_drops), 1);
        assert!(token.is_cancellation_requested());
        assert_eq!(token.registration_count(), 0);
        assert!(!repeated);
        assert_eq!(statuses, vec![Poll::Ready(WaitStatus::Cancelled); 3]);
    }

    #[test]
    fn timeout_result() {
        run_paused(|| async {
            let baton = TimedCancellableBaton::new(Duration::from_secs(2));
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            time::advance(Duration::from_secs(2)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(wait.await, WaitStatus::TimedOut);
        });
    }

    #[test]
    fn timeout_result_is_per_wait_while_post_remains_ready() {
        run_paused(|| async {
            let baton = CancellableBaton::new();
            let mut first_wait =
                Box::pin(baton.timed_wait(Duration::from_secs(2), CancellationToken::never()));
            assert!(futures::poll!(first_wait.as_mut()).is_pending());

            time::advance(Duration::from_secs(2)).await;
            assert!(futures::poll!(first_wait.as_mut()).is_pending());
            assert_eq!(first_wait.await, WaitStatus::TimedOut);
            assert!(baton.ready());

            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
            assert!(baton.ready());
            baton.reset();
            assert!(!baton.ready());
        });
    }

    #[test]
    fn cancellation_result_is_per_wait_while_post_remains_ready() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = CancellableBaton::new();
            let mut first_wait = Box::pin(baton.wait(source.token()));
            assert!(futures::poll!(first_wait.as_mut()).is_pending());

            assert!(source.request_cancellation());
            assert_eq!(first_wait.await, WaitStatus::Cancelled);
            assert!(baton.ready());

            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
            baton.reset();
            assert!(!baton.ready());
        });
    }

    #[test]
    fn cross_thread_cancel_posts_readiness_before_owner_repoll() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let token = source.token();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(token));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            assert!(baton.ready());
            assert_eq!(wait.await, WaitStatus::Cancelled);
        });
    }

    #[test]
    fn cancellable_signal_then_cancel_before_repoll_prefers_cancellation() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let token = source.token();
            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(token.clone()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(token.registration_count(), 1);

            baton.signal();
            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            assert!(baton.ready());
            assert_eq!(wait.await, WaitStatus::Cancelled);
            assert_eq!(token.registration_count(), 0);
            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
        });
    }

    #[test]
    fn detach_disarms_timer_without_completing_wait() {
        run_paused(|| async {
            let baton = DetachableCancellableBaton::new();
            let mut wait =
                Box::pin(baton.timed_wait(Duration::from_secs(1), CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(baton.timeout_is_armed_for_test());

            baton.detach();
            assert!(!baton.timeout_is_armed_for_test());
            time::advance(Duration::from_secs(10)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());

            baton.signal();
            assert_eq!(wait.await, WaitStatus::Signalled);
        });
    }

    #[test]
    fn signal_before_timeout() {
        run_paused(|| async {
            let baton = DetachableCancellableBaton::new();
            let mut wait =
                Box::pin(baton.timed_wait(Duration::from_secs(5), CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(baton.timeout_is_armed_for_test());

            baton.signal();
            assert!(baton.timeout_is_armed_for_test());
            assert_eq!(wait.await, WaitStatus::Signalled);
            assert!(!baton.timeout_is_armed_for_test());
        });
    }

    #[cfg(feature = "observability")]
    #[test]
    #[cfg_attr(
        not(fbcode_build),
        ignore = "tokio emits `runtime.resource` spans only when built with its `tracing` feature and `--cfg tokio_unstable`"
    )]
    fn sleep_tracing_preserves_first_poll_and_paused_deadline() {
        use std::collections::BTreeMap;
        use std::sync::Mutex;

        #[derive(Default)]
        struct Fields(BTreeMap<&'static str, String>);

        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name(), format!("{value:?}"));
            }
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "duration" {
                    self.0.insert(field.name(), "<u64>".to_owned());
                } else {
                    self.record_debug(field, &value);
                }
            }
        }

        struct SleepSubscriber {
            state_updates: bool,
            actions: Arc<Mutex<Vec<String>>>,
        }

        impl tracing::Subscriber for SleepSubscriber {
            fn register_callsite(
                &self,
                _metadata: &'static tracing::Metadata<'static>,
            ) -> tracing::subscriber::Interest {
                tracing::subscriber::Interest::sometimes()
            }
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                match metadata.target() {
                    "tokio::time::sleep" | "runtime::resource::poll_op" => true,
                    "runtime::resource::state_update" => self.state_updates,
                    _ => false,
                }
            }
            fn new_span(&self, attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                assert_eq!(attributes.metadata().target(), "tokio::time::sleep");
                assert_eq!(attributes.metadata().level(), &tracing::Level::TRACE);
                let mut fields = Fields::default();
                attributes.record(&mut fields);
                let id = match attributes.metadata().name() {
                    "runtime.resource" => {
                        assert!(attributes.is_root());
                        assert_eq!(fields.0["concrete_type"], "\"Sleep\"");
                        assert_eq!(fields.0["kind"], "\"timer\"");
                        assert_eq!(
                            fields.0.keys().copied().collect::<Vec<_>>(),
                            ["concrete_type", "kind", "loc.col", "loc.file", "loc.line"],
                        );
                        1
                    }
                    "runtime.resource.async_op" => {
                        assert_eq!(attributes.parent(), Some(&tracing::span::Id::from_u64(1)));
                        assert_eq!(
                            fields.0,
                            BTreeMap::from([("source", "\"Sleep::new_timeout\"".to_owned())]),
                        );
                        2
                    }
                    "runtime.resource.async_op.poll" => {
                        assert_eq!(attributes.parent(), Some(&tracing::span::Id::from_u64(2)));
                        assert!(fields.0.is_empty());
                        3
                    }
                    name => panic!("unexpected Sleep span {name}"),
                };
                self.actions.lock().unwrap().push(format!("new{id}"));
                tracing::span::Id::from_u64(id)
            }
            fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }
            fn event(&self, event: &tracing::Event<'_>) {
                assert_eq!(event.metadata().level(), &tracing::Level::TRACE);
                let mut fields = Fields::default();
                event.record(&mut fields);
                let action = match event.metadata().target() {
                    "runtime::resource::poll_op" => {
                        let ready = fields.0.remove("is_ready").expect("poll readiness");
                        assert!(ready == "true" || ready == "false");
                        assert_eq!(
                            fields.0,
                            BTreeMap::from([("op_name", "\"poll_elapsed\"".to_owned())]),
                        );
                        format!("poll_{ready}")
                    }
                    "runtime::resource::state_update" => {
                        assert!(self.state_updates);
                        assert_eq!(
                            fields.0,
                            BTreeMap::from([
                                ("duration", "<u64>".to_owned()),
                                ("duration.op", "\"override\"".to_owned()),
                                ("duration.unit", "\"ms\"".to_owned()),
                            ]),
                        );
                        "state_update".to_owned()
                    }
                    target => panic!("unexpected Sleep event {target}"),
                };
                self.actions.lock().unwrap().push(action);
            }
            fn enter(&self, span: &tracing::span::Id) {
                self.actions
                    .lock()
                    .unwrap()
                    .push(format!("enter{}", span.into_u64()));
            }
            fn exit(&self, span: &tracing::span::Id) {
                self.actions
                    .lock()
                    .unwrap()
                    .push(format!("exit{}", span.into_u64()));
            }
        }

        for state_updates in [true, false] {
            run_paused(|| async {
                let actions = Arc::new(Mutex::new(Vec::new()));
                let _subscriber = tracing::subscriber::set_default(SleepSubscriber {
                    state_updates,
                    actions: Arc::clone(&actions),
                });
                let start = time::Instant::now();
                let deadline = start + Duration::from_millis(7);
                let mut sleep = Box::pin(time::sleep_until(deadline));
                assert_eq!(sleep.deadline(), deadline);
                assert!(futures::poll!(sleep.as_mut()).is_pending());
                assert!(!sleep.is_elapsed());

                let mut expected = vec![
                    "new1",
                    "new2",
                    "new3",
                    "enter1",
                    "enter2",
                    "enter3",
                    "poll_true",
                ];
                if state_updates {
                    expected.push("state_update");
                }
                expected.extend(["poll_false", "exit3", "exit2", "exit1"]);
                assert_eq!(
                    actions
                        .lock()
                        .unwrap()
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    expected,
                );

                time::advance(Duration::from_millis(6)).await;
                assert!(futures::poll!(sleep.as_mut()).is_pending());
                assert_eq!(sleep.deadline(), deadline);
                sleep.as_mut().await;
                assert!(sleep.is_elapsed());
                assert_eq!(sleep.deadline(), deadline);
                assert!(
                    (Duration::from_millis(7)..=Duration::from_millis(8))
                        .contains(&start.elapsed())
                );
                drop(sleep);
                assert_eq!(
                    actions
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|action| action.as_str() == "state_update")
                        .count(),
                    usize::from(state_updates),
                );
            });
        }
    }

    #[test]
    fn timed_wait_updates_cancellation_waker() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(source.token()));
            let old_wakes = Arc::new(AtomicUsize::new(0));
            let current_wakes = Arc::new(AtomicUsize::new(0));
            let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
            let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
            let equivalent = current.clone();
            for waker in [&old, &old, &current, &equivalent] {
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(waker))
                        .is_pending()
                );
            }

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));
            assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
            assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
            assert_eq!(baton.status(), WaitStatus::NotReady);
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&equivalent)),
                Poll::Ready(WaitStatus::Cancelled)
            );
        });
    }

    #[test]
    fn timed_wait_retries_panicking_waker_update() {
        run_paused(|| async {
            for clone_fault in [1, 2] {
                WAIT_WAKER_WAKES.set(0);
                let source = CancellationSource::new();
                let baton = TimedBaton::new(None);
                let mut wait = Box::pin(baton.wait(source.token()));
                let old_wakes = Arc::new(AtomicUsize::new(0));
                let old = Waker::from(Arc::new(CountingWake(Arc::clone(&old_wakes))));
                let current = faulting_wait_waker();
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&old))
                        .is_pending()
                );

                // The second clone fails after the baton has adopted the new waker.
                WAIT_WAKER_CLONE_COUNTDOWN.set(clone_fault);
                let update = catch_unwind(AssertUnwindSafe(|| {
                    wait.as_mut().poll(&mut Context::from_waker(&current))
                }));
                WAIT_WAKER_CLONE_COUNTDOWN.set(0);
                let panic = update.expect_err("waker replacement must reach the clone fault");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&current))
                        .is_pending()
                );
                assert!(source.request_cancellation());
                assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
                assert_eq!(WAIT_WAKER_WAKES.get(), 1);
                assert_eq!(
                    wait.as_mut().poll(&mut Context::from_waker(&current)),
                    Poll::Ready(WaitStatus::Cancelled)
                );
            }

            WAIT_WAKER_WAKES.set(0);
            let source = CancellationSource::new();
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(source.token()));
            let old = faulting_wait_waker();
            let current_wakes = Arc::new(AtomicUsize::new(0));
            let current = Waker::from(Arc::new(CountingWake(Arc::clone(&current_wakes))));
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&old))
                    .is_pending()
            );
            WAIT_WAKER_DROP_ARMED.set(true);
            let update = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&current))
            }));
            WAIT_WAKER_DROP_ARMED.set(false);
            let panic = update.expect_err("waker replacement must reach the drop fault");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&current))
                    .is_pending()
            );
            assert!(source.request_cancellation());
            assert_eq!(WAIT_WAKER_WAKES.get(), 0);
            assert_eq!(current_wakes.load(Ordering::SeqCst), 1);
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&current)),
                Poll::Ready(WaitStatus::Cancelled)
            );
        });
    }

    #[test]
    fn timed_wait_retries_panicking_unregister() {
        run_paused(|| async {
            WAIT_WAKER_WAKES.set(0);
            let source = CancellationSource::new();
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(source.token()));
            let waker = faulting_wait_waker();
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            baton.signal();
            assert_eq!(WAIT_WAKER_WAKES.get(), 1);

            WAIT_WAKER_DROP_ARMED.set(true);
            let finish = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&waker))
            }));
            WAIT_WAKER_DROP_ARMED.set(false);
            let panic = finish.expect_err("unregister must reach the drop fault");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker drop"));

            // Teardown removed the cancellation entry, even though the baton waker stayed.
            WAIT_WAKER_CLONE_COUNTDOWN.set(1);
            let retry = catch_unwind(AssertUnwindSafe(|| {
                wait.as_mut().poll(&mut Context::from_waker(&waker))
            }));
            WAIT_WAKER_CLONE_COUNTDOWN.set(0);
            let panic = retry.expect_err("retained retry must resynchronize cancellation");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"wait waker clone"));
            assert_eq!(
                wait.as_mut().poll(&mut Context::from_waker(&waker)),
                Poll::Ready(WaitStatus::Signalled)
            );
            drop(wait);
            assert!(source.request_cancellation());
            assert_eq!(WAIT_WAKER_WAKES.get(), 1);

            baton.reset();
            let mut next = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(next.as_mut()).is_pending());
            baton.signal();
            assert_eq!(next.await, WaitStatus::Signalled);
        });
    }

    #[test]
    fn timed_timeout() {
        run_paused(|| async {
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(baton.timeout_is_armed());

            time::advance(Duration::from_secs(1)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(wait.await, WaitStatus::TimedOut);
            assert_eq!(baton.status(), WaitStatus::TimedOut);

            baton.signal();
            assert_eq!(baton.status(), WaitStatus::TimedOut);
        });
    }

    #[test]
    fn timed_deadline_can_change() {
        run_paused(|| async {
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(!baton.timeout_is_armed());

            baton.set_timeout(Some(Duration::from_secs(3)));
            assert!(baton.timeout_is_armed());
            assert!(futures::poll!(wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(3)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(wait.await, WaitStatus::TimedOut);
        });
    }

    #[test]
    fn timed_without_deadline() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(source.token()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(!baton.timeout_is_armed());

            time::advance(Duration::from_secs(60)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(source.request_cancellation());
            assert_eq!(wait.await, WaitStatus::Cancelled);
            assert_eq!(baton.status(), WaitStatus::Cancelled);
        });
    }

    #[test]
    fn timed_disable_deadline() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut wait = Box::pin(baton.wait(source.token()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(baton.timeout_is_armed());

            baton.set_timeout(None);
            assert!(!baton.timeout_is_armed());
            time::advance(Duration::from_secs(10)).await;
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(source.request_cancellation());
            assert_eq!(wait.await, WaitStatus::Cancelled);
        });
    }

    #[test]
    fn timed_signal() {
        run_paused(|| async {
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            baton.signal();
            assert_eq!(wait.await, WaitStatus::Signalled);
            assert_eq!(baton.status(), WaitStatus::Signalled);
            assert!(!baton.timeout_is_armed());
        });
    }

    #[test]
    fn timed_cancel() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut wait = Box::pin(baton.wait(source.token()));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            assert!(source.request_cancellation());
            assert_eq!(wait.await, WaitStatus::Cancelled);
            assert_eq!(baton.status(), WaitStatus::Cancelled);

            baton.reset();
            assert_eq!(baton.status(), WaitStatus::NotReady);
            let second_source = CancellationSource::new();
            let mut second_wait = Box::pin(baton.wait(second_source.token()));
            assert!(futures::poll!(second_wait.as_mut()).is_pending());
            assert!(second_source.request_cancellation());
            assert_eq!(second_wait.await, WaitStatus::Cancelled);
            baton.signal();
            assert_eq!(baton.status(), WaitStatus::Cancelled);
        });
    }

    #[test]
    fn signal_is_sticky() {
        run_paused(|| async {
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            baton.signal();
            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
            assert_eq!(baton.status(), WaitStatus::Signalled);
        });
    }

    #[test]
    fn cross_thread_cancel_marshals_to_owner() {
        run_paused(|| async {
            let owner = std::thread::current().id();
            let source = CancellationSource::new();
            let token = source.token();
            let baton = TimedBaton::new(Some(Duration::from_secs(5)));
            let mut wait = Box::pin(baton.wait(token));
            assert!(futures::poll!(wait.as_mut()).is_pending());

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            assert_eq!(wait.await, WaitStatus::Cancelled);
            assert_eq!(baton.status(), WaitStatus::Cancelled);
            assert_eq!(std::thread::current().id(), owner);
        });
    }

    #[test]
    fn drop_wakes_waiter_as_cancelled() {
        run_paused(|| async {
            let baton = TimedBaton::new(Some(Duration::from_secs(10)));
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert!(baton.timeout_is_armed());

            drop(baton);
            assert_eq!(wait.await, WaitStatus::Cancelled);

            let baton = CancellableBaton::new();
            let mut wait = Box::pin(baton.wait(CancellationToken::never()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            drop(baton);
            assert_eq!(wait.await, WaitStatus::Cancelled);
        });
    }

    #[test]
    fn signal_vs_timeout_commit_order() {
        run_paused(|| async {
            let signal_first = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut signal_wait = Box::pin(signal_first.wait(CancellationToken::never()));
            assert!(futures::poll!(signal_wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            signal_first.signal();
            assert_eq!(signal_wait.await, WaitStatus::Signalled);

            let timeout_first = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut timeout_wait = Box::pin(timeout_first.wait(CancellationToken::never()));
            assert!(futures::poll!(timeout_wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            wait_for_pending_commit(timeout_wait.as_mut(), || {
                timeout_first.status() == WaitStatus::TimedOut
            })
            .await;
            timeout_first.signal();
            assert_eq!(timeout_wait.await, WaitStatus::TimedOut);
        });
    }

    #[test]
    fn cancel_vs_timeout_commit_order() {
        run_paused(|| async {
            let local_source = CancellationSource::new();
            let local = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut local_wait = Box::pin(local.wait(local_source.token()));
            assert!(futures::poll!(local_wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            wait_for_pending_commit(local_wait.as_mut(), || {
                local.status() == WaitStatus::TimedOut
            })
            .await;
            assert!(local_source.request_cancellation());
            assert_eq!(local_wait.await, WaitStatus::TimedOut);

            let signal_source = CancellationSource::new();
            let signalled = TimedBaton::new(None);
            let mut signal_wait = Box::pin(signalled.wait(signal_source.token()));
            assert!(futures::poll!(signal_wait.as_mut()).is_pending());
            signalled.signal();
            assert!(signal_source.request_cancellation());
            assert_eq!(signal_wait.await, WaitStatus::Cancelled);

            let cancellable_source = CancellationSource::new();
            let cancellable = CancellableBaton::new();
            let mut cancellable_wait = Box::pin(
                cancellable.timed_wait(Duration::from_secs(1), cancellable_source.token()),
            );
            assert!(futures::poll!(cancellable_wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            wait_for_pending_commit(cancellable_wait.as_mut(), || {
                cancellable.waiter_outcome_for_test() == Some(WaitStatus::TimedOut)
            })
            .await;
            assert!(cancellable_source.request_cancellation());
            assert_eq!(cancellable_wait.await, WaitStatus::Cancelled);
        });
    }

    #[test]
    fn future_drop_unregisters_hooks_on_owner() {
        run_paused(|| async {
            let timed_source = CancellationSource::new();
            let timed_token = timed_source.token();
            let timed_baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut timed_wait = Box::pin(timed_baton.wait(timed_token.clone()));
            assert!(futures::poll!(timed_wait.as_mut()).is_pending());
            assert_eq!(timed_token.registration_count(), 1);
            assert!(timed_baton.timeout_is_armed());

            drop(timed_wait);
            assert_eq!(timed_token.registration_count(), 0);
            assert!(!timed_baton.timeout_is_armed());
            timed_baton.reset();
            assert!(timed_source.request_cancellation());
            time::advance(Duration::from_secs(10)).await;
            assert_eq!(timed_baton.status(), WaitStatus::NotReady);

            let source = CancellationSource::new();
            let token = source.token();
            let baton = DetachableCancellableBaton::new();
            let mut wait = Box::pin(baton.timed_wait(Duration::from_secs(1), token.clone()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(token.registration_count(), 1);
            assert!(baton.timeout_is_armed_for_test());
            assert!(baton.has_waiter_for_test());
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    baton.reset();
                }))
                .is_err()
            );

            drop(wait);
            assert_eq!(token.registration_count(), 0);
            assert!(!baton.timeout_is_armed_for_test());
            assert!(!baton.has_waiter_for_test());
            baton.reset();

            assert!(source.request_cancellation());
            time::advance(Duration::from_secs(10)).await;
            assert!(!baton.ready());

            baton.signal();
            assert_eq!(
                baton.wait(CancellationToken::never()).await,
                WaitStatus::Signalled
            );
        });
    }

    #[test]
    fn timed_wait_drop_commits_cancellation_that_won_teardown() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let token = source.token();
            let baton = TimedBaton::new(None);
            let mut wait = Box::pin(baton.wait(token.clone()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(token.registration_count(), 1);

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            drop(wait);
            assert_eq!(token.registration_count(), 0);
            assert_eq!(baton.status(), WaitStatus::Cancelled);
        });
    }

    #[test]
    fn timed_wait_drop_unregisters_before_later_cancellation() {
        run_paused(|| async {
            let baton = TimedBaton::new(None);

            let unpolled_source = CancellationSource::new();
            let unpolled_token = unpolled_source.token();
            let unpolled = baton.wait(unpolled_token.clone());
            assert_eq!(unpolled_token.registration_count(), 0);
            assert!(unpolled_source.request_cancellation());
            drop(unpolled);
            assert_eq!(unpolled_token.registration_count(), 0);
            assert_eq!(baton.status(), WaitStatus::NotReady);

            let source = CancellationSource::new();
            let token = source.token();
            let mut wait = Box::pin(baton.wait(token.clone()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            assert_eq!(token.registration_count(), 1);

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            drop(wait);
            assert_eq!(token.registration_count(), 0);
            assert_eq!(baton.status(), WaitStatus::NotReady);
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            assert_eq!(token.registration_count(), 0);
            assert_eq!(baton.status(), WaitStatus::NotReady);
        });
    }

    #[test]
    fn timed_wait_drop_preserves_committed_timeout() {
        run_paused(|| async {
            let source = CancellationSource::new();
            let token = source.token();
            let baton = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut wait = Box::pin(baton.wait(token.clone()));
            assert!(futures::poll!(wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            wait_for_pending_commit(wait.as_mut(), || baton.status() == WaitStatus::TimedOut).await;

            let barrier = Arc::new(Barrier::new(2));
            let request_barrier = Arc::clone(&barrier);
            let requester = std::thread::spawn(move || {
                request_barrier.wait();
                source.request_cancellation()
            });
            barrier.wait();
            assert!(requester.join().expect("cancellation requester"));

            drop(wait);
            assert_eq!(token.registration_count(), 0);
            assert_eq!(baton.status(), WaitStatus::TimedOut);
        });
    }

    /// `set_timeout` on a waiting baton moves its deadline later and earlier,
    /// from the instant of the call (`RUN-004`): a later deadline is a store
    /// on the wheel and an earlier one relinks the slot.
    #[test]
    fn set_timeout_while_waiting_moves_the_deadline_both_ways() {
        run_paused(|| async {
            let start = time::Instant::now();
            let extended = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut extended_wait = Box::pin(extended.wait(CancellationToken::never()));
            assert!(futures::poll!(extended_wait.as_mut()).is_pending());
            time::advance(Duration::from_millis(500)).await;
            extended.set_timeout(Some(Duration::from_secs(2)));
            assert!(futures::poll!(extended_wait.as_mut()).is_pending());
            time::advance(Duration::from_millis(1_000)).await;
            assert!(futures::poll!(extended_wait.as_mut()).is_pending());
            assert_eq!(extended_wait.await, WaitStatus::TimedOut);
            let extended_at = start.elapsed();
            assert!(
                (Duration::from_millis(2_500)..=Duration::from_millis(2_501))
                    .contains(&extended_at),
                "extended at 500 ms to 2 s: timed out at {extended_at:?}"
            );

            let start = time::Instant::now();
            let shortened = TimedBaton::new(Some(Duration::from_secs(10)));
            let mut shortened_wait = Box::pin(shortened.wait(CancellationToken::never()));
            assert!(futures::poll!(shortened_wait.as_mut()).is_pending());
            shortened.set_timeout(Some(Duration::from_millis(300)));
            assert_eq!(shortened_wait.await, WaitStatus::TimedOut);
            let shortened_at = start.elapsed();
            assert!(
                (Duration::from_millis(300)..=Duration::from_millis(301)).contains(&shortened_at),
                "shortened from 10 s to 300 ms: timed out at {shortened_at:?}"
            );
        });
    }

    /// A deadline that `set_timeout` moved, and that is due by the wait's next
    /// poll, times the wait out in that poll, before a later signal, exactly
    /// when a Tokio sleep made for it then is ready at its first poll, as the
    /// host sleep made at that poll was (`RUN-004`): on a timed wait, whose
    /// timeout is re-armed, and on an untimed one, whose timeout is armed
    /// first. Tokio rounds a deadline up to its next millisecond tick, so one
    /// just due may not be; 100 ms past the deadline it is.
    #[test]
    fn a_moved_deadline_due_by_the_next_poll_times_out_in_that_poll() {
        run_paused(|| async {
            for initial in [Some(Duration::from_secs(10)), None] {
                for delay in [Duration::from_millis(300), Duration::from_millis(400)] {
                    let baton = TimedBaton::new(initial);
                    let mut wait = Box::pin(baton.wait(CancellationToken::never()));
                    assert!(futures::poll!(wait.as_mut()).is_pending());
                    baton.set_timeout(Some(Duration::from_millis(300)));
                    let deadline = time::Instant::now() + Duration::from_millis(300);
                    time::advance(delay).await;
                    let mut tokio_sleep = pin!(time::sleep_until(deadline));
                    let due = futures::poll!(tokio_sleep.as_mut()).is_ready();
                    assert!(futures::poll!(wait.as_mut()).is_pending());
                    baton.signal();
                    let expected = if due {
                        WaitStatus::TimedOut
                    } else {
                        WaitStatus::Signalled
                    };
                    assert_eq!(
                        wait.await,
                        expected,
                        "a 300 ms deadline set on a wait timed {initial:?}, polled {delay:?} after \
                         it was set (Tokio sleep ready: {due})"
                    );
                    assert!(
                        due || delay < Duration::from_millis(400),
                        "a Tokio sleep is ready 100 ms past its deadline"
                    );
                }
            }
        });
    }

    /// The wheel timer of this thread's worker, on a wheel host.
    fn wheel() -> Rc<WheelTimer> {
        Host::tokio().wheel_timer().expect("a wheel host")
    }

    /// On a wheel host, an armed wait takes a slot on the wheel instead of a
    /// host sleep and times out at its deadline, never earlier; dropping a
    /// waiting baton and its wait frees the slot (`RUN-003`).
    #[test]
    fn armed_waits_take_a_wheel_slot_on_a_wheel_host() {
        run_on_wheel_host(async {
            let wheel = wheel();
            let start = time::Instant::now();
            let timed = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut timed_wait = Box::pin(timed.wait(CancellationToken::never()));
            let cancellable = CancellableBaton::new();
            let mut cancellable_wait = Box::pin(
                cancellable.timed_wait(Duration::from_secs(2), CancellationToken::never()),
            );
            assert!(futures::poll!(timed_wait.as_mut()).is_pending());
            assert!(futures::poll!(cancellable_wait.as_mut()).is_pending());
            let armed = wheel.stats();
            assert_eq!(
                (armed.live_slots, armed.armed),
                (2, 2),
                "one armed slot per waiting baton: {armed:?}"
            );

            assert_eq!(timed_wait.await, WaitStatus::TimedOut);
            let timed_out = start.elapsed();
            assert_eq!(cancellable_wait.await, WaitStatus::TimedOut);
            let cancellable_out = start.elapsed();
            assert!(
                (Duration::from_secs(1)..=Duration::from_millis(1_001)).contains(&timed_out),
                "{timed_out:?}"
            );
            assert!(
                (Duration::from_secs(2)..=Duration::from_millis(2_001)).contains(&cancellable_out),
                "{cancellable_out:?}"
            );
            assert_eq!(wheel.stats().fired, 2);

            let dropped = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut dropped_wait = Box::pin(dropped.wait(CancellationToken::never()));
            assert!(futures::poll!(dropped_wait.as_mut()).is_pending());
            assert_eq!(wheel.stats().armed, 1);
            drop(dropped);
            drop(dropped_wait);
            let stats = wheel.stats();
            assert_eq!(
                (stats.live_slots, stats.armed),
                (2, 0),
                "the dropped baton freed its slot: {stats:?}"
            );
        });
    }

    /// Successive armed waits on one baton reuse its one slot: each later
    /// deadline is a store, so only the first wait links the slot.
    #[test]
    fn successive_waits_on_one_baton_keep_one_slot() {
        run_on_wheel_host(async {
            let wheel = wheel();
            let timed = TimedBaton::new(Some(Duration::from_secs(1)));
            let cancellable = CancellableBaton::new();
            for _ in 0..10 {
                let mut timed_wait = Box::pin(timed.wait(CancellationToken::never()));
                let mut cancellable_wait = Box::pin(
                    cancellable.timed_wait(Duration::from_secs(1), CancellationToken::never()),
                );
                assert!(futures::poll!(timed_wait.as_mut()).is_pending());
                assert!(futures::poll!(cancellable_wait.as_mut()).is_pending());
                time::advance(Duration::from_millis(100)).await;
                timed.signal();
                cancellable.signal();
                assert_eq!(timed_wait.await, WaitStatus::Signalled);
                assert_eq!(cancellable_wait.await, WaitStatus::Signalled);
                timed.reset();
                cancellable.reset();
            }
            let stats = wheel.stats();
            assert_eq!(
                (
                    stats.live_slots,
                    stats.armed,
                    stats.scheduled,
                    stats.relinked,
                    stats.fired
                ),
                (2, 0, 20, 2, 0),
                "(slots, armed, schedules, links, fires): {stats:?}"
            );
        });
    }

    /// A signal beats a timeout that fired on the wheel but that the waiter
    /// has not observed yet: the waiter commits in its own poll, where the
    /// signal is seen first (`RUN-004`).
    #[test]
    fn a_signal_beats_a_fired_but_unobserved_timeout() {
        run_on_wheel_host(async {
            let wheel = wheel();
            let timed = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut timed_wait = Box::pin(timed.wait(CancellationToken::never()));
            let cancellable = CancellableBaton::new();
            let mut cancellable_wait = Box::pin(
                cancellable.timed_wait(Duration::from_secs(1), CancellationToken::never()),
            );
            assert!(futures::poll!(timed_wait.as_mut()).is_pending());
            assert!(futures::poll!(cancellable_wait.as_mut()).is_pending());
            time::advance(Duration::from_secs(1)).await;
            assert_eq!(wheel.stats().fired, 2, "both timeouts fired unobserved");

            timed.signal();
            cancellable.signal();
            assert_eq!(timed_wait.await, WaitStatus::Signalled);
            assert_eq!(cancellable_wait.await, WaitStatus::Signalled);
            assert_eq!(timed.status(), WaitStatus::Signalled);
        });
    }

    /// A wheel stopped while waits are armed (`~HHWheelTimer`'s `cancelAll`,
    /// as at a worker's teardown) completes them as cancelled, the destroyed
    /// baton's outcome (`RUN-008`), and so it does a wait armed after it.
    #[test]
    fn a_stopped_wheel_cancels_armed_waits() {
        run_on_wheel_host(async {
            let timed = TimedBaton::new(Some(Duration::from_secs(1)));
            let mut timed_wait = Box::pin(timed.wait(CancellationToken::never()));
            let cancellable = CancellableBaton::new();
            let mut cancellable_wait = Box::pin(
                cancellable.timed_wait(Duration::from_secs(1), CancellationToken::never()),
            );
            assert!(futures::poll!(timed_wait.as_mut()).is_pending());
            assert!(futures::poll!(cancellable_wait.as_mut()).is_pending());
            assert_eq!(wheel().stop(), 2, "two armed timeouts stopped");

            assert_eq!(timed_wait.await, WaitStatus::Cancelled);
            assert_eq!(timed.status(), WaitStatus::Cancelled);
            assert_eq!(cancellable_wait.await, WaitStatus::Cancelled);
            assert!(cancellable.ready());

            let late = CancellableBaton::new();
            assert_eq!(
                late.timed_wait(Duration::from_secs(1), CancellationToken::never())
                    .await,
                WaitStatus::Cancelled
            );
        });
    }

    /// On a wheel host an armed wait on a reused baton allocates nothing,
    /// where a Tokio sleep took one allocation per wait; a fresh baton and
    /// its armed wait allocate only the baton's state; an untimed wait
    /// allocates nothing. The counts are observable only where std's
    /// allocations reach this crate's counting allocator (opt builds).
    #[test]
    fn armed_waits_allocate_nothing_on_a_wheel_host() {
        run_on_wheel_host(async {
            let waker = futures::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            let timed = TimedBaton::new(Some(Duration::from_secs(1)));
            let cancellable = CancellableBaton::new();
            let untimed = TimedBaton::new(None);
            let mut reused = || {
                let mut wait = pin!(timed.wait(CancellationToken::never()));
                assert!(wait.as_mut().poll(&mut context).is_pending());
                timed.signal();
                assert_eq!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(WaitStatus::Signalled)
                );
                timed.reset();
                let mut wait = pin!(
                    cancellable.timed_wait(Duration::from_secs(1), CancellationToken::never())
                );
                assert!(wait.as_mut().poll(&mut context).is_pending());
                cancellable.signal();
                assert_eq!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(WaitStatus::Signalled)
                );
                cancellable.reset();
            };
            reused();
            let reused_allocations = count_allocations(|| (0..10).for_each(|_| reused()));
            let mut fresh = || {
                let baton = TimedBaton::new(Some(Duration::from_secs(1)));
                let mut wait = pin!(baton.wait(CancellationToken::never()));
                assert!(wait.as_mut().poll(&mut context).is_pending());
                baton.signal();
                assert_eq!(
                    wait.as_mut().poll(&mut context),
                    Poll::Ready(WaitStatus::Signalled)
                );
            };
            fresh();
            let fresh_allocations = count_allocations(|| (0..10).for_each(|_| fresh()));
            let untimed_allocations = count_allocations(|| {
                for _ in 0..10 {
                    let mut wait = pin!(untimed.wait(CancellationToken::never()));
                    assert!(wait.as_mut().poll(&mut context).is_pending());
                    untimed.signal();
                    assert_eq!(
                        wait.as_mut().poll(&mut context),
                        Poll::Ready(WaitStatus::Signalled)
                    );
                    untimed.reset();
                }
            });

            if let (Some(reused), Some(fresh), Some(untimed)) =
                (reused_allocations, fresh_allocations, untimed_allocations)
            {
                assert_eq!(
                    (reused, fresh, untimed),
                    (0, 10, 0),
                    "(20 armed waits on reused batons, 10 fresh batons with an armed wait, \
                     10 untimed waits)"
                );
            }
            assert_eq!(
                wheel().stats().scheduled,
                33,
                "every armed wait was scheduled on the wheel"
            );
        });
    }

    /// Records its name when woken, and then may panic with that name.
    struct OrderWake {
        name: &'static str,
        order: Arc<std::sync::Mutex<Vec<&'static str>>>,
        panics: bool,
    }

    impl Wake for OrderWake {
        fn wake(self: Arc<Self>) {
            self.order.lock().unwrap().push(self.name);
            if self.panics {
                panic_any(self.name);
            }
        }
    }

    fn order_waker(
        name: &'static str,
        order: &Arc<std::sync::Mutex<Vec<&'static str>>>,
        panics: bool,
    ) -> Waker {
        Waker::from(Arc::new(OrderWake {
            name,
            order: Arc::clone(order),
            panics,
        }))
    }

    #[test]
    fn cancellation_wakes_in_registration_order_after_slot_reuse() {
        for first_panics in [false, true] {
            let source = CancellationSource::new();
            let token = source.token();
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut a = token.wake_registration();
            let mut b = token.wake_registration();
            let mut c = token.wake_registration();
            assert!(!a.update_waker(&order_waker("a", &order, first_panics)));
            assert!(!b.update_waker(&order_waker("b", &order, false)));
            assert!(!c.update_waker(&order_waker("c", &order, false)));
            drop(b);
            // D takes the slot that B freed, ahead of C's slot.
            let mut d = token.wake_registration();
            assert!(!d.update_waker(&order_waker("d", &order, false)));
            assert_eq!(token.registration_count(), 3);

            let requested = catch_unwind(AssertUnwindSafe(|| source.request_cancellation()));
            if first_panics {
                let panic = requested.expect_err("the first wake panic resumes after the rest");
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"a"));
            } else {
                assert_eq!(requested.ok(), Some(true));
            }
            assert_eq!(
                *order.lock().unwrap(),
                ["a", "c", "d"],
                "panics {first_panics}"
            );
            assert_eq!(token.registration_count(), 0);
            for registration in [&mut a, &mut c, &mut d] {
                assert!(registration.update_waker(Waker::noop()));
            }
            assert!(!source.request_cancellation());
        }
    }

    #[test]
    fn promoted_first_registrant_repolls_in_place() {
        let source = CancellationSource::new();
        let token = source.token();
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut a = token.wake_registration();
        let mut b = token.wake_registration();
        assert!(!a.update_waker(&order_waker("a-old", &order, false)));
        assert_eq!(token.registration_count(), 1);
        assert!(!b.update_waker(&order_waker("b", &order, false)));
        assert_eq!(token.registration_count(), 2);
        // A's new waker does not will_wake its old one.
        assert!(!a.update_waker(&order_waker("a-new", &order, false)));
        assert_eq!(
            token.registration_count(),
            2,
            "the promoted registrant updates its entry in place"
        );
        assert!(source.request_cancellation());
        assert_eq!(*order.lock().unwrap(), ["a-new", "b"]);
        assert_eq!(token.registration_count(), 0);
    }

    #[test]
    fn registration_drop_after_cancellation_is_a_silent_no_op() {
        for registrants in [1, 2, 5] {
            let source = CancellationSource::new();
            let token = source.token();
            let wakes = Arc::new(AtomicUsize::new(0));
            let waker = Waker::from(Arc::new(CountingWake(Arc::clone(&wakes))));
            let mut registrations: Vec<Option<CancellationRegistration>> =
                (0..registrants).map(|_| None).collect();
            for registration in &mut registrations {
                assert!(!token.register(registration, &waker));
            }
            assert_eq!(token.registration_count(), registrants);
            assert!(source.request_cancellation());
            assert_eq!(wakes.load(Ordering::SeqCst), registrants);
            assert_eq!(token.registration_count(), 0);

            let mut retained = registrations.pop().expect("one registration");
            assert!(retained.is_some());
            assert!(
                token.register(&mut retained, &waker),
                "a registration after cancellation observes it"
            );
            assert!(retained.is_none(), "and retires the listener");
            for (index, registration) in registrations.into_iter().enumerate() {
                let registration = registration.expect("registered listener");
                if index % 2 == 0 {
                    assert!(registration.unregister(), "cancellation stays observed");
                } else {
                    drop(registration);
                }
            }
            assert_eq!(token.registration_count(), 0);
            assert_eq!(wakes.load(Ordering::SeqCst), registrants);
        }
    }

    #[test]
    fn registration_count_stays_exact_across_slot_reuse() {
        let source = CancellationSource::new();
        let token = source.token();
        let mut live: Vec<Option<CancellationRegistration>> = Vec::new();
        let mut state = 7_u32;
        for step in 0..2_000 {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            if live.is_empty() || !state.is_multiple_of(3) {
                let mut registration = None;
                assert!(!token.register(&mut registration, Waker::noop()));
                live.push(registration);
            } else {
                let index = (state as usize / 3) % live.len();
                let registration = live.swap_remove(index).expect("registered listener");
                if step % 2 == 0 {
                    assert!(!registration.unregister());
                } else {
                    drop(registration);
                }
            }
            assert_eq!(token.registration_count(), live.len(), "step {step}");
        }
        for registration in &mut live {
            assert!(!token.register(registration, Waker::noop()));
        }
        assert_eq!(
            token.registration_count(),
            live.len(),
            "a repeated registration updates its own entry"
        );
        assert!(source.request_cancellation());
        assert_eq!(token.registration_count(), 0);
    }

    #[test]
    fn worker_quiescing_drop_unregisters() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWake(Arc::clone(&wakes))));
        let mut context = Context::from_waker(&waker);
        let mut quiescing: Vec<_> = (0..3).map(|_| Some(handle.quiescing())).collect();
        assert_eq!(
            handle.quiescing_registrations(),
            0,
            "no listener before a poll"
        );
        for observer in quiescing.iter_mut().flatten() {
            assert!(Pin::new(observer).poll(&mut context).is_pending());
        }
        assert_eq!(handle.quiescing_registrations(), 3);
        drop(quiescing[1].take());
        assert_eq!(handle.quiescing_registrations(), 2);
        let first = quiescing[0].as_mut().expect("first observer");
        assert!(Pin::new(first).poll(&mut context).is_pending());
        assert_eq!(
            handle.quiescing_registrations(),
            2,
            "a repeated poll keeps one entry"
        );
        let mut later = handle.quiescing();
        assert!(Pin::new(&mut later).poll(&mut context).is_pending());
        assert_eq!(handle.quiescing_registrations(), 3);
        drop(quiescing[0].take());
        drop(later);
        assert_eq!(handle.quiescing_registrations(), 1);

        assert!(handle.begin_shutdown());
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "shutdown wakes the remaining observer"
        );
        let last = quiescing[2].as_mut().expect("last observer");
        assert_eq!(Pin::new(last).poll(&mut context), Poll::Ready(()));
        drop(quiescing);
        assert_eq!(handle.quiescing_registrations(), 0);
        assert!(
            Pin::new(&mut driver)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }

    #[test]
    fn worker_quiescing_repolls_with_its_registered_waker_without_the_registry() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let first_wakes = Arc::new(AtomicUsize::new(0));
        let second_wakes = Arc::new(AtomicUsize::new(0));
        let first = Waker::from(Arc::new(CountingWake(Arc::clone(&first_wakes))));
        let second = Waker::from(Arc::new(CountingWake(Arc::clone(&second_wakes))));
        let mut observer = handle.quiescing();
        let locks = handle.quiescing_registry_locks();
        assert!(
            Pin::new(&mut observer)
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        assert_eq!(
            handle.quiescing_registry_locks(),
            locks + 1,
            "the first poll registers its waker"
        );
        for _ in 0..3 {
            let clone = first.clone();
            assert!(
                Pin::new(&mut observer)
                    .poll(&mut Context::from_waker(&clone))
                    .is_pending()
            );
        }
        assert_eq!(
            handle.quiescing_registry_locks(),
            locks + 1,
            "a waker that would wake the registered task leaves the registry alone"
        );
        assert_eq!(handle.quiescing_registrations(), 1);

        // Counting registrations takes the lock too, so re-read the baseline.
        let locks = handle.quiescing_registry_locks();
        assert!(
            Pin::new(&mut observer)
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        assert_eq!(
            handle.quiescing_registry_locks(),
            locks + 1,
            "a different waker replaces the registered one"
        );
        assert!(
            Pin::new(&mut observer)
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        assert_eq!(handle.quiescing_registry_locks(), locks + 1);
        assert_eq!(
            handle.quiescing_registrations(),
            1,
            "the replacement reuses the observer's entry"
        );

        assert!(handle.begin_shutdown());
        assert_eq!(
            first_wakes.load(Ordering::SeqCst),
            0,
            "the replaced waker is not woken"
        );
        assert_eq!(
            second_wakes.load(Ordering::SeqCst),
            1,
            "shutdown wakes the registered waker once"
        );
        assert_eq!(
            Pin::new(&mut observer).poll(&mut Context::from_waker(&second)),
            Poll::Ready(()),
            "the retained waker does not hide quiescence"
        );
        assert_eq!(
            Pin::new(&mut observer).poll(&mut Context::from_waker(&second)),
            Poll::Ready(()),
            "quiescence stays observed"
        );
        assert_eq!(handle.quiescing_registrations(), 0);
        drop(observer);
        assert!(
            Pin::new(&mut driver)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }

    #[test]
    fn worker_quiescing_racing_shutdown_wakes_a_repolled_registration() {
        struct FlagWake(AtomicBool);

        impl Wake for FlagWake {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        for _ in 0..200 {
            let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
            let flag = Arc::new(FlagWake(AtomicBool::new(false)));
            let waker = Waker::from(Arc::clone(&flag));
            let mut observer = handle.quiescing();
            assert!(
                Pin::new(&mut observer)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let barrier = Arc::new(Barrier::new(2));
            let requester = std::thread::spawn({
                let handle = handle.clone();
                let barrier = Arc::clone(&barrier);
                move || {
                    barrier.wait();
                    assert!(handle.begin_shutdown());
                }
            });
            barrier.wait();
            let observed = Pin::new(&mut observer)
                .poll(&mut Context::from_waker(&waker))
                .is_ready();
            requester.join().expect("requester exits");
            assert!(
                observed || flag.0.load(Ordering::SeqCst),
                "a pending re-poll leaves a waker that shutdown wakes"
            );
            assert_eq!(
                Pin::new(&mut observer).poll(&mut Context::from_waker(&waker)),
                Poll::Ready(())
            );
            drop(observer);
            assert!(
                Pin::new(&mut driver)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_ready()
            );
        }
    }

    #[test]
    fn cancellation_registry_slots_are_reused_up_to_the_live_peak() {
        let source = CancellationSource::new();
        let token = source.token();
        let mut peak = 0;
        let mut check = |live: usize| {
            peak = live.max(peak);
            assert_eq!(token.registration_count(), live);
            assert_eq!(token.registration_slots(), peak, "{live} live");
        };
        check(0);
        let mut live: Vec<Option<CancellationRegistration>> = Vec::new();
        let register = |live: &mut Vec<Option<CancellationRegistration>>| {
            let mut registration = None;
            assert!(!token.register(&mut registration, Waker::noop()));
            live.push(registration);
        };
        for count in 1..=10 {
            register(&mut live);
            check(count);
        }
        // Retire seven out of registration order.
        for (retired, index) in [3, 0, 7, 1, 5, 2, 4].into_iter().enumerate() {
            live[index] = None;
            check(9 - retired);
        }
        for count in 4..=10 {
            register(&mut live);
            check(count);
        }
        for _ in 0..1_000 {
            register(&mut live);
            check(11);
            let index = live
                .iter()
                .rposition(Option::is_some)
                .expect("live listener");
            live[index] = None;
            check(10);
        }
        drop(live);
        check(0);
        assert!(source.request_cancellation());
        assert_eq!(
            token.registration_slots(),
            0,
            "cancellation releases the registry"
        );
    }
}

mod c124_operation_benchmark {
    use std::env;
    use std::future::Future;
    use std::hint::black_box;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Instant;

    use crate::cancellation::CancellationSource;
    use crate::cancellation::CancellationToken;
    use crate::host::Host;
    use crate::wait::TimedBaton;
    use crate::wait::WaitStatus;

    const OPERATIONS: usize = 131_072;
    const WARMUP_OPERATIONS: usize = 1_024;
    const SCHEDULE_LENGTH: usize = 1_024;

    struct Case {
        name: &'static str,
        cancellable: bool,
        ready_percent: usize,
    }

    const CASES: [Case; 10] = [
        Case {
            name: "cancellable_ready_000",
            cancellable: true,
            ready_percent: 0,
        },
        Case {
            name: "cancellable_ready_025",
            cancellable: true,
            ready_percent: 25,
        },
        Case {
            name: "cancellable_ready_050",
            cancellable: true,
            ready_percent: 50,
        },
        Case {
            name: "cancellable_ready_075",
            cancellable: true,
            ready_percent: 75,
        },
        Case {
            name: "cancellable_ready_100",
            cancellable: true,
            ready_percent: 100,
        },
        Case {
            name: "never_ready_000",
            cancellable: false,
            ready_percent: 0,
        },
        Case {
            name: "never_ready_025",
            cancellable: false,
            ready_percent: 25,
        },
        Case {
            name: "never_ready_050",
            cancellable: false,
            ready_percent: 50,
        },
        Case {
            name: "never_ready_075",
            cancellable: false,
            ready_percent: 75,
        },
        Case {
            name: "never_ready_100",
            cancellable: false,
            ready_percent: 100,
        },
    ];

    #[derive(Default)]
    struct Batch {
        elapsed_ns: u128,
        cpu_ns: u128,
        completed: usize,
        polls: usize,
        first_ready: usize,
        first_pending: usize,
        wakes: usize,
        schedule_mismatches: usize,
        residual_listeners: usize,
        retained_waker_refs: usize,
    }

    struct BenchmarkWake(AtomicUsize);

    impl Wake for BenchmarkWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn schedule(ready_percent: usize) -> [bool; SCHEDULE_LENGTH] {
        let mut schedule = [false; SCHEDULE_LENGTH];
        for ready in schedule
            .iter_mut()
            .take(SCHEDULE_LENGTH * ready_percent / 100)
        {
            *ready = true;
        }
        let mut state = 0x124b_a7c0_5eed_f00d_u64;
        for index in (1..SCHEDULE_LENGTH).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            schedule.swap(index, (state as usize) % (index + 1));
        }
        schedule
    }

    fn thread_cpu_ns() -> u128 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: The pointer refers to a live, writable timespec throughout the call.
        let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        assert_eq!(result, 0, "thread CPU clock must be available");
        let seconds = u128::try_from(time.tv_sec).expect("nonnegative thread CPU seconds");
        let nanos = u128::try_from(time.tv_nsec).expect("nonnegative thread CPU nanoseconds");
        seconds * 1_000_000_000 + nanos
    }

    fn run_batch(case: &Case, schedule: &[bool; SCHEDULE_LENGTH], operations: usize) -> Batch {
        let mut batch = Batch::default();
        let start = Instant::now();
        let cpu_start = thread_cpu_ns();
        let host = black_box(Host::tokio());
        let source = case.cancellable.then(CancellationSource::new);
        let wake = Arc::new(BenchmarkWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        {
            let mut cx = Context::from_waker(&waker);
            for index in 0..operations {
                let ready_before_poll = black_box(schedule[index % SCHEDULE_LENGTH]);
                let token = black_box(
                    source
                        .as_ref()
                        .map_or_else(CancellationToken::never, CancellationSource::token),
                );
                let baton = black_box(TimedBaton::with_host(None, host.clone()));
                let mut wait = black_box(baton.wait(token));
                if ready_before_poll {
                    black_box(&baton).signal();
                }
                let first = black_box(Pin::new(&mut wait)).poll(black_box(&mut cx));
                batch.polls += 1;
                batch.schedule_mismatches += usize::from(first.is_ready() != ready_before_poll);
                let result = match first {
                    Poll::Ready(status) => {
                        batch.first_ready += 1;
                        Poll::Ready(status)
                    }
                    Poll::Pending => {
                        batch.first_pending += 1;
                        black_box(&baton).signal();
                        batch.polls += 1;
                        black_box(Pin::new(&mut wait)).poll(black_box(&mut cx))
                    }
                };
                batch.completed += usize::from(result == Poll::Ready(WaitStatus::Signalled));
                drop(wait);
                drop(baton);
            }
        }
        batch.wakes = wake.0.load(Ordering::Relaxed);
        batch.residual_listeners = source
            .as_ref()
            .map_or(0, |source| source.token().registration_count());
        batch.retained_waker_refs = Arc::strong_count(&wake);
        drop(waker);
        drop(wake);
        drop(source);
        drop(host);
        let cpu_end = thread_cpu_ns();
        batch.elapsed_ns = start.elapsed().as_nanos();
        batch.cpu_ns = cpu_end
            .checked_sub(cpu_start)
            .expect("monotonic thread CPU clock");
        batch
    }

    fn check_batch(case: &Case, batch: &Batch, operations: usize) {
        assert_eq!(batch.completed, operations);
        assert_eq!(batch.first_ready, operations * case.ready_percent / 100);
        assert_eq!(batch.first_pending, operations - batch.first_ready);
        assert_eq!(batch.polls, operations + batch.first_pending);
        assert_eq!(batch.wakes, batch.first_pending);
        assert_eq!(batch.schedule_mismatches, 0);
        assert_eq!(batch.residual_listeners, 0);
        assert_eq!(batch.retained_waker_refs, 2);
    }

    #[test]
    #[ignore = "explicit fixed-work TimedWait mechanism measurement"]
    fn timed_wait_complete_operation_cost() {
        let arm = env::var("COMPONENT_BENCH_ARM").expect("set COMPONENT_BENCH_ARM");
        assert!(matches!(arm.as_str(), "baseline" | "candidate"));
        let pair: usize = env::var("COMPONENT_BENCH_PAIR")
            .expect("set COMPONENT_BENCH_PAIR")
            .parse()
            .expect("COMPONENT_BENCH_PAIR is an integer");
        assert!((1..=10).contains(&pair));

        for offset in 0..CASES.len() {
            let case = &CASES[(offset + pair - 1) % CASES.len()];
            let schedule = schedule(case.ready_percent);
            let warmup = run_batch(case, &schedule, WARMUP_OPERATIONS);
            check_batch(case, &warmup, WARMUP_OPERATIONS);
            let batch = run_batch(case, &schedule, OPERATIONS);
            check_batch(case, &batch, OPERATIONS);
            println!(
                concat!(
                    "COMPONENT_BENCH_JSON {{\"schema\":\"component-bench-v1\",",
                    "\"event\":\"batch\",\"mechanism\":\"wait\",\"case\":\"{}\",",
                    "\"arm\":\"{}\",\"pair\":{},\"operations\":{},\"elapsed_ns\":{},",
                    "\"cpu_clock\":\"thread\",\"cpu_ns\":{},\"checksum\":{},\"assertions_passed\":true,",
                    "\"ready_percent\":{},\"cancellable\":{},\"warmup_operations\":{},",
                    "\"warmup_elapsed_ns\":{},\"warmup_cpu_ns\":{},\"counts\":{{\"polls\":{},\"wakes\":{},",
                    "\"first_poll_ready\":{},\"first_poll_pending\":{},",
                    "\"schedule_mismatches\":{},\"residual_listeners\":{},",
                    "\"retained_waker_refs\":{}}}}}"
                ),
                case.name,
                arm,
                pair,
                batch.completed,
                batch.elapsed_ns,
                batch.cpu_ns,
                batch.completed * 3 + batch.polls + batch.wakes,
                case.ready_percent,
                case.cancellable,
                WARMUP_OPERATIONS,
                warmup.elapsed_ns,
                warmup.cpu_ns,
                batch.polls,
                batch.wakes,
                batch.first_ready,
                batch.first_pending,
                batch.schedule_mismatches,
                batch.residual_listeners,
                batch.retained_waker_refs,
            );
        }
    }
}
