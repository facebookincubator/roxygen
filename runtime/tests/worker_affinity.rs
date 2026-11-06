mod worker {
    use std::any::Any;
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::future::pending;
    use std::marker::PhantomData;
    use std::marker::PhantomPinned;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Instant;

    use futures::task::noop_waker;
    use tokio::runtime::Builder;

    use crate::context::ContextEnvelope;
    use crate::context::RuntimeContext;
    use crate::driver::WorkerDriver;
    use crate::host::Host;
    use crate::host::LocalFuture;
    use crate::host::LocalHost;
    use crate::observability::OperationKind;
    use crate::observability::SpanObserver;
    use crate::observability::TerminalOutcome;
    use crate::task_group::TaskGroup;
    use crate::task_group::TaskGroupError;
    use crate::tokio_host::ManagedWorker;
    use crate::worker::CleanupRegistrationError;
    use crate::worker::SubmitError;
    use crate::worker::WorkerCall;
    use crate::worker::WorkerCallError;
    use crate::worker::WorkerCleanupObservationError;
    use crate::worker::WorkerCleanupObserver;
    use crate::worker::WorkerCleanupRegistration;
    use crate::worker::WorkerCleanupTask;
    use crate::worker::WorkerCleanupTaskRegistration;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerHandle;
    use crate::worker::WorkerSelector;
    use crate::worker::WorkerState;

    thread_local! {
        static ESCAPED_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
        static ESCAPED_CLEANUP_TASK: RefCell<Option<WorkerCleanupTaskRegistration>> =
            const { RefCell::new(None) };
        static RETIREMENT_REGISTRATIONS: RefCell<Vec<WorkerCleanupTaskRegistration>> =
            const { RefCell::new(Vec::new()) };
        static RETIREMENT_TASKS: RefCell<Vec<WorkerCleanupTask>> =
            const { RefCell::new(Vec::new()) };
    }

    #[derive(Clone)]
    struct RetirementProbe {
        owner: ThreadId,
        ready: Arc<AtomicBool>,
        panic_on_poll: Arc<AtomicBool>,
        panic_on_drop: Arc<AtomicBool>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        captures: Arc<AtomicUsize>,
        observer: Arc<Mutex<Option<WorkerCleanupObserver>>>,
    }

    impl RetirementProbe {
        fn new() -> Self {
            Self {
                owner: std::thread::current().id(),
                ready: Arc::new(AtomicBool::new(false)),
                panic_on_poll: Arc::new(AtomicBool::new(false)),
                panic_on_drop: Arc::new(AtomicBool::new(false)),
                polls: Arc::new(AtomicUsize::new(0)),
                drops: Arc::new(AtomicUsize::new(0)),
                captures: Arc::new(AtomicUsize::new(0)),
                observer: Arc::new(Mutex::new(None)),
            }
        }

        fn assert_not_retired(&self) {
            assert_eq!(std::thread::current().id(), self.owner);
            let observer = self.observer.lock().expect("probe observation");
            assert!(
                retirement_result(observer.as_ref().expect("observation installed")).is_pending()
            );
        }

        fn assert_destroyed(&self) {
            assert_eq!(self.drops.load(Ordering::SeqCst), 1);
            assert_eq!(self.captures.load(Ordering::SeqCst), 1);
        }
    }

    struct RetirementCapture(RetirementProbe);

    impl Drop for RetirementCapture {
        fn drop(&mut self) {
            self.0.assert_not_retired();
            self.0.captures.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct RetirementFuture {
        probe: RetirementProbe,
        _capture: RetirementCapture,
        _not_send: Rc<()>,
    }

    impl Future for RetirementFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.probe.polls.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.probe.panic_on_poll.load(Ordering::SeqCst),
                "cleanup poll panic"
            );
            if self.probe.ready.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for RetirementFuture {
        fn drop(&mut self) {
            self.probe.assert_not_retired();
            self.probe.drops.fetch_add(1, Ordering::SeqCst);
            assert!(
                !self.probe.panic_on_drop.load(Ordering::SeqCst),
                "cleanup drop panic"
            );
        }
    }

    fn retirement_result(
        observer: &WorkerCleanupObserver,
    ) -> Poll<Result<(), WorkerCleanupObservationError>> {
        Box::pin(observer.wait())
            .as_mut()
            .poll(&mut Context::from_waker(&noop_waker()))
    }

    fn poll_retirement_driver(driver: &mut WorkerDriver) -> Poll<()> {
        Pin::new(driver).poll(&mut Context::from_waker(&noop_waker()))
    }

    fn add_retirement(
        handle: &WorkerHandle,
        driver: &mut WorkerDriver,
        probe: &RetirementProbe,
        scope: bool,
        active: bool,
    ) -> WorkerCleanupObserver {
        let probe = probe.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        handle
            .submit(move |context| {
                let future = RetirementFuture {
                    probe: probe.clone(),
                    _capture: RetirementCapture(probe.clone()),
                    _not_send: Rc::new(()),
                };
                let registration = if scope {
                    context.register_scope(future)
                } else {
                    context.register_cleanup_task(future)
                }
                .expect("cleanup reserved");
                let observer = registration.observe();
                *probe.observer.lock().expect("probe observation") = Some(observer.clone());
                if active {
                    RETIREMENT_TASKS.with(|tasks| {
                        tasks
                            .borrow_mut()
                            .push(registration.activate().expect("cleanup activated"));
                    });
                } else {
                    RETIREMENT_REGISTRATIONS.with(|registrations| {
                        registrations.borrow_mut().push(registration);
                    });
                }
                sender.send(observer).expect("observation exported");
            })
            .expect("initialization admitted");
        assert!(poll_retirement_driver(driver).is_pending());
        receiver
            .try_recv()
            .expect("initialization ran synchronously")
    }

    fn activate_retirement() {
        let registration = RETIREMENT_REGISTRATIONS
            .with(|registrations| registrations.borrow_mut().pop().expect("reserved cleanup"));
        RETIREMENT_TASKS.with(|tasks| {
            tasks
                .borrow_mut()
                .push(registration.activate().expect("cleanup activated"));
        });
    }

    fn clear_retirement_handles() {
        RETIREMENT_REGISTRATIONS.with(|registrations| registrations.borrow_mut().clear());
        RETIREMENT_TASKS.with(|tasks| tasks.borrow_mut().clear());
    }

    #[derive(Clone, Copy)]
    enum RetirementWakePanic {
        None,
        Message(&'static str),
        DroppingPayload,
    }

    struct RetirementWake {
        calls: Arc<AtomicUsize>,
        panic: RetirementWakePanic,
    }

    struct RetirementPanicPayload;

    impl Drop for RetirementPanicPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }

    impl Wake for RetirementWake {
        fn wake(self: Arc<Self>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.panic {
                RetirementWakePanic::None => {}
                RetirementWakePanic::Message(message) => std::panic::panic_any(message),
                RetirementWakePanic::DroppingPayload => {
                    std::panic::panic_any(RetirementPanicPayload)
                }
            }
        }
    }

    fn retirement_waker(panic: RetirementWakePanic) -> (Waker, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Waker::from(Arc::new(RetirementWake {
                calls: Arc::clone(&calls),
                panic,
            })),
            calls,
        )
    }

    #[test]
    fn cleanup_observer_before_activation_is_send_sync_cloneable() {
        fn assert_traits<Value: Send + Sync + Clone>() {}
        assert_traits::<WorkerCleanupObserver>();
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let probe = RetirementProbe::new();
        let observer = add_retirement(&handle, &mut driver, &probe, false, false);
        let clone = observer.clone();
        let returned = std::thread::spawn(move || {
            assert!(retirement_result(&clone).is_pending());
            clone
        })
        .join()
        .expect("observation thread joins");
        let (waker, calls) = retirement_waker(RetirementWakePanic::None);
        let mut first = Box::pin(observer.wait());
        let mut second = Box::pin(returned.wait());
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let (dropped_waker, dropped_calls) = retirement_waker(RetirementWakePanic::None);
        let mut dropped_wait = Box::pin(observer.wait());
        assert!(
            dropped_wait
                .as_mut()
                .poll(&mut Context::from_waker(&dropped_waker))
                .is_pending()
        );
        drop(dropped_wait);
        drop(observer.clone());
        drop(observer.wait());
        assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
        activate_retirement();
        probe.ready.store(true, Ordering::SeqCst);
        assert!(poll_retirement_driver(&mut driver).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(dropped_calls.load(Ordering::SeqCst), 0);
        assert_eq!(retirement_result(&observer), Poll::Ready(Ok(())));
        RETIREMENT_TASKS.with(|tasks| {
            let mut tasks = tasks.borrow_mut();
            let task = tasks.first_mut().expect("local observation retained");
            assert!(task.is_finished());
            assert_eq!(retirement_result(&task.observe()), Poll::Ready(Ok(())));
            assert!(
                Pin::new(task)
                    .poll(&mut Context::from_waker(&noop_waker()))
                    .is_ready()
            );
        });
        probe.assert_destroyed();
        clear_retirement_handles();
        drop(driver);
    }

    #[test]
    fn cleanup_observer_follows_future_and_capture_drop() {
        for scope in [false, true] {
            let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
            let probe = RetirementProbe::new();
            let observer = add_retirement(&handle, &mut driver, &probe, scope, true);
            assert!(retirement_result(&observer).is_pending());
            probe.ready.store(true, Ordering::SeqCst);
            assert!(poll_retirement_driver(&mut driver).is_pending());
            probe.assert_destroyed();
            assert_eq!(retirement_result(&observer), Poll::Ready(Ok(())));
            assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
            clear_retirement_handles();
            drop(driver);
        }
    }

    #[test]
    fn cleanup_observer_disarm_preserves_retirement() {
        for panic_on_drop in [false, true] {
            let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
            let probe = RetirementProbe::new();
            let observer = add_retirement(&handle, &mut driver, &probe, true, false);
            probe.panic_on_drop.store(panic_on_drop, Ordering::SeqCst);
            let disarmed = catch_unwind(AssertUnwindSafe(|| {
                RETIREMENT_REGISTRATIONS
                    .with(|registrations| {
                        registrations
                            .borrow_mut()
                            .pop()
                            .expect("reservation")
                            .disarm()
                    })
                    .expect("owner-local disarm")
            }));
            if panic_on_drop {
                assert!(disarmed.is_err());
                assert_eq!(
                    retirement_result(&observer),
                    Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
                );
            } else {
                assert!(disarmed.expect("disarm succeeds"));
                assert_eq!(retirement_result(&observer), Poll::Ready(Ok(())));
            }
            assert_eq!(probe.polls.load(Ordering::SeqCst), 0);
            probe.assert_destroyed();
            drop(driver);
        }
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let probe = RetirementProbe::new();
        let observer = add_retirement(&handle, &mut driver, &probe, false, false);
        handle.begin_shutdown();
        assert!(poll_retirement_driver(&mut driver).is_pending());
        let disarmed = RETIREMENT_REGISTRATIONS
            .with(|registrations| {
                registrations
                    .borrow_mut()
                    .pop()
                    .expect("reservation")
                    .disarm()
            })
            .expect("owner-local disarm");
        assert!(!disarmed);
        assert!(retirement_result(&observer).is_pending());
        probe.ready.store(true, Ordering::SeqCst);
        assert!(poll_retirement_driver(&mut driver).is_ready());
        assert_eq!(retirement_result(&observer), Poll::Ready(Ok(())));
        probe.assert_destroyed();
    }

    #[test]
    fn cleanup_observer_registration_drop_only_activates() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let probe = RetirementProbe::new();
        let observer = add_retirement(&handle, &mut driver, &probe, true, false);
        RETIREMENT_REGISTRATIONS.with(|registrations| registrations.borrow_mut().clear());
        assert!(retirement_result(&observer).is_pending());
        assert!(poll_retirement_driver(&mut driver).is_pending());
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        probe.ready.store(true, Ordering::SeqCst);
        assert!(poll_retirement_driver(&mut driver).is_pending());
        probe.assert_destroyed();
        assert_eq!(retirement_result(&observer), Poll::Ready(Ok(())));
    }

    #[test]
    fn cleanup_observer_driver_drop_retires_pending_futures() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let reserved = RetirementProbe::new();
        let active = RetirementProbe::new();
        let reserved_observer = add_retirement(&handle, &mut driver, &reserved, true, false);
        let active_observer = add_retirement(&handle, &mut driver, &active, true, true);
        handle
            .submit(|_| panic!("queued factory must not run"))
            .expect("queue filled");
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Overloaded));
        drop(driver);
        assert_eq!(handle.state(), WorkerState::Closed);
        reserved.assert_destroyed();
        active.assert_destroyed();
        assert_eq!(retirement_result(&reserved_observer), Poll::Ready(Ok(())));
        assert_eq!(retirement_result(&active_observer), Poll::Ready(Ok(())));
        clear_retirement_handles();
    }

    #[test]
    fn cleanup_observer_panic_paths_preserve_independent_notifications() {
        for (panic_on_poll, panic_on_drop) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
            let first_probe = RetirementProbe::new();
            let gated_probe = RetirementProbe::new();
            let first = add_retirement(&handle, &mut driver, &first_probe, false, true);
            let gated = add_retirement(&handle, &mut driver, &gated_probe, false, true);
            let mut waits = Vec::new();
            let mut counts = Vec::new();
            for panic in [
                RetirementWakePanic::Message("observer wake panic"),
                RetirementWakePanic::DroppingPayload,
                RetirementWakePanic::None,
            ] {
                let (waker, calls) = retirement_waker(panic);
                let mut waiting = Box::pin(first.wait());
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                waits.push(waiting);
                counts.push(calls);
            }
            let (legacy_waker, legacy_calls) =
                retirement_waker(RetirementWakePanic::Message("legacy wake panic"));
            RETIREMENT_TASKS.with(|tasks| {
                assert!(
                    Pin::new(tasks.borrow_mut().first_mut().expect("legacy observer"))
                        .poll(&mut Context::from_waker(&legacy_waker))
                        .is_pending()
                );
            });
            let (shutdown_waker, shutdown_calls) = retirement_waker(RetirementWakePanic::Message(
                "shutdown transition wake panic",
            ));
            let mut quiescing = Box::pin(handle.quiescing());
            assert!(
                quiescing
                    .as_mut()
                    .poll(&mut Context::from_waker(&shutdown_waker))
                    .is_pending()
            );
            let (closed_waker, closed_calls) = retirement_waker(RetirementWakePanic::None);
            let mut closed = Box::pin(handle.closed());
            assert!(
                closed
                    .as_mut()
                    .poll(&mut Context::from_waker(&closed_waker))
                    .is_pending()
            );
            first_probe.ready.store(true, Ordering::SeqCst);
            first_probe
                .panic_on_poll
                .store(panic_on_poll, Ordering::SeqCst);
            first_probe
                .panic_on_drop
                .store(panic_on_drop, Ordering::SeqCst);
            assert!(poll_retirement_driver(&mut driver).is_pending());
            assert!(retirement_result(&gated).is_pending());
            assert_eq!(gated_probe.drops.load(Ordering::SeqCst), 0);
            assert_eq!(shutdown_calls.load(Ordering::SeqCst), 1);
            assert_eq!(closed_calls.load(Ordering::SeqCst), 0);
            let expected = if panic_on_drop {
                Err(WorkerCleanupObservationError::Unconfirmed)
            } else {
                Ok(())
            };
            assert_eq!(retirement_result(&first), Poll::Ready(expected));
            assert!(counts.iter().all(|count| count.load(Ordering::SeqCst) == 1));
            assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);
            assert_eq!(handle.state(), WorkerState::Closing);
            gated_probe.ready.store(true, Ordering::SeqCst);
            let panic = catch_unwind(AssertUnwindSafe(|| poll_retirement_driver(&mut driver)))
                .expect_err("first panic follows all cleanup");
            let message = if panic_on_poll {
                "cleanup poll panic"
            } else if panic_on_drop {
                "cleanup drop panic"
            } else {
                "observer wake panic"
            };
            assert_eq!(panic.downcast_ref::<&str>(), Some(&message));
            assert_eq!(handle.state(), WorkerState::Closed);
            assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
            assert_eq!(retirement_result(&gated), Poll::Ready(Ok(())));
            first_probe.assert_destroyed();
            gated_probe.assert_destroyed();
            clear_retirement_handles();
        }
    }

    #[test]
    fn cleanup_observer_scope_inner_drop_panic_is_unconfirmed() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let probe = RetirementProbe::new();
        let observer = add_retirement(&handle, &mut driver, &probe, true, true);
        probe.ready.store(true, Ordering::SeqCst);
        probe.panic_on_drop.store(true, Ordering::SeqCst);
        let panic = catch_unwind(AssertUnwindSafe(|| poll_retirement_driver(&mut driver)))
            .expect_err("inner future destructor panic propagates");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"cleanup drop panic"));
        assert_eq!(
            retirement_result(&observer),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        assert_eq!(
            retirement_result(&observer.clone()),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
        probe.assert_destroyed();
        assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
        clear_retirement_handles();
    }

    #[test]
    fn cleanup_observer_scope_accounting_drop_panic_is_unconfirmed() {
        for accounting_panics in [true, false] {
            let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
            let probe = RetirementProbe::new();
            let observer = add_retirement(&handle, &mut driver, &probe, true, true);
            let supervisor_panic = if accounting_panics {
                RetirementWakePanic::Message("scope accounting drop panic")
            } else {
                RetirementWakePanic::None
            };
            let (supervisor_waker, supervisor_calls) = retirement_waker(supervisor_panic);
            assert_eq!(handle.quiesce_snapshot(&supervisor_waker), (None, 1));
            let (observer_waker, observer_calls) =
                retirement_waker(RetirementWakePanic::Message("retirement wake panic"));
            let mut waiting = Box::pin(observer.wait());
            assert!(
                waiting
                    .as_mut()
                    .poll(&mut Context::from_waker(&observer_waker))
                    .is_pending()
            );
            let (independent_waker, independent_calls) =
                retirement_waker(RetirementWakePanic::None);
            let mut independent = Box::pin(observer.clone().wait());
            assert!(
                independent
                    .as_mut()
                    .poll(&mut Context::from_waker(&independent_waker))
                    .is_pending()
            );
            let (legacy_waker, legacy_calls) = retirement_waker(RetirementWakePanic::None);
            RETIREMENT_TASKS.with(|tasks| {
                assert!(
                    Pin::new(tasks.borrow_mut().first_mut().expect("legacy observer"))
                        .poll(&mut Context::from_waker(&legacy_waker))
                        .is_pending()
                );
            });
            let (closed_waker, closed_calls) = retirement_waker(RetirementWakePanic::None);
            let mut closed = Box::pin(handle.closed());
            assert!(
                closed
                    .as_mut()
                    .poll(&mut Context::from_waker(&closed_waker))
                    .is_pending()
            );
            probe.ready.store(true, Ordering::SeqCst);
            probe
                .panic_on_poll
                .store(!accounting_panics, Ordering::SeqCst);
            let panic = catch_unwind(AssertUnwindSafe(|| poll_retirement_driver(&mut driver)))
                .expect_err("first failure follows retirement and closure");
            let message = if accounting_panics {
                "scope accounting drop panic"
            } else {
                "cleanup poll panic"
            };
            assert_eq!(panic.downcast_ref::<&str>(), Some(&message));
            let expected = if accounting_panics {
                Err(WorkerCleanupObservationError::Unconfirmed)
            } else {
                Ok(())
            };
            assert_eq!(retirement_result(&observer), Poll::Ready(expected));
            assert_eq!(retirement_result(&observer.clone()), Poll::Ready(expected));
            assert_eq!(handle.state(), WorkerState::Closed);
            assert_eq!(handle.quiesce_snapshot(&noop_waker()).1, 0);
            assert_eq!(supervisor_calls.load(Ordering::SeqCst), 1);
            assert_eq!(observer_calls.load(Ordering::SeqCst), 1);
            assert_eq!(independent_calls.load(Ordering::SeqCst), 1);
            assert_eq!(legacy_calls.load(Ordering::SeqCst), 1);
            assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
            probe.assert_destroyed();
            clear_retirement_handles();
            drop(driver);
            assert_eq!(supervisor_calls.load(Ordering::SeqCst), 1);
            probe.assert_destroyed();
        }
    }

    #[test]
    fn driver_drop_shutdown_panic_still_retires_all_scopes() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let reserved = RetirementProbe::new();
        let active = RetirementProbe::new();
        let reserved_observer = add_retirement(&handle, &mut driver, &reserved, true, false);
        let active_observer = add_retirement(&handle, &mut driver, &active, true, true);
        let (quiescing_waker, quiescing_calls) =
            retirement_waker(RetirementWakePanic::Message("shutdown wake panic"));
        let mut quiescing = Box::pin(handle.quiescing());
        assert!(
            quiescing
                .as_mut()
                .poll(&mut Context::from_waker(&quiescing_waker))
                .is_pending()
        );
        let (driver_waker, driver_calls) = retirement_waker(RetirementWakePanic::DroppingPayload);
        assert!(
            Pin::new(&mut driver)
                .poll(&mut Context::from_waker(&driver_waker))
                .is_pending()
        );
        let (closed_waker, closed_calls) =
            retirement_waker(RetirementWakePanic::Message("closed wake panic"));
        let mut closed = Box::pin(handle.closed());
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(&closed_waker))
                .is_pending()
        );
        let panic = catch_unwind(AssertUnwindSafe(|| drop(driver)))
            .expect_err("first shutdown panic propagates last");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"shutdown wake panic"));
        assert_eq!(quiescing_calls.load(Ordering::SeqCst), 1);
        assert_eq!(driver_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handle.state(), WorkerState::Closed);
        assert!(!handle.is_accepting());
        reserved.assert_destroyed();
        active.assert_destroyed();
        assert_eq!(retirement_result(&reserved_observer), Poll::Ready(Ok(())));
        assert_eq!(retirement_result(&active_observer), Poll::Ready(Ok(())));
        clear_retirement_handles();
    }

    #[test]
    fn driver_drop_during_unwind_still_retires_all_scopes() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker starts");
        let reserved = RetirementProbe::new();
        let active = RetirementProbe::new();
        let reserved_observer = add_retirement(&handle, &mut driver, &reserved, true, false);
        let active_observer = add_retirement(&handle, &mut driver, &active, true, true);
        let (waker, calls) = retirement_waker(RetirementWakePanic::DroppingPayload);
        let mut quiescing = Box::pin(handle.quiescing());
        assert!(
            quiescing
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let panic = catch_unwind(AssertUnwindSafe(move || {
            let _owned_driver = driver;
            panic!("original unwind");
        }))
        .expect_err("original unwind survives driver destruction");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"original unwind"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(handle.state(), WorkerState::Closed);
        reserved.assert_destroyed();
        active.assert_destroyed();
        assert_eq!(retirement_result(&reserved_observer), Poll::Ready(Ok(())));
        assert_eq!(retirement_result(&active_observer), Poll::Ready(Ok(())));
        clear_retirement_handles();
    }

    fn wait_for<T>(call: crate::worker::WorkerCall<T>) -> T {
        Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(call)
            .expect("worker call completes")
    }

    #[test]
    fn local_state_runs_on_owner() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let result = wait_for(
            worker
                .handle()
                .call(|context| {
                    let local = Rc::new(Cell::new(0));
                    local.set(7);
                    (
                        context.owner_thread(),
                        std::thread::current().id(),
                        local.get(),
                    )
                })
                .expect("command admitted"),
        );

        assert_eq!(result, (owner, owner, 7));
        worker.shutdown().expect("worker shuts down");
    }

    #[test]
    fn guard_pins_owner_until_release() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let counts = wait_for(
            worker
                .handle()
                .call(|context| {
                    let before = context.active_guards();
                    let during = {
                        let _guard = context.acquire_guard();
                        context.active_guards()
                    };
                    (before, during, context.active_guards())
                })
                .expect("command admitted"),
        );

        assert_eq!(counts, (0, 1, 0));
        worker.shutdown().expect("worker shuts down");
    }

    #[test]
    fn closed_worker_rejects_new_task() {
        let worker = ManagedWorker::new(1).expect("managed worker starts");
        let handle = worker.handle();

        assert!(handle.begin_shutdown());
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        worker.shutdown().expect("worker shuts down");
    }

    struct DropProbe {
        owner: ThreadId,
        handle: WorkerHandle,
        dropped: Option<mpsc::Sender<ThreadId>>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            debug_assert_eq!(std::thread::current().id(), self.owner);
            debug_assert!(self.handle.is_cancelled());
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(std::thread::current().id());
            }
        }
    }

    #[test]
    fn shutdown_joins_admitted_task() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let (started_tx, started_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        worker
            .handle()
            .spawn_local(move |context| async move {
                let _probe = DropProbe {
                    owner: context.owner_thread(),
                    handle: context.handle().clone(),
                    dropped: Some(dropped_tx),
                };
                started_tx.send(()).expect("announce task start");
                pending::<()>().await;
            })
            .expect("task admitted");

        started_rx.recv().expect("task started on owner");
        worker.shutdown().expect("shutdown joins cancellation");
        assert_eq!(dropped_rx.recv().expect("task state dropped"), owner);
    }

    #[test]
    fn scheduled_task_may_enqueue_before_shutdown_boundary() {
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let handle = worker.handle();
        let nested = wait_for(
            handle
                .call({
                    let handle = handle.clone();
                    move |_| {
                        handle
                            .call(|context| context.worker_id())
                            .expect("nested command admitted before shutdown")
                    }
                })
                .expect("outer command admitted"),
        );

        assert_eq!(wait_for(nested), handle.id());
        worker.shutdown().expect("worker shuts down");
    }

    #[test]
    fn session_handle_never_migrates_local_state() {
        struct PinnedSession {
            worker: WorkerHandle,
        }

        impl PinnedSession {
            fn observe_owner(
                &self,
            ) -> crate::worker::WorkerCall<(crate::worker::WorkerId, ThreadId)> {
                self.worker
                    .call(|context| (context.worker_id(), std::thread::current().id()))
                    .expect("session command admitted")
            }
        }

        let first = ManagedWorker::new(4).expect("first worker starts");
        let second = ManagedWorker::new(4).expect("second worker starts");
        let selector = WorkerSelector::new(vec![first.handle(), second.handle()]);
        let selected = selector.select().expect("first worker selected");
        let session = PinnedSession {
            worker: selected.clone(),
        };

        let other = selector.select().expect("second worker selected");
        assert!(!selected.same_worker(&other));
        let first_observation = wait_for(session.observe_owner());
        let second_observation = wait_for(session.observe_owner());
        assert_eq!(first_observation, (selected.id(), selected.owner_thread()));
        assert_eq!(second_observation, first_observation);

        first.shutdown().expect("first worker shuts down");
        second.shutdown().expect("second worker shuts down");
    }

    #[test]
    fn cleanup_registration_can_be_disarmed_after_normal_cleanup() {
        let (handle, mut driver) = WorkerDriver::new(2).expect("worker driver starts");
        let cleanup_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cleanup_count_for_task = Arc::clone(&cleanup_count);
        handle
            .spawn_local(move |context| async move {
                let local = Rc::new(());
                let registration = context
                    .register_cleanup(move || {
                        drop(local);
                        cleanup_count_for_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    })
                    .expect("cleanup registered on owner");
                assert!(registration.disarm().expect("cleanup disarmed on owner"));
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert_eq!(Pin::new(&mut driver).poll(&mut task_context), Poll::Pending,);
        assert!(handle.begin_shutdown());
        assert_eq!(
            Pin::new(&mut driver).poll(&mut task_context),
            Poll::Ready(()),
        );
        assert_eq!(cleanup_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn cleanup_task_is_owner_local_observable_and_bypasses_command_overload() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_completed = Arc::clone(&completed);
        handle
            .spawn_local(move |context| async move {
                context
                    .handle()
                    .submit(|_| {})
                    .expect("the sole command slot is filled");
                assert_eq!(
                    context.handle().submit(|_| {}),
                    Err(SubmitError::Overloaded)
                );

                let local = Rc::new(Cell::new(0));
                let cleanup_local = Rc::clone(&local);
                let registration = context
                    .register_cleanup_task(async move {
                        cleanup_local.set(7);
                        task_completed.store(true, std::sync::atomic::Ordering::SeqCst);
                    })
                    .expect("owner-local cleanup registration bypasses the command queue");
                let mut cleanup = registration
                    .activate()
                    .expect("the pre-admitted cleanup activates while open");
                assert!(!cleanup.is_finished());
                (&mut cleanup).await;
                assert!(cleanup.is_finished());
                assert_eq!(local.get(), 7);
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(handle.begin_shutdown());
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_ready());
    }

    #[derive(Debug, Eq, PartialEq)]
    enum AsyncCleanupAction {
        TaskDropped(WorkerState, ThreadId),
        CleanupPolled(WorkerState, ThreadId),
        CleanupFinished(WorkerState, ThreadId),
    }

    struct YieldCleanup {
        handle: WorkerHandle,
        actions: Arc<Mutex<Vec<AsyncCleanupAction>>>,
        yielded: bool,
        _not_send: Rc<()>,
    }

    impl Future for YieldCleanup {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                self.actions
                    .lock()
                    .unwrap()
                    .push(AsyncCleanupAction::CleanupFinished(
                        self.handle.state(),
                        std::thread::current().id(),
                    ));
                Poll::Ready(())
            } else {
                self.yielded = true;
                self.actions
                    .lock()
                    .unwrap()
                    .push(AsyncCleanupAction::CleanupPolled(
                        self.handle.state(),
                        std::thread::current().id(),
                    ));
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    struct ActivateCleanupOnDrop {
        registration: Option<WorkerCleanupTaskRegistration>,
        handle: WorkerHandle,
        actions: Arc<Mutex<Vec<AsyncCleanupAction>>>,
        _not_send: Rc<()>,
    }

    impl Drop for ActivateCleanupOnDrop {
        fn drop(&mut self) {
            self.actions
                .lock()
                .unwrap()
                .push(AsyncCleanupAction::TaskDropped(
                    self.handle.state(),
                    std::thread::current().id(),
                ));
            self.registration
                .take()
                .expect("the cleanup reservation is armed")
                .activate()
                .expect("a cleanup reserved while open activates during task cancellation")
                .detach();
        }
    }

    #[test]
    fn shutdown_drains_cleanup_activated_while_dropping_an_admitted_task() {
        let owner = std::thread::current().id();
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let task_actions = Arc::clone(&actions);
        handle
            .spawn_local(move |context| async move {
                let registration = context
                    .register_cleanup_task(YieldCleanup {
                        handle: context.handle().clone(),
                        actions: Arc::clone(&task_actions),
                        yielded: false,
                        _not_send: Rc::new(()),
                    })
                    .expect("cleanup ownership is reserved while open");
                let activate_on_drop = ActivateCleanupOnDrop {
                    registration: Some(registration),
                    handle: context.handle().clone(),
                    actions: task_actions,
                    _not_send: Rc::new(()),
                };
                let _never_polled_run = async move {
                    let _activate_on_drop = activate_on_drop;
                    pending::<()>().await;
                };
                pending::<()>().await;
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert!(handle.begin_shutdown());
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert_eq!(handle.state(), WorkerState::Closing);
        assert_eq!(
            *actions.lock().unwrap(),
            [
                AsyncCleanupAction::TaskDropped(WorkerState::Closing, owner),
                AsyncCleanupAction::CleanupPolled(WorkerState::Closing, owner),
            ],
        );
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_ready());
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            *actions.lock().unwrap(),
            [
                AsyncCleanupAction::TaskDropped(WorkerState::Closing, owner),
                AsyncCleanupAction::CleanupPolled(WorkerState::Closing, owner),
                AsyncCleanupAction::CleanupFinished(WorkerState::Closing, owner),
            ],
        );
    }

    #[test]
    fn shutdown_activates_an_escaped_pre_registered_cleanup() {
        let owner = std::thread::current().id();
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let task_actions = Arc::clone(&actions);
        handle
            .spawn_local(move |context| async move {
                let registration = context
                    .register_cleanup_task(YieldCleanup {
                        handle: context.handle().clone(),
                        actions: task_actions,
                        yielded: false,
                        _not_send: Rc::new(()),
                    })
                    .expect("cleanup ownership is reserved while open");
                ESCAPED_CLEANUP_TASK.with(|escaped| {
                    assert!(escaped.replace(Some(registration)).is_none());
                });
                pending::<()>().await;
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert!(handle.begin_shutdown());
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert_eq!(
            *actions.lock().unwrap(),
            [AsyncCleanupAction::CleanupPolled(
                WorkerState::Closing,
                owner
            )],
        );
        assert_eq!(handle.state(), WorkerState::Closing);
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_ready());
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            *actions.lock().unwrap(),
            [
                AsyncCleanupAction::CleanupPolled(WorkerState::Closing, owner),
                AsyncCleanupAction::CleanupFinished(WorkerState::Closing, owner),
            ],
        );
        ESCAPED_CLEANUP_TASK.with(|escaped| drop(escaped.replace(None)));
        assert_eq!(actions.lock().unwrap().len(), 2);
    }

    #[derive(Debug, Eq, PartialEq)]
    enum PanicCleanupAction {
        Polled(u8, WorkerState),
        Dropped(u8, WorkerState),
    }

    struct ReadyCleanupWithDrop {
        id: u8,
        handle: WorkerHandle,
        actions: Arc<Mutex<Vec<PanicCleanupAction>>>,
        panic_on_drop: bool,
        _not_send: Rc<()>,
    }

    impl Future for ReadyCleanupWithDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.actions
                .lock()
                .unwrap()
                .push(PanicCleanupAction::Polled(self.id, self.handle.state()));
            Poll::Ready(())
        }
    }

    impl Drop for ReadyCleanupWithDrop {
        fn drop(&mut self) {
            self.actions
                .lock()
                .unwrap()
                .push(PanicCleanupAction::Dropped(self.id, self.handle.state()));
            if self.panic_on_drop {
                panic!("first async cleanup drop panic");
            }
        }
    }

    #[test]
    fn cleanup_future_drop_panic_does_not_skip_later_tasks_or_closed() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let task_actions = Arc::clone(&actions);
        handle
            .spawn_local(move |context| async move {
                let first = context
                    .register_cleanup_task(ReadyCleanupWithDrop {
                        id: 1,
                        handle: context.handle().clone(),
                        actions: Arc::clone(&task_actions),
                        panic_on_drop: true,
                        _not_send: Rc::new(()),
                    })
                    .expect("first cleanup reserved");
                let second = context
                    .register_cleanup_task(ReadyCleanupWithDrop {
                        id: 2,
                        handle: context.handle().clone(),
                        actions: Arc::clone(&task_actions),
                        panic_on_drop: false,
                        _not_send: Rc::new(()),
                    })
                    .expect("second cleanup reserved");
                let _registrations = (first, second);
                pending::<()>().await;
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert!(Pin::new(&mut driver).poll(&mut task_context).is_pending());
        assert!(handle.begin_shutdown());
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = Pin::new(&mut driver).poll(&mut task_context);
        }))
        .expect_err("the first cleanup destructor panic is propagated");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"first async cleanup drop panic"),
        );
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            *actions.lock().unwrap(),
            [
                PanicCleanupAction::Polled(1, WorkerState::Closing),
                PanicCleanupAction::Dropped(1, WorkerState::Closing),
                PanicCleanupAction::Polled(2, WorkerState::Closing),
                PanicCleanupAction::Dropped(2, WorkerState::Closing),
            ],
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    enum ShutdownAction {
        PendingTaskDropped(WorkerState, ThreadId),
        Cleanup(u8, WorkerState, ThreadId),
        ResourceDropped(u8, WorkerState, ThreadId),
    }

    struct LocalCleanupResource {
        id: u8,
        handle: WorkerHandle,
        actions: Arc<Mutex<Vec<ShutdownAction>>>,
        _not_send: Rc<()>,
    }

    impl Drop for LocalCleanupResource {
        fn drop(&mut self) {
            self.actions
                .lock()
                .unwrap()
                .push(ShutdownAction::ResourceDropped(
                    self.id,
                    self.handle.state(),
                    std::thread::current().id(),
                ));
        }
    }

    struct PendingTaskDrop {
        handle: WorkerHandle,
        actions: Arc<Mutex<Vec<ShutdownAction>>>,
    }

    impl Drop for PendingTaskDrop {
        fn drop(&mut self) {
            self.actions
                .lock()
                .unwrap()
                .push(ShutdownAction::PendingTaskDropped(
                    self.handle.state(),
                    std::thread::current().id(),
                ));
        }
    }

    #[test]
    fn shutdown_drains_escaped_owner_cleanups_after_tasks_and_before_closed() {
        let owner = std::thread::current().id();
        let (handle, mut driver) = WorkerDriver::new(4).expect("worker driver starts");
        let actions = Arc::new(Mutex::new(Vec::new()));
        let task_actions = Arc::clone(&actions);
        handle
            .spawn_local(move |context| async move {
                ESCAPED_CONTEXT.with(|escaped| escaped.replace(Some(context.clone())));
                let pending_drop = PendingTaskDrop {
                    handle: context.handle().clone(),
                    actions: Arc::clone(&task_actions),
                };

                for id in 1..=2 {
                    let resource = LocalCleanupResource {
                        id,
                        handle: context.handle().clone(),
                        actions: Arc::clone(&task_actions),
                        _not_send: Rc::new(()),
                    };
                    let callback_actions = Arc::clone(&task_actions);
                    let callback_handle = context.handle().clone();
                    let _registration = context
                        .register_cleanup(move || {
                            callback_actions
                                .lock()
                                .unwrap()
                                .push(ShutdownAction::Cleanup(
                                    id,
                                    callback_handle.state(),
                                    std::thread::current().id(),
                                ));
                            if id == 1 {
                                let _resource = resource;
                                panic!("first cleanup panic");
                            }
                            drop(resource);
                        })
                        .expect("cleanup registered on owner");
                }

                let _pending_drop = pending_drop;
                pending::<()>().await;
            })
            .expect("task admitted");

        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert_eq!(Pin::new(&mut driver).poll(&mut task_context), Poll::Pending,);
        assert!(handle.begin_shutdown());
        ESCAPED_CONTEXT.with(|escaped| {
            assert!(matches!(
                escaped
                    .borrow()
                    .as_ref()
                    .expect("context escaped the task")
                    .register_cleanup(|| {}),
                Err(CleanupRegistrationError::Closed),
            ));
            assert!(matches!(
                escaped
                    .borrow()
                    .as_ref()
                    .expect("context escaped the task")
                    .register_cleanup_task(async {}),
                Err(CleanupRegistrationError::Closed),
            ));
        });

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = Pin::new(&mut driver).poll(&mut task_context);
        }))
        .expect_err("the first cleanup panic is propagated");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"first cleanup panic"));
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            *actions.lock().unwrap(),
            vec![
                ShutdownAction::PendingTaskDropped(WorkerState::Closing, owner),
                ShutdownAction::Cleanup(1, WorkerState::Closing, owner),
                ShutdownAction::ResourceDropped(1, WorkerState::Closing, owner),
                ShutdownAction::Cleanup(2, WorkerState::Closing, owner),
                ShutdownAction::ResourceDropped(2, WorkerState::Closing, owner),
            ],
        );

        ESCAPED_CONTEXT.with(|escaped| {
            assert!(matches!(
                escaped
                    .borrow()
                    .as_ref()
                    .expect("context remains escaped after closure")
                    .register_cleanup(|| {}),
                Err(CleanupRegistrationError::Closed),
            ));
            assert!(matches!(
                escaped
                    .borrow()
                    .as_ref()
                    .expect("context remains escaped after closure")
                    .register_cleanup_task(async {}),
                Err(CleanupRegistrationError::Closed),
            ));
            drop(escaped.replace(None));
        });
    }

    #[test]
    fn cleanup_registration_rejects_a_non_owner_thread() {
        let owner = std::thread::current().id();
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let other_handle = handle.clone();
        let (actual, error, task_error) = std::thread::spawn(move || {
            let actual = std::thread::current().id();
            let context = WorkerContext::new(other_handle);
            let error = match context.register_cleanup(|| {}) {
                Ok(_) => panic!("non-owner registration accepted"),
                Err(error) => error,
            };
            let task_error = match context.register_cleanup_task(async {}) {
                Ok(_) => panic!("non-owner cleanup-task registration accepted"),
                Err(error) => error,
            };
            (actual, error, task_error)
        })
        .join()
        .expect("thread reports registration error");
        assert_eq!(
            error,
            CleanupRegistrationError::WrongOwnerThread {
                expected: owner,
                actual,
            },
        );
        assert_eq!(task_error, error);

        assert!(handle.begin_shutdown());
        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut driver).poll(&mut task_context),
            Poll::Ready(()),
        );
    }

    struct PanickingClosedWake {
        id: u8,
        handle: WorkerHandle,
        awakened: Arc<Mutex<Vec<(u8, WorkerState)>>>,
        panic_message: Option<&'static str>,
    }

    impl Wake for PanickingClosedWake {
        fn wake(self: Arc<Self>) {
            self.awakened
                .lock()
                .unwrap()
                .push((self.id, self.handle.state()));
            if let Some(message) = self.panic_message {
                std::panic::panic_any(message);
            }
        }
    }

    #[test]
    fn shutdown_wakes_every_closed_waiter_and_then_propagates_the_first_panic() {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let awakened = Arc::new(Mutex::new(Vec::new()));
        let mut closed_waiters = Vec::new();
        let mut wakers = Vec::new();
        for (id, panic_message) in [
            (1, Some("first closed waker panic")),
            (2, Some("second closed waker panic")),
            (3, None),
        ] {
            closed_waiters.push(Box::pin(handle.closed()));
            wakers.push(Waker::from(Arc::new(PanickingClosedWake {
                id,
                handle: handle.clone(),
                awakened: Arc::clone(&awakened),
                panic_message,
            })));
        }
        for (closed, waker) in closed_waiters.iter_mut().zip(&wakers) {
            let mut context = Context::from_waker(waker);
            assert!(closed.as_mut().poll(&mut context).is_pending());
        }

        let observer = SpanObserver::new(4).expect("span observer starts");
        ContextEnvelope::capture()
            .with_span_observer(observer.clone())
            .run(|| assert!(handle.begin_shutdown()));
        let waker = noop_waker();
        let mut task_context = Context::from_waker(&waker);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = Pin::new(&mut driver).poll(&mut task_context);
        }))
        .expect_err("the first closed-waker panic is propagated");

        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"first closed waker panic"),
        );
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(
            *awakened.lock().unwrap(),
            [
                (1, WorkerState::Closed),
                (2, WorkerState::Closed),
                (3, WorkerState::Closed),
            ],
        );
        assert!(observer.snapshots().iter().any(|span| {
            span.kind == OperationKind::Shutdown
                && span.outcome == Some(TerminalOutcome::Shutdown)
                && span.close_count == 1
        }));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CommandSpan {
        Absent,
        Open,
        Closed(TerminalOutcome),
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CallStep {
        FactoryDropped,
        JobDropped {
            panicking: bool,
            command: CommandSpan,
        },
        OutputDropped {
            panicking: bool,
        },
        CallerWoke {
            panicking: bool,
            command: CommandSpan,
        },
    }

    fn command_span(observer: &SpanObserver) -> CommandSpan {
        observer
            .snapshots()
            .iter()
            .find(|span| span.kind == OperationKind::Command)
            .map_or(CommandSpan::Absent, |span| {
                span.outcome.map_or(CommandSpan::Open, CommandSpan::Closed)
            })
    }

    /// The command span as the span observer of the installed context reports
    /// it. Only a context captured under the probe's submission carries that
    /// observer, so this is `Absent` when no such context is installed.
    fn installed_command() -> CommandSpan {
        ContextEnvelope::capture()
            .span_observer()
            .map_or(CommandSpan::Absent, command_span)
    }

    // Thread-local, so that a job output that the worker leaks owns no heap
    // memory for the leak sanitizer to report.
    thread_local! {
        static CALL_STEPS: RefCell<Vec<CallStep>> = const { RefCell::new(Vec::new()) };
    }

    /// The log of one `call_local` call, kept on the owner thread: the job
    /// future's drop and the caller's wake record `std::thread::panicking()`
    /// and the call's command span as the installed context reports it.
    #[derive(Clone)]
    struct CallProbe {
        owner: ThreadId,
        observer: SpanObserver,
        addresses: Arc<Mutex<Vec<usize>>>,
    }

    impl CallProbe {
        fn new() -> Self {
            CALL_STEPS.with_borrow_mut(Vec::clear);
            Self {
                owner: std::thread::current().id(),
                observer: SpanObserver::new(8).expect("span observer starts"),
                addresses: Arc::default(),
            }
        }

        fn command(&self) -> CommandSpan {
            command_span(&self.observer)
        }

        fn record(&self, step: CallStep) {
            assert_eq!(std::thread::current().id(), self.owner);
            CALL_STEPS.with_borrow_mut(|steps| steps.push(step));
        }

        fn steps(&self) -> Vec<CallStep> {
            CALL_STEPS.with_borrow(Vec::clone)
        }

        fn addresses(&self) -> Vec<usize> {
            self.addresses.lock().unwrap().clone()
        }

        /// Submits under this probe's span observer, then polls the call once
        /// so that the caller's wake is recorded.
        fn call<F>(
            &self,
            handle: &WorkerHandle,
            caller_panics: bool,
            factory: F,
        ) -> WorkerCall<CallOutput>
        where
            F: FnOnce(WorkerContext) -> CallJob + Send + 'static,
        {
            let mut call = ContextEnvelope::capture()
                .with_span_observer(self.observer.clone())
                .run(|| handle.call_local(factory))
                .expect("call_local is admitted");
            let waker = Waker::from(Arc::new(CallerWake {
                probe: self.clone(),
                panics: caller_panics,
            }));
            assert!(
                Pin::new(&mut call)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            call
        }

        fn job(&self, plan: JobPlan) -> impl FnOnce(WorkerContext) -> CallJob + Send + 'static {
            let probe = self.clone();
            move |_| CallJob {
                probe,
                plan,
                polls: Cell::new(0),
                _pinned: PhantomPinned,
                _not_send: PhantomData,
            }
        }

        /// The job future kept one address across its polls and its drop, it
        /// dropped once, and the caller woke once.
        fn assert_in_place_once(&self, records: usize) {
            let addresses = self.addresses();
            assert_eq!(addresses.len(), records, "{addresses:?}");
            assert!(
                addresses.iter().all(|address| *address == addresses[0]),
                "{addresses:?}"
            );
            let steps = self.steps();
            let count =
                |matches: fn(&CallStep) -> bool| steps.iter().filter(|s| matches(s)).count();
            assert_eq!(
                count(|step| matches!(step, CallStep::JobDropped { .. })),
                1,
                "{steps:?}"
            );
            assert_eq!(
                count(|step| matches!(step, CallStep::CallerWoke { .. })),
                1,
                "{steps:?}"
            );
        }
    }

    #[derive(Clone, Copy)]
    enum JobEnd {
        Ready,
        Panic,
        Never,
    }

    #[derive(Clone, Copy)]
    struct JobPlan {
        pending_polls: usize,
        end: JobEnd,
        panic_on_drop: bool,
    }

    impl JobPlan {
        const NEVER: Self = Self {
            pending_polls: 0,
            end: JobEnd::Never,
            panic_on_drop: false,
        };

        const fn ready_after(pending_polls: usize) -> Self {
            Self {
                pending_polls,
                end: JobEnd::Ready,
                panic_on_drop: false,
            }
        }

        const fn panic_after(pending_polls: usize) -> Self {
            Self {
                pending_polls,
                end: JobEnd::Panic,
                panic_on_drop: false,
            }
        }

        const fn panicking_on_drop(self) -> Self {
            Self {
                panic_on_drop: true,
                ..self
            }
        }
    }

    /// A `!Unpin`, `!Send` job future that records its address at every poll
    /// and in its `Drop`.
    struct CallJob {
        probe: CallProbe,
        plan: JobPlan,
        polls: Cell<usize>,
        _pinned: PhantomPinned,
        _not_send: PhantomData<Rc<()>>,
    }

    impl CallJob {
        fn record_address(&self) {
            assert_eq!(std::thread::current().id(), self.probe.owner);
            self.probe
                .addresses
                .lock()
                .unwrap()
                .push(std::ptr::from_ref(self).addr());
        }
    }

    impl Future for CallJob {
        type Output = CallOutput;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<CallOutput> {
            let job = self.into_ref().get_ref();
            job.record_address();
            let polls = job.polls.replace(job.polls.get() + 1);
            if polls < job.plan.pending_polls {
                return Poll::Pending;
            }
            match job.plan.end {
                JobEnd::Ready => Poll::Ready(CallOutput { value: 7 }),
                JobEnd::Panic => std::panic::panic_any("call_local job poll panic"),
                JobEnd::Never => Poll::Pending,
            }
        }
    }

    impl Drop for CallJob {
        fn drop(&mut self) {
            self.record_address();
            self.probe.record(CallStep::JobDropped {
                panicking: std::thread::panicking(),
                command: installed_command(),
            });
            if self.plan.panic_on_drop {
                std::panic::panic_any("call_local job drop panic");
            }
        }
    }

    struct CallOutput {
        value: u32,
    }

    impl Drop for CallOutput {
        fn drop(&mut self) {
            CALL_STEPS.with_borrow_mut(|steps| {
                steps.push(CallStep::OutputDropped {
                    panicking: std::thread::panicking(),
                });
            });
        }
    }

    struct FactoryCapture(CallProbe);

    impl Drop for FactoryCapture {
        fn drop(&mut self) {
            self.0.record(CallStep::FactoryDropped);
        }
    }

    struct CallerWake {
        probe: CallProbe,
        panics: bool,
    }

    impl Wake for CallerWake {
        fn wake(self: Arc<Self>) {
            self.probe.record(CallStep::CallerWoke {
                panicking: std::thread::panicking(),
                command: installed_command(),
            });
            if self.panics {
                std::panic::panic_any("call_local caller waker panic");
            }
        }
    }

    fn call_result(call: &mut WorkerCall<CallOutput>) -> Result<u32, WorkerCallError> {
        let Poll::Ready(result) = Pin::new(call).poll(&mut Context::from_waker(&noop_waker()))
        else {
            panic!("the call has resolved");
        };
        result.map(|output| output.value)
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

    /// `Owner` is `WorkerDriver::new`, which polls its tasks itself; `Hosted`
    /// is `WorkerDriver::with_host`, the driver of `HostedWorker` and
    /// `roxygen_explicit`, which runs each command as a task-group host task.
    #[derive(Clone, Copy, Debug)]
    enum CallDriver {
        Owner,
        Hosted,
    }

    const CALL_DRIVERS: [CallDriver; 2] = [CallDriver::Owner, CallDriver::Hosted];

    struct CallWorker {
        kind: CallDriver,
        handle: WorkerHandle,
        driver: WorkerDriver,
        host: Option<ManualHost>,
    }

    impl CallWorker {
        fn start(kind: CallDriver) -> Self {
            let host = match kind {
                CallDriver::Owner => None,
                CallDriver::Hosted => Some(ManualHost::default()),
            };
            let (handle, driver) = match &host {
                None => WorkerDriver::new(4),
                Some(host) => WorkerDriver::with_host(4, Host::new(Rc::new(host.clone()))),
            }
            .expect("worker driver starts");
            Self {
                kind,
                handle,
                driver,
                host,
            }
        }

        /// Polls the driver; a hosted driver's tasks then run once, and the
        /// driver is polled again to join them.
        fn turn(&mut self) -> Poll<()> {
            let polled = poll_retirement_driver(&mut self.driver);
            let Some(host) = &self.host else {
                return polled;
            };
            host.run();
            poll_retirement_driver(&mut self.driver)
        }

        /// A job panic escapes the owner driver's poll, which stays `Open`
        /// when the panic left a poll and `Closed` when it left shutdown. The
        /// hosted driver's task group contains it, and the driver then shuts
        /// down and reports `TaskPanicked`. Under either driver, the job's
        /// context is no longer installed.
        fn assert_job_panic(
            &self,
            payload: Box<dyn Any + Send>,
            message: &'static str,
            owner_state: WorkerState,
        ) {
            assert_eq!(
                installed_command(),
                CommandSpan::Absent,
                "{:?}: the unwind restored the test thread's context",
                self.kind
            );
            match self.kind {
                CallDriver::Owner => {
                    assert_eq!(payload.downcast_ref::<&str>(), Some(&message));
                    assert_eq!(self.handle.state(), owner_state);
                }
                CallDriver::Hosted => {
                    assert_eq!(
                        payload.downcast_ref::<TaskGroupError>(),
                        Some(&TaskGroupError::TaskPanicked)
                    );
                    assert_eq!(self.handle.state(), WorkerState::Closed);
                }
            }
        }
    }

    /// The driver pass that converts this command converts the commands queued
    /// before it too, and then drops their jobs without polling them.
    fn shut_down_in_next_factory(handle: &WorkerHandle) {
        handle
            .spawn_local(|context| {
                assert!(context.handle().begin_shutdown());
                std::future::ready(())
            })
            .expect("the shutdown command is admitted");
    }

    #[test]
    fn call_local_command_dropped_while_queued_keeps_its_drop_order() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let probe = CallProbe::new();
            let capture = FactoryCapture(probe.clone());
            let job = probe.job(JobPlan::ready_after(0));
            let mut call = probe.call(&worker.handle, false, move |context| {
                let _capture = capture;
                job(context)
            });
            assert!(worker.handle.begin_shutdown());

            assert!(worker.turn().is_ready());
            assert_eq!(
                probe.steps(),
                [
                    CallStep::FactoryDropped,
                    CallStep::CallerWoke {
                        panicking: false,
                        command: CommandSpan::Absent,
                    },
                ],
                "{kind:?}"
            );
            assert!(probe.addresses().is_empty());
            assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        }
    }

    #[test]
    fn call_local_job_dropped_unpolled_wakes_the_caller_before_dropping_its_future() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let probe = CallProbe::new();
            let mut call = probe.call(&worker.handle, false, probe.job(JobPlan::ready_after(0)));
            shut_down_in_next_factory(&worker.handle);

            assert!(worker.turn().is_ready());
            assert_eq!(
                probe.steps(),
                [
                    CallStep::CallerWoke {
                        panicking: false,
                        command: CommandSpan::Open,
                    },
                    CallStep::JobDropped {
                        panicking: false,
                        command: CommandSpan::Open,
                    },
                ],
                "{kind:?}"
            );
            assert_eq!(probe.addresses().len(), 1, "{kind:?}: never polled");
            assert_eq!(
                probe.command(),
                CommandSpan::Closed(TerminalOutcome::Cancelled)
            );
            assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        }
    }

    #[test]
    fn call_local_job_dropped_while_pending_drops_its_future_before_waking_the_caller() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let probe = CallProbe::new();
            let mut call = probe.call(&worker.handle, false, probe.job(JobPlan::NEVER));
            assert!(worker.turn().is_pending());
            assert!(probe.steps().is_empty());

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready());
            assert_eq!(
                probe.steps(),
                [
                    CallStep::JobDropped {
                        panicking: false,
                        command: CommandSpan::Open,
                    },
                    CallStep::CallerWoke {
                        panicking: false,
                        command: CommandSpan::Open,
                    },
                ],
                "{kind:?}"
            );
            assert_eq!(
                probe.command(),
                CommandSpan::Closed(TerminalOutcome::Cancelled)
            );
            assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        }
    }

    #[test]
    fn call_local_job_drops_its_future_before_sending_the_result() {
        for kind in CALL_DRIVERS {
            for pending_polls in [0, 1] {
                let mut worker = CallWorker::start(kind);
                let probe = CallProbe::new();
                let mut call = probe.call(
                    &worker.handle,
                    false,
                    probe.job(JobPlan::ready_after(pending_polls)),
                );
                for _ in 0..pending_polls {
                    assert!(worker.turn().is_pending());
                }
                assert!(probe.steps().is_empty());

                assert!(worker.turn().is_pending());
                assert_eq!(
                    probe.steps(),
                    [
                        CallStep::JobDropped {
                            panicking: false,
                            command: CommandSpan::Open,
                        },
                        CallStep::CallerWoke {
                            panicking: false,
                            command: CommandSpan::Open,
                        },
                    ],
                    "{kind:?} after {pending_polls} pending polls"
                );
                assert_eq!(
                    probe.command(),
                    CommandSpan::Closed(TerminalOutcome::Success)
                );
                assert_eq!(call_result(&mut call), Ok(7));
                assert_eq!(
                    probe.steps().last(),
                    Some(&CallStep::OutputDropped { panicking: false })
                );
            }
        }
    }

    #[test]
    fn call_local_job_poll_panic_drops_its_future_then_wakes_the_caller() {
        for kind in CALL_DRIVERS {
            for pending_polls in [0, 1] {
                let mut worker = CallWorker::start(kind);
                let probe = CallProbe::new();
                let mut call = probe.call(
                    &worker.handle,
                    false,
                    probe.job(JobPlan::panic_after(pending_polls)),
                );
                for _ in 0..pending_polls {
                    assert!(worker.turn().is_pending());
                }

                let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
                    .expect_err("the job's poll panic propagates");
                assert_eq!(
                    probe.steps(),
                    [
                        CallStep::JobDropped {
                            panicking: true,
                            command: CommandSpan::Open,
                        },
                        CallStep::CallerWoke {
                            panicking: true,
                            command: CommandSpan::Open,
                        },
                    ],
                    "{kind:?} after {pending_polls} pending polls"
                );
                assert_eq!(
                    probe.command(),
                    CommandSpan::Closed(TerminalOutcome::Failure)
                );
                worker.assert_job_panic(payload, "call_local job poll panic", WorkerState::Open);
                assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
            }
        }
    }

    #[test]
    fn call_local_factory_panic_cancels_the_call_and_fails_the_command_span() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let probe = CallProbe::new();
            let mut call = probe.call(&worker.handle, false, |_| -> CallJob {
                std::panic::panic_any("call_local factory panic")
            });

            let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
                .expect_err("the factory panic propagates");
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"call_local factory panic"),
                "{kind:?}"
            );
            assert_eq!(
                probe.steps(),
                [CallStep::CallerWoke {
                    panicking: true,
                    command: CommandSpan::Open,
                }],
                "{kind:?}"
            );
            assert_eq!(
                probe.command(),
                CommandSpan::Closed(TerminalOutcome::Failure)
            );
            assert_eq!(worker.handle.state(), WorkerState::Open);
            assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        }
    }

    /// An `N`-byte job future with the alignment of `A`.
    struct LargeJob<A, const N: usize> {
        _align: [A; 0],
        bytes: [u8; N],
    }

    impl<A, const N: usize> Future for LargeJob<A, N> {
        type Output = u8;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<u8> {
            Poll::Ready(self.bytes[N - 1])
        }
    }

    /// The job that `call_local` spawns for a `LargeJob<A, N>` is at most 32
    /// bytes larger than that future.
    fn assert_call_local_job_stored_once<A: 'static, const N: usize>() {
        assert_eq!(size_of::<LargeJob<A, N>>(), N);
        let (handle, driver) = WorkerDriver::new(1).expect("worker driver starts");
        let call = handle
            .call_local(|_| LargeJob::<A, N> {
                _align: [],
                bytes: [1; N],
            })
            .expect("call_local is admitted");
        let job = crate::worker::take_spawned_job_size().expect("call_local spawned a job");
        drop(driver);
        drop(call);
        assert!(
            job <= N + 32,
            "a {N}-byte job future aligned to {} bytes spawns {job} bytes",
            align_of::<A>()
        );
    }

    #[test]
    fn call_local_job_stores_its_future_once() {
        assert_eq!(align_of::<u64>(), 8);
        assert_eq!(align_of::<u128>(), 16);
        assert_call_local_job_stored_once::<u8, 4096>();
        assert_call_local_job_stored_once::<u64, 4096>();
        assert_call_local_job_stored_once::<u128, 4096>();
        // The size of the roxygen_explicit job future before
        // `repair.explicit_exchange_layout`.
        assert_call_local_job_stored_once::<u8, 10_960>();
        assert_call_local_job_stored_once::<u64, 10_960>();
        assert_call_local_job_stored_once::<u128, 10_960>();
    }

    fn call_local_in_place_when_completed(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(&worker.handle, false, probe.job(JobPlan::ready_after(1)));
        assert!(worker.turn().is_pending());
        assert!(worker.turn().is_pending());
        assert_eq!(call_result(&mut call), Ok(7));
        probe.assert_in_place_once(3);
    }

    fn call_local_in_place_when_dropped_pending(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(&worker.handle, false, probe.job(JobPlan::NEVER));
        assert!(worker.turn().is_pending());
        assert!(worker.handle.begin_shutdown());
        assert!(worker.turn().is_ready());
        assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        probe.assert_in_place_once(2);
    }

    fn call_local_in_place_when_poll_panics(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(&worker.handle, false, probe.job(JobPlan::panic_after(1)));
        assert!(worker.turn().is_pending());
        let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
            .expect_err("the job's poll panic propagates");
        worker.assert_job_panic(payload, "call_local job poll panic", WorkerState::Open);
        assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        probe.assert_in_place_once(3);
    }

    /// When the job future's drop unwinds after the future returns its output,
    /// that output is never dropped, as in an async block that awaits the
    /// future and then sends the output.
    fn call_local_drop_panic_after_completion(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let plan = JobPlan::ready_after(0).panicking_on_drop();
        let mut call = probe.call(&worker.handle, false, probe.job(plan));
        let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
            .expect_err("the job future's drop panic propagates");
        let steps = [
            CallStep::JobDropped {
                panicking: false,
                command: CommandSpan::Open,
            },
            CallStep::CallerWoke {
                panicking: true,
                command: CommandSpan::Open,
            },
        ];
        assert_eq!(probe.steps(), steps, "{kind:?}");
        worker.assert_job_panic(payload, "call_local job drop panic", WorkerState::Open);
        assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        probe.assert_in_place_once(2);
        drop(worker);
        assert_eq!(probe.steps(), steps, "{kind:?}");
    }

    fn call_local_drop_panic_while_pending(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(
            &worker.handle,
            false,
            probe.job(JobPlan::NEVER.panicking_on_drop()),
        );
        assert!(worker.turn().is_pending());
        assert!(worker.handle.begin_shutdown());
        let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
            .expect_err("the job future's drop panic propagates");
        assert_eq!(
            probe.steps(),
            [
                CallStep::JobDropped {
                    panicking: false,
                    command: CommandSpan::Open,
                },
                CallStep::CallerWoke {
                    panicking: true,
                    command: CommandSpan::Open,
                },
            ],
            "{kind:?}"
        );
        worker.assert_job_panic(payload, "call_local job drop panic", WorkerState::Closed);
        assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        probe.assert_in_place_once(2);
    }

    fn call_local_caller_wake_panic_on_send(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(&worker.handle, true, probe.job(JobPlan::ready_after(0)));
        let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
            .expect_err("the caller's wake panic propagates");
        assert_eq!(
            probe.steps(),
            [
                CallStep::JobDropped {
                    panicking: false,
                    command: CommandSpan::Open,
                },
                CallStep::CallerWoke {
                    panicking: false,
                    command: CommandSpan::Open,
                },
            ],
            "{kind:?}"
        );
        worker.assert_job_panic(payload, "call_local caller waker panic", WorkerState::Open);
        assert_eq!(call_result(&mut call), Ok(7));
        probe.assert_in_place_once(2);
    }

    fn call_local_caller_wake_panic_unpolled(kind: CallDriver) {
        let mut worker = CallWorker::start(kind);
        let probe = CallProbe::new();
        let mut call = probe.call(&worker.handle, true, probe.job(JobPlan::ready_after(0)));
        shut_down_in_next_factory(&worker.handle);
        let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
            .expect_err("the caller's wake panic propagates");
        assert_eq!(
            probe.steps(),
            [
                CallStep::CallerWoke {
                    panicking: false,
                    command: CommandSpan::Open,
                },
                CallStep::JobDropped {
                    panicking: true,
                    command: CommandSpan::Open,
                },
            ],
            "{kind:?}"
        );
        worker.assert_job_panic(
            payload,
            "call_local caller waker panic",
            WorkerState::Closed,
        );
        assert_eq!(call_result(&mut call), Err(WorkerCallError::Cancelled));
        probe.assert_in_place_once(1);
    }

    #[test]
    fn call_local_job_keeps_its_future_in_place_and_cleans_up_once() {
        for kind in CALL_DRIVERS {
            call_local_in_place_when_completed(kind);
            call_local_in_place_when_dropped_pending(kind);
            call_local_in_place_when_poll_panics(kind);
            call_local_drop_panic_after_completion(kind);
            call_local_drop_panic_while_pending(kind);
            call_local_caller_wake_panic_on_send(kind);
            call_local_caller_wake_panic_unpolled(kind);
        }
    }

    impl CallWorker {
        /// The driver's owner-local context; the test thread is the owner.
        fn context(&self) -> WorkerContext {
            self.driver.context().clone()
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RegistryStep {
        /// A cleanup callback ran: in this worker state, on the owner thread
        /// or not, and inside the request context entered when it registered
        /// or not.
        Ran {
            id: usize,
            state: WorkerState,
            owner: bool,
            registered_context: bool,
        },
        CapturesDropped(usize),
        Disarmed {
            id: usize,
            removed: bool,
        },
    }

    /// The run of a callback that the shutdown drain takes.
    const fn drained(id: usize) -> RegistryStep {
        RegistryStep::Ran {
            id,
            state: WorkerState::Closing,
            owner: true,
            registered_context: true,
        }
    }

    /// The log of one cleanup registry oracle.
    #[derive(Clone)]
    struct RegistryProbe {
        owner: ThreadId,
        handle: WorkerHandle,
        steps: Arc<Mutex<Vec<RegistryStep>>>,
    }

    impl RegistryProbe {
        fn new(handle: &WorkerHandle) -> Self {
            Self {
                owner: std::thread::current().id(),
                handle: handle.clone(),
                steps: Arc::default(),
            }
        }

        fn record(&self, step: RegistryStep) {
            self.steps.lock().unwrap().push(step);
        }

        fn steps(&self) -> Vec<RegistryStep> {
            self.steps.lock().unwrap().clone()
        }

        fn register(&self, context: &WorkerContext, id: usize) -> WorkerCleanupRegistration {
            self.register_inner(context, id, || {}, None)
        }

        fn register_then<F>(
            &self,
            context: &WorkerContext,
            id: usize,
            then: F,
        ) -> WorkerCleanupRegistration
        where
            F: FnOnce() + 'static,
        {
            self.register_inner(context, id, then, None)
        }

        fn register_dropping<F>(
            &self,
            context: &WorkerContext,
            id: usize,
            on_drop: F,
        ) -> WorkerCleanupRegistration
        where
            F: FnOnce() + 'static,
        {
            self.register_inner(context, id, || {}, Some(Box::new(on_drop)))
        }

        /// Registers cleanup `id` inside a new request context. Its callback
        /// records its run, calls `then`, and then drops its captures, which
        /// record their drop and then call `on_drop`.
        fn register_inner<F>(
            &self,
            context: &WorkerContext,
            id: usize,
            then: F,
            on_drop: Option<Box<dyn FnOnce()>>,
        ) -> WorkerCleanupRegistration
        where
            F: FnOnce() + 'static,
        {
            let request = RuntimeContext::new();
            let _entered = request.enter();
            let captures = RegistryCaptures {
                id,
                probe: self.clone(),
                on_drop,
            };
            let probe = self.clone();
            context
                .register_cleanup(move || {
                    let captures = captures;
                    probe.record(RegistryStep::Ran {
                        id,
                        state: probe.handle.state(),
                        owner: std::thread::current().id() == probe.owner,
                        registered_context: RuntimeContext::current() == Some(request),
                    });
                    then();
                    drop(captures);
                })
                .expect("cleanup registers on the owner while the worker is open")
        }
    }

    /// The only part of a probe's callback whose drop is observable.
    struct RegistryCaptures {
        id: usize,
        probe: RegistryProbe,
        on_drop: Option<Box<dyn FnOnce()>>,
    }

    impl Drop for RegistryCaptures {
        fn drop(&mut self) {
            self.probe.record(RegistryStep::CapturesDropped(self.id));
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }

    #[test]
    fn cleanup_drain_runs_armed_callbacks_once_in_registration_order() {
        use RegistryStep::CapturesDropped;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            let mut registrations: Vec<_> = (0..8)
                .map(|id| {
                    let panic = match id {
                        1 => Some("first registry cleanup panic"),
                        4 => Some("second registry cleanup panic"),
                        _ => None,
                    };
                    Some(probe.register_then(&context, id, move || {
                        if let Some(message) = panic {
                            std::panic::panic_any(message);
                        }
                    }))
                })
                .collect();
            let disarm = |registrations: &mut [Option<WorkerCleanupRegistration>],
                          ids: [usize; 5]| {
                for id in ids {
                    let registration = registrations[id].take().expect("armed registration");
                    assert_eq!(registration.disarm(), Ok(true), "{kind:?}: cleanup {id}");
                    assert_eq!(
                        probe.steps().last(),
                        Some(&CapturesDropped(id)),
                        "{kind:?}: disarm dropped cleanup {id} before returning"
                    );
                }
            };

            // The first, a middle one, two adjacent ones, and the last.
            disarm(&mut registrations, [6, 0, 7, 3, 5]);
            // These follow the freed ones, so a store that reuses freed slots
            // and drains in slot order fails. With the three still armed they
            // exceed one B-tree leaf (11 entries).
            registrations.extend((8..20).map(|id| Some(probe.register(&context, id))));
            drop(context);
            disarm(&mut registrations, [13, 8, 19, 10, 11]);
            drop(registrations[1].take());
            drop(registrations[4].take());
            let retained = registrations[2].take().expect("armed registration");

            assert!(worker.handle.begin_shutdown());
            let payload = catch_unwind(AssertUnwindSafe(|| worker.turn()))
                .expect_err("the first cleanup panic propagates after the drain");
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"first registry cleanup panic"),
                "{kind:?}"
            );
            assert_eq!(worker.handle.state(), WorkerState::Closed, "{kind:?}");
            assert_eq!(RuntimeContext::current(), None, "{kind:?}");
            let survivors = [1, 2, 4, 9, 12, 14, 15, 16, 17, 18]
                .into_iter()
                .flat_map(|id| [drained(id), CapturesDropped(id)]);
            let steps: Vec<_> = [6, 0, 7, 3, 5, 13, 8, 19, 10, 11]
                .map(CapturesDropped)
                .into_iter()
                .chain(survivors)
                .collect();
            assert_eq!(probe.steps(), steps, "{kind:?}");
            assert_eq!(retained.disarm(), Ok(false), "{kind:?}");
            assert_eq!(probe.steps(), steps, "{kind:?}");
        }
    }

    #[test]
    fn cleanup_disarm_reports_armed_drained_and_retired_registrations() {
        use RegistryStep::CapturesDropped;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            let armed = probe.register(&context, 0);
            let taken = probe.register(&context, 1);
            drop(context);

            assert_eq!(armed.disarm(), Ok(true), "{kind:?}");
            assert_eq!(
                probe.steps(),
                [CapturesDropped(0)],
                "{kind:?}: disarm dropped the callback without running it"
            );
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            let steps = [CapturesDropped(0), drained(1), CapturesDropped(1)];
            assert_eq!(probe.steps(), steps, "{kind:?}");
            assert_eq!(taken.disarm(), Ok(false), "{kind:?}: the drain took it");
            assert_eq!(probe.steps(), steps, "{kind:?}");

            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let retired = probe.register(&worker.context(), 0);
            drop(worker);
            let steps = [drained(0), CapturesDropped(0)];
            assert_eq!(probe.steps(), steps, "{kind:?}");
            assert_eq!(
                retired.disarm(),
                Ok(false),
                "{kind:?}: the owner state is gone"
            );
            assert_eq!(probe.steps(), steps, "{kind:?}");
        }
    }

    #[test]
    fn cleanup_disarmed_by_a_running_cleanup_still_runs() {
        use RegistryStep::CapturesDropped;
        use RegistryStep::Disarmed;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            let earlier = probe.register(&context, 0);
            let later: Rc<RefCell<Option<WorkerCleanupRegistration>>> = Rc::default();
            let disarming = probe.register_then(&context, 1, {
                let probe = probe.clone();
                let later = Rc::clone(&later);
                move || {
                    let later = later.take().expect("later registration");
                    for (id, registration) in [(0, earlier), (2, later)] {
                        let removed = registration.disarm().expect("owner-local disarm");
                        probe.record(Disarmed { id, removed });
                    }
                }
            });
            later.replace(Some(probe.register(&context, 2)));
            drop(context);
            drop(disarming);

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                probe.steps(),
                [
                    drained(0),
                    CapturesDropped(0),
                    drained(1),
                    Disarmed {
                        id: 0,
                        removed: false
                    },
                    Disarmed {
                        id: 2,
                        removed: false
                    },
                    CapturesDropped(1),
                    drained(2),
                    CapturesDropped(2),
                ],
                "{kind:?}"
            );
        }
    }

    #[test]
    fn disarmed_callback_drop_may_register_and_disarm() {
        use RegistryStep::CapturesDropped;
        use RegistryStep::Disarmed;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            let victim = probe.register(&context, 0);
            let dropping = probe.register_dropping(&context, 1, {
                let probe = probe.clone();
                let context = context.clone();
                move || {
                    drop(probe.register(&context, 2));
                    let removed = victim.disarm().expect("owner-local disarm");
                    probe.record(Disarmed { id: 0, removed });
                }
            });
            drop(context);

            assert_eq!(dropping.disarm(), Ok(true), "{kind:?}");
            let steps = [
                CapturesDropped(1),
                CapturesDropped(0),
                Disarmed {
                    id: 0,
                    removed: true,
                },
            ];
            assert_eq!(probe.steps(), steps, "{kind:?}");
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                probe.steps()[steps.len()..],
                [drained(2), CapturesDropped(2)],
                "{kind:?}"
            );
        }
    }

    /// Disarms its registration when dropped.
    struct DisarmOnDrop {
        id: usize,
        registration: Option<WorkerCleanupRegistration>,
        probe: RegistryProbe,
    }

    impl Drop for DisarmOnDrop {
        fn drop(&mut self) {
            let registration = self.registration.take().expect("armed registration");
            let removed = registration.disarm().expect("owner-local disarm");
            self.probe.record(RegistryStep::Disarmed {
                id: self.id,
                removed,
            });
        }
    }

    #[test]
    fn task_dropped_by_shutdown_disarms_its_cleanup_before_the_drain() {
        use RegistryStep::CapturesDropped;
        use RegistryStep::Disarmed;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let probe = RegistryProbe::new(&worker.handle);
            let task_probe = probe.clone();
            worker
                .handle
                .spawn_local(move |context| async move {
                    drop(task_probe.register(&context, 0));
                    let _disarm_on_drop = DisarmOnDrop {
                        id: 1,
                        registration: Some(task_probe.register(&context, 1)),
                        probe: task_probe,
                    };
                    pending::<()>().await;
                })
                .expect("task admitted");
            assert!(worker.turn().is_pending());
            assert!(probe.steps().is_empty(), "{kind:?}: the task is pending");

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                probe.steps(),
                [
                    CapturesDropped(1),
                    Disarmed {
                        id: 1,
                        removed: true
                    },
                    drained(0),
                    CapturesDropped(0),
                ],
                "{kind:?}"
            );
        }
    }

    /// After every step the armed count equals the armed registrations, and
    /// the stored slots equal the most registrations ever armed at once.
    #[test]
    fn cleanup_registry_stores_only_armed_registrations() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            drop(probe.register(&context, 0));
            assert_eq!(context.armed_cleanup_entries(), 1, "{kind:?}");
            assert_eq!(context.stored_cleanup_entries(), 1, "{kind:?}");

            for cycle in 0..10_000 {
                let registration = context.register_cleanup(|| {}).expect("cleanup registers");
                assert_eq!(context.armed_cleanup_entries(), 2, "{kind:?}: {cycle}");
                assert_eq!(context.stored_cleanup_entries(), 2, "{kind:?}: {cycle}");
                assert_eq!(registration.disarm(), Ok(true), "{kind:?}: {cycle}");
                assert_eq!(context.armed_cleanup_entries(), 1, "{kind:?}: {cycle}");
                assert_eq!(context.stored_cleanup_entries(), 2, "{kind:?}: {cycle}");
            }

            let mut live: Vec<_> = (0..1_000)
                .map(|index| {
                    let registration = context.register_cleanup(|| {}).expect("cleanup registers");
                    assert_eq!(context.armed_cleanup_entries(), index + 2, "{kind:?}");
                    assert_eq!(context.stored_cleanup_entries(), index + 2, "{kind:?}");
                    Some(registration)
                })
                .collect();
            // 919 is coprime to 1,000, so this retires each live registration
            // once, out of registration order.
            for (retired, index) in (0..1_000).map(|step| step * 919 % 1_000).enumerate() {
                let registration = live[index].take().expect("retired once");
                assert_eq!(registration.disarm(), Ok(true), "{kind:?}: {index}");
                assert_eq!(
                    context.armed_cleanup_entries(),
                    1_000 - retired,
                    "{kind:?}: {index}"
                );
                assert_eq!(context.stored_cleanup_entries(), 1_001, "{kind:?}: {index}");
            }

            // A second wave fills the freed slots without growing the store.
            let wave: Vec<_> = (0..1_000)
                .map(|index| {
                    let registration = context.register_cleanup(|| {}).expect("cleanup registers");
                    assert_eq!(context.armed_cleanup_entries(), index + 2, "{kind:?}");
                    assert_eq!(context.stored_cleanup_entries(), 1_001, "{kind:?}");
                    registration
                })
                .collect();
            for (retired, registration) in wave.into_iter().enumerate() {
                assert_eq!(registration.disarm(), Ok(true), "{kind:?}: {retired}");
                assert_eq!(
                    context.armed_cleanup_entries(),
                    1_000 - retired,
                    "{kind:?}: {retired}"
                );
                assert_eq!(
                    context.stored_cleanup_entries(),
                    1_001,
                    "{kind:?}: {retired}"
                );
            }
            drop(context);

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                probe.steps(),
                [drained(0), RegistryStep::CapturesDropped(0)],
                "{kind:?}"
            );
        }
    }

    impl CallWorker {
        /// `start` with a command queue that takes `capacity` commands.
        fn with_capacity(kind: CallDriver, capacity: usize) -> Self {
            let host = match kind {
                CallDriver::Owner => None,
                CallDriver::Hosted => Some(ManualHost::default()),
            };
            let (handle, driver) = match &host {
                None => WorkerDriver::new(capacity),
                Some(host) => WorkerDriver::with_host(capacity, Host::new(Rc::new(host.clone()))),
            }
            .expect("worker driver starts");
            Self {
                kind,
                handle,
                driver,
                host,
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum OrderStep {
        Polled(usize),
        Dropped(usize),
    }

    /// The owner-thread log of the `OrderFuture`s that one oracle registers.
    #[derive(Clone, Default)]
    struct OrderLog(Rc<RefCell<Vec<OrderStep>>>);

    impl OrderLog {
        fn steps(&self) -> Vec<OrderStep> {
            self.0.borrow().clone()
        }

        fn future(&self, id: usize, ready: bool) -> OrderFuture {
            OrderFuture {
                id,
                ready,
                log: self.clone(),
            }
        }

        fn reserve(&self, context: &WorkerContext, id: usize) -> WorkerCleanupTaskRegistration {
            context
                .register_cleanup_task(self.future(id, true))
                .expect("cleanup reserved on the owner while the worker is open")
        }
    }

    /// A cleanup future that logs each poll and its drop.
    struct OrderFuture {
        id: usize,
        ready: bool,
        log: OrderLog,
    }

    impl Future for OrderFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.log.0.borrow_mut().push(OrderStep::Polled(self.id));
            if self.ready {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for OrderFuture {
        fn drop(&mut self) {
            self.log.0.borrow_mut().push(OrderStep::Dropped(self.id));
        }
    }

    #[test]
    fn stale_cleanup_disarm_after_slot_reuse_returns_false() {
        use RegistryStep::CapturesDropped;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RegistryProbe::new(&worker.handle);
            let context = worker.context();
            let first = probe.register(&context, 0);
            let kept = probe.register(&context, 1);
            let stale = first.stale_copy();
            assert_eq!(first.disarm(), Ok(true), "{kind:?}");
            // Cleanup 2 takes the slot that disarming cleanup 0 freed.
            let reusing = probe.register(&context, 2);
            assert_eq!(
                stale.disarm(),
                Ok(false),
                "{kind:?}: a stale registration removes nothing"
            );
            assert_eq!(context.armed_cleanup_entries(), 2, "{kind:?}");
            drop(context);
            drop((kept, reusing));

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                probe.steps(),
                [
                    CapturesDropped(0),
                    drained(1),
                    CapturesDropped(1),
                    drained(2),
                    CapturesDropped(2),
                ],
                "{kind:?}: the reusing registration stayed armed"
            );
        }
    }

    #[test]
    fn hosted_reserved_cleanups_activate_in_registration_order_after_slot_reuse() {
        use OrderStep::Dropped;
        use OrderStep::Polled;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let log = OrderLog::default();
            let context = worker.context();
            let mut reserved: Vec<_> = (0..3).map(|id| log.reserve(&context, id)).collect();
            assert_eq!(reserved.remove(1).disarm(), Ok(true), "{kind:?}");
            // Cleanup 3 takes the slot that disarming cleanup 1 freed, so
            // slot order is 0, 3, 2.
            reserved.push(log.reserve(&context, 3));
            drop(context);
            assert_eq!(log.steps(), [Dropped(1)], "{kind:?}");

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                log.steps(),
                [
                    Dropped(1),
                    Polled(0),
                    Dropped(0),
                    Polled(2),
                    Dropped(2),
                    Polled(3),
                    Dropped(3),
                ],
                "{kind:?}: shutdown activates reserved cleanups in registration order"
            );
            drop(reserved);
        }
    }

    #[test]
    fn abort_cleanup_tasks_drops_reserved_cleanups_in_registration_order() {
        use OrderStep::Dropped;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let log = OrderLog::default();
            let context = worker.context();
            let mut reserved: Vec<_> = (0..4).map(|id| Some(log.reserve(&context, id))).collect();
            for id in [1, 2] {
                let registration = reserved[id].take().expect("reserved cleanup");
                assert_eq!(registration.disarm(), Ok(true), "{kind:?}: {id}");
            }
            // Cleanups 4 and 5 take the freed slots, 2's and then 1's.
            reserved.extend((4..6).map(|id| Some(log.reserve(&context, id))));
            let observers: Vec<_> = reserved
                .iter()
                .flatten()
                .map(WorkerCleanupTaskRegistration::observe)
                .collect();
            drop(context);

            drop(worker);
            assert_eq!(
                log.steps(),
                [1, 2, 0, 3, 4, 5].map(Dropped),
                "{kind:?}: forced shutdown drops reserved cleanups in registration order"
            );
            for observer in &observers {
                assert_eq!(retirement_result(observer), Poll::Ready(Ok(())), "{kind:?}");
            }
            drop(reserved);
        }
    }

    #[test]
    fn repeated_or_stale_tracked_cleanup_finish_leaves_the_reusing_registration_intact() {
        use OrderStep::Dropped;
        use OrderStep::Polled;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let log = OrderLog::default();
            let context = worker.context();
            let first = log.reserve(&context, 0);
            let finish_again = first.finish_hook();
            let finish_late = first.finish_hook();
            assert_eq!(first.disarm(), Ok(true), "{kind:?}");

            // Each reservation below takes the slot that cleanup 0 left.
            let second = log.reserve(&context, 1);
            finish_again();
            assert_eq!(
                second.disarm(),
                Ok(true),
                "{kind:?}: finishing cleanup 0 again removed nothing"
            );
            let third = log.reserve(&context, 2);
            finish_late();
            drop(context);

            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(
                log.steps(),
                [Dropped(0), Dropped(1), Polled(2), Dropped(2)],
                "{kind:?}: the reusing cleanup drained exactly once"
            );
            drop(third);
        }
    }

    #[test]
    fn tracked_cleanup_finish_during_abort_does_not_panic() {
        use OrderStep::Dropped;
        use OrderStep::Polled;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let log = OrderLog::default();
            let context = worker.context();
            let mut reserved: Vec<_> = (0..3)
                .map(|id| {
                    context
                        .register_cleanup_task(log.future(id, false))
                        .expect("cleanup reserved")
                })
                .collect();
            assert_eq!(reserved.remove(0).disarm(), Ok(true), "{kind:?}");
            reserved.push(
                context
                    .register_cleanup_task(log.future(3, false))
                    .expect("cleanup reserved"),
            );
            let active = context
                .register_cleanup_task(log.future(4, false))
                .expect("cleanup reserved")
                .activate()
                .expect("cleanup activated");
            let observers: Vec<_> = reserved
                .iter()
                .map(WorkerCleanupTaskRegistration::observe)
                .chain([active.observe()])
                .collect();
            drop(context);
            assert!(worker.turn().is_pending(), "{kind:?}");
            assert_eq!(log.steps(), [Dropped(0), Polled(4)], "{kind:?}");

            // Each dropped cleanup finishes against the registry that the
            // forced shutdown already took.
            drop(worker);
            let retired = match kind {
                CallDriver::Owner => [Dropped(1), Dropped(2), Dropped(3), Dropped(4)],
                CallDriver::Hosted => [Dropped(4), Dropped(1), Dropped(2), Dropped(3)],
            };
            assert_eq!(log.steps()[2..], retired, "{kind:?}");
            for observer in &observers {
                assert_eq!(retirement_result(observer), Poll::Ready(Ok(())), "{kind:?}");
            }
            assert!(active.is_finished(), "{kind:?}");
            drop(reserved);
        }
    }

    thread_local! {
        static SPAN_COMPLETION_PANIC: Cell<bool> = const { Cell::new(false) };
    }

    /// Panics at the first span completion after `SPAN_COMPLETION_PANIC` is set.
    fn panic_at_armed_span_completion(
        _: crate::context::SpanLink,
        _: TerminalOutcome,
        _: Option<&SpanObserver>,
    ) {
        if SPAN_COMPLETION_PANIC.replace(false) {
            std::panic::panic_any("armed span completion panic");
        }
    }

    #[test]
    fn undrained_cleanups_drop_in_registration_order_with_the_owner_state() {
        use RegistryStep::CapturesDropped;

        let mut worker = CallWorker::start(CallDriver::Hosted);
        assert!(worker.turn().is_pending());
        let probe = RegistryProbe::new(&worker.handle);
        let context = worker.context();
        let mut registrations: Vec<_> = (0..6)
            .map(|id| Some(probe.register(&context, id)))
            .collect();
        for id in [1, 3] {
            let registration = registrations[id].take().expect("armed registration");
            assert_eq!(registration.disarm(), Ok(true));
        }
        // Cleanups 6 and 7 take the freed slots, 3's and then 1's.
        registrations.extend((6..8).map(|id| Some(probe.register(&context, id))));
        drop(context);
        drop(registrations);

        // The hosted driver's start_shutdown closes its command group before
        // the drain; that group's shutdown span completes first and panics.
        let previous =
            crate::observability::set_span_completion_hook(Some(panic_at_armed_span_completion));
        SPAN_COMPLETION_PANIC.set(true);
        assert!(worker.handle.begin_shutdown());
        let started = catch_unwind(AssertUnwindSafe(|| {
            poll_retirement_driver(&mut worker.driver)
        }));
        SPAN_COMPLETION_PANIC.set(false);
        crate::observability::set_span_completion_hook(previous);
        let payload = started.expect_err("start_shutdown panics before draining cleanups");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"armed span completion panic")
        );
        assert_eq!(probe.steps(), [CapturesDropped(1), CapturesDropped(3)]);

        drop(worker);
        assert_eq!(
            probe.steps(),
            [1, 3, 0, 2, 4, 5, 6, 7].map(CapturesDropped),
            "the undrained cleanups drop with the owner state in registration order"
        );
    }

    #[test]
    fn hosted_cleanup_slots_stay_bounded_by_the_peak_armed_count() {
        use OrderStep::Dropped;
        use OrderStep::Polled;

        let mut worker = CallWorker::start(CallDriver::Hosted);
        assert!(worker.turn().is_pending());
        let log = OrderLog::default();
        let context = worker.context();
        let reserve = || {
            context
                .register_cleanup_task(std::future::ready(()))
                .expect("cleanup reserved")
        };
        let long_lived = log.reserve(&context, 0);
        assert_eq!(context.armed_hosted_cleanups(), 1);
        assert_eq!(context.stored_hosted_cleanups(), 1);

        for cycle in 0..10_000 {
            let registration = reserve();
            assert_eq!(context.armed_hosted_cleanups(), 2, "{cycle}");
            assert_eq!(context.stored_hosted_cleanups(), 2, "{cycle}");
            assert_eq!(registration.disarm(), Ok(true), "{cycle}");
            assert_eq!(context.armed_hosted_cleanups(), 1, "{cycle}");
            assert_eq!(context.stored_hosted_cleanups(), 2, "{cycle}");
        }

        let mut live: Vec<_> = (0..1_000)
            .map(|index| {
                let registration = reserve();
                assert_eq!(context.armed_hosted_cleanups(), index + 2);
                assert_eq!(context.stored_hosted_cleanups(), index + 2);
                Some(registration)
            })
            .collect();
        for (retired, index) in (0..1_000).map(|step| step * 919 % 1_000).enumerate() {
            let registration = live[index].take().expect("retired once");
            assert_eq!(registration.disarm(), Ok(true), "{index}");
            assert_eq!(context.armed_hosted_cleanups(), 1_000 - retired, "{index}");
            assert_eq!(context.stored_hosted_cleanups(), 1_001, "{index}");
        }

        let wave: Vec<_> = (0..1_000)
            .map(|index| {
                let registration = reserve();
                assert_eq!(context.armed_hosted_cleanups(), index + 2);
                assert_eq!(context.stored_hosted_cleanups(), 1_001);
                registration
            })
            .collect();
        for (retired, registration) in wave.into_iter().enumerate() {
            assert_eq!(registration.disarm(), Ok(true), "{retired}");
            assert_eq!(
                context.armed_hosted_cleanups(),
                1_000 - retired,
                "{retired}"
            );
            assert_eq!(context.stored_hosted_cleanups(), 1_001, "{retired}");
        }
        drop(context);

        assert!(worker.handle.begin_shutdown());
        assert!(worker.turn().is_ready());
        assert_eq!(log.steps(), [Polled(0), Dropped(0)]);
        drop(long_lived);
    }

    #[test]
    fn driver_admits_commands_in_fifo_order_across_bursts() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::with_capacity(kind, 64);
            let ran = Arc::new(Mutex::new(Vec::new()));
            let mut submitted = 0;
            // The bursts of 40 and 57 outgrow the intake the driver kept.
            for burst in [3, 40, 1, 57] {
                for _ in 0..burst {
                    let ran = Arc::clone(&ran);
                    let index = submitted;
                    submitted += 1;
                    worker
                        .handle
                        .submit(move |_| ran.lock().unwrap().push(index))
                        .expect("command admitted");
                }
                assert!(worker.turn().is_pending(), "{kind:?}");
                assert_eq!(
                    *ran.lock().unwrap(),
                    (0..submitted).collect::<Vec<_>>(),
                    "{kind:?}: burst of {burst}"
                );
            }
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
        }
    }

    /// Counts a command's admissions (its factory ran), runs, and unrun drops.
    #[derive(Clone, Default)]
    struct CommandCounters {
        admitted: Arc<AtomicUsize>,
        ran: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    impl CommandCounters {
        fn get(&self) -> (usize, usize, usize) {
            (
                self.admitted.load(Ordering::SeqCst),
                self.ran.load(Ordering::SeqCst),
                self.dropped.load(Ordering::SeqCst),
            )
        }

        fn submit(&self, handle: &WorkerHandle) -> Result<(), SubmitError> {
            let guard = CommandGuard {
                counters: self.clone(),
                done: false,
            };
            let admitted = Arc::clone(&self.admitted);
            handle.spawn_local(move |_| {
                admitted.fetch_add(1, Ordering::SeqCst);
                async move { guard.run() }
            })
        }
    }

    /// Records whether its command ran before the guard was dropped.
    struct CommandGuard {
        counters: CommandCounters,
        done: bool,
    }

    impl CommandGuard {
        fn run(mut self) {
            self.done = true;
            self.counters.ran.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Drop for CommandGuard {
        fn drop(&mut self) {
            if !self.done {
                self.counters.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl CallWorker {
        /// `turn`, with each driver poll made under `waker`.
        fn turn_with(&mut self, waker: &Waker) -> Poll<()> {
            let mut context = Context::from_waker(waker);
            let polled = Pin::new(&mut self.driver).poll(&mut context);
            let Some(host) = &self.host else {
                return polled;
            };
            host.run();
            Pin::new(&mut self.driver).poll(&mut context)
        }
    }

    #[test]
    fn driver_pass_after_the_lifecycle_leaves_open_admits_no_command() {
        for kind in CALL_DRIVERS {
            // A pass that starts after the lifecycle left Open drops the
            // queued commands through shutdown and admits none.
            let mut worker = CallWorker::with_capacity(kind, 8);
            let counters = CommandCounters::default();
            for _ in 0..3 {
                counters.submit(&worker.handle).expect("command admitted");
            }
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
            assert_eq!(counters.get(), (0, 0, 3), "{kind:?}");
            assert_eq!(worker.handle.outstanding_work(), 0, "{kind:?}");

            // The lifecycle leaves Open inside one pass, after its Open
            // pre-check and before it takes the queue: that pass takes no
            // command, reaches its non-Open exit, and closes the worker,
            // dropping the queued commands without running their factories.
            let mut worker = CallWorker::with_capacity(kind, 8);
            assert!(worker.turn().is_pending(), "{kind:?}");
            let counters = CommandCounters::default();
            for _ in 0..3 {
                counters.submit(&worker.handle).expect("command admitted");
            }
            let previous = crate::worker::set_driver_intake_hook(Some(|handle: &WorkerHandle| {
                assert!(handle.begin_shutdown(), "the hook runs on an Open pass");
            }));
            let pass = catch_unwind(AssertUnwindSafe(|| {
                poll_retirement_driver(&mut worker.driver)
            }));
            crate::worker::set_driver_intake_hook(previous);
            assert_eq!(pass.ok(), Some(Poll::Ready(())), "{kind:?}");
            assert_eq!(worker.handle.state(), WorkerState::Closed, "{kind:?}");
            assert_eq!(counters.get(), (0, 0, 3), "{kind:?}");
            assert_eq!(worker.handle.outstanding_work(), 0, "{kind:?}");

            // Another thread ends the lifecycle at an arbitrary point of the
            // driver's passes, and the driver is polled again only after a
            // wake: the worker still closes, and every command is either run
            // or dropped by then.
            for round in 0..200 {
                let mut worker = CallWorker::with_capacity(kind, 64);
                let counters = CommandCounters::default();
                let submitter = {
                    let handle = worker.handle.clone();
                    let counters = counters.clone();
                    std::thread::spawn(move || {
                        for _ in 0..32 {
                            if counters.submit(&handle).is_err() {
                                break;
                            }
                            if round % 4 == 0 {
                                std::thread::yield_now();
                            }
                        }
                        handle.begin_shutdown();
                    })
                };
                let (waker, wakes) = retirement_waker(RetirementWakePanic::None);
                let deadline = Instant::now() + std::time::Duration::from_secs(30);
                let mut seen = wakes.load(Ordering::SeqCst);
                while worker.turn_with(&waker).is_pending() {
                    while wakes.load(Ordering::SeqCst) == seen {
                        assert!(
                            Instant::now() < deadline,
                            "{kind:?}: round {round}: the pending driver was never woken"
                        );
                        std::thread::yield_now();
                    }
                    seen = wakes.load(Ordering::SeqCst);
                }
                submitter.join().expect("the submitter exits");
                assert_eq!(worker.handle.state(), WorkerState::Closed);
                let (admitted, ran, dropped) = counters.get();
                assert!(admitted >= ran, "{kind:?}: round {round}");
                assert_eq!(ran + dropped, 32, "{kind:?}: round {round}");
                assert_eq!(
                    worker.handle.outstanding_work(),
                    0,
                    "{kind:?}: round {round}"
                );
            }
        }
    }

    #[test]
    fn cleanup_observe_after_publish_returns_the_sticky_outcome() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let log = OrderLog::default();
            let task = log
                .reserve(&worker.context(), 0)
                .activate()
                .expect("cleanup activated");
            assert!(worker.turn().is_pending(), "{kind:?}");
            assert!(task.is_finished(), "{kind:?}");
            for _ in 0..2 {
                assert_eq!(
                    retirement_result(&task.observe()),
                    Poll::Ready(Ok(())),
                    "{kind:?}"
                );
            }
            drop(task);
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");

            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let probe = RetirementProbe::new();
            probe.ready.store(true, Ordering::SeqCst);
            probe.panic_on_drop.store(true, Ordering::SeqCst);
            let task = retirement_task(&worker.context(), &probe);
            let failed = catch_unwind(AssertUnwindSafe(|| worker.turn()));
            assert!(
                failed.is_err(),
                "{kind:?}: the destruction panic reaches the driver"
            );
            assert!(task.is_finished(), "{kind:?}");
            for _ in 0..2 {
                assert_eq!(
                    retirement_result(&task.observe()),
                    Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed)),
                    "{kind:?}"
                );
            }
        }
    }

    /// Activates a `RetirementFuture` for `probe` and installs the observer
    /// that its destructor checks.
    fn retirement_task(context: &WorkerContext, probe: &RetirementProbe) -> WorkerCleanupTask {
        let registration = context
            .register_cleanup_task(RetirementFuture {
                probe: probe.clone(),
                _capture: RetirementCapture(probe.clone()),
                _not_send: Rc::new(()),
            })
            .expect("cleanup reserved");
        *probe.observer.lock().expect("probe observation") = Some(registration.observe());
        registration.activate().expect("cleanup activated")
    }

    #[test]
    fn cleanup_observers_before_and_after_publication_see_identical_outcomes() {
        for kind in CALL_DRIVERS {
            for destruction_panics in [false, true] {
                let mut worker = CallWorker::start(kind);
                assert!(worker.turn().is_pending());
                let probe = RetirementProbe::new();
                probe
                    .panic_on_drop
                    .store(destruction_panics, Ordering::SeqCst);
                let context = worker.context();
                let registration = context
                    .register_cleanup_task(RetirementFuture {
                        probe: probe.clone(),
                        _capture: RetirementCapture(probe.clone()),
                        _not_send: Rc::new(()),
                    })
                    .expect("cleanup reserved");
                let reserved = registration.observe();
                *probe.observer.lock().expect("probe observation") = Some(reserved.clone());
                let task = registration.activate().expect("cleanup activated");
                let active = task.observe();
                let cloned = active.clone();
                drop(context);
                assert!(worker.turn().is_pending(), "{kind:?}");
                assert!(retirement_result(&reserved).is_pending(), "{kind:?}");

                probe.ready.store(true, Ordering::SeqCst);
                let turned = catch_unwind(AssertUnwindSafe(|| worker.turn()));
                assert_eq!(turned.is_err(), destruction_panics, "{kind:?}");
                let expected = if destruction_panics {
                    Err(WorkerCleanupObservationError::Unconfirmed)
                } else {
                    Ok(())
                };
                let published = task.observe();
                for observer in [&reserved, &active, &cloned, &published] {
                    assert_eq!(
                        retirement_result(observer),
                        Poll::Ready(expected),
                        "{kind:?}: destruction panics {destruction_panics}"
                    );
                }
                drop(task);
                if !destruction_panics {
                    assert!(worker.handle.begin_shutdown());
                    assert!(worker.turn().is_ready(), "{kind:?}");
                }
            }
        }
    }

    thread_local! {
        static JOIN_CLONE_HOST: RefCell<Option<ManualHost>> = const { RefCell::new(None) };
    }

    /// A waker whose clone drops every task queued on `JOIN_CLONE_HOST`.
    fn task_dropping_waker() -> Waker {
        fn clone(_: *const ()) -> RawWaker {
            let tasks: Vec<_> = JOIN_CLONE_HOST.with_borrow(|host| {
                host.as_ref()
                    .map(|host| host.tasks.borrow_mut().drain(..).collect())
                    .unwrap_or_default()
            });
            drop(tasks);
            raw()
        }
        fn noop(_: *const ()) {}
        fn raw() -> RawWaker {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        // SAFETY: the vtable never dereferences its null data pointer and owns
        // no resources, so every clone is independently valid.
        unsafe { Waker::from_raw(raw()) }
    }

    #[test]
    fn join_waker_clone_that_finishes_a_task_does_not_panic() {
        let host = ManualHost::default();
        let mut group = TaskGroup::with_host(Host::new(Rc::new(host.clone())));
        let log = OrderLog::default();
        let id = group
            .spawn_local_tracked({
                let future = log.future(0, false);
                move |_| future
            })
            .expect("task admitted");
        JOIN_CLONE_HOST.set(Some(host));
        let waker = task_dropping_waker();
        let mut context = Context::from_waker(&waker);
        // Storing the joiner's waker clones it, and the clone drops the
        // unpolled host task, which finishes it inside this poll.
        let joined = catch_unwind(AssertUnwindSafe(|| group.poll_join_next(&mut context)));
        JOIN_CLONE_HOST.set(None);
        assert!(
            joined
                .expect("the joiner's waker clone may finish a task")
                .is_pending()
        );
        assert_eq!(log.steps(), [OrderStep::Dropped(0)]);
        let Poll::Ready(Some(completion)) = group.poll_join_next(&mut context) else {
            panic!("the finished task's completion is ready");
        };
        assert_eq!(completion.id, id);
        assert_eq!(completion.result, Err(TaskGroupError::TaskCancelled));
        assert!(group.close());
        assert!(group.is_empty());
    }

    #[test]
    fn unobserved_cleanup_registration_allocates_no_publisher_state() {
        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            assert!(worker.turn().is_pending());
            let context = worker.context();
            let before = crate::worker::cleanup_publisher_allocations();
            let unobserved = context
                .register_cleanup_task(std::future::ready(()))
                .expect("cleanup reserved");
            let scope = context
                .register_scope(std::future::ready(()))
                .expect("scope reserved");
            assert_eq!(
                crate::worker::cleanup_publisher_allocations(),
                before,
                "{kind:?}: registering allocates no publisher state"
            );
            assert_eq!(unobserved.disarm(), Ok(true), "{kind:?}");
            assert_eq!(scope.disarm(), Ok(true), "{kind:?}");
            assert_eq!(
                crate::worker::cleanup_publisher_allocations(),
                before,
                "{kind:?}: an unobserved retirement allocates nothing"
            );

            let observed = context
                .register_cleanup_task(std::future::ready(()))
                .expect("cleanup reserved");
            let first = observed.observe();
            assert_eq!(crate::worker::cleanup_publisher_allocations(), before + 1);
            let task = observed.activate().expect("cleanup activated");
            let later = [first.clone(), task.observe(), task.observe()];
            assert_eq!(
                crate::worker::cleanup_publisher_allocations(),
                before + 1,
                "{kind:?}: later observers share the first one's state"
            );
            drop(context);
            assert!(worker.turn().is_pending(), "{kind:?}");
            for observer in later.iter().chain([&first]) {
                assert_eq!(retirement_result(observer), Poll::Ready(Ok(())), "{kind:?}");
            }
            assert!(worker.handle.begin_shutdown());
            assert!(worker.turn().is_ready(), "{kind:?}");
        }
    }

    thread_local! {
        static DRIVER_WAKER_CLONES: Cell<usize> = const { Cell::new(0) };
    }

    /// A waker that counts its clones. Wakers built over the same `data`
    /// `will_wake` each other.
    fn clone_counting_waker(data: &'static u8) -> Waker {
        fn clone(data: *const ()) -> RawWaker {
            DRIVER_WAKER_CLONES.set(DRIVER_WAKER_CLONES.get() + 1);
            raw(data)
        }
        fn noop(_: *const ()) {}
        fn raw(data: *const ()) -> RawWaker {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
            RawWaker::new(data, &VTABLE)
        }
        // SAFETY: the vtable never dereferences its data pointer, which points
        // to a static, and owns no resources.
        unsafe { Waker::from_raw(raw(std::ptr::from_ref(data).cast())) }
    }

    #[test]
    fn driver_poll_clones_its_waker_only_when_it_changes() {
        static FIRST: u8 = 0;
        static SECOND: u8 = 0;

        for kind in CALL_DRIVERS {
            let mut worker = CallWorker::start(kind);
            let first = clone_counting_waker(&FIRST);
            let second = clone_counting_waker(&SECOND);
            let mut poll =
                |waker: &Waker| Pin::new(&mut worker.driver).poll(&mut Context::from_waker(waker));
            DRIVER_WAKER_CLONES.set(0);
            assert!(poll(&first).is_pending());
            assert_eq!(
                DRIVER_WAKER_CLONES.get(),
                1,
                "{kind:?}: the first pass stores one clone"
            );
            for _ in 0..3 {
                assert!(poll(&first).is_pending());
            }
            assert_eq!(
                DRIVER_WAKER_CLONES.get(),
                1,
                "{kind:?}: a stored waker that will_wake the new one is kept"
            );
            assert!(poll(&second).is_pending());
            assert_eq!(
                DRIVER_WAKER_CLONES.get(),
                2,
                "{kind:?}: a different waker replaces it"
            );
            // begin_shutdown takes the stored waker to wake it, so the
            // closing pass registers a fresh clone.
            assert!(worker.handle.begin_shutdown());
            assert!(poll(&second).is_ready(), "{kind:?}");
            assert_eq!(DRIVER_WAKER_CLONES.get(), 3, "{kind:?}");
        }
    }

    #[test]
    fn hosted_cleanups_run_without_a_boxed_task_future() {
        use OrderStep::Dropped;
        use OrderStep::Polled;

        let mut worker = CallWorker::start(CallDriver::Hosted);
        assert!(worker.turn().is_pending());
        let log = OrderLog::default();
        let before = crate::task_group::boxed_task_futures();
        let task = worker
            .context()
            .register_cleanup_task(log.future(0, false))
            .expect("cleanup reserved")
            .activate()
            .expect("cleanup activated");
        assert!(worker.turn().is_pending());
        assert_eq!(log.steps(), [Polled(0)]);
        assert_eq!(
            crate::task_group::boxed_task_futures(),
            before,
            "the activated cleanup runs inline in its task body"
        );

        let host = ManualHost::default();
        let mut group = TaskGroup::with_host(Host::new(Rc::new(host.clone())));
        group.spawn_local(|_| async {}).expect("task admitted");
        assert_eq!(
            crate::task_group::boxed_task_futures(),
            before + 1,
            "a task group future is boxed"
        );
        host.run();
        assert!(group.close());
        assert!(group.is_empty());

        drop(worker);
        assert_eq!(log.steps(), [Polled(0), Dropped(0)]);
        assert!(task.is_finished());
    }
}
