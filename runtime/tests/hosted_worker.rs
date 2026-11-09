//! Tokio-hosted `Send + Sync` worker oracles.

mod hosted_worker {
    use std::cell::RefCell;
    use std::future::Future;
    use std::future::pending;
    use std::future::poll_fn;
    use std::num::NonZeroUsize;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::process::Command;
    use std::process::Output;
    use std::process::Stdio;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;
    use std::time::Instant;

    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::sync::watch;

    use crate::host::Host;
    use crate::host::timer::Expiry;
    use crate::host::timer::Timeout;
    use crate::task_group::TaskGroup;
    use crate::task_group::TaskGroupError;
    use crate::tokio_host::HostedWorker;
    use crate::tokio_host::HostedWorkerBlockingPool;
    use crate::tokio_host::HostedWorkerJoin;
    use crate::tokio_host::HostedWorkerOptions;
    use crate::tokio_host::ManagedWorker;
    use crate::tokio_host::ManagedWorkerError;
    use crate::tokio_host::WakeGate;
    use crate::tokio_host::supervisor_polls_on_this_thread;
    use crate::worker::QuiesceDeadline;
    use crate::worker::SubmitError;
    use crate::worker::WorkerCallError;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerHandle;
    use crate::worker::WorkerQuiescing;
    use crate::worker::WorkerState;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);
    const NOTIFICATION_PANIC: &str = "injected shutdown notification panic";
    const OWNER_DROP_CHILD: &str = "ROXYGEN_HOSTED_WORKER_OWNER_DROP_CHILD";

    type Completion = watch::Receiver<Option<Result<(), ManagedWorkerError>>>;

    fn options(thread_name: &str, command_capacity: usize) -> HostedWorkerOptions {
        HostedWorkerOptions {
            command_capacity: NonZeroUsize::new(command_capacity).expect("nonzero capacity"),
            thread_name: thread_name.to_owned(),
            on_owner_thread_stop: None,
        }
    }

    /// Reports the thread that drops it.
    struct DropProbe(mpsc::Sender<ThreadId>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let _ = self.0.send(std::thread::current().id());
        }
    }

    /// Reports the dropping thread and whether the worker was cancelled by then.
    struct CancellationProbe {
        handle: WorkerHandle,
        report: mpsc::Sender<(ThreadId, bool)>,
    }

    impl Drop for CancellationProbe {
        fn drop(&mut self) {
            let _ = self
                .report
                .send((std::thread::current().id(), self.handle.is_cancelled()));
        }
    }

    /// Reports whether `completion()` was still unpublished when it was dropped.
    struct CompletionProbe {
        label: &'static str,
        completion: Completion,
        report: mpsc::Sender<(&'static str, bool, ThreadId)>,
    }

    impl Drop for CompletionProbe {
        fn drop(&mut self) {
            let unpublished = self.completion.borrow().is_none();
            let _ = self
                .report
                .send((self.label, unpublished, std::thread::current().id()));
        }
    }

    thread_local! {
        static THREAD_EXIT: RefCell<Option<DropProbe>> = const { RefCell::new(None) };
    }

    /// Reports the owner thread from its last thread-local destructor.
    fn record_thread_exit(handle: &WorkerHandle) -> mpsc::Receiver<ThreadId> {
        let (exited_tx, exited_rx) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    THREAD_EXIT.with(|slot| *slot.borrow_mut() = Some(DropProbe(exited_tx)));
                })
                .expect("exit probe admitted"),
        )
        .expect("exit probe installed");
        exited_rx
    }

    struct PanickingWake(Arc<AtomicUsize>);

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic_any(NOTIFICATION_PANIC);
        }
    }

    /// A panic payload whose destructor also panics.
    struct PanickingPayload;

    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            panic!("injected panic payload destructor panic");
        }
    }

    struct PayloadPanickingWake(Arc<AtomicUsize>);

    impl Wake for PayloadPanickingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic_any(PanickingPayload);
        }
    }

    fn panicking_observer(
        handle: &WorkerHandle,
        wakes: &Arc<AtomicUsize>,
    ) -> Pin<Box<WorkerQuiescing>> {
        let waker = Waker::from(Arc::new(PanickingWake(Arc::clone(wakes))));
        let mut observer = Box::pin(handle.quiescing());
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        observer
    }

    /// Drops the worker on a new thread; a Drop that never returns fails the
    /// test instead of hanging it.
    fn drop_off_owner_thread(worker: HostedWorker) -> std::thread::Result<()> {
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            let dropped = catch_unwind(AssertUnwindSafe(|| drop(worker)));
            dropped_tx.send(dropped).expect("test observes the drop");
        });
        let dropped = dropped_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("an off-owner drop returns within the test timeout");
        dropper.join().expect("dropper thread exits");
        dropped
    }

    fn wait<Work: Future>(future: Work) -> Work::Output {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime starts")
            .block_on(async { tokio::time::timeout(TEST_TIMEOUT, future).await })
            .expect("hosted worker progresses within the test timeout")
    }

    /// Runs `test` from this module alone in a child test process with `marker`
    /// set in its environment.
    fn run_in_child_process(test: &str, marker: &str) -> Output {
        let (_, module) = module_path!()
            .split_once("::")
            .expect("crate-qualified module path");
        Command::new(std::env::current_exe().expect("test binary path"))
            .args([&format!("{module}::{test}"), "--exact", "--test-threads=1"])
            .env(marker, "1")
            .stdin(Stdio::null())
            .output()
            .expect("child test process runs")
    }

    fn completed(mut completion: Completion) -> Result<(), ManagedWorkerError> {
        wait(async move {
            completion
                .wait_for(Option::is_some)
                .await
                .expect("owner thread publishes completion before exiting")
                .clone()
                .expect("completion is published")
        })
    }

    #[test]
    fn hosted_worker_is_send_and_sync() {
        fn assert_send_sync<Value: Send + Sync>() {}

        assert_send_sync::<HostedWorker>();
    }

    #[test]
    fn owner_thread_uses_configured_name() {
        let worker = HostedWorker::spawn(options("rox-hosted-name", 1)).expect("worker starts");
        let handle = worker.handle();
        let (name, thread) = wait(
            handle
                .call(|context| {
                    assert!(context.is_owner_thread());
                    (
                        std::thread::current().name().map(str::to_owned),
                        std::thread::current().id(),
                    )
                })
                .expect("probe admitted"),
        )
        .expect("probe completes");
        assert_eq!(name.as_deref(), Some("rox-hosted-name"));
        assert_eq!(thread, handle.owner_thread());
        assert_ne!(thread, std::thread::current().id());
        drop(worker);

        assert!(
            matches!(
                HostedWorker::spawn(options("rox\0hosted", 1)),
                Err(ManagedWorkerError::ThreadSpawn(_))
            ),
            "an unrepresentable thread name is a spawn error, not a panic"
        );
    }

    #[test]
    fn waking_one_of_1000_parked_commands_polls_only_it() {
        const COMMANDS: usize = 1000;
        const WOKEN: usize = 500;
        let worker =
            HostedWorker::spawn(options("rox-hosted-parked", COMMANDS)).expect("worker starts");
        let handle = worker.handle();
        let (polled_tx, polled_rx) = mpsc::channel();
        let parked: Vec<_> = (0..COMMANDS)
            .map(|index| {
                let polls = Arc::new(AtomicUsize::new(0));
                let waker = Arc::new(Mutex::new(None::<Waker>));
                let task_polls = Arc::clone(&polls);
                let task_waker = Arc::clone(&waker);
                let polled = polled_tx.clone();
                handle
                    .spawn_local(move |_| {
                        poll_fn(move |context| {
                            let count = task_polls.fetch_add(1, Ordering::SeqCst) + 1;
                            *task_waker.lock().expect("waker slot") = Some(context.waker().clone());
                            polled.send((index, count)).expect("test observes polls");
                            Poll::<()>::Pending
                        })
                    })
                    .expect("parked command admitted");
                (polls, waker)
            })
            .collect();
        for _ in 0..COMMANDS {
            polled_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("every command parks");
        }
        let driver_polls = handle.driver_poll_count();

        parked[WOKEN]
            .1
            .lock()
            .expect("waker slot")
            .take()
            .expect("parked command registered its waker")
            .wake();
        assert_eq!(polled_rx.recv_timeout(TEST_TIMEOUT), Ok((WOKEN, 2)));
        assert_eq!(
            handle.driver_poll_count(),
            driver_polls,
            "a command wake does not poll the worker driver"
        );
        wait(handle.call(|_| ()).expect("barrier admitted")).expect("barrier completes");
        assert!(
            polled_rx.try_recv().is_err(),
            "no other parked command was polled again"
        );
        for (index, (polls, _)) in parked.iter().enumerate() {
            let expected = if index == WOKEN { 2 } else { 1 };
            assert_eq!(polls.load(Ordering::SeqCst), expected, "command {index}");
        }
    }

    /// A hosted worker with owner thread `name`, and a managed worker.
    fn both_worker_kinds(name: &str) -> (HostedWorker, ManagedWorker) {
        (
            HostedWorker::spawn(options(name, 2)).expect("hosted worker starts"),
            ManagedWorker::new(2).expect("managed worker starts"),
        )
    }

    /// Counts its wakes.
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_wake_gate_reports_its_own_wakes_and_wakes_its_latest_parent() {
        let mut gate = WakeGate::new();
        let (first, second) = (
            Arc::new(WakeCount(AtomicUsize::new(0))),
            Arc::new(WakeCount(AtomicUsize::new(0))),
        );
        let (first_waker, second_waker) = (
            Waker::from(Arc::clone(&first)),
            Waker::from(Arc::clone(&second)),
        );
        let parent_wakes = || {
            (
                first.0.load(Ordering::SeqCst),
                second.0.load(Ordering::SeqCst),
            )
        };

        assert!(
            gate.poll_woken(&first_waker),
            "the first poll runs the supervisor"
        );
        assert!(!gate.poll_woken(&first_waker), "an unrelated wake does not");
        assert!(!gate.poll_woken(&second_waker), "nor does a new parent");

        gate.waker().wake_by_ref();
        assert_eq!(
            parent_wakes(),
            (0, 1),
            "the gate's wake wakes the latest parent only"
        );
        assert!(
            gate.poll_woken(&second_waker),
            "the next poll runs the supervisor"
        );
        assert!(!gate.poll_woken(&second_waker), "once per wake");

        gate.waker().wake_by_ref();
        assert!(
            gate.poll_woken(&first_waker),
            "a wake before a parent change is kept"
        );
        assert_eq!(parent_wakes(), (0, 2));

        let input = gate.waker().clone();
        input.wake();
        assert_eq!(parent_wakes(), (1, 2), "an input's clone wakes the parent");
        assert!(gate.poll_woken(&first_waker));
        gate.waker().wake_by_ref();
        assert_eq!(
            parent_wakes(),
            (2, 2),
            "a wake while the supervisor runs wakes the parent"
        );
        assert!(
            gate.poll_woken(&first_waker),
            "and runs the supervisor again"
        );
        assert!(!gate.poll_woken(&first_waker));
    }

    #[test]
    fn a_command_wake_does_not_poll_the_worker_supervisor() {
        const WAKES: usize = 32;
        let (hosted, managed) = both_worker_kinds("rox-hosted-supervisor-polls");
        let polls: Vec<_> = [("hosted", hosted.handle()), ("managed", managed.handle())]
            .into_iter()
            .map(|(kind, handle)| {
                let polls = wait(
                    handle
                        .call_local(|_| async {
                            let before = supervisor_polls_on_this_thread();
                            for _ in 0..WAKES {
                                tokio::task::yield_now().await;
                            }
                            supervisor_polls_on_this_thread() - before
                        })
                        .expect("command admitted"),
                )
                .expect("command completes");
                (kind, polls)
            })
            .collect();
        assert_eq!(
            polls,
            [("hosted", 0), ("managed", 0)],
            "{WAKES} wakes of a command run the worker supervisor no times"
        );
    }

    #[test]
    fn quiescing_an_idle_worker_wakes_its_supervisor() {
        let (hosted, managed) = both_worker_kinds("rox-hosted-idle-quiesce");
        for (kind, handle, completion) in [
            ("hosted", hosted.handle(), hosted.completion()),
            ("managed", managed.handle(), managed.completion()),
        ] {
            wait(handle.call(|_| ()).expect("barrier admitted")).expect("barrier completes");
            // Lets the worker finish the barrier's release and park.
            std::thread::sleep(Duration::from_millis(50));

            // Longer than the test timeout: only the supervisor's wake ends the worker in time.
            handle.begin_quiesce(QuiesceDeadline::new(TEST_TIMEOUT * 6));
            assert_eq!(completed(completion), Ok(()), "{kind}");
            assert!(
                !handle.was_forced(),
                "{kind}: an idle worker has nothing to force"
            );
            assert_eq!(handle.state(), WorkerState::Closed, "{kind}");
        }
    }

    #[test]
    fn a_quiesce_deadline_passes_while_a_command_keeps_waking_the_worker() {
        const GRACE: Duration = Duration::from_millis(20);
        let (hosted, managed) = both_worker_kinds("rox-hosted-busy-deadline");
        for (kind, handle, completion) in [
            ("hosted", hosted.handle(), hosted.completion()),
            ("managed", managed.handle(), managed.completion()),
        ] {
            let (started_tx, started_rx) = oneshot::channel();
            let (dropped_tx, dropped_rx) = mpsc::channel();
            let call = handle
                .call_local(move |context| async move {
                    let _probe = CancellationProbe {
                        handle: context.handle().clone(),
                        report: dropped_tx,
                    };
                    started_tx.send(()).expect("test observes startup");
                    poll_fn(|task| {
                        task.waker().wake_by_ref();
                        Poll::<()>::Pending
                    })
                    .await;
                })
                .expect("command admitted");
            wait(started_rx).expect("command started");

            let quiesced = Instant::now();
            handle.begin_quiesce(QuiesceDeadline::new(GRACE));
            assert_eq!(wait(call), Err(WorkerCallError::Cancelled), "{kind}");
            assert_eq!(completed(completion), Ok(()), "{kind}");
            assert!(
                quiesced.elapsed() >= GRACE,
                "{kind}: the busy command runs until the deadline"
            );
            assert!(
                handle.was_forced(),
                "{kind}: the deadline cancelled the busy command"
            );
            assert_eq!(
                dropped_rx.try_recv(),
                Ok((handle.owner_thread(), true)),
                "{kind}: the busy command is dropped, cancelled, on the owner thread"
            );
        }
    }

    #[test]
    fn a_command_woken_after_the_quiesce_deadline_is_cancelled_before_it_runs() {
        // A round tests the deadline only if the wake reaches the worker before
        // tokio fires the deadline's timer, which it rounds up to the next
        // millisecond; most rounds do.
        const GRACE: Duration = Duration::from_millis(5);
        const ROUNDS: usize = 20;
        for round in 0..ROUNDS {
            let (hosted, managed) = both_worker_kinds("rox-hosted-wake-after-deadline");
            for (kind, handle, completion) in [
                ("hosted", hosted.handle(), hosted.completion()),
                ("managed", managed.handle(), managed.completion()),
            ] {
                let released = Arc::new(AtomicBool::new(false));
                let parked = Arc::new(Mutex::new(None::<Waker>));
                let (started_tx, started_rx) = oneshot::channel();
                let call = handle
                    .call_local({
                        let (released, parked) = (Arc::clone(&released), Arc::clone(&parked));
                        move |context| async move {
                            let mut started = Some(started_tx);
                            poll_fn(|task| {
                                if released.load(Ordering::SeqCst) && !context.is_cancelled() {
                                    return Poll::Ready(());
                                }
                                *parked.lock().expect("waker slot") = Some(task.waker().clone());
                                if let Some(started) = started.take() {
                                    started.send(()).expect("test observes startup");
                                }
                                Poll::Pending
                            })
                            .await;
                        }
                    })
                    .expect("command admitted");
                wait(started_rx).expect("command started");

                let deadline = QuiesceDeadline::new(GRACE);
                let passed = Instant::now() + GRACE;
                handle.begin_quiesce(deadline);
                while Instant::now() < passed {
                    std::hint::spin_loop();
                }
                released.store(true, Ordering::SeqCst);
                parked
                    .lock()
                    .expect("waker slot")
                    .take()
                    .expect("the command parked its waker")
                    .wake();

                assert_eq!(
                    wait(call),
                    Err(WorkerCallError::Cancelled),
                    "{kind}, round {round}: the deadline passed before the command's wake"
                );
                assert!(handle.was_forced(), "{kind}, round {round}");
                assert_eq!(completed(completion), Ok(()), "{kind}, round {round}");
            }
        }
    }

    #[test]
    fn a_cleanup_woken_after_the_teardown_deadline_is_dropped_before_it_runs() {
        // As for the quiesce deadline above: a round tests the teardown deadline
        // only if the wake reaches the worker before tokio fires the deadline's
        // timer, which it rounds up to the next millisecond; most rounds do.
        const GRACE: Duration = Duration::from_millis(5);
        const ROUNDS: usize = 20;
        for round in 0..ROUNDS {
            let worker = HostedWorker::spawn(options("rox-hosted-wake-after-teardown", 2))
                .expect("worker starts");
            let released = Arc::new(AtomicBool::new(false));
            let parked = Arc::new(Mutex::new(None::<Waker>));
            let (started_tx, started_rx) = oneshot::channel();
            let (cleanup_parked_tx, cleanup_parked_rx) = mpsc::channel();
            worker
                .handle()
                .spawn_local({
                    let (released, parked) = (Arc::clone(&released), Arc::clone(&parked));
                    move |context| async move {
                        let _cleanup = context
                            .register_cleanup_task(poll_fn(move |task| {
                                if released.load(Ordering::SeqCst) {
                                    return Poll::Ready(());
                                }
                                *parked.lock().expect("waker slot") = Some(task.waker().clone());
                                let _ = cleanup_parked_tx.send(());
                                Poll::Pending
                            }))
                            .expect("cleanup registered while open");
                        started_tx.send(()).expect("test observes startup");
                        pending::<()>().await;
                    }
                })
                .expect("command admitted");
            wait(started_rx).expect("command parked");

            assert!(worker.shutdown());
            let cleanup_parked = cleanup_parked_rx.recv_timeout(TEST_TIMEOUT);
            let deadline = Instant::now() + GRACE;
            assert!(!worker.shutdown_by(deadline));
            cleanup_parked.expect("the cleanup runs and parks during the teardown");
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
            released.store(true, Ordering::SeqCst);
            parked
                .lock()
                .expect("waker slot")
                .take()
                .expect("the cleanup parked its waker")
                .wake();

            assert_eq!(
                completed(worker.completion()),
                Err(ManagedWorkerError::TeardownDeadlineExceeded),
                "round {round}: the teardown deadline passed before the cleanup's wake"
            );
        }
    }

    #[test]
    fn command_panic_closes_worker_and_reports_task_panicked() {
        let worker = HostedWorker::spawn(options("rox-hosted-panic", 2)).expect("worker starts");
        let handle = worker.handle();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        release_rx.await.expect("test releases cleanup");
                    })
                    .expect("cleanup registered while open");
                tokio::task::yield_now().await;
                panic!("injected hosted command panic");
            })
            .expect("command admitted");

        wait(handle.quiescing());
        assert_eq!(handle.state(), WorkerState::Closing);
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        release_tx
            .send(())
            .expect("cleanup is retained while closing");
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked))
        );
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
    }

    #[test]
    fn quiesce_drains_admitted_command_and_reports_ok() {
        let worker = HostedWorker::spawn(options("rox-hosted-drain", 2)).expect("worker starts");
        let handle = worker.handle();
        let (started_tx, started_rx) = oneshot::channel();
        let call = handle
            .call_local(move |context| async move {
                started_tx.send(()).expect("test observes startup");
                context.quiescing().await;
                assert!(!context.is_cancelled(), "draining does not cancel");
                tokio::task::yield_now().await;
                23_u8
            })
            .expect("command admitted");
        wait(started_rx).expect("command started");

        handle.begin_quiesce(QuiesceDeadline::new(TEST_TIMEOUT));
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        assert_eq!(wait(call), Ok(23));
        assert_eq!(completed(worker.completion()), Ok(()));
        assert!(!handle.was_forced(), "drained before the deadline");
        assert_eq!(handle.state(), WorkerState::Closed);
    }

    #[test]
    fn quiesce_deadline_forces_shutdown() {
        let worker = HostedWorker::spawn(options("rox-hosted-forced", 2)).expect("worker starts");
        let handle = worker.handle();
        let (started_tx, started_rx) = oneshot::channel();
        let (cleaned_tx, cleaned_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let call = handle
            .call_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup(move || cleaned_tx.send(()).expect("test observes cleanup"))
                    .expect("cleanup registered while open");
                let _probe = CancellationProbe {
                    handle: context.handle().clone(),
                    report: dropped_tx,
                };
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command started");

        handle.begin_quiesce(QuiesceDeadline::new(Duration::from_millis(20)));
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        assert_eq!(wait(call), Err(WorkerCallError::Cancelled));
        assert_eq!(completed(worker.completion()), Ok(()));
        assert!(handle.was_forced(), "the deadline cancelled admitted work");
        assert_eq!(
            dropped_rx.try_recv(),
            Ok((handle.owner_thread(), true)),
            "the running command is dropped, cancelled, on the owner thread"
        );
        assert_eq!(
            cleaned_rx.try_recv(),
            Ok(()),
            "forced shutdown still runs registered cleanup"
        );

        let worker =
            HostedWorker::spawn(options("rox-hosted-forced-scope", 2)).expect("worker starts");
        let handle = worker.handle();
        let (finished_tx, finished_rx) = mpsc::channel();
        wait(
            handle
                .call(move |context| {
                    let groups = Rc::new(RefCell::new(Vec::<TaskGroup>::new()));
                    let cleanup_groups = Rc::clone(&groups);
                    let mut scope = Some(
                        context
                            .register_scope(async move {
                                let groups = std::mem::take(&mut *cleanup_groups.borrow_mut());
                                for mut group in groups {
                                    group
                                        .cancel_and_join()
                                        .await
                                        .expect("each child finishes after cancellation");
                                }
                            })
                            .expect("scope registered while open"),
                    );
                    for _ in 0..2 {
                        let mut group = TaskGroup::new();
                        let finished = finished_tx.clone();
                        let scope = scope.take();
                        group
                            .spawn_local(move |token| async move {
                                let _scope = scope;
                                let mut cancellation = token.wake_registration();
                                poll_fn(|task| {
                                    if cancellation.update_waker(task.waker()) {
                                        Poll::Ready(())
                                    } else {
                                        Poll::Pending
                                    }
                                })
                                .await;
                                tokio::task::yield_now().await;
                                finished
                                    .send(std::thread::current().id())
                                    .expect("test observes the child");
                            })
                            .expect("child admitted");
                        groups.borrow_mut().push(group);
                    }
                })
                .expect("scope initializer admitted"),
        )
        .expect("scope initializer returns");

        handle.begin_quiesce(QuiesceDeadline::new(Duration::from_millis(20)));
        assert_eq!(
            completed(worker.completion()),
            Ok(()),
            "after a forced quiesce, the scope's cleanup still cancels and joins both groups"
        );
        assert!(handle.was_forced(), "the children ignored the drain");
        for _ in 0..2 {
            assert_eq!(finished_rx.try_recv(), Ok(handle.owner_thread()));
        }
    }

    #[test]
    fn shutdown_starts_once_and_completion_reports_ok() {
        let worker = HostedWorker::spawn(options("rox-hosted-shutdown", 2)).expect("worker starts");
        let handle = worker.handle();
        let (started_tx, started_rx) = oneshot::channel();
        let parked = handle
            .call_local(move |_| async move {
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");

        assert!(worker.shutdown(), "the first call starts shutdown");
        assert!(!worker.shutdown(), "shutdown has already started");
        assert!(!handle.clone().begin_shutdown());
        assert_eq!(completed(worker.completion()), Ok(()));
        assert_eq!(wait(parked), Err(WorkerCallError::Cancelled));
        assert_eq!(handle.state(), WorkerState::Closed);
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        assert!(!worker.shutdown());

        let worker = Arc::new(
            HostedWorker::spawn(options("rox-hosted-shutdown-queued", 2)).expect("worker starts"),
        );
        let handle = worker.handle();
        let (ran_tx, ran_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let command_worker = Arc::clone(&worker);
        let command_handle = handle.clone();
        let (started, queued) = wait(
            handle
                .call(move |_| {
                    let probe = DropProbe(dropped_tx);
                    let queued = command_handle
                        .call(move |_| {
                            let _probe = probe;
                            ran_tx.send(()).expect("test observes a run");
                        })
                        .expect("probe admitted while open");
                    (command_worker.shutdown(), queued)
                })
                .expect("command admitted"),
        )
        .expect("command completes");
        assert!(started, "shutdown() inside a command starts shutdown");
        assert_eq!(wait(queued), Err(WorkerCallError::Cancelled));
        assert_eq!(completed(worker.completion()), Ok(()));
        assert_eq!(
            ran_rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected),
            "a command still queued when shutdown starts never runs"
        );
        assert_eq!(
            dropped_rx.try_recv(),
            Ok(handle.owner_thread()),
            "the queued command is dropped on the owner thread before completion"
        );
    }

    #[test]
    fn shutdown_reraises_notification_panic_after_closing() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-shutdown-panic", 1)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let wakes = Arc::new(AtomicUsize::new(0));
        let observer = panicking_observer(&handle, &wakes);

        let raised = catch_unwind(AssertUnwindSafe(|| worker.shutdown()))
            .expect_err("shutdown() re-raises the notification panic");
        assert_eq!(raised.downcast_ref::<&str>(), Some(&NOTIFICATION_PANIC));
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert_ne!(handle.state(), WorkerState::Open);
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Closed));
        assert!(!worker.shutdown(), "shutdown had already started");

        let completion = worker.completion();
        assert!(
            drop_off_owner_thread(worker).is_ok(),
            "Drop does not unwind"
        );
        assert_eq!(
            exited.try_recv(),
            Ok(handle.owner_thread()),
            "Drop still joins"
        );
        assert_eq!(completion.borrow().clone(), Some(Ok(())));
        drop(observer);
    }

    #[test]
    fn completion_is_published_after_worker_state_is_dropped() {
        let worker = HostedWorker::spawn(options("rox-hosted-state", 2)).expect("worker starts");
        let handle = worker.handle();
        let (report_tx, report_rx) = mpsc::channel();
        let probe = |label| CompletionProbe {
            label,
            completion: worker.completion(),
            report: report_tx.clone(),
        };
        let command_probe = probe("command");
        let cleanup_probe = probe("cleanup");
        let local_set_probe = probe("local_set");
        let blocking_probe = probe("blocking");
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        let _probe = cleanup_probe;
                        tokio::task::yield_now().await;
                    })
                    .expect("cleanup registered while open");
                drop(tokio::task::spawn_local(async move {
                    let _probe = local_set_probe;
                    pending::<()>().await;
                }));
                let (cancelled_tx, cancelled_rx) = mpsc::channel::<()>();
                drop(tokio::task::spawn_blocking(move || {
                    let _probe = blocking_probe;
                    let _ = cancelled_rx.recv_timeout(TEST_TIMEOUT);
                    std::thread::sleep(Duration::from_millis(50));
                }));
                let _cancelled = cancelled_tx;
                let _command = command_probe;
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");

        assert!(worker.shutdown());
        assert_eq!(completed(worker.completion()), Ok(()));
        let owner = handle.owner_thread();
        let mut reports: Vec<_> = (0..4)
            .map(|_| {
                let (label, unpublished, thread) = report_rx
                    .recv_timeout(TEST_TIMEOUT)
                    .expect("worker-owned state is dropped");
                (label, unpublished, thread == owner)
            })
            .collect();
        reports.sort_by_key(|(label, _, _)| *label);
        assert_eq!(
            reports,
            [
                ("blocking", true, false),
                ("cleanup", true, true),
                ("command", true, true),
                ("local_set", true, true),
            ],
            "command, cleanup, and LocalSet state drop on the owner thread, and blocking-pool \
             work finishes, before completion"
        );
    }

    #[test]
    fn shutdown_and_drop_let_registered_cleanup_run_to_completion() {
        fn park_with_cleanup(
            handle: &WorkerHandle,
        ) -> (oneshot::Sender<()>, mpsc::Receiver<ThreadId>) {
            let (release_tx, release_rx) = oneshot::channel::<()>();
            let (finished_tx, finished_rx) = mpsc::channel();
            let (started_tx, started_rx) = oneshot::channel();
            handle
                .spawn_local(move |context| async move {
                    let _cleanup = context
                        .register_cleanup_task(async move {
                            release_rx.await.expect("test releases cleanup");
                            finished_tx
                                .send(std::thread::current().id())
                                .expect("test observes cleanup");
                        })
                        .expect("cleanup registered while open");
                    started_tx.send(()).expect("test observes startup");
                    pending::<()>().await;
                })
                .expect("command admitted");
            wait(started_rx).expect("command parked");
            (release_tx, finished_rx)
        }

        let worker = HostedWorker::spawn(options("rox-hosted-cleanup", 2)).expect("worker starts");
        let handle = worker.handle();
        let (release, finished) = park_with_cleanup(&handle);
        assert!(worker.shutdown());
        assert!(
            worker.completion().borrow().is_none(),
            "shutdown waits for the registered cleanup"
        );
        release.send(()).expect("cleanup is still waiting");
        assert_eq!(completed(worker.completion()), Ok(()));
        assert_eq!(
            finished.try_recv(),
            Ok(handle.owner_thread()),
            "cleanup finished on the owner thread before completion"
        );
        drop(worker);

        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-cleanup", 2)).expect("worker starts");
        let handle = worker.handle();
        let completion = worker.completion();
        let (release, finished) = park_with_cleanup(&handle);
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(worker);
            dropped_tx.send(()).expect("test observes the drop");
        });
        wait(handle.quiescing());
        assert!(
            dropped_rx.try_recv().is_err(),
            "Drop waits for the registered cleanup"
        );
        release.send(()).expect("cleanup is still waiting");
        assert_eq!(dropped_rx.recv_timeout(TEST_TIMEOUT), Ok(()));
        dropper.join().expect("dropper thread exits");
        assert_eq!(
            finished.try_recv(),
            Ok(handle.owner_thread()),
            "cleanup finished on the owner thread before Drop returned"
        );
        assert_eq!(completion.borrow().clone(), Some(Ok(())));
    }

    #[test]
    fn drop_off_owner_thread_begins_shutdown_and_joins() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-join", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let completion = worker.completion();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let call = handle
            .call_local(move |context| async move {
                let _probe = CancellationProbe {
                    handle: context.handle().clone(),
                    report: dropped_tx,
                };
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command started");

        assert!(drop_off_owner_thread(worker).is_ok());
        assert_eq!(
            dropped_rx.try_recv(),
            Ok((handle.owner_thread(), true)),
            "the running command was dropped, cancelled, on the owner thread"
        );
        assert_eq!(
            exited.try_recv(),
            Ok(handle.owner_thread()),
            "the join waited for the owner thread's thread-local destructors"
        );
        assert_eq!(completion.borrow().clone(), Some(Ok(())));
        assert_eq!(wait(call), Err(WorkerCallError::Cancelled));
        assert_eq!(handle.state(), WorkerState::Closed);
    }

    #[test]
    fn drop_joins_owner_thread_when_shutdown_notification_panics() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-panic", 1)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let wakes = Arc::new(AtomicUsize::new(0));
        let observer = panicking_observer(&handle, &wakes);
        let completion = worker.completion();

        assert!(
            drop_off_owner_thread(worker).is_ok(),
            "Drop does not unwind"
        );
        drop(observer);
        assert_eq!(wakes.load(Ordering::SeqCst), 1, "the notification panicked");
        assert_eq!(exited.try_recv(), Ok(handle.owner_thread()));
        assert_eq!(completion.borrow().clone(), Some(Ok(())));

        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-payload", 1)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(PayloadPanickingWake(Arc::clone(&wakes))));
        let mut completion = worker.completion();
        let mut observer = Box::pin(completion.changed());
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        assert!(
            drop_off_owner_thread(worker).is_ok(),
            "Drop does not unwind when a completion waker panics with a payload that panics on drop"
        );
        drop(observer);
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "the completion notification panicked"
        );
        assert_eq!(exited.try_recv(), Ok(handle.owner_thread()));
        assert_eq!(completion.borrow().clone(), Some(Ok(())));
    }

    /// An owner-thread Drop whose completion waiters' wakers panic with a
    /// payload that panics on drop. Tokio spreads waiters across notify shards,
    /// so closing the channel also reaches a waiter that the panicking
    /// publication skipped.
    fn drop_on_owner_thread_with_panicking_completion_wakers() {
        const WAITERS: usize = 16;
        let worker = HostedWorker::spawn(options("rox-hosted-owner-drop-payload", 2))
            .expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(PayloadPanickingWake(Arc::clone(&wakes))));
        let mut completions: Vec<_> = (0..WAITERS).map(|_| worker.completion()).collect();
        let mut observers: Vec<_> = completions
            .iter_mut()
            .map(|completion| Box::pin(completion.changed()))
            .collect();
        for observer in &mut observers {
            assert!(
                observer
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }

        let dropped = handle
            .call(move |_| drop(worker))
            .expect("owner-thread drop admitted");
        assert_eq!(wait(dropped), Ok(()));
        assert_eq!(
            exited.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the unjoined owner thread exits after its completion wakers panic"
        );
        drop(observers);
        assert!(
            wakes.load(Ordering::SeqCst) >= 1,
            "a completion notification panicked"
        );
        assert_eq!(completions[0].borrow().clone(), Some(Ok(())));
    }

    #[test]
    fn drop_on_owner_thread_does_not_deadlock() {
        if std::env::var_os(OWNER_DROP_CHILD).is_some() {
            drop_on_owner_thread_with_panicking_completion_wakers();
            return;
        }

        let worker =
            HostedWorker::spawn(options("rox-hosted-owner-drop", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let completion = worker.completion();

        let dropped = handle
            .call(move |context| {
                assert!(context.is_owner_thread());
                drop(worker);
            })
            .expect("owner-thread drop admitted");
        assert_eq!(wait(dropped), Ok(()));
        assert_eq!(completed(completion), Ok(()));
        assert_eq!(
            exited.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the unjoined owner thread still exits"
        );
        assert_eq!(handle.state(), WorkerState::Closed);

        let worker =
            HostedWorker::spawn(options("rox-hosted-owner-drop-panic", 2)).expect("worker starts");
        let handle = worker.handle();
        let completion = worker.completion();
        let wakes = Arc::new(AtomicUsize::new(0));
        let observer = panicking_observer(&handle, &wakes);

        let dropped = handle
            .call(move |_| drop(worker))
            .expect("owner-thread drop admitted");
        assert_eq!(
            wait(dropped),
            Ok(()),
            "an owner-thread Drop does not unwind into its command"
        );
        assert_eq!(completed(completion), Ok(()));
        drop(observer);
        assert_eq!(wakes.load(Ordering::SeqCst), 1, "the notification panicked");

        // std drops an unjoined thread's result on that thread and aborts the
        // process if the drop panics.
        let child =
            run_in_child_process("drop_on_owner_thread_does_not_deadlock", OWNER_DROP_CHILD);
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(
            child.status.success() && stdout.contains("1 passed"),
            "an owner-thread Drop survives a panicking completion waker: {}\nstdout:\n{stdout}\nstderr:\n{}",
            child.status,
            String::from_utf8_lossy(&child.stderr)
        );
    }

    thread_local! {
        static STARTED_BY_HOOK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// The start hook's log of thread names, and the blocking-pool options that
    /// name threads `name` and run that hook.
    fn blocking_pool(name: &str) -> (Arc<Mutex<Vec<Option<String>>>>, HostedWorkerBlockingPool) {
        let started = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&started);
        let pool = HostedWorkerBlockingPool {
            thread_name: name.to_owned(),
            on_thread_start: Some(Arc::new(move || {
                STARTED_BY_HOOK.with(|started| started.set(true));
                log.lock()
                    .unwrap()
                    .push(std::thread::current().name().map(str::to_owned));
            })),
            on_thread_stop: None,
        };
        (started, pool)
    }

    #[test]
    fn blocking_pool_threads_take_the_configured_name_and_start_hook() {
        let (started, pool) = blocking_pool("rox-w0-blk");
        let worker = HostedWorker::spawn_with_blocking_pool(options("rox-w0", 1), pool)
            .expect("worker starts");
        let (owner_name, blocking) = wait(
            worker
                .handle()
                .call_local(|_context| async {
                    let owner_name = std::thread::current().name().map(str::to_owned);
                    let blocking = tokio::task::spawn_blocking(|| {
                        (
                            std::thread::current().name().map(str::to_owned),
                            STARTED_BY_HOOK.with(std::cell::Cell::get),
                        )
                    })
                    .await
                    .expect("blocking work completes");
                    (owner_name, blocking)
                })
                .expect("probe admitted"),
        )
        .expect("probe completes");
        assert_eq!(
            owner_name.as_deref(),
            Some("rox-w0"),
            "the owner thread keeps its name"
        );
        assert_eq!(
            blocking,
            (Some("rox-w0-blk".to_owned()), true),
            "the blocking thread is named and ran the hook before the work"
        );
        assert_eq!(*started.lock().unwrap(), [Some("rox-w0-blk".to_owned())]);
        drop(worker);
    }

    #[test]
    fn the_blocking_pool_start_hook_never_runs_on_the_owner_thread() {
        let (started, pool) = blocking_pool("rox-w1-blk");
        let worker = HostedWorker::spawn_with_blocking_pool(options("rox-w1", 1), pool)
            .expect("worker starts");
        let hooked = wait(
            worker
                .handle()
                .call(|_context| STARTED_BY_HOOK.with(std::cell::Cell::get))
                .expect("probe admitted"),
        )
        .expect("probe completes");
        assert!(!hooked);
        drop(worker);
        assert!(
            started.lock().unwrap().is_empty(),
            "no blocking thread started"
        );
    }

    #[test]
    fn an_unrepresentable_blocking_thread_name_is_a_spawn_error() {
        let (_, pool) = blocking_pool("rox\0blk");
        assert!(
            matches!(
                HostedWorker::spawn_with_blocking_pool(options("rox-w2", 1), pool),
                Err(ManagedWorkerError::ThreadSpawn(_))
            ),
            "a NUL byte is a spawn error, not a panic on the first blocking thread"
        );
    }

    /// Teardown deadline of the bounded-shutdown oracles (`DIV-043`).
    const TEARDOWN_DEADLINE: Duration = Duration::from_millis(400);
    /// How long past a teardown deadline these oracles join a worker.
    const JOIN_SLACK: Duration = Duration::from_secs(2);
    /// Bound on how late past its deadline a bounded wait may return.
    const LATENESS: Duration = Duration::from_secs(1);

    /// Reports the dropping thread and when it dropped.
    struct TimedDropProbe(mpsc::Sender<(ThreadId, Instant)>);

    impl Drop for TimedDropProbe {
        fn drop(&mut self) {
            let _ = self.0.send((std::thread::current().id(), Instant::now()));
        }
    }

    thread_local! {
        static TIMED_THREAD_EXIT: RefCell<Option<TimedDropProbe>> = const { RefCell::new(None) };
    }

    /// Reports the owner thread, and when it exited, from its last thread-local
    /// destructor.
    fn record_timed_thread_exit(handle: &WorkerHandle) -> mpsc::Receiver<(ThreadId, Instant)> {
        let (exited_tx, exited_rx) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    TIMED_THREAD_EXIT
                        .with(|slot| *slot.borrow_mut() = Some(TimedDropProbe(exited_tx)));
                })
                .expect("exit probe admitted"),
        )
        .expect("exit probe installed");
        exited_rx
    }

    /// Logs its label, the dropping thread and when it dropped.
    struct LogProbe {
        label: &'static str,
        log: Arc<Mutex<Vec<(&'static str, ThreadId, Instant)>>>,
    }

    impl Drop for LogProbe {
        fn drop(&mut self) {
            self.log.lock().unwrap().push((
                self.label,
                std::thread::current().id(),
                Instant::now(),
            ));
        }
    }

    /// Starts blocking-pool work on the worker that runs until the returned
    /// sender releases it (or is dropped) and then reports its thread. With
    /// `awaited`, a command awaits it, as a command awaiting
    /// `tokio::net::lookup_host`, which resolves on the blocking pool, awaits a
    /// held resolver; otherwise nothing does.
    fn hold_blocking_work(
        handle: &WorkerHandle,
        awaited: bool,
    ) -> (mpsc::Sender<()>, mpsc::Receiver<ThreadId>) {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (finished_tx, finished_rx) = mpsc::channel();
        let (blocked_tx, blocked_rx) = mpsc::channel();
        handle
            .spawn_local(move |_| async move {
                let blocking = tokio::task::spawn_blocking(move || {
                    blocked_tx
                        .send(())
                        .expect("test observes the blocking work");
                    let _ = release_rx.recv();
                    let _ = finished_tx.send(std::thread::current().id());
                });
                if awaited {
                    let _ = blocking.await;
                }
            })
            .expect("command admitted");
        blocked_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the blocking work starts");
        (release_tx, finished_rx)
    }

    /// Parks a command that runs until the worker drops it.
    fn park_command(handle: &WorkerHandle) {
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |_| async move {
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");
    }

    /// Runs `join` on a new thread and returns its result and when it returned;
    /// a join that is not back by `bound` fails the test instead of hanging it.
    fn join_off_owner_thread<Joined: Send + 'static>(
        bound: Instant,
        join: impl FnOnce() -> Joined + Send + 'static,
    ) -> (Joined, Instant) {
        let (joined_tx, joined_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let joined = join();
            let _ = joined_tx.send((joined, Instant::now()));
        });
        joined_rx
            .recv_timeout(bound.saturating_duration_since(Instant::now()) + LATENESS)
            .expect("the bounded join returns by its deadline")
    }

    #[test]
    fn a_teardown_deadline_abandons_held_blocking_work_and_ends_the_owner_thread() {
        for awaited in [false, true] {
            let worker =
                HostedWorker::spawn(options("rox-hosted-deadline", 2)).expect("worker starts");
            let handle = worker.handle();
            let exited = record_timed_thread_exit(&handle);
            let completion = worker.completion();
            let (release, finished) = hold_blocking_work(&handle, awaited);

            let deadline = Instant::now() + TEARDOWN_DEADLINE;
            assert!(
                worker.shutdown_by(deadline),
                "the first call starts shutdown"
            );
            let join_deadline = deadline + JOIN_SLACK;
            let (joined, returned) =
                join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
            assert_eq!(
                joined,
                HostedWorkerJoin::Joined(Ok(())),
                "awaited: {awaited}"
            );
            assert!(
                returned < deadline + LATENESS,
                "the join returned soon after the teardown deadline, awaited: {awaited}"
            );
            let (thread, exited_at) = exited
                .try_recv()
                .expect("the join waited for the owner thread's thread-local destructors");
            assert_eq!(thread, handle.owner_thread());
            assert!(
                exited_at >= deadline,
                "the held work kept the owner thread until the deadline, awaited: {awaited}"
            );
            assert_eq!(completion.borrow().clone(), Some(Ok(())));
            assert_eq!(
                finished.try_recv(),
                Err(mpsc::TryRecvError::Empty),
                "the blocking work was abandoned, still running, awaited: {awaited}"
            );
            release.send(()).expect("the blocking work still waits");
            assert!(
                finished.recv_timeout(TEST_TIMEOUT).is_ok(),
                "abandoned blocking work still runs to its end"
            );
        }
    }

    #[test]
    fn bounded_shutdowns_of_many_workers_do_not_wait_for_ones_held_in_blocking_work() {
        const WORKERS: usize = 4;
        const DEADLINE: Duration = Duration::from_secs(1);
        let workers: Vec<_> = (0..WORKERS)
            .map(|index| {
                HostedWorker::spawn(options(&format!("rox-hosted-many-{index}"), 2))
                    .expect("worker starts")
            })
            .collect();
        let exits: Vec<_> = workers
            .iter()
            .map(|worker| record_timed_thread_exit(&worker.handle()))
            .collect();
        // Worker 0 holds blocking work nothing awaits, worker 1 a held "resolver"
        // its command awaits; workers 2 and 3 are healthy.
        let held = [
            hold_blocking_work(&workers[0].handle(), false),
            hold_blocking_work(&workers[1].handle(), true),
        ];
        workers[2..]
            .iter()
            .for_each(|worker| park_command(&worker.handle()));

        let start = Instant::now();
        let deadline = start + DEADLINE;
        for worker in &workers {
            assert!(worker.shutdown_by(deadline));
        }
        // Joined in order, the held workers first, as one join thread would.
        let join_deadline = deadline + JOIN_SLACK;
        let (joined, returned) = join_off_owner_thread(join_deadline, move || {
            workers
                .into_iter()
                .map(|worker| worker.join_by(join_deadline))
                .collect::<Vec<_>>()
        });
        assert_eq!(joined, vec![HostedWorkerJoin::Joined(Ok(())); WORKERS]);
        assert!(
            returned < deadline + LATENESS,
            "the joins returned soon after the shared deadline"
        );
        for (index, exited) in exits.iter().enumerate() {
            let (_, exited_at) = exited.try_recv().expect("every owner thread exited");
            if index < 2 {
                assert!(
                    exited_at >= deadline,
                    "held worker {index} exited at the deadline"
                );
            } else {
                assert!(
                    exited_at < start + DEADLINE / 2,
                    "healthy worker {index} exited without waiting for the held workers"
                );
            }
        }
        for (release, finished) in held {
            release.send(()).expect("the blocking work still waits");
            assert!(finished.recv_timeout(TEST_TIMEOUT).is_ok());
        }
    }

    #[test]
    fn a_teardown_deadline_drops_tasks_whose_cleanup_never_finishes() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let hook_log = Arc::clone(&log);
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            on_owner_thread_stop: Some(Box::new(move || {
                hook_log.lock().unwrap().push((
                    "hook",
                    std::thread::current().id(),
                    Instant::now(),
                ));
            })),
            ..options("rox-hosted-deadline-cleanup", 2)
        })
        .expect("worker starts");
        let handle = worker.handle();
        let completion = worker.completion();
        let probe = |label| LogProbe {
            label,
            log: Arc::clone(&log),
        };
        let (command, cleanup, local, runtime) = (
            probe("command"),
            probe("cleanup"),
            probe("local"),
            probe("runtime"),
        );
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        let _probe = cleanup;
                        pending::<()>().await;
                    })
                    .expect("cleanup registered while open");
                drop(tokio::task::spawn_local(async move {
                    let _probe = local;
                    pending::<()>().await;
                }));
                drop(tokio::spawn(async move {
                    let _probe = runtime;
                    pending::<()>().await;
                }));
                let _probe = command;
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");

        let deadline = Instant::now() + TEARDOWN_DEADLINE;
        assert!(worker.shutdown_by(deadline));
        let join_deadline = deadline + JOIN_SLACK;
        let (joined, returned) =
            join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
        assert_eq!(
            joined,
            HostedWorkerJoin::Joined(Err(ManagedWorkerError::TeardownDeadlineExceeded))
        );
        assert!(returned < deadline + LATENESS);
        assert_eq!(
            completion.borrow().clone(),
            Some(Err(ManagedWorkerError::TeardownDeadlineExceeded))
        );
        let log = log.lock().unwrap();
        let mut labels: Vec<_> = log.iter().map(|(label, _, _)| *label).collect();
        assert_eq!(
            labels.pop(),
            Some("hook"),
            "the stop hook ran after every task was dropped, though the cleanup overran the \
             deadline"
        );
        labels.sort_unstable();
        assert_eq!(labels, ["cleanup", "command", "local", "runtime"]);
        assert!(
            log.iter()
                .all(|(_, thread, _)| *thread == handle.owner_thread()),
            "every LocalSet and runtime task was dropped, and the hook run, on the owner thread: \
             {log:?}"
        );
        let cleanup_dropped = log
            .iter()
            .find_map(|(label, _, at)| (*label == "cleanup").then_some(*at))
            .expect("cleanup dropped");
        assert!(
            cleanup_dropped >= deadline,
            "the unfinished cleanup ran until the deadline"
        );
    }

    #[test]
    fn a_deadline_set_after_shutdown_began_still_drops_unfinished_cleanup() {
        for first_deadline in [None, Some(Duration::from_secs(3600))] {
            let worker =
                HostedWorker::spawn(options("rox-hosted-late-deadline", 2)).expect("worker starts");
            let handle = worker.handle();
            let (started_tx, started_rx) = oneshot::channel();
            handle
                .spawn_local(move |context| async move {
                    let _cleanup = context
                        .register_cleanup_task(pending::<()>())
                        .expect("cleanup registered while open");
                    started_tx.send(()).expect("test observes startup");
                    pending::<()>().await;
                })
                .expect("command admitted");
            wait(started_rx).expect("command parked");

            let started = match first_deadline {
                None => worker.shutdown(),
                Some(far) => worker.shutdown_by(Instant::now() + far),
            };
            assert!(started);
            std::thread::sleep(Duration::from_millis(100));
            assert!(
                worker.completion().borrow().is_none(),
                "teardown waits for the cleanup, first deadline: {first_deadline:?}"
            );
            assert!(
                !worker.shutdown_by(Instant::now() + Duration::from_millis(100)),
                "shutdown had already started"
            );
            assert_eq!(
                completed(worker.completion()),
                Err(ManagedWorkerError::TeardownDeadlineExceeded),
                "an earlier deadline set later bounds the teardown, first deadline: \
                 {first_deadline:?}"
            );
        }
    }

    #[test]
    fn the_owner_thread_stop_hook_runs_on_the_owner_thread_after_every_task_is_dropped() {
        for bounded in [false, true] {
            stop_hook_runs_after_every_task_is_dropped(bounded);
        }
    }

    /// With `bounded`, the teardown has a deadline it does not reach.
    fn stop_hook_runs_after_every_task_is_dropped(bounded: bool) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let hook_log = Arc::clone(&log);
        let completion_slot = Arc::new(Mutex::new(None::<Completion>));
        let hook_completion = Arc::clone(&completion_slot);
        let hook_saw = Arc::new(Mutex::new(None));
        let hook_report = Arc::clone(&hook_saw);
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            on_owner_thread_stop: Some(Box::new(move || {
                *hook_report.lock().unwrap() = hook_completion
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|completion| completion.borrow().is_none());
                hook_log.lock().unwrap().push((
                    "hook",
                    std::thread::current().id(),
                    Instant::now(),
                ));
            })),
            ..options("rox-hosted-stop-hook", 2)
        })
        .expect("worker starts");
        *completion_slot.lock().unwrap() = Some(worker.completion());
        let handle = worker.handle();
        let probe = |label| LogProbe {
            label,
            log: Arc::clone(&log),
        };
        let (command, cleanup, local, runtime, blocking) = (
            probe("command"),
            probe("cleanup"),
            probe("local"),
            probe("runtime"),
            probe("blocking"),
        );
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        let _probe = cleanup;
                        tokio::task::yield_now().await;
                    })
                    .expect("cleanup registered while open");
                drop(tokio::task::spawn_local(async move {
                    let _probe = local;
                    pending::<()>().await;
                }));
                drop(tokio::spawn(async move {
                    let _probe = runtime;
                    pending::<()>().await;
                }));
                drop(tokio::task::spawn_blocking(move || {
                    let _probe = blocking;
                    std::thread::sleep(Duration::from_millis(50));
                }));
                let _probe = command;
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");

        if bounded {
            assert!(worker.shutdown_by(Instant::now() + Duration::from_secs(3600)));
        } else {
            assert!(worker.shutdown());
        }
        assert_eq!(completed(worker.completion()), Ok(()));
        let log = log.lock().unwrap();
        let mut entries: Vec<_> = log
            .iter()
            .map(|(label, thread, _)| (*label, *thread == handle.owner_thread()))
            .collect();
        assert_eq!(
            entries.pop(),
            Some(("hook", true)),
            "the hook ran last, on the owner thread, bounded: {bounded}: {log:?}"
        );
        entries.sort_unstable();
        assert_eq!(
            entries,
            [
                ("blocking", false),
                ("cleanup", true),
                ("command", true),
                ("local", true),
                ("runtime", true),
            ],
            "every task was dropped, the worker's on the owner thread, before the hook ran, \
             bounded: {bounded}"
        );
        assert_eq!(
            *hook_saw.lock().unwrap(),
            Some(true),
            "completion was published after the hook, bounded: {bounded}"
        );
        drop(worker);
    }

    #[test]
    fn a_panicking_owner_thread_stop_hook_is_reported_as_an_owner_thread_panic() {
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            on_owner_thread_stop: Some(Box::new(|| {
                panic_any("injected owner-thread stop hook panic");
            })),
            ..options("rox-hosted-stop-hook-panic", 1)
        })
        .expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let completion = worker.completion();
        assert!(
            drop_off_owner_thread(worker).is_ok(),
            "Drop does not unwind"
        );
        assert_eq!(exited.try_recv(), Ok(handle.owner_thread()));
        assert_eq!(
            completion.borrow().clone(),
            Some(Err(ManagedWorkerError::OwnerThreadPanicked))
        );
    }

    #[test]
    fn blocking_pool_threads_run_the_stop_hook_as_they_exit() {
        for blocking_work in [true, false] {
            let (started, mut pool) = blocking_pool("rox-w3-blk");
            let stopped = Arc::new(Mutex::new(Vec::new()));
            let log = Arc::clone(&stopped);
            pool.on_thread_stop = Some(Arc::new(move || {
                log.lock().unwrap().push((
                    std::thread::current().name().map(str::to_owned),
                    STARTED_BY_HOOK.with(std::cell::Cell::get),
                ));
            }));
            let worker = HostedWorker::spawn_with_blocking_pool(options("rox-w3", 1), pool)
                .expect("worker starts");
            if blocking_work {
                wait(
                    worker
                        .handle()
                        .call_local(|_context| async {
                            tokio::task::spawn_blocking(|| ())
                                .await
                                .expect("blocking work completes");
                        })
                        .expect("probe admitted"),
                )
                .expect("probe completes");
            }
            drop(worker);
            let expected = if blocking_work {
                vec![(Some("rox-w3-blk".to_owned()), true)]
            } else {
                Vec::new()
            };
            assert_eq!(
                *stopped.lock().unwrap(),
                expected,
                "the stop hook ran on each blocking thread, the start hook's, as the runtime \
                 joined it, and never on the owner thread"
            );
            assert_eq!(started.lock().unwrap().len(), expected.len());
        }
    }

    #[test]
    fn join_by_detaches_an_owner_thread_still_tearing_down_at_its_deadline() {
        let worker = HostedWorker::spawn(options("rox-hosted-detach", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let completion = worker.completion();
        let (release, finished) = hold_blocking_work(&handle, false);

        let deadline = Instant::now() + Duration::from_millis(200);
        let (joined, returned) = join_off_owner_thread(deadline, move || worker.join_by(deadline));
        assert_eq!(joined, HostedWorkerJoin::Detached);
        assert!(returned >= deadline && returned < deadline + LATENESS);
        assert!(
            completion.borrow().is_none(),
            "without a teardown deadline, teardown still waits for blocking work"
        );
        assert!(exited.try_recv().is_err(), "the owner thread still runs");

        release.send(()).expect("the blocking work still waits");
        assert!(finished.recv_timeout(TEST_TIMEOUT).is_ok());
        assert_eq!(completed(completion), Ok(()));
        assert_eq!(
            exited.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the detached owner thread exits once its teardown finishes"
        );
    }

    #[test]
    fn drop_after_shutdown_by_returns_by_the_teardown_deadline() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-deadline", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_timed_thread_exit(&handle);
        let (release, finished) = hold_blocking_work(&handle, true);

        let deadline = Instant::now() + TEARDOWN_DEADLINE;
        assert!(worker.shutdown_by(deadline));
        let ((), returned) = join_off_owner_thread(deadline, move || drop(worker));
        assert!(
            returned < deadline + LATENESS,
            "Drop returned by the deadline"
        );
        let (thread, exited_at) = exited
            .recv_timeout(TEST_TIMEOUT)
            .expect("the owner thread exits");
        assert_eq!(thread, handle.owner_thread());
        assert!(exited_at >= deadline);
        assert_eq!(finished.try_recv(), Err(mpsc::TryRecvError::Empty));
        release.send(()).expect("the blocking work still waits");
        assert!(finished.recv_timeout(TEST_TIMEOUT).is_ok());
    }

    /// Reports that the owner thread's thread-local destructors reached it, holds
    /// them until released (or until the release sender is dropped), then
    /// reports that it returned.
    struct HeldThreadExit {
        entered: mpsc::Sender<ThreadId>,
        release: mpsc::Receiver<()>,
        returned: mpsc::Sender<()>,
    }

    impl Drop for HeldThreadExit {
        fn drop(&mut self) {
            let _ = self.entered.send(std::thread::current().id());
            let _ = self.release.recv();
            let _ = self.returned.send(());
        }
    }

    thread_local! {
        static HELD_THREAD_EXIT: RefCell<Option<HeldThreadExit>> = const { RefCell::new(None) };
    }

    /// Installs a thread-local destructor on the owner thread that runs after
    /// `completion()` is published and holds the thread until the returned
    /// sender releases it; the receivers report its entry and its return.
    fn hold_thread_exit(
        handle: &WorkerHandle,
    ) -> (
        mpsc::Sender<()>,
        mpsc::Receiver<ThreadId>,
        mpsc::Receiver<()>,
    ) {
        let (release_tx, release) = mpsc::channel();
        let (entered, entered_rx) = mpsc::channel();
        let (returned, returned_rx) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    HELD_THREAD_EXIT.with(|slot| {
                        *slot.borrow_mut() = Some(HeldThreadExit {
                            entered,
                            release,
                            returned,
                        });
                    });
                })
                .expect("held exit admitted"),
        )
        .expect("held exit installed");
        (release_tx, entered_rx, returned_rx)
    }

    #[test]
    fn bounded_joins_return_by_their_deadline_while_a_thread_local_destructor_holds_the_owner_thread()
     {
        for by_drop in [false, true] {
            let worker =
                HostedWorker::spawn(options("rox-hosted-held-tls", 2)).expect("worker starts");
            let handle = worker.handle();
            let completion = worker.completion();
            let (release, entered, returned_from_destructor) = hold_thread_exit(&handle);

            let deadline = Instant::now() + TEARDOWN_DEADLINE;
            assert!(worker.shutdown_by(deadline));
            let (joined, returned) = if by_drop {
                let ((), returned) = join_off_owner_thread(deadline, move || drop(worker));
                (None, returned)
            } else {
                let (joined, returned) =
                    join_off_owner_thread(deadline, move || worker.join_by(deadline));
                (Some(joined), returned)
            };
            assert!(
                joined.is_none_or(|joined| joined == HostedWorkerJoin::Detached),
                "join_by detached the owner thread held in its destructors, by drop: {by_drop}"
            );
            assert!(
                returned >= deadline && returned < deadline + LATENESS,
                "the join waited for the thread's exit until the deadline, and no longer, by drop: \
                 {by_drop}"
            );
            assert_eq!(
                entered.try_recv(),
                Ok(handle.owner_thread()),
                "the owner thread was in its thread-local destructors"
            );
            assert_eq!(completion.borrow().clone(), Some(Ok(())));
            assert_eq!(
                returned_from_destructor.try_recv(),
                Err(mpsc::TryRecvError::Empty),
                "the destructor still holds the detached owner thread"
            );
            release.send(()).expect("the destructor still waits");
            assert_eq!(
                returned_from_destructor.recv_timeout(TEST_TIMEOUT),
                Ok(()),
                "the detached owner thread finishes its destructors once released"
            );
        }
    }

    #[test]
    fn a_join_after_its_deadline_still_reports_an_owner_thread_that_exited_in_time() {
        let held = HostedWorker::spawn(options("rox-hosted-held-first", 2)).expect("worker starts");
        let healthy =
            HostedWorker::spawn(options("rox-hosted-exits-early", 2)).expect("worker starts");
        let healthy_exited = record_timed_thread_exit(&healthy.handle());
        let (release, _, _) = hold_thread_exit(&held.handle());

        let deadline = Instant::now() + TEARDOWN_DEADLINE;
        assert!(held.shutdown_by(deadline));
        assert!(healthy.shutdown_by(deadline));
        // Joined in order, as one join thread would: the held worker uses up the
        // deadline, so the healthy one is joined after it.
        let (joined, _) = join_off_owner_thread(deadline, move || {
            [held, healthy].map(|worker| worker.join_by(deadline))
        });
        let (_, healthy_exited_at) = healthy_exited
            .try_recv()
            .expect("the healthy owner thread exited");
        assert!(healthy_exited_at < deadline);
        assert_eq!(
            joined,
            [HostedWorkerJoin::Detached, HostedWorkerJoin::Joined(Ok(()))],
            "a worker whose owner thread exited before the deadline is joined, though the join \
             starts after it"
        );
        release.send(()).expect("the destructor still waits");
    }

    #[test]
    fn a_shorter_deadline_set_during_the_runtimes_blocking_wait_ends_the_teardown_by_it() {
        let worker = HostedWorker::spawn(options("rox-hosted-shorten", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_timed_thread_exit(&handle);
        let completion = worker.completion();
        let (release, finished) = hold_blocking_work(&handle, false);
        // The runtime drops its tasks before it waits for blocking work, so this
        // task's drop marks that the teardown has reached the runtime.
        let (runtime_dropped_tx, runtime_dropped) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    drop(tokio::spawn(async move {
                        let _probe = TimedDropProbe(runtime_dropped_tx);
                        pending::<()>().await;
                    }));
                })
                .expect("runtime task admitted"),
        )
        .expect("runtime task spawned");

        assert!(worker.shutdown_by(Instant::now() + Duration::from_secs(3600)));
        runtime_dropped
            .recv_timeout(TEST_TIMEOUT)
            .expect("the teardown drops the runtime's tasks before waiting for blocking work");
        // Lets the owner thread finish dropping the runtime and start waiting, by the
        // far deadline, for the held blocking work.
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            completion.borrow().is_none(),
            "the runtime teardown waits for the held blocking work"
        );
        let deadline = Instant::now() + TEARDOWN_DEADLINE;
        assert!(
            !worker.shutdown_by(deadline),
            "shutdown had already started"
        );
        let join_deadline = deadline + JOIN_SLACK;
        let (joined, returned) =
            join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
        assert_eq!(joined, HostedWorkerJoin::Joined(Ok(())));
        assert!(
            returned < deadline + LATENESS,
            "the teardown ended by the shorter deadline"
        );
        let (thread, exited_at) = exited
            .try_recv()
            .expect("the join waited for the owner thread to exit");
        assert_eq!(thread, handle.owner_thread());
        assert!(exited_at >= deadline, "the owner thread waited until then");
        assert_eq!(
            finished.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the blocking work was abandoned, still running"
        );
        release.send(()).expect("the blocking work still waits");
        assert!(finished.recv_timeout(TEST_TIMEOUT).is_ok());
    }

    #[test]
    fn a_runtime_task_whose_drop_releases_blocking_work_lets_a_bounded_teardown_finish_promptly() {
        let worker = HostedWorker::spawn(options("rox-hosted-gated", 2)).expect("worker starts");
        let handle = worker.handle();
        let (blocked_tx, blocked) = mpsc::channel();
        let (finished_tx, finished) = mpsc::channel();
        let (task_dropped_tx, task_dropped) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    let (gate_tx, gate) = mpsc::channel::<()>();
                    drop(tokio::task::spawn_blocking(move || {
                        blocked_tx
                            .send(())
                            .expect("test observes the blocking work");
                        // Returns once the runtime task holding the sender is dropped.
                        let _ = gate.recv();
                        let _ = finished_tx.send(());
                    }));
                    drop(tokio::spawn(async move {
                        let _gate = gate_tx;
                        let _probe = DropProbe(task_dropped_tx);
                        pending::<()>().await;
                    }));
                })
                .expect("work admitted"),
        )
        .expect("work started");
        blocked
            .recv_timeout(TEST_TIMEOUT)
            .expect("the blocking work waits on the runtime task");

        let start = Instant::now();
        let deadline = start + Duration::from_secs(5);
        assert!(worker.shutdown_by(deadline));
        let (joined, returned) = join_off_owner_thread(deadline, move || worker.join_by(deadline));
        assert_eq!(joined, HostedWorkerJoin::Joined(Ok(())));
        assert!(
            returned < start + Duration::from_secs(2),
            "dropping the runtime's tasks first released the blocking work, so the teardown \
             finished well before its deadline"
        );
        assert_eq!(
            finished.try_recv(),
            Ok(()),
            "the blocking work ran to its end"
        );
        let dropped_on = task_dropped
            .try_recv()
            .expect("the runtime task was dropped");
        assert_eq!(
            dropped_on,
            handle.owner_thread(),
            "with a teardown deadline the runtime's tasks are still dropped on the owner thread"
        );
    }

    #[test]
    fn a_command_panic_is_still_reported_when_its_cleanup_overruns_the_teardown_deadline() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-panic-deadline", 2)).expect("worker starts");
        let handle = worker.handle();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(pending::<()>())
                    .expect("cleanup registered while open");
                tokio::task::yield_now().await;
                panic!("injected hosted command panic");
            })
            .expect("command admitted");
        wait(handle.quiescing());
        assert_eq!(handle.state(), WorkerState::Closing);

        let deadline = Instant::now() + Duration::from_millis(200);
        assert!(!worker.shutdown_by(deadline), "the panic started shutdown");
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the panic is reported, not a clean overrun"
        );
    }

    #[test]
    fn a_driver_panic_is_still_reported_when_cleanup_overruns_the_teardown_deadline() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-driver-panic", 2)).expect("worker starts");
        let handle = worker.handle();
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(pending::<()>())
                    .expect("cleanup registered while open");
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");
        // The next driver pass on the owner thread panics outside any command.
        wait(
            handle
                .call(|_| {
                    crate::worker::set_driver_intake_hook(Some(|_: &WorkerHandle| {
                        panic_any("injected worker driver panic");
                    }));
                })
                .expect("hook admitted"),
        )
        .expect("hook installed");
        wait(handle.quiescing());
        assert_eq!(handle.state(), WorkerState::Closing);

        let deadline = Instant::now() + Duration::from_millis(200);
        assert!(!worker.shutdown_by(deadline), "the panic started shutdown");
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the driver's panic is reported, not a clean overrun"
        );
    }

    /// Registers `scope` as a worker scope, as an owned session's run is registered.
    fn start_scope(context: &WorkerContext, scope: impl Future<Output = ()> + 'static) {
        context
            .register_scope(scope)
            .expect("scope registered while open")
            .activate()
            .expect("scope activated while open")
            .detach();
    }

    thread_local! {
        /// The wheel timeout that an abandoned cleanup holds, kept on its owner thread past the
        /// final join for the worker's stop hook.
        static ABANDONED_TIMEOUT: RefCell<Option<Rc<RefCell<Timeout>>>> =
            const { RefCell::new(None) };
    }

    /// Reports, as it is dropped, whether the thread's Tokio host still has the worker's wheel
    /// and what `timeout` had completed with by then.
    struct WheelAtDrop {
        timeout: Rc<RefCell<Timeout>>,
        report: mpsc::Sender<(bool, Option<Expiry>)>,
    }

    impl Drop for WheelAtDrop {
        fn drop(&mut self) {
            let expired = self.timeout.borrow_mut().take_expired();
            let _ = self
                .report
                .send((Host::tokio().wheel_timer().is_some(), expired));
        }
    }

    /// Past its teardown deadline the supervisor destroys unfinished cleanup, which abandons the
    /// task groups that cleanup owns: here one still joining a task that never finishes, and one
    /// never closed. Their drops still cancel and drop their tasks on the owner thread, but do not
    /// assert that they were closed and joined: the worker reports a clean overrun. The cleanup
    /// that never finishes also holds a timeout armed on the owner thread's wheel, which the
    /// worker stops only after that final join: the cleanup is dropped with the wheel installed
    /// and the timeout still armed, then the timeout completes as stopped and its waker is woken
    /// once.
    #[test]
    fn a_forced_teardown_abandons_the_task_groups_of_unfinished_cleanup() {
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            on_owner_thread_stop: Some(Box::new(move || {
                let expired = ABANDONED_TIMEOUT
                    .take()
                    .and_then(|timeout| timeout.borrow_mut().take_expired());
                let _ = stopped_tx.send((Host::tokio().wheel_timer().is_some(), expired));
            })),
            ..options("rox-hosted-forced-groups", 2)
        })
        .expect("worker starts");
        let handle = worker.handle();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (joining_tx, joining_rx) = mpsc::channel();
        let (at_drop_tx, at_drop_rx) = mpsc::channel();
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        handle
            .spawn_local(move |context| async move {
                let mut joining = TaskGroup::new();
                joining
                    .spawn_local(move |_| async move {
                        let _probe = DropProbe(dropped_tx);
                        pending::<()>().await;
                    })
                    .expect("task admitted");
                start_scope(&context, async move {
                    joining.cancel();
                    joining_tx.send(()).expect("test observes the join");
                    let _ = joining.join().await;
                });
                let open = TaskGroup::new();
                let host = context.host().clone();
                let timeout = Rc::new(RefCell::new(Timeout::new(&host)));
                timeout
                    .borrow_mut()
                    .schedule_at(host.now() + Duration::from_secs(600), &waker);
                ABANDONED_TIMEOUT.set(Some(Rc::clone(&timeout)));
                let at_drop = WheelAtDrop {
                    timeout,
                    report: at_drop_tx,
                };
                start_scope(&context, async move {
                    let _open = open;
                    let _at_drop = at_drop;
                    pending::<()>().await;
                });
            })
            .expect("command admitted");
        joining_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the cleanup is joining its group");

        // The passed deadline can wake the supervisor, which then begins the shutdown itself.
        worker.shutdown_by(Instant::now());
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TeardownDeadlineExceeded),
            "abandoning the cleanup's task groups is a clean overrun"
        );
        assert_eq!(
            dropped_rx.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the abandoned group's task was dropped on the owner thread"
        );
        assert_eq!(
            at_drop_rx.recv_timeout(TEST_TIMEOUT),
            Ok((true, None)),
            "(wheel installed, timeout's expiry) as the final join dropped the cleanup"
        );
        assert_eq!(
            stopped_rx.recv_timeout(TEST_TIMEOUT),
            Ok((false, Some(Expiry::Stopped))),
            "(wheel installed, timeout's expiry) in the owner thread's stop hook"
        );
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "stopping the wheel woke the abandoned timeout's waker once"
        );
    }

    /// Panics with its message as it drops.
    struct PanicOnDrop(&'static str);

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("{}", self.0);
        }
    }

    /// A forced teardown abandons the joins of the task groups it destroys, not the panics their
    /// tasks raise as they are destroyed: the worker reports one, as it does a cleanup's.
    #[test]
    fn a_forced_teardown_still_reports_a_task_that_panics_as_its_abandoned_group_drops_it() {
        let worker = HostedWorker::spawn(options("rox-hosted-forced-group-panic", 2))
            .expect("worker starts");
        let handle = worker.handle();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (joining_tx, joining_rx) = mpsc::channel();
        handle
            .spawn_local(move |context| async move {
                let mut joining = TaskGroup::new();
                joining
                    .spawn_local(move |_| async move {
                        let _probe = DropProbe(dropped_tx);
                        let _panics = PanicOnDrop("injected task destructor panic");
                        pending::<()>().await;
                    })
                    .expect("task admitted");
                start_scope(&context, async move {
                    joining.cancel();
                    joining_tx.send(()).expect("test observes the join");
                    let _ = joining.join().await;
                });
            })
            .expect("command admitted");
        joining_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the cleanup is joining its group");

        worker.shutdown_by(Instant::now());
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the task's destructor panic is still reported"
        );
        assert_eq!(
            dropped_rx.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the abandoned group's task was dropped on the owner thread"
        );
    }

    /// Fields drop in order: the panic first, then the group, during that unwind.
    struct GroupDroppedWhileUnwinding {
        _panics: PanicOnDrop,
        _group: TaskGroup,
    }

    /// A forced teardown that destroys an abandoned group while the thread already unwinds
    /// discards the panic a task of that group raises as it is destroyed, rather than aborting
    /// the process: the worker reports the panic that was unwinding.
    #[test]
    fn a_forced_teardown_discards_a_task_panic_its_abandoned_group_raises_while_unwinding() {
        let worker = HostedWorker::spawn(options("rox-hosted-forced-group-unwind", 2))
            .expect("worker starts");
        let handle = worker.handle();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (held_tx, held_rx) = mpsc::channel();
        handle
            .spawn_local(move |context| async move {
                let mut group = TaskGroup::new();
                group
                    .spawn_local(move |_| async move {
                        let _probe = DropProbe(dropped_tx);
                        let _panics = PanicOnDrop("injected task destructor panic");
                        pending::<()>().await;
                    })
                    .expect("task admitted");
                start_scope(&context, async move {
                    let _held = GroupDroppedWhileUnwinding {
                        _panics: PanicOnDrop("injected cleanup destructor panic"),
                        _group: group,
                    };
                    held_tx.send(()).expect("test observes the cleanup");
                    pending::<()>().await;
                });
            })
            .expect("command admitted");
        held_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the cleanup holds its group");

        worker.shutdown_by(Instant::now());
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the cleanup's destructor panic is reported"
        );
        assert_eq!(
            dropped_rx.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the abandoned group's task was dropped on the owner thread"
        );
    }

    /// Raises a panic whose payload's destructor panics too, as it drops.
    struct PayloadPanicOnDrop;

    impl Drop for PayloadPanicOnDrop {
        fn drop(&mut self) {
            panic_any(PanickingPayload);
        }
    }

    /// A forced teardown that destroys an abandoned group whose cancellation runs a waker that
    /// panics disposes of the panic a task of that group raises as it is destroyed, here one whose
    /// payload's destructor panics too, before that cancellation's panic unwinds, rather than
    /// aborting the process: the worker reports the cancellation's panic.
    #[test]
    fn a_forced_teardown_disposes_of_a_task_panic_when_its_abandoned_group_s_cancellation_panics() {
        let worker = HostedWorker::spawn(options("rox-hosted-forced-group-cancel", 2))
            .expect("worker starts");
        let handle = worker.handle();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (registered_tx, registered_rx) = mpsc::channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        let cancellation_wakes = Arc::clone(&wakes);
        handle
            .spawn_local(move |context| async move {
                let mut group = TaskGroup::new();
                group
                    .spawn_local(move |cancellation| async move {
                        let _probe = DropProbe(dropped_tx);
                        let _panics = PayloadPanicOnDrop;
                        let mut registration = cancellation.wake_registration();
                        let waker = Waker::from(Arc::new(PanickingWake(cancellation_wakes)));
                        assert!(!registration.update_waker(&waker));
                        registered_tx
                            .send(())
                            .expect("test observes the registration");
                        pending::<()>().await;
                    })
                    .expect("task admitted");
                start_scope(&context, async move {
                    let _group = group;
                    pending::<()>().await;
                });
            })
            .expect("command admitted");
        registered_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the task registered its cancellation waker");

        worker.shutdown_by(Instant::now());
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the cancellation's panic is reported"
        );
        assert_eq!(
            dropped_rx.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the abandoned group's task was dropped on the owner thread"
        );
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "the abandoned group's cancellation ran the panicking waker once"
        );
    }

    /// A forced teardown that destroys an abandoned group while the thread already unwinds
    /// discards the panic of that group's cancellation, here a waker that panics, and the panic a
    /// task of that group raises as it is destroyed, here one whose payload's destructor panics
    /// too, rather than aborting the process: the worker reports the panic that was unwinding.
    #[test]
    fn a_forced_teardown_discards_its_abandoned_group_s_cancellation_panic_while_unwinding() {
        let worker = HostedWorker::spawn(options("rox-hosted-forced-group-cancel-unwind", 2))
            .expect("worker starts");
        let handle = worker.handle();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let (registered_tx, registered_rx) = mpsc::channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        let cancellation_wakes = Arc::clone(&wakes);
        handle
            .spawn_local(move |context| async move {
                let mut group = TaskGroup::new();
                group
                    .spawn_local(move |cancellation| async move {
                        let _probe = DropProbe(dropped_tx);
                        let _panics = PayloadPanicOnDrop;
                        let mut registration = cancellation.wake_registration();
                        let waker = Waker::from(Arc::new(PanickingWake(cancellation_wakes)));
                        assert!(!registration.update_waker(&waker));
                        registered_tx
                            .send(())
                            .expect("test observes the registration");
                        pending::<()>().await;
                    })
                    .expect("task admitted");
                start_scope(&context, async move {
                    let _held = GroupDroppedWhileUnwinding {
                        _panics: PanicOnDrop("injected cleanup destructor panic"),
                        _group: group,
                    };
                    pending::<()>().await;
                });
            })
            .expect("command admitted");
        registered_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("the task registered its cancellation waker");

        worker.shutdown_by(Instant::now());
        assert_eq!(
            completed(worker.completion()),
            Err(ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)),
            "the cleanup's destructor panic is reported"
        );
        assert_eq!(
            dropped_rx.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the abandoned group's task was dropped on the owner thread"
        );
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "the abandoned group's cancellation ran the panicking waker once"
        );
    }

    /// The text of the panic that `drop` raised.
    fn drop_panic(drop: impl FnOnce()) -> String {
        let payload = catch_unwind(AssertUnwindSafe(drop)).expect_err("the drop asserts");
        payload
            .downcast_ref::<&str>()
            .map(|message| (*message).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .expect("a text panic")
    }

    /// Unclosed and unjoined task groups that the worker's owner thread drops.
    fn ordinary_task_group_drops() -> [String; 2] {
        let mut unjoined = TaskGroup::new();
        unjoined
            .spawn_local(|_| pending::<()>())
            .expect("task admitted");
        [
            drop_panic(|| drop(TaskGroup::new())),
            drop_panic(move || drop(unjoined)),
        ]
    }

    /// Only a forced teardown abandons task groups: on a worker's owner thread, an unclosed or
    /// unjoined group still asserts as it drops, while the worker runs and during a bounded
    /// teardown whose deadline has not passed.
    #[test]
    fn task_group_drops_outside_a_forced_teardown_still_assert() {
        const ORDINARY: [&str; 2] = [
            "task group must be explicitly closed before drop",
            "task group must join every admitted task before drop",
        ];
        let worker =
            HostedWorker::spawn(options("rox-hosted-ordinary-groups", 2)).expect("worker starts");
        let handle = worker.handle();
        let running = wait(
            handle
                .call(|_| ordinary_task_group_drops())
                .expect("call admitted"),
        )
        .expect("call completes");
        assert_eq!(running, ORDINARY, "while the worker runs");

        let (tearing_down_tx, tearing_down_rx) = mpsc::channel();
        wait(
            handle
                .call(move |context| {
                    let quiescing = context.quiescing();
                    start_scope(context, async move {
                        quiescing.await;
                        tearing_down_tx
                            .send(ordinary_task_group_drops())
                            .expect("test observes the drops");
                    });
                })
                .expect("call admitted"),
        )
        .expect("call completes");
        assert!(worker.shutdown_by(Instant::now() + TEST_TIMEOUT));
        assert_eq!(completed(worker.completion()), Ok(()));
        assert_eq!(
            tearing_down_rx.recv_timeout(TEST_TIMEOUT),
            Ok(ORDINARY.map(str::to_owned)),
            "during a bounded teardown before its deadline"
        );
    }

    #[test]
    fn drop_after_shutdown_by_detaches_an_owner_thread_that_cannot_finish_by_the_deadline() {
        let worker =
            HostedWorker::spawn(options("rox-hosted-drop-detach", 2)).expect("worker starts");
        let handle = worker.handle();
        let exited = record_thread_exit(&handle);
        let completion = worker.completion();
        // A held borrow keeps the owner thread from publishing completion, which
        // no teardown deadline can bound.
        let borrowed = completion.borrow();

        let deadline = Instant::now() + Duration::from_millis(200);
        assert!(worker.shutdown_by(deadline));
        let ((), returned) = join_off_owner_thread(deadline, move || drop(worker));
        assert!(
            returned >= deadline && returned < deadline + LATENESS,
            "Drop waited until the deadline and then detached the owner thread"
        );
        assert!(borrowed.is_none());
        assert!(exited.try_recv().is_err(), "the owner thread still runs");
        drop(borrowed);
        assert_eq!(
            exited.recv_timeout(TEST_TIMEOUT),
            Ok(handle.owner_thread()),
            "the detached owner thread exits once it can publish"
        );
        assert_eq!(completion.borrow().clone(), Some(Ok(())));
    }

    /// What a [`ThreadAffine`] value does when it is dropped off its owner thread.
    #[derive(Clone, Copy, Debug)]
    enum OnForeignDrop {
        /// Reports it, then panics (Tokio catches a panic dropping a task).
        Panic,
        /// Aborts the process, as the router's `LocalOnly` bridge does.
        Abort,
    }

    /// A value bound to the thread that creates it, such as a request body the
    /// router wraps in its `LocalOnly` Send bridge: it reports whether it was
    /// dropped on that thread and whether the owner-thread stop hook had run
    /// by then, and panics or aborts if it was dropped anywhere else.
    struct ThreadAffine {
        owner: ThreadId,
        on_foreign_drop: OnForeignDrop,
        hook_ran: Arc<AtomicBool>,
        report: mpsc::Sender<(bool, bool)>,
    }

    impl Drop for ThreadAffine {
        fn drop(&mut self) {
            let on_owner = std::thread::current().id() == self.owner;
            let _ = self
                .report
                .send((on_owner, self.hook_ran.load(Ordering::SeqCst)));
            if on_owner {
                return;
            }
            match self.on_foreign_drop {
                OnForeignDrop::Panic => {
                    panic!("a thread-affine value was dropped off its owner thread")
                }
                OnForeignDrop::Abort => {
                    eprintln!("a thread-affine value was dropped off its owner thread");
                    std::process::abort();
                }
            }
        }
    }

    /// Stops a worker with `shutdown_by` while a pending `tokio::spawn` task
    /// holds a [`ThreadAffine`] value created on the owner thread, as a hidden
    /// prefill drain holds a `LocalOnly` engine response. With `overrun`, a
    /// registered cleanup that never finishes holds the teardown past its
    /// deadline. Returns how the join ended and the value's report.
    fn stop_with_a_pending_thread_affine_runtime_task(
        overrun: bool,
        on_foreign_drop: OnForeignDrop,
    ) -> (HostedWorkerJoin, (bool, bool)) {
        let hook_ran = Arc::new(AtomicBool::new(false));
        let hook_flag = Arc::clone(&hook_ran);
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            on_owner_thread_stop: Some(Box::new(move || hook_flag.store(true, Ordering::SeqCst))),
            ..options("rox-hosted-affine", 2)
        })
        .expect("worker starts");
        let handle = worker.handle();
        let (report_tx, report) = mpsc::channel();
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .spawn_local(move |context| async move {
                let _cleanup = overrun.then(|| {
                    context
                        .register_cleanup_task(pending::<()>())
                        .expect("cleanup registered while open")
                });
                let affine = ThreadAffine {
                    owner: std::thread::current().id(),
                    on_foreign_drop,
                    hook_ran,
                    report: report_tx,
                };
                drop(tokio::spawn(async move {
                    let _affine = affine;
                    pending::<()>().await;
                }));
                started_tx.send(()).expect("test observes startup");
                pending::<()>().await;
            })
            .expect("command admitted");
        wait(started_rx).expect("command parked");

        let deadline = Instant::now() + TEARDOWN_DEADLINE;
        assert!(worker.shutdown_by(deadline));
        let join_deadline = deadline + JOIN_SLACK;
        let (joined, _) =
            join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
        let report = report
            .recv_timeout(TEST_TIMEOUT)
            .expect("the runtime task's value was dropped");
        (joined, report)
    }

    #[test]
    fn a_pending_runtime_task_is_dropped_on_the_owner_thread_before_the_hook_under_a_teardown_deadline()
     {
        for (overrun, expected) in [
            (false, HostedWorkerJoin::Joined(Ok(()))),
            (
                true,
                HostedWorkerJoin::Joined(Err(ManagedWorkerError::TeardownDeadlineExceeded)),
            ),
        ] {
            let (joined, (on_owner, hook_ran)) =
                stop_with_a_pending_thread_affine_runtime_task(overrun, OnForeignDrop::Panic);
            assert_eq!(joined, expected, "overrun: {overrun}");
            assert!(
                on_owner,
                "the runtime task was dropped on the owner thread, overrun: {overrun}"
            );
            assert!(
                !hook_ran,
                "the runtime task was dropped before the stop hook ran, overrun: {overrun}"
            );
        }
    }

    const THREAD_AFFINE_CHILD: &str = "ROXYGEN_HOSTED_WORKER_THREAD_AFFINE_CHILD";

    #[test]
    fn a_bounded_teardown_never_drops_a_thread_affine_runtime_task_off_the_owner_thread() {
        if std::env::var_os(THREAD_AFFINE_CHILD).is_some() {
            for overrun in [false, true] {
                let (_, report) =
                    stop_with_a_pending_thread_affine_runtime_task(overrun, OnForeignDrop::Abort);
                assert_eq!(report, (true, false), "overrun: {overrun}");
            }
            return;
        }
        let child = run_in_child_process(
            "a_bounded_teardown_never_drops_a_thread_affine_runtime_task_off_the_owner_thread",
            THREAD_AFFINE_CHILD,
        );
        let stdout = String::from_utf8_lossy(&child.stdout);
        assert!(
            child.status.success() && stdout.contains("1 passed"),
            "a pending runtime task holding a value that aborts the process when dropped off its \
             owner thread survives a clean and an overrun bounded teardown: {}\nstdout:\n\
             {stdout}\nstderr:\n{}",
            child.status,
            String::from_utf8_lossy(&child.stderr)
        );
    }

    #[test]
    fn a_bounded_teardown_waits_for_a_blocking_thread_still_in_its_start_hook() {
        let (entered_tx, entered) = mpsc::channel();
        let (gate_tx, gate) = mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        let worker = HostedWorker::spawn_with_blocking_pool(
            options("rox-w4", 2),
            HostedWorkerBlockingPool {
                thread_name: "rox-w4-blk".to_owned(),
                on_thread_start: Some(Arc::new(move || {
                    let _ = entered_tx.send(());
                    let _ = gate.lock().unwrap().recv();
                })),
                on_thread_stop: None,
            },
        )
        .expect("worker starts");
        let handle = worker.handle();
        let completion = worker.completion();
        let (finished_tx, finished) = mpsc::channel();
        wait(
            handle
                .call(move |_| {
                    drop(tokio::task::spawn_blocking(move || {
                        let _ = finished_tx.send(());
                    }));
                })
                .expect("work admitted"),
        )
        .expect("work spawned");
        entered
            .recv_timeout(TEST_TIMEOUT)
            .expect("the blocking thread runs its start hook");

        assert!(worker.shutdown_by(Instant::now() + Duration::from_secs(3600)));
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            completion.borrow().is_none(),
            "the teardown waits for a blocking thread that has not finished starting"
        );
        drop(gate_tx);
        assert_eq!(completed(completion), Ok(()));
        assert_eq!(
            finished.try_recv(),
            Ok(()),
            "the work queued for that thread ran before completion was published"
        );
        drop(worker);
    }

    #[test]
    fn a_bounded_teardown_waits_for_blocking_work_its_cleanup_spawns_as_it_ends() {
        // Each round's teardown reaches its wait for blocking-pool threads microseconds
        // after the cleanup spawned the thread, often before that thread has started.
        for round in 0..500 {
            let finished = Arc::new(AtomicBool::new(false));
            let hook_saw = Arc::new(Mutex::new(None));
            let (hook_finished, hook_report) = (Arc::clone(&finished), Arc::clone(&hook_saw));
            let worker = HostedWorker::spawn(HostedWorkerOptions {
                on_owner_thread_stop: Some(Box::new(move || {
                    *hook_report.lock().unwrap() = Some(hook_finished.load(Ordering::SeqCst));
                })),
                ..options("rox-hosted-late-blocking", 2)
            })
            .expect("worker starts");
            let work_finished = Arc::clone(&finished);
            let (started_tx, started_rx) = oneshot::channel();
            worker
                .handle()
                .spawn_local(move |context| async move {
                    let _cleanup = context
                        .register_cleanup_task(async move {
                            drop(tokio::task::spawn_blocking(move || {
                                work_finished.store(true, Ordering::SeqCst);
                            }));
                        })
                        .expect("cleanup registered while open");
                    started_tx.send(()).expect("test observes startup");
                    pending::<()>().await;
                })
                .expect("command admitted");
            wait(started_rx).expect("command parked");

            assert!(worker.shutdown_by(Instant::now() + Duration::from_secs(3600)));
            assert_eq!(completed(worker.completion()), Ok(()));
            assert_eq!(
                *hook_saw.lock().unwrap(),
                Some(true),
                "the blocking work the cleanup spawned finished before the stop hook, round \
                 {round}"
            );
            drop(worker);
        }
    }

    #[test]
    fn a_dropped_worker_releases_its_runtime_and_blocking_pool_hooks() {
        for bounded in [false, true] {
            let sentinel = Arc::new(());
            let held = Arc::clone(&sentinel);
            let worker = HostedWorker::spawn_with_blocking_pool(
                options("rox-w7", 1),
                HostedWorkerBlockingPool {
                    thread_name: "rox-w7-blk".to_owned(),
                    on_thread_start: Some(Arc::new(move || {
                        let _ = &held;
                    })),
                    on_thread_stop: None,
                },
            )
            .expect("worker starts");
            if bounded {
                assert!(worker.shutdown_by(Instant::now() + TEST_TIMEOUT));
            }
            assert!(drop_off_owner_thread(worker).is_ok());
            assert_eq!(
                Arc::strong_count(&sentinel),
                1,
                "the runtime, which owns the blocking-pool hooks, was released, bounded: {bounded}"
            );
        }
    }

    /// Stops a worker whose blocking pool ran a thread with `pool`'s hooks
    /// under a far teardown deadline; returns how long the join took.
    fn bounded_teardown_after_a_blocking_thread(
        thread_name: &str,
        pool: HostedWorkerBlockingPool,
        started: mpsc::Receiver<()>,
    ) -> (HostedWorkerJoin, Duration) {
        let worker = HostedWorker::spawn_with_blocking_pool(options(thread_name, 2), pool)
            .expect("worker starts");
        wait(
            worker
                .handle()
                .call(|_| drop(tokio::task::spawn_blocking(|| ())))
                .expect("work admitted"),
        )
        .expect("work spawned");
        started
            .recv_timeout(TEST_TIMEOUT)
            .expect("the blocking thread starts");

        let start = Instant::now();
        assert!(worker.shutdown_by(start + Duration::from_secs(3600)));
        let join_deadline = start + Duration::from_secs(5);
        let (joined, returned) =
            join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
        (joined, returned - start)
    }

    #[test]
    fn a_panicking_blocking_stop_hook_does_not_hold_a_bounded_teardown() {
        let (started_tx, started) = mpsc::channel();
        let (joined, took) = bounded_teardown_after_a_blocking_thread(
            "rox-w5",
            HostedWorkerBlockingPool {
                thread_name: "rox-w5-blk".to_owned(),
                on_thread_start: Some(Arc::new(move || {
                    let _ = started_tx.send(());
                })),
                on_thread_stop: Some(Arc::new(|| {
                    panic_any("injected blocking stop hook panic");
                })),
            },
            started,
        );
        assert_eq!(joined, HostedWorkerJoin::Joined(Ok(())));
        assert!(
            took < Duration::from_secs(2),
            "the blocking thread counted as exited though its stop hook panicked: {took:?}"
        );
    }

    #[test]
    fn a_panicking_blocking_start_hook_does_not_hold_a_bounded_teardown() {
        let (panicked_tx, panicked) = mpsc::channel();
        let first = AtomicBool::new(true);
        let worker = HostedWorker::spawn_with_blocking_pool(
            options("rox-w6", 2),
            HostedWorkerBlockingPool {
                thread_name: "rox-w6-blk".to_owned(),
                on_thread_start: Some(Arc::new(move || {
                    if first.swap(false, Ordering::SeqCst) {
                        let _ = panicked_tx.send(());
                        panic_any("injected blocking start hook panic");
                    }
                })),
                on_thread_stop: None,
            },
        )
        .expect("worker starts");
        let handle = worker.handle();
        wait(
            handle
                .call(|_| drop(tokio::task::spawn_blocking(|| ())))
                .expect("work admitted"),
        )
        .expect("work spawned");
        panicked
            .recv_timeout(TEST_TIMEOUT)
            .expect("the first blocking thread's start hook panics");
        // A second blocking thread runs this and the work the first one left queued,
        // which Tokio would otherwise keep queued, holding the runtime, forever.
        wait(
            handle
                .call_local(|_| async {
                    tokio::task::spawn_blocking(|| ())
                        .await
                        .expect("blocking work completes");
                })
                .expect("work admitted"),
        )
        .expect("work completes");

        let start = Instant::now();
        assert!(worker.shutdown_by(start + Duration::from_secs(3600)));
        let join_deadline = start + Duration::from_secs(5);
        let (joined, returned) =
            join_off_owner_thread(join_deadline, move || worker.join_by(join_deadline));
        assert_eq!(joined, HostedWorkerJoin::Joined(Ok(())));
        assert!(
            returned - start < Duration::from_secs(2),
            "the blocking thread whose start hook panicked counted as exited: {:?}",
            returned - start
        );
    }
}
