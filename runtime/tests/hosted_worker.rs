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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread::ThreadId;
    use std::time::Duration;

    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::sync::watch;

    use crate::task_group::TaskGroup;
    use crate::task_group::TaskGroupError;
    use crate::tokio_host::HostedWorker;
    use crate::tokio_host::HostedWorkerOptions;
    use crate::tokio_host::ManagedWorkerError;
    use crate::worker::QuiesceDeadline;
    use crate::worker::SubmitError;
    use crate::worker::WorkerCallError;
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
}
