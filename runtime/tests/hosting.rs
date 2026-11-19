/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod hosting {
    use std::future::pending;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::thread::ThreadId;

    use tokio::runtime::Builder;

    use crate::driver::WorkerDriver;
    use crate::tokio_host::CallerDrivenGuard;
    use crate::tokio_host::DriverError;
    use crate::tokio_host::ManagedWorker;
    use crate::tokio_host::caller_driven_worker;
    use crate::worker::SubmitError;
    use crate::worker::WorkerCall;
    use crate::worker::WorkerCleanupRegistration;
    use crate::worker::WorkerContext;
    use crate::worker::WorkerGuard;
    use crate::worker::WorkerHandle;

    macro_rules! assert_not_send {
        ($name:ident, $type:ty) => {
            const $name: fn() = || {
                struct Invalid;
                trait AmbiguousIfSend<A> {
                    fn marker() {}
                }
                impl<T: ?Sized> AmbiguousIfSend<()> for T {}
                impl<T: ?Sized + Send> AmbiguousIfSend<Invalid> for T {}
                let _ = <$type as AmbiguousIfSend<_>>::marker;
            };
        };
    }

    assert_not_send!(WORKER_CONTEXT_IS_NOT_SEND, WorkerContext);
    assert_not_send!(
        WORKER_CLEANUP_REGISTRATION_IS_NOT_SEND,
        WorkerCleanupRegistration
    );
    assert_not_send!(WORKER_GUARD_IS_NOT_SEND, WorkerGuard);
    assert_not_send!(WORKER_DRIVER_IS_NOT_SEND, WorkerDriver);
    assert_not_send!(CALLER_GUARD_IS_NOT_SEND, CallerDrivenGuard);

    fn runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    #[test]
    fn managed_thread_drives_local_set() {
        let caller = std::thread::current().id();
        let worker = ManagedWorker::new(4).expect("managed worker starts");
        let owner = worker.owner_thread();
        let call = worker
            .handle()
            .call_local(|context| async move {
                let local = Rc::new(3_u8);
                tokio::task::yield_now().await;
                (context.owner_thread(), std::thread::current().id(), *local)
            })
            .expect("local task admitted");
        let observed = runtime()
            .block_on(call)
            .expect("local task completes on managed driver");

        assert_ne!(owner, caller);
        assert_eq!(observed, (owner, owner, 3));
        worker.shutdown().expect("managed worker shuts down");
    }

    #[test]
    fn caller_drives_local_set_on_stable_thread() {
        let owner = std::thread::current().id();
        let (handle, mut guard) = caller_driven_worker(4).expect("caller driver starts");
        let call = handle
            .call_local(|context| async move {
                let local = Rc::new(5_u8);
                tokio::task::yield_now().await;
                (context.owner_thread(), std::thread::current().id(), *local)
            })
            .expect("local task admitted");
        let runtime = runtime();
        let observed = runtime
            .block_on(guard.run_until(call))
            .expect("owner thread accepted")
            .expect("worker call completes");

        assert_eq!(guard.owner_thread(), owner);
        assert_eq!(observed, (owner, owner, 5));
        runtime
            .block_on(guard.shutdown())
            .expect("caller-driven worker shuts down");
    }

    #[test]
    fn caller_driver_rejects_wrong_owner_before_poll() {
        let original_owner = std::thread::current().id();
        let other_thread = std::thread::spawn(|| std::thread::current().id())
            .join()
            .expect("thread reports its identity");
        let (_handle, mut guard) = caller_driven_worker(1).expect("caller driver starts");
        guard.set_checked_owner_thread_for_test(other_thread);
        let polls_before = guard.driver_poll_count();

        let error = runtime()
            .block_on(guard.run_until(std::future::ready(())))
            .expect_err("mismatched owner is rejected");
        assert_eq!(
            error,
            DriverError::WrongOwnerThread {
                expected: other_thread,
                actual: original_owner,
            },
        );
        assert_eq!(guard.driver_poll_count(), polls_before);

        guard.set_checked_owner_thread_for_test(original_owner);
        runtime()
            .block_on(guard.shutdown())
            .expect("caller-driven worker shuts down");
    }

    #[test]
    fn bounded_admission_rejects_overload() {
        let (handle, mut guard) = caller_driven_worker(1).expect("caller driver starts");
        let admitted = handle.call(|_| 11_u8).expect("first command admitted");
        assert_eq!(handle.submit(|_| {}), Err(SubmitError::Overloaded));
        assert_eq!(handle.queued_commands(), 1);

        let runtime = runtime();
        assert_eq!(
            runtime
                .block_on(guard.run_until(admitted))
                .expect("owner thread accepted")
                .expect("admitted command completes"),
            11,
        );
        runtime
            .block_on(guard.shutdown())
            .expect("caller-driven worker shuts down");
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
    fn callbacks_and_drop_run_on_owner_thread() {
        let managed = ManagedWorker::new(4).expect("managed worker starts");
        let managed_owner = managed.owner_thread();
        let (managed_started_tx, managed_started_rx) = mpsc::channel();
        let (managed_drop_tx, managed_drop_rx) = mpsc::channel();
        managed
            .handle()
            .spawn_local(move |context| async move {
                let _probe = DropProbe {
                    owner: context.owner_thread(),
                    handle: context.handle().clone(),
                    dropped: Some(managed_drop_tx),
                };
                managed_started_tx
                    .send(std::thread::current().id())
                    .expect("announce managed task start");
                pending::<()>().await;
            })
            .expect("managed task admitted");
        assert_eq!(
            managed_started_rx.recv().expect("managed task starts"),
            managed_owner,
        );
        managed.shutdown().expect("managed worker shuts down");
        assert_eq!(
            managed_drop_rx.recv().expect("managed task drops"),
            managed_owner,
        );

        let caller_owner = std::thread::current().id();
        let (handle, mut guard) = caller_driven_worker(4).expect("caller driver starts");
        let (caller_started_tx, caller_started_rx) = mpsc::channel();
        let (caller_drop_tx, caller_drop_rx) = mpsc::channel();
        handle
            .spawn_local(move |context| async move {
                let _probe = DropProbe {
                    owner: context.owner_thread(),
                    handle: context.handle().clone(),
                    dropped: Some(caller_drop_tx),
                };
                caller_started_tx
                    .send(std::thread::current().id())
                    .expect("announce caller-driven task start");
                pending::<()>().await;
            })
            .expect("caller-driven task admitted");
        let barrier = handle.call(|_| ()).expect("owner barrier admitted");
        let runtime = runtime();
        runtime
            .block_on(guard.run_until(barrier))
            .expect("owner thread accepted")
            .expect("owner barrier completes");
        assert_eq!(
            caller_started_rx.recv().expect("caller-driven task starts"),
            caller_owner,
        );
        runtime
            .block_on(guard.shutdown())
            .expect("caller-driven worker shuts down");
        assert_eq!(
            caller_drop_rx.recv().expect("caller-driven task drops"),
            caller_owner,
        );
    }

    #[test]
    fn send_handles_cannot_export_local_state() {
        fn assert_send_sync<T: Send + Sync>() {}
        fn assert_send<T: Send>() {}

        assert_send_sync::<WorkerHandle>();
        assert_send::<WorkerCall<usize>>();
        let _ = WORKER_CONTEXT_IS_NOT_SEND;
        let _ = WORKER_CLEANUP_REGISTRATION_IS_NOT_SEND;
        let _ = WORKER_GUARD_IS_NOT_SEND;
        let _ = WORKER_DRIVER_IS_NOT_SEND;
        let _ = CALLER_GUARD_IS_NOT_SEND;
    }

    #[test]
    fn managed_exit_callback_follows_async_cleanup() {
        let actions = Arc::new(Mutex::new(Vec::new()));
        let callback_actions = Arc::clone(&actions);
        let worker = ManagedWorker::new_with_exit_callback(4, move || {
            callback_actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("exit");
        })
        .expect("managed worker starts");
        let (started_tx, started_rx) = mpsc::channel();
        let cleanup_actions = Arc::clone(&actions);
        worker
            .handle()
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        tokio::task::yield_now().await;
                        cleanup_actions
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push("cleanup");
                    })
                    .expect("async cleanup registered");
                started_tx.send(()).expect("task reports startup");
                pending::<()>().await;
            })
            .expect("task admitted");
        started_rx.recv().expect("task starts");
        worker.shutdown().expect("managed worker stops");
        assert_eq!(
            *actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            ["cleanup", "exit"]
        );
    }
}
