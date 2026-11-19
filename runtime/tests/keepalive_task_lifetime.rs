/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

mod lifetime {
    use std::cell::Cell;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::thread::ThreadId;

    use futures::task::noop_waker;
    use tokio::runtime::Builder;

    use crate::driver::WorkerDriver;
    use crate::keep_alive::CoroSessionHandle;
    use crate::keep_alive::KeepAliveError;
    use crate::keep_alive::KeepAliveOwner;
    use crate::keep_alive::KeepAlivePtr;
    use crate::keep_alive::RefCount;
    use crate::keep_alive::SessionHandleError;
    use crate::wait::WaitStatus;

    #[derive(Default)]
    struct TestValue {
        value: Cell<u64>,
    }

    struct ThreadDropProbe {
        drops: Arc<AtomicUsize>,
        thread: Arc<Mutex<Option<ThreadId>>>,
    }

    impl Drop for ThreadDropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            *self
                .thread
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(std::thread::current().id());
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(future)
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        future.poll(&mut context)
    }

    #[test]
    fn zero_refs_immediate() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        assert_eq!(owner.num_keep_alives(), 1);

        block_on(owner.zero_refs());

        assert_eq!(owner.num_keep_alives(), 0);
        assert!(owner.is_owner_released());
        block_on(owner.finalize());
    }

    #[test]
    fn waits_for_one_ref() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        let mut zero_refs = Box::pin(owner.zero_refs());

        assert!(poll_once(zero_refs.as_mut()).is_pending());
        assert_eq!(owner.num_keep_alives(), 1);
        drop(keep_alive);
        assert!(poll_once(zero_refs.as_mut()).is_ready());
        assert_eq!(owner.num_keep_alives(), 0);
        drop(zero_refs);
        block_on(owner.finalize());
    }

    #[test]
    fn waits_for_all_refs() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let first = owner
            .acquire_keep_alive()
            .expect("first reference acquired");
        let second = first.clone();
        let mut zero_refs = Box::pin(owner.zero_refs());

        assert!(poll_once(zero_refs.as_mut()).is_pending());
        drop(first);
        assert!(poll_once(zero_refs.as_mut()).is_pending());
        drop(second);
        assert!(poll_once(zero_refs.as_mut()).is_ready());
        drop(zero_refs);
        block_on(owner.finalize());
    }

    #[test]
    fn proxy_clone_counts_once() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let empty = KeepAlivePtr::<TestValue>::default();
        assert!(empty.is_empty());

        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        assert_eq!(owner.num_keep_alives(), 2);
        let mut copy = keep_alive.clone();
        assert_eq!(owner.num_keep_alives(), 3);
        assert!(copy.is_some());
        copy = empty;
        assert!(copy.is_empty());
        assert_eq!(owner.num_keep_alives(), 2);
        copy = keep_alive.clone();
        assert!(copy.is_some());
        assert_eq!(owner.num_keep_alives(), 3);

        drop(copy);
        drop(keep_alive);
        block_on(owner.finalize());
    }

    #[test]
    fn proxy_move_does_not_duplicate_ref() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        assert_eq!(owner.num_keep_alives(), 2);

        let mut moved = keep_alive;
        assert_eq!(owner.num_keep_alives(), 2);
        moved.reset();
        assert_eq!(owner.num_keep_alives(), 1);
        moved = owner.acquire_keep_alive().expect("reference reacquired");
        assert_eq!(owner.num_keep_alives(), 2);

        drop(moved);
        block_on(owner.finalize());
    }

    #[test]
    fn concurrent_proxy_clone_and_drop() {
        const THREADS: usize = 10;
        const COPIES: usize = 100;

        let mut owner = KeepAliveOwner::new(TestValue::default());
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let thread_keep_alive = keep_alive.clone();
                scope.spawn(move || {
                    for _ in 0..COPIES {
                        let copy = thread_keep_alive.clone();
                        let mut moved = copy;
                        let empty = KeepAlivePtr::<TestValue>::default();
                        assert!(empty.is_empty());
                        moved.reset();
                        assert!(moved.is_empty());
                    }
                });
            }
        });

        assert_eq!(owner.num_keep_alives(), 2);
        drop(keep_alive);
        block_on(owner.finalize());
    }

    #[test]
    fn acquire_after_owner_release_is_rejected() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        let mut zero_refs = Box::pin(owner.zero_refs());
        assert!(poll_once(zero_refs.as_mut()).is_pending());

        assert!(matches!(
            owner.acquire_keep_alive(),
            Err(KeepAliveError::OwnerReleased),
        ));
        let final_clone = keep_alive.clone();
        drop(keep_alive);
        assert!(poll_once(zero_refs.as_mut()).is_pending());
        drop(final_clone);
        assert!(poll_once(zero_refs.as_mut()).is_ready());
        drop(zero_refs);
        block_on(owner.finalize());
    }

    #[test]
    fn dropped_zero_ref_wait_unregisters_without_reopening_owner() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        let mut zero_refs = Box::pin(owner.zero_refs());
        assert!(poll_once(zero_refs.as_mut()).is_pending());
        assert_eq!(owner.zero_waiter_count(), 1);

        drop(zero_refs);
        assert_eq!(owner.zero_waiter_count(), 0);
        assert!(owner.is_owner_released());
        assert!(matches!(
            owner.acquire_keep_alive(),
            Err(KeepAliveError::OwnerReleased),
        ));

        drop(keep_alive);
        block_on(owner.finalize());
    }

    #[test]
    fn finalize_waits_for_external_refs_and_drops_value_on_owner() {
        let drops = Arc::new(AtomicUsize::new(0));
        let drop_thread = Arc::new(Mutex::new(None));
        let mut owner = KeepAliveOwner::new(ThreadDropProbe {
            drops: Arc::clone(&drops),
            thread: Arc::clone(&drop_thread),
        });
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");
        let mut finalize = Box::pin(owner.finalize());

        assert!(poll_once(finalize.as_mut()).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(keep_alive);
        assert!(poll_once(finalize.as_mut()).is_ready());
        drop(finalize);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            *drop_thread
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            Some(std::thread::current().id()),
        );
    }

    #[test]
    fn owner_drop_with_outstanding_ref_panics_and_unwinds_value_on_owner() {
        let drops = Arc::new(AtomicUsize::new(0));
        let drop_thread = Arc::new(Mutex::new(None));
        let owner = KeepAliveOwner::new(ThreadDropProbe {
            drops: Arc::clone(&drops),
            thread: Arc::clone(&drop_thread),
        });
        let keep_alive = owner.acquire_keep_alive().expect("reference acquired");

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(owner)));
        assert!(result.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            *drop_thread
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            Some(std::thread::current().id()),
        );
        drop(keep_alive);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn proxy_accesses_live_owner() {
        let mut owner = KeepAliveOwner::new(TestValue::default());
        let mut empty = KeepAlivePtr::<TestValue>::default();
        assert!(owner.get(&empty).is_none());

        empty = owner.acquire_keep_alive().expect("reference acquired");
        owner
            .get_mut(&empty)
            .expect("matching live owner")
            .value
            .set(1);
        assert_eq!(
            owner.get(&empty).expect("const owner access").value.get(),
            1
        );

        empty.reset();
        assert!(owner.get(&empty).is_none());
        block_on(owner.finalize());
    }

    #[test]
    fn refcount_reaches_zero_once() {
        let mut refcount = RefCount::default();
        refcount.inc_ref();
        assert_eq!(refcount.count(), 1);
        let mut zero_refs = Box::pin(refcount.zero_refs());

        assert!(poll_once(zero_refs.as_mut()).is_pending());
        refcount.dec_ref().expect("decrement succeeds");
        assert_eq!(
            poll_once(zero_refs.as_mut()),
            Poll::Ready(WaitStatus::Signalled)
        );
        assert_eq!(refcount.count(), 0);
    }

    #[test]
    fn zero_ref_wait_is_sticky() {
        let refcount = RefCount::default();
        assert_eq!(block_on(refcount.zero_refs()), WaitStatus::Signalled);
        assert_eq!(block_on(refcount.zero_refs()), WaitStatus::Signalled);
    }

    #[test]
    fn refcount_increment_preserves_size_t_wraparound() {
        let mut refcount = RefCount::new(usize::MAX);
        refcount.inc_ref();
        assert_eq!(refcount.count(), 0);
    }

    #[test]
    fn coro_session_handle_retains_liveness_and_worker_identity() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<CoroSessionHandle<TestValue>>();
        let (worker, driver) = WorkerDriver::new(2).expect("worker created");
        let mut owner = KeepAliveOwner::new_for_worker(TestValue::default(), &worker)
            .expect("owner bound to worker");
        let mut session = CoroSessionHandle::new(&owner, worker.clone())
            .expect("session handle bound to its owner worker");
        assert!(session.is_some());
        assert!(
            session
                .worker()
                .is_some_and(|bound| bound.same_worker(&worker))
        );
        assert_eq!(session.get(&owner).expect("live session").value.get(), 0);

        let copy = session.clone();
        assert_eq!(owner.num_keep_alives(), 3);
        session.reset();
        assert!(session.is_empty());
        assert_eq!(owner.num_keep_alives(), 2);
        drop(copy);
        block_on(owner.finalize());
        drop(driver);
    }

    #[test]
    fn coro_session_handle_rejects_a_different_worker_on_the_same_thread() {
        let (worker, driver) = WorkerDriver::new(2).expect("worker created");
        let (other_worker, other_driver) = WorkerDriver::new(2).expect("second worker created");
        assert_eq!(worker.owner_thread(), other_worker.owner_thread());
        assert_ne!(worker.id(), other_worker.id());

        let mut owner = KeepAliveOwner::new_for_worker(TestValue::default(), &worker)
            .expect("owner bound to first worker");
        assert!(matches!(
            CoroSessionHandle::new(&owner, other_worker.clone()),
            Err(SessionHandleError::WrongOwnerWorker {
                owner: owner_id,
                worker: candidate,
            }) if owner_id == worker.id() && candidate == other_worker.id()
        ));
        assert_eq!(owner.num_keep_alives(), 1);

        block_on(owner.finalize());
        drop(other_driver);
        drop(driver);
    }

    #[test]
    fn coro_session_handle_rejects_an_unbound_generic_owner() {
        let (worker, driver) = WorkerDriver::new(2).expect("worker created");
        let mut owner = KeepAliveOwner::new(TestValue::default());

        assert!(matches!(
            CoroSessionHandle::new(&owner, worker),
            Err(SessionHandleError::UnboundOwner),
        ));

        block_on(owner.finalize());
        drop(driver);
    }
}

mod task_group {
    use std::cell::RefCell;
    use std::future::pending;
    use std::panic::AssertUnwindSafe;
    use std::rc::Rc;
    use std::thread::ThreadId;

    use tokio::runtime::Builder;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    use crate::task_group::TaskGroup;
    use crate::task_group::TaskGroupError;
    use crate::wait::TimedBaton;
    use crate::wait::WaitStatus;

    fn runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    struct DropProbe {
        events: Rc<RefCell<Vec<&'static str>>>,
        owner: ThreadId,
        event: &'static str,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner);
            self.events.borrow_mut().push(self.event);
        }
    }

    #[test]
    fn cancel_then_join_all_before_owner_drop() {
        let runtime = runtime();
        let local = LocalSet::new();
        local.block_on(&runtime, async {
            let owner_thread = std::thread::current().id();
            let events = Rc::new(RefCell::new(Vec::new()));
            let owner_probe = DropProbe {
                events: Rc::clone(&events),
                owner: owner_thread,
                event: "owner_drop",
            };
            let (started_tx, started_rx) = oneshot::channel();
            let mut group = TaskGroup::new();
            group
                .spawn_local({
                    let events = Rc::clone(&events);
                    move |cancellation| async move {
                        let _probe = DropProbe {
                            events: Rc::clone(&events),
                            owner: owner_thread,
                            event: "task_drop",
                        };
                        events.borrow_mut().push("started");
                        started_tx.send(()).expect("start barrier receiver lives");
                        let cancellation_baton = TimedBaton::new(None);
                        let cancelled = cancellation_baton.wait(cancellation).await;
                        assert_eq!(cancelled, WaitStatus::Cancelled);
                        events.borrow_mut().push("cancelled");
                    }
                })
                .expect("task admitted");

            started_rx.await.expect("task reached start barrier");
            group
                .cancel_and_join()
                .await
                .expect("all cancelled tasks joined");
            assert!(!group.is_accepting());
            assert_eq!(group.spawn_local(|_| async {}), Err(TaskGroupError::Closed),);
            assert!(group.is_empty());
            drop(group);
            drop(owner_probe);
            assert_eq!(
                &*events.borrow(),
                &["started", "cancelled", "task_drop", "owner_drop"],
            );
        });
    }

    #[test]
    fn dropped_join_future_retains_task_for_later_join() {
        let runtime = runtime();
        let local = LocalSet::new();
        local.block_on(&runtime, async {
            let (cancel_seen_tx, mut cancel_seen_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();
            let mut group = TaskGroup::new();
            group
                .spawn_local(move |cancellation| async move {
                    let cancellation_baton = TimedBaton::new(None);
                    let cancelled = cancellation_baton.wait(cancellation).await;
                    assert_eq!(cancelled, WaitStatus::Cancelled);
                    cancel_seen_tx
                        .send(())
                        .expect("cancellation barrier receiver lives");
                    finish_rx.await.expect("finish barrier sender lives");
                })
                .expect("task admitted");

            let mut first_join = Box::pin(group.cancel_and_join());
            tokio::select! {
                result = &mut first_join => panic!("join completed before barrier: {result:?}"),
                result = &mut cancel_seen_rx => result.expect("task observed cancellation"),
            }
            drop(first_join);
            assert_eq!(group.len(), 1);
            assert!(group.is_cancelled());

            finish_tx.send(()).expect("task still retained");
            group
                .cancel_and_join()
                .await
                .expect("second join consumes retained task");
            assert!(group.is_empty());
        });
    }

    #[test]
    fn join_waits_for_sibling_without_requesting_cancellation() {
        let runtime = runtime();
        let local = LocalSet::new();
        local.block_on(&runtime, async {
            let owner_thread = std::thread::current().id();
            let events = Rc::new(RefCell::new(Vec::new()));
            let owner_probe = DropProbe {
                events: Rc::clone(&events),
                owner: owner_thread,
                event: "owner_drop",
            };
            let (started_tx, started_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();
            let mut group = TaskGroup::new();
            group
                .spawn_local({
                    let events = Rc::clone(&events);
                    move |cancellation| async move {
                        let _probe = DropProbe {
                            events: Rc::clone(&events),
                            owner: owner_thread,
                            event: "task_drop",
                        };
                        assert!(!cancellation.is_cancellation_requested());
                        events.borrow_mut().push("started");
                        started_tx.send(()).expect("start barrier receiver lives");
                        finish_rx.await.expect("finish barrier sender lives");
                        assert!(!cancellation.is_cancellation_requested());
                        events.borrow_mut().push("finished");
                    }
                })
                .expect("task admitted");

            started_rx.await.expect("task reached start barrier");
            finish_tx.send(()).expect("task still retained");
            group.join().await.expect("sibling task joined");
            assert!(!group.is_cancelled());
            assert!(group.is_empty());
            drop(group);
            drop(owner_probe);
            assert_eq!(
                &*events.borrow(),
                &["started", "finished", "task_drop", "owner_drop"],
            );
        });
    }

    #[test]
    fn dropping_nonempty_group_fails_closed_in_all_build_modes() {
        let runtime = runtime();
        let local = LocalSet::new();
        local.block_on(&runtime, async {
            let observed_cancellation = Rc::new(RefCell::new(None));
            let mut group = TaskGroup::new();
            group
                .spawn_local({
                    let observed_cancellation = Rc::clone(&observed_cancellation);
                    move |cancellation| {
                        *observed_cancellation.borrow_mut() = Some(cancellation);
                        async move {
                            pending::<()>().await;
                        }
                    }
                })
                .expect("task admitted");

            let result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(group)));
            assert!(result.is_err());
            assert!(
                observed_cancellation
                    .borrow()
                    .as_ref()
                    .is_some_and(|token| token.is_cancellation_requested()),
            );
        });
    }

    #[test]
    fn empty_group_requires_explicit_close_before_drop() {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| drop(TaskGroup::new())));
        assert!(result.is_err());

        let mut group = TaskGroup::new();
        assert!(group.close());
        drop(group);
    }

    #[test]
    fn completed_tasks_can_be_reaped_without_losing_terminal_errors() {
        let runtime = runtime();
        let local = LocalSet::new();
        local.block_on(&runtime, async {
            let mut group = TaskGroup::new();
            group
                .spawn_local(|_| async move {})
                .expect("task is admitted");
            group
                .spawn_local(|_| async move { panic!("injected task panic") })
                .expect("panicking task is admitted");
            tokio::task::yield_now().await;
            group.reap_finished();
            assert!(
                group.is_empty(),
                "completed tasks should release their slots"
            );
            assert_eq!(group.join().await, Err(TaskGroupError::TaskPanicked));
        });
    }
}
