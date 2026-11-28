/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::LocalSet;

use crate::driver::WorkerDriver;
use crate::managed::QuiesceFuture;
use crate::managed::RetirementReport;
use crate::managed::RuntimeConfig;
use crate::managed::RuntimeError;
use crate::managed::RuntimeHandle;
use crate::managed::RuntimeOwner;
use crate::managed::ShutdownReport;
use crate::managed::WorkerFailure;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupError;
use crate::tokio_host::BlockingPoolLimits;
use crate::tokio_host::ManagedWorker;
use crate::tokio_host::ManagedWorkerError;
use crate::tokio_host::ManagedWorkerOptions;
use crate::wait::TimedBaton;
use crate::wait::WaitStatus;
use crate::worker::QuiesceDeadline;
use crate::worker::SubmitError;
use crate::worker::WorkerCallError;
use crate::worker::WorkerCleanupTaskRegistration;
use crate::worker::WorkerHandle;
use crate::worker::WorkerPollStats;
use crate::worker::WorkerQuiescing;
use crate::worker::WorkerState;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

const OWNER_IS_NOT_SEND: fn() = || {
    struct Invalid;
    trait AmbiguousIfSend<Marker> {
        fn marker() {}
    }
    impl<Value: ?Sized> AmbiguousIfSend<()> for Value {}
    impl<Value: ?Sized + Send> AmbiguousIfSend<Invalid> for Value {}
    let _ = <RuntimeOwner as AmbiguousIfSend<_>>::marker;
};

fn config(workers: usize, command_capacity: usize) -> RuntimeConfig {
    RuntimeConfig {
        workers,
        command_capacity,
    }
}

fn wait<Work: Future>(future: Work) -> Work::Output {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime starts")
        .block_on(async {
            tokio::time::timeout(TEST_TIMEOUT, future)
                .await
                .expect("barrier completes without stranding runtime work")
        })
}

struct ThreadExitProbe {
    exits: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl Drop for ThreadExitProbe {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static THREAD_EXIT: RefCell<Option<ThreadExitProbe>> = const { RefCell::new(None) };
    static ESCAPED_RETIREMENT_SCOPE: RefCell<Option<WorkerCleanupTaskRegistration>> =
        const { RefCell::new(None) };
}

#[test]
fn cleanup_observer_survives_grace_and_escaped_registration() {
    for grace in [Duration::ZERO, Duration::from_secs(60)] {
        let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
        let handle = owner.handle();
        let worker = handle.worker(0).expect("worker exists");
        let exits = Arc::new(AtomicUsize::new(0));
        let captures = Arc::new(AtomicUsize::new(0));
        let child_finishes = Arc::new(AtomicUsize::new(0));
        let task_exits = Arc::clone(&exits);
        let task_captures = Arc::clone(&captures);
        let task_child_finishes = Arc::clone(&child_finishes);
        let (draining_tx, draining_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let observer = wait(
            worker
                .call_local(move |context| async move {
                    record_thread_exit(task_exits);
                    let group = Rc::new(RefCell::new(Some(TaskGroup::new())));
                    let cleanup_group = Rc::clone(&group);
                    let capture = ThreadExitProbe {
                        exits: task_captures,
                        owner: std::thread::current().id(),
                    };
                    let scope = context
                        .register_scope(async move {
                            let _capture = capture;
                            let mut group = cleanup_group
                                .borrow_mut()
                                .take()
                                .expect("rooted task group");
                            group.join().await.expect("scope joins its child");
                        })
                        .expect("scope reserved before child activation");
                    let observer = scope.observe();
                    ESCAPED_RETIREMENT_SCOPE.with(|slot| {
                        assert!(slot.replace(Some(scope)).is_none());
                    });
                    group
                        .borrow_mut()
                        .as_mut()
                        .expect("live group")
                        .spawn_local(move |_| async move {
                            context.quiescing().await;
                            draining_tx.send(()).expect("root observes quiescence");
                            release_rx.await.expect("root releases child");
                            ESCAPED_RETIREMENT_SCOPE.with(|slot| drop(slot.replace(None)));
                            task_child_finishes.fetch_add(1, Ordering::SeqCst);
                        })
                        .expect("rooted child starts");
                    observer
                })
                .expect("initialization admitted"),
        )
        .expect("observation exported");
        drop(observer.clone());
        drop(observer.wait());
        drop(handle.quiesce(grace));
        wait(draining_rx).expect("child observes admission closure");
        assert!(!worker.is_accepting());
        assert_eq!(captures.load(Ordering::SeqCst), 0);
        let mut waiting = Box::pin(observer.wait());
        let waker = futures::task::noop_waker();
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        release_tx.send(()).expect("child is still rooted");
        wait(waiting).expect("registered future retires");
        assert_eq!(captures.load(Ordering::SeqCst), 1);
        assert_eq!(child_finishes.load(Ordering::SeqCst), 1);
        wait(handle.quiesce(grace)).expect("worker quiesces after cleanup");
        let report = owner.join().expect("worker physically joins");
        assert_eq!(report.workers_forced, usize::from(grace.is_zero()));
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn cleanup_retirement_does_not_imply_worker_join() {
    let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let exits = Arc::new(AtomicUsize::new(0));
    let task_exits = Arc::clone(&exits);
    let captures = Arc::new(AtomicUsize::new(0));
    let task_captures = Arc::clone(&captures);
    let (release_tx, release_rx) = oneshot::channel();
    let (retired, pending_scope) = wait(
        worker
            .call_local(move |context| async move {
                record_thread_exit(task_exits);
                let capture = ThreadExitProbe {
                    exits: task_captures,
                    owner: std::thread::current().id(),
                };
                let first = context
                    .register_scope(async move {
                        let _capture = capture;
                    })
                    .expect("first scope reserved");
                let retired = first.observe();
                first.activate().expect("first scope activates").detach();
                let second = context
                    .register_scope(async move {
                        release_rx.await.expect("root releases unrelated scope");
                    })
                    .expect("unrelated scope reserved");
                let pending_scope = second.observe();
                second
                    .activate()
                    .expect("unrelated scope activates")
                    .detach();
                (retired, pending_scope)
            })
            .expect("initialization admitted"),
    )
    .expect("observations exported");
    wait(retired.wait()).expect("first registered future retires");
    assert_eq!(captures.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert_eq!(worker.state(), WorkerState::Open);
    assert_eq!(owner.join(), Err(RuntimeError::NotQuiesced));
    let mut pending_wait = Box::pin(pending_scope.wait());
    let waker = futures::task::noop_waker();
    assert!(
        pending_wait
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(handle.quiesce(Duration::from_secs(60)));
    release_tx
        .send(())
        .expect("unrelated scope is still rooted");
    wait(pending_wait).expect("unrelated scope retires independently");
    wait(handle.quiesce(Duration::from_secs(60))).expect("worker quiesces");
    owner.join().expect("explicit physical join completes");
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

fn record_thread_exit(exits: Arc<AtomicUsize>) {
    THREAD_EXIT.with(|slot| {
        assert!(
            slot.replace(Some(ThreadExitProbe {
                exits,
                owner: std::thread::current().id(),
            }))
            .is_none()
        );
    });
}

#[test]
fn quiesce_then_join() {
    let mut owner = RuntimeOwner::start(config(2, 4)).expect("runtime starts");
    let handle = owner.handle();
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let mut calls = Vec::new();
    for index in 0..handle.worker_count() {
        let worker = handle.worker(index).expect("configured worker");
        let task_exits = Arc::clone(&exits);
        let task_cleanups = Arc::clone(&cleanup_count);
        let (started_tx, started_rx) = oneshot::channel();
        calls.push(
            worker
                .call_local(move |context| async move {
                    record_thread_exit(task_exits);
                    let group = Rc::new(RefCell::new(Some(TaskGroup::new())));
                    let cleanup_group = Rc::clone(&group);
                    let cleanup_owner = context.owner_thread();
                    let _cleanup = context
                        .register_cleanup_task(async move {
                            let mut group = cleanup_group
                                .borrow_mut()
                                .take()
                                .expect("cleanup owns the child group");
                            group.cancel_and_join().await.expect("child task joins");
                            assert_eq!(std::thread::current().id(), cleanup_owner);
                            task_cleanups.fetch_add(1, Ordering::SeqCst);
                        })
                        .expect("cleanup registered before child admission");
                    let (child_tx, child_rx) = oneshot::channel();
                    group
                        .borrow_mut()
                        .as_mut()
                        .expect("live child group")
                        .spawn_local(move |cancel| async move {
                            let local = Rc::new(11_u8);
                            child_tx.send(()).expect("parent waits for child");
                            let cancelled = TimedBaton::new(None).wait(cancel).await;
                            assert_eq!(cancelled, WaitStatus::Cancelled);
                            assert_eq!(*local, 11);
                        })
                        .expect("child admitted");
                    child_rx.await.expect("child has started");
                    started_tx.send(()).expect("root waits for startup");
                    pending::<()>().await;
                })
                .expect("scope admitted"),
        );
        wait(started_rx).expect("scope and child started");
    }

    wait(handle.quiesce(Duration::ZERO)).expect("async cleanup completes");
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 2);
    for call in calls {
        assert_eq!(wait(call), Err(WorkerCallError::Cancelled));
    }
    let report = owner.join().expect("root physically joins workers");
    assert_eq!(
        report,
        ShutdownReport {
            workers_joined: 2,
            workers_forced: 2,
        }
    );
    assert_eq!(exits.load(Ordering::SeqCst), 2);
    assert_eq!(owner.join(), Ok(report));
    assert!(handle.worker(2).is_none());
    assert_eq!(
        handle
            .worker(0)
            .expect("closed worker survives")
            .submit(|_| {}),
        Err(SubmitError::Closed)
    );
}

#[test]
fn quiesce_starts_without_polling_and_join_preserves_wrong_phase() {
    let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let (started_tx, started_rx) = oneshot::channel();
    let (cleaning_tx, cleaning_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    worker
        .spawn_local(move |context| async move {
            let _cleanup = context
                .register_cleanup_task(async move {
                    cleaning_tx.send(()).expect("root observes cleanup");
                    finish_rx
                        .await
                        .expect("application remains alive for cleanup");
                })
                .expect("async cleanup registered");
            started_tx.send(()).expect("root observes startup");
            pending::<()>().await;
        })
        .expect("scope admitted");
    wait(started_rx).expect("scope started");
    assert_eq!(owner.join(), Err(RuntimeError::NotQuiesced));
    assert!(worker.is_accepting());

    let unpolled = handle.quiesce(Duration::ZERO);
    assert!(!worker.is_accepting());
    drop(unpolled);
    drop(handle);
    wait(cleaning_rx).expect("cleanup starts without a public observer");
    assert_eq!(owner.join(), Err(RuntimeError::NotQuiesced));

    wait(async {
        let handle = owner.handle();
        let mut observer = Box::pin(handle.quiesce(Duration::ZERO));
        assert!(futures::poll!(&mut observer).is_pending());
        drop(observer);
        drop(handle);
        tokio::task::yield_now().await;
        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        finish_tx
            .send(())
            .expect("cleanup retained after observer drop");
        owner.handle().quiesce(Duration::ZERO).await
    })
    .expect("root can resume observation after drops");
    assert_eq!(owner.join().expect("worker joins").workers_joined, 1);
}

#[test]
fn saturated_queue_does_not_block_quiescence() {
    let mut owner = RuntimeOwner::start(config(1, 1)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let active = worker
        .call_local(move |_| {
            entered_tx.send(()).expect("root waits for factory entry");
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("root releases deliberately stalled factory");
            async { pending::<()>().await }
        })
        .expect("first command admitted");
    entered_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("factory is stalled");
    let queued = worker.call(|_| 7_u8).expect("sole queue slot filled");
    assert_eq!(worker.submit(|_| {}), Err(SubmitError::Overloaded));
    assert_eq!(worker.queued_commands(), 1);

    drop(handle.quiesce(Duration::ZERO));
    assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
    release_tx
        .send(())
        .expect("factory retained until root releases it");
    wait(handle.quiesce(Duration::ZERO)).expect("queue-independent shutdown completes");
    assert_eq!(wait(active), Err(WorkerCallError::Cancelled));
    assert_eq!(wait(queued), Err(WorkerCallError::Cancelled));
    owner.join().expect("saturated worker joined");
}

#[test]
fn admitted_work_observes_drain_and_finishes_without_cancellation() {
    let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let (started_tx, started_rx) = oneshot::channel();
    let call = worker
        .call_local(move |context| async move {
            started_tx.send(()).expect("root observes startup");
            context.quiescing().await;
            assert!(!context.is_cancelled());
            assert!(!context.handle().is_accepting());
            19_u8
        })
        .expect("scope admitted");
    wait(started_rx).expect("scope started");
    drop(handle.quiesce(Duration::MAX));
    assert_eq!(wait(call), Ok(19));
    wait(handle.quiesce(Duration::MAX)).expect("idle drain completes without waiting for grace");
    assert_eq!(owner.join().expect("worker joins").workers_forced, 0);
}

#[test]
fn registered_scope_outlives_initialization_and_drains_its_group() {
    let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let (draining_tx, draining_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let task_cleanups = Arc::clone(&cleanup_count);
    let initialized = worker
        .call_local(move |context| async move {
            let group = Rc::new(RefCell::new(Some(TaskGroup::new())));
            let cleanup_group = Rc::clone(&group);
            let scope = context
                .register_scope(async move {
                    let mut group = cleanup_group.borrow_mut().take().expect("scope owns group");
                    group.join().await.expect("graceful child joins");
                    task_cleanups.fetch_add(1, Ordering::SeqCst);
                })
                .expect("scope registered before child admission");
            group
                .borrow_mut()
                .as_mut()
                .expect("live group")
                .spawn_local(move |_| async move {
                    context.quiescing().await;
                    assert!(!context.is_cancelled());
                    draining_tx
                        .send(())
                        .expect("root observes graceful draining");
                    finish_rx.await.expect("root permits graceful completion");
                    assert!(!context.is_cancelled());
                    drop(scope);
                })
                .expect("scope child admitted");
        })
        .expect("scope initialization admitted");
    wait(initialized).expect("initialization command finishes before draining");
    drop(handle.quiesce(Duration::MAX));
    wait(draining_rx).expect("scope receives drain after initializer returns");
    assert_eq!(worker.state(), WorkerState::Open);
    assert_eq!(owner.join(), Err(RuntimeError::NotQuiesced));
    finish_tx
        .send(())
        .expect("scope remains alive during grace");
    wait(handle.quiesce(Duration::MAX)).expect("scope completes graceful cleanup");
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        owner
            .join()
            .expect("worker physically joins")
            .workers_forced,
        0
    );
}

#[test]
fn later_quiesce_can_shorten_grace_without_an_observer() {
    let mut owner = RuntimeOwner::start(config(1, 1)).expect("runtime starts");
    let handle = owner.handle();
    let (started_tx, started_rx) = oneshot::channel();
    let call = handle
        .worker(0)
        .expect("worker exists")
        .call_local(move |_| async move {
            started_tx.send(()).expect("root observes startup");
            pending::<()>().await;
        })
        .expect("task admitted");
    wait(started_rx).expect("task started");
    drop(handle.quiesce(Duration::MAX));
    drop(handle.quiesce(Duration::ZERO));
    wait(handle.quiesce(Duration::MAX)).expect("later grace cannot extend zero deadline");
    assert_eq!(wait(call), Err(WorkerCallError::Cancelled));
    assert_eq!(owner.join().expect("worker joins").workers_forced, 1);
}

#[test]
fn partial_startup_failure_joins_already_started_workers() {
    let exits = Arc::new(AtomicUsize::new(0));
    let mut survivor = None;
    let failure = ManagedWorkerError::ThreadSpawn("injected second-worker failure".to_owned());
    let result = RuntimeOwner::start_with(config(2, 2), |index, capacity| {
        if index == 1 {
            return Err(failure.clone());
        }
        let worker = ManagedWorker::new(capacity)?;
        let worker_handle = worker.handle();
        let task_exits = Arc::clone(&exits);
        wait(
            worker_handle
                .call(move |_| record_thread_exit(task_exits))
                .expect("probe admitted"),
        )
        .expect("thread exit probe installed");
        survivor = Some(worker_handle);
        Ok(worker)
    });
    let error = match result {
        Ok(_) => panic!("injected startup must fail"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        RuntimeError::Startup {
            worker: 1,
            source: failure,
            rollback: Vec::new(),
        }
    );
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(
        survivor.expect("first worker started").state(),
        WorkerState::Closed
    );
}

#[test]
fn worker_failure_is_reported_and_every_worker_is_joined() {
    let mut owner = RuntimeOwner::start(config(2, 2)).expect("runtime starts");
    let handle = owner.handle();
    let exits = Arc::new(AtomicUsize::new(0));
    for index in 0..2 {
        let task_exits = Arc::clone(&exits);
        let (started_tx, started_rx) = oneshot::channel();
        handle
            .worker(index)
            .expect("worker exists")
            .spawn_local(move |context| async move {
                record_thread_exit(task_exits);
                let _cleanup = context
                    .register_cleanup_task(async move {
                        if index == 0 {
                            panic!("injected cleanup failure");
                        }
                    })
                    .expect("cleanup registered");
                started_tx.send(()).expect("root observes startup");
                pending::<()>().await;
            })
            .expect("scope admitted");
        wait(started_rx).expect("scope started");
    }
    let error = wait(handle.quiesce(Duration::ZERO)).expect_err("cleanup failure surfaced");
    let RuntimeError::WorkersFailed { failures } = &error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].worker, 0);
    assert_eq!(
        failures[0].error,
        ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked)
    );
    assert_eq!(owner.join(), Err(error.clone()));
    assert_eq!(exits.load(Ordering::SeqCst), 2);
    assert_eq!(owner.join(), Err(error));
}

#[test]
fn command_panic_still_awaits_registered_child_cleanup() {
    let mut owner = RuntimeOwner::start(config(1, 2)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let (started_tx, started_rx) = oneshot::channel();
    let (panic_tx, panic_rx) = oneshot::channel();
    let (cleaning_tx, cleaning_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let task_cleanups = Arc::clone(&cleanup_count);
    worker
        .spawn_local(move |context| async move {
            let group = Rc::new(RefCell::new(Some(TaskGroup::new())));
            let cleanup_group = Rc::clone(&group);
            let _scope = context
                .register_scope(async move {
                    cleaning_tx
                        .send(())
                        .expect("root observes async panic cleanup");
                    finish_rx
                        .await
                        .expect("root keeps cleanup dependencies alive");
                    let mut group = cleanup_group
                        .borrow_mut()
                        .take()
                        .expect("cleanup owns group");
                    group.cancel_and_join().await.expect("panic sibling joins");
                    task_cleanups.fetch_add(1, Ordering::SeqCst);
                })
                .expect("scope reserves cleanup before spawning child");
            group
                .borrow_mut()
                .as_mut()
                .expect("live group")
                .spawn_local(|cancel| async move {
                    assert_eq!(
                        TimedBaton::new(None).wait(cancel).await,
                        WaitStatus::Cancelled
                    );
                })
                .expect("child admitted");
            started_tx.send(()).expect("root observes startup");
            panic_rx.await.expect("root triggers command failure");
            panic!("injected command failure");
        })
        .expect("command admitted");
    wait(started_rx).expect("command and scope initialized");
    panic_tx.send(()).expect("command still pending");
    wait(cleaning_rx).expect("failure preserves pending asynchronous cleanup");
    assert_eq!(worker.state(), WorkerState::Closing);
    drop(handle.quiesce(Duration::ZERO));
    assert_eq!(owner.join(), Err(RuntimeError::NotQuiesced));
    finish_tx
        .send(())
        .expect("worker retains cleanup after panic");
    let error = wait(handle.quiesce(Duration::ZERO)).expect_err("task failure surfaced");
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
    assert_eq!(owner.join(), Err(error));
}

#[test]
fn root_operations_reject_tokio_and_preserve_ownership() {
    let mut owner = RuntimeOwner::start(config(1, 1)).expect("synchronous root starts runtime");
    wait(async {
        assert!(matches!(
            RuntimeOwner::start(config(1, 1)),
            Err(RuntimeError::InsideTokio)
        ));
        assert_eq!(owner.join(), Err(RuntimeError::InsideTokio));
        owner
            .handle()
            .quiesce(Duration::ZERO)
            .await
            .expect("async quiescence succeeds");
        assert_eq!(owner.join(), Err(RuntimeError::InsideTokio));
    });
    owner
        .join()
        .expect("ownership survives until synchronous join");
}

#[test]
fn root_drop_closes_surviving_handles_and_physically_joins() {
    let owner = RuntimeOwner::start(config(1, 1)).expect("runtime starts");
    let handle = owner.handle();
    let worker = handle.worker(0).expect("worker exists");
    let exits = Arc::new(AtomicUsize::new(0));
    let task_exits = Arc::clone(&exits);
    wait(
        worker
            .call(move |_| record_thread_exit(task_exits))
            .expect("probe admitted"),
    )
    .expect("probe installed");
    drop(owner);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(worker.state(), WorkerState::Closed);
    assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
    wait(handle.quiesce(Duration::ZERO)).expect("surviving handle observes completion");
}

#[test]
fn cancelled_group_join_retains_a_previously_observed_failure() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    LocalSet::new().block_on(&runtime, async {
        let mut tasks = TaskGroup::new();
        let (finish_tx, finish_rx) = oneshot::channel();
        tasks
            .spawn_local(|_| async { panic!("injected child failure") })
            .expect("failing child admitted");
        tasks
            .spawn_local(move |_| async move { finish_rx.await.expect("root retains child") })
            .expect("pending sibling admitted");
        tokio::time::timeout(TEST_TIMEOUT, async {
            while tasks.len() != 1 {
                tokio::task::yield_now().await;
                tasks.reap_finished();
            }
        })
        .await
        .expect("panicking child is reaped");
        let mut abandoned = Box::pin(tasks.join());
        assert!(futures::poll!(&mut abandoned).is_pending());
        drop(abandoned);
        finish_tx
            .send(())
            .expect("sibling retained after join observer drops");
        assert_eq!(tasks.join().await, Err(TaskGroupError::TaskPanicked));
        assert_eq!(tasks.join().await, Err(TaskGroupError::TaskPanicked));
    });
}

#[test]
fn public_capabilities_are_send_but_owner_is_not() {
    fn assert_send_sync<Value: Send + Sync + Clone>() {}
    fn assert_send<Value: Send>() {}

    assert_send_sync::<RuntimeHandle>();
    assert_send::<QuiesceFuture>();
    let _ = OWNER_IS_NOT_SEND;
    assert!(matches!(
        RuntimeOwner::start(config(0, 1)),
        Err(RuntimeError::ZeroWorkers)
    ));
    assert!(matches!(
        RuntimeOwner::start(config(1, 0)),
        Err(RuntimeError::ZeroCapacity)
    ));
}

struct NotificationWake {
    count: Arc<AtomicUsize>,
    panic_message: Option<&'static str>,
}

impl Wake for NotificationWake {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.panic_message {
            panic_any(message);
        }
    }
}

fn panicking_observer(
    worker: &WorkerHandle,
    wakes: &Arc<AtomicUsize>,
    message: &'static str,
) -> Pin<Box<WorkerQuiescing>> {
    let waker = Waker::from(Arc::new(NotificationWake {
        count: Arc::clone(wakes),
        panic_message: Some(message),
    }));
    let mut observer = Box::pin(worker.quiescing());
    assert!(
        observer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    observer
}

#[test]
fn notification_panic_does_not_skip_driver_or_supervisor_wake() {
    for grace in [Duration::ZERO, Duration::from_secs(60)] {
        let (handle, mut driver) = WorkerDriver::new(1).expect("worker driver starts");
        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let task_cleanups = Arc::clone(&cleanup_count);
        handle
            .spawn_local(move |context| async move {
                let _cleanup = context
                    .register_cleanup_task(async move {
                        task_cleanups.fetch_add(1, Ordering::SeqCst);
                    })
                    .expect("cleanup registered before parking");
                pending::<()>().await;
            })
            .expect("command admitted");
        let driver_wakes = Arc::new(AtomicUsize::new(0));
        let driver_waker = Waker::from(Arc::new(NotificationWake {
            count: Arc::clone(&driver_wakes),
            panic_message: None,
        }));
        let mut context = Context::from_waker(&driver_waker);
        assert!(Pin::new(&mut driver).poll(&mut context).is_pending());
        assert_eq!(driver_wakes.load(Ordering::SeqCst), 0);
        let supervisor_wakes = Arc::new(AtomicUsize::new(0));
        let supervisor_waker = Waker::from(Arc::new(NotificationWake {
            count: Arc::clone(&supervisor_wakes),
            panic_message: None,
        }));
        assert_eq!(handle.quiesce_snapshot(&supervisor_waker), (None, 1));
        let notification_wakes = Arc::new(AtomicUsize::new(0));
        let observer = panicking_observer(&handle, &notification_wakes, "quiescing wake panic");

        let panic = catch_unwind(AssertUnwindSafe(|| {
            handle.begin_quiesce(QuiesceDeadline::new(grace));
        }))
        .expect_err("the original notification panic must propagate");
        let observed_driver_wakes = driver_wakes.load(Ordering::SeqCst);
        let observed_supervisor_wakes = supervisor_wakes.load(Ordering::SeqCst);
        let admission_closed = !handle.is_accepting();
        drop(observer);
        handle.begin_shutdown();
        assert!(Pin::new(&mut driver).poll(&mut context).is_ready());

        assert_eq!(panic.downcast_ref::<&str>(), Some(&"quiescing wake panic"));
        assert!(admission_closed);
        assert_eq!(notification_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(observed_supervisor_wakes, 1);
        assert_eq!(observed_driver_wakes, usize::from(grace.is_zero()));
        assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
        assert_eq!(handle.state(), WorkerState::Closed);
    }
}

fn is_accepting_on_another_thread(handle: &WorkerHandle) -> bool {
    let handle = handle.clone();
    std::thread::spawn(move || handle.is_accepting())
        .join()
        .expect("reader thread joins")
}

#[test]
fn is_accepting_follows_every_admission_close_on_every_thread() {
    let (quiesced, quiesced_driver) = WorkerDriver::new(1).expect("worker driver starts");
    assert!(quiesced.is_accepting());
    assert!(is_accepting_on_another_thread(&quiesced));
    quiesced.begin_quiesce(QuiesceDeadline::new(Duration::from_secs(60)));
    assert!(!quiesced.is_accepting());
    assert!(!is_accepting_on_another_thread(&quiesced));
    assert_eq!(quiesced.state(), WorkerState::Open);
    assert_eq!(quiesced.submit(|_| {}), Err(SubmitError::Closed));
    drop(quiesced_driver);

    let (shut_down, mut shut_down_driver) = WorkerDriver::new(1).expect("worker driver starts");
    assert!(shut_down.begin_shutdown());
    assert!(!shut_down.is_accepting());
    assert!(!is_accepting_on_another_thread(&shut_down));
    let waker = futures::task::noop_waker();
    assert!(
        Pin::new(&mut shut_down_driver)
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    assert_eq!(shut_down.state(), WorkerState::Closed);
    assert!(!shut_down.is_accepting());
    assert!(!is_accepting_on_another_thread(&shut_down));

    let (closed, closed_driver) = WorkerDriver::new(1).expect("worker driver starts");
    assert!(closed.shared.mark_closed().is_none());
    assert!(!closed.is_accepting());
    assert!(!is_accepting_on_another_thread(&closed));
    assert_eq!(closed.submit(|_| {}), Err(SubmitError::Closed));
    drop(closed_driver);
}

/// Readers on other threads spin until they observe shutdown, one through
/// cancellation and one through admission. Both must then find admission
/// closed, and `submit` must refuse their command.
#[test]
fn racing_readers_that_observe_shutdown_find_admission_closed() {
    let (handle, driver) = WorkerDriver::new(1).expect("worker driver starts");
    let spin_until = |handle: &WorkerHandle, observed: fn(&WorkerHandle) -> bool| {
        let handle = handle.clone();
        std::thread::spawn(move || {
            let deadline = Instant::now() + TEST_TIMEOUT;
            while !observed(&handle) {
                assert!(Instant::now() < deadline, "shutdown becomes visible");
                std::hint::spin_loop();
            }
            (handle.is_accepting(), handle.submit(|_| {}))
        })
    };
    let through_cancellation = spin_until(&handle, WorkerHandle::is_cancelled);
    let through_admission = spin_until(&handle, |handle| !handle.is_accepting());
    assert!(handle.begin_shutdown());
    for reader in [through_cancellation, through_admission] {
        let (accepting, submitted) = reader.join().expect("reader thread joins");
        assert!(!accepting);
        assert_eq!(submitted, Err(SubmitError::Closed));
    }
    drop(driver);
}

#[test]
fn is_accepting_does_not_lock_the_command_queue() {
    let (handle, driver) = WorkerDriver::new(1).expect("worker driver starts");
    let locks = handle.queue_locks();
    assert_eq!(handle.queued_commands(), 0);
    assert_eq!(handle.queue_locks(), locks + 1, "one queue lock");

    let locks = handle.queue_locks();
    assert!(handle.is_accepting());
    assert_eq!(handle.queue_locks(), locks, "an open worker");

    assert!(handle.begin_shutdown());
    let locks = handle.queue_locks();
    assert!(!handle.is_accepting());
    assert_eq!(handle.queue_locks(), locks, "a closing worker");
    drop(driver);
}

struct ParkedWorker {
    handle: WorkerHandle,
    completion: watch::Receiver<Option<Result<(), ManagedWorkerError>>>,
    resume: Option<oneshot::Sender<()>>,
}

fn parked_worker(
    capacity: usize,
    exits: &Arc<AtomicUsize>,
    cleanups: &Arc<AtomicUsize>,
) -> (ManagedWorker, ParkedWorker) {
    let worker = ManagedWorker::new(capacity).expect("managed worker starts");
    let handle = worker.handle();
    let (resume, mut resumed) = oneshot::channel();
    let (started, parked) = oneshot::channel();
    let task_exits = Arc::clone(exits);
    let task_cleanups = Arc::clone(cleanups);
    handle
        .spawn_local(move |context| async move {
            record_thread_exit(task_exits);
            let cleanup_owner = context.owner_thread();
            let _cleanup = context
                .register_cleanup_task(async move {
                    tokio::task::yield_now().await;
                    assert_eq!(std::thread::current().id(), cleanup_owner);
                    task_cleanups.fetch_add(1, Ordering::SeqCst);
                })
                .expect("cleanup registered before parking");
            let mut started = Some(started);
            poll_fn(move |context| {
                let result = Pin::new(&mut resumed).poll(context);
                if let Some(started) = started.take() {
                    started
                        .send(())
                        .expect("root waits until the recovery wake is armed");
                }
                result.map(|_| ())
            })
            .await;
        })
        .expect("parked command admitted");
    wait(parked).expect("worker is parked with an independent recovery wake");
    let completion = worker.completion();
    (
        worker,
        ParkedWorker {
            handle,
            completion,
            resume: Some(resume),
        },
    )
}

fn workers_complete_within_timeout(workers: &mut [ParkedWorker]) -> bool {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("watchdog runtime starts");
    matches!(
        runtime.block_on(async {
            tokio::time::timeout(TEST_TIMEOUT, async {
                for worker in workers {
                    loop {
                        if worker.completion.borrow().is_some() {
                            break;
                        }
                        if worker.completion.changed().await.is_err() {
                            return false;
                        }
                    }
                }
                true
            })
            .await
        }),
        Ok(true)
    )
}

fn with_shutdown_watchdog<Test, Output>(
    mut workers: Vec<ParkedWorker>,
    test: Test,
) -> (Output, bool)
where
    Test: FnOnce() -> Output,
{
    std::thread::scope(|scope| {
        let watchdog = scope.spawn(move || {
            if workers_complete_within_timeout(&mut workers) {
                return true;
            }
            for worker in &workers {
                let _ = catch_unwind(AssertUnwindSafe(|| worker.handle.begin_shutdown()));
            }
            for worker in &mut workers {
                if let Some(resume) = worker.resume.take() {
                    let _ = resume.send(());
                }
            }
            assert!(
                workers_complete_within_timeout(&mut workers),
                "independent recovery wakes must finish parked workers before joining"
            );
            false
        });
        let output = test();
        let unassisted = watchdog
            .join()
            .expect("shutdown watchdog is joined, never detached");
        (output, unassisted)
    })
}

#[test]
fn quiesce_notification_panics_start_every_parked_worker() {
    for grace in [Duration::ZERO, Duration::from_millis(20)] {
        let exits = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        let mut owner = RuntimeOwner::start_with(config(3, 1), |_, capacity| {
            let (worker, parked) = parked_worker(capacity, &exits, &cleanups);
            workers.push(parked);
            Ok(worker)
        })
        .expect("three parked workers start");
        let handle = owner.handle();
        let observers: Vec<_> = workers
            .iter()
            .enumerate()
            .map(|(index, worker)| {
                panicking_observer(
                    &worker.handle,
                    &wakes,
                    if index == 0 {
                        "first worker wake panic"
                    } else {
                        "later worker wake panic"
                    },
                )
            })
            .collect();
        let ((panic, all_closed, observer_returned), unassisted) =
            with_shutdown_watchdog(workers, || {
                let panic = catch_unwind(AssertUnwindSafe(|| handle.quiesce(grace))).err();
                let all_closed = (0..handle.worker_count())
                    .all(|index| !handle.worker(index).expect("worker exists").is_accepting());
                let observer = catch_unwind(AssertUnwindSafe(|| handle.quiesce(grace)));
                let observer_returned = observer.is_ok();
                drop(observer);
                drop(observers);
                (panic, all_closed, observer_returned)
            });
        let report = owner
            .join()
            .expect("all workers physically join after notification failure");

        assert!(
            unassisted,
            "shutdown must not need the watchdog's recovery wake"
        );
        assert!(
            all_closed,
            "every admission closes before the first panic is rethrown"
        );
        assert!(
            observer_returned,
            "subsequent observation is available and may be dropped"
        );
        assert_eq!(
            panic
                .expect("notification panic preserved")
                .downcast_ref::<&str>(),
            Some(&"first worker wake panic")
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 3);
        assert_eq!(cleanups.load(Ordering::SeqCst), 3);
        assert_eq!(exits.load(Ordering::SeqCst), 3);
        assert_eq!(report.workers_joined, 3);
        assert_eq!(report.workers_forced, 3);
    }
}

#[test]
fn owner_drop_joins_every_worker_after_notification_panics() {
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    let owner = RuntimeOwner::start_with(config(3, 1), |_, capacity| {
        let (worker, parked) = parked_worker(capacity, &exits, &cleanups);
        workers.push(parked);
        Ok(worker)
    })
    .expect("three parked workers start");
    let handle = owner.handle();
    let observers: Vec<_> = workers
        .iter()
        .map(|worker| panicking_observer(&worker.handle, &wakes, "root-drop wake panic"))
        .collect();
    let (dropped, unassisted) =
        with_shutdown_watchdog(workers, || catch_unwind(AssertUnwindSafe(|| drop(owner))));
    drop(observers);

    assert!(
        dropped.is_ok(),
        "root Drop discards reported failures only after joining"
    );
    assert!(unassisted, "root fallback must not need recovery wakes");
    assert_eq!(wakes.load(Ordering::SeqCst), 3);
    assert_eq!(cleanups.load(Ordering::SeqCst), 3);
    assert_eq!(exits.load(Ordering::SeqCst), 3);
    assert!((0..handle.worker_count()).all(|index| {
        let worker = handle.worker(index).expect("closed handle survives");
        worker.state() == WorkerState::Closed && !worker.is_accepting()
    }));
}

#[test]
fn managed_shutdown_reports_notification_panic_after_joining() {
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let (worker, parked) = parked_worker(1, &exits, &cleanups);
    let observer = panicking_observer(&parked.handle, &wakes, "managed shutdown wake panic");
    let (result, unassisted) = with_shutdown_watchdog(vec![parked], || worker.shutdown());
    drop(observer);

    assert_eq!(
        result,
        Err(ManagedWorkerError::ShutdownNotificationPanicked)
    );
    assert!(
        unassisted,
        "explicit managed shutdown must not need recovery wakes"
    );
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn startup_rollback_reports_notification_panic_after_joining() {
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let (worker, parked) = parked_worker(1, &exits, &cleanups);
    let observer = panicking_observer(&parked.handle, &wakes, "rollback wake panic");
    let mut first_worker = Some(worker);
    let source = ManagedWorkerError::ThreadSpawn("injected partial startup failure".to_owned());
    let (result, unassisted) = with_shutdown_watchdog(vec![parked], || {
        RuntimeOwner::start_with(config(2, 1), |index, _| {
            if index == 0 {
                Ok(first_worker
                    .take()
                    .expect("first worker starts exactly once"))
            } else {
                Err(source.clone())
            }
        })
    });
    drop(observer);
    let error = match result {
        Ok(_) => panic!("injected startup must fail"),
        Err(error) => error,
    };

    assert!(unassisted, "rollback must not need recovery wakes");
    assert_eq!(
        error,
        RuntimeError::Startup {
            worker: 1,
            source,
            rollback: vec![WorkerFailure {
                worker: 0,
                error: ManagedWorkerError::ShutdownNotificationPanicked,
            }],
        }
    );
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}

#[test]
fn explicit_retirement_after_quiesce_construction_panic() {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let (worker, parked) = parked_worker(1, &exits, &cleanups);
    let mut worker = Some(worker);
    let mut owner = RuntimeOwner::start_with(config(1, 1), |_, _| {
        Ok(worker.take().expect("parked worker starts once"))
    })
    .expect("runtime owns parked worker");
    let observer = panicking_observer(&parked.handle, &wakes, "explicit quiescing wake panic");
    let quiescence = catch_unwind(AssertUnwindSafe(|| {
        owner.handle().quiesce(Duration::from_secs(60))
    }));
    let admission_closed = !parked.handle.is_accepting();
    let completion_pending = parked.completion.borrow().is_none();
    let wrong_phase = owner.join();
    let retirement = owner
        .shutdown_and_join()
        .expect("explicit retirement joins after construction panic");
    drop(observer);

    assert!(Instant::now() <= deadline, "no grace-expiry escape");
    assert!(admission_closed && completion_pending);
    assert_eq!(wrong_phase, Err(RuntimeError::NotQuiesced));
    assert_eq!(
        retirement,
        RetirementReport {
            report: ShutdownReport {
                workers_joined: 1,
                workers_forced: 0,
            },
            failures: Vec::new(),
        }
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(parked.completion.borrow().clone(), Some(Ok(())));
    assert_eq!(parked.handle.state(), WorkerState::Closed);
    assert_eq!(parked.handle.submit(|_| {}), Err(SubmitError::Closed));
    assert!(
        parked
            .resume
            .as_ref()
            .expect("release retained")
            .is_closed()
    );
    let panic = quiescence.err().expect("construction panic retained");
    let resumed = catch_unwind(AssertUnwindSafe(|| resume_unwind(panic)))
        .expect_err("original failure remains after physical retirement");
    assert_eq!(
        resumed.downcast_ref::<&str>(),
        Some(&"explicit quiescing wake panic")
    );
}

#[test]
fn explicit_retirement_after_quiesce_observation_panic() {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (worker, parked) = parked_worker(1, &exits, &cleanups);
    let mut worker = Some(worker);
    let mut owner = RuntimeOwner::start_with(config(1, 1), |_, _| {
        Ok(worker.take().expect("parked worker starts once"))
    })
    .expect("runtime owns parked worker");
    let mut quiescence = Box::pin(owner.handle().quiesce(Duration::from_secs(60)));
    let mut observed_pending = false;
    let observation = catch_unwind(AssertUnwindSafe(|| {
        let waker = futures::task::noop_waker();
        observed_pending = quiescence
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending();
        panic_any("injected caller observation panic");
    }));
    drop(quiescence);
    let completion_pending = parked.completion.borrow().is_none();
    let retirement = owner
        .shutdown_and_join()
        .expect("explicit retirement needs no replacement observer");

    assert!(Instant::now() <= deadline, "no grace-expiry escape");
    assert!(observed_pending && completion_pending);
    assert_eq!(
        retirement,
        RetirementReport {
            report: ShutdownReport {
                workers_joined: 1,
                workers_forced: 0,
            },
            failures: Vec::new(),
        }
    );
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(parked.completion.borrow().clone(), Some(Ok(())));
    assert_eq!(parked.handle.state(), WorkerState::Closed);
    assert!(
        parked
            .resume
            .as_ref()
            .expect("release retained")
            .is_closed()
    );
    let panic = observation.expect_err("caller observation panic retained");
    let resumed = catch_unwind(AssertUnwindSafe(|| resume_unwind(panic)))
        .expect_err("original observation failure is rethrown only after joining");
    assert_eq!(
        resumed.downcast_ref::<&str>(),
        Some(&"injected caller observation panic")
    );
}

#[test]
fn explicit_retirement_reports_worker_failures_after_all_joins() {
    let deadline = Instant::now() + TEST_TIMEOUT;
    for normal_join_first in [false, true] {
        let mut owner = RuntimeOwner::start(config(2, 1)).expect("two workers start");
        let handle = owner.handle();
        let exits = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let (sibling_tx, sibling_rx) = oneshot::channel();
        let mut sibling_tx = Some(sibling_tx);
        let mut sibling_rx = Some(sibling_rx);
        for index in 0..2 {
            let task_exits = Arc::clone(&exits);
            let task_cleanups = Arc::clone(&cleanups);
            let sibling_signal = if index == 1 { sibling_tx.take() } else { None };
            let sibling_cleanup = if index == 0 { sibling_rx.take() } else { None };
            let (started_tx, started_rx) = oneshot::channel();
            handle
                .worker(index)
                .expect("worker exists")
                .spawn_local(move |context| async move {
                    record_thread_exit(task_exits);
                    let cleanup_owner = context.owner_thread();
                    let _cleanup = context
                        .register_cleanup_task(async move {
                            assert_eq!(std::thread::current().id(), cleanup_owner);
                            if let Some(sibling_signal) = sibling_signal {
                                sibling_signal.send(()).expect("first cleanup still owned");
                            }
                            if let Some(sibling_cleanup) = sibling_cleanup {
                                sibling_cleanup.await.expect("sibling shutdown requested");
                            }
                            task_cleanups.fetch_add(1, Ordering::SeqCst);
                            if index == 0 {
                                panic!("explicit retirement cleanup panic");
                            }
                        })
                        .expect("cleanup registered");
                    started_tx.send(()).expect("root observes admitted scope");
                    pending::<()>().await;
                })
                .expect("scope admitted");
            wait(started_rx).expect("scope started");
        }
        let observer = if normal_join_first {
            None
        } else {
            Some(panicking_observer(
                &handle.worker(0).expect("first worker exists"),
                &wakes,
                "explicit retirement notification panic",
            ))
        };
        let mut quiescence_result = None;
        let mut normal_join_result = None;
        if normal_join_first {
            quiescence_result = Some(wait(handle.quiesce(Duration::ZERO)));
            normal_join_result = Some(owner.join());
        }
        let retirement = owner
            .shutdown_and_join()
            .expect("physical receipt retains unhealthy workers");
        drop(observer);
        let mut failures = Vec::new();
        if !normal_join_first {
            failures.push(WorkerFailure {
                worker: 0,
                error: ManagedWorkerError::ShutdownNotificationPanicked,
            });
        }
        failures.push(WorkerFailure {
            worker: 0,
            error: ManagedWorkerError::TaskGroup(TaskGroupError::TaskPanicked),
        });
        let error = RuntimeError::WorkersFailed {
            failures: failures.clone(),
        };
        assert_eq!(
            retirement,
            RetirementReport {
                report: ShutdownReport {
                    workers_joined: 2,
                    workers_forced: if normal_join_first { 2 } else { 0 },
                },
                failures,
            }
        );
        if normal_join_first {
            assert_eq!(quiescence_result, Some(Err(error.clone())));
            assert_eq!(normal_join_result, Some(Err(error.clone())));
        }
        assert_eq!(owner.join(), Err(error));
        assert_eq!(owner.shutdown_and_join(), Ok(retirement));
        drop(owner);
        assert!(Instant::now() <= deadline, "fixed control cutoff");
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            usize::from(!normal_join_first)
        );
        assert_eq!(cleanups.load(Ordering::SeqCst), 2);
        assert_eq!(exits.load(Ordering::SeqCst), 2);
        for index in 0..2 {
            let worker = handle.worker(index).expect("surviving worker handle");
            assert_eq!(worker.state(), WorkerState::Closed);
            assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        }
    }
}

#[test]
fn explicit_retirement_rejects_tokio_without_losing_owner() {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let (worker, parked) = parked_worker(1, &exits, &cleanups);
    let mut worker = Some(worker);
    let mut owner = RuntimeOwner::start_with(config(1, 1), |_, _| {
        Ok(worker.take().expect("parked worker starts once"))
    })
    .expect("runtime owns parked worker");
    let (refused, accepting, completion_pending, exits_before) = wait(async {
        (
            owner.shutdown_and_join(),
            parked.handle.is_accepting(),
            parked.completion.borrow().is_none(),
            exits.load(Ordering::SeqCst),
        )
    });
    let quiescence = wait(owner.handle().quiesce(Duration::ZERO));
    let joined = owner.join();
    let retirement = owner
        .shutdown_and_join()
        .expect("normal join caches a complete receipt");
    let cached_refusal = wait(async { owner.shutdown_and_join() });
    let cached = owner.shutdown_and_join();

    assert!(Instant::now() <= deadline, "fixed control cutoff");
    assert_eq!(refused, Err(RuntimeError::InsideTokio));
    assert!(accepting && completion_pending);
    assert_eq!(exits_before, 0);
    assert_eq!(quiescence, Ok(()));
    assert_eq!(
        retirement,
        RetirementReport {
            report: ShutdownReport {
                workers_joined: 1,
                workers_forced: 1,
            },
            failures: Vec::new(),
        }
    );
    assert_eq!(joined, Ok(retirement.report));
    assert_eq!(cached_refusal, Err(RuntimeError::InsideTokio));
    assert_eq!(cached, Ok(retirement));
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(parked.handle.state(), WorkerState::Closed);
    assert!(
        parked
            .resume
            .as_ref()
            .expect("release retained")
            .is_closed()
    );
}

#[test]
fn explicit_retirement_reports_final_forced_state() {
    let deadline = Instant::now() + TEST_TIMEOUT;
    for grace in [Duration::ZERO, Duration::from_secs(60)] {
        let exits = Arc::new(AtomicUsize::new(0));
        let cleanups = Arc::new(AtomicUsize::new(0));
        let (worker, parked) = parked_worker(1, &exits, &cleanups);
        let mut worker = Some(worker);
        let mut owner = RuntimeOwner::start_with(config(1, 1), |_, _| {
            Ok(worker.take().expect("parked worker starts once"))
        })
        .expect("runtime owns parked worker");
        drop(owner.handle().quiesce(grace));
        let retirement = owner
            .shutdown_and_join()
            .expect("retirement observes actual grace-expiry state");
        assert!(Instant::now() <= deadline, "no nonzero-grace escape");
        assert_eq!(
            retirement,
            RetirementReport {
                report: ShutdownReport {
                    workers_joined: 1,
                    workers_forced: usize::from(grace.is_zero()),
                },
                failures: Vec::new(),
            }
        );
        assert_eq!(cleanups.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(parked.handle.is_cancelled());
        assert_eq!(parked.handle.state(), WorkerState::Closed);
        assert!(
            parked
                .resume
                .as_ref()
                .expect("release retained")
                .is_closed()
        );
    }

    let exits = Arc::new(AtomicUsize::new(0));
    let synthetic_forced_marks = Arc::new(AtomicUsize::new(0));
    let exit_marks = Arc::clone(&synthetic_forced_marks);
    let (exit_handle_tx, exit_handle_rx) = mpsc::sync_channel::<WorkerHandle>(1);
    let worker = ManagedWorker::new_with_exit_callback(1, move || {
        let Ok(handle) = exit_handle_rx.try_recv() else {
            return;
        };
        if std::thread::current().id() == handle.owner_thread() {
            handle.mark_forced();
            exit_marks.fetch_add(1, Ordering::SeqCst);
        }
    })
    .expect("worker with synthetic exit-time mark starts");
    let mut worker = Some(worker);
    let mut owner = RuntimeOwner::start_with(config(1, 1), |_, _| {
        Ok(worker.take().expect("worker starts once"))
    })
    .expect("runtime owns exit-mark worker");
    let handle = owner.handle().worker(0).expect("worker exists");
    exit_handle_tx
        .send(handle.clone())
        .expect("exit callback receives handle before shutdown");
    let task_exits = Arc::clone(&exits);
    wait(
        handle
            .call(move |_| record_thread_exit(task_exits))
            .expect("thread exit probe admitted"),
    )
    .expect("thread exit probe installed");
    let forced_before = handle.was_forced();
    let retirement = owner
        .shutdown_and_join()
        .expect("join precedes synthetic final-state sampling");
    assert!(Instant::now() <= deadline, "fixed control cutoff");
    assert!(!forced_before);
    assert_eq!(synthetic_forced_marks.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(handle.state(), WorkerState::Closed);
    assert_eq!(
        retirement,
        RetirementReport {
            report: ShutdownReport {
                workers_joined: 1,
                workers_forced: 1,
            },
            failures: Vec::new(),
        }
    );
}

/// Blocking tasks that count how many of them run at once. Each one waits at
/// a gate until the test opens it.
#[derive(Default)]
struct BlockingProbe {
    state: Mutex<BlockingProbeState>,
    changed: Condvar,
}

#[derive(Default)]
struct BlockingProbeState {
    running: usize,
    peak: usize,
    open: bool,
}

impl BlockingProbe {
    /// The body of one blocking task.
    fn run(&self) {
        let mut state = self.state.lock().unwrap();
        state.running += 1;
        state.peak = state.peak.max(state.running);
        self.changed.notify_all();
        while !state.open {
            state = self.changed.wait(state).unwrap();
        }
        state.running -= 1;
    }

    fn wait_for_running(&self, running: usize) {
        let state = self.state.lock().unwrap();
        let (_state, waited) = self
            .changed
            .wait_timeout_while(state, TEST_TIMEOUT, |state| state.running < running)
            .unwrap();
        assert!(!waited.timed_out(), "{running} blocking tasks run at once");
    }

    fn open(&self) {
        self.state.lock().unwrap().open = true;
        self.changed.notify_all();
    }

    fn peak(&self) -> usize {
        self.state.lock().unwrap().peak
    }
}

/// Opens the probe's gate when dropped, so a failed assertion does not leave
/// blocking threads that the worker's join would wait for.
struct OpenOnDrop(Arc<BlockingProbe>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

#[test]
fn managed_worker_caps_blocking_threads() {
    let limits = BlockingPoolLimits::new(NonZeroUsize::new(2), None);
    let worker =
        ManagedWorker::new_with_options(ManagedWorkerOptions::new(4).with_blocking(limits), || {})
            .expect("managed worker starts");
    let handle = worker.handle();
    let probe = Arc::new(BlockingProbe::default());
    let _open = OpenOnDrop(Arc::clone(&probe));
    let started = handle.poll_stats();

    wait(
        handle
            .call_local(|_| async {
                tokio::task::spawn_blocking(|| ())
                    .await
                    .expect("blocking task completes");
            })
            .expect("one blocking task admitted"),
    )
    .expect("one blocking task runs");
    let one = handle.poll_stats();
    assert_eq!(
        one.since(&started).blocking_cap_hits,
        0,
        "one live thread stays below the cap of 2: {one:?}"
    );

    let tasks = Arc::clone(&probe);
    let eight = handle
        .call_local(move |_| async move {
            let joins = (0..8)
                .map(|_| {
                    let probe = Arc::clone(&tasks);
                    tokio::task::spawn_blocking(move || probe.run())
                })
                .collect::<Vec<_>>();
            for join in joins {
                join.await.expect("blocking task completes");
            }
        })
        .expect("eight blocking tasks admitted");
    probe.wait_for_running(2);
    // Long enough for a third thread to start if the cap allowed one.
    std::thread::sleep(Duration::from_millis(100));
    probe.open();
    wait(eight).expect("all eight blocking tasks run");
    let saturated = handle.poll_stats();

    assert_eq!(
        probe.peak(),
        2,
        "at most max_threads blocking tasks run at once"
    );
    assert_eq!(saturated.max_blocking_threads_live, 2, "{saturated:?}");
    assert!(
        saturated.since(&one).blocking_cap_hits >= 1,
        "the thread start that reached the cap counts: {saturated:?}"
    );
    worker.shutdown().expect("worker shuts down");
}

#[test]
fn blocking_keep_alive_reaps_idle_threads() {
    let limits = BlockingPoolLimits::new(None, Some(Duration::from_millis(50)));
    let worker =
        ManagedWorker::new_with_options(ManagedWorkerOptions::new(4).with_blocking(limits), || {})
            .expect("managed worker starts");
    let handle = worker.handle();
    wait(
        handle
            .call_local(|_| async {
                tokio::task::spawn_blocking(|| ())
                    .await
                    .expect("blocking task completes");
            })
            .expect("blocking task admitted"),
    )
    .expect("blocking task runs");
    assert_eq!(
        handle.poll_stats().max_blocking_threads_live,
        1,
        "the task ran on one blocking thread"
    );

    // Tokio's default keep-alive of 10 s would keep the idle thread past this.
    let deadline = Instant::now() + Duration::from_secs(5);
    while handle.poll_stats().blocking_threads_live != 0 {
        assert!(
            Instant::now() < deadline,
            "the idle blocking thread exits after its keep-alive: {:?}",
            handle.poll_stats()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    worker.shutdown().expect("worker shuts down");
}

#[test]
fn blocking_thread_stats_since_and_merge() {
    let earlier = WorkerPollStats {
        blocking_threads_live: 3,
        max_blocking_threads_live: 4,
        blocking_cap_hits: 5,
        ..WorkerPollStats::default()
    };
    let later = WorkerPollStats {
        blocking_threads_live: 1,
        max_blocking_threads_live: 4,
        blocking_cap_hits: 7,
        ..WorkerPollStats::default()
    };
    assert_eq!(
        later.since(&earlier),
        WorkerPollStats {
            blocking_threads_live: 1,
            max_blocking_threads_live: 4,
            blocking_cap_hits: 2,
            ..WorkerPollStats::default()
        },
        "since subtracts cap hits and keeps the later gauge and maximum"
    );

    let other = WorkerPollStats {
        blocking_threads_live: 2,
        max_blocking_threads_live: 6,
        blocking_cap_hits: 1,
        ..WorkerPollStats::default()
    };
    let mut merged = later;
    merged.merge(&other);
    assert_eq!(
        merged,
        WorkerPollStats {
            blocking_threads_live: 3,
            max_blocking_threads_live: 6,
            blocking_cap_hits: 8,
            ..WorkerPollStats::default()
        },
        "merge sums the gauge and cap hits and takes the larger maximum"
    );
}
