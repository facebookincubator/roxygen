/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use futures::channel::oneshot;
use futures::executor::LocalPool;
use futures::executor::LocalSpawner;
use futures::task::LocalSpawnExt;
use futures::task::noop_waker;

use crate::cancellation::CancellationSource;
use crate::cancellation::CancellationToken;
use crate::driver::WorkerDriver;
use crate::host::Host;
use crate::host::LocalFuture;
use crate::host::LocalHost;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupError;
use crate::wait::CancellableBaton;
use crate::wait::DetachableCancellableBaton;
use crate::wait::TimedBaton;
use crate::wait::WaitStatus;
use crate::worker::WorkerState;

struct Timer {
    deadline: Instant,
    waker: RefCell<Option<Waker>>,
}

struct TestHost {
    spawner: LocalSpawner,
    now: Rc<Cell<Instant>>,
    timers: Rc<RefCell<BTreeMap<u64, Rc<Timer>>>>,
    next_timer: Cell<u64>,
    yields: Rc<Cell<usize>>,
    tasks: Rc<Cell<usize>>,
}

impl TestHost {
    fn create() -> (LocalPool, Rc<Self>, Host) {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let pool = LocalPool::new();
        let implementation = Rc::new(Self {
            spawner: pool.spawner(),
            now: Rc::new(Cell::new(Instant::now())),
            timers: Rc::new(RefCell::new(BTreeMap::new())),
            next_timer: Cell::new(0),
            yields: Rc::new(Cell::new(0)),
            tasks: Rc::new(Cell::new(0)),
        });
        let host = Host::new(implementation.clone());
        (pool, implementation, host)
    }

    fn advance(&self, duration: Duration) {
        self.now.set(self.now.get() + duration);
        let ready: Vec<_> = self
            .timers
            .borrow()
            .values()
            .filter(|timer| timer.deadline <= self.now.get())
            .filter_map(|timer| timer.waker.borrow_mut().take())
            .collect();
        ready.into_iter().for_each(Waker::wake);
    }
}

impl LocalHost for TestHost {
    fn spawn_local(&self, future: LocalFuture) {
        self.tasks.set(self.tasks.get() + 1);
        let retired = HostTaskRetirement(Rc::clone(&self.tasks));
        self.spawner
            .spawn_local(async move {
                let _retired = retired;
                future.await;
            })
            .expect("test executor is alive");
    }

    fn now(&self) -> Instant {
        self.now.get()
    }

    fn sleep_until(&self, deadline: Instant) -> LocalFuture {
        let id = self.next_timer.get();
        self.next_timer.set(id + 1);
        let timer = Rc::new(Timer {
            deadline,
            waker: RefCell::new(None),
        });
        self.timers.borrow_mut().insert(id, Rc::clone(&timer));
        Box::pin(TestSleep {
            id,
            timer,
            now: Rc::clone(&self.now),
            timers: Rc::clone(&self.timers),
        })
    }

    fn yield_to_io(&self) -> LocalFuture {
        let mut yielded = false;
        let yields = Rc::clone(&self.yields);
        Box::pin(poll_fn(move |context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                yields.set(yields.get() + 1);
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }))
    }
}

struct HostTaskRetirement(Rc<Cell<usize>>);

impl Drop for HostTaskRetirement {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

struct TestSleep {
    id: u64,
    timer: Rc<Timer>,
    now: Rc<Cell<Instant>>,
    timers: Rc<RefCell<BTreeMap<u64, Rc<Timer>>>>,
}

impl Future for TestSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.now.get() >= self.timer.deadline {
            Poll::Ready(())
        } else {
            *self.timer.waker.borrow_mut() = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for TestSleep {
    fn drop(&mut self) {
        self.timers.borrow_mut().remove(&self.id);
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(&noop_waker()))
}

struct DropProbe {
    owner: ThreadId,
    drops: Rc<Cell<usize>>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn tracked_completions_survive_spawning_and_convenience_reaping() {
    let (mut pool, implementation, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host.clone());
    assert!(!tasks.has_ready_completion());
    let first = tasks.spawn_local_tracked(|_| async {}).unwrap();
    assert!(!tasks.has_ready_completion());
    pool.run_until_stalled();
    assert!(tasks.has_ready_completion());
    let second = tasks
        .spawn_local_tracked(move |_| async move { host.yield_to_io().await })
        .unwrap();
    tasks.spawn_local(|_| async {}).unwrap();
    tasks.reap_finished();
    assert_ne!(first, second);
    assert!(tasks.has_ready_completion());
    assert_eq!(tasks.try_join_next().unwrap().id, first);
    assert!(!tasks.has_ready_completion());
    pool.run_until_stalled();
    assert!(tasks.has_ready_completion());
    assert_eq!(tasks.try_join_next().unwrap().id, second);
    assert!(
        tasks.has_ready_completion(),
        "the untracked completion remains"
    );
    assert_eq!(implementation.yields.get(), 1);
    pool.run_until(tasks.join()).unwrap();
    assert!(!tasks.has_ready_completion());
}

#[test]
fn cancellation_and_dropped_join_retain_owner_local_children() {
    let (mut pool, _, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host.clone());
    let drops = Rc::new(Cell::new(0));
    let probe = DropProbe {
        owner: std::thread::current().id(),
        drops: Rc::clone(&drops),
    };
    let (release, released) = oneshot::channel();
    tasks
        .spawn_local(move |cancellation| async move {
            let _probe = probe;
            let baton = TimedBaton::with_host(None, host);
            assert_eq!(baton.wait(cancellation).await, WaitStatus::Cancelled);
            released.await.unwrap();
        })
        .unwrap();
    pool.run_until_stalled();
    let mut joined = Box::pin(tasks.cancel_and_join());
    assert!(poll_once(joined.as_mut()).is_pending());
    pool.run_until_stalled();
    drop(joined);
    assert_eq!(drops.get(), 0);
    assert_eq!(tasks.len(), 1);
    release.send(()).unwrap();
    pool.run_until(tasks.join()).unwrap();
    assert_eq!(drops.get(), 1);
}

#[test]
fn abort_and_panic_are_sticky_and_retire_captures() {
    let (mut pool, _, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    let drops = Rc::new(Cell::new(0));
    let probe = DropProbe {
        owner: std::thread::current().id(),
        drops: Rc::clone(&drops),
    };
    let aborted = tasks
        .spawn_local_tracked(move |_| async move {
            let _probe = probe;
            pending::<()>().await;
        })
        .unwrap();
    assert!(tasks.abort(aborted));
    assert!(!tasks.abort(aborted));
    assert_eq!(drops.get(), 1);
    let completion = tasks.try_join_next().unwrap();
    assert_eq!(completion.id, aborted);
    assert_eq!(completion.result, Err(TaskGroupError::TaskCancelled));
    tasks
        .spawn_local(|_| async { panic!("hosted child panic") })
        .unwrap();
    pool.run_until_stalled();
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskCancelled)
    );
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskCancelled)
    );
}

#[test]
fn panic_is_reported_after_siblings_finish() {
    let (mut pool, _, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    tasks
        .spawn_local(|_| async { panic!("hosted child panic") })
        .unwrap();
    let (release, released) = oneshot::channel();
    tasks
        .spawn_local(move |_| async move {
            released.await.unwrap();
        })
        .unwrap();
    pool.run_until_stalled();
    let mut joined = Box::pin(tasks.join());
    assert!(poll_once(joined.as_mut()).is_pending());
    drop(joined);
    release.send(()).unwrap();
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskPanicked)
    );
}

#[test]
fn host_executor_loss_retires_task_as_cancelled() {
    let (pool, implementation, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    tasks.spawn_local(|_| pending::<()>()).unwrap();
    drop(pool);
    drop(implementation);
    assert_eq!(
        futures::executor::block_on(tasks.join()),
        Err(TaskGroupError::TaskCancelled)
    );
}

#[test]
fn absolute_deadlines_reset_disarm_and_retain_timeout_precedence() {
    let (_, implementation, host) = TestHost::create();
    let source = CancellationSource::new();
    let baton = TimedBaton::with_host(Some(Duration::from_secs(5)), host.clone());
    let mut wait = Box::pin(baton.wait(source.token()));
    assert!(poll_once(wait.as_mut()).is_pending());
    implementation.advance(Duration::from_secs(4));
    assert!(poll_once(wait.as_mut()).is_pending());
    baton.set_timeout(Some(Duration::from_secs(3)));
    assert!(poll_once(wait.as_mut()).is_pending());
    assert_eq!(implementation.timers.borrow().len(), 1);
    implementation.advance(Duration::from_secs(2));
    assert!(poll_once(wait.as_mut()).is_pending());
    implementation.advance(Duration::from_secs(1));
    assert!(poll_once(wait.as_mut()).is_pending());
    assert_eq!(baton.status(), WaitStatus::TimedOut);
    source.request_cancellation();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(WaitStatus::TimedOut));
    assert!(implementation.timers.borrow().is_empty());

    let detached = DetachableCancellableBaton::with_host(host);
    let mut wait =
        Box::pin(detached.timed_wait(Duration::from_secs(1), CancellationToken::never()));
    assert!(poll_once(wait.as_mut()).is_pending());
    detached.detach();
    assert!(poll_once(wait.as_mut()).is_pending());
    assert!(implementation.timers.borrow().is_empty());
    implementation.advance(Duration::from_secs(60));
    assert!(poll_once(wait.as_mut()).is_pending());
    detached.signal();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(WaitStatus::Signalled));
}

#[test]
fn cancellable_timeout_and_signal_lose_to_observed_cancellation() {
    let (_, implementation, host) = TestHost::create();
    let source = CancellationSource::new();
    let baton = CancellableBaton::with_host(host.clone());
    let mut wait = Box::pin(baton.timed_wait(Duration::from_secs(1), source.token()));
    assert!(poll_once(wait.as_mut()).is_pending());
    implementation.advance(Duration::from_secs(1));
    assert!(poll_once(wait.as_mut()).is_pending());
    source.request_cancellation();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(WaitStatus::Cancelled));
    let source = CancellationSource::new();
    let baton = TimedBaton::with_host(None, host);
    let mut wait = Box::pin(baton.wait(source.token()));
    assert!(poll_once(wait.as_mut()).is_pending());
    baton.signal();
    source.request_cancellation();
    assert_eq!(poll_once(wait.as_mut()), Poll::Ready(WaitStatus::Cancelled));
}

#[test]
fn command_wakes_do_not_repoll_other_commands() {
    let (mut pool, _, host) = TestHost::create();
    let (worker, driver) = WorkerDriver::with_host(2, host.clone()).unwrap();
    let mut owner = TaskGroup::with_host(host);
    owner.spawn_local(move |_| driver).unwrap();
    let first_polls = Arc::new(AtomicUsize::new(0));
    let second_polls = Arc::new(AtomicUsize::new(0));
    let first_waker = Arc::new(Mutex::new(None));
    for (polls, waker) in [
        (Arc::clone(&first_polls), Arc::clone(&first_waker)),
        (Arc::clone(&second_polls), Arc::new(Mutex::new(None))),
    ] {
        worker
            .spawn_local(move |_| {
                poll_fn(move |context| {
                    polls.fetch_add(1, Ordering::SeqCst);
                    *waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::<()>::Pending
                })
            })
            .unwrap();
    }
    pool.run_until_stalled();
    assert_eq!(first_polls.load(Ordering::SeqCst), 1);
    assert_eq!(second_polls.load(Ordering::SeqCst), 1);
    first_waker.lock().unwrap().take().unwrap().wake();
    pool.run_until_stalled();
    assert_eq!(first_polls.load(Ordering::SeqCst), 2);
    assert_eq!(second_polls.load(Ordering::SeqCst), 1);
    worker.begin_shutdown();
    pool.run_until(owner.join()).unwrap();
    assert_eq!(worker.state(), WorkerState::Closed);
}

#[test]
fn reserved_scope_activates_during_closing_and_observes_retirement() {
    let (mut pool, _, host) = TestHost::create();
    let (worker, driver) = WorkerDriver::with_host(1, host.clone()).unwrap();
    let mut owner = TaskGroup::with_host(host);
    owner.spawn_local(move |_| driver).unwrap();
    let (release, released) = oneshot::channel();
    let call = worker
        .call(move |context| {
            let owner_thread = context.owner_thread();
            let cleanup = context
                .register_scope(async move {
                    assert_eq!(std::thread::current().id(), owner_thread);
                    released.await.unwrap();
                })
                .unwrap();
            let observer = cleanup.observe();
            drop(
                context
                    .register_cleanup(move || cleanup.activate().unwrap().detach())
                    .unwrap(),
            );
            observer
        })
        .unwrap();
    let observer = pool.run_until(call).unwrap();
    worker.begin_shutdown();
    pool.run_until_stalled();
    assert_eq!(worker.state(), WorkerState::Closing);
    let mut observed = Box::pin(observer.wait());
    assert!(poll_once(observed.as_mut()).is_pending());
    release.send(()).unwrap();
    pool.run_until(owner.join()).unwrap();
    assert_eq!(worker.state(), WorkerState::Closed);
    assert_eq!(poll_once(observed.as_mut()), Poll::Ready(Ok(())));
}

#[test]
fn active_cleanup_wakes_do_not_repoll_siblings() {
    let (mut pool, _, host) = TestHost::create();
    let (worker, driver) = WorkerDriver::with_host(1, host.clone()).unwrap();
    let mut owner = TaskGroup::with_host(host);
    owner.spawn_local(move |_| driver).unwrap();
    let polls = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let wakers = Arc::new(Mutex::new([None, None]));
    let complete = Arc::new(AtomicUsize::new(0));
    let call = worker
        .call({
            let polls = Arc::clone(&polls);
            let wakers = Arc::clone(&wakers);
            let complete = Arc::clone(&complete);
            move |context| {
                for index in 0..2 {
                    let polls = Arc::clone(&polls);
                    let wakers = Arc::clone(&wakers);
                    let complete = Arc::clone(&complete);
                    context
                        .register_scope(poll_fn(move |context| {
                            polls[index].fetch_add(1, Ordering::SeqCst);
                            wakers.lock().unwrap()[index] = Some(context.waker().clone());
                            if complete.load(Ordering::SeqCst) != 0 {
                                Poll::Ready(())
                            } else {
                                Poll::Pending
                            }
                        }))
                        .unwrap()
                        .activate()
                        .unwrap()
                        .detach();
                }
            }
        })
        .unwrap();
    pool.run_until(call).unwrap();
    pool.run_until_stalled();
    assert_eq!(polls[0].load(Ordering::SeqCst), 1);
    assert_eq!(polls[1].load(Ordering::SeqCst), 1);
    wakers.lock().unwrap()[0].take().unwrap().wake();
    pool.run_until_stalled();
    assert_eq!(polls[0].load(Ordering::SeqCst), 2);
    assert_eq!(polls[1].load(Ordering::SeqCst), 1);
    worker.begin_shutdown();
    pool.run_until_stalled();
    complete.store(1, Ordering::SeqCst);
    let ready = std::mem::take(&mut *wakers.lock().unwrap());
    ready.into_iter().flatten().for_each(Waker::wake);
    pool.run_until(owner.join()).unwrap();
    assert_eq!(worker.state(), WorkerState::Closed);
}

struct PanicDropProbe {
    owner: ThreadId,
    primary: Arc<AtomicUsize>,
    secondary: Arc<AtomicUsize>,
}

struct SecondaryPanicDrop(Arc<AtomicUsize>);

impl Drop for SecondaryPanicDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for PanicDropProbe {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.primary.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(SecondaryPanicDrop(Arc::clone(&self.secondary)));
    }
}

#[test]
fn panic_payload_retirement_follows_reaping_and_survives_secondary_panic() {
    let (mut pool, _, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    let primary = Arc::new(AtomicUsize::new(0));
    let secondary = Arc::new(AtomicUsize::new(0));
    let payload = PanicDropProbe {
        owner: std::thread::current().id(),
        primary: Arc::clone(&primary),
        secondary: Arc::clone(&secondary),
    };
    tasks
        .spawn_local(move |_| async move { std::panic::panic_any(payload) })
        .unwrap();
    pool.run_until_stalled();
    assert_eq!(primary.load(Ordering::SeqCst), 0);
    tasks.reap_finished();
    assert!(tasks.is_empty());
    assert_eq!(primary.load(Ordering::SeqCst), 1);
    assert_eq!(secondary.load(Ordering::SeqCst), 1);
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskPanicked)
    );
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskPanicked)
    );
}

#[test]
fn abort_wakes_and_retires_an_already_pending_host_wrapper() {
    let (mut pool, implementation, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    let id = tasks.spawn_local_tracked(|_| pending::<()>()).unwrap();
    pool.run_until_stalled();
    assert_eq!(implementation.tasks.get(), 1);
    assert!(tasks.abort(id));
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskCancelled)
    );
    pool.run_until_stalled();
    assert_eq!(implementation.tasks.get(), 0);
}

#[test]
fn tracked_completion_returns_identity_before_panicking_payload_disposal() {
    struct Payload(Arc<AtomicUsize>);
    impl Drop for Payload {
        fn drop(&mut self) {
            assert_eq!(
                self.0.load(Ordering::SeqCst),
                1,
                "caller retired the ID first"
            );
            panic!("tracked payload drop");
        }
    }

    let (mut pool, _, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    let retired = Arc::new(AtomicUsize::new(0));
    let payload = Payload(Arc::clone(&retired));
    let id = tasks
        .spawn_local_tracked(move |_| async move { std::panic::panic_any(payload) })
        .unwrap();
    pool.run_until_stalled();
    let completion = tasks.try_join_next().unwrap();
    assert_eq!(completion.id, id);
    assert_eq!(completion.result, Err(TaskGroupError::TaskPanicked));
    assert!(tasks.is_empty());
    retired.store(1, Ordering::SeqCst);
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(completion))).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"tracked payload drop"));
    assert_eq!(
        pool.run_until(tasks.join()),
        Err(TaskGroupError::TaskPanicked)
    );
}

#[test]
fn unrepresentable_timeout_uses_far_future_deadline_without_a_runtime() {
    let (_, implementation, host) = TestHost::create();
    let expected = host.now() + Duration::from_secs(86_400 * 365 * 30);
    assert_eq!(host.deadline_after(Duration::MAX), expected);
    let baton = TimedBaton::with_host(Some(Duration::MAX), host);
    let mut wait = Box::pin(baton.wait(CancellationToken::never()));
    assert!(poll_once(wait.as_mut()).is_pending());
    assert_eq!(
        implementation
            .timers
            .borrow()
            .values()
            .next()
            .unwrap()
            .deadline,
        expected
    );
    drop(wait);
    assert!(implementation.timers.borrow().is_empty());
}

#[test]
fn tokio_host_overflow_deadline_matches_tokio_sleep() {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
        .block_on(async {
            let expected = tokio::time::sleep(Duration::MAX).deadline().into_std();
            assert_eq!(Host::tokio().deadline_after(Duration::MAX), expected);
        });
}

#[test]
fn manually_reaped_closed_group_finishes_its_shutdown_span() {
    use crate::context::ContextEnvelope;
    use crate::observability::OperationKind;
    use crate::observability::SpanObserver;
    use crate::observability::TerminalOutcome;

    let (mut pool, _, host) = TestHost::create();
    let observer = SpanObserver::new(16).unwrap();
    let context = ContextEnvelope::capture().with_span_observer(observer.clone());
    let mut tasks = TaskGroup::with_host(host);
    context.run(|| {
        tasks.spawn_local(|_| async {}).unwrap();
        tasks.close();
    });
    pool.run_until_stalled();
    tasks.reap_finished();
    assert!(tasks.is_empty());
    assert_eq!(observer.active_count(), 0);
    let shutdown = observer
        .snapshots()
        .into_iter()
        .find(|span| span.kind == OperationKind::Shutdown)
        .unwrap();
    assert_eq!(shutdown.outcome, Some(TerminalOutcome::Success));
    assert_eq!(shutdown.close_count, 1);
}

#[test]
fn hosted_cleanup_finishes_shutdown_spans_before_driver_destruction() {
    use crate::context::ContextEnvelope;
    use crate::observability::OperationKind;
    use crate::observability::SpanObserver;
    use crate::observability::TerminalOutcome;

    let (mut pool, _, host) = TestHost::create();
    let observer = SpanObserver::new(32).unwrap();
    let context = ContextEnvelope::capture().with_span_observer(observer.clone());
    let (worker, driver) = WorkerDriver::with_host(1, host).unwrap();
    let (release, released) = oneshot::channel();
    let registration = driver
        .context()
        .register_scope(async move {
            released.await.unwrap();
        })
        .unwrap();
    let mut driver = Box::pin(driver);
    context.run(|| worker.begin_shutdown());
    assert!(context.run(|| poll_once(driver.as_mut())).is_pending());
    pool.run_until_stalled();
    assert!(
        observer
            .snapshots()
            .iter()
            .any(|span| { span.kind == OperationKind::Shutdown && span.outcome.is_none() })
    );
    release.send(()).unwrap();
    pool.run_until_stalled();
    assert!(context.run(|| poll_once(driver.as_mut())).is_ready());
    assert_eq!(worker.state(), WorkerState::Closed);
    assert_eq!(observer.active_count(), 0);
    let snapshots = observer.snapshots();
    assert!(snapshots.iter().all(|span| span.close_count == 1));
    assert!(
        snapshots
            .iter()
            .filter(|span| span.kind == OperationKind::Shutdown)
            .all(|span| matches!(
                span.outcome,
                Some(TerminalOutcome::Success | TerminalOutcome::Shutdown)
            ))
    );
    assert!(!registration.disarm().unwrap());
}

#[test]
fn panicking_cancellation_waker_cannot_skip_group_drop_retirement() {
    struct PanicWake;
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("cancellation notification panic");
        }
    }

    let (mut pool, implementation, host) = TestHost::create();
    let mut tasks = TaskGroup::with_host(host);
    let drops = Rc::new(Cell::new(0));
    let probe = DropProbe {
        owner: std::thread::current().id(),
        drops: Rc::clone(&drops),
    };
    tasks
        .spawn_local(move |cancellation| {
            let mut registration = None;
            let waker = Waker::from(Arc::new(PanicWake));
            assert!(!cancellation.register(&mut registration, &waker));
            async move {
                let _registration = registration;
                let _probe = probe;
                pending::<()>().await;
            }
        })
        .unwrap();
    pool.run_until_stalled();
    assert_eq!(implementation.tasks.get(), 1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(tasks))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"cancellation notification panic")
    );
    assert_eq!(
        drops.get(),
        1,
        "the future retires before group drop unwinds"
    );
    pool.run_until_stalled();
    assert_eq!(implementation.tasks.get(), 0);
    assert_eq!(drops.get(), 1);
}

mod external_host {
    use super::*;
    use crate::task_group::TaskId;

    /// Records its index in a shared order when dropped.
    struct OrderProbe {
        index: usize,
        order: Rc<RefCell<Vec<usize>>>,
    }

    impl Drop for OrderProbe {
        fn drop(&mut self) {
            self.order.borrow_mut().push(self.index);
        }
    }

    fn spawn_ordered(
        tasks: &mut TaskGroup,
        order: &Rc<RefCell<Vec<usize>>>,
        index: usize,
        ready: bool,
    ) -> TaskId {
        let probe = OrderProbe {
            index,
            order: Rc::clone(order),
        };
        tasks
            .spawn_local_tracked(move |_| async move {
                let _probe = probe;
                if !ready {
                    pending::<()>().await;
                }
            })
            .unwrap()
    }

    #[test]
    fn stale_task_abort_after_slot_reuse_returns_false() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let order = Rc::new(RefCell::new(Vec::new()));
        let first = spawn_ordered(&mut tasks, &order, 0, true);
        pool.run_until_stalled();
        assert_eq!(tasks.try_join_next().unwrap().id, first);
        // Task 1 takes the slot that joining task 0 freed.
        let reusing = spawn_ordered(&mut tasks, &order, 1, false);
        assert_ne!(first, reusing);
        assert!(!tasks.abort(first), "a joined task's id aborts nothing");
        assert_eq!(*order.borrow(), [0]);
        assert_eq!(tasks.len(), 1);
        assert!(tasks.abort(reusing));
        assert_eq!(*order.borrow(), [0, 1]);
        assert_eq!(
            pool.run_until(tasks.join()),
            Err(TaskGroupError::TaskCancelled)
        );
    }

    #[test]
    fn abort_all_finishes_tasks_in_spawn_order_after_slot_reuse() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let order = Rc::new(RefCell::new(Vec::new()));
        let ids: Vec<_> = (0..3)
            .map(|index| spawn_ordered(&mut tasks, &order, index, index == 0))
            .collect();
        pool.run_until_stalled();
        assert_eq!(tasks.try_join_next().unwrap().id, ids[0]);
        // Task 3 takes task 0's slot, ahead of the slots of tasks 1 and 2.
        let last = spawn_ordered(&mut tasks, &order, 3, false);
        tasks.abort_all();
        assert_eq!(*order.borrow(), [0, 1, 2, 3]);
        let joined: Vec<_> = std::iter::from_fn(|| tasks.try_join_next())
            .map(|completion| (completion.id, completion.result))
            .collect();
        let cancelled = Err(TaskGroupError::TaskCancelled);
        assert_eq!(
            joined,
            [(ids[1], cancelled), (ids[2], cancelled), (last, cancelled)]
        );
        assert!(tasks.is_empty());
        assert_eq!(pool.run_until(tasks.join()), cancelled);
    }

    /// A panic payload that records its index when released.
    struct ReleasedPayload {
        index: usize,
        released: Arc<Mutex<Vec<usize>>>,
    }

    impl Drop for ReleasedPayload {
        fn drop(&mut self) {
            self.released.lock().unwrap().push(self.index);
        }
    }

    #[test]
    fn dropped_group_releases_retained_panics_in_spawn_order() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let released = Arc::new(Mutex::new(Vec::new()));
        let panicking =
            |tasks: &mut TaskGroup, index: usize, gate: Option<oneshot::Receiver<()>>| {
                let payload = ReleasedPayload {
                    index,
                    released: Arc::clone(&released),
                };
                tasks
                    .spawn_local_tracked(move |_| async move {
                        if let Some(gate) = gate {
                            gate.await.unwrap();
                        }
                        std::panic::panic_any(payload)
                    })
                    .unwrap()
            };
        let first = tasks.spawn_local_tracked(|_| async {}).unwrap();
        let gates: Vec<_> = (1..3)
            .map(|index| {
                let (open, gate) = oneshot::channel();
                panicking(&mut tasks, index, Some(gate));
                open
            })
            .collect();
        pool.run_until_stalled();
        assert_eq!(tasks.try_join_next().unwrap().id, first);
        // Task 3 takes task 0's slot, ahead of the slots of tasks 1 and 2.
        panicking(&mut tasks, 3, None);
        gates.into_iter().for_each(|open| open.send(()).unwrap());
        pool.run_until_stalled();
        assert_eq!(tasks.len(), 3);
        assert!(
            released.lock().unwrap().is_empty(),
            "each payload is retained until its task is joined"
        );

        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(tasks))).unwrap_err();
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
        assert_eq!(
            message,
            Some("task group must join every admitted task before drop")
        );
        assert_eq!(*released.lock().unwrap(), [1, 2, 3]);
    }

    #[test]
    fn abort_with_a_foreign_or_out_of_range_id_returns_false() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host.clone());
        let mut other = TaskGroup::with_host(host);
        let foreign: Vec<_> = (0..5)
            .map(|_| other.spawn_local_tracked(|_| pending::<()>()).unwrap())
            .collect();
        let first = tasks.spawn_local_tracked(|_| async {}).unwrap();
        tasks.spawn_local_tracked(|_| pending::<()>()).unwrap();
        pool.run_until_stalled();
        assert_eq!(tasks.try_join_next().unwrap().id, first);
        tasks.spawn_local_tracked(|_| pending::<()>()).unwrap();
        // The other group's task 0 shares task 0's freed slot, now held by task
        // 2; its tasks 3 and 4 name slots this group never had.
        for id in [foreign[0], foreign[3], foreign[4]] {
            assert!(!tasks.abort(id), "{id:?}");
        }
        assert_eq!(tasks.len(), 2);
        for group in [&mut tasks, &mut other] {
            group.abort_all();
            assert_eq!(
                pool.run_until(group.join()),
                Err(TaskGroupError::TaskCancelled)
            );
        }
    }

    fn hash_of(value: &impl std::hash::Hash) -> u64 {
        use std::hash::Hasher;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn task_ids_keep_equality_ordering_hash_and_debug() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let zero = tasks.spawn_local_tracked(|_| async {}).unwrap();
        pool.run_until_stalled();
        let joined = tasks.try_join_next().unwrap();
        assert_eq!(joined.id, zero);
        // Task 1 reuses task 0's slot; task 2 takes a new one.
        let one = tasks.spawn_local_tracked(|_| pending::<()>()).unwrap();
        let two = tasks.spawn_local_tracked(|_| pending::<()>()).unwrap();

        assert_eq!(
            format!("{zero:?} {one:?} {two:?}"),
            "TaskId(0) TaskId(1) TaskId(2)"
        );
        assert_eq!(format!("{one:#?}"), "TaskId(\n    1,\n)");
        assert_eq!(
            format!("{joined:?}"),
            "TaskCompletion { id: TaskId(0), result: Ok(()), .. }"
        );
        let copy = one;
        assert_eq!(one, copy);
        assert_ne!(zero, one);
        assert!(zero < one && one < two);
        assert_eq!(two.cmp(&one), std::cmp::Ordering::Greater);
        for (id, seq) in [(zero, 0_u64), (one, 1), (two, 2)] {
            assert_eq!(hash_of(&id), hash_of(&seq), "{id:?}");
        }
        let ordered: BTreeMap<TaskId, usize> = [(two, 2), (zero, 0), (one, 1)].into();
        assert_eq!(ordered.into_values().collect::<Vec<_>>(), [0, 1, 2]);

        tasks.abort_all();
        assert_eq!(
            pool.run_until(tasks.join()),
            Err(TaskGroupError::TaskCancelled)
        );
    }

    fn panicking_factory(_: CancellationToken) -> std::future::Ready<()> {
        panic!("task factory panic")
    }

    #[test]
    fn task_ids_exhaust_only_at_u64_overflow() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let ids: Vec<_> = (0..3)
            .map(|_| tasks.spawn_local_tracked(|_| async {}).unwrap())
            .collect();
        assert_eq!(format!("{:?}", ids[2]), "TaskId(2)");

        tasks.set_next_task_seq(u64::MAX - 2);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tasks.spawn_local_tracked(panicking_factory)
        }));
        assert!(panicked.is_err());
        let last = tasks.spawn_local_tracked(|_| async {}).unwrap();
        assert_eq!(
            format!("{last:?}"),
            format!("TaskId({})", u64::MAX - 1),
            "the panicking factory consumed its id"
        );
        assert_eq!(
            tasks.spawn_local_tracked(|_| async {}),
            Err(TaskGroupError::IdentifierExhausted)
        );
        assert_eq!(
            tasks.spawn_local(|_| async {}),
            Err(TaskGroupError::IdentifierExhausted)
        );
        pool.run_until_stalled();
        let joined: Vec<_> = std::iter::from_fn(|| tasks.try_join_next())
            .map(|completion| completion.id)
            .collect();
        assert_eq!(joined.len(), 4);
        assert!(joined.contains(&last));
        pool.run_until(tasks.join()).unwrap();
    }

    #[test]
    fn cancel_on_an_unused_group_returns_true_then_false() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        assert!(!tasks.is_cancelled());
        assert!(tasks.cancel());
        assert!(tasks.is_cancelled());
        assert!(!tasks.cancel());
        assert!(!tasks.is_accepting());
        assert_eq!(pool.run_until(tasks.join()), Ok(()));
    }

    fn span_outcomes(
        observer: &crate::observability::SpanObserver,
    ) -> Vec<(
        crate::observability::OperationKind,
        Option<crate::observability::TerminalOutcome>,
        u8,
    )> {
        observer
            .snapshots()
            .into_iter()
            .map(|span| (span.kind, span.outcome, span.close_count))
            .collect()
    }

    #[test]
    fn unused_group_close_and_cancel_record_their_spans() {
        use crate::context::ContextEnvelope;
        use crate::observability::OperationKind::Cancellation;
        use crate::observability::OperationKind::Shutdown;
        use crate::observability::SpanObserver;
        use crate::observability::TerminalOutcome::Success;

        let (_, _, host) = TestHost::create();
        let observer = SpanObserver::new(16).unwrap();
        let context = ContextEnvelope::capture().with_span_observer(observer.clone());
        let mut tasks = TaskGroup::with_host(host);
        context.run(|| {
            assert!(tasks.close());
            assert!(tasks.cancel());
            assert!(!tasks.close());
            assert!(!tasks.cancel());
        });
        assert_eq!(
            span_outcomes(&observer),
            [
                (Shutdown, Some(Success), 1),
                (Cancellation, Some(Success), 1),
                (Cancellation, Some(Success), 1),
            ]
        );
    }

    #[test]
    fn spawn_on_a_closed_unused_group_fails_closed_with_its_span() {
        use crate::context::ContextEnvelope;
        use crate::observability::OperationKind::Shutdown;
        use crate::observability::SpanObserver;
        use crate::observability::TerminalOutcome;

        let (_, _, host) = TestHost::create();
        let observer = SpanObserver::new(16).unwrap();
        let context = ContextEnvelope::capture().with_span_observer(observer.clone());
        let mut tasks = TaskGroup::with_host(host);
        let drops = Rc::new(Cell::new(0));
        let probe = DropProbe {
            owner: std::thread::current().id(),
            drops: Rc::clone(&drops),
        };
        context.run(|| {
            assert!(tasks.close());
            let rejected = tasks.spawn_local(move |_| {
                let _probe = probe;
                async {}
            });
            assert_eq!(rejected, Err(TaskGroupError::Closed));
            assert_eq!(
                tasks.spawn_local_tracked(|_| async {}),
                Err(TaskGroupError::Closed)
            );
        });
        assert_eq!(drops.get(), 1, "the rejected factory is dropped");
        assert!(tasks.is_empty());
        assert_eq!(
            span_outcomes(&observer),
            [
                (Shutdown, Some(TerminalOutcome::Success), 1),
                (Shutdown, Some(TerminalOutcome::Shutdown), 1),
                (Shutdown, Some(TerminalOutcome::Shutdown), 1),
            ]
        );
    }

    #[test]
    fn cancel_before_spawn_reports_cancelled_and_rejects_spawns() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        assert!(tasks.cancel());
        assert!(tasks.is_cancelled());
        assert_eq!(tasks.spawn_local(|_| async {}), Err(TaskGroupError::Closed));
        assert_eq!(
            tasks.spawn_local_tracked(|_| async {}),
            Err(TaskGroupError::Closed)
        );
        assert!(tasks.is_empty());
        assert_eq!(pool.run_until(tasks.join()), Ok(()));
    }

    #[test]
    fn join_on_an_empty_group_returns_ok() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        assert!(!tasks.has_ready_completion());
        assert!(tasks.try_join_next().is_none());
        assert!(matches!(
            tasks.poll_join_next(&mut Context::from_waker(&noop_waker())),
            Poll::Ready(None)
        ));
        assert_eq!(pool.run_until(tasks.join()), Ok(()));
        assert_eq!(pool.run_until(tasks.join()), Ok(()));
        assert!(!tasks.is_accepting());
        assert!(!tasks.has_ready_completion());
    }

    #[test]
    fn task_group_slots_are_reused_up_to_the_live_peak() {
        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        // A failed check leaves tasks in the group; join them before the
        // failure unwinds, so the group's drop assertions cannot abort the
        // test process.
        let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut peak = 0;
            let mut check = |tasks: &TaskGroup, live: usize| {
                peak = live.max(peak);
                assert_eq!(tasks.len(), live);
                assert_eq!(tasks.task_slots(), peak, "{live} live");
            };
            check(&tasks, 0);
            for live in 1..=8 {
                tasks.spawn_local_tracked(|_| async {}).unwrap();
                check(&tasks, live);
            }
            pool.run_until_stalled();
            check(&tasks, 8);
            for live in (5..8).rev() {
                tasks.try_join_next().unwrap();
                check(&tasks, live);
            }
            for live in 6..=8 {
                tasks.spawn_local_tracked(|_| async {}).unwrap();
                check(&tasks, live);
            }
            pool.run_until_stalled();
            for live in (0..8).rev() {
                tasks.try_join_next().unwrap();
                check(&tasks, live);
            }
            for _ in 0..100 {
                tasks.spawn_local_tracked(|_| async {}).unwrap();
                check(&tasks, 1);
                pool.run_until_stalled();
                tasks.try_join_next().unwrap();
                check(&tasks, 0);
            }
            for live in 1..=3 {
                tasks.spawn_local(|_| async {}).unwrap();
                check(&tasks, live);
            }
            pool.run_until_stalled();
            tasks.reap_finished();
            check(&tasks, 0);
        }));
        let joined = pool.run_until(tasks.join());
        if let Err(payload) = checked {
            std::panic::resume_unwind(payload);
        }
        joined.unwrap();
    }

    #[test]
    fn unused_group_allocates_no_completions_or_cancellation_state() {
        let (mut pool, _, host) = TestHost::create();
        let before = crate::task_group::group_state_allocations();
        let mut unused = TaskGroup::with_host(host.clone());
        assert!(!unused.has_ready_completion());
        assert!(!unused.is_cancelled());
        assert!(unused.try_join_next().is_none());
        assert!(unused.close());
        assert_eq!(
            unused.spawn_local(|_| async {}),
            Err(TaskGroupError::Closed)
        );
        assert_eq!(pool.run_until(unused.join()), Ok(()));
        drop(unused);
        assert_eq!(
            crate::task_group::group_state_allocations(),
            before,
            "a group that never spawns allocates neither"
        );

        let mut used = TaskGroup::with_host(host);
        used.spawn_local(|_| async {}).unwrap();
        let first = (before.0 + 1, before.1 + 1);
        assert_eq!(crate::task_group::group_state_allocations(), first);
        used.spawn_local_tracked(|_| async {}).unwrap();
        assert!(used.cancel());
        assert_eq!(
            crate::task_group::group_state_allocations(),
            first,
            "later spawns and cancellation reuse them"
        );
        pool.run_until(used.join()).unwrap();
    }

    #[test]
    fn custom_host_still_receives_a_boxed_local_future() {
        let (mut pool, implementation, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        let polls = Rc::new(Cell::new(0));
        for _ in 0..3 {
            let polls = Rc::clone(&polls);
            tasks
                .spawn_local(move |_| async move { polls.set(polls.get() + 1) })
                .unwrap();
        }
        assert_eq!(
            implementation.tasks.get(),
            3,
            "the host received each task as a LocalFuture"
        );
        assert_eq!(polls.get(), 0, "the host enqueues without polling inline");
        pool.run_until_stalled();
        assert_eq!(polls.get(), 3);
        assert_eq!(implementation.tasks.get(), 0);
        pool.run_until(tasks.join()).unwrap();
    }

    /// A task group lent to one other thread and returned.
    struct Lent(TaskGroup);

    // SAFETY: `TaskGroup` is `!Send` because it holds `Rc`s and cells that one
    // thread may access at a time. The lending thread blocks in `join` while the
    // other thread holds the group, and the other thread only reaches the group's
    // owner assertion, which panics before it touches any `Rc` or cell.
    unsafe impl Send for Lent {}

    #[test]
    fn tokio_hosted_spawn_never_polls_inline_and_rejects_other_threads() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let owner = std::thread::current().id();
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let mut tasks = TaskGroup::new();
            let polls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let probe = DropProbe {
                owner,
                drops: Rc::clone(&drops),
            };
            let ready = tasks
                .spawn_local_tracked({
                    let polls = Rc::clone(&polls);
                    move |_| async move { polls.set(polls.get() + 1) }
                })
                .unwrap();
            let aborted = tasks
                .spawn_local_tracked(move |_| async move {
                    let _probe = probe;
                    pending::<()>().await;
                })
                .unwrap();
            assert_eq!(polls.get(), 0, "spawn enqueues without polling inline");
            tokio::task::yield_now().await;
            assert_eq!(polls.get(), 1);
            assert!(tasks.abort(aborted));
            assert_eq!(drops.get(), 1, "abort drops the future on the owner thread");

            let lent = Lent(tasks);
            let (lent, rejected) = std::thread::spawn(move || {
                let mut lent = lent;
                let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    lent.0.spawn_local(|_| async {})
                }));
                (lent, spawned.is_err())
            })
            .join()
            .unwrap();
            assert!(rejected, "another thread's spawn panics");
            let mut tasks = lent.0;
            let joined: Vec<_> = std::iter::from_fn(|| tasks.try_join_next())
                .map(|completion| (completion.id, completion.result))
                .collect();
            assert!(joined.contains(&(ready, Ok(()))));
            assert!(joined.contains(&(aborted, Err(TaskGroupError::TaskCancelled))));
            assert_eq!(tasks.join().await, Err(TaskGroupError::TaskCancelled));
        });
    }

    #[test]
    fn tokio_host_spawns_task_group_tasks_without_a_local_future_box() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let before = crate::host::local_future_boxes();
        tokio::task::LocalSet::new().block_on(&runtime, async {
            let mut tasks = TaskGroup::new();
            tasks.spawn_local(|_| async {}).unwrap();
            tasks.spawn_local_tracked(|_| async {}).unwrap();
            tasks.join().await.unwrap();
        });
        assert_eq!(
            crate::host::local_future_boxes(),
            before,
            "the Tokio host spawns HostedTask unboxed"
        );

        let (mut pool, _, host) = TestHost::create();
        let mut tasks = TaskGroup::with_host(host);
        tasks.spawn_local(|_| async {}).unwrap();
        assert_eq!(
            crate::host::local_future_boxes(),
            before + 1,
            "a custom host still receives a LocalFuture box"
        );
        pool.run_until(tasks.join()).unwrap();
    }

    #[test]
    fn task_state_keeps_its_per_poll_fields_ahead_of_the_body() {
        let offsets = crate::task_group::task_state_offsets();
        for (field, offset) in offsets.per_poll {
            assert!(
                offset < offsets.body,
                "{field} at {offset} sits before body at {}",
                offsets.body
            );
        }
        assert!(offsets.body <= 32, "body starts at {}", offsets.body);
    }
}
