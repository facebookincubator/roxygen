/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;

use crate::cancellation::CancellationToken;
use crate::wait::TimedBaton;
use crate::wait::TimedWait;
use crate::worker::WorkerHandle;
use crate::worker::WorkerId;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KeepAliveError {
    #[error("keep-alive owner no longer accepts new references")]
    OwnerReleased,
    #[error("keep-alive reference count overflow")]
    RefCountOverflow,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionHandleError {
    #[error(transparent)]
    KeepAlive(#[from] KeepAliveError),
    #[error("session owner thread {owner:?} does not match worker thread {worker:?}")]
    WrongOwnerThread { owner: ThreadId, worker: ThreadId },
    #[error("session owner is not bound to a worker")]
    UnboundOwner,
    #[error("session owner worker {owner:?} does not match handle worker {worker:?}")]
    WrongOwnerWorker { owner: WorkerId, worker: WorkerId },
}

struct KeepAliveCounters {
    owner_released: bool,
    next_waiter_id: u64,
    waiters: BTreeMap<u64, Waker>,
}

struct KeepAliveState {
    /// The references, including the owner's implicit one until it is released. Token clones
    /// and releases change it without the lock; a release that reaches zero then takes the lock
    /// to wake the waiters. Changes that depend on `owner_released` hold the lock.
    count: AtomicU64,
    counters: Mutex<KeepAliveCounters>,
}

impl KeepAliveState {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(1),
            counters: Mutex::new(KeepAliveCounters {
                owner_released: false,
                next_waiter_id: 1,
                waiters: BTreeMap::new(),
            }),
        }
    }

    fn counters(&self) -> std::sync::MutexGuard<'_, KeepAliveCounters> {
        self.counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Adds one reference unless the count would overflow, and returns whether it did.
    fn increment(&self) -> bool {
        let mut count = self.count.load(Ordering::Relaxed);
        loop {
            let Some(next) = count.checked_add(1) else {
                return false;
            };
            match self.count.compare_exchange_weak(
                count,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => count = current,
            }
        }
    }

    /// Drops one reference and returns how many remain.
    fn decrement(&self, underflow: &str) -> u64 {
        let mut count = self.count.load(Ordering::Relaxed);
        loop {
            let next = count.checked_sub(1).expect(underflow);
            match self
                .count
                .compare_exchange_weak(count, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(current) => count = current,
            }
        }
    }

    fn acquire_from_owner(&self) -> Result<(), KeepAliveError> {
        let counters = self.counters();
        if counters.owner_released {
            return Err(KeepAliveError::OwnerReleased);
        }
        if !self.increment() {
            return Err(KeepAliveError::RefCountOverflow);
        }
        Ok(())
    }

    fn clone_existing(&self) {
        debug_assert!(self.count.load(Ordering::Relaxed) > 0);
        assert!(
            self.increment(),
            "a live keep-alive reference count cannot overflow"
        );
    }

    fn release_reference(&self) {
        if self.decrement("a live keep-alive reference cannot underflow") != 0 {
            return;
        }
        // A waiter registers under the lock after it reads a nonzero count, so taking the
        // waiters under the lock after the count reached zero finds every one.
        let waiters = std::mem::take(&mut self.counters().waiters);
        for (_, waker) in waiters {
            waker.wake();
        }
    }

    /// Releases the owner's implicit reference once. Call with the lock held.
    fn release_owner_reference(&self, counters: &mut KeepAliveCounters) {
        if !counters.owner_released {
            counters.owner_released = true;
            self.decrement("the implicit keep-alive reference cannot underflow");
        }
    }

    fn release_owner(&self) -> u64 {
        let (remaining, waiters) = {
            let mut counters = self.counters();
            self.release_owner_reference(&mut counters);
            let remaining = self.count.load(Ordering::Acquire);
            let waiters = if remaining == 0 {
                std::mem::take(&mut counters.waiters)
            } else {
                BTreeMap::new()
            };
            (remaining, waiters)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
        remaining
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    fn owner_released(&self) -> bool {
        self.counters().owner_released
    }
}

/// Owner-local state whose destruction can be delayed by thread-safe tokens.
///
/// Tokens carry only liveness. Access to the value requires this `!Send`
/// owner, so dropping a token on another thread cannot touch or destroy local
/// state.
///
/// Call [`Self::finalize`] before dropping the owner. Dropping an unfinalized
/// owner panics in all build modes, unless the thread is already unwinding.
/// In that case the checks are skipped so that the original panic propagates,
/// and the value is dropped without waiting for outstanding tokens.
pub struct KeepAliveOwner<T> {
    value: Option<T>,
    state: Arc<KeepAliveState>,
    owner_thread: ThreadId,
    owner_worker: Option<WorkerId>,
    _not_send: PhantomData<Rc<()>>,
}

impl<T> KeepAliveOwner<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: Some(value),
            state: Arc::new(KeepAliveState::new()),
            owner_thread: std::thread::current().id(),
            owner_worker: None,
            _not_send: PhantomData,
        }
    }

    pub fn new_for_worker(value: T, worker: &WorkerHandle) -> Result<Self, SessionHandleError> {
        let owner_thread = std::thread::current().id();
        if owner_thread != worker.owner_thread() {
            return Err(SessionHandleError::WrongOwnerThread {
                owner: owner_thread,
                worker: worker.owner_thread(),
            });
        }
        Ok(Self {
            value: Some(value),
            state: Arc::new(KeepAliveState::new()),
            owner_thread,
            owner_worker: Some(worker.id()),
            _not_send: PhantomData,
        })
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }

    pub fn owner_worker(&self) -> Option<WorkerId> {
        self.owner_worker
    }

    pub fn num_keep_alives(&self) -> u64 {
        self.state.count()
    }

    pub fn is_owner_released(&self) -> bool {
        self.state.owner_released()
    }

    pub fn acquire_keep_alive(&self) -> Result<KeepAlivePtr<T>, KeepAliveError> {
        self.assert_owner_thread();
        self.state.acquire_from_owner()?;
        Ok(KeepAlivePtr {
            state: Some(Arc::clone(&self.state)),
            _type: PhantomData,
        })
    }

    pub fn get(&self, keep_alive: &KeepAlivePtr<T>) -> Option<&T> {
        if !self.owns(keep_alive) {
            return None;
        }
        self.value.as_ref()
    }

    pub fn get_mut(&mut self, keep_alive: &KeepAlivePtr<T>) -> Option<&mut T> {
        if !self.owns(keep_alive) {
            return None;
        }
        self.value.as_mut()
    }

    pub fn zero_refs(&self) -> ZeroRefs<'_, T> {
        self.assert_owner_thread();
        ZeroRefs {
            owner: self,
            waiter_id: None,
            completed: false,
        }
    }

    /// Releases the implicit owner reference, waits for external references,
    /// and destroys the owner-local value on this owner thread.
    pub async fn finalize(&mut self) {
        self.assert_owner_thread();
        self.zero_refs().await;
        if let Some(value) = self.value.take() {
            drop(value);
        }
    }

    #[cfg(test)]
    pub(crate) fn zero_waiter_count(&self) -> usize {
        self.state.counters().waiters.len()
    }

    fn owns(&self, keep_alive: &KeepAlivePtr<T>) -> bool {
        keep_alive
            .state
            .as_ref()
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
    }

    fn assert_owner_thread(&self) {
        debug_assert_eq!(std::thread::current().id(), self.owner_thread);
    }
}

impl<T> Drop for KeepAliveOwner<T> {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let current_thread = std::thread::current().id();
        let remaining = self.state.release_owner();
        if unwinding {
            return;
        }
        assert_eq!(
            current_thread, self.owner_thread,
            "keep-alive value must be destroyed on its owner thread"
        );
        assert_eq!(
            remaining, 0,
            "keep-alive owner must await zero_refs before destruction"
        );
        assert!(
            self.value.is_none(),
            "keep-alive owner must be finalized before destruction"
        );
    }
}

/// A thread-safe liveness token for owner-local state.
pub struct KeepAlivePtr<T> {
    state: Option<Arc<KeepAliveState>>,
    _type: PhantomData<fn() -> T>,
}

impl<T> KeepAlivePtr<T> {
    pub fn is_some(&self) -> bool {
        self.state.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_none()
    }

    pub fn reset(&mut self) {
        if let Some(state) = self.state.take() {
            state.release_reference();
        }
    }
}

impl<T> Clone for KeepAlivePtr<T> {
    fn clone(&self) -> Self {
        if let Some(state) = &self.state {
            state.clone_existing();
        }
        Self {
            state: self.state.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> Default for KeepAlivePtr<T> {
    fn default() -> Self {
        Self {
            state: None,
            _type: PhantomData,
        }
    }
}

impl<T> fmt::Debug for KeepAlivePtr<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeepAlivePtr")
            .field("is_some", &self.is_some())
            .finish()
    }
}

impl<T> Drop for KeepAlivePtr<T> {
    fn drop(&mut self) {
        self.reset();
    }
}

pub struct ZeroRefs<'a, T> {
    owner: &'a KeepAliveOwner<T>,
    waiter_id: Option<u64>,
    completed: bool,
}

impl<T> Future for ZeroRefs<'_, T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            return Poll::Ready(());
        }

        let state = Arc::clone(&self.owner.state);
        let mut counters = state.counters();
        state.release_owner_reference(&mut counters);
        if state.count() == 0 {
            if let Some(waiter_id) = self.waiter_id.take() {
                counters.waiters.remove(&waiter_id);
            }
            drop(counters);
            self.completed = true;
            return Poll::Ready(());
        }

        if let Some(waiter_id) = self.waiter_id {
            let waiter = counters
                .waiters
                .get_mut(&waiter_id)
                .expect("registered zero-ref waiter must remain installed");
            if !waiter.will_wake(context.waker()) {
                *waiter = context.waker().clone();
            }
        } else {
            let waiter_id = counters.next_waiter_id;
            counters.next_waiter_id = counters.next_waiter_id.wrapping_add(1);
            counters.waiters.insert(waiter_id, context.waker().clone());
            drop(counters);
            self.waiter_id = Some(waiter_id);
        }
        Poll::Pending
    }
}

impl<T> Drop for ZeroRefs<'_, T> {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take() {
            self.owner.state.counters().waiters.remove(&waiter_id);
        }
    }
}

/// A typed, sendable session token bound permanently to one worker.
pub struct CoroSessionHandle<T> {
    worker: Option<WorkerHandle>,
    keep_alive: KeepAlivePtr<T>,
}

impl<T> CoroSessionHandle<T> {
    pub fn new(
        owner: &KeepAliveOwner<T>,
        worker: WorkerHandle,
    ) -> Result<Self, SessionHandleError> {
        if owner.owner_thread() != worker.owner_thread() {
            return Err(SessionHandleError::WrongOwnerThread {
                owner: owner.owner_thread(),
                worker: worker.owner_thread(),
            });
        }
        let Some(owner_worker) = owner.owner_worker() else {
            return Err(SessionHandleError::UnboundOwner);
        };
        if owner_worker != worker.id() {
            return Err(SessionHandleError::WrongOwnerWorker {
                owner: owner_worker,
                worker: worker.id(),
            });
        }
        Ok(Self {
            worker: Some(worker),
            keep_alive: owner.acquire_keep_alive()?,
        })
    }

    pub fn get<'a>(&self, owner: &'a KeepAliveOwner<T>) -> Option<&'a T> {
        owner.get(&self.keep_alive)
    }

    pub fn get_mut<'a>(&mut self, owner: &'a mut KeepAliveOwner<T>) -> Option<&'a mut T> {
        owner.get_mut(&self.keep_alive)
    }

    pub fn worker(&self) -> Option<&WorkerHandle> {
        self.worker.as_ref()
    }

    pub fn is_some(&self) -> bool {
        self.keep_alive.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.keep_alive.is_empty()
    }

    pub fn reset(&mut self) {
        self.keep_alive.reset();
        self.worker = None;
    }
}

impl<T> Clone for CoroSessionHandle<T> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            keep_alive: self.keep_alive.clone(),
        }
    }
}

impl<T> Default for CoroSessionHandle<T> {
    fn default() -> Self {
        Self {
            worker: None,
            keep_alive: KeepAlivePtr::default(),
        }
    }
}

/// Owner-local reference count with a sticky zero notification.
pub struct RefCount {
    count: usize,
    zero: TimedBaton,
}

impl RefCount {
    pub fn new(count: usize) -> Self {
        let zero = TimedBaton::new(None);
        if count == 0 {
            zero.signal();
        }
        Self { count, zero }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn inc_ref(&mut self) {
        let was_zero = self.count == 0;
        self.count = self.count.wrapping_add(1);
        if was_zero {
            self.zero.reset();
        }
    }

    pub fn dec_ref(&mut self) -> Result<(), RefCountUnderflow> {
        if self.count == 0 {
            return Err(RefCountUnderflow);
        }
        self.count -= 1;
        if self.count == 0 {
            self.zero.signal();
        }
        Ok(())
    }

    pub fn zero_refs(&self) -> TimedWait {
        self.zero.wait(CancellationToken::never())
    }
}

impl Default for RefCount {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("reference count is already zero")]
pub struct RefCountUnderflow;

#[cfg(test)]
mod owner_drop_tests {
    use std::cell::Cell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::panic::panic_any;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::Context;
    use std::thread::ThreadId;

    use futures::task::noop_waker;

    use super::KeepAliveOwner;

    struct ValueProbe {
        drops: Rc<Cell<usize>>,
        owner_thread: ThreadId,
    }

    impl Drop for ValueProbe {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner_thread);
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct FailingParent {
        owner: KeepAliveOwner<ValueProbe>,
        original: Arc<()>,
    }

    impl Drop for FailingParent {
        fn drop(&mut self) {
            assert_eq!(self.owner.owner_thread(), std::thread::current().id());
            panic_any(Arc::clone(&self.original));
        }
    }

    #[test]
    fn unwinding_owner_drop_preserves_first_panic_and_tokens() {
        for (external_count, already_released) in [(2, false), (2, true), (0, false)] {
            let drops = Rc::new(Cell::new(0));
            let original = Arc::new(());
            let parent = FailingParent {
                owner: KeepAliveOwner::new(ValueProbe {
                    drops: Rc::clone(&drops),
                    owner_thread: std::thread::current().id(),
                }),
                original: Arc::clone(&original),
            };
            let tokens: Vec<_> = (0..external_count)
                .map(|_| parent.owner.acquire_keep_alive().expect("open owner"))
                .collect();
            let state = Arc::clone(&parent.owner.state);
            let weak = Arc::downgrade(&state);
            if already_released {
                let mut wait = Box::pin(parent.owner.zero_refs());
                let waker = noop_waker();
                let mut context = Context::from_waker(&waker);
                assert!(std::future::Future::poll(wait.as_mut(), &mut context).is_pending());
                drop(wait);
                assert!(state.counters().waiters.is_empty());
            }
            let payload = catch_unwind(AssertUnwindSafe(|| drop(parent)))
                .expect_err("the containing destructor must fail");
            let retained = payload.downcast::<Arc<()>>().expect("original typed panic");
            assert!(Arc::ptr_eq(&retained, &original));
            assert_eq!(drops.get(), 1);
            assert!(state.owner_released());
            assert_eq!(state.count(), external_count);
            assert!(state.acquire_from_owner().is_err());
            std::thread::spawn(move || {
                for mut token in tokens {
                    let mut cloned = token.clone();
                    token.reset();
                    cloned.reset();
                    assert!(token.is_empty());
                    assert!(cloned.is_empty());
                }
            })
            .join()
            .expect("state-only token retirement must not panic");
            assert_eq!(state.count(), 0);
            assert_eq!(drops.get(), 1);
            drop(state);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn normal_unfinalized_owner_without_tokens_still_panics() {
        let drops = Rc::new(Cell::new(0));
        let owner = KeepAliveOwner::new(ValueProbe {
            drops: Rc::clone(&drops),
            owner_thread: std::thread::current().id(),
        });
        let state = Arc::clone(&owner.state);
        let result = catch_unwind(AssertUnwindSafe(|| drop(owner)));
        assert!(result.is_err());
        assert!(state.owner_released());
        assert_eq!(state.count(), 0);
        assert_eq!(drops.get(), 1);
    }
}

#[cfg(test)]
mod count_tests {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use super::KeepAliveOwner;

    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn token_clones_and_releases_above_zero_skip_the_lock() {
        let mut owner = KeepAliveOwner::new(());
        let token = owner.acquire_keep_alive().expect("open owner");
        let held = owner.state.counters();
        let (done, finished) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            drop(token.clone());
            done.send(()).expect("the test waits");
            token
        });
        let unblocked = finished.recv_timeout(Duration::from_secs(5)).is_ok();
        drop(held);
        let token = worker.join().expect("the worker completes");
        assert!(
            unblocked,
            "a clone and a release that leaves references do not wait for the lock"
        );
        assert_eq!(owner.num_keep_alives(), 2);
        drop(token);
        futures::executor::block_on(owner.finalize());
    }

    #[test]
    fn the_last_release_on_another_thread_wakes_the_zero_waiter() {
        for _ in 0..200 {
            let mut owner = KeepAliveOwner::new(());
            let token = owner.acquire_keep_alive().expect("open owner");
            let wakes = Arc::new(Wakes::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut context = Context::from_waker(&waker);
            {
                let mut wait = pin!(owner.zero_refs());
                let releasing = std::thread::spawn(move || drop(token));
                let first = wait.as_mut().poll(&mut context);
                releasing.join().expect("the release completes");
                if first.is_pending() {
                    assert_eq!(
                        wakes.0.load(Ordering::SeqCst),
                        1,
                        "the release that reached zero woke the registered waiter"
                    );
                }
                assert!(wait.as_mut().poll(&mut context).is_ready());
            }
            assert_eq!(owner.num_keep_alives(), 0);
            futures::executor::block_on(owner.finalize());
        }
    }
}
