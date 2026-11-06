use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use crate::cancellation::CancellationRegistration;
use crate::cancellation::CancellationToken;
use crate::host::Host;
use crate::host::LocalFuture;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitStatus {
    NotReady,
    Signalled,
    TimedOut,
    Cancelled,
}

#[derive(Debug)]
struct WaiterRegistration {
    id: u64,
    waker: Waker,
}

#[derive(Debug, Default)]
struct TimeoutRegistration {
    deadline: Option<Instant>,
    generation: u64,
}

impl TimeoutRegistration {
    fn arm(&mut self, deadline: Instant) {
        self.generation = self.generation.wrapping_add(1);
        self.deadline = Some(deadline);
    }

    fn disarm(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.deadline = None;
    }

    fn is_armed(&self) -> bool {
        self.deadline.is_some()
    }
}

fn enabled_timeout(timeout: Duration) -> Option<Duration> {
    (!timeout.is_zero()).then_some(timeout)
}

fn wake_waiter(waiter: &Option<WaiterRegistration>) -> Option<Waker> {
    waiter.as_ref().map(|waiter| waiter.waker.clone())
}

#[derive(Debug)]
struct TimedState {
    status: WaitStatus,
    timeout: Option<Duration>,
    timeout_registration: TimeoutRegistration,
    waiter: Option<WaiterRegistration>,
    next_waiter_id: u64,
    destroyed: bool,
}

/// A single-waiter, owner-thread timed baton.
///
/// The baton is deliberately `!Send`: cancellation may be requested from any
/// thread, but status mutation and completion happen only when its owner polls
/// the wait future.
#[derive(Debug)]
pub struct TimedBaton {
    host: Host,
    state: Rc<RefCell<TimedState>>,
}

impl TimedBaton {
    pub fn new(timeout: Option<Duration>) -> Self {
        Self::with_host(timeout, Host::tokio())
    }

    pub fn with_host(timeout: Option<Duration>, host: Host) -> Self {
        Self {
            host,
            state: Rc::new(RefCell::new(TimedState {
                status: WaitStatus::NotReady,
                timeout: timeout.and_then(enabled_timeout),
                timeout_registration: TimeoutRegistration::default(),
                waiter: None,
                next_waiter_id: 1,
                destroyed: false,
            })),
        }
    }

    pub fn wait(&self, cancellation: CancellationToken) -> TimedWait {
        TimedWait {
            host: self.host.clone(),
            state: Rc::clone(&self.state),
            cancellation,
            cancellation_registration: None,
            cancellation_waker_synchronized: false,
            waiter_id: None,
            timer_generation: 0,
            sleep: None,
            completed: false,
        }
    }

    pub fn signal(&self) {
        Self::signal_status(&self.state, WaitStatus::Signalled);
    }

    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        assert!(
            state.waiter.is_none(),
            "cannot reset with a registered waiter"
        );
        assert!(
            !state.timeout_registration.is_armed(),
            "cannot reset with a registered timeout"
        );
        state.status = WaitStatus::NotReady;
    }

    pub fn set_timeout(&self, timeout: Option<Duration>) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.timeout = timeout.and_then(enabled_timeout);
            let timeout = state.timeout;
            if state.waiter.is_some() {
                if let Some(timeout) = timeout {
                    state
                        .timeout_registration
                        .arm(self.host.deadline_after(timeout));
                } else {
                    state.timeout_registration.disarm();
                }
            }
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    pub fn status(&self) -> WaitStatus {
        self.state.borrow().status
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.state.borrow().timeout
    }

    pub fn timeout_is_armed(&self) -> bool {
        self.state.borrow().timeout_registration.is_armed()
    }

    fn signal_status(state: &Rc<RefCell<TimedState>>, status: WaitStatus) {
        let wake = {
            let mut state = state.borrow_mut();
            if state.status == status {
                return;
            }
            state.timeout_registration.disarm();
            if matches!(state.status, WaitStatus::NotReady | WaitStatus::Signalled) {
                state.status = status;
            }
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

impl Default for TimedBaton {
    fn default() -> Self {
        Self::new(Some(Duration::from_secs(5)))
    }
}

impl Drop for TimedBaton {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.destroyed = true;
            state.timeout_registration.disarm();
            if state.status == WaitStatus::NotReady {
                state.status = WaitStatus::Cancelled;
            }
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

pub struct TimedWait {
    host: Host,
    state: Rc<RefCell<TimedState>>,
    cancellation: CancellationToken,
    cancellation_registration: Option<CancellationRegistration>,
    cancellation_waker_synchronized: bool,
    waiter_id: Option<u64>,
    timer_generation: u64,
    sleep: Option<LocalFuture>,
    completed: bool,
}

impl fmt::Debug for TimedWait {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimedWait")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl TimedWait {
    fn register_waiter(&mut self, waker: &Waker) -> bool {
        let mut state = self.state.borrow_mut();
        if let Some(id) = self.waiter_id {
            let waiter = state
                .waiter
                .as_mut()
                .expect("registered waiter must remain installed");
            assert_eq!(waiter.id, id, "only one waiter is supported");
            let unchanged = waiter.waker.will_wake(waker);
            if !unchanged {
                waiter.waker = waker.clone();
            }
            return unchanged;
        }

        assert!(state.waiter.is_none(), "only one waiter is supported");
        let id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.waiter = Some(WaiterRegistration {
            id,
            waker: waker.clone(),
        });
        self.waiter_id = Some(id);
        if state.status == WaitStatus::NotReady {
            if let Some(timeout) = state.timeout {
                state
                    .timeout_registration
                    .arm(self.host.deadline_after(timeout));
            }
        }
        false
    }

    fn finish(&mut self) -> bool {
        self.cancellation_waker_synchronized = false;
        let cancellation_requested = self.cancellation_registration.take().map_or_else(
            || self.cancellation.is_cancellation_requested(),
            CancellationRegistration::unregister,
        );
        if let Some(id) = self.waiter_id.take() {
            let mut state = self.state.borrow_mut();
            if state.waiter.as_ref().is_some_and(|waiter| waiter.id == id) {
                state.waiter = None;
                state.timeout_registration.disarm();
            }
        }
        self.sleep = None;
        self.completed = true;
        cancellation_requested
    }

    fn synchronize_timer(&mut self) {
        let (generation, deadline) = {
            let state = self.state.borrow();
            (
                state.timeout_registration.generation,
                state.timeout_registration.deadline,
            )
        };
        if self.timer_generation != generation {
            self.timer_generation = generation;
            self.sleep = deadline.map(|deadline| self.host.sleep_until(deadline));
        }
    }
}

impl Future for TimedWait {
    type Output = WaitStatus;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.completed, "a completed wait cannot be polled again");

        if this.cancellation.is_cancellation_requested() && this.waiter_id.is_none() {
            TimedBaton::signal_status(&this.state, WaitStatus::Cancelled);
            this.finish();
            return Poll::Ready(WaitStatus::Cancelled);
        }

        let synchronized = this.cancellation_waker_synchronized;
        // A callback panic must leave the retained wait unsynchronized.
        this.cancellation_waker_synchronized = false;
        let unchanged = this.register_waiter(cx.waker());
        let cancellation_requested = if synchronized && unchanged {
            false
        } else {
            this.cancellation
                .register(&mut this.cancellation_registration, cx.waker())
        };
        this.cancellation_waker_synchronized = true;
        if cancellation_requested || this.cancellation.is_cancellation_requested() {
            TimedBaton::signal_status(&this.state, WaitStatus::Cancelled);
        }

        let ready = {
            let state = this.state.borrow();
            state.destroyed || state.status != WaitStatus::NotReady
        };
        if ready {
            if this.finish() {
                TimedBaton::signal_status(&this.state, WaitStatus::Cancelled);
            }
            let state = this.state.borrow();
            return Poll::Ready(if state.destroyed {
                WaitStatus::Cancelled
            } else {
                state.status
            });
        }

        this.synchronize_timer();
        if let Some(sleep) = &mut this.sleep {
            if sleep.as_mut().poll(cx).is_ready() {
                {
                    let mut state = this.state.borrow_mut();
                    if state.timeout_registration.generation == this.timer_generation
                        && state.timeout_registration.is_armed()
                    {
                        state.timeout_registration.disarm();
                        state.status = WaitStatus::TimedOut;
                    }
                }
                cx.waker().wake_by_ref();
            }
        }
        Poll::Pending
    }
}

impl Drop for TimedWait {
    fn drop(&mut self) {
        if !self.completed {
            let was_registered = self.waiter_id.is_some();
            if self.finish() && was_registered {
                TimedBaton::signal_status(&self.state, WaitStatus::Cancelled);
            }
        }
    }
}

#[derive(Debug)]
struct CancellableState {
    posted: bool,
    waiter_outcome: Option<WaitStatus>,
    active_cancellation: Option<CancellationToken>,
    timeout_registration: TimeoutRegistration,
    waiter: Option<WaiterRegistration>,
    next_waiter_id: u64,
    destroyed: bool,
}

#[derive(Debug)]
pub struct CancellableBaton {
    host: Host,
    state: Rc<RefCell<CancellableState>>,
}

impl CancellableBaton {
    pub fn new() -> Self {
        Self::with_host(Host::tokio())
    }

    pub fn with_host(host: Host) -> Self {
        Self {
            host,
            state: Rc::new(RefCell::new(CancellableState {
                posted: false,
                waiter_outcome: None,
                active_cancellation: None,
                timeout_registration: TimeoutRegistration::default(),
                waiter: None,
                next_waiter_id: 1,
                destroyed: false,
            })),
        }
    }

    pub fn wait(&self, cancellation: CancellationToken) -> CancellableWait {
        self.timed_wait(Duration::ZERO, cancellation)
    }

    pub fn timed_wait(
        &self,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> CancellableWait {
        CancellableWait {
            host: self.host.clone(),
            state: Rc::clone(&self.state),
            timeout: enabled_timeout(timeout),
            cancellation,
            cancellation_registration: None,
            cancellation_waker_synchronized: false,
            waiter_id: None,
            timer_generation: 0,
            sleep: None,
            completed: false,
        }
    }

    pub fn signal(&self) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.posted = true;
            if state.waiter.is_some() && state.waiter_outcome.is_none() {
                state.waiter_outcome = Some(WaitStatus::Signalled);
            }
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    pub fn reset(&self) {
        let mut state = self.state.borrow_mut();
        assert!(
            state.waiter.is_none(),
            "cannot reset with a registered waiter"
        );
        assert!(
            !state.timeout_registration.is_armed(),
            "cannot reset with a registered timeout"
        );
        state.posted = false;
        state.waiter_outcome = None;
        state.active_cancellation = None;
    }

    pub fn ready(&self) -> bool {
        let state = self.state.borrow();
        state.posted
            || state
                .active_cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancellation_requested)
    }

    fn detach_timeout(&self) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.timeout_registration.disarm();
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    #[cfg(test)]
    fn timeout_is_armed_for_test(&self) -> bool {
        self.state.borrow().timeout_registration.is_armed()
    }

    #[cfg(test)]
    fn has_waiter_for_test(&self) -> bool {
        self.state.borrow().waiter.is_some()
    }

    #[cfg(test)]
    pub(crate) fn waiter_outcome_for_test(&self) -> Option<WaitStatus> {
        self.state.borrow().waiter_outcome
    }
}

impl Default for CancellableBaton {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CancellableBaton {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.destroyed = true;
            state.posted = true;
            state.timeout_registration.disarm();
            wake_waiter(&state.waiter)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

pub struct CancellableWait {
    host: Host,
    state: Rc<RefCell<CancellableState>>,
    timeout: Option<Duration>,
    cancellation: CancellationToken,
    cancellation_registration: Option<CancellationRegistration>,
    cancellation_waker_synchronized: bool,
    waiter_id: Option<u64>,
    timer_generation: u64,
    sleep: Option<LocalFuture>,
    completed: bool,
}

impl fmt::Debug for CancellableWait {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellableWait")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl CancellableWait {
    fn register_waiter(&mut self, waker: &Waker) -> bool {
        let mut state = self.state.borrow_mut();
        if let Some(id) = self.waiter_id {
            let waiter = state
                .waiter
                .as_mut()
                .expect("registered waiter must remain installed");
            assert_eq!(waiter.id, id, "only one waiter is supported");
            let unchanged = waiter.waker.will_wake(waker);
            if !unchanged {
                waiter.waker = waker.clone();
            }
            return unchanged;
        }

        assert!(state.waiter.is_none(), "only one waiter is supported");
        let id = state.next_waiter_id;
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
        state.waiter = Some(WaiterRegistration {
            id,
            waker: waker.clone(),
        });
        self.waiter_id = Some(id);
        state.waiter_outcome = None;
        state.active_cancellation = Some(self.cancellation.clone());
        if let Some(timeout) = self.timeout {
            state
                .timeout_registration
                .arm(self.host.deadline_after(timeout));
        }
        false
    }

    fn finish(&mut self) -> bool {
        self.cancellation_waker_synchronized = false;
        let cancellation_requested = self.cancellation_registration.take().map_or_else(
            || self.cancellation.is_cancellation_requested(),
            CancellationRegistration::unregister,
        );
        if let Some(id) = self.waiter_id.take() {
            let mut state = self.state.borrow_mut();
            if state.waiter.as_ref().is_some_and(|waiter| waiter.id == id) {
                if cancellation_requested {
                    state.posted = true;
                }
                state.waiter = None;
                state.waiter_outcome = None;
                state.active_cancellation = None;
                state.timeout_registration.disarm();
            }
        }
        self.sleep = None;
        self.completed = true;
        cancellation_requested
    }

    fn synchronize_timer(&mut self) {
        let (generation, deadline) = {
            let state = self.state.borrow();
            (
                state.timeout_registration.generation,
                state.timeout_registration.deadline,
            )
        };
        if self.timer_generation != generation {
            self.timer_generation = generation;
            self.sleep = deadline.map(|deadline| self.host.sleep_until(deadline));
        }
    }
}

impl Future for CancellableWait {
    type Output = WaitStatus;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        assert!(!this.completed, "a completed wait cannot be polled again");

        if this.cancellation.is_cancellation_requested() && this.waiter_id.is_none() {
            let mut state = this.state.borrow_mut();
            state.posted = true;
            drop(state);
            this.finish();
            return Poll::Ready(WaitStatus::Cancelled);
        }

        let synchronized = this.cancellation_waker_synchronized;
        // A callback panic must leave the retained wait unsynchronized.
        this.cancellation_waker_synchronized = false;
        let unchanged = this.register_waiter(cx.waker());
        let cancellation_requested = if synchronized && unchanged {
            false
        } else {
            this.cancellation
                .register(&mut this.cancellation_registration, cx.waker())
        };
        this.cancellation_waker_synchronized = true;
        if cancellation_requested || this.cancellation.is_cancellation_requested() {
            {
                let mut state = this.state.borrow_mut();
                state.posted = true;
            }
            this.finish();
            return Poll::Ready(WaitStatus::Cancelled);
        }

        let result = {
            let state = this.state.borrow();
            if state.destroyed {
                Some(WaitStatus::Cancelled)
            } else if state.posted {
                Some(state.waiter_outcome.unwrap_or(WaitStatus::Signalled))
            } else {
                None
            }
        };
        if let Some(result) = result {
            return Poll::Ready(if this.finish() {
                WaitStatus::Cancelled
            } else {
                result
            });
        }

        this.synchronize_timer();
        if let Some(sleep) = &mut this.sleep {
            if sleep.as_mut().poll(cx).is_ready() {
                {
                    let mut state = this.state.borrow_mut();
                    if state.timeout_registration.generation == this.timer_generation
                        && state.timeout_registration.is_armed()
                    {
                        state.timeout_registration.disarm();
                        state.posted = true;
                        if state.waiter_outcome.is_none() {
                            state.waiter_outcome = Some(WaitStatus::TimedOut);
                        }
                    }
                }
                cx.waker().wake_by_ref();
            }
        }
        Poll::Pending
    }
}

impl Drop for CancellableWait {
    fn drop(&mut self) {
        if !self.completed {
            self.finish();
        }
    }
}

#[derive(Debug, Default)]
pub struct DetachableCancellableBaton {
    baton: CancellableBaton,
}

impl DetachableCancellableBaton {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_host(host: Host) -> Self {
        Self {
            baton: CancellableBaton::with_host(host),
        }
    }

    pub fn wait(&self, cancellation: CancellationToken) -> CancellableWait {
        self.baton.wait(cancellation)
    }

    pub fn timed_wait(
        &self,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> CancellableWait {
        self.baton.timed_wait(timeout, cancellation)
    }

    pub fn signal(&self) {
        self.baton.signal();
    }

    pub fn reset(&self) {
        self.baton.reset();
    }

    pub fn ready(&self) -> bool {
        self.baton.ready()
    }

    pub fn detach(&self) {
        self.baton.detach_timeout();
    }

    #[cfg(test)]
    pub(crate) fn timeout_is_armed_for_test(&self) -> bool {
        self.baton.timeout_is_armed_for_test()
    }

    #[cfg(test)]
    pub(crate) fn has_waiter_for_test(&self) -> bool {
        self.baton.has_waiter_for_test()
    }
}

#[derive(Debug)]
pub struct TimedCancellableBaton {
    timeout: Duration,
    baton: DetachableCancellableBaton,
}

impl TimedCancellableBaton {
    pub fn new(timeout: Duration) -> Self {
        Self::with_host(timeout, Host::tokio())
    }

    pub fn with_host(timeout: Duration, host: Host) -> Self {
        Self {
            timeout,
            baton: DetachableCancellableBaton::with_host(host),
        }
    }

    pub fn wait(&self, cancellation: CancellationToken) -> CancellableWait {
        self.baton.timed_wait(self.timeout, cancellation)
    }

    pub fn signal(&self) {
        self.baton.signal();
    }

    pub fn reset(&self) {
        self.baton.reset();
    }

    pub fn detach(&self) {
        self.baton.detach();
    }
}
