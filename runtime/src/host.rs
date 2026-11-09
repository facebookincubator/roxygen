//! Owner-local execution, clock, and I/O fairness capabilities.

pub mod timer;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use crate::thread::current_id;

/// A future whose polling and destruction stay on its owner thread.
pub type LocalFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// Application-provided execution capabilities for one stable owner thread.
///
/// `spawn_local` must enqueue without polling inline, and must retain and drive
/// each future until completion. Polling and destruction, including executor
/// shutdown, must occur on the creating thread. The host must support wakes
/// originating from other threads. Roxygen retains task and shutdown ownership.
///
/// A wake that a task issues during its own poll should not re-poll that task
/// before the host has processed ready I/O; `yield_to_io` is the explicit form
/// of that rule.
pub trait LocalHost {
    fn spawn_local(&self, future: LocalFuture);

    /// Returns monotonic time in the same clock domain as `sleep_until`.
    fn now(&self) -> Instant;

    /// Sleeps until an absolute deadline. Dropping the future disarms its timer.
    fn sleep_until(&self, deadline: Instant) -> LocalFuture;

    /// Yields until the host has had an opportunity to process ready I/O.
    fn yield_to_io(&self) -> LocalFuture;

    /// Whether a task woken now, from outside its own poll, is polled in the
    /// host's current iteration, before the host processes further I/O. Roxygen
    /// may then apply an idle task's pending bookkeeping at the start of its
    /// next poll instead of waking it. The default, `false`, keeps every wake.
    fn runs_wakes_this_iteration(&self) -> bool {
        false
    }
}

#[cfg(test)]
thread_local! {
    static TOKIO_HOST_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static LOCAL_FUTURE_BOXES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many `Host::tokio()` calls on this thread built a new `Host` because
/// the thread's cached one was already destroyed.
#[cfg(test)]
pub(crate) fn tokio_host_fallbacks() -> usize {
    TOKIO_HOST_FALLBACKS.get()
}

/// How many futures `Host::spawn_unboxed` has boxed on this thread for a
/// custom host.
#[cfg(test)]
pub(crate) fn local_future_boxes() -> usize {
    LOCAL_FUTURE_BOXES.get()
}

/// Cloneable, owner-local access to an application's execution capabilities.
#[derive(Clone)]
pub struct Host {
    implementation: Rc<dyn LocalHost>,
    owner_thread: ThreadId,
    /// Whether `implementation` is a `TokioHost` built by `Host::tokio()`.
    tokio: bool,
}

thread_local! {
    static TOKIO_HOST: Host = Host::new_tokio();
}

impl Host {
    pub fn new(implementation: Rc<dyn LocalHost>) -> Self {
        Self {
            implementation,
            owner_thread: current_id(),
            tokio: false,
        }
    }

    fn new_tokio() -> Self {
        Self {
            implementation: Rc::new(TokioHost),
            owner_thread: current_id(),
            tokio: true,
        }
    }

    /// Uses the current Tokio runtime and LocalSet when capabilities are used.
    /// Calls on one thread share one implementation.
    pub fn tokio() -> Self {
        TOKIO_HOST.try_with(Self::clone).unwrap_or_else(|_| {
            #[cfg(test)]
            TOKIO_HOST_FALLBACKS.set(TOKIO_HOST_FALLBACKS.get() + 1);
            Self::new_tokio()
        })
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }

    /// The address of the shared implementation, which identifies it while
    /// it is alive.
    #[cfg(test)]
    pub(crate) fn implementation_id(&self) -> usize {
        Rc::as_ptr(&self.implementation).cast::<()>() as usize
    }

    pub(crate) fn spawn_local(&self, future: LocalFuture) {
        self.assert_owner();
        self.implementation.spawn_local(future);
    }

    /// `spawn_local` without the `LocalFuture` box on the Tokio host, whose
    /// `spawn_local` is `tokio::task::spawn_local`. A custom host still
    /// receives a `LocalFuture`.
    pub(crate) fn spawn_unboxed<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        if self.tokio {
            self.assert_owner();
            tokio::task::spawn_local(future);
        } else {
            #[cfg(test)]
            LOCAL_FUTURE_BOXES.set(LOCAL_FUTURE_BOXES.get() + 1);
            self.spawn_local(Box::pin(future));
        }
    }

    pub fn now(&self) -> Instant {
        self.assert_owner();
        self.implementation.now()
    }

    /// Converts a duration to an absolute deadline in this host's clock domain.
    /// Unrepresentable durations use Tokio's thirty-year far-future fallback.
    pub fn deadline_after(&self, duration: Duration) -> Instant {
        let now = self.now();
        now.checked_add(duration)
            .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365 * 30))
    }

    pub fn sleep_until(&self, deadline: Instant) -> LocalFuture {
        self.assert_owner();
        self.implementation.sleep_until(deadline)
    }

    pub fn yield_to_io(&self) -> LocalFuture {
        self.assert_owner();
        self.implementation.yield_to_io()
    }

    /// See [`LocalHost::runs_wakes_this_iteration`].
    pub fn runs_wakes_this_iteration(&self) -> bool {
        debug_assert_eq!(current_id(), self.owner_thread);
        self.implementation.runs_wakes_this_iteration()
    }

    fn assert_owner(&self) {
        assert_eq!(current_id(), self.owner_thread);
    }
}

impl Default for Host {
    fn default() -> Self {
        Self::tokio()
    }
}

impl fmt::Debug for Host {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Host")
            .field("owner_thread", &self.owner_thread)
            .finish_non_exhaustive()
    }
}

/// The standalone Tokio execution backend.
pub struct TokioHost;

impl LocalHost for TokioHost {
    fn spawn_local(&self, future: LocalFuture) {
        tokio::task::spawn_local(future);
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn sleep_until(&self, deadline: Instant) -> LocalFuture {
        Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )))
    }

    fn yield_to_io(&self) -> LocalFuture {
        Box::pin(tokio::task::yield_now())
    }
}
