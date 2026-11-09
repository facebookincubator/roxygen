use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::JoinHandle;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle as LocalJoinHandle;
use tokio::task::LocalSet;
use tokio::time::Sleep;

use crate::driver::WorkerDriver;
use crate::host::Host;
use crate::host::timer::WheelTimer;
use crate::host::timer::WheelWakeup;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupError;
use crate::task_group::begin_forced_teardown;
use crate::worker::PanicPayload;
use crate::worker::QuiesceDeadline;
use crate::worker::WorkerCreateError;
use crate::worker::WorkerHandle;
use crate::worker::WorkerState;
use crate::worker::retain_first_panic;

struct ExitCallback<F>
where
    F: FnOnce(),
{
    callback: Option<F>,
}

impl<F> ExitCallback<F>
where
    F: FnOnce(),
{
    fn new(callback: F) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

impl<F> Drop for ExitCallback<F>
where
    F: FnOnce(),
{
    fn drop(&mut self) {
        let Some(callback) = self.callback.take() else {
            return;
        };
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ManagedWorkerError {
    #[error(transparent)]
    Worker(#[from] WorkerCreateError),
    #[error("failed to spawn worker thread: {0}")]
    ThreadSpawn(String),
    #[error("failed to initialize worker runtime: {0}")]
    RuntimeInitialization(String),
    #[error("worker thread exited before initialization")]
    StartupChannelClosed,
    #[error("worker owner thread panicked")]
    OwnerThreadPanicked,
    #[error("worker exited without publishing cleanup completion")]
    CompletionChannelClosed,
    #[error("worker shutdown notification panicked")]
    ShutdownNotificationPanicked,
    #[error("worker task group failed: {0}")]
    TaskGroup(#[from] TaskGroupError),
    /// Roxygen-only (`DIV-043`): the worker's teardown deadline passed before its
    /// driver finished, so its remaining tasks, unfinished cleanup included, were
    /// dropped.
    #[error("worker teardown passed its deadline")]
    TeardownDeadlineExceeded,
}

/// The longest single timer sleep; a supervisor re-arms past it.
const LONGEST_SLEEP: Duration = Duration::from_secs(86_400);

/// Name Tokio gives blocking-pool threads by default (`Builder::thread_name`).
const DEFAULT_BLOCKING_THREAD_NAME: &str = "tokio-runtime-worker";

/// Name of the thread that joins a `HostedWorker`'s owner thread for a bounded
/// join (`DIV-043`).
const JOINER_THREAD_NAME: &str = "roxygen-join";

/// A teardown deadline that [`HostedWorker::shutdown_by`] sets, or moves earlier,
/// at any time, read by the owner thread's supervisor and runtime teardown, and
/// the count of the runtime's live blocking-pool threads that teardown waits for
/// (`DIV-043`).
#[derive(Default)]
struct TeardownDeadline {
    state: Mutex<TeardownState>,
    /// Notified when the deadline is set or moved earlier, and when the last live
    /// blocking-pool thread exits.
    changed: Condvar,
    /// Blocking-pool threads Tokio has spawned that have not finished their stop
    /// hook. A thread counts from when Tokio names it, under the pool's lock and
    /// before the thread exists, so the count is complete once the pool is shut
    /// down, including threads that have not started running yet.
    blocking_threads: AtomicUsize,
}

#[derive(Default)]
struct TeardownState {
    deadline: Option<Instant>,
    /// Woken when the deadline is set or moved earlier.
    supervisor: Option<Waker>,
}

impl TeardownDeadline {
    fn lock(&self) -> MutexGuard<'_, TeardownState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_no_later_than(&self, deadline: Instant) {
        let supervisor = {
            let mut state = self.lock();
            if state.deadline.is_some_and(|current| current <= deadline) {
                return;
            }
            state.deadline = Some(deadline);
            state.supervisor.take()
        };
        self.changed.notify_all();
        if let Some(supervisor) = supervisor {
            supervisor.wake();
        }
    }

    fn deadline(&self) -> Option<Instant> {
        self.lock().deadline
    }

    /// The deadline, with `waker` registered to learn of a new or earlier one.
    fn poll_deadline(&self, waker: &Waker) -> Option<Instant> {
        let mut state = self.lock();
        match &mut state.supervisor {
            Some(registered) if registered.will_wake(waker) => {}
            slot => *slot = Some(waker.clone()),
        }
        state.deadline
    }

    /// Counts a blocking-pool thread Tokio is about to spawn.
    fn blocking_thread_spawning(&self) {
        self.blocking_threads.fetch_add(1, Ordering::SeqCst);
    }

    /// Counts a blocking-pool thread as exited, after its stop hook, or when its
    /// start hook panicked, which ends the thread without its stop hook.
    fn blocking_thread_exited(&self) {
        if self.blocking_threads.fetch_sub(1, Ordering::SeqCst) == 1 {
            // Taking the lock orders this after a waiter's check of the count, so a
            // waiter that saw a live thread is already waiting for this notification.
            drop(self.lock());
            self.changed.notify_all();
        }
    }

    /// Waits until every blocking-pool thread has exited, or until the deadline,
    /// following it as it moves earlier.
    fn wait_for_blocking_threads(&self) {
        let mut state = self.lock();
        while self.blocking_threads.load(Ordering::SeqCst) != 0 {
            state = match state.deadline {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return;
                    }
                    self.changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
                None => self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner),
            };
        }
    }
}

/// A supervisor's timer for its [`TeardownDeadline`].
#[derive(Default)]
struct TeardownTimer(Option<(Instant, Pin<Box<Sleep>>)>);

impl TeardownTimer {
    /// Whether a deadline that has not passed yet is armed.
    fn is_armed(&self) -> bool {
        self.0.is_some()
    }

    /// Whether the deadline has passed; otherwise arranges for `context` to be
    /// woken when it passes or when one is set or moved earlier.
    fn poll_passed(&mut self, teardown: &TeardownDeadline, context: &mut Context<'_>) -> bool {
        let Some(deadline) = teardown.poll_deadline(context.waker()) else {
            return false;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        if self.0.as_ref().is_none_or(|(armed, _)| *armed != deadline) {
            self.0 = Some((
                deadline,
                Box::pin(tokio::time::sleep(remaining.min(LONGEST_SLEEP))),
            ));
        }
        if let Some((_, sleep)) = self.0.as_mut()
            && sleep.as_mut().poll(context).is_ready()
        {
            self.0 = None;
            context.waker().wake_by_ref();
        }
        false
    }
}

/// A worker's runtime. Without a teardown deadline, dropping it drops the
/// runtime here, which drops its tasks and then waits for blocking-pool work.
/// With one, it shuts the blocking pool down without waiting and drops the
/// runtime's tasks here, inside the runtime's context, as `shutdown_background`
/// does, then waits for the blocking-pool threads to exit only until the
/// deadline, following it as it moves earlier, and leaves any still running to
/// finish on their own (`DIV-043`).
struct OwnedRuntime {
    runtime: Option<Runtime>,
    teardown: Arc<TeardownDeadline>,
}

impl OwnedRuntime {
    fn get(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("should hold the runtime until it is dropped")
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if self.teardown.deadline().is_none() {
            drop(runtime);
            return;
        }
        // Shuts the blocking pool down, then drops every task on this thread inside
        // the runtime's context, without waiting for blocking-pool threads.
        let dropped = catch_unwind(AssertUnwindSafe(move || runtime.shutdown_background()));
        self.teardown.wait_for_blocking_threads();
        if let Err(payload) = dropped {
            if std::thread::panicking() {
                crate::panic::discard_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

struct ManagedDriver {
    driver: WorkerDriver,
    failure: Option<PanicPayload>,
}

impl Drop for ManagedDriver {
    fn drop(&mut self) {
        // Destroyed unfinished (past a teardown deadline): the panic it caught is
        // still its task's.
        let Some(payload) = self.failure.take() else {
            return;
        };
        if std::thread::panicking() {
            crate::panic::discard_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

impl Future for ManagedDriver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        match catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut self.driver).poll(context)
        })) {
            Ok(Poll::Ready(())) => {
                if let Some(payload) = self.failure.take() {
                    resume_unwind(payload);
                }
                Poll::Ready(())
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                if self.failure.is_none() {
                    self.failure = Some(payload);
                }
                self.driver.handle().begin_shutdown();
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// What a [`WakeGate`]'s waker records: that it fired, and the waker of the
/// task that polls the gate, which it wakes in turn.
struct GateWake {
    woken: AtomicBool,
    parent: Mutex<Option<Waker>>,
}

impl Wake for GateWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        let parent = self
            .parent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}

/// Tells a worker supervisor whether its own waker fired since it last ran.
/// `LocalSet::run_until` polls the future it drives on every wake of any local
/// task, and a supervisor has work only when one of its inputs (quiescence,
/// outstanding work, its deadline timers, its teardown deadline, its task group
/// join) wakes it.
pub(crate) struct WakeGate {
    wake: Arc<GateWake>,
    waker: Waker,
    /// The parent waker that `wake` holds. Only `poll_woken` replaces it, so
    /// this copy compares it without the lock.
    parent: Option<Waker>,
}

impl WakeGate {
    pub(crate) fn new() -> Self {
        let wake = Arc::new(GateWake {
            woken: AtomicBool::new(true),
            parent: Mutex::new(None),
        });
        Self {
            waker: Waker::from(Arc::clone(&wake)),
            wake,
            parent: None,
        }
    }

    /// The waker that the supervisor registers with its inputs.
    pub(crate) fn waker(&self) -> &Waker {
        &self.waker
    }

    /// Makes `parent` the waker that the gate's waker wakes, then reports
    /// whether the gate's waker fired since the last call, and consumes that.
    /// The first call reports true.
    pub(crate) fn poll_woken(&mut self, parent: &Waker) -> bool {
        // The parent is registered before the flag is read, so a wake that the
        // read misses wakes this poll's parent.
        if !self
            .parent
            .as_ref()
            .is_some_and(|registered| registered.will_wake(parent))
        {
            self.register_parent(parent);
        }
        self.wake.woken.load(Ordering::Relaxed) && self.wake.woken.swap(false, Ordering::Acquire)
    }

    #[cold]
    #[inline(never)]
    fn register_parent(&mut self, waker: &Waker) {
        let replaced = self
            .wake
            .parent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(waker.clone());
        drop(replaced);
        self.parent = Some(waker.clone());
    }
}

#[cfg(test)]
thread_local! {
    static SUPERVISOR_POLLS: Cell<usize> = const { Cell::new(0) };
}

/// How many times a worker supervisor has run on this thread; read on a
/// worker's owner thread, for example from one of its commands.
#[cfg(test)]
pub(crate) fn supervisor_polls_on_this_thread() -> usize {
    SUPERVISOR_POLLS.get()
}

/// A worker wheel's tick: Tokio's timer resolution.
const WHEEL_TICK: Duration = Duration::from_millis(1);

/// Polls of the wheel's sleep in one supervisor poll.
const MAX_SLEEP_POLLS: usize = 2;

thread_local! {
    /// The wheel of the hosted or managed worker this thread drives.
    static WORKER_WHEEL: RefCell<Option<Rc<WorkerWheel>>> = const { RefCell::new(None) };
}

/// The wheel of the hosted or managed worker this thread drives, which
/// `TokioHost::wheel_timer` reports.
pub(crate) fn worker_wheel_timer() -> Option<Rc<WheelTimer>> {
    let wheel = WORKER_WHEEL
        .try_with(|wheel| wheel.borrow().clone())
        .ok()
        .flatten()?;
    Some(wheel.timer())
}

/// What the wheels of workers driven on this thread did, for tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
struct WakeupCounts {
    /// Wheels created.
    wheels: usize,
    /// Supervisor polls.
    supervisor_polls: usize,
    /// Supervisor polls that refreshed the supervisor's waker and checked
    /// the sleep.
    wheel_steps: usize,
    /// Calls of the supervisor's wheel step, `WorkerWheel::drive`.
    wheel_drives: usize,
    /// Tokio sleeps created.
    sleeps: usize,
    /// Tokio sleeps dropped to withdraw the wakeup.
    withdrawals: usize,
    /// Polls of a sleep.
    sleep_polls: usize,
    /// Supervisor wakes requested by `arm`.
    supervisor_wakes: usize,
}

#[cfg(test)]
thread_local! {
    static WAKEUP_COUNTS: Cell<WakeupCounts> = Cell::new(WakeupCounts::default());
}

#[cfg(test)]
fn wakeup_counts() -> WakeupCounts {
    WAKEUP_COUNTS.get()
}

#[cfg(test)]
fn count(update: impl FnOnce(&mut WakeupCounts)) {
    let mut counts = WAKEUP_COUNTS.get();
    update(&mut counts);
    WAKEUP_COUNTS.set(counts);
}

/// The host wakeup of a worker's wheel: one Tokio `Sleep`, created at the
/// first arm and reset afterwards, polled by the worker's supervisor future,
/// which expires the wheel when it resolves. Withdrawing the wakeup drops the
/// sleep, which deregisters it from Tokio's timer, and the next arm makes it
/// anew in the same box.
#[derive(Default)]
struct TokioWakeup {
    sleep: RefCell<Option<Pin<Box<Option<Sleep>>>>>,
    /// The sleep's last poll was pending, so Tokio holds the supervisor's
    /// waker for it until it fires. After a ready poll this is false: Tokio
    /// keeps the waker then too, but not as a documented contract, so the
    /// next arm has the sleep polled again.
    registered: Cell<bool>,
    /// The sleep must be polled for Tokio to hold the supervisor's waker: it
    /// is new, or it was reset while not registered.
    needs_poll: Cell<bool>,
    /// The supervisor is driving the wheel, and polls a sleep that `expire`
    /// re-arms without being woken.
    driving: Cell<bool>,
    supervisor: RefCell<Option<Waker>>,
}

impl TokioWakeup {
    fn arm(&self, deadline: Instant) {
        let deadline = tokio::time::Instant::from_std(deadline);
        {
            let mut boxed = self.sleep.borrow_mut();
            let boxed = boxed.get_or_insert_with(|| Box::pin(None));
            match boxed.as_mut().as_pin_mut() {
                Some(sleep) => {
                    // A sleep that fired after its pending poll gave the
                    // supervisor's waker up, and a reset does not restore it.
                    if sleep.is_elapsed() {
                        self.registered.set(false);
                    }
                    sleep.reset(deadline);
                }
                None => {
                    #[cfg(test)]
                    count(|counts| counts.sleeps += 1);
                    boxed.set(Some(tokio::time::sleep_until(deadline)));
                }
            }
        }
        if self.registered.get() || self.needs_poll.replace(true) || self.driving.get() {
            return;
        }
        if let Some(supervisor) = &*self.supervisor.borrow() {
            #[cfg(test)]
            count(|counts| counts.supervisor_wakes += 1);
            supervisor.wake_by_ref();
        }
    }

    /// Keeps the supervisor's waker, cloning it only when it changes.
    fn refresh_supervisor(&self, waker: &Waker) {
        let mut supervisor = self.supervisor.borrow_mut();
        if !supervisor
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            *supervisor = Some(waker.clone());
        }
    }

    /// Whether the supervisor must poll the sleep: Tokio may have fired it,
    /// or must take the supervisor's waker for it. Otherwise the supervisor
    /// leaves it alone, a resolved or withdrawn sleep included. Inlined into
    /// the supervisor, which runs the wheel step only then.
    #[inline(always)]
    fn due(&self) -> bool {
        self.needs_poll.get()
            || (self.registered.get()
                && self
                    .sleep
                    .borrow()
                    .as_ref()
                    .and_then(|boxed| boxed.as_ref().as_pin_ref())
                    .is_some_and(|sleep| sleep.is_elapsed()))
    }

    /// Withdraws the wakeup: drops the sleep, which deregisters it from
    /// Tokio's timer, and keeps its box for the next arm.
    fn withdraw(&self) {
        if let Some(boxed) = self.sleep.borrow_mut().as_mut()
            && boxed.is_some()
        {
            #[cfg(test)]
            count(|counts| counts.withdrawals += 1);
            boxed.set(None);
        }
        self.registered.set(false);
        self.needs_poll.set(false);
    }

    /// Polls the sleep with the supervisor's context. The supervisor's poll
    /// starts with the wheel step, and Tokio polls the `LocalSet` root with
    /// a fresh coop budget, so coop never refuses this poll.
    fn poll_sleep(&self, context: &mut Context<'_>) -> Poll<()> {
        self.needs_poll.set(false);
        let mut boxed = self.sleep.borrow_mut();
        let Some(sleep) = boxed.as_mut().and_then(|boxed| boxed.as_mut().as_pin_mut()) else {
            return Poll::Pending;
        };
        #[cfg(test)]
        count(|counts| counts.sleep_polls += 1);
        let poll = sleep.poll(context);
        self.registered.set(poll.is_pending());
        poll
    }
}

/// A worker wheel's `WheelWakeup`.
struct SharedWakeup(Rc<TokioWakeup>);

impl WheelWakeup for SharedWakeup {
    fn arm(&self, deadline: Instant) {
        self.0.arm(deadline);
    }

    /// A Tokio sleep is withdrawn only by dropping it. The wheel disarms when
    /// it frees its last slot and in `stop`, after which `expire` does
    /// nothing.
    fn disarm(&self) {
        self.0.withdraw();
    }
}

/// The wheel timer of one hosted or managed worker and its wakeup. The wheel
/// is created when a timer first asks for it.
struct WorkerWheel {
    host: Host,
    wakeup: Rc<TokioWakeup>,
    timer: OnceCell<Rc<WheelTimer>>,
}

impl WorkerWheel {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            host: Host::tokio(),
            wakeup: Rc::default(),
            timer: OnceCell::new(),
        })
    }

    fn timer(&self) -> Rc<WheelTimer> {
        let timer = self.timer.get_or_init(|| {
            #[cfg(test)]
            count(|counts| counts.wheels += 1);
            WheelTimer::new(
                WHEEL_TICK,
                self.host.now(),
                Box::new(SharedWakeup(Rc::clone(&self.wakeup))),
            )
        });
        Rc::clone(timer)
    }

    /// The supervisor's wheel step, run at its polls where the wheel's sleep
    /// is due: polls the sleep, and expires the wheel when it resolves. A
    /// sleep that `expire` re-arms is polled again, so that Tokio holds the
    /// supervisor's waker for it; if it keeps resolving at once, the
    /// supervisor is woken to continue. An arm while the sleep is not due
    /// wakes the supervisor's waker stored here, or taken when the supervisor
    /// started. The panic of an owner's waker that `expire` re-raised (the
    /// wheel stays consistent and driven) goes to `wheel_panicked`. Out of
    /// line, so that a supervisor poll with no sleep due carries none of it.
    #[inline(never)]
    fn drive(
        &self,
        context: &mut Context<'_>,
        handle: &WorkerHandle,
        wheel_panic: &mut Option<PanicPayload>,
    ) {
        #[cfg(test)]
        count(|counts| counts.wheel_drives += 1);
        let Some(timer) = self.timer.get() else {
            return;
        };
        #[cfg(test)]
        count(|counts| counts.wheel_steps += 1);
        self.wakeup.refresh_supervisor(context.waker());
        if !self.wakeup.due() {
            return;
        }
        self.wakeup.driving.set(true);
        let expired = catch_unwind(AssertUnwindSafe(|| {
            for _ in 0..MAX_SLEEP_POLLS {
                if self.wakeup.poll_sleep(context).is_pending() {
                    return;
                }
                timer.expire(self.host.now());
                if !self.wakeup.due() {
                    return;
                }
            }
        }));
        self.wakeup.driving.set(false);
        if self.wakeup.due() {
            context.waker().wake_by_ref();
        }
        if let Err(payload) = expired {
            wheel_panicked(handle, wheel_panic, payload);
        }
    }
}

/// Keeps the first panic of an owner's waker that a worker's wheel re-raised,
/// and begins the worker's shutdown, keeping a panic of that too.
#[cold]
#[inline(never)]
fn wheel_panicked(
    handle: &WorkerHandle,
    wheel_panic: &mut Option<PanicPayload>,
    payload: PanicPayload,
) {
    retain_first_panic(wheel_panic, Some(payload));
    let shutdown = catch_unwind(AssertUnwindSafe(|| handle.begin_shutdown()));
    retain_first_panic(wheel_panic, shutdown.err());
}

/// Installs a worker's wheel on its owner thread until uninstalled or
/// dropped, whichever comes first.
struct InstalledWheel {
    wheel: Option<Rc<WorkerWheel>>,
    previous: Option<Rc<WorkerWheel>>,
}

impl InstalledWheel {
    fn install(wheel: Rc<WorkerWheel>) -> Self {
        let previous = WORKER_WHEEL.with(|slot| slot.replace(Some(Rc::clone(&wheel))));
        Self {
            wheel: Some(wheel),
            previous,
        }
    }

    /// The wheel's teardown, done once: stops the wheel (`~HHWheelTimer`'s
    /// `cancelAll`), drops the sleep and the supervisor's waker while the
    /// runtime is alive, and uninstalls the wheel. Returns the panic of an
    /// owner's waker that `stop` re-raised.
    fn uninstall(&mut self) -> Option<PanicPayload> {
        let wheel = self.wheel.take()?;
        let stopped = catch_unwind(AssertUnwindSafe(|| {
            if let Some(timer) = wheel.timer.get() {
                timer.stop();
            }
        }));
        drop(wheel.wakeup.sleep.take());
        drop(wheel.wakeup.supervisor.take());
        let previous = self.previous.take();
        let _ = WORKER_WHEEL.try_with(|slot| slot.replace(previous));
        stopped.err()
    }
}

/// Tears the wheel down on unwind. `run_managed_driver` uninstalls it on
/// every other path that can have created it.
impl Drop for InstalledWheel {
    fn drop(&mut self) {
        if let Some(payload) = self.uninstall() {
            crate::panic::discard_panic(payload);
        }
    }
}

/// How a supervisor stopped waiting for its driver.
enum Supervised {
    Joined(Result<(), TaskGroupError>),
    TeardownDeadlinePassed,
}

async fn run_managed_driver(
    handle: WorkerHandle,
    driver: WorkerDriver,
    teardown: Option<&TeardownDeadline>,
) -> Result<(), ManagedWorkerError> {
    let wheel = WorkerWheel::new();
    let mut installed = InstalledWheel::install(Rc::clone(&wheel));
    let mut gate = WakeGate::new();
    wheel.wakeup.refresh_supervisor(gate.waker());
    let mut tasks = TaskGroup::new();
    if let Err(error) = tasks.spawn_local(move |_| ManagedDriver {
        driver,
        failure: None,
    }) {
        tasks.close();
        return Err(error.into());
    }
    let mut joined = Box::pin(tasks.join());
    let mut timer: Option<(QuiesceDeadline, Pin<Box<Sleep>>)> = None;
    let mut teardown_timer = TeardownTimer::default();
    // Unwinding out of this future would drop unjoined tasks, so a panic of
    // an owner's waker during expiry shuts the worker down. After the final
    // join and the wheel's teardown (which an owner's waker can panic in
    // too), the first such panic is reported as the owner thread's; payloads
    // are disposed of with `discard_panic`, since their destructors can panic.
    let mut wheel_panic = None;
    let supervised = poll_fn(|parent| {
        // While a quiesce or teardown deadline is armed, the supervisor runs at
        // every poll, so a wake after the deadline forces shutdown, or ends the
        // teardown, before the woken work runs, even if the deadline's timer has
        // not fired yet.
        if !gate.poll_woken(parent.waker()) && timer.is_none() && !teardown_timer.is_armed() {
            return Poll::Pending;
        }
        #[cfg(test)]
        SUPERVISOR_POLLS.set(SUPERVISOR_POLLS.get() + 1);
        let context = &mut Context::from_waker(gate.waker());
        #[cfg(test)]
        count(|counts| counts.supervisor_polls += 1);
        if wheel.wakeup.due() {
            wheel.drive(context, &handle, &mut wheel_panic);
        }
        if handle.state() == WorkerState::Open {
            let (deadline, outstanding) = handle.quiesce_snapshot(context.waker());
            if let Some(deadline) = deadline {
                let remaining = deadline.remaining(Instant::now());
                if outstanding == 0 || remaining.is_zero() {
                    if outstanding != 0 {
                        handle.mark_forced();
                    }
                    handle.begin_shutdown();
                    timer = None;
                } else {
                    if timer
                        .as_ref()
                        .is_none_or(|(previous, _)| *previous != deadline)
                    {
                        timer = Some((
                            deadline,
                            Box::pin(tokio::time::sleep(remaining.min(LONGEST_SLEEP))),
                        ));
                    }
                    if let Some((_, sleep)) = timer.as_mut()
                        && sleep.as_mut().poll(context).is_ready()
                    {
                        timer = None;
                        context.waker().wake_by_ref();
                    }
                }
            }
        }
        if let Poll::Ready(result) = joined.as_mut().poll(context) {
            return Poll::Ready(Supervised::Joined(result));
        }
        match teardown {
            Some(teardown) if teardown_timer.poll_passed(teardown, context) => {
                Poll::Ready(Supervised::TeardownDeadlinePassed)
            }
            _ => Poll::Pending,
        }
    })
    .await;
    drop(joined);
    let result = match supervised {
        Supervised::Joined(result) => result.map_err(ManagedWorkerError::from),
        Supervised::TeardownDeadlinePassed => {
            // Destroying the driver aborts its unfinished cleanup tasks and re-raises a
            // command, cleanup or driver panic it recorded, which the join reports. The work
            // they abandon unjoined, from here to the thread's exit, is expected.
            begin_forced_teardown();
            tasks.abort_all();
            match tasks.join().await {
                Err(TaskGroupError::TaskPanicked) => Err(TaskGroupError::TaskPanicked.into()),
                Ok(()) | Err(_) => Err(ManagedWorkerError::TeardownDeadlineExceeded),
            }
        }
    };
    retain_first_panic(&mut wheel_panic, installed.uninstall());
    if let Some(payload) = wheel_panic {
        crate::panic::discard_panic(payload);
        return Err(ManagedWorkerError::OwnerThreadPanicked);
    }
    result
}

/// A worker hosted by a dedicated thread, current-thread runtime, and LocalSet.
/// Like `HostedWorker`, it drives a wheel timer for its owner thread, which
/// `Host::tokio().wheel_timer()` reports there (`DIV-003`).
pub struct ManagedWorker {
    handle: WorkerHandle,
    owner: Option<JoinHandle<Result<(), ManagedWorkerError>>>,
    completion: watch::Receiver<Option<Result<(), ManagedWorkerError>>>,
    _not_send: PhantomData<Rc<()>>,
}

impl ManagedWorker {
    pub fn new(command_capacity: usize) -> Result<Self, ManagedWorkerError> {
        Self::new_with_exit_callback(command_capacity, || {})
    }

    /// Starts a worker and invokes `exit_callback` on its owner thread after
    /// the driver has completed every synchronous and asynchronous cleanup.
    pub fn new_with_exit_callback<F>(
        command_capacity: usize,
        exit_callback: F,
    ) -> Result<Self, ManagedWorkerError>
    where
        F: FnOnce() + Send + 'static,
    {
        if command_capacity == 0 {
            return Err(WorkerCreateError::ZeroCapacity.into());
        }

        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (completed_tx, completion) = watch::channel(None);
        let owner = std::thread::Builder::new()
            .name("roxygen-worker".to_owned())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let runtime = match Builder::new_current_thread().enable_all().build() {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            let message = error.to_string();
                            let _ = started_tx.send(Err(message.clone()));
                            return Err(ManagedWorkerError::RuntimeInitialization(message));
                        }
                    };
                    let local_set = LocalSet::new();
                    let (handle, driver) = WorkerDriver::new(command_capacity)?;
                    if started_tx.send(Ok(handle.clone())).is_err() {
                        handle.begin_shutdown();
                    }
                    let _exit_callback = ExitCallback::new(exit_callback);
                    local_set.block_on(&runtime, run_managed_driver(handle, driver, None))
                }))
                .unwrap_or(Err(ManagedWorkerError::OwnerThreadPanicked));
                completed_tx.send_replace(Some(result.clone()));
                result
            })
            .map_err(|error| ManagedWorkerError::ThreadSpawn(error.to_string()))?;

        match started_rx.recv() {
            Ok(Ok(handle)) => Ok(Self {
                handle,
                owner: Some(owner),
                completion,
                _not_send: PhantomData,
            }),
            Ok(Err(error)) => {
                let _ = owner.join();
                Err(ManagedWorkerError::RuntimeInitialization(error))
            }
            Err(_) => match owner.join() {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(ManagedWorkerError::StartupChannelClosed),
                Err(_) => Err(ManagedWorkerError::OwnerThreadPanicked),
            },
        }
    }

    pub fn handle(&self) -> WorkerHandle {
        self.handle.clone()
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.handle.owner_thread()
    }

    pub(crate) fn completion(&self) -> watch::Receiver<Option<Result<(), ManagedWorkerError>>> {
        self.completion.clone()
    }

    /// Cancels and physically joins the worker. Notification panics are
    /// reported as errors only after joining, including on the Drop path.
    pub fn shutdown(mut self) -> Result<(), ManagedWorkerError> {
        self.shutdown_and_join()
    }

    pub(crate) fn request_shutdown(&self) -> Result<(), ManagedWorkerError> {
        catch_unwind(AssertUnwindSafe(|| self.handle.begin_shutdown()))
            .map(|_| ())
            .map_err(|_| ManagedWorkerError::ShutdownNotificationPanicked)
    }

    fn shutdown_and_join(&mut self) -> Result<(), ManagedWorkerError> {
        let requested = self.request_shutdown();
        let Some(owner) = self.owner.take() else {
            return requested;
        };
        let joined = match owner.join() {
            Ok(result) => result,
            Err(_) => Err(ManagedWorkerError::OwnerThreadPanicked),
        };
        requested.and(joined)
    }
}

impl Drop for ManagedWorker {
    fn drop(&mut self) {
        let _ = self.shutdown_and_join();
    }
}

/// Configuration for a [`HostedWorker`].
pub struct HostedWorkerOptions {
    /// Bound on queued commands.
    pub command_capacity: NonZeroUsize,
    /// Name of the dedicated owner thread. It must not contain a NUL byte.
    pub thread_name: String,
    /// Roxygen-only (`DIV-043`): runs once on the owner thread after its LocalSet and
    /// runtime are gone, with nothing left to poll there, and before `completion()` is
    /// published; also when the worker fails to start once its thread runs. Every
    /// LocalSet task (commands, cleanup, `spawn_local`) and every runtime task
    /// (`tokio::spawn`) has been dropped on the owner thread by then, with or without a
    /// [`HostedWorker::shutdown_by`] deadline, also when cleanup overran it; only
    /// blocking-pool work such a deadline abandoned can still be running, on its own
    /// threads. A panic in it is caught and reported as
    /// `ManagedWorkerError::OwnerThreadPanicked` unless the worker already failed.
    pub on_owner_thread_stop: Option<Box<dyn FnOnce() + Send + 'static>>,
}

/// Roxygen-only (`DIV-037`): configuration of the blocking-pool threads of a
/// [`HostedWorker`]'s runtime, which run work such as `spawn_blocking` and
/// `tokio::net::lookup_host` that the worker's commands start.
pub struct HostedWorkerBlockingPool {
    /// Name of every blocking-pool thread. It must not contain a NUL byte.
    pub thread_name: String,
    /// Runs on each blocking-pool thread when it starts, before it runs any
    /// work, and never on the owner thread.
    pub on_thread_start: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    /// Roxygen-only (`DIV-043`): runs on each blocking-pool thread as it exits, after
    /// its last work, and never on the owner thread (Tokio's `on_thread_stop`). A
    /// bounded teardown waits for it as part of the thread's exit; a thread that a
    /// teardown deadline abandoned runs it when its work returns.
    pub on_thread_stop: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
}

/// Roxygen-only (`DIV-043`): how [`HostedWorker::join_by`] ended.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub enum HostedWorkerJoin {
    /// The owner thread exited by the deadline and was joined; its result, the one
    /// `completion()` publishes.
    Joined(Result<(), ManagedWorkerError>),
    /// The owner thread was still running at the deadline, or the caller is the
    /// owner thread, and was detached; it publishes `completion()` when it ends.
    Detached,
}

type OwnerThreadHandle = JoinHandle<Result<(), ManagedWorkerError>>;

fn joined(result: std::thread::Result<Result<(), ManagedWorkerError>>) -> HostedWorkerJoin {
    match result {
        Ok(result) => HostedWorkerJoin::Joined(result),
        Err(payload) => {
            crate::panic::discard_panic(payload);
            HostedWorkerJoin::Joined(Err(ManagedWorkerError::OwnerThreadPanicked))
        }
    }
}

/// Configures `builder`'s blocking pool as `blocking_pool` says (Tokio's
/// defaults without one) and counts its live threads in `teardown`, which a
/// bounded teardown waits for (`DIV-043`). The hooks hold `teardown` weakly: the
/// runtime owns them, and the deadline cell holds the supervisor's waker, which
/// holds the runtime's handle.
fn configure_blocking_pool(
    builder: &mut Builder,
    blocking_pool: Option<HostedWorkerBlockingPool>,
    teardown: &Arc<TeardownDeadline>,
) {
    let (thread_name, on_thread_start, on_thread_stop) = match blocking_pool {
        Some(pool) => (pool.thread_name, pool.on_thread_start, pool.on_thread_stop),
        None => (DEFAULT_BLOCKING_THREAD_NAME.to_owned(), None, None),
    };
    // Tokio names each blocking thread just before it spawns it.
    let spawning = Arc::downgrade(teardown);
    builder.thread_name_fn(move || {
        if let Some(teardown) = spawning.upgrade() {
            teardown.blocking_thread_spawning();
        }
        thread_name.clone()
    });
    if let Some(start) = on_thread_start {
        let started = Arc::downgrade(teardown);
        builder.on_thread_start(move || {
            // A panicking start hook ends the thread without its stop hook.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| start())) {
                if let Some(teardown) = started.upgrade() {
                    teardown.blocking_thread_exited();
                }
                resume_unwind(payload);
            }
        });
    }
    let stopped = Arc::downgrade(teardown);
    builder.on_thread_stop(move || {
        let stop = catch_unwind(AssertUnwindSafe(|| {
            if let Some(stop) = &on_thread_stop {
                stop();
            }
        }));
        if let Some(teardown) = stopped.upgrade() {
            teardown.blocking_thread_exited();
        }
        if let Err(payload) = stop {
            resume_unwind(payload);
        }
    });
}

/// The owner thread of a [`HostedWorker`].
enum OwnerThread {
    /// Joined, without a deadline, by whoever joins it.
    Unjoined(OwnerThreadHandle),
    /// A joiner thread joins it, thread-local destructors included, and sends
    /// the result, so that a wait for its exit can end at a deadline (`DIV-043`).
    Joining {
        owner: ThreadId,
        joined: mpsc::Receiver<HostedWorkerJoin>,
    },
}

impl OwnerThread {
    /// Hands an unjoined thread to a new joiner thread; keeps it unjoined if no
    /// joiner thread can be started.
    fn hand_to_joiner(self) -> Self {
        let Self::Unjoined(thread) = self else {
            return self;
        };
        let owner = thread.thread().id();
        let (thread_tx, thread_rx) = mpsc::sync_channel::<OwnerThreadHandle>(1);
        let (joined_tx, joined_rx) = mpsc::sync_channel(1);
        let joiner = std::thread::Builder::new()
            .name(JOINER_THREAD_NAME.to_owned())
            .spawn(move || {
                if let Ok(thread) = thread_rx.recv() {
                    let _ = joined_tx.send(joined(thread.join()));
                }
            });
        if joiner.is_err() {
            return Self::Unjoined(thread);
        }
        match thread_tx.send(thread) {
            Ok(()) => Self::Joining {
                owner,
                joined: joined_rx,
            },
            Err(mpsc::SendError(thread)) => Self::Unjoined(thread),
        }
    }

    /// Joins the thread, waiting for its exit until `deadline` if there is one
    /// and detaching it if it is still running then.
    fn join_until(self, deadline: Option<Instant>) -> HostedWorkerJoin {
        let owner = match &self {
            Self::Unjoined(thread) => thread.thread().id(),
            Self::Joining { owner, .. } => *owner,
        };
        // The owner thread cannot join itself; completion() is this path's join point.
        if owner == std::thread::current().id() {
            return HostedWorkerJoin::Detached;
        }
        let Some(deadline) = deadline else {
            return match self {
                Self::Unjoined(thread) => joined(thread.join()),
                Self::Joining { joined, .. } => joined.recv().unwrap_or(HostedWorkerJoin::Detached),
            };
        };
        match self.hand_to_joiner() {
            Self::Joining { joined, .. } => joined
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or(HostedWorkerJoin::Detached),
            // No joiner thread: a join could outlast any deadline.
            Self::Unjoined(_) => HostedWorkerJoin::Detached,
        }
    }
}

/// A `Send + Sync` worker that owns a dedicated thread, current-thread runtime, and `LocalSet`.
/// Unlike `ManagedWorker`, it drives `WorkerDriver::with_host`, so commands and activated
/// cleanups run as independent Tokio local tasks.
///
/// Shutdown and Drop follow `ManagedWorker`'s, with no deadline: when the driver processes
/// shutdown, it drops queued commands and every running command still alive on the owner thread,
/// and lets registered cleanup run to completion. Each command is its own Tokio local task, so
/// until then a woken command may still be polled, sees `is_cancelled()`, and may finish, which a
/// `ManagedWorker` command never does. `completion()` is published after every task is joined and
/// the LocalSet and runtime are dropped. Dropping the runtime waits for blocking-pool work that
/// commands started, such as `spawn_blocking` or `tokio::net::lookup_host`.
///
/// The owner thread has a wheel timer, which `Host::tokio().wheel_timer()` and the context
/// host report there and a `Timeout` uses; the worker's supervisor drives it through one Tokio
/// `Sleep` and stops it after the final join (`DIV-003`).
///
/// Drop calls `begin_shutdown` and then, off the owner thread, joins the owner thread, which
/// blocks until all of that finishes and the thread exits. Async code therefore drops the worker
/// from a blocking context such as `spawn_blocking`, never on a Tokio worker thread. A joining Drop
/// deadlocks on a blocking-pool thread of this worker's runtime, and while the dropping caller
/// holds a `completion()` borrow, which keeps the owner thread from publishing completion. A Drop
/// on the owner thread does not join; `completion()` is that path's join point.
///
/// Roxygen-only (`DIV-043`): [`Self::shutdown_by`] bounds that teardown by a deadline, as Tokio's
/// `Runtime::shutdown_timeout` does, and [`Self::join_by`] bounds the join; Drop after
/// `shutdown_by` waits for the owner thread's exit until the deadline and then detaches it.
pub struct HostedWorker {
    handle: WorkerHandle,
    owner: Mutex<Option<OwnerThread>>,
    completion: watch::Receiver<Option<Result<(), ManagedWorkerError>>>,
    teardown: Arc<TeardownDeadline>,
}

impl HostedWorker {
    /// Starts the owner thread and blocks until its worker is running; async
    /// callers use `spawn_blocking`. A thread name with a NUL byte returns
    /// `ManagedWorkerError::ThreadSpawn` without starting a thread.
    pub fn spawn(options: HostedWorkerOptions) -> Result<Self, ManagedWorkerError> {
        Self::spawn_inner(options, None)
    }

    fn spawn_inner(
        options: HostedWorkerOptions,
        blocking_pool: Option<HostedWorkerBlockingPool>,
    ) -> Result<Self, ManagedWorkerError> {
        let HostedWorkerOptions {
            command_capacity,
            thread_name,
            on_owner_thread_stop,
        } = options;
        // std::thread::Builder panics on such a name.
        if thread_name.contains('\0')
            || blocking_pool
                .as_ref()
                .is_some_and(|pool| pool.thread_name.contains('\0'))
        {
            return Err(ManagedWorkerError::ThreadSpawn(
                "thread name contains a NUL byte".to_owned(),
            ));
        }

        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (completed_tx, completion) = watch::channel(None);
        let teardown = Arc::new(TeardownDeadline::default());
        let owner_teardown = Arc::clone(&teardown);
        let owner = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut builder = Builder::new_current_thread();
                    builder.enable_all();
                    configure_blocking_pool(&mut builder, blocking_pool, &owner_teardown);
                    let runtime = match builder.build() {
                        Ok(runtime) => OwnedRuntime {
                            runtime: Some(runtime),
                            teardown: Arc::clone(&owner_teardown),
                        },
                        Err(error) => {
                            let message = error.to_string();
                            let _ = started_tx.send(Err(message.clone()));
                            return Err(ManagedWorkerError::RuntimeInitialization(message));
                        }
                    };
                    let local_set = LocalSet::new();
                    let (handle, driver) =
                        WorkerDriver::with_host(command_capacity.get(), Host::tokio())?;
                    if started_tx.send(Ok(handle.clone())).is_err() {
                        handle.begin_shutdown();
                    }
                    local_set.block_on(
                        runtime.get(),
                        run_managed_driver(handle, driver, Some(&owner_teardown)),
                    )
                }))
                .unwrap_or(Err(ManagedWorkerError::OwnerThreadPanicked));
                // The LocalSet and runtime are gone, with every task: nothing is polled here now.
                let result =
                    match on_owner_thread_stop.map(|hook| catch_unwind(AssertUnwindSafe(hook))) {
                        Some(Err(payload)) => {
                            crate::panic::discard_panic(payload);
                            result.and(Err(ManagedWorkerError::OwnerThreadPanicked))
                        }
                        Some(Ok(())) | None => result,
                    };
                // Completion wakers are caller code, and dropping the sender notifies them too.
                // After an owner-thread Drop no one joins this thread, and std aborts the process
                // if its unjoined result panics on drop.
                let published = catch_unwind(AssertUnwindSafe(|| {
                    completed_tx.send_replace(Some(result.clone()));
                }));
                let closed = catch_unwind(AssertUnwindSafe(move || drop(completed_tx)));
                [published.err(), closed.err()]
                    .into_iter()
                    .flatten()
                    .for_each(crate::panic::discard_panic);
                result
            })
            .map_err(|error| ManagedWorkerError::ThreadSpawn(error.to_string()))?;

        match started_rx.recv() {
            Ok(Ok(handle)) => Ok(Self {
                handle,
                owner: Mutex::new(Some(OwnerThread::Unjoined(owner))),
                completion,
                teardown,
            }),
            Ok(Err(error)) => {
                let _ = owner.join();
                Err(ManagedWorkerError::RuntimeInitialization(error))
            }
            Err(_) => match owner.join() {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(ManagedWorkerError::StartupChannelClosed),
                Err(_) => Err(ManagedWorkerError::OwnerThreadPanicked),
            },
        }
    }

    /// [`Self::spawn`], with the runtime's blocking-pool threads named and started as
    /// `blocking_pool` says (`DIV-037`). A blocking thread name with a NUL byte returns
    /// `ManagedWorkerError::ThreadSpawn` without starting a thread.
    pub fn spawn_with_blocking_pool(
        options: HostedWorkerOptions,
        blocking_pool: HostedWorkerBlockingPool,
    ) -> Result<Self, ManagedWorkerError> {
        Self::spawn_inner(options, Some(blocking_pool))
    }

    pub fn handle(&self) -> WorkerHandle {
        self.handle.clone()
    }

    /// The owner thread's result, published once the worker is fully torn
    /// down. Release every borrow of it before dropping the worker. Wakers
    /// registered through it run on the owner thread and must not panic: the
    /// owner thread discards such a panic, but Tokio may then drop other
    /// waiters' wakers without waking them.
    pub fn completion(&self) -> watch::Receiver<Option<Result<(), ManagedWorkerError>>> {
        self.completion.clone()
    }

    /// Starts shutdown without joining and returns `WorkerHandle::begin_shutdown`'s
    /// result. Like `begin_shutdown`, it re-raises a shutdown notification panic
    /// after the worker has left `Open`.
    pub fn shutdown(&self) -> bool {
        self.handle.begin_shutdown()
    }

    /// Roxygen-only (`DIV-043`): [`Self::shutdown`], with the teardown bounded by
    /// `deadline`, as Tokio's `Runtime::shutdown_timeout` bounds a runtime's. If the
    /// driver has not finished by then (registered cleanup still running), the owner
    /// thread stops polling it and drops it and every command and cleanup task,
    /// unfinished cleanup included, and `completion()` reports
    /// `ManagedWorkerError::TeardownDeadlineExceeded`, or
    /// `ManagedWorkerError::TaskGroup(TaskPanicked)` if a command, cleanup or the
    /// driver panicked, as without a deadline. That destruction abandons the work it
    /// finds unjoined: from then until the owner thread exits,
    /// [`crate::task_group::in_forced_teardown`] holds there, and a task group or an
    /// owned session driver dropped with work unjoined still cancels that work, and
    /// raises a panic that work raises as it is destroyed, but does not assert that
    /// it was joined. The owner thread then drops its
    /// LocalSet, as without a deadline, and its runtime as Tokio's
    /// `Runtime::shutdown_background` does: the blocking pool is shut down (blocking
    /// work spawned from then on is refused) and every `tokio::spawn` task is dropped
    /// on the owner thread, inside the runtime's context, without waiting. It then
    /// waits for the runtime's blocking-pool threads to exit until the deadline;
    /// blocking-pool work still running then (such as `spawn_blocking`, or
    /// `tokio::net::lookup_host` waiting on a resolver) is abandoned to finish on its
    /// own thread, unreported, as Tokio's `shutdown_timeout` abandons it. Only Drop
    /// impls of the worker's tasks and the `on_owner_thread_stop` hook run on the
    /// owner thread after the deadline. Every task is thus dropped on the owner thread
    /// before the stop hook runs and `completion()` is published, also when cleanup
    /// overran the deadline; the wait for blocking-pool threads then returns at once,
    /// and work still running on them runs alongside the hook.
    ///
    /// A later call can move the deadline earlier, never later; every teardown phase
    /// that waits (the driver's cleanup, the wait for blocking-pool threads, and a
    /// bounded join) follows the earlier deadline, except a runtime drop that began
    /// without any deadline, which runs on the owner thread as without one. The first
    /// call also hands the owner thread to a joiner thread (named `roxygen-join`), so
    /// that a later [`Self::join_by`] or Drop can wait for the thread's exit until a
    /// deadline. Calling it on many workers before joining any lets them all tear down
    /// at once, each by the same deadline. Returns `WorkerHandle::begin_shutdown`'s
    /// result and, like it, re-raises a shutdown notification panic after the worker
    /// has left `Open`.
    pub fn shutdown_by(&self, deadline: Instant) -> bool {
        self.teardown.set_no_later_than(deadline);
        {
            let mut owner = self.owner.lock().unwrap_or_else(PoisonError::into_inner);
            *owner = owner.take().map(OwnerThread::hand_to_joiner);
        }
        self.handle.begin_shutdown()
    }

    /// Roxygen-only (`DIV-043`): starts shutdown as Drop does (without changing any
    /// teardown deadline), waits until `deadline` for the owner thread to exit, its
    /// thread-local destructors included (a joiner thread joins it), and returns its
    /// result if it did; otherwise detaches it. Like Drop, it never unwinds, and on the
    /// owner thread it detaches at once. With a teardown deadline from
    /// [`Self::shutdown_by`], a little slack past it lets the owner thread finish
    /// dropping its tasks and running its stop hook.
    pub fn join_by(mut self, deadline: Instant) -> HostedWorkerJoin {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.handle.begin_shutdown())) {
            crate::panic::discard_panic(payload);
        }
        self.owner
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .map_or(HostedWorkerJoin::Detached, |owner| {
                owner.join_until(Some(deadline))
            })
    }
}

impl Drop for HostedWorker {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.handle.begin_shutdown())) {
            crate::panic::discard_panic(payload);
        }
        let owner = self
            .owner
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(owner) = owner else {
            return;
        };
        let _ = owner.join_until(self.teardown.deadline());
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DriverError {
    #[error("caller-driven worker polled on {actual:?}, expected {expected:?}")]
    WrongOwnerThread {
        expected: ThreadId,
        actual: ThreadId,
    },
    #[error("caller-driven worker task failed")]
    DriverTaskFailed,
}

/// A caller-owned Tokio LocalSet driver pinned to its creating thread.
pub struct CallerDrivenGuard {
    handle: WorkerHandle,
    local_set: LocalSet,
    driver_task: Option<LocalJoinHandle<()>>,
    owner_thread: ThreadId,
    checked_owner_thread: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl CallerDrivenGuard {
    pub fn new(command_capacity: usize) -> Result<Self, WorkerCreateError> {
        let (handle, driver) = WorkerDriver::new(command_capacity)?;
        let owner_thread = std::thread::current().id();
        let local_set = LocalSet::new();
        let driver_task = local_set.spawn_local(driver);
        Ok(Self {
            handle,
            local_set,
            driver_task: Some(driver_task),
            owner_thread,
            checked_owner_thread: owner_thread,
            _not_send: PhantomData,
        })
    }

    pub fn handle(&self) -> WorkerHandle {
        self.handle.clone()
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }

    pub async fn run_until<F>(&mut self, future: F) -> Result<F::Output, DriverError>
    where
        F: Future,
    {
        self.check_owner_thread()?;
        Ok(self.local_set.run_until(future).await)
    }

    pub async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.check_owner_thread()?;
        self.handle.begin_shutdown();
        let Some(driver_task) = self.driver_task.take() else {
            return Ok(());
        };
        self.local_set
            .run_until(driver_task)
            .await
            .map_err(|_| DriverError::DriverTaskFailed)
    }

    fn check_owner_thread(&self) -> Result<(), DriverError> {
        let actual = std::thread::current().id();
        if actual == self.checked_owner_thread {
            Ok(())
        } else {
            Err(DriverError::WrongOwnerThread {
                expected: self.checked_owner_thread,
                actual,
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn set_checked_owner_thread_for_test(&mut self, owner: ThreadId) {
        self.checked_owner_thread = owner;
    }

    #[cfg(test)]
    pub(crate) fn driver_poll_count(&self) -> usize {
        self.handle.driver_poll_count()
    }
}

impl Drop for CallerDrivenGuard {
    fn drop(&mut self) {
        debug_assert_eq!(std::thread::current().id(), self.owner_thread);
        self.handle.begin_shutdown();
    }
}

pub fn caller_driven_worker(
    command_capacity: usize,
) -> Result<(WorkerHandle, CallerDrivenGuard), WorkerCreateError> {
    let guard = CallerDrivenGuard::new(command_capacity)?;
    let handle = guard.handle();
    Ok((handle, guard))
}

#[cfg(test)]
#[path = "../tests/wheel_driving.rs"]
pub(crate) mod wheel_driving_tests;
