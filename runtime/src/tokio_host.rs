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
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::JoinHandle;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use tokio::runtime::Builder;
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
}

struct ManagedDriver {
    driver: WorkerDriver,
    failure: Option<PanicPayload>,
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
/// outstanding work, its deadline timer, its task group join) wakes it.
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

async fn run_managed_driver(
    handle: WorkerHandle,
    driver: WorkerDriver,
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
    // Unwinding out of this future would drop unjoined tasks, so a panic of
    // an owner's waker during expiry shuts the worker down. After the final
    // join and the wheel's teardown (which an owner's waker can panic in
    // too), the first such panic is reported as the owner thread's; payloads
    // are disposed of with `discard_panic`, since their destructors can panic.
    let mut wheel_panic = None;
    let result = poll_fn(|parent| {
        // While a quiesce deadline is armed, the supervisor runs at every
        // poll, so a wake after the deadline forces shutdown before the woken
        // work runs, even if the deadline's timer has not fired yet.
        if !gate.poll_woken(parent.waker()) && timer.is_none() {
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
                            Box::pin(tokio::time::sleep(
                                remaining.min(Duration::from_secs(86_400)),
                            )),
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
        joined.as_mut().poll(context)
    })
    .await;
    retain_first_panic(&mut wheel_panic, installed.uninstall());
    if let Some(payload) = wheel_panic {
        crate::panic::discard_panic(payload);
        return Err(ManagedWorkerError::OwnerThreadPanicked);
    }
    result.map_err(ManagedWorkerError::from)
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
                    local_set.block_on(&runtime, run_managed_driver(handle, driver))
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
pub struct HostedWorker {
    handle: WorkerHandle,
    owner: Mutex<Option<JoinHandle<Result<(), ManagedWorkerError>>>>,
    completion: watch::Receiver<Option<Result<(), ManagedWorkerError>>>,
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
        let owner = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut builder = Builder::new_current_thread();
                    builder.enable_all();
                    if let Some(pool) = blocking_pool {
                        builder.thread_name(pool.thread_name);
                        if let Some(start) = pool.on_thread_start {
                            builder.on_thread_start(move || start());
                        }
                    }
                    let runtime = match builder.build() {
                        Ok(runtime) => runtime,
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
                    local_set.block_on(&runtime, run_managed_driver(handle, driver))
                }))
                .unwrap_or(Err(ManagedWorkerError::OwnerThreadPanicked));
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
                owner: Mutex::new(Some(owner)),
                completion,
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
        // The owner thread cannot join itself; completion() is this path's join point.
        if owner.thread().id() == std::thread::current().id() {
            return;
        }
        if let Err(payload) = owner.join() {
            crate::panic::discard_panic(payload);
        }
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
