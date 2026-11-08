#[cfg(test)]
use std::cell::Cell;
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
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupError;
use crate::worker::PanicPayload;
use crate::worker::QuiesceDeadline;
use crate::worker::WorkerCreateError;
use crate::worker::WorkerHandle;
use crate::worker::WorkerState;

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

async fn run_managed_driver(
    handle: WorkerHandle,
    driver: WorkerDriver,
) -> Result<(), ManagedWorkerError> {
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
    let mut gate = WakeGate::new();
    poll_fn(|parent| {
        // While a quiesce deadline is armed, the supervisor runs at every
        // poll, so a wake after the deadline forces shutdown before the woken
        // work runs, even if the deadline's timer has not fired yet.
        if !gate.poll_woken(parent.waker()) && timer.is_none() {
            return Poll::Pending;
        }
        #[cfg(test)]
        SUPERVISOR_POLLS.set(SUPERVISOR_POLLS.get() + 1);
        let context = &mut Context::from_waker(gate.waker());
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
    .await
    .map_err(ManagedWorkerError::from)
}

/// A worker hosted by a dedicated thread, current-thread runtime, and LocalSet.
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
