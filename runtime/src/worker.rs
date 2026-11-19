/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use crate::cancellation::CancellationRegistration;
use crate::cancellation::CancellationSource;
use crate::cancellation::CancellationToken;
use crate::context::ContextEnvelope;
use crate::host::Host;
use crate::observability::ObservedTask;
use crate::observability::OperationKind;
use crate::observability::OperationSpan;
use crate::observability::TerminalOutcome;
use crate::task_group::IdSlab;
use crate::task_group::NO_SLOT;
use crate::task_group::TaskGroup;
use crate::thread::current_id;

mod poll_stats;
mod woken;

pub(crate) use poll_stats::PassStats;
use poll_stats::PollCounters;
pub use poll_stats::WorkerPollStats;
use woken::ReadyEntry;
use woken::WokenCleanups;
use woken::WokenHandle;

static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WorkerId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerState {
    Open,
    Closing,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerCreateError {
    #[error("worker command capacity must be non-zero")]
    ZeroCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SubmitError {
    #[error("worker is closed to new work")]
    Closed,
    #[error("worker command queue is full")]
    Overloaded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerCallError {
    #[error("worker call was cancelled")]
    Cancelled,
}

/// Failure to register, activate, or disarm owner-local shutdown cleanup work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CleanupRegistrationError {
    #[error("cleanup registration must run on {expected:?}, not {actual:?}")]
    WrongOwnerThread {
        expected: ThreadId,
        actual: ThreadId,
    },
    #[error("worker is closed to new cleanup registrations")]
    Closed,
    #[error("worker cleanup registration identifiers are exhausted")]
    IdentifierExhausted,
}

/// The scope at which one of Roxygen's owned HTTP drivers contained a panic,
/// which selects the `WorkerPollStats` counter that counts it.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainedPanicScope {
    /// One exchange's callbacks.
    Exchange,
    /// A callback of a whole connection.
    Connection,
    /// The driver, outside any narrower boundary.
    Driver,
}

impl ContainedPanicScope {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Exchange => "exchange",
            Self::Connection => "connection",
            Self::Driver => "driver",
        }
    }
}

pub(crate) type PanicPayload = Box<dyn Any + Send + 'static>;
pub(crate) type LocalTask = Pin<Box<dyn Future<Output = ()> + 'static>>;
type WorkerCommandFactory = Box<dyn FnOnce(WorkerContext) -> LocalTask + Send + 'static>;

pub(crate) fn discard_panic(payload: PanicPayload) {
    crate::panic::discard_panic(payload);
}

pub(crate) fn retain_first_panic(first: &mut Option<PanicPayload>, next: Option<PanicPayload>) {
    if let Some(payload) = next {
        if first.is_none() {
            *first = Some(payload);
        } else {
            discard_panic(payload);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QuiesceDeadline {
    started: Instant,
    grace: Duration,
}

impl QuiesceDeadline {
    pub(crate) fn new(grace: Duration) -> Self {
        Self {
            started: Instant::now(),
            grace,
        }
    }

    pub(crate) fn remaining(self, now: Instant) -> Duration {
        self.grace
            .saturating_sub(now.saturating_duration_since(self.started))
    }
}

struct WorkCompletion {
    shared: Arc<WorkerShared>,
}

impl Drop for WorkCompletion {
    fn drop(&mut self) {
        let waker = {
            let mut queue = self.shared.queue();
            queue.outstanding_work -= 1;
            queue.supervisor_waker.take()
        };
        #[cfg(test)]
        if let Some(hook) = WORK_RELEASE_HOOK.get() {
            let outstanding = self.shared.queue().outstanding_work;
            hook(outstanding, std::thread::panicking());
        }
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct TrackedTask {
    task: Option<LocalTask>,
    completion: Option<WorkCompletion>,
    destruction_failed: Option<Rc<Cell<bool>>>,
}

impl TrackedTask {
    fn drop_task(&mut self) {
        let task = self.task.take();
        if let Some(failed) = &self.destruction_failed {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(task))) {
                failed.set(true);
                resume_unwind(payload);
            }
        } else {
            drop(task);
        }
    }

    fn drop_completion(&mut self) {
        let completion = self.completion.take();
        if let Some(failed) = &self.destruction_failed {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(completion))) {
                failed.set(true);
                resume_unwind(payload);
            }
        } else {
            drop(completion);
        }
    }
}

impl Future for TrackedTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let Some(task) = self.task.as_mut() else {
            return Poll::Ready(());
        };
        if task.as_mut().poll(context).is_pending() {
            return Poll::Pending;
        }
        self.drop_task();
        self.drop_completion();
        Poll::Ready(())
    }
}

impl Drop for TrackedTask {
    fn drop(&mut self) {
        let mut first_panic = catch_unwind(AssertUnwindSafe(|| self.drop_task())).err();
        let completion_panic = catch_unwind(AssertUnwindSafe(|| self.drop_completion())).err();
        retain_first_panic(&mut first_panic, completion_panic);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

/// An admitted command, holding its job (under its command span) and its
/// `WorkCompletion`. `WorkerDriver::new` boxes it as one task; a hosted
/// driver's task group keeps it inline beneath the group span. Destruction
/// drops the job and then releases the completion, each at most once.
pub(crate) struct CommandTask {
    task: Option<ObservedTask<LocalTask>>,
    completion: Option<WorkCompletion>,
}

impl CommandTask {
    /// The job, until the command completes.
    #[cfg(not(feature = "observability"))]
    pub(crate) fn job_mut(&mut self) -> Option<&mut ObservedTask<LocalTask>> {
        self.task.as_mut()
    }

    /// After the job's poll returned ready: closes its command span, drops
    /// the job, and releases the completion.
    #[cfg(not(feature = "observability"))]
    pub(crate) fn finish_ready(&mut self) {
        self.task
            .as_mut()
            .expect("a ready command retains its job")
            .finish_ready();
        self.release();
    }

    fn release(&mut self) {
        drop(self.task.take());
        drop(self.completion.take());
    }
}

impl Future for CommandTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let Some(task) = self.task.as_mut() else {
            return Poll::Ready(());
        };
        if task.poll(context).is_pending() {
            return Poll::Pending;
        }
        self.release();
        Poll::Ready(())
    }
}

impl Drop for CommandTask {
    fn drop(&mut self) {
        let mut first_panic = catch_unwind(AssertUnwindSafe(|| drop(self.task.take()))).err();
        let completion_panic =
            catch_unwind(AssertUnwindSafe(|| drop(self.completion.take()))).err();
        retain_first_panic(&mut first_panic, completion_panic);
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

pub(crate) struct WorkerCommand {
    context: ContextEnvelope,
    factory: Option<WorkerCommandFactory>,
    queue_span: Option<OperationSpan>,
    completion: Option<WorkCompletion>,
}

impl WorkerCommand {
    fn capture(factory: WorkerCommandFactory) -> Self {
        let context = ContextEnvelope::capture();
        let queue_span = OperationSpan::start(OperationKind::Queue, &context);
        Self {
            context,
            factory: Some(factory),
            queue_span: Some(queue_span),
            completion: None,
        }
    }

    pub(crate) fn into_command(mut self, context: WorkerContext) -> CommandTask {
        let mut queue_span = self
            .queue_span
            .take()
            .expect("an admitted command retains its queue span");
        let mut command_span = queue_span
            .in_scope(|| OperationSpan::start(OperationKind::Command, &ContextEnvelope::capture()));
        queue_span.complete(TerminalOutcome::Success);
        drop(queue_span);

        let factory = self
            .factory
            .take()
            .expect("an admitted command factory executes once");
        let future = match catch_unwind(AssertUnwindSafe(|| {
            command_span.in_scope(|| factory(context))
        })) {
            Ok(future) => future,
            Err(payload) => {
                command_span.complete(TerminalOutcome::Failure);
                resume_unwind(payload)
            }
        };
        CommandTask {
            task: Some(ObservedTask::new(command_span, future)),
            completion: self.completion.take(),
        }
    }

    fn reject(mut self, kind: OperationKind, outcome: TerminalOutcome) {
        let mut rejection = OperationSpan::start(kind, &self.context);
        self.context.run(|| drop(self.factory.take()));
        if let Some(mut queue_span) = self.queue_span.take() {
            queue_span.complete(outcome);
        }
        rejection.complete(outcome);
    }
}

impl Drop for WorkerCommand {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            self.context.run(|| drop(factory));
        }
    }
}

struct QueueState {
    lifecycle: WorkerState,
    accepting: bool,
    capacity: usize,
    commands: VecDeque<WorkerCommand>,
    outstanding_work: usize,
    quiesce_deadline: Option<QuiesceDeadline>,
    forced: bool,
    supervisor_waker: Option<Waker>,
    driver_waker: Option<Waker>,
    closed_wakers: Vec<Waker>,
    /// Cleanups woken since the last woken pass drained the list, each at
    /// most once.
    ready: Vec<ReadyEntry>,
}

pub(crate) struct WorkerShared {
    id: WorkerId,
    owner_thread: ThreadId,
    queue: Mutex<QueueState>,
    cancelled: AtomicBool,
    quiescing: CancellationSource,
    poll_counters: PollCounters,
    shutdown_span: Mutex<Option<OperationSpan>>,
}

impl WorkerShared {
    fn queue(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Registers the driver's waker and, while the worker is `Open`, moves the
    /// queued commands to `intake`. A stored waker that `will_wake` this one is
    /// kept. A replaced one is dropped outside the queue lock, before the
    /// commands are taken.
    pub(crate) fn take_open_commands(&self, waker: &Waker, intake: &mut Vec<WorkerCommand>) {
        let mut queue = self.queue();
        if !queue
            .driver_waker
            .as_ref()
            .is_some_and(|stored| stored.will_wake(waker))
        {
            let replaced = queue.driver_waker.replace(waker.clone());
            if replaced.is_some() {
                drop(queue);
                drop(replaced);
                queue = self.queue();
            }
        }
        if queue.lifecycle == WorkerState::Open {
            intake.extend(queue.commands.drain(..));
        }
    }

    pub(crate) fn register_driver_waker(&self, waker: &Waker) {
        self.queue().driver_waker = Some(waker.clone());
    }

    fn wake_driver(&self) {
        let waker = self.queue().driver_waker.take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Queues a woken cleanup for the next woken pass and wakes the driver.
    /// Every pass registers the driver waker before it drains the list, and
    /// any code that takes the waker wakes it. So when no waker is stored, the
    /// driver is already due to poll again, as is a driver not yet polled,
    /// and that poll drains this entry. A closed worker drops the entry.
    fn push_ready_cleanup(&self, entry: ReadyEntry) {
        let driver = {
            let mut queue = self.queue();
            if queue.lifecycle == WorkerState::Closed {
                return;
            }
            queue.ready.push(entry);
            queue.driver_waker.take()
        };
        if let Some(driver) = driver {
            driver.wake();
        }
    }

    /// Swaps the woken cleanups into the empty `batch`.
    fn take_ready_cleanups(&self, batch: &mut Vec<ReadyEntry>) {
        debug_assert!(batch.is_empty());
        std::mem::swap(&mut self.queue().ready, batch);
    }

    pub(crate) fn take_commands_for_shutdown(&self) -> Vec<WorkerCommand> {
        self.queue().commands.drain(..).collect()
    }

    pub(crate) fn mark_closed(&self) -> Option<PanicPayload> {
        let (closed_wakers, driver_waker, supervisor_waker) = {
            let mut queue = self.queue();
            queue.lifecycle = WorkerState::Closed;
            queue.accepting = false;
            queue.ready = Vec::new();
            (
                std::mem::take(&mut queue.closed_wakers),
                queue.driver_waker.take(),
                queue.supervisor_waker.take(),
            )
        };
        let mut first_panic = None;
        for waker in [driver_waker, supervisor_waker] {
            retain_first_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| drop(waker))).err(),
            );
        }
        for waker in closed_wakers {
            let result = catch_unwind(AssertUnwindSafe(|| waker.wake()));
            retain_first_panic(&mut first_panic, result.err());
        }
        if let Some(mut shutdown) = self
            .shutdown_span
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let result = catch_unwind(AssertUnwindSafe(|| {
                shutdown.complete(TerminalOutcome::Shutdown);
            }));
            retain_first_panic(&mut first_panic, result.err());
        }
        first_panic
    }

    #[inline]
    pub(crate) fn state(&self) -> WorkerState {
        // Cancellation is monotonic and precedes publication of every non-Open state.
        if !self.cancelled.load(Ordering::Acquire) {
            return WorkerState::Open;
        }
        self.state_slow()
    }

    #[cold]
    #[inline(never)]
    fn state_slow(&self) -> WorkerState {
        self.queue().lifecycle
    }

    /// The poll counters, for their single writer, the owner thread.
    #[inline]
    pub(crate) fn poll_counters(&self) -> &PollCounters {
        debug_assert_eq!(current_id(), self.owner_thread);
        &self.poll_counters
    }

    pub(crate) fn start_cancellation_span(&self) -> OperationSpan {
        let shutdown_span = self
            .shutdown_span
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(shutdown_span) = shutdown_span.as_ref() {
            shutdown_span.in_scope(|| {
                OperationSpan::start(OperationKind::Cancellation, &ContextEnvelope::capture())
            })
        } else {
            OperationSpan::start(OperationKind::Cancellation, &ContextEnvelope::capture())
        }
    }
}

/// A cloneable, thread-safe command handle permanently bound to one worker.
#[derive(Clone)]
pub struct WorkerHandle {
    pub(crate) shared: Arc<WorkerShared>,
}

impl fmt::Debug for WorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerHandle")
            .field("id", &self.id())
            .field("state", &self.state())
            .finish()
    }
}

impl WorkerHandle {
    pub(crate) fn new(capacity: usize, owner_thread: ThreadId) -> Result<Self, WorkerCreateError> {
        if capacity == 0 {
            return Err(WorkerCreateError::ZeroCapacity);
        }
        Ok(Self {
            shared: Arc::new(WorkerShared {
                id: WorkerId(NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed)),
                owner_thread,
                queue: Mutex::new(QueueState {
                    lifecycle: WorkerState::Open,
                    accepting: true,
                    capacity,
                    commands: VecDeque::with_capacity(capacity),
                    outstanding_work: 0,
                    quiesce_deadline: None,
                    forced: false,
                    supervisor_waker: None,
                    driver_waker: None,
                    closed_wakers: Vec::new(),
                    ready: Vec::new(),
                }),
                cancelled: AtomicBool::new(false),
                quiescing: CancellationSource::new(),
                poll_counters: PollCounters::default(),
                shutdown_span: Mutex::new(None),
            }),
        })
    }

    pub fn id(&self) -> WorkerId {
        self.shared.id
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.shared.owner_thread
    }

    #[inline]
    pub fn state(&self) -> WorkerState {
        self.shared.state()
    }

    pub fn is_cancelled(&self) -> bool {
        self.shared.cancelled.load(Ordering::Acquire)
    }

    /// Whether new commands may be admitted, independently of driver state.
    /// A draining managed worker remains `Open` while admitted work finishes.
    pub fn is_accepting(&self) -> bool {
        self.shared.queue().accepting
    }

    /// Observes the close of admission without requesting cancellation.
    pub fn quiescing(&self) -> WorkerQuiescing {
        WorkerQuiescing {
            token: self.shared.quiescing.token(),
            registration: None,
            waker: None,
        }
    }

    pub(crate) fn begin_quiesce(&self, deadline: QuiesceDeadline) {
        let (waker, force) = {
            let mut queue = self.shared.queue();
            if queue.lifecycle != WorkerState::Open {
                return;
            }
            queue.accepting = false;
            let now = Instant::now();
            if queue
                .quiesce_deadline
                .is_none_or(|previous| deadline.remaining(now) < previous.remaining(now))
            {
                queue.quiesce_deadline = Some(deadline);
            }
            let force = queue
                .quiesce_deadline
                .is_some_and(|deadline| deadline.remaining(now).is_zero());
            if force && queue.outstanding_work != 0 {
                queue.forced = true;
            }
            (queue.supervisor_waker.take(), force)
        };
        let requested = catch_unwind(AssertUnwindSafe(|| {
            if force {
                self.begin_shutdown();
            } else {
                self.shared.quiescing.request_cancellation();
            }
        }));
        let awakened = catch_unwind(AssertUnwindSafe(|| {
            if let Some(waker) = waker {
                waker.wake();
            }
        }));
        if let Err(payload) = requested.and(awakened) {
            resume_unwind(payload);
        }
    }

    pub(crate) fn quiesce_snapshot(&self, waker: &Waker) -> (Option<QuiesceDeadline>, usize) {
        let mut queue = self.shared.queue();
        queue.supervisor_waker = Some(waker.clone());
        (queue.quiesce_deadline, queue.outstanding_work)
    }

    pub(crate) fn mark_forced(&self) {
        self.shared.queue().forced = true;
    }

    pub(crate) fn was_forced(&self) -> bool {
        self.shared.queue().forced
    }

    pub fn queued_commands(&self) -> usize {
        self.shared.queue().commands.len()
    }

    pub fn same_worker(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub fn submit<F>(&self, callback: F) -> Result<(), SubmitError>
    where
        F: FnOnce(&WorkerContext) + Send + 'static,
    {
        self.spawn_local(move |context| async move {
            callback(&context);
        })
    }

    pub fn spawn_local<F, Fut>(&self, factory: F) -> Result<(), SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        #[cfg(test)]
        SPAWNED_JOB_SIZE.set(Some(size_of::<Fut>()));
        self.enqueue(WorkerCommand::capture(Box::new(move |context| {
            Box::pin(factory(context))
        })))
    }

    pub fn call<F, T>(&self, callback: F) -> Result<WorkerCall<T>, SubmitError>
    where
        F: FnOnce(&WorkerContext) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.call_local(move |context| async move { callback(&context) })
    }

    pub fn call_local<F, Fut, T>(&self, factory: F) -> Result<WorkerCall<T>, SubmitError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let (response, call) = response_channel(ContextEnvelope::capture());
        self.spawn_local(move |context| CallLocalJob::new(factory(context), response))?;
        Ok(call)
    }

    pub fn begin_shutdown(&self) -> bool {
        let mut shutdown_span = self
            .shared
            .shutdown_span
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let driver_waker = {
            let mut queue = self.shared.queue();
            if queue.lifecycle != WorkerState::Open {
                return false;
            }
            queue.lifecycle = WorkerState::Closing;
            queue.accepting = false;
            self.shared.cancelled.store(true, Ordering::Release);
            queue.driver_waker.take()
        };
        let parent = ContextEnvelope::capture();
        *shutdown_span = Some(OperationSpan::start(OperationKind::Shutdown, &parent));
        drop(shutdown_span);
        let requested = catch_unwind(AssertUnwindSafe(|| {
            self.shared.quiescing.request_cancellation();
        }));
        let awakened = catch_unwind(AssertUnwindSafe(|| {
            if let Some(waker) = driver_waker {
                waker.wake();
            }
        }));
        let mut first_panic = requested.err();
        retain_first_panic(&mut first_panic, awakened.err());
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        true
    }

    pub fn closed(&self) -> WorkerClosed {
        WorkerClosed {
            shared: Arc::clone(&self.shared),
        }
    }

    fn enqueue(&self, command: WorkerCommand) -> Result<(), SubmitError> {
        let mut command = Some(command);
        let (driver_waker, rejection) = {
            let mut queue = self.shared.queue();
            if !queue.accepting {
                (None, Some(SubmitError::Closed))
            } else if queue.commands.len() == queue.capacity {
                (None, Some(SubmitError::Overloaded))
            } else {
                let mut admitted = command.take().expect("an admitted command is retained");
                admitted.completion = Some(WorkCompletion {
                    shared: Arc::clone(&self.shared),
                });
                queue.outstanding_work += 1;
                queue.commands.push_back(admitted);
                (queue.driver_waker.take(), None)
            }
        };
        if let Some(error) = rejection {
            let (kind, outcome) = match error {
                SubmitError::Closed => (OperationKind::Shutdown, TerminalOutcome::Shutdown),
                SubmitError::Overloaded => (OperationKind::Overload, TerminalOutcome::Overloaded),
            };
            command
                .take()
                .expect("a rejected command is retained")
                .reject(kind, outcome);
            return Err(error);
        }
        if let Some(waker) = driver_waker {
            waker.wake();
        }
        Ok(())
    }

    /// This worker's driver poll counters, read from any thread.
    pub fn poll_stats(&self) -> WorkerPollStats {
        self.shared.poll_counters.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn driver_poll_count(&self) -> usize {
        self.poll_stats().driver_polls as usize
    }

    /// How many listeners the worker's quiescing source has registered.
    #[cfg(test)]
    pub(crate) fn quiescing_registrations(&self) -> usize {
        self.shared.quiescing.token().registration_count()
    }

    /// How many times the worker's quiescing registry lock has been taken.
    #[cfg(test)]
    pub(crate) fn quiescing_registry_locks(&self) -> usize {
        self.shared.quiescing.token().registry_locks()
    }

    /// Admitted commands and scopes whose `WorkCompletion` is unreleased.
    #[cfg(test)]
    pub(crate) fn outstanding_work(&self) -> usize {
        self.shared.queue().outstanding_work
    }
}

#[cfg(test)]
thread_local! {
    static SPAWNED_JOB_SIZE: Cell<Option<usize>> = const { Cell::new(None) };
    static WORK_RELEASE_HOOK: Cell<Option<fn(usize, bool)>> = const { Cell::new(None) };
    static DRIVER_INTAKE_HOOK: Cell<Option<fn(&WorkerHandle)>> = const { Cell::new(None) };
}

/// Replaces this thread's hook that a driver pass calls with its handle after
/// the pass found the worker `Open` and before it takes queued commands, and
/// returns the previous hook.
#[cfg(test)]
pub(crate) fn set_driver_intake_hook(hook: Option<fn(&WorkerHandle)>) -> Option<fn(&WorkerHandle)> {
    DRIVER_INTAKE_HOOK.replace(hook)
}

#[cfg(test)]
pub(crate) fn run_driver_intake_hook(handle: &WorkerHandle) {
    if let Some(hook) = DRIVER_INTAKE_HOOK.get() {
        hook(handle);
    }
}

/// Replaces this thread's hook that each `WorkCompletion` release calls with
/// the outstanding work left and `std::thread::panicking()`, and returns the
/// previous hook.
#[cfg(test)]
pub(crate) fn set_work_release_hook(hook: Option<fn(usize, bool)>) -> Option<fn(usize, bool)> {
    WORK_RELEASE_HOOK.replace(hook)
}

/// The size of the future type that this thread's latest `spawn_local` boxes.
#[cfg(test)]
pub(crate) fn take_spawned_job_size() -> Option<usize> {
    SPAWNED_JOB_SIZE.take()
}

/// A future that completes when worker admission closes.
/// Dropping it unregisters its waker and does not affect draining.
pub struct WorkerQuiescing {
    token: CancellationToken,
    registration: Option<CancellationRegistration>,
    /// A clone of the waker that `registration` holds in the registry, set
    /// only after the registry accepted it.
    waker: Option<Waker>,
}

impl WorkerQuiescing {
    #[inline(never)]
    fn register(&mut self, waker: &Waker) -> Poll<()> {
        drop(self.waker.take());
        if self.token.register(&mut self.registration, waker) {
            return Poll::Ready(());
        }
        if self.registration.is_some() {
            self.waker = Some(waker.clone());
        }
        Poll::Pending
    }
}

impl Future for WorkerQuiescing {
    type Output = ();

    /// A registered waker that would wake this task stays registered without
    /// taking the registry lock. Cancellation takes and wakes it after marking
    /// the token, so a poll that reads the token unmarked can stay pending.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.token.is_cancellation_requested() {
            drop(this.registration.take());
            drop(this.waker.take());
            return Poll::Ready(());
        }
        if this.registration.is_some()
            && this
                .waker
                .as_ref()
                .is_some_and(|registered| registered.will_wake(context.waker()))
        {
            return Poll::Pending;
        }
        this.register(context.waker())
    }
}

pub struct WorkerClosed {
    shared: Arc<WorkerShared>,
}

impl Future for WorkerClosed {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut queue = self.shared.queue();
        if queue.lifecycle == WorkerState::Closed {
            return Poll::Ready(());
        }
        if !queue
            .closed_wakers
            .iter()
            .any(|waker| waker.will_wake(context.waker()))
        {
            queue.closed_wakers.push(context.waker().clone());
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub struct WorkerContext {
    handle: WorkerHandle,
    local: Rc<OwnerState>,
}

impl WorkerContext {
    pub(crate) fn new(handle: WorkerHandle) -> Self {
        Self::create(handle, Host::tokio(), false)
    }

    pub(crate) fn with_host(handle: WorkerHandle, host: Host) -> Self {
        Self::create(handle, host, true)
    }

    fn create(handle: WorkerHandle, host: Host, independently_scheduled: bool) -> Self {
        let woken = (!independently_scheduled).then(WokenCleanups::new);
        Self {
            local: Rc::new(OwnerState {
                shared: Arc::clone(&handle.shared),
                active_guards: Cell::new(0),
                next_cleanup_id: Cell::new(0),
                cleanups: RefCell::new(IdSlab::new()),
                hosted_cleanups: RefCell::new(IdSlab::new()),
                hosted_tasks: independently_scheduled
                    .then(|| RefCell::new(TaskGroup::with_host(host.clone()))),
                hosted_shutdown_started: Cell::new(false),
                hosted_aborting: Cell::new(false),
                hosted_panic: RefCell::new(None),
                woken,
                host,
            }),
            handle,
        }
    }

    pub fn handle(&self) -> &WorkerHandle {
        &self.handle
    }

    /// Execution and clock capabilities for this owner worker.
    pub fn host(&self) -> &Host {
        &self.local.host
    }

    pub fn worker_id(&self) -> WorkerId {
        self.handle.id()
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.handle.owner_thread()
    }

    pub fn is_owner_thread(&self) -> bool {
        current_id() == self.owner_thread()
    }

    /// Returns the same value as `self.handle().is_cancelled()`.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.handle.shared.cancelled.load(Ordering::Acquire)
    }

    /// Lets an admitted scope start its own graceful drain before cancellation.
    /// The scope must retain and join its tasks, using pre-registered cleanup
    /// for the forced-shutdown path.
    pub fn quiescing(&self) -> WorkerQuiescing {
        self.handle.quiescing()
    }

    pub fn acquire_guard(&self) -> WorkerGuard {
        self.local
            .active_guards
            .set(self.local.active_guards.get().saturating_add(1));
        WorkerGuard {
            local: Rc::clone(&self.local),
            owner_thread: self.owner_thread(),
            _not_send: PhantomData,
        }
    }

    pub fn active_guards(&self) -> usize {
        self.local.active_guards.get()
    }

    /// Counts a panic that one of Roxygen's owned HTTP drivers contained at
    /// `scope` in this worker's `WorkerPollStats`.
    ///
    /// Under `observability` it also emits an error event with target
    /// `roxygen::contained_panic` and fields `scope`, `site`, `stream` (when
    /// known) and `message`, the payload's text truncated to 128 bytes. A panic
    /// in the subscriber is discarded. The caller keeps, and disposes of,
    /// `payload`.
    #[doc(hidden)]
    #[cold]
    #[inline(never)]
    pub fn record_contained_panic(
        &self,
        scope: ContainedPanicScope,
        site: &'static str,
        stream: Option<u64>,
        payload: &(dyn Any + Send),
    ) {
        self.handle
            .shared
            .poll_counters()
            .record_contained_panic(scope);
        #[cfg(feature = "observability")]
        emit_contained_panic(scope, site, stream, payload);
        #[cfg(not(feature = "observability"))]
        let _ = (site, stream, payload);
    }

    /// Registers an owner-local callback that the driver runs during shutdown.
    ///
    /// The callback may own `!Send` state. Registration succeeds only on the
    /// worker's owner thread while the worker is `Open`.
    pub fn register_cleanup<F>(
        &self,
        cleanup: F,
    ) -> Result<WorkerCleanupRegistration, CleanupRegistrationError>
    where
        F: FnOnce() + 'static,
    {
        let actual = current_id();
        let expected = self.owner_thread();
        if actual != expected {
            return Err(CleanupRegistrationError::WrongOwnerThread { expected, actual });
        }

        let queue = self.handle.shared.queue();
        if queue.lifecycle != WorkerState::Open {
            return Err(CleanupRegistrationError::Closed);
        }
        let id = self.local.next_cleanup_id.get();
        let Some(next_id) = id.checked_add(1) else {
            return Err(CleanupRegistrationError::IdentifierExhausted);
        };
        self.local.next_cleanup_id.set(next_id);
        let slot = self.local.cleanups.borrow_mut().insert(
            id,
            CleanupEntry {
                context: ContextEnvelope::capture(),
                cleanup: Box::new(cleanup),
            },
        );
        drop(queue);

        Ok(WorkerCleanupRegistration {
            local: Rc::downgrade(&self.local),
            id: Some(id),
            slot,
            owner_thread: expected,
            _not_send: PhantomData,
        })
    }

    /// Reserves owner-local driver ownership for a later asynchronous cleanup.
    ///
    /// Reservation succeeds only on the owner thread while the worker is
    /// `Open`, and it bypasses the bounded cross-thread command queue. The
    /// returned registration may be activated while the worker is `Closing`
    /// because its driver ownership was established by this call while open.
    pub fn register_cleanup_task<F>(
        &self,
        cleanup: F,
    ) -> Result<WorkerCleanupTaskRegistration, CleanupRegistrationError>
    where
        F: Future<Output = ()> + 'static,
    {
        self.register_cleanup_task_inner(
            TrackedTask {
                task: Some(Box::pin(cleanup)),
                completion: None,
                destruction_failed: None,
            },
            None,
        )
    }

    fn register_cleanup_task_inner(
        &self,
        cleanup: TrackedTask,
        destruction_failed: Option<Rc<Cell<bool>>>,
    ) -> Result<WorkerCleanupTaskRegistration, CleanupRegistrationError> {
        let actual = current_id();
        let expected = self.owner_thread();
        if actual != expected {
            return Err(CleanupRegistrationError::WrongOwnerThread { expected, actual });
        }

        let queue = self.handle.shared.queue();
        if queue.lifecycle != WorkerState::Open {
            return Err(CleanupRegistrationError::Closed);
        }
        let id = self.local.next_cleanup_id.get();
        let Some(next_id) = id.checked_add(1) else {
            return Err(CleanupRegistrationError::IdentifierExhausted);
        };
        self.local.next_cleanup_id.set(next_id);
        let state = Rc::new(TrackedCleanupState {
            id,
            local: Rc::downgrade(&self.local),
            scheduled: Cell::new(false),
            phase: RefCell::new(TrackedCleanupPhase::Reserved(cleanup)),
            completion_waker: RefCell::new(None),
            destruction_failed,
            publisher: CleanupPublisher::new(),
            registered: Cell::new(true),
            activated: Cell::new(false),
            woken: OnceCell::new(),
            #[cfg(test)]
            poll_waker_override: OnceCell::new(),
        });
        let slot = self
            .local
            .hosted_cleanups
            .borrow_mut()
            .insert(id, Rc::clone(&state));
        drop(queue);
        self.handle.shared.poll_counters().cleanup_registered();

        Ok(WorkerCleanupTaskRegistration {
            local: Rc::downgrade(&self.local),
            state: Some(state),
            slot,
            handle: self.handle.clone(),
            owner_thread: expected,
            _not_send: PhantomData,
        })
    }

    /// Registers cleanup for a scope that may outlive its initialization command.
    ///
    /// Register before spawning the scope's child task group. Retain this token
    /// while the scope runs, and observe `quiescing` to start a graceful drain.
    /// Activate or drop the token to run cleanup; disarm it only after joining
    /// the children yourself. Graceful worker shutdown waits for this cleanup,
    /// even after the initialization command returns. At the deadline, the
    /// driver runs the cleanup regardless of surviving registration handles.
    /// The cleanup must cancel as needed and join every child before returning.
    pub fn register_scope<F>(
        &self,
        cleanup: F,
    ) -> Result<WorkerCleanupTaskRegistration, CleanupRegistrationError>
    where
        F: Future<Output = ()> + 'static,
    {
        let actual = current_id();
        let expected = self.owner_thread();
        if actual != expected {
            return Err(CleanupRegistrationError::WrongOwnerThread { expected, actual });
        }
        let mut queue = self.handle.shared.queue();
        if queue.lifecycle != WorkerState::Open {
            return Err(CleanupRegistrationError::Closed);
        }
        queue.outstanding_work += 1;
        drop(queue);
        let destruction_failed = Rc::new(Cell::new(false));
        self.register_cleanup_task_inner(
            TrackedTask {
                task: Some(Box::pin(cleanup)),
                completion: Some(WorkCompletion {
                    shared: Arc::clone(&self.handle.shared),
                }),
                destruction_failed: Some(Rc::clone(&destruction_failed)),
            },
            Some(destruction_failed),
        )
    }

    pub(crate) fn drain_cleanups(&self) -> Option<PanicPayload> {
        let cleanups = std::mem::take(&mut *self.local.cleanups.borrow_mut());
        let mut first_panic = None;
        for (_, entry) in cleanups.into_sorted() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                entry.context.run(entry.cleanup);
            }));
            retain_first_panic(&mut first_panic, result.err());
        }
        first_panic
    }

    /// How many slots the cleanup registry stores, armed or vacant.
    #[cfg(test)]
    pub(crate) fn stored_cleanup_entries(&self) -> usize {
        self.local.cleanups.borrow().slots()
    }

    /// How many armed registrations the cleanup registry holds.
    #[cfg(test)]
    pub(crate) fn armed_cleanup_entries(&self) -> usize {
        self.local.cleanups.borrow().len()
    }

    /// How many slots the hosted cleanup registry stores, reserved or vacant.
    #[cfg(test)]
    pub(crate) fn stored_hosted_cleanups(&self) -> usize {
        self.local.hosted_cleanups.borrow().slots()
    }

    /// How many unfinished cleanups the hosted cleanup registry holds.
    #[cfg(test)]
    pub(crate) fn armed_hosted_cleanups(&self) -> usize {
        self.local.hosted_cleanups.borrow().len()
    }

    /// Overrides the newest cleanup's poll waker for fault injection.
    #[cfg(test)]
    pub(crate) fn set_newest_cleanup_poll_waker(&self, waker: Waker) {
        assert_eq!(current_id(), self.owner_thread());
        assert!(self.local.hosted_tasks.is_none());
        let cleanups = self.local.hosted_cleanups.borrow();
        let (id, slot) = cleanups.keys_by_id().pop().expect("a registered cleanup");
        cleanups
            .get(slot, id)
            .expect("the newest cleanup is still registered")
            .poll_waker_override
            .set(waker)
            .expect("the fixture installs its waker only once");
    }

    /// The woken cleanups awaiting the next drain and the ready list's
    /// capacity, for a non-hosted driver.
    #[cfg(test)]
    pub(crate) fn queued_cleanup_wakes(&self) -> Option<(usize, usize)> {
        self.local.woken.as_ref()?;
        let queue = self.handle.shared.queue();
        Some((queue.ready.len(), queue.ready.capacity()))
    }

    pub(crate) fn poll_cleanup_tasks(
        &self,
        context: &mut Context<'_>,
        finish_reserved: bool,
    ) -> CleanupPass {
        if self.local.hosted_tasks.is_some() {
            return self.poll_hosted_cleanup_tasks(context, finish_reserved);
        }
        self.poll_woken_cleanup_tasks(
            self.local
                .woken
                .as_ref()
                .expect("a non-hosted driver keeps a ready list"),
            finish_reserved,
        )
    }

    /// Polls the cleanups woken since the last pass, each once, in wake
    /// order, or, once shutdown starts, every unfinished cleanup in
    /// registration order. A registry borrow is never held across a poll. The
    /// driver registered its waker before its commands ran.
    fn poll_woken_cleanup_tasks(
        &self,
        woken: &WokenCleanups,
        finish_reserved: bool,
    ) -> CleanupPass {
        let mut batch = woken.spare.take();
        self.local.shared.take_ready_cleanups(&mut batch);
        let mut pass = CleanupPass::default();
        if finish_reserved {
            batch.clear();
            let keys = self.local.hosted_cleanups.borrow().keys_by_id();
            for (id, slot) in keys {
                let Some(state) = self.local.hosted_cleanups.borrow().get(slot, id).cloned() else {
                    continue;
                };
                let phase =
                    std::mem::replace(&mut *state.phase.borrow_mut(), TrackedCleanupPhase::Polling);
                match phase {
                    TrackedCleanupPhase::Reserved(future) | TrackedCleanupPhase::Active(future) => {
                        let handle = state
                            .woken
                            .get_or_init(|| woken.handle_for(&self.local.shared, slot, id));
                        // A wake whose entry this pass discarded left the flag set.
                        handle.node.queued.swap(false, Ordering::SeqCst);
                        let poll_waker = &handle.waker;
                        #[cfg(test)]
                        let poll_waker = state.poll_waker_override.get().unwrap_or(poll_waker);
                        let (_, panic) = poll_tracked_cleanup(
                            &state,
                            slot,
                            future,
                            &mut Context::from_waker(poll_waker),
                        );
                        pass.polls += 1;
                        retain_first_panic(&mut pass.panic, panic);
                    }
                    TrackedCleanupPhase::Finished => {
                        *state.phase.borrow_mut() = TrackedCleanupPhase::Finished;
                    }
                    TrackedCleanupPhase::Polling => {
                        unreachable!("a cleanup task cannot be polled reentrantly")
                    }
                }
            }
            pass.complete = self.local.hosted_cleanups.borrow().is_empty();
        } else {
            pass.ready = batch.len() as u64;
            for entry in batch.drain(..) {
                let state = self
                    .local
                    .hosted_cleanups
                    .borrow()
                    .get(entry.slot, entry.id)
                    .cloned();
                let Some(state) = state else {
                    pass.stale += 1;
                    continue;
                };
                let phase =
                    std::mem::replace(&mut *state.phase.borrow_mut(), TrackedCleanupPhase::Polling);
                match phase {
                    TrackedCleanupPhase::Active(future) => {
                        let handle = state
                            .woken
                            .get()
                            .expect("an activated woken cleanup has its waker");
                        handle.node.queued.swap(false, Ordering::SeqCst);
                        let poll_waker = &handle.waker;
                        #[cfg(test)]
                        let poll_waker = state.poll_waker_override.get().unwrap_or(poll_waker);
                        let (_, panic) = poll_tracked_cleanup(
                            &state,
                            entry.slot,
                            future,
                            &mut Context::from_waker(poll_waker),
                        );
                        pass.polls += 1;
                        retain_first_panic(&mut pass.panic, panic);
                    }
                    other @ (TrackedCleanupPhase::Reserved(_) | TrackedCleanupPhase::Finished) => {
                        *state.phase.borrow_mut() = other;
                        pass.stale += 1;
                    }
                    TrackedCleanupPhase::Polling => {
                        unreachable!("a cleanup task cannot be polled reentrantly")
                    }
                }
            }
        }
        woken.spare.replace(batch);
        pass
    }

    fn poll_hosted_cleanup_tasks(
        &self,
        context: &mut Context<'_>,
        finish_reserved: bool,
    ) -> CleanupPass {
        let tasks = self
            .local
            .hosted_tasks
            .as_ref()
            .expect("hosted cleanup task group");
        if finish_reserved && !self.local.hosted_shutdown_started.replace(true) {
            let reserved: Vec<_> = {
                let cleanups = self.local.hosted_cleanups.borrow();
                cleanups
                    .keys_by_id()
                    .into_iter()
                    .filter_map(|(id, slot)| Some((slot, Rc::clone(cleanups.get(slot, id)?))))
                    .collect()
            };
            for (slot, state) in reserved {
                let phase =
                    std::mem::replace(&mut *state.phase.borrow_mut(), TrackedCleanupPhase::Polling);
                *state.phase.borrow_mut() = match phase {
                    TrackedCleanupPhase::Reserved(future) => TrackedCleanupPhase::Active(future),
                    other => other,
                };
                schedule_hosted_cleanup(&self.local, &state, slot, &self.handle);
            }
            tasks.borrow_mut().close();
        }
        let mut tasks = tasks.borrow_mut();
        while let Poll::Ready(Some(completion)) = tasks.poll_join_next(context) {
            if let Err(error) = completion.result {
                retain_first_panic(
                    &mut self.local.hosted_panic.borrow_mut(),
                    Some(Box::new(error)),
                );
            }
            completion.discard();
        }
        CleanupPass {
            complete: self.local.hosted_cleanups.borrow().is_empty() && tasks.is_empty(),
            panic: self.local.hosted_panic.borrow_mut().take(),
            ..CleanupPass::default()
        }
    }

    pub(crate) fn abort_cleanup_tasks(&self) -> Option<PanicPayload> {
        if let Some(tasks) = &self.local.hosted_tasks {
            self.local.hosted_aborting.set(true);
            let mut tasks = tasks.borrow_mut();
            tasks.abort_all();
            while let Some(completion) = tasks.try_join_next() {
                completion.discard();
            }
            let reserved = std::mem::take(&mut *self.local.hosted_cleanups.borrow_mut());
            let mut first_panic = self.local.hosted_panic.borrow_mut().take();
            for (_, state) in reserved.into_sorted() {
                let phase = std::mem::replace(
                    &mut *state.phase.borrow_mut(),
                    TrackedCleanupPhase::Finished,
                );
                if let TrackedCleanupPhase::Reserved(future) | TrackedCleanupPhase::Active(future) =
                    phase
                {
                    retain_first_panic(&mut first_panic, drop_tracked_cleanup(&state, future));
                }
            }
            return first_panic;
        }
        self.local.hosted_aborting.set(true);
        let taken = std::mem::take(&mut *self.local.hosted_cleanups.borrow_mut());
        let mut first_panic = None;
        for (_, state) in taken.into_sorted() {
            let phase = std::mem::replace(
                &mut *state.phase.borrow_mut(),
                TrackedCleanupPhase::Finished,
            );
            if let TrackedCleanupPhase::Reserved(future) | TrackedCleanupPhase::Active(future) =
                phase
            {
                retain_first_panic(&mut first_panic, drop_tracked_cleanup(&state, future));
            }
        }
        first_panic
    }
}

#[cfg(feature = "observability")]
const CONTAINED_PANIC_MESSAGE_LIMIT: usize = 128;

#[cfg(feature = "observability")]
fn emit_contained_panic(
    scope: ContainedPanicScope,
    site: &'static str,
    stream: Option<u64>,
    payload: &(dyn Any + Send),
) {
    let message = crate::panic::panic_message(payload)
        .map(|text| crate::panic::truncated(text, CONTAINED_PANIC_MESSAGE_LIMIT));
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
        tracing::error!(
            target: "roxygen::contained_panic",
            scope = scope.name(),
            site,
            stream,
            message,
        );
    })) {
        discard_panic(panic);
    }
}

/// What one `WorkerContext::poll_cleanup_tasks` call did.
#[derive(Default)]
pub(crate) struct CleanupPass {
    /// Whether every cleanup task has finished.
    pub(crate) complete: bool,
    pub(crate) panic: Option<PanicPayload>,
    pub(crate) polls: u64,
    /// Ready-list entries that an open woken pass drained.
    pub(crate) ready: u64,
    /// Drained entries whose cleanup had finished or was not active.
    pub(crate) stale: u64,
}

impl CleanupPass {
    pub(crate) fn add_to(&self, pass: &mut PassStats) {
        pass.cleanup_polls += self.polls;
        pass.ready_entries += self.ready;
        pass.stale_ready_entries += self.stale;
        pass.max_ready_batch = pass.max_ready_batch.max(self.ready);
    }
}

struct OwnerState {
    shared: Arc<WorkerShared>,
    active_guards: Cell<usize>,
    next_cleanup_id: Cell<u64>,
    cleanups: RefCell<IdSlab<CleanupEntry>>,
    hosted_cleanups: RefCell<IdSlab<Rc<TrackedCleanupState>>>,
    hosted_tasks: Option<RefCell<TaskGroup>>,
    hosted_shutdown_started: Cell<bool>,
    hosted_aborting: Cell<bool>,
    hosted_panic: RefCell<Option<PanicPayload>>,
    /// Woken-only cleanup polling, which keeps cleanups in `hosted_cleanups`.
    woken: Option<WokenCleanups>,
    host: Host,
}

enum TrackedCleanupPhase {
    Reserved(TrackedTask),
    Active(TrackedTask),
    Polling,
    Finished,
}

struct TrackedCleanupState {
    id: u64,
    local: Weak<OwnerState>,
    scheduled: Cell<bool>,
    phase: RefCell<TrackedCleanupPhase>,
    completion_waker: RefCell<Option<Waker>>,
    destruction_failed: Option<Rc<Cell<bool>>>,
    publisher: CleanupPublisher,
    /// Whether `cleanups_registered` counts this cleanup.
    registered: Cell<bool>,
    /// Whether `cleanups_active` counts this cleanup.
    activated: Cell<bool>,
    /// The cleanup's own waker under woken polling, created at activation or
    /// at its first shutdown poll.
    woken: OnceCell<WokenHandle>,
    #[cfg(test)]
    poll_waker_override: OnceCell<Waker>,
}

/// Queues an activated cleanup for the next woken pass and wakes the driver.
fn schedule_woken_cleanup(local: &OwnerState, state: &TrackedCleanupState, slot: usize) {
    let Some(woken) = &local.woken else {
        return;
    };
    if local.hosted_aborting.get() {
        return;
    }
    state
        .woken
        .get_or_init(|| woken.handle_for(&local.shared, slot, state.id))
        .waker
        .wake_by_ref();
}

fn schedule_hosted_cleanup(
    local: &Rc<OwnerState>,
    state: &Rc<TrackedCleanupState>,
    slot: usize,
    handle: &WorkerHandle,
) {
    let Some(tasks) = &local.hosted_tasks else {
        return;
    };
    if local.hosted_aborting.get() {
        return;
    }
    if state.scheduled.replace(true) {
        return;
    }
    if matches!(&*state.phase.borrow(), TrackedCleanupPhase::Finished) {
        return;
    }
    let task = HostedCleanup {
        state: Rc::clone(state),
        slot,
        handle: handle.clone(),
    };
    tasks
        .borrow_mut()
        .spawn_cleanup(task)
        .expect("reserved cleanup is scheduled before closing cleanup admission");
}

/// An activated hosted cleanup, which its hosted task polls; `slot` is its
/// hosted registry slot.
pub(crate) struct HostedCleanup {
    state: Rc<TrackedCleanupState>,
    slot: usize,
    handle: WorkerHandle,
}

impl HostedCleanup {
    #[cold]
    #[inline(never)]
    fn record_panic(&self, payload: PanicPayload) {
        if let Some(local) = self.state.local.upgrade() {
            retain_first_panic(&mut local.hosted_panic.borrow_mut(), Some(payload));
        }
        self.handle.begin_shutdown();
    }
}

impl Future for HostedCleanup {
    type Output = ();

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let phase = std::mem::replace(
            &mut *self.state.phase.borrow_mut(),
            TrackedCleanupPhase::Polling,
        );
        match phase {
            TrackedCleanupPhase::Active(future) => {
                self.handle
                    .shared
                    .poll_counters()
                    .record_hosted_cleanup_poll();
                let (pending, panic) =
                    poll_tracked_cleanup(&self.state, self.slot, future, context);
                if let Some(panic) = panic {
                    self.record_panic(panic);
                }
                if pending {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
            TrackedCleanupPhase::Finished => {
                *self.state.phase.borrow_mut() = TrackedCleanupPhase::Finished;
                Poll::Ready(())
            }
            TrackedCleanupPhase::Reserved(_) | TrackedCleanupPhase::Polling => {
                unreachable!("only activated cleanup tasks are independently scheduled")
            }
        }
    }
}

impl Drop for HostedCleanup {
    fn drop(&mut self) {
        let phase = std::mem::replace(
            &mut *self.state.phase.borrow_mut(),
            TrackedCleanupPhase::Finished,
        );
        if let TrackedCleanupPhase::Active(future) = phase
            && let Some(panic) = drop_slotted_cleanup(&self.state, self.slot, future)
        {
            self.record_panic(panic);
        }
    }
}

/// Failure to confirm destruction of an owner-local registered cleanup future.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerCleanupObservationError {
    /// The publisher was lost or destruction of the registered future panicked.
    #[error("registered cleanup retirement was not confirmed")]
    Unconfirmed,
}

type CleanupOutcome = Result<(), WorkerCleanupObservationError>;

#[derive(Default)]
struct CleanupObservationState {
    outcome: Option<CleanupOutcome>,
    waiters: Vec<Option<Waker>>,
}

#[cfg(test)]
thread_local! {
    static PUBLISHER_STATES: Cell<usize> = const { Cell::new(0) };
}

/// How many cleanup publishers on this thread have allocated the state that
/// their observers share.
#[cfg(test)]
pub(crate) fn cleanup_publisher_allocations() -> usize {
    PUBLISHER_STATES.get()
}

/// The owner-local publisher of one cleanup's sticky retirement outcome. The
/// state that observers share is allocated at the first `observe`.
struct CleanupPublisher {
    outcome: Cell<Option<CleanupOutcome>>,
    shared: OnceCell<Arc<Mutex<CleanupObservationState>>>,
}

impl CleanupPublisher {
    fn new() -> Self {
        Self {
            outcome: Cell::new(None),
            shared: OnceCell::new(),
        }
    }

    fn observe(&self) -> WorkerCleanupObserver {
        let shared = self.shared.get_or_init(|| {
            #[cfg(test)]
            PUBLISHER_STATES.set(PUBLISHER_STATES.get() + 1);
            Arc::new(Mutex::new(CleanupObservationState {
                outcome: self.outcome.get(),
                waiters: Vec::new(),
            }))
        });
        WorkerCleanupObserver {
            shared: Arc::clone(shared),
        }
    }

    fn publish(&self, outcome: CleanupOutcome) -> Option<PanicPayload> {
        if self.outcome.get().is_some() {
            return None;
        }
        self.outcome.set(Some(outcome));
        let shared = self.shared.get()?;
        let waiters = {
            let mut state = shared.lock().unwrap_or_else(|error| error.into_inner());
            state.outcome = Some(outcome);
            std::mem::take(&mut state.waiters)
        };
        let mut first_panic = None;
        for waker in waiters.into_iter().flatten() {
            retain_first_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| waker.wake())).err(),
            );
        }
        first_panic
    }
}

impl Drop for CleanupPublisher {
    fn drop(&mut self) {
        if self.outcome.get().is_some() {
            return;
        }
        if let Some(payload) = self.publish(Err(WorkerCleanupObservationError::Unconfirmed)) {
            discard_panic(payload);
        }
    }
}

/// A cloneable, `Send` observer of a registered cleanup future's retirement.
///
/// [`Self::wait`] completes once the future and its captures are dropped on the
/// owner thread, including scope accounting. It does not report whether the
/// cleanup succeeded. Disarming or forcibly dropping the future also counts as
/// retirement. The observer holds no worker-local state, and dropping it does
/// not activate, cancel, or join the cleanup.
#[derive(Clone)]
pub struct WorkerCleanupObserver {
    shared: Arc<Mutex<CleanupObservationState>>,
}

impl WorkerCleanupObserver {
    /// Waits for the sticky retirement result independently of other observers.
    ///
    /// Dropping this future unregisters its waiter without affecting cleanup.
    pub fn wait(
        &self,
    ) -> impl Future<Output = Result<(), WorkerCleanupObservationError>> + Send + 'static {
        CleanupObservationWait {
            shared: Arc::clone(&self.shared),
            slot: None,
        }
    }
}

struct CleanupObservationWait {
    shared: Arc<Mutex<CleanupObservationState>>,
    slot: Option<usize>,
}

impl Future for CleanupObservationWait {
    type Output = CleanupOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let waiter = self.get_mut();
        let waker = context.waker().clone();
        let mut state = waiter
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(outcome) = state.outcome {
            drop(state);
            return Poll::Ready(outcome);
        }
        let previous = if let Some(slot) = waiter.slot {
            state.waiters[slot].replace(waker)
        } else if let Some(slot) = state.waiters.iter().position(Option::is_none) {
            state.waiters[slot] = Some(waker);
            waiter.slot = Some(slot);
            None
        } else {
            waiter.slot = Some(state.waiters.len());
            state.waiters.push(Some(waker));
            None
        };
        drop(state);
        drop(previous);
        Poll::Pending
    }
}

impl Drop for CleanupObservationWait {
    fn drop(&mut self) {
        let waker = self.slot.and_then(|slot| {
            self.shared
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .waiters
                .get_mut(slot)
                .and_then(Option::take)
        });
        drop(waker);
    }
}

struct CleanupEntry {
    context: ContextEnvelope,
    cleanup: Box<dyn FnOnce() + 'static>,
}

/// An owner-local token for removing a shutdown cleanup after normal cleanup.
///
/// Dropping the token leaves the cleanup armed. This lets the driver drain the
/// callback even when the token or a clone of its `WorkerContext` escapes the
/// driver-owned task that registered it.
#[must_use = "the cleanup remains armed unless this token is explicitly disarmed"]
pub struct WorkerCleanupRegistration {
    local: Weak<OwnerState>,
    id: Option<u64>,
    slot: usize,
    owner_thread: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl WorkerCleanupRegistration {
    /// Removes the armed callback without running it.
    ///
    /// Returns `true` when this call removed the callback. It returns `false`
    /// when shutdown already drained it or its owner state has been dropped.
    pub fn disarm(mut self) -> Result<bool, CleanupRegistrationError> {
        let actual = current_id();
        if actual != self.owner_thread {
            return Err(CleanupRegistrationError::WrongOwnerThread {
                expected: self.owner_thread,
                actual,
            });
        }
        let Some(id) = self.id.take() else {
            return Ok(false);
        };
        let Some(local) = self.local.upgrade() else {
            return Ok(false);
        };
        let removed = local.cleanups.borrow_mut().remove(self.slot, id);
        let disarmed = removed.is_some();
        // The removed callback's captures may register or disarm cleanups when
        // dropped, so they drop only after the registry borrow ends.
        drop(removed);
        Ok(disarmed)
    }

    /// A second token for the same registration.
    #[cfg(test)]
    pub(crate) fn stale_copy(&self) -> Self {
        Self {
            local: self.local.clone(),
            id: self.id,
            slot: self.slot,
            owner_thread: self.owner_thread,
            _not_send: PhantomData,
        }
    }
}

/// An owner-local reservation for one asynchronous shutdown cleanup task.
///
/// Dropping an unactivated registration activates its reserved cleanup.
/// Explicit activation returns a handle that can observe completion.
pub struct WorkerCleanupTaskRegistration {
    local: Weak<OwnerState>,
    state: Option<Rc<TrackedCleanupState>>,
    slot: usize,
    handle: WorkerHandle,
    owner_thread: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl WorkerCleanupTaskRegistration {
    /// Observes retirement before activation without owning or activating cleanup.
    pub fn observe(&self) -> WorkerCleanupObserver {
        self.state
            .as_ref()
            .expect("an accessible registration retains its cleanup state")
            .publisher
            .observe()
    }

    /// Activates the pre-admitted `!Send` owner-local cleanup future.
    ///
    /// Activation is allowed during `Closing`: unlike a new registration, the
    /// task's cleanup ownership was established while the worker was `Open`.
    /// The returned handle may be awaited or explicitly detached.
    pub fn activate(mut self) -> Result<WorkerCleanupTask, CleanupRegistrationError> {
        self.check_owner()?;
        if self.handle.state() == WorkerState::Closed {
            return Err(CleanupRegistrationError::Closed);
        }
        let Some(state) = self.state.take() else {
            return Err(CleanupRegistrationError::Closed);
        };
        let mut phase = state.phase.borrow_mut();
        let reserved = std::mem::replace(&mut *phase, TrackedCleanupPhase::Polling);
        match reserved {
            TrackedCleanupPhase::Reserved(future) => {
                *phase = TrackedCleanupPhase::Active(future);
            }
            TrackedCleanupPhase::Active(future) => {
                *phase = TrackedCleanupPhase::Active(future);
            }
            TrackedCleanupPhase::Finished => {
                *phase = TrackedCleanupPhase::Finished;
                return Err(CleanupRegistrationError::Closed);
            }
            TrackedCleanupPhase::Polling => {
                unreachable!("a cleanup registration cannot activate while polling")
            }
        }
        drop(phase);
        if !state.activated.replace(true) {
            self.handle.shared.poll_counters().cleanup_activated();
        }
        if let Some(local) = self.local.upgrade() {
            schedule_hosted_cleanup(&local, &state, self.slot, &self.handle);
            schedule_woken_cleanup(&local, &state, self.slot);
        }
        self.handle.shared.wake_driver();
        Ok(WorkerCleanupTask {
            state,
            owner_thread: self.owner_thread,
            _not_send: PhantomData,
        })
    }

    /// Removes an unused cleanup reservation without running its future.
    ///
    /// Returns `true` when the reserved slot was still armed and is now
    /// finished. It returns `false` after the driver has already resolved it.
    pub fn disarm(mut self) -> Result<bool, CleanupRegistrationError> {
        self.check_owner()?;
        Ok(self.disarm_inner())
    }

    /// Finishes the registration's cleanup state again, as its first finish did.
    #[cfg(test)]
    pub(crate) fn finish_hook(&self) -> Box<dyn FnOnce()> {
        let state = Rc::clone(
            self.state
                .as_ref()
                .expect("an accessible registration retains its cleanup state"),
        );
        let slot = self.slot;
        Box::new(move || {
            if let Some(payload) = finish_tracked_cleanup(&state, slot, true) {
                discard_panic(payload);
            }
        })
    }

    fn check_owner(&self) -> Result<(), CleanupRegistrationError> {
        let actual = current_id();
        if actual == self.owner_thread {
            Ok(())
        } else {
            Err(CleanupRegistrationError::WrongOwnerThread {
                expected: self.owner_thread,
                actual,
            })
        }
    }

    fn disarm_inner(&mut self) -> bool {
        let Some(state) = self.state.take() else {
            return false;
        };
        if self.local.upgrade().is_none() {
            return false;
        }
        let phase = std::mem::replace(
            &mut *state.phase.borrow_mut(),
            TrackedCleanupPhase::Finished,
        );
        match phase {
            TrackedCleanupPhase::Reserved(future) => {
                if let Some(payload) = drop_slotted_cleanup(&state, self.slot, future) {
                    resume_unwind(payload);
                }
                true
            }
            TrackedCleanupPhase::Active(future) => {
                *state.phase.borrow_mut() = TrackedCleanupPhase::Active(future);
                false
            }
            TrackedCleanupPhase::Polling => {
                *state.phase.borrow_mut() = TrackedCleanupPhase::Polling;
                false
            }
            TrackedCleanupPhase::Finished => false,
        }
    }
}

impl Drop for WorkerCleanupTaskRegistration {
    fn drop(&mut self) {
        debug_assert_eq!(current_id(), self.owner_thread);
        let Some(state) = self.state.take() else {
            return;
        };
        if self.local.upgrade().is_none() || self.handle.state() == WorkerState::Closed {
            return;
        }
        let mut phase = state.phase.borrow_mut();
        let reserved = std::mem::replace(&mut *phase, TrackedCleanupPhase::Polling);
        match reserved {
            TrackedCleanupPhase::Reserved(future) => {
                *phase = TrackedCleanupPhase::Active(future);
                drop(phase);
                if !state.activated.replace(true) {
                    self.handle.shared.poll_counters().cleanup_activated();
                }
                if let Some(local) = self.local.upgrade() {
                    schedule_hosted_cleanup(&local, &state, self.slot, &self.handle);
                    schedule_woken_cleanup(&local, &state, self.slot);
                }
                self.handle.shared.wake_driver();
            }
            other => {
                *phase = other;
            }
        }
    }
}

/// Observation handle for asynchronous cleanup owned by a worker driver.
///
/// Dropping or detaching this handle never cancels the cleanup future.
pub struct WorkerCleanupTask {
    state: Rc<TrackedCleanupState>,
    owner_thread: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl WorkerCleanupTask {
    /// Returns an observer of the cleanup's retirement, without taking the task.
    pub fn observe(&self) -> WorkerCleanupObserver {
        self.state.publisher.observe()
    }

    pub fn is_finished(&self) -> bool {
        matches!(&*self.state.phase.borrow(), TrackedCleanupPhase::Finished)
    }

    /// Drops this handle without cancelling the cleanup task.
    pub fn detach(self) {}
}

impl Future for WorkerCleanupTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        debug_assert_eq!(current_id(), self.owner_thread);
        if self.is_finished() {
            Poll::Ready(())
        } else {
            let mut waker = self.state.completion_waker.borrow_mut();
            if waker
                .as_ref()
                .is_none_or(|registered| !registered.will_wake(context.waker()))
            {
                *waker = Some(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// Finishes a cleanup's state, freeing `slot` in the hosted registry only if
/// it still holds this cleanup.
fn finish_tracked_cleanup(
    state: &Rc<TrackedCleanupState>,
    slot: usize,
    confirmed: bool,
) -> Option<PanicPayload> {
    *state.phase.borrow_mut() = TrackedCleanupPhase::Finished;
    if let Some(handle) = state.woken.get() {
        handle.node.queued.store(true, Ordering::Release);
    }
    if let Some(local) = state.local.upgrade() {
        local.shared.poll_counters().cleanup_finished(
            state.registered.replace(false),
            state.activated.replace(false),
        );
        debug_assert!(
            slot != NO_SLOT || local.hosted_aborting.get(),
            "a slotted cleanup finishes through its registry slot until the registry is taken"
        );
        local.hosted_cleanups.borrow_mut().remove(slot, state.id);
    }
    let outcome = if confirmed {
        Ok(())
    } else {
        Err(WorkerCleanupObservationError::Unconfirmed)
    };
    let mut first_panic = state.publisher.publish(outcome);
    let waker = state.completion_waker.borrow_mut().take();
    retain_first_panic(
        &mut first_panic,
        waker.and_then(|waker| catch_unwind(AssertUnwindSafe(|| waker.wake())).err()),
    );
    first_panic
}

/// `drop_slotted_cleanup` for a cleanup in a registry that shutdown has taken.
fn drop_tracked_cleanup(
    state: &Rc<TrackedCleanupState>,
    future: TrackedTask,
) -> Option<PanicPayload> {
    drop_slotted_cleanup(state, NO_SLOT, future)
}

fn drop_slotted_cleanup(
    state: &Rc<TrackedCleanupState>,
    slot: usize,
    future: TrackedTask,
) -> Option<PanicPayload> {
    let mut first_panic = catch_unwind(AssertUnwindSafe(|| drop(future))).err();
    let confirmed = first_panic.is_none()
        && !state
            .destruction_failed
            .as_ref()
            .is_some_and(|failed| failed.get());
    retain_first_panic(
        &mut first_panic,
        finish_tracked_cleanup(state, slot, confirmed),
    );
    first_panic
}

#[inline(always)]
fn poll_tracked_cleanup(
    state: &Rc<TrackedCleanupState>,
    slot: usize,
    mut future: TrackedTask,
    context: &mut Context<'_>,
) -> (bool, Option<PanicPayload>) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        future
            .task
            .as_mut()
            .expect("registered cleanup retains its future")
            .as_mut()
            .poll(context)
    }));
    if matches!(&result, Ok(Poll::Pending)) {
        *state.phase.borrow_mut() = TrackedCleanupPhase::Active(future);
        return (true, None);
    }
    finish_polled_cleanup(state, slot, future, result)
}

#[cold]
#[inline(never)]
fn finish_polled_cleanup(
    state: &Rc<TrackedCleanupState>,
    slot: usize,
    future: TrackedTask,
    result: Result<Poll<()>, PanicPayload>,
) -> (bool, Option<PanicPayload>) {
    let mut first_panic = match result {
        Err(payload) => Some(payload),
        Ok(Poll::Ready(())) => None,
        Ok(Poll::Pending) => unreachable!(),
    };
    retain_first_panic(&mut first_panic, drop_slotted_cleanup(state, slot, future));
    (false, first_panic)
}

#[cfg(test)]
mod observer_tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::rc::Weak;
    use std::task::Context;
    use std::task::Poll;

    use futures::task::noop_waker;

    use super::CleanupPublisher;
    use super::TrackedCleanupPhase;
    use super::TrackedCleanupState;
    use super::TrackedTask;
    use super::WorkerCleanupObservationError;
    use super::drop_tracked_cleanup;

    #[test]
    fn cleanup_observer_publisher_loss_is_unconfirmed() {
        let publisher = CleanupPublisher::new();
        let observer = publisher.observe();
        let clone = observer.clone();
        let mut waiting = Box::pin(observer.wait());
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        drop(publisher);
        let expected = Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed));
        assert_eq!(waiting.as_mut().poll(&mut context), expected);
        assert_eq!(Box::pin(clone.wait()).as_mut().poll(&mut context), expected);
        assert_eq!(
            Box::pin(observer.wait()).as_mut().poll(&mut context),
            expected
        );
    }

    #[test]
    fn cleanup_destruction_failure_in_poll_is_unconfirmed() {
        struct PanicDropFuture;

        impl Future for PanicDropFuture {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<()> {
                Poll::Ready(())
            }
        }

        impl Drop for PanicDropFuture {
            fn drop(&mut self) {
                panic!("cleanup future destructor failed");
            }
        }

        let destruction_failed = Rc::new(Cell::new(false));
        let state = Rc::new(TrackedCleanupState {
            id: 0,
            local: Weak::new(),
            scheduled: Cell::new(false),
            phase: RefCell::new(TrackedCleanupPhase::Finished),
            completion_waker: RefCell::new(None),
            destruction_failed: Some(Rc::clone(&destruction_failed)),
            publisher: CleanupPublisher::new(),
            registered: Cell::new(false),
            activated: Cell::new(false),
            woken: std::cell::OnceCell::new(),
            poll_waker_override: std::cell::OnceCell::new(),
        });
        let observer = state.publisher.observe();
        let mut task = TrackedTask {
            task: Some(Box::pin(PanicDropFuture)),
            completion: None,
            destruction_failed: Some(Rc::clone(&destruction_failed)),
        };
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let polled = catch_unwind(AssertUnwindSafe(|| {
            let _ = Pin::new(&mut task).poll(&mut context);
        }));
        assert!(polled.is_err(), "destructor panic should escape the poll");
        assert!(destruction_failed.get());
        assert!(drop_tracked_cleanup(&state, task).is_none());
        assert_eq!(
            Box::pin(observer.wait()).as_mut().poll(&mut context),
            Poll::Ready(Err(WorkerCleanupObservationError::Unconfirmed))
        );
    }
}

/// An owner-local lifetime guard. Its `Rc` marker prevents cross-thread drop.
pub struct WorkerGuard {
    local: Rc<OwnerState>,
    owner_thread: ThreadId,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        debug_assert_eq!(current_id(), self.owner_thread);
        let active = self.local.active_guards.get();
        debug_assert!(active > 0);
        self.local.active_guards.set(active.saturating_sub(1));
    }
}

/// Deterministic selection for new work. A selected handle remains bound to
/// its original worker even after later selections.
#[derive(Debug)]
pub struct WorkerSelector {
    workers: Box<[WorkerHandle]>,
    next: AtomicUsize,
}

impl WorkerSelector {
    pub fn new(workers: Vec<WorkerHandle>) -> Self {
        Self {
            workers: workers.into_boxed_slice(),
            next: AtomicUsize::new(0),
        }
    }

    pub fn select(&self) -> Option<WorkerHandle> {
        if self.workers.is_empty() {
            return None;
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        Some(self.workers[index].clone())
    }

    pub fn len(&self) -> usize {
        self.workers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}

struct ResponseState<T> {
    result: Option<Result<T, WorkerCallError>>,
    waker: Option<Waker>,
}

struct ResponseSender<T> {
    shared: Arc<Mutex<ResponseState<T>>>,
    sent: bool,
}

impl<T> ResponseSender<T> {
    fn send(mut self, value: T) {
        let waker = {
            let mut state = self
                .shared
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.result = Some(Ok(value));
            state.waker.take()
        };
        self.sent = true;
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for ResponseSender<T> {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        let waker = {
            let mut state = self
                .shared
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.result.is_none() {
                state.result = Some(Err(WorkerCallError::Cancelled));
            }
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// The job that `call_local` spawns: its job future, stored once, and the
/// sender for that future's output.
///
/// The `call_local` tests check its destruction order. An unpolled job drops
/// its sender and then its future. A started job drops its future and then its
/// sender. A completed job drops its future in place before it sends the
/// output. Whatever is still live when a poll unwinds is dropped during that
/// unwind, inside the poll.
struct CallLocalJob<Fut, T> {
    // Structurally pinned: never moved, and dropped in place once, when
    // `phase` leaves `Unpolled` or `Started` for `Retired`.
    future: ManuallyDrop<Fut>,
    response: Option<ResponseSender<T>>,
    phase: CallLocalPhase,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CallLocalPhase {
    Unpolled,
    Started,
    Retired,
}

impl<Fut, T> CallLocalJob<Fut, T> {
    fn new(future: Fut, response: ResponseSender<T>) -> Self {
        Self {
            future: ManuallyDrop::new(future),
            response: Some(response),
            phase: CallLocalPhase::Unpolled,
        }
    }

    fn retire_future(&mut self) {
        if std::mem::replace(&mut self.phase, CallLocalPhase::Retired) != CallLocalPhase::Retired {
            // SAFETY: the future was live, and `phase` records `Retired` before
            // its destructor runs, so it is dropped once and never used again,
            // even if that destructor unwinds.
            unsafe { ManuallyDrop::drop(&mut self.future) };
        }
    }

    /// Drops the future, if it is still live, and then the sender.
    fn retire(&mut self) {
        let response = self.response.take();
        self.retire_future();
        drop(response);
    }
}

/// Retires its job when dropped, including during an unwind.
struct RetireCallLocalJob<'a, Fut, T>(&'a mut CallLocalJob<Fut, T>);

impl<Fut, T> RetireCallLocalJob<'_, Fut, T> {
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl<Fut, T> Drop for RetireCallLocalJob<'_, Fut, T> {
    fn drop(&mut self) {
        self.0.retire();
    }
}

impl<Fut, T> Future for CallLocalJob<Fut, T>
where
    Fut: Future<Output = T>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `future` is the only structurally pinned field, and the
        // type is `Unpin` only when `Fut` is. This module never moves
        // `future` out of a job: `poll` polls it in place, and `retire_future`
        // drops it in place. The other fields are not pinned.
        let job = unsafe { self.get_unchecked_mut() };
        assert!(
            job.phase != CallLocalPhase::Retired,
            "a call_local job is not polled after it retires"
        );
        job.phase = CallLocalPhase::Started;
        let job = RetireCallLocalJob(job);
        // SAFETY: the phase is not `Retired`, so the future is live, and it
        // stays at this address until `retire_future` drops it in place, after
        // this borrow ends.
        let polled = unsafe { Pin::new_unchecked(&mut *job.0.future) }.poll(context);
        let Poll::Ready(output) = polled else {
            job.disarm();
            return Poll::Pending;
        };
        // If the future's drop unwinds, the output leaks, as it does in an
        // async block that awaits the future; the guard still drops the sender.
        let output = ManuallyDrop::new(output);
        job.0.retire_future();
        let response = job
            .0
            .response
            .take()
            .expect("a started call_local job retains its sender");
        job.disarm();
        response.send(ManuallyDrop::into_inner(output));
        Poll::Ready(())
    }
}

impl<Fut, T> Drop for CallLocalJob<Fut, T> {
    fn drop(&mut self) {
        let unpolled_response = if self.phase == CallLocalPhase::Unpolled {
            self.response.take()
        } else {
            None
        };
        let _retire = RetireCallLocalJob(self);
        drop(unpolled_response);
    }
}

pub struct WorkerCall<T> {
    shared: Option<Arc<Mutex<ResponseState<T>>>>,
    context: ContextEnvelope,
}

impl<T> Future for WorkerCall<T> {
    type Output = Result<T, WorkerCallError>;

    fn poll(self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let installed = this.context.clone();
        let (result, next_request) = installed.run_and_capture(|| {
            let mut state = this
                .shared
                .as_ref()
                .expect("a worker call retains its response state")
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(result) = state.result.take() {
                Poll::Ready(result)
            } else {
                state.waker = Some(task.waker().clone());
                Poll::Pending
            }
        });
        this.context.set_request_context(next_request);
        result
    }
}

impl<T> Drop for WorkerCall<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        self.context.run(|| drop(shared));
    }
}

fn response_channel<T>(context: ContextEnvelope) -> (ResponseSender<T>, WorkerCall<T>) {
    let shared = Arc::new(Mutex::new(ResponseState {
        result: None,
        waker: None,
    }));
    (
        ResponseSender {
            shared: Arc::clone(&shared),
            sent: false,
        },
        WorkerCall {
            shared: Some(shared),
            context,
        },
    )
}
