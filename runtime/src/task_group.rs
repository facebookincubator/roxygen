/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;

use crate::cancellation::CancellationSource;
use crate::cancellation::CancellationToken;
use crate::context::ContextEnvelope;
use crate::host::Host;
use crate::host::LocalFuture;
use crate::observability::ObservedTask;
use crate::observability::OperationKind;
use crate::observability::OperationSpan;
use crate::observability::TerminalOutcome;
use crate::thread::current_id;
use crate::worker::CommandTask;
use crate::worker::HostedCleanup;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TaskGroupError {
    #[error("task group is closed to new work")]
    Closed,
    #[error("task group task was cancelled before completing")]
    TaskCancelled,
    #[error("task group task panicked")]
    TaskPanicked,
    #[error("task group task identifiers are exhausted")]
    IdentifierExhausted,
}

/// A stable identity within one task group, never reused by that group.
///
/// Equality, ordering, hashing, and `Debug` use the group's sequence number
/// alone; the slot only locates the task in its group's table.
#[derive(Clone, Copy)]
pub struct TaskId {
    seq: u64,
    slot: usize,
}

impl PartialEq for TaskId {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}

impl Eq for TaskId {}

impl PartialOrd for TaskId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq.cmp(&other.seq)
    }
}

impl Hash for TaskId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seq.hash(state);
    }
}

impl fmt::Debug for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("TaskId").field(&self.seq).finish()
    }
}

/// The slot of no slab entry; removal there finds nothing.
pub(crate) const NO_SLOT: usize = usize::MAX;

/// Owner-local entries addressed by slot, each stored with the identifier it
/// was inserted under. Lookup and removal check both, so a stale key never
/// reaches the entry that reused its slot. Freed slots are reused last-freed
/// first, so storage stays at the peak live count. Paths that need insertion
/// order sort the live entries by identifier.
pub(crate) struct IdSlab<T> {
    slots: Vec<Slot<T>>,
    free: usize,
    live: usize,
}

enum Slot<T> {
    Occupied(u64, T),
    Vacant(usize),
}

impl<T> IdSlab<T> {
    pub(crate) const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: NO_SLOT,
            live: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.live
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Every slot the slab stores, occupied or vacant.
    #[cfg(test)]
    pub(crate) fn slots(&self) -> usize {
        self.slots.len()
    }

    /// The slot that the next `insert` fills.
    pub(crate) fn vacant_slot(&self) -> usize {
        if self.free == NO_SLOT {
            self.slots.len()
        } else {
            self.free
        }
    }

    pub(crate) fn insert(&mut self, id: u64, value: T) -> usize {
        let entry = Slot::Occupied(id, value);
        let slot = match self.slots.get_mut(self.free) {
            Some(vacant) => {
                let Slot::Vacant(next) = *vacant else {
                    unreachable!("the free list holds only vacant slots");
                };
                *vacant = entry;
                mem::replace(&mut self.free, next)
            }
            None => {
                self.slots.push(entry);
                self.slots.len() - 1
            }
        };
        self.live += 1;
        slot
    }

    pub(crate) fn get(&self, slot: usize, id: u64) -> Option<&T> {
        match self.slots.get(slot)? {
            Slot::Occupied(stored, value) if *stored == id => Some(value),
            _ => None,
        }
    }

    /// Vacates the slot only if it holds `id`; the freed value returns to
    /// the caller to drop.
    pub(crate) fn remove(&mut self, slot: usize, id: u64) -> Option<T> {
        let entry = self.slots.get_mut(slot)?;
        if !matches!(entry, Slot::Occupied(stored, _) if *stored == id) {
            return None;
        }
        let Slot::Occupied(_, value) = mem::replace(entry, Slot::Vacant(self.free)) else {
            unreachable!("the slot was checked to be occupied");
        };
        self.free = slot;
        self.live -= 1;
        Some(value)
    }

    /// The live entries' identifiers and slots, in ascending identifier order.
    pub(crate) fn keys_by_id(&self) -> Vec<(u64, usize)> {
        let mut keys: Vec<_> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| match entry {
                Slot::Occupied(id, _) => Some((*id, slot)),
                Slot::Vacant(_) => None,
            })
            .collect();
        keys.sort_unstable_by_key(|&(id, _)| id);
        keys
    }

    /// Takes every live entry, in ascending identifier order.
    pub(crate) fn into_sorted(mut self) -> Vec<(u64, T)> {
        self.live = 0;
        let mut entries: Vec<_> = mem::take(&mut self.slots)
            .into_iter()
            .filter_map(|entry| match entry {
                Slot::Occupied(id, value) => Some((id, value)),
                Slot::Vacant(_) => None,
            })
            .collect();
        entries.sort_unstable_by_key(|&(id, _)| id);
        entries
    }
}

impl<T> Default for IdSlab<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Drops the live entries in ascending identifier order, as `BTreeMap` does,
/// continuing past one whose drop panics.
impl<T> Drop for IdSlab<T> {
    fn drop(&mut self) {
        if self.live != 0 {
            drop(mem::take(self).into_sorted());
        }
    }
}

/// Completion follows owner-thread destruction of the task's future.
///
/// A panic payload remains owned here until this value is dropped. Retire state
/// keyed by `id` before dropping it: payload destruction may itself panic.
/// `discard` provides the task group's bounded, panic-suppressing disposal policy.
pub struct TaskCompletion {
    pub id: TaskId,
    pub result: Result<(), TaskGroupError>,
    panic: Option<Box<dyn Any + Send>>,
    _not_send: PhantomData<Rc<()>>,
}

impl TaskCompletion {
    /// Disposes any panic payload without propagating a payload destructor panic.
    pub fn discard(mut self) {
        if let Some(payload) = self.panic.take() {
            crate::panic::discard_panic(payload);
        }
    }
}

impl fmt::Debug for TaskCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskCompletion")
            .field("id", &self.id)
            .field("result", &self.result)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Completions {
    tracked: RefCell<VecDeque<TaskCompletion>>,
    untracked: RefCell<VecDeque<TaskCompletion>>,
    error: Cell<Option<TaskGroupError>>,
    waker: RefCell<Option<Waker>>,
}

/// What a task's group span observes: a user future, a hosted worker command
/// with its own command span, or an activated hosted worker cleanup.
enum TaskBody {
    Task(ObservedTask<LocalFuture>),
    Command(ObservedTask<CommandTask>),
    Cleanup(ObservedTask<HostedCleanup>),
}

// The command variant stays the largest, so the cleanup variant adds nothing
// to `TaskState`.
const _: () = assert!(size_of::<ObservedTask<HostedCleanup>>() <= size_of::<CommandTask>());

type Payload = Box<dyn Any + Send>;

/// The fields that every poll reads lead the struct, ahead of the body.
#[repr(C)]
struct TaskState {
    completed: Cell<bool>,
    polling: Cell<bool>,
    abort_requested: Cell<bool>,
    tracked: bool,
    host_waker: Cell<Option<Waker>>,
    body: RefCell<Option<TaskBody>>,
    id: TaskId,
    panic: RefCell<Option<Box<dyn Any + Send>>>,
    completions: Weak<Completions>,
}

impl TaskState {
    fn finish(&self, mut result: Result<(), TaskGroupError>) {
        if self.polling.get() {
            self.abort_requested.set(true);
            return;
        }
        if self.completed.replace(true) {
            return;
        }
        let body = self.body.borrow_mut().take();
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(body)));
        if let Err(payload) = dropped {
            result = Err(TaskGroupError::TaskPanicked);
            self.retain_panic(payload);
        }
        let host_waker = self.host_waker.take();
        if let Some(waker) = host_waker
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake()))
        {
            crate::panic::discard_panic(payload);
        }
        if let Some(completions) = self.completions.upgrade() {
            if let Err(error) = result
                && completions.error.get().is_none()
            {
                completions.error.set(Some(error));
            }
            let queue = if self.tracked {
                &completions.tracked
            } else {
                &completions.untracked
            };
            queue.borrow_mut().push_back(TaskCompletion {
                id: self.id,
                result,
                panic: None,
                _not_send: PhantomData,
            });
            let waker = completions.waker.borrow_mut().take();
            if let Some(waker) = waker
                && let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake()))
            {
                crate::panic::discard_panic(payload);
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn finish_polled(&self, polled: Result<Poll<()>, Box<dyn Any + Send>>) {
        self.host_waker.take();
        match polled {
            Ok(Poll::Pending) => self.finish(Err(TaskGroupError::TaskCancelled)),
            Ok(Poll::Ready(())) => self.finish(Ok(())),
            Err(payload) => {
                self.retain_panic(payload);
                self.finish(Err(TaskGroupError::TaskPanicked));
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn replace_host_waker(&self, previous: Option<Waker>, waker: &Waker) {
        self.host_waker.set(Some(waker.clone()));
        drop(previous);
    }

    /// One host poll of the task's wrapper, with `poll_body` polling the body
    /// under its own `catch_unwind`.
    #[inline(always)]
    fn poll_hosted(
        &self,
        context: &mut Context<'_>,
        poll_body: impl FnOnce(&mut TaskBody, &mut Context<'_>) -> Result<Poll<()>, Payload>,
    ) -> Poll<()> {
        if self.completed.get() {
            return Poll::Ready(());
        }
        let waker = context.waker();
        match self.host_waker.take() {
            Some(current) if current.will_wake(waker) => self.host_waker.set(Some(current)),
            previous => self.replace_host_waker(previous, waker),
        }
        self.polling.set(true);
        let polled = {
            // While `polling` is set, `finish` defers instead of borrowing the body.
            let mut body = self.body.borrow_mut();
            poll_body(
                body.as_mut().expect("a live task retains its body"),
                context,
            )
        };
        self.polling.set(false);
        match polled {
            Ok(Poll::Pending) if !self.abort_requested.get() => Poll::Pending,
            polled => {
                self.finish_polled(polled);
                Poll::Ready(())
            }
        }
    }

    fn retain_panic(&self, payload: Box<dyn Any + Send>) {
        if self.panic.borrow().is_some() {
            crate::panic::discard_panic(payload);
        } else {
            *self.panic.borrow_mut() = Some(payload);
        }
    }

    fn discard_panic(&self) {
        let payload = self.panic.borrow_mut().take();
        if let Some(payload) = payload {
            crate::panic::discard_panic(payload);
        }
    }
}

impl Drop for TaskState {
    fn drop(&mut self) {
        self.discard_panic();
    }
}

struct HostedTask(Rc<TaskState>);

/// The wrapper that a task group spawns for an activated hosted cleanup. Its
/// poll reaches the cleanup body without choosing among the body kinds.
struct HostedCleanupTask(Rc<TaskState>);

#[cfg(test)]
thread_local! {
    static GROUP_STATE_ALLOCATIONS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static BOXED_TASK_FUTURES: Cell<usize> = const { Cell::new(0) };
    static TASK_BODY_DISPATCHES: Cell<usize> = const { Cell::new(0) };
}

/// How many host polls on this thread have chosen among the task body kinds.
#[cfg(test)]
pub(crate) fn task_body_dispatches() -> usize {
    TASK_BODY_DISPATCHES.get()
}

/// How many `Completions` and group `CancellationSource`s this thread's task
/// groups have created.
#[cfg(test)]
pub(crate) fn group_state_allocations() -> (usize, usize) {
    GROUP_STATE_ALLOCATIONS.get()
}

/// How many task futures this thread's task groups have boxed.
#[cfg(test)]
pub(crate) fn boxed_task_futures() -> usize {
    BOXED_TASK_FUTURES.get()
}

/// The size of the wrapper that a task group spawns on its host.
#[cfg(all(test, feature = "observability"))]
pub(crate) fn hosted_task_size() -> usize {
    size_of::<HostedTask>()
}

/// `TaskState` field offsets: the body's, and those of the fields that every
/// poll reads.
#[cfg(test)]
pub(crate) struct TaskStateOffsets {
    pub(crate) body: usize,
    pub(crate) per_poll: [(&'static str, usize); 4],
}

#[cfg(test)]
pub(crate) fn task_state_offsets() -> TaskStateOffsets {
    TaskStateOffsets {
        body: mem::offset_of!(TaskState, body),
        per_poll: [
            ("completed", mem::offset_of!(TaskState, completed)),
            ("polling", mem::offset_of!(TaskState, polling)),
            (
                "abort_requested",
                mem::offset_of!(TaskState, abort_requested),
            ),
            ("host_waker", mem::offset_of!(TaskState, host_waker)),
        ],
    }
}

fn new_completions() -> Rc<Completions> {
    #[cfg(test)]
    GROUP_STATE_ALLOCATIONS.set({
        let (completions, cancellations) = GROUP_STATE_ALLOCATIONS.get();
        (completions + 1, cancellations)
    });
    Rc::default()
}

fn new_cancellation_source() -> CancellationSource {
    #[cfg(test)]
    GROUP_STATE_ALLOCATIONS.set({
        let (completions, cancellations) = GROUP_STATE_ALLOCATIONS.get();
        (completions, cancellations + 1)
    });
    CancellationSource::new()
}

impl Future for HostedTask {
    type Output = ();

    #[inline(never)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.0.poll_hosted(context, |body, context| {
            #[cfg(test)]
            TASK_BODY_DISPATCHES.set(TASK_BODY_DISPATCHES.get() + 1);
            match body {
                TaskBody::Task(task) => poll_observed(task, context),
                #[cfg(feature = "observability")]
                TaskBody::Command(command) => poll_observed(command, context),
                #[cfg(not(feature = "observability"))]
                TaskBody::Command(command) => poll_command(command, context),
                TaskBody::Cleanup(_) => {
                    unreachable!("a hosted cleanup is spawned as a `HostedCleanupTask`")
                }
            }
        })
    }
}

impl Future for HostedCleanupTask {
    type Output = ();

    #[inline(never)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.0.poll_hosted(context, |body, context| {
            let TaskBody::Cleanup(cleanup) = body else {
                unreachable!("a `HostedCleanupTask` holds a cleanup body")
            };
            poll_observed(cleanup, context)
        })
    }
}

/// Polls an observed future under one `catch_unwind`. A ready poll's drop and
/// span close stay inside it, and a panicking poll's cleanup runs after it,
/// with the poll's payload alive.
#[inline(always)]
fn poll_observed<F>(
    task: &mut ObservedTask<F>,
    context: &mut Context<'_>,
) -> Result<Poll<()>, Payload>
where
    F: Future<Output = ()> + Unpin,
{
    #[cfg(test)]
    crate::observability::count_task_catch_unwind();
    let mut finishing = false;
    let polled = catch_unwind(AssertUnwindSafe(|| {
        if task.poll_unguarded(context).is_pending() {
            return Poll::Pending;
        }
        finishing = true;
        task.finish_ready();
        Poll::Ready(())
    }));
    match polled {
        Err(payload) if !finishing => Err(clean_up_panic(payload, || task.finish_unwind())),
        polled => polled,
    }
}

#[cfg(not(feature = "observability"))]
#[derive(Clone, Copy, Eq, PartialEq)]
enum CommandPhase {
    Polling,
    Finishing,
    GroupFinishing,
}

/// Polls a hosted command's job directly under the command context. Nothing
/// runs between the group and command installs, so the group context is
/// installed only on the terminal paths that run under it. Under
/// `observability`, `HostedTask::poll` uses `poll_observed` instead, which
/// keeps the group install, and a `catch_unwind` inside it, around the poll so
/// that the subscriber sees the command span's enter and exit under the group
/// context.
#[cfg(not(feature = "observability"))]
#[inline(always)]
fn poll_command(
    command: &mut ObservedTask<CommandTask>,
    context: &mut Context<'_>,
) -> Result<Poll<()>, Payload> {
    #[cfg(test)]
    crate::observability::count_task_catch_unwind();
    let mut phase = CommandPhase::Polling;
    let polled = catch_unwind(AssertUnwindSafe(|| {
        let job = command
            .future_mut()
            .and_then(CommandTask::job_mut)
            .expect("a polled command retains its job");
        if job.poll_unguarded(context).is_pending() {
            return Poll::Pending;
        }
        phase = CommandPhase::Finishing;
        let finished = command.poll_unguarded_with(|command| {
            command.finish_ready();
            Poll::Ready(())
        });
        debug_assert!(finished.is_ready());
        phase = CommandPhase::GroupFinishing;
        command.finish_ready();
        Poll::Ready(())
    }));
    match (polled, phase) {
        (Err(payload), CommandPhase::Polling) => {
            let group = command.envelope().clone();
            let job = command
                .future_mut()
                .and_then(CommandTask::job_mut)
                .expect("a panicking command retains its job");
            let payload = clean_up_panic_under(&group, payload, || job.finish_unwind());
            Err(clean_up_panic(payload, || command.finish_unwind()))
        }
        (Err(payload), CommandPhase::Finishing) => {
            Err(clean_up_panic(payload, || command.finish_unwind()))
        }
        (polled, _) => polled,
    }
}

/// Runs a panicking poll's cleanup while that poll's payload stays alive. If
/// the cleanup panics, the payload drops during that unwind, and the
/// cleanup's payload is the one returned.
#[cold]
#[inline(never)]
fn clean_up_panic(payload: Payload, cleanup: impl FnOnce()) -> Payload {
    match catch_unwind(AssertUnwindSafe(move || {
        let payload = payload;
        cleanup();
        payload
    })) {
        Ok(payload) | Err(payload) => payload,
    }
}

/// `clean_up_panic` with `envelope` installed around the cleanup and the
/// payload's drop.
#[cfg(not(feature = "observability"))]
#[cold]
#[inline(never)]
fn clean_up_panic_under(
    envelope: &ContextEnvelope,
    payload: Payload,
    cleanup: impl FnOnce(),
) -> Payload {
    match catch_unwind(AssertUnwindSafe(move || {
        envelope.run(move || {
            let payload = payload;
            cleanup();
            payload
        })
    })) {
        Ok(payload) | Err(payload) => payload,
    }
}

impl Drop for HostedTask {
    fn drop(&mut self) {
        self.0.finish(Err(TaskGroupError::TaskCancelled));
    }
}

impl Drop for HostedCleanupTask {
    fn drop(&mut self) {
        self.0.finish(Err(TaskGroupError::TaskCancelled));
    }
}

/// Owner-local structured task set for non-`Send` work.
///
/// Closing admission precedes cancellation. Dropping a join future retains
/// every task and completion. Owners must explicitly close and join the group.
/// The host only schedules wrappers; this group owns the user futures, stable
/// identities, cancellation, panic isolation, and completion records.
///
/// A group creates its completion records and cancellation source at its
/// first spawn, and its task table keeps the slots of its peak live count.
pub struct TaskGroup {
    host: Host,
    tasks: IdSlab<Rc<TaskState>>,
    completions: Option<Rc<Completions>>,
    next_id: u64,
    cancellation: Option<CancellationSource>,
    accepting: bool,
    shutdown_span: Option<OperationSpan>,
}

impl TaskGroup {
    pub fn new() -> Self {
        Self::with_host(Host::tokio())
    }

    pub fn with_host(host: Host) -> Self {
        Self {
            host,
            tasks: IdSlab::new(),
            completions: None,
            next_id: 0,
            cancellation: None,
            accepting: true,
            shutdown_span: None,
        }
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.host.owner_thread()
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation
            .as_ref()
            .is_some_and(CancellationSource::is_cancellation_requested)
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Every slot the task table stores, occupied or vacant.
    #[cfg(test)]
    pub(crate) fn task_slots(&self) -> usize {
        self.tasks.slots()
    }

    #[cfg(test)]
    pub(crate) fn set_next_task_seq(&mut self, next: u64) {
        self.next_id = next;
    }

    pub fn spawn_local<F, Fut>(&mut self, factory: F) -> Result<(), TaskGroupError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + 'static,
    {
        self.reap_finished();
        self.spawn(factory, false).map(|_| ())
    }

    /// Admits a task whose completion ID remains available to `poll_join_next`.
    /// Convenience spawning and `reap_finished` never consume tracked IDs.
    pub fn spawn_local_tracked<F, Fut>(&mut self, factory: F) -> Result<TaskId, TaskGroupError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + 'static,
    {
        self.spawn(factory, true)
    }

    fn spawn<F, Fut>(&mut self, factory: F, tracked: bool) -> Result<TaskId, TaskGroupError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + 'static,
    {
        self.spawn_body(
            factory,
            tracked,
            |task, future| {
                #[cfg(test)]
                BOXED_TASK_FUTURES.set(BOXED_TASK_FUTURES.get() + 1);
                TaskBody::Task(ObservedTask::new(task, Box::pin(future)))
            },
            HostedTask,
        )
    }

    /// Admits a worker command, tracked, as `spawn_local_tracked` does with
    /// a factory that returns it, and keeps it inline beneath the group span.
    pub(crate) fn spawn_command(&mut self, command: CommandTask) -> Result<TaskId, TaskGroupError> {
        self.spawn_body(
            move |_| command,
            true,
            |task, command| TaskBody::Command(ObservedTask::new(task, command)),
            HostedTask,
        )
    }

    /// Admits an activated hosted worker cleanup, tracked, as `spawn_command`
    /// admits a command, and keeps it inline beneath the group span.
    pub(crate) fn spawn_cleanup(
        &mut self,
        cleanup: HostedCleanup,
    ) -> Result<TaskId, TaskGroupError> {
        self.spawn_body(
            move |_| cleanup,
            true,
            |task, cleanup| TaskBody::Cleanup(ObservedTask::new(task, cleanup)),
            HostedCleanupTask,
        )
    }

    /// Admits the body that `body` builds and spawns `wrapper` around its
    /// state on the host.
    #[inline(always)]
    fn spawn_body<F, T, W>(
        &mut self,
        factory: F,
        tracked: bool,
        body: impl FnOnce(OperationSpan, T) -> TaskBody,
        wrapper: impl FnOnce(Rc<TaskState>) -> W,
    ) -> Result<TaskId, TaskGroupError>
    where
        F: FnOnce(CancellationToken) -> T,
        W: Future<Output = ()> + 'static,
    {
        self.assert_owner_thread();
        let context = ContextEnvelope::capture();
        if !self.accepting {
            let mut rejected = OperationSpan::start(OperationKind::Shutdown, &context);
            context.run(|| drop(factory));
            rejected.complete(TerminalOutcome::Shutdown);
            return Err(TaskGroupError::Closed);
        }
        let next_id = self
            .next_id
            .checked_add(1)
            .ok_or(TaskGroupError::IdentifierExhausted)?;
        let seq = self.next_id;
        self.next_id = next_id;
        let cancellation = &*self
            .cancellation
            .get_or_insert_with(new_cancellation_source);
        let completions = &*self.completions.get_or_insert_with(new_completions);
        let mut task = OperationSpan::start(OperationKind::Command, &context);
        let future = match catch_unwind(AssertUnwindSafe(|| {
            task.in_scope(|| factory(cancellation.token()))
        })) {
            Ok(future) => future,
            Err(payload) => {
                task.complete(TerminalOutcome::Failure);
                resume_unwind(payload)
            }
        };
        let id = TaskId {
            seq,
            slot: self.tasks.vacant_slot(),
        };
        let state = Rc::new(TaskState {
            completed: Cell::new(false),
            polling: Cell::new(false),
            abort_requested: Cell::new(false),
            tracked,
            host_waker: Cell::new(None),
            body: RefCell::new(Some(body(task, future))),
            id,
            panic: RefCell::new(None),
            completions: Rc::downgrade(completions),
        });
        let slot = self.tasks.insert(seq, Rc::clone(&state));
        debug_assert_eq!(slot, id.slot);
        self.host.spawn_unboxed(wrapper(state));
        Ok(id)
    }

    /// Reaps completed untracked tasks; explicitly tracked completion IDs remain.
    pub fn reap_finished(&mut self) {
        self.assert_owner_thread();
        loop {
            let completion = self
                .completions
                .as_ref()
                .and_then(|completions| completions.untracked.borrow_mut().pop_front());
            let Some(completion) = completion else {
                break;
            };
            if let Some(state) = self.tasks.remove(completion.id.slot, completion.id.seq) {
                state.discard_panic();
            }
        }
        self.finish_shutdown();
    }

    /// Reports whether a completion is available to `try_join_next`.
    pub fn has_ready_completion(&self) -> bool {
        self.completions.as_ref().is_some_and(|completions| {
            !completions.tracked.borrow().is_empty() || !completions.untracked.borrow().is_empty()
        })
    }

    /// Consumes one available completion without closing admission.
    pub fn try_join_next(&mut self) -> Option<TaskCompletion> {
        self.assert_owner_thread();
        let completions = self.completions.as_ref()?;
        let mut completion = completions
            .tracked
            .borrow_mut()
            .pop_front()
            .or_else(|| completions.untracked.borrow_mut().pop_front())?;
        if let Some(state) = self.tasks.remove(completion.id.slot, completion.id.seq) {
            completion.panic = state.panic.borrow_mut().take();
        }
        self.finish_shutdown();
        Some(completion)
    }

    /// Consumes one completed task, or waits without closing admission.
    pub fn poll_join_next(&mut self, context: &mut Context<'_>) -> Poll<Option<TaskCompletion>> {
        if let Some(completion) = self.try_join_next() {
            return Poll::Ready(Some(completion));
        }
        let Some(completions) = &self.completions else {
            debug_assert!(self.tasks.is_empty());
            return Poll::Ready(None);
        };
        if self.tasks.is_empty() {
            return Poll::Ready(None);
        }
        // A waker clone may finish a task in this group, which borrows the
        // waker slot, so no borrow is held while it runs.
        let stale = completions
            .waker
            .borrow()
            .as_ref()
            .is_none_or(|stored| !stored.will_wake(context.waker()));
        if stale {
            let waker = context.waker().clone();
            let previous = completions.waker.replace(Some(waker));
            drop(previous);
        }
        Poll::Pending
    }

    pub fn close(&mut self) -> bool {
        self.assert_owner_thread();
        self.reap_finished();
        if !self.accepting {
            return false;
        }
        self.accepting = false;
        let context = ContextEnvelope::capture();
        self.shutdown_span = Some(OperationSpan::start(OperationKind::Shutdown, &context));
        if self.tasks.is_empty() {
            self.finish_shutdown();
        }
        true
    }

    pub fn cancel(&mut self) -> bool {
        self.close();
        let context = ContextEnvelope::capture();
        let mut cancellation = OperationSpan::start(OperationKind::Cancellation, &context);
        let requested = cancellation.in_scope(|| {
            self.cancellation
                .get_or_insert_with(new_cancellation_source)
                .request_cancellation()
        });
        cancellation.complete(TerminalOutcome::Success);
        requested
    }

    /// Destroys one task on its owner thread and retains its cancellation result.
    /// The host's scheduled wrapper becomes inert and retains no user future.
    /// The result for an id from another group is unspecified.
    pub fn abort(&mut self, id: TaskId) -> bool {
        self.assert_owner_thread();
        let Some(state) = self.tasks.get(id.slot, id.seq) else {
            return false;
        };
        if state.completed.get() {
            return false;
        }
        state.finish(Err(TaskGroupError::TaskCancelled));
        true
    }

    /// Closes admission and aborts all remaining futures on their owner thread.
    pub fn abort_all(&mut self) {
        self.abort_remaining(false);
    }

    /// `abort_all`, which with `take_panic` also takes the first panic that a task raises as
    /// this abort destroys it.
    fn abort_remaining(&mut self, take_panic: bool) -> Option<Payload> {
        self.close();
        let mut raised = None;
        for (seq, slot) in self.tasks.keys_by_id() {
            let Some(state) = self.tasks.get(slot, seq) else {
                continue;
            };
            let destroyed = !state.completed.get();
            state.finish(Err(TaskGroupError::TaskCancelled));
            if take_panic && destroyed && raised.is_none() {
                raised = state.panic.borrow_mut().take();
            }
        }
        raised
    }

    pub async fn join(&mut self) -> Result<(), TaskGroupError> {
        self.close();
        while let Some(completion) = poll_fn(|context| self.poll_join_next(context)).await {
            completion.discard();
        }
        self.finish_shutdown();
        self.completions
            .as_ref()
            .and_then(|completions| completions.error.get())
            .map_or(Ok(()), Err)
    }

    pub async fn cancel_and_join(&mut self) -> Result<(), TaskGroupError> {
        self.cancel();
        self.join().await
    }

    fn assert_owner_thread(&self) {
        assert_eq!(current_id(), self.owner_thread());
    }

    fn finish_shutdown(&mut self) {
        if self.accepting || !self.tasks.is_empty() {
            return;
        }
        if let Some(mut shutdown) = self.shutdown_span.take() {
            let failed = self
                .completions
                .as_ref()
                .is_some_and(|completions| completions.error.get().is_some());
            shutdown.complete(if failed {
                TerminalOutcome::Failure
            } else {
                TerminalOutcome::Success
            });
        }
    }
}

impl Default for TaskGroup {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// Set on a `HostedWorker`'s owner thread when its supervisor destroys the worker's driver
    /// past the teardown deadline (`DIV-043`), and never cleared: the thread only tears down
    /// from then on.
    static FORCED_TEARDOWN: Cell<bool> = const { Cell::new(false) };
}

/// Roxygen-only (`DIV-043`): whether this thread is the owner thread of a `HostedWorker` whose
/// teardown passed its deadline, so that its supervisor destroyed the worker's driver and every
/// unfinished cleanup. Every later drop on the thread belongs to that forced teardown, which
/// abandons the work it finds unjoined: a drop that asserts an ordinary teardown joined its
/// work, as a task group's or an owned session driver's does, does not assert it then, but
/// still raises a panic that work raises as it is destroyed.
pub fn in_forced_teardown() -> bool {
    FORCED_TEARDOWN.get()
}

/// Marks this owner thread's teardown as forced (`DIV-043`).
pub(crate) fn begin_forced_teardown() {
    FORCED_TEARDOWN.set(true);
}

impl Drop for TaskGroup {
    fn drop(&mut self) {
        self.assert_owner_thread();
        let had_tasks = !self.tasks.is_empty();
        if had_tasks {
            self.accepting = false;
            let cancellation = catch_unwind(AssertUnwindSafe(|| {
                if let Some(source) = &self.cancellation {
                    source.request_cancellation();
                }
            }));
            let raised = self.abort_remaining(in_forced_teardown());
            let payload = match cancellation {
                Err(payload) => {
                    // The cancellation's panic wins. The task's is disposed of here: dropped as
                    // the cancellation's panic unwinds, a payload whose destructor panics would
                    // abort the process.
                    if let Some(raised) = raised {
                        crate::panic::discard_panic(raised);
                    }
                    Some(payload)
                }
                Ok(()) => raised,
            };
            if let Some(payload) = payload {
                // Resumed while the thread already unwinds, it would abort the process.
                if std::thread::panicking() {
                    crate::panic::discard_panic(payload);
                } else {
                    resume_unwind(payload);
                }
            }
        }
        assert!(
            !self.accepting || in_forced_teardown(),
            "task group must be explicitly closed before drop"
        );
        assert!(
            !had_tasks || in_forced_teardown(),
            "task group must join every admitted task before drop"
        );
    }
}
