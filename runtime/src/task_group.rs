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

/// A slot index that never holds an entry, so lookups and removals there miss.
pub(crate) const NO_SLOT: usize = usize::MAX;

/// How a slab slot holds a live value together with the identifier it was
/// inserted under.
pub(crate) trait SlabEntry {
    type Value;

    fn new(id: u64, value: Self::Value) -> Self;

    fn id(&self) -> u64;

    fn value(&self) -> &Self::Value;

    fn into_value(self) -> Self::Value;
}

/// A value stored beside the identifier it was inserted under.
pub(crate) struct Keyed<T>(u64, T);

impl<T> SlabEntry for Keyed<T> {
    type Value = T;

    fn new(id: u64, value: T) -> Self {
        Self(id, value)
    }

    fn id(&self) -> u64 {
        self.0
    }

    fn value(&self) -> &T {
        &self.1
    }

    fn into_value(self) -> T {
        self.1
    }
}

/// Owner-local entries addressed by slot, each holding the identifier it was
/// inserted under. Lookup and removal check both, so a stale key never
/// reaches the entry that reused its slot. Freed slots are reused last-freed
/// first, so storage stays at the peak live count. Paths that need insertion
/// order sort the live entries by identifier.
pub(crate) struct Slab<E: SlabEntry> {
    slots: Vec<Slot<E>>,
    free: usize,
    live: usize,
}

/// A slab that stores each value beside its identifier.
pub(crate) type IdSlab<T> = Slab<Keyed<T>>;

enum Slot<E> {
    Occupied(E),
    Vacant(usize),
}

impl<E: SlabEntry> Slab<E> {
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

    pub(crate) fn insert(&mut self, id: u64, value: E::Value) -> usize {
        let entry = Slot::Occupied(E::new(id, value));
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

    pub(crate) fn get(&self, slot: usize, id: u64) -> Option<&E::Value> {
        match self.slots.get(slot)? {
            Slot::Occupied(entry) if entry.id() == id => Some(entry.value()),
            _ => None,
        }
    }

    /// Vacates the slot only if it holds `id`; the freed value returns to
    /// the caller to drop.
    pub(crate) fn remove(&mut self, slot: usize, id: u64) -> Option<E::Value> {
        let entry = self.slots.get_mut(slot)?;
        if !matches!(entry, Slot::Occupied(stored) if stored.id() == id) {
            return None;
        }
        let Slot::Occupied(stored) = mem::replace(entry, Slot::Vacant(self.free)) else {
            unreachable!("the slot was checked to be occupied");
        };
        self.free = slot;
        self.live -= 1;
        Some(stored.into_value())
    }

    /// The live entries' identifiers and slots, in ascending identifier order.
    pub(crate) fn keys_by_id(&self) -> Vec<(u64, usize)> {
        let mut keys: Vec<_> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| match entry {
                Slot::Occupied(entry) => Some((entry.id(), slot)),
                Slot::Vacant(_) => None,
            })
            .collect();
        keys.sort_unstable_by_key(|&(id, _)| id);
        keys
    }

    /// Takes every live entry, in ascending identifier order.
    pub(crate) fn into_sorted(mut self) -> Vec<(u64, E::Value)> {
        self.live = 0;
        let mut entries: Vec<_> = mem::take(&mut self.slots)
            .into_iter()
            .filter_map(|entry| match entry {
                Slot::Occupied(entry) => Some((entry.id(), entry.into_value())),
                Slot::Vacant(_) => None,
            })
            .collect();
        entries.sort_unstable_by_key(|&(id, _)| id);
        entries
    }
}

impl<E: SlabEntry> Default for Slab<E> {
    fn default() -> Self {
        Self::new()
    }
}

/// Drops the live entries in ascending identifier order, as `BTreeMap` does,
/// continuing past one whose drop panics.
impl<E: SlabEntry> Drop for Slab<E> {
    fn drop(&mut self) {
        if self.live != 0 {
            drop(mem::take(self).into_sorted());
        }
    }
}

/// The result of a task, reported after its future was dropped on the owner thread.
///
/// A panic payload stays owned by this value until it is dropped. Clean up state
/// keyed by `id` before dropping it, because dropping the payload may itself panic.
/// [`Self::discard`] drops the payload and suppresses panics from its destructor,
/// leaking it after a bounded number of attempts.
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

/// What a task's group span observes, kept inline in the task's state, which
/// is sized to this kind of body alone.
trait TaskBody: Sized + 'static {
    /// The task table's handle on a state of this kind.
    fn handle(state: Rc<TaskState<Self>>) -> TaskHandle;

    /// One poll of the body under its own `catch_unwind`.
    fn poll_body(&mut self, context: &mut Context<'_>) -> Result<Poll<()>, Payload>;
}

/// A user future.
type FutureBody = ObservedTask<LocalFuture>;

/// A hosted worker command with its own command span.
type CommandBody = ObservedTask<CommandTask>;

/// An activated hosted worker cleanup.
type CleanupBody = ObservedTask<HostedCleanup>;

impl TaskBody for FutureBody {
    fn handle(state: Rc<TaskState<Self>>) -> TaskHandle {
        TaskHandle::Task(state)
    }

    #[inline(always)]
    fn poll_body(&mut self, context: &mut Context<'_>) -> Result<Poll<()>, Payload> {
        poll_observed(self, context)
    }
}

impl TaskBody for CommandBody {
    fn handle(state: Rc<TaskState<Self>>) -> TaskHandle {
        TaskHandle::Command(state)
    }

    #[cfg(feature = "observability")]
    #[inline(always)]
    fn poll_body(&mut self, context: &mut Context<'_>) -> Result<Poll<()>, Payload> {
        poll_observed(self, context)
    }

    #[cfg(not(feature = "observability"))]
    #[inline(always)]
    fn poll_body(&mut self, context: &mut Context<'_>) -> Result<Poll<()>, Payload> {
        poll_command(self, context)
    }
}

impl TaskBody for CleanupBody {
    fn handle(state: Rc<TaskState<Self>>) -> TaskHandle {
        TaskHandle::Cleanup(state)
    }

    #[inline(always)]
    fn poll_body(&mut self, context: &mut Context<'_>) -> Result<Poll<()>, Payload> {
        poll_observed(self, context)
    }
}

type Payload = Box<dyn Any + Send>;

/// The fields that every poll reads lead the struct, ahead of the body.
#[repr(C)]
struct TaskState<B> {
    completed: Cell<bool>,
    polling: Cell<bool>,
    abort_requested: Cell<bool>,
    tracked: bool,
    host_waker: Cell<Option<Waker>>,
    body: RefCell<Option<B>>,
    id: TaskId,
    panic: RefCell<Option<Box<dyn Any + Send>>>,
    completions: Weak<Completions>,
}

/// A task table entry: the task's state, typed by its body kind. It is its own
/// slab entry, because the state holds the task's identifier.
enum TaskHandle {
    Task(Rc<TaskState<FutureBody>>),
    Command(Rc<TaskState<CommandBody>>),
    Cleanup(Rc<TaskState<CleanupBody>>),
}

/// Runs `$action` with `$state` bound to the handle's state, whatever its kind.
macro_rules! with_state {
    ($handle:expr, $state:ident => $action:expr) => {
        match $handle {
            TaskHandle::Task($state) => $action,
            TaskHandle::Command($state) => $action,
            TaskHandle::Cleanup($state) => $action,
        }
    };
}

impl TaskHandle {
    fn task_id(&self) -> TaskId {
        with_state!(self, state => state.id)
    }

    fn is_completed(&self) -> bool {
        with_state!(self, state => state.completed.get())
    }

    fn finish(&self, result: Result<(), TaskGroupError>) {
        with_state!(self, state => state.finish(result))
    }

    fn take_panic(&self) -> Option<Payload> {
        with_state!(self, state => state.panic.borrow_mut().take())
    }

    fn discard_panic(&self) {
        with_state!(self, state => state.discard_panic())
    }

    #[cfg(test)]
    fn kind(&self) -> TaskKind {
        match self {
            Self::Task(_) => TaskKind::Task,
            Self::Command(_) => TaskKind::Command,
            Self::Cleanup(_) => TaskKind::Cleanup,
        }
    }
}

impl SlabEntry for TaskHandle {
    type Value = Self;

    fn new(id: u64, handle: Self) -> Self {
        debug_assert_eq!(handle.task_id().seq, id);
        handle
    }

    fn id(&self) -> u64 {
        self.task_id().seq
    }

    fn value(&self) -> &Self {
        self
    }

    fn into_value(self) -> Self {
        self
    }
}

impl<B> TaskState<B> {
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
        poll_body: impl FnOnce(&mut B, &mut Context<'_>) -> Result<Poll<()>, Payload>,
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

impl<B> Drop for TaskState<B> {
    fn drop(&mut self) {
        self.discard_panic();
    }
}

/// The wrapper that a task group spawns on its host. It holds one kind of
/// state, so its poll reaches the body without choosing among the body kinds.
struct HostedTask<B>(Rc<TaskState<B>>);

#[cfg(test)]
thread_local! {
    static GROUP_STATE_ALLOCATIONS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static BOXED_TASK_FUTURES: Cell<usize> = const { Cell::new(0) };
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

/// The size of the wrapper that a task group spawns on its host for a user
/// future.
#[cfg(all(test, feature = "observability"))]
pub(crate) fn hosted_task_size() -> usize {
    size_of::<HostedTask<FutureBody>>()
}

/// A task body kind, as the state size records name it.
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum TaskKind {
    Task,
    Command,
    Cleanup,
}

#[cfg(test)]
thread_local! {
    static TASK_STATE_SIZES: Cell<[usize; 3]> = const { Cell::new([0; 3]) };
}

/// The size of the state allocation that this thread's task groups last made
/// for a task of `kind`, or zero before the first.
#[cfg(test)]
pub(crate) fn task_state_size(kind: TaskKind) -> usize {
    TASK_STATE_SIZES.get()[kind as usize]
}

/// Records the size of `state`'s allocation, which holds the `Rc`'s two
/// counts ahead of the state.
#[cfg(test)]
fn record_task_state_size<S>(kind: TaskKind, state: &Rc<S>) {
    let mut sizes = TASK_STATE_SIZES.get();
    sizes[kind as usize] = 2 * size_of::<usize>() + size_of_val(&**state);
    TASK_STATE_SIZES.set(sizes);
}

/// The size of one slot of a task group's task table.
#[cfg(test)]
pub(crate) fn task_table_slot_size() -> usize {
    size_of::<Slot<TaskHandle>>()
}

/// `TaskState` field offsets: the body's, the latest among the body kinds,
/// and those of the fields that every poll reads, which do not depend on the
/// body kind.
#[cfg(test)]
pub(crate) struct TaskStateOffsets {
    pub(crate) body: usize,
    pub(crate) per_poll: [(&'static str, usize); 4],
}

#[cfg(test)]
pub(crate) fn task_state_offsets() -> TaskStateOffsets {
    type State = TaskState<FutureBody>;
    TaskStateOffsets {
        body: mem::offset_of!(State, body)
            .max(mem::offset_of!(TaskState<CommandBody>, body))
            .max(mem::offset_of!(TaskState<CleanupBody>, body)),
        per_poll: [
            ("completed", mem::offset_of!(State, completed)),
            ("polling", mem::offset_of!(State, polling)),
            ("abort_requested", mem::offset_of!(State, abort_requested)),
            ("host_waker", mem::offset_of!(State, host_waker)),
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

impl<B: TaskBody> Future for HostedTask<B> {
    type Output = ();

    #[inline(never)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.0.poll_hosted(context, B::poll_body)
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

/// Polls a hosted command's job directly under the command context.
///
/// The group context would wrap only the command install, so it is installed
/// only on the terminal paths that run code under it. Under `observability`,
/// a command body polls with `poll_observed` instead, which keeps the group
/// install and a `catch_unwind` around the poll so that the subscriber receives
/// the command span's enter and exit under the group context.
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

impl<B> Drop for HostedTask<B> {
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
    tasks: Slab<TaskHandle>,
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
            tasks: Slab::new(),
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
        self.spawn_body(factory, tracked, |task, future| {
            #[cfg(test)]
            BOXED_TASK_FUTURES.set(BOXED_TASK_FUTURES.get() + 1);
            FutureBody::new(task, Box::pin(future))
        })
    }

    /// Admits a worker command, tracked, as `spawn_local_tracked` does with
    /// a factory that returns it, and keeps it inline beneath the group span.
    pub(crate) fn spawn_command(&mut self, command: CommandTask) -> Result<TaskId, TaskGroupError> {
        self.spawn_body(move |_| command, true, CommandBody::new)
    }

    /// Admits an activated hosted worker cleanup, tracked, as `spawn_command`
    /// admits a command, and keeps it inline beneath the group span.
    pub(crate) fn spawn_cleanup(
        &mut self,
        cleanup: HostedCleanup,
    ) -> Result<TaskId, TaskGroupError> {
        self.spawn_body(move |_| cleanup, true, CleanupBody::new)
    }

    /// Admits the body that `body` builds, in a state of its own kind, and
    /// spawns that kind's wrapper around the state on the host.
    #[inline(always)]
    fn spawn_body<F, T, B>(
        &mut self,
        factory: F,
        tracked: bool,
        body: impl FnOnce(OperationSpan, T) -> B,
    ) -> Result<TaskId, TaskGroupError>
    where
        F: FnOnce(CancellationToken) -> T,
        B: TaskBody,
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
        let handle = B::handle(Rc::clone(&state));
        #[cfg(test)]
        record_task_state_size(handle.kind(), &state);
        let slot = self.tasks.insert(seq, handle);
        debug_assert_eq!(slot, id.slot);
        self.host.spawn_unboxed(HostedTask(state));
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
            completion.panic = state.take_panic();
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
        if state.is_completed() {
            return false;
        }
        state.finish(Err(TaskGroupError::TaskCancelled));
        true
    }

    /// Closes admission and aborts all remaining futures on their owner thread.
    pub fn abort_all(&mut self) {
        self.abort_remaining(false);
    }

    /// Like `abort_all`. With `take_panic`, also returns the first panic that a task raises as
    /// this abort destroys it.
    fn abort_remaining(&mut self, take_panic: bool) -> Option<Payload> {
        self.close();
        let mut raised = None;
        for (seq, slot) in self.tasks.keys_by_id() {
            let Some(state) = self.tasks.get(slot, seq) else {
                continue;
            };
            let destroyed = !state.is_completed();
            state.finish(Err(TaskGroupError::TaskCancelled));
            if take_panic && destroyed && raised.is_none() {
                raised = state.take_panic();
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
    /// after the teardown deadline. It is never cleared, because the thread only tears down from
    /// then on.
    static FORCED_TEARDOWN: Cell<bool> = const { Cell::new(false) };
}

/// Returns whether this thread is in a `HostedWorker`'s forced teardown.
///
/// That happens when the worker misses its teardown deadline and its supervisor destroys the
/// driver and every unfinished cleanup. Every later drop on the thread is part of that teardown,
/// which abandons unjoined work. Drops that normally assert that their work was joined, such as
/// a task group's or an owned session driver's, skip that assertion but still raise any panic
/// the work raises as it is destroyed.
pub fn in_forced_teardown() -> bool {
    FORCED_TEARDOWN.get()
}

/// Marks this owner thread's teardown as forced.
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
                    // The cancellation's panic takes precedence. The task's payload is disposed
                    // of here, because a panicking payload destructor would abort the process if
                    // it ran while the cancellation's panic unwinds.
                    if let Some(raised) = raised {
                        crate::panic::discard_panic(raised);
                    }
                    Some(payload)
                }
                Ok(()) => raised,
            };
            if let Some(payload) = payload {
                // Resuming while the thread is already unwinding would abort the process.
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
