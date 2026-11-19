/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use thiserror::Error;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::ids::StreamId;
use crate::ingress_completion::IngressCompletion;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;

struct SourceEntry {
    identity: Rc<()>,
    source: Box<dyn HttpSource>,
    /// The task of the last in-place poll of this generation that returned
    /// `Pending`. It is woken if the generation goes away while that read is
    /// still active, because the source may never wake it. A replaced waker, and
    /// the waker of a retired or finished generation, are dropped outside the
    /// holder's borrow.
    reader: Option<Waker>,
}

impl SourceEntry {
    fn new(source: Box<dyn HttpSource>) -> Self {
        Self {
            identity: Rc::new(()),
            source,
            reader: None,
        }
    }
}

#[derive(Default)]
struct HolderState {
    source: Option<SourceEntry>,
    active_read: bool,
    forwarded: ForwardedPoll,
}

/// The read of a consumer that polls the holder itself as a source, through
/// `HttpSource::poll_body_event`. Such a read has no read future, so a poll that
/// follows a `Pending` poll continues the same read.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum ForwardedPoll {
    #[default]
    Idle,
    /// The last poll returned `Pending`, and the current generation's `reader` is its waker.
    Pending,
    /// The generation of a pending poll went away, so the next poll fails with `DROPPED`.
    Orphaned,
}

impl HolderState {
    /// Takes the current generation out for retirement and installs `replacement`.
    /// A read waiting on it, through a read future or a forwarded poll, is woken
    /// after the retirement stops and drops the source, and then fails.
    fn retire_generation(
        &mut self,
        replacement: Option<SourceEntry>,
        error: Option<HttpErrorCode>,
    ) -> SourceRetirement {
        let active_read = std::mem::take(&mut self.active_read);
        let forwarded = self.orphan_forwarded_poll();
        let previous = match replacement {
            Some(entry) => self.source.replace(entry),
            None => self.source.take(),
        };
        SourceRetirement::of(previous, error, true, active_read || forwarded)
    }

    /// Returns whether a forwarded poll is pending on the current generation,
    /// which is going away, and marks it so that its next poll fails.
    fn orphan_forwarded_poll(&mut self) -> bool {
        let pending = self.forwarded == ForwardedPoll::Pending;
        if pending {
            self.forwarded = ForwardedPoll::Orphaned;
        }
        pending
    }
}

pub(crate) type RetirementPanic = Box<dyn std::any::Any + Send>;

pub(crate) fn retain_retirement_panic(
    first: &mut Option<RetirementPanic>,
    next: Option<RetirementPanic>,
) {
    if let Some(payload) = next {
        if first.is_none() {
            *first = Some(payload);
        } else if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    }
}

#[inline]
pub(crate) fn finish_retirement_panic(first: Option<RetirementPanic>, unwinding: bool) {
    if let Some(payload) = first {
        finish_retirement_payload(payload, unwinding);
    }
}

#[cold]
#[inline(never)]
fn finish_retirement_payload(payload: RetirementPanic, unwinding: bool) {
    if unwinding {
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    } else {
        resume_unwind(payload);
    }
}

pub(crate) fn retire_source<S>(
    mut source: S,
    stop: impl FnOnce(&mut S),
) -> Option<RetirementPanic> {
    let mut first = catch_unwind(AssertUnwindSafe(|| stop(&mut source))).err();
    retain_retirement_panic(
        &mut first,
        catch_unwind(AssertUnwindSafe(|| drop(source))).err(),
    );
    first
}

pub(crate) struct SourceRetirement {
    source: Option<Box<dyn HttpSource>>,
    error: Option<HttpErrorCode>,
    /// Whether the source is unfinished and so is stopped before it is dropped.
    stop: bool,
    /// The generation's cached reader, dropped once its source is stopped and
    /// dropped, and first woken if `wake_reader`.
    reader: Option<Waker>,
    wake_reader: bool,
}

impl SourceRetirement {
    /// Prepares a generation taken out of its holder for retirement outside the
    /// holder's borrow. Retirement stops the source if `stop` and drops it, then
    /// wakes the cached reader if `wake_reader` and drops it.
    fn of(
        entry: Option<SourceEntry>,
        error: Option<HttpErrorCode>,
        stop: bool,
        wake_reader: bool,
    ) -> Self {
        let (source, reader) =
            entry.map_or((None, None), |entry| (Some(entry.source), entry.reader));
        Self {
            source,
            error,
            stop,
            reader,
            wake_reader,
        }
    }

    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        let (stop, error) = (self.stop, self.error);
        let mut first = self.source.take().and_then(|source| {
            retire_source(source, |source| {
                if stop {
                    source.stop_reading(error);
                }
            })
        });
        retain_retirement_panic(
            &mut first,
            retire_reader(self.reader.take(), self.wake_reader),
        );
        first
    }

    pub(crate) fn retire(mut self) -> std::thread::Result<()> {
        self.retire_inner().map_or(Ok(()), Err)
    }
}

impl Drop for SourceRetirement {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let first = self.retire_inner();
        finish_retirement_panic(first, unwinding);
    }
}

/// Wakes (if `wake`) and drops a generation's cached reader, each in its own
/// panic containment, so a panicking wake cannot drop the waker while unwinding.
#[inline]
fn retire_reader(reader: Option<Waker>, wake: bool) -> Option<RetirementPanic> {
    reader.and_then(|reader| retire_some_reader(reader, wake))
}

#[cold]
#[inline(never)]
fn retire_some_reader(reader: Waker, wake: bool) -> Option<RetirementPanic> {
    let mut first = if wake {
        catch_unwind(AssertUnwindSafe(|| reader.wake_by_ref())).err()
    } else {
        None
    };
    retain_retirement_panic(
        &mut first,
        catch_unwind(AssertUnwindSafe(|| drop(reader))).err(),
    );
    first
}

/// Drops a generation consumed by a terminal event or error, without stopping
/// it and outside the holder's borrow. The source is dropped before the reader.
#[inline]
fn retire_finished(entry: Option<SourceEntry>) {
    if entry.is_some() {
        drop(SourceRetirement::of(entry, None, false, false));
    }
}

/// Consumes the generation that `take` removes, as an error would, while a
/// source call is unwinding. The source is dropped without `stop_reading`, then
/// the cached reader. Every panic here, including one from `take`, is
/// discarded, because this runs in a destructor during that unwind.
#[cold]
#[inline(never)]
fn retire_panicked(take: impl FnOnce() -> Option<SourceEntry>) {
    let retired = catch_unwind(AssertUnwindSafe(|| {
        SourceRetirement::of(take(), None, false, false).retire()
    }));
    if let Err(secondary) | Ok(Err(secondary)) = retired {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

/// Retires the current generation if the source call that it guards unwinds.
/// It is declared before the holder's borrow, so the unwind releases the borrow
/// first.
struct SourceCallGuard<'a> {
    state: &'a RefCell<HolderState>,
    armed: bool,
}

impl Drop for SourceCallGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            retire_panicked(|| {
                let mut state = self.state.borrow_mut();
                state.forwarded = ForwardedPoll::Idle;
                state.source.take()
            });
        }
    }
}

/// [`SourceCallGuard`] for a poll that owns the holder's state outright.
struct ExclusivePollGuard<'a> {
    source: &'a mut Option<SourceEntry>,
    armed: bool,
}

impl Drop for ExclusivePollGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            retire_panicked(|| self.source.take());
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceReleaseError {
    #[error("cannot release an HTTP source while a read is active")]
    ReadInProgress,
}

trait TerminalEvent {
    fn is_terminal(&self) -> bool;
}

impl TerminalEvent for HttpHeaderEvent {
    fn is_terminal(&self) -> bool {
        self.eom()
    }
}

impl TerminalEvent for HttpBodyEvent {
    fn is_terminal(&self) -> bool {
        self.eom()
    }
}

/// Polls a holder's source in place for one event (`HttpSource::poll_body_event`).
type InPlacePoll<T> = fn(&mut dyn HttpSource, &mut Context<'_>, u32) -> Poll<Result<T, HttpError>>;

enum ReadInner<T> {
    /// The future the source returned when the read started.
    Future(LocalSourceFuture<Result<T, HttpError>>),
    /// The source itself, polled while the holder's state is borrowed.
    InPlace { poll: InPlacePoll<T>, maximum: u32 },
    /// A read that panicked while polled. Its future has been dropped.
    Retired,
}

struct HolderReadFuture<T>
where
    T: TerminalEvent,
{
    state: Rc<RefCell<HolderState>>,
    source_identity: Rc<()>,
    inner: ReadInner<T>,
    completed: bool,
}

fn poll_source_body(
    source: &mut dyn HttpSource,
    context: &mut Context<'_>,
    maximum: u32,
) -> Poll<Result<HttpBodyEvent, HttpError>> {
    source.poll_body_event(context, maximum)
}

/// Polls the source generation that the read started on, failing with `DROPPED`
/// if a stop, clear, or replacement has made it unavailable since. A `Pending`
/// poll registers the read's task with the generation, which wakes the task if
/// the generation goes away.
fn poll_in_place<T>(
    state: &RefCell<HolderState>,
    source_identity: &Rc<()>,
    poll: InPlacePoll<T>,
    maximum: u32,
    context: &mut Context<'_>,
) -> Poll<Result<T, HttpError>> {
    {
        let Ok(mut state) = state.try_borrow_mut() else {
            return Poll::Ready(Err(HttpSourceHolder::concurrent_read_error()));
        };
        match state.source.as_mut() {
            Some(entry) if Rc::ptr_eq(&entry.identity, source_identity) => {
                let polled = poll(entry.source.as_mut(), context, maximum);
                if polled.is_ready() || reader_registered(entry, context) {
                    return polled;
                }
                if entry.reader.is_none() {
                    entry.reader = Some(context.waker().clone());
                    return polled;
                }
            }
            _ => return Poll::Ready(Err(HttpSourceHolder::unavailable_error())),
        }
    }
    if replace_reader(state, source_identity, context, true) {
        Poll::Pending
    } else {
        Poll::Ready(Err(HttpSourceHolder::unavailable_error()))
    }
}

/// Whether the generation's cached reader already wakes this poll's task.
#[inline(always)]
fn reader_registered(entry: &SourceEntry, context: &Context<'_>) -> bool {
    entry
        .reader
        .as_ref()
        .is_some_and(|reader| reader.will_wake(context.waker()))
}

/// Whether a read of generation `source_identity` still waits on it. The read is
/// an admitted read if `admitted`, and otherwise a pending forwarded poll.
fn reader_waits(state: &HolderState, source_identity: &Rc<()>, admitted: bool) -> bool {
    (if admitted {
        state.active_read
    } else {
        !state.active_read && state.forwarded == ForwardedPoll::Pending
    }) && state
        .source
        .as_ref()
        .is_some_and(|entry| Rc::ptr_eq(&entry.identity, source_identity))
}

/// The slow path of a `Pending` poll when its task is not the cached reader.
/// The new waker is cloned, and the waker it replaces is dropped, outside the
/// holder's borrow, because cloning or dropping a waker may reach the holder.
/// Returns whether the read still waits on its generation afterwards.
#[cold]
#[inline(never)]
fn replace_reader(
    state: &RefCell<HolderState>,
    source_identity: &Rc<()>,
    context: &Context<'_>,
    admitted: bool,
) -> bool {
    let mut replacement = Some(context.waker().clone());
    let previous = {
        let mut state = state.borrow_mut();
        if reader_waits(&state, source_identity, admitted) {
            let entry = state.source.as_mut().expect("a waiting read has a source");
            std::mem::replace(&mut entry.reader, replacement.take())
        } else {
            None
        }
    };
    let mut first = retire_reader(previous, false);
    retain_retirement_panic(&mut first, retire_reader(replacement, false));
    finish_retirement_panic(first, std::thread::panicking());
    reader_waits(&state.borrow(), source_identity, admitted)
}

impl<T> Future for HolderReadFuture<T>
where
    T: TerminalEvent,
{
    type Output = Result<T, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.completed,
            "a completed holder read cannot be polled again"
        );
        let poll = {
            let mut guard = ReadPollGuard {
                read: &mut *this,
                armed: true,
            };
            let poll = guard.read.poll_inner(context);
            guard.armed = false;
            poll
        };
        let Poll::Ready(result) = &poll else {
            return Poll::Pending;
        };
        this.completed = true;
        let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
        retire_finished(finish_read(&this.state, &this.source_identity, terminal));
        poll
    }
}

impl<T> HolderReadFuture<T>
where
    T: TerminalEvent,
{
    fn poll_inner(&mut self, context: &mut Context<'_>) -> Poll<Result<T, HttpError>> {
        match &mut self.inner {
            ReadInner::Future(inner) => inner.as_mut().poll(context),
            ReadInner::InPlace { poll, maximum } => {
                poll_in_place(&self.state, &self.source_identity, *poll, *maximum, context)
            }
            ReadInner::Retired => unreachable!("a retired holder read is completed"),
        }
    }

    /// Completes a read while its poll is unwinding. The future is dropped before
    /// the generation is retired, because the future may still reach the source.
    #[cold]
    #[inline(never)]
    fn complete_panicked(&mut self) {
        self.completed = true;
        let inner = std::mem::replace(&mut self.inner, ReadInner::Retired);
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
        retire_panicked(|| finish_read(&self.state, &self.source_identity, true));
    }
}

struct ReadPollGuard<'a, T>
where
    T: TerminalEvent,
{
    read: &'a mut HolderReadFuture<T>,
    armed: bool,
}

impl<T> Drop for ReadPollGuard<'_, T>
where
    T: TerminalEvent,
{
    fn drop(&mut self) {
        if self.armed {
            self.read.complete_panicked();
        }
    }
}

impl<T> Drop for HolderReadFuture<T>
where
    T: TerminalEvent,
{
    fn drop(&mut self) {
        if !self.completed {
            release_read(&self.state, &self.source_identity);
        }
    }
}

enum HolderRead<T>
where
    T: TerminalEvent,
{
    Read(HolderReadFuture<T>),
    Error(Option<HttpError>),
}

impl<T> Future for HolderRead<T>
where
    T: TerminalEvent,
{
    type Output = Result<T, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Read(read) => Pin::new(read).poll(context),
            Self::Error(error) => Poll::Ready(Err(error
                .take()
                .expect("a completed holder read cannot be polled again"))),
        }
    }
}

/// The header read [`HttpSourceHolder::read_header_event_unboxed`] returns.
///
/// It owns its read state and retires a terminal source generation exactly as
/// the boxed holder read does.
pub struct HolderHeaderRead(HolderRead<HttpHeaderEvent>);

impl Future for HolderHeaderRead {
    type Output = Result<HttpHeaderEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context)
    }
}

/// The body read [`HttpSourceHolder::read_body_event_unboxed`] returns.
///
/// It owns its read state and retires a terminal source generation exactly as
/// the boxed holder read does.
pub struct HolderBodyRead(HolderRead<HttpBodyEvent>);

impl Future for HolderBodyRead {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context)
    }
}

/// Owns one local source and cleans it up on every unfinished path.
///
/// A terminal event or error consumes only the source generation that
/// produced it. Early drop, explicit replacement, and explicit stop call
/// `stop_reading` exactly once before destroying that source. The internal
/// `Rc` also makes the holder and every read future owner-local.
///
/// A read that panics, while starting or while polled, consumes its source
/// generation as an error does. The source is dropped without `stop_reading`,
/// and then the panic resumes. The same applies when an in-place poll panics,
/// whether through [`Self::poll_body_event_exclusive`] or through the holder
/// polled as a source. It also applies when [`HttpSource::polls_body_events`]
/// panics on the holder while no read is active.
pub struct HttpSourceHolder {
    state: Rc<RefCell<HolderState>>,
}

impl HttpSourceHolder {
    pub fn new(source: Box<dyn HttpSource>) -> Self {
        Self {
            state: Rc::new(RefCell::new(HolderState {
                source: Some(SourceEntry::new(source)),
                ..HolderState::default()
            })),
        }
    }

    pub fn empty() -> Self {
        Self {
            state: Rc::new(RefCell::new(HolderState::default())),
        }
    }

    pub fn readable(&self) -> bool {
        self.state.borrow().source.is_some()
    }

    pub const fn filter_name(&self) -> &'static str {
        "SourceHolder"
    }

    /// Takes the source out without stopping it. Fails with
    /// [`SourceReleaseError::ReadInProgress`] while a read is active.
    ///
    /// A consumer with a pending poll of the holder as a source is woken, and its
    /// next poll fails. If that wake, or dropping the generation's cached waker,
    /// panics, the source is stopped and dropped instead, and the panic resumes.
    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, SourceReleaseError> {
        let mut retirement = {
            let mut state = self.state.borrow_mut();
            if state.active_read {
                return Err(SourceReleaseError::ReadInProgress);
            }
            let forwarded = state.orphan_forwarded_poll();
            SourceRetirement::of(state.source.take(), None, true, forwarded)
        };
        let mut first = retire_reader(retirement.reader.take(), retirement.wake_reader);
        if first.is_some() {
            retain_retirement_panic(&mut first, retirement.retire_inner());
            finish_retirement_panic(first, std::thread::panicking());
        }
        Ok(retirement.source.take())
    }

    pub fn set_source(&mut self, source: Box<dyn HttpSource>) {
        let retirement = self
            .state
            .borrow_mut()
            .retire_generation(Some(SourceEntry::new(source)), None);
        let unwinding = std::thread::panicking();
        finish_retirement_panic(retirement.retire().err(), unwinding);
    }

    pub fn clear(&mut self, error: Option<HttpErrorCode>) {
        let unwinding = std::thread::panicking();
        let retirement = self.prepare_clear(error);
        finish_retirement_panic(retirement.retire().err(), unwinding);
    }

    pub(crate) fn prepare_clear(&mut self, error: Option<HttpErrorCode>) -> SourceRetirement {
        self.state.borrow_mut().retire_generation(None, error)
    }

    /// Starts a header read without boxing the holder's read future.
    ///
    /// Admission and the underlying source call happen immediately. The returned
    /// future owns its read state and does not retain this mutable borrow.
    pub fn read_header_event_unboxed(&mut self) -> HolderHeaderRead {
        let mut guard = SourceCallGuard {
            state: &self.state,
            armed: false,
        };
        let (inner, source_identity) = {
            let Ok(mut state) = self.state.try_borrow_mut() else {
                return HolderHeaderRead(HolderRead::Error(Some(Self::concurrent_read_error())));
            };
            if state.active_read {
                return HolderHeaderRead(HolderRead::Error(Some(Self::concurrent_read_error())));
            }
            let Some(entry) = state.source.as_mut() else {
                return HolderHeaderRead(HolderRead::Error(Some(Self::unavailable_error())));
            };
            guard.armed = true;
            let inner = ReadInner::Future(entry.source.read_header_event());
            guard.armed = false;
            let source_identity = Rc::clone(&entry.identity);
            state.active_read = true;
            state.forwarded = ForwardedPoll::Idle;
            (inner, source_identity)
        };
        HolderHeaderRead(HolderRead::Read(HolderReadFuture {
            state: Rc::clone(&self.state),
            source_identity,
            inner,
            completed: false,
        }))
    }

    /// Starts a body read without boxing the holder's read future.
    ///
    /// Like [`Self::read_header_event_unboxed`], this admits the read immediately
    /// and returns a future independent of this mutable borrow. If the source's
    /// [`HttpSource::polls_body_events`] is `true`, the source is not called here.
    /// Instead each poll of the returned future polls it in place, and fails with
    /// `DROPPED` once the source has been stopped, cleared, replaced, or destroyed
    /// with the holder. A read pending at that point is woken, because its source
    /// may never wake it.
    pub fn read_body_event_unboxed(&mut self, maximum: u32) -> HolderBodyRead {
        self.read_body_event_concrete(maximum)
    }

    pub(crate) fn read_body_event_concrete(&mut self, maximum: u32) -> HolderBodyRead {
        let mut guard = SourceCallGuard {
            state: &self.state,
            armed: false,
        };
        let (inner, source_identity) = {
            let Ok(mut state) = self.state.try_borrow_mut() else {
                return HolderBodyRead(HolderRead::Error(Some(Self::concurrent_read_error())));
            };
            if state.active_read {
                return HolderBodyRead(HolderRead::Error(Some(Self::concurrent_read_error())));
            }
            let Some(entry) = state.source.as_mut() else {
                return HolderBodyRead(HolderRead::Error(Some(Self::unavailable_error())));
            };
            guard.armed = true;
            let inner = if entry.source.polls_body_events() {
                ReadInner::InPlace {
                    poll: poll_source_body,
                    maximum,
                }
            } else {
                ReadInner::Future(entry.source.read_body_event(maximum))
            };
            guard.armed = false;
            let source_identity = Rc::clone(&entry.identity);
            state.active_read = true;
            state.forwarded = ForwardedPoll::Idle;
            (inner, source_identity)
        };
        HolderBodyRead(HolderRead::Read(HolderReadFuture {
            state: Rc::clone(&self.state),
            source_identity,
            inner,
            completed: false,
        }))
    }

    /// Polls the held source in place for a consumer that owns this holder
    /// outright.
    ///
    /// This works like [`HttpSource::poll_body_event`] on the holder, with the
    /// caller's `context` passed through unchanged. Only the owner can clear, stop,
    /// replace, release or destroy the generation it polls, so it does not need to be
    /// woken when that generation goes away, or to fail its next poll after it
    /// installs a replacement itself. When there is no read of this holder, no other
    /// handle to its state, and no pending forwarded poll, this call does not borrow
    /// the state or register a task. Otherwise it is the forwarded poll.
    ///
    /// Call it only while [`HttpSource::polls_body_events`] is `true`. A terminal
    /// event or error consumes the source, as it does for a read.
    pub fn poll_body_event_exclusive(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        // A unique state has no read future, so no read is active and the state is unborrowed.
        let Some(state) = Rc::get_mut(&mut self.state)
            .map(RefCell::get_mut)
            .filter(|state| state.forwarded == ForwardedPoll::Idle)
        else {
            return HttpSource::poll_body_event(self, context, maximum);
        };
        debug_assert!(!state.active_read, "an active read holds the state");
        let mut guard = ExclusivePollGuard {
            source: &mut state.source,
            armed: false,
        };
        let Some(entry) = guard.source.as_mut() else {
            return Poll::Ready(Err(Self::unavailable_error()));
        };
        guard.armed = true;
        let poll = entry.source.poll_body_event(context, maximum);
        guard.armed = false;
        if let Poll::Ready(result) = &poll {
            if result.as_ref().map_or(true, TerminalEvent::is_terminal) {
                retire_finished(guard.source.take());
            }
        }
        poll
    }

    pub(crate) fn unavailable_error() -> HttpError {
        HttpError::with_message(HttpErrorCode::DROPPED, b"HTTP source is no longer readable")
    }

    fn concurrent_read_error() -> HttpError {
        HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"HTTP source already has an active read",
        )
    }

    #[cfg(test)]
    pub(crate) fn readable_probe(&self) -> Box<dyn Fn() -> bool> {
        let state = Rc::clone(&self.state);
        Box::new(move || state.borrow().source.is_some())
    }
}

impl Default for HttpSourceHolder {
    fn default() -> Self {
        Self::empty()
    }
}

impl HttpSource for HttpSourceHolder {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(self.read_header_event_unboxed())
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(self.read_body_event_unboxed(maximum))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.clear(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.state
            .try_borrow()
            .ok()?
            .source
            .as_ref()
            .and_then(|entry| entry.source.get_stream_id())
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return;
        };
        if let Some(entry) = state.source.as_mut() {
            entry.source.set_read_timeout(timeout);
        }
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.state
            .try_borrow()
            .ok()?
            .source
            .as_ref()
            .and_then(|entry| entry.source.ingress_completion())
    }

    /// While no read is active, a panic in the source's `polls_body_events`
    /// consumes its generation without stopping it, as it does when a read starts.
    fn polls_body_events(&self) -> bool {
        let mut guard = SourceCallGuard {
            state: &self.state,
            armed: false,
        };
        let Ok(state) = self.state.try_borrow() else {
            return false;
        };
        let Some(entry) = state.source.as_ref() else {
            return false;
        };
        guard.armed = !state.active_read;
        let polls = entry.source.polls_body_events();
        guard.armed = false;
        polls
    }

    /// Polls the held source in place, outside any read of this holder. A
    /// terminal event or error consumes the source, as it does for a read.
    ///
    /// A poll that follows a `Pending` poll continues that read, with the same
    /// guarantees as an in-place read. The `Pending` poll registers its waker with
    /// the generation it polled, which wakes it when the generation is cleared,
    /// stopped, replaced, released or destroyed. The next poll then fails with
    /// `DROPPED` and never reaches a replacement source. The holder cannot detect a
    /// consumer that abandoned its pending poll, so the first poll after such a
    /// change fails the same way. A read of this holder resets that state.
    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let mut guard = SourceCallGuard {
            state: &self.state,
            armed: false,
        };
        let (poll, retired, replaced) = {
            let Ok(mut state) = self.state.try_borrow_mut() else {
                return Poll::Ready(Err(Self::concurrent_read_error()));
            };
            if state.active_read {
                return Poll::Ready(Err(Self::concurrent_read_error()));
            }
            let state = &mut *state;
            let orphaned = std::mem::take(&mut state.forwarded) == ForwardedPoll::Orphaned;
            let Some(entry) = state.source.as_mut().filter(|_| !orphaned) else {
                return Poll::Ready(Err(Self::unavailable_error()));
            };
            guard.armed = true;
            let poll = entry.source.poll_body_event(context, maximum);
            guard.armed = false;
            match &poll {
                Poll::Pending => {
                    state.forwarded = ForwardedPoll::Pending;
                    if reader_registered(entry, context) {
                        return poll;
                    }
                    if entry.reader.is_none() {
                        entry.reader = Some(context.waker().clone());
                        return poll;
                    }
                    (poll, None, Some(Rc::clone(&entry.identity)))
                }
                Poll::Ready(result) => {
                    let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
                    (poll, terminal.then(|| state.source.take()).flatten(), None)
                }
            }
        };
        retire_finished(retired);
        if let Some(identity) = replaced {
            if !replace_reader(&self.state, &identity, context, false) {
                return Poll::Ready(Err(Self::unavailable_error()));
            }
        }
        poll
    }
}

impl Drop for HttpSourceHolder {
    fn drop(&mut self) {
        self.clear(None);
    }
}

/// Releases a dropped read's admission, then drops its generation's cached
/// reader. A panic in that drop resumes, unless the read was dropped while
/// unwinding, in which case the panic is discarded to avoid an abort.
fn release_read(state: &Rc<RefCell<HolderState>>, source_identity: &Rc<()>) {
    let reader = {
        let mut state = state.borrow_mut();
        let state = &mut *state;
        match state.source.as_mut() {
            Some(entry) if Rc::ptr_eq(&entry.identity, source_identity) => {
                state.active_read = false;
                entry.reader.take()
            }
            _ => None,
        }
    };
    finish_retirement_panic(retire_reader(reader, false), std::thread::panicking());
}

fn finish_read(
    state: &Rc<RefCell<HolderState>>,
    source_identity: &Rc<()>,
    terminal: bool,
) -> Option<SourceEntry> {
    let mut state = state.borrow_mut();
    if state
        .source
        .as_ref()
        .is_some_and(|entry| Rc::ptr_eq(&entry.identity, source_identity))
    {
        state.active_read = false;
        if terminal {
            return state.source.take();
        }
    }
    None
}
