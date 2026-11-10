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
    /// The task of this generation's last in-place read poll that returned
    /// `Pending` (`DIV-039`), woken if the generation goes away while that read
    /// is still active, since the source it was polling may never wake it.
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

/// The read of a consumer polling the holder itself as a source
/// (`HttpSource::poll_body_event`, `DIV-039`), which has no read future: a poll
/// that follows a `Pending` poll continues that read.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum ForwardedPoll {
    #[default]
    Idle,
    /// The last poll returned `Pending`; the current generation's `reader` is its waker.
    Pending,
    /// The generation a pending poll was polling went away; the next poll fails `DROPPED`.
    Orphaned,
}

impl HolderState {
    /// Takes the current generation out for retirement, installing `replacement`;
    /// a read waiting on it, through a read future or a forwarded poll, is woken
    /// once the retirement has stopped and dropped it, and fails.
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
        SourceRetirement::of(previous, error, active_read || forwarded)
    }

    /// Whether a forwarded poll is pending on the current generation, which is
    /// going away; its next poll then fails.
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
    /// A pending in-place read of the retired generation, woken once its
    /// source is stopped and dropped.
    reader: Option<Waker>,
}

impl SourceRetirement {
    /// Retires a generation taken out of its holder; `active_read` says whether
    /// its reader is still waiting.
    fn of(entry: Option<SourceEntry>, error: Option<HttpErrorCode>, active_read: bool) -> Self {
        let (source, reader) = entry.map_or((None, None), |entry| {
            (Some(entry.source), entry.reader.filter(|_| active_read))
        });
        Self {
            source,
            error,
            reader,
        }
    }

    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        let mut first = self
            .source
            .take()
            .and_then(|source| retire_source(source, |source| source.stop_reading(self.error)));
        if let Some(reader) = self.reader.take() {
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| reader.wake())).err(),
            );
        }
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
    /// The source itself, polled while the holder's state is borrowed (`DIV-039`).
    InPlace { poll: InPlacePoll<T>, maximum: u32 },
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

/// Polls the read's own source generation, which a stop, clear, or replacement
/// since the read started has made unavailable. A `Pending` poll registers the
/// read's task with the generation, which wakes it if it goes away.
fn poll_in_place<T>(
    state: &RefCell<HolderState>,
    source_identity: &Rc<()>,
    poll: InPlacePoll<T>,
    maximum: u32,
    context: &mut Context<'_>,
) -> Poll<Result<T, HttpError>> {
    let Ok(mut state) = state.try_borrow_mut() else {
        return Poll::Ready(Err(HttpSourceHolder::concurrent_read_error()));
    };
    match state.source.as_mut() {
        Some(entry) if Rc::ptr_eq(&entry.identity, source_identity) => {
            let polled = poll(entry.source.as_mut(), context, maximum);
            if polled.is_pending() {
                register_reader(entry, context);
            }
            polled
        }
        _ => Poll::Ready(Err(HttpSourceHolder::unavailable_error())),
    }
}

/// Records the task of a `Pending` in-place poll with its generation, without
/// a clone while the same task polls again.
fn register_reader(entry: &mut SourceEntry, context: &Context<'_>) {
    if !entry
        .reader
        .as_ref()
        .is_some_and(|reader| reader.will_wake(context.waker()))
    {
        entry.reader = Some(context.waker().clone());
    }
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
        let poll = match &mut this.inner {
            ReadInner::Future(inner) => inner.as_mut().poll(context),
            ReadInner::InPlace { poll, maximum } => {
                poll_in_place(&this.state, &this.source_identity, *poll, *maximum, context)
            }
        };
        let Poll::Ready(result) = &poll else {
            return Poll::Pending;
        };
        this.completed = true;
        let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
        let retired_source = finish_read(&this.state, &this.source_identity, terminal);
        drop(retired_source);
        poll
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

pub(crate) struct HolderBodyRead(HolderRead<HttpBodyEvent>);

impl Future for HolderBodyRead {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context)
    }
}

/// Owns one local source and guarantees cleanup of every unfinished path.
///
/// A terminal event or error consumes only the source generation that
/// produced it. Early drop, explicit replacement, and explicit stop call
/// `stop_reading` exactly once before destroying that source. The internal
/// `Rc` also makes the holder and every read future owner-local.
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

    /// Takes the source out unstopped. A consumer polling the holder as a
    /// source whose poll is pending is woken, and its next poll fails.
    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, SourceReleaseError> {
        let (entry, forwarded) = {
            let mut state = self.state.borrow_mut();
            if state.active_read {
                return Err(SourceReleaseError::ReadInProgress);
            }
            let forwarded = state.orphan_forwarded_poll();
            (state.source.take(), forwarded)
        };
        Ok(entry.map(|mut entry| {
            if let Some(reader) = entry.reader.take().filter(|_| forwarded) {
                reader.wake();
            }
            entry.source
        }))
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
            let inner = ReadInner::Future(entry.source.read_header_event());
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
    /// and returns a future independent of this mutable borrow. A source whose
    /// [`HttpSource::polls_body_events`] is `true` is not called here: each poll of the
    /// returned future polls it in place (`DIV-039`), and fails with `DROPPED` once
    /// the source was stopped, cleared, replaced, or destroyed with the holder; a
    /// read pending then is woken, since its source may never wake it.
    pub fn read_body_event_unboxed(
        &mut self,
        maximum: u32,
    ) -> impl Future<Output = Result<HttpBodyEvent, HttpError>> + Unpin + 'static + use<> {
        self.read_body_event_concrete(maximum)
    }

    pub(crate) fn read_body_event_concrete(&mut self, maximum: u32) -> HolderBodyRead {
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
            let inner = if entry.source.polls_body_events() {
                ReadInner::InPlace {
                    poll: poll_source_body,
                    maximum,
                }
            } else {
                ReadInner::Future(entry.source.read_body_event(maximum))
            };
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

    fn unavailable_error() -> HttpError {
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

    fn polls_body_events(&self) -> bool {
        self.state.try_borrow().is_ok_and(|state| {
            state
                .source
                .as_ref()
                .is_some_and(|entry| entry.source.polls_body_events())
        })
    }

    /// Polls the held source in place, outside any read of this holder; a
    /// terminal event or error consumes the source, as it does for a read. A
    /// poll that follows a `Pending` poll continues that read, with an in-place
    /// read's guarantees: the `Pending` poll registers its waker with the
    /// generation it polled, which wakes it when it is cleared, stopped,
    /// replaced, released or destroyed, and the next poll then fails with
    /// `DROPPED`, never reaching a replacement source. The holder cannot tell a
    /// consumer that abandoned its pending poll, so the first poll after such a
    /// generation change fails that way too; a read of this holder resets it.
    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let (poll, retired) = {
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
            let poll = entry.source.poll_body_event(context, maximum);
            let Poll::Ready(result) = &poll else {
                register_reader(entry, context);
                state.forwarded = ForwardedPoll::Pending;
                return poll;
            };
            let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
            let retired = if terminal { state.source.take() } else { None };
            (poll, retired)
        };
        drop(retired);
        poll
    }
}

impl Drop for HttpSourceHolder {
    fn drop(&mut self) {
        self.clear(None);
    }
}

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
    drop(reader);
}

fn finish_read(
    state: &Rc<RefCell<HolderState>>,
    source_identity: &Rc<()>,
    terminal: bool,
) -> Option<Box<dyn HttpSource>> {
    let mut state = state.borrow_mut();
    if state
        .source
        .as_ref()
        .is_some_and(|entry| Rc::ptr_eq(&entry.identity, source_identity))
    {
        state.active_read = false;
        if terminal {
            return state.source.take().map(|entry| entry.source);
        }
    }
    None
}
