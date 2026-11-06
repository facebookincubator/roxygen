use std::cell::RefCell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
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
}

#[derive(Default)]
struct HolderState {
    source: Option<SourceEntry>,
    active_read: bool,
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
}

impl SourceRetirement {
    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        self.source
            .take()
            .and_then(|source| retire_source(source, |source| source.stop_reading(self.error)))
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

struct HolderReadFuture<T>
where
    T: TerminalEvent,
{
    state: Rc<RefCell<HolderState>>,
    source_identity: Rc<()>,
    inner: LocalSourceFuture<Result<T, HttpError>>,
    completed: bool,
}

impl<T> Future for HolderReadFuture<T>
where
    T: TerminalEvent,
{
    type Output = Result<T, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.completed,
            "a completed holder read cannot be polled again"
        );
        let poll = self.inner.as_mut().poll(context);
        let Poll::Ready(result) = &poll else {
            return Poll::Pending;
        };
        self.completed = true;
        let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
        let retired_source = finish_read(&self.state, &self.source_identity, terminal);
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
                source: Some(SourceEntry {
                    identity: Rc::new(()),
                    source,
                }),
                active_read: false,
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

    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, SourceReleaseError> {
        let mut state = self.state.borrow_mut();
        if state.active_read {
            return Err(SourceReleaseError::ReadInProgress);
        }
        Ok(state.source.take().map(|entry| entry.source))
    }

    pub fn set_source(&mut self, source: Box<dyn HttpSource>) {
        let previous = {
            let mut state = self.state.borrow_mut();
            state.active_read = false;
            state
                .source
                .replace(SourceEntry {
                    identity: Rc::new(()),
                    source,
                })
                .map(|entry| entry.source)
        };
        stop_source(previous, None);
    }

    pub fn clear(&mut self, error: Option<HttpErrorCode>) {
        let unwinding = std::thread::panicking();
        let retirement = self.prepare_clear(error);
        finish_retirement_panic(retirement.retire().err(), unwinding);
    }

    pub(crate) fn prepare_clear(&mut self, error: Option<HttpErrorCode>) -> SourceRetirement {
        let mut state = self.state.borrow_mut();
        state.active_read = false;
        SourceRetirement {
            source: state.source.take().map(|entry| entry.source),
            error,
        }
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
            let inner = entry.source.read_header_event();
            let source_identity = Rc::clone(&entry.identity);
            state.active_read = true;
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
    /// and returns a future independent of this mutable borrow.
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
            let inner = entry.source.read_body_event(maximum);
            let source_identity = Rc::clone(&entry.identity);
            state.active_read = true;
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
}

impl Drop for HttpSourceHolder {
    fn drop(&mut self) {
        self.clear(None);
    }
}

fn release_read(state: &Rc<RefCell<HolderState>>, source_identity: &Rc<()>) {
    let mut state = state.borrow_mut();
    if state
        .source
        .as_ref()
        .is_some_and(|entry| Rc::ptr_eq(&entry.identity, source_identity))
    {
        state.active_read = false;
    }
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

fn stop_source(source: Option<Box<dyn HttpSource>>, error: Option<HttpErrorCode>) {
    let unwinding = std::thread::panicking();
    finish_retirement_panic(SourceRetirement { source, error }.retire().err(), unwinding);
}
