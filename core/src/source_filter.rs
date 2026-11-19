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
use std::rc::Weak;
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

/// A reason that changing filter ownership could not be completed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FilterMutationError {
    #[error("cannot mutate an HTTP source filter while a read is active")]
    ReadInProgress,
    #[error("cannot mutate an HTTP source filter during a source callback")]
    CallbackInProgress,
}

/// Returns ownership of a source rejected by a filter mutation.
pub struct RejectedFilterSource {
    reason: FilterMutationError,
    source: Box<dyn HttpSource>,
}

impl RejectedFilterSource {
    pub(crate) fn new(reason: FilterMutationError, source: Box<dyn HttpSource>) -> Self {
        Self { reason, source }
    }

    pub const fn reason(&self) -> FilterMutationError {
        self.reason
    }

    pub fn into_source(self) -> Box<dyn HttpSource> {
        self.source
    }
}

/// Supplies the event behavior of an [`HttpSourceFilter`].
///
/// Each method receives the downstream read as an owned, owner-local future.
/// Implementations can await and transform that result without extending a
/// borrow of either the filter or the source across suspension.
pub trait HttpSourceFilterHooks: 'static {
    fn filter_name(&self) -> &'static str;

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        downstream
    }

    fn read_body_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        downstream
    }
}

struct SourceEntry {
    identity: Rc<()>,
    source: Box<dyn HttpSource>,
}

struct ActiveRead {
    source_identity: Rc<()>,
    read_identity: Rc<()>,
}

struct StopRequest(Option<HttpErrorCode>);

#[derive(Default)]
struct FilterSourceState {
    source: Option<SourceEntry>,
    dispatching: Option<Rc<()>>,
    active_read: Option<ActiveRead>,
    pending_stop: Option<StopRequest>,
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

struct FilterReadFuture<T>
where
    T: TerminalEvent,
{
    state: Rc<RefCell<FilterSourceState>>,
    source_identity: Rc<()>,
    read_identity: Rc<()>,
    inner: LocalSourceFuture<Result<T, HttpError>>,
    completed: bool,
}

impl<T> Future for FilterReadFuture<T>
where
    T: TerminalEvent,
{
    type Output = Result<T, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Poll::Ready(result) = self.inner.as_mut().poll(context) else {
            return Poll::Pending;
        };
        self.completed = true;
        let terminal = result.as_ref().map_or(true, TerminalEvent::is_terminal);
        let retired = finish_read(
            &self.state,
            &self.source_identity,
            &self.read_identity,
            terminal,
        );
        drop(retired);
        Poll::Ready(result)
    }
}

impl<T> Drop for FilterReadFuture<T>
where
    T: TerminalEvent,
{
    fn drop(&mut self) {
        if !self.completed {
            release_read(&self.state, &self.source_identity, &self.read_identity);
        }
    }
}

struct FilterSourceSlot {
    state: Rc<RefCell<FilterSourceState>>,
}

impl FilterSourceSlot {
    fn empty() -> Self {
        Self {
            state: Rc::new(RefCell::new(FilterSourceState::default())),
        }
    }

    fn new(source: Box<dyn HttpSource>) -> Self {
        let slot = Self::empty();
        slot.state.borrow_mut().source = Some(new_source_entry(source));
        slot
    }

    fn readable(&self) -> bool {
        let state = self.state.borrow();
        state.source.is_some() || (state.dispatching.is_some() && state.pending_stop.is_none())
    }

    fn release(&self) -> Result<Option<Box<dyn HttpSource>>, FilterMutationError> {
        let mut state = self.state.borrow_mut();
        if state.active_read.is_some() {
            return Err(FilterMutationError::ReadInProgress);
        }
        if state.dispatching.is_some() {
            return Err(FilterMutationError::CallbackInProgress);
        }
        Ok(state.source.take().map(|entry| entry.source))
    }

    fn set_source(&self, source: Box<dyn HttpSource>) -> Result<(), RejectedFilterSource> {
        let previous = {
            let mut state = self.state.borrow_mut();
            if state.active_read.is_some() {
                return Err(RejectedFilterSource::new(
                    FilterMutationError::ReadInProgress,
                    source,
                ));
            }
            if state.dispatching.is_some() {
                return Err(RejectedFilterSource::new(
                    FilterMutationError::CallbackInProgress,
                    source,
                ));
            }
            state.source.replace(new_source_entry(source))
        };
        stop_source(previous, None);
        Ok(())
    }

    fn stop(&self, error: Option<HttpErrorCode>) {
        let previous = {
            let mut state = self.state.borrow_mut();
            state.active_read = None;
            if state.dispatching.is_some() {
                if state.pending_stop.is_none() {
                    state.pending_stop = Some(StopRequest(error));
                }
                return;
            }
            state.source.take()
        };
        stop_source(previous, error);
    }

    fn read_header_event(&self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let (mut source, source_identity, read_identity) = match self.begin_read() {
            Ok(read) => read,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let factory = catch_unwind(AssertUnwindSafe(|| source.source.read_header_event()));
        match factory {
            Ok(inner) => {
                if let Some((source, stop)) = restore_after_dispatch(&self.state, source, true) {
                    drop(inner);
                    stop_source(Some(source), stop.0);
                    return Box::pin(std::future::ready(Err(stopped_read_error())));
                }
                Box::pin(FilterReadFuture {
                    state: Rc::clone(&self.state),
                    source_identity,
                    read_identity,
                    inner,
                    completed: false,
                })
            }
            Err(panic) => {
                if let Some((source, stop)) = restore_after_dispatch(&self.state, source, false) {
                    stop_source(Some(source), stop.0);
                }
                resume_unwind(panic)
            }
        }
    }

    fn read_body_event(&self, maximum: u32) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let (mut source, source_identity, read_identity) = match self.begin_read() {
            Ok(read) => read,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let factory = catch_unwind(AssertUnwindSafe(|| source.source.read_body_event(maximum)));
        match factory {
            Ok(inner) => {
                if let Some((source, stop)) = restore_after_dispatch(&self.state, source, true) {
                    drop(inner);
                    stop_source(Some(source), stop.0);
                    return Box::pin(std::future::ready(Err(stopped_read_error())));
                }
                Box::pin(FilterReadFuture {
                    state: Rc::clone(&self.state),
                    source_identity,
                    read_identity,
                    inner,
                    completed: false,
                })
            }
            Err(panic) => {
                if let Some((source, stop)) = restore_after_dispatch(&self.state, source, false) {
                    stop_source(Some(source), stop.0);
                }
                resume_unwind(panic)
            }
        }
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        let source = self.take_for_callback()?;
        let result = catch_unwind(AssertUnwindSafe(|| source.source.get_stream_id()));
        if let Some((source, stop)) = restore_after_dispatch(&self.state, source, true) {
            stop_source(Some(source), stop.0);
        }
        match result {
            Ok(stream_id) => stream_id,
            Err(panic) => resume_unwind(panic),
        }
    }

    fn set_read_timeout(&self, timeout: Duration) {
        let Some(mut source) = self.take_for_callback() else {
            return;
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            source.source.set_read_timeout(timeout);
        }));
        if let Some((source, stop)) = restore_after_dispatch(&self.state, source, true) {
            stop_source(Some(source), stop.0);
        }
        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        let source = self.take_for_callback()?;
        let result = catch_unwind(AssertUnwindSafe(|| source.source.ingress_completion()));
        if let Some((source, stop)) = restore_after_dispatch(&self.state, source, true) {
            stop_source(Some(source), stop.0);
        }
        match result {
            Ok(completion) => completion,
            Err(panic) => resume_unwind(panic),
        }
    }

    fn begin_read(&self) -> Result<(SourceEntry, Rc<()>, Rc<()>), HttpError> {
        let mut state = self.state.borrow_mut();
        if state.active_read.is_some() || state.dispatching.is_some() {
            return Err(concurrent_read_error());
        }
        let Some(source) = state.source.take() else {
            return Err(unavailable_error());
        };
        let source_identity = Rc::clone(&source.identity);
        let read_identity = Rc::new(());
        state.dispatching = Some(Rc::clone(&source_identity));
        state.active_read = Some(ActiveRead {
            source_identity: Rc::clone(&source_identity),
            read_identity: Rc::clone(&read_identity),
        });
        Ok((source, source_identity, read_identity))
    }

    fn take_for_callback(&self) -> Option<SourceEntry> {
        let mut state = self.state.borrow_mut();
        if state.dispatching.is_some() {
            return None;
        }
        let source = state.source.take()?;
        state.dispatching = Some(Rc::clone(&source.identity));
        Some(source)
    }
}

impl Drop for FilterSourceSlot {
    fn drop(&mut self) {
        self.stop(None);
    }
}

struct FilterState {
    hooks: RefCell<Option<Box<dyn HttpSourceFilterHooks>>>,
    filter_name: &'static str,
    source: FilterSourceSlot,
}

struct HookLease {
    state: Rc<FilterState>,
    hooks: Option<Box<dyn HttpSourceFilterHooks>>,
}

impl HookLease {
    fn take(state: &Rc<FilterState>) -> Option<Self> {
        let hooks = state.hooks.borrow_mut().take()?;
        Some(Self {
            state: Rc::clone(state),
            hooks: Some(hooks),
        })
    }

    fn hooks(&mut self) -> &mut dyn HttpSourceFilterHooks {
        self.hooks
            .as_deref_mut()
            .expect("a live hook lease retains its filter hooks")
    }
}

impl Drop for HookLease {
    fn drop(&mut self) {
        let hooks = self
            .hooks
            .take()
            .expect("a live hook lease retains its filter hooks");
        let previous = self.state.hooks.borrow_mut().replace(hooks);
        assert!(previous.is_none(), "a filter hook lease is exclusive");
    }
}

/// An owner-local source node that delegates to one downstream source.
///
/// Replacing or dropping an unfinished downstream source stops it exactly
/// once. A terminal event or error retires it before the event leaves this
/// node.
pub struct HttpSourceFilter {
    state: Rc<FilterState>,
}

impl HttpSourceFilter {
    pub fn new(hooks: impl HttpSourceFilterHooks) -> Self {
        let filter_name = hooks.filter_name();
        Self {
            state: Rc::new(FilterState {
                hooks: RefCell::new(Some(Box::new(hooks))),
                filter_name,
                source: FilterSourceSlot::empty(),
            }),
        }
    }

    pub fn with_source(hooks: impl HttpSourceFilterHooks, source: Box<dyn HttpSource>) -> Self {
        let filter_name = hooks.filter_name();
        Self {
            state: Rc::new(FilterState {
                hooks: RefCell::new(Some(Box::new(hooks))),
                filter_name,
                source: FilterSourceSlot::new(source),
            }),
        }
    }

    pub fn readable(&self) -> bool {
        self.state.source.readable()
    }

    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, FilterMutationError> {
        self.state.source.release()
    }

    pub fn set_source(&mut self, source: Box<dyn HttpSource>) -> Result<(), RejectedFilterSource> {
        self.state.source.set_source(source)
    }

    pub fn filter_name(&self) -> &'static str {
        self.state.filter_name
    }

    pub(crate) fn tail_handle(&self) -> HttpSourceFilterHandle {
        HttpSourceFilterHandle {
            state: Rc::downgrade(&self.state),
        }
    }

    #[cfg(test)]
    pub(crate) fn readable_probe(&self) -> Box<dyn Fn() -> bool> {
        let state = Rc::downgrade(&self.state);
        Box::new(move || state.upgrade().is_some_and(|state| state.source.readable()))
    }

    fn hooks_busy_error() -> HttpError {
        HttpError::with_message(
            HttpErrorCode::INVALID_STATE_TRANSITION,
            b"HTTP source filter hook is already dispatching",
        )
    }
}

impl HttpSource for HttpSourceFilter {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let Some(mut hooks) = HookLease::take(&self.state) else {
            return Box::pin(std::future::ready(Err(Self::hooks_busy_error())));
        };
        let downstream = self.state.source.read_header_event();
        hooks.hooks().read_header_event(downstream)
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let Some(mut hooks) = HookLease::take(&self.state) else {
            return Box::pin(std::future::ready(Err(Self::hooks_busy_error())));
        };
        let downstream = self.state.source.read_body_event(maximum);
        hooks.hooks().read_body_event(downstream)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.state.source.stop(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.state.source.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.state.source.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.state.source.set_read_timeout(timeout);
    }
}

pub(crate) struct HttpSourceFilterHandle {
    state: Weak<FilterState>,
}

impl HttpSourceFilterHandle {
    pub(crate) fn is_live(&self) -> bool {
        self.state.strong_count() != 0
    }

    pub(crate) fn readable(&self) -> bool {
        self.state
            .upgrade()
            .is_some_and(|state| state.source.readable())
    }

    pub(crate) fn release(
        &self,
    ) -> Option<Result<Option<Box<dyn HttpSource>>, FilterMutationError>> {
        self.state.upgrade().map(|state| state.source.release())
    }

    pub(crate) fn set_source(
        &self,
        source: Box<dyn HttpSource>,
    ) -> Result<(), RejectedFilterSource> {
        let Some(state) = self.state.upgrade() else {
            return Err(RejectedFilterSource::new(
                FilterMutationError::CallbackInProgress,
                source,
            ));
        };
        state.source.set_source(source)
    }
}

pub type HeaderMutateFn = Box<dyn FnMut(&mut HttpHeaderEvent) + 'static>;
pub type BodyMutateFn = Box<dyn FnMut(&mut HttpBodyEvent) + 'static>;

struct MutateHooks {
    header_hook: Option<Rc<RefCell<HeaderMutateFn>>>,
    body_hook: Option<Rc<RefCell<BodyMutateFn>>>,
}

impl HttpSourceFilterHooks for MutateHooks {
    fn filter_name(&self) -> &'static str {
        "Mutate"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let hook = self.header_hook.clone();
        Box::pin(async move {
            let mut event = downstream.await?;
            if let Some(hook) = hook {
                (hook.borrow_mut())(&mut event);
            }
            Ok(event)
        })
    }

    fn read_body_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let hook = self.body_hook.clone();
        Box::pin(async move {
            let mut event = downstream.await?;
            if let Some(hook) = hook {
                (hook.borrow_mut())(&mut event);
            }
            Ok(event)
        })
    }
}

/// A filter that mutates successful header and body events in place.
///
/// Errors pass through unchanged and do not invoke either hook.
pub struct MutateFilter {
    inner: HttpSourceFilter,
}

impl MutateFilter {
    pub fn new(header_hook: Option<HeaderMutateFn>, body_hook: Option<BodyMutateFn>) -> Self {
        Self {
            inner: HttpSourceFilter::new(MutateHooks {
                header_hook: header_hook.map(|hook| Rc::new(RefCell::new(hook))),
                body_hook: body_hook.map(|hook| Rc::new(RefCell::new(hook))),
            }),
        }
    }

    pub fn with_source(
        source: Box<dyn HttpSource>,
        header_hook: Option<HeaderMutateFn>,
        body_hook: Option<BodyMutateFn>,
    ) -> Self {
        Self {
            inner: HttpSourceFilter::with_source(
                MutateHooks {
                    header_hook: header_hook.map(|hook| Rc::new(RefCell::new(hook))),
                    body_hook: body_hook.map(|hook| Rc::new(RefCell::new(hook))),
                },
                source,
            ),
        }
    }

    pub fn readable(&self) -> bool {
        self.inner.readable()
    }

    pub fn release(&mut self) -> Result<Option<Box<dyn HttpSource>>, FilterMutationError> {
        self.inner.release()
    }

    pub fn set_source(&mut self, source: Box<dyn HttpSource>) -> Result<(), RejectedFilterSource> {
        self.inner.set_source(source)
    }

    pub fn filter_name(&self) -> &'static str {
        self.inner.filter_name()
    }

    pub fn into_filter(self) -> HttpSourceFilter {
        self.inner
    }
}

impl From<MutateFilter> for HttpSourceFilter {
    fn from(filter: MutateFilter) -> Self {
        filter.into_filter()
    }
}

impl HttpSource for MutateFilter {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.inner.stop_reading(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.inner.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.inner.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.inner.set_read_timeout(timeout);
    }
}

fn new_source_entry(source: Box<dyn HttpSource>) -> SourceEntry {
    SourceEntry {
        identity: Rc::new(()),
        source,
    }
}

fn active_read_matches(
    active_read: &ActiveRead,
    source_identity: &Rc<()>,
    read_identity: &Rc<()>,
) -> bool {
    Rc::ptr_eq(&active_read.source_identity, source_identity)
        && Rc::ptr_eq(&active_read.read_identity, read_identity)
}

fn release_read(
    state: &Rc<RefCell<FilterSourceState>>,
    source_identity: &Rc<()>,
    read_identity: &Rc<()>,
) {
    let mut state = state.borrow_mut();
    if state
        .active_read
        .as_ref()
        .is_some_and(|active_read| active_read_matches(active_read, source_identity, read_identity))
    {
        state.active_read = None;
    }
}

fn finish_read(
    state: &Rc<RefCell<FilterSourceState>>,
    source_identity: &Rc<()>,
    read_identity: &Rc<()>,
    terminal: bool,
) -> Option<SourceEntry> {
    let mut state = state.borrow_mut();
    if state
        .active_read
        .as_ref()
        .is_some_and(|active_read| active_read_matches(active_read, source_identity, read_identity))
    {
        state.active_read = None;
    }
    if terminal
        && state
            .source
            .as_ref()
            .is_some_and(|entry| Rc::ptr_eq(&entry.identity, source_identity))
    {
        return state.source.take();
    }
    None
}

fn restore_after_dispatch(
    state: &Rc<RefCell<FilterSourceState>>,
    source: SourceEntry,
    retain_active_read: bool,
) -> Option<(SourceEntry, StopRequest)> {
    let mut state = state.borrow_mut();
    debug_assert!(
        state
            .dispatching
            .as_ref()
            .is_some_and(|identity| Rc::ptr_eq(identity, &source.identity))
    );
    state.dispatching = None;
    if !retain_active_read {
        state.active_read = None;
    }
    if let Some(stop) = state.pending_stop.take() {
        state.active_read = None;
        Some((source, stop))
    } else {
        state.source = Some(source);
        None
    }
}

fn stop_source(mut source: Option<SourceEntry>, error: Option<HttpErrorCode>) {
    if let Some(source) = source.as_mut() {
        source.source.stop_reading(error);
    }
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

fn stopped_read_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::DROPPED,
        b"HTTP source was stopped while beginning a read",
    )
}
