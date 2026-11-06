use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroU64;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::CancellableWait;
use roxygen_runtime::wait::WaitStatus;
use thiserror::Error;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::events::HttpBodyEvent;
use crate::events::HttpBodyEventKind;
use crate::events::HttpBodyEventType;
use crate::events::HttpHeaderEvent;
use crate::events::ResumeFuture;
use crate::events::ResumeStatus;
use crate::headers::HttpHeaderCode;
use crate::ids::StreamId;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HolderBodyRead;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::RetirementPanic;
use crate::source_holder::SourceRetirement;
use crate::source_holder::finish_retirement_panic;
use crate::source_holder::retain_retirement_panic;
use crate::stream_source::HttpIngressBody;
use crate::stream_source::HttpStreamBodyRead;
use crate::stream_source::HttpStreamSourceOwner;
use crate::stream_source::IngressBodyReceiver;
use crate::stream_source::source_not_readable;

const DEFAULT_BUFFER_LIMIT: usize = 65_535;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Owner callbacks associated with egress queue accounting and diagnostics.
pub trait BodyEventQueueCallback: 'static {
    fn on_egress_bytes_buffered(&self, bytes: i64);

    fn on_content_length_mismatch(&self, _expected: u64, _observed: u64) {}
}

/// The non-error result of reading and buffering one body event.
pub struct ReadBodyResult {
    pub resume: Option<ResumeFuture>,
    pub eom: bool,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BodyEventQueueMutationError {
    #[error("cannot replace an HTTP body queue source while a read is active")]
    ReadInProgress,
}

/// Returns ownership of a source rejected by a queue mutation.
pub struct RejectedBodyEventQueueSource {
    reason: BodyEventQueueMutationError,
    source: Box<dyn HttpSource>,
}

impl RejectedBodyEventQueueSource {
    fn new(reason: BodyEventQueueMutationError, source: Box<dyn HttpSource>) -> Self {
        Self { reason, source }
    }

    pub const fn reason(&self) -> BodyEventQueueMutationError {
        self.reason
    }

    pub fn into_source(self) -> Box<dyn HttpSource> {
        self.source
    }
}

enum BufferedEvents {
    Inline(Option<HttpBodyEvent>),
    Heap(VecDeque<HttpBodyEvent>),
}

impl Default for BufferedEvents {
    fn default() -> Self {
        Self::Inline(None)
    }
}

impl BufferedEvents {
    fn is_empty(&self) -> bool {
        match self {
            Self::Inline(event) => event.is_none(),
            Self::Heap(events) => events.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Inline(event) => usize::from(event.is_some()),
            Self::Heap(events) => events.len(),
        }
    }

    fn front(&self) -> Option<&HttpBodyEvent> {
        match self {
            Self::Inline(event) => event.as_ref(),
            Self::Heap(events) => events.front(),
        }
    }

    fn back(&self) -> Option<&HttpBodyEvent> {
        match self {
            Self::Inline(event) => event.as_ref(),
            Self::Heap(events) => events.back(),
        }
    }

    fn pop_front(&mut self) -> Option<HttpBodyEvent> {
        match self {
            Self::Inline(event) => event.take(),
            Self::Heap(events) => events.pop_front(),
        }
    }

    fn pop_back(&mut self) -> Option<HttpBodyEvent> {
        match self {
            Self::Inline(event) => event.take(),
            Self::Heap(events) => events.pop_back(),
        }
    }

    fn push_front(&mut self, event: HttpBodyEvent) {
        match self {
            Self::Inline(slot) => {
                if let Some(previous) = slot.take() {
                    let mut events = VecDeque::new();
                    events.push_back(event);
                    events.push_back(previous);
                    *self = Self::Heap(events);
                } else {
                    *slot = Some(event);
                }
            }
            Self::Heap(events) => events.push_front(event),
        }
    }

    fn push_back(&mut self, event: HttpBodyEvent) {
        match self {
            Self::Inline(slot) => {
                if let Some(previous) = slot.take() {
                    let mut events = VecDeque::new();
                    events.push_back(previous);
                    events.push_back(event);
                    *self = Self::Heap(events);
                } else {
                    *slot = Some(event);
                }
            }
            Self::Heap(events) => events.push_back(event),
        }
    }

    fn for_each_mut(&mut self, mut visit: impl FnMut(&mut HttpBodyEvent)) {
        match self {
            Self::Inline(event) => {
                if let Some(event) = event {
                    visit(event);
                }
            }
            Self::Heap(events) => events.iter_mut().for_each(visit),
        }
    }
}

#[derive(Default)]
struct BodyEventBuffer {
    events: BufferedEvents,
    bytes: usize,
}

impl BodyEventBuffer {
    // The caller retains rejected and coalesced input until its borrow ends.
    fn push(&mut self, event: &mut Option<HttpBodyEvent>) -> Result<(bool, usize), HttpError> {
        let body_event = event.as_mut().expect("an input event is owned");
        let eom = body_event.eom();
        let added_body_length = body_event.as_body().map_or(0, BufQueue::chain_length);
        if let Some(body) = body_event.as_body_mut() {
            let coalesce = self.events.back().is_some_and(|back| {
                back.event_type() == HttpBodyEventType::Body
                    && back.byte_event_registrations().is_empty()
            });
            if coalesce {
                let back_body = self
                    .events
                    .back()
                    .and_then(HttpBodyEvent::as_body)
                    .expect("a coalescing decision requires a queued body event");
                let combined_limit = back_body
                    .chain_length()
                    .checked_add(body.chain_length())
                    .ok_or_else(|| {
                        HttpError::with_message(
                            HttpErrorCode::INTERNAL_ERROR,
                            b"body-event coalescing length overflow",
                        )
                    })?;
                // Nonempty segments bound growth before the payload owners move.
                let move_bodies = combined_limit <= DEFAULT_BUFFER_LIMIT;
                let mut back_body = if move_bodies {
                    let HttpBodyEventKind::Body(body) = self
                        .events
                        .pop_back()
                        .expect("a coalescing decision requires a queued body event")
                        .into_kind()
                    else {
                        unreachable!("the queued event was checked to be a body");
                    };
                    body
                } else {
                    back_body.clone_shared()
                };
                let mut incoming_body = if move_bodies {
                    body.take()
                } else {
                    body.clone_shared()
                };
                let mut combined = BufQueue::new(combined_limit);
                combined.append(&mut back_body).map_err(|_| {
                    HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"body-event coalescing rejected the queued body",
                    )
                })?;
                combined.append(&mut incoming_body).map_err(|_| {
                    HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"body-event coalescing rejected the incoming body",
                    )
                })?;
                if !move_bodies {
                    self.events.pop_back();
                }
                let registrations = body_event.take_byte_event_registrations();
                let mut combined_event = HttpBodyEvent::body(combined, eom);
                *combined_event.byte_event_registrations_mut() = registrations;
                self.events.push_back(combined_event);
            } else {
                self.events
                    .push_back(event.take().expect("an input event is owned"));
            }
            self.bytes = self.bytes.wrapping_add(added_body_length);
        } else {
            self.events
                .push_back(event.take().expect("an input event is owned"));
        }
        Ok((eom, added_body_length))
    }

    fn can_dequeue(&self, maximum: u32) -> bool {
        self.events.front().is_some_and(|event| {
            maximum != 0 || event.as_body().is_none_or(|body| body.chain_length() == 0)
        })
    }

    fn dequeue(&mut self, maximum: u32) -> Option<HttpBodyEvent> {
        let mut body_event = self.events.pop_front()?;
        if let Some(body) = body_event.as_body_mut() {
            let length = body.chain_length();
            let to_return = usize::try_from(maximum.min(length as u32))
                .expect("u32 always fits in the supported target usize");
            debug_assert!(self.bytes >= to_return);
            self.bytes -= to_return;
            if to_return < length {
                let result = body.split_at_most(to_return);
                self.events.push_front(body_event);
                body_event = HttpBodyEvent::body(result, false);
            }
        }
        Some(body_event)
    }
}

struct QueueState {
    host: Host,
    stream_id: StreamId,
    callback: Option<Weak<dyn BodyEventQueueCallback>>,
    buffer_available: Option<Rc<CancellableBaton>>,
    buffer: BodyEventBuffer,
    limit: usize,
    expected_content_length: u64,
    observed_body_length: u64,
    should_validate_content_length: bool,
    active_read: Option<NonZeroU64>,
    next_read: Option<NonZeroU64>,
    stopped_error: Option<HttpErrorCode>,
    destroyed: bool,
}

struct ReadLease {
    state: Rc<RefCell<QueueState>>,
    identity: NonZeroU64,
    released: bool,
}

impl ReadLease {
    fn is_current(&self) -> bool {
        self.state.borrow().active_read == Some(self.identity)
    }

    fn stale_error(&self) -> HttpError {
        let state = self.state.borrow();
        if state.destroyed {
            return HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"HTTP body event queue was destroyed during a read",
            );
        }
        HttpError::with_message(
            state.stopped_error.unwrap_or(HttpErrorCode::DROPPED),
            b"HTTP body event queue read was stopped",
        )
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let mut state = self.state.borrow_mut();
        if state.active_read == Some(self.identity) {
            state.active_read = None;
        }
        self.released = true;
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        self.release();
    }
}

enum BodyReadState {
    Unstarted,
    Capacity(Option<CancellableWait>),
    Source {
        cancellation_wait: Option<(CancellableWait, CancellableBaton)>,
        read: HolderBodyRead,
    },
    Complete,
}

/// One lazy queue read whose state remains owned across pending polls.
///
/// Dropping an unpolled operation does not admit a read or invoke the source.
pub struct BodyEventQueueRead {
    // Retire the source operation before releasing the queue read lease.
    phase: BodyReadState,
    lease: Option<ReadLease>,
    state: Rc<RefCell<QueueState>>,
    source: Rc<RefCell<HttpSourceHolder>>,
    buffer_available: Option<Rc<CancellableBaton>>,
    write_timeout: Duration,
    maximum: u32,
    cancellation: CancellationToken,
}

impl BodyEventQueueRead {
    fn complete(
        &mut self,
        result: Result<ReadBodyResult, HttpError>,
    ) -> Poll<Result<ReadBodyResult, HttpError>> {
        self.phase = BodyReadState::Complete;
        drop(self.lease.take());
        Poll::Ready(result)
    }

    fn poll_inner(&mut self, context: &mut Context<'_>) -> Poll<Result<ReadBodyResult, HttpError>> {
        loop {
            match &mut self.phase {
                BodyReadState::Unstarted => {
                    self.lease = match begin_read(&self.state) {
                        Ok(lease) => Some(lease),
                        Err(error) => return self.complete(Err(error)),
                    };
                    self.phase = BodyReadState::Capacity(None);
                }
                BodyReadState::Capacity(wait) => {
                    if let Some(pending) = wait {
                        let Poll::Ready(status) = Pin::new(pending).poll(context) else {
                            return Poll::Pending;
                        };
                        *wait = None;
                        match status {
                            WaitStatus::Signalled => {}
                            WaitStatus::TimedOut => {
                                return self.complete(Err(HttpError::with_message(
                                    HttpErrorCode::READ_TIMEOUT,
                                    b"timed out waiting for buffer space",
                                )));
                            }
                            WaitStatus::Cancelled => {
                                return self.complete(Err(HttpError::with_message(
                                    HttpErrorCode::CORO_CANCELLED,
                                    b"cancelled waiting for buffer space",
                                )));
                            }
                            WaitStatus::NotReady => {
                                unreachable!("a completed capacity wait has a ready status")
                            }
                        }
                    }
                    let lease = self
                        .lease
                        .as_ref()
                        .expect("an active queue read owns its lease");
                    if !lease.is_current() {
                        return self.complete(Err(lease.stale_error()));
                    }
                    let available = available_buffer(&self.state.borrow());
                    if available == 0 {
                        let buffer_available = self.buffer_available.get_or_insert_with(|| {
                            let mut state = self.state.borrow_mut();
                            if state.buffer_available.is_none() {
                                state.buffer_available =
                                    Some(Rc::new(CancellableBaton::with_host(state.host.clone())));
                            }
                            Rc::clone(
                                state
                                    .buffer_available
                                    .as_ref()
                                    .expect("a capacity wait owns its baton"),
                            )
                        });
                        buffer_available.reset();
                        *wait = Some(
                            buffer_available
                                .timed_wait(self.write_timeout, self.cancellation.clone()),
                        );
                        continue;
                    }
                    let read = self
                        .source
                        .borrow_mut()
                        .read_body_event_concrete(self.maximum.min(available as u32));
                    self.phase = BodyReadState::Source {
                        cancellation_wait: None,
                        read,
                    };
                }
                BodyReadState::Source {
                    cancellation_wait,
                    read,
                } => {
                    let Poll::Ready(result) = poll_source_or_cancellation(
                        read,
                        cancellation_wait,
                        &self.cancellation,
                        context,
                    ) else {
                        return Poll::Pending;
                    };
                    self.phase = BodyReadState::Complete;
                    let lease = self
                        .lease
                        .as_ref()
                        .expect("an active queue read owns its lease");
                    let result = if !lease.is_current() {
                        Err(lease.stale_error())
                    } else {
                        result.and_then(|event| process_read_body_event(&self.state, event))
                    };
                    return self.complete(result);
                }
                BodyReadState::Complete => panic!("a completed queue read cannot be polled again"),
            }
        }
    }
}

// Unwinding must release read admission even if the caller retains the future.
struct BodyReadPollGuard<'a> {
    read: &'a mut BodyEventQueueRead,
    armed: bool,
}

impl Drop for BodyReadPollGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let phase = std::mem::replace(&mut self.read.phase, BodyReadState::Complete);
            let lease = self.read.lease.take();
            drop(phase);
            drop(lease);
        }
    }
}

impl Future for BodyEventQueueRead {
    type Output = Result<ReadBodyResult, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = BodyReadPollGuard {
            read: self.get_mut(),
            armed: true,
        };
        let result = guard.read.poll_inner(context);
        guard.armed = false;
        result
    }
}

/// A move-owned body input retaining its original generic or concrete source.
///
/// Generic inputs preserve their holder's admission and terminal semantics.
/// Concrete stream inputs remain concrete until explicitly converted to a holder.
pub struct HttpBodySource {
    kind: BodySourceKind,
}

enum BodySourceKind {
    Generic(HttpSourceHolder),
    Stream(HttpStreamSourceOwner),
    Ingress(HttpIngressBody),
}

impl From<HttpSourceHolder> for HttpBodySource {
    fn from(source: HttpSourceHolder) -> Self {
        Self {
            kind: BodySourceKind::Generic(source),
        }
    }
}

impl From<HttpStreamSourceOwner> for HttpBodySource {
    fn from(source: HttpStreamSourceOwner) -> Self {
        Self {
            kind: BodySourceKind::Stream(source),
        }
    }
}

impl From<HttpIngressBody> for HttpBodySource {
    fn from(source: HttpIngressBody) -> Self {
        Self {
            kind: BodySourceKind::Ingress(source),
        }
    }
}

impl HttpBodySource {
    /// Whether the original source remains owned.
    pub fn readable(&self) -> bool {
        match &self.kind {
            BodySourceKind::Generic(source) => source.readable(),
            BodySourceKind::Stream(source) => source.source().is_some(),
            BodySourceKind::Ingress(source) => source.readable(),
        }
    }

    /// Stops and destroys the original source with the supplied cause.
    pub fn clear(&mut self, error: Option<HttpErrorCode>) {
        match &mut self.kind {
            BodySourceKind::Generic(source) => source.clear(error),
            BodySourceKind::Stream(source) => source.clear(error),
            BodySourceKind::Ingress(source) => source.clear(error),
        }
    }

    /// Moves the original source into a holder without reading or stopping it.
    pub fn into_source_holder(self) -> HttpSourceHolder {
        match self.kind {
            BodySourceKind::Generic(source) => source,
            BodySourceKind::Stream(source) => source.into_source_holder(),
            BodySourceKind::Ingress(source) => source.into_source_owner().into_source_holder(),
        }
    }
}

enum TransferInput {
    Generic {
        pending: Option<HolderBodyRead>,
        source: HttpSourceHolder,
    },
    Stream {
        pending: Option<HttpStreamBodyRead>,
        source: Option<HttpStreamSourceOwner>,
    },
    Ingress(IngressBodyReceiver),
}

/// A persistent body reader driven by one owner before serialization.
///
/// The caller owns each returned event and chooses when another read is admitted.
/// Pending reads retain their original maximum. The source keeps its admission
/// and terminal semantics, and the owner supplies cancellation wakeups for the
/// entire transfer lifetime.
pub struct HttpBodyTransfer {
    input: TransferInput,
    stream_id: StreamId,
    limit: usize,
}

impl HttpBodyTransfer {
    pub fn new(source: impl Into<HttpBodySource>, stream_id: StreamId, limit: usize) -> Self {
        let input = match source.into().kind {
            BodySourceKind::Generic(source) => TransferInput::Generic {
                pending: None,
                source,
            },
            BodySourceKind::Stream(source) => TransferInput::Stream {
                pending: None,
                source: Some(source),
            },
            BodySourceKind::Ingress(source) => TransferInput::Ingress(source.into_receiver()),
        };
        Self {
            input,
            stream_id,
            limit,
        }
    }

    /// Polls an exclusive body's retained suspension before another body read.
    ///
    /// A completed status has retired the wait. The owner must check its selected
    /// error and cancellation before reading again. Generic Suspend events retain
    /// their existing externally owned resume future.
    pub fn poll_resume(&mut self, context: &mut Context<'_>) -> Poll<Option<ResumeStatus>> {
        let mut guard = BodyTransferPollGuard {
            transfer: self,
            armed: true,
        };
        let result = match &mut guard.transfer.input {
            TransferInput::Ingress(receiver) => receiver.poll_resume(context),
            _ => Poll::Ready(None),
        };
        guard.armed = false;
        result
    }

    /// Polls one source event, retaining its read across pending polls.
    ///
    /// The requested maximum is capped by this reader's fixed limit. The caller
    /// retains returned events and must wake this poll on cancellation.
    /// For an exclusive ingress input, poll `poll_resume` first and observe its
    /// completed-status cancellation checkpoint before admitting another read.
    pub fn poll_read_body_event(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
        cancellation: &CancellationToken,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let mut guard = BodyTransferPollGuard {
            transfer: self,
            armed: true,
        };
        let result = guard
            .transfer
            .poll_read_inner(context, maximum, cancellation);
        guard.armed = false;
        result
    }

    fn poll_read_inner(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
        cancellation: &CancellationToken,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let maximum = maximum.min(self.limit.min(u32::MAX as usize) as u32);
        let poll = match &mut self.input {
            TransferInput::Generic { pending, source } => {
                if pending.is_none() {
                    if self.limit == 0 {
                        return Poll::Pending;
                    }
                    *pending = Some(source.read_body_event_concrete(maximum));
                }
                Pin::new(pending.as_mut().expect("a source read is owned")).poll(context)
            }
            TransferInput::Stream { pending, source } => {
                let owner = source
                    .as_mut()
                    .expect("a transfer cannot be polled after prepared retirement");
                if pending.is_none() {
                    if self.limit == 0 {
                        return Poll::Pending;
                    }
                    let Some(source) = owner.source() else {
                        return Poll::Ready(Err(source_not_readable()));
                    };
                    *pending =
                        Some(source.read_body_event_unboxed(maximum, CancellationToken::never()));
                }
                let poll =
                    Pin::new(pending.as_mut().expect("a source read is owned")).poll(context);
                if let Poll::Ready(result) = &poll
                    && result.as_ref().map_or(true, HttpBodyEvent::eom)
                {
                    owner.retire_terminal();
                }
                poll
            }
            TransferInput::Ingress(receiver) => {
                if self.limit == 0 {
                    return Poll::Pending;
                }
                receiver.poll_body(context, maximum)
            }
        };
        let result = match poll {
            Poll::Ready(Err(error)) => Err(error),
            Poll::Ready(Ok(event)) => {
                if cancellation.is_cancellation_requested() {
                    Err(transfer_cancelled())
                } else {
                    Ok(event)
                }
            }
            Poll::Pending => {
                if cancellation.is_cancellation_requested() {
                    Err(transfer_cancelled())
                } else {
                    return Poll::Pending;
                }
            }
        };
        self.drop_pending_read();
        let mut event = result?;
        set_event_stream_id(&mut event, self.stream_id);
        Poll::Ready(Ok(event))
    }

    /// Extracts the unfinished source without invoking callbacks.
    ///
    /// Retire this record, then drop the pending read before retiring any event
    /// retained by the caller. The transfer must not be polled again.
    pub fn prepare_stop_and_clear(&mut self, error: HttpError) -> HttpBodyQueueRetirement {
        let source = match &mut self.input {
            TransferInput::Generic { source, .. } => Some(BodySourceRetirement::Generic(
                source.prepare_clear(Some(error.code())),
            )),
            TransferInput::Stream { source, .. } => source.take().map(BodySourceRetirement::Stream),
            TransferInput::Ingress(receiver) => {
                receiver.prepare_stop().map(BodySourceRetirement::Stream)
            }
        };
        HttpBodyQueueRetirement {
            source,
            events: BufferedEvents::default(),
            bytes: 0,
            callback: None,
            capacity: None,
            error: Some(error),
        }
    }

    /// Releases a pending read after prepared source/event retirement.
    pub fn drop_pending_read(&mut self) {
        match &mut self.input {
            TransferInput::Generic { pending, .. } => drop(pending.take()),
            TransferInput::Stream { pending, .. } => drop(pending.take()),
            TransferInput::Ingress(receiver) => receiver.drop_pending_read(),
        }
    }
}

impl Drop for HttpBodyTransfer {
    fn drop(&mut self) {
        self.drop_pending_read();
    }
}

struct BodyTransferPollGuard<'a> {
    transfer: &'a mut HttpBodyTransfer,
    armed: bool,
}

impl Drop for BodyTransferPollGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.transfer.drop_pending_read();
        }
    }
}

/// An owner-local bounded queue between an egress source and serialization.
pub struct HttpBodyEventQueue {
    state: Rc<RefCell<QueueState>>,
    source: Rc<RefCell<HttpSourceHolder>>,
    write_timeout: Duration,
}

/// Owns the effects extracted from one stopped queue generation.
///
/// Preparation is not completion of any outstanding read or task. Retire this
/// record after releasing all queue and holder borrows. Retirement never accesses
/// a replacement generation; dropping the record also attempts every effect.
#[must_use]
pub struct HttpBodyQueueRetirement {
    source: Option<BodySourceRetirement>,
    events: BufferedEvents,
    bytes: usize,
    callback: Option<Rc<dyn BodyEventQueueCallback>>,
    capacity: Option<Rc<CancellableBaton>>,
    error: Option<HttpError>,
}

enum BodySourceRetirement {
    Generic(SourceRetirement),
    Stream(HttpStreamSourceOwner),
}

impl HttpBodyQueueRetirement {
    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        let mut first = None;
        if let Some(source) = self.source.take() {
            let failure = match source {
                BodySourceRetirement::Generic(source) => source.retire().err(),
                BodySourceRetirement::Stream(mut source) => catch_unwind(AssertUnwindSafe(|| {
                    source.clear(self.error.as_ref().map(HttpError::code));
                }))
                .err(),
            };
            retain_retirement_panic(&mut first, failure);
        }
        while let Some(mut event) = self.events.pop_front() {
            for mut registration in event.take_byte_event_registrations() {
                retain_retirement_panic(
                    &mut first,
                    catch_unwind(AssertUnwindSafe(|| {
                        registration
                            .cancel(self.error.as_ref().expect("owned retirement error"), None);
                    }))
                    .err(),
                );
                retain_retirement_panic(
                    &mut first,
                    catch_unwind(AssertUnwindSafe(|| drop(registration))).err(),
                );
            }
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(event))).err(),
            );
        }
        if let Some(callback) = self.callback.take() {
            let bytes = std::mem::take(&mut self.bytes);
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| {
                    callback.on_egress_bytes_buffered((bytes as i64).wrapping_neg())
                }))
                .err(),
            );
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(callback))).err(),
            );
        }
        if let Some(capacity) = self.capacity.take() {
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| capacity.signal())).err(),
            );
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(capacity))).err(),
            );
        }
        let error = self.error.take();
        retain_retirement_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| drop(error))).err(),
        );
        first
    }

    /// Attempts all extracted effects and returns the first original panic.
    /// This does not certify retirement of independently owned read futures.
    pub fn retire(mut self) -> std::thread::Result<()> {
        self.retire_inner().map_or(Ok(()), Err)
    }
}

impl Drop for HttpBodyQueueRetirement {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let first = self.retire_inner();
        finish_retirement_panic(first, unwinding);
    }
}

impl HttpBodyEventQueue {
    pub fn new<C>(stream_id: StreamId, callback: &Rc<C>) -> Self
    where
        C: BodyEventQueueCallback,
    {
        Self::with_configuration(
            stream_id,
            callback,
            DEFAULT_BUFFER_LIMIT,
            DEFAULT_WRITE_TIMEOUT,
        )
    }

    pub fn with_configuration<C>(
        stream_id: StreamId,
        callback: &Rc<C>,
        limit: usize,
        write_timeout: Duration,
    ) -> Self
    where
        C: BodyEventQueueCallback,
    {
        Self::with_configuration_and_host(stream_id, callback, limit, write_timeout, Host::tokio())
    }

    /// Creates a bounded queue whose capacity waits use the owner's host.
    pub fn with_configuration_and_host<C>(
        stream_id: StreamId,
        callback: &Rc<C>,
        limit: usize,
        write_timeout: Duration,
        host: Host,
    ) -> Self
    where
        C: BodyEventQueueCallback,
    {
        Self::from_source_with_configuration(
            HttpSourceHolder::empty(),
            stream_id,
            callback,
            limit,
            write_timeout,
            host,
        )
    }

    /// Creates a bounded queue by taking ownership of an existing source holder.
    ///
    /// The holder's source and active-read state move unchanged into the queue,
    /// which owns stopping and retiring the source without another holder layer.
    pub fn from_source_with_configuration<C>(
        source: HttpSourceHolder,
        stream_id: StreamId,
        callback: &Rc<C>,
        limit: usize,
        write_timeout: Duration,
        host: Host,
    ) -> Self
    where
        C: BodyEventQueueCallback,
    {
        let callback: Rc<dyn BodyEventQueueCallback> = callback.clone();
        Self {
            state: Rc::new(RefCell::new(QueueState {
                host,
                stream_id,
                callback: Some(Rc::downgrade(&callback)),
                buffer_available: None,
                buffer: BodyEventBuffer::default(),
                limit,
                expected_content_length: 0,
                observed_body_length: 0,
                should_validate_content_length: true,
                active_read: None,
                next_read: NonZeroU64::new(1),
                stopped_error: None,
                destroyed: false,
            })),
            source: Rc::new(RefCell::new(source)),
            write_timeout,
        }
    }

    pub fn set_source(
        &mut self,
        source: Box<dyn HttpSource>,
    ) -> Result<(), RejectedBodyEventQueueSource> {
        if self.state.borrow().active_read.is_some() {
            return Err(RejectedBodyEventQueueSource::new(
                BodyEventQueueMutationError::ReadInProgress,
                source,
            ));
        }
        self.state.borrow_mut().stopped_error = None;
        self.source.borrow_mut().set_source(source);
        Ok(())
    }

    pub fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let state = Rc::clone(&self.state);
        let source = Rc::clone(&self.source);
        Box::pin(async move {
            let mut lease = begin_read(&state)?;
            let inner = source.borrow_mut().read_header_event_unboxed();
            let result = inner.await;
            let result = if !lease.is_current() {
                Err(lease.stale_error())
            } else {
                match result {
                    Ok(mut event) => {
                        process_header_event(&state, &mut event)?;
                        Ok(event)
                    }
                    Err(error) => Err(error),
                }
            };
            lease.release();
            result
        })
    }

    pub fn read_body_event(
        &mut self,
        maximum: u32,
        cancellation: CancellationToken,
    ) -> LocalSourceFuture<Result<ReadBodyResult, HttpError>> {
        Box::pin(self.read_body_event_unboxed(maximum, cancellation))
    }

    /// Creates a body read without boxing its queue operation.
    ///
    /// Read admission and source invocation begin only when the operation is polled.
    pub fn read_body_event_unboxed(
        &mut self,
        maximum: u32,
        cancellation: CancellationToken,
    ) -> BodyEventQueueRead {
        BodyEventQueueRead {
            phase: BodyReadState::Unstarted,
            lease: None,
            state: Rc::clone(&self.state),
            source: Rc::clone(&self.source),
            buffer_available: None,
            write_timeout: self.write_timeout,
            maximum,
            cancellation,
        }
    }

    pub fn stop_reading(&mut self, error: HttpErrorCode) {
        {
            let mut state = self.state.borrow_mut();
            state.active_read = None;
            state.stopped_error = Some(error);
        }
        self.source.borrow_mut().clear(Some(error));
        let previous = self.state.borrow_mut().buffer_available.take();
        if let Some(previous) = previous {
            previous.signal();
        }
    }

    /// Seals the current read and extracts source, buffered events and wakeups
    /// without invoking foreign code. Outstanding reads retain their own owners.
    /// Call `retire` only after the caller's queue borrow has ended.
    pub fn prepare_stop_and_clear(&mut self, error: HttpError) -> HttpBodyQueueRetirement {
        let mut state = self.state.borrow_mut();
        let mut holder = self.source.borrow_mut();
        let callback = state.callback.as_ref().and_then(Weak::upgrade);
        let source = holder.prepare_clear(Some(error.code()));
        state.active_read = None;
        state.stopped_error = Some(error.code());
        HttpBodyQueueRetirement {
            source: Some(BodySourceRetirement::Generic(source)),
            events: std::mem::take(&mut state.buffer.events),
            bytes: std::mem::take(&mut state.buffer.bytes),
            callback,
            capacity: state.buffer_available.take(),
            error: Some(error),
        }
    }

    pub fn observed_body_length(&self) -> u64 {
        self.state.borrow().observed_body_length
    }

    pub fn buffered_body_bytes(&self) -> usize {
        self.state.borrow().buffer.bytes
    }

    pub fn limit(&self) -> usize {
        self.state.borrow().limit
    }

    pub fn empty(&self) -> bool {
        self.state.borrow().buffer.events.is_empty()
    }

    /// Number of retained body events, independent of their visible DATA bytes.
    pub fn buffered_event_count(&self) -> usize {
        self.state.borrow().buffer.events.len()
    }

    /// Whether the next queued event can advance at the given body-byte limit.
    pub fn can_dequeue_body_event(&self, maximum: u32) -> bool {
        self.state.borrow().buffer.can_dequeue(maximum)
    }

    #[cfg(test)]
    pub(crate) fn capacity_wait_ready_for_test(&self) -> bool {
        self.state
            .borrow()
            .buffer_available
            .as_ref()
            .is_some_and(|capacity| capacity.ready())
    }

    #[cfg(test)]
    pub(crate) fn retirement_borrows_available_for_test(&self) -> bool {
        self.state.try_borrow_mut().is_ok()
            && self.source.try_borrow_mut().is_ok_and(|holder| {
                let _ = holder.readable();
                true
            })
    }

    pub fn skip_content_length_validation(&mut self) {
        self.state.borrow_mut().should_validate_content_length = false;
    }

    pub fn set_expected_egress_content_length(&mut self, content_length: &[u8], eom: bool) {
        set_expected_content_length(&self.state, content_length, eom);
    }

    pub fn validate_content_length(&mut self, eom: bool) {
        notify_content_length_mismatch(&self.state, eom);
    }

    pub fn dequeue_body_event(&mut self, maximum: u32) -> Option<HttpBodyEvent> {
        let (event, signal) = {
            let mut state = self.state.borrow_mut();
            let was_over_limit = state.buffer.bytes >= state.limit;
            let event = state.buffer.dequeue(maximum)?;
            let capacity = if was_over_limit && state.buffer.bytes < state.limit {
                state.buffer_available.clone()
            } else {
                None
            };
            (event, capacity)
        };
        if let Some(capacity) = signal {
            capacity.signal();
        }
        Some(event)
    }

    pub fn clear(&mut self, error: &HttpError) {
        let (mut events, bytes_buffered, callback, capacity) = {
            let mut state = self.state.borrow_mut();
            let bytes_buffered = std::mem::take(&mut state.buffer.bytes);
            (
                std::mem::take(&mut state.buffer.events),
                bytes_buffered,
                state.callback.as_ref().and_then(Weak::upgrade),
                state.buffer_available.clone(),
            )
        };

        if let Some(capacity) = capacity {
            capacity.signal();
        }
        let cancellation_started_while_panicking = std::thread::panicking();
        let mut first_panic = None;
        events.for_each_mut(|event| {
            for registration in event.byte_event_registrations_mut() {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    registration.cancel(error, None);
                }));
                if let Err(payload) = result {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    }
                }
            }
        });
        drop(events);

        if let Some(callback) = callback {
            callback.on_egress_bytes_buffered((bytes_buffered as i64).wrapping_neg());
        }
        if let Some(payload) = first_panic {
            if cancellation_started_while_panicking {
                drop(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

impl Drop for HttpBodyEventQueue {
    fn drop(&mut self) {
        let (events, capacity) = {
            let mut state = self.state.borrow_mut();
            state.destroyed = true;
            state.active_read = None;
            state.callback = None;
            (
                std::mem::take(&mut state.buffer.events),
                state.buffer_available.take(),
            )
        };
        drop(events);
        self.source.borrow_mut().clear(None);
        if let Some(capacity) = capacity {
            capacity.signal();
        }
    }
}

#[cold]
#[inline(never)]
fn transfer_cancelled() -> HttpError {
    HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"")
}

fn begin_read(state: &Rc<RefCell<QueueState>>) -> Result<ReadLease, HttpError> {
    let identity = {
        let mut state = state.borrow_mut();
        if state.destroyed {
            return Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"HTTP body event queue is no longer readable",
            ));
        }
        if state.active_read.is_some() {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"HTTP body event queue already has an active read",
            ));
        }
        let identity = state.next_read.ok_or_else(|| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"HTTP body event queue read identities are exhausted",
            )
        })?;
        state.next_read = identity.checked_add(1);
        state.active_read = Some(identity);
        identity
    };
    Ok(ReadLease {
        state: Rc::clone(state),
        identity,
        released: false,
    })
}

fn process_header_event(
    state: &Rc<RefCell<QueueState>>,
    event: &mut HttpHeaderEvent,
) -> Result<(), HttpError> {
    if event.headers().is_response() {
        let status = event.headers_mut().status_code().map_err(|_| {
            HttpError::with_message(
                HttpErrorCode::MESSAGE_ERROR,
                b"invalid response header event",
            )
        })?;
        if status == 304 {
            state.borrow_mut().should_validate_content_length = false;
            return Ok(());
        }
    }

    if !state.borrow().should_validate_content_length {
        return Ok(());
    }
    let is_final = event.is_final().map_err(|_| {
        HttpError::with_message(HttpErrorCode::MESSAGE_ERROR, b"invalid HTTP header event")
    })?;
    if is_final {
        let content_length = event
            .headers()
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength);
        set_expected_content_length(state, content_length, event.eom());
    }
    Ok(())
}

fn process_read_body_event(
    state: &Rc<RefCell<QueueState>>,
    body_event: HttpBodyEvent,
) -> Result<ReadBodyResult, HttpError> {
    if body_event.event_type() == HttpBodyEventType::Suspend {
        let HttpBodyEventKind::Suspend(resume) = body_event.into_kind() else {
            unreachable!("the event type and owned body-event kind must agree");
        };
        return Ok(ReadBodyResult {
            resume: Some(resume),
            eom: false,
        });
    }

    let (eom, added_body_length) = process_body_event(state, body_event)?;
    if added_body_length != 0 {
        let callback = state.borrow().callback.as_ref().and_then(Weak::upgrade);
        if let Some(callback) = callback {
            callback.on_egress_bytes_buffered(added_body_length as i64);
        }
    }
    Ok(ReadBodyResult { resume: None, eom })
}

fn poll_source_or_cancellation(
    source: &mut HolderBodyRead,
    cancellation_wait: &mut Option<(CancellableWait, CancellableBaton)>,
    cancellation: &CancellationToken,
    context: &mut Context<'_>,
) -> Poll<Result<HttpBodyEvent, HttpError>> {
    match Pin::new(source).poll(context) {
        Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        Poll::Ready(Ok(value)) => {
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"",
                )))
            } else {
                Poll::Ready(Ok(value))
            }
        }
        Poll::Pending => {
            let (wait, _) = cancellation_wait.get_or_insert_with(|| {
                let baton = CancellableBaton::new();
                // Unregister the wait before destroying its baton.
                (baton.wait(cancellation.clone()), baton)
            });
            match Pin::new(wait).poll(context) {
                Poll::Ready(WaitStatus::Cancelled) => Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"",
                ))),
                Poll::Ready(_) | Poll::Pending => Poll::Pending,
            }
        }
    }
}

fn process_body_event(
    state: &Rc<RefCell<QueueState>>,
    mut body_event: HttpBodyEvent,
) -> Result<(bool, usize), HttpError> {
    let stream_id = state.borrow().stream_id;
    set_event_stream_id(&mut body_event, stream_id);
    let mut body_event = Some(body_event);
    let (eom, added_body_length) = {
        let mut state = state.borrow_mut();
        let result = state.buffer.push(&mut body_event)?;
        state.observed_body_length = state.observed_body_length.wrapping_add(result.1 as u64);
        result
    };
    drop(body_event);
    notify_content_length_mismatch(state, eom);
    Ok((eom, added_body_length))
}

fn set_event_stream_id(event: &mut HttpBodyEvent, stream_id: StreamId) {
    for registration in event.byte_event_registrations_mut() {
        registration.set_stream_id_if_missing(stream_id);
    }
}

fn set_expected_content_length(state: &Rc<RefCell<QueueState>>, content_length: &[u8], eom: bool) {
    if content_length.is_empty() {
        state.borrow_mut().should_validate_content_length = false;
        return;
    }
    let Some(content_length) = parse_folly_u64(content_length) else {
        state.borrow_mut().should_validate_content_length = false;
        return;
    };
    state.borrow_mut().expected_content_length = content_length;
    notify_content_length_mismatch(state, eom);
}

fn notify_content_length_mismatch(state: &Rc<RefCell<QueueState>>, eom: bool) {
    let notification = {
        let mut state = state.borrow_mut();
        let mismatch = state.should_validate_content_length
            && (state.observed_body_length > state.expected_content_length
                || (eom && state.observed_body_length < state.expected_content_length));
        if !mismatch {
            None
        } else {
            state.should_validate_content_length = false;
            state
                .callback
                .as_ref()
                .and_then(Weak::upgrade)
                .map(|callback| {
                    (
                        callback,
                        state.expected_content_length,
                        state.observed_body_length,
                    )
                })
        }
    };
    if let Some((callback, expected, observed)) = notification {
        callback.on_content_length_mismatch(expected, observed);
    }
}

fn parse_folly_u64(value: &[u8]) -> Option<u64> {
    let start = value
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t'..=b'\r'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(*byte, b' ' | b'\t'..=b'\r'))
        .map_or(start, |index| index + 1);
    let value = value.get(start..end)?;
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn available_buffer(state: &QueueState) -> usize {
    state.limit.saturating_sub(state.buffer.bytes)
}
