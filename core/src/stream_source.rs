use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::TimedWait;
use roxygen_runtime::wait::WaitStatus;
use thiserror::Error;

use crate::buf_queue::BufQueue;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::error::get_error_string;
use crate::events::HeaderEventError;
use crate::events::HttpBodyEvent;
use crate::events::HttpHeaderEvent;
use crate::events::HttpPushEvent;
use crate::events::ResumeStatus;
use crate::flow_control::WindowContainer;
use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::ids::MAX_STREAM_ID;
use crate::ids::StreamId;
use crate::ingress_completion::IngressCompletion;
use crate::ingress_completion::IngressCompletionPublisher;
use crate::ingress_state::HttpTransactionIngressStateMachine;
use crate::ingress_state::IngressEvent;
use crate::ingress_state::IngressState;
use crate::message::HttpMessage;
use crate::source::HttpSource;
use crate::source::LocalSourceFuture;
use crate::source_holder::HttpSourceHolder;
use crate::source_holder::RetirementPanic;
use crate::source_holder::finish_retirement_panic;
use crate::source_holder::retain_retirement_panic;
use crate::source_holder::retire_source;

const DEFAULT_RECEIVE_WINDOW: u32 = 65_535;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowControlState {
    Open,
    Closed,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StreamSourceCreateError {
    #[error("stream flow-control capacity exceeds the signed window range")]
    InvalidFlowControlCapacity,
}

/// A pending read timeout and its handling of consumer updates before arming.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeferredReadTimeout {
    /// Consumer updates remain deferred; arming applies the latest duration.
    DeferUpdates(Duration),
    /// Consumer updates apply immediately; arming restores this duration.
    ReplaceUpdates(Duration),
    /// The first consumer update applies immediately and cancels this timeout.
    CancelOnUpdate(Duration),
}

impl DeferredReadTimeout {
    fn duration(self) -> Duration {
        match self {
            Self::DeferUpdates(timeout)
            | Self::ReplaceUpdates(timeout)
            | Self::CancelOnUpdate(timeout) => timeout,
        }
    }
}

/// Owner-local flow-control and completion callbacks for a stream source.
pub trait HttpStreamSourceCallback: 'static {
    fn bytes_processed(&mut self, _id: StreamId, _amount: usize, _to_acknowledge: usize) {}

    fn window_open(&mut self, _id: StreamId) {}

    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {}
}

/// Owner-local callbacks retained through shared ownership by a stream source.
/// Each source serializes its event delivery, including reentrant events.
pub trait HttpStreamSourceSharedCallback: 'static {
    fn bytes_processed(&self, _id: StreamId, _amount: usize, _to_acknowledge: usize) {}

    fn window_open(&self, _id: StreamId) {}

    fn source_complete(&self, _id: StreamId, _error: Option<HttpError>) {}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadState {
    HeaderEvents,
    BodyEvents,
}

struct QueuedHeader {
    message: Box<HttpMessage>,
    eom: bool,
}

enum QueuedBodyKind {
    Body(BufQueue),
    PushPromise(HttpPushEvent),
    Trailers(HttpHeaders),
    Padding(u16),
}

struct QueuedBody {
    kind: QueuedBodyKind,
    eom: bool,
}

impl QueuedBody {
    fn into_event(self) -> HttpBodyEvent {
        match self.kind {
            QueuedBodyKind::Body(body) => HttpBodyEvent::body(body, self.eom),
            QueuedBodyKind::PushPromise(push) => HttpBodyEvent::from_push_event(push, self.eom),
            QueuedBodyKind::Trailers(trailers) => HttpBodyEvent::trailers(trailers),
            QueuedBodyKind::Padding(bytes) => HttpBodyEvent::padding(bytes, self.eom),
        }
    }
}

enum IngressQueue<T> {
    Inline(Option<T>),
    Heap(VecDeque<T>),
}

impl<T> IngressQueue<T> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Inline(event) => event.is_none(),
            Self::Heap(events) => events.is_empty(),
        }
    }

    fn back(&self) -> Option<&T> {
        match self {
            Self::Inline(event) => event.as_ref(),
            Self::Heap(events) => heap_back(events),
        }
    }

    fn back_mut(&mut self) -> Option<&mut T> {
        match self {
            Self::Inline(event) => event.as_mut(),
            Self::Heap(events) => heap_back_mut(events),
        }
    }

    fn pop_back(&mut self) -> Option<T> {
        match self {
            Self::Inline(event) => event.take(),
            Self::Heap(events) => heap_pop_back(events),
        }
    }

    fn push_back(&mut self, event: T) {
        match self {
            Self::Inline(slot) => {
                if let Some(previous) = slot.take() {
                    self.spill(previous, event);
                } else {
                    *slot = Some(event);
                }
            }
            Self::Heap(events) => heap_push_back(events, event),
        }
    }

    fn pop_front(&mut self) -> Option<T> {
        match self {
            Self::Inline(event) => event.take(),
            Self::Heap(events) => heap_pop_front(events),
        }
    }

    fn push_front(&mut self, event: T) {
        match self {
            Self::Inline(slot) => {
                if let Some(previous) = slot.take() {
                    self.spill(event, previous);
                } else {
                    *slot = Some(event);
                }
            }
            Self::Heap(events) => heap_push_front(events, event),
        }
    }

    #[cold]
    #[inline(never)]
    fn spill(&mut self, first: T, second: T) {
        let mut events = VecDeque::new();
        events.push_back(first);
        events.push_back(second);
        *self = Self::Heap(events);
    }

    fn clear(&mut self) {
        match self {
            Self::Inline(event) => drop(event.take()),
            Self::Heap(events) => events.clear(),
        }
    }
}

#[inline(never)]
fn heap_back<T>(events: &VecDeque<T>) -> Option<&T> {
    events.back()
}

#[inline(never)]
fn heap_back_mut<T>(events: &mut VecDeque<T>) -> Option<&mut T> {
    events.back_mut()
}

#[inline(never)]
fn heap_pop_back<T>(events: &mut VecDeque<T>) -> Option<T> {
    events.pop_back()
}

#[inline(never)]
fn heap_pop_front<T>(events: &mut VecDeque<T>) -> Option<T> {
    events.pop_front()
}

#[inline(never)]
fn heap_push_back<T>(events: &mut VecDeque<T>, event: T) {
    events.push_back(event);
}

#[inline(never)]
fn heap_push_front<T>(events: &mut VecDeque<T>, event: T) {
    events.push_front(event);
}

struct StoredError {
    error: HttpError,
    ingress: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadActivity {
    General(u64),
    Exclusive,
}

struct StreamState {
    id: StreamId,
    error: Option<StoredError>,
    header_queue: IngressQueue<QueuedHeader>,
    body_queue: IngressQueue<QueuedBody>,
    receive_window: WindowContainer,
    expected_content_length: u64,
    remaining_content_length: u64,
    ingress_state: IngressState,
    read_state: ReadState,
    active_read: Option<ReadActivity>,
    next_read_id: u64,
    sink_mode: bool,
    drain_on_drop: bool,
    draining: bool,
    source_complete: bool,
    strict_flow_control: bool,
    should_validate_content_length: bool,
    can_suspend: bool,
    suspend_wait_active: bool,
    /// The largest growth hint of a body that left the queue, so coalescing
    /// grows a body container once instead of doubling it.
    body_segment_hint: u16,
}

struct StreamShared {
    owner_thread: ThreadId,
    state: RefCell<StreamState>,
    callback: RefCell<CallbackSlot>,
    // Keep the host alive through state and callback destruction without a baton.
    host: Host,
    event: OnceCell<TimedBaton>,
    /// A pending exclusive read waiting for the event without a `TimedWait`.
    direct_wait: RefCell<Option<DirectWait>>,
    /// The `TimedWait` that took over that read's wait when a read timeout was
    /// set during it, until the read next polls.
    retimed_wait: RefCell<Option<TimedWait>>,
    uninitialized_read_timeout: Cell<Duration>,
    deferred_read_timeout: Cell<Option<DeferredReadTimeout>>,
    ingress_completion: IngressCompletionPublisher,
}

/// An exclusive read's wait for the source event while the source has no read
/// timeout. It stands in for a `TimedWait` on `event` with no cancellation, so
/// it is woken as `event` wakes its registered waiter, and `event` is neither
/// created nor signalled for it.
struct DirectWait {
    waker: Waker,
    /// Whether the event was signalled since the wait started, the one status
    /// change after which `TimedBaton` stops waking its waiter.
    signalled: bool,
}

enum CallbackOwner {
    Owned(Box<dyn HttpStreamSourceCallback>),
    Shared(Rc<dyn HttpStreamSourceSharedCallback>),
}

impl HttpStreamSourceCallback for CallbackOwner {
    fn bytes_processed(&mut self, id: StreamId, amount: usize, to_acknowledge: usize) {
        match self {
            Self::Owned(callback) => callback.bytes_processed(id, amount, to_acknowledge),
            Self::Shared(callback) => callback.bytes_processed(id, amount, to_acknowledge),
        }
    }

    fn window_open(&mut self, id: StreamId) {
        match self {
            Self::Owned(callback) => callback.window_open(id),
            Self::Shared(callback) => callback.window_open(id),
        }
    }

    fn source_complete(&mut self, id: StreamId, error: Option<HttpError>) {
        match self {
            Self::Owned(callback) => callback.source_complete(id, error),
            Self::Shared(callback) => callback.source_complete(id, error),
        }
    }
}

struct CallbackSlot {
    callback: Option<CallbackOwner>,
    pending: VecDeque<CallbackEvent>,
    dispatching: bool,
}

enum CallbackEvent {
    BytesProcessed {
        id: StreamId,
        amount: usize,
        to_acknowledge: usize,
    },
    WindowOpen(StreamId),
    SourceComplete {
        id: StreamId,
        error: Option<HttpError>,
    },
}

impl StreamShared {
    fn assert_owner(&self) {
        debug_assert_eq!(std::thread::current().id(), self.owner_thread);
    }

    fn reset_event(&self) {
        if let Some(event) = self.event.get() {
            event.reset();
        }
    }

    fn event(&self) -> &TimedBaton {
        self.event.get_or_init(|| {
            TimedBaton::with_host(
                Some(self.uninitialized_read_timeout.get()),
                self.host.clone(),
            )
        })
    }

    fn wait_event(&self, cancellation: CancellationToken) -> TimedWait {
        self.event().wait(cancellation)
    }

    fn signal_event(&self) {
        if self.direct_wait.borrow().is_some() {
            return self.signal_direct_wait();
        }
        if let Some(event) = self.event.get() {
            event.signal();
        }
    }

    fn signal_direct_wait(&self) {
        let waker = match self.direct_wait.borrow_mut().as_mut() {
            Some(wait) if !wait.signalled => {
                wait.signalled = true;
                wait.waker.clone()
            }
            _ => return,
        };
        waker.wake();
    }

    /// Whether a wait that starts now would arm a read timeout.
    fn has_read_timeout(&self) -> bool {
        self.event.get().map_or_else(
            || !self.uninitialized_read_timeout.get().is_zero(),
            |event| event.timeout().is_some(),
        )
    }

    /// Applies a timeout update to a direct wait as `TimedBaton::set_timeout`
    /// applies it to a registered `TimedWait`: it wakes the waiter, and an
    /// enabled timeout counts from now. For that, an unsignalled wait moves to
    /// the `TimedWait` it stands in for, registered with the same waker.
    #[cold]
    #[inline(never)]
    fn retime_direct_wait(&self, timeout: Duration) {
        let signalled = self
            .direct_wait
            .borrow()
            .as_ref()
            .is_some_and(|wait| wait.signalled);
        if timeout.is_zero() || signalled {
            self.store_read_timeout(timeout);
            let waker = self
                .direct_wait
                .borrow()
                .as_ref()
                .map(|wait| wait.waker.clone());
            if let Some(waker) = waker {
                waker.wake();
            }
            return;
        }
        let Some(DirectWait { waker, .. }) = self.direct_wait.take() else {
            return self.store_read_timeout(timeout);
        };
        // The event was reset when the wait started and is not signalled.
        let event = self.event();
        let mut wait = event.wait(CancellationToken::never());
        let registered = Pin::new(&mut wait).poll(&mut Context::from_waker(&waker));
        debug_assert!(registered.is_pending(), "an unsignalled wait registers");
        *self.retimed_wait.borrow_mut() = Some(wait);
        event.set_timeout(Some(timeout));
    }

    fn event_status(&self) -> WaitStatus {
        self.event
            .get()
            .map_or(WaitStatus::NotReady, TimedBaton::status)
    }

    fn read_timeout(&self) -> Duration {
        self.event.get().map_or_else(
            || self.uninitialized_read_timeout.get(),
            |event| event.timeout().unwrap_or_default(),
        )
    }

    fn set_read_timeout(&self, timeout: Duration) {
        if self.direct_wait.borrow().is_some() {
            self.retime_direct_wait(timeout);
        } else {
            self.store_read_timeout(timeout);
        }
    }

    fn store_read_timeout(&self, timeout: Duration) {
        if let Some(event) = self.event.get() {
            event.set_timeout(Some(timeout));
        } else {
            self.uninitialized_read_timeout.set(timeout);
        }
    }
}

/// The single-consumer, owner-local half of a producer-fed HTTP source.
pub struct HttpStreamSource {
    shared: Rc<StreamShared>,
}

/// Owns one concrete stream source and stops it before unfinished destruction.
///
/// Reads retain their own state without borrowing this move-only owner.
pub struct HttpStreamSourceOwner {
    source: Option<HttpStreamSource>,
}

impl HttpStreamSourceOwner {
    /// Takes ownership without reading or changing the source.
    pub fn new(source: HttpStreamSource) -> Self {
        Self {
            source: Some(source),
        }
    }

    /// Borrows the source while it remains owned.
    pub fn source(&self) -> Option<&HttpStreamSource> {
        self.source.as_ref()
    }

    /// Stops and destroys the owned source, even if its stop callback panics.
    ///
    /// The owner is emptied before callbacks run. The first cleanup panic is
    /// propagated unless the caller is already unwinding.
    pub fn clear(&mut self, error: Option<HttpErrorCode>) {
        let unwinding = std::thread::panicking();
        let failure = self
            .source
            .take()
            .and_then(|source| retire_source(source, |source| source.stop_reading(error)));
        finish_retirement_panic(failure, unwinding);
    }

    /// Destroys the source after a terminal read without an additional explicit stop.
    pub fn retire_terminal(&mut self) {
        let unwinding = std::thread::panicking();
        let source = self.source.take();
        let failure = catch_unwind(AssertUnwindSafe(|| drop(source))).err();
        finish_retirement_panic(failure, unwinding);
    }

    /// Transfers the same source into the generic holder without reading or stopping it.
    ///
    /// An already empty owner produces an empty holder.
    pub fn into_source_holder(mut self) -> HttpSourceHolder {
        self.source
            .take()
            .map_or_else(HttpSourceHolder::empty, |source| {
                HttpSourceHolder::new(Box::new(source))
            })
    }
}

impl Drop for HttpStreamSourceOwner {
    fn drop(&mut self) {
        self.clear(None);
    }
}

/// A fresh response source that has never exposed an independently owned read.
pub struct HttpResponseSource {
    owner: HttpStreamSourceOwner,
}

impl HttpResponseSource {
    /// Creates a response source with the session's receive window and no read timeout.
    pub fn new(
        id: StreamId,
        callback: Option<Box<dyn HttpStreamSourceCallback>>,
        receive_window: u32,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        HttpStreamSource::new(
            id,
            callback.map(CallbackOwner::Owned),
            receive_window,
            Duration::ZERO,
            true,
            host,
        )
        .map(|source| Self {
            owner: HttpStreamSourceOwner::new(source),
        })
    }

    /// Creates a response source retaining a shared callback and no read timeout.
    pub fn new_shared(
        id: StreamId,
        callback: Rc<dyn HttpStreamSourceSharedCallback>,
        receive_window: u32,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        HttpStreamSource::new(
            id,
            Some(CallbackOwner::Shared(callback)),
            receive_window,
            Duration::ZERO,
            true,
            host,
        )
        .map(|source| Self {
            owner: HttpStreamSourceOwner::new(source),
        })
    }

    /// Returns the original source's non-owning producer.
    pub fn producer(&self) -> HttpStreamSourceProducer {
        self.owner
            .source()
            .expect("a fresh response is owned")
            .producer()
    }

    /// Observes producer completion independently of response consumption.
    pub fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.owner.source().and_then(HttpSource::ingress_completion)
    }

    /// Suppresses body-length validation for a response to HEAD.
    pub fn skip_content_length_validation(&self) {
        self.owner
            .source()
            .expect("a fresh response is owned")
            .skip_content_length_validation();
    }

    /// Initializes a source whose header has been delivered separately.
    pub fn initialize_body_only(&self, message: &mut HttpMessage, eom: bool) -> bool {
        self.owner
            .source()
            .expect("a fresh response is owned")
            .initialize_body_only(message, eom)
    }

    /// Separates the detachable source owner from its unique read capability.
    pub fn split(self) -> (HttpResponseSourceOwner, HttpResponseReader) {
        let shared = Rc::downgrade(
            &self
                .owner
                .source()
                .expect("a fresh response is owned")
                .shared,
        );
        (
            HttpResponseSourceOwner { owner: self.owner },
            HttpResponseReader {
                shared,
                active: None,
                kind: ReadKind::Header,
                maximum: u32::MAX,
                wait: ExclusiveWait::Idle,
                admitted: false,
                completed: true,
                body_started: false,
                suspend_free: false,
                armed_idle: false,
            },
        )
    }

    /// Consumes fresh exclusivity before exposing the general source API.
    pub fn into_source_owner(self) -> HttpStreamSourceOwner {
        self.owner
    }
}

/// The detachable owner paired with one exclusive response reader.
pub struct HttpResponseSourceOwner {
    owner: HttpStreamSourceOwner,
}

impl HttpResponseSourceOwner {
    /// Whether the original source is still owned.
    pub fn readable(&self) -> bool {
        self.owner.source().is_some()
    }

    /// Identifies the original stream while its source is still owned.
    pub fn stream_id(&self) -> Option<StreamId> {
        self.owner.source().map(HttpStreamSource::id)
    }

    /// Stops and destroys the source before independently owned read retirement.
    pub fn clear(&mut self, error: Option<HttpErrorCode>) {
        self.owner.clear(error);
    }

    /// Destroys the source after a terminal read without explicitly stopping it.
    pub fn retire_terminal(&mut self) {
        self.owner.retire_terminal();
    }

    /// Converts matching idle handles after final headers and before body polling.
    /// Rejection returns both handles unchanged, without stopping or reading them.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the original capabilities without allocating"
    )]
    pub fn try_into_source_holder(
        self,
        reader: HttpResponseReader,
    ) -> Result<HttpSourceHolder, (Self, HttpResponseReader)> {
        let valid = self.owner.source().is_some_and(|source| {
            let state = source.shared.state.borrow();
            reader.matches(source)
                && !reader.is_active()
                && !reader.body_started
                && state.read_state == ReadState::BodyEvents
                && state.active_read.is_none()
                && !state.suspend_wait_active
        });
        if !valid {
            return Err((self, reader));
        }
        drop(reader);
        Ok(self.owner.into_source_holder())
    }
}

/// A unique response read phase, weak between operations and stationary while pending.
pub struct HttpResponseReader {
    shared: Weak<StreamShared>,
    active: Option<Rc<StreamShared>>,
    kind: ReadKind,
    maximum: u32,
    wait: ExclusiveWait,
    admitted: bool,
    completed: bool,
    body_started: bool,
    /// Empty body reads wait instead of suspending (`DIV-045`).
    suspend_free: bool,
    /// A suspend-free body read stays armed after a non-terminal event,
    /// holding only `shared` until its next poll (`DIV-045`).
    armed_idle: bool,
}

impl HttpResponseReader {
    fn matches(&self, source: &HttpStreamSource) -> bool {
        self.shared.as_ptr() == Rc::as_ptr(&source.shared)
    }

    /// Whether a read is armed or pending. A pending operation retains the
    /// original source state; a suspend-free reader that stays armed between
    /// body events holds only its weak reference (`DIV-045`).
    pub fn is_active(&self) -> bool {
        self.active.is_some() || self.armed_idle
    }

    /// Makes an empty body read wait in place for the next event instead of
    /// returning `HttpBodyEvent::suspend`, the ported `SUSPEND` (`DIV-045`).
    /// The default, `false`, keeps the ported protocol. The setting belongs to
    /// this reader: header reads and a source converted by
    /// `try_into_source_holder` are unchanged. A change does not interrupt a
    /// read that is already waiting: that read still completes with its next
    /// event, error or read timeout, and the setting applies when a body read
    /// next finds no event queued.
    ///
    /// With it set, a body read that returns a non-terminal event leaves the
    /// reader armed for the next body read with the same maximum: it stays
    /// active, and `poll_body` reads on without `arm_body`, which still re-arms
    /// it with a new maximum. Between events it holds only its weak reference,
    /// so if the owner releases the source, that next poll fails as `arm_body`
    /// would. Terminal events and errors retire the read.
    pub fn set_suspend_free(&mut self, suspend_free: bool) {
        self.suspend_free = suspend_free;
    }

    /// Arms a header read without validation, wait registration, or callbacks.
    pub fn arm_header(&mut self, owner: &HttpResponseSourceOwner) -> Result<(), HttpError> {
        self.arm(owner, ReadKind::Header, u32::MAX)
    }

    /// Arms a body read; its maximum remains fixed until the operation retires.
    /// This method does not validate HTTP state, register waits, or invoke callbacks.
    pub fn arm_body(
        &mut self,
        owner: &HttpResponseSourceOwner,
        maximum: u32,
    ) -> Result<(), HttpError> {
        self.arm(owner, ReadKind::Body, maximum)
    }

    fn arm(
        &mut self,
        owner: &HttpResponseSourceOwner,
        kind: ReadKind,
        maximum: u32,
    ) -> Result<(), HttpError> {
        // A reader armed between body events re-arms or, on failure, retires.
        self.armed_idle = false;
        let Some(source) = owner.owner.source() else {
            return Err(source_not_readable());
        };
        if self.active.is_some() || !self.matches(source) {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response read requires its matching idle source owner",
            ));
        }
        self.active = Some(Rc::clone(&source.shared));
        self.kind = kind;
        self.maximum = maximum;
        self.admitted = false;
        self.completed = false;
        Ok(())
    }

    /// Polls the armed header operation after the source-owner borrow has ended.
    pub fn poll_header(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        self.poll(context, ReadKind::Header).map(|result| {
            result.map(|event| match event {
                StreamReadOutput::Header(event) => event,
                StreamReadOutput::Body(_) => unreachable!("a header operation returns headers"),
            })
        })
    }

    /// Polls the armed body operation after the source-owner borrow has ended.
    #[inline]
    pub fn poll_body(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.poll(context, ReadKind::Body).map(|result| {
            result.map(|event| match event {
                StreamReadOutput::Body(event) => event,
                StreamReadOutput::Header(_) => unreachable!("a body operation returns body events"),
            })
        })
    }

    fn poll(
        &mut self,
        context: &mut Context<'_>,
        kind: ReadKind,
    ) -> Poll<Result<StreamReadOutput, HttpError>> {
        let mut guard = ResponseReadPollGuard {
            reader: self,
            armed: true,
        };
        let result = guard.reader.poll_inner(context, kind);
        if let Poll::Ready(output) = &result {
            if guard.reader.stays_armed(output) {
                guard.reader.admitted = false;
                guard.reader.armed_idle = true;
                drop(guard.reader.active.take());
            } else {
                guard.reader.retire_active_read();
            }
        }
        guard.armed = false;
        result
    }

    /// Whether a completed read leaves this reader armed for the next body read
    /// (`DIV-045`). The owner's reference makes dropping the read's reference
    /// trivial.
    fn stays_armed(&self, output: &Result<StreamReadOutput, HttpError>) -> bool {
        self.suspend_free
            && matches!(output, Ok(StreamReadOutput::Body(event)) if !event.eom())
            && self
                .active
                .as_ref()
                .is_some_and(|shared| Rc::strong_count(shared) > 1)
    }

    fn poll_inner(
        &mut self,
        context: &mut Context<'_>,
        kind: ReadKind,
    ) -> Poll<Result<StreamReadOutput, HttpError>> {
        if self.armed_idle {
            self.armed_idle = false;
            if kind != self.kind {
                return Poll::Ready(Err(response_read_not_armed()));
            }
            let Some(shared) = self.shared.upgrade() else {
                return Poll::Ready(Err(source_not_readable()));
            };
            self.active = Some(shared);
            self.completed = false;
        }
        let Some(shared) = &self.active else {
            return Poll::Ready(Err(response_read_not_armed()));
        };
        assert!(
            kind == self.kind,
            "response read kind changed while pending"
        );
        shared.assert_owner();
        if !self.admitted {
            self.body_started |= matches!(kind, ReadKind::Body);
            validate_read(shared, kind, self.maximum)?;
            let mut state = shared.state.borrow_mut();
            if state.active_read.is_some() || state.suspend_wait_active {
                return Poll::Ready(Err(read_already_active()));
            }
            state.active_read = Some(ReadActivity::Exclusive);
            self.admitted = true;
        }
        ReadOperation {
            shared,
            role: ExclusiveRead {
                suspend_free: self.suspend_free,
            },
            kind,
            maximum: self.maximum,
            wait: &mut self.wait,
            completed: &mut self.completed,
        }
        .poll(context)
    }

    /// Retires only the active read and wait, leaving the real source owner intact.
    pub fn retire_active_read(&mut self) {
        self.armed_idle = false;
        let Some(shared) = self.active.take() else {
            return;
        };
        let admitted = std::mem::replace(&mut self.admitted, false);
        let unwinding = std::thread::panicking();
        let mut first = if admitted {
            ReadOperation {
                shared: &shared,
                role: ExclusiveRead {
                    suspend_free: self.suspend_free,
                },
                kind: self.kind,
                maximum: self.maximum,
                wait: &mut self.wait,
                completed: &mut self.completed,
            }
            .retire_inner()
        } else {
            catch_unwind(AssertUnwindSafe(|| self.wait.release(&shared))).err()
        };
        self.completed = true;
        retain_retirement_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| drop(shared))).err(),
        );
        finish_retirement_panic(first, unwinding);
    }
}

impl Drop for HttpResponseReader {
    fn drop(&mut self) {
        self.retire_active_read();
    }
}

struct ResponseReadPollGuard<'a> {
    reader: &'a mut HttpResponseReader,
    armed: bool,
}

impl Drop for ResponseReadPollGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.reader.retire_active_read();
        }
    }
}

/// A fresh, exclusive ingress body with no independently admitted reader.
///
/// It can be moved into a body transfer or consumed into the general source API
/// before reading. Construction never adopts an existing source or read.
pub struct HttpIngressBody {
    owner: HttpStreamSourceOwner,
}

impl HttpIngressBody {
    /// Creates an owner-local ingress body with no read timeout.
    pub fn new(
        id: StreamId,
        callback: impl HttpStreamSourceCallback,
        receive_window: u32,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        HttpStreamSource::new_session_with_host(id, callback, receive_window, Duration::ZERO, host)
            .map(|source| Self {
                owner: HttpStreamSourceOwner::new(source),
            })
    }

    /// Creates an ingress body retaining a shared callback and no read timeout.
    pub fn new_shared(
        id: StreamId,
        callback: Rc<dyn HttpStreamSourceSharedCallback>,
        receive_window: u32,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        HttpStreamSource::new(
            id,
            Some(CallbackOwner::Shared(callback)),
            receive_window,
            Duration::ZERO,
            true,
            host,
        )
        .map(|source| Self {
            owner: HttpStreamSourceOwner::new(source),
        })
    }

    /// Returns the non-owning producer for this fresh body.
    pub fn producer(&self) -> HttpStreamSourceProducer {
        self.owner
            .source()
            .expect("a fresh source is owned")
            .producer()
    }

    /// Observes producer completion independently of body consumption.
    pub fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.owner.source().and_then(HttpSource::ingress_completion)
    }

    /// Keeps graceful draining and its flow credit alive after consumer Drop.
    pub fn drain_on_drop(&self) -> HttpStreamSourceDrain {
        self.owner
            .source()
            .expect("a fresh source is owned")
            .drain_on_drop()
    }

    /// Validates the separately delivered request head and enters body reading.
    pub fn initialize_body_only(&self, message: &mut HttpMessage, eom: bool) -> bool {
        self.owner
            .source()
            .expect("a fresh source is owned")
            .initialize_body_only(message, eom)
    }

    /// Consumes the terminal empty event before a header-only request handoff.
    /// Success returns an empty capability. Only a genuine EOM retires the
    /// source as terminal; a read error takes the normal stop path on drop.
    pub fn retire_empty_body(mut self, context: &mut Context<'_>) -> Result<Self, HttpError> {
        let mut terminal = self
            .owner
            .source()
            .expect("a fresh source is owned")
            .read_body_event_unboxed(1, CancellationToken::never());
        let poll = Pin::new(&mut terminal).poll(context);
        if let Poll::Ready(result) = &poll
            && result.as_ref().is_ok_and(HttpBodyEvent::eom)
        {
            self.owner.retire_terminal();
        }
        let result = match poll {
            Poll::Ready(Ok(event))
                if event.eom() && event.as_body().is_some_and(BufQueue::is_empty) =>
            {
                Ok(())
            }
            Poll::Ready(Err(error)) => Err(error),
            _ => Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"header-only request did not expose its terminal body read",
            )),
        };
        drop(terminal);
        result?;
        Ok(self)
    }

    /// Transfers the original source to the general API without polling it.
    pub fn into_source_owner(self) -> HttpStreamSourceOwner {
        self.owner
    }

    pub(crate) fn readable(&self) -> bool {
        self.owner.source().is_some()
    }

    pub(crate) fn clear(&mut self, error: Option<HttpErrorCode>) {
        self.owner.clear(error);
    }

    pub(crate) fn into_receiver(self) -> IngressBodyReceiver {
        IngressBodyReceiver {
            source: IngressReceiverSource::Owned(self.owner),
            phase: IngressReadPhase::Idle,
        }
    }
}

/// Keeps producer validation and flow credit alive after the consumer stops reading.
///
/// A graceful consumer stop discards unread body until producer EOM. Dropping this
/// owner before EOM cancels an active drain, or restores normal consumer-drop behavior
/// if the consumer is still reading. It is not another consumer.
pub struct HttpStreamSourceDrain {
    shared: Rc<StreamShared>,
}

impl Drop for HttpStreamSourceDrain {
    fn drop(&mut self) {
        let draining = {
            let mut state = self.shared.state.borrow_mut();
            state.drain_on_drop = false;
            state.draining && !state.source_complete && !is_eom_seen(&state)
        };
        if draining {
            let unwinding = std::thread::panicking();
            let failure = catch_unwind(AssertUnwindSafe(|| {
                stop_reading(&self.shared, Some(HttpErrorCode::DROPPED));
            }))
            .err();
            finish_retirement_panic(failure, unwinding);
        }
    }
}

impl HttpStreamSource {
    pub fn new_session(
        id: StreamId,
        callback: impl HttpStreamSourceCallback,
        receive_window: u32,
        read_timeout: Duration,
    ) -> Result<Self, StreamSourceCreateError> {
        Self::new_session_with_host(id, callback, receive_window, read_timeout, Host::tokio())
    }

    /// Creates a session source using the owner's clock and timers.
    pub fn new_session_with_host(
        id: StreamId,
        callback: impl HttpStreamSourceCallback,
        receive_window: u32,
        read_timeout: Duration,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        Self::new(
            id,
            Some(CallbackOwner::Owned(Box::new(callback))),
            receive_window,
            read_timeout,
            true,
            host,
        )
    }

    pub fn new_async(
        id: Option<StreamId>,
        callback: Option<Box<dyn HttpStreamSourceCallback>>,
        egress_buffer_size: u32,
    ) -> Result<Self, StreamSourceCreateError> {
        Self::new_async_with_host(id, callback, egress_buffer_size, Host::tokio())
    }

    /// Creates an asynchronous source using the owner's clock and timers.
    pub fn new_async_with_host(
        id: Option<StreamId>,
        callback: Option<Box<dyn HttpStreamSourceCallback>>,
        egress_buffer_size: u32,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        Self::new(
            id.unwrap_or(MAX_STREAM_ID),
            callback.map(CallbackOwner::Owned),
            egress_buffer_size,
            Duration::ZERO,
            false,
            host,
        )
    }

    pub fn default_session(
        id: StreamId,
        callback: impl HttpStreamSourceCallback,
    ) -> Result<Self, StreamSourceCreateError> {
        Self::new_session(id, callback, DEFAULT_RECEIVE_WINDOW, DEFAULT_READ_TIMEOUT)
    }

    fn new(
        id: StreamId,
        callback: Option<CallbackOwner>,
        receive_window: u32,
        read_timeout: Duration,
        strict_flow_control: bool,
        host: Host,
    ) -> Result<Self, StreamSourceCreateError> {
        let receive_window = WindowContainer::new(receive_window)
            .ok_or(StreamSourceCreateError::InvalidFlowControlCapacity)?;
        // `new_cyclic` allocates before the value is built, so the value is written in place
        // instead of being built on the stack and copied into the allocation.
        Ok(Self {
            shared: Rc::new_cyclic(|_| StreamShared {
                owner_thread: host.owner_thread(),
                state: RefCell::new(StreamState {
                    id,
                    error: None,
                    header_queue: IngressQueue::Inline(None),
                    body_queue: IngressQueue::Inline(None),
                    receive_window,
                    expected_content_length: 0,
                    remaining_content_length: 0,
                    ingress_state: HttpTransactionIngressStateMachine::get_new_instance(),
                    read_state: ReadState::HeaderEvents,
                    active_read: None,
                    next_read_id: 1,
                    sink_mode: false,
                    drain_on_drop: false,
                    draining: false,
                    source_complete: false,
                    strict_flow_control,
                    should_validate_content_length: true,
                    can_suspend: true,
                    suspend_wait_active: false,
                    body_segment_hint: 0,
                }),
                callback: RefCell::new(CallbackSlot {
                    callback,
                    pending: VecDeque::new(),
                    dispatching: false,
                }),
                host,
                event: OnceCell::new(),
                direct_wait: RefCell::new(None),
                retimed_wait: RefCell::new(None),
                uninitialized_read_timeout: Cell::new(read_timeout),
                deferred_read_timeout: Cell::new(None),
                ingress_completion: IngressCompletionPublisher::default(),
            }),
        })
    }

    pub fn producer(&self) -> HttpStreamSourceProducer {
        HttpStreamSourceProducer {
            shared: Rc::downgrade(&self.shared),
        }
    }

    /// Preserve ingress through producer EOM when the consumer stops without an error.
    ///
    /// The returned owner must remain alive until ingress completes or is aborted.
    /// Only one drain owner may be installed for a source at a time.
    pub fn drain_on_drop(&self) -> HttpStreamSourceDrain {
        self.shared.assert_owner();
        let mut state = self.shared.state.borrow_mut();
        assert!(!state.drain_on_drop, "source already has a drain owner");
        state.drain_on_drop = true;
        HttpStreamSourceDrain {
            shared: Rc::clone(&self.shared),
        }
    }

    pub fn id(&self) -> StreamId {
        self.shared.state.borrow().id
    }

    pub fn is_unprocessed(&self) -> bool {
        self.shared.state.borrow().ingress_state == IngressState::Start
    }

    pub fn headers_allowed(&self) -> bool {
        matches!(
            self.shared.state.borrow().ingress_state,
            IngressState::Start | IngressState::NonFinalHeadersReceived
        )
    }

    pub fn mark_egress_only(&self) {
        self.shared.assert_owner();
        self.shared.state.borrow_mut().ingress_state = IngressState::ReceivingDone;
        enable_sink_mode(&self.shared);
    }

    pub fn skip_content_length_validation(&self) {
        self.shared
            .state
            .borrow_mut()
            .should_validate_content_length = false;
    }

    pub fn is_eom_seen(&self) -> bool {
        matches!(
            self.shared.state.borrow().ingress_state,
            IngressState::EomQueued | IngressState::ReceivingDone
        )
    }

    pub fn input_finished(&self) -> bool {
        let state = self.shared.state.borrow();
        state.error.is_some()
            || matches!(
                state.ingress_state,
                IngressState::EomQueued | IngressState::ReceivingDone
            )
    }

    pub fn source_complete(&self) -> bool {
        self.shared.state.borrow().source_complete
    }

    pub fn set_callback(&self, callback: Option<Box<dyn HttpStreamSourceCallback>>) {
        self.shared.callback.borrow_mut().callback = callback.map(CallbackOwner::Owned);
    }

    pub fn window_size(&self) -> i32 {
        self.shared.state.borrow().receive_window.size()
    }

    pub fn body_bytes_buffered(&self) -> u32 {
        self.shared
            .state
            .borrow()
            .receive_window
            .window()
            .outstanding()
    }

    pub fn read_timeout(&self) -> Duration {
        self.shared.read_timeout()
    }

    pub fn set_read_timeout_value(&self, timeout: Duration) {
        match self.shared.deferred_read_timeout.get() {
            Some(DeferredReadTimeout::DeferUpdates(_)) => {
                self.shared
                    .deferred_read_timeout
                    .set(Some(DeferredReadTimeout::DeferUpdates(timeout)));
                return;
            }
            Some(DeferredReadTimeout::CancelOnUpdate(_)) => {
                self.shared.deferred_read_timeout.set(None);
            }
            Some(DeferredReadTimeout::ReplaceUpdates(_)) | None => {}
        }
        self.shared.set_read_timeout(timeout);
    }

    pub fn read_header_event_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(self.read_header_event_unboxed(cancellation))
    }

    /// Creates a lazy header read without boxing its operation state.
    ///
    /// Admission and phase validation happen on first poll. The returned
    /// owner-local, `Unpin` future does not borrow this source. Dropping it before
    /// polling neither admits a read nor marks the source consumed.
    pub fn read_header_event_unboxed(
        &self,
        cancellation: CancellationToken,
    ) -> HttpStreamHeaderRead {
        HttpStreamHeaderRead {
            phase: StreamReadState::Unstarted {
                shared: Rc::clone(&self.shared),
                kind: ReadKind::Header,
                maximum: u32::MAX,
                cancellation,
            },
        }
    }

    pub fn read_body_event_with_cancellation(
        &self,
        maximum: u32,
        cancellation: CancellationToken,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(self.read_body_event_unboxed(maximum, cancellation))
    }

    /// Creates a lazy body read without boxing its operation state.
    ///
    /// Admission and phase/maximum validation happen on first poll. The returned
    /// owner-local, `Unpin` future does not borrow this source. Dropping it before
    /// polling neither admits a read nor marks the source consumed.
    pub fn read_body_event_unboxed(
        &self,
        maximum: u32,
        cancellation: CancellationToken,
    ) -> HttpStreamBodyRead {
        HttpStreamBodyRead {
            phase: StreamReadState::Unstarted {
                shared: Rc::clone(&self.shared),
                kind: ReadKind::Body,
                maximum,
                cancellation,
            },
        }
    }

    /// Initializes the source for a consumer that intentionally skips headers.
    pub fn initialize_body_only(&self, message: &mut HttpMessage, eom: bool) -> bool {
        self.shared.assert_owner();
        let valid_setup = {
            let state = self.shared.state.borrow();
            state.read_state == ReadState::HeaderEvents && state.header_queue.is_empty()
        };
        if !valid_setup {
            set_error(
                &self.shared,
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"body-only initialization after header consumption",
                false,
            );
            return false;
        }
        let valid = validate_headers(&self.shared, message, eom);
        self.shared.state.borrow_mut().read_state = ReadState::BodyEvents;
        if valid && eom {
            record_ingress_eom(&self.shared);
        }
        valid
    }
}

impl HttpSource for HttpStreamSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.read_header_event_with_cancellation(CancellationToken::never())
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.read_body_event_with_cancellation(maximum, CancellationToken::never())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        stop_reading(&self.shared, error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        Some(self.id())
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        Some(self.shared.ingress_completion.observe())
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.set_read_timeout_value(timeout);
    }
}

impl Drop for HttpStreamSource {
    fn drop(&mut self) {
        if !self.shared.state.borrow().source_complete {
            stop_reading(&self.shared, None);
        }
    }
}

/// A weak, owner-local producer capability for existing source state.
#[derive(Clone)]
pub struct HttpStreamSourceProducer {
    shared: Weak<StreamShared>,
}

impl HttpStreamSourceProducer {
    pub fn is_live(&self) -> bool {
        self.shared.strong_count() != 0
    }

    /// Whether the live source accepted EOM without an ingress error.
    ///
    /// Producer operations can accept input into sink mode after validation
    /// failed. Their success return alone does not establish this condition.
    pub fn ingress_eom_succeeded(&self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        let state = shared.state.borrow();
        is_eom_seen(&state) && state.error.is_none()
    }

    pub fn headers(&self, message: HttpMessage, eom: bool) -> bool {
        self.headers_impl(message, eom)
    }

    /// Takes ownership of an allocated header message without moving its contents.
    pub fn headers_boxed(&self, message: Box<HttpMessage>, eom: bool) -> bool {
        self.headers_impl(message, eom)
    }

    fn headers_impl<M>(&self, mut message: M, eom: bool) -> bool
    where
        M: std::borrow::BorrowMut<HttpMessage> + Into<Box<HttpMessage>>,
    {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        if !validate_headers(
            &shared,
            std::borrow::BorrowMut::borrow_mut(&mut message),
            eom,
        ) {
            return false;
        }
        let queued = {
            let mut state = shared.state.borrow_mut();
            if state.sink_mode {
                false
            } else {
                state.header_queue.push_back(QueuedHeader {
                    message: message.into(),
                    eom,
                });
                true
            }
        };
        if eom {
            record_ingress_eom(&shared);
        }
        if queued {
            shared.signal_event();
        }
        true
    }

    pub fn body(&self, mut body: BufQueue, padding: u16, eom: bool) -> FlowControlState {
        let Some(shared) = self.shared.upgrade() else {
            return FlowControlState::Error;
        };
        shared.assert_owner();
        if !validate_transition(&shared, IngressEvent::Body, eom) {
            return FlowControlState::Error;
        }

        let length = body.chain_length();
        validate_content_length(&shared, eom, length);
        let (reserved, sink_mode) = {
            let mut state = shared.state.borrow_mut();
            let strict = state.strict_flow_control;
            let reserved = state.receive_window.reserve(length, padding, strict);
            (reserved, state.sink_mode)
        };
        if !reserved {
            reject_flow_control(&shared);
            return FlowControlState::Error;
        }

        shared.ingress_completion.add_body_bytes(length);

        if sink_mode {
            if eom {
                record_ingress_eom(&shared);
            }
            update_flow_control(&shared, length);
        } else {
            let mut state = shared.state.borrow_mut();
            let coalesce = state
                .body_queue
                .back()
                .is_some_and(|event| matches!(event.kind, QueuedBodyKind::Body(_)));
            if coalesce {
                let previous = state
                    .body_queue
                    .pop_back()
                    .expect("checked the final body event");
                let QueuedBodyKind::Body(mut previous_body) = previous.kind else {
                    unreachable!("checked the final body event kind")
                };
                let combined_limit = previous_body
                    .chain_length()
                    .checked_add(body.chain_length())
                    .expect("flow-control admission bounds the combined body length");
                let mut combined = BufQueue::new(combined_limit);
                combined
                    .append(&mut previous_body)
                    .expect("the combined queue is sized for the previous body");
                combined
                    .append_with_growth_hint(&mut body, state.body_segment_hint)
                    .expect("the combined queue is sized for both bodies");
                state.body_queue.push_back(QueuedBody {
                    kind: QueuedBodyKind::Body(combined),
                    eom,
                });
            } else {
                state.body_queue.push_back(QueuedBody {
                    kind: QueuedBodyKind::Body(body),
                    eom,
                });
            }
            drop(state);
            if eom {
                record_ingress_eom(&shared);
            }
            shared.signal_event();
        }

        if shared.state.borrow().receive_window.size() > 0 {
            FlowControlState::Open
        } else {
            FlowControlState::Closed
        }
    }

    pub fn push_promise(
        &self,
        promise: HttpMessage,
        mut push_source: Box<dyn HttpSource>,
        eom: bool,
    ) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            push_source.stop_reading(None);
            return false;
        };
        shared.assert_owner();
        if !validate_transition(&shared, IngressEvent::Body, eom) {
            push_source.stop_reading(None);
            return false;
        }
        if shared.state.borrow().sink_mode {
            push_source.stop_reading(None);
            return false;
        }
        shared.state.borrow_mut().body_queue.push_back(QueuedBody {
            kind: QueuedBodyKind::PushPromise(HttpPushEvent::new(promise, Some(push_source))),
            eom,
        });
        if eom {
            record_ingress_eom(&shared);
        }
        shared.signal_event();
        true
    }

    pub fn trailers(&self, trailers: HttpHeaders) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        if !validate_transition(&shared, IngressEvent::Trailers, false) {
            return false;
        }
        validate_content_length(&shared, true, 0);
        let queued = {
            let mut state = shared.state.borrow_mut();
            if state.sink_mode {
                false
            } else {
                state.body_queue.push_back(QueuedBody {
                    kind: QueuedBodyKind::Trailers(trailers),
                    eom: true,
                });
                true
            }
        };
        record_ingress_eom(&shared);
        if queued {
            shared.signal_event();
        }
        true
    }

    pub fn padding(&self, bytes: u16) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        if !validate_transition(&shared, IngressEvent::Body, false) {
            return false;
        }
        let queued = {
            let mut state = shared.state.borrow_mut();
            if state.sink_mode {
                false
            } else {
                state.body_queue.push_back(QueuedBody {
                    kind: QueuedBodyKind::Padding(bytes),
                    eom: false,
                });
                true
            }
        };
        if queued {
            shared.signal_event();
        }
        true
    }

    pub fn eom(&self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        if !validate_transition(&shared, IngressEvent::Eom, false) {
            return false;
        }
        validate_content_length(&shared, true, 0);
        let mut signal = false;
        {
            let mut state = shared.state.borrow_mut();
            if state.sink_mode {
                drop(state);
                record_ingress_eom(&shared);
                return true;
            }
            if let Some(body) = state.body_queue.back_mut() {
                body.eom = true;
            } else if let Some(header) = state.header_queue.back_mut() {
                header.eom = true;
            } else {
                state.body_queue.push_back(QueuedBody {
                    kind: QueuedBodyKind::Body(BufQueue::new(0)),
                    eom: true,
                });
                signal = true;
            }
        }
        record_ingress_eom(&shared);
        if signal {
            shared.signal_event();
        }
        true
    }

    pub fn abort(&self, error: HttpErrorCode, details: impl AsRef<[u8]>) -> bool {
        self.abort_with_error(HttpError::new(error), details)
    }

    /// Aborts unfinished ingress with an owned error, preserving its typed cause.
    ///
    /// The first error wins. Diagnostics use the existing code/details format,
    /// and a queued final header is attached exactly as for `abort`.
    pub fn abort_with_error(&self, mut error: HttpError, details: impl AsRef<[u8]>) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        if is_eom_seen(&shared.state.borrow()) {
            return false;
        }
        shared.state.borrow_mut().ingress_state = IngressState::ReceivingDone;
        let details = details.as_ref();
        let mut message =
            format!("HTTP source aborted err={}", get_error_string(error.code())).into_bytes();
        if !details.is_empty() {
            message.extend_from_slice(b", details=");
            message.extend_from_slice(details);
        }
        error.set_message(message);
        set_owned_error(&shared, error, true)
    }

    pub fn body_bytes_buffered(&self) -> Option<u32> {
        self.shared
            .upgrade()
            .map(|shared| shared.state.borrow().receive_window.window().outstanding())
    }

    pub fn stop_reading(&self, error: Option<HttpErrorCode>) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        stop_reading(&shared, error)
    }

    pub fn set_read_timeout(&self, timeout: Duration) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        shared.set_read_timeout(timeout);
        true
    }

    /// Disables the current timeout until explicitly armed, using this update policy.
    ///
    /// Producer `set_read_timeout` remains immediate and bypasses this policy.
    /// Returns false if the source state has retired.
    pub fn defer_read_timeout(&self, timeout: DeferredReadTimeout) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        shared.deferred_read_timeout.set(Some(timeout));
        shared.set_read_timeout(Duration::ZERO);
        true
    }

    /// Applies the pending timeout once; returns false if none remains or the state retired.
    pub fn arm_read_timeout(&self) -> bool {
        let Some(shared) = self.shared.upgrade() else {
            return false;
        };
        shared.assert_owner();
        let Some(timeout) = shared.deferred_read_timeout.take() else {
            return false;
        };
        shared.set_read_timeout(timeout.duration());
        true
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReadKind {
    Header,
    Body,
}

enum StreamReadOutput {
    Header(HttpHeaderEvent),
    Body(HttpBodyEvent),
}

enum StreamReadState {
    Unstarted {
        shared: Rc<StreamShared>,
        kind: ReadKind,
        maximum: u32,
        cancellation: CancellationToken,
    },
    Reading(StreamReadFuture),
    Done,
}

/// One lazy, owner-local header read that keeps its admitted state inline.
///
/// Construction and unpolled Drop do not reserve read admission. If polling
/// panics, admission is released even when the caller retains this future.
pub struct HttpStreamHeaderRead {
    phase: StreamReadState,
}

impl Future for HttpStreamHeaderRead {
    type Output = Result<HttpHeaderEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().phase)
            .poll(context)
            .map(|result| {
                result.map(|output| match output {
                    StreamReadOutput::Header(event) => event,
                    StreamReadOutput::Body(_) => {
                        unreachable!("a header read returns a header event")
                    }
                })
            })
    }
}

/// One lazy, owner-local body read that keeps its admitted state inline.
///
/// Construction and unpolled Drop do not reserve read admission. If polling
/// panics, admission is released even when the caller retains this future.
pub struct HttpStreamBodyRead {
    phase: StreamReadState,
}

impl Future for HttpStreamBodyRead {
    type Output = Result<HttpBodyEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().phase)
            .poll(context)
            .map(|result| {
                result.map(|output| match output {
                    StreamReadOutput::Body(event) => event,
                    StreamReadOutput::Header(_) => unreachable!("a body read returns a body event"),
                })
            })
    }
}

impl StreamReadState {
    fn poll_inner(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<StreamReadOutput, HttpError>> {
        if matches!(self, Self::Unstarted { .. }) {
            let Self::Unstarted {
                shared,
                kind,
                maximum,
                cancellation,
            } = std::mem::replace(self, Self::Done)
            else {
                unreachable!("an unstarted stream read retains its input");
            };
            *self = match begin_read(shared, kind, maximum, cancellation) {
                Ok(read) => Self::Reading(read),
                Err(error) => return Poll::Ready(Err(error)),
            };
        }
        let Self::Reading(read) = self else {
            panic!("a completed stream read cannot be polled again");
        };
        let Poll::Ready(result) = Pin::new(read).poll(context) else {
            return Poll::Pending;
        };
        let phase = std::mem::replace(self, Self::Done);
        drop(phase);
        Poll::Ready(result)
    }
}

struct StreamReadPollGuard<'a> {
    phase: &'a mut StreamReadState,
    armed: bool,
}

impl Drop for StreamReadPollGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let unwinding = std::thread::panicking();
            let phase = std::mem::replace(self.phase, StreamReadState::Done);
            let failure = catch_unwind(AssertUnwindSafe(|| drop(phase))).err();
            finish_retirement_panic(failure, unwinding);
        }
    }
}

impl Future for StreamReadState {
    type Output = Result<StreamReadOutput, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = StreamReadPollGuard {
            phase: self.get_mut(),
            armed: true,
        };
        let result = guard.phase.poll_inner(context);
        guard.armed = false;
        result
    }
}

struct StreamReadFuture {
    shared: Rc<StreamShared>,
    read_id: u64,
    kind: ReadKind,
    maximum: u32,
    cancellation: CancellationToken,
    wait: Option<TimedWait>,
    completed: bool,
}

impl StreamReadFuture {
    fn operation(&mut self) -> ReadOperation<'_, GeneralRead<'_>> {
        ReadOperation {
            shared: &self.shared,
            role: GeneralRead {
                id: self.read_id,
                cancellation: &self.cancellation,
            },
            kind: self.kind,
            maximum: self.maximum,
            wait: &mut self.wait,
            completed: &mut self.completed,
        }
    }

    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        self.operation().retire_inner()
    }
}

impl Future for StreamReadFuture {
    type Output = Result<StreamReadOutput, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().operation().poll(context)
    }
}

trait ReadRole {
    type Wait: OperationWait;

    fn finish(&self, shared: &Rc<StreamShared>, eom: bool);

    fn wait_token(&self) -> CancellationToken;

    /// Whether an empty body read may return `Suspend` instead of waiting.
    fn suspends(&self) -> bool;
}

struct GeneralRead<'a> {
    id: u64,
    cancellation: &'a CancellationToken,
}

impl ReadRole for GeneralRead<'_> {
    type Wait = Option<TimedWait>;

    fn finish(&self, shared: &Rc<StreamShared>, eom: bool) {
        finish_read(shared, self.id, eom);
    }

    fn wait_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn suspends(&self) -> bool {
        true
    }
}

struct ExclusiveRead {
    suspend_free: bool,
}

impl ReadRole for ExclusiveRead {
    type Wait = ExclusiveWait;

    fn finish(&self, shared: &Rc<StreamShared>, eom: bool) {
        finish_exclusive_read(shared, eom);
    }

    fn wait_token(&self) -> CancellationToken {
        CancellationToken::never()
    }

    fn suspends(&self) -> bool {
        !self.suspend_free
    }
}

/// The wait of one read operation on its source event.
trait OperationWait {
    fn is_waiting(&self) -> bool;

    /// Starts waiting on the reset event.
    fn start(&mut self, shared: &StreamShared, cancellation: CancellationToken);

    /// Polls the wait; it is no longer waiting once ready.
    fn poll(&mut self, shared: &StreamShared, context: &mut Context<'_>) -> Poll<WaitStatus>;

    /// Ends an unfinished wait without reading the event status.
    fn release(&mut self, shared: &StreamShared);
}

impl OperationWait for Option<TimedWait> {
    #[inline]
    fn is_waiting(&self) -> bool {
        self.is_some()
    }

    #[inline]
    fn start(&mut self, shared: &StreamShared, cancellation: CancellationToken) {
        self.get_or_insert_with(|| shared.wait_event(cancellation));
    }

    #[inline]
    fn poll(&mut self, _shared: &StreamShared, context: &mut Context<'_>) -> Poll<WaitStatus> {
        let wait = self
            .as_mut()
            .expect("a waiting read retains its timed wait");
        let Poll::Ready(status) = Pin::new(wait).poll(context) else {
            return Poll::Pending;
        };
        self.take();
        Poll::Ready(status)
    }

    #[inline]
    fn release(&mut self, _shared: &StreamShared) {
        if self.is_some() {
            *self = None;
        }
    }
}

/// An exclusive read's wait. It takes no cancellation, so while the source has
/// no read timeout it is a `DirectWait` instead of a `TimedWait`.
enum ExclusiveWait {
    Idle,
    Direct,
    Timed(TimedWait),
}

impl OperationWait for ExclusiveWait {
    #[inline]
    fn is_waiting(&self) -> bool {
        !matches!(self, Self::Idle)
    }

    fn start(&mut self, shared: &StreamShared, cancellation: CancellationToken) {
        debug_assert!(!cancellation.can_be_cancelled());
        *self = if shared.has_read_timeout() {
            Self::Timed(shared.wait_event(cancellation))
        } else {
            Self::Direct
        };
    }

    #[inline]
    fn poll(&mut self, shared: &StreamShared, context: &mut Context<'_>) -> Poll<WaitStatus> {
        if matches!(self, Self::Direct) && shared.retimed_wait.borrow().is_some() {
            self.adopt_retimed_wait(shared);
        }
        let status = match self {
            Self::Idle => unreachable!("only a waiting read polls its wait"),
            Self::Direct => {
                let mut direct = shared.direct_wait.borrow_mut();
                match direct.as_mut() {
                    Some(wait) if wait.signalled => *direct = None,
                    Some(wait) => {
                        if !wait.waker.will_wake(context.waker()) {
                            wait.waker = context.waker().clone();
                        }
                        return Poll::Pending;
                    }
                    None => {
                        *direct = Some(DirectWait {
                            waker: context.waker().clone(),
                            signalled: false,
                        });
                        return Poll::Pending;
                    }
                }
                WaitStatus::Signalled
            }
            Self::Timed(wait) => {
                let Poll::Ready(status) = Pin::new(wait).poll(context) else {
                    return Poll::Pending;
                };
                status
            }
        };
        *self = Self::Idle;
        Poll::Ready(status)
    }

    #[inline]
    fn release(&mut self, shared: &StreamShared) {
        if !matches!(self, Self::Idle) {
            self.release_waiting(shared);
        }
    }
}

impl ExclusiveWait {
    fn release_waiting(&mut self, shared: &StreamShared) {
        if matches!(std::mem::replace(self, Self::Idle), Self::Direct) {
            let direct = shared.direct_wait.take();
            drop(shared.retimed_wait.take());
            drop(direct);
        }
    }

    #[cold]
    #[inline(never)]
    fn adopt_retimed_wait(&mut self, shared: &StreamShared) {
        if let Some(wait) = shared.retimed_wait.take() {
            *self = Self::Timed(wait);
        }
    }
}

struct ReadOperation<'a, R: ReadRole> {
    shared: &'a Rc<StreamShared>,
    role: R,
    kind: ReadKind,
    maximum: u32,
    wait: &'a mut R::Wait,
    completed: &'a mut bool,
}

impl<R: ReadRole> ReadOperation<'_, R> {
    fn finish(&self, eom: bool) {
        self.role.finish(self.shared, eom);
    }

    fn try_ready(&mut self) -> Option<Result<StreamReadOutput, HttpError>> {
        // An existing wait's cancellation or timeout must be consumed first.
        if let Some(result) = take_ready_event(self.shared, self.kind, self.maximum) {
            self.wait.release(self.shared);
            *self.completed = true;
            let eom = result.as_ref().ok().is_some_and(|event| match event {
                StreamReadOutput::Header(event) => event.eom(),
                StreamReadOutput::Body(event) => event.eom(),
            });
            self.finish(eom);
            return Some(result);
        }

        if matches!(self.kind, ReadKind::Body) && self.role.suspends() {
            let suspend = {
                let mut state = self.shared.state.borrow_mut();
                if state.can_suspend {
                    state.can_suspend = false;
                    true
                } else {
                    false
                }
            };
            if suspend {
                self.shared.reset_event();
                self.shared.state.borrow_mut().suspend_wait_active = true;
                *self.completed = true;
                self.finish(false);
                let resume = SuspendResumeFuture {
                    shared: Rc::clone(self.shared),
                    wait: Some(self.shared.wait_event(self.role.wait_token())),
                    completed: false,
                };
                return Some(Ok(StreamReadOutput::Body(HttpBodyEvent::suspend(
                    Box::pin(resume),
                ))));
            }
        }

        None
    }

    #[inline]
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<StreamReadOutput, HttpError>> {
        self.shared.assert_owner();
        loop {
            if self.wait.is_waiting() {
                let Poll::Ready(status) = self.wait.poll(self.shared, context) else {
                    return Poll::Pending;
                };
                apply_wait_status(self.shared, status);
                if matches!(self.kind, ReadKind::Body) {
                    self.shared.state.borrow_mut().can_suspend = true;
                }
                continue;
            }

            if let Some(result) = self.try_ready() {
                return Poll::Ready(result);
            }

            if !self.wait.is_waiting() {
                self.shared.reset_event();
                self.wait.start(self.shared, self.role.wait_token());
            }
        }
    }

    fn retire_inner(&mut self) -> Option<RetirementPanic> {
        if *self.completed {
            return None;
        }
        *self.completed = true;
        let mut first = catch_unwind(AssertUnwindSafe(|| self.wait.release(self.shared))).err();
        retain_retirement_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| {
                apply_wait_status(self.shared, self.shared.event_status());
            }))
            .err(),
        );
        retain_retirement_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| self.finish(false))).err(),
        );
        first
    }
}

impl Drop for StreamReadFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let unwinding = std::thread::panicking();
        let first = self.retire_inner();
        finish_retirement_panic(first, unwinding);
    }
}

enum IngressReceiverSource {
    Owned(HttpStreamSourceOwner),
    Retiring(Rc<StreamShared>),
    Empty,
}

impl IngressReceiverSource {
    fn shared(&self) -> Option<&Rc<StreamShared>> {
        match self {
            Self::Owned(owner) => owner.source().map(|source| &source.shared),
            Self::Retiring(shared) => Some(shared),
            Self::Empty => None,
        }
    }
}

enum IngressReadPhase {
    Idle,
    Reading {
        maximum: u32,
        wait: Option<TimedWait>,
    },
    Suspended(TimedWait),
    ResumeReady(ResumeStatus),
}

pub(crate) struct IngressBodyReceiver {
    source: IngressReceiverSource,
    phase: IngressReadPhase,
}

impl IngressBodyReceiver {
    pub(crate) fn poll_resume(&mut self, context: &mut Context<'_>) -> Poll<Option<ResumeStatus>> {
        if let IngressReadPhase::ResumeReady(_) = self.phase {
            let IngressReadPhase::ResumeReady(status) =
                std::mem::replace(&mut self.phase, IngressReadPhase::Idle)
            else {
                unreachable!("the completed resume status is retained");
            };
            return Poll::Ready(Some(status));
        }
        let IngressReadPhase::Suspended(wait) = &mut self.phase else {
            return Poll::Ready(None);
        };
        let Poll::Ready(status) = Pin::new(wait).poll(context) else {
            return Poll::Pending;
        };
        self.drop_pending_read();
        Poll::Ready(Some(match status {
            WaitStatus::NotReady => ResumeStatus::NotReady,
            WaitStatus::Signalled => ResumeStatus::Signalled,
            WaitStatus::TimedOut => ResumeStatus::TimedOut,
            WaitStatus::Cancelled => ResumeStatus::Cancelled,
        }))
    }

    pub(crate) fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let Some(shared) = self.source.shared() else {
            return Poll::Ready(Err(source_not_readable()));
        };
        shared.assert_owner();
        if matches!(self.phase, IngressReadPhase::Idle) {
            if let Err(error) = validate_read(shared, ReadKind::Body, maximum) {
                self.retire_rejected_read();
                return Poll::Ready(Err(error));
            }
            shared.state.borrow_mut().active_read = Some(ReadActivity::Exclusive);
            self.phase = IngressReadPhase::Reading {
                maximum,
                wait: None,
            };
        }
        loop {
            let IngressReadPhase::Reading { maximum, wait } = &mut self.phase else {
                return Poll::Pending;
            };
            if let Some(pending) = wait {
                let Poll::Ready(status) = Pin::new(pending).poll(context) else {
                    return Poll::Pending;
                };
                drop(wait.take());
                apply_wait_status(shared, status);
                shared.state.borrow_mut().can_suspend = true;
                continue;
            }
            if let Some(result) = take_ready_event(shared, ReadKind::Body, *maximum) {
                self.phase = IngressReadPhase::Idle;
                let result = result.map(|event| match event {
                    StreamReadOutput::Body(event) => event,
                    StreamReadOutput::Header(_) => {
                        unreachable!("a body receiver reads body events")
                    }
                });
                finish_exclusive_read(shared, result.as_ref().is_ok_and(HttpBodyEvent::eom));
                if result.as_ref().map_or(true, HttpBodyEvent::eom)
                    && let IngressReceiverSource::Owned(owner) = &mut self.source
                {
                    owner.retire_terminal();
                }
                return Poll::Ready(result);
            }
            shared.reset_event();
            let suspend = shared.state.borrow().can_suspend;
            if suspend {
                {
                    let mut state = shared.state.borrow_mut();
                    state.can_suspend = false;
                    state.suspend_wait_active = true;
                }
                self.phase =
                    IngressReadPhase::Suspended(shared.wait_event(CancellationToken::never()));
                finish_exclusive_read(shared, false);
                // The first wait poll must register before returning Pending. A
                // synchronous completion still crosses the owner's resume checkpoint.
                if let Poll::Ready(Some(status)) = self.poll_resume(context) {
                    self.phase = IngressReadPhase::ResumeReady(status);
                    context.waker().wake_by_ref();
                }
                return Poll::Pending;
            }
            *wait = Some(shared.wait_event(CancellationToken::never()));
        }
    }

    #[cold]
    #[inline(never)]
    fn retire_rejected_read(&mut self) {
        if let IngressReceiverSource::Owned(owner) = &mut self.source {
            owner.retire_terminal();
        }
    }

    pub(crate) fn prepare_stop(&mut self) -> Option<HttpStreamSourceOwner> {
        let source = std::mem::replace(&mut self.source, IngressReceiverSource::Empty);
        let IngressReceiverSource::Owned(owner) = source else {
            self.source = source;
            return None;
        };
        if matches!(
            self.phase,
            IngressReadPhase::Reading { .. } | IngressReadPhase::Suspended(_)
        ) && let Some(source) = owner.source()
        {
            // Source retirement precedes operation retirement. Only this cold
            // handoff needs a separate owner for an outstanding read or resume.
            self.source = IngressReceiverSource::Retiring(Rc::clone(&source.shared));
        }
        Some(owner)
    }

    pub(crate) fn drop_pending_read(&mut self) {
        if matches!(self.phase, IngressReadPhase::Idle)
            && !matches!(self.source, IngressReceiverSource::Retiring(_))
        {
            return;
        }
        let unwinding = std::thread::panicking();
        let phase = std::mem::replace(&mut self.phase, IngressReadPhase::Idle);
        let mut first = None;
        match phase {
            IngressReadPhase::Reading { wait, .. } => {
                first = catch_unwind(AssertUnwindSafe(|| drop(wait))).err();
                if let Some(shared) = self.source.shared() {
                    retain_retirement_panic(
                        &mut first,
                        catch_unwind(AssertUnwindSafe(|| {
                            apply_wait_status(shared, shared.event_status());
                        }))
                        .err(),
                    );
                    retain_retirement_panic(
                        &mut first,
                        catch_unwind(AssertUnwindSafe(|| {
                            finish_exclusive_read(shared, false);
                        }))
                        .err(),
                    );
                }
            }
            IngressReadPhase::Suspended(wait) => {
                first = catch_unwind(AssertUnwindSafe(|| drop(wait))).err();
                if let Some(shared) = self.source.shared() {
                    shared.state.borrow_mut().suspend_wait_active = false;
                }
            }
            IngressReadPhase::Idle | IngressReadPhase::ResumeReady(_) => {}
        }
        if matches!(self.source, IngressReceiverSource::Retiring(_)) {
            let source = std::mem::replace(&mut self.source, IngressReceiverSource::Empty);
            retain_retirement_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(source))).err(),
            );
        }
        finish_retirement_panic(first, unwinding);
    }
}

impl Drop for IngressBodyReceiver {
    fn drop(&mut self) {
        self.drop_pending_read();
    }
}

fn finish_exclusive_read(shared: &Rc<StreamShared>, eom_return: bool) {
    shared.state.borrow_mut().active_read = None;
    maybe_complete(shared, eom_return);
}

struct SuspendResumeFuture {
    shared: Rc<StreamShared>,
    wait: Option<TimedWait>,
    completed: bool,
}

impl Future for SuspendResumeFuture {
    type Output = ResumeStatus;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.completed,
            "a completed resume wait cannot be polled again"
        );
        let wait = self
            .wait
            .as_mut()
            .expect("an incomplete resume wait retains its timed wait");
        let Poll::Ready(status) = Pin::new(wait).poll(context) else {
            return Poll::Pending;
        };
        self.wait.take();
        self.shared.state.borrow_mut().suspend_wait_active = false;
        self.completed = true;
        let status = match status {
            WaitStatus::NotReady => ResumeStatus::NotReady,
            WaitStatus::Signalled => ResumeStatus::Signalled,
            WaitStatus::TimedOut => ResumeStatus::TimedOut,
            WaitStatus::Cancelled => ResumeStatus::Cancelled,
        };
        Poll::Ready(status)
    }
}

impl Drop for SuspendResumeFuture {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.wait.take();
        self.shared.state.borrow_mut().suspend_wait_active = false;
    }
}

fn apply_wait_status(shared: &Rc<StreamShared>, status: WaitStatus) {
    match status {
        WaitStatus::TimedOut => {
            let timeout = shared.read_timeout();
            let message = format!("Read timeout after {}", timeout.as_millis());
            set_error(
                shared,
                HttpErrorCode::READ_TIMEOUT,
                message.as_bytes(),
                false,
            );
        }
        WaitStatus::Cancelled => {
            set_error(
                shared,
                HttpErrorCode::CORO_CANCELLED,
                b"Read cancelled",
                false,
            );
        }
        WaitStatus::NotReady | WaitStatus::Signalled => {}
    }
}

fn validate_read(shared: &Rc<StreamShared>, kind: ReadKind, maximum: u32) -> Result<(), HttpError> {
    let stopped = {
        let state = shared.state.borrow();
        state.draining && state.error.is_none()
    };
    if stopped {
        return Err(stopped_reading());
    }
    let invalid_kind = {
        let state = shared.state.borrow();
        match kind {
            ReadKind::Header => state.read_state != ReadState::HeaderEvents,
            ReadKind::Body => state.read_state != ReadState::BodyEvents || maximum == 0,
        }
    };
    if invalid_kind {
        reject_read_kind(shared, kind, maximum);
    }

    Ok(())
}

#[cold]
#[inline(never)]
fn reject_read_kind(shared: &Rc<StreamShared>, kind: ReadKind, maximum: u32) {
    let message: &[u8] = match kind {
        ReadKind::Header => b"readHeaderEvent called after final headers read",
        ReadKind::Body if maximum == 0 => b"readBodyEvent called with a zero maximum",
        ReadKind::Body => b"readBodyEvent called before final headers read",
    };
    set_error(
        shared,
        HttpErrorCode::INVALID_STATE_TRANSITION,
        message,
        false,
    );
}

#[cold]
#[inline(never)]
fn stopped_reading() -> HttpError {
    HttpError::with_message(HttpErrorCode::NO_ERROR, b"Stopped reading")
}

#[cold]
#[inline(never)]
pub(crate) fn source_not_readable() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"HTTP source is no longer readable")
}

#[cold]
#[inline(never)]
fn response_read_not_armed() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"response read is not armed")
}

#[cold]
#[inline(never)]
fn read_already_active() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"HTTP stream source already has an active read or resume wait",
    )
}

fn begin_read(
    shared: Rc<StreamShared>,
    kind: ReadKind,
    maximum: u32,
    cancellation: CancellationToken,
) -> Result<StreamReadFuture, HttpError> {
    validate_read(&shared, kind, maximum)?;

    let read_id = {
        let mut state = shared.state.borrow_mut();
        if state.active_read.is_some() || state.suspend_wait_active {
            return Err(read_already_active());
        }
        let id = state.next_read_id;
        state.next_read_id = state.next_read_id.wrapping_add(1);
        state.active_read = Some(ReadActivity::General(id));
        id
    };
    Ok(StreamReadFuture {
        shared,
        read_id,
        kind,
        maximum,
        cancellation,
        wait: None,
        completed: false,
    })
}

fn take_ready_event(
    shared: &Rc<StreamShared>,
    kind: ReadKind,
    maximum: u32,
) -> Option<Result<StreamReadOutput, HttpError>> {
    let mut flow_length = 0;
    let result = {
        let mut state = shared.state.borrow_mut();
        if state.error.is_some() || state.draining {
            return Some(Err(unreadable(&state)));
        }
        match kind {
            ReadKind::Header => {
                let header = state.header_queue.pop_front()?;
                let mut event =
                    match HttpHeaderEvent::from_boxed_headers(header.message, header.eom) {
                        Ok(event) => event,
                        Err(error) => return Some(Err(header_event_failed(error))),
                    };
                if event.is_final().unwrap_or(false) {
                    state.read_state = ReadState::BodyEvents;
                }
                Ok(StreamReadOutput::Header(event))
            }
            ReadKind::Body => {
                let mut queued = state.body_queue.pop_front()?;
                if let QueuedBodyKind::Body(body) = &mut queued.kind {
                    let length = body.chain_length();
                    if length > maximum as usize {
                        let prefix = body.split_at_most(maximum as usize);
                        let remainder = std::mem::replace(body, prefix);
                        let original_eom = queued.eom;
                        queued.eom = false;
                        state.body_queue.push_front(QueuedBody {
                            kind: QueuedBodyKind::Body(remainder),
                            eom: original_eom,
                        });
                        flow_length = maximum as usize;
                    } else if length != 0 {
                        flow_length = length;
                    }
                    state.body_segment_hint =
                        state.body_segment_hint.max(body.segment_growth_hint());
                }
                Ok(StreamReadOutput::Body(queued.into_event()))
            }
        }
    };
    if flow_length != 0 {
        update_flow_control(shared, flow_length);
    }
    Some(result)
}

#[cold]
#[inline(never)]
fn unreadable(state: &StreamState) -> HttpError {
    match &state.error {
        Some(error) => error.error.clone(),
        None => stopped_reading(),
    }
}

#[cold]
#[inline(never)]
fn header_event_failed(error: HeaderEventError) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
}

fn finish_read(shared: &Rc<StreamShared>, read_id: u64, eom_return: bool) {
    {
        let mut state = shared.state.borrow_mut();
        if state.active_read == Some(ReadActivity::General(read_id)) {
            state.active_read = None;
        }
    }
    maybe_complete(shared, eom_return);
}

fn validate_headers(shared: &Rc<StreamShared>, message: &mut HttpMessage, eom: bool) -> bool {
    let final_headers = match message.is_final() {
        Ok(final_headers) => final_headers,
        Err(error) => {
            set_error(
                shared,
                HttpErrorCode::INVALID_STATE_TRANSITION,
                error.to_string().as_bytes(),
                false,
            );
            return false;
        }
    };
    let event = if final_headers {
        IngressEvent::FinalHeaders
    } else {
        IngressEvent::NonFinalHeaders
    };
    if !validate_transition(shared, event, eom) {
        return false;
    }

    if message.is_response() && message.status_code().ok() == Some(304) {
        shared.state.borrow_mut().should_validate_content_length = false;
    }
    if final_headers && shared.state.borrow().should_validate_content_length {
        let content_length = message
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength);
        if let Some(content_length) = parse_folly_u64(content_length) {
            {
                let mut state = shared.state.borrow_mut();
                state.expected_content_length = content_length;
                state.remaining_content_length = content_length;
            }
            validate_content_length(shared, eom, 0);
        } else {
            shared.state.borrow_mut().should_validate_content_length = false;
        }
    }
    true
}

fn parse_folly_u64(value: &[u8]) -> Option<u64> {
    let mut start = 0;
    while start < value.len() && is_folly_c_whitespace(value[start]) {
        start += 1;
    }
    let mut end = value.len();
    while end > start && is_folly_c_whitespace(value[end - 1]) {
        end -= 1;
    }
    let value = &value[start..end];
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

const fn is_folly_c_whitespace(byte: u8) -> bool {
    byte == b' ' || (byte >= b'\t' && byte <= b'\r')
}

fn validate_transition(shared: &Rc<StreamShared>, event: IngressEvent, eom: bool) -> bool {
    let valid = {
        let mut state = shared.state.borrow_mut();
        let first = HttpTransactionIngressStateMachine::transit(&mut state.ingress_state, event);
        first
            && (!eom
                || HttpTransactionIngressStateMachine::transit(
                    &mut state.ingress_state,
                    IngressEvent::Eom,
                ))
    };
    if !valid {
        reject_transition(shared);
    }
    valid
}

#[cold]
#[inline(never)]
fn reject_flow_control(shared: &Rc<StreamShared>) {
    set_error(
        shared,
        HttpErrorCode::FLOW_CONTROL_ERROR,
        b"Sender exceeded flow control",
        false,
    );
}

#[cold]
#[inline(never)]
fn reject_transition(shared: &Rc<StreamShared>) {
    set_error(
        shared,
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"Invalid ingress state transition",
        false,
    );
}

fn validate_content_length(shared: &Rc<StreamShared>, eom: bool, added_body_length: usize) {
    let mismatch = {
        let mut state = shared.state.borrow_mut();
        if !state.should_validate_content_length {
            return;
        }
        let added = u64::try_from(added_body_length).unwrap_or(u64::MAX);
        let observed = if added > state.remaining_content_length {
            Some(
                state
                    .expected_content_length
                    .saturating_add(added - state.remaining_content_length),
            )
        } else {
            state.remaining_content_length -= added;
            if eom && state.remaining_content_length > 0 {
                Some(state.expected_content_length - state.remaining_content_length)
            } else {
                None
            }
        };
        if observed.is_some() {
            state.remaining_content_length = 0;
            state.should_validate_content_length = false;
        }
        observed.map(|observed| (state.expected_content_length, observed))
    };
    if let Some((expected, observed)) = mismatch {
        reject_content_length(shared, expected, observed);
    }
}

#[cold]
#[inline(never)]
fn reject_content_length(shared: &Rc<StreamShared>, expected: u64, observed: u64) {
    let message = format!(
        "Content-Length/body mismatch on ingress: expected= {expected}, actual= {observed}"
    );
    set_error(
        shared,
        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        message.as_bytes(),
        false,
    );
}

fn set_error(
    shared: &Rc<StreamShared>,
    code: HttpErrorCode,
    message: &[u8],
    ingress: bool,
) -> bool {
    set_owned_error(shared, HttpError::with_message(code, message), ingress)
}

fn set_owned_error(shared: &Rc<StreamShared>, mut error: HttpError, ingress: bool) -> bool {
    let _completion = CompleteDrainedSource(shared);
    let installed = {
        let mut state = shared.state.borrow_mut();
        if state.error.is_some() {
            return false;
        }
        let attach_final = state
            .header_queue
            .back_mut()
            .is_some_and(|header| header.message.is_final().unwrap_or(false));
        if attach_final {
            if let Some(header) = state.header_queue.pop_back() {
                error.set_http_message(Some(*header.message));
            }
        }
        state.error = Some(StoredError { error, ingress });
        true
    };
    if installed {
        enable_sink_mode(shared);
        let error = shared
            .state
            .borrow()
            .error
            .as_ref()
            .expect("the first error was installed")
            .error
            .clone();
        // A completion waker's panic must not leave a parked reader asleep.
        let unwinding = std::thread::panicking();
        let mut failure = catch_unwind(AssertUnwindSafe(|| {
            shared.ingress_completion.finish(Err(error));
        }))
        .err();
        retain_retirement_panic(
            &mut failure,
            catch_unwind(AssertUnwindSafe(|| shared.signal_event())).err(),
        );
        finish_retirement_panic(failure, unwinding);
    }
    installed
}

fn record_ingress_eom(shared: &Rc<StreamShared>) {
    let _completion = CompleteDrainedSource(shared);
    let outcome = shared
        .state
        .borrow()
        .error
        .as_ref()
        .map_or_else(|| Ok(shared.host.now()), |error| Err(error.error.clone()));
    shared.ingress_completion.finish(outcome);
}

// Producer observers may panic after publishing EOM. An abandoned consumer must
// still notify the session of completion on that path.
struct CompleteDrainedSource<'a>(&'a Rc<StreamShared>);

impl Drop for CompleteDrainedSource<'_> {
    fn drop(&mut self) {
        if !self.0.state.borrow().draining {
            return;
        }
        complete_drained_source(self.0);
    }
}

#[cold]
#[inline(never)]
fn complete_drained_source(shared: &Rc<StreamShared>) {
    let unwinding = std::thread::panicking();
    let mut failure = catch_unwind(AssertUnwindSafe(|| shared.signal_event())).err();
    retain_retirement_panic(
        &mut failure,
        catch_unwind(AssertUnwindSafe(|| maybe_complete(shared, false))).err(),
    );
    finish_retirement_panic(failure, unwinding);
}

fn enable_sink_mode(shared: &Rc<StreamShared>) {
    {
        let mut state = shared.state.borrow_mut();
        state.header_queue.clear();
    }
    loop {
        let Some(event) = shared.state.borrow_mut().body_queue.pop_front() else {
            break;
        };
        if let QueuedBodyKind::Body(body) = &event.kind
            && !body.is_empty()
        {
            update_flow_control(shared, body.chain_length());
        }
        drop(event);
    }
    shared.state.borrow_mut().sink_mode = true;
}

fn update_flow_control(shared: &Rc<StreamShared>, length: usize) {
    let (id, to_acknowledge, opened) = {
        let mut state = shared.state.borrow_mut();
        let was_blocked = !state.sink_mode && state.receive_window.size() <= 0;
        let to_acknowledge = state
            .receive_window
            .processed(length)
            .expect("admitted body credit must remain representable");
        let input_finished = state.error.is_some() || is_eom_seen(&state);
        (
            state.id,
            if input_finished { 0 } else { to_acknowledge },
            was_blocked && state.receive_window.size() > 0,
        )
    };
    emit_callback(
        shared,
        CallbackEvent::BytesProcessed {
            id,
            amount: length,
            to_acknowledge,
        },
    );
    if opened {
        emit_callback(shared, CallbackEvent::WindowOpen(id));
    }
}

fn maybe_complete(shared: &Rc<StreamShared>, eom_return: bool) {
    let completion = {
        let mut state = shared.state.borrow_mut();
        let drained = state.draining && is_eom_seen(&state);
        if state.source_complete
            || (!eom_return && !drained && state.error.is_none())
            || state.active_read.is_some()
        {
            None
        } else {
            state.source_complete = true;
            let error = state.error.as_ref().and_then(|error| {
                if error.ingress {
                    None
                } else {
                    Some(error.error.clone())
                }
            });
            Some((state.id, error))
        }
    };
    if let Some((id, error)) = completion {
        emit_callback(shared, CallbackEvent::SourceComplete { id, error });
    }
}

fn stop_reading(shared: &Rc<StreamShared>, error: Option<HttpErrorCode>) -> bool {
    shared.assert_owner();
    let drain = {
        let mut state = shared.state.borrow_mut();
        if state.source_complete {
            return false;
        }
        let drain = error.is_none() && state.drain_on_drop && state.error.is_none();
        if drain {
            if state.draining {
                return false;
            }
            state.draining = true;
        }
        drain
    };
    let _completion = CompleteDrainedSource(shared);
    if drain {
        enable_sink_mode(shared);
        return true;
    }
    let code = error.unwrap_or(HttpErrorCode::NO_ERROR);
    set_error(shared, code, b"Abandoned reading", false);
    maybe_complete(shared, false);
    true
}

fn emit_callback(shared: &Rc<StreamShared>, event: CallbackEvent) {
    let mut callback = {
        let mut slot = shared.callback.borrow_mut();
        if slot.dispatching {
            return defer_callback_event(&mut slot, event);
        }
        let Some(callback) = slot.callback.take() else {
            return discard_callback_event(slot, event);
        };
        slot.dispatching = true;
        callback
    };
    let mut event = event;
    let result = loop {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match event {
            CallbackEvent::BytesProcessed {
                id,
                amount,
                to_acknowledge,
            } => callback.bytes_processed(id, amount, to_acknowledge),
            CallbackEvent::WindowOpen(id) => callback.window_open(id),
            CallbackEvent::SourceComplete { id, error } => callback.source_complete(id, error),
        }));
        let mut slot = shared.callback.borrow_mut();
        if result.is_ok()
            && let Some(next) = slot.pending.pop_front()
        {
            drop(slot);
            event = next;
            continue;
        }
        slot.dispatching = false;
        if let Some(replaced) = slot.callback.replace(callback) {
            drop_replaced_callback(replaced);
        }
        break result;
    };
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

#[cold]
#[inline(never)]
fn defer_callback_event(slot: &mut CallbackSlot, event: CallbackEvent) {
    slot.pending.push_back(event);
}

#[cold]
#[inline(never)]
fn discard_callback_event(slot: RefMut<'_, CallbackSlot>, event: CallbackEvent) {
    drop(slot);
    drop(event);
}

#[cold]
#[inline(never)]
fn drop_replaced_callback(callback: CallbackOwner) {
    drop(callback);
}

fn is_eom_seen(state: &StreamState) -> bool {
    matches!(
        state.ingress_state,
        IngressState::EomQueued | IngressState::ReceivingDone
    )
}
