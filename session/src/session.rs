/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

pub(crate) mod small_map;

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Poll;
use std::thread::ThreadId;

use roxygen_codec::CodecSetting;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::fixed_source::get_error_response;
use roxygen_core::handler::HandlerInvocation;
use roxygen_core::handler::HandlerResponseProposal;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HolderHeaderRead;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::FlowControlState;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::keep_alive::CoroSessionHandle;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::keep_alive::SessionHandleError;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::task_group::TaskGroupError;
use roxygen_runtime::task_group::TaskId;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::CancellableWait;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerId;
use smallvec::SmallVec;
use thiserror::Error;

use crate::exchange::ExchangeGuard;
use crate::exchange::ExchangeOwner;
use crate::exchange::ExchangeTracker;
use crate::session::small_map::SmallMap;
use crate::session::small_map::SmallSet;
use crate::shutdown::DrainTrigger;
use crate::stream_state::EgressPanic;
use crate::stream_state::EgressTransfer;
use crate::stream_state::PreparedStreamReset;
use crate::stream_state::ResponseState;
use crate::stream_state::StreamCallbackEvent;
use crate::stream_state::StreamCallbackQueue;
use crate::stream_state::StreamCallbacks;
use crate::stream_state::StreamState;
use crate::stream_state::discard_egress_panic;
use crate::stream_state::retain_egress_panic;
use crate::uniplex::UniplexLifecycleDispatcher;

const DEFAULT_INGRESS_RECEIVE_WINDOW: u32 = 65_535;
const DEFAULT_EGRESS_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A lifecycle notification emitted synchronously on the session owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleEvent {
    ActivateConnection,
    TransactionAttached,
    /// `None` releases an uncommitted reservation; `Some` identifies a stream
    /// removed after its source and producer ownership has retired.
    TransactionDetached {
        stream: Option<StreamId>,
    },
    DeactivateConnection,
    RequestBegin {
        stream: StreamId,
        sequence_number: u64,
    },
    RequestEnd {
        stream: StreamId,
    },
    /// The stream has released its outgoing protocol capacity. Its sources
    /// and exchange cleanup may still be alive.
    OutgoingCapacityReleased {
        stream: StreamId,
    },
    /// The session has stopped admitting outgoing requests. Accepted ingress
    /// can still finish independently of the session's drain transition.
    OutgoingAdmissionClosed,
    PeerReset {
        stream: StreamId,
        code: HttpErrorCode,
    },
    DrainStarted(DrainTrigger),
    Http2Read {
        bytes: usize,
        stream: Option<StreamId>,
    },
    Http2Write {
        bytes: usize,
    },
    Http2IngressMessage {
        stream: StreamId,
    },
    Http2IngressError {
        stream: StreamId,
        code: HttpErrorCode,
    },
    Http2IngressEof,
    Http2Settings {
        count: usize,
    },
    Http2Setting {
        setting: CodecSetting,
    },
    Http2StreamsFull,
    Http2StreamsNotFull,
    Http2SettingsAck,
    Http2PingRequest {
        data: u64,
    },
    Http2PingReply {
        data: u64,
    },
    Http2PingReplySent {
        latency: std::time::Duration,
    },
    Http2PingReplyReceived,
    /// An unanswered PING probe (`experimental::PingProbeConfig`) is dropping
    /// the connection. Emitted after `Http2SessionStats::record_ping_timeout`
    /// and before the drop fails any stream still open. On a session that
    /// already failed, it follows that failure and the session keeps the first
    /// error, so use `experimental::is_ping_probe_timeout` to classify errors.
    Http2PingProbeTimeout,
    Http2Goaway {
        last_good_stream: StreamId,
        code: HttpErrorCode,
    },
    Http2FlowWindowClosed,
    Http2FlowWindowOpen,
    Destroy,
}

/// Owner-local observer of session lifecycle transitions.
pub trait LifecycleObserver: 'static {
    fn on_attached(&self) {}

    fn on_attached_with_registration(&self, _registration: LifecycleObserverRegistration) {
        self.on_attached();
    }

    fn on_event(&self, _event: LifecycleEvent) {}
}

/// Opaque token used to remove one lifecycle observer.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LifecycleObserverId(u64);

impl LifecycleObserverId {
    pub(crate) const fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

type LifecycleObserverEntry = (LifecycleObserverId, Rc<dyn LifecycleObserver>);
pub(crate) type LifecycleSnapshot = SmallVec<[Rc<dyn LifecycleObserver>; 2]>;

/// Observers in ascending id order, delivered newest first.
#[derive(Default)]
pub(crate) struct LifecycleObservers(RefCell<SmallVec<[LifecycleObserverEntry; 2]>>);

impl LifecycleObservers {
    /// A duplicate id, reachable only after the id counter wraps, replaces the
    /// stored observer while the registry is borrowed.
    pub(crate) fn insert(&self, id: LifecycleObserverId, observer: Rc<dyn LifecycleObserver>) {
        let mut observers = self.0.borrow_mut();
        match observers.binary_search_by_key(&id, |entry| entry.0) {
            Ok(position) => observers[position].1 = observer,
            Err(position) => observers.insert(position, (id, observer)),
        }
    }

    /// The removed observer drops while the registry is still borrowed.
    pub(crate) fn remove(&self, id: LifecycleObserverId) -> bool {
        let mut observers = self.0.borrow_mut();
        match observers.binary_search_by_key(&id, |entry| entry.0) {
            Ok(position) => {
                observers.remove(position);
                true
            }
            Err(_) => false,
        }
    }

    pub(crate) fn snapshot(&self) -> LifecycleSnapshot {
        self.0
            .borrow()
            .iter()
            .rev()
            .map(|(_, observer)| Rc::clone(observer))
            .collect()
    }

    pub(crate) fn take(&self) -> SmallVec<[LifecycleObserverEntry; 2]> {
        let mut observers = self.0.borrow_mut();
        std::mem::take(&mut *observers)
    }
}

#[derive(Clone)]
pub(crate) enum LifecycleRegistry {
    Session(Weak<SharedSessionState>),
    Uniplex(Weak<UniplexLifecycleDispatcher>),
}

#[derive(Clone)]
pub struct LifecycleObserverRegistration {
    id: LifecycleObserverId,
    registry: LifecycleRegistry,
}

impl LifecycleObserverRegistration {
    pub fn id(&self) -> LifecycleObserverId {
        self.id
    }

    pub fn remove(&self) -> bool {
        match &self.registry {
            LifecycleRegistry::Session(shared) => shared
                .upgrade()
                .is_some_and(|shared| shared.observers.remove(self.id)),
            LifecycleRegistry::Uniplex(dispatcher) => dispatcher
                .upgrade()
                .is_some_and(|dispatcher| dispatcher.remove_observer(self.id)),
        }
    }

    pub fn add_observer(&self, observer: Rc<dyn LifecycleObserver>) -> LifecycleObserverId {
        match &self.registry {
            LifecycleRegistry::Session(shared) => shared
                .upgrade()
                .expect("an attachment callback retains its session")
                .add_observer(observer),
            LifecycleRegistry::Uniplex(dispatcher) => dispatcher
                .upgrade()
                .expect("an attachment callback retains its session")
                .add_observer(observer),
        }
    }

    pub(crate) fn new(id: LifecycleObserverId, registry: LifecycleRegistry) -> Self {
        Self { id, registry }
    }
}

pub(crate) struct SharedSessionState {
    pending_reservations: Cell<u32>,
    active_streams: Cell<u32>,
    outgoing_streams: Cell<u32>,
    outgoing_limit: Cell<u32>,
    outgoing_admission_closed: Cell<bool>,
    transaction_wake: Rc<tokio::sync::Notify>,
    next_observer_id: Cell<u64>,
    observers: LifecycleObservers,
    lifecycle_panic: RefCell<Option<Box<dyn std::any::Any + Send>>>,
}

#[derive(Clone)]
pub struct OutgoingTransactionActivity {
    shared: Rc<SharedSessionState>,
}

impl OutgoingTransactionActivity {
    pub fn available(&self) -> u32 {
        if self.shared.outgoing_admission_closed.get() {
            return 0;
        }
        self.shared.outgoing_limit.get().saturating_sub(
            self.shared
                .pending_reservations
                .get()
                .saturating_add(self.shared.outgoing_streams.get()),
        )
    }

    pub fn supports_more(&self) -> bool {
        self.available() != 0
    }

    /// Reserved and active transactions. The count changes before lifecycle
    /// callbacks run.
    pub fn admitted_transactions(&self) -> u32 {
        self.shared.total_transactions()
    }
}

impl SharedSessionState {
    fn release_outgoing_capacity(&self, stream: StreamId) {
        self.outgoing_streams.set(
            self.outgoing_streams
                .get()
                .checked_sub(1)
                .expect("a live outgoing stream must be accounted"),
        );
        self.notify(LifecycleEvent::OutgoingCapacityReleased { stream });
    }

    fn add_observer(self: &Rc<Self>, observer: Rc<dyn LifecycleObserver>) -> LifecycleObserverId {
        let id = LifecycleObserverId(self.next_observer_id.get());
        self.next_observer_id
            .set(self.next_observer_id.get().wrapping_add(1));
        self.observers.insert(id, observer.clone());
        let registration =
            LifecycleObserverRegistration::new(id, LifecycleRegistry::Session(Rc::downgrade(self)));
        let attached = catch_unwind(AssertUnwindSafe(|| {
            observer.on_attached_with_registration(registration)
        }));
        if attached.is_err() {
            self.observers.remove(id);
        }
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(observer)));
        if let Err(payload) = attached {
            resume_unwind(payload);
        }
        if let Err(payload) = dropped {
            resume_unwind(payload);
        }
        id
    }

    fn total_transactions(&self) -> u32 {
        self.pending_reservations
            .get()
            .checked_add(self.active_streams.get())
            .expect("admitted transaction accounting cannot overflow")
    }

    fn transaction_attached(&self) {
        if self.total_transactions() == 1 {
            self.notify(LifecycleEvent::ActivateConnection);
        }
        self.notify(LifecycleEvent::TransactionAttached);
        self.resume_lifecycle_panic();
    }

    fn transaction_detached(&self, stream: Option<StreamId>) {
        self.transaction_wake.notify_one();
        self.notify(LifecycleEvent::TransactionDetached { stream });
        if self.total_transactions() == 0 {
            self.notify(LifecycleEvent::DeactivateConnection);
        }
        self.resume_lifecycle_panic();
    }

    fn notify(&self, event: LifecycleEvent) {
        let observers = self.observers.snapshot();
        for observer in observers {
            let callback = catch_unwind(AssertUnwindSafe(|| observer.on_event(event)));
            let dropped = catch_unwind(AssertUnwindSafe(|| drop(observer)));
            for result in [callback, dropped] {
                if let Err(payload) = result {
                    let mut first = self.lifecycle_panic.borrow_mut();
                    if first.is_none() {
                        *first = Some(payload);
                    }
                }
            }
        }
    }

    fn resume_lifecycle_panic(&self) {
        if let Some(payload) = self.lifecycle_panic.borrow_mut().take() {
            resume_unwind(payload);
        }
    }
}

/// Capacity reserved for one future upstream request.
pub struct RequestReservation {
    session: Option<Rc<SharedSessionState>>,
    release_hook: Option<Rc<dyn Fn()>>,
}

impl RequestReservation {
    fn new(session: Rc<SharedSessionState>) -> Self {
        let pending = session
            .pending_reservations
            .get()
            .checked_add(1)
            .expect("admitted reservation accounting cannot overflow");
        session.pending_reservations.set(pending);
        let reservation = Self {
            session: Some(Rc::clone(&session)),
            release_hook: None,
        };
        session.transaction_attached();
        reservation
    }

    pub fn cancel(&mut self) -> bool {
        let Some(session) = self.session.take() else {
            return false;
        };
        let pending = session.pending_reservations.get();
        session.pending_reservations.set(
            pending
                .checked_sub(1)
                .expect("a live reservation must be accounted"),
        );
        let lifecycle_panic = catch_unwind(AssertUnwindSafe(|| session.transaction_detached(None)));
        let hook_panic = self
            .release_hook
            .take()
            .map(|hook| catch_unwind(AssertUnwindSafe(|| hook())));
        if let Err(payload) = lifecycle_panic {
            resume_unwind(payload);
        }
        if let Some(Err(payload)) = hook_panic {
            resume_unwind(payload);
        }
        true
    }

    pub const fn is_active(&self) -> bool {
        self.session.is_some()
    }

    fn belongs_to(&self, session: &Rc<SharedSessionState>) -> bool {
        self.session
            .as_ref()
            .is_some_and(|owner| Rc::ptr_eq(owner, session))
    }

    pub(crate) fn set_release_hook(&mut self, hook: Rc<dyn Fn()>) {
        self.release_hook = Some(hook);
    }

    fn consume_for(&mut self, session: &Rc<SharedSessionState>) -> Result<(), HttpError> {
        if !self.belongs_to(session) {
            return Err(invalid_reservation());
        }
        let owner = self
            .session
            .take()
            .expect("a validated reservation must remain active");
        self.release_hook.take();
        let pending = owner.pending_reservations.get();
        owner.pending_reservations.set(
            pending
                .checked_sub(1)
                .expect("a consumed reservation must be accounted"),
        );
        Ok(())
    }
}

impl Drop for RequestReservation {
    fn drop(&mut self) {
        let _ = catch_unwind(AssertUnwindSafe(|| self.cancel()));
    }
}

/// A newly accepted request and its real single-consumer ingress source.
pub struct IncomingRequest {
    stream: StreamId,
    sequence_number: u64,
    source: HttpSourceHolder,
    session: Rc<SharedSessionState>,
    completion: ExchangeCompletion,
}

impl IncomingRequest {
    pub const fn stream(&self) -> StreamId {
        self.stream
    }

    pub const fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn source_mut(&mut self) -> &mut HttpSourceHolder {
        &mut self.source
    }

    pub fn completion(&self) -> &ExchangeCompletion {
        &self.completion
    }

    pub(crate) fn into_source(self) -> HttpSourceHolder {
        self.source
    }
}

/// An upstream request after its header event has been consumed.
pub struct OutgoingRequest {
    pub completion: ExchangeCompletion,
    pub sequence_number: u64,
    pub headers: HttpHeaderEvent,
    pub response_source: HttpSourceHolder,
    pub body: Option<EgressTransfer>,
}

/// Wraps the request source's head read for a gated send.
///
/// The gate acts like a source filter in front of the request source. When the
/// gated read fails, the session retires the request source before it checks
/// the caller's cancellation, as an `HttpSourceFilter` retires its source
/// before passing a failure on.
pub trait RequestHeadGate: 'static {
    type Read: Future<Output = Result<HttpHeaderEvent, HttpError>> + Unpin + 'static;

    fn gate(self, read: HolderHeaderRead) -> Self::Read;
}

/// A request whose header read completed without retaining a mutable session
/// borrow. It can be committed to its owning session exactly once.
pub struct PreparedOutgoingRequest {
    headers: HttpHeaderEvent,
    source: HttpSourceHolder,
    reservation: RequestReservation,
}

impl PreparedOutgoingRequest {
    pub(crate) fn message_mut(&mut self) -> &mut HttpMessage {
        self.headers.headers_mut()
    }
}

struct ExpectedStatusSource {
    inner: Rc<RefCell<HttpSourceHolder>>,
    expected_status: u16,
}

impl ExpectedStatusSource {
    fn new(inner: HttpSourceHolder, expected_status: u16) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner)),
            expected_status,
        }
    }
}

impl HttpSource for ExpectedStatusSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let inner = Rc::clone(&self.inner);
        let expected_status = self.expected_status;
        let read = inner.borrow_mut().read_header_event();
        Box::pin(async move {
            let mut event = read.await?;
            let final_headers = event.is_final().map_err(|error| {
                HttpError::with_message(HttpErrorCode::MESSAGE_ERROR, error.to_string().as_bytes())
            })?;
            if !final_headers {
                return Ok(event);
            }
            let status = event.headers_mut().status_code().map_err(|error| {
                HttpError::with_message(HttpErrorCode::MESSAGE_ERROR, error.to_string().as_bytes())
            })?;
            if status == expected_status {
                return Ok(event);
            }

            inner.borrow_mut().stop_reading(Some(HttpErrorCode::CANCEL));
            *inner.borrow_mut() = error_response(expected_status, b"")?;
            let replacement = inner.borrow_mut().read_header_event();
            replacement.await
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<roxygen_core::events::HttpBodyEvent, HttpError>> {
        self.inner.borrow_mut().read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.inner.borrow_mut().stop_reading(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.inner.borrow().get_stream_id()
    }

    fn set_read_timeout(&mut self, timeout: std::time::Duration) {
        self.inner.borrow_mut().set_read_timeout(timeout);
    }
}

/// Owner-local preparation or callback wait that does not borrow a session.
pub type LocalSessionFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// Identifies which side of the session produced a wire reset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResetOrigin {
    Source,
    Ingress,
}

impl ResetOrigin {
    pub const fn from_source(self) -> bool {
        matches!(self, Self::Source)
    }
}

/// Wire work requested by a stream lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamAction {
    None,
    Reset {
        code: HttpErrorCode,
        origin: ResetOrigin,
    },
    SendErrorResponse(u16),
}

/// Result of one stream transition.
pub struct StreamUpdate {
    pub stream: StreamId,
    pub action: StreamAction,
    pub detached: bool,
    pub cancel_egress: bool,
    pub cancel_egress_error: Option<HttpError>,
    pub replacement: Option<EgressTransfer>,
}

pub(crate) struct ProcessedCallbacks {
    pub(crate) updates: Vec<StreamUpdate>,
    pub(crate) flow_timeout_activity: bool,
}

impl StreamUpdate {
    pub(crate) fn new(stream: StreamId, action: StreamAction, detached: bool) -> Self {
        Self {
            stream,
            action,
            detached,
            cancel_egress: false,
            cancel_egress_error: None,
            replacement: None,
        }
    }
}

/// Receive-window credit returned by consumption of ingress body bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowControlUpdate {
    pub stream: StreamId,
    pub amount: usize,
    pub to_acknowledge: usize,
    pub window_opened: bool,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SessionTaskError {
    #[error("session read/write tasks were already started")]
    AlreadyStarted,
    #[error(transparent)]
    TaskGroup(#[from] TaskGroupError),
}

struct SiblingWake {
    cancellation: Rc<CancellationSource>,
}

struct HandlerTaskControl {
    generation: u64,
    cancellation: Rc<HandlerInvocation>,
}

struct HandlerInvocationGuard(Rc<HandlerInvocation>);

impl HandlerInvocationGuard {
    fn token(&self) -> CancellationToken {
        self.0.token()
    }

    fn finish(&self, eligible: bool) -> Option<HandlerResponseProposal> {
        let proposal = self.0.seal_response_proposal();
        if eligible { proposal } else { None }
    }
}

impl Drop for HandlerInvocationGuard {
    fn drop(&mut self) {
        drop(self.0.seal_response_proposal());
    }
}

pub(crate) struct StreamAbortRetirement {
    stream: StreamId,
    handler: Option<(u64, Rc<HandlerInvocation>)>,
    reset: Option<PreparedStreamReset>,
    removed: Option<StreamState>,
    detached: bool,
    released_outgoing: bool,
    deactivate: bool,
    shared: Rc<SharedSessionState>,
    exchange: ExchangeTracker,
    guard: Option<ExchangeGuard>,
    retired: bool,
}

impl StreamAbortRetirement {
    pub(crate) fn prepare_queues(&mut self) -> Option<EgressPanic> {
        let first = self
            .reset
            .as_mut()
            .and_then(PreparedStreamReset::prepare_queues);
        if first.is_some() {
            self.exchange.fail();
        }
        first
    }

    pub(crate) fn update(&self) -> StreamUpdate {
        StreamUpdate::new(self.stream, StreamAction::None, self.detached)
    }

    fn notify(&self, event: LifecycleEvent, first: &mut Option<EgressPanic>) {
        let observers = self.shared.observers.snapshot();
        for observer in observers {
            retain_egress_panic(
                first,
                catch_unwind(AssertUnwindSafe(|| observer.on_event(event))).err(),
            );
            retain_egress_panic(
                first,
                catch_unwind(AssertUnwindSafe(|| drop(observer))).err(),
            );
        }
    }

    fn retire_inner(&mut self) -> Option<EgressPanic> {
        if self.retired {
            return None;
        }
        let mut first = self.prepare_queues();
        if let Some((_, cancellation)) = self.handler.take() {
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| cancellation.request_cancellation())).err(),
            );
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(cancellation))).err(),
            );
        }
        if let Some(mut reset) = self.reset.take() {
            match catch_unwind(AssertUnwindSafe(|| reset.retire())) {
                Ok(panic) => retain_egress_panic(&mut first, panic),
                Err(payload) => retain_egress_panic(&mut first, Some(payload)),
            }
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(reset))).err(),
            );
        }
        let removed = self.removed.take();
        retain_egress_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| drop(removed))).err(),
        );
        if first.is_some() || std::thread::panicking() {
            self.exchange.fail();
        }
        if self.detached {
            if self.released_outgoing {
                self.notify(
                    LifecycleEvent::OutgoingCapacityReleased {
                        stream: self.stream,
                    },
                    &mut first,
                );
            }
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| {
                    self.shared.transaction_wake.notify_one()
                }))
                .err(),
            );
            let notified = catch_unwind(AssertUnwindSafe(|| {
                self.notify(
                    LifecycleEvent::TransactionDetached {
                        stream: Some(self.stream),
                    },
                    &mut first,
                )
            }))
            .err();
            retain_egress_panic(&mut first, notified);
            if self.deactivate {
                let notified = catch_unwind(AssertUnwindSafe(|| {
                    self.notify(LifecycleEvent::DeactivateConnection, &mut first)
                }))
                .err();
                retain_egress_panic(&mut first, notified);
            }
            if first.is_some() {
                self.exchange.fail();
            }
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| self.exchange.mark_detached())).err(),
            );
        }
        if first.is_some() {
            self.exchange.fail();
        }
        let guard = self.guard.take();
        retain_egress_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| drop(guard))).err(),
        );
        if first.is_some() {
            self.exchange.fail();
        }
        self.retired = true;
        first
    }

    pub(crate) fn retire(mut self) -> std::thread::Result<()> {
        self.retire_inner().map_or(Ok(()), Err)
    }
}

impl Drop for StreamAbortRetirement {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        if let Some(payload) = self.retire_inner() {
            if unwinding {
                discard_egress_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

impl Drop for SiblingWake {
    fn drop(&mut self) {
        self.cancellation.request_cancellation();
    }
}

/// Structured owner for the session's sibling read and write operations.
pub struct SessionTaskSet {
    tasks: TaskGroup,
    sibling_cancellation: Rc<CancellationSource>,
    started: bool,
}

impl SessionTaskSet {
    pub fn new() -> Self {
        Self::with_host(Host::tokio())
    }

    /// Owns the sibling operations scheduled by this worker's host.
    pub fn with_host(host: Host) -> Self {
        Self {
            tasks: TaskGroup::with_host(host),
            sibling_cancellation: Rc::new(CancellationSource::new()),
            started: false,
        }
    }

    pub fn start<R, RF, W, WF>(&mut self, read: R, write: W) -> Result<(), SessionTaskError>
    where
        R: FnOnce(CancellationToken) -> RF + 'static,
        RF: Future<Output = ()> + 'static,
        W: FnOnce(CancellationToken) -> WF + 'static,
        WF: Future<Output = ()> + 'static,
    {
        if self.started {
            return Err(SessionTaskError::AlreadyStarted);
        }
        self.started = true;
        let read_cancellation = Rc::clone(&self.sibling_cancellation);
        self.tasks.spawn_local(move |_| async move {
            let wake = SiblingWake {
                cancellation: Rc::clone(&read_cancellation),
            };
            read(read_cancellation.token()).await;
            drop(wake);
        })?;
        let write_cancellation = Rc::clone(&self.sibling_cancellation);
        self.tasks.spawn_local(move |_| async move {
            let wake = SiblingWake {
                cancellation: Rc::clone(&write_cancellation),
            };
            write(write_cancellation.token()).await;
            drop(wake);
        })?;
        Ok(())
    }

    pub fn close(&mut self) -> bool {
        self.tasks.close()
    }

    pub fn cancel(&mut self) -> bool {
        let sibling_requested = self.sibling_cancellation.request_cancellation();
        let group_requested = self.tasks.cancel();
        sibling_requested || group_requested
    }

    pub async fn join(&mut self) -> Result<(), SessionTaskError> {
        self.tasks.join().await.map_err(SessionTaskError::from)
    }

    pub async fn cancel_and_join(&mut self) -> Result<(), SessionTaskError> {
        self.cancel();
        self.tasks.join().await.map_err(SessionTaskError::from)
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for SessionTaskSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SessionTaskSet {
    fn drop(&mut self) {
        self.tasks.close();
    }
}

/// Protocol-neutral, worker-local HTTP session and stream lifecycle owner.
pub struct SessionCore {
    host: Host,
    direction: TransportDirection,
    protocol: CodecProtocol,
    max_concurrent_outgoing_streams: u32,
    ingress_receive_window: u32,
    drain_request_body_on_drop: bool,
    deliver_ingress_before_abort: bool,
    empty_content_length_is_zero: bool,
    egress_write_timeout: std::time::Duration,
    buffered_egress_stats_enabled: bool,
    peer_identity: Option<Rc<[u8]>>,
    streams: SmallMap<StreamId, StreamState>,
    exchanges: SmallMap<u64, (StreamId, ExchangeOwner)>,
    next_exchange_id: u64,
    driver_cleanup_required: bool,
    local_streams: SmallSet<StreamId>,
    shared: Rc<SharedSessionState>,
    callbacks: StreamCallbackQueue,
    flow_control_updates: VecDeque<FlowControlUpdate>,
    draining: bool,
    allow_draining_ingress: bool,
    drain_trigger: Option<DrainTrigger>,
    connection_error: Option<HttpError>,
    tasks: Option<SessionTaskSet>,
    handler_tasks: Option<TaskGroup>,
    handler_retirements: BTreeMap<TaskId, ExchangeGuard>,
    handlers: BTreeMap<StreamId, HandlerTaskControl>,
    next_handler_generation: u64,
    lifetime: Option<KeepAliveOwner<()>>,
    worker: WorkerHandle,
    destroyed: bool,
    owner_thread: ThreadId,
}

impl SessionCore {
    /// Creates an in-scope HTTP/1.1 or HTTP/2 session.
    pub fn new(
        direction: TransportDirection,
        protocol: CodecProtocol,
        worker: WorkerHandle,
    ) -> Result<Self, HttpError> {
        Self::with_host(direction, protocol, worker, Host::tokio())
    }

    /// Creates a session whose local tasks and stream waits use the worker's host.
    pub fn with_host(
        direction: TransportDirection,
        protocol: CodecProtocol,
        worker: WorkerHandle,
        host: Host,
    ) -> Result<Self, HttpError> {
        let maximum = match protocol {
            CodecProtocol::Http11 => 1,
            CodecProtocol::Http2 => 100,
            _ => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"session core supports only HTTP/1.1 and HTTP/2",
                ));
            }
        };
        let lifetime = KeepAliveOwner::new_for_worker((), &worker).map_err(|error| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                error.to_string().as_bytes(),
            )
        })?;
        Ok(Self {
            direction,
            protocol,
            max_concurrent_outgoing_streams: maximum,
            ingress_receive_window: DEFAULT_INGRESS_RECEIVE_WINDOW,
            drain_request_body_on_drop: false,
            deliver_ingress_before_abort: false,
            empty_content_length_is_zero: false,
            egress_write_timeout: DEFAULT_EGRESS_WRITE_TIMEOUT,
            buffered_egress_stats_enabled: true,
            peer_identity: None,
            streams: SmallMap::new(),
            exchanges: SmallMap::new(),
            next_exchange_id: 0,
            driver_cleanup_required: false,
            local_streams: SmallSet::new(),
            shared: Rc::new(SharedSessionState {
                pending_reservations: Cell::new(0),
                active_streams: Cell::new(0),
                outgoing_streams: Cell::new(0),
                outgoing_limit: Cell::new(maximum),
                outgoing_admission_closed: Cell::new(false),
                transaction_wake: Rc::new(tokio::sync::Notify::new()),
                next_observer_id: Cell::new(1),
                observers: LifecycleObservers::default(),
                lifecycle_panic: RefCell::new(None),
            }),
            callbacks: Rc::new(StreamCallbacks::new()),
            flow_control_updates: VecDeque::new(),
            draining: false,
            allow_draining_ingress: false,
            drain_trigger: None,
            connection_error: None,
            tasks: None,
            handler_tasks: None,
            host,
            handler_retirements: BTreeMap::new(),
            handlers: BTreeMap::new(),
            next_handler_generation: 1,
            lifetime: Some(lifetime),
            worker,
            destroyed: false,
            owner_thread: current_id(),
        })
    }

    pub const fn direction(&self) -> TransportDirection {
        self.direction
    }

    pub const fn protocol(&self) -> CodecProtocol {
        self.protocol
    }

    pub fn worker_id(&self) -> WorkerId {
        self.worker.id()
    }

    pub fn acquire_session_handle(
        &self,
        worker: WorkerHandle,
    ) -> Result<CoroSessionHandle<()>, SessionHandleError> {
        CoroSessionHandle::new(
            self.lifetime
                .as_ref()
                .expect("a live session retains its keep-alive owner"),
            worker,
        )
    }

    pub fn add_lifecycle_observer(
        &self,
        observer: Rc<dyn LifecycleObserver>,
    ) -> LifecycleObserverId {
        self.assert_owner();
        self.shared.add_observer(observer)
    }

    pub fn remove_lifecycle_observer(&self, id: LifecycleObserverId) -> bool {
        self.assert_owner();
        self.shared.observers.remove(id)
    }

    pub fn set_max_concurrent_outgoing_streams(&mut self, maximum: u32) -> bool {
        self.assert_owner();
        if self.protocol != CodecProtocol::Http2 {
            return false;
        }
        self.max_concurrent_outgoing_streams = maximum;
        self.shared.outgoing_limit.set(maximum);
        true
    }

    pub fn outgoing_transaction_activity(&self) -> OutgoingTransactionActivity {
        OutgoingTransactionActivity {
            shared: Rc::clone(&self.shared),
        }
    }

    pub fn set_ingress_receive_window(&mut self, capacity: u32) -> bool {
        self.assert_owner();
        if capacity == 0 || capacity > i32::MAX as u32 || !self.streams.is_empty() {
            return false;
        }
        self.ingress_receive_window = capacity;
        true
    }

    pub fn set_egress_write_timeout(&mut self, timeout: std::time::Duration) -> bool {
        self.assert_owner();
        if !self.streams.is_empty() {
            return false;
        }
        self.egress_write_timeout = timeout;
        true
    }

    /// Keep downstream ingress and receive credit alive until EOM after a graceful
    /// request-consumer drop. Returns `false`, leaving the setting unchanged,
    /// if the session already has streams.
    pub fn set_drain_request_body_on_drop(&mut self, enabled: bool) -> bool {
        self.assert_owner();
        if !self.streams.is_empty() {
            return false;
        }
        self.drain_request_body_on_drop = enabled;
        true
    }

    pub(crate) fn drains_request_body_on_drop(&self) -> bool {
        self.drain_request_body_on_drop
    }

    /// Sets whether an ingress abort lets the reader first read the events
    /// already queued on the stream's source, then the error. Returns `false`,
    /// leaving the setting unchanged, if the session already has streams.
    pub fn set_deliver_ingress_before_abort(&mut self, enabled: bool) -> bool {
        self.assert_owner();
        if !self.streams.is_empty() {
            return false;
        }
        self.deliver_ingress_before_abort = enabled;
        true
    }

    /// Sets whether a final head with an empty `content-length` value has its
    /// body checked against zero, as h2 treats such a field. Returns `false`,
    /// leaving the setting unchanged, if the session already has streams.
    pub fn set_empty_content_length_is_zero(&mut self, enabled: bool) -> bool {
        self.assert_owner();
        if !self.streams.is_empty() {
            return false;
        }
        self.empty_content_length_is_zero = enabled;
        true
    }

    pub(crate) fn empty_content_length_is_zero(&self) -> bool {
        self.empty_content_length_is_zero
    }

    pub(crate) fn set_peer_identity(&mut self, peer_identity: Option<Rc<[u8]>>) -> bool {
        self.assert_owner();
        if !self.streams.is_empty() {
            return false;
        }
        self.peer_identity = peer_identity;
        true
    }

    pub(crate) fn arm_stream_read_timeout(&self, id: StreamId) -> Result<bool, HttpError> {
        Ok(self.stream(id)?.ingress_producer().arm_read_timeout())
    }

    pub fn num_outgoing_streams(&self) -> u32 {
        if self.protocol == CodecProtocol::Http2 || self.direction.is_upstream() {
            return self
                .shared
                .outgoing_streams
                .get()
                .saturating_add(self.shared.pending_reservations.get());
        }
        0
    }

    pub fn num_incoming_streams(&self) -> u32 {
        if self.protocol == CodecProtocol::Http2 {
            let local = u32::try_from(self.local_streams.len()).unwrap_or(u32::MAX);
            return self.shared.active_streams.get().saturating_sub(local);
        }
        if self.direction.is_downstream() {
            self.shared.active_streams.get()
        } else {
            0
        }
    }

    pub fn num_transactions(&self) -> u32 {
        self.shared.total_transactions()
    }

    pub(crate) fn stream_ids(&self) -> Vec<StreamId> {
        self.streams.keys().copied().collect()
    }

    pub(crate) fn ingress_reconciliation_pending(&self) -> bool {
        self.callbacks.ingress_reconciliation_pending()
    }

    pub(crate) fn mark_ingress_reconciliation_pending(&self) {
        self.callbacks.mark_ingress_reconciliation_pending();
    }

    pub(crate) fn acknowledge_ingress_reconciliation(&self) {
        self.callbacks.acknowledge_ingress_reconciliation();
    }

    pub fn transaction_detach_wait(&self) -> LocalSessionFuture<()> {
        let wake = Rc::clone(&self.shared.transaction_wake);
        Box::pin(async move {
            wake.notified().await;
        })
    }

    pub fn supports_more_transactions(&self) -> bool {
        !self.draining
            && !self.shared.outgoing_admission_closed.get()
            && self.direction.is_upstream()
            && self.num_outgoing_streams() < self.max_concurrent_outgoing_streams
    }

    pub(crate) fn outgoing_capacity_available(&self) -> bool {
        !self.draining
            && !self.shared.outgoing_admission_closed.get()
            && self.num_outgoing_streams() < self.max_concurrent_outgoing_streams
    }

    pub(crate) fn close_outgoing_admission(&self) {
        self.assert_owner();
        if !self.shared.outgoing_admission_closed.replace(true) {
            self.shared.notify(LifecycleEvent::OutgoingAdmissionClosed);
            self.shared.resume_lifecycle_panic();
        }
    }

    pub fn reserve_request(&self) -> Result<RequestReservation, HttpError> {
        self.assert_owner();
        if !self.supports_more_transactions() {
            return Err(HttpError::with_message(
                HttpErrorCode::REFUSED_STREAM,
                b"exceeded stream limit",
            ));
        }
        Ok(RequestReservation::new(Rc::clone(&self.shared)))
    }

    /// Starts a request-header read that owns its source and reservation.
    ///
    /// The returned future is `'static`, so polling it never retains a borrow
    /// of `SessionCore` while the source is pending.
    pub fn prepare_request(
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalSessionFuture<Result<PreparedOutgoingRequest, HttpError>> {
        Box::pin(Self::prepare_request_unboxed(
            request_source,
            reservation,
            cancellation,
        ))
    }

    /// [`Self::prepare_request`] without boxing the returned future.
    pub(crate) fn prepare_request_unboxed(
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> impl Future<Output = Result<PreparedOutgoingRequest, HttpError>> + 'static {
        Self::prepare_request_with(
            request_source,
            reservation,
            cancellation,
            |read| read,
            false,
        )
    }

    /// Starts a request-header read through `gate` that owns its source and
    /// reservation, as [`RequestHeadGate`] describes.
    pub(crate) fn prepare_request_gated<G: RequestHeadGate>(
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
        gate: G,
    ) -> impl Future<Output = Result<PreparedOutgoingRequest, HttpError>> + 'static {
        Self::prepare_request_with(
            request_source,
            reservation,
            cancellation,
            |read| gate.gate(read),
            true,
        )
    }

    #[expect(
        clippy::manual_async_fn,
        reason = "the async block keeps `prepare_request`'s capture and drop order"
    )]
    fn prepare_request_with<R>(
        mut request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
        head: impl FnOnce(HolderHeaderRead) -> R + 'static,
        retire_on_error: bool,
    ) -> impl Future<Output = Result<PreparedOutgoingRequest, HttpError>> + 'static
    where
        R: Future<Output = Result<HttpHeaderEvent, HttpError>> + Unpin + 'static,
    {
        async move {
            let headers = await_request_head(
                head(request_source.read_header_event_unboxed()),
                retire_on_error.then_some(&mut request_source),
                cancellation,
                b"request header read cancelled",
            )
            .await?;
            Ok(PreparedOutgoingRequest {
                headers,
                source: request_source,
                reservation,
            })
        }
    }

    /// Owns caller-provided request headers without reading them from the body
    /// source. The reservation is still validated when the request is sent.
    pub fn prepare_request_with_headers(
        headers: HttpHeaderEvent,
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
    ) -> PreparedOutgoingRequest {
        PreparedOutgoingRequest {
            headers,
            source: request_source,
            reservation,
        }
    }

    /// Checks whether a prepared request can be committed without mutating it.
    pub fn validate_prepared_request(
        &self,
        request: &PreparedOutgoingRequest,
    ) -> Result<(), HttpError> {
        self.assert_owner();
        if !self.direction.is_upstream() || !request.reservation.belongs_to(&self.shared) {
            return Err(invalid_reservation());
        }
        if self.shared.outgoing_admission_closed.get()
            || (self.draining && !self.allow_draining_ingress)
        {
            return Err(HttpError::with_message(
                HttpErrorCode::REFUSED_STREAM,
                b"session is draining",
            ));
        }
        Ok(())
    }

    /// Commits a prepared request to a new stream without awaiting source I/O.
    pub fn send_prepared_request(
        &mut self,
        mut request: PreparedOutgoingRequest,
        id: StreamId,
    ) -> Result<OutgoingRequest, HttpError> {
        self.validate_prepared_request(&request)?;
        self.validate_new_stream(id)?;
        let head_response = matches!(
            request.headers.headers_mut().method(),
            Ok(Some(HttpMethod::Head))
        );
        let (state, response_source) = self.new_stream_state(id, head_response)?;
        // The request must drop before the handoff guard on every exit.
        let _handoff = request
            .source
            .readable()
            .then(|| state.exchange_tracker().guard());
        let mut prepared = request;
        prepared.reservation.consume_for(&self.shared)?;
        if prepared.headers.eom() {
            state.validate_header_only_content_length(
                prepared
                    .headers
                    .headers()
                    .headers()
                    .get_single_or_empty(HttpHeaderCode::ContentLength),
            );
        }
        self.insert_stream(state, false)?;
        self.local_streams.insert(id);
        self.shared
            .outgoing_streams
            .set(self.shared.outgoing_streams.get().saturating_add(1));

        let body = if prepared.headers.eom() {
            // Keep response ownership local if destruction of the unused source panics.
            drop(prepared.source);
            None
        } else {
            Some(
                self.streams
                    .get_mut(&id)
                    .expect("inserted stream")
                    .install_egress_source(
                        prepared.source,
                        false,
                        Some(
                            prepared
                                .headers
                                .headers()
                                .headers()
                                .get_single_or_empty(HttpHeaderCode::ContentLength),
                        ),
                    )?,
            )
        };
        Ok(OutgoingRequest {
            completion: self
                .exchange_tracker(id)
                .expect("new exchange")
                .completion(),
            sequence_number: self.stream_sequence_number(id)?,
            headers: prepared.headers,
            response_source,
            body,
        })
    }

    /// Creates one downstream stream and transfers its source to the caller.
    pub fn begin_incoming_stream(&mut self, id: StreamId) -> Result<IncomingRequest, HttpError> {
        self.assert_owner();
        if !self.direction.is_downstream() {
            return Err(invalid_direction(
                b"only a downstream session accepts request streams",
            ));
        }
        if self.draining && !self.allow_draining_ingress {
            return Err(HttpError::with_message(
                HttpErrorCode::REFUSED_STREAM,
                b"session is draining",
            ));
        }
        self.validate_new_stream(id)?;
        let sequence_number = self.stream_sequence_number(id)?;
        let (state, source) = self.new_stream_state(id, false)?;
        self.shared.notify(LifecycleEvent::RequestBegin {
            stream: id,
            sequence_number,
        });
        match catch_unwind(AssertUnwindSafe(|| self.insert_stream(state, true))) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.shared.resume_lifecycle_panic();
                return Err(error);
            }
            Err(payload) => {
                self.mark_ingress_reconciliation_pending();
                if self.streams.remove(&id).is_some() {
                    let active = self.shared.active_streams.get();
                    self.shared.active_streams.set(
                        active
                            .checked_sub(1)
                            .expect("a rolled-back stream must be accounted"),
                    );
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        self.shared.transaction_detached(Some(id))
                    }));
                }
                self.exchanges
                    .retain(|_, (stream, exchange)| *stream != id || exchange.tracker().detached());
                resume_unwind(payload);
            }
        }
        Ok(IncomingRequest {
            stream: id,
            sequence_number,
            source,
            session: Rc::clone(&self.shared),
            completion: self
                .exchange_tracker(id)
                .expect("new exchange")
                .completion(),
        })
    }

    /// Creates a peer-initiated pushed response stream for an upstream H2
    /// session. Ownership of the returned source is transferred into the
    /// associated request's push event.
    pub(crate) fn begin_pushed_stream(
        &mut self,
        id: StreamId,
    ) -> Result<IncomingRequest, HttpError> {
        self.assert_owner();
        if !self.direction.is_upstream() || self.protocol != CodecProtocol::Http2 {
            return Err(invalid_direction(
                b"only an upstream HTTP/2 session accepts pushed streams",
            ));
        }
        if self.draining {
            return Err(HttpError::with_message(
                HttpErrorCode::REFUSED_STREAM,
                b"session is draining",
            ));
        }
        self.validate_new_stream(id)?;
        let sequence_number = self.stream_sequence_number(id)?;
        let (mut state, source) = self.new_stream_state(id, false)?;
        state.mark_egress_complete();
        self.insert_stream(state, true)?;
        Ok(IncomingRequest {
            stream: id,
            sequence_number,
            source,
            session: Rc::clone(&self.shared),
            completion: self
                .exchange_tracker(id)
                .expect("new exchange")
                .completion(),
        })
    }

    pub(crate) fn observe_request_begin(&self, id: StreamId) -> Result<(), HttpError> {
        self.shared.notify(LifecycleEvent::RequestBegin {
            stream: id,
            sequence_number: self.stream_sequence_number(id)?,
        });
        self.shared.resume_lifecycle_panic();
        Ok(())
    }

    pub(crate) fn observe_lifecycle(&self, event: LifecycleEvent) {
        self.shared.notify(event);
    }

    pub(crate) fn start_outgoing_push(
        &mut self,
        id: StreamId,
        promise: HttpMessage,
        source: HttpSourceHolder,
    ) -> Result<EgressTransfer, HttpError> {
        self.assert_owner();
        if !self.direction.is_downstream() || self.protocol != CodecProtocol::Http2 {
            return Err(invalid_direction(
                b"only a downstream HTTP/2 session can originate a push",
            ));
        }
        self.validate_new_stream(id)?;
        let (mut state, ingress_source) = self.new_stream_state(id, false)?;
        let transfer = state.install_egress_source(source, true, None)?;
        self.local_streams.insert(id);
        if let Err(error) = self.insert_stream(state, true) {
            self.local_streams.remove(&id);
            return Err(error);
        }
        self.shared
            .outgoing_streams
            .set(self.shared.outgoing_streams.get().saturating_add(1));
        if !self.ingress_headers(id, promise, true)? {
            return Err(invalid_transition(
                b"outgoing push promise headers were rejected",
            ));
        }
        self.stream_mut(id)?.mark_source_complete(None);
        drop(ingress_source);
        Ok(transfer)
    }

    /// Starts a session-owned handler task without borrowing the session while
    /// the handler consumes streaming ingress.
    pub fn start_request_handler(
        &mut self,
        request: IncomingRequest,
        handler: HttpHandlerRef,
    ) -> Result<(), HttpError> {
        self.assert_owner();
        if self.connection_error.is_some() {
            return Err(HttpError::with_message(
                HttpErrorCode::REFUSED_STREAM,
                b"session has failed",
            ));
        }
        let id = request.stream;
        if !Rc::ptr_eq(&request.session, &self.shared)
            || request.sequence_number != self.stream_sequence_number(id)?
            || request.source.get_stream_id() != Some(id)
        {
            return Err(invalid_transition(
                b"incoming request does not belong to this session stream",
            ));
        }
        if self.stream(id)?.egress_complete() {
            return Err(invalid_transition(b"cannot handle a reset stream"));
        }
        if self.handlers.contains_key(&id) {
            return Err(invalid_transition(b"stream already has a handler task"));
        }
        let lifetime = self
            .lifetime
            .as_ref()
            .expect("a live session retains its keep-alive owner");
        let context =
            HandlerSessionContext::acquire_with_peer_identity(lifetime, self.peer_identity.clone())
                .map_err(|error| {
                    HttpError::with_message(HttpErrorCode::DROPPED, error.to_string().as_bytes())
                })?
                .with_exchange_completion(self.stream(id)?.exchange_tracker().completion())?;
        let task_context = context.clone();
        let generation = self.next_handler_generation;
        self.next_handler_generation = self.next_handler_generation.wrapping_add(1);
        let cancellation = Rc::new(HandlerInvocation::new());
        let context = context.with_response_proposer(cancellation.response_proposer())?;
        let task_context = task_context.with_response_proposer(cancellation.response_proposer())?;
        self.handlers.insert(
            id,
            HandlerTaskControl {
                generation,
                cancellation: Rc::clone(&cancellation),
            },
        );
        let events = Rc::clone(&self.callbacks);
        let exchange = self.exchange_tracker(id).expect("handler exchange");
        let retirement = exchange.guard();
        let invocation = HandlerInvocationGuard(cancellation);
        let task = self
            .handler_tasks_mut()
            .spawn_local_tracked(move |_| async move {
                let (result, eligible) = invoke_handler_or_cancellation(
                    handler,
                    context,
                    request.source,
                    invocation.token(),
                )
                .await;
                let result = result.map(|source| exchange.source(source));
                drop(task_context);
                let proposal = invocation.finish(eligible);
                events.push_handler_complete(StreamCallbackEvent::HandlerComplete {
                    id,
                    generation,
                    result,
                    proposal,
                });
            })
            .map_err(|error| {
                self.handlers.remove(&id);
                retirement.fail();
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
            })?;
        self.handler_retirements.insert(task, retirement);
        Ok(())
    }

    /// Waits until a callback is queued or cancellation is requested.
    ///
    /// The returned future owns the callback queue handle and therefore does
    /// not borrow the session while pending. One uniplex event loop should
    /// wait at a time.
    pub fn pending_event_wait(
        &self,
        cancellation: CancellationToken,
    ) -> LocalSessionFuture<WaitStatus> {
        #[cfg(test)]
        crate::uniplex::writer_probe::note_pending_event_wait();
        let callbacks = Rc::clone(&self.callbacks);
        Box::pin(async move { callbacks.wait(cancellation).await })
    }

    /// Returns the session's stable callback queue for an owner loop that
    /// keeps its own wait state.
    pub(crate) fn callback_queue(&self) -> StreamCallbackQueue {
        Rc::clone(&self.callbacks)
    }

    /// Reports whether the owner loop has callback work ready to process.
    pub fn has_pending_events(&self) -> bool {
        self.callbacks.has_pending()
    }

    pub(crate) fn has_pending_driver_work(&self) -> bool {
        !self.driver_cleanup_required
            || self.callbacks.has_pending()
            || self
                .handler_tasks
                .as_ref()
                .is_some_and(TaskGroup::has_ready_completion)
            || !self.flow_control_updates.is_empty()
            || self.ingress_reconciliation_pending()
    }

    #[cfg(test)]
    pub(crate) fn has_pending_handler_completion(&self) -> bool {
        self.callbacks.has_handler_completion()
    }

    pub fn install_egress_source(
        &mut self,
        id: StreamId,
        source: HttpSourceHolder,
        starts_with_headers: bool,
    ) -> Result<EgressTransfer, HttpError> {
        self.stream_mut(id)?
            .install_egress_source(source, starts_with_headers, None)
    }

    /// Records CONNECT or WebSocket upgrade progress for a header block on a
    /// live stream and returns whether an upgrade is in progress or complete.
    pub(crate) fn check_for_upgrade(
        &mut self,
        id: StreamId,
        message: &HttpMessage,
        is_ingress: bool,
    ) -> bool {
        self.streams
            .get_mut(&id)
            .is_some_and(|state| state.check_for_upgrade(message, is_ingress))
    }

    pub fn ingress_headers(
        &mut self,
        id: StreamId,
        message: HttpMessage,
        end_stream: bool,
    ) -> Result<bool, HttpError> {
        let producer = self.stream(id)?.ingress_producer();
        let accepted = producer.headers(message, end_stream);
        if accepted && end_stream {
            self.stream_mut(id)?.mark_ingress_eom_seen();
            self.release_completed_outgoing_capacity(id);
        }
        self.shared.resume_lifecycle_panic();
        Ok(accepted)
    }

    pub fn ingress_body(
        &mut self,
        id: StreamId,
        body: BufQueue,
        padding: u16,
        end_stream: bool,
    ) -> Result<FlowControlState, HttpError> {
        let producer = self.stream(id)?.ingress_producer();
        let result = producer.body(body, padding, end_stream);
        if result != FlowControlState::Error && end_stream {
            self.stream_mut(id)?.mark_ingress_eom_seen();
            self.release_completed_outgoing_capacity(id);
        }
        self.shared.resume_lifecycle_panic();
        Ok(result)
    }

    pub fn ingress_eom(&mut self, id: StreamId) -> Result<bool, HttpError> {
        let producer = self.stream(id)?.ingress_producer();
        let accepted = producer.eom();
        if accepted {
            self.stream_mut(id)?.mark_ingress_eom_seen();
            self.release_completed_outgoing_capacity(id);
        }
        self.shared.resume_lifecycle_panic();
        Ok(accepted)
    }

    /// Applies callbacks emitted by real ingress sources and egress producers.
    pub fn process_pending_events(&mut self) -> Result<Vec<StreamUpdate>, HttpError> {
        self.process_pending_events_for_driver()
            .map(|processed| processed.updates)
    }

    pub(crate) fn process_pending_events_for_driver(
        &mut self,
    ) -> Result<ProcessedCallbacks, HttpError> {
        self.assert_owner();
        self.callbacks.acknowledge_retirement();
        let mut flow_timeout_activity = self.reap_completed_handler_tasks()?;
        let mut updates = Vec::new();
        loop {
            let event = self.callbacks.pop_front();
            let Some(event) = event else {
                break;
            };
            flow_timeout_activity |= !matches!(&event, StreamCallbackEvent::BytesProcessed { .. });
            match event {
                StreamCallbackEvent::SourceComplete { id, error } => {
                    if self.streams.contains_key(&id) {
                        updates.push(self.apply_source_completion(id, error)?);
                    }
                }
                StreamCallbackEvent::EgressProducerComplete {
                    id,
                    generation,
                    error,
                } => {
                    if let Some(state) = self.streams.get_mut(&id) {
                        let current = state.current_egress_generation() == generation;
                        state.finish_egress_generation(generation, error.clone());
                        let mut cancel_handler = false;
                        let action = if current
                            && error.is_some()
                            && !state.egress_complete()
                            && state.error_response_status().is_none()
                        {
                            let code = error
                                .as_ref()
                                .map(HttpError::code)
                                .unwrap_or(HttpErrorCode::INTERNAL_ERROR);
                            state.abort_ingress(
                                HttpErrorCode::CORO_CANCELLED,
                                b"egress source failed",
                            );
                            state.reset(HttpError::with_message(code, b"egress source failed"));
                            cancel_handler = true;
                            StreamAction::Reset {
                                code,
                                origin: ResetOrigin::Source,
                            }
                        } else {
                            StreamAction::None
                        };
                        if cancel_handler {
                            self.cancel_handler(id);
                        }
                        let detached = self.detach_if_complete(id);
                        updates.push(StreamUpdate::new(id, action, detached));
                    }
                }
                StreamCallbackEvent::HandlerComplete {
                    id,
                    generation,
                    result,
                    proposal,
                } => {
                    let current = self
                        .handlers
                        .get(&id)
                        .is_some_and(|control| control.generation == generation);
                    if !current {
                        drop(result);
                        continue;
                    }
                    self.handlers.remove(&id);
                    let Some(state) = self.streams.get_mut(&id) else {
                        drop(result);
                        continue;
                    };
                    if state.egress_complete()
                        || state.error_response_status().is_some()
                        || state.has_handler_error_response()
                    {
                        drop(result);
                        let detached = self.detach_if_complete(id);
                        updates.push(StreamUpdate::new(id, StreamAction::None, detached));
                        continue;
                    }
                    if state.can_send_headers() {
                        if let Some(proposal) = proposal {
                            proposal.accept();
                        }
                    }
                    let expected_status = state.take_expected_handler_error_status();
                    let (mut response, mut action, failure, handler_error_response) = match result {
                        Ok(response) if response.readable() => {
                            (Some(response), StreamAction::None, None, false)
                        }
                        Ok(_) => (
                            Some(error_response(500, b"handler returned no response")?),
                            StreamAction::SendErrorResponse(500),
                            Some(HttpError::with_message(
                                HttpErrorCode::INTERNAL_ERROR,
                                b"handler returned no response",
                            )),
                            true,
                        ),
                        Err(error) if error.code() == HttpErrorCode::READ_TIMEOUT => (
                            Some(error_response(408, b"")?),
                            StreamAction::SendErrorResponse(408),
                            Some(error),
                            true,
                        ),
                        Err(error) => (
                            Some(error_response(500, error.message())?),
                            StreamAction::SendErrorResponse(500),
                            Some(error),
                            true,
                        ),
                    };
                    if let Some(expected_status) = expected_status {
                        let source = response
                            .take()
                            .expect("an expected error status always has a handler response");
                        response = Some(HttpSourceHolder::new(Box::new(
                            ExpectedStatusSource::new(source, expected_status),
                        )));
                        action = StreamAction::SendErrorResponse(expected_status);
                    }
                    let mut update = if state.can_send_headers() {
                        if handler_error_response {
                            state.mark_handler_error_response();
                        }
                        let response =
                            response.expect("a handler result always selects a response source");
                        let transfer = if !handler_error_response && expected_status.is_none() {
                            state.install_tracked_egress_source(response, true, None)?
                        } else {
                            state.install_egress_source(response, true, None)?
                        };
                        let mut update = StreamUpdate::new(id, action, false);
                        update.replacement = Some(transfer);
                        update
                    } else {
                        drop(response);
                        let error = failure.unwrap_or_else(|| {
                            HttpError::with_message(
                                HttpErrorCode::INTERNAL_ERROR,
                                b"handler completed after response headers",
                            )
                        });
                        let code = error.code();
                        state.abort_ingress(
                            HttpErrorCode::CORO_CANCELLED,
                            b"handler failed after response headers",
                        );
                        state.reset(error);
                        StreamUpdate::new(
                            id,
                            StreamAction::Reset {
                                code,
                                origin: ResetOrigin::Source,
                            },
                            false,
                        )
                    };
                    update.detached = self.detach_if_complete(id);
                    updates.push(update);
                }
                StreamCallbackEvent::BytesProcessed {
                    id,
                    amount,
                    to_acknowledge,
                } => self.flow_control_updates.push_back(FlowControlUpdate {
                    stream: id,
                    amount,
                    to_acknowledge,
                    window_opened: false,
                }),
                StreamCallbackEvent::WindowOpen(id) => {
                    if let Some(update) = self
                        .flow_control_updates
                        .iter_mut()
                        .rev()
                        .find(|update| update.stream == id && !update.window_opened)
                    {
                        update.window_opened = true;
                    }
                }
            }
        }
        flow_timeout_activity |= self.reap_completed_handler_tasks()?;
        if !self.driver_cleanup_required {
            self.publish_exchange_completions();
        }
        self.callbacks.acknowledge_retirement();
        Ok(ProcessedCallbacks {
            updates,
            flow_timeout_activity,
        })
    }

    pub fn take_flow_control_updates(&mut self) -> Vec<FlowControlUpdate> {
        self.flow_control_updates.drain(..).collect()
    }

    /// Maps a codec stream ID to its protocol-defined sequence number.
    pub fn stream_sequence_number(&self, id: StreamId) -> Result<u64, HttpError> {
        match self.protocol {
            CodecProtocol::Http11 => id
                .get()
                .checked_sub(1)
                .ok_or_else(|| invalid_transition(b"HTTP/1.1 stream identifiers start at one")),
            CodecProtocol::Http2 => Ok(id.get() / 2),
            _ => Err(invalid_transition(b"unsupported session protocol")),
        }
    }

    pub fn stream(&self, id: StreamId) -> Result<&StreamState, HttpError> {
        self.streams.get(&id).ok_or_else(|| unknown_stream(id))
    }

    pub(crate) fn stream_if_present(&self, id: StreamId) -> Option<&StreamState> {
        self.streams.get(&id)
    }

    pub fn response_headers(
        &mut self,
        id: StreamId,
        is_final: bool,
        end_stream: bool,
    ) -> Result<(ResponseState, StreamUpdate), HttpError> {
        let state = self.stream_mut(id)?;
        let was_complete = state.egress_complete();
        let response_state = state.process_response_headers(is_final, end_stream)?;
        let just_finished = !was_complete && state.egress_complete();
        let deferred_stop = just_finished && state.take_deferred_stop_sending();
        self.release_completed_outgoing_capacity(id);
        if just_finished {
            self.shared
                .notify(LifecycleEvent::RequestEnd { stream: id });
        }
        let detached = self.detach_if_complete(id);
        self.shared.resume_lifecycle_panic();
        Ok((
            response_state,
            StreamUpdate::new(
                id,
                if deferred_stop {
                    StreamAction::Reset {
                        code: HttpErrorCode::NO_ERROR,
                        origin: ResetOrigin::Ingress,
                    }
                } else {
                    StreamAction::None
                },
                detached,
            ),
        ))
    }

    pub fn egress_finished(&mut self, id: StreamId) -> Result<StreamUpdate, HttpError> {
        let state = self.stream_mut(id)?;
        let just_finished = state.mark_egress_complete();
        let deferred_stop = just_finished && state.take_deferred_stop_sending();
        self.release_completed_outgoing_capacity(id);
        if just_finished {
            self.shared
                .notify(LifecycleEvent::RequestEnd { stream: id });
        }
        let detached = self.detach_if_complete(id);
        self.shared.resume_lifecycle_panic();
        Ok(StreamUpdate::new(
            id,
            if deferred_stop {
                StreamAction::Reset {
                    code: HttpErrorCode::NO_ERROR,
                    origin: ResetOrigin::Ingress,
                }
            } else {
                StreamAction::None
            },
            detached,
        ))
    }

    pub fn set_error_response_status(
        &mut self,
        id: StreamId,
        status: u16,
    ) -> Result<(), HttpError> {
        let handler_active = self.handlers.contains_key(&id);
        self.stream_mut(id)?
            .set_error_response_status(status, handler_active)?;
        self.cancel_handler(id);
        Ok(())
    }

    /// Installs the error response left pending by `set_error_response_status`
    /// if the request source has already completed. Otherwise
    /// `apply_source_completion` installs it when the source completes. A
    /// source completes only once, so the already-completed case needs this
    /// call.
    pub(crate) fn install_error_response_after_source_complete(
        &mut self,
        id: StreamId,
    ) -> Result<Option<StreamUpdate>, HttpError> {
        if !self.stream(id)?.stream_source_complete() {
            return Ok(None);
        }
        self.install_pending_error_response(id)
    }

    fn install_pending_error_response(
        &mut self,
        id: StreamId,
    ) -> Result<Option<StreamUpdate>, HttpError> {
        if !self.direction.is_downstream()
            || !self.stream(id)?.can_send_headers()
            || self.stream(id)?.error_response_status().is_none()
        {
            return Ok(None);
        }
        self.cancel_handler(id);
        let state = self.stream_mut(id)?;
        let status = state
            .take_error_response_status()
            .expect("the pending error response status was checked");
        let replacement = error_response(status, b"")?;
        let transfer = state.install_egress_source(replacement, true, None)?;
        state.mark_handler_error_response();
        let mut update = StreamUpdate::new(id, StreamAction::SendErrorResponse(status), false);
        update.replacement = Some(transfer);
        Ok(Some(update))
    }

    #[cfg(test)]
    pub(crate) fn set_error_response_status_without_handler_cancel_for_test(
        &mut self,
        id: StreamId,
        status: u16,
    ) -> Result<(), HttpError> {
        self.stream_mut(id)?.set_error_response_status(status, true)
    }

    /// Applies an internal egress reset without reporting a peer reset.
    pub fn reset_stream_state(
        &mut self,
        id: StreamId,
        error: HttpError,
        origin: ResetOrigin,
    ) -> Result<StreamUpdate, HttpError> {
        let code = error.code();
        self.cancel_handler(id);
        let state = self.stream_mut(id)?;
        state.abort_ingress_with_error(error.clone(), error.message());
        state.reset(error);
        let detached = self.detach_if_complete(id);
        Ok(StreamUpdate::new(
            id,
            StreamAction::Reset { code, origin },
            detached,
        ))
    }

    /// Aborts one locally admitted stream without requesting a wire reset.
    pub fn abort_stream_without_wire(
        &mut self,
        id: StreamId,
        error: HttpError,
    ) -> Result<StreamUpdate, HttpError> {
        self.cancel_handler(id);
        let state = self.stream_mut(id)?;
        state.abort_ingress_with_error(error.clone(), b"session stopped servicing the stream");
        state.reset(error);
        let detached = self.detach_if_complete(id);
        Ok(StreamUpdate::new(id, StreamAction::None, detached))
    }

    pub(crate) fn prepare_abort_stream_without_wire(
        &mut self,
        id: StreamId,
        error: HttpError,
    ) -> Result<StreamAbortRetirement, HttpError> {
        let stream = self.stream(id)?;
        let detached = stream.stream_source_complete()
            && stream.egress_task_complete()
            && !self.handlers.contains_key(&id);
        let active = detached.then(|| {
            self.shared
                .active_streams
                .get()
                .checked_sub(1)
                .expect("live stream accounting")
        });
        let outgoing =
            (detached && self.local_streams.contains(&id) && !stream.outgoing_capacity_released())
                .then(|| {
                    self.shared
                        .outgoing_streams
                        .get()
                        .checked_sub(1)
                        .expect("live outgoing stream accounting")
                });
        let exchange = stream.exchange_tracker();
        let shared = Rc::clone(&self.shared);
        let handler = self
            .handlers
            .get(&id)
            .map(|control| (control.generation, Rc::clone(&control.cancellation)));
        let guard = exchange.guard();
        let reset = self
            .streams
            .get_mut(&id)
            .expect("validated stream")
            .prepare_reset_without_wire(error);
        let removed = if let Some(active) = active {
            self.shared.active_streams.set(active);
            self.local_streams.remove(&id);
            if let Some(outgoing) = outgoing {
                self.shared.outgoing_streams.set(outgoing);
            }
            self.mark_ingress_reconciliation_pending();
            self.streams.remove(&id)
        } else {
            None
        };
        Ok(StreamAbortRetirement {
            stream: id,
            handler,
            reset: Some(reset),
            removed,
            detached,
            released_outgoing: outgoing.is_some(),
            deactivate: detached && self.shared.total_transactions() == 0,
            shared,
            exchange,
            guard: Some(guard),
            retired: false,
        })
    }

    /// Reports every peer reset, including unknown and repeated stream IDs,
    /// while mutating only a live stream.
    pub fn peer_reset(
        &mut self,
        id: StreamId,
        code: HttpErrorCode,
    ) -> Result<Option<StreamUpdate>, HttpError> {
        self.shared
            .notify(LifecycleEvent::PeerReset { stream: id, code });
        self.cancel_handler(id);
        let Some(state) = self.streams.get_mut(&id) else {
            self.shared.resume_lifecycle_panic();
            return Ok(None);
        };
        state.abort_ingress(code, b"received reset from peer");
        state.reset_with_egress_error(
            HttpError::with_message(code, b"peer reset the stream"),
            HttpError::with_message(code, b"peer reset cancelled local stream production"),
        );
        let detached = self.detach_if_complete(id);
        self.shared.resume_lifecycle_panic();
        let mut update = StreamUpdate::new(id, StreamAction::None, detached);
        update.cancel_egress = true;
        update.cancel_egress_error = Some(HttpError::with_message(
            code,
            b"peer reset cancelled queued stream work",
        ));
        Ok(Some(update))
    }

    pub fn ingress_transport_eof(&mut self, id: StreamId) -> Result<(), HttpError> {
        let handler_active = self.handlers.contains_key(&id);
        let state = self.stream_mut(id)?;
        if handler_active && state.can_send_headers() {
            state.expect_error_response_status(500);
        }
        state.abort_ingress(HttpErrorCode::TRANSPORT_EOF, b"transport EOF");
        Ok(())
    }

    pub fn fail_connection(&mut self, error: HttpError) -> Vec<StreamId> {
        self.assert_owner();
        self.allow_draining_ingress = false;
        if !self.draining {
            self.draining = true;
            self.drain_trigger = Some(DrainTrigger::ConnectionError);
            self.shared
                .notify(LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError));
        }
        if self.connection_error.is_none() {
            self.connection_error = Some(error.clone());
        }
        self.tasks_mut().cancel();
        self.cancel_all_handlers();
        let ids: Vec<_> = self.streams.keys().copied().collect();
        for id in &ids {
            if let Some(state) = self.streams.get_mut(id) {
                state.abort_ingress_with_error(error.clone(), b"connection error");
                state.reset(error.clone());
            }
            self.detach_if_complete(*id);
        }
        self.shared.resume_lifecycle_panic();
        ids
    }

    pub fn initiate_drain(&mut self) -> bool {
        self.initiate_drain_with(DrainTrigger::Local)
    }

    pub fn initiate_drain_with(&mut self, trigger: DrainTrigger) -> bool {
        if self.draining {
            return false;
        }
        self.draining = true;
        self.drain_trigger = Some(trigger);
        self.shared.notify(LifecycleEvent::DrainStarted(trigger));
        self.shared.resume_lifecycle_panic();
        true
    }

    pub(crate) fn set_draining_ingress_grace(&mut self, allowed: bool) {
        self.assert_owner();
        self.allow_draining_ingress = allowed;
    }

    pub const fn drain_trigger(&self) -> Option<DrainTrigger> {
        self.drain_trigger
    }

    pub const fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn connection_error(&self) -> Option<&HttpError> {
        self.connection_error.as_ref()
    }

    pub fn num_keep_alives(&self) -> u64 {
        self.lifetime
            .as_ref()
            .map_or(0, KeepAliveOwner::num_keep_alives)
    }

    pub fn start_read_write_tasks<R, RF, W, WF>(
        &mut self,
        read: R,
        write: W,
    ) -> Result<(), SessionTaskError>
    where
        R: FnOnce(CancellationToken) -> RF + 'static,
        RF: Future<Output = ()> + 'static,
        W: FnOnce(CancellationToken) -> WF + 'static,
        WF: Future<Output = ()> + 'static,
    {
        self.assert_owner();
        self.tasks_mut().start(read, write)
    }

    pub async fn join_read_write_tasks(&mut self) -> Result<(), SessionTaskError> {
        self.tasks_mut().join().await
    }

    pub async fn cancel_and_join_read_write_tasks(&mut self) -> Result<(), SessionTaskError> {
        self.tasks_mut().cancel_and_join().await
    }

    pub fn pending_task_count(&self) -> usize {
        self.tasks.as_ref().map_or(0, SessionTaskSet::len)
    }

    /// Returns handler task entries that have not yet been reaped.
    pub fn pending_handler_task_count(&self) -> usize {
        self.handler_tasks.as_ref().map_or(0, TaskGroup::len)
    }

    fn tasks_mut(&mut self) -> &mut SessionTaskSet {
        self.tasks
            .get_or_insert_with(|| SessionTaskSet::with_host(self.host.clone()))
    }

    fn handler_tasks_mut(&mut self) -> &mut TaskGroup {
        self.handler_tasks
            .get_or_insert_with(|| TaskGroup::with_host(self.host.clone()))
    }

    /// Reports whether the sibling task set and the handler group exist.
    #[cfg(test)]
    pub(crate) fn task_groups_created_for_test(&self) -> (bool, bool) {
        (self.tasks.is_some(), self.handler_tasks.is_some())
    }

    /// Releases the implicit keep-alive, waits for external contexts, then
    /// emits destruction after every stream, reservation, and task is gone.
    pub async fn finalize(mut self) -> Result<(), SessionTaskError> {
        self.assert_owner();
        let handler_streams: Vec<_> = self.handlers.keys().copied().collect();
        self.cancel_all_handlers();
        for id in handler_streams {
            if let Some(state) = self.streams.get_mut(&id) {
                state.abort_ingress(HttpErrorCode::CORO_CANCELLED, b"session finalized");
                state.reset(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"session finalized with an active handler",
                ));
            }
        }
        let mut task_result = self.join_handler_tasks().await;
        let sibling_result = match self.tasks.as_mut() {
            Some(tasks) if !tasks.is_empty() => tasks.cancel_and_join().await,
            Some(tasks) => {
                tasks.close();
                Ok(())
            }
            None => Ok(()),
        };
        if task_result.is_ok() {
            task_result = sibling_result;
        }
        self.process_pending_events()
            .expect("joined session tasks leave valid completion callbacks");
        assert_eq!(self.shared.pending_reservations.get(), 0);
        assert!(self.streams.is_empty());
        assert!(self.handlers.is_empty());
        let mut exchange_panic = None;
        for (_, exchange) in std::mem::take(&mut self.exchanges).into_values() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(exchange))) {
                if exchange_panic.is_none() {
                    exchange_panic = Some(payload);
                } else {
                    roxygen_runtime::panic::discard_panic(payload);
                }
            }
        }
        let mut lifetime = self
            .lifetime
            .take()
            .expect("session lifetime finalizes exactly once");
        lifetime.finalize().await;
        drop(lifetime);
        self.destroyed = true;
        self.shared.notify(LifecycleEvent::Destroy);
        let lifecycle = catch_unwind(AssertUnwindSafe(|| self.shared.resume_lifecycle_panic()));
        if exchange_panic.is_none() {
            exchange_panic = lifecycle.err();
        } else if let Err(payload) = lifecycle {
            roxygen_runtime::panic::discard_panic(payload);
        }
        if let Some(payload) = exchange_panic {
            resume_unwind(payload);
        }
        task_result
    }

    fn apply_source_completion(
        &mut self,
        id: StreamId,
        error: Option<HttpError>,
    ) -> Result<StreamUpdate, HttpError> {
        let error_code = error.as_ref().map(HttpError::code);
        let already_reset = self
            .stream(id)
            .is_ok_and(|state| state.egress_complete() && state.terminal_error().is_some());
        if !self.stream_mut(id)?.mark_source_complete(error) {
            let detached = self.detach_if_complete(id);
            return Ok(StreamUpdate::new(id, StreamAction::None, detached));
        }
        if already_reset {
            let detached = self.detach_if_complete(id);
            return Ok(StreamUpdate::new(id, StreamAction::None, detached));
        }
        if let Some(update) = self.install_pending_error_response(id)? {
            return Ok(update);
        }
        let action = if let Some(error_code) = error_code {
            if self.stream(id)?.has_handler_error_response() {
                if self.direction.is_downstream()
                    && error_code == HttpErrorCode::NO_ERROR
                    && !self.stream(id)?.ingress_eom_seen()
                    && !self.stream(id)?.egress_complete()
                    && self.protocol == CodecProtocol::Http2
                {
                    self.stream_mut(id)?.set_deferred_stop_sending();
                }
                let detached = self.detach_if_complete(id);
                return Ok(StreamUpdate::new(id, StreamAction::None, detached));
            }
            let downstream = self.direction.is_downstream();
            let handler_active = self.handlers.contains_key(&id);
            if downstream
                && handler_active
                && error_code == HttpErrorCode::READ_TIMEOUT
                && self.stream(id)?.can_send_headers()
            {
                self.stream_mut(id)?.expect_error_response_status(408);
                return Ok(StreamUpdate::new(
                    id,
                    StreamAction::SendErrorResponse(408),
                    false,
                ));
            }
            if !downstream || error_code != HttpErrorCode::NO_ERROR {
                self.cancel_handler(id);
            }
            let protocol = self.protocol;
            let state = self.stream_mut(id)?;
            if downstream
                && state.can_send_headers()
                && (state.error_response_status().is_some()
                    || error_code == HttpErrorCode::READ_TIMEOUT)
            {
                let status = state.take_error_response_status().unwrap_or(408);
                let replacement = error_response(status, b"")?;
                let transfer = state.install_egress_source(replacement, true, None)?;
                state.mark_handler_error_response();
                let mut update =
                    StreamUpdate::new(id, StreamAction::SendErrorResponse(status), false);
                update.replacement = Some(transfer);
                return Ok(update);
            }
            if !state.ingress_eom_seen() || !state.egress_complete() {
                let mapped = source_complete_reset_code(error_code);
                // INTERNAL_ERROR also covers every unmapped code, so those
                // reset upgraded streams too.
                let resets_upgraded = matches!(
                    mapped,
                    HttpErrorCode::FLOW_CONTROL_ERROR
                        | HttpErrorCode::PROTOCOL_ERROR
                        | HttpErrorCode::INTERNAL_ERROR
                );
                if state.is_upgraded() && !resets_upgraded {
                    StreamAction::None
                } else if downstream && mapped == HttpErrorCode::NO_ERROR {
                    if protocol == CodecProtocol::Http2 {
                        state.set_deferred_stop_sending();
                    }
                    StreamAction::None
                } else {
                    let reset_code = if mapped == HttpErrorCode::NO_ERROR {
                        HttpErrorCode::CANCEL
                    } else {
                        mapped
                    };
                    state.reset(HttpError::with_message(reset_code, b"stream source failed"));
                    StreamAction::Reset {
                        code: reset_code,
                        origin: ResetOrigin::Ingress,
                    }
                }
            } else {
                state.reset(HttpError::with_message(
                    error_code,
                    b"stream source completed with an error",
                ));
                StreamAction::None
            }
        } else {
            StreamAction::None
        };
        let detached = self.detach_if_complete(id);
        Ok(StreamUpdate::new(id, action, detached))
    }

    fn validate_new_stream(&self, id: StreamId) -> Result<(), HttpError> {
        if self.streams.contains_key(&id) {
            return Err(invalid_transition(b"duplicate stream identifier"));
        }
        self.stream_sequence_number(id).map(|_| ())
    }

    fn new_stream_state(
        &self,
        id: StreamId,
        head_response: bool,
    ) -> Result<(StreamState, HttpSourceHolder), HttpError> {
        let callbacks = Rc::downgrade(&self.callbacks);
        let exchange = ExchangeOwner::new(
            Rc::new(move || {
                if let Some(callbacks) = callbacks.upgrade() {
                    callbacks.wake_retirement();
                }
            }),
            self.driver_cleanup_required,
        );
        let (mut state, source) = StreamState::new(
            id,
            self.stream_sequence_number(id)?,
            Rc::clone(&self.callbacks),
            self.ingress_receive_window,
            self.egress_write_timeout,
            exchange,
            head_response,
            self.direction.is_downstream() && self.drain_request_body_on_drop,
            self.deliver_ingress_before_abort,
            self.empty_content_length_is_zero,
            self.host.clone(),
        )?;
        if !self.buffered_egress_stats_enabled {
            state.disable_buffered_egress_stats();
        }
        Ok((state, source))
    }

    pub(crate) fn set_buffered_egress_stats_enabled(&mut self, enabled: bool) {
        assert!(self.streams.is_empty());
        self.buffered_egress_stats_enabled = enabled;
    }

    pub(crate) fn require_driver_cleanup(&mut self) {
        assert!(self.exchanges.is_empty());
        self.driver_cleanup_required = true;
    }

    pub(crate) fn exchange_tracker(&self, id: StreamId) -> Option<ExchangeTracker> {
        self.streams
            .get(&id)
            .map(StreamState::exchange_tracker)
            .or_else(|| {
                self.exchanges
                    .values()
                    .rev()
                    .find_map(|(stream, exchange)| (*stream == id).then(|| exchange.tracker()))
            })
    }

    pub(crate) fn detached_exchanges(&self) -> Vec<(StreamId, ExchangeTracker)> {
        // Every live stream has an owner; extra owners outlive removed streams.
        let maximum_detached = self.exchanges.len() - self.streams.len();
        if maximum_detached == 0 {
            return Vec::new();
        }

        self.exchanges
            .values()
            .filter_map(|(id, exchange)| exchange.detached_tracker().map(|tracker| (*id, tracker)))
            .take(maximum_detached)
            .collect()
    }

    pub(crate) fn publish_exchange_completions(&mut self) -> usize {
        let ready = self.take_ready_exchanges();
        let count = ready.len();
        for exchange in ready {
            exchange.publish();
        }
        count
    }

    pub(crate) fn pending_exchange_count(&self) -> usize {
        self.exchanges.len()
    }

    pub(crate) fn take_ready_exchanges(&mut self) -> Vec<ExchangeOwner> {
        let ready: Vec<_> = self
            .exchanges
            .iter()
            .filter_map(|(id, (_, exchange))| exchange.ready().then_some(*id))
            .collect();
        ready
            .into_iter()
            .map(|id| self.exchanges.remove(&id).expect("ready exchange").1)
            .collect()
    }

    fn insert_stream(
        &mut self,
        mut state: StreamState,
        notify_attached: bool,
    ) -> Result<(), HttpError> {
        let active = self
            .shared
            .active_streams
            .get()
            .checked_add(1)
            .ok_or_else(|| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"stream count overflow")
            })?;
        let exchange_id = self.next_exchange_id;
        self.next_exchange_id = exchange_id
            .checked_add(1)
            .ok_or_else(|| invalid_transition(b"exchange identity space exhausted"))?;
        self.exchanges
            .insert(exchange_id, (state.id(), state.take_exchange_owner()));
        self.mark_ingress_reconciliation_pending();
        self.streams.insert(state.id(), state);
        self.shared.active_streams.set(active);
        if notify_attached {
            self.shared.transaction_attached();
        }
        Ok(())
    }

    fn stream_mut(&mut self, id: StreamId) -> Result<&mut StreamState, HttpError> {
        self.streams.get_mut(&id).ok_or_else(|| unknown_stream(id))
    }

    fn release_completed_outgoing_capacity(&mut self, id: StreamId) {
        if !self.direction.is_upstream() || !self.local_streams.contains(&id) {
            return;
        }
        let state = self
            .streams
            .get_mut(&id)
            .expect("validated outgoing stream");
        if state.ingress_eom_seen()
            && state.egress_complete()
            && state.terminal_error().is_none()
            && (state.stream_source_complete() || state.ingress_producer().ingress_eom_succeeded())
            && state.release_outgoing_capacity()
        {
            self.shared.release_outgoing_capacity(id);
        }
    }

    fn detach_if_complete(&mut self, id: StreamId) -> bool {
        let detachable = self
            .streams
            .get(&id)
            .is_some_and(StreamState::is_detachable)
            && !self.handlers.contains_key(&id);
        if !detachable {
            return false;
        }
        self.mark_ingress_reconciliation_pending();
        let removed = self.streams.remove(&id);
        let exchange = removed.as_ref().map(StreamState::exchange_tracker);
        let capacity_released = removed
            .as_ref()
            .is_some_and(StreamState::outgoing_capacity_released);
        let mut first_panic = catch_unwind(AssertUnwindSafe(|| drop(removed))).err();
        if self.local_streams.remove(&id) && !capacity_released {
            self.shared.release_outgoing_capacity(id);
        }
        let active = self.shared.active_streams.get();
        self.shared.active_streams.set(
            active
                .checked_sub(1)
                .expect("a live stream must be accounted"),
        );
        let detached = catch_unwind(AssertUnwindSafe(|| {
            self.shared.transaction_detached(Some(id))
        }));
        if first_panic.is_none() {
            first_panic = detached.err();
        } else if let Err(payload) = detached {
            roxygen_runtime::panic::discard_panic(payload);
        }
        if let Some(exchange) = exchange {
            if first_panic.is_some() {
                exchange.fail();
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| exchange.mark_detached())) {
                if first_panic.is_none() {
                    first_panic = Some(payload);
                } else {
                    roxygen_runtime::panic::discard_panic(payload);
                }
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        true
    }

    fn cancel_handler(&mut self, id: StreamId) -> bool {
        self.handlers
            .get(&id)
            .is_some_and(|control| control.cancellation.request_cancellation())
    }

    #[cfg(test)]
    pub(crate) fn handler_cancellation_for_test(&self, id: StreamId) -> Option<CancellationToken> {
        self.handlers
            .get(&id)
            .map(|control| control.cancellation.token())
    }

    fn cancel_all_handlers(&mut self) {
        for control in self.handlers.values() {
            control.cancellation.request_cancellation();
        }
    }

    fn reap_completed_handler_tasks(&mut self) -> Result<bool, HttpError> {
        let Some(handler_tasks) = self.handler_tasks.as_mut() else {
            return Ok(false);
        };
        let mut reaped = false;
        while let Some(completion) = handler_tasks.try_join_next() {
            reaped = true;
            let retirement = self
                .handler_retirements
                .remove(&completion.id)
                .expect("tracked handler task");
            if let Err(error) = completion.result {
                retirement.fail();
                drop(completion);
                let error = handler_join_error(error);
                drop(retirement);
                return Err(error);
            }
            drop(completion);
            drop(retirement);
        }
        Ok(reaped)
    }

    async fn join_handler_tasks(&mut self) -> Result<(), SessionTaskError> {
        let Some(handler_tasks) = self.handler_tasks.as_mut() else {
            return Ok(());
        };
        handler_tasks.close();
        let mut first_error = None;
        while let Some(completion) = poll_fn(|context| handler_tasks.poll_join_next(context)).await
        {
            let retirement = self
                .handler_retirements
                .remove(&completion.id)
                .expect("tracked handler task");
            if let Err(error) = completion.result {
                retirement.fail();
                first_error.get_or_insert(SessionTaskError::TaskGroup(error));
            }
            drop(completion);
            drop(retirement);
        }
        first_error.map_or(Ok(()), Err)
    }

    fn assert_owner(&self) {
        debug_assert_eq!(current_id(), self.owner_thread);
        debug_assert_eq!(self.worker.owner_thread(), self.owner_thread);
        debug_assert_eq!(
            self.lifetime
                .as_ref()
                .and_then(KeepAliveOwner::owner_worker),
            Some(self.worker.id())
        );
    }
}

impl Drop for SessionCore {
    fn drop(&mut self) {
        if let Some(tasks) = self.tasks.as_mut() {
            tasks.close();
        }
        if let Some(handler_tasks) = self.handler_tasks.as_mut() {
            handler_tasks.close();
        }
        assert_eq!(current_id(), self.owner_thread);
        let tasks_joined = self.tasks.as_ref().is_none_or(SessionTaskSet::is_empty);
        let handler_tasks_joined = self.handler_tasks.as_ref().is_none_or(TaskGroup::is_empty);
        if !tasks_joined || !handler_tasks_joined {
            // Child groups must retire before the session's invariant panic unwinds.
            if let Some(tasks) = self.tasks.as_mut()
                && let Err(payload) = catch_unwind(AssertUnwindSafe(|| tasks.cancel()))
            {
                discard_panic(payload);
            }
            for handler in self.handlers.values() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    handler.cancellation.request_cancellation()
                })) {
                    discard_panic(payload);
                }
            }
            for tasks in [
                self.tasks.as_mut().map(|tasks| &mut tasks.tasks),
                self.handler_tasks.as_mut(),
            ]
            .into_iter()
            .flatten()
            {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| tasks.cancel())) {
                    discard_panic(payload);
                }
                tasks.abort_all();
            }
            if let Some(tasks) = self.tasks.as_mut() {
                while let Some(completion) = tasks.tasks.try_join_next() {
                    completion.discard();
                }
            }
            while let Some(completion) = self
                .handler_tasks
                .as_mut()
                .and_then(TaskGroup::try_join_next)
            {
                let retirement = self
                    .handler_retirements
                    .remove(&completion.id)
                    .expect("tracked handler task");
                retirement.fail();
                completion.discard();
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(retirement))) {
                    discard_panic(payload);
                }
            }
        }
        assert!(tasks_joined, "session tasks must be joined");
        assert!(handler_tasks_joined, "session handler tasks must be joined");
        assert!(self.handlers.is_empty(), "session handlers must be reaped");
        assert_eq!(self.shared.pending_reservations.get(), 0);
        assert!(self.streams.is_empty(), "session streams must be detached");
        assert!(self.destroyed, "session must be explicitly finalized");
        assert!(self.lifetime.is_none());
    }
}

fn error_response(status: u16, body: impl AsRef<[u8]>) -> Result<HttpSourceHolder, HttpError> {
    get_error_response(status, body)
        .map(|source| HttpSourceHolder::new(Box::new(source)))
        .map_err(|error| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
        })
}

fn invalid_reservation() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INTERNAL_ERROR,
        b"reservation does not belong to this session",
    )
}

fn invalid_direction(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

fn invalid_transition(message: &'static [u8]) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

fn unknown_stream(id: StreamId) -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        format!("unknown stream {}", id.get()).as_bytes(),
    )
}

pub(crate) fn source_complete_reset_code(code: HttpErrorCode) -> HttpErrorCode {
    match code {
        HttpErrorCode::NO_ERROR | HttpErrorCode::CANCEL | HttpErrorCode::FLOW_CONTROL_ERROR => code,
        HttpErrorCode::CORO_CANCELLED | HttpErrorCode::READ_TIMEOUT => HttpErrorCode::CANCEL,
        HttpErrorCode::INVALID_STATE_TRANSITION => HttpErrorCode::MESSAGE_ERROR,
        HttpErrorCode::CONTENT_LENGTH_MISMATCH => HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        _ => HttpErrorCode::INTERNAL_ERROR,
    }
}

/// Awaits a request head, then checks `cancellation` before the head read is
/// dropped, so a cancellation requested during the read wins even when the head
/// arrives. The cancellation wait is built only when the head is not ready on a
/// poll. When the head read fails, `retire` is cleared before that check.
async fn await_request_head<R>(
    mut head: R,
    mut retire: Option<&mut HttpSourceHolder>,
    cancellation: CancellationToken,
    message: &'static [u8],
) -> Result<HttpHeaderEvent, HttpError>
where
    R: Future<Output = Result<HttpHeaderEvent, HttpError>> + Unpin,
{
    let mut cancellation_wait: Option<HeadCancellationWait> = None;
    poll_fn(|context| match Pin::new(&mut head).poll(context) {
        Poll::Ready(result) => {
            if result.is_err()
                && let Some(source) = retire.as_deref_mut()
            {
                source.clear(None);
            }
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    message,
                )))
            } else {
                Poll::Ready(result)
            }
        }
        Poll::Pending => {
            let wait = cancellation_wait.get_or_insert_with(|| {
                #[cfg(test)]
                HEAD_CANCELLATION_WAITS.with(|built| built.set(built.get() + 1));
                let baton = CancellableBaton::new();
                HeadCancellationWait {
                    wait: Box::pin(baton.wait(cancellation.clone())),
                    _baton: baton,
                }
            });
            match wait.wait.as_mut().poll(context) {
                Poll::Ready(WaitStatus::Cancelled) => Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    message,
                ))),
                Poll::Ready(_) | Poll::Pending => Poll::Pending,
            }
        }
    })
    .await
}

/// The caller-cancellation wait of a pending request head. The wait drops
/// before its baton.
struct HeadCancellationWait {
    wait: Pin<Box<CancellableWait>>,
    _baton: CancellableBaton,
}

#[cfg(test)]
thread_local! {
    static HEAD_CANCELLATION_WAITS: Cell<usize> = const { Cell::new(0) };
}

/// Counts the caller-cancellation waits that request-head reads built on this thread.
#[cfg(test)]
pub(crate) fn head_cancellation_waits_built_for_test() -> usize {
    HEAD_CANCELLATION_WAITS.with(Cell::get)
}

async fn await_handler_or_cancellation(
    future: LocalHandlerFuture,
    cancellation: CancellationToken,
) -> (Result<HttpSourceHolder, HttpError>, bool) {
    let mut future = Some(future);
    let mut eligible = true;
    let cancellation_baton = CancellableBaton::new();
    let mut cancellation_wait = Box::pin(cancellation_baton.wait(cancellation.clone()));
    let result = poll_fn(|context| {
        let poll = catch_unwind(AssertUnwindSafe(|| {
            future
                .as_mut()
                .expect("handler future remains owned until explicit drop")
                .as_mut()
                .poll(context)
        }));
        match poll {
            Err(_) => {
                eligible = false;
                Poll::Ready(Err(handler_panic_error()))
            }
            Ok(Poll::Ready(result)) => {
                if cancellation.is_cancellation_requested() {
                    eligible = false;
                    Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"request handler cancelled",
                    )))
                } else {
                    Poll::Ready(result)
                }
            }
            Ok(Poll::Pending) => match cancellation_wait.as_mut().poll(context) {
                Poll::Ready(WaitStatus::Cancelled) => {
                    eligible = false;
                    Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"request handler cancelled",
                    )))
                }
                Poll::Ready(_) | Poll::Pending => Poll::Pending,
            },
        }
    })
    .await;
    let drop_result = catch_unwind(AssertUnwindSafe(|| drop(future.take())));
    eligible &= drop_result.is_ok();
    let result = match (result, drop_result) {
        (Err(error), _) => Err(error),
        (Ok(response), Ok(())) => Ok(response),
        (Ok(response), Err(_)) => {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(response)));
            Err(handler_panic_error())
        }
    };
    (result, eligible)
}

async fn invoke_handler_or_cancellation(
    handler: HttpHandlerRef,
    context: HandlerSessionContext,
    request_source: HttpSourceHolder,
    cancellation: CancellationToken,
) -> (Result<HttpSourceHolder, HttpError>, bool) {
    let mut handler = Some(handler);
    let future = catch_unwind(AssertUnwindSafe(|| {
        handler
            .as_ref()
            .expect("handler remains owned through invocation")
            .handle_request(context, request_source)
    }))
    .map_err(|_| handler_panic_error());
    let (result, mut eligible) = match future {
        Ok(future) => await_handler_or_cancellation(future, cancellation).await,
        Err(error) => (Err(error), false),
    };
    let handler_drop = catch_unwind(AssertUnwindSafe(|| drop(handler.take())));
    eligible &= handler_drop.is_ok();
    let result = match (result, handler_drop) {
        (Err(error), _) => Err(error),
        (Ok(response), Ok(())) => Ok(response),
        (Ok(response), Err(_)) => {
            let _ = catch_unwind(AssertUnwindSafe(|| drop(response)));
            Err(handler_panic_error())
        }
    };
    (result, eligible)
}

fn handler_panic_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, b"request handler panicked")
}

fn handler_join_error(error: TaskGroupError) -> HttpError {
    let message: &[u8] = if error == TaskGroupError::TaskCancelled {
        b"request handler task was cancelled"
    } else {
        b"request handler task panicked"
    };
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message)
}

#[cfg(test)]
#[path = "../tests/handler_retirement.rs"]
mod handler_retirement_tests;

#[cfg(test)]
mod response_proposal_tests {
    use std::sync::Arc;
    use std::task::Context;

    use roxygen_core::handler::HttpHandler;
    use roxygen_core::handler::ResponseCounters;
    use tokio::runtime::Builder;
    use tokio::task::JoinSet;
    use tokio::task::LocalSet;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Failure {
        Conversion,
        FutureDrop,
        HandlerDrop,
        Poll,
        Invoke,
        ReadyCancellation,
        PendingCancellation,
        DropCancellation,
    }

    struct ErrorFuture {
        failure: Failure,
        cancellation: Rc<CancellationSource>,
        drops: Rc<Cell<usize>>,
    }

    impl Future for ErrorFuture {
        type Output = Result<HttpSourceHolder, HttpError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            match self.failure {
                Failure::Poll => panic!("handler poll"),
                Failure::ReadyCancellation | Failure::PendingCancellation => {
                    self.cancellation.request_cancellation();
                }
                _ => {}
            }
            if self.failure == Failure::PendingCancellation {
                Poll::Pending
            } else {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"shared response conversion",
                )))
            }
        }
    }

    impl Drop for ErrorFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.failure == Failure::DropCancellation {
                self.cancellation.request_cancellation();
            }
            if self.failure == Failure::FutureDrop {
                panic!("handler future destruction");
            }
        }
    }

    struct ErrorHandler {
        failure: Failure,
        cancellation: Rc<CancellationSource>,
        counters: Arc<ResponseCounters>,
        future_drops: Rc<Cell<usize>>,
        handler_drops: Rc<Cell<usize>>,
    }

    impl HttpHandler for ErrorHandler {
        fn handle_request(
            &self,
            context: HandlerSessionContext,
            _: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            assert!(
                context
                    .response_proposer()
                    .unwrap()
                    .propose(Arc::clone(&self.counters), 200)
            );
            if self.failure == Failure::Invoke {
                panic!("handler construction");
            }
            Box::pin(ErrorFuture {
                failure: self.failure,
                cancellation: Rc::clone(&self.cancellation),
                drops: Rc::clone(&self.future_drops),
            })
        }
    }

    impl Drop for ErrorHandler {
        fn drop(&mut self) {
            self.handler_drops.set(self.handler_drops.get() + 1);
            if self.failure == Failure::HandlerDrop {
                panic!("handler destruction");
            }
        }
    }

    #[test]
    fn proposal_eligibility_keeps_original_error_and_existing_cancellation_point() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async {
                for failure in [
                    Failure::Conversion,
                    Failure::FutureDrop,
                    Failure::HandlerDrop,
                    Failure::Poll,
                    Failure::Invoke,
                    Failure::ReadyCancellation,
                    Failure::PendingCancellation,
                    Failure::DropCancellation,
                ] {
                    let mut owner = KeepAliveOwner::new(());
                    let invocation = Rc::new(HandlerInvocation::new());
                    let proposer = invocation.response_proposer();
                    let context = HandlerSessionContext::acquire(&owner)
                        .unwrap()
                        .with_response_proposer(proposer.clone())
                        .unwrap();
                    let guard = HandlerInvocationGuard(invocation);
                    let cancellation = Rc::new(CancellationSource::new());
                    let counters = Arc::new(ResponseCounters::default());
                    let future_drops = Rc::new(Cell::new(0));
                    let handler_drops = Rc::new(Cell::new(0));
                    let (result, eligible) = invoke_handler_or_cancellation(
                        Rc::new(ErrorHandler {
                            failure,
                            cancellation: Rc::clone(&cancellation),
                            counters: Arc::clone(&counters),
                            future_drops: Rc::clone(&future_drops),
                            handler_drops: Rc::clone(&handler_drops),
                        }),
                        context,
                        HttpSourceHolder::empty(),
                        cancellation.token(),
                    )
                    .await;
                    let error = result.err().unwrap();
                    let expected = match failure {
                        Failure::Poll | Failure::Invoke => HttpErrorCode::INTERNAL_ERROR,
                        Failure::ReadyCancellation | Failure::PendingCancellation => {
                            HttpErrorCode::CORO_CANCELLED
                        }
                        _ => HttpErrorCode::INVALID_STATE_TRANSITION,
                    };
                    assert_eq!(error.code(), expected, "{failure:?}");
                    if expected == HttpErrorCode::INVALID_STATE_TRANSITION {
                        assert_eq!(error.message(), b"shared response conversion");
                    }
                    let accepted =
                        matches!(failure, Failure::Conversion | Failure::DropCancellation);
                    assert_eq!(eligible, accepted, "{failure:?}");
                    if let Some(proposal) = guard.finish(eligible) {
                        proposal.accept();
                    }
                    assert_eq!(
                        counters.snapshot(),
                        if accepted { [1, 0, 1, 0, 0, 0] } else { [0; 6] }
                    );
                    assert!(!proposer.propose(Arc::clone(&counters), 500));
                    assert_eq!(future_drops.get(), usize::from(failure != Failure::Invoke));
                    assert_eq!(handler_drops.get(), 1);
                    drop(guard);
                    owner.finalize().await;
                }
            }));
    }

    #[test]
    fn stale_completion_and_retained_proposer_cannot_reach_reused_stream_generation() {
        use roxygen_core::fixed_source::HttpFixedSource;
        use roxygen_core::handler::HandlerResponseProposer;
        use roxygen_runtime::driver::WorkerDriver;

        struct Handler {
            counters: Arc<ResponseCounters>,
            retained: Rc<RefCell<Vec<HandlerResponseProposer>>>,
            release: Option<Rc<CancellableBaton>>,
        }
        impl HttpHandler for Handler {
            fn handle_request(
                &self,
                context: HandlerSessionContext,
                mut source: HttpSourceHolder,
            ) -> LocalHandlerFuture {
                let proposer = context.response_proposer().unwrap();
                assert!(proposer.propose(Arc::clone(&self.counters), 201));
                self.retained.borrow_mut().push(proposer);
                let release = self.release.clone();
                Box::pin(async move {
                    source.read_header_event().await?;
                    if let Some(release) = release {
                        release.wait(CancellationToken::never()).await;
                    }
                    Ok(HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_fixed_response(201, None).unwrap(),
                    )))
                })
            }
        }
        async fn wait_completion(session: &SessionCore) {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while !session.has_pending_handler_completion() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        async fn complete(session: &mut SessionCore, id: StreamId) {
            let mut updates = session.process_pending_events().unwrap();
            let mut transfer = updates
                .iter_mut()
                .find_map(|update| update.replacement.take())
                .unwrap();
            let event = transfer.next_event().await.unwrap().unwrap();
            let crate::stream_state::EgressEvent::Headers(event) = event else {
                panic!("response header");
            };
            assert_eq!(event.headers().response_status(), Some(201));
            assert!(event.eom());
            session.response_headers(id, true, true).unwrap();
            drop(transfer);
            session.process_pending_events().unwrap();
            assert!(session.stream(id).is_err());
        }

        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async {
                let (worker, _driver) = WorkerDriver::new(8).unwrap();
                let mut session =
                    SessionCore::new(TransportDirection::Downstream, CodecProtocol::Http2, worker)
                        .unwrap();
                let id = StreamId::new(1);
                let retained = Rc::new(RefCell::new(Vec::new()));
                let old = Arc::new(ResponseCounters::default());
                let current = Arc::new(ResponseCounters::default());
                let release = Rc::new(CancellableBaton::new());
                let request = session.begin_incoming_stream(id).unwrap();
                let mut head = HttpMessage::new();
                head.set_method(HttpMethod::Get).unwrap();
                head.set_url(b"/", false).unwrap();
                session.ingress_headers(id, head.clone(), true).unwrap();
                session
                    .start_request_handler(
                        request,
                        Rc::new(Handler {
                            counters: Arc::clone(&old),
                            retained: Rc::clone(&retained),
                            release: None,
                        }),
                    )
                    .unwrap();
                let generation = session.handlers[&id].generation;
                wait_completion(&session).await;
                complete(&mut session, id).await;
                assert_eq!(old.snapshot(), [1, 0, 1, 0, 0, 0]);
                assert!(!retained.borrow()[0].propose(Arc::clone(&current), 500));

                let request = session.begin_incoming_stream(id).unwrap();
                session.ingress_headers(id, head, true).unwrap();
                session
                    .start_request_handler(
                        request,
                        Rc::new(Handler {
                            counters: Arc::clone(&current),
                            retained: Rc::clone(&retained),
                            release: Some(Rc::clone(&release)),
                        }),
                    )
                    .unwrap();
                let new_generation = session.handlers[&id].generation;
                assert_ne!(generation, new_generation);
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    while retained.borrow().len() != 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let stale = Rc::new(HandlerInvocation::new());
                stale.response_proposer().propose(Arc::clone(&current), 500);
                session
                    .callbacks
                    .push_handler_complete(StreamCallbackEvent::HandlerComplete {
                        id,
                        generation,
                        result: Err(HttpError::new(HttpErrorCode::INTERNAL_ERROR)),
                        proposal: stale.seal_response_proposal(),
                    });
                session.process_pending_events().unwrap();
                assert_eq!(session.handlers[&id].generation, new_generation);
                assert_eq!(current.snapshot(), [0; 6]);
                release.signal();
                wait_completion(&session).await;
                complete(&mut session, id).await;
                assert_eq!(current.snapshot(), [1, 0, 1, 0, 0, 0]);
                assert!(!retained.borrow()[1].propose(Arc::clone(&current), 500));
                session.finalize().await.unwrap();
            }));
    }

    #[test]
    fn task_abort_seals_pending_and_never_polled_invocation_while_control_retains_owner() {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(LocalSet::new().run_until(async {
                for poll_first in [false, true] {
                    let control = Rc::new(HandlerInvocation::new());
                    let proposer = control.response_proposer();
                    let counters = Arc::new(ResponseCounters::default());
                    assert!(proposer.propose(Arc::clone(&counters), 200));
                    let guard = HandlerInvocationGuard(Rc::clone(&control));
                    let polled = Rc::new(Cell::new(false));
                    let task_polled = Rc::clone(&polled);
                    let mut tasks = JoinSet::new();
                    let task = tasks.spawn_local(async move {
                        task_polled.set(true);
                        let _token = guard.token();
                        std::future::pending::<()>().await;
                        drop(guard);
                    });
                    if poll_first {
                        tokio::time::timeout(std::time::Duration::from_secs(1), async {
                            while !polled.get() {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                    }
                    task.abort();
                    assert!(tasks.join_next().await.unwrap().unwrap_err().is_cancelled());
                    assert_eq!(polled.get(), poll_first);
                    assert!(control.seal_response_proposal().is_none());
                    assert!(!proposer.propose(Arc::clone(&counters), 500));
                    assert_eq!(counters.snapshot(), [0; 6]);
                }
            }));
    }
}
