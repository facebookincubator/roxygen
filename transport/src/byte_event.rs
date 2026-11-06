//! TCP byte-event delivery and cancellation helpers.

#[cfg(test)]
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::rc::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
#[cfg(test)]
use std::thread::LocalKey;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::FieldSectionInfo;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::ids::StreamId;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::keep_alive::RefCount;
use roxygen_runtime::thread::current_id;
use roxygen_runtime::wait::TimedWait;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerGuard;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;

const MAX_TX_ACK_EVENTS: usize = 96;
const DEFAULT_BYTE_EVENT_TIMEOUT: Duration = Duration::from_secs(10);

type PanicPayload = Box<dyn std::any::Any + Send>;

fn retain_first_panic(first: &mut Option<PanicPayload>, next: Option<PanicPayload>) {
    if first.is_none() {
        *first = next;
    } else if let Some(payload) = next {
        roxygen_runtime::panic::discard_panic(payload);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketByteEventType {
    Write,
    Tx,
    Ack,
}

/// Session and stream offsets attached to one registration boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteEventOffsets {
    pub stream_id: StreamId,
    pub session_byte_offset: u64,
    pub session_bytes_scheduled: u64,
    pub stream_offset: u64,
    pub body_offset: u64,
}

/// Socket timestamp flags required for a write ending at a byte-event boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ByteEventWriteFlags {
    pub timestamp_write: bool,
    pub timestamp_tx: bool,
    pub timestamp_ack: bool,
}

struct SharedRegistrationState {
    registration: Option<HttpByteEventRegistration>,
    deferred_actions: VecDeque<DeferredRegistrationAction>,
    terminal: bool,
    _retirement: Option<Rc<StreamEventRetirement>>,
}

type StreamEventRegistry = Rc<RefCell<BTreeMap<StreamId, Weak<StreamEventRetirement>>>>;

struct StreamEventRetirement {
    stream: StreamId,
    registry: Weak<RefCell<BTreeMap<StreamId, Weak<StreamEventRetirement>>>>,
    waker: RefCell<Option<Waker>>,
}

impl Drop for StreamEventRetirement {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.borrow_mut().remove(&self.stream);
        }
        if let Some(waker) = self.waker.get_mut().take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                if std::thread::panicking() {
                    roxygen_runtime::panic::discard_panic(payload);
                } else {
                    resume_unwind(payload);
                }
            }
        }
    }
}

fn track_stream_event(
    registry: &StreamEventRegistry,
    stream: StreamId,
) -> Rc<StreamEventRetirement> {
    if let Some(existing) = registry.borrow().get(&stream).and_then(Weak::upgrade) {
        return existing;
    }
    let retirement = Rc::new(StreamEventRetirement {
        stream,
        registry: Rc::downgrade(registry),
        waker: RefCell::new(None),
    });
    registry
        .borrow_mut()
        .insert(stream, Rc::downgrade(&retirement));
    retirement
}

enum DeferredRegistrationAction {
    Fire(HttpByteEvent),
    Cancel(HttpError, HttpByteEvent),
}

#[derive(Clone)]
struct SharedRegistration(Rc<RefCell<SharedRegistrationState>>);

impl SharedRegistration {
    fn new(registration: HttpByteEventRegistration) -> Self {
        Self(Rc::new(RefCell::new(SharedRegistrationState {
            registration: Some(registration),
            deferred_actions: VecDeque::new(),
            terminal: false,
            _retirement: None,
        })))
    }

    fn tracked(
        registration: HttpByteEventRegistration,
        registry: &StreamEventRegistry,
        stream: StreamId,
    ) -> Self {
        Self(Rc::new(RefCell::new(SharedRegistrationState {
            registration: Some(registration),
            deferred_actions: VecDeque::new(),
            terminal: false,
            _retirement: Some(track_stream_event(registry, stream)),
        })))
    }

    fn is_pending(&self, event_type: ByteEventType) -> bool {
        let state = self.0.borrow();
        if state.terminal {
            return false;
        }
        state
            .registration
            .as_ref()
            .is_none_or(|registration| registration.pending_events() & event_type.bit() != 0)
    }

    fn fire(&self, event: HttpByteEvent) -> bool {
        self.run_or_defer(DeferredRegistrationAction::Fire(event))
    }

    fn cancel(&self, error: &HttpError, event: &HttpByteEvent) {
        self.run_or_defer(DeferredRegistrationAction::Cancel(
            error.clone(),
            event.clone(),
        ));
    }

    fn run_or_defer(&self, action: DeferredRegistrationAction) -> bool {
        let registration = {
            let mut state = self.0.borrow_mut();
            if state.terminal {
                return false;
            }
            let Some(registration) = state.registration.take() else {
                state.deferred_actions.push_back(action);
                return true;
            };
            registration
        };
        let mut registration = registration;
        let mut action = Some(action);
        let mut first_action_fired = false;
        let mut first = true;
        let mut first_panic = None;
        let mut terminal = false;
        while let Some(next) = action {
            let attempt = match next {
                DeferredRegistrationAction::Fire(event) => {
                    let attempt = catch_unwind(AssertUnwindSafe(|| registration.fire(event)));
                    if first {
                        first_action_fired = matches!(&attempt, Ok(true));
                    }
                    if registration.pending_events() == 0 {
                        terminal = true;
                    }
                    attempt.map(|_| ())
                }
                DeferredRegistrationAction::Cancel(error, event) => {
                    terminal = true;
                    catch_unwind(AssertUnwindSafe(|| {
                        registration.cancel(&error, Some(&event));
                    }))
                }
            };
            retain_first_panic(&mut first_panic, attempt.err());
            first = false;

            let mut state = self.0.borrow_mut();
            if terminal {
                state.terminal = true;
                state.deferred_actions.clear();
                action = None;
            } else {
                action = state.deferred_actions.pop_front();
                if action.is_none() {
                    state.registration = Some(registration);
                    break;
                }
            }
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        first_action_fired
    }
}

/// One ordered transport byte event awaiting a byte offset or timeout.
pub struct PendingByteEvent {
    session_offset: u64,
    event: HttpByteEvent,
    registration: SharedRegistration,
    timeout: Option<LocalFuture>,
    ref_count: Option<Rc<RefCell<RefCount>>>,
    complete: bool,
}

impl PendingByteEvent {
    /// Creates one pending event from a matching single-kind registration.
    ///
    /// Use [`HttpByteEventRegistration::into_event_registrations`] before this
    /// constructor when the original registration requests multiple kinds.
    pub fn new(
        session_offset: u64,
        event: HttpByteEvent,
        registration: HttpByteEventRegistration,
    ) -> Self {
        assert_eq!(
            registration.pending_events(),
            event.event_type.bit(),
            "a pending byte event requires one matching registration kind",
        );
        Self::from_shared(session_offset, event, SharedRegistration::new(registration))
    }

    fn from_shared(
        session_offset: u64,
        event: HttpByteEvent,
        registration: SharedRegistration,
    ) -> Self {
        Self {
            session_offset,
            event,
            registration,
            timeout: None,
            ref_count: None,
            complete: false,
        }
    }

    pub const fn session_offset(&self) -> u64 {
        self.session_offset
    }

    pub fn event(&self) -> &HttpByteEvent {
        &self.event
    }

    fn arm_timeout(&mut self, host: &Host, timeout: Duration, ref_count: Rc<RefCell<RefCount>>) {
        let sleep = host.sleep_until(host.deadline_after(timeout));
        ref_count.borrow_mut().inc_ref();
        self.timeout = Some(sleep);
        self.ref_count = Some(ref_count);
    }

    fn timeout_is_ready(&mut self, context: &mut Context<'_>) -> bool {
        self.timeout
            .as_mut()
            .is_some_and(|timeout| timeout.as_mut().poll(context).is_ready())
    }

    fn is_pending(&self) -> bool {
        !self.complete && self.registration.is_pending(self.event.event_type)
    }

    fn release_ref(&mut self) {
        if let Some(ref_count) = self.ref_count.take() {
            ref_count
                .borrow_mut()
                .dec_ref()
                .expect("a scheduled byte event owns one reference");
        }
    }

    fn discard(&mut self) {
        self.complete = true;
        self.timeout = None;
        self.release_ref();
    }

    pub fn fire(&mut self) -> bool {
        if !self.is_pending() {
            self.discard();
            return false;
        }
        self.complete = true;
        self.timeout = None;
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.registration.fire(self.event.clone())
        }));
        self.release_ref();
        match result {
            Ok(fired) => fired,
            Err(payload) => resume_unwind(payload),
        }
    }

    pub fn cancel(&mut self, error: &HttpError) {
        if !self.is_pending() {
            self.discard();
            return;
        }
        self.complete = true;
        self.timeout = None;
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.registration.cancel(error, &self.event);
        }));
        self.release_ref();
        if let Err(payload) = result {
            resume_unwind(payload);
        }
    }

    pub fn fire_events(events: &mut VecDeque<Self>, offset: u64) -> usize {
        let mut ready = Vec::new();
        while events
            .front()
            .is_some_and(|event| event.session_offset <= offset)
        {
            ready.push(events.pop_front().expect("the front event is present"));
        }
        run_pending(ready, |event| event.fire())
    }

    pub fn cancel_events(events: &mut VecDeque<Self>, error: &HttpError) {
        let pending = events.drain(..).collect::<Vec<_>>();
        run_pending(pending, |event| {
            event.cancel(error);
            true
        });
    }
}

impl Drop for PendingByteEvent {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let already_panicking = std::thread::panicking();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.cancel(&cancelled_error());
        }));
        if !already_panicking {
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        } else if let Err(payload) = result {
            roxygen_runtime::panic::discard_panic(payload);
        }
    }
}

fn run_pending(
    pending: Vec<PendingByteEvent>,
    mut action: impl FnMut(&mut PendingByteEvent) -> bool,
) -> usize {
    let mut completed = 0;
    let mut first_panic = None;
    for mut event in pending {
        let attempt = catch_unwind(AssertUnwindSafe(|| action(&mut event)));
        match attempt {
            Ok(true) => completed += 1,
            Ok(false) => {}
            Err(payload) if first_panic.is_none() => first_panic = Some(payload),
            Err(payload) => roxygen_runtime::panic::discard_panic(payload),
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(event))) {
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
    completed
}

struct RegistrationAndEvent {
    registration: SharedRegistration,
    event: HttpByteEvent,
}

enum RegistrationAction {
    Fire(PendingByteEvent),
    Cancel(RegistrationAndEvent, HttpError),
}

struct TxAckBoundary {
    session_byte_offset: u64,
    events: Vec<RegistrationAndEvent>,
    event_count: usize,
    complete: bool,
}

impl TxAckBoundary {
    fn write_flags(&self) -> ByteEventWriteFlags {
        let mut flags = ByteEventWriteFlags::default();
        for entry in &self.events {
            flags.timestamp_tx |= entry.registration.is_pending(ByteEventType::NicTx);
            flags.timestamp_ack |= entry.registration.is_pending(ByteEventType::CumulativeAck);
        }
        flags.timestamp_write = flags.timestamp_tx || flags.timestamp_ack;
        flags
    }

    fn cancel(mut self, error: &HttpError) {
        self.complete = true;
        let events = std::mem::take(&mut self.events);
        cancel_registration_entries(&events, error);
    }

    fn take_events(mut self) -> Vec<RegistrationAndEvent> {
        self.complete = true;
        std::mem::take(&mut self.events)
    }

    fn take_kind(&mut self, event_type: ByteEventType) -> Vec<RegistrationAndEvent> {
        let mut matching = Vec::new();
        let mut retained = Vec::with_capacity(self.events.len());
        for entry in std::mem::take(&mut self.events) {
            if entry.event.event_type == event_type {
                matching.push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.events = retained;
        self.event_count = self
            .event_count
            .checked_sub(matching.len())
            .expect("TX/ACK boundary accounting cannot underflow");
        matching
    }

    fn take_stream(&mut self, stream: StreamId) -> Vec<RegistrationAndEvent> {
        let (matching, retained): (Vec<_>, Vec<_>) = std::mem::take(&mut self.events)
            .into_iter()
            .partition(|entry| entry.event.stream_id == Some(stream));
        self.events = retained;
        self.event_count = self
            .event_count
            .checked_sub(matching.len())
            .expect("TX/ACK boundary accounting cannot underflow");
        matching
    }

    fn event_count(&self) -> usize {
        self.event_count
    }
}

impl Drop for TxAckBoundary {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let already_panicking = std::thread::panicking();
        let error = cancelled_error();
        let result = catch_unwind(AssertUnwindSafe(|| {
            cancel_registration_entries(&self.events, &error);
        }));
        if !already_panicking {
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        } else if let Err(payload) = result {
            roxygen_runtime::panic::discard_panic(payload);
        }
    }
}

/// Owner-local token for one dequeued TX/ACK write boundary.
pub struct TxAckEvent {
    state: Weak<RefCell<ObserverState>>,
    id: u64,
    session_byte_offset: u64,
    complete: bool,
}

impl TxAckEvent {
    pub const fn session_byte_offset(&self) -> u64 {
        self.session_byte_offset
    }

    pub fn write_flags(&self) -> ByteEventWriteFlags {
        let Some(state) = self.state.upgrade() else {
            return ByteEventWriteFlags::default();
        };
        state
            .borrow()
            .in_flight_tx_ack_events
            .get(&self.id)
            .map_or_else(ByteEventWriteFlags::default, TxAckBoundary::write_flags)
    }

    pub fn cancel(mut self, error: &HttpError) {
        let boundary = self.take_boundary();
        self.complete = true;
        if let Some(boundary) = boundary {
            boundary.cancel(error);
        }
    }

    fn take_boundary(&self) -> Option<TxAckBoundary> {
        let state = self.state.upgrade()?;
        let mut state = state.borrow_mut();
        state.assert_owner();
        state.in_flight_tx_ack_events.remove(&self.id)
    }
}

impl Drop for TxAckEvent {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let already_panicking = std::thread::panicking();
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(boundary) = self.take_boundary() {
                boundary.cancel(&cancelled_error());
            }
        }));
        self.complete = true;
        if !already_panicking {
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        } else if let Err(payload) = result {
            roxygen_runtime::panic::discard_panic(payload);
        }
    }
}

struct ObserverState {
    host: Host,
    owner_thread: ThreadId,
    closed: bool,
    tx_ack_enabled: bool,
    byte_event_timeout: Duration,
    maximum_transport_write_offset: u64,
    maximum_transport_tx_offset: u64,
    maximum_transport_ack_offset: u64,
    ref_count: Rc<RefCell<RefCount>>,
    transport_write_events: VecDeque<PendingByteEvent>,
    kernel_write_events: VecDeque<PendingByteEvent>,
    tx_ack_events: VecDeque<TxAckBoundary>,
    in_flight_tx_ack_events: BTreeMap<u64, TxAckBoundary>,
    next_in_flight_tx_ack_id: u64,
    tx_events: VecDeque<PendingByteEvent>,
    ack_events: VecDeque<PendingByteEvent>,
    pending_tx_ack_events: usize,
    stream_retirements: StreamEventRegistry,
}

impl ObserverState {
    fn new(host: Host) -> Self {
        Self {
            owner_thread: host.owner_thread(),
            host,
            closed: false,
            tx_ack_enabled: false,
            byte_event_timeout: DEFAULT_BYTE_EVENT_TIMEOUT,
            maximum_transport_write_offset: 0,
            maximum_transport_tx_offset: 0,
            maximum_transport_ack_offset: 0,
            ref_count: Rc::new(RefCell::new(RefCount::default())),
            transport_write_events: VecDeque::new(),
            kernel_write_events: VecDeque::new(),
            tx_ack_events: VecDeque::new(),
            in_flight_tx_ack_events: BTreeMap::new(),
            next_in_flight_tx_ack_id: 0,
            tx_events: VecDeque::new(),
            ack_events: VecDeque::new(),
            pending_tx_ack_events: 0,
            stream_retirements: Rc::new(RefCell::new(BTreeMap::new())),
        }
    }

    fn assert_owner(&self) {
        assert_eq!(current_id(), self.owner_thread);
    }

    fn can_register(&self, event_count: usize) -> bool {
        self.tx_ack_enabled
            && event_count != 0
            && self.pending_tx_ack_events
                + self
                    .in_flight_tx_ack_events
                    .values()
                    .map(TxAckBoundary::event_count)
                    .sum::<usize>()
                + self.tx_events.len()
                + self.ack_events.len()
                + event_count
                <= MAX_TX_ACK_EVENTS
    }

    /// Whether every byte-event container is empty. Stale, timeout-armed
    /// entries and zero-count in-flight boundaries still count, because only
    /// their cancellation releases them.
    fn has_no_byte_events(&self) -> bool {
        let Self {
            host: _,
            owner_thread: _,
            closed: _,
            tx_ack_enabled: _,
            byte_event_timeout: _,
            maximum_transport_write_offset: _,
            maximum_transport_tx_offset: _,
            maximum_transport_ack_offset: _,
            ref_count: _,
            transport_write_events,
            kernel_write_events,
            tx_ack_events,
            in_flight_tx_ack_events,
            next_in_flight_tx_ack_id: _,
            tx_events,
            ack_events,
            pending_tx_ack_events: _,
            stream_retirements: _,
        } = self;
        transport_write_events.is_empty()
            && kernel_write_events.is_empty()
            && tx_ack_events.is_empty()
            && in_flight_tx_ack_events.is_empty()
            && tx_events.is_empty()
            && ack_events.is_empty()
    }
}

#[cfg(test)]
std::thread_local! {
    static OBSERVER_CLEANUP_REGISTRATIONS: Cell<usize> = const { Cell::new(0) };
    static OBSERVER_CANCELLATION_DRAINS: Cell<usize> = const { Cell::new(0) };
    static TIMEOUT_QUEUE_TAKES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn record_test_count(counter: &'static LocalKey<Cell<usize>>) {
    counter.with(|count| count.set(count.get() + 1));
}

/// Worker cleanup registrations that observers made on this thread.
#[cfg(test)]
pub(crate) fn observer_cleanup_registrations_for_test() -> usize {
    OBSERVER_CLEANUP_REGISTRATIONS.with(Cell::get)
}

/// Observer closes on this thread that reached `cancel_observer_events`.
#[cfg(test)]
pub(crate) fn observer_cancellation_drains_for_test() -> usize {
    OBSERVER_CANCELLATION_DRAINS.with(Cell::get)
}

/// `take_timeout_events` calls on this thread.
#[cfg(test)]
pub(crate) fn timeout_queue_takes_for_test() -> usize {
    TIMEOUT_QUEUE_TAKES.with(Cell::get)
}

/// Owner-worker TCP byte-event observer used by H1 and H2 sessions.
pub struct AsyncSocketByteEventObserver {
    state: Rc<RefCell<ObserverState>>,
    worker: WorkerHandle,
    /// Registered with the first live byte event, so that worker shutdown
    /// cancels the queued events with `DROPPED`.
    cleanup_registration: RefCell<Option<WorkerCleanupRegistration>>,
    _worker_guard: WorkerGuard,
}

impl AsyncSocketByteEventObserver {
    pub fn new(context: &WorkerContext) -> Result<Self, HttpError> {
        if !context.is_owner_thread() || context.handle().state() != WorkerState::Open {
            return Err(observer_closed_error());
        }
        Ok(Self {
            state: Rc::new(RefCell::new(ObserverState::new(context.host().clone()))),
            worker: context.handle().clone(),
            cleanup_registration: RefCell::new(None),
            _worker_guard: context.acquire_guard(),
        })
    }

    fn ensure_cleanup_registered(&self, context: &WorkerContext) -> Result<(), HttpError> {
        if self.cleanup_registration.borrow().is_some() {
            return Ok(());
        }
        let cleanup_state = Rc::clone(&self.state);
        let registration = context
            .register_cleanup(move || close_observer(&cleanup_state, dropped_error))
            .map_err(|error| match error {
                CleanupRegistrationError::Closed => observer_closed_error(),
                error => HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    error.to_string().as_bytes(),
                ),
            })?;
        #[cfg(test)]
        record_test_count(&OBSERVER_CLEANUP_REGISTRATIONS);
        *self.cleanup_registration.borrow_mut() = Some(registration);
        Ok(())
    }

    pub fn set_byte_event_timeout(
        &self,
        context: &WorkerContext,
        timeout: Duration,
    ) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.state.borrow_mut().byte_event_timeout = timeout;
        Ok(())
    }

    /// Whether the observer is still registered with its worker. An observer
    /// without byte events holds no worker cleanup, so it reports `false` once
    /// the worker leaves `Open`, rather than once the shutdown drain closes it.
    pub fn is_registered(&self) -> bool {
        !self.state.borrow().closed
            && (self.cleanup_registration.borrow().is_some()
                || self.worker.state() == WorkerState::Open)
    }

    pub fn set_tx_ack_events_enabled(
        &self,
        context: &WorkerContext,
        enabled: bool,
    ) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.state.borrow_mut().tx_ack_enabled = enabled;
        Ok(())
    }

    pub fn register_byte_events(
        &self,
        context: &WorkerContext,
        offsets: ByteEventOffsets,
        field_section_info: Option<FieldSectionInfo>,
        registrations: Vec<HttpByteEventRegistration>,
        eom: bool,
    ) -> Result<(), HttpError> {
        self.check_context(context)?;
        self.register_checked_byte_events(context, offsets, field_section_info, registrations, eom)
    }

    /// Whether this observer holds its worker cleanup registration.
    #[cfg(test)]
    pub(crate) fn has_cleanup_registration_for_test(&self) -> bool {
        self.cleanup_registration.borrow().is_some()
    }

    /// `register_byte_events` after its context check has passed, as when the
    /// worker begins shutdown from another thread just after that check.
    #[cfg(test)]
    pub(crate) fn register_byte_events_after_context_check_for_test(
        &self,
        context: &WorkerContext,
        offsets: ByteEventOffsets,
        field_section_info: Option<FieldSectionInfo>,
        registrations: Vec<HttpByteEventRegistration>,
        eom: bool,
    ) -> Result<(), HttpError> {
        self.register_checked_byte_events(context, offsets, field_section_info, registrations, eom)
    }

    /// Cancels the registration of the first queued TX or ACK event while the
    /// event stays queued with its timeout armed, leaving a stale entry.
    #[cfg(test)]
    pub(crate) fn cancel_queued_socket_registration_for_test(
        &self,
        event_type: SocketByteEventType,
        error: &HttpError,
    ) {
        let (registration, event) = {
            let state = self.state.borrow();
            let events = match event_type {
                SocketByteEventType::Tx => &state.tx_events,
                SocketByteEventType::Ack => &state.ack_events,
                SocketByteEventType::Write => unreachable!("write events have no registration"),
            };
            let pending = events.front().expect("a queued socket event");
            (pending.registration.clone(), pending.event.clone())
        };
        registration.cancel(error, &event);
    }

    fn register_checked_byte_events(
        &self,
        context: &WorkerContext,
        offsets: ByteEventOffsets,
        field_section_info: Option<FieldSectionInfo>,
        registrations: Vec<HttpByteEventRegistration>,
        eom: bool,
    ) -> Result<(), HttpError> {
        if registrations.iter().any(|registration| {
            registration.pending_events() != 0 && registration.callback_is_alive()
        }) {
            self.ensure_cleanup_registered(context)?;
        }
        let mut actions = Vec::new();
        {
            let mut state = self.state.borrow_mut();
            for registration in registrations {
                if registration.pending_events() == 0 || !registration.callback_is_alive() {
                    continue;
                }
                let event_bits = registration.pending_events();
                let tx_ack_count = usize::from(event_bits & ByteEventType::NicTx.bit() != 0)
                    + usize::from(event_bits & ByteEventType::CumulativeAck.bit() != 0);
                let accept_tx_ack = state.can_register(tx_ack_count);
                let base_event = HttpByteEvent {
                    stream_id: registration.stream_id().or(Some(offsets.stream_id)),
                    field_section_info,
                    body_offset: offsets.body_offset,
                    transport_offset: offsets.session_byte_offset,
                    stream_offset: offsets.stream_offset,
                    eom,
                    event_type: ByteEventType::TransportWrite,
                };
                for registration in registration.into_event_registrations() {
                    let event_type = ByteEventType::ALL
                        .into_iter()
                        .find(|event_type| registration.pending_events() == event_type.bit())
                        .expect("a split registration owns one known event kind");
                    let mut event = base_event.clone();
                    event.event_type = event_type;
                    let registration = SharedRegistration::tracked(
                        registration,
                        &state.stream_retirements,
                        base_event.stream_id.expect("registered stream"),
                    );
                    match event_type {
                        ByteEventType::TransportWrite => {
                            state
                                .transport_write_events
                                .push_back(PendingByteEvent::from_shared(
                                    offsets.session_byte_offset,
                                    event,
                                    registration,
                                ));
                        }
                        ByteEventType::KernelWrite => {
                            state
                                .kernel_write_events
                                .push_back(PendingByteEvent::from_shared(
                                    offsets.session_byte_offset,
                                    event,
                                    registration,
                                ));
                        }
                        ByteEventType::NicTx | ByteEventType::CumulativeAck => {
                            let entry = RegistrationAndEvent {
                                registration,
                                event,
                            };
                            if !accept_tx_ack {
                                let message: &[u8] = if state.tx_ack_enabled {
                                    b"Too many TX/ACK Events registered"
                                } else {
                                    b"TX/ACK Events disabled"
                                };
                                actions.push(RegistrationAction::Cancel(
                                    entry,
                                    HttpError::with_message(HttpErrorCode::CANCEL, message),
                                ));
                            } else if offsets.session_byte_offset == offsets.session_bytes_scheduled
                            {
                                actions.extend(
                                    schedule_tx_ack(&mut state, entry)
                                        .into_iter()
                                        .map(RegistrationAction::Fire),
                                );
                            } else {
                                if state.tx_ack_events.back().is_none_or(|event| {
                                    event.session_byte_offset != offsets.session_byte_offset
                                }) {
                                    state.tx_ack_events.push_back(TxAckBoundary {
                                        session_byte_offset: offsets.session_byte_offset,
                                        events: Vec::new(),
                                        event_count: 0,
                                        complete: false,
                                    });
                                }
                                let boundary = state
                                    .tx_ack_events
                                    .back_mut()
                                    .expect("a TX/ACK boundary was just inserted");
                                boundary.events.push(entry);
                                boundary.event_count += 1;
                                state.pending_tx_ack_events += 1;
                            }
                        }
                    }
                }
            }
        }
        run_registration_actions(actions);
        Ok(())
    }

    pub fn transport_write(
        &self,
        context: &WorkerContext,
        session_written_offset: u64,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let ready = {
            let mut state = self.state.borrow_mut();
            if !has_ready(&state.transport_write_events, session_written_offset) {
                return Ok(0);
            }
            take_ready(&mut state.transport_write_events, session_written_offset)
        };
        Ok(run_pending(ready, |event| event.fire()))
    }

    pub fn next_tx_ack_event(
        &self,
        context: &WorkerContext,
    ) -> Result<Option<TxAckEvent>, HttpError> {
        self.check_context(context)?;
        let mut state = self.state.borrow_mut();
        let Some(boundary) = state.tx_ack_events.pop_front() else {
            return Ok(None);
        };
        state.pending_tx_ack_events = state
            .pending_tx_ack_events
            .checked_sub(boundary.event_count())
            .expect("queued TX/ACK accounting cannot underflow");
        let id = state.next_in_flight_tx_ack_id;
        state.next_in_flight_tx_ack_id = id
            .checked_add(1)
            .expect("in-flight TX/ACK identifiers cannot be exhausted");
        let session_byte_offset = boundary.session_byte_offset;
        assert!(state.in_flight_tx_ack_events.insert(id, boundary).is_none());
        Ok(Some(TxAckEvent {
            state: Rc::downgrade(&self.state),
            id,
            session_byte_offset,
            complete: false,
        }))
    }

    pub fn transport_write_complete(
        &self,
        context: &WorkerContext,
        session_written_offset: u64,
        tx_ack_event: Option<TxAckEvent>,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        if tx_ack_event
            .as_ref()
            .is_some_and(|event| event.state.as_ptr() != Rc::as_ptr(&self.state))
        {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"TX/ACK boundary belongs to a different observer",
            ));
        }
        let mut ready = {
            let mut state = self.state.borrow_mut();
            if tx_ack_event.is_none()
                && !has_ready(&state.kernel_write_events, session_written_offset)
            {
                return Ok(0);
            }
            take_ready(&mut state.kernel_write_events, session_written_offset)
        };
        if let Some(mut tx_ack_event) = tx_ack_event {
            let entries = tx_ack_event
                .take_boundary()
                .map(TxAckBoundary::take_events)
                .unwrap_or_default();
            tx_ack_event.complete = true;
            let mut state = self.state.borrow_mut();
            for entry in entries {
                ready.extend(schedule_tx_ack(&mut state, entry));
            }
        }
        Ok(run_pending(ready, |event| event.fire()))
    }

    pub fn socket_byte_event(
        &self,
        context: &WorkerContext,
        event_type: SocketByteEventType,
        offset: u64,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let ready = {
            let mut state = self.state.borrow_mut();
            match event_type {
                SocketByteEventType::Write => {
                    state.maximum_transport_write_offset = offset;
                    return Ok(0);
                }
                SocketByteEventType::Tx => {
                    state.maximum_transport_tx_offset = offset;
                    take_ready(&mut state.tx_events, offset)
                }
                SocketByteEventType::Ack => {
                    state.maximum_transport_ack_offset = offset;
                    take_ready(&mut state.ack_events, offset)
                }
            }
        };
        Ok(run_pending(ready, |event| event.fire()))
    }

    /// Cancels one socket TX or ACK kind at an exact session write boundary.
    ///
    /// The cancellation follows the registration through queued, dequeued,
    /// and timeout-armed ownership. Other kinds and boundaries remain pending.
    /// `Write` has no user-visible registration and therefore cancels zero
    /// events. The returned value is the number of registrations cancelled.
    pub fn cancel_socket_byte_event(
        &self,
        context: &WorkerContext,
        event_type: SocketByteEventType,
        session_byte_offset: u64,
        error: &HttpError,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let Some(http_event_type) = (match event_type {
            SocketByteEventType::Write => None,
            SocketByteEventType::Tx => Some(ByteEventType::NicTx),
            SocketByteEventType::Ack => Some(ByteEventType::CumulativeAck),
        }) else {
            return Ok(0);
        };
        let (entries, pending) = {
            let mut state = self.state.borrow_mut();
            let mut entries = take_boundary_registrations(
                &mut state.tx_ack_events,
                session_byte_offset,
                http_event_type,
            );
            state.pending_tx_ack_events = state
                .pending_tx_ack_events
                .checked_sub(entries.len())
                .expect("queued TX/ACK accounting cannot underflow");
            for boundary in state.in_flight_tx_ack_events.values_mut() {
                if boundary.session_byte_offset == session_byte_offset {
                    entries.extend(boundary.take_kind(http_event_type));
                }
            }
            let pending = match event_type {
                SocketByteEventType::Tx => take_boundary_pending(
                    &mut state.tx_events,
                    session_byte_offset,
                    http_event_type,
                ),
                SocketByteEventType::Ack => take_boundary_pending(
                    &mut state.ack_events,
                    session_byte_offset,
                    http_event_type,
                ),
                SocketByteEventType::Write => unreachable!(),
            };
            (entries, pending)
        };
        let count = entries.len() + pending.len();
        let mut first_panic = None;
        for result in [
            catch_unwind(AssertUnwindSafe(|| {
                cancel_registration_entries(&entries, error)
            })),
            catch_unwind(AssertUnwindSafe(|| cancel_pending(pending, error))),
        ] {
            retain_first_panic(&mut first_panic, result.err());
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        Ok(count)
    }

    /// Cancels every pending byte event owned by one HTTP stream.
    ///
    /// Registrations are followed through queued write/kernel events, queued
    /// or in-flight TX/ACK boundaries, and timeout-armed socket events. Events
    /// belonging to other streams retain their position and timeout state.
    pub fn cancel_stream_events(
        &self,
        context: &WorkerContext,
        stream: StreamId,
        error: &HttpError,
    ) -> Result<usize, HttpError> {
        self.check_cleanup_context(context)?;
        let (entries, pending) = {
            let mut state = self.state.borrow_mut();
            let mut pending = take_stream_pending(&mut state.transport_write_events, stream);
            pending.extend(take_stream_pending(&mut state.kernel_write_events, stream));

            let mut entries = take_stream_registrations(&mut state.tx_ack_events, stream);
            state.pending_tx_ack_events = state
                .pending_tx_ack_events
                .checked_sub(entries.len())
                .expect("queued TX/ACK accounting cannot underflow");
            entries.extend(
                state
                    .in_flight_tx_ack_events
                    .values_mut()
                    .flat_map(|boundary| boundary.take_stream(stream)),
            );
            state.in_flight_tx_ack_events.retain(|_, boundary| {
                let retained = boundary.event_count() != 0;
                if !retained {
                    boundary.complete = true;
                }
                retained
            });

            pending.extend(take_stream_pending(&mut state.tx_events, stream));
            pending.extend(take_stream_pending(&mut state.ack_events, stream));
            (entries, pending)
        };
        let count = entries.len() + pending.len();
        let mut first_panic = None;
        for result in [
            catch_unwind(AssertUnwindSafe(|| {
                cancel_registration_entries(&entries, error)
            })),
            catch_unwind(AssertUnwindSafe(|| cancel_pending(pending, error))),
        ] {
            retain_first_panic(&mut first_panic, result.err());
        }
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
        Ok(count)
    }

    pub fn poll_timeouts(
        &self,
        context: &WorkerContext,
        task_context: &mut Context<'_>,
    ) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let (stale, expired) = {
            let mut state = self.state.borrow_mut();
            if state.tx_events.is_empty() && state.ack_events.is_empty() {
                return Ok(0);
            }
            let (mut stale, mut expired) = take_timeout_events(&mut state.tx_events, task_context);
            let (ack_stale, ack_expired) = take_timeout_events(&mut state.ack_events, task_context);
            stale.extend(ack_stale);
            expired.extend(ack_expired);
            (stale, expired)
        };
        for mut event in stale {
            event.discard();
        }
        let count = expired.len();
        if count != 0 {
            cancel_pending(expired, &timeout_error());
        }
        Ok(count)
    }

    pub fn cancel_events(
        &self,
        context: &WorkerContext,
        error: &HttpError,
    ) -> Result<(), HttpError> {
        self.check_cleanup_context(context)?;
        cancel_observer_events(&self.state, error);
        Ok(())
    }

    /// Waits for timeout-reference retirement on the owner, including during shutdown.
    /// Only one waiter may be registered at a time.
    pub fn zero_refs(&self, context: &WorkerContext) -> Result<TimedWait, HttpError> {
        self.check_cleanup_context(context)?;
        let state = self.state.borrow();
        let wait = state.ref_count.borrow().zero_refs();
        Ok(wait)
    }

    /// Observes retirement of registrations, executing callbacks and timeout refs
    /// for one stream. The caller must first stop producing new registrations.
    /// One driver waiter is supported per stream; this never cancels byte events.
    pub fn poll_stream_events_drained(
        &self,
        context: &WorkerContext,
        stream: StreamId,
        task_context: &mut Context<'_>,
    ) -> Poll<Result<(), HttpError>> {
        if !context.is_owner_thread() || !self.worker.same_worker(context.handle()) {
            return Poll::Ready(Err(observer_closed_error()));
        }
        let registry = Rc::clone(&self.state.borrow().stream_retirements);
        let retirement = registry.borrow().get(&stream).and_then(Weak::upgrade);
        let Some(retirement) = retirement else {
            return Poll::Ready(Ok(()));
        };
        let replacement = task_context.waker().clone();
        let previous = retirement.waker.borrow_mut().replace(replacement);
        drop(previous);
        Poll::Pending
    }

    pub fn pending_timeout_refs(&self, context: &WorkerContext) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let state = self.state.borrow();
        let count = state.ref_count.borrow().count();
        Ok(count)
    }

    pub fn pending_events(&self, context: &WorkerContext) -> Result<usize, HttpError> {
        self.check_context(context)?;
        let state = self.state.borrow();
        Ok(state.transport_write_events.len()
            + state.kernel_write_events.len()
            + state
                .tx_ack_events
                .iter()
                .map(|event| event.event_count())
                .sum::<usize>()
            + state
                .in_flight_tx_ack_events
                .values()
                .map(TxAckBoundary::event_count)
                .sum::<usize>()
            + state.tx_events.len()
            + state.ack_events.len())
    }

    fn check_cleanup_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow().assert_owner();
        if !self.worker.same_worker(context.handle()) || !context.is_owner_thread() {
            return Err(wrong_context_error());
        }
        Ok(())
    }

    #[inline(always)]
    fn check_context(&self, context: &WorkerContext) -> Result<(), HttpError> {
        let state = self.state.borrow();
        state.assert_owner();
        if !self.worker.same_worker(context.handle()) || !context.is_owner_thread() {
            return Err(wrong_context_error());
        }
        if state.closed || self.worker.state() != WorkerState::Open {
            return Err(observer_closed_error());
        }
        Ok(())
    }
}

impl Drop for AsyncSocketByteEventObserver {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let result = catch_unwind(AssertUnwindSafe(|| {
            close_observer(&self.state, cancelled_error);
        }));
        if let Some(registration) = self.cleanup_registration.get_mut().take() {
            let _ = registration.disarm();
        }
        if !already_panicking {
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        } else if let Err(payload) = result {
            roxygen_runtime::panic::discard_panic(payload);
        }
    }
}

fn schedule_tx_ack(
    state: &mut ObserverState,
    entry: RegistrationAndEvent,
) -> Vec<PendingByteEvent> {
    let mut ready = Vec::new();
    for event_type in [ByteEventType::NicTx, ByteEventType::CumulativeAck] {
        if !entry.registration.is_pending(event_type) {
            continue;
        }
        let offset = state.maximum_transport_write_offset;
        let mut event = entry.event.clone();
        event.event_type = event_type;
        let mut pending = PendingByteEvent::from_shared(offset, event, entry.registration.clone());
        let observed_offset = match event_type {
            ByteEventType::NicTx => state.maximum_transport_tx_offset,
            ByteEventType::CumulativeAck => state.maximum_transport_ack_offset,
            ByteEventType::TransportWrite | ByteEventType::KernelWrite => unreachable!(),
        };
        if observed_offset >= offset {
            ready.push(pending);
        } else {
            pending.arm_timeout(
                &state.host,
                state.byte_event_timeout,
                Rc::clone(&state.ref_count),
            );
            match event_type {
                ByteEventType::NicTx => state.tx_events.push_back(pending),
                ByteEventType::CumulativeAck => state.ack_events.push_back(pending),
                ByteEventType::TransportWrite | ByteEventType::KernelWrite => unreachable!(),
            }
        }
    }
    ready
}

fn has_ready(events: &VecDeque<PendingByteEvent>, offset: u64) -> bool {
    events
        .front()
        .is_some_and(|event| event.session_offset <= offset)
}

fn take_ready(events: &mut VecDeque<PendingByteEvent>, offset: u64) -> Vec<PendingByteEvent> {
    let mut ready = Vec::new();
    while has_ready(events, offset) {
        ready.push(events.pop_front().expect("the front event is present"));
    }
    ready
}

fn take_boundary_pending(
    events: &mut VecDeque<PendingByteEvent>,
    session_byte_offset: u64,
    event_type: ByteEventType,
) -> Vec<PendingByteEvent> {
    let mut matching = Vec::new();
    let mut retained = VecDeque::with_capacity(events.len());
    while let Some(event) = events.pop_front() {
        if event.event.event_type == event_type
            && event.event.transport_offset == session_byte_offset
        {
            matching.push(event);
        } else {
            retained.push_back(event);
        }
    }
    *events = retained;
    matching
}

fn take_boundary_registrations(
    boundaries: &mut VecDeque<TxAckBoundary>,
    session_byte_offset: u64,
    event_type: ByteEventType,
) -> Vec<RegistrationAndEvent> {
    let mut matching = Vec::new();
    let mut retained = VecDeque::with_capacity(boundaries.len());
    while let Some(mut boundary) = boundaries.pop_front() {
        if boundary.session_byte_offset == session_byte_offset {
            matching.extend(boundary.take_kind(event_type));
        }
        if boundary.event_count() == 0 {
            boundary.complete = true;
        } else {
            retained.push_back(boundary);
        }
    }
    *boundaries = retained;
    matching
}

fn take_stream_pending(
    events: &mut VecDeque<PendingByteEvent>,
    stream: StreamId,
) -> Vec<PendingByteEvent> {
    let (matching, retained): (Vec<_>, Vec<_>) = std::mem::take(events)
        .into_iter()
        .partition(|event| event.event.stream_id == Some(stream));
    *events = retained.into();
    matching
}

fn take_stream_registrations(
    boundaries: &mut VecDeque<TxAckBoundary>,
    stream: StreamId,
) -> Vec<RegistrationAndEvent> {
    let mut matching = Vec::new();
    let mut retained = VecDeque::with_capacity(boundaries.len());
    while let Some(mut boundary) = boundaries.pop_front() {
        matching.extend(boundary.take_stream(stream));
        if boundary.event_count() == 0 {
            boundary.complete = true;
        } else {
            retained.push_back(boundary);
        }
    }
    *boundaries = retained;
    matching
}

fn take_timeout_events(
    events: &mut VecDeque<PendingByteEvent>,
    context: &mut Context<'_>,
) -> (Vec<PendingByteEvent>, Vec<PendingByteEvent>) {
    #[cfg(test)]
    record_test_count(&TIMEOUT_QUEUE_TAKES);
    let mut retained = VecDeque::with_capacity(events.len());
    let mut stale = Vec::new();
    let mut expired = Vec::new();
    while let Some(mut event) = events.pop_front() {
        if !event.is_pending() {
            stale.push(event);
        } else if event.timeout_is_ready(context) {
            expired.push(event);
        } else {
            retained.push_back(event);
        }
    }
    *events = retained;
    (stale, expired)
}

fn close_observer(state: &Rc<RefCell<ObserverState>>, error: fn() -> HttpError) {
    {
        let mut state = state.borrow_mut();
        state.assert_owner();
        if state.closed {
            return;
        }
        state.closed = true;
        if state.has_no_byte_events() {
            state.pending_tx_ack_events = 0;
            return;
        }
    }
    #[cfg(test)]
    record_test_count(&OBSERVER_CANCELLATION_DRAINS);
    cancel_observer_events(state, &error());
}

fn cancel_observer_events(state: &Rc<RefCell<ObserverState>>, error: &HttpError) {
    let (transport, kernel, tx_ack, in_flight_tx_ack, tx, ack) = {
        let mut state = state.borrow_mut();
        state.assert_owner();
        state.pending_tx_ack_events = 0;
        (
            state.transport_write_events.drain(..).collect::<Vec<_>>(),
            state.kernel_write_events.drain(..).collect::<Vec<_>>(),
            state.tx_ack_events.drain(..).collect::<Vec<_>>(),
            std::mem::take(&mut state.in_flight_tx_ack_events)
                .into_values()
                .collect::<Vec<_>>(),
            state.tx_events.drain(..).collect::<Vec<_>>(),
            state.ack_events.drain(..).collect::<Vec<_>>(),
        )
    };
    let mut first_panic = None;
    for pending in [transport, kernel] {
        let result = catch_unwind(AssertUnwindSafe(|| cancel_pending(pending, error)));
        retain_first_panic(&mut first_panic, result.err());
    }
    for boundaries in [tx_ack, in_flight_tx_ack] {
        for event in boundaries {
            let result = catch_unwind(AssertUnwindSafe(|| event.cancel(error)));
            retain_first_panic(&mut first_panic, result.err());
        }
    }
    for pending in [tx, ack] {
        let result = catch_unwind(AssertUnwindSafe(|| cancel_pending(pending, error)));
        retain_first_panic(&mut first_panic, result.err());
    }
    if let Some(payload) = first_panic {
        resume_unwind(payload);
    }
}

fn run_registration_actions(actions: Vec<RegistrationAction>) {
    let mut first_panic = None;
    for action in actions {
        let result = catch_unwind(AssertUnwindSafe(|| match action {
            RegistrationAction::Fire(mut event) => {
                event.fire();
            }
            RegistrationAction::Cancel(entry, error) => {
                entry.registration.cancel(&error, &entry.event);
            }
        }));
        retain_first_panic(&mut first_panic, result.err());
    }
    if let Some(payload) = first_panic {
        resume_unwind(payload);
    }
}

fn cancel_registration_entries(entries: &[RegistrationAndEvent], error: &HttpError) {
    let mut first_panic = None;
    for entry in entries {
        let result = catch_unwind(AssertUnwindSafe(|| {
            entry.registration.cancel(error, &entry.event);
        }));
        retain_first_panic(&mut first_panic, result.err());
    }
    if let Some(payload) = first_panic {
        resume_unwind(payload);
    }
}

fn cancel_pending(mut pending: Vec<PendingByteEvent>, error: &HttpError) {
    cancel_pending_slice(&mut pending, error);
}

fn cancel_pending_slice(pending: &mut [PendingByteEvent], error: &HttpError) {
    let mut first_panic = None;
    for event in pending {
        let result = catch_unwind(AssertUnwindSafe(|| event.cancel(error)));
        retain_first_panic(&mut first_panic, result.err());
    }
    if let Some(payload) = first_panic {
        resume_unwind(payload);
    }
}

fn cancelled_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::CANCEL, b"ByteEvent Cancelled")
}

fn timeout_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::READ_TIMEOUT, b"ByteEvent Timeout")
}

fn dropped_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"byte-event observer was dropped")
}

#[cold]
#[inline(never)]
fn wrong_context_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"byte-event observer requires its exact worker context",
    )
}

#[cold]
fn observer_closed_error() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"byte-event observer is closed",
    )
}
