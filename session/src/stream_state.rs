use std::any::Any;
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
use std::pin::pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::body_event_queue::BodyEventQueueCallback;
use roxygen_core::body_event_queue::HttpBodyEventQueue;
use roxygen_core::body_event_queue::HttpBodyQueueRetirement;
use roxygen_core::body_event_queue::ReadBodyResult;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeFuture;
use roxygen_core::handler::HandlerResponseProposal;
use roxygen_core::http::HttpMethod;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceDrain;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::host::Host;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use crate::exchange::ExchangeGuard;
use crate::exchange::ExchangeOwner;
use crate::exchange::ExchangeTracker;

const DEFAULT_BODY_READ_SIZE: u32 = 65_535;
const DEFAULT_BODY_BUFFER_LIMIT: usize = 65_535;
const MAX_SOURCE_CHILD_POLLS_PER_TURN: usize = 64;
const MAX_QUEUED_SOURCE_EVENTS: usize = 256;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The next part of a response expected after processing one header event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseState {
    Headers,
    Body,
    Done,
}

pub(crate) enum StreamCallbackEvent {
    SourceComplete {
        id: StreamId,
        error: Option<HttpError>,
    },
    EgressProducerComplete {
        id: StreamId,
        generation: u64,
        error: Option<HttpError>,
    },
    HandlerComplete {
        id: StreamId,
        generation: u64,
        result: Result<HttpSourceHolder, HttpError>,
        proposal: Option<HandlerResponseProposal>,
    },
    BytesProcessed {
        id: StreamId,
        amount: usize,
        to_acknowledge: usize,
    },
    WindowOpen(StreamId),
}

pub(crate) struct StreamCallbacks {
    events: RefCell<VecDeque<StreamCallbackEvent>>,
    wake: Notify,
    retirement_changed: Cell<bool>,
    ingress_reconciliation_pending: Cell<bool>,
}

pub(crate) type StreamCallbackQueue = Rc<StreamCallbacks>;

impl StreamCallbacks {
    pub(crate) fn new() -> Self {
        Self {
            events: RefCell::new(VecDeque::new()),
            wake: Notify::new(),
            retirement_changed: Cell::new(false),
            ingress_reconciliation_pending: Cell::new(true),
        }
    }

    pub(crate) fn ingress_reconciliation_pending(&self) -> bool {
        self.ingress_reconciliation_pending.get()
    }

    pub(crate) fn mark_ingress_reconciliation_pending(&self) {
        self.ingress_reconciliation_pending.set(true);
    }

    pub(crate) fn acknowledge_ingress_reconciliation(&self) {
        self.ingress_reconciliation_pending.set(false);
    }

    pub(crate) fn push(&self, event: StreamCallbackEvent) {
        self.events.borrow_mut().push_back(event);
        self.wake.notify_one();
    }

    pub(crate) fn push_handler_complete(&self, event: StreamCallbackEvent) {
        let (id, fallback_response) = match &event {
            StreamCallbackEvent::HandlerComplete { id, result, .. } => (
                *id,
                result
                    .as_ref()
                    .map_or(true, |response| !response.readable()),
            ),
            _ => unreachable!("only handler completion uses priority insertion"),
        };
        let mut events = self.events.borrow_mut();
        let source_completion = if fallback_response {
            events.iter().position(|queued| {
                matches!(queued, StreamCallbackEvent::SourceComplete { id: queued_id, .. } if *queued_id == id)
            })
        } else {
            None
        };
        if let Some(index) = source_completion {
            events.insert(index, event);
        } else {
            events.push_back(event);
        }
        drop(events);
        self.wake.notify_one();
    }

    pub(crate) fn pop_front(&self) -> Option<StreamCallbackEvent> {
        self.events.borrow_mut().pop_front()
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.events.borrow().is_empty() || self.retirement_changed.get()
    }

    pub(crate) fn wake_retirement(&self) {
        self.retirement_changed.set(true);
        self.wake.notify_one();
    }

    pub(crate) fn acknowledge_retirement(&self) {
        self.retirement_changed.set(false);
    }

    /// Reports whether the wake holds a stored permit, leaving it in place.
    #[cfg(test)]
    pub(crate) fn has_stored_wake_for_test(&self) -> bool {
        let notified = pin!(self.wake.notified());
        let stored = notified
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready();
        if stored {
            self.wake.notify_one();
        }
        stored
    }

    #[cfg(test)]
    pub(crate) fn has_handler_completion(&self) -> bool {
        self.events
            .borrow()
            .iter()
            .any(|event| matches!(event, StreamCallbackEvent::HandlerComplete { .. }))
    }

    pub(crate) async fn wait(&self, cancellation: CancellationToken) -> WaitStatus {
        let mut registration = cancellation.wake_registration();
        let mut slot = pin!(None);
        poll_fn(|context| {
            self.poll_wait(
                slot.as_mut(),
                |waker| {
                    // A wait that returns without parking leaves the token unregistered.
                    if self.has_pending() {
                        cancellation.is_cancellation_requested()
                    } else {
                        registration.update_waker(waker)
                    }
                },
                context,
            )
        })
        .await
    }

    /// Waits once, checking cancellation through `update_registration`, which
    /// updates a wake registration that the caller keeps across waits and
    /// reports whether cancellation was observed.
    pub(crate) async fn wait_registered(
        &self,
        mut update_registration: impl FnMut(&Waker) -> bool,
    ) -> WaitStatus {
        let mut slot = pin!(None);
        poll_fn(|context| self.poll_wait(slot.as_mut(), &mut update_registration, context)).await
    }

    /// Polls one callback wait whose `Notified` lives in `slot`.
    ///
    /// Cancellation is checked first and consumes no permit. A pending event
    /// consumes one stored permit or assigned notification. A wake that finds
    /// no pending event rechecks cancellation and re-arms a fresh `Notified`.
    pub(crate) fn poll_wait<'a>(
        &'a self,
        mut slot: Pin<&mut Option<Notified<'a>>>,
        mut cancelled: impl FnMut(&Waker) -> bool,
        context: &mut Context<'_>,
    ) -> Poll<WaitStatus> {
        loop {
            if slot.is_none() {
                slot.set(Some(self.wake.notified()));
            }
            if cancelled(context.waker()) {
                return Poll::Ready(WaitStatus::Cancelled);
            }
            let notified = slot
                .as_mut()
                .as_pin_mut()
                .expect("the callback wait was armed above");
            if self.has_pending() {
                let _ = notified.poll(context);
                return Poll::Ready(WaitStatus::Signalled);
            }
            if notified.poll(context).is_pending() {
                return Poll::Pending;
            }
            if self.has_pending() {
                return Poll::Ready(WaitStatus::Signalled);
            }
            slot.set(None);
        }
    }
}

struct IngressCallbacks {
    events: StreamCallbackQueue,
}

impl HttpStreamSourceCallback for IngressCallbacks {
    fn bytes_processed(&mut self, id: StreamId, amount: usize, to_acknowledge: usize) {
        self.events.push(StreamCallbackEvent::BytesProcessed {
            id,
            amount,
            to_acknowledge,
        });
    }

    fn window_open(&mut self, id: StreamId) {
        self.events.push(StreamCallbackEvent::WindowOpen(id));
    }

    fn source_complete(&mut self, id: StreamId, error: Option<HttpError>) {
        self.events
            .push(StreamCallbackEvent::SourceComplete { id, error });
    }
}

#[derive(Default)]
struct EgressQueueCallbacks {
    buffered_bytes: Cell<i64>,
    buffered_deltas: Option<RefCell<VecDeque<i64>>>,
    mismatch: RefCell<Option<(u64, u64)>>,
}

impl EgressQueueCallbacks {
    fn new(stats_enabled: bool) -> Self {
        Self {
            buffered_deltas: stats_enabled.then(RefCell::default),
            ..Self::default()
        }
    }
}

impl BodyEventQueueCallback for EgressQueueCallbacks {
    fn on_egress_bytes_buffered(&self, bytes: i64) {
        if let Some(deltas) = &self.buffered_deltas {
            deltas.borrow_mut().push_back(bytes);
        }
        self.buffered_bytes
            .set(self.buffered_bytes.get().saturating_add(bytes));
    }

    fn on_content_length_mismatch(&self, expected: u64, observed: u64) {
        let mut mismatch = self.mismatch.borrow_mut();
        if mismatch.is_none() {
            *mismatch = Some((expected, observed));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EgressReadState {
    Headers,
    Body,
}

enum EgressPendingRead {
    Idle,
    Headers(LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>),
    Body(LocalSourceFuture<Result<ReadBodyResult, HttpError>>),
    Resume(LocalSourceFuture<Result<(), HttpError>>),
}

/// One source event produced for later codec serialization.
pub enum EgressEvent {
    Headers(HttpHeaderEvent),
    Body(HttpBodyEvent),
}

/// One generation of response or request-body production.
///
/// Dropping an unfinished transfer requests cancellation and reports producer
/// completion. An obsolete generation cannot stop a newer replacement source.
/// Dropping an individual next-event future leaves its pending source operation
/// owned by the transfer for the next scheduler poll.
pub struct EgressTransfer {
    host: Host,
    id: StreamId,
    generation: u64,
    state: EgressReadState,
    pending_read: EgressPendingRead,
    queue: Rc<RefCell<HttpBodyEventQueue>>,
    cancellation: Rc<CancellationSource>,
    current_generation: Rc<Cell<u64>>,
    events: StreamCallbackQueue,
    write_timeout: Duration,
    buffer_full_deadline: Cell<Option<Instant>>,
    complete: bool,
    // Source EOM ends capacity waits; generation retirement still follows serialization.
    production_complete: bool,
    retirement: ExchangeGuard,
}

impl EgressTransfer {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn cancellation_requested(&self) -> bool {
        self.cancellation.token().is_cancellation_requested()
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.token()
    }

    /// Whether this transfer still owns the stream's current source generation.
    pub(crate) fn generation_is_current(&self) -> bool {
        self.current_generation.get() == self.generation
    }

    pub fn is_reading_body(&self) -> bool {
        self.state == EgressReadState::Body
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn has_buffered_body_bytes(&self) -> bool {
        self.queue.borrow().buffered_body_bytes() != 0
    }

    pub(crate) fn has_buffered_events(&self) -> bool {
        !self.queue.borrow().empty()
    }

    pub(crate) fn can_read_source(&self) -> bool {
        if self.complete || self.production_complete {
            return false;
        }
        if !matches!(self.pending_read, EgressPendingRead::Idle) {
            return true;
        }
        let queue = self.queue.borrow();
        queue.buffered_body_bytes() < queue.limit()
            && queue.buffered_event_count() < MAX_QUEUED_SOURCE_EVENTS
    }

    pub(crate) fn can_dequeue_body(&self, maximum_body: u32) -> bool {
        self.queue.borrow().can_dequeue_body_event(maximum_body)
    }

    pub fn buffer_timeout_deadline(&self) -> Option<Instant> {
        let full = {
            let queue = self.queue.borrow();
            queue.buffered_body_bytes() >= queue.limit()
        };
        if self.complete || self.production_complete || !full || self.write_timeout.is_zero() {
            self.buffer_full_deadline.set(None);
            return None;
        }
        let deadline = self.buffer_full_deadline.get().unwrap_or_else(|| {
            let deadline = self.host.deadline_after(self.write_timeout);
            self.buffer_full_deadline.set(Some(deadline));
            deadline
        });
        Some(deadline)
    }

    pub fn timeout_buffer_space(&mut self) {
        if self.complete {
            return;
        }
        let error = HttpError::with_message(
            HttpErrorCode::READ_TIMEOUT,
            b"timed out waiting for buffer space",
        );
        let retirement = self
            .queue
            .borrow_mut()
            .prepare_stop_and_clear(error.clone());
        let mut first_panic = retirement.retire().err();
        retain_egress_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| self.finish(Some(error)))).err(),
        );
        if let Some(payload) = first_panic {
            self.retirement.fail();
            resume_unwind(payload);
        }
    }

    /// Reads the next event through the bounded egress queue.
    pub async fn next_event(&mut self) -> Result<Option<EgressEvent>, HttpError> {
        self.next_event_with_limit(DEFAULT_BODY_READ_SIZE).await
    }

    /// Reads the next event while bounding a body chunk by available protocol
    /// flow-control credit. Headers and non-body events are unaffected.
    pub async fn next_event_with_limit(
        &mut self,
        maximum_body: u32,
    ) -> Result<Option<EgressEvent>, HttpError> {
        poll_fn(|context| self.poll_next_event_with_limit(context, maximum_body)).await
    }

    pub(crate) async fn next_event_with_credit(
        &mut self,
        maximum_body: Option<u32>,
    ) -> Result<Option<EgressEvent>, HttpError> {
        poll_fn(|context| self.poll_next_event_with_credit(context, maximum_body)).await
    }

    /// Polls the bounded egress queue while retaining a pending child read.
    pub(crate) fn poll_next_event_with_limit(
        &mut self,
        context: &mut Context<'_>,
        maximum_body: u32,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        self.poll_next_event_with_credit(context, Some(maximum_body))
    }

    // None blocks every body event; Some(0) permits terminal events at zero stream credit.
    pub(crate) fn poll_next_event_with_credit(
        &mut self,
        context: &mut Context<'_>,
        maximum_body: Option<u32>,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        if self.complete {
            return Poll::Ready(Ok(None));
        }
        match self.state {
            EgressReadState::Headers => {
                if matches!(self.pending_read, EgressPendingRead::Idle) {
                    let read = self.queue.borrow_mut().read_header_event();
                    self.pending_read = EgressPendingRead::Headers(Box::pin(
                        await_header_or_cancellation(read, self.cancellation.token()),
                    ));
                }
                let EgressPendingRead::Headers(read) = &mut self.pending_read else {
                    unreachable!("header state retains only a header read");
                };
                let Poll::Ready(result) = read.as_mut().poll(context) else {
                    return Poll::Pending;
                };
                self.pending_read = EgressPendingRead::Idle;
                let mut event = match result {
                    Ok(event) => event,
                    Err(error) => {
                        self.finish(Some(error.clone()));
                        return Poll::Ready(Err(error));
                    }
                };
                let is_final = match event.is_final() {
                    Ok(is_final) => is_final,
                    Err(error) => {
                        let message = error.to_string();
                        let error = HttpError::with_message(
                            HttpErrorCode::MESSAGE_ERROR,
                            message.as_bytes(),
                        );
                        self.finish(Some(error.clone()));
                        return Poll::Ready(Err(error));
                    }
                };
                if !is_final && event.eom() {
                    let error =
                        invalid_transition(b"informational response headers cannot end the stream");
                    self.finish(Some(error.clone()));
                    return Poll::Ready(Err(error));
                }
                if is_final {
                    if event.eom() {
                        self.finish(None);
                    } else {
                        self.state = EgressReadState::Body;
                    }
                }
                Poll::Ready(Ok(Some(EgressEvent::Headers(event))))
            }
            EgressReadState::Body => self.poll_body_progress(context, maximum_body),
        }
    }

    /// Admits Body-phase source events without dequeuing or advancing headers.
    pub(crate) fn poll_body_source(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        if self.complete || !self.is_reading_body() {
            return Poll::Ready(Ok(None));
        }
        self.poll_body_progress(context, None)
    }

    fn poll_body_progress(
        &mut self,
        context: &mut Context<'_>,
        maximum_body: Option<u32>,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        let mut remaining_polls = MAX_SOURCE_CHILD_POLLS_PER_TURN;
        loop {
            let mut pending = false;
            if remaining_polls != 0 && self.can_read_source() {
                if matches!(self.pending_read, EgressPendingRead::Idle) {
                    self.pending_read = EgressPendingRead::Body(
                        self.queue
                            .borrow_mut()
                            .read_body_event(DEFAULT_BODY_READ_SIZE, self.cancellation.token()),
                    );
                }
                remaining_polls -= 1;
                let body_read = matches!(self.pending_read, EgressPendingRead::Body(_));
                let result = match &mut self.pending_read {
                    EgressPendingRead::Body(read) => {
                        read.as_mut().poll(context).map(|result| result.map(Some))
                    }
                    EgressPendingRead::Resume(resume) => resume
                        .as_mut()
                        .poll(context)
                        .map(|result| result.map(|()| None)),
                    EgressPendingRead::Idle | EgressPendingRead::Headers(_) => {
                        unreachable!("body state retains a body read or resume");
                    }
                };
                match result {
                    Poll::Pending => pending = true,
                    Poll::Ready(result) => {
                        self.pending_read = EgressPendingRead::Idle;
                        let result = match result {
                            Ok(result) => result,
                            Err(error) => {
                                if body_read && !self.cancellation_requested() {
                                    self.finish_body_source_error(error.clone());
                                } else {
                                    self.finish(Some(error.clone()));
                                }
                                return Poll::Ready(Err(error));
                            }
                        };
                        let Some(result) = result else {
                            continue;
                        };
                        if let Some(resume) = result.resume {
                            self.pending_read = EgressPendingRead::Resume(Box::pin(
                                await_resume_or_cancellation(resume, self.cancellation.token()),
                            ));
                            continue;
                        }
                        self.production_complete = result.eom;
                    }
                }
            }
            if let Some(maximum_body) = maximum_body
                && self.can_dequeue_body(maximum_body)
            {
                let event = self
                    .queue
                    .borrow_mut()
                    .dequeue_body_event(maximum_body)
                    .expect("a non-empty egress queue has a body event");
                self.buffer_timeout_deadline();
                if event.eom() {
                    self.finish(None);
                }
                return Poll::Ready(Ok(Some(EgressEvent::Body(event))));
            }
            self.buffer_timeout_deadline();
            if pending {
                return Poll::Pending;
            }
            if !self.can_read_source() {
                return Poll::Ready(Ok(None));
            }
            if remaining_polls == 0 {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }

    fn finish_body_source_error(&mut self, error: HttpError) {
        let retirement = self
            .queue
            .borrow_mut()
            .prepare_stop_and_clear(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"Sent reset",
            ));
        let mut first_panic = retirement.retire().err();
        retain_egress_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| self.finish(Some(error)))).err(),
        );
        if let Some(payload) = first_panic {
            self.retirement.fail();
            resume_unwind(payload);
        }
    }

    fn finish(&mut self, error: Option<HttpError>) {
        if self.complete {
            return;
        }
        self.complete = true;
        self.events
            .push(StreamCallbackEvent::EgressProducerComplete {
                id: self.id,
                generation: self.generation,
                error,
            });
    }
}

impl Drop for EgressTransfer {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut first_panic = None;
        if !self.complete {
            retain_egress_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| {
                    self.cancellation.request_cancellation()
                }))
                .err(),
            );
            if self.current_generation.get() == self.generation {
                retain_egress_panic(
                    &mut first_panic,
                    catch_unwind(AssertUnwindSafe(|| {
                        self.queue.borrow_mut().stop_reading(HttpErrorCode::CANCEL);
                    }))
                    .err(),
                );
            }
            retain_egress_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| {
                    self.finish(Some(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"egress producer dropped",
                    )));
                }))
                .err(),
            );
        }
        let pending = std::mem::replace(&mut self.pending_read, EgressPendingRead::Idle);
        retain_egress_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| drop(pending))).err(),
        );
        if unwinding || first_panic.is_some() {
            self.retirement.fail();
        }
        if let Some(payload) = first_panic {
            if unwinding {
                discard_egress_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

pub(crate) type EgressPanic = Box<dyn Any + Send>;

pub(crate) fn retain_egress_panic(first: &mut Option<EgressPanic>, next: Option<EgressPanic>) {
    if first.is_none() {
        *first = next;
    } else if let Some(payload) = next {
        discard_egress_panic(payload);
    }
}

pub(crate) fn discard_egress_panic(payload: EgressPanic) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

struct EgressGenerationResources {
    // Cancellation ownership must retire before queue ownership.
    cancellation: Rc<CancellationSource>,
    queue: Rc<RefCell<HttpBodyEventQueue>>,
}

pub(crate) struct PreparedStreamReset {
    ingress: Option<HttpStreamSourceProducer>,
    error: Option<HttpError>,
    cancellations: Vec<Rc<CancellationSource>>,
    queues: Vec<Rc<RefCell<HttpBodyEventQueue>>>,
    prepared_queues: Vec<HttpBodyQueueRetirement>,
    prepared: bool,
}

impl PreparedStreamReset {
    pub(crate) fn prepare_queues(&mut self) -> Option<EgressPanic> {
        if self.prepared {
            return None;
        }
        self.prepared = true;
        let mut first = None;
        for queue in &self.queues {
            let error = self
                .error
                .as_ref()
                .expect("prepared reset owns its error")
                .clone();
            match catch_unwind(AssertUnwindSafe(|| {
                queue.borrow_mut().prepare_stop_and_clear(error)
            })) {
                Ok(retirement) => self.prepared_queues.push(retirement),
                Err(payload) => retain_egress_panic(&mut first, Some(payload)),
            }
        }
        first
    }

    pub(crate) fn retire(&mut self) -> Option<EgressPanic> {
        let mut first = self.prepare_queues();
        if let Some(ingress) = self.ingress.take() {
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| {
                    ingress.abort_with_error(
                        self.error
                            .as_ref()
                            .expect("prepared reset owns its error")
                            .clone(),
                        b"session stopped servicing the stream",
                    );
                }))
                .err(),
            );
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(ingress))).err(),
            );
        }
        for cancellation in std::mem::take(&mut self.cancellations) {
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| cancellation.request_cancellation())).err(),
            );
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(cancellation))).err(),
            );
        }
        for retirement in self.prepared_queues.drain(..) {
            retain_egress_panic(&mut first, retirement.retire().err());
        }
        for queue in std::mem::take(&mut self.queues) {
            retain_egress_panic(
                &mut first,
                catch_unwind(AssertUnwindSafe(|| drop(queue))).err(),
            );
        }
        let error = self.error.take();
        retain_egress_panic(
            &mut first,
            catch_unwind(AssertUnwindSafe(|| drop(error))).err(),
        );
        first
    }
}

impl Drop for PreparedStreamReset {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        if let Some(payload) = self.retire() {
            if unwinding {
                discard_egress_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Upgrade {
    None,
    Pending,
    Upgraded,
}

/// Worker-local lifecycle and source ownership for one HTTP/1.1 or HTTP/2
/// stream.
pub struct StreamState {
    host: Host,
    id: StreamId,
    sequence_number: u64,
    upgrade: Upgrade,
    stream_source_complete: bool,
    ingress_eom_seen: bool,
    egress_complete: bool,
    outgoing_capacity_released: bool,
    egress_started: bool,
    ingress_cancel_requested: bool,
    deferred_stop_sending: bool,
    error_response_status: Option<u16>,
    expected_handler_error_status: Option<u16>,
    handler_error_response: bool,
    terminal_error: Option<HttpError>,
    ingress: HttpStreamSourceProducer,
    _ingress_drain: Option<HttpStreamSourceDrain>,
    egress_queue_callbacks: Rc<EgressQueueCallbacks>,
    // An evacuated generation stays active until its transfer completes.
    egress_generations: BTreeMap<u64, Option<EgressGenerationResources>>,
    current_egress_generation: Rc<Cell<u64>>,
    next_egress_generation: u64,
    events: StreamCallbackQueue,
    egress_write_timeout: Duration,
    exchange: ExchangeTracker,
    exchange_owner: Option<ExchangeOwner>,
}

impl StreamState {
    pub(crate) fn new(
        id: StreamId,
        sequence_number: u64,
        events: StreamCallbackQueue,
        ingress_receive_window: u32,
        egress_write_timeout: Duration,
        exchange_owner: ExchangeOwner,
        head_response: bool,
        drain_request_body_on_drop: bool,
        host: Host,
    ) -> Result<(Self, HttpSourceHolder), HttpError> {
        let exchange = exchange_owner.tracker();
        let source = HttpStreamSource::new_session_with_host(
            id,
            IngressCallbacks {
                events: Rc::clone(&events),
            },
            ingress_receive_window,
            DEFAULT_READ_TIMEOUT,
            host.clone(),
        )
        .map_err(|error| {
            let message = error.to_string();
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, message.as_bytes())
        })?;
        if head_response {
            source.skip_content_length_validation();
        }
        let ingress = source.producer();
        let ingress_drain = drain_request_body_on_drop.then(|| source.drain_on_drop());
        let queue_callbacks = Rc::new(EgressQueueCallbacks::new(true));
        Ok((
            Self {
                host,
                id,
                sequence_number,
                upgrade: Upgrade::None,
                stream_source_complete: false,
                ingress_eom_seen: false,
                egress_complete: false,
                outgoing_capacity_released: false,
                egress_started: false,
                ingress_cancel_requested: false,
                deferred_stop_sending: false,
                error_response_status: None,
                expected_handler_error_status: None,
                handler_error_response: false,
                terminal_error: None,
                ingress,
                _ingress_drain: ingress_drain,
                egress_queue_callbacks: queue_callbacks,
                egress_generations: BTreeMap::new(),
                current_egress_generation: Rc::new(Cell::new(0)),
                next_egress_generation: 1,
                events,
                egress_write_timeout,
                exchange: exchange.clone(),
                exchange_owner: Some(exchange_owner),
            },
            exchange.source(HttpSourceHolder::new(Box::new(source))),
        ))
    }

    pub const fn id(&self) -> StreamId {
        self.id
    }

    pub(crate) fn disable_buffered_egress_stats(&mut self) {
        Rc::get_mut(&mut self.egress_queue_callbacks)
            .expect("egress stats are configured before creating any egress queue")
            .buffered_deltas = None;
    }

    pub(crate) fn exchange_tracker(&self) -> ExchangeTracker {
        self.exchange.clone()
    }

    pub(crate) fn take_exchange_owner(&mut self) -> ExchangeOwner {
        self.exchange_owner.take().expect("exchange admitted once")
    }

    pub const fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn ingress_producer(&self) -> HttpStreamSourceProducer {
        self.ingress.clone()
    }

    pub fn buffered_ingress_bytes(&self) -> u32 {
        self.ingress.body_bytes_buffered().unwrap_or_default()
    }

    pub(crate) fn mark_source_complete(&mut self, error: Option<HttpError>) -> bool {
        if self.stream_source_complete {
            return false;
        }
        self.stream_source_complete = true;
        self.events.mark_ingress_reconciliation_pending();
        if let Some(error) = error {
            self.record_terminal_error(error);
        }
        true
    }

    pub(crate) fn mark_ingress_eom_seen(&mut self) {
        self.ingress_eom_seen = true;
        self.events.mark_ingress_reconciliation_pending();
    }

    /// Records the upgrade progress of one header block and returns whether
    /// an upgrade is in progress or complete.
    pub(crate) fn check_for_upgrade(&mut self, message: &HttpMessage, is_ingress: bool) -> bool {
        let websocket_upgrade = if is_ingress {
            message.is_ingress_websocket_upgrade()
        } else {
            message.is_egress_websocket_upgrade()
        };
        if message.is_request()
            && (message.request_method() == Some(HttpMethod::Connect.as_str()) || websocket_upgrade)
        {
            debug_assert_eq!(self.upgrade, Upgrade::None);
            self.upgrade = Upgrade::Pending;
        } else if self.upgrade == Upgrade::Pending
            && message.is_response()
            && (message.response_status() == Some(200) || websocket_upgrade)
        {
            self.upgrade = Upgrade::Upgraded;
        }
        self.upgrade != Upgrade::None
    }

    pub const fn is_upgraded(&self) -> bool {
        matches!(self.upgrade, Upgrade::Upgraded)
    }

    pub(crate) fn install_egress_source(
        &mut self,
        source: HttpSourceHolder,
        starts_with_headers: bool,
        expected_content_length: Option<&[u8]>,
    ) -> Result<EgressTransfer, HttpError> {
        self.install_egress_source_impl(source, starts_with_headers, expected_content_length, false)
    }

    /// Adopts a source already guarded by this stream's exchange tracker.
    pub(crate) fn install_tracked_egress_source(
        &mut self,
        source: HttpSourceHolder,
        starts_with_headers: bool,
        expected_content_length: Option<&[u8]>,
    ) -> Result<EgressTransfer, HttpError> {
        self.install_egress_source_impl(source, starts_with_headers, expected_content_length, true)
    }

    fn install_egress_source_impl(
        &mut self,
        source: HttpSourceHolder,
        starts_with_headers: bool,
        expected_content_length: Option<&[u8]>,
        already_tracked: bool,
    ) -> Result<EgressTransfer, HttpError> {
        let previous_generation = self.current_egress_generation.get();
        if let Some(Some(previous)) = self.egress_generations.get(&previous_generation) {
            previous.cancellation.request_cancellation();
            let error = HttpError::with_message(
                HttpErrorCode::CANCEL,
                b"replaced by a newer egress producer",
            );
            let mut queue = previous.queue.borrow_mut();
            queue.stop_reading(HttpErrorCode::CANCEL);
            queue.clear(&error);
        }

        let source = if already_tracked {
            source
        } else {
            self.exchange.source(source)
        };
        let queue = Rc::new(RefCell::new(
            HttpBodyEventQueue::from_source_with_configuration(
                source,
                self.id,
                &self.egress_queue_callbacks,
                DEFAULT_BODY_BUFFER_LIMIT,
                self.egress_write_timeout,
                self.host.clone(),
            ),
        ));
        if let Some(content_length) = expected_content_length {
            queue
                .borrow_mut()
                .set_expected_egress_content_length(content_length, false);
        }

        let generation = self.next_egress_generation;
        self.next_egress_generation = self.next_egress_generation.wrapping_add(1);
        let cancellation = Rc::new(CancellationSource::new());
        self.egress_generations.insert(
            generation,
            Some(EgressGenerationResources {
                cancellation: Rc::clone(&cancellation),
                queue: Rc::clone(&queue),
            }),
        );
        self.current_egress_generation.set(generation);
        Ok(EgressTransfer {
            host: self.host.clone(),
            id: self.id,
            generation,
            state: if starts_with_headers {
                EgressReadState::Headers
            } else {
                EgressReadState::Body
            },
            pending_read: EgressPendingRead::Idle,
            queue,
            cancellation,
            current_generation: Rc::clone(&self.current_egress_generation),
            events: Rc::clone(&self.events),
            write_timeout: self.egress_write_timeout,
            buffer_full_deadline: Cell::new(None),
            complete: false,
            production_complete: false,
            retirement: self.exchange.guard(),
        })
    }

    pub(crate) fn finish_egress_generation(
        &mut self,
        generation: u64,
        _error: Option<HttpError>,
    ) -> bool {
        let Some(resources) = self.egress_generations.remove(&generation) else {
            return false;
        };
        drop(resources);
        if self.current_egress_generation.get() == generation {
            self.current_egress_generation.set(0);
        }
        true
    }

    pub(crate) fn current_egress_generation(&self) -> u64 {
        self.current_egress_generation.get()
    }

    pub(crate) fn cancel_egress_producers(&mut self) {
        let error = HttpError::with_message(HttpErrorCode::CANCEL, b"egress was cancelled");
        self.cancel_egress_producers_with_error(&error);
    }

    fn cancel_egress_producers_with_error(&mut self, error: &HttpError) {
        for resources in self.egress_generations.values().flatten() {
            resources.cancellation.request_cancellation();
        }
        for resources in self.egress_generations.values().flatten() {
            let mut queue = resources.queue.borrow_mut();
            queue.stop_reading(error.code());
            queue.clear(error);
        }
    }

    pub(crate) fn abort_ingress(&mut self, code: HttpErrorCode, details: &[u8]) -> bool {
        self.ingress_cancel_requested = true;
        self.ingress.abort(code, details)
    }

    pub(crate) fn abort_ingress_with_error(&mut self, error: HttpError, details: &[u8]) -> bool {
        self.ingress_cancel_requested = true;
        self.ingress.abort_with_error(error, details)
    }

    pub(crate) fn process_response_headers(
        &mut self,
        is_final: bool,
        end_stream: bool,
    ) -> Result<ResponseState, HttpError> {
        if self.egress_complete {
            return Ok(ResponseState::Done);
        }
        if self.egress_started {
            return Err(invalid_transition(
                b"response headers arrived after final headers",
            ));
        }
        if !is_final {
            if end_stream {
                return Err(invalid_transition(
                    b"informational response headers cannot end the stream",
                ));
            }
            return Ok(ResponseState::Headers);
        }
        self.egress_started = true;
        if end_stream {
            self.egress_complete = true;
            Ok(ResponseState::Done)
        } else {
            Ok(ResponseState::Body)
        }
    }

    pub(crate) fn mark_egress_complete(&mut self) -> bool {
        if self.egress_complete {
            return false;
        }
        self.egress_complete = true;
        true
    }

    pub(crate) fn set_error_response_status(
        &mut self,
        status: u16,
        handler_active: bool,
    ) -> Result<(), HttpError> {
        if !self.can_send_headers() || (!handler_active && self.egress_generations.is_empty()) {
            return Err(invalid_transition(
                b"an error response requires an active producer before final headers",
            ));
        }
        self.error_response_status = Some(status);
        self.expected_handler_error_status = None;
        self.ingress_cancel_requested = true;
        self.cancel_egress_producers();
        Ok(())
    }

    pub(crate) fn expect_error_response_status(&mut self, status: u16) {
        self.expected_handler_error_status = Some(status);
        self.ingress_cancel_requested = true;
    }

    pub(crate) fn take_expected_handler_error_status(&mut self) -> Option<u16> {
        self.expected_handler_error_status.take()
    }

    pub(crate) fn take_error_response_status(&mut self) -> Option<u16> {
        self.error_response_status.take()
    }

    pub(crate) fn mark_handler_error_response(&mut self) {
        self.handler_error_response = true;
    }

    pub(crate) const fn has_handler_error_response(&self) -> bool {
        self.handler_error_response
    }

    pub(crate) fn set_deferred_stop_sending(&mut self) {
        self.deferred_stop_sending = true;
    }

    pub(crate) fn take_deferred_stop_sending(&mut self) -> bool {
        std::mem::take(&mut self.deferred_stop_sending)
    }

    pub(crate) fn validate_header_only_content_length(&self, content_length: &[u8]) {
        if content_length.is_empty() {
            return;
        }
        let mut queue = HttpBodyEventQueue::new(self.id, &self.egress_queue_callbacks);
        queue.set_expected_egress_content_length(content_length, true);
    }

    pub(crate) fn reset(&mut self, error: HttpError) -> bool {
        self.reset_with_egress_error(error.clone(), error)
    }

    pub(crate) fn prepare_reset_without_wire(&mut self, error: HttpError) -> PreparedStreamReset {
        let generations = self.egress_generations.values().flatten().count();
        let mut cancellations = Vec::with_capacity(generations);
        let mut queues = Vec::with_capacity(generations);
        let prepared_queues = Vec::with_capacity(generations);
        let ingress = self.ingress.clone();
        let terminal_error = error.clone();
        self.record_terminal_error(terminal_error);
        self.ingress_cancel_requested = true;
        self.egress_complete = true;
        self.error_response_status = None;
        self.expected_handler_error_status = None;
        self.handler_error_response = false;
        self.deferred_stop_sending = false;
        for resources in self
            .egress_generations
            .values_mut()
            .filter_map(Option::take)
        {
            cancellations.push(resources.cancellation);
            queues.push(resources.queue);
        }
        PreparedStreamReset {
            ingress: Some(ingress),
            error: Some(error),
            cancellations,
            queues,
            prepared_queues,
            prepared: false,
        }
    }

    pub(crate) fn reset_with_egress_error(
        &mut self,
        error: HttpError,
        egress_error: HttpError,
    ) -> bool {
        let first_error = self.terminal_error.is_none();
        self.record_terminal_error(error);
        self.egress_complete = true;
        self.cancel_egress_producers_with_error(&egress_error);
        self.error_response_status = None;
        self.expected_handler_error_status = None;
        self.handler_error_response = false;
        self.deferred_stop_sending = false;
        first_error
    }

    pub const fn can_send_headers(&self) -> bool {
        !self.egress_started && !self.egress_complete
    }

    pub const fn stream_source_complete(&self) -> bool {
        self.stream_source_complete
    }

    pub const fn ingress_eom_seen(&self) -> bool {
        self.ingress_eom_seen
    }

    pub const fn egress_complete(&self) -> bool {
        self.egress_complete
    }

    pub const fn outgoing_capacity_released(&self) -> bool {
        self.outgoing_capacity_released
    }

    pub(crate) fn release_outgoing_capacity(&mut self) -> bool {
        !std::mem::replace(&mut self.outgoing_capacity_released, true)
    }

    pub fn egress_task_complete(&self) -> bool {
        self.egress_generations.is_empty()
    }

    pub const fn ingress_cancel_requested(&self) -> bool {
        self.ingress_cancel_requested
    }

    pub fn egress_cancel_requested(&self) -> bool {
        self.egress_generations
            .values()
            .flatten()
            .any(|resources| resources.cancellation.token().is_cancellation_requested())
    }

    pub const fn error_response_status(&self) -> Option<u16> {
        self.error_response_status
    }

    pub fn terminal_error(&self) -> Option<&HttpError> {
        self.terminal_error.as_ref()
    }

    pub fn buffered_egress_bytes(&self) -> i64 {
        self.egress_queue_callbacks.buffered_bytes.get()
    }

    pub fn take_buffered_egress_deltas(&self) -> Vec<i64> {
        self.egress_queue_callbacks
            .buffered_deltas
            .as_ref()
            .map_or_else(Vec::new, |deltas| deltas.borrow_mut().drain(..).collect())
    }

    #[cfg(test)]
    pub(crate) fn buffered_egress_stats_storage_for_test(&self) -> Option<usize> {
        self.egress_queue_callbacks
            .buffered_deltas
            .as_ref()
            .map(|deltas| deltas.borrow().capacity())
    }

    pub fn record_egress_dequeued(&self, bytes: usize) {
        self.egress_queue_callbacks.buffered_bytes.set(
            self.egress_queue_callbacks
                .buffered_bytes
                .get()
                .saturating_sub(i64::try_from(bytes).unwrap_or(i64::MAX)),
        );
    }

    pub fn egress_content_length_mismatch(&self) -> Option<(u64, u64)> {
        *self.egress_queue_callbacks.mismatch.borrow()
    }

    pub fn is_detachable(&self) -> bool {
        self.stream_source_complete && self.egress_complete && self.egress_task_complete()
    }

    fn record_terminal_error(&mut self, error: HttpError) {
        if self.terminal_error.is_none() {
            self.terminal_error = Some(error);
        }
    }
}

fn invalid_transition(message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

async fn await_header_or_cancellation(
    mut read: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    cancellation: CancellationToken,
) -> Result<HttpHeaderEvent, HttpError> {
    let mut cancellation_wait = None;
    poll_fn(|context| match read.as_mut().poll(context) {
        Poll::Ready(result) => {
            if cancellation.is_cancellation_requested() {
                Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"egress header read cancelled",
                )))
            } else {
                Poll::Ready(result)
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
                    b"egress header read cancelled",
                ))),
                Poll::Ready(_) | Poll::Pending => Poll::Pending,
            }
        }
    })
    .await
}

async fn await_resume_or_cancellation(
    mut resume: ResumeFuture,
    cancellation: CancellationToken,
) -> Result<(), HttpError> {
    let mut cancellation_wait = None;
    poll_fn(|context| match resume.as_mut().poll(context) {
        Poll::Ready(_) if cancellation.is_cancellation_requested() => {
            Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"egress body resume cancelled",
            )))
        }
        Poll::Ready(_) => Poll::Ready(Ok(())),
        Poll::Pending => {
            let (wait, _) = cancellation_wait.get_or_insert_with(|| {
                let baton = CancellableBaton::new();
                // Unregister the wait before destroying its baton.
                (baton.wait(cancellation.clone()), baton)
            });
            match Pin::new(wait).poll(context) {
                Poll::Ready(WaitStatus::Cancelled) => Poll::Ready(Err(HttpError::with_message(
                    HttpErrorCode::CORO_CANCELLED,
                    b"egress body resume cancelled",
                ))),
                Poll::Ready(_) | Poll::Pending => Poll::Pending,
            }
        }
    })
    .await
}
