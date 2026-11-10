//! Experimental owned HTTP/1.1 upstream session.
//!
//! The poll-driven HTTP/1.1 sibling of [`crate::owned_h2::OwnedH2UpstreamSession`], with the
//! same admission contract: reserve a slot, submit a head and its body source, and read the
//! response from a source the caller owns. One driver future runs the connection's upstream
//! `Http1xCodec`, the request upload and the response source, with no task per request.
//!
//! Capacity is one. Capacity is released once the request is serialized and the response's EOM
//! is parsed; once the response's reader is done and the request's bytes are written, a
//! connection that `Http1xCodec` leaves reusable, between messages, returns its slot
//! (`capacity_available`, and the connection is idle), and any other connection closes admission
//! and shuts down its write side. Response bytes behind a finished response fail the connection
//! before a next request is committed. An HTTP/1.1 exchange cannot be reset alone, so a cancel
//! before both messages end resets the connection. A request body that does not match its declared `Content-Length` fails the
//! request with `CONTENT_LENGTH_MISMATCH`, and no byte past that length or after a short end is
//! written. Trailers on a `Content-Length` request have no HTTP/1.1 encoding and are dropped.
//! Peer EOF closes admission. Under [`Http1UpgradePolicy::Proxygen`] a request that can switch
//! protocols (CONNECT, an `Upgrade` header or the egress websocket flag) is refused. Under
//! [`Http1UpgradePolicy::AsOrdinaryRequest`] CONNECT and `Upgrade` requests are sent as
//! ordinary requests, a head that does not frame its body as the codec writes it is refused, a
//! `101` fails the connection, and the egress websocket flag is still refused (`DIV-031`).
//! Byte events are cancelled, never fired, and the driver is untimed. A cancel from any
//! callback the driver runs, including a request body's resume future and a waker that a
//! response event's publication runs, is observed at the end of that phase, before it reads
//! more of the request, parses the next response event, writes or closes normally. As in
//! `UniplexSession`, a failed read fails the connection only after every byte read before it is
//! parsed, flow control included, and resets the transport
//! only once the request bytes already serialized are written.
//! [`OwnedH1UpstreamSession::new`] validates [`UniplexConfig`] as [`OwnedH1Session::new`]
//! does, except that `drain_request_body_on_drop` must be false,
//! `unframed_bodiless_http1_requests` (`DIV-036`) is honored: a request whose head ends it and
//! carries no framing header goes out with no body framing, as through `UniplexSession`, and
//! `deliver_ingress_before_abort` (`DIV-033`) is honored: a response that fails keeps its
//! received head and body events for its reader, before the error, unless the request's
//! consumer cancelled it (a cancel, a dropped submission or the cancellation token), which
//! discards them, as `UniplexStreamAbort` does. DIV-029 lists where this driver differs from
//! `UniplexSession`'s client. Each turn reads before it commits a queued request, so a peer EOF
//! already readable, or a failed read, closes admission first; a request that admission refuses
//! reports `OwnedRequestProgressExt::request_unprocessed` (`DIV-035`).

use std::cell::RefCell;

use roxygen_core::body_event_queue::HttpBodySource;
use roxygen_core::body_event_queue::HttpBodyTransfer;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::ResumeFuture;
use roxygen_core::events::ResumeStatus;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpMethod;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpResponseSource;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
use crate::owned::OwnedResponseObserver;
use crate::owned::OwnedUpstreamObserver;

/// Connection state shared by the handle, reservations, submissions and the driver.
struct Shared {
    context: WorkerContext,
    /// The driver's exchange signal: submissions and request controls wake the driver here.
    wake: Arc<Signal>,
    complete: Cell<bool>,
    cancel: Cell<bool>,
    drain: Cell<bool>,
    idle: Cell<bool>,
    used: Cell<u32>,
    /// A committed exchange holds the slot until it retires.
    spent: Cell<bool>,
    /// Requests committed on the connection (`DIV-035`).
    committed: Cell<u64>,
    /// The live exchange's response signals once only its consumer's completion and the
    /// driver's retirement keep its slot (`OwnedH1UpstreamHandle::returning_capacity`).
    retiring: RefCell<Option<Rc<IngressSignals>>>,
    closed: RefCell<Option<HttpError>>,
    observer: RefCell<Option<Rc<dyn OwnedUpstreamObserver>>>,
    queued: RefCell<Option<Submission>>,
}

impl Shared {
    /// The connection is cancelled, by its handle or its worker.
    #[inline]
    fn cancelled(&self) -> bool {
        self.cancel.get() || self.context.is_cancelled()
    }

    fn available(&self) -> u32 {
        if self.spent.get()
            || self.cancel.get()
            || self.closed.borrow().is_some()
            || self.context.is_cancelled()
        {
            0
        } else {
            1 - self.used.get()
        }
    }

    fn release(&self, stream: Option<StreamId>, undelivered: Option<&Cell<bool>>) {
        self.used.set(
            self.used
                .get()
                .checked_sub(1)
                .expect("one outgoing reservation is owned"),
        );
        let observer = self.observer.borrow().clone();
        let mut panic = None;
        if let Some(observer) = observer {
            if let Some(undelivered) = undelivered {
                undelivered.set(true);
                capture(&mut panic, || observer.capacity_released_before_delivery());
            }
            if let Some(stream) = stream {
                capture(&mut panic, || observer.capacity_released(stream));
            }
            if self.available() != 0 {
                capture(&mut panic, || observer.capacity_available());
            }
        }
        capture(&mut panic, || self.wake.wake_by_ref());
        finish_panic(panic);
    }

    /// Returns the slot after a finished exchange: the connection admits a request again.
    fn return_slot(&self) {
        self.spent.set(false);
        let observer = (self.available() != 0)
            .then(|| self.observer.borrow().clone())
            .flatten();
        if let Some(observer) = observer {
            observer.capacity_available();
        }
    }

    /// Closes admission with `error`: a queued submission is rejected with it.
    fn close(&self, error: HttpError) {
        let changed = {
            let mut closed = self.closed.borrow_mut();
            if closed.is_none() {
                *closed = Some(error.clone());
                true
            } else {
                false
            }
        };
        let observer = changed.then(|| self.observer.borrow().clone()).flatten();
        let queued = self.queued.borrow_mut().take();
        if !changed && queued.is_none() {
            return;
        }
        let mut panic = None;
        if let Some(mut submission) = queued {
            capture(&mut panic, || submission.refuse(error.clone()));
        }
        if let Some(observer) = observer {
            capture(&mut panic, || observer.admission_closed());
        }
        capture(&mut panic, || self.wake.wake_by_ref());
        finish_panic(panic);
    }
}

fn finish_panic(panic: Option<PanicPayload>) {
    if let Some(panic) = panic {
        if std::thread::panicking() {
            discard_panic(panic);
        } else {
            resume_unwind(panic);
        }
    }
}

/// Owner-local upstream admission and connection lifetime controls.
#[derive(Clone)]
pub struct OwnedH1UpstreamHandle(Rc<Shared>);

impl OwnedH1UpstreamHandle {
    /// 1 while the connection's slot is free and admission is open, otherwise 0.
    pub fn available_capacity(&self) -> u32 {
        if self.0.complete.get() {
            0
        } else {
            self.0.available()
        }
    }

    /// 1 while the connection's slot is held only by an exchange whose request is written and
    /// whose response is complete and read to its end (its source completed), on a connection
    /// that `Http1xCodec` left reusable after that response: unless the connection fails first,
    /// the driver's next turn, which the source's completion woke, retires the exchange and
    /// returns the slot (`capacity_available`). Otherwise 0.
    pub fn returning_capacity(&self) -> u32 {
        let shared = &self.0;
        u32::from(
            shared
                .retiring
                .borrow()
                .as_ref()
                .is_some_and(|signals| signals.source_complete.get())
                && shared.closed.borrow().is_none()
                && !shared.cancelled()
                && !shared.complete.get(),
        )
    }

    /// Installs the connection's one observer. Fails with `INVALID_STATE_TRANSITION` once an
    /// observer is installed or the session has finished; when admission is already closed,
    /// the observer gets `admission_closed` before this returns.
    pub fn set_observer(&self, observer: Rc<dyn OwnedUpstreamObserver>) -> Result<(), HttpError> {
        if self.0.complete.get() || self.0.observer.borrow().is_some() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned upstream observer is already installed or session finished",
            ));
        }
        *self.0.observer.borrow_mut() = Some(Rc::clone(&observer));
        if self.0.closed.borrow().is_some() {
            observer.admission_closed();
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.0.complete.get()
    }

    /// Includes response consumers, upload retirement and native write ownership. The driver
    /// recomputes it on each quiescent turn, so a slot returned uncommitted leaves the
    /// connection idle again once the driver has run.
    pub fn is_idle(&self) -> bool {
        !self.is_finished() && self.0.idle.get() && self.0.used.get() == 0
    }

    /// Fails the connection with `CORO_CANCELLED`: admission closes, the live exchange is
    /// cancelled and dropped and the transport is reset, on the worker, before `run` completes.
    pub fn cancel(&self) {
        self.0.cancel.set(true);
        self.0.idle.set(false);
        self.0.wake.wake_by_ref();
    }

    /// Closes admission and closes the connection once its exchange, if any, retires.
    /// Returns whether this call changed the session's state.
    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        if self.is_finished() {
            return Ok(false);
        }
        let changed = !self.0.drain.replace(true);
        self.0.close(failure(
            HttpErrorCode::REFUSED_STREAM,
            b"owned upstream connection is draining",
        ));
        Ok(changed)
    }

    /// Takes the connection's one slot. Fails with `REFUSED_STREAM` while the slot is taken or
    /// admission is closed.
    pub fn reserve_request(&self) -> Result<OwnedH1UpstreamReservation, HttpError> {
        if self.available_capacity() == 0 {
            return Err(failure(
                HttpErrorCode::REFUSED_STREAM,
                b"owned upstream admission is unavailable",
            ));
        }
        self.0.used.set(1);
        self.0.idle.set(false);
        Ok(OwnedH1UpstreamReservation {
            shared: Rc::clone(&self.0),
            active: true,
        })
    }
}

/// The connection's one admission slot, returned exactly once if it is not committed.
pub struct OwnedH1UpstreamReservation {
    shared: Rc<Shared>,
    active: bool,
}

/// What releases a submission's waiter.
enum ResponseWait {
    Commitment,
    /// The first published head or the response's end, with the request producer's
    /// completion when the submitter's deadline observes it.
    FirstHead(Option<IngressCompletion>),
}

impl OwnedH1UpstreamReservation {
    /// Submits a request while preserving its fresh response read capability.
    pub fn submit_exclusive(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH1ExclusiveSubmission {
        OwnedH1ExclusiveSubmission(self.submit(headers, source, cancellation))
    }

    /// Submits a request and waits for its first published response head, preserving the
    /// fresh response read capability, as
    /// `OwnedH2UpstreamReservation::submit_with_response_interest`. Codec commitment alone does
    /// not notify this waiter. Informational heads also satisfy this interest and remain
    /// ordered in the original source for the caller to drain. A response that ends or fails
    /// before any head also releases the waiter. Upload, source and physical-write retirement
    /// remain independent of this notification. The caller retains timeout responsibility
    /// while waiting, including any timeout anchored to request-producer EOM.
    ///
    /// A caller whose deadline observes the request producer's completion supplies it as
    /// `producer_completion`, unless that producer has already succeeded: the outcome is
    /// first-wins, so such a producer cannot fail. Until the caller takes the response or the
    /// response ends before that producer fails, an upload that fails after that producer has
    /// failed selects the producer's error for the request. Otherwise a failed upload fails the
    /// response with the session's upload-failure outcome. Either way the connection closes.
    pub fn submit_with_response_interest(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        producer_completion: Option<IngressCompletion>,
    ) -> OwnedH1ExclusiveSubmission {
        OwnedH1ExclusiveSubmission(self.submit_impl(
            headers,
            source,
            cancellation,
            ResponseWait::FirstHead(producer_completion),
        ))
    }

    /// Takes the already-read request head and its original body-only source.
    /// The returned future acknowledges codec commitment, not a native write.
    pub fn submit(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH1UpstreamSubmission {
        self.submit_impl(headers, source, cancellation, ResponseWait::Commitment)
    }

    fn submit_impl(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        wait: ResponseWait,
    ) -> OwnedH1UpstreamSubmission {
        let (response_interest, producer) = match wait {
            ResponseWait::Commitment => (false, None),
            ResponseWait::FirstHead(completion) => (true, completion),
        };
        let shared = Rc::clone(&self.shared);
        let status = Rc::new(RequestStatus {
            shared: Rc::clone(&shared),
            stream: Cell::new(None),
            ingress: RefCell::new(None),
            counted: Cell::new(false),
            released: Cell::new(false),
            cancellation,
            error: RefCell::new(None),
            response: RefCell::new(None),
            response_ready: Cell::new(false),
            response_interest,
            response_published: Cell::new(false),
            delivered: Cell::new(false),
            awaiting_delivery: Cell::new(true),
            released_undelivered: Cell::new(false),
            response_observation_attached: Cell::new(false),
            response_observer: RefCell::new(None),
            live: Cell::new(false),
            waiter: RefCell::new(None),
            unprocessed: Cell::new(false),
            sequence: Cell::new(None),
        });
        let cancellation_wake = status.cancellation.can_be_cancelled().then(|| {
            let mut registration = status.cancellation.wake_registration();
            registration.update_waker(&Waker::from(Arc::clone(&shared.wake)));
            registration
        });
        let mut submission = Submission {
            cancellation_wake,
            reservation: Some(self),
            headers: Some(headers),
            source: Some(source.into()),
            producer,
            status: Rc::clone(&status),
        };
        let result = OwnedH1UpstreamSubmission {
            status,
            complete: false,
        };
        let closed = shared.closed.borrow().clone();
        if let Some(error) = closed {
            submission.refuse(error);
        } else {
            *shared.queued.borrow_mut() = Some(submission);
            shared.wake.wake_by_ref();
        }
        result
    }
}

impl Drop for OwnedH1UpstreamReservation {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.shared.release(None, None);
        }
    }
}

/// A cancellation-safe acknowledgment of the driver's request commitment.
pub struct OwnedH1UpstreamSubmission {
    status: Rc<RequestStatus>,
    complete: bool,
}

impl OwnedH1UpstreamSubmission {
    fn poll_response(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedH1ExclusiveResponse, HttpError>> {
        // As OwnedH2: a cause selected before the response is claimed fails the submission,
        // whichever of it and the driver is polled first.
        if self.status.response_ready.get()
            && let Some(error) = self.status.selected_error()
        {
            self.status.fail(error);
        }
        if self.status.response_ready.get()
            && (!self.status.response_interest
                || self.status.response_published.get()
                || self.status.error.borrow().is_some())
        {
            let response = self
                .status
                .response
                .borrow_mut()
                .take()
                .expect("submission acknowledgment is consumed once");
            self.status.delivered.set(true);
            self.complete = true;
            self.status.retire_delivery_wait();
            return Poll::Ready(response.map(|(stream, source)| OwnedH1ExclusiveResponse {
                stream,
                source,
                control: OwnedH1UpstreamRequest(Rc::clone(&self.status)),
            }));
        }
        let previous = self
            .status
            .waiter
            .borrow_mut()
            .replace(context.waker().clone());
        drop(previous);
        Poll::Pending
    }
}

impl OwnedH1UpstreamSubmission {
    /// Retains the first explicit cause, including a caller's route timeout. Selected before
    /// the response is claimed, it fails the submission.
    pub fn cancel(&self, error: HttpError) {
        self.status.cancel(error);
    }

    #[cold]
    #[inline(never)]
    fn cancel_unresolved(&mut self) {
        let error = if self.status.response_interest && self.status.response_ready.get() {
            HttpError::new(HttpErrorCode::CANCEL)
        } else {
            failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request submission was dropped",
            )
        };
        let mut panic = None;
        capture(&mut panic, || self.status.cancel(error.clone()));
        // A ready but unclaimed raw source must not hold its own cancellation fence.
        capture(&mut panic, || self.status.fail(error));
        capture(&mut panic, || self.status.retire_delivery_wait());
        finish_panic(panic);
    }
}

/// Dropping the submission after commitment and before it resolves cancels the request with
/// `CANCEL` when it waits for response interest, otherwise with `CORO_CANCELLED`; a committed
/// HTTP/1.1 request cannot be reset alone, so the connection closes.
impl Drop for OwnedH1UpstreamSubmission {
    fn drop(&mut self) {
        if !self.complete {
            self.cancel_unresolved();
        }
    }
}

impl Future for OwnedH1UpstreamSubmission {
    type Output = Result<OwnedH1UpstreamResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_response(context).map(|result| {
            result.map(|response| OwnedH1UpstreamResponse {
                stream: response.stream,
                source: response.source.into_source_owner(),
                control: response.control,
            })
        })
    }
}

/// A submission preserving the response's fresh exclusive capability. Ordinary submissions
/// acknowledge codec commitment; response-interest submissions additionally wait for the
/// first published response head or the response's end.
pub struct OwnedH1ExclusiveSubmission(OwnedH1UpstreamSubmission);

impl OwnedH1ExclusiveSubmission {
    /// Retains the first explicit cause through the ordinary submission owner.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }
}

impl Future for OwnedH1ExclusiveSubmission {
    type Output = Result<OwnedH1ExclusiveResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_response(context)
    }
}

/// The fresh response source and the request's capacity observation.
pub struct OwnedH1ExclusiveResponse {
    pub stream: StreamId,
    pub source: HttpResponseSource,
    pub control: OwnedH1UpstreamRequest,
}

/// The original response source and a sticky capacity observation.
pub struct OwnedH1UpstreamResponse {
    pub stream: StreamId,
    pub source: HttpStreamSourceOwner,
    pub control: OwnedH1UpstreamRequest,
}

impl OwnedH1UpstreamResponse {
    /// Converts the original response to the generic source interface without reading it.
    pub fn into_source_parts(self) -> (StreamId, HttpSourceHolder, OwnedH1UpstreamRequest) {
        (self.stream, self.source.into_source_holder(), self.control)
    }
}

#[derive(Clone)]
pub struct OwnedH1UpstreamRequest(Rc<RequestStatus>);

impl OwnedH1UpstreamRequest {
    pub fn capacity_released(&self) -> bool {
        self.0.released.get()
    }

    /// Selects `error` for the request; before both messages end, the connection closes.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }

    /// Attaches one response observer, or immediately delivers an already-published result.
    /// Callbacks run outside request-state borrows. Repeated attachment is rejected.
    pub fn observe_response(
        &self,
        observer: Box<dyn OwnedResponseObserver>,
    ) -> Result<(), HttpError> {
        if self.0.response_observation_attached.get() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation is already attached",
            ));
        }
        let completed = self
            .0
            .ingress
            .borrow()
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        if let Some(completed) = completed {
            self.0.response_observation_attached.set(true);
            observer.response_complete(completed);
            return Ok(());
        }
        if !self.0.live.get() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation requires a live stream",
            ));
        }
        self.0.response_observation_attached.set(true);
        *self.0.response_observer.borrow_mut() = Some(observer);
        self.0.shared.wake.wake_by_ref();
        Ok(())
    }
}

impl OwnedRequestProgressExt for OwnedH1UpstreamSubmission {
    fn request_unprocessed(&self) -> bool {
        self.status.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.status.sequence.get()
    }
}

impl OwnedRequestProgressExt for OwnedH1ExclusiveSubmission {
    fn request_unprocessed(&self) -> bool {
        self.0.request_unprocessed()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence_number()
    }
}

impl OwnedRequestProgressExt for OwnedH1UpstreamRequest {
    fn request_unprocessed(&self) -> bool {
        self.0.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence.get()
    }
}

struct RequestStatus {
    shared: Rc<Shared>,
    stream: Cell<Option<StreamId>>,
    ingress: RefCell<Option<IngressCompletion>>,
    counted: Cell<bool>,
    released: Cell<bool>,
    cancellation: CancellationToken,
    error: RefCell<Option<HttpError>>,
    response: RefCell<Option<Result<(StreamId, HttpResponseSource), HttpError>>>,
    response_ready: Cell<bool>,
    response_interest: bool,
    response_published: Cell<bool>,
    delivered: Cell<bool>,
    awaiting_delivery: Cell<bool>,
    released_undelivered: Cell<bool>,
    response_observation_attached: Cell<bool>,
    response_observer: RefCell<Option<Box<dyn OwnedResponseObserver>>>,
    /// The exchange is live in the driver, which polls an attached observer.
    live: Cell<bool>,
    waiter: RefCell<Option<Waker>>,
    /// Its connection refused it before commit (`DIV-035`).
    unprocessed: Cell<bool>,
    /// The connection's earlier commits, fixed at commit or at that refusal (`DIV-035`).
    sequence: Cell<Option<u64>>,
}

impl RequestStatus {
    fn ingress_succeeded(&self) -> bool {
        self.ingress
            .borrow()
            .as_ref()
            .is_some_and(|receipt| matches!(receipt.snapshot(), Some(Ok(_))))
    }

    fn selected_error(&self) -> Option<HttpError> {
        if let Some(error) = &*self.error.borrow() {
            return Some(error.clone());
        }
        self.cancellation.is_cancellation_requested().then(|| {
            failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request body production cancelled",
            )
        })
    }

    fn cancel(&self, error: HttpError) {
        self.error.borrow_mut().get_or_insert(error);
        self.shared.wake.wake_by_ref();
    }

    /// Whether a cause is selected for the request: its consumer's cancel (the request's or its
    /// submission's cancel, the submission's drop, or its cancellation token), or a supplied
    /// producer's failure, which is selected only before the response is delivered.
    fn cause_selected(&self) -> bool {
        self.error.borrow().is_some() || self.cancellation.is_cancellation_requested()
    }

    fn notify(&self) {
        let waiter = self.waiter.borrow_mut().take();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn fail(&self, error: HttpError) {
        let error = {
            let mut selected = self.error.borrow_mut();
            selected.get_or_insert(error).clone()
        };
        if !self.delivered.get() {
            let old = self.response.borrow_mut().replace(Err(error));
            self.response_ready.set(true);
            let mut panic = None;
            capture(&mut panic, || drop(old));
            capture(&mut panic, || self.notify());
            finish_panic(panic);
        }
    }

    fn acknowledge(&self, stream: StreamId, source: HttpResponseSource) {
        assert!(!self.response_ready.get(), "request is acknowledged once");
        *self.response.borrow_mut() = Some(Ok((stream, source)));
        self.response_ready.set(true);
        if !self.response_interest || self.response_published.get() {
            self.notify();
        }
    }

    /// Releases a response-interest waiter once its source holds a first head or a terminal
    /// outcome, which the caller then reads from the source.
    fn publish_response(&self) {
        if self.response_interest
            && !self.response_published.replace(true)
            && self.response_ready.get()
        {
            self.notify();
        }
    }

    fn release(&self) {
        if self.counted.replace(false) {
            self.released.set(true);
            let undelivered = self
                .awaiting_delivery
                .get()
                .then_some(&self.released_undelivered);
            self.shared.release(self.stream.get(), undelivered);
        }
    }

    fn retire_delivery_wait(&self) {
        self.awaiting_delivery.set(false);
        if self.released_undelivered.replace(false) {
            let observer = self.shared.observer.borrow().clone();
            if let Some(observer) = observer {
                observer.released_submission_retired();
            }
        }
    }
}

struct Submission {
    cancellation_wake: Option<CancellationWakeRegistration>,
    reservation: Option<OwnedH1UpstreamReservation>,
    headers: Option<HttpHeaderEvent>,
    source: Option<HttpBodySource>,
    producer: Option<IngressCompletion>,
    status: Rc<RequestStatus>,
}

impl Submission {
    /// Rejects the submission because its connection's admission closed before commit: unless
    /// its consumer selected a cause, it failed unprocessed (`DIV-035`). One sample of the cause
    /// decides both, as a cancellation can arrive from another thread.
    fn refuse(&mut self, error: HttpError) {
        let selected = self.status.selected_error();
        #[cfg(test)]
        crate::experimental::refusal_cause_sampled();
        let error = selected.unwrap_or_else(|| {
            self.status.unprocessed.set(true);
            self.status
                .sequence
                .set(Some(self.status.shared.committed.get()));
            error
        });
        self.reject_selected(error);
    }

    fn reject(&mut self, error: HttpError) {
        let error = self.status.selected_error().unwrap_or(error);
        self.reject_selected(error);
    }

    fn reject_selected(&mut self, error: HttpError) {
        let mut panic = None;
        capture(&mut panic, || self.status.fail(error.clone()));
        if let Some(headers) = self.headers.as_mut() {
            for registration in headers.byte_event_registrations_mut() {
                capture(&mut panic, || registration.cancel(&error, None));
            }
        }
        capture(&mut panic, || drop(self.headers.take()));
        if let Some(source) = self.source.as_mut() {
            capture(&mut panic, || source.clear(Some(error.code())));
        }
        capture(&mut panic, || drop(self.source.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.reservation.take()));
        finish_panic(panic);
    }
}

impl Drop for Submission {
    fn drop(&mut self) {
        if self.reservation.is_some() {
            self.reject(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request handoff retired before commitment",
            ));
        }
    }
}

/// The one committed exchange: its upload and its response producer.
struct Exchange {
    status: Rc<RequestStatus>,
    id: StreamId,
    signals: Rc<IngressSignals>,
    producer: HttpStreamSourceProducer,
    transfer: Option<HttpBodyTransfer>,
    unused_source: Option<HttpBodySource>,
    /// A suspended upload's resume, polled before the next body read.
    resume: Option<ResumeFuture>,
    /// Wakes the driver when the request's cancellation token is cancelled.
    cancellation_wake: Option<CancellationWakeRegistration>,
    response_observer: Option<Box<dyn OwnedResponseObserver>>,
    /// The supplied request producer's completion while its failure can still be selected.
    request_producer: Option<IngressCompletion>,
    /// The request's declared `Content-Length`, which its body must match.
    expected_request_length: Option<u64>,
    /// Request body bytes serialized so far.
    request_body_length: u64,
    /// The request's end is serialized.
    request_complete: bool,
    /// The response's end was parsed.
    response_complete: bool,
    /// The caller's response source completed.
    consumed: bool,
    write_boundary: u64,
}

impl Exchange {
    /// Whether `cancel_error` selects a cause, without building it. Its terms are pure, so
    /// their order sets only the cost: the worker's flag, one pointer further away, comes last.
    #[inline]
    fn cancel_requested(&self) -> bool {
        let shared = &self.status.shared;
        shared.cancel.get()
            || (!(self.request_complete && self.response_complete)
                && (self.status.error.borrow().is_some()
                    || self.status.cancellation.is_cancellation_requested()))
            || shared.context.is_cancelled()
    }

    /// What stops the exchange now: a connection cancel, or the request's selected cause
    /// while it still closes the connection (an HTTP/1.1 exchange cannot be reset alone before
    /// both messages end). The cause is selected only once `cancel_requested` holds.
    #[inline]
    fn cancel_error(&self) -> Option<HttpError> {
        if self.cancel_requested() {
            self.select_cancel_error()
        } else {
            None
        }
    }

    #[cold]
    #[inline(never)]
    fn select_cancel_error(&self) -> Option<HttpError> {
        #[cfg(test)]
        count(&CANCEL_SELECTIONS);
        if self.status.shared.cancelled() {
            return Some(cancelled());
        }
        if self.request_complete && self.response_complete {
            return None;
        }
        self.status.selected_error()
    }

    /// As OwnedH2: the sticky producer result takes precedence over the observer's deadline.
    fn poll_response_observer(&mut self, context: &mut Context<'_>) {
        if self.response_observer.is_none() {
            self.response_observer = self.status.response_observer.borrow_mut().take();
        }
        let Some(observer) = self.response_observer.as_mut() else {
            self.retire_producer_watch();
            return;
        };
        let completed = self
            .status
            .ingress
            .borrow()
            .as_ref()
            .and_then(IngressCompletion::snapshot);
        let completed = match completed {
            Some(completed) => completed,
            None => match observer.poll_deadline(context) {
                Poll::Ready(error) => Err(error),
                Poll::Pending => return,
            },
        };
        self.response_observer
            .take()
            .expect("a polled response observer is owned")
            .response_complete(completed);
    }

    fn retire_response_observer(&mut self) {
        let observer = self
            .response_observer
            .take()
            .or_else(|| self.status.response_observer.borrow_mut().take());
        if let Some(observer) = observer {
            let completed = self
                .status
                .ingress
                .borrow()
                .as_ref()
                .and_then(IngressCompletion::snapshot);
            if let Some(completed) = completed {
                observer.response_complete(completed);
            }
        }
    }

    /// Reads at most `maximum` request body bytes, after any suspension resumes, as
    /// `OwnedH2UpstreamSession` polls its upload.
    fn poll_upload(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Result<Option<HttpBodyEvent>, HttpError> {
        if let Some(resume) = &mut self.resume {
            match resume.as_mut().poll(context) {
                Poll::Pending => return Ok(None),
                Poll::Ready(ResumeStatus::Cancelled) => return Err(resume_cancelled()),
                Poll::Ready(_) => {
                    self.resume = None;
                }
            }
            // The resume future is caller code: it may cancel the connection or the request.
            if self.cancel_error().is_some() {
                return Ok(None);
            }
        }
        let transfer = self
            .transfer
            .as_mut()
            .expect("an unfinished upload retains its transfer");
        match transfer.poll_resume(context) {
            Poll::Pending => return Ok(None),
            Poll::Ready(Some(ResumeStatus::Cancelled)) => return Err(resume_cancelled()),
            Poll::Ready(Some(_)) if self.status.selected_error().is_some() => return Ok(None),
            Poll::Ready(_) => {}
        }
        let Poll::Ready(result) =
            transfer.poll_read_body_event(context, maximum, &self.status.cancellation)
        else {
            return Ok(None);
        };
        result.map(Some)
    }

    /// Stops watching the supplied producer once it has succeeded, which is terminal, or once
    /// the response has ended while it has not failed.
    fn retire_producer_watch(&mut self) {
        let Some(producer) = &self.request_producer else {
            return;
        };
        let retire = match producer.snapshot() {
            Some(Ok(_)) => true,
            Some(Err(_)) => false,
            None => self
                .status
                .ingress
                .borrow()
                .as_ref()
                .is_some_and(|ingress| ingress.snapshot().is_some()),
        };
        if retire {
            self.request_producer = None;
        }
    }

    /// Selects the supplied request producer's failure for an upload that failed before the
    /// submitter took the response and before the response ended.
    fn select_producer_failure(&self) -> Option<HttpError> {
        if self.status.delivered.get() {
            return None;
        }
        let Some(Err(error)) = self
            .request_producer
            .as_ref()
            .and_then(IngressCompletion::snapshot)
        else {
            return None;
        };
        Some(self.status.error.borrow_mut().get_or_insert(error).clone())
    }

    /// The connection's error for a failed upload: the selected producer failure, or the
    /// upload's own error with the response failed as the session's upload-failure outcome.
    #[cold]
    fn upload_failed(&self, error: HttpError) -> HttpError {
        if let Some(selected) = self.select_producer_failure() {
            return selected;
        }
        self.producer.abort_with_error(
            failure(HttpErrorCode::CORO_CANCELLED, b"egress source failed"),
            b"egress source failed",
        );
        error
    }

    /// Fails the response with `error` and stops the upload. A cancel the request's consumer
    /// selected discards the response's queued events, as `UniplexStreamAbort` does (`DIV-033`).
    fn cancel(&mut self, error: &HttpError) {
        let retirement = self
            .transfer
            .as_mut()
            .map(|transfer| transfer.prepare_stop_and_clear(error.clone()));
        // A selected cause is the consumer's cancel, or a producer failure whose response is
        // then dropped undelivered: neither keeps the response's queued events.
        let discard = self.status.cause_selected();
        let mut panic = None;
        capture(&mut panic, || {
            let details = b"owned H1 upstream exchange cancelled";
            if discard {
                self.producer
                    .abort_discarding_queued(error.clone(), details)
            } else {
                self.producer.abort_with_error(error.clone(), details)
            }
        });
        if !self.status.response_ready.get() || self.status.selected_error().is_some() {
            capture(&mut panic, || self.status.fail(error.clone()));
        } else {
            capture(&mut panic, || self.status.publish_response());
        }
        if let Some(source) = self.unused_source.as_mut() {
            capture(&mut panic, || source.clear(Some(error.code())));
        }
        capture(&mut panic, || drop(self.unused_source.take()));
        if let Some(retirement) = retirement
            && let Err(payload) = retirement.retire()
        {
            if panic.is_none() {
                panic = Some(payload);
            } else {
                discard_panic(payload);
            }
        }
        if let Some(transfer) = self.transfer.as_mut() {
            capture(&mut panic, || transfer.drop_pending_read());
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        self.request_complete = true;
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        let mut panic = None;
        if !self.request_complete {
            capture(&mut panic, || {
                self.cancel(&failure(
                    HttpErrorCode::CORO_CANCELLED,
                    b"upstream upload owner retired",
                ))
            });
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        capture(&mut panic, || drop(self.unused_source.take()));
        self.status.live.set(false);
        capture(&mut panic, || self.status.release());
        capture(&mut panic, || self.status.publish_response());
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }
}

#[cold]
#[inline(never)]
fn request_length_mismatch() -> HttpError {
    failure(
        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
        b"request Content-Length did not match body bytes",
    )
}

#[cold]
#[inline(never)]
fn resume_cancelled() -> HttpError {
    failure(
        HttpErrorCode::CORO_CANCELLED,
        b"request body resume cancelled",
    )
}

/// A poll-driven HTTP/1.1 upstream connection with one exchange at a time.
pub struct OwnedH1UpstreamSession(UpstreamDriver);

impl OwnedH1UpstreamSession {
    pub fn new(options: OwnedH1Options) -> Result<(Self, OwnedH1UpstreamHandle), HttpError> {
        let (driver, handle) = UpstreamDriver::new(options)?;
        Ok((Self(driver), handle))
    }

    /// Runs inside the caller's registered worker cleanup scope, as `OwnedH2Session::run`.
    pub fn run(self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        UpstreamRun(Some(self.0))
    }
}

/// Drives an [`UpstreamDriver`] to completion, isolating callback panics in every poll.
struct UpstreamRun(Option<UpstreamDriver>);

impl Future for UpstreamRun {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let driver = this
            .0
            .as_mut()
            .expect("owned H1 upstream runner polled after completion");
        driver.started = true;
        match catch_unwind(AssertUnwindSafe(|| driver.poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                let mut driver = this
                    .0
                    .take()
                    .expect("owned H1 upstream runner completes once");
                driver.shared.complete.set(true);
                if let Some(panic) = driver.panic.take() {
                    resume_unwind(panic);
                }
                Poll::Ready(result)
            }
            Err(panic) => {
                driver.retain_panic(panic);
                driver.abort(failure(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"owned H1 upstream callback panicked",
                ));
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct UpstreamDriver {
    context: WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    shared: Rc<Shared>,
    config: UniplexConfig,
    codec: IngressCodec,
    exchange: Option<Box<Exchange>>,
    ingress: BufQueue,
    egress: BufQueue,
    read: Option<LocalTransportFuture<Result<UniplexRead, HttpError>>>,
    write: Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<Box<dyn UniplexWriteQueue>>,
    task: Arc<TaskWaker>,
    read_wake: Arc<Signal>,
    read_waker: Waker,
    write_wake: Arc<Signal>,
    write_waker: Waker,
    stream_wake: Arc<Signal>,
    stream_waker: Waker,
    cancellation: CancellationSource,
    write_cancellation: CancellationSource,
    eof: bool,
    eof_processed: bool,
    /// A failed read, applied once the bytes read before it are parsed.
    read_error: Option<HttpError>,
    /// An applied failed read whose connection resets once the request bytes already
    /// serialized are written; the run's error.
    failed_read: Option<HttpError>,
    /// Parsing waits while the unread response body is at `ingress_buffer_limit`.
    flow_pause: bool,
    /// The response ended; nothing more is parsed on this connection.
    response_pause: bool,
    /// The request body was not polled because the next write must make room for it.
    body_awaits_egress: bool,
    fatal: Option<HttpError>,
    panic: Option<PanicPayload>,
    transport_reset: bool,
    write_end: u64,
    summary: UniplexRunSummary,
    budget_yield: Option<LocalFuture>,
    started: bool,
    /// `Http1UpgradePolicy::AsOrdinaryRequest`: CONNECT and `Upgrade` requests are sent as
    /// ordinary requests (`DIV-031`).
    ordinary_upgrades: bool,
}

/// The codec callback: borrows what one response `on_ingress` call may change.
struct ResponseInput<'a> {
    config: &'a UniplexConfig,
    exchange: &'a mut Option<Box<Exchange>>,
    stream_wake: &'a Arc<Signal>,
    flow_pause: &'a mut bool,
    response_pause: &'a mut bool,
    failure: Option<HttpError>,
}

impl ResponseInput<'_> {
    fn fail(&mut self, error: HttpError) {
        self.failure.get_or_insert(error);
    }
}

impl HttpCodecCallback for ResponseInput<'_> {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let mut actions = Vec::new();
        if self.failure.is_some() {
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        let Some(exchange) = self.exchange.as_mut() else {
            self.fail(failure(
                HttpErrorCode::PROTOCOL_ERROR,
                b"owned H1 upstream response without a request",
            ));
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        };
        // Publishing an event can wake code that cancels: the next event is not parsed for it.
        if let Some(error) = exchange.cancel_error() {
            self.fail(error);
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        match event {
            CodecEvent::HeadersComplete { message, .. } => {
                if !exchange.producer.headers_boxed(message, false) {
                    self.fail(failure(
                        HttpErrorCode::HEADER_PARSE_ERROR,
                        b"upstream source rejected response headers",
                    ));
                } else {
                    exchange.status.publish_response();
                }
                self.stream_wake.mark();
            }
            CodecEvent::Body { body, padding, .. } => {
                match exchange.producer.body(body, padding, false) {
                    FlowControlState::Error => self.fail(failure(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"upstream source rejected body bytes",
                    )),
                    state
                        if state == FlowControlState::Closed
                            || exchange.producer.body_bytes_buffered().is_some_and(
                                |buffered| buffered >= self.config.ingress_buffer_limit,
                            ) =>
                    {
                        *self.flow_pause = true;
                        actions.push(CodecCallbackAction::SetParserPaused(true));
                    }
                    _ => {}
                }
            }
            CodecEvent::TrailersComplete { trailers, .. } => {
                // As OwnedH2: the source's `trailers` leaves the body open for a later EOM, and
                // the trailers end this message, so EOM follows them here.
                exchange.response_complete = true;
                if !(exchange.producer.trailers(*trailers) && exchange.producer.eom()) {
                    self.fail(failure(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"upstream source rejected trailers",
                    ));
                }
                self.stream_wake.mark();
            }
            CodecEvent::MessageComplete { .. } => {
                if !exchange.response_complete {
                    exchange.response_complete = true;
                    // A rejected EOM leaves the response source terminal, and the caller reads
                    // that error from the source.
                    if !exchange.producer.eom() {
                        exchange.status.publish_response();
                    }
                }
                self.stream_wake.mark();
                *self.response_pause = true;
                actions.push(CodecCallbackAction::SetParserPaused(true));
            }
            CodecEvent::Error { error, .. } => {
                let code = http_exception_to_http_error_code(error);
                self.fail(HttpError::with_message(code, error.message()));
            }
            _ => {}
        }
        actions.into()
    }
}

impl UpstreamDriver {
    fn new(options: OwnedH1Options) -> Result<(Self, OwnedH1UpstreamHandle), HttpError> {
        let OwnedH1Options {
            context,
            transport,
            io,
            http1,
        } = options;
        let io = io.validate()?;
        // As `OwnedH1Session::new`, except that an upstream request body is never drained
        // on drop (the rule of `OwnedH2Session::new` upstream).
        if !context.is_owner_thread()
            || context.is_cancelled()
            || io.maximum_write_size > io.buffer_limit
            || io.maximum_write_size <= CHUNK_OVERHEAD
            || !io.read_timeout.is_zero()
            || !io.stream_read_timeout.is_zero()
            || !io.write_timeout.is_zero()
            || io.drain_request_body_on_drop
            || io.wait_for_http1_response_on_drain
            || io.abort_exchanges_on_eof
            || io.exact_method_tokens
            || transport.byte_event_capability(&context)? != ByteEventCapability::Unsupported
        {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 upstream configuration is not supported",
            ));
        }
        let task = Arc::new(TaskWaker::default());
        let read_wake = Signal::new(&task);
        let write_wake = Signal::new(&task);
        let stream_wake = Signal::new(&task);
        let shared = Rc::new(Shared {
            context: context.clone(),
            wake: Arc::clone(&stream_wake),
            complete: Cell::new(false),
            cancel: Cell::new(false),
            drain: Cell::new(false),
            idle: Cell::new(true),
            used: Cell::new(0),
            spent: Cell::new(false),
            committed: Cell::new(0),
            retiring: RefCell::new(None),
            closed: RefCell::new(None),
            observer: RefCell::new(None),
            queued: RefCell::new(None),
        });
        let cancellation = CancellationSource::new();
        let write_cancellation = CancellationSource::new();
        let write_queue = transport.try_write_queue(
            &context,
            io.maximum_write_size,
            io.write_timeout,
            write_cancellation.token(),
        )?;
        let mut codec =
            Http1xCodec::with_options(TransportDirection::Upstream, false, http1.strict_validation);
        let ordinary_upgrades = http1.upgrade_policy == Http1UpgradePolicy::AsOrdinaryRequest;
        codec.set_upstream_upgrade_as_ordinary(ordinary_upgrades);
        codec.set_unframed_bodiless_requests(io.unframed_bodiless_http1_requests);
        Ok((
            Self {
                context,
                transport,
                shared: Rc::clone(&shared),
                config: io,
                codec: IngressCodec::new(codec),
                exchange: None,
                ingress: BufQueue::new(io.buffer_limit),
                egress: BufQueue::new(io.buffer_limit),
                read: None,
                write: None,
                write_queue,
                task,
                read_waker: Waker::from(Arc::clone(&read_wake)),
                read_wake,
                write_waker: Waker::from(Arc::clone(&write_wake)),
                write_wake,
                stream_waker: Waker::from(Arc::clone(&stream_wake)),
                stream_wake,
                cancellation,
                write_cancellation,
                eof: false,
                eof_processed: false,
                read_error: None,
                failed_read: None,
                flow_pause: false,
                response_pause: false,
                body_awaits_egress: false,
                fatal: None,
                panic: None,
                transport_reset: false,
                write_end: 0,
                summary: UniplexRunSummary::default(),
                budget_yield: None,
                started: false,
                ordinary_upgrades,
            },
            OwnedH1UpstreamHandle(shared),
        ))
    }

    fn retain_panic(&mut self, panic: PanicPayload) {
        if self.panic.is_none() {
            self.panic = Some(panic);
        } else {
            discard_panic(panic);
        }
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRunSummary, HttpError>> {
        self.task.register(context.waker());
        if self.budget_yield.is_some() && self.poll_budget_yield(context).is_pending() {
            return Poll::Pending;
        }
        let mut turn = 0;
        while turn < TURN_BUDGET {
            #[cfg(test)]
            count(&DRIVER_TURNS);
            self.observe_cancel();
            let mut progress = false;
            if let Some(wrote) = self.poll_write() {
                progress |= wrote;
                self.observe_cancel();
            }
            if let Some(read) = self.poll_read(&mut turn) {
                progress |= read;
                self.observe_cancel();
            }
            if self.fatal.is_none() && self.poll_admission() {
                progress = true;
                self.observe_cancel();
            }
            if self.fatal.is_none() && !self.ingress.is_empty() && self.exchange.is_none() {
                self.abort(failure(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"owned H1 upstream response without a request",
                ));
            }
            if self.parse_due() {
                match self.parse_input() {
                    Ok(parsed) => progress |= parsed,
                    Err(error) => self.abort(error),
                }
            }
            // While flow control pauses an unfinished response, the bytes read before the
            // failure are not all parsed yet: it waits for them.
            if !(self.flow_pause && self.codec.is_busy())
                && let Some(error) = self.read_error.take()
            {
                self.fail_read(error);
                progress = true;
            }
            if self.fatal.is_none() && self.stream_wake.take_ready() {
                progress = true;
                if let Err(error) = self.poll_exchange() {
                    self.abort(error);
                }
                self.observe_cancel();
            }
            if self.fatal.is_none() && self.settle_exchange() {
                progress = true;
                self.observe_cancel();
            }
            // The write waits while a phase before it can still add to it: one that acted this
            // turn may act again. When none would, it starts in place of the next turn, which
            // still counts against the budget that places the write it forces, the idleness it
            // notes and the yield.
            let write_waits = progress
                && self.egress.chain_length() < self.config.maximum_write_size
                && turn + 1 < TURN_BUDGET;
            if !self.transport_reset
                && !self.egress.is_empty()
                && self.can_start_write()
                && (!write_waits || !self.phases_due_before_write())
            {
                if write_waits {
                    turn += 1;
                }
                // A write queue's capacity check, like the capacity callbacks before it, may
                // cancel the connection or the request; the abort discards the egress.
                self.observe_cancel();
                if !self.transport_reset
                    && let Err(error) = self.start_write()
                {
                    self.abort(error);
                }
                progress = true;
            }
            if self.failed_read.is_some() && self.egress.is_empty() && !self.writes_pending() {
                let error = self.failed_read.take().expect("checked above");
                self.abort(error);
            }
            if self.fatal.is_some() && !self.transport_reset {
                self.reset_transport();
                progress = true;
            }
            if self.transport_reset
                && !self.writes_pending()
                && self.read.is_none()
                && self.exchange.is_none()
            {
                return Poll::Ready(Err(self
                    .fatal
                    .take()
                    .expect("a reset connection retains its cause")));
            }
            // Bytes behind a retired response fail the connection on the next turn instead.
            if self.fatal.is_none()
                && self.exchange.is_none()
                && self.ingress.is_empty()
                && self.egress.is_empty()
                && !self.writes_pending()
                && self.shared.closed.borrow().is_some()
                && self.shared.queued.borrow().is_none()
            {
                // The write queue's idle check may cancel the connection.
                self.observe_cancel();
                if self.fatal.is_none() {
                    if let Some(closed) = self.close_idle() {
                        return Poll::Ready(closed);
                    }
                    // Only the outstanding read can progress: its completion wakes the task.
                    if !progress {
                        return Poll::Pending;
                    }
                }
                turn += 1;
                continue;
            }
            if !progress || turn + 1 == TURN_BUDGET {
                self.update_idle();
            }
            if !progress {
                return Poll::Pending;
            }
            turn += 1;
            // A next turn that would find nothing to do would note idleness and return here.
            if turn < TURN_BUDGET && !self.turn_due() {
                self.update_idle();
                return Poll::Pending;
            }
        }
        self.budget_yield = Some(self.context.host().yield_to_io());
        // A yield that completes on its first poll arranged no wake.
        if self.poll_budget_yield(context).is_ready() {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// As OwnedH2, idleness is recomputed on every quiescent turn, so a slot returned
    /// uncommitted leaves the connection idle again.
    fn update_idle(&self) {
        self.shared.idle.set(
            self.exchange.is_none()
                && self.shared.used.get() == 0
                && self.egress.is_empty()
                && !self.writes_pending()
                && self.fatal.is_none()
                && !self.transport_reset,
        );
    }

    /// Whether a next turn would act. Each term is the condition under which a step of the
    /// turn acts, or a wider one, and none runs transport or application code: a phase before
    /// the write; the write; a failure or a reset; or the idle close.
    fn turn_due(&self) -> bool {
        self.transport_reset
            || self.fatal.is_some()
            || self.phases_due_before_write()
            || if self.egress.is_empty() {
                // The idle check on the write queue is transport code.
                self.exchange.is_none()
                    && self.ingress.is_empty()
                    && self.shared.closed.borrow().is_some()
                    && self.shared.queued.borrow().is_none()
            } else {
                // A queue's capacity check is transport code.
                self.write_queue.is_some() || self.write.is_none()
            }
    }

    /// Whether a phase before the write would act in a next turn: the cancel check, the write
    /// completion, the read, admission, the check for a response without a request, the
    /// parse, a held read failure, the exchange or its settlement. Each term is the condition
    /// under which that phase acts, or a wider one, and none runs transport or application
    /// code.
    fn phases_due_before_write(&self) -> bool {
        (!self.transport_reset
            && match &self.exchange {
                Some(exchange) => exchange.cancel_requested(),
                None => self.shared.cancelled(),
            })
            || self.write_wake.is_ready()
            || self.read_wake.is_ready()
            || (self.read.is_none() && self.read_capacity() != 0)
            || (self.fatal.is_none()
                && (self.admission_due() || (!self.ingress.is_empty() && self.exchange.is_none())))
            || self.parse_due()
            || (self.read_error.is_some() && !(self.flow_pause && self.codec.is_busy()))
            || (self.fatal.is_none() && (self.stream_wake.is_ready() || self.settle_due()))
    }

    /// Whether `poll_admission` acts: EOF closes admission, or a queued submission can commit.
    fn admission_due(&self) -> bool {
        (self.eof && self.shared.closed.borrow().is_none())
            || (self.exchange.is_none()
                && self.ingress.is_empty()
                && self.shared.queued.borrow().is_some())
    }

    /// Whether the parse phase acts: the input or the codec changed since a parse consumed all
    /// of its input, and the live exchange's response has something to parse.
    fn parse_due(&self) -> bool {
        self.codec.reparse
            && self.fatal.is_none()
            && self.exchange.is_some()
            && !self.flow_pause
            && !self.response_pause
            && (!self.ingress.is_empty()
                || self.codec.is_busy()
                || (self.eof && !self.eof_processed))
    }

    /// Whether `settle_exchange` acts: it releases the capacity of an exchange whose messages
    /// ended, or retires one whose response is consumed and request written.
    fn settle_due(&self) -> bool {
        self.exchange.as_ref().is_some_and(|exchange| {
            exchange.request_complete
                && exchange.response_complete
                && ((exchange.status.counted.get() && exchange.status.ingress_succeeded())
                    || (exchange.consumed && exchange.write_boundary <= self.summary.bytes_written))
        })
    }

    /// Run at each phase boundary: callbacks may cancel the session or the live request, and
    /// the phases after them must not act for it.
    fn observe_cancel(&mut self) {
        if self.transport_reset {
            return;
        }
        let error = match &self.exchange {
            Some(exchange) => exchange.cancel_error(),
            None => self.shared.cancelled().then(cancelled),
        };
        if let Some(error) = error {
            self.abort(error);
        }
    }

    fn poll_budget_yield(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let Some(mut wait) = self.budget_yield.take() else {
            return Poll::Ready(());
        };
        let poll = wait.as_mut().poll(context);
        if poll.is_pending() {
            self.budget_yield = Some(wait);
        }
        poll
    }

    /// Closes admission once the connection can take no request, and otherwise commits a
    /// queued submission when no exchange is live. Returns whether either happened.
    fn poll_admission(&mut self) -> bool {
        if self.eof && self.shared.closed.borrow().is_none() {
            self.shared
                .close(failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
            return true;
        }
        // Bytes behind a retired response are no response to the next request: they fail the
        // connection before anything is committed, and a failed read closes admission first.
        if self.exchange.is_some() || !self.ingress.is_empty() || self.read_error.is_some() {
            return false;
        }
        let Some(mut submission) = self.shared.queued.borrow_mut().take() else {
            return false;
        };
        let result = catch_unwind(AssertUnwindSafe(|| self.commit(&mut submission)));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => submission.reject(error),
            Err(panic) => {
                let mut first = Some(panic);
                capture(&mut first, || {
                    submission.reject(failure(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"upstream request commitment panicked",
                    ))
                });
                finish_panic(first);
            }
        }
        true
    }

    fn commit(&mut self, submission: &mut Submission) -> Result<(), HttpError> {
        if let Some(error) = submission.status.selected_error() {
            return Err(error);
        }
        let headers = submission
            .headers
            .as_mut()
            .expect("queued submission owns headers");
        // As `UniplexSession`, a body on a method that forbids one is refused for HTTP/2 only.
        if !headers.headers().is_request() {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"invalid upstream request head",
            ));
        }
        let method = headers.headers_mut().method().ok().flatten();
        // This driver carries no tunnel: a request that can switch protocols is refused before
        // any byte is written (`DIV-029`). The codec switches on a CONNECT method, on an
        // `Upgrade` header that a `101` accepts, and on the egress websocket flag, for which it
        // writes the `Upgrade` header itself. Under `AsOrdinaryRequest` the codec sends CONNECT
        // and `Upgrade` requests as ordinary requests (`DIV-031`), but the flag still asks for
        // a handshake.
        if headers.headers().is_egress_websocket_upgrade()
            || (!self.ordinary_upgrades
                && (method == Some(HttpMethod::Connect)
                    || headers.headers().headers().exists(HttpHeaderCode::Upgrade)))
        {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"owned H1 upstream carries no upgrade",
            ));
        }
        let expected_request_length = crate::uniplex::message_content_length(headers.headers());
        // A head that ends the request cannot carry the body it declares (`DIV-029`).
        if headers.eom() && expected_request_length.is_some_and(|expected| expected != 0) {
            return Err(request_length_mismatch());
        }
        // `DIV-031`: a CONNECT or `Upgrade` request sent as ordinary whose head does not frame
        // its body as the codec writes it is refused before any byte.
        if self.ordinary_upgrades
            && !self
                .codec
                .frames_ordinary_request(headers.headers(), None, headers.eom())
        {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"owned H1 upstream request sent as ordinary has ambiguous framing",
            ));
        }
        let head_request = method == Some(HttpMethod::Head);
        let id = self.codec.get_mut().create_stream();
        let signals = Rc::new(IngressSignals {
            source_complete: Cell::new(false),
            completed: Cell::new(None),
            stream: Arc::clone(&self.stream_wake),
        });
        let source = HttpResponseSource::new_shared(
            id,
            signals.clone(),
            self.config.buffer_limit as u32,
            self.context.host().clone(),
        )
        .map_err(|_| {
            failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid owned H1 response window",
            )
        })?;
        if head_request {
            source.skip_content_length_validation();
        }
        if self.config.deliver_ingress_before_abort {
            source.deliver_ingress_before_abort();
        }
        let producer = source.producer();
        let eom = headers.eom();
        let generated = self.codec.get_mut().generate(
            &mut self.egress,
            EgressFrame::Headers {
                stream: id,
                message: headers.headers(),
                end_stream: eom,
                extra_headers: None,
            },
            &mut NoopCallback,
        )?;
        let mut headers = submission
            .headers
            .take()
            .expect("queued request headers are owned");
        cancel_byte_events(headers.take_byte_event_registrations());
        if let Some(size) = generated.header_size {
            headers.on_headers_encoded(size);
        }
        drop(headers);
        let upload = submission
            .source
            .take()
            .expect("queued request source is owned");
        let (transfer, unused_source) = if eom {
            (None, Some(upload))
        } else {
            (
                Some(HttpBodyTransfer::new(
                    upload,
                    id,
                    self.config.maximum_write_size,
                )),
                None,
            )
        };
        let status = Rc::clone(&submission.status);
        status.stream.set(Some(id));
        let committed = self.shared.committed.get();
        status.sequence.set(Some(committed));
        self.shared.committed.set(committed + 1);
        *status.ingress.borrow_mut() = source.ingress_completion();
        submission
            .reservation
            .as_mut()
            .expect("queued request owns a reservation")
            .active = false;
        submission.reservation.take();
        status.counted.set(true);
        status.live.set(true);
        self.shared.spent.set(true);
        self.exchange = Some(Box::new(Exchange {
            status: Rc::clone(&status),
            id,
            signals,
            producer,
            transfer,
            unused_source,
            resume: None,
            cancellation_wake: submission.cancellation_wake.take(),
            response_observer: None,
            request_producer: submission.producer.take(),
            expected_request_length,
            request_body_length: 0,
            request_complete: eom,
            response_complete: false,
            consumed: false,
            write_boundary: self
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64),
        }));
        self.flow_pause = false;
        self.response_pause = false;
        self.codec.get_mut().set_parser_paused(false);
        status.acknowledge(id, source);
        self.stream_wake.mark();
        Ok(())
    }

    fn parse_input(&mut self) -> Result<bool, HttpError> {
        let mut callback = ResponseInput {
            config: &self.config,
            exchange: &mut self.exchange,
            stream_wake: &self.stream_wake,
            flow_pause: &mut self.flow_pause,
            response_pause: &mut self.response_pause,
            failure: None,
        };
        let length = self.ingress.chain_length();
        // A pause can land between a response's last body byte and its completion: the codec
        // completes it on a later, empty, parse.
        let (consumed, mut egress_results) = if length == 0 && !self.codec.is_busy() {
            (0, Vec::new())
        } else {
            #[cfg(test)]
            count(&CODEC_PARSES);
            let result =
                self.codec
                    .codec
                    .on_ingress(&self.ingress, &mut callback, &mut self.egress);
            (result.consumed, result.egress_results)
        };
        // EOF ends a close-delimited response, or fails an unfinished one, once every byte
        // before it is parsed.
        let finishing_eof = self.eof
            && !self.eof_processed
            && !*callback.flow_pause
            && !*callback.response_pause
            && callback.failure.is_none()
            && consumed == length;
        if finishing_eof {
            self.eof_processed = true;
            let result = self
                .codec
                .codec
                .on_ingress_eof(&mut callback, &mut self.egress);
            egress_results.extend(result.egress_results);
            // Before a response begins, the codec raises nothing at EOF, as `UniplexSession`
            // fails the request then.
            if callback.failure.is_none()
                && callback
                    .exchange
                    .as_ref()
                    .is_some_and(|exchange| !exchange.response_complete)
            {
                callback.fail(failure(
                    HttpErrorCode::TRANSPORT_EOF,
                    b"transport EOF before the response",
                ));
            }
        }
        let failed = callback.failure.take();
        if consumed > length {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"codec consumed beyond the supplied prefix",
            ));
        }
        self.ingress
            .trim_start(consumed)
            .expect("codec consumption was range checked");
        // As downstream: a parse of what remains acts only after a change.
        self.codec.reparse = !self.ingress.is_empty();
        for result in egress_results {
            result?;
        }
        if let Some(error) = failed {
            return Err(error);
        }
        Ok(consumed != 0 || finishing_eof)
    }

    /// Notes the caller's response consumption and serializes the request body.
    fn poll_exchange(&mut self) -> Result<(), HttpError> {
        let Some(exchange) = self.exchange.as_mut() else {
            return Ok(());
        };
        if self.flow_pause
            && exchange
                .producer
                .body_bytes_buffered()
                .is_none_or(|buffered| buffered < self.config.ingress_buffer_limit)
        {
            self.flow_pause = false;
            self.codec.get_mut().set_parser_paused(self.response_pause);
        }
        // A cancel before both messages end closes the connection. After that, the buffered
        // response stays readable, as OwnedH2 leaves a finished stream.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        let mut context = Context::from_waker(&self.stream_waker);
        exchange.poll_response_observer(&mut context);
        // The observer's callbacks may cancel the request.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        if !exchange.consumed && exchange.signals.source_complete.get() {
            exchange.consumed = true;
            let completed = exchange.signals.completed.take();
            if !exchange.response_complete {
                // The caller stopped reading before the response ended: the rest of it cannot
                // be skipped, so the connection closes.
                return Err(match completed {
                    Some(Err(error)) => error,
                    _ => failure(
                        HttpErrorCode::CANCEL,
                        b"response consumer retired before the response ended",
                    ),
                });
            }
        }
        if exchange.request_complete {
            return Ok(());
        }
        let room = self
            .config
            .maximum_write_size
            .saturating_sub(self.egress.chain_length())
            .min(self.egress.remaining_capacity())
            .saturating_sub(CHUNK_OVERHEAD);
        if room == 0 {
            self.body_awaits_egress = true;
            return Ok(());
        }
        let mut event = match exchange.poll_upload(&mut context, room as u32) {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(()),
            // As OwnedH2, a cancel selected during the failed read is the first cause.
            Err(error) => {
                return Err(exchange
                    .cancel_error()
                    .unwrap_or_else(|| exchange.upload_failed(error)));
            }
        };
        cancel_byte_events(event.take_byte_event_registrations());
        // As OwnedH2, the source's read may cancel the request: its event is not serialized.
        if let Some(error) = exchange.cancel_error() {
            return Err(error);
        }
        let eom = event.eom();
        let request_body_length = exchange
            .request_body_length
            .saturating_add(event.as_body().map_or(0, |body| body.chain_length() as u64));
        // Bytes past the declared length would be read as the next message, and a short body
        // leaves the origin waiting: neither is written.
        if exchange.expected_request_length.is_some_and(|expected| {
            request_body_length > expected || (eom && request_body_length < expected)
        }) {
            return Err(request_length_mismatch());
        }
        exchange.request_body_length = request_body_length;
        match event.into_kind() {
            HttpBodyEventKind::Suspend(resume) => {
                exchange.resume = Some(resume);
            }
            HttpBodyEventKind::Trailers(trailers) => {
                let generated = self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Trailers {
                        stream: exchange.id,
                        trailers: &trailers,
                    },
                    &mut NoopCallback,
                )?;
                // On a `Content-Length` request the codec writes nothing for trailers and leaves
                // its message open, though the request is complete on the wire (DIV-029): the
                // message ends here, writing nothing, so the connection can carry the next one.
                if generated.bytes_written == 0 {
                    self.codec.get_mut().generate(
                        &mut self.egress,
                        EgressFrame::EndOfMessage {
                            stream: exchange.id,
                        },
                        &mut NoopCallback,
                    )?;
                }
            }
            HttpBodyEventKind::Body(body) => {
                self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Body {
                        stream: exchange.id,
                        body,
                        padding: None,
                        end_stream: eom,
                    },
                    &mut NoopCallback,
                )?;
            }
            HttpBodyEventKind::Padding(length) => {
                self.codec.get_mut().generate(
                    &mut self.egress,
                    EgressFrame::Padding {
                        stream: exchange.id,
                        length,
                    },
                    &mut NoopCallback,
                )?;
                // Padding writes nothing in HTTP/1.1, so its EOM ends the message on its own.
                if eom {
                    self.codec.get_mut().generate(
                        &mut self.egress,
                        EgressFrame::EndOfMessage {
                            stream: exchange.id,
                        },
                        &mut NoopCallback,
                    )?;
                }
            }
            _ => {
                return Err(failure(
                    HttpErrorCode::PROTOCOL_ERROR,
                    b"owned H1 request event is not supported",
                ));
            }
        }
        self.stream_wake.mark();
        if eom {
            exchange.request_complete = true;
            exchange.transfer = None;
            exchange.write_boundary = self
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64);
        }
        Ok(())
    }

    /// Releases capacity once the request is serialized and the response produced, and
    /// retires the exchange once its response is consumed and its request written: a reusable
    /// connection then returns its slot, and any other closes admission.
    fn settle_exchange(&mut self) -> bool {
        let Some(exchange) = self.exchange.as_mut() else {
            return false;
        };
        if exchange.request_complete
            && exchange.response_complete
            && exchange.status.ingress_succeeded()
        {
            exchange.status.release();
        }
        if !(exchange.request_complete
            && exchange.response_complete
            && exchange.consumed
            && exchange.write_boundary <= self.summary.bytes_written)
        {
            // Only the consumer's completion is left: the turn it wakes returns the slot.
            if exchange.request_complete
                && exchange.response_complete
                && exchange.status.ingress_succeeded()
                && exchange.write_boundary <= self.summary.bytes_written
                && self.codec.is_reusable()
                && !self.codec.is_busy()
                && self.shared.retiring.borrow().is_none()
            {
                *self.shared.retiring.borrow_mut() = Some(Rc::clone(&exchange.signals));
            }
            return false;
        }
        self.shared.retiring.take();
        let exchange = self.exchange.take().expect("the exchange was present");
        capture(&mut self.panic, || drop(exchange));
        // As OwnedH2: a retirement panic fails the connection, and `run` resumes it.
        if self.panic.is_some() {
            self.abort(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H1 upstream retirement panicked",
            ));
            return true;
        }
        self.shared.idle.set(true);
        // A codec that still holds a message open cannot start the next one.
        if self.codec.is_reusable() && !self.codec.is_busy() {
            self.shared.return_slot();
        } else {
            self.shared.close(failure(
                HttpErrorCode::REFUSED_STREAM,
                b"owned H1 upstream connection is not reusable",
            ));
        }
        true
    }

    fn poll_read(&mut self, turn: &mut usize) -> Option<bool> {
        let mut read = self.poll_read_once()?;
        let progress = read != ReadPoll::Pending;
        while read == ReadPoll::Data && *turn + 1 < TURN_BUDGET {
            read = match self.poll_read_once() {
                Some(ReadPoll::Pending) | None => break,
                Some(next) => next,
            };
            *turn += 1;
        }
        Some(progress)
    }

    /// How much a new read may take: 0 while no read can start, as the connection read EOF or
    /// a failure or is ending, or while the ingress buffer is full.
    fn read_capacity(&self) -> usize {
        if self.eof
            || self.read_error.is_some()
            || self.failed_read.is_some()
            || self.fatal.is_some()
            || self.cancellation.is_cancellation_requested()
        {
            return 0;
        }
        self.config
            .maximum_read_size
            .min(self.ingress.remaining_capacity())
    }

    fn poll_read_once(&mut self) -> Option<ReadPoll> {
        if self.read.is_none() {
            let maximum = self.read_capacity();
            if maximum != 0 {
                self.read = Some(self.transport.read(
                    &self.context,
                    maximum,
                    self.config.read_allocation_size.min(maximum),
                    self.config.read_timeout,
                    self.cancellation.token(),
                ));
                self.read_wake.mark();
            }
        }
        let read = self.read.as_mut()?;
        if !self.read_wake.take_ready() {
            return None;
        }
        let mut context = Context::from_waker(&self.read_waker);
        let Poll::Ready(result) = read.as_mut().poll(&mut context) else {
            return Some(ReadPoll::Pending);
        };
        self.read.take();
        match result {
            Ok(read) if read.bytes_read() == 0 && !read.is_eof() => return Some(ReadPoll::Empty),
            Ok(read) => {
                self.summary.bytes_read = self
                    .summary
                    .bytes_read
                    .saturating_add(read.bytes_read() as u64);
                if read.is_eof() {
                    self.eof = true;
                }
                self.codec.reparse = true;
                let mut data = read.into_data();
                if self.ingress.append(&mut data).is_err() {
                    self.abort(failure(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"transport read exceeded requested ingress capacity",
                    ));
                }
            }
            Err(error) => {
                if self.fatal.is_none() && !self.cancellation.is_cancellation_requested() {
                    self.read_error = Some(error);
                }
            }
        }
        Some(ReadPoll::Data)
    }

    fn writes_pending(&self) -> bool {
        self.write.is_some()
            || self
                .write_queue
                .as_ref()
                .is_some_and(|queue| !queue.is_idle())
    }

    fn can_start_write(&self) -> bool {
        match &self.write_queue {
            Some(queue) => queue.can_write(
                self.egress
                    .chain_length()
                    .min(self.config.maximum_write_size),
            ),
            None => self.write.is_none(),
        }
    }

    fn start_write(&mut self) -> Result<(), HttpError> {
        let data = self.egress.split_at_most(self.config.maximum_write_size);
        self.summary.bytes_scheduled = self
            .summary
            .bytes_scheduled
            .saturating_add(data.chain_length() as u64);
        self.write_end = self.summary.bytes_scheduled;
        if std::mem::take(&mut self.body_awaits_egress) {
            self.stream_wake.mark();
        }
        if let Some(queue) = &mut self.write_queue {
            queue.start_write(data)?;
        } else {
            self.write = Some(self.transport.write(
                &self.context,
                data,
                ByteEventWriteFlags::default(),
                self.config.write_timeout,
                self.write_cancellation.token(),
            ));
        }
        self.write_wake.mark();
        Ok(())
    }

    fn poll_write(&mut self) -> Option<bool> {
        if !self.write_wake.take_ready() {
            return None;
        }
        let mut context = Context::from_waker(&self.write_waker);
        let mut progress = false;
        let mut failed = None;
        if let Some(queue) = &mut self.write_queue {
            while !queue.is_idle() {
                let Poll::Ready(completion) = queue.poll_complete(&mut context) else {
                    break;
                };
                progress = true;
                let bytes = match &completion.result {
                    Ok(info) => info.bytes_written,
                    Err(error) => error.bytes_written(),
                };
                self.summary.bytes_written =
                    self.summary.bytes_written.saturating_add(bytes as u64);
                match completion.result {
                    Ok(_) if bytes == completion.requested_bytes => {}
                    Ok(_) => {
                        failed.get_or_insert(failure(
                            HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            b"transport reported a short successful write",
                        ));
                    }
                    Err(error) => {
                        failed.get_or_insert_with(|| error.error().clone());
                    }
                }
            }
        } else if let Some(write) = &mut self.write {
            let Poll::Ready(result) = write.as_mut().poll(&mut context) else {
                return Some(false);
            };
            self.write.take();
            progress = true;
            let bytes = match &result {
                Ok(info) => info.bytes_written,
                Err(error) => error.bytes_written(),
            };
            self.summary.bytes_written = self.summary.bytes_written.saturating_add(bytes as u64);
            match result {
                Err(error) => failed = Some(error.error().clone()),
                Ok(_) if self.summary.bytes_written != self.write_end => {
                    failed = Some(failure(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"transport reported a short successful write",
                    ));
                }
                Ok(_) => {}
            }
        }
        if let Some(error) = failed {
            self.abort(error);
        }
        Some(progress)
    }

    /// `None` while the outstanding read has yet to observe cancellation.
    fn close_idle(&mut self) -> Option<Result<UniplexRunSummary, HttpError>> {
        if self.cancellation.request_cancellation() {
            self.read_wake.mark();
        }
        if self.read.is_some() {
            return None;
        }
        Some(
            self.transport
                .shutdown_write(&self.context)
                .map(|()| self.summary),
        )
    }

    #[cold]
    fn reset_transport(&mut self) {
        self.read_wake.mark();
        self.write_wake.mark();
        if !self.transport_reset {
            self.transport_reset = true;
            self.summary.reset = true;
            capture(&mut self.panic, || self.cancellation.request_cancellation());
            capture(&mut self.panic, || {
                self.write_cancellation.request_cancellation()
            });
            if let Some(Err(error)) = capture(&mut self.panic, || {
                self.transport.close_with_reset(&self.context)
            }) {
                self.fatal.get_or_insert(error);
            }
        }
    }

    /// Fails the connection for a failed read. As `UniplexSession`, admission closes and the
    /// live exchange is cancelled at once, nothing more is read, and the transport resets once
    /// the request bytes already serialized are written. A panic in that teardown resets it at
    /// once, and `run` resumes the panic.
    #[cold]
    fn fail_read(&mut self, error: HttpError) {
        if self.fatal.is_some() || (self.egress.is_empty() && !self.writes_pending()) {
            self.abort(error);
            return;
        }
        self.failed_read = Some(error.clone());
        self.close_and_cancel(&error);
        if self.panic.is_some() {
            self.abort(error);
        }
    }

    /// Fails the connection: the live exchange is cancelled and dropped, admission closes,
    /// pending egress is discarded, and the transport is reset once outstanding operations are
    /// cancelled. A failed read that was waiting for its writes stays the first cause.
    #[cold]
    fn abort(&mut self, error: HttpError) {
        let first = self.failed_read.take();
        self.fatal
            .get_or_insert_with(|| first.unwrap_or_else(|| error.clone()));
        self.close_and_cancel(&error);
        self.egress = BufQueue::new(self.config.buffer_limit);
        self.reset_transport();
    }

    /// Closes admission with `error`, then cancels and drops the live exchange with it.
    #[cold]
    fn close_and_cancel(&mut self, error: &HttpError) {
        // Admission closes first, so the exchange's capacity release announces no capacity.
        capture(&mut self.panic, || self.shared.close(error.clone()));
        self.shared.retiring.take();
        if let Some(mut exchange) = self.exchange.take() {
            capture(&mut self.panic, || exchange.cancel(error));
            capture(&mut self.panic, || drop(exchange));
        }
    }
}

impl Drop for UpstreamDriver {
    fn drop(&mut self) {
        if !self.shared.complete.get() {
            self.abort(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"owned H1 upstream runner abandoned before cleanup completed",
            ));
            if let Some(panic) = self.panic.take() {
                if std::thread::panicking() {
                    discard_panic(panic);
                } else {
                    resume_unwind(panic);
                }
            }
            assert!(
                !self.started || std::thread::panicking() || in_forced_teardown(),
                "started owned H1 upstream runner dropped before native I/O cleanup joined"
            );
        }
    }
}
