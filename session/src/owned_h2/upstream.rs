use roxygen_core::body_event_queue::HttpBodySource;
use roxygen_core::body_event_queue::HttpBodyTransfer;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::DeferredReadTimeout;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;

use super::*;
use crate::experimental::OwnedRequestProgressExt;
pub use crate::owned::OwnedResponseObserver as OwnedH2ResponseObserver;
use crate::owned::OwnedResponseObserver;
pub use crate::owned::OwnedUpstreamObserver as OwnedH2UpstreamObserver;
use crate::owned::OwnedUpstreamObserver;

const UPLOAD_BYTES: usize = 65_535;

pub(super) struct Admission {
    context: WorkerContext,
    ready: Arc<ReadyQueue>,
    submission_waker: Waker,
    maximum: u32,
    peer_maximum: Cell<u32>,
    pub(super) used: Cell<u32>,
    /// Requests committed on the connection (`DIV-035`).
    committed: Cell<u64>,
    closed: RefCell<Option<HttpError>>,
    observer: RefCell<Option<Rc<dyn OwnedUpstreamObserver>>>,
    queued: RefCell<VecDeque<Submission>>,
    /// Set while the owning driver is polling.
    pub(super) polling: Cell<bool>,
    /// A release inside the owning driver's poll, which the turn loop takes instead of a wake.
    release_pending: Cell<bool>,
}

impl Admission {
    pub(super) fn new(ready: Arc<ReadyQueue>, context: WorkerContext, maximum: u32) -> Self {
        let submission_waker = Waker::from(Arc::new(SubmissionWake(Arc::clone(&ready))));
        Self {
            context,
            ready,
            submission_waker,
            maximum,
            peer_maximum: Cell::new(u32::MAX),
            used: Cell::new(0),
            committed: Cell::new(0),
            closed: RefCell::new(None),
            observer: RefCell::new(None),
            queued: RefCell::new(VecDeque::new()),
            polling: Cell::new(false),
            release_pending: Cell::new(false),
        }
    }

    fn available(&self) -> u32 {
        if self.closed.borrow().is_some() || self.context.is_cancelled() {
            0
        } else {
            self.maximum
                .min(self.peer_maximum.get())
                .saturating_sub(self.used.get())
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
        if self.polling.get() {
            self.release_pending.set(true);
        } else {
            capture(&mut panic, || self.ready.wake());
        }
        finish_panic(panic);
    }

    pub(super) fn set_peer_limit(&self, maximum: u32) {
        let previous = self.available();
        self.peer_maximum.set(maximum);
        if self.available() > previous {
            let observer = self.observer.borrow().clone();
            let mut panic = None;
            if let Some(observer) = observer {
                capture(&mut panic, || observer.capacity_available());
            }
            finish_panic(panic);
        }
    }

    pub(super) fn close(&self, error: HttpError) {
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
        let queued = std::mem::take(&mut *self.queued.borrow_mut());
        if !changed && queued.is_empty() {
            return;
        }
        let mut panic = None;
        for mut submission in queued {
            capture(&mut panic, || submission.refuse(error.clone()));
        }
        if let Some(observer) = observer {
            capture(&mut panic, || observer.admission_closed());
        }
        capture(&mut panic, || self.ready.wake());
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
pub struct OwnedH2UpstreamHandle {
    control: Rc<Control>,
    admission: Rc<Admission>,
}

impl OwnedH2UpstreamHandle {
    pub fn available_capacity(&self) -> u32 {
        if self.control.cancel.get() || self.control.complete.get() {
            0
        } else {
            self.admission.available()
        }
    }

    /// Reservations and streams holding an admission slot: what `available_capacity` subtracts
    /// from the stream limit.
    pub fn admitted_streams(&self) -> u32 {
        self.admission.used.get()
    }

    pub fn reserve_request(&self) -> Result<OwnedH2UpstreamReservation, HttpError> {
        if self.available_capacity() == 0
            || self.control.complete.get()
            || self.control.cancel.get()
        {
            return Err(failure(
                HttpErrorCode::REFUSED_STREAM,
                b"owned upstream admission is unavailable",
            ));
        }
        self.admission.used.set(
            self.admission
                .used
                .get()
                .checked_add(1)
                .expect("bounded reservation count"),
        );
        self.control.idle.set(false);
        Ok(OwnedH2UpstreamReservation {
            admission: Rc::clone(&self.admission),
            active: true,
        })
    }

    pub fn set_observer(&self, observer: Rc<dyn OwnedUpstreamObserver>) -> Result<(), HttpError> {
        if self.control.complete.get() || self.admission.observer.borrow().is_some() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned upstream observer is already installed or session finished",
            ));
        }
        *self.admission.observer.borrow_mut() = Some(Rc::clone(&observer));
        if self.admission.closed.borrow().is_some() {
            observer.admission_closed();
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        self.control.complete.get()
    }

    /// Includes response consumers, upload retirement and native write ownership.
    pub fn is_idle(&self) -> bool {
        !self.is_finished() && self.control.idle.get() && self.admission.used.get() == 0
    }

    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        if self.is_finished() {
            return Ok(false);
        }
        let changed = !self.control.drain.replace(true);
        self.admission.close(failure(
            HttpErrorCode::REFUSED_STREAM,
            b"owned upstream connection is draining",
        ));
        self.control.ready.wake();
        Ok(changed)
    }

    pub fn cancel(&self) {
        self.control.cancel.set(true);
        self.control.idle.set(false);
        self.control.ready.wake();
    }
}

/// One bounded admission slot, returned exactly once if it is not committed.
pub struct OwnedH2UpstreamReservation {
    admission: Rc<Admission>,
    active: bool,
}

/// What releases a submission's waiter.
enum ResponseWait {
    Commitment,
    /// The first published head or the response's end, with the request producer's
    /// completion when the submitter's deadline observes it.
    FirstHead(Option<IngressCompletion>),
}

impl OwnedH2UpstreamReservation {
    /// Submits a request while preserving its fresh response read capability.
    pub fn submit_exclusive(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedH2ExclusiveSubmission {
        OwnedH2ExclusiveSubmission(self.submit(headers, source, cancellation))
    }

    /// Submits a request and waits for its first published response head, preserving
    /// the fresh response read capability. Codec commitment alone does not notify this
    /// waiter. Informational heads also satisfy this interest and remain ordered in the
    /// original source for the caller to drain. A response that ends or is reset before
    /// any head also releases the waiter, with the same result, source outcome and wire
    /// behavior as an ordinary submission. Upload, source and physical-write retirement
    /// remain independent of this notification. The caller retains timeout
    /// responsibility while waiting, including any timeout anchored to request-producer EOM.
    ///
    /// A caller whose deadline observes the request producer's completion supplies it as
    /// `producer_completion`, unless that producer has already succeeded: the outcome is
    /// first-wins, so such a producer cannot fail. Until the caller takes the response or
    /// the response ends before that producer fails, an upload that fails after that
    /// producer has failed selects the producer's error and resets with its cancellation
    /// code, as the caller's deadline selects it when that runs first. Otherwise a failed
    /// upload takes the session's upload-failure reset.
    ///
    /// Dropping the submission after commitment and before it resolves cancels the request
    /// with `CANCEL`, as an ordinary caller that took its commitment cancels by dropping its
    /// response. Before commitment it is dropped as an ordinary submission is.
    pub fn submit_with_response_interest(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        producer_completion: Option<IngressCompletion>,
    ) -> OwnedH2ExclusiveSubmission {
        OwnedH2ExclusiveSubmission(self.submit_impl(
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
    ) -> OwnedH2UpstreamSubmission {
        self.submit_impl(headers, source, cancellation, ResponseWait::Commitment)
    }

    fn submit_impl(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        wait: ResponseWait,
    ) -> OwnedH2UpstreamSubmission {
        let (response_interest, producer) = match wait {
            ResponseWait::Commitment => (false, None),
            ResponseWait::FirstHead(completion) => (true, completion),
        };
        let source = source.into();
        let admission = Rc::clone(&self.admission);
        let status = Rc::new(RequestStatus {
            admission: Rc::clone(&admission),
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
            response_observation_attached: Cell::new(false),
            response_observer: RefCell::new(None),
            delivered: Cell::new(false),
            awaiting_delivery: Cell::new(true),
            released_undelivered: Cell::new(false),
            waiter: RefCell::new(None),
            wake: RefCell::new(None),
            response_head: Cell::new(false),
            unprocessed: Cell::new(false),
            sequence: Cell::new(None),
        });
        let cancellation_wake = if status.cancellation.can_be_cancelled() {
            let mut registration = status.cancellation.wake_registration();
            registration.update_waker(&admission.submission_waker);
            Some(registration)
        } else {
            None
        };
        let mut submission = Submission {
            cancellation_wake,
            reservation: Some(self),
            headers: Some(headers),
            source: Some(source),
            producer,
            status: Rc::clone(&status),
        };
        let result = OwnedH2UpstreamSubmission {
            status,
            complete: false,
        };
        let closed = admission.closed.borrow().clone();
        if let Some(error) = closed {
            submission.refuse(error);
        } else {
            admission.queued.borrow_mut().push_back(submission);
            admission.ready.wake();
        }
        result
    }
}

impl Drop for OwnedH2UpstreamReservation {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            let mut panic = None;
            capture(&mut panic, || self.admission.release(None, None));
            finish_panic(panic);
        }
    }
}

/// A cancellation-safe acknowledgment of the driver's request commitment.
pub struct OwnedH2UpstreamSubmission {
    status: Rc<RequestStatus>,
    complete: bool,
}

impl OwnedH2UpstreamSubmission {
    /// Retains the first explicit cause, including a caller's route timeout.
    pub fn cancel(&self, error: HttpError) {
        self.status.cancel(error);
    }

    fn poll_response(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<OwnedH2ExclusiveResponse, HttpError>> {
        if self.status.response_ready.get() {
            if let Some(error) = self.status.selected_error() {
                self.status.fail(error);
            }
            if !self.status.response_interest
                || self.status.response_published.get()
                || self.status.error.borrow().is_some()
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
                return Poll::Ready(response.map(|(stream, source)| OwnedH2ExclusiveResponse {
                    stream,
                    source,
                    control: OwnedH2UpstreamRequest(Rc::clone(&self.status)),
                }));
            }
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

impl Future for OwnedH2UpstreamSubmission {
    type Output = Result<OwnedH2UpstreamResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_response(context).map(|result| {
            result.map(|response| OwnedH2UpstreamResponse {
                stream: response.stream,
                source: response.source.into_source_owner(),
                control: response.control,
            })
        })
    }
}

/// A submission preserving the response's fresh exclusive capability.
/// Ordinary submissions acknowledge codec commitment; response-interest
/// submissions additionally wait for the first published response head or the
/// response's end.
pub struct OwnedH2ExclusiveSubmission(OwnedH2UpstreamSubmission);

impl OwnedH2ExclusiveSubmission {
    /// Retains the first explicit cause through the ordinary submission owner.
    pub fn cancel(&self, error: HttpError) {
        self.0.cancel(error);
    }
}

impl Future for OwnedH2ExclusiveSubmission {
    type Output = Result<OwnedH2ExclusiveResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_response(context)
    }
}

/// The fresh response source and the existing protocol-capacity observation.
pub struct OwnedH2ExclusiveResponse {
    pub stream: StreamId,
    pub source: HttpResponseSource,
    pub control: OwnedH2UpstreamRequest,
}

impl OwnedH2UpstreamSubmission {
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

impl Drop for OwnedH2UpstreamSubmission {
    fn drop(&mut self) {
        if !self.complete {
            self.cancel_unresolved();
        }
    }
}

/// The original response source and a sticky protocol-capacity observation.
pub struct OwnedH2UpstreamResponse {
    pub stream: StreamId,
    pub source: HttpStreamSourceOwner,
    pub control: OwnedH2UpstreamRequest,
}

impl OwnedH2UpstreamResponse {
    /// Converts the original response to the generic source interface without reading it.
    pub fn into_source_parts(self) -> (StreamId, HttpSourceHolder, OwnedH2UpstreamRequest) {
        (self.stream, self.source.into_source_holder(), self.control)
    }
}

#[derive(Clone)]
pub struct OwnedH2UpstreamRequest(Rc<RequestStatus>);

impl OwnedH2UpstreamRequest {
    pub fn capacity_released(&self) -> bool {
        self.0.released.get()
    }

    /// Retains the first explicit cause, which resets an unfinished request. A no-op once
    /// `capacity_released()` is true: the exchange has ended, so it records no cause and wakes
    /// nothing.
    pub fn cancel(&self, error: HttpError) {
        if self.capacity_released() {
            return;
        }
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
        let wake = self
            .0
            .wake
            .borrow()
            .clone()
            .filter(|wake| wake.live.load(Ordering::Acquire));
        let Some(wake) = wake else {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"response observation requires a live stream",
            ));
        };
        self.0.response_observation_attached.set(true);
        *self.0.response_observer.borrow_mut() = Some(observer);
        wake.schedule();
        Ok(())
    }
}

impl OwnedRequestProgressExt for OwnedH2UpstreamSubmission {
    fn request_unprocessed(&self) -> bool {
        self.status.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.status.sequence.get()
    }
}

impl OwnedRequestProgressExt for OwnedH2ExclusiveSubmission {
    fn request_unprocessed(&self) -> bool {
        self.0.request_unprocessed()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence_number()
    }
}

impl OwnedRequestProgressExt for OwnedH2UpstreamRequest {
    fn request_unprocessed(&self) -> bool {
        self.0.unprocessed.get()
    }

    fn sequence_number(&self) -> Option<u64> {
        self.0.sequence.get()
    }
}

pub(super) struct RequestStatus {
    admission: Rc<Admission>,
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
    response_observation_attached: Cell<bool>,
    response_observer: RefCell<Option<Box<dyn OwnedResponseObserver>>>,
    delivered: Cell<bool>,
    awaiting_delivery: Cell<bool>,
    released_undelivered: Cell<bool>,
    waiter: RefCell<Option<Waker>>,
    wake: RefCell<Option<Arc<StreamWake>>>,
    /// A response head arrived.
    response_head: Cell<bool>,
    /// Its connection refused it before commit, or the peer refused its stream before any
    /// response head (`DIV-035`).
    unprocessed: Cell<bool>,
    /// The connection's earlier commits, fixed at commit or at a refusal before it (`DIV-035`).
    sequence: Cell<Option<u64>>,
}

impl RequestStatus {
    pub(super) fn ingress_succeeded(&self) -> bool {
        self.ingress
            .borrow()
            .as_ref()
            .is_some_and(|receipt| matches!(receipt.snapshot(), Some(Ok(_))))
    }

    pub(super) fn selected_error(&self) -> Option<HttpError> {
        if let Some(error) = &*self.error.borrow() {
            return Some(clone_selected_error(error));
        }
        self.cancellation.is_cancellation_requested().then(|| {
            failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request body production cancelled",
            )
        })
    }

    /// Whether a cause is selected for the request, as `selected_error` reports one, without
    /// cloning it.
    pub(super) fn cause_selected(&self) -> bool {
        self.error.borrow().is_some() || self.cancellation.is_cancellation_requested()
    }

    /// Whether the response's source already failed, as a stream read timeout fails it before
    /// the driver applies that failure.
    fn response_failed(&self) -> bool {
        self.ingress
            .borrow()
            .as_ref()
            .is_some_and(|receipt| matches!(receipt.snapshot(), Some(Err(_))))
    }

    /// The error a peer reset of the request's stream applies, and whether it failed the request
    /// unprocessed: the request's selected cause, or else `reset`. A `refusal` is the peer's
    /// `REFUSED_STREAM`, or a GOAWAY whose last-stream-id is below the stream, as the stream's
    /// first reset. Before any response head, any selected cause and any failure of the response's
    /// source, it failed the request unprocessed, and `reset` becomes the request's selected cause,
    /// as `Submission::refuse` selects its connection's error, so a later cancel cannot replace it
    /// (`DIV-035`). One sample of the cause decides both, as a cancellation can arrive from another
    /// thread.
    pub(super) fn peer_reset_error(
        &self,
        refusal: bool,
        reset: impl FnOnce() -> HttpError,
    ) -> (HttpError, bool) {
        let selected = self.selected_error();
        #[cfg(test)]
        if refusal {
            crate::experimental::refusal_cause_sampled();
        }
        if let Some(selected) = selected {
            return (selected, false);
        }
        let reset = reset();
        if !refusal || self.response_head.get() || self.response_failed() {
            return (reset, false);
        }
        self.unprocessed.set(true);
        (self.error.borrow_mut().get_or_insert(reset).clone(), true)
    }

    /// Records the arrival of a response head, which also publishes the response.
    pub(super) fn response_head_arrived(&self) {
        self.response_head.set(true);
        self.publish_response();
    }

    fn cancel(&self, error: HttpError) {
        self.error.borrow_mut().get_or_insert(error);
        let wake = self.wake.borrow().clone();
        if let Some(wake) = wake {
            wake.schedule();
        } else {
            self.admission.ready.wake();
        }
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

    /// Releases a response-interest waiter once its source holds a first head or a
    /// terminal outcome, which the caller then reads from the source.
    pub(super) fn publish_response(&self) {
        if self.response_interest
            && !self.response_published.replace(true)
            && self.response_ready.get()
        {
            self.notify();
        }
    }

    pub(super) fn release(&self) {
        if self.counted.replace(false) {
            self.released.set(true);
            let undelivered = self
                .awaiting_delivery
                .get()
                .then_some(&self.released_undelivered);
            self.admission.release(self.stream.get(), undelivered);
        }
    }

    fn retire_delivery_wait(&self) {
        self.awaiting_delivery.set(false);
        if self.released_undelivered.replace(false) {
            let observer = self.admission.observer.borrow().clone();
            if let Some(observer) = observer {
                observer.released_submission_retired();
            }
        }
    }
}

struct SubmissionWake(Arc<ReadyQueue>);

impl Wake for SubmissionWake {
    fn wake(self: Arc<Self>) {
        self.0.wake();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake();
    }
}

struct Submission {
    cancellation_wake: Option<CancellationWakeRegistration>,
    reservation: Option<OwnedH2UpstreamReservation>,
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
                .set(Some(self.status.admission.committed.get()));
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

#[cold]
#[inline(never)]
fn clone_selected_error(error: &HttpError) -> HttpError {
    error.clone()
}

#[cold]
#[inline(never)]
fn admission_closed_egress_error(error: &HttpError) -> (HttpError, H2ErrorCode) {
    let error = error.clone();
    let wire = http_error_code_to_h2_error_code(error.code(), false);
    (error, wire)
}

pub(super) fn cancellation_wire_code(code: HttpErrorCode) -> H2ErrorCode {
    let code = crate::session::source_complete_reset_code(code);
    let code = if code == HttpErrorCode::NO_ERROR {
        HttpErrorCode::CANCEL
    } else {
        code
    };
    http_error_code_to_h2_error_code(code, false)
}

pub(super) struct Exchange {
    pub(super) status: Rc<RequestStatus>,
    transfer: Option<HttpBodyTransfer>,
    unused_source: Option<HttpBodySource>,
    resume: Option<roxygen_core::events::ResumeFuture>,
    production_complete: bool,
    cancellation_wake: Option<CancellationWakeRegistration>,
    response_observer: Option<Box<dyn OwnedResponseObserver>>,
    /// The supplied request producer's completion while its failure can still be selected.
    producer: Option<IngressCompletion>,
}

impl Exchange {
    pub(super) fn poll_response_observer(&mut self, context: &mut Context<'_>) {
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

    /// Stops watching the supplied producer once it has succeeded, which is terminal, or
    /// once the response has ended while it has not failed, where a caller's deadline
    /// observer would finish.
    fn retire_producer_watch(&mut self) {
        let Some(producer) = &self.producer else {
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
            self.producer = None;
        }
    }

    /// Selects the supplied request producer's failure for an upload that failed before
    /// the submitter took the response and before the response ended.
    fn select_producer_failure(&self) -> Option<HttpError> {
        if self.status.delivered.get() {
            return None;
        }
        let Some(Err(error)) = self.producer.as_ref().and_then(IngressCompletion::snapshot) else {
            return None;
        };
        Some(self.status.error.borrow_mut().get_or_insert(error).clone())
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

    pub(super) fn egress_error(&self, headers_committed: bool) -> Option<(HttpError, H2ErrorCode)> {
        if let Some(error) = self.status.selected_error() {
            let wire = cancellation_wire_code(error.code());
            return Some((error, wire));
        }
        if headers_committed {
            return None;
        }
        self.status
            .admission
            .closed
            .borrow()
            .as_ref()
            .map(admission_closed_egress_error)
    }

    pub(super) fn cancel(&mut self, error: &HttpError) {
        let retirement = self
            .transfer
            .as_mut()
            .map(|transfer| transfer.prepare_stop_and_clear(error.clone()));
        let mut panic = None;
        if !self.status.response_ready.get() || self.status.selected_error().is_some() {
            capture(&mut panic, || self.status.fail(error.clone()));
        } else {
            capture(&mut panic, || self.status.publish_response());
        }
        if let Some(source) = self.unused_source.as_mut() {
            capture(&mut panic, || source.clear(Some(error.code())));
        }
        capture(&mut panic, || drop(self.unused_source.take()));
        if let Some(retirement) = retirement {
            if let Err(payload) = retirement.retire() {
                retain_panic(&mut panic, payload);
            }
        }
        if let Some(transfer) = self.transfer.as_mut() {
            capture(&mut panic, || transfer.drop_pending_read());
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        self.production_complete = true;
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }

    fn poll_upload(
        &mut self,
        context: &mut Context<'_>,
        control: &Control,
    ) -> Result<Option<HttpBodyEvent>, HttpError> {
        if self.production_complete {
            return Ok(None);
        }
        if let Some(resume) = &mut self.resume {
            match resume.as_mut().poll(context) {
                Poll::Pending => return Ok(None),
                Poll::Ready(roxygen_core::events::ResumeStatus::Cancelled) => {
                    return Err(failure(
                        HttpErrorCode::CORO_CANCELLED,
                        b"request body resume cancelled",
                    ));
                }
                Poll::Ready(_) => {
                    self.resume.take();
                }
            }
            if control.cancel.get()
                || self.status.admission.context.is_cancelled()
                || self.status.selected_error().is_some()
            {
                return Ok(None);
            }
        }
        let transfer = self
            .transfer
            .as_mut()
            .expect("unfinished upload retains its transfer");
        match transfer.poll_resume(context) {
            Poll::Pending => return Ok(None),
            Poll::Ready(Some(roxygen_core::events::ResumeStatus::Cancelled)) => {
                return Err(failure(
                    HttpErrorCode::CORO_CANCELLED,
                    b"request body resume cancelled",
                ));
            }
            Poll::Ready(Some(_)) => {
                if control.cancel.get()
                    || self.status.admission.context.is_cancelled()
                    || self.status.selected_error().is_some()
                {
                    return Ok(None);
                }
            }
            Poll::Ready(None) => {}
        }
        let Poll::Ready(result) =
            transfer.poll_read_body_event(context, UPLOAD_BYTES as u32, &self.status.cancellation)
        else {
            return Ok(None);
        };
        let event = result?;
        self.production_complete = event.eom();
        Ok(Some(event))
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        let mut panic = None;
        if !self.production_complete {
            capture(&mut panic, || {
                self.cancel(&failure(
                    HttpErrorCode::CORO_CANCELLED,
                    b"upstream upload owner retired",
                ))
            });
        }
        if let Some(transfer) = self.transfer.as_mut() {
            capture(&mut panic, || transfer.drop_pending_read());
        }
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.cancellation_wake.take()));
        capture(&mut panic, || drop(self.transfer.take()));
        capture(&mut panic, || drop(self.unused_source.take()));
        capture(&mut panic, || self.status.release());
        capture(&mut panic, || self.status.publish_response());
        capture(&mut panic, || self.retire_response_observer());
        finish_panic(panic);
    }
}

struct UpstreamApplication;
enum NoExchange {}

impl OwnedApplication for UpstreamApplication {
    type Exchange = NoExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> NoExchange {
        unreachable!("upstream responses cannot admit downstream application exchanges")
    }
}

impl OwnedExchange for NoExchange {
    fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {
        match *self {}
    }
    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        match *self {}
    }
    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        match *self {}
    }
    fn cancel(&mut self, _error: &HttpError, _peer: bool) {
        match *self {}
    }
    fn response_complete(&mut self) {
        match *self {}
    }
    fn complete(self) {
        match self {}
    }
}

/// Concrete upstream role over the same codec, pump, flow control and write fences.
///
/// It honors `UniplexConfig::deliver_ingress_before_abort` (`DIV-033`), which the downstream
/// role refuses: a response whose stream or connection fails keeps its received head and body
/// events for its reader, before the error, unless the request's consumer cancelled it (a
/// cancel, a dropped submission or the cancellation token), which discards them, as
/// `UniplexStreamAbort` does.
pub struct OwnedH2UpstreamSession(Driver<UpstreamApplication, true>);

impl OwnedH2UpstreamSession {
    /// Creates the experimental upstream session. It honors `read_timeout`, whose expiry drains
    /// the connection, `write_timeout`, which bounds a stalled transport write and an upload's
    /// wait for flow-control credit, and `stream_read_timeout`, which bounds each wait for
    /// response data from the request's EOM on (`DIV-028`). It refuses
    /// `Http2SessionConfig::abort_streams_on_eof`.
    pub fn new(options: OwnedH2Options) -> Result<(Self, OwnedH2UpstreamHandle), HttpError> {
        let (session, handle) =
            Driver::<UpstreamApplication, true>::new(UpstreamApplication, options)?;
        let admission = Rc::clone(
            handle
                .0
                .upstream
                .as_ref()
                .expect("upstream session has admission"),
        );
        Ok((
            Self(session),
            OwnedH2UpstreamHandle {
                control: handle.0,
                admission,
            },
        ))
    }

    pub fn run(self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        self.0.run()
    }

    /// Replaces the per-session random stream-table seed with a fixed one.
    #[cfg(test)]
    pub(crate) fn with_stream_seed(self, seed: u64) -> Self {
        Self(self.0.with_stream_seed(seed))
    }

    /// Whether the session built a flood-limit counter.
    #[cfg(test)]
    pub(crate) fn limits_rates(&self) -> bool {
        self.0.limits_rates()
    }
}

impl<A, const UPSTREAM: bool> Driver<A, UPSTREAM>
where
    A: OwnedApplication,
    A::Exchange: ExchangeStorage<UPSTREAM>,
{
    /// `None` when admission stayed open and no queued submission was taken.
    #[inline]
    pub(super) fn poll_upstream_submission(&mut self) -> Option<bool> {
        let admission = self.connection.state.control.upstream.as_ref()?;
        // A release inside this poll replaced its wake with this turn's progress.
        let released = admission.release_pending.replace(false);
        if self.eof
            || self.connection.state.draining
            || self.cancelled()
            || self.failure.fatal.is_some()
        {
            self.close_upstream_admission(admission);
            return Some(released);
        }
        if admission.queued.borrow().is_empty() {
            return released.then_some(true);
        }
        (self.submit_upstream() || released).then_some(true)
    }

    #[cold]
    fn close_upstream_admission(&self, admission: &Admission) {
        let error = self.failure.fatal.clone().unwrap_or_else(|| {
            if self.cancelled() {
                failure(
                    HttpErrorCode::CORO_CANCELLED,
                    b"owned upstream connection cancelled",
                )
            } else if self.eof {
                failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF")
            } else {
                failure(
                    HttpErrorCode::REFUSED_STREAM,
                    b"owned upstream connection is draining",
                )
            }
        });
        admission.close(error);
    }

    // Out of line so the per-turn check above stays small enough to inline into `poll`.
    // Returns whether a queued submission was taken.
    #[inline(never)]
    fn submit_upstream(&mut self) -> bool {
        let Some(admission) = self.connection.state.control.upstream.as_ref() else {
            return false;
        };
        // Admission bounds retained headers and sources even while native writes are held.
        let submission =
            if self.egress.chain_length() >= self.connection.state.config.maximum_write_size {
                let mut queued = admission.queued.borrow_mut();
                let cancelled = queued
                    .iter()
                    .position(|submission| submission.status.selected_error().is_some());
                cancelled.and_then(|position| queued.remove(position))
            } else {
                admission.queued.borrow_mut().pop_front()
            };
        let Some(mut submission) = submission else {
            return false;
        };
        let result = catch_unwind(AssertUnwindSafe(|| self.commit_upstream(&mut submission)));
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

    fn commit_upstream(&mut self, submission: &mut Submission) -> Result<(), HttpError> {
        if let Some(error) = submission.status.selected_error() {
            return Err(error);
        }
        let headers = submission
            .headers
            .as_mut()
            .expect("queued submission owns headers");
        if !headers.headers().is_request()
            || crate::uniplex::request_has_forbidden_body(headers.headers_mut())
        {
            return Err(failure(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"invalid upstream request head",
            ));
        }
        let head_response = headers.headers_mut().method().ok().flatten() == Some(HttpMethod::Head);
        let id = self.connection.codec.create_stream();
        let wake = Arc::new(StreamWake {
            id,
            queue: Arc::clone(&self.connection.state.control.ready),
            queued: AtomicBool::new(false),
            headers_poll_ready: AtomicBool::new(true),
            live: AtomicBool::new(true),
        });
        let signals = Rc::new(IngressSignals {
            control: Rc::clone(&self.connection.state.control),
            active: Cell::new(true),
            source_complete: Cell::new(false),
            response_complete: Cell::new(false),
            upstream: true,
            ingress_complete: Cell::new(false),
            wake: Arc::clone(&wake),
            consumed: Cell::new(0),
            acknowledge: Cell::new(0),
            completed: Cell::new(None),
            read_timed_out: Cell::new(false),
            wake_on_completion: Cell::new(false),
        });
        let source = HttpResponseSource::new_shared(
            id,
            signals.clone(),
            self.h2.stream_receive_window,
            self.context.host().clone(),
        )
        .map_err(|_| {
            failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid upstream receive window",
            )
        })?;
        if head_response {
            source.skip_content_length_validation();
        }
        if self.connection.state.config.deliver_ingress_before_abort {
            source.deliver_ingress_before_abort();
        }
        let producer = source.producer();
        // Untimed until the request's EOM is serialized, as in Proxygen (HTTPCoroSession.cpp:816)
        // and Uniplex's H2 deferral.
        producer.defer_read_timeout(DeferredReadTimeout::DeferUpdates(
            self.connection.state.config.stream_read_timeout,
        ));
        let status = Rc::clone(&submission.status);
        status.stream.set(Some(id));
        let committed = status.admission.committed.get();
        status.sequence.set(Some(committed));
        status.admission.committed.set(committed + 1);
        *status.ingress.borrow_mut() = source.ingress_completion();
        *status.wake.borrow_mut() = Some(Arc::clone(&wake));
        let headers = submission
            .headers
            .take()
            .expect("queued request headers are owned");
        let upload = submission
            .source
            .take()
            .expect("queued request source is owned");
        let exchange_status = Rc::clone(&status);
        let (transfer, unused_source) = if headers.eom() {
            (None, Some(upload))
        } else {
            (Some(HttpBodyTransfer::new(upload, id, UPLOAD_BYTES)), None)
        };
        let send_window = Window::new(self.connection.state.peer_initial_window)
            .expect("peer initial window was validated");
        // Allocating before the value is built lets it be written in place instead of copied.
        let stream = Box::write(
            Box::new_uninit(),
            Stream {
                application: Some(<A::Exchange as ExchangeStorage<UPSTREAM>>::store_upstream(
                    Exchange {
                        status: exchange_status,
                        transfer,
                        unused_source,
                        resume: None,
                        production_complete: headers.eom(),
                        cancellation_wake: submission.cancellation_wake.take(),
                        response_observer: None,
                        producer: submission.producer.take(),
                    },
                )),
                wake: Arc::clone(&wake),
                signals,
                source: Some(IngressSource::Response(source)),
                producer,
                drain: None,
                head: None,
                started: true,
                wire_ingress_complete: false,
                ingress_rejected: false,
                consumed: false,
                peer_reset: None,
                reset_after_response: false,
                resume: None,
                request_head: head_response,
                request_body_length: 0,
                request_body_events: false,
                expected_request_length: None,
                send_window,
                scheduling: PriorityEntry::default(),
                stalled: false,
                pending: Some(PendingEgress::Headers(headers)),
                response_headers: false,
                expects_408: false,
                response_complete: false,
                write_boundary: None,
                physically_complete: false,
                registered_byte_events: false,
                reset: false,
                stream_offset: 0,
                body_offset: 0,
            },
        );
        submission
            .reservation
            .as_mut()
            .expect("queued request owns a reservation")
            .active = false;
        submission.reservation.take();
        status.counted.set(true);
        self.connection
            .state
            .control
            .active_incoming
            .set(self.connection.state.control.active_incoming.get() + 1);
        self.connection.state.awaiting_ingress += 1;
        let stream = match self.streams.entry(id) {
            Entry::Vacant(entry) => entry.insert(stream),
            Entry::Occupied(mut entry) => {
                drop(entry.insert(stream));
                panic!("codec allocates each wire stream once");
            }
        };
        self.connection
            .state
            .scheduler
            .enqueue(id, &mut stream.scheduling);
        wake.enqueue();
        Ok(())
    }
}

impl<const UPSTREAM: bool> StreamConnection<'_, UPSTREAM> {
    pub(super) fn poll_upstream_stream<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) -> Result<(), HttpError> {
        let error = stream
            .upstream()
            .expect("upstream stream owns its exchange")
            .egress_error(stream.response_headers);
        if let Some((error, wire)) = error {
            self.egress_failed(id, stream, error, wire);
            return Ok(());
        }
        let exchange = stream
            .upstream_mut()
            .expect("upstream stream owns its exchange");
        // Without a source to drop, `egress_error` has just checked the selected error.
        if let Some(source) = exchange.unused_source.take() {
            drop(source);
            if let Some(error) = exchange.status.selected_error() {
                self.reset_selected(id, stream, error);
                return Ok(());
            }
        }
        if self.cancelled() {
            return Ok(());
        }
        let progress = {
            let waker = waker_ref(&stream.wake);
            let mut context = Context::from_waker(&waker);
            let Some(exchange) = stream
                .application
                .as_mut()
                .and_then(<E as ExchangeStorage<UPSTREAM>>::upstream_mut)
            else {
                unreachable!("upstream stream owns its exchange");
            };
            if exchange.cancellation_wake.is_none()
                && exchange.status.cancellation.can_be_cancelled()
            {
                exchange.cancellation_wake = Some(exchange.status.cancellation.wake_registration());
            }
            if let Some(registration) = &mut exchange.cancellation_wake {
                registration.update_waker(context.waker());
            }
            if !stream.response_headers || stream.pending.is_some() {
                return Ok(());
            }
            exchange.poll_upload(&mut context, &self.connection.state.control)
        };
        let progress = progress.map(|event| {
            let Some(event) = event else {
                return;
            };
            if matches!(event.kind(), HttpBodyEventKind::Suspend(_)) {
                let HttpBodyEventKind::Suspend(resume) = event.into_kind() else {
                    unreachable!("the upload event kind was checked");
                };
                stream
                    .upstream_mut()
                    .expect("upstream stream owns its exchange")
                    .resume = Some(resume);
                stream.wake.enqueue();
            } else {
                stream.pending = Some(PendingEgress::Body(event));
            }
        });
        if let Some(error) = stream
            .upstream()
            .expect("upstream stream owns its exchange")
            .status
            .selected_error()
        {
            self.reset_selected(id, stream, error);
            return Ok(());
        }
        if let Err(error) = progress {
            self.upload_failed(id, stream, error);
            return Ok(());
        }
        if stream.pending.is_some() {
            self.connection
                .state
                .scheduler
                .enqueue(id, &mut stream.scheduling);
        }
        Ok(())
    }

    #[cold]
    #[inline(never)]
    fn egress_failed<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: HttpError,
        wire: H2ErrorCode,
    ) {
        if !stream.response_complete || !stream.wire_ingress_complete {
            self.reset_owner(id, stream, &error, false, Some(wire));
        }
    }

    #[cold]
    #[inline(never)]
    fn reset_selected<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: HttpError,
    ) {
        self.reset_owner(
            id,
            stream,
            &error,
            false,
            Some(cancellation_wire_code(error.code())),
        );
    }

    #[cold]
    #[inline(never)]
    fn upload_failed<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
        error: HttpError,
    ) {
        if let Some(error) = stream
            .upstream()
            .expect("upstream stream owns its exchange")
            .select_producer_failure()
        {
            self.reset_owner(
                id,
                stream,
                &error,
                false,
                Some(cancellation_wire_code(error.code())),
            );
            return;
        }
        stream.producer.abort_with_error(
            failure(HttpErrorCode::CORO_CANCELLED, b"egress source failed"),
            b"egress source failed",
        );
        self.reset_owner(
            id,
            stream,
            &error,
            false,
            Some(http_error_code_to_h2_error_code(error.code(), true)),
        );
    }

    pub(super) fn acknowledge_upstream<E: ExchangeStorage<UPSTREAM>>(
        &mut self,
        id: StreamId,
        stream: &mut Stream<E, UPSTREAM>,
    ) -> Result<(), HttpError> {
        let status = &stream
            .upstream()
            .expect("upstream stream owns its exchange")
            .status;
        if status.response_ready.get() {
            return Ok(());
        }
        let status = Rc::clone(status);
        if let Some(error) = status.selected_error() {
            self.reset_selected(id, stream, error);
            return Ok(());
        }
        if self.cancelled() {
            return Err(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"request commitment cancelled the connection",
            ));
        }
        let source = stream
            .source
            .take()
            .expect("unacknowledged request retains its response source");
        let IngressSource::Response(source) = source else {
            unreachable!("upstream requests retain their response source");
        };
        status.acknowledge(id, source);
        Ok(())
    }
}
