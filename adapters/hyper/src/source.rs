//! Demand-driven application bodies with worker- or application-owned joins.
//!
//! Prepare on the Roxygen worker, then attach on the application's live Tokio
//! runtime. Keep that runtime progressing through producer joins, including
//! application-owned joins which outlive forced native-worker destruction.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use http::HeaderMap;
use http_body::Body;
use http_body::SizeHint;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_runtime::context::ContextEnvelope;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerCleanupObserver;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinError;
use tokio::task::JoinSet;

use crate::body::BoxError;

/// Logical byte bounds for an application body bridge.
///
/// At most one accepted application frame is retained, plus one delivered
/// demand chunk. Data is split by sharing `Bytes`, not copying it. These bounds
/// do not constrain the producer's own allocations or a slice's backing capacity.
#[derive(Clone, Copy, Debug)]
pub struct ApplicationBodyOptions {
    /// Reject larger data frames, or trailers whose name/value bytes exceed this
    /// ceiling. The default is 16 MiB, including the readiness 4 MiB fixtures.
    pub maximum_frame_bytes: NonZeroUsize,
    /// Upper bound on a chunk delivered for any one worker read.
    pub maximum_demand_bytes: NonZeroU32,
}

impl Default for ApplicationBodyOptions {
    fn default() -> Self {
        Self {
            maximum_frame_bytes: NonZeroUsize::new(16 * 1024 * 1024)
                .expect("a fixed frame ceiling is nonzero"),
            maximum_demand_bytes: NonZeroU32::new(64 * 1024)
                .expect("a fixed demand bound is nonzero"),
        }
    }
}

/// Failure to admit an application body, before polling or taking its ownership.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ApplicationBodyAttachError {
    /// Attachment requires the live application Tokio runtime to be entered.
    #[error("attach must run on the live application Tokio runtime")]
    NoApplicationRuntime,
    /// Worker admission or this source's attachment admission has closed.
    #[error("application body attachment is closed")]
    Closed,
}

/// Returns an unaccepted body to its application-side caller.
#[derive(Debug)]
pub struct RejectedApplicationBody<B> {
    /// The body, never polled or moved into a worker task.
    pub body: B,
    /// The reason admission failed.
    pub reason: ApplicationBodyAttachError,
}

/// The physically joined application's original completion outcome.
///
/// Application errors retain their original boxed value for facade classification.
/// The core source maps cancellation to `CORO_CANCELLED`, read timeout to
/// `READ_TIMEOUT`, and producer/bridge failures to `BODY_PARSE_ERROR`.
#[derive(Debug, Error)]
pub enum ApplicationBodyError {
    /// The producer supplied an arbitrary application error.
    #[error("application body failed: {0}")]
    Application(#[source] BoxError),
    /// The bridge rejected an oversized frame without copying its data.
    #[error("application body frame has {actual} bytes, exceeding {maximum}")]
    FrameTooLarge {
        /// Logical bytes in the rejected frame.
        actual: usize,
        /// Configured accepted-frame ceiling.
        maximum: usize,
    },
    /// A trailer value would be changed by Roxygen header normalization.
    #[error("application trailers contain boundary whitespace")]
    InvalidTrailers,
    /// A future frame kind has no Roxygen representation.
    #[error("unsupported application body frame")]
    UnsupportedFrame,
    /// The source, attachment endpoint, or worker cancelled the producer.
    #[error("application body cancelled")]
    Cancelled,
    /// The application task panicked or its runtime cancelled it.
    #[error("application body task failed: {0}")]
    Task(#[source] JoinError),
    /// Rooted cleanup could not deliver its joined outcome.
    #[error("application body cleanup outcome was lost")]
    CleanupLost,
}

impl ApplicationBodyError {
    fn as_http_error(&self) -> HttpError {
        let code = if matches!(self, Self::Cancelled) {
            HttpErrorCode::CORO_CANCELLED
        } else {
            HttpErrorCode::BODY_PARSE_ERROR
        };
        HttpError::with_message(code, self.to_string())
    }
}

type ApplicationOutcome = Result<(), ApplicationBodyError>;

/// A Send observation of completion after the app task has actually been joined.
/// Dropping this observer neither cancels nor detaches any task.
pub struct ApplicationBodyCompletion {
    receiver: oneshot::Receiver<ApplicationOutcome>,
}

impl Future for ApplicationBodyCompletion {
    type Output = Result<(), ApplicationBodyError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(context)
            .map(|result| result.unwrap_or(Err(ApplicationBodyError::CleanupLost)))
    }
}

/// An application-side join owner and its single-use attachment handshake.
/// Register the driver with a bounded caller-owned parent before awaiting
/// readiness. That parent must drive and separately reap the driver itself.
pub struct ApplicationBodyJoinPreparation {
    driver: ApplicationBodyJoinDriver,
    ready: ApplicationBodyAttachmentReady,
    cleanup: ApplicationBodyCleanupObserver,
}

impl ApplicationBodyJoinPreparation {
    /// Separates execution ownership from attachment and cleanup observation.
    pub fn into_parts(
        self,
    ) -> (
        ApplicationBodyJoinDriver,
        ApplicationBodyAttachmentReady,
        ApplicationBodyCleanupObserver,
    ) {
        (self.driver, self.ready, self.cleanup)
    }
}

/// A caller-owned application-runtime future which reaps the producer task.
///
/// Its first poll arms attachment; its output preserves the original producer
/// outcome after joining. The caller must separately reap this driver and keep
/// the attachment runtime alive. Neither this output nor cleanup observation
/// certifies the enclosing driver's join, arbitrary children, or worker joins.
/// Dropping this ultimate owner after acceptance requests cancellation but
/// cannot asynchronously join: cleanup becomes unconfirmed, without a reaper.
/// A first poll outside Tokio rejects readiness with `NoApplicationRuntime`
/// and returns `Cancelled` without accepting a producer.
pub struct ApplicationBodyJoinDriver {
    future: Pin<Box<dyn Future<Output = ApplicationOutcome> + Send>>,
}

impl Future for ApplicationBodyJoinDriver {
    type Output = Result<(), ApplicationBodyError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(context)
    }
}

/// Delivers an attachment only after its application join driver is armed.
/// Dropping an unconsumed readiness cancels admission; dropping an exhausted
/// readiness does not cancel the attachment or an accepted producer.
pub struct ApplicationBodyAttachmentReady {
    receiver: oneshot::Receiver<Result<ApplicationBodyAttachment, ApplicationBodyAttachError>>,
    pending: Option<Arc<ApplicationShared>>,
}

impl Future for ApplicationBodyAttachmentReady {
    type Output = Result<ApplicationBodyAttachment, ApplicationBodyAttachError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.pending.take();
                Poll::Ready(result.unwrap_or(Err(ApplicationBodyAttachError::Closed)))
            }
        }
    }
}

impl Drop for ApplicationBodyAttachmentReady {
    fn drop(&mut self) {
        if let Some(shared) = self.pending.take() {
            shared.request_stop();
        }
    }
}

/// Missing confirmation of producer reaping, worker-scope retirement, or both.
/// A joined producer error or panic is an operation outcome, not a missing join.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("application body cleanup unconfirmed (producer: {producer}, worker scope: {scope:?})")]
pub struct ApplicationBodyCleanupError {
    producer: bool,
    scope: Option<WorkerCleanupObservationError>,
}

impl ApplicationBodyCleanupError {
    /// Whether no producer-join (or closed empty-group) certificate was obtained.
    pub fn producer_join_unconfirmed(&self) -> bool {
        self.producer
    }

    /// The independent registered worker-scope retirement failure, if any.
    pub fn worker_scope_error(&self) -> Option<WorkerCleanupObservationError> {
        self.scope
    }
}

/// Repeatable observation of producer reaping AND actual worker-scope retirement.
/// It neither owns execution nor cancels it when dropped. Success excludes the
/// enclosing driver join, shared referents, arbitrary children, I/O and workers.
#[derive(Clone)]
pub struct ApplicationBodyCleanupObserver {
    producer: Arc<ProducerJoinState>,
    scope: WorkerCleanupObserver,
}

impl ApplicationBodyCleanupObserver {
    /// Waits for both boundaries even if either reports uncertainty first.
    pub fn wait(
        &self,
    ) -> impl Future<Output = Result<(), ApplicationBodyCleanupError>> + Send + 'static {
        let producer = ProducerJoinWait::new(Arc::clone(&self.producer));
        let scope = self.scope.wait();
        async move {
            let (producer, scope) = tokio::join!(producer, scope);
            if producer && scope.is_ok() {
                Ok(())
            } else {
                Err(ApplicationBodyCleanupError {
                    producer: !producer,
                    scope: scope.err(),
                })
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn joining(&self) -> bool {
        self.producer.joining.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn reaped(&self) -> usize {
        self.producer.reaped.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn test_observation(
        scope: WorkerCleanupObserver,
    ) -> (Self, ApplicationJoinTestPublisher) {
        let producer = Arc::new(ProducerJoinState::default());
        let publisher = ProducerJoinPublisher(Some(Arc::clone(&producer)));
        (
            Self { producer, scope },
            ApplicationJoinTestPublisher(publisher),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_publish(&self, confirmed: bool) {
        self.producer.publish(confirmed);
    }
}

#[cfg(test)]
pub(crate) struct ApplicationJoinTestPublisher(ProducerJoinPublisher);

#[cfg(test)]
impl ApplicationJoinTestPublisher {
    pub(crate) fn confirm(mut self) {
        self.0.publish(true);
    }
}

fn discard_application_panic(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

/// Notification failures cannot replace the separately owned producer outcome
/// or prevent an independent cancellation/destruction notification attempt.
fn application_cleanup(action: impl FnOnce()) -> bool {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(()) => true,
        Err(payload) => {
            discard_application_panic(payload);
            false
        }
    }
}

#[derive(Default)]
struct ProducerJoinObservation {
    outcome: Option<bool>,
    waiters: Vec<Option<Waker>>,
}

#[derive(Default)]
struct ProducerJoinState {
    observation: Mutex<ProducerJoinObservation>,
    #[cfg(test)]
    joining: AtomicBool,
    #[cfg(test)]
    reaped: AtomicUsize,
}

impl ProducerJoinState {
    fn lock(&self) -> MutexGuard<'_, ProducerJoinObservation> {
        self.observation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn publish(&self, confirmed: bool) {
        let waiters = {
            let mut state = self.lock();
            if state.outcome.is_some() {
                return;
            }
            state.outcome = Some(confirmed);
            std::mem::take(&mut state.waiters)
        };
        for waker in waiters.into_iter().flatten() {
            application_cleanup(|| waker.wake());
        }
    }
}

struct ProducerJoinPublisher(Option<Arc<ProducerJoinState>>);

impl ProducerJoinPublisher {
    fn publish(&mut self, confirmed: bool) {
        if let Some(shared) = self.0.take() {
            shared.publish(confirmed);
        }
    }
}

impl Drop for ProducerJoinPublisher {
    fn drop(&mut self) {
        self.publish(false);
    }
}

struct ProducerJoinWait {
    shared: Arc<ProducerJoinState>,
    slot: Option<usize>,
}

impl ProducerJoinWait {
    fn new(shared: Arc<ProducerJoinState>) -> Self {
        Self { shared, slot: None }
    }
}

impl Future for ProducerJoinWait {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<bool> {
        let wait = self.get_mut();
        let waker = context.waker().clone();
        let mut state = wait.shared.lock();
        if let Some(outcome) = state.outcome {
            return Poll::Ready(outcome);
        }
        let previous = if let Some(slot) = wait.slot {
            state.waiters[slot].replace(waker)
        } else if let Some(slot) = state.waiters.iter().position(Option::is_none) {
            state.waiters[slot] = Some(waker);
            wait.slot = Some(slot);
            None
        } else {
            wait.slot = Some(state.waiters.len());
            state.waiters.push(Some(waker));
            None
        };
        drop(state);
        application_cleanup(|| drop(previous));
        Poll::Pending
    }
}

impl Drop for ProducerJoinWait {
    fn drop(&mut self) {
        let waker = self.slot.and_then(|slot| {
            self.shared
                .lock()
                .waiters
                .get_mut(slot)
                .and_then(Option::take)
        });
        application_cleanup(|| drop(waker));
    }
}

enum ApplicationJoinPhase {
    Unarmed(JoinSet<ApplicationOutcome>),
    Ready(JoinSet<ApplicationOutcome>),
    Attaching,
    Running(JoinSet<ApplicationOutcome>),
    Joining,
    Closed,
}

struct ApplicationJoinState {
    phase: ApplicationJoinPhase,
    alive: bool,
    accepted: bool,
}

struct ApplicationJoinOwner(Mutex<ApplicationJoinState>);

impl ApplicationJoinOwner {
    fn lock(&self) -> MutexGuard<'_, ApplicationJoinState> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }
}

struct ApplicationJoinLifetime {
    shared: Arc<ApplicationShared>,
    owner: Arc<ApplicationJoinOwner>,
    publisher: ProducerJoinPublisher,
}

impl Drop for ApplicationJoinLifetime {
    fn drop(&mut self) {
        if self.publisher.0.is_none() {
            return;
        }
        let (phase, accepted) = {
            let _admission = self.shared.tasks();
            let mut owner = self.owner.lock();
            owner.alive = false;
            (
                std::mem::replace(&mut owner.phase, ApplicationJoinPhase::Closed),
                owner.accepted,
            )
        };
        application_cleanup(|| self.shared.request_stop());
        let destroyed = application_cleanup(|| drop(phase));
        self.publisher.publish(!accepted && destroyed);
    }
}

/// Admission transfers the empty group outside locks before spawning. The
/// transfer guard returns even a partially initialized group to its app joiner.
/// A concurrent ultimate-owner loss is explicitly unconfirmed, never rescued.
struct ApplicationGroupTransfer {
    group: Option<JoinSet<ApplicationOutcome>>,
    owner: Arc<ApplicationJoinOwner>,
    shared: Arc<ApplicationShared>,
}

impl Drop for ApplicationGroupTransfer {
    fn drop(&mut self) {
        {
            let mut owner = self.owner.lock();
            if owner.alive {
                owner.phase = ApplicationJoinPhase::Running(
                    self.group
                        .take()
                        .expect("an attachment transfers its group once"),
                );
            }
        }
        application_cleanup(|| self.shared.changed.notify_one());
        if let Some(group) = self.group.take() {
            application_cleanup(|| self.shared.request_stop());
            application_cleanup(|| drop(group));
        }
    }
}

struct ApplicationOwnedShutdown {
    shared: Arc<ApplicationShared>,
    registration: Option<WorkerCleanupRegistration>,
    completed: bool,
}

impl Drop for ApplicationOwnedShutdown {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        let mut failure = None;
        if !self.completed {
            failure = catch_unwind(AssertUnwindSafe(|| self.shared.request_stop())).err();
        }
        if let Some(registration) = self.registration.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                let _ = registration.disarm();
            }))
        {
            if failure.is_none() {
                failure = Some(payload);
            } else {
                discard_application_panic(payload);
            }
        }
        if let Some(payload) = failure {
            if unwinding {
                discard_application_panic(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}

enum ApplicationTasks {
    Prepared(JoinSet<ApplicationOutcome>),
    Running(JoinSet<ApplicationOutcome>),
    Joining,
    Closed,
    ApplicationOwned,
}

struct ApplicationShared {
    worker: WorkerHandle,
    tasks: Mutex<ApplicationTasks>,
    stopped: AtomicBool,
    initially_terminal: AtomicBool,
    cancellation: watch::Sender<bool>,
    changed: Notify,
    size_hint: Mutex<Option<SizeHint>>,
}

impl ApplicationShared {
    fn closed_chunk(&self) -> Result<BodyChunk, HttpError> {
        if self.initially_terminal.load(Ordering::Acquire) {
            Ok(BodyChunk::Data(Bytes::new(), true))
        } else {
            Err(producer_closed())
        }
    }

    fn tasks(&self) -> MutexGuard<'_, ApplicationTasks> {
        self.tasks.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn request_stop(&self) {
        let (requested, application_owned) = {
            let tasks = self.tasks();
            (
                !self.stopped.swap(true, Ordering::AcqRel),
                matches!(*tasks, ApplicationTasks::ApplicationOwned),
            )
        };
        if requested {
            if application_owned {
                application_cleanup(|| {
                    self.cancellation.send_replace(true);
                });
                application_cleanup(|| self.changed.notify_one());
            } else {
                self.cancellation.send_replace(true);
                self.changed.notify_one();
            }
        }
    }

    fn consume_hint(&self, bytes: usize, terminal: bool) {
        let mut hint = self
            .size_hint
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if terminal {
            *hint = Some(SizeHint::with_exact(0));
        } else if let Some(previous) = *hint {
            let mut remaining = SizeHint::new();
            remaining.set_lower(previous.lower().saturating_sub(bytes as u64));
            if let Some(upper) = previous.upper() {
                remaining.set_upper(upper.saturating_sub(bytes as u64));
            }
            *hint = Some(remaining);
        }
    }
}

struct ApplicationShutdown(Option<WorkerCleanupRegistration>);

impl Drop for ApplicationShutdown {
    fn drop(&mut self) {
        if let Some(registration) = self.0.take() {
            let _ = registration.disarm();
        }
    }
}

enum BodyChunk {
    Data(Bytes, bool),
    Trailers(HttpHeaders),
}

struct Demand {
    maximum: u32,
    response: oneshot::Sender<Result<BodyChunk, HttpError>>,
}

struct ProducerInput {
    demands: mpsc::Receiver<Demand>,
    metadata: oneshot::Sender<bool>,
}

#[derive(Clone, Copy)]
enum AttachmentAdmission {
    New,
    ReservedResponse,
}

impl AttachmentAdmission {
    fn permits(self, worker: &WorkerHandle) -> bool {
        match self {
            Self::New => worker.is_accepting(),
            Self::ReservedResponse => worker.state() == WorkerState::Open,
        }
    }
}

/// A Send, single-use attachment endpoint. It never owns an application body
/// until `attach` admits that body into the pre-registered task group.
pub struct ApplicationBodyAttachment {
    shared: Arc<ApplicationShared>,
    input: Option<ProducerInput>,
    options: ApplicationBodyOptions,
    admission: AttachmentAdmission,
    join_owner: Option<Arc<ApplicationJoinOwner>>,
}

impl ApplicationBodyAttachment {
    /// Attaches on the application's live Tokio runtime, which must continue
    /// progressing until Roxygen quiescence completes. All body methods and its
    /// destructor run in that runtime's task. Neither `Sync` nor `Unpin` is needed.
    /// In app-owned mode the runtime must also progress after native-worker loss
    /// until the caller's join driver and its parent have been reaped.
    ///
    /// Rejection returns the untouched body. Successful attachment does not
    /// transfer task-join ownership: legacy preparation retains it on the worker;
    /// application-join preparation retains it in the already-armed app driver.
    /// Ordinary preparation requires admission still open at attachment. Only
    /// response preparation reserves attachment through
    /// graceful quiescence; forced shutdown and source cancellation still reject.
    pub fn attach<B>(mut self, body: B) -> Result<(), RejectedApplicationBody<B>>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        if Handle::try_current().is_err() {
            return Err(RejectedApplicationBody {
                body,
                reason: ApplicationBodyAttachError::NoApplicationRuntime,
            });
        }
        if let Some(owner) = self.join_owner.take() {
            let envelope = ContextEnvelope::capture();
            let group = {
                let _admission = self.shared.tasks();
                let mut state = owner.lock();
                if self.shared.stopped.load(Ordering::Acquire)
                    || !self.admission.permits(&self.shared.worker)
                    || !state.alive
                    || !matches!(state.phase, ApplicationJoinPhase::Ready(_))
                {
                    return Err(RejectedApplicationBody {
                        body,
                        reason: ApplicationBodyAttachError::Closed,
                    });
                }
                state.accepted = true;
                let ApplicationJoinPhase::Ready(group) =
                    std::mem::replace(&mut state.phase, ApplicationJoinPhase::Attaching)
                else {
                    unreachable!("attachment admission retains an armed group")
                };
                group
            };
            let mut transfer = ApplicationGroupTransfer {
                group: Some(group),
                owner,
                shared: Arc::clone(&self.shared),
            };
            let input = self
                .input
                .take()
                .expect("an admitted attachment retains its input");
            let future = pump_application(body, input, Arc::clone(&self.shared), self.options);
            transfer
                .group
                .as_mut()
                .expect("the attachment owns its group")
                .spawn(envelope.wrap(future));
            return Ok(());
        }
        let mut tasks = self.shared.tasks();
        if self.shared.stopped.load(Ordering::Acquire)
            || !self.admission.permits(&self.shared.worker)
            || !matches!(*tasks, ApplicationTasks::Prepared(_))
        {
            return Err(RejectedApplicationBody {
                body,
                reason: ApplicationBodyAttachError::Closed,
            });
        }
        let ApplicationTasks::Prepared(mut group) =
            std::mem::replace(&mut *tasks, ApplicationTasks::Joining)
        else {
            unreachable!("attachment admission was checked under the task lock")
        };
        let input = self
            .input
            .take()
            .expect("an admitted attachment retains its input");
        let future = pump_application(body, input, Arc::clone(&self.shared), self.options);
        group.spawn(ContextEnvelope::capture().wrap(future));
        *tasks = ApplicationTasks::Running(group);
        drop(tasks);
        self.shared.changed.notify_one();
        Ok(())
    }
}

impl Drop for ApplicationBodyAttachment {
    fn drop(&mut self) {
        if self.input.is_some() {
            self.shared.request_stop();
        }
    }
}

/// An owner-local response-body reservation made before application service work.
/// It cannot be read until bound to the service's real response head. Dropping it
/// cancels the attachment and releases its rooted scope after application join.
pub struct PreparedApplicationResponse {
    source: ApplicationBodySource,
}

impl PreparedApplicationResponse {
    /// Binds the actual native response head without new worker admission.
    /// Call on the owner through the exchange's already-rooted response delivery,
    /// not a fresh worker command during grace. The enclosing exchange must keep
    /// its own service work and response delivery alive through quiescence.
    ///
    /// Cancellation remains sticky: binding a head cannot reopen the body, and
    /// reads on a cancelled reservation report `CORO_CANCELLED`.
    pub fn into_source(mut self, message: HttpMessage) -> ApplicationBodySource {
        self.source.message = Some(message);
        self.source
    }
}

/// An owner-local source backed by an application-runtime body producer.
///
/// Reads have one outstanding demand. Dropping a pending read or this source
/// requests cancellation without waiting for any queue or task join. Scope
/// cleanup retains credit even if this source outlives its initializer. Legacy
/// preparation joins on the worker; app-owned preparation observes its joiner.
pub struct ApplicationBodySource {
    shared: Arc<ApplicationShared>,
    message: Option<HttpMessage>,
    metadata: Option<oneshot::Receiver<bool>>,
    demands: mpsc::Sender<Demand>,
    reading: Rc<Cell<bool>>,
    timeout: Option<Duration>,
    options: ApplicationBodyOptions,
}

impl ApplicationBodySource {
    /// Prepares a source whose producer is joined by a caller-owned app driver.
    /// Arm that driver before awaiting attachment; keep its parent and the app
    /// runtime alive through producer reaping and separately reap the driver.
    pub fn prepare_with_application_join(
        context: &WorkerContext,
        message: HttpMessage,
        options: ApplicationBodyOptions,
    ) -> Result<(Self, ApplicationBodyJoinPreparation), CleanupRegistrationError> {
        Self::prepare_application_join(context, Some(message), options, AttachmentAdmission::New)
    }

    /// Reserves an app-owned response producer before service dispatch. Its real
    /// head can be bound and its body attached during grace without new worker
    /// admission. Forced shutdown/source cancellation still closes attachment.
    pub fn prepare_response_with_application_join(
        context: &WorkerContext,
        options: ApplicationBodyOptions,
    ) -> Result<
        (PreparedApplicationResponse, ApplicationBodyJoinPreparation),
        CleanupRegistrationError,
    > {
        let (source, preparation) = Self::prepare_application_join(
            context,
            None,
            options,
            AttachmentAdmission::ReservedResponse,
        )?;
        Ok((PreparedApplicationResponse { source }, preparation))
    }

    fn prepare_application_join(
        context: &WorkerContext,
        message: Option<HttpMessage>,
        options: ApplicationBodyOptions,
        admission: AttachmentAdmission,
    ) -> Result<(Self, ApplicationBodyJoinPreparation), CleanupRegistrationError> {
        if !context.handle().is_accepting() {
            return Err(CleanupRegistrationError::Closed);
        }
        let (cancellation, _) = watch::channel(false);
        let shared = Arc::new(ApplicationShared {
            worker: context.handle().clone(),
            tasks: Mutex::new(ApplicationTasks::ApplicationOwned),
            stopped: AtomicBool::new(false),
            initially_terminal: AtomicBool::new(false),
            cancellation,
            changed: Notify::new(),
            size_hint: Mutex::new(None),
        });
        let producer = Arc::new(ProducerJoinState::default());
        let owner = Arc::new(ApplicationJoinOwner(Mutex::new(ApplicationJoinState {
            phase: ApplicationJoinPhase::Unarmed(JoinSet::new()),
            alive: true,
            accepted: false,
        })));
        let lifetime = ApplicationJoinLifetime {
            shared: Arc::clone(&shared),
            owner: Arc::clone(&owner),
            publisher: ProducerJoinPublisher(Some(Arc::clone(&producer))),
        };
        let shutdown_shared = Arc::clone(&shared);
        let shutdown = ApplicationOwnedShutdown {
            shared: Arc::clone(&shared),
            registration: Some(context.register_cleanup(move || shutdown_shared.request_stop())?),
            completed: false,
        };
        let joined = ProducerJoinWait::new(Arc::clone(&producer));
        let scope = context.register_scope(async move {
            let mut shutdown = shutdown;
            joined.await;
            shutdown.completed = true;
        })?;
        let observation = scope.observe();
        drop(scope.activate()?);
        if !context.handle().is_accepting() {
            return Err(CleanupRegistrationError::Closed);
        }
        let (demand_tx, demand_rx) = mpsc::channel(1);
        let (metadata_tx, metadata_rx) = oneshot::channel();
        let attachment = ApplicationBodyAttachment {
            shared: Arc::clone(&shared),
            input: Some(ProducerInput {
                demands: demand_rx,
                metadata: metadata_tx,
            }),
            options,
            admission,
            join_owner: Some(owner),
        };
        let (ready_tx, ready_rx) = oneshot::channel();
        let preparation = ApplicationBodyJoinPreparation {
            driver: ApplicationBodyJoinDriver {
                future: Box::pin(drive_application_join(lifetime, ready_tx, attachment)),
            },
            ready: ApplicationBodyAttachmentReady {
                receiver: ready_rx,
                pending: Some(Arc::clone(&shared)),
            },
            cleanup: ApplicationBodyCleanupObserver {
                producer,
                scope: observation,
            },
        };
        Ok((
            Self {
                shared,
                message,
                metadata: Some(metadata_rx),
                demands: demand_tx,
                reading: Rc::new(Cell::new(false)),
                timeout: None,
                options,
            },
            preparation,
        ))
    }

    /// Registers ownership on the worker before exposing the application-side
    /// attachment endpoint. No application body crosses this preparation call.
    /// New preparation is rejected during graceful admission closure as well as
    /// forced shutdown. The completion observer resolves only after physical join.
    pub fn prepare(
        context: &WorkerContext,
        message: HttpMessage,
        options: ApplicationBodyOptions,
    ) -> Result<
        (Self, ApplicationBodyAttachment, ApplicationBodyCompletion),
        CleanupRegistrationError,
    > {
        Self::prepare_with_admission(context, Some(message), options, AttachmentAdmission::New)
    }

    /// Reserves a response-body scope and Send attachment before app service
    /// dispatch, while worker admission is open. No placeholder head is created.
    /// Bind the actual head later with [`PreparedApplicationResponse::into_source`].
    ///
    /// This single-use attachment survives graceful admission closure, but not
    /// forced shutdown or source cancellation. Dropping either unused endpoint
    /// cancels the reservation. All accepted app tasks are joined by the scope;
    /// the app runtime must keep progressing until quiescence completes.
    pub fn prepare_response(
        context: &WorkerContext,
        options: ApplicationBodyOptions,
    ) -> Result<
        (
            PreparedApplicationResponse,
            ApplicationBodyAttachment,
            ApplicationBodyCompletion,
        ),
        CleanupRegistrationError,
    > {
        let (source, attachment, completion) = Self::prepare_with_admission(
            context,
            None,
            options,
            AttachmentAdmission::ReservedResponse,
        )?;
        Ok((
            PreparedApplicationResponse { source },
            attachment,
            completion,
        ))
    }

    fn prepare_with_admission(
        context: &WorkerContext,
        message: Option<HttpMessage>,
        options: ApplicationBodyOptions,
        admission: AttachmentAdmission,
    ) -> Result<
        (Self, ApplicationBodyAttachment, ApplicationBodyCompletion),
        CleanupRegistrationError,
    > {
        if !context.handle().is_accepting() {
            return Err(CleanupRegistrationError::Closed);
        }
        let (cancellation, _) = watch::channel(false);
        let shared = Arc::new(ApplicationShared {
            worker: context.handle().clone(),
            tasks: Mutex::new(ApplicationTasks::Prepared(JoinSet::new())),
            stopped: AtomicBool::new(false),
            initially_terminal: AtomicBool::new(false),
            cancellation,
            changed: Notify::new(),
            size_hint: Mutex::new(None),
        });
        let shutdown_shared = Arc::clone(&shared);
        let shutdown = ApplicationShutdown(Some(context.register_cleanup(move || {
            shutdown_shared.request_stop();
        })?));
        let (completion_tx, completion_rx) = oneshot::channel();
        let cleanup_shared = Arc::clone(&shared);
        let scope = context.register_scope(async move {
            let _shutdown = shutdown;
            let outcome = join_application(&cleanup_shared).await;
            let _ = completion_tx.send(outcome);
        })?;
        drop(scope.activate()?);
        if !context.handle().is_accepting() {
            shared.request_stop();
            return Err(CleanupRegistrationError::Closed);
        }
        let (demand_tx, demand_rx) = mpsc::channel(1);
        let (metadata_tx, metadata_rx) = oneshot::channel();
        Ok((
            Self {
                shared: Arc::clone(&shared),
                message,
                metadata: Some(metadata_rx),
                demands: demand_tx,
                reading: Rc::new(Cell::new(false)),
                timeout: None,
                options,
            },
            ApplicationBodyAttachment {
                shared,
                input: Some(ProducerInput {
                    demands: demand_rx,
                    metadata: metadata_tx,
                }),
                options,
                admission,
                join_owner: None,
            },
            ApplicationBodyCompletion {
                receiver: completion_rx,
            },
        ))
    }

    /// Returns the application's remaining-data hint after task initialization.
    /// A zero byte hint does not imply end-of-stream: trailers may still follow.
    pub fn size_hint(&self) -> Option<SizeHint> {
        *self
            .shared
            .size_hint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn begin_read(&self) -> Result<ApplicationRead, HttpError> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(HttpError::new(HttpErrorCode::CORO_CANCELLED));
        }
        if self.reading.replace(true) {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"an application body read is already in flight",
            ));
        }
        Ok(ApplicationRead {
            shared: Arc::clone(&self.shared),
            reading: Rc::clone(&self.reading),
            completed: false,
        })
    }
}

impl HttpSource for ApplicationBodySource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let guard = match self.begin_read() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let message = self.message.take();
        let metadata = self.metadata.take();
        let timeout = self.timeout;
        Box::pin(await_read(
            async move {
                let (Some(message), Some(metadata)) = (message, metadata) else {
                    return Err(HttpError::new(HttpErrorCode::INVALID_STATE_TRANSITION));
                };
                let terminal = metadata.await.map_err(|_| producer_closed())?;
                HttpHeaderEvent::new(message, terminal).map_err(|error| {
                    HttpError::with_message(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        error.to_string(),
                    )
                })
            },
            guard,
            timeout,
        ))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if maximum == 0 {
            return Box::pin(std::future::ready(Err(HttpError::new(
                HttpErrorCode::INVALID_STATE_TRANSITION,
            ))));
        }
        let guard = match self.begin_read() {
            Ok(guard) => guard,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let (response, receiver) = oneshot::channel();
        let maximum = maximum.min(self.options.maximum_demand_bytes.get());
        let admitted = self.demands.try_send(Demand { maximum, response });
        let shared = Arc::clone(&self.shared);
        let timeout = self.timeout;
        Box::pin(await_read(
            async move {
                let chunk = match admitted {
                    Ok(()) => match receiver.await {
                        Ok(result) => result?,
                        Err(_) => shared.closed_chunk()?,
                    },
                    Err(_) => shared.closed_chunk()?,
                };
                match chunk {
                    BodyChunk::Data(bytes, terminal) => {
                        shared.consume_hint(bytes.len(), terminal);
                        let body =
                            BufQueue::from_bytes(bytes, maximum as usize).map_err(|error| {
                                HttpError::with_message(
                                    HttpErrorCode::BODY_PARSE_ERROR,
                                    error.to_string(),
                                )
                            })?;
                        Ok(HttpBodyEvent::body(body, terminal))
                    }
                    BodyChunk::Trailers(trailers) => {
                        shared.consume_hint(0, true);
                        Ok(HttpBodyEvent::trailers(trailers))
                    }
                }
            },
            guard,
            timeout,
        ))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.shared.request_stop();
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.timeout = (!timeout.is_zero()).then_some(timeout);
    }
}

impl Drop for ApplicationBodySource {
    fn drop(&mut self) {
        self.shared.request_stop();
    }
}

struct ApplicationRead {
    shared: Arc<ApplicationShared>,
    reading: Rc<Cell<bool>>,
    completed: bool,
}

impl Drop for ApplicationRead {
    fn drop(&mut self) {
        self.reading.set(false);
        if !self.completed {
            self.shared.request_stop();
        }
    }
}

async fn await_read<T>(
    future: impl Future<Output = Result<T, HttpError>>,
    mut guard: ApplicationRead,
    timeout: Option<Duration>,
) -> Result<T, HttpError> {
    let timed = async {
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, future)
                .await
                .unwrap_or_else(|_| Err(HttpError::new(HttpErrorCode::READ_TIMEOUT))),
            None => future.await,
        }
    };
    let result = tokio::select! {
        biased;
        () = cancelled(guard.shared.cancellation.subscribe()) => {
            Err(HttpError::new(HttpErrorCode::CORO_CANCELLED))
        }
        result = timed => result,
    };
    if result.is_err() {
        guard.shared.request_stop();
    }
    guard.completed = true;
    result
}

fn producer_closed() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::BODY_PARSE_ERROR,
        b"application body producer closed; inspect its joined completion outcome",
    )
}

async fn cancelled(mut receiver: watch::Receiver<bool>) {
    if !*receiver.borrow_and_update() {
        let _ = receiver.changed().await;
    }
}

async fn join_application(shared: &ApplicationShared) -> ApplicationOutcome {
    let mut group = loop {
        let notified = shared.changed.notified();
        let ready = {
            let mut tasks = shared.tasks();
            match &*tasks {
                ApplicationTasks::Running(_) => true,
                ApplicationTasks::Prepared(_) => shared.stopped.load(Ordering::Acquire),
                ApplicationTasks::Joining
                | ApplicationTasks::Closed
                | ApplicationTasks::ApplicationOwned => {
                    unreachable!("one scope owns joining")
                }
            }
            .then(|| std::mem::replace(&mut *tasks, ApplicationTasks::Joining))
        };
        if let Some(ApplicationTasks::Prepared(group) | ApplicationTasks::Running(group)) = ready {
            break group;
        }
        notified.await;
    };
    let mut outcome = Err(ApplicationBodyError::Cancelled);
    while let Some(result) = group.join_next().await {
        outcome = result.unwrap_or_else(|error| Err(ApplicationBodyError::Task(error)));
    }
    *shared.tasks() = ApplicationTasks::Closed;
    outcome
}

async fn drive_application_join(
    mut lifetime: ApplicationJoinLifetime,
    ready: oneshot::Sender<Result<ApplicationBodyAttachment, ApplicationBodyAttachError>>,
    attachment: ApplicationBodyAttachment,
) -> ApplicationOutcome {
    let admission = if Handle::try_current().is_err() {
        Err(ApplicationBodyAttachError::NoApplicationRuntime)
    } else {
        let _admission = lifetime.shared.tasks();
        let mut owner = lifetime.owner.lock();
        if lifetime.shared.stopped.load(Ordering::Acquire)
            || !attachment.admission.permits(&lifetime.shared.worker)
        {
            Err(ApplicationBodyAttachError::Closed)
        } else {
            let ApplicationJoinPhase::Unarmed(group) =
                std::mem::replace(&mut owner.phase, ApplicationJoinPhase::Attaching)
            else {
                unreachable!("a driver arms its initial group exactly once")
            };
            owner.phase = ApplicationJoinPhase::Ready(group);
            Ok(())
        }
    };
    if let Err(error) = admission {
        application_cleanup(|| {
            let _ = ready.send(Err(error));
        });
        return Err(ApplicationBodyError::Cancelled);
    }
    application_cleanup(|| {
        let _ = ready.send(Ok(attachment));
    });
    let mut group = loop {
        let notified = lifetime.shared.changed.notified();
        let group = {
            let mut owner = lifetime.owner.lock();
            let take = match owner.phase {
                ApplicationJoinPhase::Running(_) => true,
                ApplicationJoinPhase::Ready(_) => lifetime.shared.stopped.load(Ordering::Acquire),
                ApplicationJoinPhase::Attaching => false,
                _ => unreachable!("an armed app driver owns the only join continuation"),
            };
            if take {
                match std::mem::replace(&mut owner.phase, ApplicationJoinPhase::Joining) {
                    ApplicationJoinPhase::Ready(group) | ApplicationJoinPhase::Running(group) => {
                        Some(group)
                    }
                    _ => unreachable!("only ready or running groups can be joined"),
                }
            } else {
                None
            }
        };
        if let Some(group) = group {
            break group;
        }
        notified.await;
    };
    #[cfg(test)]
    lifetime
        .publisher
        .0
        .as_ref()
        .expect("live join publisher")
        .joining
        .store(true, Ordering::Release);
    let mut outcome = None;
    while let Some(result) = group.join_next().await {
        #[cfg(test)]
        lifetime
            .publisher
            .0
            .as_ref()
            .expect("live join publisher")
            .reaped
            .fetch_add(1, Ordering::Release);
        let result = result.unwrap_or_else(|error| Err(ApplicationBodyError::Task(error)));
        if outcome.is_none() {
            outcome = Some(result);
        } else {
            application_cleanup(|| drop(result));
        }
    }
    drop(group);
    {
        let mut owner = lifetime.owner.lock();
        owner.alive = false;
        owner.phase = ApplicationJoinPhase::Closed;
    }
    lifetime.publisher.publish(true);
    outcome.unwrap_or(Err(ApplicationBodyError::Cancelled))
}

async fn pump_application<B>(
    body: B,
    mut input: ProducerInput,
    shared: Arc<ApplicationShared>,
    options: ApplicationBodyOptions,
) -> ApplicationOutcome
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let mut body = Box::pin(body);
    if shared.stopped.load(Ordering::Acquire) {
        return Err(ApplicationBodyError::Cancelled);
    }
    *shared
        .size_hint
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(body.size_hint());
    let mut terminal = body.is_end_stream();
    shared.initially_terminal.store(terminal, Ordering::Release);
    if input.metadata.send(terminal).is_err() {
        return Err(ApplicationBodyError::Cancelled);
    }
    if terminal {
        return Ok(());
    }
    let mut pending = Bytes::new();
    loop {
        let demand = tokio::select! {
            biased;
            () = cancelled(shared.cancellation.subscribe()) => return Err(ApplicationBodyError::Cancelled),
            demand = input.demands.recv() => match demand {
                Some(demand) => demand,
                None => return Err(ApplicationBodyError::Cancelled),
            },
        };
        if pending.is_empty() {
            let frame = tokio::select! {
                biased;
                () = cancelled(shared.cancellation.subscribe()) => return Err(ApplicationBodyError::Cancelled),
                frame = poll_fn(|context| body.as_mut().poll_frame(context)) => frame,
            };
            let frame = match frame {
                None => {
                    demand
                        .response
                        .send(Ok(BodyChunk::Data(Bytes::new(), true)))
                        .map_err(|_| ApplicationBodyError::Cancelled)?;
                    return Ok(());
                }
                Some(Err(error)) => {
                    let error = ApplicationBodyError::Application(error.into());
                    let _ = demand.response.send(Err(error.as_http_error()));
                    return Err(error);
                }
                Some(Ok(frame)) => frame,
            };
            match frame.into_data() {
                Ok(bytes) => {
                    if bytes.len() > options.maximum_frame_bytes.get() {
                        let error = ApplicationBodyError::FrameTooLarge {
                            actual: bytes.len(),
                            maximum: options.maximum_frame_bytes.get(),
                        };
                        let _ = demand.response.send(Err(error.as_http_error()));
                        return Err(error);
                    }
                    pending = bytes;
                    terminal = body.is_end_stream();
                }
                Err(frame) => {
                    let trailers = frame
                        .into_trailers()
                        .map_err(|_| ApplicationBodyError::UnsupportedFrame)
                        .and_then(|trailers| {
                            admit_trailers(trailers, options.maximum_frame_bytes.get())
                        });
                    return match trailers {
                        Ok(trailers) => {
                            demand
                                .response
                                .send(Ok(BodyChunk::Trailers(trailers)))
                                .map_err(|_| ApplicationBodyError::Cancelled)?;
                            Ok(())
                        }
                        Err(error) => {
                            let _ = demand.response.send(Err(error.as_http_error()));
                            Err(error)
                        }
                    };
                }
            }
        }
        let bytes = pending.split_to(pending.len().min(demand.maximum as usize));
        let finished = terminal && pending.is_empty();
        demand
            .response
            .send(Ok(BodyChunk::Data(bytes, finished)))
            .map_err(|_| ApplicationBodyError::Cancelled)?;
        if finished {
            return Ok(());
        }
    }
}

fn admit_trailers(
    trailers: HeaderMap,
    maximum: usize,
) -> Result<HttpHeaders, ApplicationBodyError> {
    let actual = trailers.iter().fold(0usize, |total, (name, value)| {
        total
            .saturating_add(name.as_str().len())
            .saturating_add(value.len())
    });
    if actual > maximum {
        return Err(ApplicationBodyError::FrameTooLarge { actual, maximum });
    }
    let mut converted = HttpHeaders::new();
    for (name, value) in &trailers {
        let bytes = value.as_bytes();
        if bytes.first().is_some_and(u8::is_ascii_whitespace)
            || bytes.last().is_some_and(u8::is_ascii_whitespace)
        {
            return Err(ApplicationBodyError::InvalidTrailers);
        }
        converted.add(name.as_str().as_bytes(), bytes);
    }
    Ok(converted)
}
