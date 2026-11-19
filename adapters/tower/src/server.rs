/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Serves a Tower service on established I/O, with the run owned by the caller.
//!
//! Reserve caller capacity before preparing a connection, drive `ServerRun` on
//! the entered application runtime, and then start it. Reap the task that
//! drives that run before releasing caller capacity. Only `ServerRun` joins the
//! connection's tasks; the handles and their wait methods do not.
//!
//! Each request's extensions hold the connection's `Arc<ConnectionContext>`
//! and a [`ServerExchange`] that cancels or observes that exchange.
//!
//! The response body for a HEAD request, or for a 1xx, 204 or 304 response, is
//! dropped on the application runtime without being polled, and the head is
//! sent without `Transfer-Encoding`. HEAD and 304 responses keep an explicit
//! `Content-Length`. A service returns only its final response, so returning a
//! 100 status is an error.

mod application;
mod connection;

#[cfg(test)]
pub(crate) use connection::install_head_hook;
#[cfg(test)]
pub(crate) use connection::install_setup_hook;

#[cfg(test)]
pub(crate) fn cancel_run(run: &ServerRun) -> Arc<dyn Fn() + Send + Sync> {
    let signals = Arc::clone(&run.signals);
    Arc::new(move || {
        signals.change(Mode::Cancel);
    })
}

#[cfg(test)]
pub(crate) fn connection_diagnostics(connection: &ServerConnection) -> impl fmt::Debug {
    let signals = &connection.lease.0;
    let mode = match signals.mode() {
        Mode::Open => "open",
        Mode::Drain => "drain",
        Mode::Cancel => "cancel",
    };
    let native = signals.native.borrow().clone();
    let faults = signals.faults.result();
    (mode, native, faults)
}

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use http_body::Body;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::http::CodecProtocol;
use roxygen_hyper_adapter::body::BoxError;
use roxygen_hyper_adapter::body::LocalBodyOptions;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyCleanupError;
use roxygen_hyper_adapter::source::ApplicationBodyError;
use roxygen_hyper_adapter::source::ApplicationBodyOptions;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_server::acceptor::ServerSessionConfig;
use roxygen_transport::established::AdoptedIoCompletionError;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tower_service::Service;

/// A fixed, empty HTTP/1 rejection which closes rather than switches protocols.
///
/// Only 4xx/5xx statuses are accepted. This does not add CONNECT, WebSocket,
/// or HTTP/2 extended CONNECT support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Http1ConnectionSwitchRejection {
    status: StatusCode,
}

impl Http1ConnectionSwitchRejection {
    /// Validates a rejection status before any connection is prepared.
    pub fn new(status: StatusCode) -> Result<Self, InvalidRejectionStatus> {
        if status.is_client_error() || status.is_server_error() {
            Ok(Self { status })
        } else {
            Err(InvalidRejectionStatus { status })
        }
    }

    /// Selects an empty 403 response followed by connection closure.
    pub const fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
        }
    }

    /// Returns the validated response status.
    pub const fn status(self) -> StatusCode {
        self.status
    }
}

/// A status outside the 400..=599 connection-switch rejection range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("connection-switch rejection requires 400..=599, not {status}")]
pub struct InvalidRejectionStatus {
    status: StatusCode,
}

impl InvalidRejectionStatus {
    /// Returns the rejected status.
    pub const fn status(self) -> StatusCode {
        self.status
    }
}

/// Options for one served connection, including its protocol, transport
/// security, and limits.
#[derive(Clone)]
pub struct ServerOptions {
    pub protocol: CodecProtocol,
    pub security: TransportSecurity,
    pub upstream_origin: Option<UpstreamOrigin>,
    /// The connection's session settings. The adapter waits for each exchange's native
    /// cleanup, which the owned drivers do not report yet, so `prepare_connection` rejects
    /// `owned_http1` and `owned_http2` with `ServerFailure::InvalidOptions`.
    pub session: ServerSessionConfig,
    pub max_exchanges: NonZeroUsize,
    pub dispatch_capacity: NonZeroUsize,
    pub request_body: LocalBodyOptions,
    pub response_body: ApplicationBodyOptions,
    /// Optional empty rejection for HTTP/1 CONNECT and Upgrade requests.
    ///
    /// `None` (the default) leaves these requests to the usual request
    /// conversion and service. A rejection drains the connection without
    /// calling the service. HTTP/2 requests, and HTTP/1 requests that fail
    /// conversion for another reason, are not affected.
    pub http1_connection_switch_rejection: Option<Http1ConnectionSwitchRejection>,
}

impl ServerOptions {
    pub fn new(protocol: CodecProtocol, security: TransportSecurity) -> Self {
        Self {
            protocol,
            security,
            upstream_origin: None,
            session: ServerSessionConfig::default(),
            max_exchanges: NonZeroUsize::new(100).expect("fixed nonzero capacity"),
            dispatch_capacity: NonZeroUsize::new(100).expect("fixed nonzero capacity"),
            request_body: LocalBodyOptions::default(),
            response_body: ApplicationBodyOptions::default(),
            http1_connection_switch_rejection: None,
        }
    }
}

/// The first failure of a server operation, whether or not cleanup finished.
#[derive(Debug, thiserror::Error)]
pub enum ServerFailure {
    #[error("prepare/run requires the same live application Tokio runtime")]
    ApplicationRuntime,
    #[error("invalid server bounds, unsupported protocol, or an owned session driver")]
    InvalidOptions,
    #[error("connection or exchange cancelled")]
    Cancelled,
    #[error("bounded server admission is closed or full")]
    Admission,
    #[error("native attachment or exchange failed")]
    Native(#[source] HttpError),
    #[error("HTTP network projection failed")]
    Projection(#[source] roxygen_http_adapter::HttpAdapterError),
    #[error("application service failed")]
    Service(#[source] BoxError),
    #[error("application response body failed")]
    Body(#[source] ApplicationBodyError),
    #[error("application task failed")]
    Task(#[source] tokio::task::JoinError),
    #[error("native exchange cleanup failed")]
    ExchangeCleanup(#[source] ExchangeCompletionError),
    #[error("registered native scope retirement failed")]
    Scope(#[source] WorkerCleanupObservationError),
    #[error("request source retirement failed")]
    RequestCleanup(#[source] WorkerCleanupObservationError),
    #[error("response producer/source retirement failed")]
    ResponseCleanup(#[source] ApplicationBodyCleanupError),
    #[error("adopted I/O retirement failed")]
    Io(#[source] AdoptedIoCompletionError),
    #[error("application callback or destructor panicked")]
    Panic(Arc<PanicPayload>),
    #[error("cleanup could not be confirmed: {0}")]
    Cleanup(&'static str),
}

/// A caught panic payload. Its `Debug` output does not format the payload.
pub struct PanicPayload(Mutex<Option<Box<dyn Any + Send>>>, u64);

static NEXT_FAILURE: AtomicU64 = AtomicU64::new(1);

impl PanicPayload {
    #[cfg(test)]
    pub(crate) fn cloned<T: Any + Clone>(&self) -> Option<T> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()?
            .downcast_ref::<T>()
            .cloned()
    }
}

impl fmt::Debug for PanicPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PanicPayload")
    }
}

impl Drop for PanicPayload {
    fn drop(&mut self) {
        let payload = self
            .0
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        discard(payload);
    }
}

/// A failed server operation and whether its cleanup is confirmed.
///
/// An operation error does not by itself mean that resource cleanup failed.
#[derive(Clone, Debug)]
pub struct ServerError {
    cause: Arc<RetainedFailure>,
    cleanup_confirmed: bool,
    sequence: u64,
    panic: Option<Arc<PanicPayload>>,
}

#[derive(Debug)]
struct RetainedFailure(Option<ServerFailure>);

impl Drop for RetainedFailure {
    fn drop(&mut self) {
        discard(self.0.take());
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "server operation failed (cleanup confirmed: {})",
            self.cleanup_confirmed
        )
    }
}

impl std::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause())
    }
}

impl ServerError {
    fn is_consequence(&self) -> bool {
        match self.cause() {
            ServerFailure::Cancelled
            | ServerFailure::ExchangeCleanup(_)
            | ServerFailure::Body(ApplicationBodyError::Cancelled) => true,
            ServerFailure::Task(error) | ServerFailure::Body(ApplicationBodyError::Task(error)) => {
                error.is_cancelled()
            }
            ServerFailure::Native(error) => {
                error.code() == roxygen_core::error::HttpErrorCode::CORO_CANCELLED
            }
            _ => false,
        }
    }

    /// The failure that caused this error.
    pub fn cause(&self) -> &ServerFailure {
        self.cause.0.as_ref().expect("retained original cause")
    }

    /// Whether the operation's cleanup is confirmed.
    ///
    /// When false, keep the caller's capacity reserved or close its admission.
    /// A startup error does not show that cleanup finished. Even when
    /// `ServerRun` succeeds, the caller must reap the task that drove it before
    /// releasing caller capacity.
    pub fn cleanup_confirmed(&self) -> bool {
        self.cleanup_confirmed
    }

    /// The first caught panic, also retained when an earlier operation failed.
    pub fn first_panic(&self) -> Option<&PanicPayload> {
        self.panic.as_deref()
    }

    fn new(cause: ServerFailure, cleanup_confirmed: bool) -> Self {
        let panic = match &cause {
            ServerFailure::Panic(payload) => Some(Arc::clone(payload)),
            _ => None,
        };
        let sequence = panic.as_ref().map_or_else(
            || NEXT_FAILURE.fetch_add(1, Ordering::Relaxed),
            |payload| payload.1,
        );
        Self {
            cause: Arc::new(RetainedFailure(Some(cause))),
            cleanup_confirmed,
            sequence,
            panic,
        }
    }
}

type Outcome = Result<(), Arc<ServerError>>;

#[derive(Default)]
struct Outcomes {
    first: Option<ServerError>,
    uncertain: bool,
    panic: Option<Arc<PanicPayload>>,
}

impl Outcomes {
    fn record(&mut self, result: Outcome) {
        if let Err(error) = result {
            discard(self.merge(&error));
            discard(error);
        }
    }

    fn merge(
        &mut self,
        error: &Arc<ServerError>,
    ) -> (Option<ServerError>, Option<Arc<PanicPayload>>) {
        self.uncertain |= !error.cleanup_confirmed();
        let retired = if self.first.as_ref().is_none_or(|first| {
            (first.is_consequence() && !error.is_consequence())
                || (first.is_consequence() == error.is_consequence()
                    && error.sequence < first.sequence)
        }) {
            self.first.replace((**error).clone())
        } else {
            None
        };
        let panic = error
            .panic
            .as_ref()
            .filter(|payload| self.panic.as_ref().is_none_or(|first| payload.1 < first.1));
        let retired_panic = panic.and_then(|payload| self.panic.replace(Arc::clone(payload)));
        (retired, retired_panic)
    }

    fn fail(&mut self, cause: ServerFailure, confirmed: bool) {
        self.record(Err(Arc::new(ServerError::new(cause, confirmed))));
    }

    fn result(&self) -> Outcome {
        self.first.as_ref().map_or(Ok(()), |error| {
            let mut error = error.clone();
            error.cleanup_confirmed &= !self.uncertain;
            error.panic = self.panic.clone();
            Err(Arc::new(error))
        })
    }
}

fn discard<T>(value: T) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
        roxygen_runtime::panic::discard_panic(payload);
    }
}

fn destroy<T>(value: T) -> Outcome {
    catch_unwind(AssertUnwindSafe(|| drop(value))).map_err(panic_error)
}

fn panic_error(payload: Box<dyn Any + Send>) -> Arc<ServerError> {
    let payload = Arc::new(PanicPayload(
        Mutex::new(Some(payload)),
        NEXT_FAILURE.fetch_add(1, Ordering::Relaxed),
    ));
    Arc::new(ServerError::new(ServerFailure::Panic(payload), false))
}

#[derive(Default)]
struct Faults {
    outcomes: Mutex<Outcomes>,
    upstream: Option<Arc<Faults>>,
}

impl Faults {
    fn record(&self, result: Outcome) {
        if let Err(error) = result {
            let retired = self
                .outcomes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .merge(&error);
            discard(retired);
            if let Some(upstream) = &self.upstream {
                upstream.record(Err(Arc::clone(&error)));
            }
            discard(error);
        }
    }
    fn result(&self) -> Outcome {
        self.outcomes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .result()
    }
    fn fail(&self, cause: ServerFailure, confirmed: bool) {
        self.record(Err(Arc::new(ServerError::new(cause, confirmed))));
    }
    fn capture<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(operation)) {
            Ok(value) => Some(value),
            Err(payload) => {
                self.record(Err(panic_error(payload)));
                None
            }
        }
    }
    fn retire<T>(&self, value: T) {
        self.record(destroy(value));
    }
    fn uncertain(&self) -> bool {
        self.outcomes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .uncertain
    }
}

struct ObservationWaker {
    target: Option<Waker>,
    faults: Arc<Faults>,
}
impl Wake for ObservationWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.faults.capture(|| {
            self.target
                .as_ref()
                .expect("live observation waker")
                .wake_by_ref()
        });
    }
}
impl Drop for ObservationWaker {
    fn drop(&mut self) {
        self.faults.retire(self.target.take());
    }
}

fn publish(sender: &watch::Sender<Option<Outcome>>, result: Outcome, faults: &Faults) {
    faults.capture(|| {
        discard(sender.send_replace(Some(result)));
    });
}

fn publish_lost(sender: &watch::Sender<Option<Outcome>>, faults: &Faults, reason: &'static str) {
    let missing = sender.borrow().is_none();
    if missing {
        faults.fail(ServerFailure::Cleanup(reason), false);
        publish(sender, faults.result(), faults);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Mode {
    Open,
    Drain,
    Cancel,
}

struct Signals {
    faults: Arc<Faults>,
    mode: watch::Sender<Mode>,
    wake: Notify,
    app_leaves: watch::Sender<Option<Outcome>>,
    native: watch::Sender<Option<Outcome>>,
}

impl Signals {
    fn change(&self, requested: Mode) -> bool {
        let mut changed = false;
        self.faults.capture(|| {
            self.mode.send_if_modified(|mode| {
                if *mode >= requested {
                    false
                } else {
                    *mode = requested;
                    changed = true;
                    true
                }
            })
        });
        if changed {
            self.notify();
        }
        changed
    }

    fn notify(&self) {
        self.faults.capture(|| self.wake.notify_one());
    }
    fn publish_native(&self, result: Outcome) {
        self.faults.record(result.clone());
        publish(&self.native, result, &self.faults);
        self.notify();
    }
    fn publish_leaves(&self, result: Outcome) {
        publish(&self.app_leaves, result, &self.faults);
    }

    fn mode(&self) -> Mode {
        *self.mode.borrow()
    }

    async fn cancelled(&self) {
        let mut receiver = self.mode.subscribe();
        loop {
            if *receiver.borrow_and_update() == Mode::Cancel {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

fn observe(
    mut receiver: watch::Receiver<Option<Outcome>>,
    faults: Arc<Faults>,
) -> impl Future<Output = Outcome> + Send + 'static {
    let mut future = Box::pin(async move {
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result;
            }
            if receiver.changed().await.is_err() {
                return Err(Arc::new(ServerError::new(
                    ServerFailure::Cleanup("observation publisher lost"),
                    false,
                )));
            }
        }
    });
    futures::future::poll_fn(move |context| {
        let Some(target) = faults.capture(|| context.waker().clone()) else {
            return Poll::Ready(faults.result());
        };
        let waker = Waker::from(Arc::new(ObservationWaker {
            target: Some(target),
            faults: Arc::clone(&faults),
        }));
        future.as_mut().poll(&mut Context::from_waker(&waker))
    })
}

struct ExchangeState {
    faults: Arc<Faults>,
    cancel: watch::Sender<bool>,
    app_leaf: watch::Sender<Option<Outcome>>,
    completed: watch::Sender<Option<Outcome>>,
    connection: Arc<Signals>,
}

impl ExchangeState {
    fn cancel(&self) {
        self.faults.capture(|| self.cancel.send_replace(true));
    }
    fn fail(&self, cause: ServerFailure, confirmed: bool) {
        self.faults.fail(cause, confirmed);
    }
    fn publish_leaf(&self, result: Outcome) {
        publish(&self.app_leaf, result, &self.faults);
        self.connection.notify();
    }
    fn lost(&self) {
        publish_lost(
            &self.app_leaf,
            &self.faults,
            "application exchange parent lost before joining",
        );
        publish_lost(
            &self.completed,
            &self.faults,
            "exchange execution publisher lost before all joins",
        );
        self.cancel();
        self.connection.notify();
    }

    async fn cancelled(&self) {
        let mut receiver = self.cancel.subscribe();
        let local = async {
            loop {
                if *receiver.borrow_and_update() {
                    return;
                }
                if receiver.changed().await.is_err() {
                    return;
                }
            }
        };
        tokio::select! { () = local => {}, () = self.connection.cancelled() => {} }
    }

    fn is_cancelled(&self) -> bool {
        *self.cancel.borrow() || self.connection.mode() == Mode::Cancel
    }
}

/// Cancels one exchange and waits for its cleanup, without owning its tasks.
#[derive(Clone)]
pub struct ServerExchange {
    state: Arc<ExchangeState>,
}

impl fmt::Debug for ServerExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerExchange")
    }
}

impl ServerExchange {
    /// Requests cancellation of the exchange.
    pub fn cancel(&self) {
        self.state.cancel();
    }

    /// Waits until the native exchange, its source scopes, the service and body
    /// tasks, and the native task that watches the exchange are released. The
    /// native handler must return before this completes.
    pub fn wait_completed(&self) -> impl Future<Output = Outcome> + Send + 'static {
        observe(
            self.state.completed.subscribe(),
            Arc::clone(&self.state.faults),
        )
    }
}

struct PublicLease(Arc<Signals>);
impl Drop for PublicLease {
    fn drop(&mut self) {
        self.0.change(Mode::Cancel);
    }
}

/// `Send` controls for a connection. The caller's `ServerRun` runs and joins
/// its work.
#[derive(Clone)]
pub struct ServerConnection {
    context: Arc<ConnectionContext>,
    lease: Arc<PublicLease>,
}

impl ServerConnection {
    /// The connection's native context.
    pub fn connection_context(&self) -> &Arc<ConnectionContext> {
        &self.context
    }
    /// Starts a graceful drain. Returns `false` if the connection was already
    /// draining or cancelled.
    pub fn begin_drain(&self) -> bool {
        self.lease.0.change(Mode::Drain)
    }
    /// Requests cancellation of the connection and its exchanges.
    pub fn cancel(&self) {
        self.lease.0.change(Mode::Cancel);
    }

    /// Waits until the application's service calls and response bodies finish.
    /// Does not wait for native or I/O cleanup, or for `ServerRun`.
    pub fn wait_app_leaves_drained(&self) -> impl Future<Output = Outcome> + Send + 'static {
        observe(
            self.lease.0.app_leaves.subscribe(),
            Arc::clone(&self.lease.0.faults),
        )
    }
}

/// Starts a prepared connection. Dropping it before a successful `start`
/// signals the run to cancel, and the run must still be polled until it
/// returns.
pub struct ServerStartup {
    requested: Option<oneshot::Sender<()>>,
    started: oneshot::Receiver<Result<Arc<ConnectionContext>, ServerError>>,
    signals: Arc<Signals>,
    accepted: bool,
}

impl ServerStartup {
    /// Starts serving. The caller must already be driving the matching
    /// `ServerRun` in a task it owns.
    pub async fn start(mut self) -> Result<ServerConnection, ServerError> {
        if let Some(requested) = self.requested.take() {
            self.signals
                .faults
                .capture(|| requested.send(()))
                .unwrap_or(Err(()))
                .map_err(|_| ServerError::new(ServerFailure::Cancelled, false))?;
        }
        let faults = Arc::clone(&self.signals.faults);
        let context = futures::future::poll_fn(|context| {
            let Some(target) = faults.capture(|| context.waker().clone()) else {
                return Poll::Ready(Err((*faults.result().unwrap_err()).clone()));
            };
            let waker = Waker::from(Arc::new(ObservationWaker {
                target: Some(target),
                faults: Arc::clone(&faults),
            }));
            Pin::new(&mut self.started)
                .poll(&mut Context::from_waker(&waker))
                .map(|result| {
                    result.unwrap_or_else(|_| {
                        Err(ServerError::new(
                            ServerFailure::Cleanup("application run lost"),
                            false,
                        ))
                    })
                })
        })
        .await?;
        self.accepted = true;
        Ok(ServerConnection {
            context,
            lease: Arc::new(PublicLease(Arc::clone(&self.signals))),
        })
    }
}

impl Drop for ServerStartup {
    fn drop(&mut self) {
        if !self.accepted {
            self.signals.change(Mode::Cancel);
        }
    }
}

/// The connection's execution, which the caller owns and polls to completion.
///
/// Poll it on the runtime that called [`prepare_connection`]. Its output does
/// not mean that the caller has reaped the task that polled it. Dropping the
/// run or its runtime early cannot wait for asynchronous cleanup.
#[must_use]
pub struct ServerRun {
    future: Pin<Box<dyn Future<Output = Outcome> + Send>>,
    signals: Arc<Signals>,
    admission: Arc<Admission>,
    completed: bool,
}

impl Future for ServerRun {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let faults = Arc::clone(&self.signals.faults);
        let Some(target) = faults.capture(|| context.waker().clone()) else {
            self.signals.change(Mode::Cancel);
            faults.capture(|| context.waker().wake_by_ref());
            return Poll::Pending;
        };
        let waker = Waker::from(Arc::new(ObservationWaker {
            target: Some(target),
            faults,
        }));
        let result = self.future.as_mut().poll(&mut Context::from_waker(&waker));
        drop(waker);
        match result {
            Poll::Ready(result) => {
                let mut outcomes = Outcomes::default();
                outcomes.record(result);
                outcomes.record(self.signals.faults.result());
                self.completed = true;
                Poll::Ready(outcomes.result())
            }
            Poll::Pending => {
                if self.signals.faults.uncertain() {
                    self.signals.change(Mode::Cancel);
                }
                Poll::Pending
            }
        }
    }
}

impl Drop for ServerRun {
    fn drop(&mut self) {
        self.signals.change(Mode::Cancel);
        if !self.completed {
            publish_lost(
                &self.signals.app_leaves,
                &self.signals.faults,
                "application run lost before joining",
            );
            for state in self.admission.states() {
                state.lost();
            }
        }
    }
}

#[derive(Clone, Copy)]
struct SlotId {
    index: usize,
    generation: u64,
}

#[derive(Default)]
struct Slot {
    generation: u64,
    state: Option<Arc<ExchangeState>>,
    packet: Option<application::Packet>,
    finished: bool,
}

struct Admission {
    slots: Mutex<Vec<Slot>>,
    dispatch: mpsc::Sender<SlotId>,
    signals: Arc<Signals>,
}

impl Admission {
    fn reserve(&self) -> Option<(SlotId, Arc<ExchangeState>)> {
        if self.signals.mode() != Mode::Open || self.signals.faults.uncertain() {
            return None;
        }
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        if self.signals.mode() != Mode::Open || self.signals.faults.uncertain() {
            return None;
        }
        let (index, slot) = slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.state.is_none())?;
        slot.generation = slot.generation.checked_add(1)?;
        slot.finished = false;
        let state = Arc::new(ExchangeState {
            faults: Arc::new(Faults {
                outcomes: Mutex::new(Outcomes::default()),
                upstream: Some(Arc::clone(&self.signals.faults)),
            }),
            cancel: watch::channel(false).0,
            app_leaf: watch::channel(None).0,
            completed: watch::channel(None).0,
            connection: Arc::clone(&self.signals),
        });
        slot.state = Some(Arc::clone(&state));
        Some((
            SlotId {
                index,
                generation: slot.generation,
            },
            state,
        ))
    }

    fn dispatch(&self, id: SlotId, packet: application::Packet) {
        let state = Arc::clone(&packet.state);
        {
            let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
            slots[id.index].packet = Some(packet);
        }
        if self
            .signals
            .faults
            .capture(|| self.dispatch.try_send(id))
            .is_none_or(|result| result.is_err())
        {
            state.cancel();
        }
        self.signals.notify();
    }

    fn take_packets(&self) -> Vec<(SlotId, application::Packet)> {
        let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
        slots
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| {
                slot.packet.take().map(|packet| {
                    (
                        SlotId {
                            index,
                            generation: slot.generation,
                        },
                        packet,
                    )
                })
            })
            .collect()
    }

    fn finish(&self, id: SlotId, result: Outcome) {
        let (state, removed) = {
            let mut slots = self.slots.lock().unwrap_or_else(|error| error.into_inner());
            let slot = &mut slots[id.index];
            assert_eq!(slot.generation, id.generation);
            let state = Arc::clone(slot.state.as_ref().expect("charged slot"));
            slot.finished = true;
            let removed = if result.is_ok()
                || result
                    .as_ref()
                    .is_err_and(|error| error.cleanup_confirmed())
            {
                slot.state.take()
            } else {
                None
            };
            (state, removed)
        };
        publish(&state.completed, result.clone(), &state.faults);
        if result
            .as_ref()
            .is_err_and(|error| !error.cleanup_confirmed())
        {
            self.signals.change(Mode::Cancel);
        }
        discard(removed);
    }

    fn states(&self) -> Vec<Arc<ExchangeState>> {
        self.slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(|slot| slot.state.clone())
            .collect()
    }

    fn app_leaves_done(&self) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .all(|slot| {
                slot.state
                    .as_ref()
                    .is_none_or(|state| state.app_leaf.borrow().is_some())
            })
    }

    fn leaf_results(&self) -> Vec<Outcome> {
        self.slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(|slot| slot.state.as_ref()?.app_leaf.borrow().clone())
            .collect()
    }

    fn settled(&self) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .all(|slot| slot.state.is_none() || slot.finished)
    }
}

/// Prepares a connection without spawning tasks, submitting native work, or
/// adopting the I/O.
///
/// The service, body, and I/O types need not be `Sync` or `Unpin`. The caller
/// must keep the I/O's reactor running until the run and the native runtime
/// have been joined.
pub fn prepare_connection<Io, S, B>(
    worker: WorkerHandle,
    established: EstablishedIo<Io>,
    service: S,
    options: ServerOptions,
) -> Result<(ServerStartup, ServerRun), ServerError>
where
    Io: AsyncRead + AsyncWrite + Send + 'static,
    S: Service<Request<RoxygenBody>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let runtime = Handle::try_current()
        .map_err(|_| ServerError::new(ServerFailure::ApplicationRuntime, true))?
        .id();
    if options.dispatch_capacity > options.max_exchanges
        || options.request_body.command_capacity == 0
        || options.session.owned_http1
        || options.session.owned_http2
        || !matches!(
            options.protocol,
            CodecProtocol::Http11 | CodecProtocol::Http2
        )
    {
        return Err(ServerError::new(ServerFailure::InvalidOptions, true));
    }
    let signals = Arc::new(Signals {
        faults: Arc::new(Faults::default()),
        mode: watch::channel(Mode::Open).0,
        wake: Notify::new(),
        app_leaves: watch::channel(None).0,
        native: watch::channel(None).0,
    });
    let (dispatch, receiver) = mpsc::channel(options.dispatch_capacity.get());
    let admission = Arc::new(Admission {
        slots: Mutex::new(
            (0..options.max_exchanges.get())
                .map(|_| Slot::default())
                .collect(),
        ),
        dispatch,
        signals: Arc::clone(&signals),
    });
    let (requested, request) = oneshot::channel();
    let (started, startup) = oneshot::channel();
    let run_signals = Arc::clone(&signals);
    let run_admission = Arc::clone(&admission);
    let future = async move {
        if Handle::try_current().ok().map(|handle| handle.id()) != Some(runtime) {
            let error = ServerError::new(ServerFailure::ApplicationRuntime, true);
            admission
                .signals
                .faults
                .capture(|| started.send(Err(error.clone())));
            return Err(Arc::new(error));
        }
        application::run(
            worker,
            established,
            service,
            options,
            application::Channels {
                admission,
                dispatch: receiver,
                request,
                started,
            },
        )
        .await
    };
    Ok((
        ServerStartup {
            requested: Some(requested),
            started: startup,
            signals,
            accepted: false,
        },
        ServerRun {
            future: Box::pin(future),
            signals: run_signals,
            admission: run_admission,
            completed: false,
        },
    ))
}
