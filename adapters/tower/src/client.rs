/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! An application-runtime client backed by native Roxygen session pools.
//!
//! Start driving [`ClientRun`] in a task the caller owns before calling
//! [`ClientStartup::start`]. Keep that task and its runtime running through
//! shutdown, and reap the run before dropping the runtime. Only the run joins
//! the client's work; [`ClientOwner`] and exchange handles do not.

pub mod connector;
pub mod owner;

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http::Response;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use roxygen_client::pool::AttachedConnectFailure;
use roxygen_client::pool::AttachedPoolParams;
use roxygen_client::pool::PoolError;
use roxygen_core::error::HttpError;
use roxygen_hyper_adapter::body::LocalBodyOptions;
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyOptions;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_transport::byte_stream::ByteStreamLimits;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tower_service::Service;

use self::connector::ConnectRequest;
use self::connector::ConnectedIo;
use self::connector::ConnectorPolicy;
pub use self::owner::ClientAppDrainError;
pub use self::owner::ClientCompletionError;
pub use self::owner::ClientExchange;
pub use self::owner::ClientOwner;
pub use self::owner::ClientRun;
pub use self::owner::ClientShutdownError;
use self::owner::Job;
use self::owner::Signal;
use crate::route::RouteError;
use crate::route::RouteSpec;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub(super) type AppFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Native session settings that are `Send`. Worker-local callbacks cannot be
/// set here.
#[derive(Clone, Debug, Default)]
pub struct SessionSettings {
    pub max_concurrent_outgoing_streams: Option<u32>,
    pub connection_flow_control: Option<u32>,
    pub stream_read_timeout: Option<Duration>,
    pub connection_read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

/// Fixed limits and connector policy for one immutable client generation.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub maximum_exchanges: usize,
    pub command_capacity: usize,
    pub maximum_connector_tasks: usize,
    pub maximum_physical_connections: usize,
    pub maximum_routes: usize,
    pub connector_policy: ConnectorPolicy,
    pub pool: AttachedPoolParams,
    pub session: SessionSettings,
    pub upload: ApplicationBodyOptions,
    /// Queue and read limits for the response body. Its size hint comes from
    /// the native response head, not from this option's `size_hint`.
    pub response: LocalBodyOptions,
    pub transport: ByteStreamLimits,
    #[cfg(test)]
    pub(crate) test_handoff: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_app_return: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_native_return: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_acquisitions: Option<Arc<std::sync::atomic::AtomicUsize>>,
    #[cfg(test)]
    pub(crate) test_before_claim: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_after_adoption: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_application: Option<Arc<owner::TestApplicationJoin>>,
    #[cfg(test)]
    pub(crate) test_application_following: Vec<Arc<owner::TestApplicationJoin>>,
    #[cfg(test)]
    pub(crate) test_close_claim_registration: bool,
    #[cfg(test)]
    pub(crate) test_claim_registration_rejected: Option<Arc<AtomicBool>>,
    #[cfg(test)]
    pub(crate) test_io: Option<Arc<owner::TestIoJoin>>,
    #[cfg(test)]
    pub(crate) test_shutdown_return: Option<Arc<owner::TestLatch>>,
    #[cfg(test)]
    pub(crate) test_app_leaf_snapshot: Option<Arc<owner::TestAppLeafSnapshot>>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            maximum_exchanges: 64,
            command_capacity: 64,
            maximum_connector_tasks: 8,
            maximum_physical_connections: 8,
            maximum_routes: 16,
            connector_policy: ConnectorPolicy::Direct,
            pool: AttachedPoolParams::default(),
            session: SessionSettings::default(),
            upload: ApplicationBodyOptions::default(),
            response: LocalBodyOptions::default(),
            transport: ByteStreamLimits::new(64 * 1024, 64 * 1024, 64)
                .expect("fixed nonzero transport limits"),
            #[cfg(test)]
            test_handoff: None,
            #[cfg(test)]
            test_app_return: None,
            #[cfg(test)]
            test_native_return: None,
            #[cfg(test)]
            test_acquisitions: None,
            #[cfg(test)]
            test_before_claim: None,
            #[cfg(test)]
            test_after_adoption: None,
            #[cfg(test)]
            test_application: None,
            #[cfg(test)]
            test_application_following: Vec::new(),
            #[cfg(test)]
            test_close_claim_registration: false,
            #[cfg(test)]
            test_claim_registration_rejected: None,
            #[cfg(test)]
            test_io: None,
            #[cfg(test)]
            test_shutdown_return: None,
            #[cfg(test)]
            test_app_leaf_snapshot: None,
        }
    }
}

impl ClientOptions {
    /// Disables native deadlines so that only the caller's timeouts apply.
    ///
    /// Disables the attached-pool acquisition and connect-attempt timeout and
    /// the native session connection-read, stream-read, and write timeouts.
    /// This does not install timers of its own, so these phases are bounded
    /// only by the caller's connector or stream policy, which may leave them
    /// unbounded. Other options are unchanged.
    #[must_use]
    pub fn with_external_timeouts(mut self) -> Self {
        self.pool.connect_timeout = Duration::ZERO;
        self.session.connection_read_timeout = Some(Duration::ZERO);
        self.session.stream_read_timeout = Some(Duration::ZERO);
        self.session.write_timeout = Some(Duration::ZERO);
        self
    }
}

/// Why a client operation failed.
///
/// Native errors and some original connector causes are kept as sources.
/// `Display` never calls into application error types.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ClientError {
    #[error("client entry requires its captured application runtime")]
    WrongRuntime,
    #[error("client admission is closed")]
    Closed,
    #[error("client capacity is exhausted")]
    Capacity,
    #[error("Tower call requires an unused readiness reservation")]
    NotReady,
    #[error("route belongs to another client generation")]
    WrongGeneration,
    #[error("route is not covered by the immutable connector policy")]
    ConnectorPolicy,
    #[error("connector result disagrees with the admitted route")]
    ConnectorMismatch,
    #[error("invalid request route")]
    Route(#[source] Arc<RouteError>),
    #[error("native pool failed")]
    Pool(#[source] PoolError),
    #[error("native HTTP operation failed")]
    Native(#[source] HttpError),
    #[error("application operation failed")]
    Application(#[source] AttachedConnectFailure),
    #[error("HTTP head projection failed")]
    Projection,
    #[error("application callback or destructor panicked")]
    Panicked,
    #[error("client operation was cancelled")]
    Cancelled,
    #[error("cleanup ownership could not be confirmed")]
    CleanupUnconfirmed,
}

/// Why [`Client::prepare`] or [`ClientStartup::start`] failed.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ClientStartError {
    #[error("prepare requires an entered application runtime")]
    NoApplicationRuntime,
    #[error("all client capacities must be nonzero and within Tokio limits")]
    InvalidOptions,
    #[error("client startup failed")]
    Client(#[source] ClientError),
}

pub(super) struct Shared {
    pub worker: WorkerHandle,
    pub app: Handle,
    pub options: ClientOptions,
    pub commands: mpsc::Sender<Job>,
    pub admission: Arc<Semaphore>,
    pub physical: Arc<Semaphore>,
    pub stop: Arc<Signal>,
    pub generation: Arc<()>,
    pub routes: Mutex<HashMap<RouteSpec, Route>>,
    pub exchanges: Mutex<Vec<Arc<Signal>>>,
    pub admission_gate: Mutex<bool>,
    pub publishing: std::sync::atomic::AtomicUsize,
    pub published: tokio::sync::Notify,
    pub destruction_failed: Arc<AtomicBool>,
    closing: AtomicBool,
    #[cfg(test)]
    pub application_index: std::sync::atomic::AtomicUsize,
}

impl Shared {
    pub fn entered(&self) -> bool {
        Handle::try_current().is_ok_and(|handle| handle.id() == self.app.id())
    }

    pub fn close(&self) {
        if self.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        *self
            .admission_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        owner::notify(self, || self.admission.close());
        owner::notify(self, || self.physical.close());
        owner::notify(self, || self.stop.cancel());
        let exchanges = self
            .exchanges
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for exchange in exchanges {
            owner::notify(self, || exchange.cancel());
        }
    }

    pub fn notification_failed(&self) {
        if !self.destruction_failed.swap(true, Ordering::AcqRel) {
            self.stop.cancel();
        }
    }

    fn check(&self, route: &Route) -> Result<(), ClientError> {
        if !self.entered() {
            return Err(ClientError::WrongRuntime);
        }
        if self.closing.load(Ordering::Acquire)
            || self.stop.is_cancelled()
            || !self.worker.is_accepting()
        {
            return Err(ClientError::Closed);
        }
        if !Arc::ptr_eq(&self.generation, &route.generation) {
            return Err(ClientError::WrongGeneration);
        }
        Ok(())
    }
}

struct SubmissionLife(Arc<Shared>);

impl Drop for SubmissionLife {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// A cloneable handle for submitting requests. It does not own the
/// application tasks that requests start.
#[derive(Clone)]
pub struct Client {
    pub(super) shared: Arc<Shared>,
    _submissions: Arc<SubmissionLife>,
}

/// A route bound to the client generation that registered it.
#[derive(Clone)]
pub struct Route {
    spec: RouteSpec,
    generation: Arc<()>,
}

impl Route {
    pub fn spec(&self) -> &RouteSpec {
        &self.spec
    }
}

impl fmt::Debug for Route {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Route")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

/// Starts a prepared client. Dropping it before a successful `start` closes the
/// client, but the run must still be driven to completion.
pub struct ClientStartup {
    shared: Arc<Shared>,
    activate: Option<oneshot::Sender<()>>,
    ready: oneshot::Receiver<Result<(), ClientStartError>>,
    leaves: tokio::sync::watch::Receiver<Option<Result<(), ClientAppDrainError>>>,
    accepted: bool,
}

impl Client {
    /// Captures the entered application runtime without starting worker or
    /// connector work.
    ///
    /// The caller must start driving the run in a task it owns before calling
    /// [`ClientStartup::start`], and keep driving it on this runtime through
    /// shutdown. The run holds the connector, so dropping an undriven run on
    /// another runtime runs the connector's destructor there. That is misuse.
    pub fn prepare<C, Io>(
        worker: WorkerHandle,
        connector: C,
        options: ClientOptions,
    ) -> Result<(ClientStartup, ClientRun), ClientStartError>
    where
        C: Service<ConnectRequest, Response = ConnectedIo<Io>> + Send + 'static,
        C::Future: Send + 'static,
        C::Error: Into<BoxError>,
        Io: AsyncRead + AsyncWrite + Send + 'static,
    {
        let app = Handle::try_current().map_err(|_| ClientStartError::NoApplicationRuntime)?;
        if [
            options.maximum_exchanges,
            options.command_capacity,
            options.maximum_connector_tasks,
            options.maximum_physical_connections,
            options.maximum_routes,
            options.response.command_capacity,
        ]
        .into_iter()
        .any(|capacity| capacity == 0 || capacity > Semaphore::MAX_PERMITS)
        {
            return Err(ClientStartError::InvalidOptions);
        }
        let (commands, receiver) = mpsc::channel(options.command_capacity);
        let shared = Arc::new(Shared {
            worker,
            app,
            commands,
            admission: Arc::new(Semaphore::new(options.maximum_exchanges)),
            physical: Arc::new(Semaphore::new(options.maximum_physical_connections)),
            options,
            stop: Arc::new(Signal::new()),
            generation: Arc::new(()),
            routes: Mutex::new(HashMap::new()),
            exchanges: Mutex::new(Vec::new()),
            admission_gate: Mutex::new(false),
            publishing: std::sync::atomic::AtomicUsize::new(0),
            published: tokio::sync::Notify::new(),
            destruction_failed: Arc::new(AtomicBool::new(false)),
            closing: AtomicBool::new(false),
            #[cfg(test)]
            application_index: std::sync::atomic::AtomicUsize::new(0),
        });
        let (activate, activation) = oneshot::channel();
        let (ready, readiness) = oneshot::channel();
        let (leaves, leaf_receiver) = tokio::sync::watch::channel(None);
        let run = owner::make_run(
            Arc::clone(&shared),
            connector,
            receiver,
            activation,
            ready,
            leaves,
        );
        Ok((
            ClientStartup {
                shared,
                activate: Some(activate),
                ready: readiness,
                leaves: leaf_receiver,
                accepted: false,
            },
            run,
        ))
    }

    /// Registers a route for `spec`, or returns the existing one. Routes are
    /// kept until client shutdown.
    pub fn route(&self, spec: RouteSpec) -> Result<Route, ClientError> {
        if !self.shared.entered() {
            return Err(ClientError::WrongRuntime);
        }
        if self.shared.stop.is_cancelled() {
            return Err(ClientError::Closed);
        }
        if !self.shared.options.connector_policy.permits(&spec) {
            return Err(ClientError::ConnectorPolicy);
        }
        let mut routes = self
            .shared
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(route) = routes.get(&spec) {
            return Ok(route.clone());
        }
        if routes.len() == self.shared.options.maximum_routes {
            return Err(ClientError::Capacity);
        }
        let route = Route {
            spec: spec.clone(),
            generation: Arc::clone(&self.shared.generation),
        };
        routes.insert(spec, route.clone());
        Ok(route)
    }

    /// A Tower service that sends requests on `route`.
    pub fn service<B>(&self, route: &Route) -> Result<ClientService<B>, ClientError> {
        self.shared.check(route)?;
        Ok(ClientService {
            client: self.clone(),
            route: route.clone(),
            readiness: Readiness::Idle,
            body: PhantomData,
        })
    }

    /// Submits a request without waiting, and returns it unchanged if it is
    /// rejected.
    ///
    /// On success the body has already moved into the application's bounded
    /// queue, so polling or dropping the response future on another runtime
    /// does not move the body's polls or destructor there. On any rejection,
    /// including `WrongRuntime`, the caller still owns the body.
    pub fn try_request<B>(
        &self,
        route: &Route,
        request: Request<B>,
    ) -> Result<ResponseFuture, Box<RejectedRequest<B>>>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        if let Err(error) = self.validate(route, &request) {
            return Err(Box::new(RejectedRequest { request, error }));
        }
        let permit = match Arc::clone(&self.shared.admission).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Err(Box::new(RejectedRequest {
                    request,
                    error: ClientError::Capacity,
                }));
            }
        };
        let slot = match self.shared.commands.clone().try_reserve_owned() {
            Ok(slot) => slot,
            Err(_) => {
                return Err(Box::new(RejectedRequest {
                    request,
                    error: ClientError::Capacity,
                }));
            }
        };
        self.admit(route, request, Reservation { permit, slot })
    }

    fn validate<B>(&self, route: &Route, request: &Request<B>) -> Result<(), ClientError> {
        self.shared.check(route)?;
        route
            .spec
            .validate_request(request)
            .map_err(|error| ClientError::Route(Arc::new(error)))
    }

    fn admit<B>(
        &self,
        route: &Route,
        request: Request<B>,
        reservation: Reservation,
    ) -> Result<ResponseFuture, Box<RejectedRequest<B>>>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        let closed = self
            .shared
            .admission_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *closed {
            return Err(Box::new(RejectedRequest {
                request,
                error: ClientError::Closed,
            }));
        }
        self.shared.publishing.fetch_add(1, Ordering::AcqRel);
        drop(closed);
        let publication = Publication(Arc::clone(&self.shared));
        let (exchange, publisher) = owner::new_exchange(Arc::downgrade(&self.shared));
        self.shared
            .exchanges
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Arc::clone(&exchange.cancel));
        let (response, receiver) = oneshot::channel();
        let job = owner::request_job(
            request,
            route.spec.clone(),
            exchange.clone(),
            publisher,
            response,
            reservation.permit,
        );
        reservation.slot.send(job);
        drop(publication);
        Ok(ResponseFuture {
            receiver,
            exchange,
            finished: false,
        })
    }
}

impl ClientStartup {
    /// Starts the client. Call this only once the run is being driven.
    pub async fn start(mut self) -> Result<(Client, ClientOwner), ClientStartError> {
        if !self.shared.entered() {
            return Err(ClientStartError::Client(ClientError::WrongRuntime));
        }
        self.activate
            .take()
            .ok_or(ClientStartError::Client(ClientError::Closed))?
            .send(())
            .map_err(|_| ClientStartError::Client(ClientError::Closed))?;
        owner::protect(&mut self.ready, Arc::downgrade(&self.shared))
            .await
            .map_err(|_| ClientStartError::Client(ClientError::CleanupUnconfirmed))??;
        if self.shared.stop.is_cancelled() {
            return Err(ClientStartError::Client(ClientError::Closed));
        }
        self.accepted = true;
        Ok((
            Client {
                shared: Arc::clone(&self.shared),
                _submissions: Arc::new(SubmissionLife(Arc::clone(&self.shared))),
            },
            ClientOwner::new(Arc::clone(&self.shared), self.leaves.clone()),
        ))
    }
}

impl Drop for ClientStartup {
    fn drop(&mut self) {
        if !self.accepted {
            self.shared.close();
        }
    }
}

/// A request that [`Client::try_request`] rejected, returned with the reason.
pub struct RejectedRequest<B> {
    pub request: Request<B>,
    pub error: ClientError,
}

impl<B> fmt::Debug for RejectedRequest<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RejectedRequest")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

struct Reservation {
    permit: OwnedSemaphorePermit,
    slot: mpsc::OwnedPermit<Job>,
}

struct Publication(Arc<Shared>);
impl Drop for Publication {
    fn drop(&mut self) {
        self.0.publishing.fetch_sub(1, Ordering::AcqRel);
        owner::notify(&self.0, || self.0.published.notify_one());
    }
}

enum Readiness {
    Idle,
    Waiting(AppFuture<Result<Reservation, ClientError>>),
    Reserved(Reservation),
}

/// A Tower service whose readiness reserves an exchange and an application
/// queue slot.
///
/// `poll_ready` and `call` must run on the captured application runtime. A
/// misused `call` (without readiness, or on another runtime) drops the request
/// body on the calling thread. Use [`Client::try_request`] when a rejected
/// request must be returned.
pub struct ClientService<B> {
    client: Client,
    route: Route,
    readiness: Readiness,
    body: PhantomData<fn(B)>,
}

impl<B> Clone for ClientService<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            route: self.route.clone(),
            readiness: Readiness::Idle,
            body: PhantomData,
        }
    }
}

impl<B> Service<Request<B>> for ClientService<B>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = Response<ClientBody>;
    type Error = ClientError;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let waker = owner::safe_waker(context, Arc::downgrade(&self.client.shared));
        let context = &mut Context::from_waker(&waker);
        if let Err(error) = self.client.shared.check(&self.route) {
            self.readiness = Readiness::Idle;
            return Poll::Ready(Err(error));
        }
        loop {
            match &mut self.readiness {
                Readiness::Idle => {
                    let shared = Arc::clone(&self.client.shared);
                    self.readiness = Readiness::Waiting(Box::pin(async move {
                        let permit = Arc::clone(&shared.admission)
                            .acquire_owned()
                            .await
                            .map_err(|_| ClientError::Closed)?;
                        let slot = shared
                            .commands
                            .clone()
                            .reserve_owned()
                            .await
                            .map_err(|_| ClientError::Closed)?;
                        Ok(Reservation { permit, slot })
                    }));
                }
                Readiness::Waiting(future) => match future.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(reservation)) => {
                        self.readiness = Readiness::Reserved(reservation)
                    }
                    Poll::Ready(Err(error)) => {
                        self.readiness = Readiness::Idle;
                        return Poll::Ready(Err(error));
                    }
                },
                Readiness::Reserved(_) => return Poll::Ready(Ok(())),
            }
        }
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let readiness = std::mem::replace(&mut self.readiness, Readiness::Idle);
        let validation = self.client.validate(&self.route, &request);
        match (validation, readiness) {
            (Ok(()), Readiness::Reserved(reservation)) => {
                match self.client.admit(&self.route, request, reservation) {
                    Ok(response) => response,
                    Err(rejected) => {
                        let dropped = owner::dispose(rejected.request);
                        ResponseFuture::failed(if dropped {
                            rejected.error
                        } else {
                            ClientError::Panicked
                        })
                    }
                }
            }
            (result, _) => {
                let error = result.err().unwrap_or(ClientError::NotReady);
                let dropped = owner::dispose(request);
                ResponseFuture::failed(if dropped {
                    error
                } else {
                    ClientError::Panicked
                })
            }
        }
    }
}

/// The `Send` future of an exchange's response. It never owns the request body.
pub struct ResponseFuture {
    receiver: oneshot::Receiver<Result<Response<ClientBody>, ClientError>>,
    exchange: ClientExchange,
    finished: bool,
}

impl ResponseFuture {
    pub fn exchange(&self) -> ClientExchange {
        self.exchange.clone()
    }

    fn failed(error: ClientError) -> Self {
        let (exchange, publisher) = owner::new_exchange(std::sync::Weak::new());
        let _ = publisher.send(Some(Err(ClientCompletionError::failure(
            error.clone(),
            false,
        ))));
        let (sender, receiver) = oneshot::channel();
        let _ = sender.send(Err(error));
        Self {
            receiver,
            exchange,
            finished: false,
        }
    }
}

impl Future for ResponseFuture {
    type Output = Result<Response<ClientBody>, ClientError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = owner::safe_waker(context, self.exchange.shared.clone());
        let context = &mut Context::from_waker(&waker);
        match Pin::new(&mut self.receiver).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.finished = true;
                let result = result.unwrap_or(Err(ClientError::CleanupUnconfirmed));
                if result.is_err() {
                    self.exchange.cancel();
                }
                Poll::Ready(result)
            }
        }
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        if !self.finished {
            self.exchange.cancel();
        }
    }
}

/// A response body read from the native source, with a handle that cancels or
/// observes the whole exchange.
pub struct ClientBody {
    pub(super) inner: RoxygenBody,
    pub(super) exchange: ClientExchange,
}

impl ClientBody {
    pub fn exchange(&self) -> ClientExchange {
        self.exchange.clone()
    }
}

impl Body for ClientBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let waker = owner::safe_waker(context, self.exchange.shared.clone());
        let context = &mut Context::from_waker(&waker);
        let result = Pin::new(&mut self.inner).poll_frame(context);
        if matches!(&result, Poll::Ready(Some(Err(_)))) {
            self.exchange.cancel();
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for ClientBody {
    fn drop(&mut self) {
        if !self.inner.is_end_stream() {
            self.exchange.cancel();
        }
    }
}
