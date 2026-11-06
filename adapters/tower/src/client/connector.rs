//! Existing application connector integration, not an HTTP client or scheduler.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use http::Extensions;
use http::Uri;
use hyper_util::client::legacy::connect::Connection;
use hyper_util::rt::TokioIo;
use roxygen_client::connector::ClientSession;
use roxygen_client::pool::AttachedConnectFailure;
use roxygen_client::pool::AttachedSessionFactoryWithCause;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::wait::TimedBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tower_service::Service;

use super::AppFuture;
use super::BoxError;
use super::ClientError;
use super::Shared;
use super::owner::ConnectionRecord;
use super::owner::Guarded;
use super::owner::Handoff;
use super::owner::Payload;
use super::owner::Signal;
use super::owner::callback;
use super::owner::dispose;
use crate::extensions::RetiringExtensions;
use crate::route::HopSecurity;
use crate::route::ProxyEndpoint;
use crate::route::RouteSpec;

/// Caller-audited immutable aggregate connector configuration.
///
/// Forward means exactly this plaintext, header-free, always-intercepting proxy,
/// with no forced CONNECT or fallback. An opaque connector cannot be inspected
/// to prove this assertion; the owner must audit its construction. Replacing its
/// configuration requires a new client. Direct permits no proxy interception.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorPolicy {
    Direct,
    Forward(ProxyEndpoint),
}

impl ConnectorPolicy {
    pub fn permits(&self, route: &RouteSpec) -> bool {
        match (self, route.proxy()) {
            (Self::Direct, None) => true,
            (Self::Forward(expected), Some(actual)) => expected == actual,
            _ => false,
        }
    }
}

/// One native-pool attempt's immutable request, supplied on the app runtime.
/// Connector errors remain original typed causes. A connector explicitly returning
/// `AttachedConnectFailure` retains that already-classified native/cause pair.
#[derive(Clone)]
pub struct ConnectRequest {
    pub(super) route: RouteSpec,
    pub(super) timeout: Duration,
    pub(super) cancellation: CancellationToken,
    failures: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    pub(crate) claimed: Arc<std::sync::atomic::AtomicBool>,
}

impl ConnectRequest {
    pub fn route(&self) -> &RouteSpec {
        &self.route
    }
    pub fn uri(&self) -> Uri {
        self.route.connector_uri()
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// Actual established connection metadata supplied by a trusted connector.
/// TLS indicates encryption, not an inferred certificate/authentication result.
pub struct ConnectorMetadata {
    pub protocol: CodecProtocol,
    pub security: TransportSecurity,
    pub local_endpoint: TransportEndpoint,
    pub peer_endpoint: TransportEndpoint,
    pub peer_identity: Option<PeerIdentity>,
}

/// One completed attempt, with its original I/O wrappers and response extras.
/// The route must describe the actual audited dial decision, not request Host.
pub struct ConnectedIo<Io> {
    pub(super) route: RouteSpec,
    pub(super) established: EstablishedIo<Io>,
    pub(super) protocol: CodecProtocol,
    pub(super) security: TransportSecurity,
    pub(super) extensions: RetiringExtensions,
}

impl<Io> ConnectedIo<Io> {
    pub fn new(
        route: RouteSpec,
        established: EstablishedIo<Io>,
        protocol: CodecProtocol,
        security: TransportSecurity,
    ) -> Self {
        Self {
            route,
            established,
            protocol,
            security,
            extensions: RetiringExtensions(Extensions::new()),
        }
    }

    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = RetiringExtensions(extensions);
        self
    }

    /// Recovers the actual attempt values without unwrapping the I/O stack.
    pub fn into_parts(
        self,
    ) -> (
        RouteSpec,
        EstablishedIo<Io>,
        CodecProtocol,
        TransportSecurity,
        Extensions,
    ) {
        (
            self.route,
            self.established,
            self.protocol,
            self.security,
            self.extensions.into_inner(),
        )
    }

    pub(super) fn retire(self) -> bool {
        let (_, established, _, _, extensions) = self.into_parts();
        let io_destroyed = dispose(established);
        let extras_destroyed = dispose(RetiringExtensions(extensions));
        io_destroyed && extras_destroyed
    }

    pub(super) fn agrees(&self, request: &ConnectRequest) -> bool {
        self.route == request.route
            && self.security == security(&request.route)
            && match self.protocol {
                CodecProtocol::Http11 => request.route.allows_http1(),
                CodecProtocol::Http2 => request.route.allows_http2(),
                _ => false,
            }
    }
}

pub(super) fn security(route: &RouteSpec) -> TransportSecurity {
    match route.security() {
        HopSecurity::Plaintext => TransportSecurity::Plaintext,
        HopSecurity::Tls => TransportSecurity::Tls,
    }
}

/// Adapts the complete Hyper-compatible connector result using `TokioIo`.
///
/// No Hyper HTTP driver, resolver replacement, TLS unwrap, CONNECT or replay is
/// introduced. The policy must match the aggregate's audited construction. The
/// mapper supplies facts unavailable through generic Hyper `Connected`; actual
/// ALPN/proxy flags are checked independently and response extras are retained.
pub struct HyperConnector<C, M> {
    connector: C,
    policy: ConnectorPolicy,
    metadata: M,
}

impl<C, M> HyperConnector<C, M> {
    pub fn new(connector: C, policy: ConnectorPolicy, metadata: M) -> Self {
        Self {
            connector,
            policy,
            metadata,
        }
    }
}

impl<C, M> Service<ConnectRequest> for HyperConnector<C, M>
where
    C: Service<Uri> + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
    C::Response: hyper::rt::Read + hyper::rt::Write + Connection + Send + 'static,
    M: Fn(&ConnectRequest, &C::Response) -> Result<ConnectorMetadata, BoxError>
        + Clone
        + Send
        + 'static,
{
    type Response = ConnectedIo<TokioIo<C::Response>>;
    type Error = BoxError;
    type Future = AppFuture<Result<Self::Response, BoxError>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.connector.poll_ready(context).map_err(Into::into)
    }

    fn call(&mut self, request: ConnectRequest) -> Self::Future {
        if !self.policy.permits(request.route()) {
            return Box::pin(async { Err(Box::new(ClientError::ConnectorPolicy) as BoxError) });
        }
        let metadata = match callback(|| self.metadata.clone()) {
            Ok(metadata) => metadata,
            Err(error) => return Box::pin(async move { Err(Box::new(error) as BoxError) }),
        };
        let mut metadata = Payload::tracked(metadata, &request.failures);
        let future = match callback(|| self.connector.call(request.uri())) {
            Ok(future) => Guarded::new(future),
            Err(error) => {
                return Box::pin(async move {
                    dispose(metadata.take());
                    Err(Box::new(error) as BoxError)
                });
            }
        };
        Box::pin(async move {
            let mut io = match future.await {
                Ok(Ok(io)) => Payload::tracked(io, &request.failures),
                result => {
                    let error = match result {
                        Ok(Err(error)) => error.into(),
                        Err(error) => Box::new(error) as BoxError,
                        _ => unreachable!(),
                    };
                    return if dispose(metadata.take()) {
                        Err(error)
                    } else {
                        dispose(error);
                        Err(Box::new(ClientError::Panicked) as BoxError)
                    };
                }
            };
            let mut extensions =
                Payload::tracked(RetiringExtensions(Extensions::new()), &request.failures);
            let failed = Arc::clone(&request.failures);
            let result = callback(|| -> Result<ConnectorMetadata, BoxError> {
                let mut connected = Payload::tracked(io.get().connected(), &failed);
                let facts = metadata.get()(&request, io.get())?;
                if connected.get().is_proxied() != request.route.proxy().is_some()
                    || (connected.get().is_negotiated_h2()
                        && facts.protocol != CodecProtocol::Http2)
                    || (facts.security == TransportSecurity::Tls
                        && (facts.protocol == CodecProtocol::Http2)
                            != connected.get().is_negotiated_h2())
                {
                    return Err(Box::new(ClientError::ConnectorMismatch));
                }
                connected.get().get_extras(&mut extensions.get_mut().0);
                if !dispose(connected.take()) {
                    return Err(Box::new(ClientError::Panicked));
                }
                Ok(facts)
            })
            .map_err(|error| Box::new(error) as BoxError)
            .and_then(|result| result);
            let context_destroyed =
                dispose(metadata.take()) && !failed.load(std::sync::atomic::Ordering::Acquire);
            match result {
                Ok(facts) if context_destroyed => Ok(ConnectedIo::new(
                    request.route,
                    EstablishedIo::new(
                        TokioIo::new(io.take()),
                        facts.local_endpoint,
                        facts.peer_endpoint,
                        facts.peer_identity,
                    ),
                    facts.protocol,
                    facts.security,
                )
                .with_extensions(extensions.take().into_inner())),
                result => {
                    let error = result
                        .err()
                        .unwrap_or_else(|| Box::new(ClientError::Panicked));
                    let io_destroyed = dispose(io.take());
                    let extras_destroyed = dispose(extensions.take());
                    if context_destroyed && io_destroyed && extras_destroyed {
                        Err(error)
                    } else {
                        dispose(error);
                        Err(Box::new(ClientError::Panicked) as BoxError)
                    }
                }
            }
        })
    }
}

pub(super) struct ConnectorCommand<Io> {
    pub request: ConnectRequest,
    pub stop: Arc<Signal>,
    pub reply: oneshot::Sender<Result<Arc<Handoff<Io>>, AttachedConnectFailure>>,
}

pub(super) struct Factory<Io> {
    pub shared: Arc<Shared>,
    pub route: RouteSpec,
    pub commands: mpsc::Sender<ConnectorCommand<Io>>,
    pub records: Rc<RefCell<Vec<Arc<ConnectionRecord>>>>,
}

pub(super) fn failure(error: ClientError) -> AttachedConnectFailure {
    AttachedConnectFailure::with_cause(
        HttpError::with_message(
            HttpErrorCode::CONNECT_ERROR,
            b"application connector failed",
        ),
        Arc::new(error),
    )
}

pub(super) fn connector_failure(error: BoxError) -> AttachedConnectFailure {
    match error.downcast::<AttachedConnectFailure>() {
        Ok(failure) => *failure,
        Err(error) => AttachedConnectFailure::with_cause(
            HttpError::with_message(
                HttpErrorCode::CONNECT_ERROR,
                b"application connector failed",
            ),
            Arc::from(error),
        ),
    }
}

impl<Io> AttachedSessionFactoryWithCause for Factory<Io>
where
    Io: AsyncRead + AsyncWrite + Send + 'static,
{
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ClientSession, AttachedConnectFailure>> + 'static>>
    {
        let shared = Arc::clone(&self.shared);
        let commands = self.commands.clone();
        let route = self.route.clone();
        let records = Rc::clone(&self.records);
        Box::pin(async move {
            let stop = Arc::new(Signal::new());
            let (reply, receiver) = oneshot::channel();
            let command = ConnectorCommand {
                request: ConnectRequest {
                    route,
                    timeout,
                    cancellation: stop.token(),
                    failures: Arc::clone(&shared.destruction_failed),
                    #[cfg(test)]
                    claimed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
                stop: Arc::clone(&stop),
                reply,
            };
            let transfer = async {
                commands
                    .send(command)
                    .await
                    .map_err(|_| failure(ClientError::Closed))?;
                receiver
                    .await
                    .map_err(|_| failure(ClientError::CleanupUnconfirmed))?
            };
            tokio::pin!(transfer);
            let timeout_baton = TimedBaton::new(Some(timeout));
            let timed_wait = timeout_baton.wait(cancellation);
            tokio::pin!(timed_wait);
            let mut interrupted = None;
            let handoff = tokio::select! {
                result = &mut transfer => result,
                status = &mut timed_wait => {
                    interrupted = Some(if status == WaitStatus::TimedOut {
                        AttachedConnectFailure::with_cause(
                            HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"connector timed out"),
                            Arc::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "connector timed out")),
                        )
                    } else { failure(ClientError::Cancelled) });
                    stop.cancel();
                    transfer.await
                }
                _ = shared.stop.wait() => {
                    interrupted = Some(failure(ClientError::Cancelled));
                    stop.cancel();
                    transfer.await
                }
            };
            let handoff = match handoff {
                Ok(handoff) => handoff,
                Err(error) => return Err(interrupted.unwrap_or(error)),
            };
            #[cfg(test)]
            if let Some(latch) = &shared.options.test_before_claim {
                if interrupted.is_none() {
                    tokio::select! {
                        _ = latch.arrive(&shared.stop) => {}
                        status = &mut timed_wait => {
                            interrupted = Some(if status == WaitStatus::TimedOut {
                                AttachedConnectFailure::with_cause(HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"connector timed out"), Arc::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "connector timed out")))
                            } else { failure(ClientError::Cancelled) });
                            stop.cancel();
                        }
                    }
                }
            }
            if stop.is_cancelled() || shared.stop.is_cancelled() {
                stop.cancel();
                handoff.abandon();
                let _ = handoff.wait_retired().await;
                return Err(interrupted.unwrap_or_else(|| failure(ClientError::Cancelled)));
            }
            let attachment = super::owner::attach_handoff(
                &context,
                Arc::clone(&handoff),
                Arc::clone(&shared),
                Arc::clone(&stop),
            );
            tokio::pin!(attachment);
            let result = tokio::select! {
                result = &mut attachment => result,
                status = &mut timed_wait => {
                    stop.cancel();
                    interrupted = Some(if status == WaitStatus::TimedOut {
                        AttachedConnectFailure::with_cause(
                            HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"connector timed out"),
                            Arc::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "connector timed out")),
                        )
                    } else { failure(ClientError::Cancelled) });
                    attachment.await
                }
                _ = shared.stop.wait() => { stop.cancel(); attachment.await }
            };
            match result {
                Ok(session) if !stop.is_cancelled() => {
                    let previous = std::mem::take(&mut *records.borrow_mut());
                    let (mut retained, retired): (Vec<_>, Vec<_>) = previous
                        .into_iter()
                        .partition(|record| !record.retired.is_cancelled());
                    retained.push(Arc::clone(&handoff.record));
                    *records.borrow_mut() = retained;
                    dispose(retired);
                    Ok(session)
                }
                result => {
                    if let Ok(session) = &result {
                        session.cancel();
                        let _ = session.wait_closed().await;
                    }
                    let _ = handoff.wait_retired().await;
                    Err(interrupted.unwrap_or_else(|| match result {
                        Err(error) => AttachedConnectFailure::new(error),
                        Ok(_) => failure(ClientError::Cancelled),
                    }))
                }
            }
        })
    }
}
