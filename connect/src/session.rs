//! The `HTTPSessionContext` subset plus `sendRequest` that CONNECT streams use.

use std::future::Future;
use std::pin::Pin;

use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::message::SocketAddress;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerId;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexResponse;

/// An owner-local future returned by a CONNECT operation.
pub type LocalConnectFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// The proxy-session members that `HTTPConnectStream` uses
/// (`HTTPSessionContext`, HTTPCoroSession.h:55-87, plus `sendRequest`).
///
/// The trait stays dyn-compatible: `HttpConnectStream::connect_unique` stores
/// the session as `Box<dyn ConnectSession>`.
pub trait ConnectSession: 'static {
    /// The worker that owns the session (`getEventBase`).
    fn worker_id(&self) -> WorkerId;

    /// The session's local address (`getLocalAddress`).
    fn local_address(&self) -> SocketAddress;

    /// The session's immediate peer address (`getPeerAddress`).
    fn peer_address(&self) -> SocketAddress;

    /// The session's local transport endpoint, which can be a Unix-domain name.
    fn local_endpoint(&self) -> TransportEndpoint;

    /// The session's immediate peer transport endpoint, which can be a
    /// Unix-domain name.
    fn peer_endpoint(&self) -> TransportEndpoint;

    /// Commits `request` on `reservation`
    /// (`HTTPCoroSession::sendRequest(source, reservation)`).
    ///
    /// `cancellation` covers admission and the request-head read only
    /// (HTTPCoroSession.cpp:2242-2247). Implementations must not attach it to
    /// the request body, which carries the tunnel's egress.
    fn send_connect_request(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<UniplexResponse, HttpError>>;

    /// Starts a graceful drain of the session (`initiateDrain`).
    fn initiate_drain(&self) -> Result<bool, HttpError>;
}
