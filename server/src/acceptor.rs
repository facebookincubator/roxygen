//! Accepted-connection admission and downstream session construction.

use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use roxygen_codec::CodecSetting;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::http2::DEFAULT_STREAM_WINDOW;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::http2::StreamReceiveWindowMode;
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h2::OwnedH2Handle;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexHandle;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexSession;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::ByteStreamTransport;
#[cfg(test)]
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::TlsTransport;

use crate::connection::bind_connection;
use crate::server::CallbackFailure;
use crate::server::ServerCallbackStage;
use crate::server::discard_panicking_value;
use crate::server::record_callback_failure;

mod handler_application;
mod owned_http1;
mod owned_http2;

use owned_http1::OwnedHttp1Session;
use owned_http2::OwnedHttp2Session;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionInfo {
    pub local_address: SocketAddress,
    pub peer_address: SocketAddress,
    pub protocol: CodecProtocol,
    pub secure: bool,
    pub peer_identity: Option<Box<[u8]>>,
    pub zero_copy_enabled: bool,
    /// The actual immediate connection, distinct from any logical origin.
    pub context: Arc<ConnectionContext>,
}

pub trait NewConnectionFilter: Send + Sync + 'static {
    fn accept(&self, connection: &ConnectionInfo) -> bool;
}

impl<F> NewConnectionFilter for F
where
    F: Fn(&ConnectionInfo) -> bool + Send + Sync + 'static,
{
    fn accept(&self, connection: &ConnectionInfo) -> bool {
        self(connection)
    }
}

pub trait TlsHandshakeObserver: Send + Sync + 'static {
    /// Reports a successful native TLS transport accept before connection metadata
    /// or admission. It does not imply session construction or subsequent I/O, and
    /// excludes handshakes whose post-cryptographic transport setup failed.
    fn on_handshake_success(&self) {}

    fn on_success(&self, _connection: &ConnectionInfo) {}

    /// Reports a failed native TLS transport accept. `READ_TIMEOUT` means the handshake missed
    /// its deadline (`ServerSessionConfig::tls_handshake_timeout`).
    fn on_error(&self, _error: &HttpError) {}
}

pub trait AcceptedConnectionObserver: Send + Sync + 'static {
    fn on_accepted(&self, connection: &ConnectionInfo);

    /// Reports standalone HttpServer session admission and control registration,
    /// before transport run. Embedded serve_transport does not emit this event.
    fn on_registered(&self) {}
}

#[cfg(test)]
type AcceptedSocketProfile = dyn Fn(&std::net::TcpStream) -> Result<(), HttpError> + Send + Sync;

#[derive(Clone)]
pub struct ServerSessionConfig {
    pub settings: Vec<CodecSetting>,
    pub max_concurrent_outgoing_streams: u32,
    pub stream_flow_control: u32,
    pub connection_flow_control: u32,
    pub stream_read_timeout: Duration,
    pub connection_idle_timeout: Duration,
    pub write_timeout: Duration,
    /// Bounds a native TLS accept from the start of the handshake. The default is 60 seconds,
    /// matching wangle's `sslHandshakeTimeout`; zero leaves the handshake untimed. A missed deadline
    /// fails the accept with `READ_TIMEOUT`, which `TlsHandshakeObserver::on_error` receives.
    /// Both owned modes accept it. `serve_transport`, whose transport is already established,
    /// ignores it.
    pub tls_handshake_timeout: Duration,
    /// How long an HTTP/2 drain holds its final GOAWAY after GOAWAY(2^31-1), so that streams
    /// the peer opened before the first GOAWAY reached it are still served. Zero sends both
    /// GOAWAYs at once. The default is `UniplexSession`'s one second. Both HTTP/2 drivers
    /// honor it (`DIV-028`); HTTP/1.1 connections ignore it.
    pub goaway_timeout: Duration,
    pub read_allocation_size: usize,
    pub byte_stream_limits: ByteStreamLimits,
    pub lifecycle_observer_factory:
        Option<Arc<dyn Fn() -> Rc<dyn LifecycleObserver> + Send + Sync>>,
    /// Optional owner-local recorder for the actual accepted H1 or H2 session.
    /// None preserves the ordinary session construction path.
    pub stats_factory:
        Option<Arc<dyn Fn(&ConnectionInfo) -> Rc<dyn Http2SessionStats> + Send + Sync>>,
    /// HTTP/2 flood limits for both downstream drivers, using `Http2RateLimitConfig::default()`
    /// unless overridden. Each category can be disabled with an unbounded threshold.
    pub http2_rate_limits: Http2RateLimitConfig,
    /// Experimental, default off (`DIV-029`): serves HTTP/1.1 connections on the owned
    /// driver (`roxygen_session::owned_h1`) instead of `UniplexSession`, with no task per
    /// request. HTTP/2 connections are unchanged. The owned mode is untimed: the caller keeps
    /// timeout responsibility, and `stream_read_timeout`, `connection_idle_timeout` and
    /// `write_timeout` must be zero, which also leaves HTTP/2 sessions untimed apart from
    /// `goaway_timeout`. TLS handshakes use their independent `tls_handshake_timeout`. It
    /// records no statistics and observes no lifecycle, so `stats_factory` and
    /// `lifecycle_observer_factory` must be `None`. Server construction refuses anything else
    /// with the reason, as `HttpServerError::InvalidConfig` (or, for `serve_transport`,
    /// `INVALID_STATE_TRANSITION`).
    pub owned_http1: bool,
    /// Experimental, default off: serves HTTP/2 connections on the owned driver
    /// (`roxygen_session::owned_h2`) instead of `UniplexSession`, with no task per
    /// request. HTTP/1.1 connections are unchanged. The owned mode honors
    /// `connection_idle_timeout`, which drains an idle connection with GOAWAY
    /// (`DIV-028`), `write_timeout`, which bounds a stalled transport
    /// write and a response's wait for flow-control credit (`DIV-028`), and
    /// `stream_read_timeout`, which bounds each wait for request body and answers 408
    /// while the response head is unsent (`DIV-028`). Its drains hold the final GOAWAY for
    /// `goaway_timeout`, as the managed session's do (`DIV-028`). It is stat-less:
    /// `stats_factory` must be `None`. Server construction refuses anything else with the
    /// reason, as `HttpServerError::InvalidConfig` (or, for `serve_transport`,
    /// `INVALID_STATE_TRANSITION`). It applies `http2_rate_limits`, as the managed driver does.
    /// A `lifecycle_observer_factory` observer
    /// sees the connection's lifecycle as under `UniplexSession`, with the differences
    /// recorded in `DIV-051`: among them, events arrive at the end of each driver poll, and
    /// a stream detaches when it retires. The driver requires the configured
    /// stream receive window to equal the advertised `INITIAL_WINDOW_SIZE`; the acceptor
    /// aligns the two (see `owned_http2_session_config`).
    pub owned_http2: bool,
    /// Experimental, default off (`DIV-053`): when a downstream HTTP/1.1 peer's EOF arrives
    /// while a transaction other than an established tunnel is open, fail the connection as a
    /// reset does, cancelling every handler and discarding unwritten egress. Reads go on past
    /// a request that closes the connection, discarding what they return, to see its EOF. The
    /// default ends only incomplete requests and keeps serving one whose request completed, so
    /// a half-closed client still gets its response. The EOF is seen only while reads run, so
    /// an ingress pause (flow control, pipelining) defers it. HTTP/2 connections are
    /// unchanged, and `owned_http1` refuses the option.
    pub abort_exchanges_on_eof: bool,
    /// Roxygen-only, default off (`DIV-038`): a connection the session resets (an
    /// HTTP/1.1 stream reset, such as a response body error, after writing what it
    /// already serialized; a connection error; `ServerConnection::cancel`) closes its
    /// socket without `SO_LINGER` zero, as hyper's server, which never
    /// resets, closes one by dropping it: the kernel still sends the bytes already
    /// written, then FIN, or RST only if received bytes are unread. An HTTP/1.1 response
    /// body error also abandons the output serialized but not yet written, as hyper drops
    /// its unflushed buffer, so the close never waits on a peer that stopped reading.
    /// Everything else about the reset is unchanged. Plaintext only: `HttpServer` refuses
    /// it with TLS credentials, and `serve_transport` for a transport declared TLS.
    pub non_abortive_close: bool,
    #[cfg(test)]
    pub(crate) accepted_socket_profile: Option<Arc<AcceptedSocketProfile>>,
}

impl Default for ServerSessionConfig {
    fn default() -> Self {
        Self {
            settings: vec![
                CodecSetting {
                    identifier: 6,
                    value: 32 * 1024,
                },
                CodecSetting {
                    identifier: 1,
                    value: 4_096,
                },
                CodecSetting {
                    identifier: 5,
                    value: 16_384,
                },
                CodecSetting {
                    identifier: 3,
                    value: 100,
                },
            ],
            max_concurrent_outgoing_streams: 100,
            stream_flow_control: 256 * 1024,
            connection_flow_control: 20 * 256 * 1024,
            stream_read_timeout: Duration::from_secs(10),
            connection_idle_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            tls_handshake_timeout: Duration::from_secs(60),
            goaway_timeout: Http2SessionConfig::default().goaway_timeout,
            read_allocation_size: 4_000,
            byte_stream_limits: ByteStreamLimits::new(256 * 1024, 256 * 1024, 1_024)
                .expect("default byte-stream limits are nonzero"),
            lifecycle_observer_factory: None,
            stats_factory: None,
            http2_rate_limits: Http2RateLimitConfig::default(),
            owned_http1: false,
            owned_http2: false,
            abort_exchanges_on_eof: false,
            non_abortive_close: false,
            #[cfg(test)]
            accepted_socket_profile: None,
        }
    }
}

pub(crate) const NON_ABORTIVE_CLOSE_WITH_TLS: &str = "non_abortive_close does not support TLS";

impl ServerSessionConfig {
    /// Refuses what the owned HTTP/1.1 mode cannot honor, instead of ignoring it.
    pub(crate) fn check_owned_http1(&self) -> Result<(), &'static str> {
        if !self.owned_http1 {
            return Ok(());
        }
        if self.stats_factory.is_some() {
            return Err("owned_http1 does not support stats_factory");
        }
        if self.lifecycle_observer_factory.is_some() {
            return Err("owned_http1 does not support lifecycle_observer_factory");
        }
        if self.abort_exchanges_on_eof {
            return Err("owned_http1 does not support abort_exchanges_on_eof");
        }
        if !self.stream_read_timeout.is_zero() {
            return Err("owned_http1 is untimed: stream_read_timeout must be zero");
        }
        if !self.connection_idle_timeout.is_zero() {
            return Err("owned_http1 is untimed: connection_idle_timeout must be zero");
        }
        if !self.write_timeout.is_zero() {
            return Err("owned_http1 is untimed: write_timeout must be zero");
        }
        Ok(())
    }

    /// Refuses what the owned HTTP/2 mode cannot honor, instead of ignoring it.
    pub(crate) fn check_owned_http2(&self) -> Result<(), &'static str> {
        if !self.owned_http2 {
            return Ok(());
        }
        if self.stats_factory.is_some() {
            return Err("owned_http2 does not support stats_factory");
        }
        Ok(())
    }

    /// Refuses what either owned mode cannot honor.
    pub(crate) fn check_owned(&self) -> Result<(), &'static str> {
        self.check_owned_http1()?;
        self.check_owned_http2()
    }
}

#[derive(Clone)]
pub(crate) struct AcceptorConfig {
    pub plaintext_protocol: CodecProtocol,
    pub session: ServerSessionConfig,
    pub connection_filter: Option<Arc<dyn NewConnectionFilter>>,
    pub tls: Option<Arc<ArcSwap<TlsServerOptions>>>,
    pub tls_observer: Option<Arc<dyn TlsHandshakeObserver>>,
    pub connection_observer: Option<Arc<dyn AcceptedConnectionObserver>>,
    pub zero_copy_threshold: usize,
}

pub(crate) enum AcceptedTransport {
    ByteStream(roxygen_transport::byte_stream::OwnedByteStream),
    Tls(TlsTransport),
}

impl AcceptedTransport {
    pub(crate) fn connection_context(
        &self,
        context: &WorkerContext,
        security: TransportSecurity,
        origin: Option<UpstreamOrigin>,
    ) -> Result<ConnectionContext, HttpError> {
        let transport: &dyn ByteStreamTransport = match self {
            Self::ByteStream(transport) => transport,
            Self::Tls(transport) => transport,
        };
        let metadata = ConnectionContext::new(
            transport.local_endpoint(context)?,
            transport.peer_endpoint(context)?,
            security,
            transport
                .peer_identity(context)?
                .map(|identity| Arc::from(identity.as_bytes())),
        )
        .with_upstream_origin(origin);
        Ok(match self {
            Self::ByteStream(_) => metadata,
            Self::Tls(transport) => {
                let handshake = transport.connection_info();
                metadata
                    .with_tls_handshake(
                        handshake.requested_server_name().map(Box::from),
                        handshake.application_protocol().map(Box::from),
                    )
                    .with_tls_parameters(
                        handshake.cipher_suite().map(Box::from),
                        handshake.key_exchange_group().map(Box::from),
                    )
            }
        })
    }

    fn enable_zero_copy(
        &self,
        context: &WorkerContext,
        threshold: usize,
    ) -> Result<bool, HttpError> {
        match self {
            Self::ByteStream(transport) => transport.enable_zero_copy(context, threshold),
            Self::Tls(transport) => transport.enable_zero_copy(context, threshold),
        }
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        match self {
            Self::ByteStream(transport) => {
                ByteStreamTransport::close_with_reset(transport, context)
            }
            Self::Tls(transport) => ByteStreamTransport::close_with_reset(transport, context),
        }
    }

    fn into_uniplex(self) -> Rc<dyn UniplexTransport> {
        match self {
            Self::ByteStream(transport) => Rc::new(transport),
            Self::Tls(transport) => Rc::new(transport),
        }
    }
}

#[derive(Clone)]
enum SessionHandle {
    Uniplex(UniplexHandle),
    OwnedHttp1 {
        handle: OwnedH1Handle,
        finished: Rc<Cell<bool>>,
    },
    OwnedHttp2 {
        handle: OwnedH2Handle,
        finished: Rc<Cell<bool>>,
    },
}

#[derive(Clone)]
pub(crate) struct SessionControl {
    handle: SessionHandle,
    transport: Rc<dyn UniplexTransport>,
    context: WorkerContext,
}

impl SessionControl {
    pub(crate) fn is_finished(&self) -> bool {
        match &self.handle {
            SessionHandle::Uniplex(handle) => handle.is_finished(),
            SessionHandle::OwnedHttp1 { finished, .. } => finished.get(),
            SessionHandle::OwnedHttp2 { finished, .. } => finished.get(),
        }
    }

    pub(crate) fn drain(&self) {
        let _ = self.try_drain();
    }

    pub(crate) fn try_drain(&self) -> Result<bool, HttpError> {
        match &self.handle {
            SessionHandle::Uniplex(handle) => handle.initiate_drain(),
            SessionHandle::OwnedHttp1 { handle, .. } => handle.initiate_drain(),
            SessionHandle::OwnedHttp2 { handle, .. } => handle.initiate_drain(),
        }
    }

    pub(crate) fn force_stop(&self) {
        let _ = self.transport.close_with_reset(&self.context);
    }
}

/// The downstream session an accepted connection runs.
pub(crate) enum ServerSession {
    Uniplex(UniplexSession),
    OwnedHttp1(OwnedHttp1Session),
    OwnedHttp2(OwnedHttp2Session),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerSessionKind {
    Uniplex,
    OwnedHttp1,
    OwnedHttp2,
}

impl ServerSession {
    pub(crate) fn run(
        self,
    ) -> Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>> {
        match self {
            Self::Uniplex(session) => session.run(),
            Self::OwnedHttp1(session) => Box::pin(session.run()),
            Self::OwnedHttp2(session) => Box::pin(session.run()),
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> ServerSessionKind {
        match self {
            Self::Uniplex(_) => ServerSessionKind::Uniplex,
            Self::OwnedHttp1(_) => ServerSessionKind::OwnedHttp1,
            Self::OwnedHttp2(_) => ServerSessionKind::OwnedHttp2,
        }
    }
}

pub(crate) struct AcceptedSession {
    pub control: SessionControl,
    pub session: ServerSession,
    #[cfg(test)]
    pub retirement: Option<TransportRetirement>,
}

pub(crate) async fn accept_session(
    context: WorkerContext,
    socket: std::net::TcpStream,
    handler: HttpHandlerRef,
    config: Rc<AcceptorConfig>,
    draining: Arc<AtomicBool>,
    worker_index: usize,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
) -> Result<AcceptedSession, HttpError> {
    socket.set_nodelay(true).map_err(|error| {
        HttpError::with_message(HttpErrorCode::CONNECT_ERROR, error.to_string().as_bytes())
            .with_cause(Arc::new(error))
    })?;
    #[cfg(test)]
    if let Some(profile) = config.session.accepted_socket_profile.as_ref() {
        invoke_accept_callback(
            &callback_failure,
            worker_index,
            ServerCallbackStage::AcceptedConnectionObserver,
            || profile(&socket),
        )??;
    }
    let (transport, protocol, secure) = if let Some(tls) = config.tls.as_ref() {
        let tls = (*tls.load_full()).clone();
        let result = TlsTransport::accept(
            &context,
            socket,
            tls,
            config.session.byte_stream_limits,
            config.session.tls_handshake_timeout,
            CancellationSource::new().token(),
        )
        .await;
        let tls = match result {
            Ok(tls) => {
                if let Some(observer) = config.tls_observer.as_ref() {
                    invoke_accept_callback(
                        &callback_failure,
                        worker_index,
                        ServerCallbackStage::TlsHandshakeObserver,
                        || observer.on_handshake_success(),
                    )?;
                }
                tls
            }
            Err(error) => {
                if let Some(observer) = config.tls_observer.as_ref() {
                    let _ = invoke_accept_callback(
                        &callback_failure,
                        worker_index,
                        ServerCallbackStage::TlsHandshakeObserver,
                        || observer.on_error(&error),
                    );
                }
                return Err(error);
            }
        };
        let protocol = match tls.connection_info().application_protocol() {
            Some(b"h2") => CodecProtocol::Http2,
            Some(b"http/1.1") | Some(_) | None => CodecProtocol::Http11,
        };
        (AcceptedTransport::Tls(tls), protocol, true)
    } else {
        let transport =
            TcpTransport::from_accepted(&context, socket, config.session.byte_stream_limits)?;
        (
            AcceptedTransport::ByteStream(transport),
            config.plaintext_protocol,
            false,
        )
    };

    let metadata = Arc::new(
        transport
            .connection_context(
                &context,
                if secure {
                    TransportSecurity::Tls
                } else {
                    TransportSecurity::Plaintext
                },
                None,
            )?
            .with_http_protocol(protocol),
    );
    let connection = connection_info(protocol, metadata);
    if secure && let Some(observer) = config.tls_observer.as_ref() {
        invoke_accept_callback(
            &callback_failure,
            worker_index,
            ServerCallbackStage::TlsHandshakeObserver,
            || observer.on_success(&connection),
        )?;
    }
    attach_session(
        transport,
        SessionAttachment {
            context: &context,
            handler,
            config: &config,
            connection,
            draining: Some(&draining),
            transport_limits: None,
            worker_index,
            callback_failure: &callback_failure,
        },
    )
}

pub(crate) fn connection_info(
    protocol: CodecProtocol,
    context: Arc<ConnectionContext>,
) -> ConnectionInfo {
    ConnectionInfo {
        local_address: context
            .local_endpoint()
            .socket_address()
            .cloned()
            .unwrap_or_default(),
        peer_address: context
            .peer_endpoint()
            .socket_address()
            .cloned()
            .unwrap_or_default(),
        protocol,
        secure: context.security() == TransportSecurity::Tls,
        peer_identity: context.peer_identity().map(Box::from),
        zero_copy_enabled: false,
        context,
    }
}

pub(crate) struct SessionAttachment<'a> {
    pub context: &'a WorkerContext,
    pub handler: HttpHandlerRef,
    pub config: &'a AcceptorConfig,
    pub connection: ConnectionInfo,
    pub draining: Option<&'a AtomicBool>,
    pub transport_limits: Option<ByteStreamLimits>,
    pub worker_index: usize,
    pub callback_failure: &'a Arc<Mutex<Option<CallbackFailure>>>,
}

pub(crate) fn attach_session(
    transport: AcceptedTransport,
    attachment: SessionAttachment<'_>,
) -> Result<AcceptedSession, HttpError> {
    let SessionAttachment {
        context,
        handler,
        config,
        mut connection,
        draining,
        transport_limits,
        worker_index,
        callback_failure,
    } = attachment;
    config.session.check_owned().map_err(|message| {
        HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message.as_bytes())
    })?;
    if !context.handle().is_accepting() || context.is_cancelled() {
        return Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"server admission closed",
        ));
    }
    if config.session.non_abortive_close {
        match &transport {
            AcceptedTransport::ByteStream(transport) if !connection.secure => {
                transport.enable_non_abortive_close(context)?;
            }
            AcceptedTransport::ByteStream(_) | AcceptedTransport::Tls(_) => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    NON_ABORTIVE_CLOSE_WITH_TLS.as_bytes(),
                ));
            }
        }
    }
    if let Some(filter) = config.connection_filter.as_ref() {
        let accepted =
            catch_unwind(AssertUnwindSafe(|| filter.accept(&connection))).unwrap_or(false);
        if !accepted {
            transport.close_with_reset(context)?;
            return Err(HttpError::with_message(
                HttpErrorCode::CONNECT_ERROR,
                b"connection rejected by server admission filter",
            ));
        }
    }
    connection.zero_copy_enabled =
        transport.enable_zero_copy(context, config.zero_copy_threshold)?;
    if let Some(observer) = config.connection_observer.as_ref() {
        invoke_accept_callback(
            callback_failure,
            worker_index,
            ServerCallbackStage::AcceptedConnectionObserver,
            || observer.on_accepted(&connection),
        )?;
    }
    let handler = bind_connection(handler, Arc::clone(&connection.context));
    #[cfg(test)]
    let retirement = match &transport {
        AcceptedTransport::ByteStream(transport) => transport.retirement(),
        AcceptedTransport::Tls(transport) => transport.retirement(),
    };
    let transport = transport.into_uniplex();

    let mut uniplex = UniplexConfig {
        buffer_limit: config.session.connection_flow_control as usize,
        ingress_buffer_limit: config.session.stream_flow_control,
        maximum_read_size: config.session.stream_flow_control as usize,
        read_allocation_size: config.session.read_allocation_size,
        maximum_write_size: config.session.byte_stream_limits.maximum_write_size(),
        read_timeout: config.session.connection_idle_timeout,
        stream_read_timeout: config.session.stream_read_timeout,
        write_timeout: config.session.write_timeout,
        drain_request_body_on_drop: false,
        wait_for_http1_response_on_drain: false,
        abort_exchanges_on_eof: config.session.abort_exchanges_on_eof,
        deliver_ingress_before_abort: false,
        unframed_bodiless_http1_requests: false,
        non_abortive_close: config.session.non_abortive_close,
    };
    if let Some(limits) = transport_limits {
        uniplex.maximum_read_size = uniplex.maximum_read_size.min(limits.maximum_read_size());
        uniplex.read_allocation_size = uniplex.read_allocation_size.min(limits.maximum_read_size());
        uniplex.maximum_write_size = uniplex.maximum_write_size.min(limits.maximum_write_size());
    }
    if config.session.owned_http1 && connection.protocol == CodecProtocol::Http11 {
        let io = UniplexConfig {
            drain_request_body_on_drop: true,
            ..uniplex
        };
        let (session, handle, finished) = OwnedHttp1Session::new(
            context,
            Rc::clone(&transport),
            handler,
            &connection.context,
            io,
        )?;
        return admit_session(
            context,
            draining,
            ServerSession::OwnedHttp1(session),
            SessionControl {
                handle: SessionHandle::OwnedHttp1 { handle, finished },
                transport,
                context: context.clone(),
            },
            #[cfg(test)]
            retirement,
        );
    }
    if config.session.owned_http2 && connection.protocol == CodecProtocol::Http2 {
        let io = UniplexConfig {
            drain_request_body_on_drop: true,
            ..uniplex
        };
        let (mut session, handle, finished) = OwnedHttp2Session::new(
            context,
            Rc::clone(&transport),
            handler,
            &connection.context,
            io,
            owned_http2_session_config(&config.session),
        )?;
        if let Some(factory) = config.session.lifecycle_observer_factory.as_ref() {
            let observer = invoke_accept_callback(
                callback_failure,
                worker_index,
                ServerCallbackStage::LifecycleObserverFactory,
                || factory(),
            )?;
            session.add_lifecycle_observer(observer)?;
        }
        return admit_session(
            context,
            draining,
            ServerSession::OwnedHttp2(session),
            SessionControl {
                handle: SessionHandle::OwnedHttp2 { handle, finished },
                transport,
                context: context.clone(),
            },
            #[cfg(test)]
            retirement,
        );
    }
    let stats = config
        .session
        .stats_factory
        .as_ref()
        .map(|factory| {
            invoke_accept_callback(
                callback_failure,
                worker_index,
                ServerCallbackStage::LifecycleObserverFactory,
                || factory(&connection),
            )
        })
        .transpose()?;
    let (session, handle) = match connection.protocol {
        CodecProtocol::Http11 if stats.is_some() => UniplexSession::new_with_stats(
            context.clone(),
            TransportDirection::Downstream,
            Rc::clone(&transport),
            Some(handler),
            uniplex,
            stats.expect("statistics factory returned a recorder"),
        )?,
        CodecProtocol::Http11 => UniplexSession::new(
            context.clone(),
            TransportDirection::Downstream,
            Rc::clone(&transport),
            Some(handler),
            uniplex,
        )?,
        CodecProtocol::Http2 => {
            let http2 = http2_session_config(&config.session);
            let http2 = Http2SessionConfig { stats, ..http2 };
            UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Downstream,
                Rc::clone(&transport),
                Some(handler),
                uniplex,
                http2,
            )?
        }
        _ => return Err(unsupported_protocol()),
    };
    if let Some(factory) = config.session.lifecycle_observer_factory.as_ref() {
        let observer = invoke_accept_callback(
            callback_failure,
            worker_index,
            ServerCallbackStage::LifecycleObserverFactory,
            || factory(),
        )?;
        session.add_lifecycle_observer(observer)?;
    }
    admit_session(
        context,
        draining,
        ServerSession::Uniplex(session),
        SessionControl {
            handle: SessionHandle::Uniplex(handle),
            transport,
            context: context.clone(),
        },
        #[cfg(test)]
        retirement,
    )
}

fn admit_session(
    context: &WorkerContext,
    draining: Option<&AtomicBool>,
    session: ServerSession,
    control: SessionControl,
    #[cfg(test)] retirement: Option<TransportRetirement>,
) -> Result<AcceptedSession, HttpError> {
    if !context.handle().is_accepting() || context.is_cancelled() {
        return Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"server admission closed",
        ));
    }
    if draining.is_some_and(|draining| draining.load(Ordering::Acquire)) {
        control.try_drain()?;
    }
    Ok(AcceptedSession {
        control,
        session,
        #[cfg(test)]
        retirement,
    })
}

pub(crate) fn invoke_accept_callback<T>(
    callback_failure: &Arc<Mutex<Option<CallbackFailure>>>,
    worker_index: usize,
    stage: ServerCallbackStage,
    callback: impl FnOnce() -> T,
) -> Result<T, HttpError> {
    match catch_unwind(AssertUnwindSafe(callback)) {
        Ok(value) => Ok(value),
        Err(payload) => {
            record_callback_failure(callback_failure, worker_index, stage);
            discard_panicking_value(payload);
            Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"server connection callback panicked",
            ))
        }
    }
}

pub(crate) fn http2_session_config(config: &ServerSessionConfig) -> Http2SessionConfig {
    let stream_receive_window = config
        .settings
        .iter()
        .find(|setting| setting.identifier == 4)
        .and_then(|setting| u32::try_from(setting.value).ok())
        .unwrap_or(config.stream_flow_control);
    Http2SessionConfig {
        settings: config.settings.clone(),
        connection_receive_window: config.connection_flow_control,
        stream_receive_window,
        stream_receive_window_mode: StreamReceiveWindowMode::PreserveConfigured,
        max_concurrent_outgoing_streams: config.max_concurrent_outgoing_streams,
        rate_limits: config.http2_rate_limits,
        goaway_timeout: config.goaway_timeout,
        ..Http2SessionConfig::default()
    }
}

/// The owned driver's HTTP/2 configuration: the managed session's, with the parts
/// the driver cannot honor turned off instead of silently kept. It records no
/// statistics, so stats and ping probes are `None`. Both drivers use the same flood limits.
/// The GOAWAY timeout is the managed session's, `goaway_timeout`. The driver
/// also requires its stream receive window to equal the advertised
/// `INITIAL_WINDOW_SIZE`, so the window follows the advertised setting (the
/// protocol default when `settings` carries none); where the setting is present,
/// as the parity server sets it, this is the managed value unchanged.
pub(crate) fn owned_http2_session_config(config: &ServerSessionConfig) -> Http2SessionConfig {
    let mut http2 = http2_session_config(config);
    let advertised = http2
        .settings
        .iter()
        .rev()
        .find(|setting| setting.identifier == 4)
        .and_then(|setting| u32::try_from(setting.value).ok())
        .unwrap_or(DEFAULT_STREAM_WINDOW);
    http2.stream_receive_window = advertised;
    Http2SessionConfig {
        stats: None,
        ping_probes: None,
        ..http2
    }
}

pub(crate) async fn run_session(session: ServerSession) -> Result<UniplexRunSummary, HttpError> {
    session.run().await
}

fn unsupported_protocol() -> HttpError {
    HttpError::with_message(
        HttpErrorCode::CONNECT_ERROR,
        b"accepted connection negotiated an unsupported protocol",
    )
}
use arc_swap::ArcSwap;
