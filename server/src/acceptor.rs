/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

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
use roxygen_session::OwnedConnectionDeadlines;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::http2::StreamReceiveWindowMode;
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1Handle;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h2::OwnedH2Handle;
use roxygen_session::owned_h2::OwnedH2Options;
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
use roxygen_transport::tls::TlsCloseControl;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::TlsTransport;

use crate::connection::bind_connection;
use crate::owned_application::OwnedConnectionAttach;
use crate::owned_application::OwnedConnectionFactory;
use crate::server::CallbackFailure;
use crate::server::ServerCallbackStage;
use crate::server::discard_panicking_value;
use crate::server::record_callback_failure;

mod handler_application;
mod owned_http1;
mod owned_http2;

pub(crate) use handler_application::HandlerApplication;
use owned_http1::OwnedHttp1Session;
use owned_http2::OwnedHttp2Session;
#[cfg(test)]
pub(crate) use owned_http2::fault_next_http2_run;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionInfo {
    pub local_address: SocketAddress,
    pub peer_address: SocketAddress,
    pub protocol: CodecProtocol,
    pub secure: bool,
    pub peer_identity: Option<Box<[u8]>>,
    pub zero_copy_enabled: bool,
    /// The immediate connection, not any logical origin behind it.
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
    /// Called when a native TLS accept succeeds, before connection metadata is read and
    /// before admission. If transport setup fails after the handshake, `on_error` is called
    /// instead.
    fn on_handshake_success(&self) {}

    /// Called after a successful native TLS accept, once connection metadata is available.
    fn on_success(&self, _connection: &ConnectionInfo) {}

    /// Called when a native TLS accept fails. `READ_TIMEOUT` means the handshake missed its
    /// deadline (`ServerSessionConfig::tls_handshake_timeout`).
    fn on_error(&self, _error: &HttpError) {}
}

pub trait AcceptedConnectionObserver: Send + Sync + 'static {
    fn on_accepted(&self, connection: &ConnectionInfo);

    /// Called once `HttpServer` has admitted the session and registered its control, before
    /// the session runs. `serve_transport` does not call it.
    fn on_registered(&self) {}
}

/// Identifies a connection that an `HttpServer` accepted: the index of the worker that serves
/// it, and that worker's serial number for it. Each worker numbers its connections from 1, and
/// the serial wraps.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnectionId {
    pub worker: u32,
    pub serial: u64,
}

/// How an accepted connection ended, as `ConnectionLifecycleObserver::on_socket_closed`
/// reports it.
#[derive(Clone, Copy, Debug)]
pub enum ConnectionOutcome<'a> {
    /// The TLS handshake failed or missed `ServerSessionConfig::tls_handshake_timeout`.
    TlsFailed(&'a HttpError),
    /// The connection ended before its session ran, for a reason other than its TLS handshake:
    /// the connection filter refused it, the server had stopped admitting sessions, an accept
    /// callback panicked or the connection's setup failed.
    Rejected(&'a HttpError),
    /// The session ran and returned `result`. `protocol` is the protocol it served.
    /// `handshake_complete` is whether its driver completed the connection handshake, as
    /// `ConnectionLifecycleObserver::on_protocol_established` describes. It is always false for
    /// a `UniplexSession`, which does not track the handshake.
    Closed {
        protocol: CodecProtocol,
        handshake_complete: bool,
        result: &'a Result<UniplexRunSummary, HttpError>,
    },
    /// The connection was dropped before it reached another outcome, as by a forced stop or a
    /// panic.
    Aborted,
}

/// Observes each connection that an `HttpServer` accepts, from the start of its session task on
/// its worker to its close, in both accept distributions and with every session mode.
///
/// Every callback runs in the connection's session task, on the worker that serves the
/// connection. `on_socket_accepted` comes first, when the task starts and before any TLS
/// handshake, and `on_socket_closed` comes last, exactly once. A socket whose session task never
/// starts, one that `AcceptStats::handoff_dropped` or `AcceptStats::closed_at_force_stop`
/// counts, is not reported. A panicking callback does not affect the connection: the server
/// records the first panic, and its stop then fails with `HttpServerError::CallbackPanic`
/// naming `ServerCallbackStage::ConnectionLifecycleObserver`.
pub trait ConnectionLifecycleObserver: Send + Sync + 'static {
    /// Called when the connection's session task starts, before the TLS handshake.
    fn on_socket_accepted(&self, _id: ConnectionId) {}

    /// Called once, after the driver poll in which an owned session (`owned_http1` or
    /// `owned_http2`) completes its connection handshake: an HTTP/2 session has processed the
    /// peer's first SETTINGS frame, or an HTTP/1.1 session has delivered its first request
    /// head. A `UniplexSession` never reports it.
    fn on_protocol_established(&self, _id: ConnectionId, _protocol: CodecProtocol) {}

    /// Called once, when the connection ends, with its outcome.
    fn on_socket_closed(&self, _id: ConnectionId, _outcome: &ConnectionOutcome<'_>) {}
}

/// Reports one connection's lifecycle to a `ConnectionLifecycleObserver`. Dropping it without
/// `close` reports `ConnectionOutcome::Aborted`. A callback panic is recorded as the worker's
/// callback failure and otherwise ignored.
pub(crate) struct ConnectionLifecycle {
    observer: Arc<dyn ConnectionLifecycleObserver>,
    id: ConnectionId,
    worker_index: usize,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    established: bool,
    closed: bool,
}

impl ConnectionLifecycle {
    /// Reports the accepted socket.
    pub(crate) fn accepted(
        observer: Arc<dyn ConnectionLifecycleObserver>,
        id: ConnectionId,
        worker_index: usize,
        callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    ) -> Self {
        let lifecycle = Self {
            observer,
            id,
            worker_index,
            callback_failure,
            established: false,
            closed: false,
        };
        lifecycle.invoke(|observer| observer.on_socket_accepted(id));
        lifecycle
    }

    /// Continues the lifecycle with the session admitted for the connection, which serves
    /// `protocol` and is controlled by `control`.
    pub(crate) fn admitted(
        self,
        protocol: CodecProtocol,
        control: &SessionControl,
    ) -> SessionLifecycle {
        SessionLifecycle {
            lifecycle: self,
            protocol,
            handshake: control.handshake(),
        }
    }

    /// Reports the established protocol, once.
    pub(crate) fn established(&mut self, protocol: CodecProtocol) {
        if !self.established {
            self.established = true;
            self.invoke(|observer| observer.on_protocol_established(self.id, protocol));
        }
    }

    /// Reports how the connection ended.
    pub(crate) fn close(mut self, outcome: &ConnectionOutcome<'_>) {
        self.report_closed(outcome);
    }

    fn report_closed(&mut self, outcome: &ConnectionOutcome<'_>) {
        if !self.closed {
            self.closed = true;
            self.invoke(|observer| observer.on_socket_closed(self.id, outcome));
        }
    }

    fn invoke(&self, callback: impl FnOnce(&dyn ConnectionLifecycleObserver)) {
        let _ = invoke_accept_callback(
            &self.callback_failure,
            self.worker_index,
            ServerCallbackStage::ConnectionLifecycleObserver,
            || callback(&*self.observer),
        );
    }
}

impl Drop for ConnectionLifecycle {
    fn drop(&mut self) {
        self.report_closed(&ConnectionOutcome::Aborted);
    }
}

/// The lifecycle of a connection whose session was admitted.
pub(crate) struct SessionLifecycle {
    lifecycle: ConnectionLifecycle,
    protocol: CodecProtocol,
    handshake: SessionHandshake,
}

impl SessionLifecycle {
    /// Runs `session` as `run_session` does. An owned session also reports its protocol after
    /// the poll in which its driver first completes the connection handshake.
    pub(crate) async fn run(
        &mut self,
        session: ServerSession,
    ) -> Result<UniplexRunSummary, HttpError> {
        if matches!(self.handshake, SessionHandshake::Untracked) {
            return session.run().await;
        }
        let mut run = session.run();
        std::future::poll_fn(|context| {
            let poll = run.as_mut().poll(context);
            if !self.lifecycle.established && self.handshake.is_complete() {
                self.lifecycle.established(self.protocol);
            }
            poll
        })
        .await
    }

    /// Reports that the session returned `result`.
    pub(crate) fn close(self, result: &Result<UniplexRunSummary, HttpError>) {
        let handshake_complete = self.handshake.is_complete();
        self.lifecycle.close(&ConnectionOutcome::Closed {
            protocol: self.protocol,
            handshake_complete,
            result,
        });
    }
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
    /// and zero leaves the handshake untimed. A missed deadline fails the accept with
    /// `READ_TIMEOUT`, which `TlsHandshakeObserver::on_error` receives. Both owned modes accept
    /// it. `serve_transport` ignores it because its transport is already established.
    pub tls_handshake_timeout: Duration,
    /// How long an HTTP/2 drain holds its final GOAWAY after GOAWAY(2^31-1), so that streams
    /// the peer opened before the first GOAWAY reached it are still served. Zero sends both
    /// GOAWAYs immediately. The default is one second, as for `UniplexSession`. Both HTTP/2
    /// drivers apply it. HTTP/1.1 connections ignore it.
    pub goaway_timeout: Duration,
    pub read_allocation_size: usize,
    pub byte_stream_limits: ByteStreamLimits,
    pub lifecycle_observer_factory:
        Option<Arc<dyn Fn() -> Rc<dyn LifecycleObserver> + Send + Sync>>,
    /// Builds an owner-local statistics recorder for each accepted HTTP/1.1 or HTTP/2
    /// session. `None` records no statistics.
    pub stats_factory:
        Option<Arc<dyn Fn(&ConnectionInfo) -> Rc<dyn Http2SessionStats> + Send + Sync>>,
    /// HTTP/2 flood limits for both downstream drivers, using `Http2RateLimitConfig::default()`
    /// unless overridden. Each category can be disabled with an unbounded threshold.
    pub http2_rate_limits: Http2RateLimitConfig,
    /// Serves HTTP/1.1 connections on the owned driver (`roxygen_session::owned_h1`) instead
    /// of `UniplexSession`, with no task per request. Experimental; off by default. HTTP/2
    /// connections are unchanged.
    ///
    /// The owned mode applies `owned_deadlines` and no other session timeout.
    /// `stream_read_timeout`, `connection_idle_timeout` and `write_timeout` must be zero,
    /// which also leaves HTTP/2 sessions without them. TLS handshakes still use
    /// `tls_handshake_timeout`. The mode records no statistics and observes no
    /// lifecycle, so `stats_factory` and `lifecycle_observer_factory` must be `None`. Server
    /// construction rejects any other configuration with `HttpServerError::InvalidConfig`
    /// (`INVALID_STATE_TRANSITION` from `serve_transport`), naming the reason.
    pub owned_http1: bool,
    /// Serves HTTP/2 connections on the owned driver (`roxygen_session::owned_h2`) instead of
    /// `UniplexSession`, with no task per request. Experimental; off by default. HTTP/1.1
    /// connections are unchanged.
    ///
    /// The owned mode applies the session timeouts. `connection_idle_timeout` drains an idle
    /// connection with GOAWAY. `write_timeout` bounds a stalled transport write and a
    /// response's wait for flow-control credit. `stream_read_timeout` bounds each wait for
    /// request body, and sends 408 if the response head is not yet sent. It also applies
    /// `goaway_timeout`, `http2_rate_limits`, `abort_http2_streams_on_eof`,
    /// `exact_method_tokens`, `verbatim_http2_header_values` and `add_date_header_to_response`
    /// as `UniplexSession` does, and `owned_deadlines`. It records no statistics, so
    /// `stats_factory` must be `None`.
    /// Server construction rejects any other configuration with `HttpServerError::InvalidConfig`
    /// (`INVALID_STATE_TRANSITION` from `serve_transport`), naming the reason.
    ///
    /// A `lifecycle_observer_factory` observer sees the connection's lifecycle as under
    /// `UniplexSession`, with some differences. Among them, events arrive at the end of each
    /// driver poll, a stream detaches when the driver retires it, and an observer panic aborts
    /// the connection with `INTERNAL_ERROR`. The stream receive window is `stream_flow_control`,
    /// which the driver advertises as `INITIAL_WINDOW_SIZE`, unless `settings` sets
    /// `INITIAL_WINDOW_SIZE`. The driver requires the window to equal the advertised value, so
    /// if `settings` sets it more than once, the last value applies.
    pub owned_http2: bool,
    /// The connection deadlines of the owned downstream drivers (`owned_http1` and
    /// `owned_http2`): a handshake deadline, an idle deadline, and what happens when one
    /// expires. None by default. `UniplexSession` connections ignore them. This is the only
    /// source of the drivers' deadlines: it replaces `Http1DriverConfig::deadlines` and
    /// `Http2SessionConfig::deadlines`.
    ///
    /// On `owned_http2`, `connection_idle_timeout` still drains an idle connection with GOAWAY
    /// on its own schedule, and whichever expires first acts. To apply only these deadlines,
    /// set `connection_idle_timeout` to zero.
    pub owned_deadlines: OwnedConnectionDeadlines,
    /// Fails a downstream HTTP/1.1 connection, as a reset does, when the peer's EOF arrives
    /// while a transaction other than an established tunnel is open. Experimental; off by
    /// default.
    ///
    /// The failure cancels every handler and discards unwritten egress. Reads continue past a
    /// request that closes the connection, discarding the data, so that the EOF is seen. With
    /// the option off, only incomplete requests end and a completed request is still served,
    /// so a half-closed client still gets its response. The EOF is seen only while reads run,
    /// so an ingress pause (flow control, pipelining) defers it. HTTP/2 connections use
    /// `abort_http2_streams_on_eof` instead. `owned_http1` applies this option by the same
    /// rules.
    pub abort_exchanges_on_eof: bool,
    /// Fails a downstream HTTP/2 connection with `TRANSPORT_EOF` when the peer's EOF arrives
    /// while a stream is open. Experimental; off by default.
    ///
    /// A stream is open while its response's END_STREAM is unserialized or its request head is
    /// still arriving. The failure works like a transport read error. Every stream's handler is
    /// cancelled and its sources stopped, and no error response is sent. Output already
    /// serialized is still written, followed by a GOAWAY and a FIN. With the option off, only
    /// streams with an incomplete request end and the others are still served, so a
    /// half-closed client still gets their responses. An EOF with no open stream closes the
    /// connection normally. Both HTTP/2 drivers apply it. HTTP/1.1 connections use
    /// `abort_exchanges_on_eof` instead.
    pub abort_http2_streams_on_eof: bool,
    /// When the session resets a connection, closes its socket without `SO_LINGER` zero.
    /// Off by default.
    ///
    /// Resets include an HTTP/1.1 stream reset (such as a response body error, after writing
    /// what was already serialized), an HTTP/1.1 connection error, an HTTP/2 write error and
    /// `ServerConnection::cancel`. A failed HTTP/2 connection ends with FIN either way. The
    /// socket is closed by dropping it, as hyper's server does (hyper never resets a
    /// connection). The kernel still sends the bytes already written, then FIN, or RST only if
    /// received bytes are unread. An HTTP/1.1 response body error also discards output that is
    /// serialized but not yet written, as hyper drops its unflushed buffer, so the close never
    /// waits on a peer that stopped reading. The reset is otherwise unchanged.
    ///
    /// On a TLS connection of `HttpServer`, the close kind follows the cause. A graceful end, a
    /// connection error that the session reports with GOAWAY (which is written first) and a peer
    /// EOF send close_notify and then FIN. A reset, including a hard close, a deadline's hard
    /// close, a forced stop, a transport error and the reset after an HTTP/1.1 error response,
    /// sends FIN with no close_notify; so does an owned HTTP/2 session that closes before its
    /// handshake completes, and a failed TLS handshake. Ciphertext that the socket has not taken
    /// yet is discarded by a reset. `serve_transport` rejects the option for a transport declared
    /// TLS, whose TLS layer Roxygen does not own.
    pub non_abortive_close: bool,
    /// Keeps downstream HTTP/2 request methods as exact, case-sensitive RFC tokens. Off by
    /// default. HTTP/1 always uses llhttp's table of recognized methods, whatever this is set
    /// to.
    pub exact_method_tokens: bool,
    /// Passes each HTTP/2 request field value to the application exactly as decoded,
    /// surrounding spaces and tabs included, as h2 and hyper's server do. Off by default, in
    /// which case the values are trimmed.
    ///
    /// Values that h2 rejects make the request malformed: a control byte other than HTAB, DEL,
    /// a `content-length` that is not decimal digits, or a `te` other than `trailers`. An empty
    /// `content-length` reaches the application as an empty field, and the body is checked
    /// against zero, as h2 reads it. Response heads are encoded with their values as stored, so
    /// values inserted with `HttpHeaders::add_verbatim` reach the client byte for byte, as
    /// hyper writes a `HeaderValue`. HTTP/1.1 connections still trim optional request
    /// whitespace, as `httparse` does, and write every stored value unchanged. `owned_http2`
    /// applies it.
    pub verbatim_http2_header_values: bool,
    /// Adds a `Date` header to each response that lacks one. On by default, because RFC 9110
    /// requires an origin server with a clock to send `Date`. A `Date` that the handler sets is
    /// sent unchanged either way. `owned_http1` and `owned_http2` apply it.
    pub add_date_header_to_response: bool,
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
            owned_deadlines: OwnedConnectionDeadlines::default(),
            abort_exchanges_on_eof: false,
            abort_http2_streams_on_eof: false,
            non_abortive_close: false,
            exact_method_tokens: false,
            verbatim_http2_header_values: false,
            add_date_header_to_response: true,
            #[cfg(test)]
            accepted_socket_profile: None,
        }
    }
}

impl ServerSessionConfig {
    /// Rejects settings that the owned HTTP/1.1 mode does not support.
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

    /// Rejects settings that the owned HTTP/2 mode does not support.
    pub(crate) fn check_owned_http2(&self) -> Result<(), &'static str> {
        if !self.owned_http2 {
            return Ok(());
        }
        if self.stats_factory.is_some() {
            return Err("owned_http2 does not support stats_factory");
        }
        Ok(())
    }

    /// Rejects settings that either owned mode does not support.
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
                let metadata = metadata
                    .with_tls_handshake(
                        handshake.requested_server_name().map(Box::from),
                        handshake.application_protocol().map(Box::from),
                    )
                    .with_tls_parameters(
                        handshake.cipher_suite().map(Box::from),
                        handshake.key_exchange_group().map(Box::from),
                    );
                match handshake.session_facts() {
                    Some(session) => metadata.with_tls_session(session),
                    None => metadata,
                }
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

    /// Makes an owned HTTP/2 session whose graceful close starts before its handshake completes
    /// end with FIN and no close_notify on its TLS connection `tls_close`, as a failed handshake
    /// does.
    fn skip_close_notify_before_http2_handshake(&self, tls_close: Option<&TlsCloseControl>) {
        if let (Some(tls_close), SessionHandle::OwnedHttp2 { handle, .. }) =
            (tls_close, &self.handle)
        {
            let handle = handle.clone();
            tls_close.skip_close_notify_unless(move || handle.handshake_complete());
        }
    }

    fn handshake(&self) -> SessionHandshake {
        match &self.handle {
            SessionHandle::Uniplex(_) => SessionHandshake::Untracked,
            SessionHandle::OwnedHttp1 { handle, .. } => {
                SessionHandshake::OwnedHttp1(handle.clone())
            }
            SessionHandle::OwnedHttp2 { handle, .. } => {
                SessionHandshake::OwnedHttp2(handle.clone())
            }
        }
    }
}

/// Reads whether a session's driver has completed its connection handshake.
enum SessionHandshake {
    /// A `UniplexSession` does not track the handshake.
    Untracked,
    OwnedHttp1(OwnedH1Handle),
    OwnedHttp2(OwnedH2Handle),
}

impl SessionHandshake {
    fn is_complete(&self) -> bool {
        match self {
            Self::Untracked => false,
            Self::OwnedHttp1(handle) => handle.handshake_complete(),
            Self::OwnedHttp2(handle) => handle.handshake_complete(),
        }
    }
}

/// The downstream session an accepted connection runs.
pub(crate) enum ServerSession {
    Uniplex(UniplexSession),
    OwnedHttp1(OwnedHttp1Session<HandlerApplication>),
    OwnedHttp2(OwnedHttp2Session<HandlerApplication>),
    /// A session of an application that `HttpServer::start_owned` hosts.
    Hosted(Box<dyn HostedSession>),
}

/// An unrun owned session of a hosted application, whose type the box erases.
pub(crate) trait HostedSession {
    fn run(self: Box<Self>) -> RunFuture;
}

impl<A: OwnedApplication> HostedSession for OwnedHttp1Session<A> {
    fn run(self: Box<Self>) -> RunFuture {
        Box::pin((*self).run())
    }
}

impl<A: OwnedApplication> HostedSession for OwnedHttp2Session<A> {
    fn run(self: Box<Self>) -> RunFuture {
        Box::pin((*self).run())
    }
}

type RunFuture = Pin<Box<dyn Future<Output = Result<UniplexRunSummary, HttpError>> + 'static>>;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServerSessionKind {
    Uniplex,
    OwnedHttp1,
    OwnedHttp2,
    Hosted,
}

impl ServerSession {
    pub(crate) fn run(self) -> RunFuture {
        match self {
            Self::Uniplex(session) => session.run(),
            Self::OwnedHttp1(session) => Box::pin(session.run()),
            Self::OwnedHttp2(session) => Box::pin(session.run()),
            Self::Hosted(session) => session.run(),
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> ServerSessionKind {
        match self {
            Self::Uniplex(_) => ServerSessionKind::Uniplex,
            Self::OwnedHttp1(_) => ServerSessionKind::OwnedHttp1,
            Self::OwnedHttp2(_) => ServerSessionKind::OwnedHttp2,
            Self::Hosted(_) => ServerSessionKind::Hosted,
        }
    }
}

pub(crate) struct AcceptedSession {
    pub control: SessionControl,
    pub session: ServerSession,
    pub protocol: CodecProtocol,
    #[cfg(test)]
    pub retirement: Option<TransportRetirement>,
}

/// What a worker serves its connections with.
#[derive(Clone)]
pub(crate) enum WorkerApp {
    /// The worker's handler, through `HandlerApplication` on the owned drivers.
    Handler(HttpHandlerRef),
    /// The worker's connection factory of an application that `HttpServer::start_owned` hosts.
    Owned(Rc<dyn OwnedConnectionFactory>),
}

pub(crate) async fn accept_session(
    context: WorkerContext,
    socket: std::net::TcpStream,
    app: WorkerApp,
    id: ConnectionId,
    config: Rc<AcceptorConfig>,
    draining: Arc<AtomicBool>,
    worker_index: usize,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
) -> Result<AcceptedSession, AcceptFailure> {
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
                return Err(AcceptFailure::Tls(error));
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
    let handler = match app {
        WorkerApp::Handler(handler) => handler,
        WorkerApp::Owned(factory) => {
            return Ok(attach_owned_session(
                transport,
                OwnedSessionAttachment {
                    context: &context,
                    factory: &*factory,
                    id,
                    config: &config,
                    connection,
                    draining: &draining,
                    worker_index,
                    callback_failure: &callback_failure,
                },
            )?);
        }
    };
    Ok(attach_session(
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
    )?)
}

/// Why `accept_session` returned no session.
pub(crate) enum AcceptFailure {
    /// The TLS handshake failed.
    Tls(HttpError),
    /// Anything else ended the connection before its session started.
    Rejected(HttpError),
}

impl AcceptFailure {
    pub(crate) fn outcome(&self) -> ConnectionOutcome<'_> {
        match self {
            Self::Tls(error) => ConnectionOutcome::TlsFailed(error),
            Self::Rejected(error) => ConnectionOutcome::Rejected(error),
        }
    }

    pub(crate) fn into_error(self) -> HttpError {
        match self {
            Self::Tls(error) | Self::Rejected(error) => error,
        }
    }
}

impl From<HttpError> for AcceptFailure {
    fn from(error: HttpError) -> Self {
        Self::Rejected(error)
    }
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
        connection,
        draining,
        transport_limits,
        worker_index,
        callback_failure,
    } = attachment;
    config.session.check_owned().map_err(|message| {
        HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message.as_bytes())
    })?;
    let PreparedSession {
        transport,
        tls_close,
        connection,
        uniplex,
        #[cfg(test)]
        retirement,
    } = prepare_session(
        transport,
        SessionPreparation {
            context,
            config,
            connection,
            transport_limits,
            worker_index,
            callback_failure,
        },
    )?;
    let handler = bind_connection(handler, Arc::clone(&connection.context));
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
            owned_http1_driver_config(&config.session, Http1DriverConfig::default()),
        )?;
        return admit_session(
            context,
            draining,
            connection.protocol,
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
        let control = SessionControl {
            handle: SessionHandle::OwnedHttp2 { handle, finished },
            transport,
            context: context.clone(),
        };
        if config.session.non_abortive_close {
            control.skip_close_notify_before_http2_handshake(tls_close.as_ref());
        }
        return admit_session(
            context,
            draining,
            connection.protocol,
            ServerSession::OwnedHttp2(session),
            control,
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
        connection.protocol,
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

/// The inputs of the setup that every accepted session shares.
struct SessionPreparation<'a> {
    context: &'a WorkerContext,
    config: &'a AcceptorConfig,
    connection: ConnectionInfo,
    transport_limits: Option<ByteStreamLimits>,
    worker_index: usize,
    callback_failure: &'a Arc<Mutex<Option<CallbackFailure>>>,
}

/// An admitted connection's transport and session I/O configuration.
struct PreparedSession {
    transport: Rc<dyn UniplexTransport>,
    /// How a TLS connection's graceful close ends; `None` for plaintext.
    tls_close: Option<TlsCloseControl>,
    connection: ConnectionInfo,
    uniplex: UniplexConfig,
    #[cfg(test)]
    retirement: Option<TransportRetirement>,
}

/// Admits the connection and prepares its transport: the admission and filter checks, socket
/// options and the connection observer's `on_accepted`.
fn prepare_session(
    transport: AcceptedTransport,
    preparation: SessionPreparation<'_>,
) -> Result<PreparedSession, HttpError> {
    let SessionPreparation {
        context,
        config,
        mut connection,
        transport_limits,
        worker_index,
        callback_failure,
    } = preparation;
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
            AcceptedTransport::Tls(transport) => transport.enable_non_abortive_close(context)?,
            AcceptedTransport::ByteStream(_) => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"non_abortive_close does not support a TLS layer that Roxygen does not own",
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
    #[cfg(test)]
    let retirement = match &transport {
        AcceptedTransport::ByteStream(transport) => transport.retirement(),
        AcceptedTransport::Tls(transport) => transport.retirement(),
    };
    let tls_close = match &transport {
        AcceptedTransport::ByteStream(_) => None,
        AcceptedTransport::Tls(transport) => Some(transport.close_control()),
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
        exact_method_tokens: config.session.exact_method_tokens,
        verbatim_http2_header_values: config.session.verbatim_http2_header_values,
        add_date_header_to_response: config.session.add_date_header_to_response,
    };
    if let Some(limits) = transport_limits {
        uniplex.maximum_read_size = uniplex.maximum_read_size.min(limits.maximum_read_size());
        uniplex.read_allocation_size = uniplex.read_allocation_size.min(limits.maximum_read_size());
        uniplex.maximum_write_size = uniplex.maximum_write_size.min(limits.maximum_write_size());
    }
    Ok(PreparedSession {
        transport,
        tls_close,
        connection,
        uniplex,
        #[cfg(test)]
        retirement,
    })
}

/// The inputs of an accepted connection that a hosted application serves.
pub(crate) struct OwnedSessionAttachment<'a> {
    pub context: &'a WorkerContext,
    pub factory: &'a dyn OwnedConnectionFactory,
    pub id: ConnectionId,
    pub config: &'a AcceptorConfig,
    pub connection: ConnectionInfo,
    pub draining: &'a AtomicBool,
    pub worker_index: usize,
    pub callback_failure: &'a Arc<Mutex<Option<CallbackFailure>>>,
}

/// Prepares an accepted connection as `attach_session` does, then hands it to the hosted
/// application's `attach`, and admits the session that `attach` returns.
pub(crate) fn attach_owned_session(
    transport: AcceptedTransport,
    attachment: OwnedSessionAttachment<'_>,
) -> Result<AcceptedSession, HttpError> {
    let OwnedSessionAttachment {
        context,
        factory,
        id,
        config,
        connection,
        draining,
        worker_index,
        callback_failure,
    } = attachment;
    let PreparedSession {
        transport,
        tls_close,
        connection,
        uniplex,
        #[cfg(test)]
        retirement,
    } = prepare_session(
        transport,
        SessionPreparation {
            context,
            config,
            connection,
            transport_limits: None,
            worker_index,
            callback_failure,
        },
    )?;
    let io = UniplexConfig {
        drain_request_body_on_drop: true,
        ..uniplex
    };
    let attach = OwnedConnectionAttach::new(
        context,
        worker_index,
        id,
        &connection,
        &config.session,
        transport,
        io,
    )
    .with_tls_close(tls_close.clone());
    let (session, control) = invoke_accept_callback(
        callback_failure,
        worker_index,
        ServerCallbackStage::HandlerFactory,
        || factory.attach(attach),
    )??
    .into_parts();
    if config.session.non_abortive_close {
        control.skip_close_notify_before_http2_handshake(tls_close.as_ref());
    }
    admit_session(
        context,
        Some(draining),
        connection.protocol,
        session,
        control,
        #[cfg(test)]
        retirement,
    )
}

/// A hosted application's HTTP/1.1 session on `transport`, with the driver configuration
/// `http1` and the fields that `config` overrides.
pub(crate) fn hosted_http1_session<A: OwnedApplication>(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    io: UniplexConfig,
    config: &ServerSessionConfig,
    http1: Http1DriverConfig,
    make: impl FnOnce(&OwnedH1Handle) -> A,
) -> Result<(ServerSession, SessionControl), HttpError> {
    let (session, handle, finished) = OwnedHttp1Session::new_with(
        context,
        OwnedH1Options {
            context: context.clone(),
            transport: Rc::clone(&transport),
            io,
            http1: owned_http1_driver_config(config, http1),
        },
        std::future::ready(()),
        make,
    )?;
    Ok((
        ServerSession::Hosted(Box::new(session)),
        SessionControl {
            handle: SessionHandle::OwnedHttp1 { handle, finished },
            transport,
            context: context.clone(),
        },
    ))
}

/// A hosted application's HTTP/2 session on `transport`, with the configuration that the
/// built-in owned HTTP/2 path derives from `config`.
pub(crate) fn hosted_http2_session<A: OwnedApplication>(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    io: UniplexConfig,
    config: &ServerSessionConfig,
    make: impl FnOnce(&OwnedH2Handle, &Rc<Cell<bool>>) -> A,
) -> Result<(ServerSession, SessionControl), HttpError> {
    let (session, handle, finished) = OwnedHttp2Session::new_with(
        context,
        OwnedH2Options {
            context: context.clone(),
            transport: Rc::clone(&transport),
            io,
            http2: owned_http2_session_config(config),
        },
        std::future::ready(()),
        make,
    )?;
    Ok((
        ServerSession::Hosted(Box::new(session)),
        SessionControl {
            handle: SessionHandle::OwnedHttp2 { handle, finished },
            transport,
            context: context.clone(),
        },
    ))
}

fn admit_session(
    context: &WorkerContext,
    draining: Option<&AtomicBool>,
    protocol: CodecProtocol,
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
        protocol,
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
        abort_streams_on_eof: config.abort_http2_streams_on_eof,
        ..Http2SessionConfig::default()
    }
}

/// The owned driver's HTTP/2 configuration. It differs from `http2_session_config` in two
/// ways. Stats and ping probes are `None`, because the driver records no statistics. The
/// driver also requires its stream receive window to equal the advertised
/// `INITIAL_WINDOW_SIZE`, so when `settings` sets it more than once, the window is the last
/// value rather than the first. When `settings` does not set it, the window stays
/// `stream_flow_control`, which the driver advertises itself.
pub(crate) fn owned_http2_session_config(config: &ServerSessionConfig) -> Http2SessionConfig {
    let mut http2 = http2_session_config(config);
    if let Some(advertised) = http2
        .settings
        .iter()
        .rev()
        .find(|setting| setting.identifier == 4)
        .and_then(|setting| u32::try_from(setting.value).ok())
    {
        http2.stream_receive_window = advertised;
    }
    Http2SessionConfig {
        stats: None,
        ping_probes: None,
        deadlines: config.owned_deadlines,
        ..http2
    }
}

/// `http1` with each field that has a `ServerSessionConfig` twin taken from `config`: the
/// connection deadlines.
pub(crate) fn owned_http1_driver_config(
    config: &ServerSessionConfig,
    http1: Http1DriverConfig,
) -> Http1DriverConfig {
    Http1DriverConfig {
        deadlines: config.owned_deadlines,
        ..http1
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
