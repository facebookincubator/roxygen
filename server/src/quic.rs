/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! HTTP/3 on [`HttpServer`](crate::server::HttpServer): its optional QUIC listener, on Linux
//! with the `http3` feature.
//!
//! The server binds one `SO_REUSEPORT` UDP socket per worker for each QUIC address before any
//! worker starts. Each worker runs an mvfstr endpoint driver on its sockets and serves every
//! connection with a `roxygen_h3` session that hosts the worker's application: the worker's
//! `HttpHandler`, or an owned application's connection through
//! [`OwnedConnectionFactory::attach_http3`](crate::OwnedConnectionFactory::attach_http3). With
//! steering, the kernel delivers each datagram to the worker its connection ID names and the
//! drivers of an address hand new connections round the workers in turn.
//!
//! QUIC connections go through the connection filter and both connection observers as TCP ones do,
//! but these TCP features do not apply to them: [`AcceptStats`](crate::AcceptStats),
//! `stop_accepting`, TLS credential reloads, `tls_observer`, zero copy, the timeouts and options
//! of [`ServerSessionConfig`](crate::acceptor::ServerSessionConfig) with its `stats_factory` and
//! `lifecycle_observer_factory`, [`OwnedConnectionControl`](crate::OwnedConnectionControl), and
//! the TLS facts and client certificate identity of the connection and handler contexts.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use mvfstr_driver::Conn;
use mvfstr_driver::ConnInfo;
use mvfstr_driver::EndpointDriver;
use mvfstr_driver::EndpointHandle;
use mvfstr_driver::QuicApplication;
use mvfstr_driver::QuicSession;
use mvfstr_driver::Refusal;
use mvfstr_driver::SendSink;
use mvfstr_driver::Steering;
use mvfstr_io::MmsgBackend;
use mvfstr_io::MmsgOptions;
use mvfstr_io::SocketOptions;
use mvfstr_proto::AppCode;
use mvfstr_proto::CloseReason;
use mvfstr_proto::ConnEvent;
use mvfstr_proto::ConnOptions;
use mvfstr_proto::EndpointConfig;
use mvfstr_proto::Initiator;
use mvfstr_proto::StreamId;
use mvfstr_proto::StreamReady;
use mvfstr_proto::TransportErrorCode;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_h3::H3ApplicationBuilder;
use roxygen_h3::H3Session;
use roxygen_h3::H3Settings;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::uniplex::UniplexRunSummary;
use rustls::NamedGroup;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::time_provider::DefaultTimeProvider;
use socket2::Socket;

use crate::acceptor::AcceptorConfig;
use crate::acceptor::ConnectionId;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::ConnectionLifecycle;
use crate::acceptor::ConnectionLifecycleObserver;
use crate::acceptor::ConnectionOutcome;
use crate::acceptor::HandlerApplication;
use crate::acceptor::WorkerApp;
use crate::acceptor::connection_info;
use crate::acceptor::invoke_accept_callback;
use crate::connection::bind_connection;
use crate::server::CallbackFailure;
use crate::server::HttpServerError;
use crate::server::ServerCallbackStage;

/// H3_NO_ERROR, the HTTP/3 error code of a close without error (RFC 9114).
const H3_NO_ERROR: u32 = 0x100;

/// The ALPN protocol of HTTP/3 (RFC 9114).
const H3_ALPN: &[u8] = b"h3";

/// The QUIC listener of an [`HttpServer`](crate::server::HttpServer), which serves HTTP/3.
pub struct QuicListenerConfig {
    /// The UDP addresses to accept QUIC on. When it is empty, the default, the listener uses the
    /// addresses the TCP listeners bound, so HTTP/3 answers on the same port numbers as HTTP/1.1
    /// and HTTP/2. An address with port 0 gets a port the system picks, the same on every worker.
    pub bind_addresses: Vec<SocketAddr>,
    /// The certificate and TLS settings.
    pub tls: QuicTls,
    /// HTTP/3 limits and the QUIC transport settings of accepted connections.
    pub h3: H3ApplicationBuilder,
    /// Whether a socket bound to an IPv6 address also carries IPv4 (IPV6_V6ONLY=0), so
    /// `[::]:port` serves both families. Default on.
    pub dual_stack: bool,
    /// SO_RCVBUF of each socket in bytes; `None`, the default, keeps the system's.
    pub recv_buffer: Option<usize>,
    /// SO_SNDBUF of each socket in bytes; `None`, the default, keeps the system's.
    pub send_buffer: Option<usize>,
    /// How long a drain lets QUIC connections finish their exchanges before closing them with
    /// NO_ERROR. Default unbounded, as for TCP connections.
    pub drain_grace: Duration,
    /// Whether new connections spread evenly over the workers, when there is more than one. The
    /// kernel then delivers each datagram to the worker its connection ID names, and the worker
    /// that receives a client's first Initial hands it to the next worker in turn. Without it the
    /// kernel picks a worker by a hash of the client's address, which can leave some workers idle
    /// while others serve several connections. Default on. Binding fails with the kernel's error
    /// where it refuses the steering program (SO_ATTACH_REUSEPORT_CBPF, from Linux 4.5), since a
    /// handed-off connection would break without it; turn this off to serve there.
    pub steering: bool,
}

impl QuicListenerConfig {
    /// Creates a listener on the TCP listeners' addresses with `tls` and default settings.
    pub fn new(tls: QuicTls) -> Self {
        Self {
            bind_addresses: Vec::new(),
            tls,
            h3: H3ApplicationBuilder::new(),
            dual_stack: true,
            recv_buffer: None,
            send_buffer: None,
            drain_grace: Duration::MAX,
            steering: true,
        }
    }
}

/// The TLS settings of a QUIC listener.
pub enum QuicTls {
    /// A rustls config built for QUIC, as `mvfstr_tls::server_config` builds one: TLS 1.3 only,
    /// with mvfstr's packet protection. The listener offers ALPN `h3` alone, whatever protocols
    /// the config lists.
    Config(Arc<rustls::ServerConfig>),
    /// A certificate chain and its key. The server builds the config: TLS 1.3, ALPN `h3`, and
    /// the process-default crypto provider, else ring's, with X25519 preferred.
    Certificate {
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    },
}

/// The QUIC listener's sockets, bound before any worker starts, and what every worker's
/// drivers share.
pub(crate) struct QuicBinding {
    /// One socket per address for each worker, in worker order.
    sockets: Vec<Vec<Socket>>,
    addresses: Vec<SocketAddr>,
    shared: Arc<QuicShared>,
}

struct QuicShared {
    server: Arc<mvfstr_proto::ServerConfig>,
    endpoint: Arc<EndpointConfig>,
    h3: H3ApplicationBuilder,
    options: MmsgOptions,
    drain_grace: Duration,
    /// For each address with steering, the group its drivers share; empty without steering.
    steering: Vec<Arc<Steering>>,
    /// A duplicate of every worker's socket, which keeps each socket in its port's SO_REUSEPORT
    /// group until every worker has ended. A socket that left the group would move flows of the
    /// others to sockets whose workers drop the packets of connections they do not serve.
    _group: Vec<Socket>,
}

impl QuicBinding {
    /// Builds the listener's configs and binds its sockets for `workers` workers. `tcp` holds
    /// the addresses the TCP listeners bound.
    pub(crate) fn bind(
        config: QuicListenerConfig,
        tcp: &[SocketAddr],
        workers: usize,
    ) -> Result<Self, HttpServerError> {
        config.h3.validate().map_err(quic_error)?;
        let tls = match config.tls {
            // Every connection runs HTTP/3, so the listener offers `h3` alone.
            QuicTls::Config(tls) => {
                let mut tls = rustls::ServerConfig::clone(&tls);
                tls.alpn_protocols = vec![H3_ALPN.to_vec()];
                Arc::new(tls)
            }
            QuicTls::Certificate { chain, key } => Arc::new(certificate_config(chain, key)?),
        };
        let mut reset_key = [0; 32];
        tls.crypto_provider()
            .secure_random
            .fill(&mut reset_key)
            .map_err(|_| quic_error("the crypto provider has no random source"))?;
        let server = mvfstr_proto::ServerConfig::builder(tls)
            .build()
            .map_err(quic_error)?;
        let endpoint = EndpointConfig::builder(reset_key)
            .build()
            .map_err(quic_error)?;
        let socket = SocketOptions {
            reuse_port: true,
            dual_stack: config.dual_stack,
            recv_buffer: config.recv_buffer,
            send_buffer: config.send_buffer,
            ..SocketOptions::default()
        };
        let requested = if config.bind_addresses.is_empty() {
            tcp
        } else {
            &config.bind_addresses
        };
        let count = u16::try_from(workers).map_err(quic_error)?;
        let steering = config.steering && workers > 1;
        let mut sockets: Vec<Vec<Socket>> = (0..workers).map(|_| Vec::new()).collect();
        let mut group = Vec::with_capacity(workers * requested.len());
        let mut addresses = Vec::with_capacity(requested.len());
        let mut groups = Vec::new();
        for &address in requested {
            // The first socket settles a port the system picks; the others share it.
            let first = mvfstr_io::socket::open(address, None, &socket)?;
            let bound = first.local_addr()?.as_socket().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "a QUIC socket has no IP address",
                )
            })?;
            let mut first = Some(first);
            for worker in &mut sockets {
                let socket = match first.take() {
                    Some(first) => first,
                    None => mvfstr_io::socket::open(bound, None, &socket)?,
                };
                group.push(socket.try_clone()?);
                worker.push(socket);
            }
            // The kernel numbers a group's sockets in the order they bound, and these bound one
            // after another on this thread before any worker started, worker 0's first, so the
            // socket at index `i` is worker `i`'s, as the steering program needs, unless another
            // socket already held the port with SO_REUSEPORT, as one of an overlapping process can
            // on an explicit port. A failed bind fails the whole listener, so a group is never
            // short of a socket, and `_group` keeps each in the group until every worker has ended.
            if steering && let Some(first) = sockets.first().and_then(|worker| worker.last()) {
                mvfstr_io::socket::steer_by_connection_id(first, count)?;
                groups.push(Steering::new(count));
            }
            addresses.push(bound);
        }
        Ok(Self {
            sockets,
            addresses,
            shared: Arc::new(QuicShared {
                server,
                endpoint,
                h3: config.h3,
                options: MmsgOptions {
                    socket,
                    ..MmsgOptions::default()
                },
                drain_grace: config.drain_grace,
                steering: groups,
                _group: group,
            }),
        })
    }

    /// Returns the addresses the sockets bound, in the order they were asked for.
    pub(crate) fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    /// Splits the binding into each worker's sockets and configs, in worker order.
    pub(crate) fn into_workers(self) -> impl Iterator<Item = WorkerQuicConfig> {
        let count = self.sockets.len();
        let shared = self.shared;
        self.sockets
            .into_iter()
            .map(move |sockets| WorkerQuicConfig {
                sockets,
                shared: Arc::clone(&shared),
                count,
            })
    }
}

fn quic_error(error: impl std::fmt::Display) -> HttpServerError {
    HttpServerError::Quic(error.to_string())
}

/// Builds the TLS config for a certificate: TLS 1.3, ALPN `h3`, and the process-default crypto
/// provider, else ring's, with X25519 first among its key exchange groups.
fn certificate_config(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<rustls::ServerConfig, HttpServerError> {
    let mut provider = CryptoProvider::get_default()
        .map(|provider| CryptoProvider::clone(provider))
        .unwrap_or_else(rustls::crypto::ring::default_provider);
    provider
        .kx_groups
        .sort_by_key(|group| group.name() != NamedGroup::X25519);
    mvfstr_tls::server_config(
        provider,
        Arc::new(DefaultTimeProvider),
        chain,
        key,
        &[H3_ALPN],
    )
    .map_err(quic_error)
}

/// One worker's part of the QUIC listener.
pub(crate) struct WorkerQuicConfig {
    sockets: Vec<Socket>,
    shared: Arc<QuicShared>,
    /// The number of workers.
    count: usize,
}

/// What a worker's QUIC drivers take from the worker.
pub(crate) struct WorkerQuicInputs<'a> {
    pub context: &'a WorkerContext,
    pub index: usize,
    pub app: &'a WorkerApp,
    pub acceptor: &'a Rc<AcceptorConfig>,
    pub lifecycle_observer: Option<Arc<dyn ConnectionLifecycleObserver>>,
    pub callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    /// The worker's connection serial numbers, shared with its TCP connections.
    pub serial: Rc<Cell<u64>>,
}

/// A worker's QUIC drivers: one per address.
pub(crate) struct WorkerQuic {
    drivers: Vec<EndpointHandle<ServerQuicApp, MmsgBackend>>,
    tracker: Rc<Tracker>,
    /// The shared configs and the socket group, held until the worker ends.
    shared: Arc<QuicShared>,
}

impl WorkerQuic {
    /// Starts the worker's drivers on its sockets. Runs on the worker.
    pub(crate) fn start(
        config: WorkerQuicConfig,
        inputs: WorkerQuicInputs<'_>,
    ) -> Result<Self, String> {
        let settings = config
            .shared
            .h3
            .clone()
            .build_settings(inputs.context.host().clone())
            .map_err(|error| error.to_string())?;
        let index = u16::try_from(inputs.index).map_err(|error| error.to_string())?;
        let count = u16::try_from(config.count).map_err(|error| error.to_string())?;
        let tracker = Rc::new(Tracker::default());
        let worker = Rc::new(Worker {
            context: inputs.context.clone(),
            index: inputs.index,
            settings,
            app: inputs.app.clone(),
            acceptor: Rc::clone(inputs.acceptor),
            lifecycle_observer: inputs.lifecycle_observer,
            callback_failure: inputs.callback_failure,
            serial: inputs.serial,
        });
        let mut drivers = Vec::with_capacity(config.sockets.len());
        for (address, socket) in config.sockets.into_iter().enumerate() {
            let backend = MmsgBackend::from_socket(socket, &config.shared.options)
                .map_err(|error| error.to_string())?;
            let local = backend.local_addr().map_err(|error| error.to_string())?;
            let app = ServerQuicApp {
                worker: Rc::clone(&worker),
                local,
                _driver: tracker.driver(),
            };
            let mut builder =
                EndpointDriver::<_, MmsgBackend>::builder(Arc::clone(&config.shared.endpoint), app)
                    .serve(backend, Arc::clone(&config.shared.server))
                    .worker(index, count)
                    .backend_options(config.shared.options.clone());
            if let Some(group) = config.shared.steering.get(address) {
                builder = builder.steering(Arc::clone(group));
            }
            let handle = builder
                .spawn(inputs.context)
                .map_err(|error| error.to_string())?;
            drivers.push(handle);
        }
        Ok(Self {
            drivers,
            tracker,
            shared: config.shared,
        })
    }

    /// Quiesces every driver: the drivers stop accepting connections, every session sends GOAWAY,
    /// and connections close as their exchanges end, or at the drain grace.
    pub(crate) fn drain(&self) {
        for driver in &self.drivers {
            driver.quiesce(self.shared.drain_grace);
        }
    }

    /// Ends every driver at once, dropping its connections without a close.
    pub(crate) fn abort(&self) {
        for driver in &self.drivers {
            driver.abort();
        }
    }

    /// Resolves once every driver has ended and dropped its sessions.
    pub(crate) fn finished(&self) -> impl Future<Output = ()> + '_ {
        std::future::poll_fn(|cx| self.tracker.poll_finished(cx))
    }
}

/// The number of a worker's running drivers, and the task waiting for it to reach zero.
#[derive(Default)]
struct Tracker {
    drivers: Cell<usize>,
    waker: RefCell<Option<Waker>>,
}

impl Tracker {
    fn driver(self: &Rc<Self>) -> DriverGuard {
        self.drivers.set(self.drivers.get() + 1);
        DriverGuard(Rc::clone(self))
    }

    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.drivers.get() == 0 {
            return Poll::Ready(());
        }
        *self.waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// A driver's share of the tracker's count, held by its application, which the driver drops with
/// its sessions when it ends.
struct DriverGuard(Rc<Tracker>);

impl Drop for DriverGuard {
    fn drop(&mut self) {
        let tracker = &self.0;
        tracker.drivers.set(tracker.drivers.get() - 1);
        if tracker.drivers.get() == 0
            && let Some(waker) = tracker.waker.borrow_mut().take()
        {
            waker.wake();
        }
    }
}

/// What a worker's drivers serve their connections with.
struct Worker {
    context: WorkerContext,
    index: usize,
    settings: H3Settings,
    app: WorkerApp,
    acceptor: Rc<AcceptorConfig>,
    lifecycle_observer: Option<Arc<dyn ConnectionLifecycleObserver>>,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    serial: Rc<Cell<u64>>,
}

/// The application of one driver: it hosts the worker's application on a `roxygen_h3` session
/// on each connection.
pub(crate) struct ServerQuicApp {
    worker: Rc<Worker>,
    /// The driver's socket address.
    local: SocketAddr,
    _driver: DriverGuard,
}

impl ServerQuicApp {
    fn admit(
        &self,
        id: ConnectionId,
        info: &ConnInfo,
        options: &mut ConnOptions,
    ) -> Result<Hosted, HttpError> {
        let worker = &*self.worker;
        let local = SocketAddr::new(
            info.local_ip.unwrap_or_else(|| self.local.ip()),
            self.local.port(),
        );
        let context = Arc::new(
            ConnectionContext::new(
                TransportEndpoint::Socket(socket_address(local)),
                TransportEndpoint::Socket(socket_address(info.remote)),
                TransportSecurity::Tls,
                None,
            )
            .with_http_protocol(CodecProtocol::Http3),
        );
        let connection = connection_info(CodecProtocol::Http3, context);
        if let Some(filter) = worker.acceptor.connection_filter.as_ref() {
            let accepted =
                catch_unwind(AssertUnwindSafe(|| filter.accept(&connection))).unwrap_or(false);
            if !accepted {
                return Err(HttpError::with_message(
                    HttpErrorCode::CONNECT_ERROR,
                    b"connection rejected by server admission filter",
                ));
            }
        }
        if let Some(observer) = worker.acceptor.connection_observer.as_ref() {
            invoke_accept_callback(
                &worker.callback_failure,
                worker.index,
                ServerCallbackStage::AcceptedConnectionObserver,
                || observer.on_accepted(&connection),
            )?;
        }
        let hosted = match &worker.app {
            WorkerApp::Handler(handler) => handler_session(worker, handler, &connection, options)?,
            WorkerApp::Owned(factory) => {
                let attach = Http3ConnectionAttach {
                    worker: &worker.context,
                    worker_index: worker.index,
                    id,
                    info: &connection,
                    settings: &worker.settings,
                    options,
                };
                let attached = invoke_accept_callback(
                    &worker.callback_failure,
                    worker.index,
                    ServerCallbackStage::HandlerFactory,
                    || factory.attach_http3(attach),
                )??;
                Hosted {
                    session: attached.session,
                    cleanup: None,
                }
            }
        };
        if let Some(observer) = worker.acceptor.connection_observer.as_ref() {
            invoke_accept_callback(
                &worker.callback_failure,
                worker.index,
                ServerCallbackStage::AcceptedConnectionObserver,
                || observer.on_registered(),
            )?;
        }
        Ok(hosted)
    }
}

impl QuicApplication for ServerQuicApp {
    type Session = ServerQuicSession;

    fn accept(
        &mut self,
        info: &ConnInfo,
        options: &mut ConnOptions,
    ) -> Result<ServerQuicSession, Refusal> {
        let worker = &*self.worker;
        let serial = worker.serial.get();
        worker.serial.set(serial.wrapping_add(1));
        let id = ConnectionId {
            worker: u32::try_from(worker.index).unwrap_or(u32::MAX),
            serial,
        };
        let lifecycle = worker.lifecycle_observer.clone().map(|observer| {
            ConnectionLifecycle::accepted(
                observer,
                id,
                worker.index,
                Arc::clone(&worker.callback_failure),
            )
        });
        match self.admit(id, info, options) {
            Ok(Hosted { session, cleanup }) => Ok(ServerQuicSession {
                session,
                _cleanup: cleanup,
                lifecycle,
                established: false,
                closed: None,
            }),
            Err(error) => {
                if let Some(lifecycle) = lifecycle {
                    lifecycle.close(&ConnectionOutcome::Rejected(&error));
                }
                Err(Refusal::Refuse)
            }
        }
    }
}

/// A session the server hosts, and the worker cleanup that retires its application.
struct Hosted {
    session: Box<dyn HostedH3>,
    cleanup: Option<WorkerCleanupTaskRegistration>,
}

/// A `roxygen_h3` session of any owned application.
trait HostedH3: QuicSession {
    fn handshake_complete(&self) -> bool;
}

impl<A: OwnedApplication> HostedH3 for H3Session<A> {
    fn handshake_complete(&self) -> bool {
        H3Session::handshake_complete(self)
    }
}

/// Creates a session that serves the worker's handler. Its handler contexts are finalized by a
/// worker cleanup task, which dropping the session activates.
fn handler_session(
    worker: &Worker,
    handler: &HttpHandlerRef,
    connection: &ConnectionInfo,
    options: &mut ConnOptions,
) -> Result<Hosted, HttpError> {
    let failure = |error: &dyn std::fmt::Display| {
        HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
    };
    let mut owner = KeepAliveOwner::new_for_worker((), worker.context.handle())
        .map_err(|error| failure(&error))?;
    let base = HandlerSessionContext::acquire_with_peer_identity(&owner, None)
        .map_err(|error| failure(&error))?;
    let cleanup = worker
        .context
        .register_cleanup_task(async move { owner.finalize().await })
        .map_err(|error| failure(&error))?;
    let handler = bind_connection(Rc::clone(handler), Arc::clone(&connection.context));
    let session = worker
        .settings
        .session(HandlerApplication::new(handler, base), options);
    Ok(Hosted {
        session: Box::new(session),
        cleanup: Some(cleanup),
    })
}

/// One QUIC connection's session, with its lifecycle reports.
pub(crate) struct ServerQuicSession {
    // Declared first, so the session releases its application before `_cleanup` activates
    // the application's finalization.
    session: Box<dyn HostedH3>,
    _cleanup: Option<WorkerCleanupTaskRegistration>,
    lifecycle: Option<ConnectionLifecycle>,
    established: bool,
    /// How the connection closed, once it has.
    closed: Option<Closed>,
}

impl ServerQuicSession {
    /// Reports the established protocol after the callback in which the peer's SETTINGS
    /// arrived.
    fn observe(&mut self) {
        if !self.established && self.session.handshake_complete() {
            self.established = true;
            if let Some(lifecycle) = self.lifecycle.as_mut() {
                lifecycle.established(CodecProtocol::Http3);
            }
        }
    }
}

impl QuicSession for ServerQuicSession {
    fn on_event(&mut self, conn: &mut Conn<'_>, event: ConnEvent) {
        if let ConnEvent::Closed { reason, initiator } = &event {
            self.closed = Some(Closed::new(reason, *initiator));
        }
        self.session.on_event(conn, event);
        self.observe();
    }

    fn on_stream(&mut self, conn: &mut Conn<'_>, id: StreamId, ready: StreamReady) {
        self.session.on_stream(conn, id, ready);
        self.observe();
    }

    fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        id: StreamId,
        sink: &mut SendSink<'_>,
    ) -> Poll<()> {
        self.session.poll_send(cx, id, sink)
    }

    fn poll_session(&mut self, cx: &mut Context<'_>, conn: &mut Conn<'_>) -> Poll<()> {
        let poll = self.session.poll_session(cx, conn);
        self.observe();
        poll
    }

    fn deadline(&self) -> Option<Instant> {
        self.session.deadline()
    }

    fn on_quiesce(&mut self, conn: &mut Conn<'_>) {
        self.session.on_quiesce(conn);
    }
}

impl Drop for ServerQuicSession {
    fn drop(&mut self) {
        // A session dropped without a close, by an abort, reports `Aborted` as its lifecycle
        // drops.
        let (Some(lifecycle), Some(closed)) = (self.lifecycle.take(), self.closed.take()) else {
            return;
        };
        match &closed {
            Closed::TlsFailed(error) => lifecycle.close(&ConnectionOutcome::TlsFailed(error)),
            Closed::Ran(result) => lifecycle.close(&ConnectionOutcome::Closed {
                protocol: CodecProtocol::Http3,
                handshake_complete: self.established,
                result,
            }),
        }
    }
}

/// How a closed connection ended, as its lifecycle reports it.
enum Closed {
    /// A TLS alert, carried as a CRYPTO_ERROR (RFC 9001), closed it, as when a handshake fails.
    TlsFailed(HttpError),
    /// Its session ran and ended with this result: success for a close without error.
    Ran(Result<UniplexRunSummary, HttpError>),
}

impl Closed {
    fn new(reason: &CloseReason, initiator: Initiator) -> Self {
        let error =
            |code| HttpError::with_message(code, format!("the QUIC connection closed: {reason:?}"));
        let code = match reason {
            CloseReason::Transport { code, .. } if *code == TransportErrorCode::NO_ERROR => {
                return Self::Ran(Ok(UniplexRunSummary::default()));
            }
            // TCP reports a TLS protocol failure as INADEQUATE_SECURITY too.
            CloseReason::Transport { code, .. } if code.crypto_alert().is_some() => {
                return Self::TlsFailed(error(HttpErrorCode::INADEQUATE_SECURITY));
            }
            CloseReason::Application { code, .. } if *code == AppCode::from_u32(H3_NO_ERROR) => {
                return Self::Ran(Ok(UniplexRunSummary::default()));
            }
            CloseReason::Silent if initiator == Initiator::IdleTimeout => {
                HttpErrorCode::READ_TIMEOUT
            }
            _ => HttpErrorCode::PROTOCOL_ERROR,
        };
        Self::Ran(Err(error(code)))
    }
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    let ip = match address.ip() {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4),
        ip => ip,
    };
    SocketAddress::new(ip.to_string(), address.port())
}

/// An accepted HTTP/3 connection, for
/// [`OwnedConnectionFactory::attach_http3`](crate::OwnedConnectionFactory::attach_http3) to
/// serve.
pub struct Http3ConnectionAttach<'a> {
    worker: &'a WorkerContext,
    worker_index: usize,
    id: ConnectionId,
    info: &'a ConnectionInfo,
    settings: &'a H3Settings,
    options: &'a mut ConnOptions,
}

impl Http3ConnectionAttach<'_> {
    /// Returns the worker that serves the connection.
    pub fn worker(&self) -> &WorkerContext {
        self.worker
    }

    /// Returns the index of the worker that serves the connection.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// Returns the connection's identity, as `ConnectionLifecycleObserver` reports it.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the connection's addresses and protocol.
    pub fn info(&self) -> &ConnectionInfo {
        self.info
    }

    /// Returns the connection's metadata.
    pub fn context(&self) -> &Arc<ConnectionContext> {
        &self.info.context
    }

    /// Serves the connection with a `roxygen_h3` session that hosts `application`, with the
    /// listener's HTTP/3 settings.
    pub fn serve<A: OwnedApplication>(self, application: A) -> Http3Attached {
        Http3Attached {
            session: Box::new(self.settings.session(application, self.options)),
        }
    }
}

/// A connection that [`Http3ConnectionAttach::serve`] serves, for
/// [`OwnedConnectionFactory::attach_http3`](crate::OwnedConnectionFactory::attach_http3) to
/// return.
pub struct Http3Attached {
    session: Box<dyn HostedH3>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::mem::MaybeUninit;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::net::UdpSocket;
    use std::os::fd::AsRawFd;

    use bytes::Bytes;
    use socket2::Domain;
    use socket2::Type;

    use super::*;
    use crate::h3_client::localhost_certificate;

    fn listener(address: SocketAddr) -> QuicListenerConfig {
        let (chain, key) = localhost_certificate();
        QuicListenerConfig {
            bind_addresses: vec![address],
            ..QuicListenerConfig::new(QuicTls::Certificate { chain, key })
        }
    }

    #[test]
    fn a_listener_without_dual_stack_binds_ipv6_only_sockets() {
        let config = QuicListenerConfig {
            dual_stack: false,
            ..listener(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))
        };
        let binding = QuicBinding::bind(config, &[], 2).expect("the listener binds");
        for socket in binding.sockets.iter().flatten() {
            assert!(
                socket.only_v6().expect("IPV6_V6ONLY reads"),
                "the socket carries IPv6 only"
            );
        }
    }

    #[test]
    fn the_listener_sets_the_socket_buffer_sizes() {
        let (recv, send) = (48 << 10, 40 << 10);
        let config = QuicListenerConfig {
            recv_buffer: Some(recv),
            send_buffer: Some(send),
            ..listener(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        };
        let binding = QuicBinding::bind(config, &[], 2).expect("the listener binds");
        // The kernel reports the sizes it settled on, so a socket given the same sizes is the
        // reference.
        let reference = Socket::new(Domain::IPV4, Type::DGRAM, None).expect("a UDP socket");
        reference.set_recv_buffer_size(recv).expect("SO_RCVBUF");
        reference.set_send_buffer_size(send).expect("SO_SNDBUF");
        let sizes = |socket: &Socket| {
            (
                socket.recv_buffer_size().expect("SO_RCVBUF reads"),
                socket.send_buffer_size().expect("SO_SNDBUF reads"),
            )
        };
        for socket in binding.sockets.iter().flatten() {
            assert_eq!(sizes(socket), sizes(&reference), "SO_RCVBUF and SO_SNDBUF");
        }
    }

    /// Sends `datagram` to `addr` from a socket of its own, so each call is a new flow for the
    /// kernel's hash, and returns the index of the socket in `sockets` that received it.
    fn receiver(sockets: &[&Socket], addr: SocketAddr, datagram: &[u8]) -> usize {
        let sender = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a socket");
        sender
            .send_to(datagram, addr)
            .expect("the datagram goes out");
        let mut ready: Vec<libc::pollfd> = sockets
            .iter()
            .map(|socket| libc::pollfd {
                fd: socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            })
            .collect();
        // SAFETY: `ready` is a live array of `pollfd`s of the length passed; the timeout only
        // guards against a hang.
        let n = unsafe { libc::poll(ready.as_mut_ptr(), ready.len() as libc::nfds_t, 20_000) };
        assert_eq!(n, 1, "exactly one socket receives the datagram");
        let index = ready
            .iter()
            .position(|fd| fd.revents & libc::POLLIN != 0)
            .expect("a readable socket");
        let mut buf = [MaybeUninit::uninit(); 2048];
        sockets[index].recv(&mut buf).expect("the datagram reads");
        index
    }

    /// Builds a short header whose destination connection ID starts with `first`.
    fn short_header(first: u8) -> Vec<u8> {
        let mut datagram = vec![0x40, first];
        datagram.resize(30, 0xee);
        datagram
    }

    #[test]
    fn each_workers_socket_joins_its_group_in_worker_order_and_gets_its_connection_ids() {
        const WORKERS: usize = 4;
        let config = listener(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
        let binding = QuicBinding::bind(config, &[], WORKERS).expect("the listener binds");
        let addr = binding.addresses()[0];
        let sockets: Vec<&Socket> = binding.sockets.iter().map(|worker| &worker[0]).collect();
        // Each from a socket of its own, so the kernel's hash alone would scatter them.
        for first in 0..32 {
            assert_eq!(
                receiver(&sockets, addr, &short_header(first)),
                usize::from(first) % WORKERS,
                "a connection ID that starts with {first}"
            );
        }
    }

    #[test]
    fn a_listener_without_steering_leaves_the_choice_of_worker_to_the_kernels_hash() {
        const WORKERS: usize = 4;
        let config = QuicListenerConfig {
            steering: false,
            ..listener(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        };
        let binding = QuicBinding::bind(config, &[], WORKERS).expect("the listener binds");
        let addr = binding.addresses()[0];
        let sockets: Vec<&Socket> = binding.sockets.iter().map(|worker| &worker[0]).collect();
        // Steered, all would reach worker 0; hashed, 16 flows all landing on one socket of 4 has
        // odds of about 1 in 10^9.
        let receivers: BTreeSet<usize> = (0..16)
            .map(|_| receiver(&sockets, addr, &short_header(0)))
            .collect();
        assert!(receivers.len() > 1, "every datagram reached {receivers:?}");
    }

    #[test]
    fn a_closed_connection_reports_how_it_closed() {
        let transport = |code| CloseReason::Transport {
            code,
            frame_type: None,
            reason: Bytes::new(),
        };
        let application = |code| CloseReason::Application {
            code: AppCode::from_u32(code),
            reason: Bytes::new(),
        };
        let cases = [
            (
                transport(TransportErrorCode::NO_ERROR),
                Initiator::Peer,
                ("ran", None),
            ),
            (application(H3_NO_ERROR), Initiator::Local, ("ran", None)),
            (
                CloseReason::Silent,
                Initiator::IdleTimeout,
                ("ran", Some(HttpErrorCode::READ_TIMEOUT)),
            ),
            (
                transport(TransportErrorCode::crypto(40)),
                Initiator::Peer,
                ("tls failed", Some(HttpErrorCode::INADEQUATE_SECURITY)),
            ),
            (
                transport(TransportErrorCode::FLOW_CONTROL_ERROR),
                Initiator::Local,
                ("ran", Some(HttpErrorCode::PROTOCOL_ERROR)),
            ),
            // H3_GENERAL_PROTOCOL_ERROR.
            (
                application(0x101),
                Initiator::Peer,
                ("ran", Some(HttpErrorCode::PROTOCOL_ERROR)),
            ),
            (
                CloseReason::Silent,
                Initiator::StatelessReset,
                ("ran", Some(HttpErrorCode::PROTOCOL_ERROR)),
            ),
        ];
        for (reason, initiator, expected) in cases {
            let outcome = match Closed::new(&reason, initiator) {
                Closed::TlsFailed(error) => ("tls failed", Some(error.code())),
                Closed::Ran(result) => ("ran", result.err().map(|error| error.code())),
            };
            assert_eq!(outcome, expected, "{reason:?} by {initiator:?}");
        }
    }
}
