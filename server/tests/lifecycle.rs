/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_codec::CodecSetting;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::filter_factory::FilterFactory;
use roxygen_core::filter_factory::FilterPair;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_filter::HttpSourceFilter;
use roxygen_core::source_filter::HttpSourceFilterHooks;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::rate_limit::RateLimitConfig;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexSession;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::TlsTransport;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use thiserror::Error;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::AcceptDistribution;
use crate::ConnectionId;
use crate::ConnectionLifecycleObserver;
use crate::ConnectionOutcome;
use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::TlsHandshakeObserver;
use crate::acceptor::http2_session_config;
use crate::acceptor::owned_http2_session_config;
use crate::scoped_server::ScopedHttpServer;
use crate::server::ADMISSION_EVENT_LIMIT;
use crate::server::AdmissionAttempt;
use crate::server::AdmissionEvent;
use crate::server::AdmissionJoinOutcome;
use crate::server::AdmissionProbe;
use crate::server::AdmissionProgress;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::HttpServerError;
use crate::server::HttpServerState;
use crate::server::SESSION_RETIREMENT_EVENT_LIMIT;
use crate::server::ServerCallbackStage;
use crate::server::ServerConfig;
use crate::server::ServerFilterFactory;
use crate::server::ServerObserver;
use crate::server::SessionReaped;
use crate::server::SessionRetirementEvent;
use crate::server::SessionRetirementKey;
use crate::server::SessionRetirementProbe;
use crate::server::SessionRetirementRecord;
use crate::server::SessionRunGate;
use crate::server::SessionRunOutcome;
use crate::server::TlsCredentialProvider;
use crate::server::accept_retry_delay;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Default)]
struct HandlerStats {
    requests: AtomicUsize,
    request_body_bytes: AtomicUsize,
    connections: Mutex<Vec<Arc<ConnectionContext>>>,
}

struct TestHandler {
    stats: Arc<HandlerStats>,
    fail: bool,
    response_body: Bytes,
}

impl HttpHandler for TestHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(connection) = context.connection_context() {
            self.stats
                .connections
                .lock()
                .unwrap()
                .push(Arc::clone(connection));
        }
        let stats = Arc::clone(&self.stats);
        let fail = self.fail;
        let response_body = self.response_body.clone();
        Box::pin(async move {
            let headers = request.read_header_event().await?;
            if !headers.eom() {
                loop {
                    let body = request.read_body_event(u32::MAX).await?;
                    if let Some(data) = body.as_body() {
                        stats
                            .request_body_bytes
                            .fetch_add(data.chain_length(), Ordering::Relaxed);
                    }
                    if body.eom() {
                        break;
                    }
                }
            }
            if fail {
                return Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"test handler failure",
                ));
            }
            let body = BufQueue::from_bytes(response_body.clone(), response_body.len())
                .expect("fixed response body is bounded");
            let response = HttpFixedSource::make_fixed_response(200, Some(body))
                .expect("status and body are valid");
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

fn handler_factory(stats: Arc<HandlerStats>, fail: bool) -> HandlerFactory {
    handler_factory_with_body(stats, fail, Bytes::from_static(b"OK"))
}

fn handler_factory_with_body(
    stats: Arc<HandlerStats>,
    fail: bool,
    response_body: Bytes,
) -> HandlerFactory {
    Arc::new(move |_| {
        Rc::new(TestHandler {
            stats: Arc::clone(&stats),
            fail,
            response_body: response_body.clone(),
        }) as HttpHandlerRef
    })
}

fn plain_request(address: SocketAddr, request: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT)?;
    stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    stream.set_write_timeout(Some(REQUEST_TIMEOUT))?;
    stream.write_all(request)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    Ok(response)
}

fn get_request(address: SocketAddr) -> Vec<u8> {
    plain_request(
        address,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap()
}

fn assert_ok_response(response: &[u8]) {
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"\r\n\r\nOK") || response.ends_with(b"\r\n2\r\nOK\r\n0\r\n\r\n"));
}

fn generated_tls() -> (TlsServerOptions, Vec<u8>) {
    generated_tls_with_alpn([b"http/1.1".as_slice()])
}

fn generated_tls_with_alpn<'a>(
    protocols: impl IntoIterator<Item = &'a [u8]>,
) -> (TlsServerOptions, Vec<u8>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        protocols
            .into_iter()
            .map(|protocol| protocol.to_vec().into_boxed_slice())
            .collect(),
    )
    .unwrap();
    (options, certificate)
}

fn h2_request(address: SocketAddr, certificate: Option<Vec<u8>>) -> Vec<u8> {
    let worker = ManagedWorker::new(8).expect("H2 client worker starts");
    let call = worker
        .handle()
        .call_local(move |context| async move {
            let limits = ByteStreamLimits::new(256 * 1024, 256 * 1024, 1_024)
                .expect("H2 test limits are nonzero");
            let destination = SocketAddress::new(address.ip().to_string(), address.port());
            let transport: Rc<dyn UniplexTransport> = match certificate {
                Some(certificate) => {
                    let mut options = TlsClientOptions::new("localhost");
                    options.add_root_certificate(certificate);
                    options.set_alpn_protocols([b"h2".as_slice()]);
                    let transport = TlsTransport::connect(
                        &context,
                        destination,
                        options,
                        limits,
                        REQUEST_TIMEOUT,
                        CancellationToken::never(),
                    )
                    .await
                    .expect("TLS H2 client connects");
                    assert_eq!(
                        transport.connection_info().application_protocol(),
                        Some(b"h2".as_slice())
                    );
                    Rc::new(transport)
                }
                None => Rc::new(
                    TcpTransport::connect(
                        &context,
                        destination,
                        limits,
                        REQUEST_TIMEOUT,
                        CancellationToken::never(),
                    )
                    .await
                    .expect("plaintext H2 client connects"),
                ),
            };
            let (session, handle) = UniplexSession::new_http2(
                context.clone(),
                TransportDirection::Upstream,
                Rc::clone(&transport),
                None,
                UniplexConfig::default(),
                Http2SessionConfig::default(),
            )
            .expect("H2 client session starts");
            let session_task = tokio::task::spawn_local(session.run());
            let request =
                HttpFixedSource::make_get_request(b"/h2", None).expect("fixed H2 request is valid");
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationToken::never(),
                )
                .await
                .expect("H2 request admitted")
                .source;
            let mut headers = response
                .read_header_event()
                .await
                .expect("H2 response headers");
            assert_eq!(
                headers
                    .headers_mut()
                    .status_code()
                    .expect("response status"),
                200
            );
            let mut body = Vec::new();
            if !headers.eom() {
                loop {
                    let event = response
                        .read_body_event(u32::MAX)
                        .await
                        .expect("H2 response body");
                    if let Some(data) = event.as_body() {
                        let bytes = data
                            .coalesce(data.chain_length())
                            .expect("H2 body fits its reported length");
                        body.extend_from_slice(&bytes);
                    }
                    if event.eom() {
                        break;
                    }
                }
            }
            let _ = transport.close(&context);
            let _ = session_task.await;
            body
        })
        .expect("H2 client command admitted");
    let body = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("H2 caller runtime")
        .block_on(async move { tokio::time::timeout(Duration::from_secs(10), call).await })
        .expect("H2 exchange remains bounded")
        .expect("H2 client command completes");
    worker.shutdown().expect("H2 client worker stops");
    body
}

fn h2_tls_request(address: SocketAddr, certificate: Vec<u8>) -> Vec<u8> {
    h2_request(address, Some(certificate))
}

fn h2_plain_request(address: SocketAddr) -> Vec<u8> {
    h2_request(address, None)
}

fn h1_response_body(response: &[u8]) -> &[u8] {
    assert!(
        response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "H1 response succeeds"
    );
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("H1 response has a complete header block");
    let headers = std::str::from_utf8(&response[..header_end]).expect("fixture headers are ASCII");
    let mut content_length = None;
    for (name, value) in headers.lines().filter_map(|line| line.split_once(':')) {
        assert!(
            !name.eq_ignore_ascii_case("transfer-encoding"),
            "fixture response uses Content-Length framing"
        );
        if name.eq_ignore_ascii_case("content-length") {
            assert!(
                content_length.is_none(),
                "exactly one Content-Length header"
            );
            content_length = Some(value.trim().parse::<usize>().expect("valid Content-Length"));
        }
    }
    let body = &response[header_end + 4..];
    assert_eq!(
        content_length,
        Some(body.len()),
        "complete Content-Length response body"
    );
    body
}

struct BoundedServer {
    address: SocketAddr,
    stop: Option<mpsc::SyncSender<()>>,
    result: mpsc::Receiver<(Result<bool, HttpServerError>, HttpServerState)>,
    owner: Option<thread::JoinHandle<()>>,
}

impl BoundedServer {
    fn start(
        config: ServerConfig,
        handler_factory: HandlerFactory,
        observer: Option<Arc<dyn ServerObserver>>,
        label: &str,
    ) -> Self {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = mpsc::sync_channel(1);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let owner = thread::spawn(move || {
            let server = match ScopedHttpServer::start(config, handler_factory, observer) {
                Ok(server) => server,
                Err(error) => {
                    let _ = started_tx.send(Err(error));
                    return;
                }
            };
            if started_tx.send(Ok(server.address())).is_err() || stop_rx.recv().is_err() {
                return;
            }
            let result = server.force_stop();
            let state = server.state();
            let _ = result_tx.send((result, state));
        });
        let started = match started_rx.recv_timeout(REQUEST_TIMEOUT) {
            Ok(started) => started,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Err(payload) = owner.join() {
                    resume_unwind(payload);
                }
                panic!("{label} owner exited without a startup result");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("{label} startup exceeded its deadline");
            }
        };
        let address = started
            .unwrap_or_else(|error| panic!("{label} server starts: {error}"))
            .expect("bounded server has one address");
        Self {
            address,
            stop: Some(stop_tx),
            result: result_rx,
            owner: Some(owner),
        }
    }

    fn address(&self) -> SocketAddr {
        self.address
    }

    fn force_stop(mut self, label: &str) {
        self.stop
            .take()
            .expect("bounded server retains its stop command")
            .send(())
            .unwrap_or_else(|_| panic!("{label} owner receives its stop command"));
        let owner = self
            .owner
            .take()
            .expect("bounded server retains its owner thread");
        let (result, state) = match self.result.recv_timeout(REQUEST_TIMEOUT) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Err(payload) = owner.join() {
                    resume_unwind(payload);
                }
                panic!("{label} owner exited without a force-stop result");
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("{label} force-stop exceeded its deadline");
            }
        };
        owner
            .join()
            .unwrap_or_else(|_| panic!("{label} force-stop thread panicked"));
        assert!(result.unwrap_or_else(|error| panic!("{label} force-stop succeeds: {error}")));
        assert_eq!(state, HttpServerState::Stopped);
    }
}

impl Drop for BoundedServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(owner) = self.owner.take() {
            if self.result.recv_timeout(REQUEST_TIMEOUT).is_ok() {
                let _ = owner.join();
            } else {
                drop(owner);
            }
        }
    }
}

/// An HTTP/1.1 TLS client of `localhost` that trusts only `certificate`.
fn tls_client(certificate: &[u8]) -> ClientConnection {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate.to_vec()))
        .unwrap();
    let mut config = ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    ClientConnection::new(
        Arc::new(config),
        ServerName::try_from("localhost".to_owned()).unwrap(),
    )
    .unwrap()
}

fn tls_request(
    address: SocketAddr,
    certificate: &[u8],
    request: &[u8],
) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let socket = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT)?;
    socket.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    socket.set_write_timeout(Some(REQUEST_TIMEOUT))?;
    let mut stream = StreamOwned::new(tls_client(certificate), socket);
    stream.write_all(request)?;
    let mut response = Vec::new();
    if let Err(error) = stream.read_to_end(&mut response)
        && (error.kind() != std::io::ErrorKind::UnexpectedEof || response.is_empty())
    {
        return Err(error);
    }
    let peer_certificate = stream
        .conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .map(|certificate| certificate.to_vec())
        .unwrap_or_default();
    Ok((response, peer_certificate))
}

fn tls_config(options: TlsServerOptions) -> ServerConfig {
    ServerConfig {
        tls_credentials: Some(Arc::new(crate::server::StaticTlsCredentials::new(options))),
        ..ServerConfig::default()
    }
}

fn start_plain(config: ServerConfig, stats: Arc<HandlerStats>) -> ScopedHttpServer {
    ScopedHttpServer::start(config, handler_factory(stats, false), None).unwrap()
}

fn start_tls(
    config: ServerConfig,
    stats: Arc<HandlerStats>,
    observer: Option<Arc<dyn ServerObserver>>,
) -> ScopedHttpServer {
    ScopedHttpServer::start(config, handler_factory(stats, false), observer).unwrap()
}

#[test]
fn basic() {
    let (options, certificate) = generated_tls();
    let stats = Arc::new(HandlerStats::default());
    let server = start_tls(tls_config(options), Arc::clone(&stats), None);
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
    assert_eq!(stats.requests.load(Ordering::Relaxed), 1);
}

#[test]
fn existing_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let expected = listener.local_addr().unwrap();
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.prebound_listeners.push(listener);
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    assert_eq!(server.address(), Some(expected));
    let (response, _) = tls_request(
        expected,
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

#[test]
fn socket_config_applies_tcp_max_segment_before_listen() {
    let mut config = ServerConfig::default();
    config.socket.tcp_max_segment = Some(1_200);
    let server = start_plain(config, Arc::new(HandlerStats::default()));
    assert_eq!(server.tcp_max_segment(0), Some(1_200));
}

#[derive(Default)]
struct CountingObserver {
    starts: AtomicUsize,
    stops: AtomicUsize,
}

impl ServerObserver for CountingObserver {
    fn on_thread_start(&self, _worker_index: usize) {
        self.starts.fetch_add(1, Ordering::Relaxed);
    }

    fn on_thread_stop(&self, _worker_index: usize) {
        self.stops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn stop_multiple_times() {
    let observer = Arc::new(CountingObserver::default());
    let config = ServerConfig {
        num_io_threads: 4,
        ..ServerConfig::default()
    };
    let server = start_tls(
        config,
        Arc::new(HandlerStats::default()),
        Some(observer.clone()),
    );
    assert!(server.drain());
    assert!(!server.drain());
    assert!(server.force_stop().unwrap());
    assert!(!server.force_stop().unwrap());
    assert_eq!(observer.starts.load(Ordering::Relaxed), 4);
    assert_eq!(observer.stops.load(Ordering::Relaxed), 4);
}

#[test]
fn zero_threads_means_num_cpus() {
    let observer = Arc::new(CountingObserver::default());
    let config = ServerConfig {
        num_io_threads: 0,
        ..ServerConfig::default()
    };
    let server = start_tls(
        config,
        Arc::new(HandlerStats::default()),
        Some(observer.clone()),
    );
    let expected = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    assert_eq!(server.num_io_threads(), expected);
    assert_eq!(observer.starts.load(Ordering::Relaxed), expected);
    server.force_stop().unwrap();
    assert_eq!(observer.stops.load(Ordering::Relaxed), expected);
}

#[test]
fn sampling() {
    let (options, certificate) = generated_tls();
    let server = start_tls(tls_config(options), Arc::new(HandlerStats::default()), None);
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

fn filtered_server(
    result: impl Fn(&ConnectionInfo) -> bool + Send + Sync + 'static,
) -> (ScopedHttpServer, Vec<u8>) {
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.new_connection_filter = Some(Arc::new(result));
    (
        start_tls(config, Arc::new(HandlerStats::default()), None),
        certificate,
    )
}

#[test]
fn connection_filter_pass() {
    let (server, certificate) = filtered_server(|_| true);
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

#[test]
fn connection_filter_fail() {
    let (server, certificate) = filtered_server(|_| false);
    let result = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(result.is_err() || !result.unwrap().0.starts_with(b"HTTP/1.1 200"));
}

#[test]
fn connection_filter_panic() {
    let (server, certificate) = filtered_server(|_| panic!("reject connection"));
    let result = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(result.is_err() || !result.unwrap().0.starts_with(b"HTTP/1.1 200"));
}

#[derive(Default)]
struct RegistrationObserver {
    accepted: AtomicUsize,
    registered: AtomicUsize,
    notification: Option<mpsc::SyncSender<()>>,
}

impl AcceptedConnectionObserver for RegistrationObserver {
    fn on_accepted(&self, _connection: &ConnectionInfo) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    fn on_registered(&self) {
        self.registered.fetch_add(1, Ordering::Relaxed);
        if let Some(notification) = &self.notification {
            let _ = notification.try_send(());
        }
    }
}

#[test]
fn registration_precedes_requests_and_counts_connections_not_requests() {
    let (notification, registered) = mpsc::sync_channel(2);
    let observer = Arc::new(RegistrationObserver {
        notification: Some(notification),
        ..RegistrationObserver::default()
    });
    let stats = Arc::new(HandlerStats::default());
    let config = ServerConfig {
        connection_observer: Some(observer.clone()),
        ..ServerConfig::default()
    };
    let server = start_plain(config, Arc::clone(&stats));
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut stream =
            TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
        stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        stream.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        registered.recv_timeout(REQUEST_TIMEOUT).unwrap();
        assert_eq!(observer.registered.load(Ordering::Relaxed), 1);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
        stream
            .write_all(b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            assert!(response.len() < 4096, "bounded fixture response headers");
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            response.extend_from_slice(&byte);
        }
        let mut body = [0; 2];
        stream.read_exact(&mut body).unwrap();
        response.extend_from_slice(&body);
        assert_eq!(h1_response_body(&response), b"OK");
        stream
            .write_all(b"GET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        response.clear();
        stream.read_to_end(&mut response).unwrap();
        assert_ok_response(&response);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 2);
        assert_eq!(observer.registered.load(Ordering::Relaxed), 1);
        assert_ok_response(&get_request(server.address().unwrap()));
        registered.recv_timeout(REQUEST_TIMEOUT).unwrap();
        assert_eq!(observer.registered.load(Ordering::Relaxed), 2);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 3);
    }));
    let stopped = server.force_stop();
    if let Err(payload) = result {
        resume_unwind(payload);
    }
    stopped.unwrap();
    assert_eq!(observer.accepted.load(Ordering::Relaxed), 2);
}

#[test]
fn accepted_but_rejected_session_does_not_report_registration() {
    struct CloseOnAttach {
        worker: WorkerHandle,
        attached: Arc<AtomicUsize>,
        destroyed: Arc<AtomicUsize>,
    }
    impl LifecycleObserver for CloseOnAttach {
        fn on_attached(&self) {
            self.attached.fetch_add(1, Ordering::Relaxed);
            self.worker.begin_shutdown();
        }
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::Destroy) {
                self.destroyed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    for late_rejection in [false, true] {
        let observer = Arc::new(RegistrationObserver::default());
        let stats = Arc::new(HandlerStats::default());
        let worker = Arc::new(Mutex::new(None::<WorkerHandle>));
        let attached = Arc::new(AtomicUsize::new(0));
        let destroyed = Arc::new(AtomicUsize::new(0));
        let mut config = ServerConfig {
            connection_observer: Some(observer.clone()),
            num_io_threads: 1,
            shutdown_on_signals: Vec::new(),
            ..ServerConfig::default()
        };
        if late_rejection {
            let worker = Arc::clone(&worker);
            let attached = Arc::clone(&attached);
            let destroyed = Arc::clone(&destroyed);
            config.session.lifecycle_observer_factory = Some(Arc::new(move || {
                Rc::new(CloseOnAttach {
                    worker: worker.lock().unwrap().clone().unwrap(),
                    attached: Arc::clone(&attached),
                    destroyed: Arc::clone(&destroyed),
                })
            }));
        } else {
            config.session.read_allocation_size = 0;
        }
        let handler_stats = Arc::clone(&stats);
        let server = HttpServer::start_with_worker_factory(
            config,
            Arc::new(move |context, _| {
                *worker.lock().unwrap() = Some(context.handle().clone());
                Rc::new(TestHandler {
                    stats: Arc::clone(&handler_stats),
                    fail: false,
                    response_body: Bytes::from_static(b"OK"),
                })
            }),
            None,
        )
        .unwrap();
        let _ = plain_request(
            server.address().unwrap(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        server.force_stop().unwrap();
        assert_eq!(observer.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(observer.registered.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
        assert_eq!(
            attached.load(Ordering::Relaxed),
            usize::from(late_rejection)
        );
        assert_eq!(
            destroyed.load(Ordering::Relaxed),
            usize::from(late_rejection)
        );
    }
}

#[test]
fn registration_panic_or_worker_close_joins_without_serving() {
    struct StopOnRegistration {
        observed: Arc<RegistrationObserver>,
        worker: Arc<Mutex<Option<WorkerHandle>>>,
        panic: bool,
    }
    impl AcceptedConnectionObserver for StopOnRegistration {
        fn on_accepted(&self, connection: &ConnectionInfo) {
            self.observed.on_accepted(connection);
        }
        fn on_registered(&self) {
            self.observed.on_registered();
            if self.panic {
                panic!("registered connection observer panic");
            }
            self.worker
                .lock()
                .unwrap()
                .clone()
                .unwrap()
                .begin_shutdown();
        }
    }
    struct Destroyed(Arc<AtomicUsize>);
    impl LifecycleObserver for Destroyed {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::Destroy) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    for secure in [false, true] {
        for panic in [false, true] {
            let (tls, certificate) = generated_tls();
            let (notification, registered) = mpsc::sync_channel(1);
            let observed = Arc::new(RegistrationObserver {
                notification: Some(notification),
                ..RegistrationObserver::default()
            });
            let stats = Arc::new(HandlerStats::default());
            let worker = Arc::new(Mutex::new(None));
            let destroyed = Arc::new(AtomicUsize::new(0));
            let mut trace = SessionRetirementTrace::new(None);
            let mut config = if secure {
                tls_config(tls)
            } else {
                ServerConfig::default()
            };
            config.num_io_threads = 1;
            config.shutdown_on_signals = Vec::new();
            config.session.connection_idle_timeout = Duration::ZERO;
            config.connection_observer = Some(Arc::new(StopOnRegistration {
                observed: Arc::clone(&observed),
                worker: Arc::clone(&worker),
                panic,
            }));
            if panic {
                config.session_retirement_probe = Some(Arc::clone(&trace.probe));
            }
            let destruction = Arc::clone(&destroyed);
            config.session.lifecycle_observer_factory = Some(Arc::new(move || {
                Rc::new(Destroyed(Arc::clone(&destruction)))
            }));
            let handler_stats = Arc::clone(&stats);
            let server = HttpServer::start_with_worker_factory(
                config,
                Arc::new(move |context, _| {
                    *worker.lock().unwrap() = Some(context.handle().clone());
                    Rc::new(TestHandler {
                        stats: Arc::clone(&handler_stats),
                        fail: false,
                        response_body: Bytes::from_static(b"OK"),
                    })
                }),
                None,
            )
            .unwrap();
            let result = catch_unwind(AssertUnwindSafe(|| {
                let socket =
                    TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT)
                        .unwrap();
                socket.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
                socket.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
                let mut client: Box<dyn Write> = if secure {
                    let mut roots = RootCertStore::empty();
                    roots.add(CertificateDer::from(certificate)).unwrap();
                    let mut config =
                        ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
                            .with_safe_default_protocol_versions()
                            .unwrap()
                            .with_root_certificates(roots)
                            .with_no_client_auth();
                    config.alpn_protocols = vec![b"http/1.1".to_vec()];
                    let connection = ClientConnection::new(
                        Arc::new(config),
                        ServerName::try_from("localhost".to_owned()).unwrap(),
                    )
                    .unwrap();
                    Box::new(StreamOwned::new(connection, socket.try_clone().unwrap()))
                } else {
                    Box::new(socket.try_clone().unwrap())
                };
                // Retain both client and socket until reap; peer EOF must not supply cleanup.
                let _ = client.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
                let _ = client.flush();
                registered.recv_timeout(REQUEST_TIMEOUT).unwrap();
                if panic {
                    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
                    trace.wait_for(1, SessionBoundary::RunReturned);
                    trace.wait_for(1, SessionBoundary::Reaped);
                    if !secure {
                        trace.await_backend(1, &runtime);
                        trace.assert_empty_after(1);
                    }
                    let evidence = trace.sessions[0].as_ref().unwrap();
                    let reaped = evidence.reaped.as_ref().unwrap();
                    assert!(evidence.transport_bound && evidence.run_returned);
                    assert!(reaped.mapping_removed && reaped.control_removed);
                    assert_eq!(reaped.remaining_tasks, 0);
                    assert_eq!(reaped.remaining_controls, 0);
                    assert!(
                        matches!(&reaped.outcome, SessionRunOutcome::Returned(Err(error))
                        if error.code() == HttpErrorCode::INTERNAL_ERROR
                            && error.message() == b"server connection callback panicked")
                    );
                    assert_eq!(server.state(), HttpServerState::Running);
                    assert_eq!(destroyed.load(Ordering::Relaxed), 1);
                }
                assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
                drop(client);
                drop(socket);
            }));
            let stopped = server.force_stop();
            if let Err(payload) = result {
                resume_unwind(payload);
            }
            assert_eq!(observed.accepted.load(Ordering::Relaxed), 1);
            assert_eq!(observed.registered.load(Ordering::Relaxed), 1);
            assert_eq!(destroyed.load(Ordering::Relaxed), 1);
            assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
            assert_eq!(server.state(), HttpServerState::Stopped);
            if panic {
                assert!(matches!(
                    stopped,
                    Err(HttpServerError::CallbackPanic {
                        worker_index: 0,
                        stage: ServerCallbackStage::AcceptedConnectionObserver,
                    })
                ));
            } else {
                stopped.unwrap();
            }
        }
    }
}

#[derive(Default)]
struct HandshakeObserver {
    accepts: AtomicUsize,
    successes: AtomicUsize,
    errors: AtomicUsize,
    read_timeouts: AtomicUsize,
}

impl TlsHandshakeObserver for HandshakeObserver {
    fn on_handshake_success(&self) {
        self.accepts.fetch_add(1, Ordering::Relaxed);
    }

    fn on_success(&self, _connection: &ConnectionInfo) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn on_error(&self, error: &HttpError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if error.code() == HttpErrorCode::READ_TIMEOUT {
            self.read_timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[test]
fn tls_logging_callback_invoked() {
    let (options, certificate) = generated_tls();
    let observer = Arc::new(HandshakeObserver::default());
    let mut config = tls_config(options);
    config.tls_observer = Some(observer.clone());
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    let _ = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_eq!(observer.accepts.load(Ordering::Relaxed), 1);
    assert_eq!(observer.successes.load(Ordering::Relaxed), 1);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
}

/// A TCP client that never sends its `ClientHello`.
struct SilentClient {
    stream: TcpStream,
    started: Instant,
}

impl SilentClient {
    fn connect(address: SocketAddr) -> Self {
        let started = Instant::now();
        let stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap();
        Self { stream, started }
    }

    /// How long after it began connecting the server closed the connection, or `None` if the
    /// connection stays open for `wait` more.
    fn closed_within(&mut self, wait: Duration) -> Option<Duration> {
        self.stream.set_read_timeout(Some(wait)).unwrap();
        match self.stream.read(&mut [0_u8; 1]) {
            Ok(0) => Some(self.started.elapsed()),
            Ok(_) => panic!("the server wrote to a client that never sent a ClientHello"),
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                None
            }
            Err(_) => Some(self.started.elapsed()),
        }
    }
}

/// Waits up to `REQUEST_TIMEOUT` for `counter` to reach `expected`, and returns its value.
fn wait_for_count(counter: &AtomicUsize, expected: usize) -> usize {
    let deadline = Instant::now() + REQUEST_TIMEOUT;
    while counter.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    counter.load(Ordering::Relaxed)
}

/// Starts a TLS server offering `alpn` with `session`, and returns its handshake observer and
/// certificate.
fn start_tls_observed(
    session: ServerSessionConfig,
    alpn: &[u8],
) -> (ScopedHttpServer, Arc<HandshakeObserver>, Vec<u8>) {
    let (options, certificate) = generated_tls_with_alpn([alpn]);
    let observer = Arc::new(HandshakeObserver::default());
    let config = ServerConfig {
        session,
        tls_observer: Some(observer.clone()),
        ..tls_config(options)
    };
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    (server, observer, certificate)
}

/// Asserts that the server closes a client that never starts its handshake no sooner than
/// `deadline` after it connected, and reports one `READ_TIMEOUT`.
fn assert_silent_client_times_out(
    server: &ScopedHttpServer,
    observer: &HandshakeObserver,
    deadline: Duration,
) {
    let mut client = SilentClient::connect(server.address().unwrap());
    let closed = client
        .closed_within(Duration::from_secs(5))
        .expect("the handshake deadline closes a silent client within 5 s");
    assert!(
        closed >= deadline,
        "closed after {closed:?}, before the {deadline:?} deadline"
    );
    assert_eq!(
        wait_for_count(&observer.read_timeouts, 1),
        1,
        "the observer receives one READ_TIMEOUT"
    );
    assert_eq!(observer.errors.load(Ordering::Relaxed), 1);
    assert_eq!(observer.accepts.load(Ordering::Relaxed), 0);
}

/// A handshake deadline that a real handshake meets even in a sanitized build.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(2);

/// An untimed session configuration with the given owned modes on.
fn untimed_owned_session(owned_http1: bool, owned_http2: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        owned_http2,
        ..untimed_session(owned_http1)
    }
}

#[test]
fn tls_handshake_timeout_is_independent_of_connection_idle_timeout() {
    for idle in [Duration::ZERO, Duration::from_secs(10)] {
        let defaults = ServerSessionConfig {
            connection_idle_timeout: idle,
            ..ServerSessionConfig::default()
        };
        assert_eq!(defaults.tls_handshake_timeout, Duration::from_secs(60));
        for handshake in [Duration::ZERO, Duration::from_millis(200)] {
            let session = ServerSessionConfig {
                tls_handshake_timeout: handshake,
                ..defaults.clone()
            };
            assert_eq!(session.tls_handshake_timeout, handshake);
        }
    }
}

#[test]
fn tls_handshake_timeout_bounds_a_silent_client_independently_of_idle() {
    let deadline = Duration::from_millis(200);
    let (server, observer, _) = start_tls_observed(
        ServerSessionConfig {
            connection_idle_timeout: Duration::from_secs(30),
            tls_handshake_timeout: deadline,
            ..ServerSessionConfig::default()
        },
        b"http/1.1",
    );
    assert_silent_client_times_out(&server, &observer, deadline);
}

#[test]
fn tls_handshake_default_is_not_shortened_by_the_idle_timeout() {
    let idle = Duration::from_millis(200);
    let (server, observer, _) = start_tls_observed(
        ServerSessionConfig {
            connection_idle_timeout: idle,
            ..ServerSessionConfig::default()
        },
        b"http/1.1",
    );
    let mut client = SilentClient::connect(server.address().unwrap());
    assert_eq!(client.closed_within(idle * 3), None);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
    assert!(server.force_stop().unwrap());
    assert!(client.closed_within(Duration::from_secs(5)).is_some());
}

#[test]
fn zero_tls_handshake_timeout_leaves_the_handshake_untimed() {
    let (server, observer, _) = start_tls_observed(
        ServerSessionConfig {
            connection_idle_timeout: Duration::from_millis(200),
            tls_handshake_timeout: Duration::ZERO,
            ..ServerSessionConfig::default()
        },
        b"http/1.1",
    );
    let mut client = SilentClient::connect(server.address().unwrap());
    assert_eq!(
        client.closed_within(Duration::from_millis(600)),
        None,
        "neither the zero handshake timeout nor the idle timeout closes the client"
    );
    assert!(server.force_stop().unwrap());
    assert!(
        client.closed_within(Duration::from_secs(5)).is_some(),
        "a force stop cancels the pending handshake"
    );
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
}

#[test]
fn tls_handshake_timeout_lets_a_prompt_handshake_serve() {
    let (server, observer, certificate) = start_tls_observed(
        ServerSessionConfig {
            connection_idle_timeout: Duration::from_secs(30),
            tls_handshake_timeout: HANDSHAKE_DEADLINE,
            ..ServerSessionConfig::default()
        },
        b"http/1.1",
    );
    let mut socket =
        TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
    socket.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    socket.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    let mut connection = tls_client(&certificate);
    while connection.is_handshaking() {
        connection.complete_io(&mut socket).unwrap();
    }
    // The deadline covers the handshake only, so an idle connection outlives it.
    thread::sleep(HANDSHAKE_DEADLINE + Duration::from_millis(500));
    let mut stream = StreamOwned::new(connection, socket);
    stream
        .write_all(b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    if let Err(error) = stream.read_to_end(&mut response) {
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof, "{error}");
    }
    assert_ok_response(&response);
    assert_eq!(observer.accepts.load(Ordering::Relaxed), 1);
    assert_eq!(observer.successes.load(Ordering::Relaxed), 1);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
}

#[test]
fn owned_modes_accept_a_tls_handshake_timeout() {
    for (owned_http1, owned_http2) in [(true, false), (false, true), (true, true)] {
        let session = ServerSessionConfig {
            tls_handshake_timeout: Duration::from_millis(200),
            ..untimed_owned_session(owned_http1, owned_http2)
        };
        assert_eq!(session.check_owned(), Ok(()));
        let (options, _) = generated_tls();
        let config = ServerConfig {
            session,
            ..tls_config(options)
        };
        assert!(
            ScopedHttpServer::start(config, handler_factory(Arc::default(), false), None).is_ok(),
            "owned_http1={owned_http1} owned_http2={owned_http2}"
        );
    }
}

#[test]
fn owned_http2_tls_handshake_timeout_bounds_a_silent_client() {
    let (server, observer, certificate) = start_tls_observed(
        ServerSessionConfig {
            tls_handshake_timeout: HANDSHAKE_DEADLINE,
            ..untimed_owned_session(false, true)
        },
        b"h2",
    );
    assert_silent_client_times_out(&server, &observer, HANDSHAKE_DEADLINE);
    assert_eq!(
        h2_tls_request(server.address().unwrap(), certificate),
        b"OK"
    );
}

#[test]
fn owned_http1_tls_handshake_timeout_bounds_a_silent_client() {
    let (server, observer, certificate) = start_tls_observed(
        ServerSessionConfig {
            tls_handshake_timeout: HANDSHAKE_DEADLINE,
            ..untimed_owned_session(true, false)
        },
        b"http/1.1",
    );
    assert_silent_client_times_out(&server, &observer, HANDSHAKE_DEADLINE);
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

#[test]
fn owned_drain_finishes_within_the_tls_handshake_timeout() {
    let deadline = Duration::from_secs(1);
    let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
    let mut session = ServerSessionConfig {
        tls_handshake_timeout: deadline,
        ..untimed_owned_session(false, true)
    };
    session.accepted_socket_profile = Some(Arc::new(move |_| {
        let _ = accepted_tx.try_send(());
        Ok(())
    }));
    let (server, observer, _) = start_tls_observed(session, b"h2");
    let address = server.address().unwrap();
    // The client holds its connection open for 10 s, so a handshake the deadline misses ends
    // with an EOF instead of hanging the test.
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let client = thread::spawn(move || {
        let _client = SilentClient::connect(address);
        let _ = release_rx.recv_timeout(Duration::from_secs(10));
    });
    accepted_rx
        .recv_timeout(REQUEST_TIMEOUT)
        .expect("the server accepts the silent client");
    // Drain reaches only registered sessions, so the pending handshake holds the worker until
    // its deadline.
    let draining = Instant::now();
    assert!(server.drain());
    assert!(server.wait_for_stop().unwrap());
    let stopped = draining.elapsed();
    let _ = release_tx.send(());
    client.join().unwrap();
    assert!(
        stopped < Duration::from_secs(5),
        "drain stopped after {stopped:?}"
    );
    assert_eq!(
        observer.read_timeouts.load(Ordering::Relaxed),
        1,
        "the handshake deadline, not the client, ended the handshake"
    );
}

/// A client HTTP/2 preface and an empty SETTINGS frame, which open a connection without a
/// request.
const H2_PREFACE_AND_SETTINGS: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";

/// Opens a plaintext HTTP/2 connection that sends no request and holds it open.
fn idle_h2_connection(address: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap();
    stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    stream.write_all(H2_PREFACE_AND_SETTINGS).unwrap();
    stream
}

/// Sends the preface on a plaintext HTTP/2 connection that the server drops abortively, and
/// reads until it closes. The reset can surface at the connect, the write or the read.
fn refused_h2_connection(address: SocketAddr) {
    let reset = |error: std::io::Error| {
        assert!(
            matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe
            ),
            "the refused connection fails only by a reset: {error}"
        );
    };
    let mut stream = match TcpStream::connect_timeout(&address, REQUEST_TIMEOUT) {
        Ok(stream) => stream,
        Err(error) => return reset(error),
    };
    stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    if let Err(error) = stream.write_all(H2_PREFACE_AND_SETTINGS) {
        return reset(error);
    }
    let _ = stream.read_to_end(&mut Vec::new());
}

/// Waits up to `REQUEST_TIMEOUT` for `condition`.
fn wait_until(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + REQUEST_TIMEOUT;
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
    true
}

/// Counts of an open-connection gauge as a proxy might keep one: up when its observer attaches,
/// down at `Destroy`.
#[derive(Default)]
struct GaugeCounts {
    open: AtomicUsize,
    attached: AtomicUsize,
    requests: AtomicUsize,
    destroyed: AtomicUsize,
    alive: AtomicUsize,
}

struct ConnectionGauge(Arc<GaugeCounts>);

impl ConnectionGauge {
    fn new(counts: &Arc<GaugeCounts>) -> Self {
        counts.alive.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(counts))
    }
}

impl LifecycleObserver for ConnectionGauge {
    fn on_attached(&self) {
        self.0.attached.fetch_add(1, Ordering::Relaxed);
        self.0.open.fetch_add(1, Ordering::Relaxed);
    }

    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::RequestBegin { .. } => {
                self.0.requests.fetch_add(1, Ordering::Relaxed);
            }
            LifecycleEvent::Destroy => {
                self.0.destroyed.fetch_add(1, Ordering::Relaxed);
                self.0.open.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

impl Drop for ConnectionGauge {
    fn drop(&mut self) {
        self.0.alive.fetch_sub(1, Ordering::Relaxed);
    }
}

fn owned_http2_gauge_config(counts: &Arc<GaugeCounts>) -> ServerConfig {
    let mut session = untimed_owned_session(false, true);
    let counts = Arc::clone(counts);
    session.lifecycle_observer_factory = Some(Arc::new(move || {
        Rc::new(ConnectionGauge::new(&counts)) as Rc<dyn LifecycleObserver>
    }));
    ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        session,
        num_io_threads: 1,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    }
}

#[test]
fn owned_http2_lifecycle_observer_lives_as_long_as_the_connection() {
    let counts = Arc::new(GaugeCounts::default());
    let stats = Arc::new(HandlerStats::default());
    let server = start_plain(owned_http2_gauge_config(&counts), Arc::clone(&stats));
    let address = server.address().unwrap();
    let idle = idle_h2_connection(address);
    assert!(wait_until(|| counts.attached.load(Ordering::Relaxed) == 1));
    assert_eq!(h2_plain_request(address), b"OK");
    assert!(
        wait_until(|| counts.destroyed.load(Ordering::Relaxed) == 1),
        "the served connection reports Destroy when it closes"
    );
    assert_eq!(counts.attached.load(Ordering::Relaxed), 2);
    assert_eq!(counts.requests.load(Ordering::Relaxed), 1);
    assert_eq!(
        counts.open.load(Ordering::Relaxed),
        1,
        "the idle connection stays counted while it is open"
    );
    assert_eq!(counts.alive.load(Ordering::Relaxed), 1);
    drop(idle);
    assert!(
        wait_until(|| counts.open.load(Ordering::Relaxed) == 0),
        "the idle connection reports Destroy at its peer's EOF"
    );
    assert!(wait_until(|| counts.alive.load(Ordering::Relaxed) == 0));
    assert_eq!(counts.destroyed.load(Ordering::Relaxed), 2);
    assert_eq!(stats.requests.load(Ordering::Relaxed), 1);
    server.force_stop().unwrap();
}

#[test]
fn owned_http2_late_rejection_reports_destroy() {
    struct CloseOnAttach {
        worker: WorkerHandle,
        events: Arc<Mutex<Vec<LifecycleEvent>>>,
        alive: Arc<AtomicUsize>,
    }
    impl LifecycleObserver for CloseOnAttach {
        fn on_attached(&self) {
            self.worker.begin_shutdown();
        }
        fn on_event(&self, event: LifecycleEvent) {
            self.events.lock().unwrap().push(event);
        }
    }
    impl Drop for CloseOnAttach {
        fn drop(&mut self) {
            self.alive.fetch_sub(1, Ordering::Relaxed);
        }
    }

    let observer = Arc::new(RegistrationObserver::default());
    let stats = Arc::new(HandlerStats::default());
    let worker = Arc::new(Mutex::new(None::<WorkerHandle>));
    let events = Arc::new(Mutex::new(Vec::new()));
    let alive = Arc::new(AtomicUsize::new(0));
    let mut session = untimed_owned_session(false, true);
    session.lifecycle_observer_factory = Some(Arc::new({
        let worker = Arc::clone(&worker);
        let events = Arc::clone(&events);
        let alive = Arc::clone(&alive);
        move || {
            alive.fetch_add(1, Ordering::Relaxed);
            Rc::new(CloseOnAttach {
                worker: worker.lock().unwrap().clone().unwrap(),
                events: Arc::clone(&events),
                alive: Arc::clone(&alive),
            })
        }
    }));
    let config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        session,
        connection_observer: Some(observer.clone()),
        num_io_threads: 1,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    let handler_stats = Arc::clone(&stats);
    let server = HttpServer::start_with_worker_factory(
        config,
        Arc::new(move |context, _| {
            *worker.lock().unwrap() = Some(context.handle().clone());
            Rc::new(TestHandler {
                stats: Arc::clone(&handler_stats),
                fail: false,
                response_body: Bytes::from_static(b"OK"),
            })
        }),
        None,
    )
    .unwrap();
    let mut client = idle_h2_connection(server.address().unwrap());
    let _ = client.read_to_end(&mut Vec::new());
    server.force_stop().unwrap();
    assert_eq!(observer.accepted.load(Ordering::Relaxed), 1);
    assert_eq!(observer.registered.load(Ordering::Relaxed), 0);
    assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [LifecycleEvent::Destroy],
        "a session rejected after construction reports only its destruction"
    );
    assert_eq!(alive.load(Ordering::Relaxed), 0, "the observer is released");
}

#[test]
fn owned_http2_factory_panic_is_contained() {
    let counts = Arc::new(GaugeCounts::default());
    let mut config = owned_http2_gauge_config(&counts);
    let panicked = Arc::new(AtomicBool::new(false));
    config.session.lifecycle_observer_factory = Some(Arc::new({
        let counts = Arc::clone(&counts);
        let panicked = Arc::clone(&panicked);
        move || {
            if !panicked.swap(true, Ordering::Relaxed) {
                panic!("lifecycle observer factory");
            }
            Rc::new(ConnectionGauge::new(&counts)) as Rc<dyn LifecycleObserver>
        }
    }));
    let stats = Arc::new(HandlerStats::default());
    let server = start_plain(config, Arc::clone(&stats));
    let address = server.address().unwrap();
    refused_h2_connection(address);
    assert!(panicked.load(Ordering::Relaxed));
    assert_eq!(server.state(), HttpServerState::Running);
    assert_eq!(
        h2_plain_request(address),
        b"OK",
        "the worker keeps serving after the factory panics"
    );
    assert!(wait_until(|| counts.destroyed.load(Ordering::Relaxed) == 1));
    assert_eq!(counts.attached.load(Ordering::Relaxed), 1);
    assert_eq!(stats.requests.load(Ordering::Relaxed), 1);
    assert!(matches!(
        server.force_stop(),
        Err(HttpServerError::CallbackPanic {
            worker_index: 0,
            stage: ServerCallbackStage::LifecycleObserverFactory,
        })
    ));
}

#[test]
fn http2_rate_limits_match_for_both_drivers() {
    let default = ServerSessionConfig::default();
    let defaults = Http2RateLimitConfig::default();
    assert!(!defaults.is_unbounded());
    assert_eq!(default.http2_rate_limits, defaults);
    let unbounded = RateLimitConfig::new(u64::MAX, Duration::from_secs(1), 0);
    for limits in [
        defaults,
        Http2RateLimitConfig {
            resets: RateLimitConfig::new(7, Duration::from_secs(3), 1),
            ..defaults
        },
        Http2RateLimitConfig {
            control_frames: unbounded,
            header_frames: unbounded,
            resets: unbounded,
            direct_errors: unbounded,
        },
    ] {
        let config = ServerSessionConfig {
            http2_rate_limits: limits,
            ..default.clone()
        };
        assert_eq!(http2_session_config(&config).rate_limits, limits);
        assert_eq!(owned_http2_session_config(&config).rate_limits, limits);
    }
}

#[test]
fn goaway_timeout_defaults_to_the_managed_sessions_and_reaches_both_drivers() {
    let default = ServerSessionConfig::default();
    assert_eq!(
        default.goaway_timeout,
        Http2SessionConfig::default().goaway_timeout,
        "the default is UniplexSession's"
    );
    for goaway_timeout in [
        default.goaway_timeout,
        Duration::ZERO,
        Duration::from_millis(250),
    ] {
        let config = ServerSessionConfig {
            goaway_timeout,
            owned_http2: true,
            ..ServerSessionConfig::default()
        };
        assert_eq!(http2_session_config(&config).goaway_timeout, goaway_timeout);
        assert_eq!(
            owned_http2_session_config(&config).goaway_timeout,
            goaway_timeout,
            "owned_http2 honors the configured GOAWAY timeout"
        );
        assert_eq!(
            config.check_owned_http2(),
            Ok(()),
            "owned_http2 accepts {goaway_timeout:?}"
        );
    }
}

#[test]
fn tls_accept_success_precedes_connection_rejection() {
    let (options, certificate) = generated_tls();
    let observer = Arc::new(HandshakeObserver::default());
    let registration = Arc::new(RegistrationObserver::default());
    let stats = Arc::new(HandlerStats::default());
    let observed = Arc::clone(&observer);
    let (filtered, received) = mpsc::sync_channel(1);
    let mut config = tls_config(options);
    config.tls_observer = Some(observer.clone());
    config.connection_observer = Some(registration.clone());
    config.new_connection_filter = Some(Arc::new(move |_: &ConnectionInfo| {
        let _ = filtered.try_send(observed.accepts.load(Ordering::Relaxed));
        false
    }));
    let server = start_tls(config, Arc::clone(&stats), None);
    let _ = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    let at_filter = received.recv_timeout(REQUEST_TIMEOUT);
    server.force_stop().unwrap();
    assert_eq!(at_filter.unwrap(), 1);
    assert_eq!(observer.accepts.load(Ordering::Relaxed), 1);
    assert_eq!(observer.successes.load(Ordering::Relaxed), 1);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
    assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
    assert_eq!(registration.accepted.load(Ordering::Relaxed), 0);
    assert_eq!(registration.registered.load(Ordering::Relaxed), 0);
}

#[test]
fn failed_tls_and_plaintext_do_not_report_tls_accept_success() {
    for secure in [true, false] {
        let observer = Arc::new(HandshakeObserver::default());
        let registration = Arc::new(RegistrationObserver::default());
        let stats = Arc::new(HandlerStats::default());
        let mut config = if secure {
            tls_config(generated_tls().0)
        } else {
            ServerConfig::default()
        };
        config.tls_observer = Some(observer.clone());
        config.connection_observer = Some(registration.clone());
        let server = start_plain(config, Arc::clone(&stats));
        let response = plain_request(
            server.address().unwrap(),
            b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        server.force_stop().unwrap();
        assert_eq!(observer.accepts.load(Ordering::Relaxed), 0);
        assert_eq!(observer.successes.load(Ordering::Relaxed), 0);
        assert_eq!(observer.errors.load(Ordering::Relaxed), usize::from(secure));
        assert_eq!(stats.requests.load(Ordering::Relaxed), usize::from(!secure));
        assert_eq!(
            registration.registered.load(Ordering::Relaxed),
            usize::from(!secure)
        );
        if !secure {
            assert_ok_response(&response.unwrap());
        }
    }
}

#[test]
fn tls_accept_observer_closing_or_panic_precedes_metadata_and_joins() {
    struct StopAfterAccept {
        observed: Arc<HandshakeObserver>,
        worker: Arc<Mutex<Option<WorkerHandle>>>,
        panic: bool,
    }
    impl TlsHandshakeObserver for StopAfterAccept {
        fn on_handshake_success(&self) {
            self.observed.on_handshake_success();
            if self.panic {
                panic!("TLS accept observer panic");
            }
            let worker = self.worker.lock().unwrap().clone().unwrap();
            worker.begin_shutdown();
        }
        fn on_success(&self, connection: &ConnectionInfo) {
            self.observed.on_success(connection);
        }
        fn on_error(&self, error: &HttpError) {
            self.observed.on_error(error);
        }
    }

    for panic in [false, true] {
        let (options, certificate) = generated_tls();
        let observed = Arc::new(HandshakeObserver::default());
        let stats = Arc::new(HandlerStats::default());
        let worker = Arc::new(Mutex::new(None));
        let filters = Arc::new(AtomicUsize::new(0));
        let filter_calls = Arc::clone(&filters);
        let mut config = tls_config(options);
        config.num_io_threads = 1;
        config.shutdown_on_signals = Vec::new();
        config.tls_observer = Some(Arc::new(StopAfterAccept {
            observed: Arc::clone(&observed),
            worker: Arc::clone(&worker),
            panic,
        }));
        config.new_connection_filter = Some(Arc::new(move |_: &ConnectionInfo| {
            filter_calls.fetch_add(1, Ordering::Relaxed);
            true
        }));
        let handler_stats = Arc::clone(&stats);
        let server = HttpServer::start_with_worker_factory(
            config,
            Arc::new(move |context, _| {
                *worker.lock().unwrap() = Some(context.handle().clone());
                Rc::new(TestHandler {
                    stats: Arc::clone(&handler_stats),
                    fail: false,
                    response_body: Bytes::from_static(b"OK"),
                })
            }),
            None,
        )
        .unwrap();
        let _ = tls_request(
            server.address().unwrap(),
            &certificate,
            b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let stopped = server.force_stop();
        assert_eq!(observed.accepts.load(Ordering::Relaxed), 1);
        assert_eq!(observed.successes.load(Ordering::Relaxed), 0);
        assert_eq!(observed.errors.load(Ordering::Relaxed), 0);
        assert_eq!(filters.load(Ordering::Relaxed), 0);
        assert_eq!(stats.requests.load(Ordering::Relaxed), 0);
        if panic {
            assert!(matches!(
                stopped,
                Err(HttpServerError::CallbackPanic {
                    worker_index: 0,
                    stage: ServerCallbackStage::TlsHandshakeObserver
                })
            ));
        } else {
            stopped.unwrap();
        }
    }
}

struct RotatingCredentials {
    options: Vec<TlsServerOptions>,
    next: AtomicUsize,
}

impl TlsCredentialProvider for RotatingCredentials {
    fn load(&self) -> Result<TlsServerOptions, HttpError> {
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(self.options[index.min(self.options.len() - 1)].clone())
    }
}

#[test]
fn update_tls_credentials() {
    let (first_options, first_certificate) = generated_tls();
    let (second_options, second_certificate) = generated_tls();
    let provider = Arc::new(RotatingCredentials {
        options: vec![first_options, second_options],
        next: AtomicUsize::new(0),
    });
    let config = ServerConfig {
        tls_credentials: Some(provider),
        ..ServerConfig::default()
    };
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    let (_, observed_first) = tls_request(
        server.address().unwrap(),
        &first_certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    server.update_tls_credentials().unwrap();
    let (_, observed_second) = tls_request(
        server.address().unwrap(),
        &second_certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_eq!(observed_first, first_certificate);
    assert_eq!(observed_second, second_certificate);
    assert_ne!(observed_first, observed_second);
}

#[derive(Default)]
struct FilterStats {
    factories: AtomicUsize,
    request_headers: AtomicUsize,
    request_body_bytes: AtomicUsize,
    response_headers: AtomicUsize,
    errors: AtomicUsize,
    response_drops: AtomicUsize,
}

struct CountingFilterFactory {
    stats: Arc<FilterStats>,
}

impl ServerFilterFactory for CountingFilterFactory {
    fn make_filter_factory(&self) -> Rc<dyn FilterFactory> {
        self.stats.factories.fetch_add(1, Ordering::Relaxed);
        Rc::new(PerRequestFilterFactory {
            stats: Arc::clone(&self.stats),
        })
    }
}

struct PerRequestFilterFactory {
    stats: Arc<FilterStats>,
}

impl FilterFactory for PerRequestFilterFactory {
    fn make_filters(&self) -> FilterPair {
        (
            Some(HttpSourceFilter::new(CountingFilter {
                stats: Arc::clone(&self.stats),
                response: false,
            })),
            Some(HttpSourceFilter::new(CountingFilter {
                stats: Arc::clone(&self.stats),
                response: true,
            })),
        )
    }
}

struct CountingFilter {
    stats: Arc<FilterStats>,
    response: bool,
}

impl HttpSourceFilterHooks for CountingFilter {
    fn filter_name(&self) -> &'static str {
        "ServerLifecycleStats"
    }

    fn read_header_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let stats = Arc::clone(&self.stats);
        let response = self.response;
        Box::pin(async move {
            match downstream.await {
                Ok(event) => {
                    if response {
                        stats.response_headers.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.request_headers.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(event)
                }
                Err(error) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    Err(error)
                }
            }
        })
    }

    fn read_body_event(
        &mut self,
        downstream: LocalSourceFuture<Result<HttpBodyEvent, HttpError>>,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let stats = Arc::clone(&self.stats);
        let response = self.response;
        Box::pin(async move {
            match downstream.await {
                Ok(event) => {
                    if !response && let Some(body) = event.as_body() {
                        stats
                            .request_body_bytes
                            .fetch_add(body.chain_length(), Ordering::Relaxed);
                    }
                    Ok(event)
                }
                Err(error) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    Err(error)
                }
            }
        })
    }
}

impl Drop for CountingFilter {
    fn drop(&mut self) {
        if self.response {
            self.stats.response_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[test]
fn simple_post_stats_filter() {
    let (options, certificate) = generated_tls();
    let handler_stats = Arc::new(HandlerStats::default());
    let filter_stats = Arc::new(FilterStats::default());
    let mut config = tls_config(options);
    config
        .filter_factories
        .push(Arc::new(CountingFilterFactory {
            stats: Arc::clone(&filter_stats),
        }));
    let server = start_tls(config, Arc::clone(&handler_stats), None);
    let body = vec![b'x'; 100_000];
    let request = format!(
        "POST /test HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        String::from_utf8_lossy(&body)
    );
    let (response, _) =
        tls_request(server.address().unwrap(), &certificate, request.as_bytes()).unwrap();
    assert_ok_response(&response);
    assert_eq!(
        handler_stats.request_body_bytes.load(Ordering::Relaxed),
        body.len()
    );
    assert_eq!(filter_stats.factories.load(Ordering::Relaxed), 1);
    assert_eq!(filter_stats.request_headers.load(Ordering::Relaxed), 1);
    assert_eq!(
        filter_stats.request_body_bytes.load(Ordering::Relaxed),
        body.len()
    );
    assert_eq!(filter_stats.response_headers.load(Ordering::Relaxed), 1);
}

#[test]
fn server_error_handler() {
    let (options, certificate) = generated_tls();
    let filter_stats = Arc::new(FilterStats::default());
    let mut config = tls_config(options);
    config
        .filter_factories
        .push(Arc::new(CountingFilterFactory {
            stats: Arc::clone(&filter_stats),
        }));
    let server = ScopedHttpServer::start(
        config,
        handler_factory(Arc::new(HandlerStats::default()), true),
        None,
    )
    .unwrap();
    let result = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(result.is_err() || !result.unwrap().0.starts_with(b"HTTP/1.1 200"));
    assert_eq!(filter_stats.errors.load(Ordering::Relaxed), 1);
}

#[test]
fn multiple_ports() {
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.bind_addresses = vec!["127.0.0.1:0".parse().unwrap(), "[::1]:0".parse().unwrap()];
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    assert_eq!(server.addresses().len(), 2);
    assert_ne!(server.addresses()[0].port(), server.addresses()[1].port());
    for address in server.addresses() {
        let (response, _) = tls_request(
            *address,
            &certificate,
            b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        assert_ok_response(&response);
    }
}

/// Until exec, a child inherits open sockets and can delay listener closure or a reset.
/// Tests that depend on the last socket descriptor closing share this spawn lock.
pub(super) static CHILD_SPAWN: Mutex<()> = Mutex::new(());

#[test]
fn shutdown() {
    let _spawn = CHILD_SPAWN.lock().unwrap_or_else(PoisonError::into_inner);
    let addresses = {
        let mut config = ServerConfig::default();
        config.socket.bind_addresses =
            vec!["127.0.0.1:0".parse().unwrap(), "[::1]:0".parse().unwrap()];
        let server = start_plain(config, Arc::new(HandlerStats::default()));
        server.addresses().to_vec()
    };
    for address in addresses {
        assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err());
    }
}

#[test]
fn stats_latency_on_destruction() {
    let stats = Arc::new(FilterStats::default());
    let factory = PerRequestFilterFactory {
        stats: Arc::clone(&stats),
    };
    let filters = factory.make_filters();
    drop(filters);
    assert_eq!(stats.response_drops.load(Ordering::Relaxed), 1);
}

#[cfg(target_os = "linux")]
fn socket_zero_copy_enabled(listener: &TcpListener) -> std::io::Result<bool> {
    let mut value: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    // SAFETY: `listener` owns a valid fd for this call, and the mutable value and
    // length pointers refer to initialized storage of the reported size.
    let result = unsafe {
        libc::getsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ZEROCOPY,
            (&raw mut value).cast(),
            &raw mut length,
        )
    };
    if result == 0 {
        Ok(value != 0)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn socket_zero_copy_supported() -> bool {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        return false;
    };
    let value: libc::c_int = 1;
    // SAFETY: `listener` owns a valid fd for this call, and `value` is an
    // initialized `c_int` whose pointer and reported length match.
    unsafe {
        libc::setsockopt(
            listener.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ZEROCOPY,
            (&raw const value).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        ) == 0
    }
}

#[cfg(not(target_os = "linux"))]
fn socket_zero_copy_supported() -> bool {
    false
}

fn observe_zero_copy(use_zero_copy: bool, threshold: usize) -> bool {
    let observed = Arc::new(Mutex::new(None));
    let filter_observed = Arc::clone(&observed);
    let mut config = ServerConfig::default();
    config.socket.use_zero_copy = use_zero_copy;
    config.zero_copy_enable_threshold = threshold;
    struct Observer(Arc<Mutex<Option<bool>>>);
    impl AcceptedConnectionObserver for Observer {
        fn on_accepted(&self, connection: &ConnectionInfo) {
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(connection.zero_copy_enabled);
        }
    }
    config.connection_observer = Some(Arc::new(Observer(filter_observed)));
    let server = start_plain(config, Arc::new(HandlerStats::default()));
    assert_ok_response(&get_request(server.address().unwrap()));
    observed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .expect("connection filter records zero-copy state")
}

#[test]
fn zero_copy_applied_to_accepted_transport() {
    let supported = socket_zero_copy_supported();
    assert_eq!(observe_zero_copy(false, 32_768), supported);
    assert_eq!(observe_zero_copy(true, 32_768), supported);
}

struct PanickingAcceptedObserver;

impl AcceptedConnectionObserver for PanickingAcceptedObserver {
    fn on_accepted(&self, _connection: &ConnectionInfo) {
        panic!("accepted connection observer panic");
    }
}

#[test]
fn accepted_connection_observer_panic_is_reported() {
    let config = ServerConfig {
        connection_observer: Some(Arc::new(PanickingAcceptedObserver)),
        ..ServerConfig::default()
    };
    let server = start_plain(config, Arc::new(HandlerStats::default()));

    let _ = plain_request(
        server.address().unwrap(),
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(matches!(
        server.force_stop(),
        Err(HttpServerError::CallbackPanic {
            worker_index: 0,
            stage: ServerCallbackStage::AcceptedConnectionObserver,
        })
    ));
}

#[test]
fn zero_copy_not_applied_without_threshold() {
    assert!(!observe_zero_copy(true, 0));
}

#[test]
fn zero_copy_config_basic_request() {
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.use_zero_copy = true;
    config.zero_copy_enable_threshold = 32_768;
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

#[test]
fn zero_copy_socket_only_basic_request() {
    #[cfg(target_os = "linux")]
    let zero_copy_supported = socket_zero_copy_supported();
    let listener = TcpListener::bind("127.0.0.1:0").expect("prebound listener binds");
    #[cfg(target_os = "linux")]
    let listener_probe = listener.try_clone().expect("listener clone succeeds");
    let address = listener
        .local_addr()
        .expect("listener address is available");
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.use_zero_copy = true;
    config.socket.prebound_listeners.push(listener);
    let _server = start_tls(config, Arc::new(HandlerStats::default()), None);

    #[cfg(target_os = "linux")]
    if zero_copy_supported {
        assert!(socket_zero_copy_enabled(&listener_probe).expect("SO_ZEROCOPY can be queried"));
    }

    let (response, _) = tls_request(
        address,
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert_ok_response(&response);
}

#[test]
fn zero_copy_large_plaintext_response_preserves_body() {
    let payload = Bytes::from(
        (0..256 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let mut config = ServerConfig::default();
    config.socket.use_zero_copy = true;
    config.zero_copy_enable_threshold = 1_024;
    let server = ScopedHttpServer::start(
        config,
        handler_factory_with_body(Arc::new(HandlerStats::default()), false, payload.clone()),
        None,
    )
    .unwrap();
    let response = get_request(server.address().unwrap());
    assert!(response.ends_with(&payload));
}

#[test]
fn zero_copy_config_keeps_large_tls_response_available() {
    let payload = Bytes::from(
        (0..256 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.use_zero_copy = true;
    config.zero_copy_enable_threshold = 1_024;
    let server = ScopedHttpServer::start(
        config,
        handler_factory_with_body(Arc::new(HandlerStats::default()), false, payload.clone()),
        None,
    )
    .unwrap();
    let (response, _) = tls_request(
        server.address().unwrap(),
        &certificate,
        b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .unwrap();
    assert!(response.ends_with(&payload));
}

#[test]
fn zero_copy_force_stop_with_inflight_large_plaintext_response() {
    let payload = Bytes::from(vec![b'z'; 4 * 1024 * 1024]);
    let mut config = ServerConfig::default();
    config.socket.use_zero_copy = true;
    config.zero_copy_enable_threshold = 1_024;
    let server = ScopedHttpServer::start(
        config,
        handler_factory_with_body(Arc::new(HandlerStats::default()), false, payload),
        None,
    )
    .unwrap();
    let mut client = TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT)
        .expect("client connects");
    client
        .write_all(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request writes");
    thread::sleep(Duration::from_millis(20));
    assert!(server.force_stop().unwrap());
    assert_eq!(server.state(), HttpServerState::Stopped);
}

#[test]
fn tls_zero_copy_force_stop_is_bounded() {
    let payload = Bytes::from(vec![b't'; 4 * 1024 * 1024]);
    let zero_copy_observed = Arc::new(Mutex::new(None));
    let observer_value = Arc::clone(&zero_copy_observed);
    struct ZeroCopyObserver(Arc<Mutex<Option<bool>>>);
    impl AcceptedConnectionObserver for ZeroCopyObserver {
        fn on_accepted(&self, connection: &ConnectionInfo) {
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(connection.zero_copy_enabled);
        }
    }

    let (options, certificate) = generated_tls();
    let mut config = tls_config(options);
    config.socket.use_zero_copy = true;
    config.zero_copy_enable_threshold = 1_024;
    config.connection_observer = Some(Arc::new(ZeroCopyObserver(observer_value)));
    let server = BoundedServer::start(
        config,
        handler_factory_with_body(Arc::new(HandlerStats::default()), false, payload),
        None,
        "TLS zero-copy smoke",
    );

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate))
        .expect("generated certificate is trusted");
    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
            .with_safe_default_protocol_versions()
            .expect("the built-in provider supports the default TLS versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
    client_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connection = ClientConnection::new(
        Arc::new(client_config),
        ServerName::try_from("localhost".to_owned()).expect("localhost is a valid server name"),
    )
    .expect("TLS client connection is configured");
    let socket = TcpStream::connect_timeout(&server.address(), REQUEST_TIMEOUT)
        .expect("TLS client connects");
    socket
        .set_read_timeout(Some(REQUEST_TIMEOUT))
        .expect("client read timeout is configured");
    socket
        .set_write_timeout(Some(REQUEST_TIMEOUT))
        .expect("client write timeout is configured");
    let mut client = StreamOwned::new(connection, socket);
    client
        .write_all(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("TLS request writes");

    let mut response_prefix = [0; b"HTTP/1.1 200 OK\r\n".len()];
    client
        .read_exact(&mut response_prefix)
        .expect("TLS response starts before force-stop");
    assert_eq!(response_prefix, *b"HTTP/1.1 200 OK\r\n");
    assert_eq!(
        zero_copy_observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .expect("accepted connection records its zero-copy mode"),
        socket_zero_copy_supported(),
        "the integration exercises kernel zero-copy when supported and its explicit fallback otherwise"
    );

    // Transport tests check that send buffers are retained until kernel completion.
    // This server-level test covers response progress and bounded forced shutdown.
    server.force_stop("TLS zero-copy smoke");
}

#[test]
fn zero_copy_h1_h2_plaintext_tls_matrix() {
    const PAYLOAD_LENGTH: usize = 256 * 1024;

    struct MatrixObserver(Arc<Mutex<Vec<bool>>>);

    impl AcceptedConnectionObserver for MatrixObserver {
        fn on_accepted(&self, connection: &ConnectionInfo) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(connection.zero_copy_enabled);
        }
    }

    let payload = Bytes::from(
        (0..PAYLOAD_LENGTH)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let zero_copy_supported = socket_zero_copy_supported();

    for (name, protocol, tls) in [
        ("H1 plaintext", CodecProtocol::Http11, false),
        ("H1 TLS", CodecProtocol::Http11, true),
        ("H2 plaintext", CodecProtocol::Http2, false),
        ("H2 TLS", CodecProtocol::Http2, true),
    ] {
        let (mut config, certificate) = if tls {
            let alpn = if protocol == CodecProtocol::Http2 {
                b"h2".as_slice()
            } else {
                b"http/1.1".as_slice()
            };
            let (options, certificate) = generated_tls_with_alpn([alpn]);
            (tls_config(options), Some(certificate))
        } else {
            (ServerConfig::default(), None)
        };
        let observed = Arc::new(Mutex::new(Vec::new()));
        config.plaintext_protocol = protocol;
        let nodelay = Arc::new(Mutex::new(Vec::new()));
        let observed_nodelay = Arc::clone(&nodelay);
        config.session.accepted_socket_profile = Some(Arc::new(move |socket| {
            observed_nodelay
                .lock()
                .expect("socket options")
                .push(socket.nodelay().expect("TCP_NODELAY"));
            Ok(())
        }));
        config.socket.use_zero_copy = true;
        config.zero_copy_enable_threshold = 1_024;
        config.connection_observer = Some(Arc::new(MatrixObserver(Arc::clone(&observed))));
        let stats = Arc::new(HandlerStats::default());
        let server = BoundedServer::start(
            config,
            handler_factory_with_body(Arc::clone(&stats), false, payload.clone()),
            None,
            name,
        );

        let body = match (protocol, certificate) {
            (CodecProtocol::Http11, None) => {
                let response = plain_request(
                    server.address(),
                    b"GET /matrix HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .expect("H1 plaintext response");
                h1_response_body(&response).to_vec()
            }
            (CodecProtocol::Http11, Some(certificate)) => {
                let (response, _) = tls_request(
                    server.address(),
                    &certificate,
                    b"GET /matrix HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .expect("H1 TLS response");
                h1_response_body(&response).to_vec()
            }
            (CodecProtocol::Http2, None) => h2_plain_request(server.address()),
            (CodecProtocol::Http2, Some(certificate)) => {
                h2_tls_request(server.address(), certificate)
            }
            _ => unreachable!("the matrix contains only H1 and H2"),
        };

        assert_eq!(body.as_slice(), payload.as_ref(), "{name} response body");
        assert_eq!(
            *nodelay.lock().expect("socket options"),
            [true],
            "{name} disables Nagle on its accepted socket"
        );
        assert_eq!(
            stats.requests.load(Ordering::Acquire),
            1,
            "{name} handles exactly one request"
        );
        assert_eq!(
            observed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            [zero_copy_supported],
            "{name} records kernel zero-copy support or explicit fallback"
        );

        server.force_stop(name);
    }
}

#[test]
fn http2_defaults_match_server_session_limits() {
    let server = crate::acceptor::ServerSessionConfig::default();
    let http2 = http2_session_config(&server);
    assert_eq!(http2.settings, server.settings);
    assert_eq!(
        http2.connection_receive_window,
        server.connection_flow_control
    );
    assert_eq!(http2.stream_receive_window, server.stream_flow_control);
    assert_eq!(
        http2.max_concurrent_outgoing_streams,
        server.max_concurrent_outgoing_streams
    );
}

#[test]
fn http2_first_initial_window_setting_takes_precedence() {
    let mut server = crate::acceptor::ServerSessionConfig::default();
    server.settings.extend([
        CodecSetting {
            identifier: 4,
            value: 111,
        },
        CodecSetting {
            identifier: 4,
            value: 222,
        },
    ]);
    assert_eq!(http2_session_config(&server).stream_receive_window, 111);
}

#[test]
fn owned_http2_stream_window_is_stream_flow_control_unless_settings_set_it() {
    let mut server = crate::acceptor::ServerSessionConfig {
        owned_http2: true,
        ..crate::acceptor::ServerSessionConfig::default()
    };
    assert_eq!(
        owned_http2_session_config(&server).stream_receive_window,
        server.stream_flow_control,
        "without INITIAL_WINDOW_SIZE in settings, the window matches the default driver's"
    );

    server.settings.extend([
        CodecSetting {
            identifier: 4,
            value: 111,
        },
        CodecSetting {
            identifier: 4,
            value: 222,
        },
    ]);
    assert_eq!(
        owned_http2_session_config(&server).stream_receive_window,
        222,
        "the owned driver's window must equal the last advertised value"
    );
}

#[test]
fn tls_alpn_h2_runs_real_uniplex_exchange() {
    let (options, certificate) = generated_tls_with_alpn([b"h2".as_slice()]);
    let stats = Arc::new(HandlerStats::default());
    let server = start_tls(tls_config(options), Arc::clone(&stats), None);
    assert_eq!(
        h2_tls_request(server.address().unwrap(), certificate),
        b"OK"
    );
    let connections = stats.connections.lock().unwrap();
    assert_eq!(connections.len(), 1);
    let metadata = &connections[0];
    assert_eq!(metadata.http_protocol(), Some(CodecProtocol::Http2));
    assert_eq!(metadata.security(), TransportSecurity::Tls);
    assert_eq!(metadata.requested_server_name(), Some("localhost"));
    assert_eq!(
        metadata.negotiated_application_protocol(),
        Some(b"h2".as_slice())
    );
    assert_eq!(
        metadata.local_endpoint().socket_address().unwrap().port(),
        server.address().unwrap().port()
    );
    assert_ne!(metadata.peer_endpoint().socket_address().unwrap().port(), 0);
    assert_eq!(metadata.peer_identity(), None);
}

#[test]
fn tls_alpn_h2_accepts_duplicate_initial_window_settings() {
    let (options, certificate) = generated_tls_with_alpn([b"h2".as_slice()]);
    let mut config = tls_config(options);
    config.session.settings.extend([
        CodecSetting {
            identifier: 4,
            value: 111,
        },
        CodecSetting {
            identifier: 4,
            value: 222,
        },
    ]);
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);
    assert_eq!(
        h2_tls_request(server.address().unwrap(), certificate),
        b"OK"
    );
}

struct LifecycleFactory {
    name: &'static str,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ServerFilterFactory for LifecycleFactory {
    fn make_filter_factory(&self) -> Rc<dyn FilterFactory> {
        Rc::new(PerRequestFilterFactory {
            stats: Arc::new(FilterStats::default()),
        })
    }

    fn on_server_start(&self, _worker_index: usize) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(self.name);
    }

    fn on_server_stop(&self, _worker_index: usize) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(self.name);
    }
}

struct PanickingStopObserver {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ServerObserver for PanickingStopObserver {
    fn on_thread_start(&self, _worker_index: usize) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("observer-start");
    }

    fn on_thread_stop(&self, _worker_index: usize) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("observer-stop");
        panic!("observer stop panic");
    }
}

#[test]
fn stop_callbacks_remain_fifo_and_complete_after_panic() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        filter_factories: vec![
            Arc::new(LifecycleFactory {
                name: "factory-one",
                events: Arc::clone(&events),
            }),
            Arc::new(LifecycleFactory {
                name: "factory-two",
                events: Arc::clone(&events),
            }),
        ],
        ..ServerConfig::default()
    };
    let server = start_tls(
        config,
        Arc::new(HandlerStats::default()),
        Some(Arc::new(PanickingStopObserver {
            events: Arc::clone(&events),
        })),
    );
    assert!(matches!(
        server.force_stop(),
        Err(HttpServerError::CallbackPanic {
            worker_index: 0,
            stage: ServerCallbackStage::ObserverStop,
        })
    ));
    assert_eq!(server.state(), HttpServerState::Stopped);
    assert_eq!(
        *events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        [
            "observer-start",
            "factory-one",
            "factory-two",
            "observer-stop",
            "factory-one",
            "factory-two",
        ]
    );
}

struct SessionRetirementEvidence {
    key: SessionRetirementKey,
    transport_bound: bool,
    retirement: Option<TransportRetirement>,
    backend_retired: bool,
    run_returned: bool,
    reaped: Option<SessionReaped>,
}

#[derive(Clone, Copy)]
enum SessionBoundary {
    TransportBound,
    RunReturned,
    Reaped,
}

#[derive(Debug, Error)]
enum SessionRetirementEvidenceError {
    #[error("session retirement observations are unconfirmed")]
    Unconfirmed,
    #[error("session {0:?} has not been reaped by its parent")]
    ParentNotReaped(SessionRetirementKey),
}

struct SessionRetirementTrace {
    probe: Arc<SessionRetirementProbe>,
    receiver: Receiver<SessionRetirementEvent>,
    sessions: [Option<SessionRetirementEvidence>; 2],
}

impl SessionRetirementTrace {
    fn new(gate: Option<SessionRunGate>) -> Self {
        let (probe, receiver) = SessionRetirementProbe::new(gate);
        Self {
            probe,
            receiver,
            sessions: std::array::from_fn(|_| None),
        }
    }

    fn observe(&mut self, event: SessionRetirementEvent) {
        let key = event.key;
        assert!(
            self.probe
                .confirm(key.worker_index == 0 && (1..=2).contains(&key.session_id)),
            "bounded selected session identity: {key:?}"
        );
        let index = key.session_id as usize - 1;
        match event.record {
            SessionRetirementRecord::Registered => {
                assert!(
                    self.probe.confirm(
                        self.sessions[index].is_none()
                            && self
                                .sessions
                                .iter()
                                .flatten()
                                .all(|session| session.key.task_id != key.task_id)
                    ),
                    "unique registration binds the actual task: {key:?}"
                );
                self.sessions[index] = Some(SessionRetirementEvidence {
                    key,
                    transport_bound: false,
                    retirement: None,
                    backend_retired: false,
                    run_returned: false,
                    reaped: None,
                });
            }
            record => {
                assert!(
                    self.probe.confirm(self.sessions[index].is_some()),
                    "event follows actual registration: {key:?}"
                );
                let session = self.sessions[index].as_mut().expect("registered session");
                assert!(
                    self.probe
                        .confirm(session.key == key && session.reaped.is_none()),
                    "matched unreaped session: {key:?}"
                );
                match record {
                    SessionRetirementRecord::TransportBound(retirement) => {
                        assert!(
                            self.probe
                                .confirm(!session.transport_bound && !session.run_returned),
                            "one accepted transport per session"
                        );
                        session.transport_bound = true;
                        session.retirement = retirement;
                    }
                    SessionRetirementRecord::RunReturned => {
                        assert!(
                            self.probe
                                .confirm(session.transport_bound && !session.run_returned),
                            "one Run return after accepted transport binding"
                        );
                        session.run_returned = true;
                    }
                    SessionRetirementRecord::Reaped(reaped) => {
                        assert!(
                            self.probe.confirm(
                                session.transport_bound
                                    && (!matches!(&reaped.outcome, SessionRunOutcome::Returned(_))
                                        || session.run_returned)
                            ),
                            "parent outcome preserves whether inner Run returned"
                        );
                        session.reaped = Some(reaped);
                    }
                    SessionRetirementRecord::Registered => {
                        unreachable!("registration is handled separately")
                    }
                }
            }
        }
    }

    fn wait_for(&mut self, session_id: u64, boundary: SessionBoundary) {
        assert!((1..=2).contains(&session_id));
        for _ in 0..SESSION_RETIREMENT_EVENT_LIMIT {
            assert!(!self.probe.failed(), "session observation failed");
            if self.sessions[session_id as usize - 1]
                .as_ref()
                .is_some_and(|session| match boundary {
                    SessionBoundary::TransportBound => session.transport_bound,
                    SessionBoundary::RunReturned => session.run_returned,
                    SessionBoundary::Reaped => session.reaped.is_some(),
                })
            {
                return;
            }
            match self.receiver.recv_timeout(REQUEST_TIMEOUT) {
                Ok(event) => self.observe(event),
                Err(error) => {
                    self.probe.confirm(false);
                    panic!("actual session event was not observed within its bound: {error}");
                }
            }
        }
        assert!(!self.probe.failed(), "session observation failed");
        assert!(
            self.sessions[session_id as usize - 1]
                .as_ref()
                .is_some_and(|session| match boundary {
                    SessionBoundary::TransportBound => session.transport_bound,
                    SessionBoundary::RunReturned => session.run_returned,
                    SessionBoundary::Reaped => session.reaped.is_some(),
                }),
            "selected boundary fits the eight-record bound"
        );
    }

    fn drain_available(&mut self) {
        for _ in 0..SESSION_RETIREMENT_EVENT_LIMIT {
            match self.receiver.try_recv() {
                Ok(event) => self.observe(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.probe.confirm(false);
                    panic!("session observation channel disconnected");
                }
            }
        }
        assert!(!self.probe.failed(), "session observation failed");
    }

    fn await_backend(&mut self, session_id: u64, runtime: &Runtime) {
        let session = self.sessions[session_id as usize - 1]
            .as_mut()
            .expect("registered native TCP session");
        assert!(
            self.probe.confirm(session.retirement.is_some()),
            "native TCP retirement receipt is required"
        );
        let retirement = session
            .retirement
            .as_ref()
            .expect("supported backend observation");
        let result =
            runtime.block_on(async { timeout(REQUEST_TIMEOUT, retirement.wait_retired()).await });
        assert!(
            self.probe.confirm(matches!(&result, Ok(Ok(())))),
            "actual backend retirement must succeed: {result:?}"
        );
        session.backend_retired = true;
    }

    fn retired_outcome(
        &self,
        session_id: u64,
    ) -> Result<&SessionRunOutcome, SessionRetirementEvidenceError> {
        if self.probe.failed() || !(1..=2).contains(&session_id) {
            return Err(SessionRetirementEvidenceError::Unconfirmed);
        }
        let session = self.sessions[session_id as usize - 1]
            .as_ref()
            .ok_or(SessionRetirementEvidenceError::Unconfirmed)?;
        let reaped = session
            .reaped
            .as_ref()
            .ok_or(SessionRetirementEvidenceError::ParentNotReaped(session.key))?;
        if !session.backend_retired || !reaped.mapping_removed || !reaped.control_removed {
            return Err(SessionRetirementEvidenceError::Unconfirmed);
        }
        Ok(&reaped.outcome)
    }

    fn assert_empty_after(&mut self, last_session: u64) {
        self.drain_available();
        assert_eq!(
            self.sessions.iter().flatten().count(),
            last_session as usize
        );
        for session in 1..=last_session {
            self.retired_outcome(session)
                .expect("every selected session is fully retired");
        }
        let reaped = self.sessions[last_session as usize - 1]
            .as_ref()
            .expect("last selected session")
            .reaped
            .as_ref()
            .expect("actual final parent reap");
        assert_eq!(reaped.remaining_tasks, 0, "actual task mapping is empty");
        assert_eq!(
            reaped.remaining_controls, 0,
            "actual control mapping is empty"
        );
    }
}

fn assert_session_worker_running(server: &ScopedHttpServer, observer: &CountingObserver) {
    assert_eq!(server.state(), HttpServerState::Running);
    assert_eq!(observer.starts.load(Ordering::Acquire), 1);
    assert_eq!(observer.stops.load(Ordering::Acquire), 0);
}

fn assert_healthy_session_outcome(outcome: &SessionRunOutcome) {
    match outcome {
        SessionRunOutcome::Returned(Ok(summary)) => {
            assert!(!summary.reset, "healthy session was not reset: {summary:?}");
            assert!(summary.bytes_read != 0 && summary.bytes_written != 0);
        }
        other => {
            panic!("healthy wire response must retain a healthy actual Run outcome: {other:?}")
        }
    }
}

struct PanicOnceHandler {
    attempts: Arc<AtomicUsize>,
}

impl HttpHandler for PanicOnceHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            assert_ne!(attempt, 0, "first request intentionally panics");
            let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                .expect("fixed response body is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

#[test]
fn panicked_session_does_not_stop_worker_or_leak_control() {
    #[derive(Debug, Eq, PartialEq)]
    enum FailedOperationCompletion {
        ReturnedError,
        ReturnedReset,
        ReturnedNonReset,
        OuterPanic,
    }

    let classify_failed_operation = |outcome: &SessionRunOutcome| match outcome {
        SessionRunOutcome::Returned(Err(_)) => Some(FailedOperationCompletion::ReturnedError),
        SessionRunOutcome::Returned(Ok(summary)) if summary.reset => {
            Some(FailedOperationCompletion::ReturnedReset)
        }
        SessionRunOutcome::Returned(Ok(_)) => Some(FailedOperationCompletion::ReturnedNonReset),
        SessionRunOutcome::JoinFailed {
            panic: true,
            cancelled: false,
        } => Some(FailedOperationCompletion::OuterPanic),
        SessionRunOutcome::JoinFailed { .. } => None,
    };
    let summary = UniplexRunSummary {
        bytes_read: 1,
        bytes_scheduled: 1,
        bytes_written: 1,
        reset: true,
    };
    let returned_reset = SessionRunOutcome::Returned(Ok(summary));
    let returned_non_reset = SessionRunOutcome::Returned(Ok(UniplexRunSummary {
        reset: false,
        ..summary
    }));
    assert_eq!(
        classify_failed_operation(&returned_reset),
        Some(FailedOperationCompletion::ReturnedReset)
    );
    assert_eq!(
        classify_failed_operation(&returned_non_reset),
        Some(FailedOperationCompletion::ReturnedNonReset)
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            assert_healthy_session_outcome(&returned_reset);
        }))
        .is_err()
    );
    assert_healthy_session_outcome(&returned_non_reset);
    [
        (true, false, Some(FailedOperationCompletion::OuterPanic)),
        (false, true, None),
        (true, true, None),
        (false, false, None),
    ]
    .into_iter()
    .for_each(|(panic, cancelled, expected)| {
        assert_eq!(
            classify_failed_operation(&SessionRunOutcome::JoinFailed { panic, cancelled }),
            expected
        );
    });

    let (release, gate) = oneshot::channel();
    let mut release = Some(release);
    let mut trace = SessionRetirementTrace::new(Some(SessionRunGate {
        worker_index: 0,
        session_id: 2,
        release: gate,
        timeout: REQUEST_TIMEOUT,
    }));
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    let observer = Arc::new(CountingObserver::default());
    let attempts = Arc::new(AtomicUsize::new(0));
    let factory: HandlerFactory = Arc::new({
        let attempts = Arc::clone(&attempts);
        move |_| {
            Rc::new(PanicOnceHandler {
                attempts: Arc::clone(&attempts),
            }) as HttpHandlerRef
        }
    });
    let config = ServerConfig {
        session_retirement_probe: Some(Arc::clone(&trace.probe)),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, Some(observer.clone())).unwrap();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let first = plain_request(
            server.address().unwrap(),
            b"GET /panic HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert!(first.is_err() || !first.unwrap().starts_with(b"HTTP/1.1 200"));
        assert_eq!(attempts.load(Ordering::Acquire), 1);
        assert_ok_response(&get_request(server.address().unwrap()));
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        trace.wait_for(2, SessionBoundary::RunReturned);
        trace.await_backend(2, &runtime);
        trace.wait_for(1, SessionBoundary::Reaped);
        trace.await_backend(1, &runtime);
        let first_outcome = trace
            .retired_outcome(1)
            .expect("first session was actually reaped and its backend retired");
        let first_completion = classify_failed_operation(first_outcome);
        eprintln!(
            "native-session known failed first operation classification: {first_completion:?}; actual outcome: {first_outcome:?}"
        );
        assert!(
            first_completion.is_some(),
            "actual outer panic is distinct from forced or unclassified outer cancellation"
        );
        trace.drain_available();
        assert_session_worker_running(&server, &observer);
        assert!(
            matches!(
                trace.retired_outcome(2),
                Err(SessionRetirementEvidenceError::ParentNotReaped(key))
                    if key == trace.sessions[1].as_ref().expect("second session registered").key
            ),
            "backend retirement and inner Run return cannot replace actual parent reap"
        );
        release
            .take()
            .expect("one explicit selected gate release")
            .send(())
            .expect("the actual session task still owns its gate");
        trace.wait_for(2, SessionBoundary::Reaped);
        assert_healthy_session_outcome(
            trace
                .retired_outcome(2)
                .expect("second session is fully retired"),
        );
        trace.assert_empty_after(2);
        assert_session_worker_running(&server, &observer);
    }));
    let stopped = server.force_stop();
    if let Err(payload) = outcome {
        drop(release);
        resume_unwind(payload);
    }
    assert!(stopped.unwrap());
    assert_eq!(server.state(), HttpServerState::Stopped);
    assert_eq!(observer.stops.load(Ordering::Acquire), 1);
    assert!(!trace.probe.failed());
}

#[test]
fn graceful_drain_wait_joins_workers_before_stopped() {
    let observer = Arc::new(CountingObserver::default());
    let server = start_tls(
        ServerConfig::default(),
        Arc::new(HandlerStats::default()),
        Some(observer.clone()),
    );
    assert!(server.drain());
    assert!(server.wait_for_stop().unwrap());
    assert_eq!(observer.stops.load(Ordering::Acquire), 1);
    assert_eq!(server.state(), HttpServerState::Stopped);
    assert!(!server.force_stop().unwrap());
}

struct GatedHandler {
    started: mpsc::SyncSender<()>,
    release: Arc<tokio::sync::Notify>,
}

impl HttpHandler for GatedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = self.started.clone();
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            request.read_header_event().await?;
            let _ = started.send(());
            release.notified().await;
            let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                .expect("fixed response body is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

#[test]
fn graceful_drain_waits_for_active_request() {
    let mut trace = SessionRetirementTrace::new(None);
    let runtime = Builder::new_current_thread().enable_time().build().unwrap();
    let observer = Arc::new(CountingObserver::default());
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let factory: HandlerFactory = Arc::new({
        let release = Arc::clone(&release);
        move |_| {
            Rc::new(GatedHandler {
                started: started_tx.clone(),
                release: Arc::clone(&release),
            }) as HttpHandlerRef
        }
    });
    let config = ServerConfig {
        session_retirement_probe: Some(Arc::clone(&trace.probe)),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, Some(observer.clone())).unwrap();
    let address = server.address().unwrap();
    let mut client = Some(thread::spawn(move || get_request(address)));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        started_rx
            .recv_timeout(REQUEST_TIMEOUT)
            .expect("active request reaches handler");
        trace.wait_for(1, SessionBoundary::TransportBound);
        trace.drain_available();
        assert!(
            !trace.sessions[0]
                .as_ref()
                .expect("active session registered")
                .run_returned
        );
        assert!(matches!(
            trace.retired_outcome(1),
            Err(SessionRetirementEvidenceError::ParentNotReaped(_))
        ));
        assert!(server.drain());
        assert_eq!(server.state(), HttpServerState::Draining);
        release.notify_one();
        assert_ok_response(
            &client
                .take()
                .expect("retained client join")
                .join()
                .expect("client thread completes"),
        );
        trace.wait_for(1, SessionBoundary::Reaped);
        trace.await_backend(1, &runtime);
        assert_healthy_session_outcome(
            trace
                .retired_outcome(1)
                .expect("drained session is fully retired"),
        );
        trace.assert_empty_after(1);
        assert!(server.wait_for_stop().unwrap());
        assert_eq!(server.state(), HttpServerState::Stopped);
        assert_eq!(observer.starts.load(Ordering::Acquire), 1);
        assert_eq!(observer.stops.load(Ordering::Acquire), 1);
        assert!(!trace.probe.failed());
    }));
    if let Err(payload) = outcome {
        release.notify_one();
        let _ = server.force_stop();
        if let Some(client) = client.take() {
            let _ = client.join();
        }
        resume_unwind(payload);
    }
}

struct DropOrderHandler {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for DropOrderHandler {
    fn drop(&mut self) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("handler-drop");
    }
}

impl HttpHandler for DropOrderHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        unreachable!("the stop-order oracle does not issue a request")
    }
}

struct StopOrderObserver {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl ServerObserver for StopOrderObserver {
    fn on_thread_stop(&self, _worker_index: usize) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push("observer-stop");
    }
}

#[test]
fn worker_resources_drop_before_thread_stop_callback() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let factory: HandlerFactory = Arc::new({
        let events = Arc::clone(&events);
        move |_| {
            Rc::new(DropOrderHandler {
                events: Arc::clone(&events),
            }) as HttpHandlerRef
        }
    });
    let server = ScopedHttpServer::start(
        ServerConfig::default(),
        factory,
        Some(Arc::new(StopOrderObserver {
            events: Arc::clone(&events),
        })),
    )
    .unwrap();
    server.force_stop().unwrap();
    assert_eq!(
        *events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        ["handler-drop", "observer-stop"]
    );
}

#[test]
fn worker_factory_keeps_handler_and_async_cleanup_on_owner() {
    struct LocalHandler {
        context: WorkerContext,
        index: usize,
        events: Arc<Mutex<Vec<(usize, &'static str)>>>,
        _cleanup: WorkerCleanupTaskRegistration,
    }

    impl HttpHandler for LocalHandler {
        fn handle_request(
            &self,
            _session: HandlerSessionContext,
            mut request: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            assert!(self.context.is_owner_thread());
            let context = self.context.clone();
            let events = Arc::clone(&self.events);
            let index = self.index;
            Box::pin(async move {
                let headers = request.read_header_event().await?;
                assert!(headers.eom());
                tokio::task::yield_now().await;
                assert!(context.is_owner_thread());
                events.lock().unwrap().push((index, "request"));
                let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                    .expect("fixture body fits capacity");
                let response = HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("fixture response is valid");
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    impl Drop for LocalHandler {
        fn drop(&mut self) {
            assert!(self.context.is_owner_thread());
            self.events.lock().unwrap().push((self.index, "drop"));
        }
    }

    let caller = thread::current().id();
    let events = Arc::new(Mutex::new(Vec::new()));
    let server = HttpServer::start_with_worker_factory(
        ServerConfig {
            num_io_threads: 2,
            shutdown_on_signals: Vec::new(),
            ..ServerConfig::default()
        },
        Arc::new({
            let events = Arc::clone(&events);
            move |context, index| {
                assert!(context.is_owner_thread());
                assert_ne!(context.owner_thread(), caller);
                events.lock().unwrap().push((index, "construct"));
                let cleanup = context
                    .register_cleanup_task({
                        let context = context.clone();
                        let events = Arc::clone(&events);
                        async move {
                            tokio::task::yield_now().await;
                            assert!(context.is_owner_thread());
                            events.lock().unwrap().push((index, "cleanup"));
                        }
                    })
                    .expect("factory can reserve owner-local asynchronous cleanup");
                Rc::new(LocalHandler {
                    context,
                    index,
                    events: Arc::clone(&events),
                    _cleanup: cleanup,
                })
            }
        }),
        None,
    )
    .expect("worker-aware server starts");
    let address = server.address().expect("one listener");
    assert_ok_response(&get_request(address));
    assert_ok_response(&get_request(address));
    server.drain();
    server.wait_for_stop().expect("all worker cleanup joins");
    assert_eq!(server.state(), HttpServerState::Stopped);
    let events = events.lock().unwrap();
    for index in 0..2 {
        let local: Vec<_> = events
            .iter()
            .filter_map(|(worker, event)| (*worker == index).then_some(*event))
            .collect();
        assert_eq!(local, ["construct", "request", "drop", "cleanup"]);
    }
}

#[test]
fn saturated_accept_handoff_does_not_delay_force_stop() {
    assert!(ServerConfig::default().admission_probe.is_none());
    assert_eq!(ADMISSION_EVENT_LIMIT, 8);
    let (probe, events, release) = AdmissionProbe::new();
    let stats = Arc::new(HandlerStats::default());
    let lifecycle = Arc::new(AdmissionLifecycle::default());
    let factory: HandlerFactory = Arc::new({
        let stats = Arc::clone(&stats);
        let lifecycle = Arc::clone(&lifecycle);
        move |_| {
            Rc::new(AdmissionHandler {
                inner: TestHandler {
                    stats: Arc::clone(&stats),
                    fail: false,
                    response_body: Bytes::from_static(b"OK"),
                },
                lifecycle: Arc::clone(&lifecycle),
            }) as HttpHandlerRef
        }
    });
    let config = ServerConfig {
        num_io_threads: 1,
        command_capacity: 1,
        shutdown_on_signals: Vec::new(),
        admission_probe: Some(Arc::clone(&probe)),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, Some(lifecycle.clone())).unwrap();
    let control = server.control();
    let mut clients = [None, None];
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let address = server.address().expect("one listener");
        let receive = || {
            let event = events
                .recv_timeout(REQUEST_TIMEOUT)
                .expect("bounded admission event");
            assert!(
                !probe.failed(),
                "sticky admission observation failure: {event:?}"
            );
            event
        };
        let mut progress = AdmissionProgress::default();
        let registered = receive();
        let AdmissionEvent::ListenerRegistered(key) = registered else {
            panic!("listener registration must precede dequeue hold: {registered:?}");
        };
        assert!(progress.observe(registered));
        assert!(progress.observe(receive()));
        assert!(!progress.saturated());
        assert!(!progress.retired());

        clients[0] = Some(TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap());
        let first = clients[0].as_ref().unwrap();
        first.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        first.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        let enqueued = receive();
        let AdmissionEvent::Socket {
            key: enqueue_key,
            socket: first_socket,
            attempt: AdmissionAttempt::Enqueued,
        } = enqueued
        else {
            panic!("first real accepted socket must enqueue: {enqueued:?}");
        };
        assert_eq!(enqueue_key, key);
        assert_eq!(first_socket.local, Some(first.peer_addr().unwrap()));
        assert_eq!(first_socket.peer, first.local_addr().unwrap());
        assert!(progress.observe(enqueued));
        assert!(
            !progress.saturated(),
            "enqueue and capacity zero alone are not Full"
        );
        let enqueue_only = progress.clone();

        clients[1] = Some(TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap());
        let second = clients[1].as_ref().unwrap();
        second.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        second.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        let full = receive();
        let AdmissionEvent::Socket {
            key: full_key,
            socket: second_socket,
            attempt: AdmissionAttempt::Full,
        } = full
        else {
            panic!("second real try_send must return Full, not Closed: {full:?}");
        };
        assert_eq!(full_key, key);
        assert_eq!(second_socket.local, Some(second.peer_addr().unwrap()));
        assert_eq!(second_socket.peer, second.local_addr().unwrap());
        assert!(progress.observe(full));
        assert!(progress.saturated());
        assert!(!progress.retired());
        for attempt in [AdmissionAttempt::Enqueued, AdmissionAttempt::Closed] {
            let mut negative = enqueue_only.clone();
            assert!(!negative.observe(AdmissionEvent::Socket {
                key,
                socket: second_socket,
                attempt
            }));
            assert!(!negative.saturated());
            assert!(!negative.observe(full), "invalid evidence remains sticky");
        }
        let mut queue_only = AdmissionProgress::default();
        assert!(!queue_only.observe(full));
        assert!(!queue_only.saturated());
        assert_eq!(server.state(), HttpServerState::Running);
        assert_eq!(control.state(), HttpServerState::Running);
        assert_eq!(server.num_io_threads(), 1);
        assert_eq!(lifecycle.counts.starts.load(Ordering::Acquire), 1);
        assert_eq!(lifecycle.counts.stops.load(Ordering::Acquire), 0);
        assert_eq!(lifecycle.handler_drops.load(Ordering::Acquire), 0);
        assert_eq!(stats.requests.load(Ordering::Acquire), 0);
        assert_eq!(probe.registrations(), 0);
        assert!(!probe.failed());
        assert!(!lifecycle.invalid.load(Ordering::Acquire));

        let started = std::time::Instant::now();
        assert!(
            control.force_stop(),
            "request ForceStop without preliminary Drain"
        );
        assert!(
            server.force_stop().unwrap(),
            "physical owner joins original worker"
        );
        assert!(started.elapsed() < REQUEST_TIMEOUT);
        assert_eq!(server.state(), HttpServerState::Stopped);
        assert_eq!(control.state(), HttpServerState::Stopped);
        assert_eq!(lifecycle.counts.starts.load(Ordering::Acquire), 1);
        assert_eq!(lifecycle.counts.stops.load(Ordering::Acquire), 1);
        assert_eq!(lifecycle.handler_drops.load(Ordering::Acquire), 1);
        assert!(!lifecycle.invalid.load(Ordering::Acquire));
        assert!(
            !progress.retired(),
            "worker-stop counts do not supply the listener reap"
        );

        let force = receive();
        assert!(matches!(force, AdmissionEvent::ForceObserved { .. }));
        assert!(
            progress.observe(force),
            "ForceStop observes the still-held full queue"
        );
        assert!(
            !progress.retired(),
            "abort request is not a consumed JoinSet result"
        );
        let before_reap = progress.clone();
        let reaped = receive();
        assert!(matches!(reaped, AdmissionEvent::ListenerReaped { .. }));
        assert!(progress.observe(reaped));
        assert!(progress.retired());
        for outcome in [
            AdmissionJoinOutcome::Panicked,
            AdmissionJoinOutcome::OtherFailure,
        ] {
            let mut negative = before_reap.clone();
            assert!(!negative.observe(AdmissionEvent::ListenerReaped {
                key,
                outcome,
                remaining: 0
            }));
            assert!(!negative.retired());
        }
        for outcome in [
            AdmissionJoinOutcome::Returned,
            AdmissionJoinOutcome::Cancelled,
        ] {
            let mut literal = before_reap.clone();
            assert!(literal.observe(AdmissionEvent::ListenerReaped {
                key,
                outcome,
                remaining: 0
            }));
            assert!(literal.retired());
        }
        let mut mismatched = before_reap.clone();
        let mut wrong_key = key;
        wrong_key.worker_index = 1;
        assert!(!mismatched.observe(AdmissionEvent::ListenerReaped {
            key: wrong_key,
            outcome: AdmissionJoinOutcome::Returned,
            remaining: 0,
        }));
        assert!(!mismatched.retired());
        let mut nonempty = before_reap;
        assert!(!nonempty.observe(AdmissionEvent::ListenerReaped {
            key,
            outcome: AdmissionJoinOutcome::Returned,
            remaining: 1,
        }));
        assert!(!nonempty.retired());
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(probe.registrations(), 0);
        assert_eq!(stats.requests.load(Ordering::Acquire), 0);
        assert!(!probe.failed());
        for client in &mut clients {
            let mut byte = [0_u8; 1];
            match client.as_mut().unwrap().read(&mut byte) {
                Ok(0) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) => {}
                result => {
                    panic!("retired pre-session socket must close/reset, not time out: {result:?}")
                }
            }
        }
    }));
    let cleanup = outcome
        .is_err()
        .then(|| catch_unwind(AssertUnwindSafe(|| server.force_stop())));
    drop(clients);
    drop(release);
    if let Err(primary) = outcome {
        {
            let mut stderr = std::io::stderr().lock();
            match cleanup {
                Some(Ok(Err(error))) => {
                    let _ = writeln!(stderr, "admission failure cleanup also failed: {error:?}");
                }
                Some(Err(_)) => {
                    let _ = writeln!(stderr, "admission failure cleanup also panicked");
                }
                _ => {}
            }
        }
        resume_unwind(primary);
    }
}

#[derive(Default)]
struct AdmissionLifecycle {
    counts: CountingObserver,
    handler_drops: AtomicUsize,
    invalid: AtomicBool,
}

impl ServerObserver for AdmissionLifecycle {
    fn on_thread_start(&self, worker_index: usize) {
        if worker_index != 0 || self.counts.starts.fetch_add(1, Ordering::AcqRel) != 0 {
            self.invalid.store(true, Ordering::Release);
        }
    }

    fn on_thread_stop(&self, worker_index: usize) {
        if worker_index != 0 || self.handler_drops.load(Ordering::Acquire) != 1 {
            self.invalid.store(true, Ordering::Release);
        }
        if self.counts.stops.fetch_add(1, Ordering::AcqRel) != 0 {
            self.invalid.store(true, Ordering::Release);
        }
    }
}

struct AdmissionHandler {
    inner: TestHandler,
    lifecycle: Arc<AdmissionLifecycle>,
}

impl HttpHandler for AdmissionHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.inner.handle_request(context, request)
    }
}

impl Drop for AdmissionHandler {
    fn drop(&mut self) {
        if self.lifecycle.handler_drops.fetch_add(1, Ordering::AcqRel) != 0 {
            self.lifecycle.invalid.store(true, Ordering::Release);
        }
    }
}

/// Echoes each request body and records whether its handler context carried an exchange
/// completion and a response proposer.
struct EchoHandler {
    receipts: Arc<Mutex<Vec<(bool, bool)>>>,
}

impl HttpHandler for EchoHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.receipts.lock().unwrap().push((
            context.exchange_completion().is_some(),
            context.response_proposer().is_some(),
        ));
        Box::pin(async move {
            let headers = request.read_header_event().await?;
            let mut body = BufQueue::new(64 * 1024);
            if !headers.eom() {
                loop {
                    let event = request.read_body_event(u32::MAX).await?;
                    if let Some(data) = event.as_body() {
                        body.append(&mut data.clone_shared())
                            .expect("bounded request body");
                    }
                    if event.eom() {
                        break;
                    }
                }
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

fn echo_factory(receipts: Arc<Mutex<Vec<(bool, bool)>>>) -> HandlerFactory {
    Arc::new(move |_| {
        Rc::new(EchoHandler {
            receipts: Arc::clone(&receipts),
        }) as HttpHandlerRef
    })
}

/// A session configuration both HTTP/1.1 modes accept: every timeout zero.
fn untimed_session(owned_http1: bool) -> crate::acceptor::ServerSessionConfig {
    crate::acceptor::ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1,
        ..crate::acceptor::ServerSessionConfig::default()
    }
}

/// Reads one `Content-Length` framed HTTP/1.1 response.
fn read_framed_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        if let Some(end) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = std::str::from_utf8(&response[..end]).expect("ASCII response head");
            let length = head
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map_or(0, |(_, value)| value.trim().parse::<usize>().unwrap());
            if response.len() >= end + 4 + length {
                assert_eq!(response.len(), end + 4 + length, "one response at a time");
                return response;
            }
        }
        let count = stream.read(&mut chunk).expect("response bytes");
        assert_ne!(count, 0, "the response completes before EOF");
        response.extend_from_slice(&chunk[..count]);
    }
}

/// Replaces every `Date` value, which the codec takes from the clock.
fn normalize_date(wire: &[u8]) -> Vec<u8> {
    let mut normalized = Vec::with_capacity(wire.len());
    for line in wire.split_inclusive(|byte| *byte == b'\n') {
        if line.len() > 5 && line[..5].eq_ignore_ascii_case(b"date:") {
            normalized.extend_from_slice(b"Date: <normalized>\r\n");
        } else {
            normalized.extend_from_slice(line);
        }
    }
    normalized
}

/// Sends three keep-alive requests with bodies on one connection, the last one closing it,
/// and returns every response byte and whether each handler context had an exchange
/// completion and a response proposer.
fn serve_three_requests(owned_http1: bool) -> (Vec<u8>, Vec<(bool, bool)>) {
    let receipts = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        session: untimed_session(owned_http1),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, echo_factory(Arc::clone(&receipts)), None)
        .expect("server starts");
    let mut stream =
        TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
    stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    let mut wire = Vec::new();
    for (index, body) in [&b"first"[..], &b"second body"[..], &b"third"[..]]
        .into_iter()
        .enumerate()
    {
        let close = if index == 2 {
            "Connection: close\r\n"
        } else {
            ""
        };
        let request = format!(
            "POST /echo/{index} HTTP/1.1\r\nHost: localhost\r\n{close}Content-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        wire.extend_from_slice(&read_framed_response(&mut stream));
    }
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty(), "nothing follows the closing response");
    drop(server);
    let receipts = receipts.lock().unwrap().clone();
    (normalize_date(&wire), receipts)
}

#[test]
fn owned_http1_serves_requests_through_the_handler() {
    let (default_wire, default_receipts) = serve_three_requests(false);
    let (owned_wire, owned_receipts) = serve_three_requests(true);
    assert!(default_wire.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(default_wire.ends_with(b"third"));
    assert_eq!(
        String::from_utf8_lossy(&owned_wire),
        String::from_utf8_lossy(&default_wire)
    );
    // The owned driver does not give the handler an exchange completion or a response
    // proposer.
    assert_eq!(default_receipts, [(true, true); 3]);
    assert_eq!(owned_receipts, [(false, false); 3]);
}

fn start_error(session: crate::acceptor::ServerSessionConfig) -> &'static str {
    let config = ServerConfig {
        session,
        ..ServerConfig::default()
    };
    match ScopedHttpServer::start(config, echo_factory(Arc::default()), None) {
        Err(HttpServerError::InvalidConfig(message)) => message,
        Err(error) => panic!("unexpected server error: {error}"),
        Ok(_) => panic!("the server started with an unsupported owned HTTP/1.1 configuration"),
    }
}

#[test]
fn owned_http1_rejects_unsupported_factories() {
    let mut session = untimed_session(true);
    session.stats_factory = Some(Arc::new(
        |_: &ConnectionInfo| -> Rc<dyn roxygen_session::http2::stats::Http2SessionStats> {
            unreachable!("a refused configuration builds no recorder")
        },
    ));
    assert_eq!(
        start_error(session),
        "owned_http1 does not support stats_factory"
    );
    let mut session = untimed_session(true);
    session.lifecycle_observer_factory = Some(Arc::new(|| -> Rc<dyn LifecycleObserver> {
        unreachable!("a refused configuration builds no observer")
    }));
    assert_eq!(
        start_error(session),
        "owned_http1 does not support lifecycle_observer_factory"
    );
}

#[test]
fn owned_http1_rejects_nonzero_timeouts() {
    let defaults = crate::acceptor::ServerSessionConfig {
        owned_http1: true,
        ..crate::acceptor::ServerSessionConfig::default()
    };
    assert_eq!(
        start_error(defaults),
        "owned_http1 is untimed: stream_read_timeout must be zero"
    );
    let second = Duration::from_secs(1);
    let mut session = untimed_session(true);
    session.stream_read_timeout = second;
    assert_eq!(
        start_error(session),
        "owned_http1 is untimed: stream_read_timeout must be zero"
    );
    let mut session = untimed_session(true);
    session.connection_idle_timeout = second;
    assert_eq!(
        start_error(session),
        "owned_http1 is untimed: connection_idle_timeout must be zero"
    );
    let mut session = untimed_session(true);
    session.write_timeout = second;
    assert_eq!(
        start_error(session),
        "owned_http1 is untimed: write_timeout must be zero"
    );
}

#[test]
fn owned_http1_leaves_http2_unchanged() {
    use crate::acceptor::ServerSessionKind;
    use crate::attachment_tests::session_kind_on_worker;
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http11, untimed_session(true)),
        ServerSessionKind::OwnedHttp1
    );
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http2, untimed_session(true)),
        ServerSessionKind::Uniplex
    );
    let stats = Arc::new(HandlerStats::default());
    let config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        session: untimed_session(true),
        ..ServerConfig::default()
    };
    let server = start_plain(config, Arc::clone(&stats));
    assert_eq!(h2_plain_request(server.address().unwrap()), b"OK");
    assert_eq!(stats.requests.load(Ordering::Relaxed), 1);
}

/// Responds to `/gated` only once released, and to every other request immediately.
struct DrainHandler {
    started: mpsc::SyncSender<()>,
    release: Arc<tokio::sync::Notify>,
}

impl HttpHandler for DrainHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = self.started.clone();
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if head.headers().request_url() == Some(b"/gated".as_slice()) {
                let _ = started.send(());
                release.notified().await;
            }
            let body = BufQueue::from_bytes(Bytes::from_static(b"OK"), 2)
                .expect("fixed response body is bounded");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body))
                    .expect("status and body are valid"),
            )))
        })
    }
}

/// Starts an owned HTTP/1.1 server with one idle keep-alive connection (its first response
/// read) and one connection whose `/gated` request is in its handler. Each client thread
/// returns what it reads until the server closes its connection.
fn owned_server_with_idle_and_inflight() -> (
    ScopedHttpServer,
    Arc<tokio::sync::Notify>,
    thread::JoinHandle<std::io::Result<Vec<u8>>>,
    thread::JoinHandle<std::io::Result<Vec<u8>>>,
) {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let factory: HandlerFactory = Arc::new({
        let release = Arc::clone(&release);
        move |_| {
            Rc::new(DrainHandler {
                started: started_tx.clone(),
                release: Arc::clone(&release),
            }) as HttpHandlerRef
        }
    });
    let config = ServerConfig {
        session: untimed_session(true),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, None).expect("server starts");
    let address = server.address().unwrap();
    let connect = move || {
        let stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap();
        stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        stream.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
        stream
    };
    let (idle_tx, idle_rx) = mpsc::sync_channel(1);
    let mut idle_stream = connect();
    let idle = thread::spawn(move || {
        idle_stream.write_all(b"GET /fast HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        let first = read_framed_response(&mut idle_stream);
        assert!(first.starts_with(b"HTTP/1.1 200 OK\r\n"));
        idle_tx.send(()).unwrap();
        let mut rest = Vec::new();
        idle_stream.read_to_end(&mut rest)?;
        Ok(rest)
    });
    idle_rx
        .recv_timeout(REQUEST_TIMEOUT)
        .expect("the idle connection's first response");
    let mut inflight_stream = connect();
    let inflight = thread::spawn(move || {
        inflight_stream.write_all(b"GET /gated HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        let mut response = Vec::new();
        inflight_stream.read_to_end(&mut response)?;
        Ok(response)
    });
    started_rx
        .recv_timeout(REQUEST_TIMEOUT)
        .expect("the gated request reaches its handler");
    (server, release, idle, inflight)
}

#[test]
fn owned_http1_drain_and_shutdown_are_clean() {
    let (server, release, idle, inflight) = owned_server_with_idle_and_inflight();
    assert!(server.drain());
    let rest = idle
        .join()
        .unwrap()
        .expect("drain closes the idle connection");
    assert!(rest.is_empty(), "nothing follows on the idle connection");
    release.notify_one();
    let response = inflight
        .join()
        .unwrap()
        .expect("drain finishes the in-flight request, then closes");
    let response = normalize_date(&response);
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(
        response
            .windows(b"Connection: close\r\n".len())
            .any(|window| window == b"Connection: close\r\n")
    );
    assert!(response.ends_with(b"\r\n\r\nOK"));
    assert!(server.wait_for_stop().unwrap());
    assert_eq!(server.state(), HttpServerState::Stopped);

    let (server, _release, idle, inflight) = owned_server_with_idle_and_inflight();
    assert!(server.force_stop().unwrap());
    assert_eq!(server.state(), HttpServerState::Stopped);
    for client in [idle, inflight] {
        if let Err(error) = client.join().unwrap() {
            assert!(
                !matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "shutdown closes live connections: {error}"
            );
        }
    }
}

/// Fails each request according to its path: with an error, a read timeout, or a panic in the
/// handler future; or, with `panic_call`, panics in the handler call itself.
struct FailingHandler {
    panic_call: bool,
}

impl HttpHandler for FailingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        assert!(!self.panic_call, "handler call panicked");
        Box::pin(async move {
            let head = request.read_header_event().await?;
            match head.headers().request_url() {
                Some(b"/error") => Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"handler failure",
                )),
                Some(b"/timeout") => Err(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"handler timeout",
                )),
                _ => panic!("handler future panicked"),
            }
        })
    }
}

fn failure_wire(owned_http1: bool, path: &'static str) -> Vec<u8> {
    let factory: HandlerFactory = Arc::new(move |_| {
        Rc::new(FailingHandler {
            panic_call: path == "/panic-call",
        }) as HttpHandlerRef
    });
    let config = ServerConfig {
        session: untimed_session(owned_http1),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, None).expect("server starts");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let wire = plain_request(server.address().unwrap(), request.as_bytes())
        .expect("the failed request is answered");
    normalize_date(&wire)
}

#[test]
fn owned_http1_answers_handler_failures_as_the_default_mode() {
    for (path, status) in [
        ("/error", &b"HTTP/1.1 500 "[..]),
        ("/timeout", &b"HTTP/1.1 408 "[..]),
        ("/panic", &b"HTTP/1.1 500 "[..]),
        ("/panic-call", &b"HTTP/1.1 500 "[..]),
    ] {
        let default_wire = failure_wire(false, path);
        assert!(default_wire.starts_with(status), "{path}");
        assert_eq!(
            String::from_utf8_lossy(&failure_wire(true, path)),
            String::from_utf8_lossy(&default_wire),
            "{path}"
        );
    }
}

/// A handler future that panics when dropped, after it has finished or while it waits.
struct PanicsOnDrop(LocalHandlerFuture);

impl std::future::Future for PanicsOnDrop {
    type Output = <LocalHandlerFuture as std::future::Future>::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(context)
    }
}

impl Drop for PanicsOnDrop {
    fn drop(&mut self) {
        panic!("handler future destructor panicked");
    }
}

/// Responds with `200 OK`, or fails with a read timeout when `timeout` is set, from a future
/// whose destructor panics.
struct DropPanickingHandler {
    timeout: bool,
}

impl HttpHandler for DropPanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let timeout = self.timeout;
        Box::pin(PanicsOnDrop(Box::pin(async move {
            if timeout {
                return Err(HttpError::with_message(
                    HttpErrorCode::READ_TIMEOUT,
                    b"handler timeout",
                ));
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("status and body are valid"),
            )))
        })))
    }
}

fn drop_panic_wire(owned_http1: bool, timeout: bool) -> Vec<u8> {
    let factory: HandlerFactory =
        Arc::new(move |_| Rc::new(DropPanickingHandler { timeout }) as HttpHandlerRef);
    let config = ServerConfig {
        session: untimed_session(owned_http1),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, None).expect("server starts");
    let wire = plain_request(
        server.address().unwrap(),
        b"GET /drop-panic HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .expect("the request is answered");
    normalize_date(&wire)
}

#[test]
fn owned_http1_answers_a_handler_future_drop_panic_as_the_default_mode() {
    let default_wire = drop_panic_wire(false, false);
    assert!(default_wire.starts_with(b"HTTP/1.1 500 "));
    assert_eq!(
        String::from_utf8_lossy(&drop_panic_wire(true, false)),
        String::from_utf8_lossy(&default_wire)
    );
}

#[test]
fn owned_http1_keeps_a_handler_error_when_its_future_drop_panics() {
    let default_wire = drop_panic_wire(false, true);
    assert!(default_wire.starts_with(b"HTTP/1.1 408 "));
    assert_eq!(
        String::from_utf8_lossy(&drop_panic_wire(true, true)),
        String::from_utf8_lossy(&default_wire)
    );
}

/// A response source that holds its handler context and panics when read.
struct PanickingResponse(#[allow(dead_code)] HandlerSessionContext);

impl HttpSource for PanickingResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(async { panic!("response source panicked") })
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(async { panic!("response source panicked") })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// Responds to `/panic` with a response source whose read panics, and to anything else with
/// `200 OK`.
struct ResponsePanicHandler;

impl HttpHandler for ResponsePanicHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if head.headers().request_url() == Some(&b"/panic"[..]) {
                return Ok(HttpSourceHolder::new(Box::new(PanickingResponse(context))));
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("status is valid"),
            )))
        })
    }
}

/// Starts a server on its one worker and sends a request whose response source panics,
/// which tears the connection down without a response.
fn server_after_a_response_panic(owned_http1: bool) -> ScopedHttpServer {
    let factory: HandlerFactory = Arc::new(|_| Rc::new(ResponsePanicHandler) as HttpHandlerRef);
    let config = ServerConfig {
        session: untimed_session(owned_http1),
        ..ServerConfig::default()
    };
    let server = ScopedHttpServer::start(config, factory, None).expect("server starts");
    match plain_request(
        server.address().unwrap(),
        b"GET /panic HTTP/1.1\r\nHost: localhost\r\n\r\n",
    ) {
        Ok(rest) => assert!(rest.is_empty(), "the panicked exchange writes no response"),
        Err(error) => assert!(
            !matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "the panicked connection is torn down: {error}"
        ),
    }
    server
}

#[test]
fn owned_http1_response_source_panic_keeps_the_worker_serving() {
    // `UniplexSession` fails the worker with the panic.
    let server = server_after_a_response_panic(false);
    assert!(server.drain());
    assert!(matches!(
        server.wait_for_stop(),
        Err(HttpServerError::Worker(_))
    ));

    let server = server_after_a_response_panic(true);
    for _ in 0..3 {
        let wire = plain_request(
            server.address().unwrap(),
            b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .expect("the worker serves another connection");
        assert!(wire.starts_with(b"HTTP/1.1 200 OK\r\n"));
    }
    assert!(server.drain());
    assert!(server.wait_for_stop().expect("the worker stops cleanly"));
    assert_eq!(server.state(), HttpServerState::Stopped);
}

#[test]
fn accept_retry_delay_pauses_only_for_errors_that_outlast_one_connection() {
    for errno in [
        libc::ECONNABORTED,
        libc::EINTR,
        libc::EPERM,
        libc::EPROTO,
        libc::ENETUNREACH,
        libc::EHOSTUNREACH,
    ] {
        assert_eq!(
            accept_retry_delay(&std::io::Error::from_raw_os_error(errno)),
            None,
            "errno {errno} belongs to one pending connection, so accept retries at once"
        );
    }
    for errno in [
        libc::EMFILE,
        libc::ENFILE,
        libc::ENOBUFS,
        libc::ENOMEM,
        libc::EINVAL,
    ] {
        assert_eq!(
            accept_retry_delay(&std::io::Error::from_raw_os_error(errno)),
            Some(Duration::from_secs(1)),
            "errno {errno} outlasts one connection, so accept pauses"
        );
    }
    assert_eq!(
        accept_retry_delay(&std::io::Error::other("no errno")),
        Some(Duration::from_secs(1)),
        "an error without an errno pauses"
    );
}

#[cfg(target_os = "linux")]
const DESCRIPTOR_EXHAUSTION_CHILD: &str = "ROXYGEN_DESCRIPTOR_EXHAUSTION_CHILD";

#[cfg(target_os = "linux")]
#[test]
fn listener_keeps_accepting_after_descriptor_exhaustion() {
    if std::env::var_os(DESCRIPTOR_EXHAUSTION_CHILD).is_some() {
        exhaust_descriptors_with_a_queued_connection();
        return;
    }
    // RLIMIT_NOFILE is process-wide, so only this test runs under the lowered limit.
    let child = {
        let _spawn = CHILD_SPAWN.lock().unwrap_or_else(PoisonError::into_inner);
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "lifecycle_tests::listener_keeps_accepting_after_descriptor_exhaustion",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(DESCRIPTOR_EXHAUSTION_CHILD, "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    };
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("test result: ok. 1 passed"),
        "the child runs exactly this test and passes\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
fn exhaust_descriptors_with_a_queued_connection() {
    let server = start_plain(ServerConfig::default(), Arc::new(HandlerStats::default()));
    let address = server.address().unwrap();
    // Connecting an existing socket allocates no descriptor.
    let client = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        None,
    )
    .unwrap();

    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is initialized storage of the type getrlimit writes.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) },
        0
    );
    let lowered = libc::rlimit {
        rlim_cur: limit.rlim_cur.min(512),
        ..limit
    };
    // SAFETY: `lowered` is an initialized rlimit.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const lowered) },
        0
    );
    let mut held = Vec::new();
    let exhausted = loop {
        match std::fs::File::open("/dev/null") {
            Ok(file) => held.push(file),
            Err(error) => break error,
        }
    };
    assert_eq!(exhausted.raw_os_error(), Some(libc::EMFILE));

    client.connect(&address.into()).unwrap();
    // The idle acceptor wakes for the queued connection and fails with EMFILE.
    thread::sleep(Duration::from_millis(200));
    drop(held);
    // SAFETY: `limit` is the initialized rlimit read above.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) },
        0
    );

    let mut queued = TcpStream::from(client);
    queued
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    queued
        .write_all(b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    queued.read_to_end(&mut response).unwrap();
    assert_ok_response(&response);
    assert_ok_response(&get_request(address));
}

/// Events in the pushed `/sse` response.
const PUSHED_EVENTS: usize = 5;
const PUSHED_EVENT_SIZE: usize = 64;
/// Larger than the request window and one read.
const ECHO_BODY_SIZE: usize = 1_000_003;

/// Serves `/sse` and `/echo`. `/sse` pushes its events from a task into a response source
/// without a `Content-Length`, and `/echo` streams the request body back as it arrives, with
/// the request's `Content-Length` if it has one.
struct StreamingHandler {
    producer_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl HttpHandler for StreamingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let producer_tasks = Arc::clone(&self.producer_tasks);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let mut response = roxygen_core::message::HttpMessage::new();
            response.set_status_code(200).unwrap();
            response.set_status_message(b"OK").unwrap();
            response.set_http_version(1, 1);
            if head.headers().request_url() == Some(&b"/echo"[..]) {
                if let Some(length) = head
                    .headers()
                    .headers()
                    .get_single(roxygen_core::headers::HttpHeaderCode::ContentLength)
                    .value
                {
                    response
                        .headers_mut()
                        .set(roxygen_core::headers::HttpHeaderCode::ContentLength, length);
                }
                let head = HttpHeaderEvent::new(response, false).unwrap();
                return Ok(HttpSourceHolder::new(Box::new(
                    roxygen_core::hybrid_source::HttpHybridSource::from_header_event(head, request),
                )));
            }
            assert!(head.eom(), "the pushed response's request has no body");
            drop(request);
            let source =
                roxygen_core::stream_source::HttpStreamSource::new_async(None, None, 65_535)
                    .unwrap();
            let producer = source.producer();
            assert!(producer.headers(response, false));
            let task = tokio::task::spawn_local(async move {
                for sent in 1..=PUSHED_EVENTS {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let body = BufQueue::from_vec(vec![b'e'; PUSHED_EVENT_SIZE], PUSHED_EVENT_SIZE)
                        .unwrap();
                    producer.body(body, 0, sent == PUSHED_EVENTS);
                }
            });
            producer_tasks.lock().unwrap().push(task);
            Ok(HttpSourceHolder::new(Box::new(source)))
        })
    }
}

/// The `/sse` producer tasks of one server's `StreamingHandler`s. A test joins them with
/// `join`; dropping joins any that a failed test left.
#[derive(Default)]
struct ProducerTasks(Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>);

impl ProducerTasks {
    /// Joins every task spawned so far and returns how many there were. A task's panic
    /// resumes here.
    fn join(&self) -> usize {
        let results = self.join_all();
        let count = results.len();
        for result in results {
            if let Err(error) = result {
                assert!(error.is_panic(), "a producer task did not finish: {error}");
                resume_unwind(error.into_panic());
            }
        }
        count
    }

    fn join_all(&self) -> Vec<Result<(), tokio::task::JoinError>> {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        if tasks.is_empty() {
            return Vec::new();
        }
        let runtime = Builder::new_current_thread().build().unwrap();
        tasks
            .into_iter()
            .map(|task| runtime.block_on(task))
            .collect()
    }
}

impl Drop for ProducerTasks {
    fn drop(&mut self) {
        drop(self.join_all());
    }
}

/// A server with 256 KiB stream and 5 MiB connection windows, untimed so both modes accept it.
fn streaming_server(owned_http1: bool, producers: &ProducerTasks) -> ScopedHttpServer {
    let config = ServerConfig {
        session: crate::acceptor::ServerSessionConfig {
            stream_flow_control: 262_144,
            connection_flow_control: 5_242_880,
            ..untimed_session(owned_http1)
        },
        ..ServerConfig::default()
    };
    let producer_tasks = Arc::clone(&producers.0);
    let factory: HandlerFactory = Arc::new(move |_| {
        Rc::new(StreamingHandler {
            producer_tasks: Arc::clone(&producer_tasks),
        }) as HttpHandlerRef
    });
    ScopedHttpServer::start(config, factory, None).expect("server starts")
}

fn streaming_client(server: &ScopedHttpServer) -> TcpStream {
    let stream = TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
    stream.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    stream
}

/// Reads until `wire` ends with `end`; a reset or EOF first fails with what arrived.
fn read_until_end(stream: &mut TcpStream, end: impl Fn(&[u8]) -> bool) -> Vec<u8> {
    let mut wire = Vec::new();
    let mut chunk = [0_u8; 65_536];
    while !end(&wire) {
        match stream.read(&mut chunk) {
            Ok(0) => panic!(
                "EOF after {} response bytes: {:?}",
                wire.len(),
                String::from_utf8_lossy(&wire[..wire.len().min(200)])
            ),
            Ok(count) => wire.extend_from_slice(&chunk[..count]),
            Err(error) => panic!(
                "{error} after {} response bytes: {:?}",
                wire.len(),
                String::from_utf8_lossy(&wire[..wire.len().min(200)])
            ),
        }
    }
    wire
}

/// Requests the pushed response twice on one connection and returns each response's head and
/// decoded body: events the session reads together share a chunk.
fn serve_pushed_response(owned_http1: bool) -> Vec<(String, Vec<u8>)> {
    let producers = ProducerTasks::default();
    let server = streaming_server(owned_http1, &producers);
    let mut stream = streaming_client(&server);
    let mut responses = Vec::new();
    for _ in 0..2 {
        stream
            .write_all(b"GET /sse HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let wire = normalize_date(&read_until_end(&mut stream, |wire| {
            wire.ends_with(b"\r\n0\r\n\r\n")
        }));
        let (head, body) = dechunk(&wire);
        responses.push((String::from_utf8_lossy(head).into_owned(), body));
    }
    assert_eq!(producers.join(), 2, "each response's producer finished");
    drop(stream);
    drop(server);
    responses
}

#[test]
fn owned_http1_serves_a_pushed_chunked_response() {
    let native = serve_pushed_response(false);
    let owned = serve_pushed_response(true);
    for (head, body) in &native {
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head:?}");
        assert!(head.contains("Transfer-Encoding: chunked\r\n"), "{head:?}");
        assert!(*body == [b'e'; PUSHED_EVENTS * PUSHED_EVENT_SIZE]);
    }
    assert_eq!(owned, native);
}

/// Echoes a 1 MB request body, written by another thread while the response is read, and
/// returns the response. A `chunked` request is sent in 64 KiB chunks, and its echo is chunked.
fn serve_large_echo(owned_http1: bool, body: &Arc<Vec<u8>>, chunked: bool) -> Vec<u8> {
    let producers = ProducerTasks::default();
    let server = streaming_server(owned_http1, &producers);
    let mut stream = streaming_client(&server);
    let mut writer = stream.try_clone().unwrap();
    let request_body = Arc::clone(body);
    let sender = thread::spawn(move || {
        if !chunked {
            let head = format!(
                "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
                request_body.len()
            );
            writer.write_all(head.as_bytes())?;
            return writer.write_all(&request_body);
        }
        writer.write_all(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n",
        )?;
        for chunk in request_body.chunks(65_536) {
            writer.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())?;
            writer.write_all(chunk)?;
            writer.write_all(b"\r\n")?;
        }
        writer.write_all(b"0\r\n\r\n")
    });
    let length = body.len();
    let wire = read_until_end(&mut stream, |wire| {
        if chunked {
            return wire.ends_with(b"\r\n0\r\n\r\n");
        }
        wire.windows(4)
            .position(|window| window == b"\r\n\r\n")
            .is_some_and(|end| wire.len() >= end + 4 + length)
    });
    sender.join().unwrap().expect("the request body is sent");
    drop(stream);
    drop(server);
    normalize_date(&wire)
}

fn echo_body() -> Arc<Vec<u8>> {
    Arc::new(
        (0..ECHO_BODY_SIZE)
            .map(|index| (index * 7 % 251) as u8)
            .collect(),
    )
}

/// Splits a chunked response into its head and its decoded body.
fn dechunk(wire: &[u8]) -> (&[u8], Vec<u8>) {
    let end = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response head")
        + 4;
    let (head, mut rest) = wire.split_at(end);
    let mut body = Vec::new();
    loop {
        let line = rest
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("a chunk size line");
        let size = usize::from_str_radix(std::str::from_utf8(&rest[..line]).unwrap(), 16).unwrap();
        rest = &rest[line + 2..];
        if size == 0 {
            assert_eq!(rest, b"\r\n", "nothing follows the last chunk");
            return (head, body);
        }
        body.extend_from_slice(&rest[..size]);
        assert_eq!(&rest[size..size + 2], b"\r\n");
        rest = &rest[size + 2..];
    }
}

#[test]
fn owned_http1_echoes_a_large_body_as_it_arrives() {
    let body = echo_body();
    let native = serve_large_echo(false, &body, false);
    let owned = serve_large_echo(true, &body, false);
    assert!(native.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(native.ends_with(&body[..]));
    assert_eq!(owned.len(), native.len());
    assert!(
        owned == native,
        "the owned echo differs from the native one"
    );
}

/// As `owned_http1_echoes_a_large_body_as_it_arrives`, for a chunked request; the echo's chunk
/// sizes follow the reads, so the bodies are compared decoded.
#[test]
fn owned_http1_echoes_a_large_chunked_body_as_it_arrives() {
    let body = echo_body();
    let native = serve_large_echo(false, &body, true);
    let owned = serve_large_echo(true, &body, true);
    let (native_head, native_body) = dechunk(&native);
    let (owned_head, owned_body) = dechunk(&owned);
    assert!(native_head.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(
        String::from_utf8_lossy(native_head).contains("Transfer-Encoding: chunked\r\n"),
        "{:?}",
        String::from_utf8_lossy(native_head)
    );
    assert_eq!(
        String::from_utf8_lossy(owned_head),
        String::from_utf8_lossy(native_head)
    );
    assert!(
        native_body == body[..],
        "the native echo differs from the request body"
    );
    assert!(
        owned_body == body[..],
        "the owned echo differs from the request body"
    );
}

/// What a `ConnectionLifecycleObserver` callback reported, with the outcome's borrowed fields
/// copied out.
#[derive(Clone, Debug, Eq, PartialEq)]
enum LifecycleRecord {
    Accepted(ConnectionId),
    Established(ConnectionId, CodecProtocol),
    Closed(ConnectionId, RecordedOutcome),
}

/// An owned copy of a `ConnectionOutcome`.
#[derive(Clone, Debug, Eq, PartialEq)]
enum RecordedOutcome {
    TlsFailed(HttpErrorCode),
    Rejected(HttpErrorCode),
    Closed {
        protocol: CodecProtocol,
        handshake_complete: bool,
        result: Result<(), HttpErrorCode>,
    },
    Aborted,
}

impl RecordedOutcome {
    fn copy(outcome: &ConnectionOutcome<'_>) -> Self {
        match outcome {
            ConnectionOutcome::TlsFailed(error) => Self::TlsFailed(error.code()),
            ConnectionOutcome::Rejected(error) => Self::Rejected(error.code()),
            ConnectionOutcome::Closed {
                protocol,
                handshake_complete,
                result,
            } => Self::Closed {
                protocol: *protocol,
                handshake_complete: *handshake_complete,
                result: result.as_ref().map(drop).map_err(HttpError::code),
            },
            ConnectionOutcome::Aborted => Self::Aborted,
        }
    }
}

/// Where a connection lifecycle callback ran: its thread, and the Tokio task it ran in, if any.
#[derive(Clone, Copy, Debug)]
struct CallbackContext {
    thread: thread::ThreadId,
    task: Option<tokio::task::Id>,
}

/// Records every connection lifecycle callback and where it ran.
#[derive(Default)]
struct LifecycleRecorder {
    records: Mutex<Vec<(LifecycleRecord, CallbackContext)>>,
}

impl LifecycleRecorder {
    fn record(&self, record: LifecycleRecord) {
        let context = CallbackContext {
            thread: thread::current().id(),
            task: tokio::task::try_id(),
        };
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((record, context));
    }

    fn records(&self) -> Vec<LifecycleRecord> {
        self.records_with_context()
            .into_iter()
            .map(|(record, _)| record)
            .collect()
    }

    fn records_with_context(&self) -> Vec<(LifecycleRecord, CallbackContext)> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn count(&self, matches: impl Fn(&LifecycleRecord) -> bool) -> usize {
        self.records()
            .iter()
            .filter(|record| matches(record))
            .count()
    }

    fn accepted(&self) -> usize {
        self.count(|record| matches!(record, LifecycleRecord::Accepted(_)))
    }

    fn closed(&self) -> usize {
        self.count(|record| matches!(record, LifecycleRecord::Closed(..)))
    }

    /// Waits up to `REQUEST_TIMEOUT` for `expected` closed callbacks.
    fn wait_closed(&self, expected: usize) -> bool {
        wait_until(|| self.closed() >= expected)
    }
}

impl ConnectionLifecycleObserver for LifecycleRecorder {
    fn on_socket_accepted(&self, id: ConnectionId) {
        self.record(LifecycleRecord::Accepted(id));
    }

    fn on_protocol_established(&self, id: ConnectionId, protocol: CodecProtocol) {
        self.record(LifecycleRecord::Established(id, protocol));
    }

    fn on_socket_closed(&self, id: ConnectionId, outcome: &ConnectionOutcome<'_>) {
        self.record(LifecycleRecord::Closed(id, RecordedOutcome::copy(outcome)));
    }
}

/// A one-worker server configuration without signal handlers that reports to `recorder`.
fn lifecycle_config(recorder: &Arc<LifecycleRecorder>) -> ServerConfig {
    ServerConfig {
        num_io_threads: 1,
        shutdown_on_signals: Vec::new(),
        lifecycle_observer: Some(Arc::clone(recorder) as Arc<dyn ConnectionLifecycleObserver>),
        ..ServerConfig::default()
    }
}

/// A GET that closes its connection.
const CLOSE_REQUEST: &[u8] = b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

/// The first connection on worker 0.
const FIRST_CONNECTION: ConnectionId = ConnectionId {
    worker: 0,
    serial: 1,
};

#[test]
fn lifecycle_balances_on_tls_failure() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let (options, _) = generated_tls();
    let config = ServerConfig {
        tls_credentials: tls_config(options).tls_credentials,
        ..lifecycle_config(&recorder)
    };
    let server = start_tls(config, Arc::new(HandlerStats::default()), None);

    // Plaintext HTTP where a ClientHello belongs fails the handshake.
    let _ = plain_request(server.address().unwrap(), CLOSE_REQUEST);
    assert!(recorder.wait_closed(1), "{:?}", recorder.records());

    let records = recorder.records();
    assert_eq!(
        records.len(),
        2,
        "one accepted and one closed callback: {records:?}"
    );
    assert_eq!(records[0], LifecycleRecord::Accepted(FIRST_CONNECTION));
    assert!(
        matches!(
            records[1],
            LifecycleRecord::Closed(FIRST_CONNECTION, RecordedOutcome::TlsFailed(_))
        ),
        "{records:?}"
    );
    assert_eq!(server.control().accept_stats()[0].accepted, 1);
}

/// A one-worker plaintext owned HTTP/2 server that reports to `recorder`.
fn owned_http2_lifecycle_server(recorder: &Arc<LifecycleRecorder>) -> ScopedHttpServer {
    let config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        session: untimed_owned_session(false, true),
        ..lifecycle_config(recorder)
    };
    start_plain(config, Arc::new(HandlerStats::default()))
}

#[test]
fn lifecycle_reports_stalled_preface_without_protocol_established() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let server = owned_http2_lifecycle_server(&recorder);

    // The client preface without the SETTINGS frame that completes the handshake.
    let mut client =
        TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
    client
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .unwrap();
    assert!(wait_until(|| recorder.accepted() == 1));
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        recorder.records(),
        [LifecycleRecord::Accepted(FIRST_CONNECTION)],
        "a stalled preface establishes no protocol"
    );

    drop(client);
    assert!(recorder.wait_closed(1), "{:?}", recorder.records());
    let records = recorder.records();
    assert_eq!(records.len(), 2, "{records:?}");
    assert!(
        matches!(
            records[1],
            LifecycleRecord::Closed(
                FIRST_CONNECTION,
                RecordedOutcome::Closed {
                    protocol: CodecProtocol::Http2,
                    handshake_complete: false,
                    ..
                }
            )
        ),
        "{records:?}"
    );
}

#[test]
fn lifecycle_reports_closed_h2_with_handshake() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let server = owned_http2_lifecycle_server(&recorder);

    assert_eq!(h2_plain_request(server.address().unwrap()), b"OK");
    assert!(recorder.wait_closed(1), "{:?}", recorder.records());
    let records = recorder.records();
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(records[0], LifecycleRecord::Accepted(FIRST_CONNECTION));
    assert_eq!(
        records[1],
        LifecycleRecord::Established(FIRST_CONNECTION, CodecProtocol::Http2)
    );
    // The client's close ends the run, with or without an error.
    assert!(
        matches!(
            records[2],
            LifecycleRecord::Closed(
                FIRST_CONNECTION,
                RecordedOutcome::Closed {
                    protocol: CodecProtocol::Http2,
                    handshake_complete: true,
                    ..
                }
            )
        ),
        "{records:?}"
    );
}

#[test]
fn lifecycle_reports_rejected_connection() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let config = ServerConfig {
        new_connection_filter: Some(Arc::new(|_: &ConnectionInfo| false)),
        ..lifecycle_config(&recorder)
    };
    let server = start_plain(config, Arc::new(HandlerStats::default()));

    let _ = plain_request(server.address().unwrap(), CLOSE_REQUEST);
    assert!(recorder.wait_closed(1), "{:?}", recorder.records());
    assert_eq!(
        recorder.records(),
        [
            LifecycleRecord::Accepted(FIRST_CONNECTION),
            LifecycleRecord::Closed(
                FIRST_CONNECTION,
                RecordedOutcome::Rejected(HttpErrorCode::CONNECT_ERROR)
            ),
        ]
    );
}

/// Starts a server whose handlers are `stats`'s, and returns it with each worker's handle and
/// thread, in worker index order.
fn start_with_workers(
    config: ServerConfig,
    stats: &Arc<HandlerStats>,
) -> (HttpServer, Vec<(WorkerHandle, thread::ThreadId)>) {
    let workers = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&workers);
    let factory = handler_factory(Arc::clone(stats), false);
    let server = HttpServer::start_with_worker_factory(
        config,
        Arc::new(move |context: WorkerContext, index| {
            captured.lock().unwrap().push((
                index,
                context.handle().clone(),
                thread::current().id(),
            ));
            factory(index)
        }),
        None,
    )
    .unwrap();
    let mut workers = std::mem::take(&mut *workers.lock().unwrap());
    workers.sort_by_key(|(index, _, _)| *index);
    let workers = workers
        .into_iter()
        .map(|(_, handle, thread)| (handle, thread))
        .collect();
    (server, workers)
}

/// Blocks `worker`'s thread, so it polls none of its tasks, until the returned sender is
/// dropped.
fn stall_worker(worker: &WorkerHandle) -> mpsc::Sender<()> {
    let (started, running) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    worker
        .submit(move |_| {
            let _ = started.send(());
            let _ = released.recv();
        })
        .expect("the worker admits the blocking command");
    running
        .recv_timeout(REQUEST_TIMEOUT)
        .expect("the worker runs the blocking command");
    release
}

/// Completes a TLS handshake and one keep-alive GET, and returns the open connection.
fn tls_keep_alive_request(
    address: SocketAddr,
    certificate: &[u8],
) -> StreamOwned<ClientConnection, TcpStream> {
    let socket = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap();
    socket.set_read_timeout(Some(REQUEST_TIMEOUT)).unwrap();
    let mut stream = StreamOwned::new(tls_client(certificate), socket);
    stream
        .write_all(b"GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !(response.ends_with(b"\r\n\r\nOK") || response.ends_with(b"\r\n0\r\n\r\n")) {
        let read = stream.read(&mut chunk).unwrap();
        assert_ne!(read, 0, "the server closed the keep-alive connection");
        response.extend_from_slice(&chunk[..read]);
    }
    assert_ok_response(&response);
    stream
}

/// Asserts, for each worker, `accepted = sessions started + handoff_dropped +
/// closed_at_force_stop + pending_handoff`, counting the sessions it started from `recorder`'s
/// accepted callbacks.
fn assert_accept_balance(server: &HttpServer, recorder: &LifecycleRecorder) {
    for (worker, stats) in server.control().accept_stats().iter().enumerate() {
        let started = recorder.count(|record| {
            matches!(record, LifecycleRecord::Accepted(id) if id.worker as usize == worker)
        }) as u64;
        assert_eq!(
            stats.accepted,
            started + stats.handoff_dropped + stats.closed_at_force_stop + stats.pending_handoff,
            "worker {worker}: {started} sessions started, {stats:?}"
        );
    }
}

#[test]
fn lifecycle_balances_on_force_stop() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let (options, certificate) = generated_tls();
    let mut config = ServerConfig {
        num_io_threads: 2,
        session: untimed_owned_session(true, false),
        tls_credentials: tls_config(options).tls_credentials,
        ..lifecycle_config(&recorder)
    };
    config.socket.accept_distribution = AcceptDistribution::Worker0;
    let (server, workers) = start_with_workers(config, &Arc::new(HandlerStats::default()));
    let address = server.address().unwrap();
    let stall = stall_worker(&workers[1].0);

    // Worker 0 hands sockets to workers 0, 1, 0 in turn: a served keep-alive session, a socket
    // queued behind the stalled worker 1, and a client that never sends its ClientHello.
    let _served = tls_keep_alive_request(address, &certificate);
    let _queued = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT).unwrap();
    let _silent = SilentClient::connect(address);
    assert!(
        wait_until(
            || recorder.accepted() == 2 && server.control().accept_stats()[1].pending_handoff == 1
        ),
        "{:?} {:?}",
        recorder.records(),
        server.control().accept_stats()
    );
    let mid_handshake = ConnectionId {
        worker: 0,
        serial: 2,
    };
    assert_eq!(
        recorder.records(),
        [
            LifecycleRecord::Accepted(FIRST_CONNECTION),
            LifecycleRecord::Established(FIRST_CONNECTION, CodecProtocol::Http11),
            LifecycleRecord::Accepted(mid_handshake),
        ]
    );

    // Worker 1 sees the forced stop before it can dequeue its socket.
    assert!(server.control().force_stop());
    drop(stall);
    assert!(server.force_stop().unwrap());

    let mut closed: Vec<_> = recorder.records()[3..].to_vec();
    closed.sort_by_key(|record| match record {
        LifecycleRecord::Closed(id, _) => id.serial,
        _ => u64::MAX,
    });
    assert_eq!(
        closed,
        [
            LifecycleRecord::Closed(FIRST_CONNECTION, RecordedOutcome::Aborted),
            LifecycleRecord::Closed(mid_handshake, RecordedOutcome::Aborted),
        ],
        "both sessions report Aborted once"
    );
    let stats = server.control().accept_stats();
    assert_eq!(stats[1].closed_at_force_stop, 1, "{stats:?}");
    assert_accept_balance(&server, &recorder);
}

#[test]
fn lifecycle_balances_when_force_stop_cancels_unstarted_session_task() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let (spawned, spawn_seen) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let config = ServerConfig {
        session_spawned_for_test: Some(Arc::new(move || {
            let _ = spawned.send(());
            let _ = released
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recv();
        })),
        ..lifecycle_config(&recorder)
    };
    let server = start_plain(config, Arc::new(HandlerStats::default()));

    let _client = TcpStream::connect_timeout(&server.address().unwrap(), REQUEST_TIMEOUT).unwrap();
    spawn_seen
        .recv_timeout(REQUEST_TIMEOUT)
        .expect("the worker spawns the connection's session task");
    // The worker sees the forced stop before it polls the session task it just spawned.
    assert!(server.control().force_stop());
    drop(release);
    assert!(server.force_stop().unwrap());

    assert_accept_balance(&server, &recorder);
    let stats = server.control().accept_stats();
    assert_eq!(
        stats[0].closed_at_force_stop, 1,
        "the socket of a session task that never started is closed unsessionized: {stats:?}"
    );
    assert_eq!(stats[0].pending_handoff, 0, "{stats:?}");
    assert_eq!(
        recorder.records(),
        [],
        "a session task that never started reports nothing"
    );
}

/// Counts each connection lifecycle callback, then panics.
#[derive(Default)]
struct PanickingLifecycleObserver {
    accepted: AtomicUsize,
    established: AtomicUsize,
    closed: AtomicUsize,
}

impl ConnectionLifecycleObserver for PanickingLifecycleObserver {
    fn on_socket_accepted(&self, _id: ConnectionId) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
        panic!("accepted callback panic");
    }

    fn on_protocol_established(&self, _id: ConnectionId, _protocol: CodecProtocol) {
        self.established.fetch_add(1, Ordering::Relaxed);
        panic!("established callback panic");
    }

    fn on_socket_closed(&self, _id: ConnectionId, _outcome: &ConnectionOutcome<'_>) {
        self.closed.fetch_add(1, Ordering::Relaxed);
        panic!("closed callback panic");
    }
}

#[test]
fn lifecycle_observer_panic_is_contained() {
    let observer = Arc::new(PanickingLifecycleObserver::default());
    let config = ServerConfig {
        num_io_threads: 1,
        shutdown_on_signals: Vec::new(),
        session: untimed_session(true),
        lifecycle_observer: Some(Arc::clone(&observer) as Arc<dyn ConnectionLifecycleObserver>),
        ..ServerConfig::default()
    };
    let server = start_plain(config, Arc::new(HandlerStats::default()));

    // Every callback panics, and the worker serves both connections in full.
    for _ in 0..2 {
        assert_ok_response(&get_request(server.address().unwrap()));
    }
    assert_eq!(wait_for_count(&observer.closed, 2), 2);
    assert_eq!(observer.accepted.load(Ordering::Relaxed), 2);
    assert_eq!(observer.established.load(Ordering::Relaxed), 2);
    assert!(
        matches!(
            server.force_stop(),
            Err(HttpServerError::CallbackPanic {
                worker_index: 0,
                stage: ServerCallbackStage::ConnectionLifecycleObserver,
            })
        ),
        "the first panic is reported at stop"
    );
}

#[test]
fn lifecycle_hooks_fire_on_serving_worker_in_both_accept_modes() {
    const CONNECTIONS: usize = 32;
    let mut modes = vec![AcceptDistribution::Worker0];
    if cfg!(unix) {
        modes.push(AcceptDistribution::PerWorkerReusePort);
    }
    for mode in modes {
        let recorder = Arc::new(LifecycleRecorder::default());
        let mut config = ServerConfig {
            num_io_threads: 2,
            session: untimed_session(true),
            ..lifecycle_config(&recorder)
        };
        config.socket.accept_distribution = mode;
        config.socket.reuse_port = mode == AcceptDistribution::PerWorkerReusePort;
        let (server, workers) = start_with_workers(config, &Arc::new(HandlerStats::default()));
        for _ in 0..CONNECTIONS {
            assert_ok_response(&get_request(server.address().unwrap()));
        }
        assert!(
            recorder.wait_closed(CONNECTIONS),
            "{mode:?}: {:?}",
            recorder.records()
        );

        let mut connections = BTreeMap::<(u32, u64), Vec<LifecycleRecord>>::new();
        let mut tasks = BTreeMap::<(u32, u64), Vec<Option<tokio::task::Id>>>::new();
        for (record, context) in recorder.records_with_context() {
            let (LifecycleRecord::Accepted(id)
            | LifecycleRecord::Established(id, _)
            | LifecycleRecord::Closed(id, _)) = record.clone();
            assert_eq!(
                context.thread, workers[id.worker as usize].1,
                "{mode:?}: {record:?} runs on the thread of the worker it names"
            );
            tasks
                .entry((id.worker, id.serial))
                .or_default()
                .push(context.task);
            connections
                .entry((id.worker, id.serial))
                .or_default()
                .push(record);
        }
        assert_eq!(connections.len(), CONNECTIONS, "{mode:?}: {connections:?}");
        // Every callback for a connection runs in one task, which serves no other connection:
        // the connection's session task.
        let session_tasks: HashSet<_> = tasks
            .iter()
            .map(|(connection, tasks)| {
                assert!(
                    tasks[0].is_some() && tasks.iter().all(|task| *task == tasks[0]),
                    "{mode:?}: every callback for connection {connection:?} runs in one task: \
                     {tasks:?}"
                );
                tasks[0]
            })
            .collect();
        assert_eq!(
            session_tasks.len(),
            CONNECTIONS,
            "{mode:?}: each connection's callbacks run in a task of their own: {tasks:?}"
        );
        for ((worker, serial), records) in &connections {
            let id = ConnectionId {
                worker: *worker,
                serial: *serial,
            };
            assert_eq!(
                records[..2],
                [
                    LifecycleRecord::Accepted(id),
                    LifecycleRecord::Established(id, CodecProtocol::Http11),
                ],
                "{mode:?}"
            );
            assert!(
                matches!(
                    records[2..],
                    [LifecycleRecord::Closed(
                        _,
                        RecordedOutcome::Closed {
                            protocol: CodecProtocol::Http11,
                            handshake_complete: true,
                            ..
                        }
                    )]
                ),
                "{mode:?}: {records:?}"
            );
        }
        for worker in 0..2_u32 {
            let served = connections.keys().filter(|(w, _)| *w == worker).count() as u64;
            assert!(served > 0, "{mode:?}: worker {worker} serves connections");
            assert!(
                connections
                    .keys()
                    .filter(|(w, _)| *w == worker)
                    .map(|(_, serial)| *serial)
                    .eq(1..=served),
                "{mode:?}: worker {worker} numbers its connections from 1"
            );
        }
        assert_accept_balance(&server, &recorder);
        assert!(server.force_stop().unwrap());
    }
}

#[test]
fn lifecycle_classic_session_reports_no_handshake() {
    let recorder = Arc::new(LifecycleRecorder::default());
    let server = start_plain(
        lifecycle_config(&recorder),
        Arc::new(HandlerStats::default()),
    );

    assert_ok_response(&get_request(server.address().unwrap()));
    assert!(recorder.wait_closed(1), "{:?}", recorder.records());
    let records = recorder.records();
    assert_eq!(records.len(), 2, "no protocol is established: {records:?}");
    assert_eq!(records[0], LifecycleRecord::Accepted(FIRST_CONNECTION));
    assert!(
        matches!(
            records[1],
            LifecycleRecord::Closed(
                FIRST_CONNECTION,
                RecordedOutcome::Closed {
                    protocol: CodecProtocol::Http11,
                    handshake_complete: false,
                    ..
                }
            )
        ),
        "{records:?}"
    );
}
