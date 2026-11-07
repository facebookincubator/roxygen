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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::time::Duration;

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

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::TlsHandshakeObserver;
use crate::acceptor::http2_session_config;
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

fn tls_request(
    address: SocketAddr,
    certificate: &[u8],
    request: &[u8],
) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate.to_vec()))
        .unwrap();
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connection = ClientConnection::new(
        Arc::new(config),
        ServerName::try_from("localhost".to_owned()).unwrap(),
    )
    .unwrap();
    let socket = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT)?;
    socket.set_read_timeout(Some(REQUEST_TIMEOUT))?;
    socket.set_write_timeout(Some(REQUEST_TIMEOUT))?;
    let mut stream = StreamOwned::new(connection, socket);
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
                    let mut config = ClientConfig::builder()
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
}

impl TlsHandshakeObserver for HandshakeObserver {
    fn on_handshake_success(&self) {
        self.accepts.fetch_add(1, Ordering::Relaxed);
    }

    fn on_success(&self, _connection: &ConnectionInfo) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn on_error(&self, _error: &HttpError) {
        self.errors.fetch_add(1, Ordering::Relaxed);
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

#[test]
fn shutdown() {
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
    let mut client_config = ClientConfig::builder()
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

    // Transport tests establish backing retention across kernel completion. This
    // server-level oracle covers response progress and bounded forced shutdown.
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
fn http2_first_initial_window_setting_matches_frozen_server_precedence() {
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

    fn r3_s(&self, session_id: u64) -> Result<&SessionRunOutcome, SessionRetirementEvidenceError> {
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
            self.r3_s(session)
                .expect("every selected session has actual R3-S evidence");
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
            .r3_s(1)
            .expect("first session was actually reaped and its backend retired");
        let first_completion = classify_failed_operation(first_outcome);
        eprintln!(
            "native-session known failed first operation R3-S classification: {first_completion:?}; actual outcome: {first_outcome:?}"
        );
        assert!(
            first_completion.is_some(),
            "actual outer panic is distinct from forced or unclassified outer cancellation"
        );
        trace.drain_available();
        assert_session_worker_running(&server, &observer);
        assert!(
            matches!(
                trace.r3_s(2),
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
        assert_healthy_session_outcome(trace.r3_s(2).expect("second session reaches actual R3-S"));
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
            trace.r3_s(1),
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
        assert_healthy_session_outcome(trace.r3_s(1).expect("drained session reaches actual R3-S"));
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
/// completion receipt and a response proposer.
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
/// and returns every response byte and each handler context's receipt and proposer.
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
    // The owned driver gives the handler no completion receipt or response proposer
    // (`DIV-029`).
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

/// Answers `/gated` only once released; answers every other request at once.
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

/// Fails each request as its path says: with an error, a read timeout, or a panic in the
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

/// Answers `200 OK`, or with `timeout` fails with a read timeout, from a future whose
/// destructor panics.
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

/// Answers `/panic` with a response source whose read panics, anything else `200 OK`.
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
/// which tears the connection down with no response.
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
    // `UniplexSession` fails the worker with the panic (`DIV-029`).
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
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "lifecycle_tests::listener_keeps_accepting_after_descriptor_exhaustion",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(DESCRIPTOR_EXHAUSTION_CHILD, "1")
        .output()
        .unwrap();
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
