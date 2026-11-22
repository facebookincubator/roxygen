/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpServer` serving HTTP/3 on its QUIC listener, end to end over loopback with the test
//! client of `h3_client`.

use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use mvfstr_proto::AppCode;
use mvfstr_proto::CloseReason;
use mvfstr_proto::TransportErrorCode;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_h3::H3ApplicationBuilder;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;
use roxygen_transport::tls::TlsAlpnMismatch;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::TlsServerSettings;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::pki_types::ServerName;
use rustls::time_provider::DefaultTimeProvider;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

use crate::OwnedApplicationFactory;
use crate::OwnedAttached;
use crate::OwnedConnectionAttach;
use crate::OwnedConnectionFactory;
use crate::StopAbandon;
use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionId;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::ConnectionLifecycleObserver;
use crate::acceptor::ConnectionOutcome;
use crate::acceptor::ServerSessionConfig;
use crate::h3_client::H3_NO_ERROR;
use crate::h3_client::H3Client;
use crate::h3_client::H3Conn;
use crate::h3_client::Request;
use crate::h3_client::Seen;
use crate::h3_client::WAIT;
use crate::h3_client::localhost_certificate;
use crate::quic::Http3Attached;
use crate::quic::Http3ConnectionAttach;
use crate::quic::QuicListenerConfig;
use crate::quic::QuicTls;
use crate::server::HttpServer;
use crate::server::HttpServerError;
use crate::server::ServerCallbackStage;
use crate::server::ServerConfig;
use crate::server::StaticTlsCredentials;

fn internal(error: impl std::fmt::Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

fn respond(status: u16, body: Vec<u8>) -> Result<HttpSourceHolder, HttpError> {
    let body = BufQueue::from_vec(body, usize::MAX).map_err(internal)?;
    let response = HttpFixedSource::make_fixed_response(status, Some(body)).map_err(internal)?;
    Ok(HttpSourceHolder::new(Box::new(response)))
}

/// A handler that answers 200 with the request's method, path and body.
struct Echo;

impl HttpHandler for Echo {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let mut head = request.read_header_event().await?;
            let mut eom = head.eom();
            let message = head.headers_mut();
            let method = message.method_string().map_err(internal)?.to_owned();
            let path = String::from_utf8_lossy(message.path().map_err(internal)?).into_owned();
            let mut body = format!("{method} {path} ").into_bytes();
            while !eom {
                let event = request.read_body_event(u32::MAX).await?;
                eom = event.eom();
                match event.into_kind() {
                    HttpBodyEventKind::Body(data) => {
                        for segment in data.segments() {
                            body.extend_from_slice(segment);
                        }
                    }
                    HttpBodyEventKind::Suspend(resume) => {
                        resume.await;
                    }
                    _ => {}
                }
            }
            respond(200, body)
        })
    }
}

fn echo_handlers() -> crate::server::HandlerFactory {
    Arc::new(|_| Rc::new(Echo) as HttpHandlerRef)
}

/// Returns the config of a QUIC listener on `bind_addresses` with a fresh `localhost` certificate.
pub(crate) fn quic_listener(bind_addresses: Vec<SocketAddr>) -> QuicListenerConfig {
    let (chain, key) = localhost_certificate();
    QuicListenerConfig {
        bind_addresses,
        ..QuicListenerConfig::new(QuicTls::Certificate { chain, key })
    }
}

fn start(quic: QuicListenerConfig, workers: usize) -> HttpServer {
    let config = ServerConfig {
        num_io_threads: workers,
        shutdown_on_signals: Vec::new(),
        quic: Some(quic),
        ..ServerConfig::default()
    };
    HttpServer::start(config, echo_handlers(), None).expect("the server starts")
}

/// Returns a loopback UDP address whose port was free a moment ago.
pub(crate) fn free_udp_address() -> SocketAddr {
    UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|socket| socket.local_addr())
        .expect("a free UDP port")
}

#[test]
fn a_get_over_http3_on_an_explicit_quic_address_reaches_the_handler() {
    let addr = free_udp_address();
    let server = start(quic_listener(vec![addr]), 1);
    let client = H3Client::new();
    let conn = client.connect(addr);
    let response = conn.fetch(Request::get("/hello"));
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"GET /hello ");
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn a_quic_address_already_taken_fails_the_start() {
    let taken = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a UDP socket");
    let addr = taken.local_addr().expect("its address");
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    match HttpServer::start(config, echo_handlers(), None) {
        Err(HttpServerError::Socket(error)) => assert_eq!(error.kind(), io::ErrorKind::AddrInUse),
        Err(other) => panic!("the start failed otherwise: {other}"),
        Ok(_) => panic!("the server started without its QUIC socket"),
    }
}

#[test]
fn invalid_http3_settings_fail_the_start_as_a_quic_listener_error() {
    let config = ServerConfig {
        num_io_threads: 2,
        shutdown_on_signals: Vec::new(),
        quic: Some(QuicListenerConfig {
            h3: H3ApplicationBuilder::new().max_concurrent_exchanges(0),
            ..quic_listener(vec![free_udp_address()])
        }),
        ..ServerConfig::default()
    };
    match HttpServer::start(config, echo_handlers(), None) {
        Err(HttpServerError::Quic(message)) => {
            assert!(message.contains("max_concurrent_exchanges"), "{message}");
        }
        Err(other) => panic!("the start failed otherwise: {other}"),
        Ok(_) => panic!("the server started with invalid HTTP/3 settings"),
    }
}

/// Returns whether `seen` is a close with the server's no_application_protocol TLS alert, which
/// RFC 9001 requires when the endpoints do not negotiate an application protocol.
fn no_application_protocol(seen: &Seen) -> bool {
    matches!(
        seen,
        Seen::Closed(CloseReason::Transport { code, .. }) if *code == TransportErrorCode::crypto(120)
    )
}

#[test]
fn a_supplied_tls_config_serves_only_h3_whatever_alpn_it_lists() {
    let addr = free_udp_address();
    let (chain, key) = localhost_certificate();
    let tls = mvfstr_tls::server_config(
        rustls::crypto::ring::default_provider(),
        Arc::new(DefaultTimeProvider),
        chain,
        key,
        &[b"hq-interop", b"h3"],
    )
    .expect("a QUIC TLS config");
    let server = start(
        QuicListenerConfig {
            bind_addresses: vec![addr],
            ..QuicListenerConfig::new(QuicTls::Config(Arc::new(tls)))
        },
        1,
    );
    let client = H3Client::new();
    let response = client.connect(addr).fetch(Request::get("/h3"));
    assert_eq!(response.body, b"GET /h3 ");
    let hq = H3Client::with_alpn(&[b"hq-interop"]);
    let seen = hq
        .connect(addr)
        .await_seen(|seen| matches!(seen, Seen::Closed(_)));
    assert!(
        seen.last().is_some_and(no_application_protocol),
        "a client without h3 is turned away: {seen:?}"
    );
    server.force_stop().expect("the server stops");
    hq.shutdown();
    client.shutdown();
}

#[test]
fn an_unusable_quic_certificate_fails_the_start_as_a_quic_listener_error() {
    let (chain, _) = localhost_certificate();
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(QuicListenerConfig {
            bind_addresses: vec![free_udp_address()],
            ..QuicListenerConfig::new(QuicTls::Certificate {
                chain,
                key: PrivatePkcs8KeyDer::from(vec![0; 16]).into(),
            })
        }),
        ..ServerConfig::default()
    };
    match HttpServer::start(config, echo_handlers(), None) {
        Err(HttpServerError::Quic(_)) => {}
        Err(other) => panic!("the start failed otherwise: {other}"),
        Ok(_) => panic!("the server started with an unusable key"),
    }
}

#[test]
fn every_quic_address_serves_http3() {
    let addresses = vec![
        free_udp_address(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    ];
    let server = start(quic_listener(addresses), 2);
    let bound = server.quic_addresses().to_vec();
    assert_eq!(bound.len(), 2, "{bound:?}");
    let client = H3Client::new();
    for addr in bound {
        let response = client.connect(addr).fetch(Request::get("/"));
        assert_eq!(response.body, b"GET / ", "{addr}");
    }
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A driver's shutdown line: the worker, the address, then the turns, packets and datagrams.
type ShutdownLine = (usize, SocketAddr, u64, u64, u64);

fn shutdown_line(line: &str) -> Option<ShutdownLine> {
    let rest = line.strip_prefix("roxygen_server: QUIC worker ")?;
    let (worker, rest) = rest.split_once(" on ")?;
    let (address, rest) = rest.split_once(": ")?;
    let (turns, rest) = rest.split_once(" turns, ")?;
    let (packets, rest) = rest.split_once(" packets and ")?;
    let datagrams = rest.strip_suffix(" datagrams sent")?;
    Some((
        worker.parse().ok()?,
        address.parse().ok()?,
        turns.parse().ok()?,
        packets.parse().ok()?,
        datagrams.parse().ok()?,
    ))
}

/// Returns what a shutdown log kept, as text.
fn logged(log: &Mutex<Vec<u8>>) -> String {
    let bytes = log.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    String::from_utf8(bytes.clone()).expect("the log is UTF-8")
}

#[test]
fn each_worker_logs_the_counters_of_its_quic_drivers_at_shutdown() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let addresses = vec![
        free_udp_address(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    ];
    let quic = QuicListenerConfig {
        shutdown_log: log.clone(),
        ..quic_listener(addresses)
    };
    let server = start(quic, 2);
    let bound = server.quic_addresses().to_vec();
    let client = H3Client::new();
    for &addr in &bound {
        let response = client.connect(addr).fetch(Request::get("/"));
        assert_eq!(response.status, 200, "{addr}");
    }
    server.force_stop().expect("the server stops");
    client.shutdown();

    let text = logged(&log);
    let lines: Vec<ShutdownLine> = text
        .lines()
        .map(|line| shutdown_line(line).unwrap_or_else(|| panic!("a shutdown line: {line:?}")))
        .collect();
    let drivers: BTreeSet<(usize, SocketAddr)> = lines
        .iter()
        .map(|&(worker, address, ..)| (worker, address))
        .collect();
    let expected: BTreeSet<(usize, SocketAddr)> = (0..2)
        .flat_map(|worker| bound.iter().map(move |&address| (worker, address)))
        .collect();
    assert_eq!(
        (lines.len(), drivers),
        (expected.len(), expected),
        "one line for each worker's driver on each address: {text:?}"
    );
    // Each address served a request, so its drivers' last counters, summed over the workers,
    // show turns and sends, and every datagram carries at least one packet.
    for &addr in &bound {
        let (turns, packets, datagrams) = lines
            .iter()
            .filter(|&&(_, address, ..)| address == addr)
            .fold(
                (0, 0, 0),
                |(t, p, d), &(_, _, turns, packets, datagrams)| {
                    (t + turns, p + packets, d + datagrams)
                },
            );
        assert!(
            turns > 0 && datagrams > 0 && packets >= datagrams,
            "the counters of the drivers on {addr}: {text:?}"
        );
    }
}

/// A shutdown log whose first write fails, as a write to a pipe whose reader went away does; it
/// keeps what the later writes carry.
struct BrokenOnce {
    failed: bool,
    kept: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for BrokenOnce {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !std::mem::replace(&mut self.failed, true) {
            return Err(io::Error::from(io::ErrorKind::BrokenPipe));
        }
        self.kept
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_shutdown_line_that_fails_to_write_is_lost_without_failing_the_worker() {
    let kept = Arc::new(Mutex::new(Vec::new()));
    let addresses = vec![
        free_udp_address(),
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    ];
    let quic = QuicListenerConfig {
        shutdown_log: Arc::new(Mutex::new(BrokenOnce {
            failed: false,
            kept: Arc::clone(&kept),
        })),
        ..quic_listener(addresses)
    };
    let server = start(quic, 1);
    let second = server.quic_addresses()[1];
    let stopped = server.force_stop();

    let text = logged(&kept);
    assert!(
        matches!(stopped, Ok(true)),
        "the server stops cleanly: {stopped:?}; the log kept {text:?}"
    );
    let lines: Vec<ShutdownLine> = text.lines().filter_map(shutdown_line).collect();
    assert!(
        matches!(lines[..], [(0, address, ..)] if address == second),
        "the first driver's line is lost and the second's still written: {text:?}"
    );
}

#[test]
fn a_post_over_http3_delivers_its_body_to_the_handler() {
    let addr = free_udp_address();
    let server = start(quic_listener(vec![addr]), 1);
    let client = H3Client::new();
    let conn = client.connect(addr);
    // Past the default stream credit of a request body, so the upload waits for credit.
    let body: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
    let response = conn.fetch(Request::post("/upload", body.clone()));
    assert_eq!(response.status, 200);
    let mut expected = b"POST /upload ".to_vec();
    expected.extend_from_slice(&body);
    assert!(response.body == expected, "the echo matches the upload");
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// Returns server settings with a TLS listener offering h2 and HTTP/1.1 and a QUIC listener on
/// its addresses, both with one `localhost` certificate, and that certificate as a root store.
fn tls_and_quic() -> (ServerConfig, RootCertStore) {
    let generated =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("a certificate");
    let certificate = generated.cert.der().to_vec();
    let key = generated.signing_key.serialized_der().to_vec();
    let tcp = TlsServerOptions::from_settings(
        TlsServerSettings::new(
            vec![certificate.clone().into_boxed_slice()],
            key.clone().into_boxed_slice(),
        )
        .with_alpn(
            vec![b"h2".to_vec().into(), b"http/1.1".to_vec().into()],
            TlsAlpnMismatch::ContinueWithoutAlpn,
        ),
    )
    .expect("TLS options");
    let quic = QuicListenerConfig::new(QuicTls::Certificate {
        chain: vec![CertificateDer::from(certificate.clone())],
        key: PrivatePkcs8KeyDer::from(key).into(),
    });
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate))
        .expect("the certificate is a root");
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tcp))),
        quic: Some(quic),
        ..ServerConfig::default()
    };
    (config, roots)
}

/// Sends a GET for `path` to `addr` over TLS with ALPN `h2`, and returns the response's status
/// and body.
fn h2_get(addr: SocketAddr, roots: RootCertStore, path: &str) -> (u16, Vec<u8>) {
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("TLS versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let uri = format!("https://localhost{path}");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a client runtime")
        .block_on(async move {
            tokio::time::timeout(WAIT, async move {
                let tcp = tokio::net::TcpStream::connect(addr)
                    .await
                    .expect("a TCP connection");
                let tls = TlsConnector::from(Arc::new(config))
                    .connect(ServerName::try_from("localhost").expect("a DNS name"), tcp)
                    .await
                    .expect("a TLS handshake");
                assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
                let (sender, connection) = h2::client::handshake(tls).await.expect("h2 handshake");
                tokio::spawn(connection);
                let mut sender = sender.ready().await.expect("stream capacity");
                let request = http::Request::builder()
                    .uri(uri)
                    .body(())
                    .expect("a request");
                let (response, _) = sender.send_request(request, true).expect("request sent");
                let response = response.await.expect("a response head");
                let status = response.status().as_u16();
                let mut body = response.into_body();
                let mut bytes = Vec::new();
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.expect("response body");
                    let _ = body.flow_control().release_capacity(chunk.len());
                    bytes.extend_from_slice(&chunk);
                }
                (status, bytes)
            })
            .await
            .expect("the h2 exchange finishes")
        })
}

#[test]
fn http3_answers_on_the_tcp_listeners_port_beside_h2_over_tls() {
    let (config, roots) = tls_and_quic();
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let addr = server.address().expect("one TCP listener");
    let (status, body) = h2_get(addr, roots, "/over-tcp");
    assert_eq!((status, body.as_slice()), (200, &b"GET /over-tcp "[..]));
    let client = H3Client::new();
    let response = client.connect(addr).fetch(Request::get("/over-quic"));
    assert_eq!(
        (response.status, response.body.as_slice()),
        (200, &b"GET /over-quic "[..])
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A handler that answers 200 with the index of the worker that serves the request.
struct WorkerIndex(usize);

impl HttpHandler for WorkerIndex {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let index = self.0;
        Box::pin(async move {
            request.read_header_event().await?;
            respond(200, index.to_string().into_bytes())
        })
    }
}

#[test]
fn many_concurrent_requests_on_several_connections_spread_over_the_workers() {
    const WORKERS: usize = 4;
    const CONNECTIONS: usize = 8;
    // Past the 100 request streams a connection may open at once.
    const REQUESTS: usize = 150;
    let addr = free_udp_address();
    let config = ServerConfig {
        num_io_threads: WORKERS,
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let handlers: crate::server::HandlerFactory =
        Arc::new(|index| Rc::new(WorkerIndex(index)) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None).expect("the server starts");
    let client = H3Client::new();
    let conns: Vec<_> = (0..CONNECTIONS).map(|_| client.connect(addr)).collect();
    let pending: Vec<_> = conns
        .iter()
        .flat_map(|conn| (0..REQUESTS).map(|i| conn.send(Request::get(&format!("/{i}")))))
        .collect();
    let mut workers = BTreeSet::new();
    for response in pending {
        let response = response
            .recv_timeout(WAIT)
            .expect("a response arrives")
            .expect("the request succeeds");
        assert_eq!(response.status, 200);
        workers.insert(String::from_utf8(response.body).expect("a worker index"));
    }
    assert!(
        workers.len() > 1,
        "the connections spread over the workers: {workers:?}"
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn new_connections_spread_exactly_evenly_over_the_workers() {
    const WORKERS: usize = 4;
    const CONNECTIONS: usize = 16;
    let addr = free_udp_address();
    let config = ServerConfig {
        num_io_threads: WORKERS,
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let handlers: crate::server::HandlerFactory =
        Arc::new(|index| Rc::new(WorkerIndex(index)) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None).expect("the server starts");
    let client = H3Client::new();
    // Each connection has a UDP socket of its own, which the kernel's hash alone would spread
    // unevenly.
    let conns: Vec<_> = (0..CONNECTIONS).map(|_| client.connect(addr)).collect();
    let mut served = [0; WORKERS];
    for conn in &conns {
        let response = conn.fetch(Request::get("/"));
        let worker: usize = String::from_utf8(response.body)
            .ok()
            .and_then(|body| body.parse().ok())
            .expect("a worker index");
        served[worker] += 1;
    }
    assert_eq!(
        served,
        [CONNECTIONS / WORKERS; WORKERS],
        "connections served per worker"
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A handler that counts the requests whose heads arrived and answers each only once released.
struct Held {
    started: Arc<AtomicUsize>,
    release: watch::Receiver<bool>,
}

impl HttpHandler for Held {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = Arc::clone(&self.started);
        let mut release = self.release.clone();
        Box::pin(async move {
            request.read_header_event().await?;
            started.fetch_add(1, Ordering::SeqCst);
            let _ = release.wait_for(|released| *released).await;
            respond(200, b"released".to_vec())
        })
    }
}

/// Returns handlers that hold their responses until `release` is sent `true`, counting the
/// requests they hold in `started`.
fn held_handlers(
    started: &Arc<AtomicUsize>,
    release: &watch::Receiver<bool>,
) -> crate::server::HandlerFactory {
    let (started, release) = (Arc::clone(started), release.clone());
    Arc::new(move |_| {
        Rc::new(Held {
            started: Arc::clone(&started),
            release: release.clone(),
        }) as HttpHandlerRef
    })
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "still waiting until {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

pub(crate) fn closed_without_error(seen: &Seen) -> bool {
    matches!(
        seen,
        Seen::Closed(CloseReason::Application { code, .. }) if *code == AppCode::from_u32(H3_NO_ERROR)
    )
}

#[test]
fn a_drain_sends_goaway_and_lets_the_requests_in_flight_finish() {
    const REQUESTS: usize = 4;
    let addr = free_udp_address();
    let started = Arc::new(AtomicUsize::new(0));
    let (release, released) = watch::channel(false);
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, held_handlers(&started, &released), None)
        .expect("the server starts");
    let client = H3Client::new();
    let conn = client.connect(addr);
    let pending: Vec<_> = (0..REQUESTS)
        .map(|i| conn.send(Request::get(&format!("/{i}"))))
        .collect();
    wait_until("the handler holds every request", || {
        started.load(Ordering::SeqCst) == REQUESTS
    });
    assert!(server.drain());
    // The four request streams are 0, 4, 8 and 12; the GOAWAY carries the next one.
    conn.await_seen(|seen| *seen == Seen::Goaway(16));
    release.send_replace(true);
    for response in pending {
        let response = response
            .recv_timeout(WAIT)
            .expect("a response arrives")
            .expect("the request succeeds");
        assert_eq!(
            (response.status, response.body.as_slice()),
            (200, &b"released"[..])
        );
    }
    conn.await_seen(closed_without_error);
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

/// A handler that answers with the index of the worker that serves the request and the length of
/// the request's body. It counts the POST requests whose heads arrived in `uploads`, and records
/// its worker's index in `ended` when the worker ends and drops it.
struct Upload {
    index: usize,
    uploads: Arc<AtomicUsize>,
    ended: Arc<Mutex<Vec<usize>>>,
}

impl HttpHandler for Upload {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let index = self.index;
        let uploads = Arc::clone(&self.uploads);
        Box::pin(async move {
            let mut head = request.read_header_event().await?;
            let mut eom = head.eom();
            if head.headers_mut().method_string().map_err(internal)? == "POST" {
                uploads.fetch_add(1, Ordering::SeqCst);
            }
            let mut length = 0;
            while !eom {
                let event = request.read_body_event(u32::MAX).await?;
                eom = event.eom();
                match event.into_kind() {
                    HttpBodyEventKind::Body(data) => {
                        length += data.segments().map(|segment| segment.len()).sum::<usize>();
                    }
                    HttpBodyEventKind::Suspend(resume) => {
                        resume.await;
                    }
                    _ => {}
                }
            }
            respond(200, format!("{index} {length}").into_bytes())
        })
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        self.ended
            .lock()
            .expect("the record lock is sound")
            .push(self.index);
    }
}

#[test]
fn a_drain_keeps_every_worker_reachable_until_the_last_worker_ends() {
    const WORKERS: usize = 4;
    const CONNECTIONS: usize = 16;
    let addr = free_udp_address();
    let uploads = Arc::new(AtomicUsize::new(0));
    let ended = Arc::new(Mutex::new(Vec::new()));
    let handlers: crate::server::HandlerFactory = {
        let (uploads, ended) = (Arc::clone(&uploads), Arc::clone(&ended));
        Arc::new(move |index| {
            Rc::new(Upload {
                index,
                uploads: Arc::clone(&uploads),
                ended: Arc::clone(&ended),
            }) as HttpHandlerRef
        })
    };
    let config = ServerConfig {
        num_io_threads: WORKERS,
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, handlers, None).expect("the server starts");
    let client = H3Client::new();
    let conns: Vec<(H3Conn, usize)> = (0..CONNECTIONS)
        .map(|_| {
            let conn = client.connect(addr);
            let body = conn.fetch(Request::get("/")).body;
            let worker = String::from_utf8(body)
                .ok()
                .and_then(|body| body.split(' ').next()?.parse().ok())
                .expect("a worker index");
            (conn, worker)
        })
        .collect();
    // Worker 0 loses its connections, so that it ends first in the drain while the others
    // still serve uploads. Its socket is the first in the port's SO_REUSEPORT group: Linux fills
    // the slot of a socket that leaves with the group's last one and spreads flows over one
    // socket fewer, so that socket leaving would move most flows of the others.
    for (conn, _) in conns.iter().filter(|(_, worker)| *worker == 0) {
        conn.close();
    }
    let busy: Vec<_> = conns
        .iter()
        .filter(|(_, worker)| *worker != 0)
        .map(|(conn, worker)| {
            let (request, tail) = Request::unfinished_post("/upload", vec![1; 1000]);
            (conn, *worker, conn.send(request), tail)
        })
        .collect();
    wait_until("every upload has started", || {
        uploads.load(Ordering::SeqCst) == busy.len()
    });
    assert!(server.drain());
    wait_until("worker 0 has ended", || {
        ended.lock().expect("the record lock is sound").contains(&0)
    });
    for (conn, _, _, tail) in &busy {
        conn.finish_body(tail, vec![2; 50_000]);
    }
    for (_, worker, response, _) in busy {
        let response = response
            .recv_timeout(WAIT)
            .expect("a response arrives")
            .expect("the upload succeeds");
        assert_eq!(response.body, format!("{worker} 51000").into_bytes());
    }
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

#[test]
fn a_drain_grace_closes_the_connections_still_busy_when_it_ends() {
    let addr = free_udp_address();
    let started = Arc::new(AtomicUsize::new(0));
    let (_release, released) = watch::channel(false);
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(QuicListenerConfig {
            drain_grace: Duration::from_millis(200),
            ..quic_listener(vec![addr])
        }),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, held_handlers(&started, &released), None)
        .expect("the server starts");
    let client = H3Client::new();
    let conn = client.connect(addr);
    let held = conn.send(Request::get("/held"));
    wait_until("the handler holds the request", || {
        started.load(Ordering::SeqCst) == 1
    });
    assert!(server.drain());
    let seen = conn.await_seen(|seen| matches!(seen, Seen::Closed(_)));
    assert!(
        matches!(
            seen.last(),
            Some(Seen::Closed(CloseReason::Transport { code, .. }))
                if *code == TransportErrorCode::NO_ERROR
        ),
        "the grace ends with a close without error: {seen:?}"
    );
    assert!(held.recv_timeout(WAIT).expect("the request ends").is_err());
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

/// A guard that adds one to a shared count while it lives.
struct Live(Arc<AtomicUsize>);

impl Live {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(count))
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One worker's factory of an owned application that serves HTTP/3 only.
struct H3Connections {
    /// The count of live connection applications.
    live: Arc<AtomicUsize>,
    /// The value of `live` at each `worker_stopping` call.
    stopping: Arc<Mutex<Vec<usize>>>,
    serves_http3: bool,
}

impl OwnedConnectionFactory for H3Connections {
    fn attach(&self, _connection: OwnedConnectionAttach<'_>) -> Result<OwnedAttached, HttpError> {
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"this application serves HTTP/3 only",
        ))
    }

    fn attach_http3(
        &self,
        connection: Http3ConnectionAttach<'_>,
    ) -> Result<Http3Attached, HttpError> {
        if !self.serves_http3 {
            return Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"this worker refuses HTTP/3",
            ));
        }
        assert_eq!(connection.info().protocol, CodecProtocol::Http3);
        assert!(connection.worker().is_owner_thread());
        Ok(connection.serve(OwnedOk {
            _live: Live::new(&self.live),
        }))
    }

    fn worker_stopping(&self, _abandon: StopAbandon) -> Option<Pin<Box<dyn Future<Output = ()>>>> {
        self.stopping
            .lock()
            .expect("the record lock is sound")
            .push(self.live.load(Ordering::SeqCst));
        None
    }
}

/// A connection's application: it answers `200 owned`, except that `/hold` is never answered.
struct OwnedOk {
    _live: Live,
}

impl OwnedApplication for OwnedOk {
    type Exchange = OwnedOkExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> OwnedOkExchange {
        OwnedOkExchange {
            request: None,
            held: false,
        }
    }
}

struct OwnedOkExchange {
    request: Option<HttpStreamSourceOwner>,
    held: bool,
}

impl OwnedExchange for OwnedOkExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self.held = head.request_url().unwrap_or_default() == b"/hold";
        self.request = Some(body);
    }

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if self.held {
            return Poll::Pending;
        }
        let mut message = HttpMessage::new();
        message.set_status_code(200).map_err(internal)?;
        message.set_http_version(1, 1);
        Poll::Ready(HttpHeaderEvent::new(message, false).map_err(internal))
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        let body = BufQueue::from_bytes(Bytes::from_static(b"owned"), 5).map_err(internal)?;
        Poll::Ready(Ok(HttpBodyEvent::body(body, true)))
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

/// Starts an owned application whose workers serve HTTP/3 on `addr` if `serves_http3`, and
/// returns the server, its count of live connection applications, and that count at each
/// worker's `worker_stopping` call.
fn start_owned_http3(
    addr: SocketAddr,
    serves_http3: bool,
) -> (HttpServer, Arc<AtomicUsize>, Arc<Mutex<Vec<usize>>>) {
    let live = Arc::new(AtomicUsize::new(0));
    let stopping = Arc::new(Mutex::new(Vec::new()));
    let factory: OwnedApplicationFactory = {
        let (live, stopping) = (Arc::clone(&live), Arc::clone(&stopping));
        Arc::new(move |_, _| {
            Ok(Rc::new(H3Connections {
                live: Arc::clone(&live),
                stopping: Arc::clone(&stopping),
                serves_http3,
            }) as Rc<dyn OwnedConnectionFactory>)
        })
    };
    let config = ServerConfig {
        session: ServerSessionConfig {
            stream_read_timeout: Duration::ZERO,
            connection_idle_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            owned_http1: true,
            owned_http2: true,
            ..ServerSessionConfig::default()
        },
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start_owned(config, factory, None).expect("the server starts");
    (server, live, stopping)
}

#[test]
fn an_owned_application_serves_http3_through_attach_http3() {
    let addr = free_udp_address();
    let (server, _live, _stopping) = start_owned_http3(addr, true);
    let client = H3Client::new();
    let response = client.connect(addr).fetch(Request::get("/"));
    assert_eq!(
        (response.status, response.body.as_slice()),
        (200, &b"owned"[..])
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

fn refused(seen: &Seen) -> bool {
    matches!(
        seen,
        Seen::Closed(CloseReason::Transport { code, .. })
            if *code == TransportErrorCode::CONNECTION_REFUSED
    )
}

/// An owned application that keeps the default `attach_http3`.
struct TcpOnly;

impl OwnedConnectionFactory for TcpOnly {
    fn attach(&self, _connection: OwnedConnectionAttach<'_>) -> Result<OwnedAttached, HttpError> {
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"no TCP connection arrives in this test",
        ))
    }
}

#[test]
fn an_owned_application_without_attach_http3_refuses_quic_connections() {
    let addr = free_udp_address();
    let factory: OwnedApplicationFactory =
        Arc::new(|_, _| Ok(Rc::new(TcpOnly) as Rc<dyn OwnedConnectionFactory>));
    let config = ServerConfig {
        session: ServerSessionConfig {
            stream_read_timeout: Duration::ZERO,
            connection_idle_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            owned_http1: true,
            owned_http2: true,
            ..ServerSessionConfig::default()
        },
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start_owned(config, factory, None).expect("the server starts");
    let client = H3Client::new();
    client.connect(addr).await_seen(refused);
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn a_forced_stop_ends_every_http3_connection_before_worker_stopping() {
    let addr = free_udp_address();
    let (server, live, stopping) = start_owned_http3(addr, true);
    let client = H3Client::new();
    let conn = client.connect(addr);
    let _held = conn.send(Request::get("/hold"));
    wait_until("the connection's application exists", || {
        live.load(Ordering::SeqCst) == 1
    });
    server.force_stop().expect("the server stops");
    assert_eq!(
        *stopping.lock().expect("the record lock is sound"),
        [0],
        "worker_stopping runs once no HTTP/3 connection is left"
    );
    client.shutdown();
}

/// A lifecycle observer that records each callback as text.
struct LifecycleLog(Arc<Mutex<Vec<String>>>);

impl LifecycleLog {
    fn push(&self, event: String) {
        self.0.lock().expect("the log lock is sound").push(event);
    }
}

impl ConnectionLifecycleObserver for LifecycleLog {
    fn on_socket_accepted(&self, id: ConnectionId) {
        self.push(format!("{}:{} accepted", id.worker, id.serial));
    }

    fn on_protocol_established(&self, id: ConnectionId, protocol: CodecProtocol) {
        self.push(format!(
            "{}:{} established {protocol:?}",
            id.worker, id.serial
        ));
    }

    fn on_socket_closed(&self, id: ConnectionId, outcome: &ConnectionOutcome<'_>) {
        let outcome = match outcome {
            ConnectionOutcome::Closed {
                protocol,
                handshake_complete,
                result,
            } => format!(
                "closed {protocol:?} handshake {handshake_complete} ok {}",
                result.is_ok()
            ),
            ConnectionOutcome::Rejected(_) => "rejected".to_owned(),
            ConnectionOutcome::TlsFailed(_) => "tls failed".to_owned(),
            ConnectionOutcome::Aborted => "aborted".to_owned(),
        };
        self.push(format!("{}:{} {outcome}", id.worker, id.serial));
    }
}

#[test]
fn the_lifecycle_observer_follows_http3_connections_numbered_with_the_tcp_ones() {
    let (mut config, roots) = tls_and_quic();
    let log = Arc::new(Mutex::new(Vec::new()));
    config.lifecycle_observer = Some(Arc::new(LifecycleLog(Arc::clone(&log))));
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let addr = server.address().expect("one TCP listener");
    let entries = |prefix: &str| -> Vec<String> {
        log.lock()
            .expect("the log lock is sound")
            .iter()
            .filter(|event| event.starts_with(prefix))
            .cloned()
            .collect()
    };
    h2_get(addr, roots, "/over-tcp");
    wait_until("the TCP connection closes", || entries("0:1 ").len() == 2);
    let client = H3Client::new();
    let conn = client.connect(addr);
    conn.fetch(Request::get("/over-quic"));
    conn.close();
    wait_until("the QUIC connection closes", || entries("0:2 ").len() == 3);
    assert_eq!(
        entries("0:2 "),
        [
            "0:2 accepted",
            "0:2 established Http3",
            "0:2 closed Http3 handshake true ok true",
        ]
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn the_connection_filter_sees_http3_connections_and_can_refuse_them() {
    let addr = free_udp_address();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let filter = {
        let seen = Arc::clone(&seen);
        move |connection: &ConnectionInfo| {
            seen.lock().expect("the record lock is sound").push((
                connection.protocol,
                connection.secure,
                connection.local_address.clone(),
            ));
            false
        }
    };
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        new_connection_filter: Some(Arc::new(filter)),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let client = H3Client::new();
    client.connect(addr).await_seen(refused);
    assert_eq!(
        *seen.lock().expect("the record lock is sound"),
        [(
            CodecProtocol::Http3,
            true,
            SocketAddress::new("127.0.0.1", addr.port())
        )]
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// An accepted-connection observer that records each callback as text.
struct ObserverLog(Arc<Mutex<Vec<String>>>);

impl AcceptedConnectionObserver for ObserverLog {
    fn on_accepted(&self, connection: &ConnectionInfo) {
        self.0.lock().expect("the log lock is sound").push(format!(
            "accepted {:?} secure {}",
            connection.protocol, connection.secure
        ));
    }

    fn on_registered(&self) {
        self.0
            .lock()
            .expect("the log lock is sound")
            .push("registered".to_owned());
    }
}

#[test]
fn the_connection_observer_sees_http3_connections_accepted_and_registered() {
    let addr = free_udp_address();
    let log = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        connection_observer: Some(Arc::new(ObserverLog(Arc::clone(&log)))),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let client = H3Client::new();
    client.connect(addr).fetch(Request::get("/"));
    assert_eq!(
        *log.lock().expect("the log lock is sound"),
        ["accepted Http3 secure true", "registered"]
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

/// A connection observer that panics in one of its callbacks.
struct PanicsIn(&'static str);

impl AcceptedConnectionObserver for PanicsIn {
    fn on_accepted(&self, _connection: &ConnectionInfo) {
        assert_ne!(self.0, "on_accepted", "the observer panics");
    }

    fn on_registered(&self) {
        assert_ne!(self.0, "on_registered", "the observer panics");
    }
}

#[test]
fn a_panicking_connection_observer_refuses_the_connection_and_fails_the_stop() {
    for callback in ["on_accepted", "on_registered"] {
        let addr = free_udp_address();
        let config = ServerConfig {
            shutdown_on_signals: Vec::new(),
            connection_observer: Some(Arc::new(PanicsIn(callback))),
            quic: Some(quic_listener(vec![addr])),
            ..ServerConfig::default()
        };
        let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
        let client = H3Client::new();
        client.connect(addr).await_seen(refused);
        match server.force_stop() {
            Err(HttpServerError::CallbackPanic {
                worker_index: 0,
                stage: ServerCallbackStage::AcceptedConnectionObserver,
            }) => {}
            other => panic!("{callback}: the stop reports the panic: {other:?}"),
        }
        client.shutdown();
    }
}

#[test]
fn a_quic_address_with_port_zero_takes_one_port_that_every_worker_serves() {
    const WORKERS: usize = 4;
    let config = ServerConfig {
        num_io_threads: WORKERS,
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            0,
        ))])),
        ..ServerConfig::default()
    };
    let handlers: crate::server::HandlerFactory =
        Arc::new(|index| Rc::new(WorkerIndex(index)) as HttpHandlerRef);
    let server = HttpServer::start(config, handlers, None).expect("the server starts");
    let &[addr] = server.quic_addresses() else {
        panic!("one QUIC address: {:?}", server.quic_addresses());
    };
    assert_ne!(addr.port(), 0);
    let client = H3Client::new();
    let workers: BTreeSet<_> = (0..8)
        .map(|_| client.connect(addr).fetch(Request::get("/")).body)
        .collect();
    assert!(
        workers.len() > 1,
        "several workers serve the port: {workers:?}"
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn a_dual_stack_quic_address_serves_ipv4_clients() {
    let server = start(
        quic_listener(vec![SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))]),
        1,
    );
    let &[addr] = server.quic_addresses() else {
        panic!("one QUIC address: {:?}", server.quic_addresses());
    };
    let client = H3Client::new();
    let response = client
        .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, addr.port())))
        .fetch(Request::get("/over-ipv4"));
    assert_eq!(response.body, b"GET /over-ipv4 ");
    server.force_stop().expect("the server stops");
    client.shutdown();
}

// Quiesce over the real driver. A session's first callback is always the `poll_session` of the
// turn that accepted it, and a quiesce starts at a later turn, so a drain never reaches a session
// before its first callback.

#[test]
fn a_drain_closes_an_idle_connection_after_its_goaway() {
    let addr = free_udp_address();
    let server = start(quic_listener(vec![addr]), 1);
    let client = H3Client::new();
    let conn = client.connect(addr);
    conn.await_seen(|seen| *seen == Seen::Settings);
    assert!(server.drain());
    let seen = conn.await_seen(closed_without_error);
    assert_eq!(seen[0], Seen::Goaway(0), "saw {seen:?}");
    assert_eq!(seen.len(), 2, "saw {seen:?}");
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

#[test]
fn a_drain_finishes_when_the_client_closes_after_the_goaway() {
    let addr = free_udp_address();
    let started = Arc::new(AtomicUsize::new(0));
    let (_release, released) = watch::channel(false);
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, held_handlers(&started, &released), None)
        .expect("the server starts");
    let client = H3Client::new();
    let conn = client.connect(addr);
    let held = conn.send(Request::get("/held"));
    wait_until("the handler holds the request", || {
        started.load(Ordering::SeqCst) == 1
    });
    assert!(server.drain());
    conn.await_seen(|seen| *seen == Seen::Goaway(4));
    conn.close();
    assert!(held.recv_timeout(WAIT).expect("the request ends").is_err());
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

#[test]
fn a_drain_after_the_client_closed_its_connection_finishes() {
    let addr = free_udp_address();
    let server = start(quic_listener(vec![addr]), 1);
    let client = H3Client::new();
    let conn = client.connect(addr);
    conn.fetch(Request::get("/"));
    conn.close();
    conn.await_seen(|seen| matches!(seen, Seen::Closed(_)));
    assert!(server.drain());
    server.wait_for_stop().expect("the drain finishes");
    client.shutdown();
}

/// A UDP relay between clients and a server that holds every client datagram after the first
/// until `release` is called.
struct HoldingRelay {
    addr: SocketAddr,
    released: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl HoldingRelay {
    fn new(server: SocketAddr) -> Self {
        let front = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a relay socket");
        let back = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a relay socket");
        back.connect(server)
            .expect("the relay addresses the server");
        for socket in [&front, &back] {
            socket
                .set_read_timeout(Some(Duration::from_millis(5)))
                .expect("a read timeout");
        }
        let addr = front.local_addr().expect("the relay's address");
        let (front, back) = (Arc::new(front), Arc::new(back));
        let client = Arc::new(Mutex::new(None));
        let released = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let upstream = {
            let (front, back, client) =
                (Arc::clone(&front), Arc::clone(&back), Arc::clone(&client));
            let (released, stop) = (Arc::clone(&released), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut buffer = vec![0; 65536];
                let mut held: Vec<Vec<u8>> = Vec::new();
                let mut first = true;
                while !stop.load(Ordering::SeqCst) {
                    let open = released.load(Ordering::SeqCst);
                    if open {
                        for datagram in held.drain(..) {
                            let _ = back.send(&datagram);
                        }
                    }
                    let Ok((len, from)) = front.recv_from(&mut buffer) else {
                        continue;
                    };
                    *client.lock().expect("the client lock is sound") = Some(from);
                    if first || open {
                        first = false;
                        let _ = back.send(&buffer[..len]);
                    } else {
                        held.push(buffer[..len].to_vec());
                    }
                }
            })
        };
        let downstream = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut buffer = vec![0; 65536];
                while !stop.load(Ordering::SeqCst) {
                    let Ok(len) = back.recv(&mut buffer) else {
                        continue;
                    };
                    let to = *client.lock().expect("the client lock is sound");
                    if let Some(to) = to {
                        let _ = front.send_to(&buffer[..len], to);
                    }
                }
            })
        };
        Self {
            addr,
            released,
            stop,
            threads: vec![upstream, downstream],
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

impl Drop for HoldingRelay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[test]
fn a_drain_before_the_peers_transport_parameters_still_sends_goaway_and_closes() {
    let addr = free_udp_address();
    let log = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        lifecycle_observer: Some(Arc::new(LifecycleLog(Arc::clone(&log)))),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let relay = HoldingRelay::new(addr);
    let client = H3Client::with_large_client_hello();
    // The relay passes the first half of the ClientHello and holds the half with the
    // transport parameters.
    let conn = client.connect(relay.addr);
    wait_until("the server accepts the connection", || {
        !log.lock().expect("the log lock is sound").is_empty()
    });
    assert!(server.drain());
    // A refused probe shows that the quiesce, and with it the held session's `on_quiesce`, has
    // started. A probe that arrives before the drain reaches the driver closes after its GOAWAY.
    let probe = H3Client::new();
    let mut probes = 0;
    while !probe
        .connect(addr)
        .await_seen(|seen| matches!(seen, Seen::Closed(_)))
        .last()
        .is_some_and(refused)
    {
        probes += 1;
        assert!(probes < 10, "the quiesce starts");
    }
    assert_eq!(
        conn.seen_now(),
        [],
        "the client cannot finish its handshake before the relay passes the rest of its ClientHello"
    );
    relay.release();
    let seen = conn.await_seen(closed_without_error);
    assert!(seen.contains(&Seen::Settings), "saw {seen:?}");
    assert!(seen.contains(&Seen::Goaway(0)), "saw {seen:?}");
    server.wait_for_stop().expect("the drain finishes");
    probe.shutdown();
    client.shutdown();
}

#[test]
fn a_refused_http3_connection_reports_rejected_to_the_lifecycle_observer() {
    let addr = free_udp_address();
    let log = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        new_connection_filter: Some(Arc::new(|_: &ConnectionInfo| false)),
        lifecycle_observer: Some(Arc::new(LifecycleLog(Arc::clone(&log)))),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let client = H3Client::new();
    client.connect(addr).await_seen(refused);
    assert_eq!(
        *log.lock().expect("the log lock is sound"),
        ["0:1 accepted", "0:1 rejected"]
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn a_failed_tls_handshake_reports_tls_failed_to_the_lifecycle_observer() {
    let addr = free_udp_address();
    let log = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        lifecycle_observer: Some(Arc::new(LifecycleLog(Arc::clone(&log)))),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let client = H3Client::with_alpn(&[b"hq-interop"]);
    client.connect(addr).await_seen(no_application_protocol);
    wait_until("the connection is reported closed", || {
        log.lock().expect("the log lock is sound").len() == 2
    });
    assert_eq!(
        *log.lock().expect("the log lock is sound"),
        ["0:1 accepted", "0:1 tls failed"]
    );
    server.force_stop().expect("the server stops");
    client.shutdown();
}

#[test]
fn a_forced_stop_reports_its_http3_connections_aborted_to_the_lifecycle_observer() {
    let addr = free_udp_address();
    let log = Arc::new(Mutex::new(Vec::new()));
    let config = ServerConfig {
        shutdown_on_signals: Vec::new(),
        lifecycle_observer: Some(Arc::new(LifecycleLog(Arc::clone(&log)))),
        quic: Some(quic_listener(vec![addr])),
        ..ServerConfig::default()
    };
    let server = HttpServer::start(config, echo_handlers(), None).expect("the server starts");
    let client = H3Client::new();
    client.connect(addr).fetch(Request::get("/"));
    server.force_stop().expect("the server stops");
    assert_eq!(
        *log.lock().expect("the log lock is sound"),
        ["0:1 accepted", "0:1 established Http3", "0:1 aborted"]
    );
    client.shutdown();
}
