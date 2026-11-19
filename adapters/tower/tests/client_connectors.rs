/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http::Response;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper_util::client::legacy::connect::Connection;
use hyper_util::client::legacy::connect::HttpConnector;
use roxygen_client::pool::PoolError;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::http::CodecProtocol;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::runtime::Handle;

use crate::client::BoxError;
use crate::client::ClientError;
use crate::client::connector::ConnectRequest;
use crate::client::connector::ConnectedIo;
use crate::client::connector::ConnectorMetadata;
use crate::client::connector::ConnectorPolicy;
use crate::client::connector::HyperConnector;
use crate::client::owner::TestApplicationJoin;
use crate::client::owner::TestLatch;
use crate::client_tests::Gate;
use crate::client_tests::Handler;
use crate::client_tests::Io;
use crate::client_tests::OriginalError;
use crate::client_tests::TestBody;
use crate::client_tests::TestConnector;
use crate::client_tests::body_bytes;
use crate::client_tests::case;
use crate::client_tests::full;
use crate::client_tests::options;
use crate::client_tests::probe_connector;
use crate::client_tests::producer_reaped;
use crate::client_tests::route;
use crate::client_tests::serve_peer;
use crate::route::ProtocolPolicy;
use crate::route::ProxyEndpoint;
use crate::route::RouteSpec;

struct AffinityFuture {
    app: tokio::runtime::Id,
    polled: Gate,
    drops: Arc<AtomicUsize>,
}
impl Future for AffinityFuture {
    type Output = Result<ConnectedIo<Io>, BoxError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(Handle::current().id(), self.app);
        self.polled.open();
        Poll::Pending
    }
}
impl Drop for AffinityFuture {
    fn drop(&mut self) {
        assert_eq!(Handle::current().id(), self.app);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn connector_readiness_future_and_drop_stay_on_application_runtime() {
    case(|fixture| {
        Box::pin(async move {
            let app = Handle::current().id();
            let polled = Gate::new();
            let signal = polled.clone();
            let drops = Arc::new(AtomicUsize::new(0));
            let captured = Arc::clone(&drops);
            let readiness = Gate::new();
            let ready = readiness.clone();
            let mut connector = TestConnector::new(move |_| {
                Box::pin(AffinityFuture {
                    app,
                    polled: signal.clone(),
                    drops: Arc::clone(&captured),
                })
            });
            connector.ready = Some(Box::pin(async move { ready.wait().await }));
            let events = Arc::clone(&connector.events);
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &route,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            fixture.worker.call(|_| ()).unwrap().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            readiness.open();
            polled.wait().await;
            fixture.owners.last().unwrap().shutdown();
            drop(future);
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            let events = events.lock().unwrap();
            assert!(events.iter().any(|(phase, _)| *phase == "ready"));
            assert!(events.iter().any(|(phase, _)| *phase == "connector-drop"));
            assert!(events.iter().all(|(_, runtime)| *runtime == app));
        })
    });
}

fn tls_metadata<I: Connection>(_: &ConnectRequest, io: &I) -> Result<ConnectorMetadata, BoxError> {
    Ok(ConnectorMetadata {
        protocol: if io.connected().is_negotiated_h2() {
            CodecProtocol::Http2
        } else {
            CodecProtocol::Http11
        },
        security: TransportSecurity::Tls,
        local_endpoint: TransportEndpoint::Unknown,
        peer_endpoint: TransportEndpoint::Unknown,
        peer_identity: None,
    })
}

fn plaintext_metadata<I: Connection>(
    _: &ConnectRequest,
    _: &I,
) -> Result<ConnectorMetadata, BoxError> {
    Ok(ConnectorMetadata {
        protocol: CodecProtocol::Http11,
        security: TransportSecurity::Plaintext,
        local_endpoint: TransportEndpoint::Unknown,
        peer_endpoint: TransportEndpoint::Unknown,
        peer_identity: None,
    })
}

#[test]
fn hyper_tls_timeout_stack_adopts_with_real_h1_h2_alpn() {
    for kind in 0..3 {
        tls_failure_case(kind);
    }
    case(|fixture| {
        Box::pin(async move {
            let _ = rustls::crypto::ring::default_provider().install_default();
            for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
                let generated =
                    rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
                let certificate = generated.cert.der().clone();
                let key = rustls::pki_types::PrivateKeyDer::try_from(
                    generated.signing_key.serialized_der().to_vec(),
                )
                .unwrap();
                let mut server = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![certificate.clone()], key)
                    .unwrap();
                let alpn = if protocol == CodecProtocol::Http2 {
                    b"h2".to_vec()
                } else {
                    b"http/1.1".to_vec()
                };
                server.alpn_protocols = vec![alpn.clone()];
                let mut roots = rustls::RootCertStore::empty();
                roots.add(certificate).unwrap();
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let connector = hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config(config)
                    .https_only()
                    .enable_http1()
                    .enable_http2()
                    .build();
                let mut connector = hyper_timeout::TimeoutConnector::new(connector);
                connector.set_connect_timeout(Some(Duration::from_secs(2)));
                connector.set_read_timeout(Some(Duration::from_secs(2)));
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let executor = fixture.executor.clone();
                let stop = fixture.stop.clone();
                let negotiated = Gate::new();
                let observed = negotiated.clone();
                fixture.peers.lock().unwrap().spawn(async move {
                let accepted = tokio::select! { accepted = listener.accept() => accepted.unwrap(), _ = stop.wait() => return };
                let stream = tokio::select! { stream = tokio_rustls::TlsAcceptor::from(Arc::new(server)).accept(accepted.0) => stream.unwrap(), _ = stop.wait() => return };
                assert_eq!(stream.get_ref().1.alpn_protocol(), Some(alpn.as_slice())); observed.open();
                let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("verified TLS")) }));
                serve_peer(stream, protocol, handler, executor).await;
            });
                let connector =
                    HyperConnector::new(connector, ConnectorPolicy::Direct, tls_metadata::<_>);
                let client = fixture.start(connector, options()).await;
                let origin = format!("https://localhost:{}", address.port())
                    .parse()
                    .unwrap();
                let bound = client
                    .route(RouteSpec::direct(
                        origin,
                        if protocol == CodecProtocol::Http2 {
                            ProtocolPolicy::Http2Only
                        } else {
                            ProtocolPolicy::Http1Only
                        },
                    ))
                    .unwrap();
                let response = client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                assert!(negotiated.is_open());
                assert!(
                    response
                        .extensions()
                        .get::<hyper_util::client::legacy::connect::HttpInfo>()
                        .is_some()
                );
                let exchange = response.body().exchange();
                assert_eq!(body_bytes(response.into_body()).await, "verified TLS");
                exchange.wait().await.unwrap();
            }
        })
    });
}

fn error_in_chain<'error, Error: std::error::Error + 'static>(
    error: &'error (dyn std::error::Error + 'static),
    depth: usize,
) -> Option<&'error Error> {
    if depth == 16 {
        return None;
    }
    if let Some(found) = error.downcast_ref::<Error>() {
        return Some(found);
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if let Some(inner) = io.get_ref() {
            if let Some(found) = error_in_chain(inner, depth + 1) {
                return Some(found);
            }
        }
    }
    error
        .source()
        .and_then(|source| error_in_chain(source, depth + 1))
}

#[derive(Debug, thiserror::Error)]
#[error("original post-connect read failure")]
struct PostConnectCause(Arc<()>);

struct CauseReadIo {
    socket: tokio::net::TcpStream,
    failure: crate::client_tests::TestFuture<()>,
    error: Option<std::io::Error>,
    owner: std::thread::ThreadId,
    drops: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}

impl AsyncRead for CauseReadIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.owner);
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.failure.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(self
                .error
                .take()
                .expect("the physical read failure is selected once")));
        }
        Pin::new(&mut self.socket).poll_read(context, buffer)
    }
}

impl AsyncWrite for CauseReadIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        assert_eq!(std::thread::current().id(), self.owner);
        Pin::new(&mut self.socket).poll_write(context, bytes)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.owner);
        Pin::new(&mut self.socket).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        assert_eq!(std::thread::current().id(), self.owner);
        Pin::new(&mut self.socket).poll_shutdown(context)
    }
}

impl Drop for CauseReadIo {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CausePhase {
    BeforeHead,
    AfterPrefix,
    ResetThenPrefix,
}

async fn write_cause_frame(
    socket: &mut tokio::net::TcpStream,
    kind: u8,
    flags: u8,
    stream: u32,
    payload: &[u8],
) {
    let length = u32::try_from(payload.len()).unwrap().to_be_bytes();
    let mut header = Vec::from(&length[1..]);
    header.extend_from_slice(&[kind, flags]);
    header.extend_from_slice(&stream.to_be_bytes());
    socket.write_all(&header).await.unwrap();
    socket.write_all(payload).await.unwrap();
}

async fn read_cause_frame(socket: &mut tokio::net::TcpStream) -> Option<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    if socket.read(&mut header[..1]).await.unwrap() == 0 {
        return None;
    }
    socket.read_exact(&mut header[1..]).await.unwrap();
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
    assert!(length <= 65_536);
    let mut payload = vec![0; length as usize];
    socket.read_exact(&mut payload).await.unwrap();
    Some((
        header[3],
        header[4],
        u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
        payload,
    ))
}

async fn cause_wire_peer(
    mut socket: tokio::net::TcpStream,
    protocol: CodecProtocol,
    phase: CausePhase,
    request_seen: Gate,
    requests: Arc<AtomicUsize>,
) {
    if protocol == CodecProtocol::Http11 {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            assert!(headers.len() < 4096);
            headers.push(socket.read_u8().await.unwrap());
        }
        assert!(headers.starts_with(b"GET /cause?phase=wire HTTP/1.1\r\n"));
        requests.fetch_add(1, Ordering::SeqCst);
        request_seen.open();
        if phase == CausePhase::AfterPrefix {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nprefix")
                .await
                .unwrap();
        }
        let mut after = [0; 1];
        assert_eq!(
            socket.read(&mut after).await.unwrap(),
            0,
            "no replay or additional HTTP/1 request"
        );
    } else {
        let mut preface = [0; 24];
        socket.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        write_cause_frame(&mut socket, 4, 0, 0, &[]).await;
        let mut acknowledged = false;
        while let Some((kind, flags, stream, payload)) = read_cause_frame(&mut socket).await {
            match kind {
                4 if flags & 1 == 0 => {
                    write_cause_frame(&mut socket, 4, 1, 0, &[]).await;
                    if phase == CausePhase::BeforeHead {
                        write_cause_frame(&mut socket, 6, 0, 0, b"cause-io").await;
                    }
                }
                6 if flags & 1 != 0 => {
                    assert_eq!(payload, b"cause-io");
                    acknowledged = true;
                    if requests.load(Ordering::SeqCst) == 1 {
                        request_seen.open();
                    }
                }
                1 => {
                    assert_ne!(flags & 4, 0, "bounded request fits one header block");
                    let number = requests.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(stream, if number == 0 { 1 } else { 3 });
                    assert!(
                        number
                            < if phase == CausePhase::ResetThenPrefix {
                                2
                            } else {
                                1
                            }
                    );
                    if phase != CausePhase::BeforeHead || acknowledged {
                        request_seen.open();
                    }
                    if phase == CausePhase::ResetThenPrefix && number == 0 {
                        write_cause_frame(&mut socket, 3, 0, stream, &8_u32.to_be_bytes()).await;
                    } else if phase != CausePhase::BeforeHead {
                        write_cause_frame(&mut socket, 1, 4, stream, &[0x88]).await;
                        write_cause_frame(&mut socket, 0, 0, stream, b"prefix").await;
                    }
                }
                _ => {}
            }
        }
    }
}

fn assert_post_connect_cause(error: &(dyn std::error::Error + 'static), identity: &Arc<()>) {
    let io = error_in_chain::<std::io::Error>(error, 0)
        .expect("original I/O error through actual wrapper chain");
    assert_eq!(io.kind(), std::io::ErrorKind::TimedOut);
    let cause = io
        .get_ref()
        .unwrap()
        .downcast_ref::<PostConnectCause>()
        .unwrap();
    assert!(Arc::ptr_eq(&cause.0, identity));
}

fn public_cause_case(protocol: CodecProtocol, phase: CausePhase) {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(|error| {
                matches!(error, ClientError::Pool(PoolError::SessionJoin { source })
                    if source.code() == roxygen_core::error::HttpErrorCode::TRANSPORT_READ_ERROR
                        && error_in_chain::<PostConnectCause>(source, 0).is_some())
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let request_seen = Gate::new();
            let seen = request_seen.clone();
            let peer_done = Gate::new();
            let done = peer_done.clone();
            let stop = fixture.stop.clone();
            let requests = Arc::new(AtomicUsize::new(0));
            let peer_requests = Arc::clone(&requests);
            fixture.peers.lock().unwrap().spawn(async move {
                let peer = async {
                    let (socket, _) = listener.accept().await.unwrap();
                    cause_wire_peer(socket, protocol, phase, seen, peer_requests).await;
                    done.open();
                };
                tokio::select! { _ = peer => {}, _ = stop.wait() => {} }
            });
            let failure = Gate::new();
            let fail = failure.clone();
            let identity = Arc::new(());
            let original = Arc::clone(&identity);
            let drops = Arc::new(AtomicUsize::new(0));
            let retired = Arc::clone(&drops);
            let reads = Arc::new(AtomicUsize::new(0));
            let polls = Arc::clone(&reads);
            let owner = fixture.worker.owner_thread();
            let app = Handle::current().id();
            let connector = TestConnector::new(move |request| {
                let fail = fail.clone();
                let identity = Arc::clone(&original);
                let drops = Arc::clone(&retired);
                let reads = Arc::clone(&polls);
                Box::pin(async move {
                    assert_eq!(Handle::current().id(), app);
                    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
                    let local = socket.local_addr().unwrap();
                    let io = CauseReadIo {
                        socket,
                        failure: Box::pin(async move { fail.wait().await }),
                        error: Some(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            PostConnectCause(identity),
                        )),
                        owner,
                        drops,
                        reads,
                    };
                    Ok(ConnectedIo::new(
                        request.route().clone(),
                        EstablishedIo::new(
                            Box::pin(io) as Io,
                            TransportEndpoint::Socket(roxygen_core::message::SocketAddress::new(
                                local.ip().to_string(),
                                local.port(),
                            )),
                            TransportEndpoint::Socket(roxygen_core::message::SocketAddress::new(
                                address.ip().to_string(),
                                address.port(),
                            )),
                            None,
                        ),
                        protocol,
                        TransportSecurity::Plaintext,
                    ))
                })
            });
            let calls = Arc::clone(&connector.calls);
            let mut settings = options();
            settings.pool.max_connections = 1;
            let application = Arc::new(TestApplicationJoin::default());
            match phase {
                CausePhase::BeforeHead => {}
                CausePhase::AfterPrefix => {
                    settings.test_application = Some(Arc::clone(&application));
                }
                CausePhase::ResetThenPrefix => {
                    settings
                        .test_application_following
                        .push(Arc::clone(&application));
                }
            }
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(protocol)).unwrap();
            let request = || {
                Request::builder()
                    .uri("/cause?phase=wire")
                    .body(Full::new(Bytes::new()))
                    .unwrap()
            };
            let mut selected = None;
            if phase == CausePhase::ResetThenPrefix {
                let first = client.try_request(&bound, request()).unwrap();
                let exchange = first.exchange();
                let error = first.await.err().expect("actual stream-1 wire reset");
                let ClientError::Native(native) = &error else {
                    panic!("native peer reset")
                };
                assert_eq!(native.code(), roxygen_core::error::HttpErrorCode::CANCEL);
                assert!(std::error::Error::source(native).is_none());
                assert!(!exchange.wait().await.unwrap_err().cleanup_unconfirmed());
                selected = Some(error);
            }
            let response = client.try_request(&bound, request()).unwrap();
            let exchange = response.exchange();
            if phase == CausePhase::BeforeHead {
                request_seen.wait().await;
                failure.open();
                let error = response
                    .await
                    .err()
                    .expect("original physical failure before headers");
                assert_post_connect_cause(&error, &identity);
            } else {
                let response = response.await.unwrap();
                assert_eq!(response.status(), http::StatusCode::OK);
                assert_eq!(
                    response.version(),
                    if protocol == CodecProtocol::Http2 {
                        http::Version::HTTP_2
                    } else {
                        http::Version::HTTP_11
                    }
                );
                let mut body = response.into_body();
                let mut observed = Vec::new();
                while observed.len() < 6 {
                    let frame = body.frame().await.unwrap().unwrap();
                    observed.extend_from_slice(&frame.into_data().expect("actual prefix DATA"));
                }
                assert_eq!(observed, b"prefix");
                while !application.reaped.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
                assert_eq!(*application.outcome.lock().unwrap(), Some("success"));
                failure.open();
                let error = body
                    .frame()
                    .await
                    .expect("nonterminal prefix retains body read")
                    .expect_err("original physical failure after prefix");
                assert_post_connect_cause(error.as_ref(), &identity);
                drop(body);
            }
            if phase == CausePhase::BeforeHead {
                let receipt = exchange
                    .wait()
                    .await
                    .expect_err("before-head operation failed");
                assert!(!receipt.cleanup_unconfirmed());
                let ClientError::Native(error) = receipt.cause() else {
                    panic!("before-head receipt retains the selected native failure")
                };
                assert_eq!(
                    error.code(),
                    roxygen_core::error::HttpErrorCode::TRANSPORT_READ_ERROR
                );
                assert_post_connect_cause(error, &identity);
            } else {
                exchange
                    .wait()
                    .await
                    .expect("successful post-header cleanup despite body error");
            }
            peer_done.wait().await;
            fixture.close_clients().await;
            assert_eq!(fixture.run_results.len(), 1);
            let failure = fixture.run_results[0].as_ref().unwrap_err();
            assert_post_connect_cause(&failure.0, &identity);
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "adopted I/O retired before Run joined"
            );
            assert!(reads.load(Ordering::SeqCst) > 0);
            assert_eq!(calls.load(Ordering::SeqCst), 1, "no connection replay");
            assert_eq!(
                requests.load(Ordering::SeqCst),
                if phase == CausePhase::ResetThenPrefix {
                    2
                } else {
                    1
                }
            );
            if let Some(error) = selected {
                let ClientError::Native(native) = error else {
                    panic!("retained native reset")
                };
                assert_eq!(native.code(), roxygen_core::error::HttpErrorCode::CANCEL);
                assert!(std::error::Error::source(&native).is_none());
                assert!(error_in_chain::<PostConnectCause>(&native, 0).is_none());
            }
        })
    });
}

#[test]
fn public_request_error_retains_post_connect_io_cause() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        public_cause_case(protocol, CausePhase::BeforeHead);
    }
}

#[test]
fn public_body_error_retains_post_connect_io_cause() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        public_cause_case(protocol, CausePhase::AfterPrefix);
    }
}

#[test]
fn h2_stream_error_is_not_given_unrelated_io_cause() {
    public_cause_case(CodecProtocol::Http2, CausePhase::ResetThenPrefix);
}

struct PostConnectIo<Io> {
    inner: Pin<Box<Io>>,
    writes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl<Io: Connection> Connection for PostConnectIo<Io> {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        self.inner.as_ref().get_ref().connected()
    }
}

impl<Io: hyper::rt::Read> hyper::rt::Read for PostConnectIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_read(context, buffer)
    }
}

impl<Io: hyper::rt::Write> hyper::rt::Write for PostConnectIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.fetch_add(bytes.len(), Ordering::SeqCst);
        self.inner.as_mut().poll_write(context, bytes)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_flush(context)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.inner.as_mut().poll_shutdown(context)
    }
}

impl<Io> Drop for PostConnectIo<Io> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn tls_failure_case(kind: u8) {
    case(|fixture| {
        Box::pin(async move {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let generated =
                rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
            let certificate = generated.cert.der().clone();
            let key = rustls::pki_types::PrivateKeyDer::try_from(
                generated.signing_key.serialized_der().to_vec(),
            )
            .unwrap();
            let mut server = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate.clone()], key)
                .unwrap();
            server.alpn_protocols = vec![b"h2".to_vec()];
            let mut roots = rustls::RootCertStore::empty();
            if kind != 0 {
                roots.add(certificate).unwrap();
            }
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config(config)
                .https_only()
                .enable_http1()
                .enable_http2()
                .build();
            let mut aggregate = hyper_timeout::TimeoutConnector::new(https);
            aggregate.set_connect_timeout(Some(Duration::from_secs(1)));
            let writes = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let written = Arc::clone(&writes);
            let dropped = Arc::clone(&drops);
            let aggregate = tower::ServiceExt::map_response(aggregate, move |io| PostConnectIo {
                inner: Box::pin(io),
                writes: Arc::clone(&written),
                drops: Arc::clone(&dropped),
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let observed = Gate::new();
            let observation = observed.clone();
            let rejected = Gate::new();
            let rejection = rejected.clone();
            let stop = fixture.stop.clone();
            let accepted_count = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::clone(&accepted_count);
            fixture.peers.lock().unwrap().spawn(async move {
            let (mut socket, _) = tokio::select! { result = listener.accept() => result.unwrap(), _ = stop.wait() => return };
            accepted.fetch_add(1, Ordering::SeqCst);
            if kind == 1 {
                let mut hello = [0; 4096]; let length = tokio::select! { length = socket.read(&mut hello) => length.unwrap(), _ = stop.wait() => return };
                assert!(length > 0); assert_eq!(hello[0], 22, "actual TLS client flight, not a synthetic timeout future"); observation.open(); stop.wait().await;
            } else {
                let tls = tokio::select! { result = tokio_rustls::TlsAcceptor::from(Arc::new(server)).accept(socket) => result, _ = stop.wait() => return };
                if kind == 0 {
                    let error = tls.expect_err("untrusted certificate produces a real peer alert");
                    assert!(matches!(error_in_chain::<rustls::Error>(&error, 0), Some(rustls::Error::AlertReceived(rustls::AlertDescription::UnknownCA))));
                    observation.open();
                } else {
                    let tls = tls.unwrap(); assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice())); observation.open();
                    tokio::select! { _ = rejection.wait() => {}, _ = stop.wait() => {} }
                    drop(tls);
                }
            }
        });
            let connector =
                HyperConnector::new(aggregate, ConnectorPolicy::Direct, tls_metadata::<_>);
            let mut settings = options();
            settings.pool.connect_timeout = Duration::from_secs(4);
            let client = fixture.start(connector, settings).await;
            let bound = client
                .route(RouteSpec::direct(
                    format!("https://localhost:{}", address.port())
                        .parse()
                        .unwrap(),
                    ProtocolPolicy::Http1Only,
                ))
                .unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            let ClientError::Pool(PoolError::AttachedConnectFailed { source }) =
                future.await.err().expect("actual aggregate failure")
            else {
                panic!("original connector classification missing")
            };
            let cause = source.external_cause().expect("original aggregate error");
            match kind {
                0 => assert!(matches!(
                    error_in_chain::<rustls::Error>(cause.as_ref(), 0),
                    Some(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::UnknownIssuer
                    ))
                )),
                1 => {
                    assert_eq!(
                        error_in_chain::<std::io::Error>(cause.as_ref(), 0)
                            .unwrap()
                            .kind(),
                        std::io::ErrorKind::TimedOut
                    );
                    assert!(
                        error_in_chain::<tokio::time::error::Elapsed>(cause.as_ref(), 0).is_some(),
                        "timeout came from the aggregate, not the native pool timer"
                    );
                }
                _ => assert!(matches!(
                    cause.downcast_ref::<ClientError>(),
                    Some(ClientError::ConnectorMismatch)
                )),
            }
            observed.wait().await;
            assert_eq!(accepted_count.load(Ordering::SeqCst), 1);
            assert_eq!(
                writes.load(Ordering::SeqCst),
                0,
                "no post-handshake HTTP write, including ALPN rejection"
            );
            assert_eq!(drops.load(Ordering::SeqCst), usize::from(kind == 2));
            rejected.open();
            let completion = exchange.wait().await.unwrap_err();
            assert!(!completion.cleanup_unconfirmed());
            let ClientError::Pool(PoolError::AttachedConnectFailed { source: retained }) =
                completion.cause()
            else {
                panic!("aggregate completion classification missing")
            };
            assert!(Arc::ptr_eq(cause, retained.external_cause().unwrap()));
        })
    });
}

struct SocketPath(std::path::PathBuf);
impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
static NEXT_SOCKET: AtomicUsize = AtomicUsize::new(1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataFailure {
    Clone,
    Connected,
    Mapper,
    Extras,
    ExtrasDrop,
    ContextDrop,
    FutureDrop,
}

struct ExtraPanic(Gate);
impl Clone for ExtraPanic {
    fn clone(&self) -> Self {
        self.0.open();
        panic!("connection extras clone");
    }
}

#[derive(Clone)]
struct ExtraDropPanic(Gate);
impl Drop for ExtraDropPanic {
    fn drop(&mut self) {
        self.0.open();
        panic!("connection extras destructor");
    }
}

struct MetadataTag {
    mode: MetadataFailure,
    armed: Arc<std::sync::atomic::AtomicBool>,
    peers: Gate,
    copied: bool,
}
impl Clone for MetadataTag {
    fn clone(&self) -> Self {
        if self.mode == MetadataFailure::Clone && self.armed.load(Ordering::Acquire) {
            self.peers.open();
            panic!("metadata clone before connector call");
        }
        Self {
            mode: self.mode,
            armed: Arc::clone(&self.armed),
            peers: self.peers.clone(),
            copied: true,
        }
    }
}
impl Drop for MetadataTag {
    fn drop(&mut self) {
        if self.copied
            && self.mode == MetadataFailure::ContextDrop
            && self.armed.load(Ordering::Acquire)
        {
            self.peers.open();
            panic!("metadata context destructor");
        }
    }
}
impl MetadataTag {
    fn map(
        &self,
        request: &ConnectRequest,
        io: &MetadataIo,
    ) -> Result<ConnectorMetadata, BoxError> {
        *io.claim.lock().unwrap() = Some(Arc::clone(&request.claimed));
        if io.bad && self.mode == MetadataFailure::Mapper {
            self.peers.open();
            panic!("metadata callback");
        }
        plaintext_metadata(request, io)
    }
}

struct MetadataIo {
    inner: hyper_util::rt::TokioIo<tokio::io::DuplexStream>,
    mode: MetadataFailure,
    bad: bool,
    claim: std::sync::Mutex<Option<Arc<std::sync::atomic::AtomicBool>>>,
    app: tokio::runtime::Id,
    worker: std::thread::ThreadId,
    peers: Gate,
    drops: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
}
impl Connection for MetadataIo {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        if self.bad && self.mode == MetadataFailure::Connected {
            self.peers.open();
            panic!("Connected callback");
        }
        let connected = hyper_util::client::legacy::connect::Connected::new();
        if self.bad && self.mode == MetadataFailure::Extras {
            connected.extra(ExtraPanic(self.peers.clone()))
        } else if self.bad && self.mode == MetadataFailure::ExtrasDrop {
            connected.extra(ExtraDropPanic(self.peers.clone()))
        } else {
            connected
        }
    }
}
impl hyper::rt::Read for MetadataIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.bad {
            self.polls.fetch_add(1, Ordering::SeqCst);
        }
        hyper::rt::Read::poll_read(Pin::new(&mut self.inner), context, buffer)
    }
}
impl hyper::rt::Write for MetadataIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.bad {
            self.polls.fetch_add(1, Ordering::SeqCst);
        }
        hyper::rt::Write::poll_write(Pin::new(&mut self.inner), context, bytes)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_flush(Pin::new(&mut self.inner), context)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_shutdown(Pin::new(&mut self.inner), context)
    }
}
impl Drop for MetadataIo {
    fn drop(&mut self) {
        let claimed = self
            .claim
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|claim| claim.load(Ordering::Acquire));
        if claimed {
            assert_eq!(std::thread::current().id(), self.worker);
        } else {
            assert_eq!(Handle::current().id(), self.app);
        }
        if self.bad {
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("independent ready I/O destructor");
        }
    }
}
struct MetadataFuture {
    io: Option<MetadataIo>,
    drops: Arc<AtomicUsize>,
    bad: bool,
    mode: MetadataFailure,
    peers: Gate,
}
impl Future for MetadataFuture {
    type Output = Result<MetadataIo, BoxError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.io.take().unwrap()))
    }
}
impl Drop for MetadataFuture {
    fn drop(&mut self) {
        if self.bad {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.mode == MetadataFailure::FutureDrop {
                self.peers.open();
                panic!("returned connector future destructor");
            }
        }
    }
}
struct MetadataConnector(Box<dyn FnMut(http::Uri) -> MetadataFuture + Send>);
impl tower_service::Service<http::Uri> for MetadataConnector {
    type Response = MetadataIo;
    type Error = BoxError;
    type Future = MetadataFuture;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, uri: http::Uri) -> Self::Future {
        self.0(uri)
    }
}

#[test]
fn hyper_metadata_panics_retire_outputs_before_other_joins() {
    accepted_extra_drop_case();
    for mode in [
        MetadataFailure::Clone,
        MetadataFailure::Connected,
        MetadataFailure::Mapper,
        MetadataFailure::Extras,
        MetadataFailure::ExtrasDrop,
        MetadataFailure::ContextDrop,
        MetadataFailure::FutureDrop,
    ] {
        case(|fixture| {
            Box::pin(async move {
                fixture.expected_run_error = Some(crate::client_tests::panicked_run);
                let drops = Arc::new(AtomicUsize::new(0));
                let polls = Arc::new(AtomicUsize::new(0));
                let futures = Arc::new(AtomicUsize::new(0));
                let calls = Arc::new(AtomicUsize::new(0));
                let peers = Arc::clone(&fixture.peers);
                let executor = fixture.executor.clone();
                let stop = fixture.stop.clone();
                let app = Handle::current().id();
                let worker = fixture.worker.owner_thread();
                let io_drops = Arc::clone(&drops);
                let io_polls = Arc::clone(&polls);
                let future_drops = Arc::clone(&futures);
                let counter = Arc::clone(&calls);
                let connector = MetadataConnector(Box::new(move |uri| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let bad = uri.host() == Some("bad.test");
                    let (io, peer) = tokio::io::duplex(4096);
                    if bad {
                        drop(peer);
                    } else {
                        let handler: Handler = Arc::new(|request| {
                            Box::pin(async move {
                                let _ = request.into_body().collect().await;
                                Response::new(full("other"))
                            })
                        });
                        peers.lock().unwrap().spawn(serve_peer(
                            peer,
                            CodecProtocol::Http11,
                            handler,
                            executor.clone(),
                        ));
                    }
                    MetadataFuture {
                        io: Some(MetadataIo {
                            inner: hyper_util::rt::TokioIo::new(io),
                            mode,
                            bad,
                            claim: std::sync::Mutex::new(None),
                            app,
                            worker,
                            peers: stop.clone(),
                            drops: Arc::clone(&io_drops),
                            polls: Arc::clone(&io_polls),
                        }),
                        drops: Arc::clone(&future_drops),
                        bad,
                        mode,
                        peers: stop.clone(),
                    }
                }));
                let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let tag = MetadataTag {
                    mode,
                    armed: Arc::clone(&armed),
                    peers: fixture.stop.clone(),
                    copied: false,
                };
                let connector = HyperConnector::new(
                    connector,
                    ConnectorPolicy::Direct,
                    move |request: &ConnectRequest, io: &MetadataIo| tag.map(request, io),
                );
                let probe = Arc::new(TestApplicationJoin::default());
                let mut settings = options();
                settings.test_application = Some(Arc::clone(&probe));
                let client = fixture.start(connector, settings).await;
                let good = client.route(route(CodecProtocol::Http11)).unwrap();
                let cleanup_gate = crate::client_tests::DropGate::new();
                let release = crate::client_tests::ReleaseGate(Arc::clone(&cleanup_gate));
                let mut body =
                    TestBody::new(vec![http_body::Frame::data(Bytes::from_static(b"pending"))])
                        .gated(Gate::new());
                body.drop_gate = Some(Arc::clone(&cleanup_gate));
                let body_polls = Arc::clone(&body.polls);
                let body_drops = Arc::clone(&body.drops);
                let other = client
                    .try_request(
                        &good,
                        Request::builder()
                            .method("POST")
                            .uri("/")
                            .body(body)
                            .unwrap(),
                    )
                    .unwrap();
                while body_polls.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
                armed.store(true, Ordering::Release);
                let bad = client
                    .route(RouteSpec::direct(
                        "http://bad.test".parse().unwrap(),
                        ProtocolPolicy::Http1Only,
                    ))
                    .unwrap();
                let rejected = client
                    .try_request(
                        &bad,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap();
                cleanup_gate.entered.wait().await;
                assert!(!probe.reaped.load(Ordering::Acquire));
                assert!(futures::poll!(Box::pin(fixture.runs.join_next())).is_pending());
                cleanup_gate.release();
                assert!(rejected.await.is_err());
                assert!(other.await.is_err());
                fixture.close_clients().await;
                producer_reaped(&probe).await;
                assert_eq!(body_drops.load(Ordering::SeqCst), 1);
                assert_eq!(
                    drops.load(Ordering::SeqCst),
                    usize::from(mode != MetadataFailure::Clone)
                );
                assert_eq!(
                    futures.load(Ordering::SeqCst),
                    usize::from(mode != MetadataFailure::Clone)
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    if mode == MetadataFailure::Clone { 1 } else { 2 }
                );
                assert_eq!(polls.load(Ordering::SeqCst), 0);
                assert!(client.shared.admission.is_closed());
                drop(release);
            })
        });
    }
}

struct OriginalExtra {
    original: bool,
    originals: Arc<AtomicUsize>,
    copies: Arc<AtomicUsize>,
}

impl Clone for OriginalExtra {
    fn clone(&self) -> Self {
        Self {
            original: false,
            originals: Arc::clone(&self.originals),
            copies: Arc::clone(&self.copies),
        }
    }
}

impl Drop for OriginalExtra {
    fn drop(&mut self) {
        if self.original {
            self.originals.fetch_add(1, Ordering::SeqCst);
            panic!("original accepted connection extra destructor");
        }
        self.copies.fetch_add(1, Ordering::SeqCst);
    }
}

fn accepted_extra_drop_case() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(crate::client_tests::panicked_run);
            let handler: Handler =
                Arc::new(|_| Box::pin(async { Response::new(full("accepted")) }));
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let original_drops = Arc::new(AtomicUsize::new(0));
            let copy_drops = Arc::new(AtomicUsize::new(0));
            let originals = Arc::clone(&original_drops);
            let copies = Arc::clone(&copy_drops);
            let mut factory = connector.take_factory();
            connector.factory = Box::new(move |request| {
                let future = factory(request);
                let originals = Arc::clone(&originals);
                let copies = Arc::clone(&copies);
                Box::pin(async move {
                    let connected = future.await?;
                    let mut extensions = http::Extensions::new();
                    extensions.insert(OriginalExtra {
                        original: true,
                        originals,
                        copies,
                    });
                    Ok(connected.with_extensions(extensions))
                })
            });
            let io_drops = Arc::new(AtomicUsize::new(0));
            let shutdown = Gate::new();
            shutdown.open();
            probe_connector(
                &mut connector,
                &fixture.worker,
                shutdown,
                Gate::new(),
                Arc::clone(&io_drops),
            );
            let io = Arc::new(crate::client::owner::TestIoJoin::default());
            let application = Arc::new(TestApplicationJoin::default());
            let mut settings = options();
            settings.test_io = Some(Arc::clone(&io));
            settings.test_application = Some(Arc::clone(&application));
            let client = fixture.start(connector, settings).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &route,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let copy = response
                .extensions()
                .get::<OriginalExtra>()
                .expect("successful metadata projection")
                .clone();
            drop(copy);
            let exchange = response.body().exchange();
            assert_eq!(body_bytes(response.into_body()).await, "accepted");
            exchange.wait().await.unwrap();
            producer_reaped(&application).await;
            assert_eq!(original_drops.load(Ordering::SeqCst), 0);
            assert!(copy_drops.load(Ordering::SeqCst) > 0);
            assert!(!client.shared.destruction_failed.load(Ordering::Acquire));
            fixture.close_clients().await;
            assert_eq!(original_drops.load(Ordering::SeqCst), 1);
            assert_eq!(io_drops.load(Ordering::SeqCst), 1);
            assert!(io.retired.load(Ordering::Acquire));
            let adopted = io.adopted.lock().unwrap().clone().unwrap();
            adopted.wait_retired().await.unwrap();
            assert!(client.shared.destruction_failed.load(Ordering::Acquire));
        })
    });
}

#[test]
fn forward_proxy_tcp_and_unix_preserve_absolute_target_and_logical_origin() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|request| {
                Box::pin(async move {
                    assert_eq!(request.method(), "GET");
                    assert_eq!(request.uri(), "http://logical.test/file");
                    assert_eq!(request.headers()["host"], "logical.test");
                    Response::new(full("proxy"))
                })
            });
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let executor = fixture.executor.clone();
            let peer_handler = Arc::clone(&handler);
            let stop = fixture.stop.clone();
            fixture.peers.lock().unwrap().spawn(async move {
            let accepted = tokio::select! { accepted = listener.accept() => accepted.unwrap(), _ = stop.wait() => return };
            serve_peer(accepted.0, CodecProtocol::Http11, peer_handler, executor).await;
        });
            let uri: http::Uri = format!("http://{address}").parse().unwrap();
            let endpoint = ProxyEndpoint::tcp(uri.clone()).unwrap();
            let policy = ConnectorPolicy::Forward(endpoint.clone());
            let mut tcp = HttpConnector::new();
            tcp.enforce_http(true);
            let proxy = hyper_http_proxy::Proxy::new(hyper_http_proxy::Intercept::All, uri);
            let connector = hyper_http_proxy::ProxyConnector::from_proxy_unsecured(
                hyper_timeout::TimeoutConnector::new(tcp),
                proxy,
            );
            let connector = HyperConnector::new(connector, policy.clone(), plaintext_metadata::<_>);
            let mut settings = options();
            settings.connector_policy = policy;
            let client = fixture.start(connector, settings).await;
            let bound = client
                .route(
                    RouteSpec::forward(
                        "https://logical.test".parse().unwrap(),
                        endpoint,
                        ProtocolPolicy::Http1Only,
                    )
                    .unwrap(),
                )
                .unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("https://logical.test/file")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            assert_eq!(bound.spec().origin().scheme(), "https");
            let exchange = response.body().exchange();
            assert_eq!(body_bytes(response.into_body()).await, "proxy");
            exchange.wait().await.unwrap();

            let path = SocketPath(std::env::temp_dir().join(format!(
                "roxy-facade-{}-{}.sock",
                std::process::id(),
                NEXT_SOCKET.fetch_add(1, Ordering::SeqCst)
            )));
            let listener = tokio::net::UnixListener::bind(&path.0).unwrap();
            let executor = fixture.executor.clone();
            let stop = fixture.stop.clone();
            fixture.peers.lock().unwrap().spawn(async move {
            let accepted = tokio::select! { accepted = listener.accept() => accepted.unwrap(), _ = stop.wait() => return };
            serve_peer(accepted.0, CodecProtocol::Http11, handler, executor).await;
        });
            let endpoint = ProxyEndpoint::unix(path.0.clone()).unwrap();
            let policy = ConnectorPolicy::Forward(endpoint.clone());
            let proxy = hyper_http_proxy::Proxy::new(
                hyper_http_proxy::Intercept::All,
                hyperlocal::Uri::new(&path.0, "/").into(),
            );
            let connector = hyper_http_proxy::ProxyConnector::from_proxy_unsecured(
                hyper_timeout::TimeoutConnector::new(hyperlocal::UnixConnector),
                proxy,
            );
            let connector = HyperConnector::new(connector, policy.clone(), plaintext_metadata::<_>);
            let mut settings = options();
            settings.connector_policy = policy;
            let client = fixture.start(connector, settings).await;
            let bound = client
                .route(
                    RouteSpec::forward(
                        "https://logical.test".parse().unwrap(),
                        endpoint,
                        ProtocolPolicy::Http1Only,
                    )
                    .unwrap(),
                )
                .unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/file")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            assert_eq!(bound.spec().origin().scheme(), "https");
            let exchange = response.body().exchange();
            assert_eq!(body_bytes(response.into_body()).await, "proxy");
            exchange.wait().await.unwrap();
        })
    });
}

#[test]
fn connector_route_or_protocol_disagreement_is_joined_rejection() {
    struct ReleaseRejection {
        connector: Gate,
        attachment: Arc<TestLatch>,
        native: Arc<TestLatch>,
    }
    impl Drop for ReleaseRejection {
        fn drop(&mut self) {
            self.connector.open();
            self.attachment.release();
            self.native.release();
        }
    }

    case(|fixture| {
        Box::pin(async move {
            for (mismatch, gated) in [
                (0, false),
                (1, false),
                (2, false),
                (0, true),
                (1, true),
                (2, true),
            ] {
                let handler: Handler =
                    Arc::new(|_| Box::pin(async { Response::new(full("unused")) }));
                let mut connector = fixture.connector(CodecProtocol::Http11, handler);
                let shutdown = Gate::new();
                shutdown.open();
                let claimed = Gate::new();
                let dropped = Arc::new(AtomicUsize::new(0));
                probe_connector(
                    &mut connector,
                    &fixture.worker,
                    shutdown,
                    claimed.clone(),
                    Arc::clone(&dropped),
                );
                let connector_gate = Gate::new();
                let attachment_gate = TestLatch::new();
                let native_gate = TestLatch::new();
                let _release = ReleaseRejection {
                    connector: connector_gate.clone(),
                    attachment: Arc::clone(&attachment_gate),
                    native: Arc::clone(&native_gate),
                };
                if !gated {
                    connector_gate.open();
                }
                let release = connector_gate.clone();
                let mut factory = connector.take_factory();
                connector.factory = Box::new(move |request| {
                    let future = factory(request);
                    let release = release.clone();
                    Box::pin(async move {
                        release.wait().await;
                        let (mut route, io, mut protocol, mut security, extensions) =
                            future.await?.into_parts();
                        if mismatch == 0 {
                            route = RouteSpec::direct(
                                "http://other.test".parse().unwrap(),
                                ProtocolPolicy::Http1Only,
                            );
                        }
                        if mismatch == 1 {
                            protocol = CodecProtocol::Http2;
                        }
                        if mismatch == 2 {
                            security = TransportSecurity::Tls;
                        }
                        Ok(ConnectedIo::new(route, io, protocol, security)
                            .with_extensions(extensions))
                    })
                });
                let calls = Arc::clone(&connector.calls);
                let producer = Arc::new(TestApplicationJoin {
                    before_attachment: gated.then(|| Arc::clone(&attachment_gate)),
                    ..Default::default()
                });
                let mut settings = options();
                settings.test_application = Some(Arc::clone(&producer));
                settings.test_native_return = gated.then(|| Arc::clone(&native_gate));
                let client = fixture.start(connector, settings).await;
                let bound = client.route(route(CodecProtocol::Http11)).unwrap();
                let started = std::time::Instant::now();
                let body = TestBody::new(vec![]);
                let body_polls = Arc::clone(&body.polls);
                let body_drops = Arc::clone(&body.drops);
                let mut future = Box::pin(
                    client
                        .try_request(&bound, Request::builder().uri("/").body(body).unwrap())
                        .unwrap(),
                );
                let exchange = future.exchange();
                if gated {
                    attachment_gate.wait_entered().await;
                    assert_eq!(dropped.load(Ordering::SeqCst), 0);
                    connector_gate.open();
                    native_gate.wait_entered().await;
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    assert_eq!(dropped.load(Ordering::SeqCst), 1);
                    assert!(!claimed.is_open());
                    assert_eq!(body_polls.load(Ordering::SeqCst), 0);
                    assert_eq!(body_drops.load(Ordering::SeqCst), 0);
                    assert!(!producer.attached.load(Ordering::Acquire));
                    assert!(!producer.attachment_rejected.load(Ordering::Acquire));
                    assert!(futures::poll!(&mut future).is_pending());
                    attachment_gate.release();
                }
                let error = future.await.err().expect("mismatched attachment rejected");
                let source = match error {
                    ClientError::Pool(PoolError::AttachedConnectFailed { source }) => source,
                    actual => {
                        let phase = match mismatch {
                            0 => "route",
                            1 => "protocol",
                            _ => "security",
                        };
                        panic!(
                            "expected attached connector cause: mismatch={phase}, gated={gated}, actual={actual:?}, connector_calls={}, io_drops={}, io_used={}, elapsed={:?}",
                            calls.load(Ordering::SeqCst),
                            dropped.load(Ordering::SeqCst),
                            claimed.is_open(),
                            started.elapsed(),
                        )
                    }
                };
                assert!(matches!(
                    source
                        .external_cause()
                        .unwrap()
                        .downcast_ref::<ClientError>(),
                    Some(ClientError::ConnectorMismatch)
                ));
                if gated {
                    assert!(producer.attachment_rejected.load(Ordering::Acquire));
                    assert!(!producer.attached.load(Ordering::Acquire));
                    assert!(futures::poll!(Box::pin(exchange.wait())).is_pending());
                    native_gate.release();
                }
                let completion = exchange.wait().await.unwrap_err();
                assert!(!completion.cleanup_unconfirmed());
                let ClientError::Pool(PoolError::AttachedConnectFailed { source: retained }) =
                    completion.cause()
                else {
                    panic!("mismatched attachment completion retains its cause")
                };
                assert!(Arc::ptr_eq(
                    source.external_cause().unwrap(),
                    retained.external_cause().unwrap()
                ));
                producer_reaped(&producer).await;
                let cleanup = producer.cleanup.lock().unwrap().clone().unwrap();
                cleanup.wait().await.unwrap();
                assert_eq!(body_polls.load(Ordering::SeqCst), 0);
                assert_eq!(body_drops.load(Ordering::SeqCst), 1);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert!(!claimed.is_open());
            }
        })
    });
}

#[test]
fn selected_original_connector_cause_survives_retry_and_fanout() {
    case(|fixture| {
        Box::pin(async move {
            let first = Arc::new(());
            let selected = Arc::new(());
            let expected = Arc::clone(&selected);
            let attempts = Arc::new(AtomicUsize::new(0));
            let count = Arc::clone(&attempts);
            let second = Gate::new();
            let ready = second.clone();
            let release = Gate::new();
            let gate = release.clone();
            let connector = TestConnector::new(move |_| {
                let attempt = count.fetch_add(1, Ordering::SeqCst);
                let identity = if attempt == 0 {
                    Arc::clone(&first)
                } else {
                    Arc::clone(&selected)
                };
                let ready = ready.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    if attempt == 1 {
                        ready.open();
                        gate.wait().await;
                    }
                    Err(Box::new(OriginalError(identity)) as BoxError)
                })
            });
            let mut settings = options();
            settings.pool.max_connections = 1;
            settings.pool.max_connection_attempts = 2;
            let acquisitions = Arc::new(AtomicUsize::new(0));
            settings.test_acquisitions = Some(Arc::clone(&acquisitions));
            let producers = [
                Arc::new(TestApplicationJoin::default()),
                Arc::new(TestApplicationJoin::default()),
            ];
            settings.test_application = Some(Arc::clone(&producers[0]));
            settings
                .test_application_following
                .push(Arc::clone(&producers[1]));
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let first = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/one")
                        .body(TestBody::new(vec![]))
                        .unwrap(),
                )
                .unwrap();
            second.wait().await;
            let second = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/two")
                        .body(TestBody::new(vec![]))
                        .unwrap(),
                )
                .unwrap();
            while acquisitions.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            for producer in &producers {
                while !producer.attached.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            }
            release.open();
            let mut selected_cause = None;
            for future in [first, second] {
                let exchange = future.exchange();
                let error = future.await.err().expect("selected native failure");
                let ClientError::Pool(PoolError::AttachedConnectFailed { source }) = error else {
                    panic!("selected typed cause missing")
                };
                let original = source
                    .external_cause()
                    .unwrap()
                    .downcast_ref::<OriginalError>()
                    .unwrap();
                assert!(Arc::ptr_eq(&original.0, &expected));
                let cause = source.external_cause().unwrap();
                if let Some(previous) = &selected_cause {
                    assert!(Arc::ptr_eq(previous, cause));
                } else {
                    selected_cause = Some(Arc::clone(cause));
                }
                let completion = exchange.wait().await.unwrap_err();
                assert!(!completion.cleanup_unconfirmed());
                let ClientError::Pool(PoolError::AttachedConnectFailed { source: retained }) =
                    completion.cause()
                else {
                    panic!("upload cancellation replaced selected cause")
                };
                assert!(Arc::ptr_eq(cause, retained.external_cause().unwrap()));
            }
            for producer in &producers {
                producer_reaped(producer).await;
            }
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        })
    });
    case(|fixture| {
        Box::pin(async move {
            let identities = [Arc::new(()), Arc::new(())];
            let expected = identities.clone();
            let entered = [Gate::new(), Gate::new()];
            let observed = entered.clone();
            let releases = [Gate::new(), Gate::new()];
            let release = releases.clone();
            let connector = TestConnector::new(move |request| {
                let index =
                    usize::from(request.route().origin().authority().as_str() == "second.test");
                let identity = Arc::clone(&identities[index]);
                let entered = entered[index].clone();
                let release = release[index].clone();
                Box::pin(async move {
                    entered.open();
                    release.wait().await;
                    Err(Box::new(OriginalError(identity)) as BoxError)
                })
            });
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            let mut responses = Vec::new();
            for origin in ["http://first.test", "http://second.test"] {
                let bound = client
                    .route(RouteSpec::direct(
                        origin.parse().unwrap(),
                        ProtocolPolicy::Http1Only,
                    ))
                    .unwrap();
                responses.push(
                    client
                        .try_request(
                            &bound,
                            Request::builder()
                                .uri("/")
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                        )
                        .unwrap(),
                );
            }
            for entered in observed {
                entered.wait().await;
            }
            for index in [1, 0] {
                releases[index].open();
                let future = responses.remove(index);
                let exchange = future.exchange();
                let ClientError::Pool(PoolError::AttachedConnectFailed { source }) =
                    future.await.err().unwrap()
                else {
                    panic!("parallel attempt lost original cause")
                };
                let original = source
                    .external_cause()
                    .unwrap()
                    .downcast_ref::<OriginalError>()
                    .unwrap();
                assert!(Arc::ptr_eq(&original.0, &expected[index]));
                let completion = exchange.wait().await.unwrap_err();
                assert!(!completion.cleanup_unconfirmed());
                let ClientError::Pool(PoolError::AttachedConnectFailed { source: retained }) =
                    completion.cause()
                else {
                    panic!("parallel cause replaced")
                };
                assert!(Arc::ptr_eq(
                    source.external_cause().unwrap(),
                    retained.external_cause().unwrap()
                ));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        })
    });
    for succeeds in [true, false] {
        case(|fixture| {
            Box::pin(async move {
                let requests = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&requests);
                let handler: Handler = Arc::new(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Response::new(full("retry succeeded")) })
                });
                let mut connector = fixture.connector(CodecProtocol::Http11, handler);
                let mut successful = connector.take_factory();
                let calls = Arc::clone(&connector.calls);
                let mut attempt = 0;
                connector.factory = Box::new(move |request| {
                    attempt += 1;
                    if attempt == 1 {
                        return Box::pin(async {
                            Err(Box::new(OriginalError(Arc::new(()))) as BoxError)
                        });
                    }
                    if succeeds {
                        successful(request)
                    } else {
                        Box::pin(async {
                            Err(Box::new(roxygen_client::pool::AttachedConnectFailure::new(
                                roxygen_core::error::HttpError::new(
                                    roxygen_core::error::HttpErrorCode::CONNECT_ERROR,
                                ),
                            )) as BoxError)
                        })
                    }
                });
                let mut settings = options();
                settings.pool.max_connection_attempts = 2;
                let client = fixture.start(connector, settings).await;
                let bound = client.route(route(CodecProtocol::Http11)).unwrap();
                let future = client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap();
                let exchange = future.exchange();
                if succeeds {
                    assert_eq!(
                        body_bytes(future.await.unwrap().into_body()).await,
                        "retry succeeded"
                    );
                    exchange.wait().await.unwrap();
                    assert_eq!(requests.load(Ordering::SeqCst), 1);
                } else {
                    let ClientError::Pool(PoolError::ConnectFailed { source }) =
                        future.await.err().unwrap()
                    else {
                        panic!("native-only final classification lost")
                    };
                    assert_eq!(
                        source.code(),
                        roxygen_core::error::HttpErrorCode::CONNECT_ERROR
                    );
                    let completion = exchange.wait().await.unwrap_err();
                    assert!(!completion.cleanup_unconfirmed());
                    let ClientError::Pool(PoolError::ConnectFailed { source }) = completion.cause()
                    else {
                        panic!("native-only completion classification lost")
                    };
                    assert_eq!(
                        source.code(),
                        roxygen_core::error::HttpErrorCode::CONNECT_ERROR
                    );
                    assert_eq!(requests.load(Ordering::SeqCst), 0);
                }
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            })
        });
    }
}

#[test]
fn late_connector_results_after_cancel_are_joined_without_publication() {
    for phase in 0..3 {
        unclaimed_handoff_case(phase);
    }
    late_cause_during_drain();
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| Box::pin(async { Response::new(full("unused")) }));
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let shutdown = Gate::new();
            shutdown.open();
            let claimed = Gate::new();
            let dropped = Arc::new(AtomicUsize::new(0));
            probe_connector(
                &mut connector,
                &fixture.worker,
                shutdown,
                claimed.clone(),
                Arc::clone(&dropped),
            );
            let mut factory = connector.take_factory();
            let held = Gate::new();
            let entered = held.clone();
            let release = Gate::new();
            let gate = release.clone();
            connector.factory = Box::new(move |request| {
                let future = factory(request);
                let entered = entered.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    let io = future.await?;
                    entered.open();
                    gate.wait().await;
                    Ok(io)
                })
            });
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            held.wait().await;
            fixture.owners.last().unwrap().shutdown();
            release.open();
            assert!(future.await.is_err());
            fixture.close_clients().await;
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(!claimed.is_open());
        })
    });
}

fn unclaimed_handoff_case(phase: u8) {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler =
                Arc::new(|_| Box::pin(async { Response::new(full("not adopted")) }));
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let calls = Arc::clone(&connector.calls);
            let dropped = Arc::new(AtomicUsize::new(0));
            let polled = Gate::new();
            let shutdown = Gate::new();
            shutdown.open();
            probe_connector(
                &mut connector,
                &fixture.worker,
                shutdown,
                polled.clone(),
                Arc::clone(&dropped),
            );
            let latch = crate::client::owner::TestLatch::new();
            let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut settings = options();
            settings.maximum_physical_connections = 1;
            if phase == 2 {
                settings.test_close_claim_registration = true;
                settings.test_claim_registration_rejected = Some(Arc::clone(&rejected));
            } else {
                settings.test_before_claim = Some(Arc::clone(&latch));
                settings.pool.connect_timeout = Duration::from_secs(1);
            }
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            if phase != 2 {
                latch.wait_entered().await;
                assert_eq!(client.shared.physical.available_permits(), 0);
                assert_eq!(dropped.load(Ordering::SeqCst), 0);
                if phase == 0 {
                    fixture.owners.last().unwrap().shutdown();
                }
            }
            while dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert!(!polled.is_open());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            if phase == 2 {
                assert!(
                    rejected.load(Ordering::Acquire),
                    "actual worker register_scope rejected before atomic claim"
                );
            }
            assert!(future.await.is_err());
            let completion = exchange.wait().await.unwrap_err();
            assert!(!completion.cleanup_unconfirmed());
            latch.release();
            fixture.close_clients().await;
        })
    });
}

#[derive(Debug)]
struct LateCause {
    drops: Arc<AtomicUsize>,
}
impl std::fmt::Display for LateCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("late connector cause")
    }
}
impl std::error::Error for LateCause {}
impl Drop for LateCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn late_cause_during_drain() {
    case(|fixture| {
        Box::pin(async move {
            let owner: Arc<std::sync::Mutex<Option<std::sync::Weak<crate::client::Shared>>>> =
                Arc::new(std::sync::Mutex::new(None));
            let observed_owner = Arc::clone(&owner);
            let release = Gate::new();
            let wait = release.clone();
            let entered = Gate::new();
            let started = entered.clone();
            let drops = Arc::new(AtomicUsize::new(0));
            let returned = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let produced = Arc::clone(&returned);
            let destroyed = Arc::clone(&drops);
            let connector = TestConnector::new(move |_| {
                let owner = Arc::clone(&observed_owner);
                let wait = wait.clone();
                let started = started.clone();
                let produced = Arc::clone(&produced);
                let error = LateCause {
                    drops: Arc::clone(&destroyed),
                };
                Box::pin(async move {
                    started.open();
                    wait.wait().await;
                    let shared = owner
                        .lock()
                        .unwrap()
                        .as_ref()
                        .and_then(std::sync::Weak::upgrade)
                        .unwrap();
                    shared.close();
                    assert!(shared.admission.is_closed());
                    produced.store(true, Ordering::Release);
                    Err(Box::new(error) as BoxError)
                })
            });
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            *owner.lock().unwrap() = Some(Arc::downgrade(&client.shared));
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            entered.wait().await;
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            release.open();
            assert!(future.await.is_err());
            let completion = exchange.wait().await.unwrap_err();
            assert!(!completion.cleanup_unconfirmed());
            fixture.close_clients().await;
            assert!(returned.load(Ordering::Acquire));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[test]
fn ambiguous_request_failure_is_never_replayed() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(
                |error| matches!(error, ClientError::Pool(PoolError::SessionJoin { source }) if source.code() == roxygen_core::error::HttpErrorCode::TRANSPORT_EOF),
            );
            let peers = Arc::clone(&fixture.peers);
            let stop = fixture.stop.clone();
            let received = Gate::new();
            let seen = received.clone();
            let mut attempt = 0;
            let connector = TestConnector::new(move |request| {
                attempt += 1;
                let (target, body) = match attempt {
                    1 => (
                        b"POST /mutation HTTP/1.1\r\n".as_slice(),
                        b"one mutation".as_slice(),
                    ),
                    2 => (
                        b"POST /explicit-next HTTP/1.1\r\n".as_slice(),
                        b"second explicit mutation".as_slice(),
                    ),
                    _ => panic!("no automatic request replay"),
                };
                let (client, mut peer) = tokio::io::duplex(4096);
                let stop = stop.clone();
                let seen = seen.clone();
                peers.lock().unwrap().spawn(async move {
                    let receive = async {
                        let mut head = Vec::new();
                        while !head.ends_with(b"\r\n\r\n") {
                            assert!(head.len() < 1024);
                            head.push(peer.read_u8().await.unwrap());
                        }
                        assert!(head.starts_with(target));
                        let mut actual = vec![0; body.len()];
                        peer.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, body);
                        seen.open();
                        peer.shutdown().await.unwrap();
                    };
                    tokio::select! { _ = receive => {}, _ = stop.wait() => {} }
                });
                Box::pin(async move {
                    Ok(ConnectedIo::new(
                        request.route().clone(),
                        EstablishedIo::new(
                            Box::pin(client) as Io,
                            TransportEndpoint::Unknown,
                            TransportEndpoint::Unknown,
                            None,
                        ),
                        CodecProtocol::Http11,
                        TransportSecurity::Plaintext,
                    ))
                })
            });
            let calls = Arc::clone(&connector.calls);
            let mut settings = options();
            settings.pool.max_connection_attempts = 3;
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/mutation")
                        .header("content-length", "12")
                        .body(Full::new(Bytes::from_static(b"one mutation")))
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            assert!(future.await.is_err());
            received.wait().await;
            assert!(!exchange.wait().await.unwrap_err().cleanup_unconfirmed());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            while client.shared.physical.available_permits() != 2 {
                tokio::task::yield_now().await;
            }
            let next = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/explicit-next")
                        .header("content-length", "24")
                        .body(Full::new(Bytes::from_static(b"second explicit mutation")))
                        .unwrap(),
                )
                .unwrap();
            let exchange = next.exchange();
            assert!(next.await.is_err());
            assert!(!exchange.wait().await.unwrap_err().cleanup_unconfirmed());
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        })
    });
}
