/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! PING probe plumbing through `SessionParams` and `ClientAttachOptions`.
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::ServerConfig;
use roxygen_server::server::StaticTlsCredentials;
use roxygen_session::experimental::PingProbeConfig;
use roxygen_session::experimental::is_ping_probe_timeout;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsServerOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::DuplexStream;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::HttpConnector;
use crate::connector::LocalClientFuture;
use crate::connector::SessionParams;
use crate::pool::AttachedPoolParams;
use crate::pool::AttachedSessionFactory;
use crate::pool::HttpSessionPool;
use crate::pool::PoolError;
use crate::pool::PoolParams;

const DEADLINE: Duration = Duration::from_secs(10);
const INTERVAL: Duration = Duration::from_millis(50);
/// The probe timeout for tests that expect a drop.
const TIMEOUT: Duration = Duration::from_millis(50);
/// The probe interval for tests that expect a drop after an exchange, long
/// enough that the exchange completes before the first probe.
const DROP_INTERVAL: Duration = Duration::from_millis(500);
/// The probe timeout for tests that expect acknowledged probes to keep the
/// session, long enough that a scheduling stall does not drop it.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HEADERS: u8 = 1;
const SETTINGS: u8 = 4;
const PING: u8 = 6;
const ACK: u8 = 1;
const END_STREAM: u8 = 1;
const END_HEADERS: u8 = 4;
/// HPACK's static-table entry for `:status: 200`.
const STATUS_200: u8 = 0x88;

fn probes(timeout: Duration) -> PingProbeConfig {
    PingProbeConfig {
        interval: INTERVAL,
        timeout,
        ..PingProbeConfig::default()
    }
}

fn dropping_probes() -> PingProbeConfig {
    PingProbeConfig {
        interval: DROP_INTERVAL,
        ..probes(TIMEOUT)
    }
}

fn wait<Work: Future>(work: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(DEADLINE, work)
                .await
                .expect("bounded scenario")
        })
}

fn run_on_worker<Factory, Work>(factory: Factory)
where
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(worker.call_local(factory).expect("scenario admitted")).expect("scenario finishes");
    wait(runtime.quiesce(DEADLINE)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

async fn until(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[derive(Default)]
struct Stats {
    ping_timeouts: Cell<usize>,
}

impl Http2SessionStats for Stats {
    fn record_ping_timeout(&self) {
        self.ping_timeouts.set(self.ping_timeouts.get() + 1);
    }
}

/// What a raw peer received: each PING request, and every byte.
#[derive(Default)]
struct PeerLog {
    pings: Vec<Instant>,
    received: Vec<u8>,
}

type Log = Rc<RefCell<PeerLog>>;

fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small frame");
    let mut bytes = length.to_be_bytes()[1..].to_vec();
    bytes.extend([kind, flags]);
    bytes.extend(stream.to_be_bytes());
    bytes.extend(payload);
    bytes
}

/// An HTTP/2 server written in raw frames, independent of Roxygen's codec. It
/// acknowledges SETTINGS, replies to each request with a `200` head (ending the
/// stream when `end_responses`), and acknowledges only the first `acks` PINGs.
/// It returns when the client closes or resets the connection.
async fn raw_h2_peer(mut io: DuplexStream, acks: usize, end_responses: bool, log: Log) {
    let mut preface = [0; PREFACE.len()];
    if io.read_exact(&mut preface).await.is_err() {
        return;
    }
    assert_eq!(preface, PREFACE);
    if io.write_all(&frame(SETTINGS, 0, 0, &[])).await.is_err() {
        return;
    }
    let mut acked = 0;
    loop {
        let mut header = [0; 9];
        if io.read_exact(&mut header).await.is_err() {
            return;
        }
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
        let (kind, flags) = (header[3], header[4]);
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & !(1 << 31);
        let mut payload = vec![0; length as usize];
        if io.read_exact(&mut payload).await.is_err() {
            return;
        }
        let reply = match kind {
            SETTINGS if flags & ACK == 0 => Some(frame(SETTINGS, ACK, 0, &[])),
            HEADERS => {
                let flags = if end_responses {
                    END_HEADERS | END_STREAM
                } else {
                    END_HEADERS
                };
                Some(frame(HEADERS, flags, stream, &[STATUS_200]))
            }
            PING if flags & ACK == 0 => {
                log.borrow_mut().pings.push(Instant::now());
                (acked < acks).then(|| {
                    acked += 1;
                    frame(PING, ACK, 0, &payload)
                })
            }
            _ => None,
        };
        if let Some(reply) = reply
            && io.write_all(&reply).await.is_err()
        {
            return;
        }
    }
}

/// An HTTP/1.1 server that replies to each request with an empty `200` and
/// records every byte it receives.
async fn raw_h1_peer(mut io: DuplexStream, log: Log) {
    let mut buffer = [0; 4096];
    let mut pending = Vec::<u8>::new();
    loop {
        let Ok(read) = io.read(&mut buffer).await else {
            return;
        };
        if read == 0 {
            return;
        }
        log.borrow_mut().received.extend(&buffer[..read]);
        pending.extend(&buffer[..read]);
        while let Some(end) = pending.windows(4).position(|window| window == b"\r\n\r\n") {
            pending.drain(..end + 4);
            if io
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

fn limits() -> ByteStreamLimits {
    ByteStreamLimits::new(16 * 1024, 16 * 1024, 8).expect("bounded transport")
}

/// Attaches a client session to one end of an in-memory connection and
/// returns the peer's end.
fn attach(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: SessionParams,
) -> (ClientSession, DuplexStream) {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let transport = adopt_local(
        context,
        EstablishedIo::new(
            client,
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        limits(),
    )
    .expect("I/O adopted");
    let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
    options.session = session;
    let session = ClientSession::attach_transport(context, transport, options).expect("attached");
    (session, server)
}

fn get() -> HttpSourceHolder {
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_fixed_request("/", HttpMethod::Get, None).expect("request"),
    ))
}

/// Sends a GET and reads its `200` head.
async fn open_exchange(session: &ClientSession) -> UniplexResponse {
    let mut response = session
        .send_request(get(), CancellationToken::never())
        .await
        .expect("the request is committed");
    let mut head = response
        .source
        .read_header_event()
        .await
        .expect("the response head");
    assert_eq!(head.headers_mut().status_code().expect("status"), 200);
    response
}

/// Sends a GET and reads its `200` response to the end.
async fn exchange(session: &ClientSession) {
    let mut response = session
        .send_request(get(), CancellationToken::never())
        .await
        .expect("the request is committed");
    let mut head = response
        .source
        .read_header_event()
        .await
        .expect("the response head");
    assert_eq!(head.headers_mut().status_code().expect("status"), 200);
    let mut eom = head.eom();
    while !eom {
        eom = response
            .source
            .read_body_event(u32::MAX)
            .await
            .expect("the response body")
            .eom();
    }
}

async fn read_error(response: &mut UniplexResponse) -> HttpError {
    loop {
        match response.source.read_body_event(u32::MAX).await {
            Ok(event) if event.as_body().is_none() && !event.eom() => {}
            Ok(_) => panic!("the response ended instead of failing"),
            Err(error) => return error,
        }
    }
}

fn assert_probe_timeout(error: &HttpError) {
    assert_eq!(error.code(), HttpErrorCode::DROPPED, "{error:?}");
    assert!(is_ping_probe_timeout(error), "{error:?}");
}

/// Counts the PING acknowledgements a server session sent.
struct PingsAnswered(Arc<AtomicUsize>);

impl LifecycleObserver for PingsAnswered {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::Http2PingReplySent { .. }) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct Ok200;

impl HttpHandler for Ok200 {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async {
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// Starts a Roxygen server, over TLS with ALPN h2 or over h2c, that counts the
/// PINGs it acknowledges. Returns the server and the certificate it presents.
fn start_server(tls: bool, pings: &Arc<AtomicUsize>) -> (ScopedHttpServer, Option<Vec<u8>>) {
    let mut config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    let observed = Arc::clone(pings);
    config.session.lifecycle_observer_factory = Some(Arc::new(move || {
        Rc::new(PingsAnswered(Arc::clone(&observed))) as Rc<dyn LifecycleObserver>
    }));
    let certificate = tls.then(|| {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let certificate = generated.cert.der().to_vec();
        let options = TlsServerOptions::new(
            vec![certificate.clone().into_boxed_slice()],
            generated
                .signing_key
                .serialized_der()
                .to_vec()
                .into_boxed_slice(),
            vec![b"h2".to_vec().into_boxed_slice()],
        )
        .expect("TLS options");
        config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(options)));
        certificate
    });
    let server =
        ScopedHttpServer::start(config, Arc::new(|_| Rc::new(Ok200) as HttpHandlerRef), None)
            .expect("server starts");
    (server, certificate)
}

fn connection_params(certificate: Option<Vec<u8>>) -> ConnectionParams {
    match certificate {
        Some(certificate) => {
            let mut tls = TlsClientOptions::new("localhost");
            tls.add_root_certificate(certificate);
            tls.set_alpn_protocols([b"h2".as_slice()]);
            ConnectionParams {
                transport: ClientTransportOptions::Tls(tls),
                ..ConnectionParams::default()
            }
        }
        None => ConnectionParams {
            plaintext_protocol: CodecProtocol::Http2,
            ..ConnectionParams::default()
        },
    }
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

#[test]
fn attach_options_enable_probes_on_an_http2_session() {
    run_on_worker(|context| async move {
        let mut options =
            ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
        options.session.ping_probes = Some(probes(ACK_TIMEOUT));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                client,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("I/O adopted");
        let session =
            ClientSession::attach_transport(&context, transport, options).expect("attached");
        let log = Log::default();
        let script = async {
            until("two probes", || log.borrow().pings.len() >= 2).await;
            let pings = log.borrow().pings.clone();
            assert!(pings[1] - pings[0] >= INTERVAL, "{pings:?}");
            session.close_when_idle().expect("close");
            session
                .wait_closed()
                .await
                .expect("the session closes cleanly");
        };
        tokio::join!(
            raw_h2_peer(server, usize::MAX, true, Rc::clone(&log)),
            script
        );
    });
}

#[test]
fn connector_session_params_enable_probes() {
    for tls in [true, false] {
        let pings = Arc::new(AtomicUsize::new(0));
        let (server, certificate) = start_server(tls, &pings);
        let address = server.address().expect("server address");
        run_on_worker(move |context| async move {
            let session_params = SessionParams {
                ping_probes: Some(probes(ACK_TIMEOUT)),
                ..SessionParams::default()
            };
            let session = HttpConnector::connect(
                &context,
                socket_address(address),
                DEADLINE,
                connection_params(certificate.clone()),
                session_params.clone(),
                CancellationToken::never(),
            )
            .await
            .expect("the connector builds an HTTP/2 session");
            assert_eq!(session.protocol(), CodecProtocol::Http2);
            until("two answered probes", || pings.load(Ordering::SeqCst) >= 2).await;
            assert!(!session.is_finished(), "answered probes keep the session");
            session.close_when_idle().expect("close");
            session
                .wait_closed()
                .await
                .expect("the session closes cleanly");

            // A native pool's sessions carry the same parameters.
            let answered = pings.load(Ordering::SeqCst);
            let pool = HttpSessionPool::new(
                context.clone(),
                socket_address(address),
                PoolParams::default(),
                connection_params(certificate),
                session_params,
            );
            let pooled = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("a pooled session");
            until("two answered pooled probes", || {
                pings.load(Ordering::SeqCst) >= answered + 2
            })
            .await;
            assert!(!pooled.session.is_finished());
            drop(pooled);
            pool.drain_and_join()
                .await
                .expect("the pool drains cleanly");
        });
    }
}

#[test]
fn peer_that_stops_acking_fails_the_session_and_records_the_timeout() {
    run_on_worker(|context| async move {
        let stats = Rc::new(Stats::default());
        let attached = Instant::now();
        let (session, server) = attach(
            &context,
            CodecProtocol::Http2,
            SessionParams {
                ping_probes: Some(dropping_probes()),
                connection_read_timeout: Some(Duration::ZERO),
                session_stats: Some(Rc::clone(&stats) as _),
                ..SessionParams::default()
            },
        );
        let log = Log::default();
        let script = async {
            let mut response = open_exchange(&session).await;
            assert_probe_timeout(&read_error(&mut response).await);
            let error = session.wait_closed().await.expect_err("the session fails");
            assert_probe_timeout(&error);
            assert_eq!(error.message(), b"Ping probe timed out");
            assert!(attached.elapsed() >= DROP_INTERVAL + TIMEOUT);
            assert_eq!(stats.ping_timeouts.get(), 1);
        };
        tokio::join!(raw_h2_peer(server, 0, false, Rc::clone(&log)), script);
        // The peer read everything the session wrote before the reset.
        assert_eq!(log.borrow().pings.len(), 1, "one unanswered probe");
    });
}

#[test]
fn acking_peer_keeps_the_session() {
    run_on_worker(|context| async move {
        let stats = Rc::new(Stats::default());
        let (session, server) = attach(
            &context,
            CodecProtocol::Http2,
            SessionParams {
                ping_probes: Some(probes(ACK_TIMEOUT)),
                connection_read_timeout: Some(Duration::ZERO),
                session_stats: Some(Rc::clone(&stats) as _),
                ..SessionParams::default()
            },
        );
        let log = Log::default();
        let script = async {
            // Each probe after the first needs the previous one acknowledged.
            until("three probes", || log.borrow().pings.len() >= 3).await;
            tokio::time::sleep(INTERVAL * 2).await;
            assert!(
                !session.is_finished(),
                "acknowledged probes keep the session"
            );
            exchange(&session).await;
            assert_eq!(stats.ping_timeouts.get(), 0);
            session.close_when_idle().expect("close");
            session
                .wait_closed()
                .await
                .expect("the session closes cleanly");
        };
        tokio::join!(
            raw_h2_peer(server, usize::MAX, true, Rc::clone(&log)),
            script
        );
    });
}

#[test]
fn http1_session_ignores_ping_probe_params() {
    run_on_worker(|context| async move {
        let stats = Rc::new(Stats::default());
        let (session, server) = attach(
            &context,
            CodecProtocol::Http11,
            SessionParams {
                ping_probes: Some(probes(TIMEOUT)),
                connection_read_timeout: Some(Duration::ZERO),
                session_stats: Some(Rc::clone(&stats) as _),
                ..SessionParams::default()
            },
        );
        let log = Log::default();
        let script = async {
            exchange(&session).await;
            let request = log.borrow().received.len();
            tokio::time::sleep((INTERVAL + TIMEOUT) * 3).await;
            assert!(
                !session.is_finished(),
                "an idle HTTP/1.1 session stays open"
            );
            assert_eq!(log.borrow().received.len(), request, "no probe bytes");
            exchange(&session).await;
            assert!(!log.borrow().received.starts_with(PREFACE));
            assert_eq!(stats.ping_timeouts.get(), 0);
            session.close_when_idle().expect("close");
            session
                .wait_closed()
                .await
                .expect("the session closes cleanly");
        };
        tokio::join!(raw_h1_peer(server, Rc::clone(&log)), script);
    });
}

#[test]
fn default_session_params_send_no_probe() {
    assert!(SessionParams::default().ping_probes.is_none());
    run_on_worker(|context| async move {
        let (session, server) = attach(&context, CodecProtocol::Http2, SessionParams::default());
        let log = Log::default();
        let script = async {
            exchange(&session).await;
            tokio::time::sleep((INTERVAL + TIMEOUT) * 3).await;
            assert!(log.borrow().pings.is_empty(), "no probe");
            session.close_when_idle().expect("close");
            session
                .wait_closed()
                .await
                .expect("the session closes cleanly");
        };
        tokio::join!(
            raw_h2_peer(server, usize::MAX, true, Rc::clone(&log)),
            script
        );
    });

    let pings = Arc::new(AtomicUsize::new(0));
    let (server, _) = start_server(false, &pings);
    let address = server.address().expect("server address");
    run_on_worker(move |context| async move {
        let session = HttpConnector::connect(
            &context,
            socket_address(address),
            DEADLINE,
            connection_params(None),
            SessionParams::default(),
            CancellationToken::never(),
        )
        .await
        .expect("h2c session");
        tokio::time::sleep((INTERVAL + TIMEOUT) * 3).await;
        assert_eq!(pings.load(Ordering::SeqCst), 0);
        session.close_when_idle().expect("close");
        session
            .wait_closed()
            .await
            .expect("the session closes cleanly");
    });
}

struct Factory {
    calls: Rc<Cell<usize>>,
    peers: Rc<RefCell<VecDeque<DuplexStream>>>,
}

impl AttachedSessionFactory for Factory {
    fn create(
        &self,
        context: WorkerContext,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>> {
        self.calls.set(self.calls.get() + 1);
        // Only the first session's peer stops acknowledging.
        let ping_probes = if self.calls.get() == 1 {
            dropping_probes()
        } else {
            probes(ACK_TIMEOUT)
        };
        let (session, server) = attach(
            &context,
            CodecProtocol::Http2,
            SessionParams {
                ping_probes: Some(ping_probes),
                connection_read_timeout: Some(Duration::ZERO),
                ..SessionParams::default()
            },
        );
        self.peers.borrow_mut().push_back(server);
        Box::pin(async move { Ok(session) })
    }
}

#[test]
fn attached_pool_replaces_a_session_dropped_by_a_probe_timeout() {
    run_on_worker(|context| async move {
        let calls = Rc::new(Cell::new(0));
        let peers = Rc::new(RefCell::new(VecDeque::new()));
        let pool = HttpSessionPool::try_with_attached_factory(
            context.clone(),
            AttachedPoolParams {
                max_connections: 1,
                max_connection_attempts: 1,
                connect_timeout: DEADLINE,
                max_age: Duration::ZERO,
                max_waiters: 1,
                max_retiring: None,
                pending_connect_credit: 1,
            },
            TransportSecurity::Plaintext,
            Rc::new(Factory {
                calls: Rc::clone(&calls),
                peers: Rc::clone(&peers),
            }),
        )
        .expect("attached pool");
        let first = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("first session");
        // The first peer leaves its response open and never acknowledges a probe.
        let silent = peers.borrow_mut().pop_front().expect("first peer");
        let dropped = async {
            let session = first.session.clone();
            let mut response = session
                .send_reserved_request(get(), first.reservation, CancellationToken::never())
                .await
                .expect("the request is committed");
            let head = response.source.read_header_event().await.expect("head");
            assert!(!head.eom());
            assert_probe_timeout(&read_error(&mut response).await);
            drop(response);
            assert_probe_timeout(&session.wait_closed().await.expect_err("the probe drops it"));
            session
        };
        let (session, ()) = tokio::join!(dropped, raw_h2_peer(silent, 0, false, Log::default()));

        let replacement = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("a replacement session");
        assert!(!replacement.session.is_same_session(&session));
        assert_eq!(calls.get(), 2);
        let acking = peers.borrow_mut().pop_front().expect("second peer");
        let replaced = async {
            let mut response = replacement
                .session
                .send_reserved_request(get(), replacement.reservation, CancellationToken::never())
                .await
                .expect("the request is committed");
            assert!(
                response
                    .source
                    .read_header_event()
                    .await
                    .expect("head")
                    .eom()
            );
            drop(response);
            // The pool joins the dropped session and reports its failure.
            match pool.drain_and_join().await {
                Err(PoolError::SessionJoin { source }) => assert_probe_timeout(&source),
                other => panic!("the pool reports the probe drop: {other:?}"),
            }
        };
        tokio::join!(
            replaced,
            raw_h2_peer(acking, usize::MAX, true, Log::default())
        );
    });
}
