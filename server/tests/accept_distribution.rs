/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! How `HttpServer` distributes accepted sockets to its workers, and how it
//! stops accepting.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use tokio::sync::watch;

use crate::AcceptDistribution;
use crate::AcceptStats;
use crate::StopAccepting;
use crate::server::DequeueHoldForTest;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::HttpServerError;
use crate::server::HttpServerState;
use crate::server::ServerConfig;
use crate::server::ServerObserver;
use crate::server::StaticTlsCredentials;

/// Bounds every wait in these tests.
const TIMEOUT: Duration = Duration::from_secs(10);

/// Holds requests until a test opens it, and counts the requests that reached a
/// handler.
struct Gate {
    open: watch::Sender<bool>,
    arrived: AtomicUsize,
}

impl Gate {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self {
            open: watch::Sender::new(open),
            arrived: AtomicUsize::new(0),
        })
    }

    fn open(&self) {
        self.open.send_replace(true);
    }

    fn arrived(&self) -> usize {
        self.arrived.load(Ordering::Acquire)
    }
}

/// Answers every request with `200 OK` once its gate is open.
struct GatedHandler {
    gate: Arc<Gate>,
}

impl HttpHandler for GatedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            let headers = request.read_header_event().await?;
            if !headers.eom() {
                while !request.read_body_event(u32::MAX).await?.eom() {}
            }
            gate.arrived.fetch_add(1, Ordering::AcqRel);
            let _ = gate.open.subscribe().wait_for(|open| *open).await;
            let body = BufQueue::from_vec(b"OK".to_vec(), 2).expect("fixed body is bounded");
            let response = HttpFixedSource::make_fixed_response(200, Some(body))
                .expect("status and body are valid");
            Ok(HttpSourceHolder::new(Box::new(response)))
        })
    }
}

/// A handler factory whose handlers answer through `gate`.
fn gated_factory(gate: &Arc<Gate>) -> HandlerFactory {
    let gate = Arc::clone(gate);
    Arc::new(move |_| {
        Rc::new(GatedHandler {
            gate: Arc::clone(&gate),
        }) as HttpHandlerRef
    })
}

/// A handler factory whose handlers answer at once.
fn ok_factory() -> HandlerFactory {
    gated_factory(&Gate::new(true))
}

/// A server of `workers` workers in `mode`, with no signal handler. Per-worker
/// accept requires `SO_REUSEPORT`.
fn config(mode: AcceptDistribution, workers: usize) -> ServerConfig {
    let mut config = ServerConfig {
        num_io_threads: workers,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.socket.accept_distribution = mode;
    config.socket.reuse_port = mode == AcceptDistribution::PerWorkerReusePort;
    config
}

/// Waits up to `TIMEOUT` for `condition`.
fn wait_until(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
    true
}

/// The inodes of the TCP sockets listening on the IPv4 `address` in this
/// network namespace.
fn listening_inodes(address: SocketAddr) -> Vec<u64> {
    let SocketAddr::V4(address) = address else {
        panic!("the tests listen on IPv4 loopback");
    };
    // `/proc/net/tcp` prints the address as the hex of its in-memory `u32`.
    let local = format!(
        "{:08X}:{:04X}",
        u32::from_ne_bytes(address.ip().octets()),
        address.port()
    );
    std::fs::read_to_string("/proc/net/tcp")
        .expect("the TCP socket table is readable")
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            // State `0A` is LISTEN.
            (fields.len() > 9 && fields[1] == local && fields[3] == "0A")
                .then(|| fields[9].parse().expect("an inode is a number"))
        })
        .collect()
}

/// The sockets listening on the IPv4 `address` that this process holds a
/// descriptor for. A child that another test forks inherits this process's
/// descriptors until it execs, so a listener the server has closed can keep
/// listening for that long; the count leaves such a listener out.
fn listening_sockets(address: SocketAddr) -> usize {
    let held: HashSet<_> = std::fs::read_dir("/proc/self/fd")
        .expect("this process's descriptors are listable")
        .flatten()
        .filter_map(|descriptor| std::fs::read_link(descriptor.path()).ok())
        .collect();
    listening_inodes(address)
        .into_iter()
        .filter(|inode| held.contains(Path::new(&format!("socket:[{inode}]"))))
        .count()
}

/// Connects to `address` with bounded reads and writes.
fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).expect("client connects");
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}

/// Asserts that nothing listens on `address`, once no forked child still
/// holds a closed listener open (see `listening_sockets`).
fn assert_refused(address: SocketAddr) {
    assert!(
        wait_until(|| listening_inodes(address).is_empty()),
        "every socket listening on {address} closes"
    );
    let error = TcpStream::connect_timeout(&address, TIMEOUT)
        .expect_err("no listener accepts the connection");
    assert_eq!(error.kind(), ErrorKind::ConnectionRefused, "{error}");
}

/// Sends an HTTP/1.1 GET on a new connection and returns the response.
fn get(address: SocketAddr) -> Vec<u8> {
    let mut stream = connect(address);
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    response
}

/// Runs `work` on its own thread and returns its result's receiver.
fn on_thread<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(work());
    });
    receiver
}

/// Counts the workers that stopped.
#[derive(Default)]
struct StopCounter(AtomicUsize);

impl ServerObserver for StopCounter {
    fn on_thread_stop(&self, _worker_index: usize) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

impl StopCounter {
    fn stopped(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// Each worker's `accepted` count.
fn accepted(server: &HttpServer) -> Vec<u64> {
    server
        .control()
        .accept_stats()
        .iter()
        .map(|stats| stats.accepted)
        .collect()
}

/// Asserts that the server closed `stream`: a read sees EOF or a reset.
fn assert_closed(stream: &mut impl Read) {
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::UnexpectedEof
            ) => {}
        result => panic!("the server closes the socket within the read timeout: {result:?}"),
    }
}

/// Server credentials for `localhost` that offer `protocols` through ALPN, and
/// the certificate a client trusts.
fn generated_tls(protocols: &[&[u8]]) -> (TlsServerOptions, Vec<u8>) {
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
            .iter()
            .map(|protocol| protocol.to_vec().into_boxed_slice())
            .collect(),
    )
    .unwrap();
    (options, certificate)
}

/// A TLS connection to `address` that trusts only `certificate` and offers
/// `protocol` through ALPN. The handshake runs on first use.
fn tls_connect(
    address: SocketAddr,
    certificate: &[u8],
    protocol: &[u8],
) -> StreamOwned<ClientConnection, TcpStream> {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate.to_vec()))
        .unwrap();
    let mut client = ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![protocol.to_vec()];
    let connection = ClientConnection::new(
        Arc::new(client),
        ServerName::try_from("localhost".to_owned()).unwrap(),
    )
    .unwrap();
    StreamOwned::new(connection, connect(address))
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const H2_HEADERS: u8 = 0x1;
const H2_SETTINGS: u8 = 0x4;
const H2_PING: u8 = 0x6;
const H2_GOAWAY: u8 = 0x7;
const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
/// HPACK static table index 8, `:status: 200`.
const STATUS_200: u8 = 0x88;

struct H2Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).unwrap().to_be_bytes();
    let mut frame = length[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Writes the client preface and an empty SETTINGS frame.
fn start_h2(stream: &mut impl Write) {
    let mut wire = H2_PREFACE.to_vec();
    wire.extend(h2_frame(H2_SETTINGS, 0, 0, &[]));
    stream.write_all(&wire).unwrap();
    stream.flush().unwrap();
}

/// Reads frames, acknowledging SETTINGS and PING, until `done` holds for one,
/// and returns it.
fn read_h2_until(
    stream: &mut (impl Read + Write),
    done: impl Fn(&H2Frame) -> bool,
) -> std::io::Result<H2Frame> {
    loop {
        let mut header = [0_u8; 9];
        stream.read_exact(&mut header)?;
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload)?;
        let frame = H2Frame {
            kind: header[3],
            flags: header[4],
            stream: u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
            payload,
        };
        if done(&frame) {
            return Ok(frame);
        }
        if frame.kind == H2_SETTINGS && frame.flags & ACK == 0 {
            stream.write_all(&h2_frame(H2_SETTINGS, ACK, 0, &[]))?;
        } else if frame.kind == H2_PING && frame.flags & ACK == 0 {
            stream.write_all(&h2_frame(H2_PING, ACK, 0, &frame.payload))?;
        }
    }
}

/// Reads until the server's GOAWAY. Tests that expect it from a drain disable
/// the connection idle timeout, whose GOAWAY would otherwise arrive within the
/// read timeout.
fn read_goaway(stream: &mut (impl Read + Write)) {
    read_h2_until(stream, |frame| frame.kind == H2_GOAWAY).expect("the server sends GOAWAY");
}

/// Sends a GET on `stream` and reads its response; returns whether the status
/// is 200.
fn h2_get(stream: &mut (impl Read + Write), id: u32) -> bool {
    // :method GET, :scheme http, :path /, then :authority localhost.
    let mut block = vec![0x82, 0x86, 0x84, 0x41, 9];
    block.extend_from_slice(b"localhost");
    stream
        .write_all(&h2_frame(H2_HEADERS, END_STREAM | END_HEADERS, id, &block))
        .unwrap();
    let mut status_200 = None;
    loop {
        let frame = read_h2_until(stream, |frame| frame.stream == id)
            .expect("the response arrives on its stream");
        if frame.kind == H2_HEADERS && status_200.is_none() {
            status_200 = Some(frame.payload.first() == Some(&STATUS_200));
        }
        if frame.flags & END_STREAM != 0 {
            return status_200 == Some(true);
        }
    }
}

/// Runs `future` to completion on a new runtime, within `TIMEOUT`.
fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { tokio::time::timeout(TIMEOUT, future).await })
        .expect("the future completes within the timeout")
}

/// Whether `future` completes within `TIMEOUT`, run on a new runtime.
fn completes(future: impl Future) -> bool {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { tokio::time::timeout(TIMEOUT, future).await })
        .is_ok()
}

/// Starts a server and returns it with each worker's handle, in worker index
/// order.
fn start_with_handles(
    config: ServerConfig,
    factory: HandlerFactory,
    observer: Option<Arc<dyn ServerObserver>>,
) -> (HttpServer, Vec<WorkerHandle>) {
    let handles = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&handles);
    let server = HttpServer::start_with_worker_factory(
        config,
        Arc::new(move |context: WorkerContext, index| {
            captured
                .lock()
                .unwrap()
                .push((index, context.handle().clone()));
            factory(index)
        }),
        observer,
    )
    .unwrap();
    let mut handles = std::mem::take(&mut *handles.lock().unwrap());
    handles.sort_by_key(|(index, _)| *index);
    (
        server,
        handles.into_iter().map(|(_, handle)| handle).collect(),
    )
}

/// Blocks a worker's thread with a command until dropped.
struct Stall {
    _release: mpsc::Sender<()>,
}

/// Blocks `worker`'s thread, so it polls none of its tasks, until the returned
/// `Stall` is dropped.
fn stall(worker: &WorkerHandle) -> Stall {
    let (started, running) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    worker
        .submit(move |_| {
            let _ = started.send(());
            let _ = released.recv();
        })
        .expect("the worker admits the blocking command");
    running
        .recv_timeout(TIMEOUT)
        .expect("the worker runs the blocking command");
    Stall { _release: release }
}

/// A one-worker per-worker server whose accepted-socket queue holds one socket,
/// with its dequeue held, and three plaintext H2 clients that sent their
/// preface: one socket is queued, the listener task holds the second while it
/// awaits capacity, and the third waits in the listen backlog.
struct HeldSockets {
    server: HttpServer,
    hold: Arc<DequeueHoldForTest>,
    address: SocketAddr,
    clients: Vec<TcpStream>,
    stops: Arc<StopCounter>,
}

impl HeldSockets {
    fn start() -> Self {
        let hold = DequeueHoldForTest::new();
        let mut config = config(AcceptDistribution::PerWorkerReusePort, 1);
        config.command_capacity = 1;
        config.plaintext_protocol = CodecProtocol::Http2;
        config.session.connection_idle_timeout = Duration::ZERO;
        config.dequeue_hold_for_test = Some(Arc::clone(&hold));
        let stops = Arc::new(StopCounter::default());
        let server = HttpServer::start(
            config,
            ok_factory(),
            Some(Arc::clone(&stops) as Arc<dyn ServerObserver>),
        )
        .unwrap();
        let address = server.address().expect("one listener address");
        block_on(hold.wait_held(0));
        let clients = (0..3)
            .map(|_| {
                let mut client = connect(address);
                start_h2(&mut client);
                client
            })
            .collect();
        let held = Self {
            server,
            hold,
            address,
            clients,
            stops,
        };
        assert!(
            wait_until(|| held.stats().pending_handoff == 2),
            "{:?}",
            held.stats()
        );
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            held.stats().accepted,
            2,
            "the third connection is not accepted"
        );
        held
    }

    fn stats(&self) -> AcceptStats {
        self.server.control().accept_stats()[0]
    }
}

#[test]
fn per_worker_accept_requires_reuse_port() {
    let factory = ok_factory();
    let mut without_reuse_port = config(AcceptDistribution::PerWorkerReusePort, 2);
    without_reuse_port.socket.reuse_port = false;
    let error = HttpServer::start(without_reuse_port, Arc::clone(&factory), None)
        .err()
        .expect("per-worker accept without SO_REUSEPORT is rejected");
    assert!(
        matches!(
            error,
            HttpServerError::InvalidConfig(
                "per-worker accept requires reuse_port and no prebound listeners"
            )
        ),
        "{error}"
    );

    let mut prebound = config(AcceptDistribution::PerWorkerReusePort, 2);
    prebound.socket.prebound_listeners = vec![TcpListener::bind("127.0.0.1:0").unwrap()];
    let error = HttpServer::start(prebound, Arc::clone(&factory), None)
        .err()
        .expect("per-worker accept with a prebound listener is rejected");
    assert!(
        matches!(
            error,
            HttpServerError::InvalidConfig(
                "per-worker accept requires reuse_port and no prebound listeners"
            )
        ),
        "{error}"
    );

    let server = HttpServer::start(
        config(AcceptDistribution::PerWorkerReusePort, 2),
        factory,
        None,
    )
    .expect("per-worker accept with SO_REUSEPORT starts");
    let response = get(server.address().expect("one listener address"));
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"), "{response:?}");
    assert!(server.force_stop().unwrap());
}

#[test]
fn drain_closes_every_worker_listener() {
    for mode in [
        AcceptDistribution::Worker0,
        AcceptDistribution::PerWorkerReusePort,
    ] {
        let gate = Gate::new(false);
        let server = HttpServer::start(config(mode, 2), gated_factory(&gate), None).unwrap();
        let address = server.address().expect("one listener address");
        let expected = if mode == AcceptDistribution::Worker0 {
            1
        } else {
            2
        };
        assert_eq!(listening_sockets(address), expected, "{mode:?}");

        // A request held by its handler keeps its worker draining.
        let held = on_thread(move || get(address));
        assert!(wait_until(|| gate.arrived() == 1), "{mode:?}");
        assert!(server.drain());
        assert!(
            wait_until(|| listening_sockets(address) == 0),
            "{mode:?}: drain closes every listener while a request is still active"
        );
        assert_refused(address);
        assert_eq!(server.state(), HttpServerState::Draining, "{mode:?}");

        gate.open();
        let response = held.recv_timeout(TIMEOUT).expect("held request completes");
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "{mode:?}: {response:?}"
        );
        assert!(server.wait_for_stop().unwrap(), "{mode:?}");
    }
}

#[test]
fn per_worker_accept_shares_ephemeral_port() {
    let mut config = config(AcceptDistribution::PerWorkerReusePort, 4);
    config.socket.bind_addresses = vec![
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    ];
    let server = HttpServer::start(config, ok_factory(), None).unwrap();
    let addresses = server.addresses().to_vec();
    assert_eq!(addresses.len(), 2, "one reported address per bind address");
    assert_ne!(addresses[0].port(), addresses[1].port());
    for address in addresses {
        assert_ne!(address.port(), 0, "the kernel assigned a port");
        assert_eq!(
            listening_sockets(address),
            4,
            "every worker listens on the port worker 0 was assigned"
        );
        let response = get(address);
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"), "{response:?}");
    }
    assert!(server.force_stop().unwrap());
}

#[test]
fn per_worker_accept_spreads_connections() {
    let server = HttpServer::start(
        config(AcceptDistribution::PerWorkerReusePort, 4),
        ok_factory(),
        None,
    )
    .unwrap();
    let address = server.address().expect("one listener address");
    for _ in 0..256 {
        let response = get(address);
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"), "{response:?}");
    }
    let stats: Vec<AcceptStats> = server.control().accept_stats();
    assert_eq!(stats.len(), 4, "one entry per worker");
    assert!(
        stats.iter().all(|stats| stats.accepted >= 1),
        "every worker accepts: {stats:?}"
    );
    assert_eq!(stats.iter().map(|stats| stats.accepted).sum::<u64>(), 256);
    assert!(server.force_stop().unwrap());
}

#[test]
fn per_worker_stalled_tls_client_does_not_delay_next_accept() {
    let (tls, certificate) = generated_tls(&[b"http/1.1"]);
    let mut config = config(AcceptDistribution::PerWorkerReusePort, 1);
    config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(tls)));
    let server = HttpServer::start(config, ok_factory(), None).unwrap();
    let address = server.address().expect("one listener address");

    // Opens TCP to a TLS server and never sends its ClientHello.
    let mut stalled = connect(address);
    assert!(wait_until(|| accepted(&server) == [1]));

    let started = Instant::now();
    let mut client = tls_connect(address, &certificate, b"http/1.1");
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    if let Err(error) = client.read_to_end(&mut response) {
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof, "{error}");
    }
    let elapsed = started.elapsed();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"), "{response:?}");
    assert!(
        elapsed < Duration::from_secs(1),
        "the second handshake does not wait for the stalled one: {elapsed:?}"
    );
    assert_eq!(accepted(&server), [2]);
    assert!(server.force_stop().unwrap());
    assert_closed(&mut stalled);
}

#[test]
fn per_worker_accepted_sessions_see_drain_and_force() {
    let (tls, certificate) = generated_tls(&[b"h2"]);
    let mut config = config(AcceptDistribution::PerWorkerReusePort, 1);
    config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(tls)));
    config.session.connection_idle_timeout = Duration::ZERO;
    let stops = Arc::new(StopCounter::default());
    let server = HttpServer::start(
        config,
        ok_factory(),
        Some(Arc::clone(&stops) as Arc<dyn ServerObserver>),
    )
    .unwrap();
    let address = server.address().expect("one listener address");

    // The first session stays in its TLS handshake.
    let mut handshaking = connect(address);
    assert!(wait_until(|| accepted(&server) == [1]));
    let mut client = tls_connect(address, &certificate, b"h2");
    start_h2(&mut client);
    read_h2_until(&mut client, |frame| {
        frame.kind == H2_SETTINGS && frame.flags & ACK == 0
    })
    .expect("the H2 session starts");
    assert_eq!(accepted(&server), [2]);

    assert!(server.drain());
    read_goaway(&mut client);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        server.state(),
        HttpServerState::Draining,
        "the handshaking session keeps its worker draining"
    );
    assert_eq!(stops.stopped(), 0);

    assert!(server.force_stop().unwrap());
    assert_eq!(server.state(), HttpServerState::Stopped);
    assert_eq!(stops.stopped(), 1, "the worker loop exited");
    assert_closed(&mut handshaking);
}

#[test]
fn worker0_full_handoff_is_counted_and_closed() {
    let mut config = config(AcceptDistribution::Worker0, 2);
    config.command_capacity = 1;
    let (server, workers) = start_with_handles(config, ok_factory(), None);
    let address = server.address().expect("one listener address");
    let stalled = stall(&workers[1]);

    // Round robin: worker 0, worker 1 (queued, since worker 1 cannot dequeue), worker 0, then
    // worker 1 again with its one-socket queue full.
    let mut clients = Vec::new();
    for expected in [[1, 0], [1, 1], [2, 1], [2, 2]] {
        clients.push(connect(address));
        assert!(
            wait_until(|| accepted(&server) == expected),
            "{expected:?}: {:?}",
            server.control().accept_stats()
        );
    }
    let stats = server.control().accept_stats();
    assert_eq!(stats[1].handoff_dropped, 1, "{stats:?}");
    assert_eq!(
        stats[1].pending_handoff, 1,
        "one socket sits in the queue: {stats:?}"
    );
    assert_eq!(stats[0].handoff_dropped, 0, "{stats:?}");
    assert!(
        wait_until(|| server.control().accept_stats()[0].pending_handoff == 0),
        "worker 0 sessionizes its sockets: {:?}",
        server.control().accept_stats()
    );

    // Regression check: dropping the socket closes it.
    let dropped = clients.last_mut().unwrap();
    dropped
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    assert_closed(dropped);

    drop(stalled);
    let queued = &mut clients[1];
    queued
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = Vec::new();
    queued.read_to_end(&mut response).unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "the queued socket becomes a session once worker 1 runs: {response:?}"
    );
    let stats = server.control().accept_stats();
    assert_eq!(stats[1].pending_handoff, 0, "{stats:?}");
    assert_eq!(stats[1].handoff_dropped, 1, "{stats:?}");
    assert!(server.force_stop().unwrap());
}

#[test]
fn accepted_socket_capacity_is_independent_of_command_capacity() {
    // Worker0: worker 1 cannot dequeue, and its queue holds three sockets although the worker
    // command queue holds one.
    let mut config0 = config(AcceptDistribution::Worker0, 2);
    config0.command_capacity = 1;
    config0.accepted_socket_capacity = NonZeroUsize::new(3);
    let (server, workers) = start_with_handles(config0, ok_factory(), None);
    let address = server.address().expect("one listener address");
    let stalled = stall(&workers[1]);
    let clients: Vec<_> = (0..8).map(|_| connect(address)).collect();
    assert!(
        wait_until(|| accepted(&server) == [4, 4]),
        "{:?}",
        server.control().accept_stats()
    );
    let stats = server.control().accept_stats();
    assert_eq!(stats[1].pending_handoff, 3, "{stats:?}");
    assert_eq!(stats[1].handoff_dropped, 1, "{stats:?}");
    drop(stalled);
    assert!(server.force_stop().unwrap());
    drop(clients);

    // PerWorkerReusePort: with its dequeue held, three sockets queue and the listener task
    // holds a fourth while it awaits capacity; the fifth stays in the kernel backlog.
    let hold = DequeueHoldForTest::new();
    let mut config1 = config(AcceptDistribution::PerWorkerReusePort, 1);
    config1.command_capacity = 1;
    config1.accepted_socket_capacity = NonZeroUsize::new(3);
    config1.dequeue_hold_for_test = Some(Arc::clone(&hold));
    let server = HttpServer::start(config1, ok_factory(), None).unwrap();
    let address = server.address().expect("one listener address");
    block_on(hold.wait_held(0));
    let clients: Vec<_> = (0..5).map(|_| connect(address)).collect();
    assert!(
        wait_until(|| server.control().accept_stats()[0].pending_handoff == 4),
        "{:?}",
        server.control().accept_stats()
    );
    thread::sleep(Duration::from_millis(100));
    let stats = server.control().accept_stats()[0];
    assert_eq!(stats.accepted, 4, "{stats:?}");
    assert_eq!(stats.pending_handoff, 4, "{stats:?}");
    assert_eq!(stats.handoff_dropped, 0, "{stats:?}");

    hold.release();
    assert!(
        wait_until(|| {
            let stats = server.control().accept_stats()[0];
            stats.accepted == 5 && stats.pending_handoff == 0
        }),
        "{:?}",
        server.control().accept_stats()
    );
    assert!(server.force_stop().unwrap());
    drop(clients);
}

#[test]
fn stop_accepting_closes_listeners_and_keeps_sessions() {
    for mode in [
        AcceptDistribution::Worker0,
        AcceptDistribution::PerWorkerReusePort,
    ] {
        let mut config = config(mode, 2);
        config.plaintext_protocol = CodecProtocol::Http2;
        config.session.connection_idle_timeout = Duration::ZERO;
        let server = HttpServer::start(config, ok_factory(), None).unwrap();
        let address = server.address().expect("one listener address");
        let mut client = connect(address);
        start_h2(&mut client);
        assert!(h2_get(&mut client, 1), "{mode:?}");

        block_on(server.control().stop_accepting());
        assert_eq!(
            listening_sockets(address),
            0,
            "{mode:?}: every listener is closed once the stop resolves"
        );
        assert_refused(address);
        assert_eq!(server.state(), HttpServerState::Running, "{mode:?}");
        assert!(
            h2_get(&mut client, 3),
            "{mode:?}: the open session still serves"
        );

        assert!(server.drain());
        read_goaway(&mut client);
        drop(client);
        assert!(server.wait_for_stop().unwrap(), "{mode:?}");
    }
}

#[test]
fn stop_accepting_reports_unacknowledged_workers() {
    for mode in [
        AcceptDistribution::Worker0,
        AcceptDistribution::PerWorkerReusePort,
    ] {
        let (server, workers) = start_with_handles(config(mode, 2), ok_factory(), None);
        let address = server.address().expect("one listener address");
        // Worker 0's listener, and in per-worker mode worker 1's own.
        let worker1_listeners = usize::from(mode == AcceptDistribution::PerWorkerReusePort);
        assert_eq!(
            listening_sockets(address),
            1 + worker1_listeners,
            "{mode:?}"
        );
        let stalled = stall(&workers[1]);

        let mut stop: StopAccepting = server.control().stop_accepting();
        assert!(
            wait_until(|| stop.unacknowledged() == [1]),
            "{mode:?}: {:?}",
            stop.unacknowledged()
        );
        assert_eq!(
            listening_sockets(address),
            worker1_listeners,
            "{mode:?}: worker 0's listener is closed while worker 1 is blocked"
        );

        drop(stalled);
        block_on(&mut stop);
        assert!(stop.unacknowledged().is_empty(), "{mode:?}");
        assert_eq!(listening_sockets(address), 0, "{mode:?}");
        assert!(server.force_stop().unwrap(), "{mode:?}");
    }
}

#[test]
fn stop_accepting_acks_with_default_signal_handler() {
    let config = ServerConfig {
        num_io_threads: 2,
        ..ServerConfig::default()
    };
    assert_eq!(
        config.shutdown_on_signals,
        [libc::SIGINT],
        "worker 0 runs the default signal task"
    );
    let stops = Arc::new(StopCounter::default());
    let server = HttpServer::start(
        config,
        ok_factory(),
        Some(Arc::clone(&stops) as Arc<dyn ServerObserver>),
    )
    .unwrap();
    let address = server.address().expect("one listener address");

    let stop = server.control().stop_accepting();
    assert!(
        completes(stop),
        "the signal task does not hold back worker 0's acknowledgement"
    );
    assert_eq!(
        listening_sockets(address),
        0,
        "the listener is closed once the stop resolves"
    );
    assert_refused(address);
    assert_eq!(server.state(), HttpServerState::Running);

    // A later drain still ends worker 0's signal task, so both workers exit.
    assert!(server.drain());
    assert!(
        wait_until(|| stops.stopped() == 2),
        "both workers exit after the drain"
    );
    assert!(server.wait_for_stop().unwrap());
}

#[test]
fn stop_accepting_ack_waits_for_held_socket_handoff() {
    let mut held = HeldSockets::start();
    let address = held.address;
    let stop = held.server.control().stop_accepting();
    assert!(
        wait_until(|| listening_sockets(address) == 0),
        "the accept task drops its listener at once, while it still awaits capacity"
    );
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        stop.unacknowledged(),
        [0],
        "the accept task still holds a socket"
    );
    assert_eq!(held.stats().pending_handoff, 2);

    held.hold.release();
    assert!(
        completes(stop),
        "the stop resolves once the held socket is handed off"
    );
    assert_refused(address);
    assert!(
        wait_until(|| held.stats().pending_handoff == 0),
        "{:?}",
        held.stats()
    );
    assert_eq!(held.stats().accepted, 2);
    for client in &mut held.clients[..2] {
        assert!(
            h2_get(client, 1),
            "each handed-off socket becomes a session"
        );
    }
    assert_closed(&mut held.clients[2]);
    assert!(held.server.force_stop().unwrap());
}

#[test]
fn drain_with_full_accepted_channel_sessionizes_every_accepted_socket() {
    let mut held = HeldSockets::start();
    let address = held.address;
    assert!(held.server.drain());
    assert!(
        wait_until(|| listening_sockets(address) == 0),
        "the drain closes the listener"
    );
    thread::sleep(Duration::from_millis(100));
    assert_eq!(held.stats().pending_handoff, 2, "{:?}", held.stats());
    assert_eq!(
        held.stops.stopped(),
        0,
        "the worker loop waits for the accepted sockets"
    );
    assert_eq!(held.server.state(), HttpServerState::Draining);

    held.hold.release();
    for client in &mut held.clients[..2] {
        read_goaway(client);
    }
    assert!(
        wait_until(|| held.stats().pending_handoff == 0),
        "{:?}",
        held.stats()
    );
    let stats = held.stats();
    assert_eq!(stats.accepted, 2, "{stats:?}");
    assert_eq!(stats.handoff_dropped, 0, "{stats:?}");
    assert_eq!(stats.closed_at_force_stop, 0, "{stats:?}");
    held.clients.clear();
    assert!(held.server.wait_for_stop().unwrap());
    assert_eq!(held.stops.stopped(), 1);
}

#[test]
fn force_stop_closes_and_counts_unsessionized_sockets() {
    let mut held = HeldSockets::start();
    assert!(held.server.force_stop().unwrap());
    assert_eq!(held.server.state(), HttpServerState::Stopped);
    assert_eq!(held.stops.stopped(), 1, "the worker loop exited");
    let stats = held.stats();
    assert_eq!(
        stats.closed_at_force_stop, 2,
        "the queued and the held socket are counted: {stats:?}"
    );
    assert_eq!(stats.pending_handoff, 0, "{stats:?}");
    assert_eq!(stats.accepted, 2, "{stats:?}");
    for client in &mut held.clients[..2] {
        assert_closed(client);
    }
    held.hold.release();
}

#[test]
fn worker0_drain_counts_queued_sockets_as_handoff_dropped() {
    let (server, workers) =
        start_with_handles(config(AcceptDistribution::Worker0, 2), ok_factory(), None);
    let address = server.address().expect("one listener address");
    let stalled = stall(&workers[1]);
    let _served = connect(address);
    let mut queued = connect(address);
    assert!(
        wait_until(|| accepted(&server) == [1, 1]),
        "{:?}",
        server.control().accept_stats()
    );

    // Worker 1 sees the drain before it could dequeue, so it stops dequeuing and closes the
    // queued socket when it exits.
    assert!(server.drain());
    drop(stalled);
    assert_closed(&mut queued);
    assert!(server.wait_for_stop().unwrap());
    let stats = server.control().accept_stats();
    assert_eq!(stats[1].handoff_dropped, 1, "{stats:?}");
    assert_eq!(
        stats[1].closed_at_force_stop, 0,
        "the stop was not forced: {stats:?}"
    );
    assert_eq!(stats[1].pending_handoff, 0, "{stats:?}");
}
