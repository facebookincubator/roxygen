//! The owned HTTP/1.1 client pool (`DIV-046`) as an egress client of uvicorn and Envoy peers,
//! over loopback TCP against a scripted peer, under API Proxy's pool parameters: a connection
//! the peer closes while it is idle (uvicorn closes a keep-alive connection after 5 s, scaled
//! down here), thousands of connections, responses that close their connection or delimit
//! their body by it, chunked responses, and a cancel in the middle of a response. Each scenario
//! runs on a `RuntimeOwner` worker that must quiesce without forcing, except the close race,
//! which runs on a `ManagedWorker` and a `HostedWorker`.

use std::cell::Cell;
use std::future::Future;
use std::future::poll_fn;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::experimental::OwnedRequestProgressExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::experimental::owned::OwnedClientPullThroughBody;
use crate::experimental::owned::OwnedClientRequest;
use crate::experimental::owned::OwnedClientReservation;
use crate::experimental::owned::OwnedClientSession;
use crate::experimental::owned::OwnedGetSessionResult;
use crate::experimental::owned::OwnedHttpSessionPool;
use crate::pool::IdleSweep;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;

const TIMEOUT: Duration = Duration::from_secs(60);
/// The peer's keep-alive timeout, for uvicorn's 5 s `timeout_keep_alive`.
const KEEP_ALIVE: Duration = Duration::from_millis(150);
/// Connections in the many-connection tests: thousands, as reqwest opens one per in-flight
/// request.
const MANY: usize = 2048;
const OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
/// The body of the peer's close-delimited and chunked responses, written in `PIECES`.
const BODY_LENGTH: usize = 90_000;
const PIECES: [usize; 5] = [1, 7, 1000, 16_384, 72_608];

fn wait<Work: Future>(work: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, work)
                .await
                .expect("bounded scenario")
        })
}

/// Runs `factory` on a one-worker runtime, then quiesces it: every pool and session the
/// scenario left must close without a forced worker.
fn run_on_worker<Output, Factory, Work>(factory: Factory) -> Output
where
    Output: Send + 'static,
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let result =
        wait(worker.call_local(factory).expect("scenario admitted")).expect("scenario ends");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    result
}

/// Every session timer disabled, as owned HTTP/1.1 sessions require.
fn untimed() -> SessionParams {
    SessionParams {
        stream_read_timeout: Some(Duration::ZERO),
        connection_read_timeout: Some(Duration::ZERO),
        write_timeout: Some(Duration::ZERO),
        ..SessionParams::default()
    }
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

/// API Proxy's HTTP/1.1 egress pool: no connection cap (reqwest has none), a 90 s idle timeout
/// swept on an interval, most recently used first, and the default waiter bound.
fn egress_params() -> PoolParams {
    PoolParams {
        max_connections: u32::MAX,
        connect_timeout: Duration::from_secs(10),
        max_age: Duration::ZERO,
        idle_timeout: Duration::from_secs(90),
        reuse_order: SessionReuseOrder::MostRecentlyUsed,
        idle_sweep: IdleSweep::Interval,
        ..PoolParams::default()
    }
}

fn egress_pool(
    context: &WorkerContext,
    address: SocketAddr,
    params: PoolParams,
) -> OwnedHttpSessionPool {
    OwnedHttpSessionPool::new(
        context.clone(),
        socket_address(address),
        params,
        ConnectionParams {
            plaintext_protocol: CodecProtocol::Http11,
            ..ConnectionParams::default()
        },
        untimed(),
    )
    .expect("the pool accepts its parameters")
}

fn patterned(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

/// How the peer closes a connection it no longer serves.
#[derive(Clone, Copy, Debug)]
enum PeerClose {
    /// A FIN, after which it reads until the client closes, counting what arrives.
    Fin,
    /// A full close, as uvicorn's `transport.close()` and Envoy's idle timeout do.
    Close,
}

/// The peer's behavior and its counters, shared with its connections.
struct PeerShared {
    keep_alive: Option<Duration>,
    idle_close: PeerClose,
    accepted: AtomicUsize,
    /// Connections whose service ended.
    finished: AtomicUsize,
    requests: AtomicUsize,
    /// Connections the peer closed after `keep_alive` without a request, or at the test's
    /// request.
    idle_closed: AtomicUsize,
    /// Bytes that arrived on connections after the peer's FIN.
    after_close: AtomicUsize,
    /// `/gate` connections the client closed in the middle of their response.
    cut_mid_response: AtomicUsize,
    /// `/hold` requests answer in rounds of this many, once the round's last one arrives.
    hold_round: AtomicUsize,
    held: AtomicUsize,
    hold_release: tokio::sync::watch::Sender<usize>,
    gate_release: tokio::sync::watch::Sender<bool>,
    /// Idle connections still to close at the test's request, as a keep-alive timeout would.
    close_budget: AtomicUsize,
    close_requests: tokio::sync::watch::Sender<u64>,
}

impl PeerShared {
    /// Closes `count` of the connections waiting for a next request, as their keep-alive
    /// timeout would.
    fn close_idle(&self, count: usize) {
        self.close_budget.store(count, Ordering::SeqCst);
        self.close_requests.send_modify(|requests| *requests += 1);
    }

    /// Takes one of the idle closes the test requested.
    fn take_close(&self) -> bool {
        self.close_budget
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |budget| {
                budget.checked_sub(1)
            })
            .is_ok()
    }
}

/// A scripted HTTP/1.1 peer on its own thread, serving loopback connections until dropped.
/// Each request's path selects its response:
///
/// - `/ok`: a keep-alive `Content-Length` response;
/// - `/close`: a `Content-Length` response with `Connection: close`, then a close;
/// - `/http10`: an HTTP/1.0 `Content-Length` response, then a close;
/// - `/http10-keep-alive`: an HTTP/1.0 `Content-Length` response with
///   `Connection: keep-alive`;
/// - `/http10-eof` and `/eof`: an HTTP/1.0 or HTTP/1.1 response whose body the close
///   delimits; `/eof-close` adds `Connection: close`;
/// - `/chunked`, `/chunked-trailers` and `/chunked-close`: a chunked keep-alive response,
///   one with trailers, and one with `Connection: close`;
/// - `/gate`: a chunked response's head and first chunk, then the rest once the test opens
///   the gate, unless the client closes the connection first;
/// - `/hold`: `OK` once the `/hold` round's last request has arrived.
///
/// Close-delimited and chunked bodies are `patterned(BODY_LENGTH)`, written in `PIECES` a
/// millisecond apart.
struct Peer {
    address: SocketAddr,
    shared: Arc<PeerShared>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Peer {
    fn start(keep_alive: Option<Duration>, idle_close: PeerClose) -> Self {
        let shared = Arc::new(PeerShared {
            keep_alive,
            idle_close,
            accepted: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            idle_closed: AtomicUsize::new(0),
            after_close: AtomicUsize::new(0),
            cut_mid_response: AtomicUsize::new(0),
            hold_round: AtomicUsize::new(1),
            held: AtomicUsize::new(0),
            hold_release: tokio::sync::watch::channel(0).0,
            gate_release: tokio::sync::watch::channel(false).0,
            close_budget: AtomicUsize::new(0),
            close_requests: tokio::sync::watch::channel(0).0,
        });
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (bound, address) = mpsc::channel();
        let peer = Arc::clone(&shared);
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("peer runtime")
                .block_on(async move {
                    let socket = tokio::net::TcpSocket::new_v4().expect("peer socket");
                    socket
                        .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .expect("peer binds");
                    // Thousands of dials arrive at once.
                    let listener = socket.listen(4096).expect("peer listens");
                    bound
                        .send(listener.local_addr().expect("peer address"))
                        .expect("the test waits for the address");
                    let mut connections = tokio::task::JoinSet::new();
                    tokio::pin!(stopped);
                    loop {
                        tokio::select! {
                            _ = &mut stopped => break,
                            accepted = listener.accept() => {
                                let (stream, _) = accepted.expect("peer accepts");
                                stream.set_nodelay(true).expect("no delay");
                                peer.accepted.fetch_add(1, Ordering::SeqCst);
                                let shared = Arc::clone(&peer);
                                connections.spawn(async move {
                                    serve_connection(stream, Arc::clone(&shared)).await;
                                    shared.finished.fetch_add(1, Ordering::SeqCst);
                                });
                            }
                        }
                    }
                    connections.shutdown().await;
                });
        });
        Self {
            address: address.recv().expect("the peer binds"),
            shared,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn counters(&self) -> Arc<PeerShared> {
        Arc::clone(&self.shared)
    }

    /// Waits until every connection the peer accepted has ended, so that its counters are
    /// final, then stops it.
    fn finish(self) -> Arc<PeerShared> {
        let shared = Arc::clone(&self.shared);
        for _ in 0..5000 {
            if shared.finished.load(Ordering::SeqCst) == shared.accepted.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            shared.finished.load(Ordering::SeqCst),
            shared.accepted.load(Ordering::SeqCst),
            "the client closed every connection"
        );
        drop(self);
        shared
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

enum Next {
    Request(String),
    Ended,
    Idle,
}

/// Reads the next request head, buffering what follows it. The keep-alive timer, and a close
/// the test requests, apply only while no byte of a next request has arrived, as uvicorn's
/// timer does.
async fn next_request(
    stream: &mut tokio::net::TcpStream,
    input: &mut Vec<u8>,
    shared: &PeerShared,
) -> Next {
    let mut close_requests = shared.close_requests.subscribe();
    loop {
        if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
            let head: Vec<u8> = input.drain(..end + 4).collect();
            let line = String::from_utf8_lossy(&head);
            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
            return Next::Request(path);
        }
        let mut buffer = [0; 4096];
        let idle = input.is_empty();
        let read = tokio::select! {
            read = stream.read(&mut buffer) => read,
            () = async {
                match shared.keep_alive {
                    Some(timeout) => tokio::time::sleep(timeout).await,
                    None => std::future::pending().await,
                }
            }, if idle => return Next::Idle,
            changed = close_requests.changed(), if idle => {
                if changed.is_ok() && shared.take_close() {
                    return Next::Idle;
                }
                continue;
            }
        };
        match read {
            Ok(0) | Err(_) => return Next::Ended,
            Ok(read) => input.extend_from_slice(&buffer[..read]),
        }
    }
}

/// Counts what arrives after the peer's FIN until the client closes.
async fn read_after_close(stream: &mut tokio::net::TcpStream, shared: &PeerShared) {
    let mut buffer = [0; 4096];
    while let Ok(read) = stream.read(&mut buffer).await
        && read != 0
    {
        shared.after_close.fetch_add(read, Ordering::SeqCst);
    }
}

async fn serve_connection(mut stream: tokio::net::TcpStream, shared: Arc<PeerShared>) {
    let mut input = Vec::new();
    loop {
        let path = match next_request(&mut stream, &mut input, &shared).await {
            Next::Request(path) => path,
            Next::Ended => return,
            Next::Idle => {
                match shared.idle_close {
                    PeerClose::Fin => {
                        let _ = stream.shutdown().await;
                        shared.idle_closed.fetch_add(1, Ordering::SeqCst);
                        read_after_close(&mut stream, &shared).await;
                    }
                    PeerClose::Close => {
                        drop(stream);
                        shared.idle_closed.fetch_add(1, Ordering::SeqCst);
                    }
                }
                return;
            }
        };
        shared.requests.fetch_add(1, Ordering::SeqCst);
        match respond(&mut stream, &path, &shared).await {
            Ok(true) => {}
            Ok(false) => {
                let _ = stream.shutdown().await;
                read_after_close(&mut stream, &shared).await;
                return;
            }
            Err(()) => return,
        }
    }
}

/// Writes `pieces` of `patterned(BODY_LENGTH)` a millisecond apart, each framed by `frame`.
async fn write_pieces(
    stream: &mut tokio::net::TcpStream,
    frame: impl Fn(&[u8]) -> Vec<u8>,
) -> Result<(), ()> {
    let body = patterned(BODY_LENGTH);
    let mut offset = 0;
    for length in PIECES {
        let piece = &body[offset..offset + length];
        offset += length;
        stream.write_all(&frame(piece)).await.map_err(drop)?;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(offset, BODY_LENGTH);
    Ok(())
}

fn chunk(piece: &[u8]) -> Vec<u8> {
    let mut framed = format!("{:x}\r\n", piece.len()).into_bytes();
    framed.extend_from_slice(piece);
    framed.extend_from_slice(b"\r\n");
    framed
}

/// Answers `path`; `Ok(false)` when the response closes the connection, `Err` when the
/// connection is gone.
async fn respond(
    stream: &mut tokio::net::TcpStream,
    path: &str,
    shared: &PeerShared,
) -> Result<bool, ()> {
    async fn write(stream: &mut tokio::net::TcpStream, bytes: &[u8]) -> Result<(), ()> {
        stream.write_all(bytes).await.map_err(drop)
    }
    match path {
        "/ok" => write(stream, OK).await.map(|()| true),
        "/close" => write(
            stream,
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
        )
        .await
        .map(|()| false),
        "/http10" => write(stream, b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .map(|()| false),
        "/http10-keep-alive" => write(
            stream,
            b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok",
        )
        .await
        .map(|()| true),
        "/http10-eof" | "/eof" | "/eof-close" => {
            let head: &[u8] = match path {
                "/http10-eof" => b"HTTP/1.0 200 OK\r\n\r\n",
                "/eof" => b"HTTP/1.1 200 OK\r\n\r\n",
                _ => b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
            };
            write(stream, head).await?;
            write_pieces(stream, <[u8]>::to_vec).await?;
            Ok(false)
        }
        "/chunked" | "/chunked-trailers" | "/chunked-close" => {
            let head: &[u8] = if path == "/chunked-close" {
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
            } else {
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
            };
            write(stream, head).await?;
            write_pieces(stream, chunk).await?;
            let last: &[u8] = if path == "/chunked-trailers" {
                b"0\r\nx-trailer: done\r\n\r\n"
            } else {
                b"0\r\n\r\n"
            };
            write(stream, last).await?;
            Ok(path != "/chunked-close")
        }
        "/gate" => {
            write(
                stream,
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n",
            )
            .await?;
            let mut release = shared.gate_release.subscribe();
            let mut buffer = [0; 64];
            let released = tokio::select! {
                released = release.wait_for(|open| *open) => released.is_ok(),
                // A client that cancels the exchange closes the connection.
                _ = stream.read(&mut buffer) => false,
            };
            if !released {
                shared.cut_mid_response.fetch_add(1, Ordering::SeqCst);
                return Err(());
            }
            write(stream, b"4\r\nrest\r\n0\r\n\r\n")
                .await
                .map(|()| true)
        }
        "/hold" => {
            let round_size = shared.hold_round.load(Ordering::SeqCst);
            let index = shared.held.fetch_add(1, Ordering::SeqCst);
            let round = index / round_size + 1;
            let mut release = shared.hold_release.subscribe();
            if (index + 1).is_multiple_of(round_size) {
                shared
                    .hold_release
                    .send_modify(|released| *released = round);
            }
            release
                .wait_for(|released| *released >= round)
                .await
                .expect("the peer keeps its hold");
            write(stream, OK).await.map(|()| true)
        }
        _ => write(
            stream,
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
        )
        .await
        .map(|()| true),
    }
}

/// A GET for `path` with a `Host` header, and its empty body.
async fn get(path: &str) -> (HttpHeaderEvent, HttpSourceHolder) {
    let mut source = HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_get_request(path.as_bytes(), None).expect("valid request"),
    ));
    let mut head = source.read_header_event().await.expect("request head");
    head.headers_mut().headers_mut().add("host", "peer.example");
    (head, HttpSourceHolder::empty())
}

/// A response as its reader saw it.
#[derive(Clone, Debug, Default, PartialEq)]
struct Received {
    status: Option<u16>,
    body: Vec<u8>,
    trailers: Vec<(Vec<u8>, Vec<u8>)>,
    error: Option<HttpErrorCode>,
}

impl Received {
    fn body_event(&mut self, event: &HttpBodyEvent) {
        if let Some(data) = event.as_body() {
            data.segments()
                .for_each(|segment| self.body.extend_from_slice(segment));
        }
        if let HttpBodyEventKind::Trailers(trailers) = event.kind() {
            trailers.for_each(|name, value| {
                self.trailers.push((name.to_vec(), value.to_vec()));
            });
        }
    }
}

async fn read_source(source: &mut HttpSourceHolder) -> Received {
    let mut received = Received::default();
    let head = match source.read_header_event().await {
        Ok(head) => head,
        Err(error) => {
            received.error = Some(error.code());
            return received;
        }
    };
    received.status = head.headers().response_status();
    let mut eom = head.eom();
    while !eom {
        match source.read_body_event(u32::MAX).await {
            Ok(event) => {
                received.body_event(&event);
                eom = event.eom();
            }
            Err(error) => {
                received.error = Some(error.code());
                break;
            }
        }
    }
    received
}

async fn read_pull_through(body: &mut OwnedClientPullThroughBody) -> Received {
    let mut received = Received::default();
    let head = match poll_fn(|context| body.poll_header(context)).await {
        Ok(head) => head,
        Err(error) => {
            received.error = Some(error.code());
            return received;
        }
    };
    received.status = head.headers().response_status();
    let mut eom = head.eom();
    while !eom {
        match poll_fn(|context| body.poll_body(context, u32::MAX)).await {
            Ok(event) => {
                received.body_event(&event);
                eom = event.eom();
            }
            Err(error) => {
                received.error = Some(error.code());
                break;
            }
        }
    }
    received
}

/// How an exchange reads its response.
#[derive(Clone, Copy, Debug)]
enum Reading {
    /// From the response's source, which the session's run drives.
    Source,
    /// Through a pull-through body that drives the connection (`DIV-047`).
    PullThrough,
}

const READINGS: [Reading; 2] = [Reading::Source, Reading::PullThrough];

/// Sends `path` on `reservation` and reads its response to its end, with the request's
/// sequence number on its connection.
async fn exchange_on(
    reservation: OwnedClientReservation,
    path: &str,
    reading: Reading,
) -> (Received, u64) {
    let (head, body) = get(path).await;
    match reading {
        Reading::Source => {
            let response = reservation
                .submit(head, body, CancellationToken::never())
                .await
                .expect("request is committed");
            let mut source = response.source.into_source_owner().into_source_holder();
            (read_source(&mut source).await, response.sequence)
        }
        Reading::PullThrough => {
            let mut response = reservation
                .submit_pull_through(head, body, CancellationToken::never())
                .await
                .expect("request is committed");
            (
                read_pull_through(&mut response.body).await,
                response.sequence,
            )
        }
    }
}

/// One exchange on the pool's next session: its response, its sequence number and the
/// session.
async fn exchange(
    pool: &OwnedHttpSessionPool,
    path: &str,
    reading: Reading,
) -> (Received, u64, OwnedClientSession) {
    let OwnedGetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    let (received, sequence) = exchange_on(reservation, path, reading).await;
    (received, sequence, session)
}

fn ok() -> Received {
    Received {
        status: Some(200),
        body: b"ok".to_vec(),
        ..Received::default()
    }
}

/// Polls `ready` every millisecond until it holds.
async fn until(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A pooled connection that the peer closes after its keep-alive timeout, with a FIN or a
/// full close, is retired by the pool as soon as its driver reads the close, before any
/// acquisition: the pool holds no session once the peer has closed them all, and the next
/// exchanges dial fresh connections and are served, with no request refused and no byte
/// written to a closed connection. (Per close: the peer's idle closes, the pool's sessions
/// once they were read, sessions opened and closed, the next exchanges' responses and
/// sequence numbers, the peer's connections, and bytes after its FINs.)
#[test]
fn a_pooled_connection_the_peer_closes_while_idle_is_retired_before_reuse() {
    const CONNECTIONS: usize = 4;
    for close in [PeerClose::Fin, PeerClose::Close] {
        let peer = Peer::start(Some(KEEP_ALIVE), close);
        let address = peer.address;
        let counters = peer.counters();
        let observed = run_on_worker(move |context| async move {
            let pool = egress_pool(&context, address, egress_params());
            let first = futures::future::join_all(
                (0..CONNECTIONS).map(|_| exchange(&pool, "/ok", Reading::Source)),
            )
            .await;
            assert!(first.iter().all(|(received, _, _)| *received == ok()));
            until(|| counters.idle_closed.load(Ordering::SeqCst) == CONNECTIONS).await;
            until(|| pool.session_count() == 0).await;
            let stats = pool.stats();
            let next = futures::future::join_all(
                (0..CONNECTIONS).map(|_| exchange(&pool, "/ok", Reading::Source)),
            )
            .await;
            let fresh = next.iter().all(|(_, _, session)| {
                first
                    .iter()
                    .all(|(_, _, closed)| !closed.is_same_session(session))
            });
            (
                (stats.sessions_opened, stats.sessions_closed),
                next.into_iter()
                    .map(|(received, sequence, _)| (received, sequence))
                    .collect::<Vec<_>>(),
                fresh,
            )
        });
        assert_eq!(
            observed,
            (
                (CONNECTIONS as u64, CONNECTIONS as u64),
                (0..CONNECTIONS).map(|_| (ok(), 0)).collect::<Vec<_>>(),
                true,
            ),
            "{close:?}"
        );
        let counters = peer.finish();
        assert_eq!(
            (
                counters.accepted.load(Ordering::SeqCst),
                counters.after_close.load(Ordering::SeqCst),
            ),
            (2 * CONNECTIONS, 0),
            "{close:?}: (connections, bytes after the peer's FINs)"
        );
    }
}

/// What a request submitted in the worker turn that sees the peer's close become readable met.
#[derive(Debug, PartialEq)]
struct CloseCommit {
    /// The submission failed with this code, before commit.
    refused: Option<HttpErrorCode>,
    unprocessed: bool,
    sequence_number: Option<u64>,
    /// Bytes the peer read on the closed connection after its FIN (FIN only).
    received_after_fin: usize,
    /// The resend on the next acquisition: its connection was a new one, its sequence number,
    /// and its response.
    resent: Option<(bool, u64, Received)>,
}

/// Reads request heads from `stream`, answering each with `OK`, until `answer` are answered
/// or the stream ends.
fn serve_blocking(stream: &mut TcpStream, mut answer: usize) {
    let mut input = Vec::new();
    let mut buffer = [0; 1024];
    while answer != 0 {
        if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
            input.drain(..end + 4);
            stream.write_all(OK).expect("peer replies");
            answer -= 1;
            continue;
        }
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(read) => input.extend_from_slice(&buffer[..read]),
        }
    }
}

/// `prior_exchanges` exchanges on one pooled connection, then the peer's keep-alive close and
/// the next request's submission in one worker turn, as `read_before_write.rs` schedules them
/// for `HttpSessionPool`: the worker thread blocks while the peer writes a byte on a side
/// connection and then closes, so the next reactor turn sees both, the side byte first, and
/// the client task takes the idle pooled session and submits before its driver's next poll.
/// A request refused unprocessed is resent on the next acquisition.
fn close_commit(
    context: WorkerContext,
    prior_exchanges: usize,
    close: PeerClose,
) -> Pin<Box<dyn Future<Output = CloseCommit>>> {
    Box::pin(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("peer binds");
        let address = listener.local_addr().expect("peer address");
        let side_listener = TcpListener::bind("127.0.0.1:0").expect("side binds");
        let side_client = TcpStream::connect(side_listener.local_addr().expect("side address"))
            .expect("side connects");
        side_client.set_nonblocking(true).expect("nonblocking side");
        let (mut side_peer, _) = side_listener.accept().expect("side accepted");
        let (close_now, closes) = mpsc::channel::<()>();
        let (closed, closed_rx) = mpsc::channel::<()>();
        let peer = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().expect("peer accepts");
            serve_blocking(&mut first, prior_exchanges);
            closes.recv().expect("the client asks for the close");
            side_peer.write_all(b"x").expect("side byte");
            let mut received_after_fin = 0;
            match close {
                PeerClose::Fin => {
                    first.shutdown(Shutdown::Write).expect("peer sends FIN");
                    closed.send(()).expect("the client waits");
                    let mut buffer = [0; 1024];
                    while let Ok(read) = first.read(&mut buffer)
                        && read != 0
                    {
                        received_after_fin += read;
                    }
                }
                PeerClose::Close => {
                    drop(first);
                    closed.send(()).expect("the client waits");
                }
            }
            listener
                .set_nonblocking(true)
                .expect("nonblocking listener");
            let resent = (0..500).find_map(|_| match listener.accept() {
                Ok((mut fresh, _)) => {
                    fresh.set_nonblocking(false).expect("blocking stream");
                    serve_blocking(&mut fresh, 1);
                    let mut rest = Vec::new();
                    let _ = fresh.read_to_end(&mut rest);
                    Some(())
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                }
            });
            (received_after_fin, resent.is_some())
        });
        let pool = egress_pool(&context, address, egress_params());
        let mut session = None;
        for _ in 0..prior_exchanges {
            let (received, _, used) = exchange(&pool, "/", Reading::Source).await;
            assert_eq!(received, ok());
            until(|| used.is_idle()).await;
            session = Some(used);
        }
        let session = session.expect("an earlier exchange");
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let side = tokio::net::TcpStream::from_std(side_client).expect("side registered");
        close_now.send(()).expect("the peer listens");
        closed_rx.recv().expect("the peer closed");
        side.readable().await.expect("side byte readable");
        let OwnedGetSessionResult {
            reservation,
            session: taken,
        } = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .expect("acquisition succeeds");
        assert!(
            taken.is_same_session(&session),
            "the idle pooled session is taken"
        );
        let (head, body) = get("/").await;
        let mut submission = Box::pin(reservation.submit(head, body, CancellationToken::never()));
        let outcome = submission.as_mut().await;
        let unprocessed = submission.request_unprocessed();
        let sequence_number = submission.sequence_number();
        let refused = match outcome {
            Ok(response) => {
                // Committed: read what the response met.
                let mut source = response.source.into_source_owner().into_source_holder();
                let _ = read_source(&mut source).await;
                None
            }
            Err(error) => Some(error.code()),
        };
        let resent = if unprocessed {
            let (received, sequence, fresh) = exchange(&pool, "/", Reading::Source).await;
            Some((!fresh.is_same_session(&session), sequence, received))
        } else {
            None
        };
        let _ = pool.drain_and_join().await;
        drop(side);
        let (received_after_fin, peer_resent) = peer.join().expect("peer finishes");
        assert_eq!(
            peer_resent,
            resent.is_some(),
            "the peer serves the resend on a new connection"
        );
        CloseCommit {
            refused,
            unprocessed,
            sequence_number,
            received_after_fin,
            resent,
        }
    })
}

fn bounded<T>(call: WorkerCall<T>) -> T {
    wait(call).expect("client task completes")
}

/// Runs `close_commit` on a `ManagedWorker` and on a `HostedWorker`.
fn close_commit_on_workers(prior_exchanges: usize, close: PeerClose) -> [CloseCommit; 2] {
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let managed = bounded(
        worker
            .handle()
            .call_local(move |context| close_commit(context, prior_exchanges, close))
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "owned-close-commit".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted client worker starts");
    let hosted = bounded(
        worker
            .handle()
            .call_local(move |context| close_commit(context, prior_exchanges, close))
            .expect("client task is admitted"),
    );
    drop(worker);
    [managed, hosted]
}

/// A request submitted on a pooled keep-alive connection in the worker turn in which the
/// peer's keep-alive close becomes readable is refused before commit (`DIV-035`): the driver
/// reads before it commits, so it fails with `TRANSPORT_EOF`, unprocessed, with the
/// connection's earlier requests as its sequence number, and nothing is written to the closed
/// connection; resent on the next acquisition, it is served on a new connection, whatever the
/// worker kind, the earlier exchanges and the close.
#[test]
fn a_request_submitted_as_the_peer_s_keep_alive_close_arrives_is_refused_unprocessed() {
    for close in [PeerClose::Fin, PeerClose::Close] {
        for prior_exchanges in [1, 2] {
            for outcome in close_commit_on_workers(prior_exchanges, close) {
                assert_eq!(
                    outcome,
                    CloseCommit {
                        refused: Some(HttpErrorCode::TRANSPORT_EOF),
                        unprocessed: true,
                        sequence_number: Some(prior_exchanges as u64),
                        received_after_fin: 0,
                        resent: Some((true, 0, ok())),
                    },
                    "{close:?} after {prior_exchanges} exchanges"
                );
            }
        }
    }
}

/// Each closing or close-delimited response is read whole through the pool, from its source
/// and through a pull-through body, and retires its connection: the session finishes cleanly,
/// the next exchange dials a new connection, and no request follows on the closed one. A
/// chunked or HTTP/1.0 keep-alive response leaves its connection to the next exchange.
/// (Per path and reading: the response, whether the next exchange reused the connection, the
/// first session's outcome once retired, the peer's connections, and bytes after its FINs.)
#[test]
fn closing_and_chunked_responses_are_read_whole_and_retire_or_keep_their_connection() {
    let body = patterned(BODY_LENGTH);
    let trailered = Received {
        status: Some(200),
        body: body.clone(),
        trailers: vec![(b"x-trailer".to_vec(), b"done".to_vec())],
        error: None,
    };
    let whole = Received {
        status: Some(200),
        body: body.clone(),
        ..Received::default()
    };
    // (path, the response, whether the connection is reused)
    let cases: Vec<(&'static str, Received, bool)> = vec![
        ("/ok", ok(), true),
        ("/close", ok(), false),
        ("/http10", ok(), false),
        ("/http10-keep-alive", ok(), true),
        ("/http10-eof", whole.clone(), false),
        ("/eof", whole.clone(), false),
        ("/eof-close", whole.clone(), false),
        ("/chunked", whole.clone(), true),
        ("/chunked-trailers", trailered, true),
        ("/chunked-close", whole, false),
    ];
    for reading in READINGS {
        for (path, expected, reused) in cases.clone() {
            let peer = Peer::start(None, PeerClose::Fin);
            let address = peer.address;
            let observed = run_on_worker(move |context| async move {
                let pool = egress_pool(&context, address, egress_params());
                let (received, _, first) = exchange(&pool, path, reading).await;
                let closed = if reused {
                    None
                } else {
                    Some(
                        first
                            .wait_closed()
                            .await
                            .map(|summary| summary.reset)
                            .map_err(|error| error.code()),
                    )
                };
                until(|| first.is_idle() || first.is_finished()).await;
                let (next, sequence, second) = exchange(&pool, "/ok", reading).await;
                assert_eq!(next, ok(), "{path} {reading:?}: the next exchange");
                let next_reused = first.is_same_session(&second);
                assert_eq!(sequence, u64::from(next_reused), "{path} {reading:?}");
                (received, next_reused, closed)
            });
            assert_eq!(
                observed,
                (expected, reused, (!reused).then_some(Ok(false))),
                "{path} {reading:?}"
            );
            let counters = peer.finish();
            assert_eq!(
                (
                    counters.accepted.load(Ordering::SeqCst),
                    counters.after_close.load(Ordering::SeqCst),
                ),
                (if reused { 1 } else { 2 }, 0),
                "{path} {reading:?}: (connections, bytes after a closing response)"
            );
        }
    }
}

/// How a test cancels an exchange in the middle of its response.
#[derive(Clone, Copy, Debug)]
enum Cancel {
    /// `OwnedClientRequest::cancel`, as a route timeout does.
    Request,
    /// The cancellation token the request was submitted with.
    Token,
    /// The response's reader goes away: its source, or its pull-through body, is dropped.
    DropReader,
    /// `OwnedClientPullThroughBody::stop` with an error.
    StopBody,
}

/// A response the test holds in the middle of its body.
enum Held {
    Source(HttpSourceHolder, OwnedClientRequest),
    PullThrough(OwnedClientPullThroughBody, OwnedClientRequest),
}

/// Submits `/gate` on the pool's next session and reads its head and first chunk.
async fn hold_gate(
    pool: &OwnedHttpSessionPool,
    reading: Reading,
    token: CancellationToken,
) -> (Held, OwnedClientSession) {
    let OwnedGetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    let (head, body) = get("/gate").await;
    let held = match reading {
        Reading::Source => {
            let response = reservation
                .submit(head, body, token)
                .await
                .expect("request is committed");
            let mut source = response.source.into_source_owner().into_source_holder();
            let head = source.read_header_event().await.expect("response head");
            assert_eq!(head.headers().response_status(), Some(200));
            let mut first = Received::default();
            while first.body.len() < b"first".len() {
                first.body_event(&source.read_body_event(u32::MAX).await.expect("first chunk"));
            }
            assert_eq!(first.body, b"first");
            Held::Source(source, response.request)
        }
        Reading::PullThrough => {
            let mut response = reservation
                .submit_pull_through(head, body, token)
                .await
                .expect("request is committed");
            let head = poll_fn(|context| response.body.poll_header(context))
                .await
                .expect("response head");
            assert_eq!(head.headers().response_status(), Some(200));
            let mut first = Received::default();
            while first.body.len() < b"first".len() {
                first.body_event(
                    &poll_fn(|context| response.body.poll_body(context, u32::MAX))
                        .await
                        .expect("first chunk"),
                );
            }
            assert_eq!(first.body, b"first");
            Held::PullThrough(response.body, response.request)
        }
    };
    (held, session)
}

/// Reads a held response's rest.
async fn read_rest(held: Held) -> Received {
    let mut received = Received {
        status: Some(200),
        body: b"first".to_vec(),
        ..Received::default()
    };
    match held {
        Held::Source(mut source, _) => loop {
            match source.read_body_event(u32::MAX).await {
                Ok(event) => {
                    received.body_event(&event);
                    if event.eom() {
                        return received;
                    }
                }
                Err(error) => {
                    received.error = Some(error.code());
                    return received;
                }
            }
        },
        Held::PullThrough(mut body, _) => loop {
            match poll_fn(|context| body.poll_body(context, u32::MAX)).await {
                Ok(event) => {
                    received.body_event(&event);
                    if event.eom() {
                        return received;
                    }
                }
                Err(error) => {
                    received.error = Some(error.code());
                    return received;
                }
            }
        },
    }
}

/// A cancel in the middle of one pooled exchange's response closes that exchange's connection
/// alone: of four pooled connections, two others carry exchanges held in the middle of their
/// responses, which complete once the peer continues, and the next three exchanges are served
/// on those two and on the fourth, which was idle, with no dial. The peer sees the
/// client close the cancelled connection in the middle of its response, the pool counts one
/// session closed, and a cancel that selects an error fails the cancelled response with it.
/// (Per cancel and reading: the cancelled response's error, the other responses, the pool's
/// sessions, sessions opened and closed, whether the next exchanges were served on the
/// surviving connections, the peer's connections and its cut responses.)
#[test]
fn a_cancel_mid_response_closes_only_its_connection() {
    let cases = [
        (Cancel::Request, Reading::Source),
        (Cancel::Request, Reading::PullThrough),
        (Cancel::Token, Reading::Source),
        (Cancel::Token, Reading::PullThrough),
        (Cancel::DropReader, Reading::Source),
        (Cancel::DropReader, Reading::PullThrough),
        (Cancel::StopBody, Reading::PullThrough),
    ];
    for (cancel, reading) in cases {
        let peer = Peer::start(None, PeerClose::Fin);
        let address = peer.address;
        let counters = peer.counters();
        let observed = run_on_worker(move |context| async move {
            let pool = egress_pool(&context, address, egress_params());
            let warm =
                futures::future::join_all((0..4).map(|_| exchange(&pool, "/ok", reading))).await;
            for (_, _, session) in &warm {
                until(|| session.is_idle()).await;
            }
            let source = CancellationSource::new();
            let (cancelled, cancelled_session) = hold_gate(&pool, reading, source.token()).await;
            let (one, one_session) = hold_gate(&pool, reading, CancellationToken::never()).await;
            let (two, two_session) = hold_gate(&pool, reading, CancellationToken::never()).await;
            let cancelled_error = match (cancel, cancelled) {
                (Cancel::Request, held) => {
                    let (Held::Source(_, request) | Held::PullThrough(_, request)) = &held;
                    request.cancel(HttpError::new(HttpErrorCode::READ_TIMEOUT));
                    read_rest(held).await.error
                }
                (Cancel::Token, held) => {
                    source.request_cancellation();
                    read_rest(held).await.error
                }
                (Cancel::DropReader, held) => {
                    drop(held);
                    None
                }
                (Cancel::StopBody, Held::PullThrough(mut body, _)) => {
                    body.stop(Some(HttpErrorCode::READ_TIMEOUT));
                    drop(body);
                    None
                }
                (Cancel::StopBody, Held::Source(..)) => unreachable!("a pull-through case"),
            };
            let _ = cancelled_session.wait_closed().await;
            until(|| counters.cut_mid_response.load(Ordering::SeqCst) == 1).await;
            let sessions_after_cancel = pool.session_count();
            counters.gate_release.send_replace(true);
            let others = (read_rest(one).await, read_rest(two).await);
            until(|| one_session.is_idle() && two_session.is_idle()).await;
            let next =
                futures::future::join_all((0..3).map(|_| exchange(&pool, "/ok", reading))).await;
            let on_survivors = next.iter().all(|(received, _, session)| {
                *received == ok()
                    && !session.is_same_session(&cancelled_session)
                    && warm
                        .iter()
                        .any(|(_, _, warmed)| warmed.is_same_session(session))
            });
            let stats = pool.stats();
            (
                cancelled_error,
                others,
                sessions_after_cancel,
                (stats.sessions_opened, stats.sessions_closed),
                on_survivors,
            )
        });
        let gated = Received {
            status: Some(200),
            body: b"firstrest".to_vec(),
            ..Received::default()
        };
        let expected_error = match cancel {
            Cancel::Request => Some(HttpErrorCode::READ_TIMEOUT),
            Cancel::Token => Some(HttpErrorCode::CORO_CANCELLED),
            Cancel::DropReader | Cancel::StopBody => None,
        };
        assert_eq!(
            observed,
            (expected_error, (gated.clone(), gated), 3, (4, 1), true),
            "{cancel:?} {reading:?}"
        );
        let counters = peer.finish();
        assert_eq!(
            (
                counters.accepted.load(Ordering::SeqCst),
                counters.cut_mid_response.load(Ordering::SeqCst),
            ),
            (4, 1),
            "{cancel:?} {reading:?}: (connections, responses the client cut)"
        );
    }
}

/// `MANY` concurrent exchanges on an empty pool with no connection cap dial `MANY` connections
/// on demand, all in flight at once, since the peer answers none until all have arrived; once
/// they are idle, `MANY` concurrent exchanges again are served on those connections, with no
/// dial, no waiter left and no acquisition timeout.
#[test]
fn thousands_of_concurrent_exchanges_dial_on_demand_and_reuse_their_connections() {
    let peer = Peer::start(None, PeerClose::Fin);
    peer.shared.hold_round.store(MANY, Ordering::SeqCst);
    let address = peer.address;
    let observed = run_on_worker(move |context| async move {
        let pool = egress_pool(&context, address, egress_params());
        let mut rounds = Vec::new();
        for _ in 0..2 {
            let exchanges = futures::future::join_all(
                (0..MANY).map(|_| exchange(&pool, "/hold", Reading::Source)),
            )
            .await;
            let served = exchanges
                .iter()
                .filter(|(received, _, _)| *received == ok())
                .count();
            for (_, _, session) in &exchanges {
                until(|| session.is_idle()).await;
            }
            let stats = pool.stats();
            rounds.push((
                served,
                pool.session_count(),
                stats.sessions_opened,
                stats.connect_failures,
                stats.waiters,
                stats.acquisition_timeouts,
            ));
        }
        rounds
    });
    let round = (MANY, MANY, MANY as u64, 0, 0, 0);
    assert_eq!(observed, [round, round]);
    assert_eq!(peer.finish().accepted.load(Ordering::SeqCst), MANY);
}

/// `CONSUMERS` streams, each running `ROUNDS` exchanges back to back, share a pool capped at
/// `CAP` connections, so most acquisitions wait. No waiter starves: every exchange is served,
/// none times out, and no acquisition is overtaken by more acquisitions than there are
/// streams, as a first-come order allows.
#[test]
fn waiters_on_a_capped_pool_are_served_in_turn_while_streams_reacquire() {
    const CAP: u32 = 64;
    const CONSUMERS: usize = 1024;
    const ROUNDS: usize = 4;
    let peer = Peer::start(None, PeerClose::Fin);
    let address = peer.address;
    let observed = run_on_worker(move |context| async move {
        let pool = egress_pool(
            &context,
            address,
            PoolParams {
                max_connections: CAP,
                acquisition_timeout: Some(Duration::from_secs(20)),
                ..egress_params()
            },
        );
        let acquired = Rc::new(Cell::new(0_usize));
        let worst = Rc::new(Cell::new(0_usize));
        let streams = (0..CONSUMERS).map(|_| {
            let pool = &pool;
            let acquired = Rc::clone(&acquired);
            let worst = Rc::clone(&worst);
            async move {
                let mut served = 0;
                for _ in 0..ROUNDS {
                    let before = acquired.get();
                    let OwnedGetSessionResult { reservation, .. } = pool
                        .get_session_with_reservation(CancellationToken::never())
                        .await
                        .expect("acquisition succeeds");
                    worst.set(worst.get().max(acquired.get() - before));
                    acquired.set(acquired.get() + 1);
                    let (received, _) = exchange_on(reservation, "/ok", Reading::Source).await;
                    served += usize::from(received == ok());
                }
                served
            }
        });
        let served: usize = futures::future::join_all(streams).await.into_iter().sum();
        let stats = pool.stats();
        (
            served,
            stats.acquisition_timeouts,
            stats.waiters,
            stats.sessions_opened <= u64::from(CAP),
            stats.peak_waiters > CONSUMERS / 2,
            worst.get(),
        )
    });
    let (served, timeouts, waiters, capped, waited, worst) = observed;
    assert_eq!(
        (served, timeouts, waiters, capped, waited),
        (CONSUMERS * ROUNDS, 0, 0, true, true)
    );
    assert!(
        worst <= CONSUMERS,
        "an acquisition was overtaken by {worst} others, more than the {CONSUMERS} streams"
    );
    peer.finish();
}

/// Requests that arrive one at a time and each hold their connection, as long streamed
/// responses do, grow a pool to `MANY` connections, each dialed on demand, with no pass over
/// the pool's sessions and no look at a busy session in the search for a returning one (S6.9);
/// once the peer finishes the responses, the connections serve `MANY` concurrent exchanges
/// with no dial and no pass. (Sessions grown, the growth's passes and looks, responses
/// finished, exchanges served, sessions opened, the passes after the growth.)
#[test]
fn a_pool_grows_to_thousands_of_connections_one_held_request_at_a_time() {
    let peer = Peer::start(None, PeerClose::Fin);
    let address = peer.address;
    let counters = peer.counters();
    let observed = run_on_worker(move |context| async move {
        let pool = egress_pool(&context, address, egress_params());
        let mut held = Vec::with_capacity(MANY);
        for _ in 0..MANY {
            held.push(hold_gate(&pool, Reading::Source, CancellationToken::never()).await);
        }
        let grown = pool.session_count();
        let growth = (pool.pool_scans_for_test(), pool.claim_visits_for_test());
        counters.gate_release.send_replace(true);
        let mut finished = 0;
        for (response, session) in held {
            finished += usize::from(read_rest(response).await.body == b"firstrest");
            until(|| session.is_idle()).await;
        }
        let next =
            futures::future::join_all((0..MANY).map(|_| exchange(&pool, "/ok", Reading::Source)))
                .await;
        let served = next
            .iter()
            .filter(|(received, _, _)| *received == ok())
            .count();
        (
            (grown, growth),
            (finished, served, pool.stats().sessions_opened),
            pool.pool_scans_for_test() - growth.0,
        )
    });
    assert_eq!(observed, ((MANY, (0, 0)), (MANY, MANY, MANY as u64), 0));
    assert_eq!(peer.finish().accepted.load(Ordering::SeqCst), MANY);
}

/// When the peer closes half of `MANY` idle pooled connections, as uvicorn's keep-alive timeout
/// does, the pool retires each when its driver reads the close, with no pass over the pool's
/// sessions and its selection index still settled (S6.9), and the connections left serve as
/// many concurrent exchanges with no dial and no pass. (The index settled before the closes,
/// the passes the closes took, the index settled after them, the sessions left, the exchanges
/// served, sessions opened, the passes the exchanges took.)
#[test]
fn a_peer_closing_idle_connections_among_thousands_leaves_the_rest_serving() {
    const CLOSED: usize = MANY / 2;
    let peer = Peer::start(None, PeerClose::Close);
    peer.shared.hold_round.store(MANY, Ordering::SeqCst);
    let address = peer.address;
    let counters = peer.counters();
    let observed = run_on_worker(move |context| async move {
        let pool = egress_pool(&context, address, egress_params());
        establish_many(&pool).await;
        let settled = pool.index_settled_for_test();
        let scans = pool.pool_scans_for_test();
        counters.close_idle(CLOSED);
        until(|| counters.idle_closed.load(Ordering::SeqCst) == CLOSED).await;
        until(|| pool.stats().sessions_closed == CLOSED as u64).await;
        let close_scans = pool.pool_scans_for_test() - scans;
        let settled_after = pool.index_settled_for_test();
        let left = pool.session_count();
        let scans = pool.pool_scans_for_test();
        let next = futures::future::join_all(
            (0..MANY - CLOSED).map(|_| exchange(&pool, "/ok", Reading::Source)),
        )
        .await;
        let served = next
            .iter()
            .filter(|(received, _, _)| *received == ok())
            .count();
        (
            (settled, close_scans, settled_after),
            (left, served, pool.stats().sessions_opened),
            pool.pool_scans_for_test() - scans,
        )
    });
    assert_eq!(
        observed,
        (
            (true, 0, true),
            (MANY - CLOSED, MANY - CLOSED, MANY as u64),
            0
        )
    );
    assert_eq!(peer.finish().accepted.load(Ordering::SeqCst), MANY);
}

/// Establishes `MANY` connections in `pool` with one round of `/hold` exchanges, all in flight
/// at once, and waits until each is idle or retired.
async fn establish_many(pool: &OwnedHttpSessionPool) {
    let exchanges =
        futures::future::join_all((0..MANY).map(|_| exchange(pool, "/hold", Reading::Source)))
            .await;
    for (received, _, session) in &exchanges {
        assert_eq!(*received, ok());
        until(|| session.is_idle() || session.is_finished()).await;
    }
}

/// With `MANY` idle connections in a pool under API Proxy's parameters, back-to-back exchanges
/// take and return one of them with no pass over the pool's sessions (S6.9). With a short idle
/// timeout instead, the interval sweep retires all of them with at most one retiring pass per
/// period, however many sessions became idle at distinct times.
#[test]
fn thousands_of_idle_connections_are_selected_without_a_pass_and_swept_in_a_few_passes() {
    const IDLE: Duration = Duration::from_millis(300);
    let peer = Peer::start(None, PeerClose::Fin);
    peer.shared.hold_round.store(MANY, Ordering::SeqCst);
    let address = peer.address;
    let observed = run_on_worker(move |context| async move {
        let pool = egress_pool(&context, address, egress_params());
        establish_many(&pool).await;
        let scans = pool.pool_scans_for_test();
        for _ in 0..64 {
            let (received, _, session) = exchange(&pool, "/ok", Reading::Source).await;
            assert_eq!(received, ok());
            until(|| session.is_idle()).await;
        }
        let hot_path_scans = pool.pool_scans_for_test() - scans;
        let kept = pool.session_count();

        let swept = egress_pool(
            &context,
            address,
            PoolParams {
                idle_timeout: IDLE,
                ..egress_params()
            },
        );
        let started = Instant::now();
        establish_many(&swept).await;
        until(|| swept.session_count() == 0).await;
        let periods = started.elapsed().as_millis() / IDLE.as_millis();
        (
            hot_path_scans,
            kept,
            swept.idle_retirement_retiring_passes_for_test(),
            u64::try_from(periods).expect("a bounded scenario"),
            swept.stats().sessions_closed,
        )
    });
    let (hot_path_scans, kept, retiring_passes, periods, closed) = observed;
    assert_eq!((hot_path_scans, kept, closed), (0, MANY, MANY as u64));
    assert!(
        (1..=periods + 1).contains(&retiring_passes),
        "{retiring_passes} sweeps retired the idle sessions in {periods} periods"
    );
    peer.finish();
}
