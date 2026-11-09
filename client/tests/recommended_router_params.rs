//! One `HttpSessionPool` with the parameters recommended for a router that is
//! compared with reqwest (most-recently-used reuse, hyper-util's interval
//! sweep, no session timers, buffered ingress before abort (`DIV-033`), unframed
//! bodyless requests (`DIV-036`), and non-abortive close (`DIV-038`)) against a
//! raw HTTP/1.1 engine: a normal request, a bodiless one and a truncated
//! response share one connection, the truncated response's bytes precede its
//! error, the next request dials another, and that one idles out; a request
//! cancelled before its response head closes its connection with FIN; and a
//! request committed on a reused connection the engine closed fails unstarted,
//! so the caller retries it on a fresh connection.

use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::experimental::UniplexResponseProgressExt;
use roxygen_session::uniplex::UniplexResponse;

use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSweep;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;

/// The router's `backend_pool_idle_timeout_secs` (4 s by default), shortened.
const IDLE: Duration = Duration::from_millis(200);
const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
const TRUNCATED: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nfirst";

/// What the engine saw on one of its connections, in order.
#[derive(Debug, PartialEq)]
enum Seen {
    Request(Vec<u8>),
    /// The client closed the connection, this long after the engine's last write.
    Closed(Duration),
    /// The client reset the connection.
    Reset,
}

/// A raw HTTP/1.1 engine for the router's pool.
struct Engine {
    address: SocketAddr,
    /// What the engine saw on each connection, sent once every one closed.
    served: Receiver<Vec<Vec<Seen>>>,
    /// Set once the engine read a `POST /hold` request.
    hold_seen: Arc<AtomicBool>,
    /// Lets the engine close (FIN) a connection after its `POST /then-close` reply.
    #[cfg_attr(
        fbcode_build,
        expect(
            dead_code,
            reason = "the stale-connection test needs Cargo-only session test hooks"
        )
    )]
    close: Sender<()>,
}

/// An engine that accepts `connections` connections and answers each request
/// with `ok`, except `POST /truncated`, which it answers with 5 of 10 body bytes
/// before it closes the connection; `POST /hold`, which it never answers; and
/// `POST /then-close`, after whose reply it closes its side (FIN) once told to
/// and keeps reading. Once every connection closed, it sends what it saw on each.
fn engine(connections: usize) -> Engine {
    let listener = TcpListener::bind("127.0.0.1:0").expect("engine binds");
    let address = listener.local_addr().expect("engine address");
    let (sender, served) = std::sync::mpsc::channel();
    let hold_seen = Arc::new(AtomicBool::new(false));
    let (close, closes) = std::sync::mpsc::channel();
    let closes = Arc::new(Mutex::new(closes));
    let held = Arc::clone(&hold_seen);
    std::thread::spawn(move || {
        let served: Vec<_> = (0..connections)
            .map(|_| {
                let (stream, _) = listener.accept().expect("engine accepts");
                let held = Arc::clone(&held);
                let closes = Arc::clone(&closes);
                std::thread::spawn(move || serve(stream, &held, &closes))
            })
            .collect();
        let served = served
            .into_iter()
            .map(|connection| connection.join().expect("connection served"))
            .collect();
        let _ = sender.send(served);
    });
    Engine {
        address,
        served,
        hold_seen,
        close,
    }
}

fn serve(mut stream: TcpStream, hold_seen: &AtomicBool, closes: &Mutex<Receiver<()>>) -> Vec<Seen> {
    let mut seen = Vec::new();
    let mut input = Vec::new();
    let mut buffer = [0; 4096];
    let mut last_write = Instant::now();
    loop {
        let head_end = loop {
            if let Some(end) = input.windows(4).position(|window| window == b"\r\n\r\n") {
                break end + 4;
            }
            match stream.read(&mut buffer) {
                Ok(0) => {
                    seen.push(Seen::Closed(last_write.elapsed()));
                    return seen;
                }
                Ok(read) => input.extend_from_slice(&buffer[..read]),
                Err(error) => {
                    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                    seen.push(Seen::Reset);
                    return seen;
                }
            }
        };
        let head = String::from_utf8_lossy(&input[..head_end]).to_ascii_lowercase();
        assert!(
            !head.contains("transfer-encoding"),
            "the router's requests carry no chunked framing: {head}"
        );
        let body_length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |length| length.trim().parse().expect("a valid length"));
        while input.len() < head_end + body_length {
            let read = stream.read(&mut buffer).expect("engine reads");
            assert_ne!(read, 0, "the request body arrives");
            input.extend_from_slice(&buffer[..read]);
        }
        let request: Vec<u8> = input.drain(..head_end + body_length).collect();
        let truncated = request.starts_with(b"POST /truncated ");
        let hold = request.starts_with(b"POST /hold ");
        let then_close = request.starts_with(b"POST /then-close ");
        seen.push(Seen::Request(request));
        if hold {
            hold_seen.store(true, Ordering::SeqCst);
            continue;
        }
        stream
            .write_all(if truncated { TRUNCATED } else { OK })
            .expect("engine replies");
        last_write = Instant::now();
        if then_close {
            closes
                .lock()
                .expect("close signal")
                .recv()
                .expect("the client asks for the FIN");
        }
        if truncated || then_close {
            stream.shutdown(Shutdown::Write).expect("engine closes");
        }
    }
}

/// `PoolParams` and `SessionParams` exactly as recommended for the router, with
/// a short idle timeout.
fn recommended_pool(context: WorkerContext, address: SocketAddr) -> HttpSessionPool {
    HttpSessionPool::new(
        context,
        SocketAddress::new(address.ip().to_string(), address.port()),
        PoolParams {
            max_connections: u32::MAX,
            max_connection_attempts: 1,
            connect_timeout: Duration::from_secs(1),
            max_age: Duration::ZERO,
            max_waiters: u16::MAX,
            reuse_order: SessionReuseOrder::MostRecentlyUsed,
            idle_timeout: IDLE,
            idle_sweep: IdleSweep::Interval,
            ..PoolParams::default()
        },
        ConnectionParams::default(),
        SessionParams {
            stream_read_timeout: Some(Duration::ZERO),
            write_timeout: Some(Duration::ZERO),
            connection_read_timeout: Some(Duration::ZERO),
            deliver_ingress_before_abort: true,
            unframed_bodiless_http1_requests: true,
            non_abortive_close: true,
            ..SessionParams::default()
        },
    )
}

/// What the client received of one response: its status, each body event's
/// bytes, and the error that ended it, if any.
#[derive(Debug, PartialEq)]
enum Received {
    Head(u16),
    Body(Vec<u8>),
    Error(HttpErrorCode),
}

/// Sends a POST of `body` (bodiless, as the router sends its probes, when
/// `None`) on the pool's next session; returns the request's sequence number on
/// its connection, whether it was started, and what the client read.
async fn post(
    pool: &HttpSessionPool,
    path: &[u8],
    body: Option<&[u8]>,
) -> (u64, bool, Vec<Received>) {
    let GetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds");
    let mut response = match body {
        Some(body) => {
            let body = roxygen_core::buf_queue::BufQueue::copy_from_slice(body, body.len())
                .expect("bounded body");
            let request = HttpFixedSource::make_fixed_request(path, HttpMethod::Post, Some(body))
                .expect("test request is valid");
            session
                .send_reserved_request(
                    HttpSourceHolder::new(Box::new(request)),
                    reservation,
                    CancellationToken::never(),
                )
                .await
                .expect("request is committed")
        }
        None => {
            let mut message = HttpMessage::new();
            message.set_method(HttpMethod::Post).expect("valid method");
            message.set_url(path, false).expect("valid target");
            message.set_http_version(1, 1);
            session
                .send_reserved_request_with_headers(
                    HttpHeaderEvent::new(message, true).expect("valid head"),
                    HttpSourceHolder::empty(),
                    reservation,
                    CancellationToken::never(),
                )
                .expect("request is committed")
        }
    };
    let received = read(&mut response).await;
    (
        response.sequence_number,
        response.request_started(),
        received,
    )
}

/// Waits for the engine to report every connection.
async fn served_by(engine: &Engine) -> Vec<Vec<Seen>> {
    loop {
        if let Ok(served) = engine.served.try_recv() {
            return served;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn read(response: &mut UniplexResponse) -> Vec<Received> {
    let mut reads = Vec::new();
    let head = match response.source.read_header_event().await {
        Ok(head) => head,
        Err(error) => return vec![Received::Error(error.code())],
    };
    reads.push(Received::Head(
        head.headers().response_status().unwrap_or(0),
    ));
    let mut eom = head.eom();
    while !eom {
        match response.source.read_body_event(u32::MAX).await {
            Ok(event) => {
                if let Some(body) = event.as_body()
                    && !body.is_empty()
                {
                    reads.push(Received::Body(
                        body.segments()
                            .flat_map(|segment| segment.to_vec())
                            .collect(),
                    ));
                }
                eom = event.eom();
            }
            Err(error) => {
                reads.push(Received::Error(error.code()));
                break;
            }
        }
    }
    reads
}

fn bounded<T>(call: WorkerCall<T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes")
}

/// Runs the client task on a `ManagedWorker` and on a `HostedWorker`.
fn on_workers<F, T>(factory: F) -> [T; 2]
where
    F: FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T> + 'static>> + Clone + Send + 'static,
    T: Send + 'static,
{
    let worker = ManagedWorker::new(64).expect("client worker starts");
    let managed = bounded(
        worker
            .handle()
            .call_local(factory.clone())
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "router-params".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted client worker starts");
    let hosted = bounded(
        worker
            .handle()
            .call_local(factory)
            .expect("client task is admitted"),
    );
    drop(worker);
    [managed, hosted]
}

#[test]
fn the_recommended_router_pool_reuses_retires_and_delivers_truncated_responses() {
    for client in on_workers(|context| {
        Box::pin(async move {
            let engine = engine(2);
            let pool = recommended_pool(context, engine.address);
            let exchanges = vec![
                post(&pool, b"/generate", Some(b"hello")).await,
                post(&pool, b"/v1/loads", None).await,
                post(&pool, b"/truncated", None).await,
                post(&pool, b"/generate", Some(b"again")).await,
            ];
            // The engine reports once its second connection idled out too.
            let served = served_by(&engine).await;
            // The session the truncated response failed reports its error to the join.
            let _ = pool.drain_and_join().await;
            (exchanges, served)
        })
    }) {
        let (exchanges, served) = client;
        let ok = || vec![Received::Head(200), Received::Body(b"ok".to_vec())];
        assert_eq!(
            exchanges,
            [
                (0, true, ok()),
                (1, true, ok()),
                (
                    2,
                    true,
                    vec![
                        Received::Head(200),
                        Received::Body(b"first".to_vec()),
                        Received::Error(HttpErrorCode::TRANSPORT_EOF),
                    ]
                ),
                (0, true, ok()),
            ],
            "three exchanges share the first connection; the truncated response's \
             bytes precede its error; the next request dials"
        );
        let [first, second] = <[_; 2]>::try_from(served).expect("two connections");
        assert_eq!(
            first[..3],
            [
                Seen::Request(
                    b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nhello"
                        .to_vec()
                ),
                Seen::Request(b"POST /v1/loads HTTP/1.1\r\nConnection: keep-alive\r\n\r\n".to_vec()),
                Seen::Request(b"POST /truncated HTTP/1.1\r\nConnection: keep-alive\r\n\r\n".to_vec()),
            ]
        );
        // The session that the truncated response failed closes its connection.
        assert!(
            matches!(first[3..], [Seen::Closed(_) | Seen::Reset]),
            "{first:?}"
        );
        assert_eq!(
            second[0],
            Seen::Request(
                b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nagain"
                    .to_vec()
            )
        );
        let [_, Seen::Closed(idle)] = second[..] else {
            panic!("the idle connection closes cleanly: {second:?}");
        };
        // The interval sweep retires a connection idle for longer than IDLE, at a
        // sweep every IDLE: after more than IDLE and within about two periods.
        assert!(
            idle > IDLE && idle < 3 * IDLE + Duration::from_millis(500),
            "retired after {idle:?}"
        );
    }
}

/// A request cancelled before its response head (the router drops the
/// response when its client goes away) closes the engine connection with FIN,
/// as hyper's client does, not with a reset.
#[test]
fn the_recommended_router_pool_closes_a_connection_cancelled_before_its_head_with_fin() {
    for served in on_workers(|context| {
        Box::pin(async move {
            let engine = engine(1);
            let pool = recommended_pool(context, engine.address);
            let GetSessionResult {
                reservation,
                session,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("acquisition succeeds");
            let body = roxygen_core::buf_queue::BufQueue::copy_from_slice(b"hello", 5)
                .expect("bounded body");
            let request =
                HttpFixedSource::make_fixed_request(b"/hold", HttpMethod::Post, Some(body))
                    .expect("test request is valid");
            let response = session
                .send_reserved_request(
                    HttpSourceHolder::new(Box::new(request)),
                    reservation,
                    CancellationToken::never(),
                )
                .await
                .expect("request is committed");
            while !engine.hold_seen.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            drop(response);
            let _ = session.wait_closed().await;
            let served = served_by(&engine).await;
            let _ = pool.drain_and_join().await;
            served
        })
    }) {
        let [connection] = <[_; 1]>::try_from(served).expect("one connection");
        assert!(
            matches!(
                connection[..],
                [Seen::Request(ref request), Seen::Closed(_)]
                    if request.starts_with(b"POST /hold HTTP/1.1\r\n")
            ),
            "the engine sees the request, then FIN: {connection:?}"
        );
    }
}

/// What the router's retry loop sees of a request committed on a reused
/// connection whose engine closed it before the session wrote the request.
#[cfg(not(fbcode_build))]
#[derive(Debug, PartialEq)]
struct StaleAttempt {
    sequence_number: u64,
    started: bool,
    received: Vec<Received>,
}

/// The stale reused connection of `DIV-035`, with the recommended parameters:
/// after one exchange the engine closes its side (FIN) while the next request's
/// session writer is held (the session's `test-hooks` feature), as when that
/// FIN and the commit fall in one turn. The request fails unstarted on a
/// reused connection, which is the router's (and hyper-util's) retry signal,
/// and the retry dials a fresh connection, where it succeeds.
#[cfg(not(fbcode_build))]
#[test]
fn the_recommended_router_pool_signals_a_retry_for_a_request_on_a_stale_reused_connection() {
    use std::rc::Rc;

    for (attempts, served) in on_workers(|context| {
        Box::pin(async move {
            let engine = engine(2);
            let pool = recommended_pool(context, engine.address);
            let first = post(&pool, b"/then-close", Some(b"first")).await;
            let GetSessionResult {
                reservation,
                session,
            } = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("acquisition succeeds");
            let release = Rc::new(tokio::sync::Notify::new());
            session.hold_writer_for_test(Rc::clone(&release));
            let body = roxygen_core::buf_queue::BufQueue::copy_from_slice(b"hello", 5)
                .expect("bounded body");
            let request =
                HttpFixedSource::make_fixed_request(b"/generate", HttpMethod::Post, Some(body))
                    .expect("test request is valid");
            let mut response = session
                .send_reserved_request(
                    HttpSourceHolder::new(Box::new(request)),
                    reservation,
                    CancellationToken::never(),
                )
                .await
                .expect("request is committed");
            engine
                .close
                .send(())
                .expect("the engine waits for the FIN request");
            // Loopback delivers the FIN at once; the session reads it meanwhile.
            tokio::time::sleep(Duration::from_millis(100)).await;
            release.notify_one();
            let received = tokio::time::timeout(Duration::from_secs(2), read(&mut response))
                .await
                .expect("the request fails promptly");
            let stale = StaleAttempt {
                sequence_number: response.sequence_number,
                started: response.request_started(),
                received,
            };
            drop(response);
            // The router's loop: unstarted on a reused connection -> retry.
            assert!(stale.sequence_number != 0 && !stale.started, "{stale:?}");
            let retry = post(&pool, b"/generate", Some(b"hello")).await;
            let _ = pool.drain_and_join().await;
            let served = served_by(&engine).await;
            ((first, stale, retry), served)
        })
    }) {
        let ok = || vec![Received::Head(200), Received::Body(b"ok".to_vec())];
        let (first, stale, retry) = attempts;
        assert_eq!(first, (0, true, ok()));
        assert_eq!(
            stale,
            StaleAttempt {
                sequence_number: 1,
                started: false,
                received: vec![Received::Error(HttpErrorCode::TRANSPORT_EOF)],
            },
            "the request failed unstarted on the reused connection"
        );
        assert_eq!(retry, (0, true, ok()), "the retry dials and succeeds");
        let [stale_connection, fresh] = <[_; 2]>::try_from(served).expect("two connections");
        assert!(
            matches!(
                stale_connection[..],
                [Seen::Request(ref request), Seen::Closed(_)]
                    if request.starts_with(b"POST /then-close HTTP/1.1\r\n")
            ),
            "nothing reaches the closed connection after its FIN: {stale_connection:?}"
        );
        assert_eq!(
            fresh[0],
            Seen::Request(
                b"POST /generate HTTP/1.1\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nhello"
                    .to_vec()
            )
        );
    }
}
