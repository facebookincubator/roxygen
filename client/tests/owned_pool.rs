//! The experimental owned session pool (`DIV-046`): `HttpSessionPool`'s
//! acquisition, waiter, admission and idle-retirement behavior on owned client
//! sessions, over loopback TCP against a hyper origin (HTTP/1.1, and HTTP/2 with
//! prior knowledge), on a `RuntimeOwner` worker that must quiesce without
//! forcing.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::cell::RefCell;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpErrorCode;
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
use roxygen_runtime::tokio_host::HostedWorkerJoin;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorkerError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::tls::TlsClientOptions;

use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::SessionParams;
use crate::experimental::owned::OwnedClientReservation;
use crate::experimental::owned::OwnedClientResponse;
use crate::experimental::owned::OwnedClientSession;
use crate::experimental::owned::OwnedGetSessionResult;
use crate::experimental::owned::OwnedHttpSessionPool;
use crate::experimental::owned::worker_admission_checks_for_test;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSweep;
use crate::pool::PoolError;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;

const TIMEOUT: Duration = Duration::from_secs(10);
/// `/slow` streams `SLOW_CHUNKS` chunks this far apart.
const CHUNK_GAP: Duration = Duration::from_millis(40);
const SLOW_CHUNKS: usize = 10;
const PROTOCOLS: [CodecProtocol; 2] = [CodecProtocol::Http11, CodecProtocol::Http2];

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

/// Runs `factory` on a one-worker runtime, then quiesces it: every pool and
/// session the scenario left must close without a forced worker.
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

/// Every session timer disabled explicitly.
fn untimed() -> SessionParams {
    SessionParams {
        stream_read_timeout: Some(Duration::ZERO),
        connection_read_timeout: Some(Duration::ZERO),
        write_timeout: Some(Duration::ZERO),
        ..SessionParams::default()
    }
}

fn plaintext(protocol: CodecProtocol) -> ConnectionParams {
    ConnectionParams {
        plaintext_protocol: protocol,
        ..ConnectionParams::default()
    }
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

type OriginBody = BoxBody<Bytes, Infallible>;

fn full(body: impl Into<Bytes>) -> OriginBody {
    Full::new(body.into()).boxed()
}

/// `/` answers `ok`, `/close` answers `ok` and closes its HTTP/1.1
/// connection, and `/slow` streams `SLOW_CHUNKS` chunks `CHUNK_GAP` apart.
async fn respond(
    request: hyper::Request<Incoming>,
) -> Result<hyper::Response<OriginBody>, Infallible> {
    let response = hyper::Response::builder().status(200);
    let response = match request.uri().path() {
        "/" => response.body(full("ok")),
        "/close" => response.header("connection", "close").body(full("ok")),
        "/slow" => {
            let chunks = futures::stream::unfold(0, |sent| async move {
                if sent == SLOW_CHUNKS {
                    return None;
                }
                tokio::time::sleep(CHUNK_GAP).await;
                Some((
                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"chunk"))),
                    sent + 1,
                ))
            });
            response.body(BodyExt::boxed(StreamBody::new(chunks)))
        }
        _ => hyper::Response::builder()
            .status(404)
            .body(full(Bytes::new())),
    };
    Ok(response.expect("valid origin response"))
}

async fn serve(stream: tokio::net::TcpStream, protocol: CodecProtocol) {
    let io = TokioIo::new(stream);
    // A connection the client closes or resets ends with an error no test reads.
    if protocol == CodecProtocol::Http2 {
        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(io, service_fn(respond))
            .await;
    } else {
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service_fn(respond))
            .await;
    }
}

/// A hyper origin on its own thread, serving `protocol` on loopback until
/// dropped, that counts the connections it accepted.
struct Origin {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Origin {
    fn start(protocol: CodecProtocol) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
        listener.set_nonblocking(true).expect("nonblocking origin");
        let address = listener.local_addr().expect("origin address");
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("origin runtime")
                .block_on(async move {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("origin listener");
                    let mut connections = tokio::task::JoinSet::new();
                    tokio::pin!(stopped);
                    loop {
                        tokio::select! {
                            _ = &mut stopped => break,
                            accepted = listener.accept() => {
                                let (stream, _) = accepted.expect("origin accepts");
                                counter.fetch_add(1, Ordering::SeqCst);
                                connections.spawn(serve(stream, protocol));
                            }
                        }
                    }
                    connections.shutdown().await;
                });
        });
        Self {
            address,
            accepted,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// An owned pool to `address` with `params`, a 5 s connect timeout unless
/// `params` sets another, no maximum age unless `params` sets one, and
/// `session`'s parameters.
fn pool_with(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    params: PoolParams,
    session: SessionParams,
) -> OwnedHttpSessionPool {
    OwnedHttpSessionPool::new(
        context.clone(),
        socket_address(address),
        params,
        plaintext(protocol),
        session,
    )
    .expect("the pool accepts its parameters")
}

/// Pool parameters with `max_connections`, a 5 s connect timeout and no
/// maximum age.
fn params(max_connections: u32) -> PoolParams {
    PoolParams {
        max_connections,
        connect_timeout: Duration::from_secs(5),
        max_age: Duration::ZERO,
        ..PoolParams::default()
    }
}

fn pool(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    max_connections: u32,
) -> OwnedHttpSessionPool {
    pool_with(
        context,
        address,
        protocol,
        params(max_connections),
        untimed(),
    )
}

async fn acquire(pool: &OwnedHttpSessionPool) -> OwnedGetSessionResult {
    pool.get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds")
}

/// A GET for `path` with a `Host` header, and its empty body.
async fn request(path: &str) -> (HttpHeaderEvent, HttpSourceHolder) {
    let mut source = HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_get_request(path.as_bytes(), None).expect("valid request"),
    ));
    let mut head = source.read_header_event().await.expect("request head");
    head.headers_mut()
        .headers_mut()
        .add("host", "origin.example");
    (head, HttpSourceHolder::empty())
}

/// Sends `path` on `reservation` and returns the 200 response's whole body.
async fn exchange(reservation: OwnedClientReservation, path: &str) -> Vec<u8> {
    read_body(submit(reservation, path).await).await
}

/// Sends `path` on `reservation` and returns its response once the request is committed.
async fn submit(reservation: OwnedClientReservation, path: &str) -> OwnedClientResponse {
    let (head, body) = request(path).await;
    reservation
        .submit(head, body, CancellationToken::never())
        .await
        .expect("request is committed")
}

/// Reads the 200 `response` to its end and returns its whole body.
async fn read_body(response: OwnedClientResponse) -> Vec<u8> {
    let mut source = response.source.into_source_owner().into_source_holder();
    let head = source.read_header_event().await.expect("response head");
    assert_eq!(head.headers().response_status(), Some(200));
    let mut body = Vec::new();
    let mut eom = head.eom();
    while !eom {
        let event = source
            .read_body_event(u32::MAX)
            .await
            .expect("response body");
        if let Some(data) = event.as_body() {
            data.segments()
                .for_each(|segment| body.extend_from_slice(segment));
        }
        eom = event.eom();
    }
    body
}

/// Polls `ready` every millisecond until it holds.
async fn until(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Lets the session retire its finished exchange.
async fn settle(session: &OwnedClientSession) {
    until(|| session.is_idle()).await;
}

/// Runs one exchange on the pool's next session and returns that session.
async fn one_exchange(pool: &OwnedHttpSessionPool) -> OwnedClientSession {
    let OwnedGetSessionResult {
        reservation,
        session,
    } = acquire(pool).await;
    assert_eq!(exchange(reservation, "/").await, b"ok");
    settle(&session).await;
    session
}

fn local(session: &OwnedClientSession) -> TransportEndpoint {
    session.connection_context().local_endpoint().clone()
}

#[test]
fn acquisitions_reuse_a_released_session() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (same, sessions, protocols) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 4);
            let first = one_exchange(&pool).await;
            let second = one_exchange(&pool).await;
            (
                local(&first) == local(&second),
                pool.session_count(),
                [first.protocol(), second.protocol()],
            )
        });
        assert_eq!(
            (same, sessions, protocols, origin.accepted()),
            (true, 1, [protocol; 2], 1),
            "{protocol:?}: the second acquisition reuses the first's idle session"
        );
    }
}

#[test]
fn http2_pools_serve_with_default_and_explicit_session_timeouts() {
    for explicit_timeouts in [false, true] {
        let origin = Origin::start(CodecProtocol::Http2);
        let address = origin.address;
        let (same, sessions, finished) = run_on_worker(move |context| async move {
            let session = if explicit_timeouts {
                SessionParams {
                    stream_read_timeout: Some(Duration::from_secs(30)),
                    connection_read_timeout: Some(Duration::from_secs(40)),
                    write_timeout: Some(Duration::from_secs(20)),
                    ..SessionParams::default()
                }
            } else {
                SessionParams::default()
            };
            let pool = pool_with(&context, address, CodecProtocol::Http2, params(1), session);
            let first = one_exchange(&pool).await;
            let second = one_exchange(&pool).await;
            let same = local(&first) == local(&second);
            let sessions = pool.session_count();
            pool.drain_and_join().await.expect("pool joins");
            (same, sessions, first.is_finished() && second.is_finished())
        });
        assert_eq!(
            (same, sessions, finished, origin.accepted()),
            (true, 1, true, 1),
            "explicit timeouts: {explicit_timeouts}"
        );
    }
}

#[test]
fn a_reserved_http1_session_makes_the_pool_dial_another() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (distinct, sessions, bodies) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 4);
        let first = acquire(&pool).await;
        let second = acquire(&pool).await;
        let distinct = local(&first.session) != local(&second.session);
        let sessions = pool.session_count();
        let bodies = [
            exchange(first.reservation, "/").await,
            exchange(second.reservation, "/").await,
        ];
        (distinct, sessions, bodies)
    });
    assert_eq!(
        (distinct, sessions, bodies, origin.accepted()),
        (true, 2, [b"ok".to_vec(), b"ok".to_vec()], 2)
    );
}

#[test]
fn http2_multiplexes_concurrent_acquisitions_through_one_session() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    let (sessions, distinct, bodies) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let acquired: Vec<OwnedGetSessionResult> =
            futures::stream::iter((0..8).map(|_| acquire(&pool)))
                .buffered(8)
                .collect()
                .await;
        let sessions = pool.session_count();
        let mut locals: Vec<TransportEndpoint> = acquired
            .iter()
            .map(|acquired| local(&acquired.session))
            .collect();
        locals.dedup();
        let bodies: Vec<Vec<u8>> = futures::stream::iter(
            acquired
                .into_iter()
                .map(|acquired| exchange(acquired.reservation, "/slow")),
        )
        .buffered(8)
        .collect()
        .await;
        (sessions, locals.len(), bodies)
    });
    assert_eq!(
        (sessions, distinct, origin.accepted()),
        (1, 1, 1),
        "eight held reservations share one HTTP/2 connection"
    );
    assert_eq!(bodies, vec![b"chunk".repeat(SLOW_CHUNKS); 8]);
}

/// HTTP/2 with a local stream limit of 1, so one held reservation exhausts the
/// session.
fn one_stream(protocol: CodecProtocol) -> SessionParams {
    SessionParams {
        max_concurrent_outgoing_streams: (protocol == CodecProtocol::Http2).then_some(1),
        ..untimed()
    }
}

#[test]
fn a_waiter_takes_the_released_session() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (pending, same, body) = run_on_worker(move |context| async move {
            let pool = pool_with(&context, address, protocol, params(1), one_stream(protocol));
            let first = acquire(&pool).await;
            let second = pool.get_session_with_reservation(CancellationToken::never());
            tokio::pin!(second);
            let pending = tokio::select! {
                _ = &mut second => false,
                () = tokio::time::sleep(Duration::from_millis(100)) => true,
            };
            drop(first.reservation);
            let second = second.await.expect("the waiter is served");
            let same = local(&first.session) == local(&second.session);
            (pending, same, exchange(second.reservation, "/").await)
        });
        assert_eq!(
            (pending, same, body, origin.accepted()),
            (true, true, b"ok".to_vec(), 1),
            "{protocol:?}: the waiter waits for, then reuses, the released session"
        );
    }
}

#[test]
fn acquisitions_beyond_max_waiters_are_refused() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let refused = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http11,
            PoolParams {
                max_waiters: 1,
                ..params(1)
            },
            untimed(),
        );
        let _held = acquire(&pool).await;
        let waiter = pool.get_session_with_reservation(CancellationToken::never());
        tokio::pin!(waiter);
        tokio::select! {
            _ = &mut waiter => panic!("the waiter was served while the only session was reserved"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        pool.get_session_with_reservation(CancellationToken::never())
            .await
            .err()
    });
    assert!(
        matches!(refused, Some(PoolError::MaxWaiters)),
        "{refused:?}"
    );
}

#[test]
fn a_waiter_times_out_at_the_connect_timeout() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (error, waited) = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http11,
            PoolParams {
                connect_timeout: Duration::from_millis(200),
                ..params(1)
            },
            untimed(),
        );
        let _held = acquire(&pool).await;
        let started = Instant::now();
        let error = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .err();
        (error, started.elapsed())
    });
    assert!(matches!(error, Some(PoolError::Timeout)), "{error:?}");
    assert!(
        (Duration::from_millis(200)..Duration::from_secs(2)).contains(&waited),
        "waited {waited:?}"
    );
}

#[test]
fn a_waiter_dials_a_replacement_when_the_only_session_closes_admission() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (pending, replaced, body) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 1);
        let first = acquire(&pool).await;
        let second = pool.get_session_with_reservation(CancellationToken::never());
        tokio::pin!(second);
        let pending = tokio::select! {
            _ = &mut second => false,
            () = tokio::time::sleep(Duration::from_millis(50)) => true,
        };
        first.session.close_when_idle().expect("the session drains");
        let second = second.await.expect("the waiter is served");
        drop(first.reservation);
        let replaced = local(&first.session) != local(&second.session);
        (pending, replaced, exchange(second.reservation, "/").await)
    });
    assert_eq!(
        (pending, replaced, body, origin.accepted()),
        (true, true, b"ok".to_vec(), 2)
    );
}

#[test]
fn a_connection_close_response_retires_its_session() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (replaced, sessions) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 4);
        let OwnedGetSessionResult {
            reservation,
            session: first,
        } = acquire(&pool).await;
        assert_eq!(exchange(reservation, "/close").await, b"ok");
        first.wait_closed().await.expect("the session closes");
        let second = one_exchange(&pool).await;
        (local(&first) != local(&second), pool.session_count())
    });
    assert_eq!((replaced, sessions, origin.accepted()), (true, 1, 2));
}

/// Runs one exchange on `pool`'s next uniplex session, and returns that session
/// once it has no active exchange.
async fn uniplex_one_exchange(pool: &HttpSessionPool) -> ClientSession {
    uniplex_try_one_exchange(pool)
        .await
        .expect("acquisition succeeds")
}

/// As `uniplex_one_exchange`, or the acquisition's error.
async fn uniplex_try_one_exchange(pool: &HttpSessionPool) -> Result<ClientSession, PoolError> {
    let GetSessionResult {
        reservation,
        session,
    } = pool
        .get_session_with_reservation(CancellationToken::never())
        .await?;
    let (head, body) = request("/").await;
    let mut response = session
        .send_reserved_request_with_headers(head, body, reservation, CancellationToken::never())
        .expect("request is committed");
    let head = response
        .source
        .read_header_event()
        .await
        .expect("response head");
    let mut eom = head.eom();
    while !eom {
        eom = response
            .source
            .read_body_event(u32::MAX)
            .await
            .expect("response body")
            .eom();
    }
    drop(response);
    until(|| session.active_transactions() == 0).await;
    Ok(session)
}

/// Whether the session the second exchange took, 120 ms after the first, was
/// another connection, and whether the first closed cleanly, through an owned
/// pool or an `HttpSessionPool` whose sessions age out after 28 to 52 ms.
///
/// The connections the origin accepts are not compared: in both pools the aged
/// session's drain wakes the acquisition's own waiter, which then starts a
/// second dial beside the first, and whether that dial connects before the
/// waiter is served depends on task timing.
fn aged_session_scenario(owned: bool) -> (bool, bool) {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (replaced, closed) = run_on_worker(move |context| async move {
        let params = PoolParams {
            // Sampled between 70% and 130%.
            max_age: Duration::from_millis(40),
            ..params(4)
        };
        if owned {
            let pool = pool_with(&context, address, CodecProtocol::Http11, params, untimed());
            let first = one_exchange(&pool).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            let second = one_exchange(&pool).await;
            let closed = first.wait_closed().await.is_ok();
            (local(&first) != local(&second), closed)
        } else {
            let pool = HttpSessionPool::new(
                context,
                socket_address(address),
                params,
                plaintext(CodecProtocol::Http11),
                untimed(),
            );
            let first = uniplex_one_exchange(&pool).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            let second = uniplex_one_exchange(&pool).await;
            let closed = first.wait_closed().await.is_ok();
            (first.local_address() != second.local_address(), closed)
        }
    });
    (replaced, closed)
}

#[test]
fn an_aged_session_is_retired_at_acquisition_as_by_the_uniplex_pool() {
    assert_eq!(
        [aged_session_scenario(true), aged_session_scenario(false)],
        [(true, true); 2],
        "the aged session is replaced and closes cleanly, through either pool"
    );
}

#[test]
fn drain_and_join_closes_every_session_and_refuses_later_acquisitions() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (joined, finished, refused) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 4);
            let first = acquire(&pool).await;
            let second = acquire(&pool).await;
            let (first_session, second_session) = (first.session.clone(), second.session.clone());
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            assert_eq!(exchange(second.reservation, "/").await, b"ok");
            let joined = pool.drain_and_join().await;
            let finished = first_session.is_finished() && second_session.is_finished();
            let refused = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .err();
            (joined.is_ok(), finished, refused)
        });
        assert!(joined && finished, "{protocol:?}");
        assert!(
            matches!(refused, Some(PoolError::Draining)),
            "{protocol:?}: {refused:?}"
        );
    }
}

#[test]
fn the_pool_refuses_what_owned_sessions_cannot_honor_before_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let address = listener.local_addr().expect("listener address");
    let refusals = run_on_worker(move |context| async move {
        let tls = ConnectionParams {
            transport: ClientTransportOptions::Tls(TlsClientOptions::new("origin.example")),
            ..plaintext(CodecProtocol::Http11)
        };
        let cases = [
            ("TLS", tls, untimed()),
            (
                "stream_read_timeout",
                plaintext(CodecProtocol::Http11),
                SessionParams::default(),
            ),
        ];
        cases
            .into_iter()
            .map(|(field, connection, session)| {
                let refusal = OwnedHttpSessionPool::new(
                    context.clone(),
                    socket_address(address),
                    params(4),
                    connection,
                    session,
                )
                .err()
                .map(|error| {
                    (
                        error.code(),
                        String::from_utf8_lossy(error.message()).contains(field),
                    )
                });
                (field, refusal)
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(
        refusals,
        [
            ("TLS", Some((HttpErrorCode::INVALID_STATE_TRANSITION, true))),
            (
                "stream_read_timeout",
                Some((HttpErrorCode::INVALID_STATE_TRANSITION, true))
            ),
        ]
    );
    assert_eq!(
        listener.accept().map(|_| ()).map_err(|error| error.kind()),
        Err(std::io::ErrorKind::WouldBlock),
        "no refused pool dialed"
    );
}

/// The idle timeout of the idle-retirement tests, and the scheduling slack of
/// tests that reuse a just-released session.
const IDLE: Duration = Duration::from_millis(100);

/// Pool parameters with `max_connections` and `DIV-032`'s idle retirement.
fn idle_params(
    max_connections: u32,
    reuse_order: SessionReuseOrder,
    idle_timeout: Duration,
    idle_sweep: IdleSweep,
) -> PoolParams {
    PoolParams {
        idle_timeout,
        reuse_order,
        idle_sweep,
        ..params(max_connections)
    }
}

fn idle_pool(
    context: &WorkerContext,
    address: SocketAddr,
    protocol: CodecProtocol,
    params: PoolParams,
) -> OwnedHttpSessionPool {
    pool_with(context, address, protocol, params, untimed())
}

async fn sleep_until(deadline: Instant) {
    tokio::time::sleep(deadline.saturating_duration_since(Instant::now())).await;
}

/// Opens two HTTP/1.1 sessions, then idles them in order: the second is the
/// most recently used.
async fn two_idle_sessions(
    pool: &OwnedHttpSessionPool,
) -> (OwnedClientSession, OwnedClientSession) {
    let first = acquire(pool).await;
    let second = acquire(pool).await;
    assert_ne!(
        local(&first.session),
        local(&second.session),
        "a reserved session makes the pool open another"
    );
    assert_eq!(exchange(first.reservation, "/").await, b"ok");
    settle(&first.session).await;
    assert_eq!(exchange(second.reservation, "/").await, b"ok");
    settle(&second.session).await;
    (first.session, second.session)
}

#[test]
fn idle_session_is_retired_and_replaced() {
    for protocol in PROTOCOLS {
        for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
            let origin = Origin::start(protocol);
            let address = origin.address;
            let (sessions, closed, replaced, body) = run_on_worker(move |context| async move {
                let pool = idle_pool(
                    &context,
                    address,
                    protocol,
                    idle_params(1, SessionReuseOrder::LeastRecentlyUsed, IDLE, idle_sweep),
                );
                let first = one_exchange(&pool).await;
                tokio::time::sleep(IDLE * 4).await;
                let sessions = pool.session_count();
                let closed =
                    tokio::time::timeout(Duration::from_secs(1), first.wait_closed()).await;
                let OwnedGetSessionResult {
                    reservation,
                    session: second,
                } = acquire(&pool).await;
                (
                    sessions,
                    matches!(closed, Ok(Ok(_))),
                    local(&first) != local(&second),
                    exchange(reservation, "/").await,
                )
            });
            assert_eq!(
                (sessions, closed, replaced, body),
                (0, true, true, b"ok".to_vec()),
                "{protocol:?} {idle_sweep:?}: the idle session is retired without an acquisition, \
                 closes, and the next request opens a new connection"
            );
        }
    }
}

#[test]
fn session_streaming_a_long_response_is_not_retired() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (body, draining, reused) = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                protocol,
                idle_params(
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    IDLE,
                    IdleSweep::AtExpiry,
                ),
            );
            let OwnedGetSessionResult {
                reservation,
                session: first,
            } = acquire(&pool).await;
            let body = exchange(reservation, "/slow").await;
            let draining = first.is_draining();
            settle(&first).await;
            let second = one_exchange(&pool).await;
            (body, draining, local(&first) == local(&second))
        });
        assert_eq!(
            (body, draining, reused),
            (b"chunk".repeat(SLOW_CHUNKS), false, true),
            "{protocol:?}: a response that outlives four idle timeouts keeps its session, and \
             the idle period starts when it ends"
        );
    }
}

#[test]
fn reserved_session_is_not_retired() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (draining, sessions, body) = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                protocol,
                idle_params(
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    IDLE,
                    IdleSweep::AtExpiry,
                ),
            );
            let OwnedGetSessionResult {
                reservation,
                session,
            } = acquire(&pool).await;
            tokio::time::sleep(IDLE * 4).await;
            (
                session.is_draining(),
                pool.session_count(),
                exchange(reservation, "/").await,
            )
        });
        assert_eq!(
            (draining, sessions, body),
            (false, 1, b"ok".to_vec()),
            "{protocol:?}: a reserved session is not idle"
        );
    }
}

#[test]
fn session_reused_just_before_expiry_is_not_retired() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (reused, kept, retired) = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                1,
                SessionReuseOrder::LeastRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
        );
        let first = one_exchange(&pool).await;
        let first_idle = Instant::now();
        tokio::time::sleep(IDLE * 7 / 10).await;
        let reused = one_exchange(&pool).await;
        let reused_idle = Instant::now();
        sleep_until(first_idle + IDLE * 13 / 10).await;
        let kept = !first.is_draining();
        sleep_until(reused_idle + IDLE * 2).await;
        (local(&first) == local(&reused), kept, first.is_draining())
    });
    assert_eq!(
        (reused, kept, retired),
        (true, true, true),
        "reuse restarts the idle period, and the session expires after the reuse"
    );
}

#[test]
fn least_recently_used_reuse_keeps_every_session_warm() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (both_used, warm, sessions) = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                2,
                SessionReuseOrder::LeastRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
        );
        let (first, second) = two_idle_sessions(&pool).await;
        let mut used = Vec::new();
        for _ in 0..12 {
            used.push(local(&one_exchange(&pool).await));
            tokio::time::sleep(IDLE / 3).await;
        }
        (
            used.contains(&local(&first)) && used.contains(&local(&second)),
            !first.is_draining() && !second.is_draining(),
            pool.session_count(),
        )
    });
    assert_eq!(
        (both_used, warm, sessions),
        (true, true, 2),
        "sequential traffic alternates between both sessions, so neither is retired"
    );
}

#[test]
fn most_recently_used_reuse_lets_the_cold_session_retire() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (hot_served, cold_retired, hot_kept, sessions) = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                2,
                SessionReuseOrder::MostRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
        );
        let (cold, hot) = two_idle_sessions(&pool).await;
        let mut hot_served = true;
        for _ in 0..12 {
            hot_served &= local(&one_exchange(&pool).await) == local(&hot);
            tokio::time::sleep(IDLE / 3).await;
        }
        (
            hot_served,
            cold.is_draining(),
            !hot.is_draining(),
            pool.session_count(),
        )
    });
    assert_eq!(
        (hot_served, cold_retired, hot_kept, sessions),
        (true, true, true, 1),
        "the most recently used idle session serves, and the unused one idles out"
    );
}

#[test]
fn most_recently_used_reuse_picks_the_session_released_last() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let picks = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                2,
                SessionReuseOrder::MostRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
        );
        let (older, newer) = two_idle_sessions(&pool).await;
        // Released in the other order, the older session is then the most recent.
        let first = acquire(&pool).await;
        let second = acquire(&pool).await;
        let picks = [
            local(&first.session) == local(&newer),
            local(&second.session) == local(&older),
        ];
        assert_eq!(exchange(first.reservation, "/").await, b"ok");
        settle(&first.session).await;
        assert_eq!(exchange(second.reservation, "/").await, b"ok");
        settle(&second.session).await;
        let last = local(&one_exchange(&pool).await) == local(&older);
        [picks[0], picks[1], last]
    });
    assert_eq!(picks, [true, true, true]);
}

/// Whether `items` holds no value twice.
fn all_distinct<T: PartialEq>(items: &[T]) -> bool {
    items
        .iter()
        .enumerate()
        .all(|(index, item)| !items[..index].contains(item))
}

/// How each of three sequential exchanges, 20 ms apart, ended (served, or its
/// acquisition's error), and whether each was served on a connection no
/// earlier exchange used, through an owned pool or an `HttpSessionPool` with a
/// 1 ns idle timeout and hyper-util's reuse order and sweep: a router's zero
/// `pool_idle_timeout`, "expire as soon as idle" to reqwest.
///
/// The connections the origin accepts are not compared: in both pools the
/// session the checkout retires wakes the acquisition's own waiter when its
/// drain closes its admission, and the waiter then starts a second dial beside
/// the first, as in `aged_session_scenario`; whether the next exchange takes
/// that unused session or dials again depends on task timing.
fn one_nanosecond_idle_scenario(owned: bool, protocol: CodecProtocol) -> (String, bool) {
    let origin = Origin::start(protocol);
    let address = origin.address;
    run_on_worker(move |context| async move {
        let params = idle_params(
            4,
            SessionReuseOrder::MostRecentlyUsed,
            Duration::from_nanos(1),
            IdleSweep::Interval,
        );
        let mut ends = Vec::new();
        let distinct = if owned {
            let pool = idle_pool(&context, address, protocol, params);
            let mut served = Vec::new();
            for _ in 0..3 {
                match pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                {
                    Ok(OwnedGetSessionResult {
                        reservation,
                        session,
                    }) => {
                        assert_eq!(exchange(reservation, "/").await, b"ok");
                        // The sweep may retire the session as soon as it is idle.
                        until(|| session.is_idle() || session.is_finished()).await;
                        served.push(local(&session));
                        ends.push(Ok(()));
                    }
                    Err(error) => ends.push(Err(error)),
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            all_distinct(&served)
        } else {
            let pool = HttpSessionPool::new(
                context,
                socket_address(address),
                params,
                plaintext(protocol),
                untimed(),
            );
            let mut served = Vec::new();
            for _ in 0..3 {
                match uniplex_try_one_exchange(&pool).await {
                    Ok(session) => {
                        served.push(session.local_address().clone());
                        ends.push(Ok(()));
                    }
                    Err(error) => ends.push(Err(error)),
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            all_distinct(&served)
        };
        (format!("{ends:?}"), distinct)
    })
}

/// As in an `HttpSessionPool`, a session the pool dialed counts as idle only
/// once it has served, so the dialing acquisition takes it before hyper-util's
/// checkout can retire it, however short the idle timeout: "expire as soon as
/// idle" means no reuse of an idle session, not no use.
#[test]
fn a_freshly_dialed_session_serves_its_dialing_acquisition_under_a_1_ns_idle_timeout() {
    for protocol in PROTOCOLS {
        let served = || ("[Ok(()), Ok(()), Ok(())]".to_owned(), true);
        assert_eq!(
            [true, false].map(|owned| one_nanosecond_idle_scenario(owned, protocol)),
            [served(), served()],
            "{protocol:?}: every exchange is served, each on a connection no earlier exchange \
             used, through an owned pool and an `HttpSessionPool`"
        );
    }
}

/// A reservation taken on a pooled session's handle, as a `ClientSession`'s
/// `TransactionAttached` reports it, ends the session's idle period.
#[test]
fn a_direct_reservation_restarts_the_idle_period() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (kept, retired) = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                protocol,
                idle_params(
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    IDLE,
                    IdleSweep::AtExpiry,
                ),
            );
            let session = one_exchange(&pool).await;
            let first_idle = Instant::now();
            tokio::time::sleep(IDLE * 7 / 10).await;
            let reservation = session
                .reserve_request()
                .expect("the idle session admits a reservation");
            assert_eq!(exchange(reservation, "/").await, b"ok");
            settle(&session).await;
            let reused_idle = Instant::now();
            sleep_until(first_idle + IDLE * 13 / 10).await;
            let kept = !session.is_draining();
            sleep_until(reused_idle + IDLE * 2).await;
            (kept, session.is_draining())
        });
        assert_eq!(
            (kept, retired),
            (true, true),
            "{protocol:?}: the direct reuse restarts the idle period, and the session expires \
             after it"
        );
    }
}

/// A direct reservation that outlives the idle timeout leaves the retirement
/// task waiting with no idle session; the session's next idle period wakes it.
#[test]
fn a_session_released_by_a_long_direct_exchange_is_retired_after_it() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (body, kept, retired) = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                protocol,
                idle_params(
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    IDLE,
                    IdleSweep::AtExpiry,
                ),
            );
            let session = one_exchange(&pool).await;
            tokio::time::sleep(IDLE / 2).await;
            let reservation = session
                .reserve_request()
                .expect("the idle session admits a reservation");
            let body = exchange(reservation, "/slow").await;
            let kept = !session.is_draining();
            settle(&session).await;
            let closed = tokio::time::timeout(IDLE * 10, session.wait_closed()).await;
            (body, kept, matches!(closed, Ok(Ok(_))))
        });
        assert_eq!(
            (body, kept, retired),
            (b"chunk".repeat(SLOW_CHUNKS), true, true),
            "{protocol:?}: the session outlives its exchange, then idles out and closes"
        );
    }
}

/// Under `MostRecentlyUsed`, a session released by a direct reservation is the
/// most recently used.
#[test]
fn a_direct_reservation_makes_its_session_the_most_recently_used() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let picked_older = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                2,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::ZERO,
                IdleSweep::AtExpiry,
            ),
        );
        let (older, _newer) = two_idle_sessions(&pool).await;
        let reservation = older
            .reserve_request()
            .expect("the idle session admits a reservation");
        assert_eq!(exchange(reservation, "/").await, b"ok");
        settle(&older).await;
        local(&one_exchange(&pool).await) == local(&older)
    });
    assert!(
        picked_older,
        "the session the direct reservation released last is reused first"
    );
}

std::thread_local! {
    /// What a `ReentrantWaker` runs, synchronously, when woken on this thread.
    static ON_WAKE: RefCell<Option<Box<dyn Fn()>>> = const { RefCell::new(None) };
}

/// A waker that runs this thread's `ON_WAKE` inside the notification that
/// wakes it.
struct ReentrantWaker;

impl Wake for ReentrantWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        ON_WAKE.with(|on_wake| {
            if let Some(on_wake) = on_wake.borrow().as_ref() {
                on_wake();
            }
        });
    }
}

/// As `ClientSession`'s draining flag, the admission the pool closes on the
/// sessions one idle-retirement pass retires refuses a reservation that a
/// waiter's waker takes while the pass drains the first of them.
#[test]
fn a_waker_cannot_reserve_a_session_retired_in_the_same_pass() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let refusals = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            PoolParams {
                connect_timeout: TIMEOUT,
                ..idle_params(
                    2,
                    SessionReuseOrder::LeastRecentlyUsed,
                    IDLE,
                    IdleSweep::AtExpiry,
                )
            },
        );
        let first = acquire(&pool).await;
        let second = acquire(&pool).await;
        let sessions = [first.session.clone(), second.session.clone()];
        // Each release wakes at most two waiters; the rest wait into the pass.
        let waker = Waker::from(Arc::new(ReentrantWaker));
        let mut waiting = Context::from_waker(&waker);
        let mut waiters: Vec<_> = (0..6)
            .map(|_| Box::pin(pool.get_session_with_reservation(CancellationToken::never())))
            .collect();
        for waiter in &mut waiters {
            assert!(
                waiter.as_mut().poll(&mut waiting).is_pending(),
                "both sessions are reserved"
            );
        }
        let attempts: Rc<RefCell<Vec<Result<OwnedClientReservation, HttpErrorCode>>>> =
            Rc::default();
        let reentry = (sessions.clone(), Rc::clone(&attempts));
        ON_WAKE.with(|on_wake| {
            on_wake.replace(Some(Box::new(move || {
                let (sessions, attempts) = &reentry;
                if attempts.borrow().is_empty()
                    && sessions.iter().all(OwnedClientSession::is_draining)
                {
                    let tried: Vec<_> = sessions
                        .iter()
                        .map(|session| session.reserve_request().map_err(|error| error.code()))
                        .collect();
                    attempts.borrow_mut().extend(tried);
                }
            })))
        });
        assert_eq!(exchange(first.reservation, "/").await, b"ok");
        settle(&sessions[0]).await;
        assert_eq!(exchange(second.reservation, "/").await, b"ok");
        settle(&sessions[1]).await;
        // Both expire while the worker is blocked, so one pass retires both.
        std::thread::sleep(IDLE * 2);
        until(|| sessions.iter().all(OwnedClientSession::is_draining)).await;
        ON_WAKE.with(|on_wake| on_wake.replace(None));
        drop(waiters);
        let refusals: Vec<Option<HttpErrorCode>> = attempts
            .take()
            .into_iter()
            .map(|attempt| attempt.err())
            .collect();
        refusals
    });
    assert_eq!(
        refusals,
        vec![Some(HttpErrorCode::DROPPED); 2],
        "the waker, run while the pass drains one of its sessions, reserves neither"
    );
}

/// The idle timeout, and so the sweep period, of the differential schedules.
const SWEEP_IDLE: Duration = Duration::from_millis(200);

/// hyper-util 0.1.20's legacy pool for one origin of HTTP/1.1 connections with
/// no waiters, as `pool_idle_timeout`'s differential tests model it: `put`
/// pushes a released connection onto the end of the idle list and, on the
/// first push ever, starts sweeps every `max(timeout, 90 ms)`, each evicting
/// every entry idle for longer than the timeout; a checkout pops from the end,
/// dropping expired entries until one is not, and dials when none is left.
struct HyperUtilPool {
    timeout: Duration,
    idle: Vec<(usize, Instant)>,
    next_sweep: Option<Instant>,
    dialed: usize,
    closed: Vec<usize>,
}

impl HyperUtilPool {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            idle: Vec::new(),
            next_sweep: None,
            dialed: 0,
            closed: Vec::new(),
        }
    }

    fn period(&self) -> Duration {
        self.timeout.max(Duration::from_millis(90))
    }

    /// Runs the sweeps due by `now`.
    fn advance(&mut self, now: Instant) {
        while let Some(deadline) = self.next_sweep.filter(|deadline| *deadline <= now) {
            let swept_at = deadline + Duration::from_nanos(1);
            let timeout = self.timeout;
            let closed = &mut self.closed;
            self.idle.retain(|&(connection, idle_at)| {
                let keep = swept_at.saturating_duration_since(idle_at) <= timeout;
                if !keep {
                    closed.push(connection);
                }
                keep
            });
            self.next_sweep = Some(deadline + self.period());
        }
    }

    /// The connection a request checking out at `now` takes.
    fn checkout(&mut self, now: Instant) -> usize {
        self.advance(now);
        while let Some((connection, idle_at)) = self.idle.pop() {
            if now.saturating_duration_since(idle_at) > self.timeout {
                self.closed.push(connection);
                continue;
            }
            return connection;
        }
        self.dialed += 1;
        self.dialed - 1
    }

    fn release(&mut self, connection: usize, now: Instant) {
        self.advance(now);
        self.idle.push((connection, now));
        if self.next_sweep.is_none() {
            self.next_sweep = Some(now + self.period());
        }
    }

    fn open(&mut self, now: Instant) -> Vec<usize> {
        self.advance(now);
        (0..self.dialed)
            .filter(|connection| !self.closed.contains(connection))
            .collect()
    }
}

/// One step of a differential schedule, `at` milliseconds after `Start`.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// Starts the clock: the steps before it run back to back.
    Start,
    /// Acquires a connection and holds its reservation in `slot`.
    Hold { at: u64, slot: usize },
    /// Sends one exchange on the reservation held in `slot`, then releases it.
    Release { at: u64, slot: usize },
    /// Acquires a connection, sends one exchange and releases it.
    Exchange { at: u64 },
    /// The connections still open, numbered in the order they were dialed.
    Open { at: u64, open: &'static [usize] },
}

impl Step {
    fn at(self) -> Duration {
        let (Self::Hold { at, .. }
        | Self::Release { at, .. }
        | Self::Exchange { at }
        | Self::Open { at, .. }) = self
        else {
            return Duration::ZERO;
        };
        Duration::from_millis(at)
    }
}

/// Runs `steps` on `HyperUtilPool` alone, each at its offset from `Start` (and
/// any before it at the start), every exchange taking no time.
fn assert_model_schedule(steps: &[Step]) {
    let base = Instant::now();
    let mut model = HyperUtilPool::new(SWEEP_IDLE);
    let mut held = [None; 2];
    for &step in steps {
        let now = base + step.at();
        match step {
            Step::Start => {}
            Step::Hold { slot, .. } => held[slot] = Some(model.checkout(now)),
            Step::Release { slot, .. } => {
                model.release(held[slot].take().expect("a held connection"), now);
            }
            Step::Exchange { .. } => {
                let connection = model.checkout(now);
                model.release(connection, now);
            }
            Step::Open { open, .. } => assert_eq!(model.open(now), open, "{step:?}"),
        }
    }
}

/// Acquires from `pool`, checks that it took the connection `model` takes, and
/// returns that connection's number with the reservation.
async fn take_as_hyper_util(
    pool: &OwnedHttpSessionPool,
    model: &mut HyperUtilPool,
    connections: &mut Vec<OwnedClientSession>,
    step: Step,
) -> (usize, OwnedClientReservation) {
    let at = Instant::now();
    let OwnedGetSessionResult {
        reservation,
        session,
    } = acquire(pool).await;
    let expected = model.checkout(at);
    let taken = connections
        .iter()
        .position(|known| local(known) == local(&session))
        .unwrap_or_else(|| {
            connections.push(session);
            connections.len() - 1
        });
    assert_eq!(taken, expected, "{step:?}: the connection taken");
    (taken, reservation)
}

/// Runs `steps` on a `MostRecentlyUsed`, `IdleSweep::Interval` owned pool and on
/// `HyperUtilPool` with the same idle timeout, comparing their open connections
/// at each `Step::Open`.
fn assert_sweeps_as_hyper_util(steps: &'static [Step]) {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                8,
                SessionReuseOrder::MostRecentlyUsed,
                SWEEP_IDLE,
                IdleSweep::Interval,
            ),
        );
        let mut model = HyperUtilPool::new(SWEEP_IDLE);
        let mut connections = Vec::new();
        let mut held: Vec<Option<(usize, OwnedClientReservation)>> = Vec::new();
        let mut start = None;
        for (index, &step) in steps.iter().enumerate() {
            if let Some(start) = start {
                sleep_until(start + step.at()).await;
            }
            let released = match step {
                Step::Start => {
                    start = Some(Instant::now());
                    None
                }
                Step::Hold { slot, .. } => {
                    let taken = take_as_hyper_util(&pool, &mut model, &mut connections, step).await;
                    held.resize_with(held.len().max(slot + 1), || None);
                    held[slot] = Some(taken);
                    None
                }
                Step::Release { slot, .. } => held[slot].take(),
                Step::Exchange { .. } => {
                    Some(take_as_hyper_util(&pool, &mut model, &mut connections, step).await)
                }
                Step::Open { .. } => {
                    let expected = model.open(Instant::now());
                    if index + 1 == steps.len() && expected.is_empty() {
                        tokio::time::timeout(
                            Duration::from_secs(5),
                            until(|| {
                                connections
                                    .iter()
                                    .all(|session| session.is_draining() || session.is_finished())
                            }),
                        )
                        .await
                        .expect("the final sweep retires every expired session");
                    }
                    let pooled: Vec<usize> = connections
                        .iter()
                        .enumerate()
                        .filter(|(_, session)| !session.is_draining() && !session.is_finished())
                        .map(|(connection, _)| connection)
                        .collect();
                    assert_eq!(
                        pooled, expected,
                        "{step:?}: the owned pool matches hyper-util 0.1.20"
                    );
                    None
                }
            };
            if let Some((connection, reservation)) = released {
                assert_eq!(exchange(reservation, "/").await, b"ok");
                settle(&connections[connection]).await;
                model.release(connection, Instant::now());
            }
        }
    });
}

/// The sweep timer runs from the first release, not from a dial, and keeps its
/// period while no connection is idle.
const FIRST_RELEASE_STARTS_SWEEPS: &[Step] = &[
    Step::Hold { at: 0, slot: 0 },
    Step::Start,
    Step::Release { at: 100, slot: 0 },
    Step::Open {
        at: 250,
        open: &[0],
    },
    Step::Open { at: 350, open: &[] },
    Step::Exchange { at: 400 },
    Step::Open {
        at: 650,
        open: &[1],
    },
    Step::Open { at: 750, open: &[] },
];

/// A connection that becomes idle after a sweep that found every connection
/// busy is swept on the old period.
const BUSY_SWEEP_KEEPS_THE_PERIOD: &[Step] = &[
    Step::Hold { at: 0, slot: 0 },
    Step::Start,
    Step::Release { at: 0, slot: 0 },
    Step::Hold { at: 0, slot: 0 },
    Step::Release { at: 250, slot: 0 },
    Step::Open {
        at: 300,
        open: &[0],
    },
    Step::Open {
        at: 500,
        open: &[0],
    },
    Step::Open { at: 650, open: &[] },
];

/// A checkout takes the connection released last while it is not expired and
/// leaves older expired connections to the next sweep; when the connection
/// released last has expired, it drops every idle connection and dials.
const CHECKOUT_LEAVES_EXPIRED_TO_THE_SWEEP: &[Step] = &[
    Step::Hold { at: 0, slot: 0 },
    Step::Hold { at: 0, slot: 1 },
    Step::Start,
    Step::Release { at: 0, slot: 0 },
    Step::Hold { at: 0, slot: 0 },
    Step::Release { at: 50, slot: 1 },
    Step::Release { at: 100, slot: 0 },
    Step::Open {
        at: 150,
        open: &[0, 1],
    },
    Step::Open {
        at: 225,
        open: &[0, 1],
    },
    Step::Exchange { at: 275 },
    Step::Open {
        at: 325,
        open: &[0, 1],
    },
    Step::Open {
        at: 450,
        open: &[0],
    },
    Step::Exchange { at: 525 },
    Step::Open {
        at: 550,
        open: &[2],
    },
    Step::Open {
        at: 750,
        open: &[2],
    },
    Step::Open { at: 850, open: &[] },
];

/// The model, on exact times, leaves open what each schedule expects.
#[test]
fn hyper_util_model_follows_the_differential_schedules() {
    for steps in [
        FIRST_RELEASE_STARTS_SWEEPS,
        BUSY_SWEEP_KEEPS_THE_PERIOD,
        CHECKOUT_LEAVES_EXPIRED_TO_THE_SWEEP,
    ] {
        assert_model_schedule(steps);
    }
}

#[test]
fn interval_sweeps_run_from_the_first_release_and_on_while_none_is_idle() {
    assert_sweeps_as_hyper_util(FIRST_RELEASE_STARTS_SWEEPS);
}

#[test]
fn interval_sweeps_keep_their_period_through_a_busy_sweep() {
    assert_sweeps_as_hyper_util(BUSY_SWEEP_KEEPS_THE_PERIOD);
}

#[test]
fn interval_checkout_leaves_older_expired_connections_to_the_sweep() {
    assert_sweeps_as_hyper_util(CHECKOUT_LEAVES_EXPIRED_TO_THE_SWEEP);
}

/// As hyper-util's idle interval, sweeps run at most every 90 ms, however short
/// the idle timeout.
#[test]
fn interval_sweep_runs_at_most_every_90_ms() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (waited, swept) = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                protocol,
                idle_params(
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    Duration::from_millis(20),
                    IdleSweep::Interval,
                ),
            );
            let session = one_exchange(&pool).await;
            let idle = Instant::now();
            sleep_until(idle + Duration::from_millis(50)).await;
            let waited = !session.is_draining();
            sleep_until(idle + Duration::from_millis(150)).await;
            (waited, session.is_draining())
        });
        assert_eq!(
            (waited, swept),
            (true, true),
            "{protocol:?}: past its idle timeout, the session waits for the first sweep, which \
             retires it"
        );
    }
}

#[test]
fn drain_joins_idle_retirement() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (drained, retired) = run_on_worker(move |context| async move {
            // Retirement waits without a deadline in one pool (on an interval, not yet
            // started), with one in the other.
            let pool = || {
                idle_pool(
                    &context,
                    address,
                    CodecProtocol::Http11,
                    idle_params(1, SessionReuseOrder::LeastRecentlyUsed, IDLE, idle_sweep),
                )
            };
            let empty = pool();
            let idle = pool();
            let session = one_exchange(&idle).await;
            let mut drained = Vec::new();
            for pool in [empty, idle] {
                drained.push(matches!(
                    tokio::time::timeout(Duration::from_secs(1), pool.drain_and_join()).await,
                    Ok(Ok(()))
                ));
            }
            (drained, session.is_draining())
        });
        assert_eq!(
            (drained, retired),
            (vec![true, true], true),
            "{idle_sweep:?}: both drains join, and the drain retires the idle session"
        );
    }
}

/// Counts the heap allocations a thread makes while its counter is enabled; the
/// client crate's test build uses it as its global allocator. As
/// `server/tests/allocation_count.rs` records, Buck's fbcode test builds link
/// every other crate as a shared library whose allocations bypass it, so there
/// it counts the client crate's own allocations only: the pools' and the owned
/// session's. A uniplex control checks that it sees them.
struct CountingAllocator;

std::thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation() {
    if COUNTING.with(Cell::get) {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
    }
}

// SAFETY: every method passes its arguments unchanged to the same `System`
// method, so `System` upholds the `GlobalAlloc` contract. Counting touches only
// const-initialized thread-local cells, which neither allocate nor register
// destructors.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc`'s layout precondition.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: this call's caller upheld `alloc_zeroed`'s layout precondition.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`, and this call's caller upheld `realloc`'s size precondition.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from this allocator, which is `System`, with
        // `layout`.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Creates an acquisition and polls it once, counting this thread's
/// allocations from the creation through that poll.
fn first_poll_allocations<Acquisition: Future>(
    create: impl FnOnce() -> Acquisition,
) -> (Poll<Acquisition::Output>, usize) {
    let mut context = Context::from_waker(Waker::noop());
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    let mut acquisition = pin!(create());
    let polled = acquisition.as_mut().poll(&mut context);
    COUNTING.with(|counting| counting.set(false));
    (polled, ALLOCATIONS.with(Cell::get))
}

/// The fast acquisition path (`L-R1`): an acquisition that finds an idle
/// session is served on its first poll without a boxed future.
#[test]
fn an_acquisition_that_finds_an_idle_session_allocates_nothing() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (owned, uniplex) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 4);
            let idle = one_exchange(&pool).await;
            let (polled, owned) = first_poll_allocations(|| {
                pool.get_session_with_reservation(CancellationToken::never())
            });
            let Poll::Ready(Ok(acquired)) = polled else {
                panic!("{protocol:?}: the idle session is acquired on the first poll");
            };
            assert_eq!(local(&acquired.session), local(&idle));
            drop(acquired);

            let uniplex_pool = HttpSessionPool::new(
                context.clone(),
                socket_address(address),
                params(4),
                plaintext(protocol),
                untimed(),
            );
            uniplex_one_exchange(&uniplex_pool).await;
            let (polled, uniplex) = first_poll_allocations(|| {
                uniplex_pool.get_session_with_reservation(CancellationToken::never())
            });
            assert!(
                matches!(polled, Poll::Ready(Ok(_))),
                "{protocol:?}: the uniplex pool's idle session is acquired on the first poll"
            );
            (owned, uniplex)
        });
        assert!(
            uniplex >= 1,
            "{protocol:?}: the counter sees the uniplex pool's boxed acquisition ({uniplex})"
        );
        assert_eq!(
            owned, 0,
            "{protocol:?}: the owned pool's hit allocates nothing (the uniplex pool's: {uniplex})"
        );
    }
}

/// The fast acquisition path checks its worker's admission, an uncontended
/// lock, once per acquisition instead of once per session it examines and
/// again at the reservation.
#[test]
fn an_acquisition_checks_its_worker_s_admission_once() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let checks = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                4,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::ZERO,
                IdleSweep::AtExpiry,
            ),
        );
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(acquire(&pool).await);
        }
        for acquired in held {
            assert_eq!(exchange(acquired.reservation, "/").await, b"ok");
            settle(&acquired.session).await;
        }
        assert_eq!(pool.session_count(), 4);
        let before = worker_admission_checks_for_test();
        let acquired = acquire(&pool).await;
        let checks = worker_admission_checks_for_test() - before;
        drop(acquired);
        checks
    });
    assert_eq!(
        checks, 1,
        "one worker admission check for four idle sessions"
    );
}

/// The fast acquisition path records its reservation on the selected entry
/// instead of a pass over every pooled session.
#[test]
fn an_acquisition_that_finds_an_idle_session_makes_no_pool_wide_pass() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let passes = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 4);
            one_exchange(&pool).await;
            let before = pool.session_event_passes_for_test();
            let acquired = acquire(&pool).await;
            let passes = pool.session_event_passes_for_test() - before;
            drop(acquired);
            passes
        });
        assert_eq!(passes, 0, "{protocol:?}");
    }
}

#[test]
fn a_cancelled_acquisition_fails_without_reserving() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (error, idle) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 4);
            let session = one_exchange(&pool).await;
            let cancellation = CancellationSource::new();
            cancellation.request_cancellation();
            let error = pool
                .get_session_with_reservation(cancellation.token())
                .await
                .err();
            (error, session.is_idle())
        });
        assert!(
            matches!(error, Some(PoolError::Cancelled)),
            "{protocol:?}: {error:?}"
        );
        assert!(
            idle,
            "{protocol:?}: the cancelled acquisition reserved nothing"
        );
    }
}

/// As `HttpSessionPool`'s, zero connections disables reuse: the pool drains a
/// session once its exchange releases it, from within the driver's own
/// callback.
#[test]
fn max_connections_zero_disables_reuse() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (closed, replaced, body) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 0);
            let first = acquire(&pool).await;
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            let closed = first.session.wait_closed().await.is_ok();
            let second = acquire(&pool).await;
            let replaced = local(&first.session) != local(&second.session);
            (closed, replaced, exchange(second.reservation, "/").await)
        });
        assert_eq!(
            (closed, replaced, body, origin.accepted()),
            (true, true, b"ok".to_vec(), 2),
            "{protocol:?}: the released session closes, and the next exchange dials anew"
        );
    }
}

/// Polls `future` once, with the current task's waker.
async fn poll_once<Work: Future>(mut future: Pin<&mut Work>) -> Poll<Work::Output> {
    std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

/// B2's pool parameters as the router sets them (`MostRecentlyUsed`, a 90 s idle timeout
/// swept on an interval), with `max_connections`.
fn router_params(max_connections: u32) -> PoolParams {
    idle_params(
        max_connections,
        SessionReuseOrder::MostRecentlyUsed,
        Duration::from_secs(90),
        IdleSweep::Interval,
    )
}

/// On HTTP/1.1 a stream's next request arrives before the owned driver has returned the slot
/// of the response the stream just read. The acquisition waits for that slot instead of
/// starting a connection attempt that would never dial: back-to-back exchanges on one
/// session start no attempt and spawn no pool task, and each is served on that session.
#[test]
fn back_to_back_http1_acquisitions_wait_for_the_returning_session_without_an_attempt() {
    for (name, params) in [("default", params(4)), ("router", router_params(4))] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (attempts, spawns, same, sessions) = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            let first = acquire(&pool).await;
            let session = local(&first.session);
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            let attempts = pool.connection_attempts_for_test();
            let spawns = pool.pool_task_spawns_for_test();
            let mut same = Vec::new();
            for _ in 0..3 {
                // No yield since the previous response was read: its slot is returning.
                let next = acquire(&pool).await;
                same.push(local(&next.session) == session);
                assert_eq!(exchange(next.reservation, "/").await, b"ok");
            }
            (
                pool.connection_attempts_for_test() - attempts,
                pool.pool_task_spawns_for_test() - spawns,
                same,
                pool.session_count(),
            )
        });
        assert_eq!(
            (attempts, spawns, same, sessions, origin.accepted()),
            (0, 0, vec![true; 3], 1, 1),
            "{name}: (attempts, pool task spawns, served on the first session, sessions, \
             connections)"
        );
    }
}

/// An acquisition that waits for a returning session takes it when the driver returns it,
/// ahead of an earlier waiter whose own dial is still pending, and starts no attempt itself.
#[test]
fn a_waiter_for_a_returning_session_takes_it_ahead_of_an_earlier_waiter_s_dial() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (took_returning, earlier_dialed, attempts) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 4);
        let one = acquire(&pool).await;
        let two = acquire(&pool).await;
        let first = submit(one.reservation, "/").await;
        let second = submit(two.reservation, "/").await;
        // Both responses are complete; neither slot returns while its response is unread.
        until(|| first.request.capacity_released() && second.request.capacity_released()).await;
        let attempts = pool.connection_attempts_for_test();
        let mut earlier = pin!(pool.get_session_with_reservation(CancellationToken::never()));
        assert!(
            poll_once(earlier.as_mut()).await.is_pending(),
            "no slot is free"
        );
        // Read from the buffer, with no yield: the second session's slot is returning.
        assert_eq!(read_body(second).await, b"ok");
        let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
        assert!(
            poll_once(next.as_mut()).await.is_pending(),
            "the slot is not back yet"
        );
        let next = next.await.expect("the returned session serves");
        let took_returning = local(&next.session) == local(&two.session);
        let earlier = earlier.await.expect("the earlier waiter's dial serves");
        let earlier_dialed =
            ![local(&one.session), local(&two.session)].contains(&local(&earlier.session));
        assert_eq!(exchange(next.reservation, "/").await, b"ok");
        assert_eq!(exchange(earlier.reservation, "/").await, b"ok");
        assert_eq!(read_body(first).await, b"ok");
        (
            took_returning,
            earlier_dialed,
            pool.connection_attempts_for_test() - attempts,
        )
    });
    assert_eq!(
        (took_returning, earlier_dialed, attempts, origin.accepted()),
        (true, true, 1, 3),
        "(the waiter took the returning session, the earlier waiter its own dial, attempts, \
         connections)"
    );
}

/// An acquisition dropped or cancelled while it waits for a returning session gives up its
/// claim on it: the next acquisition waits for that session instead of dialing.
#[test]
fn a_cancelled_acquisition_releases_the_returning_session_it_waited_for() {
    // (whether the acquisition is dropped, or cancelled through its token)
    for dropped in [true, false] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (cancelled, attempts, same) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            let first = acquire(&pool).await;
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            let attempts = pool.connection_attempts_for_test();
            let cancellation = CancellationSource::new();
            let mut waiting = Box::pin(pool.get_session_with_reservation(cancellation.token()));
            assert!(poll_once(waiting.as_mut()).await.is_pending());
            let cancelled = if dropped {
                drop(waiting);
                true
            } else {
                cancellation.request_cancellation();
                matches!(
                    poll_once(waiting.as_mut()).await,
                    Poll::Ready(Err(PoolError::Cancelled))
                )
            };
            let next = acquire(&pool).await;
            let same = local(&next.session) == local(&first.session);
            assert_eq!(exchange(next.reservation, "/").await, b"ok");
            (
                cancelled,
                pool.connection_attempts_for_test() - attempts,
                same,
            )
        });
        assert_eq!(
            (cancelled, attempts, same, origin.accepted()),
            (true, 0, true, 1),
            "dropped: {dropped}: (cancelled, attempts, served on the first session, \
             connections)"
        );
    }
}

/// An HTTP/1.1 origin that accepts one connection and then closes its listener, so that a
/// later dial to its address is refused, and answers `ok` to every request on that
/// connection until the client closes it.
struct OneConnectionOrigin {
    address: SocketAddr,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl OneConnectionOrigin {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
        let address = listener.local_addr().expect("origin address");
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("origin accepts");
            drop(listener);
            let mut pending = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let read = match std::io::Read::read(&mut stream, &mut buffer) {
                    Ok(0) | Err(_) => return,
                    Ok(read) => read,
                };
                pending.extend_from_slice(&buffer[..read]);
                while let Some(end) = pending.windows(4).position(|part| part == b"\r\n\r\n") {
                    pending.drain(..end + 4);
                    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                    if std::io::Write::write_all(&mut stream, response).is_err() {
                        return;
                    }
                }
            }
        });
        Self {
            address,
            thread: Some(thread),
        }
    }
}

impl Drop for OneConnectionOrigin {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A session that fails while an acquisition waits for its returning slot leaves that
/// acquisition to dial a replacement, or to fail with the dial's error, as an acquisition that
/// had started its own attempt did.
#[test]
fn an_acquisition_waiting_for_a_returning_session_that_fails_dials_or_errors_as_before() {
    // (whether the origin accepts a replacement connection)
    for accepting in [true, false] {
        let hyper_origin = accepting.then(|| Origin::start(CodecProtocol::Http11));
        let refusing_origin = (!accepting).then(OneConnectionOrigin::start);
        let address = hyper_origin.as_ref().map_or_else(
            || refusing_origin.as_ref().expect("one origin").address,
            |origin| origin.address,
        );
        let (pending, outcome) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            let first = acquire(&pool).await;
            let session = first.session.clone();
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            let first_poll = poll_once(next.as_mut()).await;
            let pending = first_poll.is_pending();
            // The session fails before its driver returns the slot.
            session.cancel();
            let acquired = match first_poll {
                Poll::Ready(acquired) => acquired,
                Poll::Pending => next.await,
            };
            let outcome = match acquired {
                Ok(acquired) => {
                    let replaced = local(&acquired.session) != local(&session);
                    assert_eq!(exchange(acquired.reservation, "/").await, b"ok");
                    format!("served, replaced: {replaced}")
                }
                Err(PoolError::ConnectFailed { .. }) => "connect failed".to_owned(),
                Err(error) => format!("{error:?}"),
            };
            (pending, outcome)
        });
        let expected = if accepting {
            "served, replaced: true"
        } else {
            "connect failed"
        };
        assert_eq!(
            (pending, outcome.as_str()),
            (true, expected),
            "accepting: {accepting}"
        );
        if let Some(origin) = hyper_origin {
            assert_eq!(origin.accepted(), 2, "the replacement was dialed");
        }
    }
}

/// Pool limits bound an acquisition that meets a returning session as before: at
/// `max_connections` it waits for the slot like any waiter, and `max_waiters` refuses it
/// there; and with a 1 ns idle timeout (S2.7) the returned session is idle and expired when
/// the acquisition takes its turn, so it is retired and the acquisition dials.
#[test]
fn pool_limits_and_idle_retirement_bound_a_back_to_back_acquisition_as_before() {
    let cases = [
        (
            "at max_connections",
            params(1),
            "served on the first session",
        ),
        (
            "no waiter allowed at max_connections",
            PoolParams {
                max_waiters: 0,
                ..params(1)
            },
            "MaxWaiters",
        ),
        (
            "1 ns idle timeout",
            idle_params(
                4,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::from_nanos(1),
                IdleSweep::Interval,
            ),
            "served on another session",
        ),
    ];
    for (name, params, expected) in cases {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let outcome = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            let first = acquire(&pool).await;
            let session = local(&first.session);
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            match pool
                .get_session_with_reservation(CancellationToken::never())
                .await
            {
                Ok(next) => {
                    let same = local(&next.session) == session;
                    assert_eq!(exchange(next.reservation, "/").await, b"ok");
                    if same {
                        "served on the first session".to_owned()
                    } else {
                        "served on another session".to_owned()
                    }
                }
                Err(error) => format!("{error:?}"),
            }
        });
        assert_eq!(outcome, expected, "{name}");
    }
}

/// `DIV-043`, as a router's late teardown meets it: a `HostedWorker` whose
/// teardown deadline has passed destroys an owned pool, its session midway
/// through a streamed response, before the session's native I/O cleanup and
/// the pool's task join finished. That destruction is an abandonment: the
/// worker reports a clean overrun instead of a panic.
#[test]
fn a_forced_worker_teardown_abandons_an_owned_pool_mid_response() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let worker = HostedWorker::spawn(HostedWorkerOptions {
            command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
            thread_name: "rox-owned-pool-forced".to_owned(),
            on_owner_thread_stop: None,
        })
        .expect("worker starts");
        let (streaming_tx, streaming_rx) = std::sync::mpsc::channel();
        worker
            .handle()
            .spawn_local(move |context| async move {
                let pool = pool(&context, address, protocol, 2);
                let acquired = acquire(&pool).await;
                let (head, body) = request("/slow").await;
                let response = acquired
                    .reservation
                    .submit(head, body, CancellationToken::never())
                    .await
                    .expect("request is committed");
                let mut source = response.source.into_source_owner().into_source_holder();
                let head = source.read_header_event().await.expect("response head");
                let first = source.read_body_event(u32::MAX).await.expect("first chunk");
                let _cleanup = context
                    .register_cleanup_task(async move {
                        std::future::pending::<()>().await;
                        drop((pool, acquired.session, source));
                    })
                    .expect("streaming pool cleanup registered");
                streaming_tx
                    .send((head.headers().response_status(), first.eom()))
                    .expect("test observes the response");
                std::future::pending::<()>().await;
            })
            .expect("command admitted");
        assert_eq!(
            streaming_rx.recv_timeout(TIMEOUT),
            Ok((Some(200), false)),
            "{protocol:?}: the response is streaming"
        );

        // The passed deadline can wake the supervisor, which then begins the
        // shutdown itself.
        worker.shutdown_by(Instant::now());
        let completion = match worker.join_by(Instant::now() + TIMEOUT) {
            HostedWorkerJoin::Joined(completion) => completion,
            HostedWorkerJoin::Detached => panic!("{protocol:?}: the teardown ends in time"),
        };
        assert_eq!(
            completion,
            Err(ManagedWorkerError::TeardownDeadlineExceeded),
            "{protocol:?}: the forced destruction of the unjoined pool and session is a clean \
             overrun"
        );
    }
}
