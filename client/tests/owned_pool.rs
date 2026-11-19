/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The experimental owned session pool: `HttpSessionPool`'s acquisition,
//! waiter, admission and idle-retirement behavior on owned client sessions,
//! over loopback TCP against a hyper origin (HTTP/1.1, and HTTP/2 with prior
//! knowledge), on a `RuntimeOwner` worker that must quiesce without forcing.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::cell::RefCell;
use std::convert::Infallible;
use std::future::Future;
use std::future::poll_fn;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
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
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
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
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerJoin;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorkerError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::ServerConfig;
use roxygen_session::experimental::OwnedRequestProgressExt;
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::rate_limit::RateLimitConfig;
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
use crate::experimental::owned::OwnedPoolStats;
use crate::experimental::owned::OwnedSessionSelection;
use crate::experimental::owned::worker_admission_checks_for_test;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSweep;
use crate::pool::PoolError;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;
use crate::pool::take_batch_passes_for_test;
use crate::pool::take_waits_for_wakeup_for_test;

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

/// `/` responds `ok`, `/close` responds `ok` and closes its HTTP/1.1
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
    // A connection that the client closes or resets ends with an error, which the tests ignore.
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

/// A socket bound to `address` that never listens. While it lives, a dial to its port is
/// refused and another test's `127.0.0.1:0` listener cannot take the port. `reuse_address` lets
/// it take over a port that an accepted connection still holds, but also lets another
/// address-reusing socket bind that port by number.
fn reserve(address: SocketAddr, reuse_address: bool) -> tokio::net::TcpSocket {
    let socket = tokio::net::TcpSocket::new_v4().expect("socket");
    socket.set_reuseaddr(reuse_address).expect("address reuse");
    socket.bind(address).expect("the port is reserved");
    socket
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

/// Waits until the session has retired its finished exchange.
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

/// One-connection pool parameters with `connect_timeout` and `acquisition_timeout`.
fn timed_params(connect_timeout: Duration, acquisition_timeout: Option<Duration>) -> PoolParams {
    PoolParams {
        connect_timeout,
        acquisition_timeout,
        ..params(1)
    }
}

/// How an acquisition from a one-connection pool with `params` ends, and after how long,
/// while an earlier acquisition holds the session's only slot. The pool is an owned pool, or an
/// `HttpSessionPool` when `native` is set.
fn held_slot_acquisition(
    protocol: CodecProtocol,
    params: PoolParams,
    native: bool,
) -> (Option<PoolError>, Duration) {
    let origin = Origin::start(protocol);
    let address = origin.address;
    run_on_worker(move |context| async move {
        if native {
            let pool = HttpSessionPool::new(
                context,
                socket_address(address),
                params,
                plaintext(protocol),
                one_stream(protocol),
            );
            let _held = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .expect("acquisition succeeds");
            let started = Instant::now();
            let error = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .err();
            (error, started.elapsed())
        } else {
            let pool = pool_with(&context, address, protocol, params, one_stream(protocol));
            let _held = acquire(&pool).await;
            let started = Instant::now();
            let error = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .err();
            (error, started.elapsed())
        }
    })
}

#[test]
fn a_waiter_times_out_at_the_acquisition_timeout() {
    let params = timed_params(Duration::from_secs(5), Some(Duration::from_millis(200)));
    for (protocol, native) in [
        (CodecProtocol::Http11, false),
        (CodecProtocol::Http2, false),
        (CodecProtocol::Http11, true),
    ] {
        let (error, waited) = held_slot_acquisition(protocol, params, native);
        assert!(
            matches!(error, Some(PoolError::Timeout)),
            "{protocol:?}, native {native}: {error:?}"
        );
        assert!(
            (Duration::from_millis(200)..Duration::from_secs(2)).contains(&waited),
            "{protocol:?}, native {native}: waited {waited:?}, not the 200 ms acquisition timeout"
        );
    }
}

#[test]
fn without_an_acquisition_timeout_a_waiter_times_out_at_the_connect_timeout() {
    assert_eq!(PoolParams::default().acquisition_timeout, None);
    let params = timed_params(Duration::from_millis(200), None);
    for protocol in PROTOCOLS {
        let (error, waited) = held_slot_acquisition(protocol, params, false);
        assert!(
            matches!(error, Some(PoolError::Timeout)),
            "{protocol:?}: {error:?}"
        );
        assert!(
            (Duration::from_millis(200)..Duration::from_secs(2)).contains(&waited),
            "{protocol:?}: waited {waited:?}, not the 200 ms connect timeout"
        );
    }
}

/// The deadline check applies `acquisition_timeout` too, not only the waits. A step that cannot
/// take a session 300 ms after the acquisition's first such step times out under a 200 ms or a
/// zero acquisition timeout with a 5 s connect timeout, but not under a 5 s acquisition timeout
/// with a 200 ms connect timeout. A zero timeout fails the acquisition that would dial the
/// session to hold, so its step runs on the empty pool.
#[test]
fn a_step_past_the_acquisition_timeout_times_out() {
    let short = Duration::from_millis(200);
    let long = Duration::from_secs(5);
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    let steps = run_on_worker(move |context| async move {
        let mut steps = Vec::new();
        for (connect_timeout, acquisition_timeout) in [
            (long, Some(short)),
            (long, Some(Duration::ZERO)),
            (short, Some(long)),
        ] {
            let pool = pool_with(
                &context,
                address,
                CodecProtocol::Http2,
                timed_params(connect_timeout, acquisition_timeout),
                one_stream(CodecProtocol::Http2),
            );
            let _held = if acquisition_timeout == Some(Duration::ZERO) {
                None
            } else {
                Some(acquire(&pool).await)
            };
            steps.push(pool.acquisition_step_for_test(Instant::now() - Duration::from_millis(300)));
        }
        steps
    });
    assert!(
        matches!(
            &steps[..],
            [Err(PoolError::Timeout), Err(PoolError::Timeout), Ok(())]
        ),
        "{steps:?}"
    );
}

#[test]
fn an_http2_waiter_outlives_the_connect_timeout_until_a_stream_frees() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    let (error, same, body, waited) = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http2,
            timed_params(Duration::from_millis(100), Some(Duration::from_secs(5))),
            one_stream(CodecProtocol::Http2),
        );
        let first = acquire(&pool).await;
        let started = Instant::now();
        let second = pool.get_session_with_reservation(CancellationToken::never());
        tokio::pin!(second);
        // The only stream stays reserved for three connect timeouts.
        let early = tokio::select! {
            result = &mut second => Some(result),
            () = tokio::time::sleep(Duration::from_millis(300)) => None,
        };
        let result = match early {
            Some(result) => result,
            None => {
                drop(first.reservation);
                second.await
            }
        };
        let waited = started.elapsed();
        match result {
            Ok(second) => (
                None,
                local(&first.session) == local(&second.session),
                exchange(second.reservation, "/").await,
                waited,
            ),
            Err(error) => (Some(error), false, Vec::new(), waited),
        }
    });
    assert!(
        error.is_none(),
        "the waiter failed after {waited:?}: {error:?}"
    );
    assert_eq!(
        (same, body, origin.accepted()),
        (true, b"ok".to_vec(), 1),
        "the waiter takes the freed stream on the one connection"
    );
    assert!(waited >= Duration::from_millis(300), "waited {waited:?}");
}

/// Linux drops the SYNs that reach a full accept queue: with a backlog of 0, one
/// connection left unaccepted fills it, and the pool's dial stalls.
#[cfg(target_os = "linux")]
#[test]
fn a_dial_stays_bounded_by_the_connect_timeout() {
    let short = Duration::from_millis(200);
    let long = Duration::from_secs(5);
    let outcomes = run_on_worker(move |context| async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("listener socket");
        socket
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .expect("listener binds");
        let listener = socket.listen(0).expect("listener listens");
        let address = listener.local_addr().expect("listener address");
        let _queued = tokio::net::TcpStream::connect(address)
            .await
            .expect("the first connection is queued");
        let mut outcomes = Vec::new();
        for (connect_timeout, acquisition_timeout) in [(short, long), (long, short)] {
            let pool = pool_with(
                &context,
                address,
                CodecProtocol::Http2,
                PoolParams {
                    max_connection_attempts: 1,
                    ..timed_params(connect_timeout, Some(acquisition_timeout))
                },
                untimed(),
            );
            let started = Instant::now();
            let error = pool
                .get_session_with_reservation(CancellationToken::never())
                .await
                .err();
            outcomes.push((error, started.elapsed()));
        }
        outcomes
    });
    let [(dial, dial_waited), (wait, wait_waited)] = &outcomes[..] else {
        panic!("two scenarios ran: {outcomes:?}");
    };
    assert!(
        matches!(dial, Some(PoolError::ConnectFailed { .. })),
        "the dial fails at the 200 ms connect timeout under a 5 s acquisition timeout: {dial:?}"
    );
    assert!(
        (short..Duration::from_secs(2)).contains(dial_waited),
        "waited {dial_waited:?}"
    );
    assert!(
        matches!(wait, Some(PoolError::Timeout)),
        "the wait for the stalled dial ends at the 200 ms acquisition timeout: {wait:?}"
    );
    assert!(
        (short..Duration::from_secs(2)).contains(wait_waited),
        "waited {wait_waited:?}"
    );
}

/// A hyper HTTP/2 origin that accepts one connection, then closes its listener, so that later
/// dials are refused, and serves that connection until the client closes it. While that
/// connection is open it holds the port, so another test's `127.0.0.1:0` listener cannot take it.
struct OneConnectionHttp2Origin {
    address: SocketAddr,
    /// 1 once the listener is closed.
    closed: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl OneConnectionHttp2Origin {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
        let address = listener.local_addr().expect("origin address");
        let closed = Arc::new(AtomicUsize::new(0));
        let thread = {
            let closed = Arc::clone(&closed);
            std::thread::spawn(move || {
                let (stream, _) = listener.accept().expect("origin accepts");
                drop(listener);
                closed.store(1, Ordering::SeqCst);
                stream
                    .set_nonblocking(true)
                    .expect("nonblocking connection");
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("origin runtime")
                    .block_on(async move {
                        let stream =
                            tokio::net::TcpStream::from_std(stream).expect("origin connection");
                        serve(stream, CodecProtocol::Http2).await;
                    });
            })
        };
        Self {
            address,
            closed,
            thread: Some(thread),
        }
    }
}

impl Drop for OneConnectionHttp2Origin {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A dial refused beside an HTTP/2 session whose only stream is reserved leaves the acquisition
/// that started it waiting, as `HttpSessionPool` does, and the session's freed stream then
/// serves it. Only an owned HTTP/1.1 pool fails such an acquisition immediately.
#[test]
fn an_http2_dial_refused_beside_a_full_session_waits_for_its_stream() {
    let origin = OneConnectionHttp2Origin::start();
    let address = origin.address;
    let closed = Arc::clone(&origin.closed);
    let observed = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http2,
            params(2),
            one_stream(CodecProtocol::Http2),
        );
        let first = acquire(&pool).await;
        until(|| closed.load(Ordering::SeqCst) == 1).await;
        let mut second = pin!(pool.get_session_with_reservation(CancellationToken::never()));
        assert!(
            poll_once(second.as_mut()).await.is_pending(),
            "the full session makes the acquisition dial"
        );
        until(|| pool.stats().connect_failures == 3).await;
        let refused = poll_once(second.as_mut()).await;
        let stats = pool.stats();
        drop(first.reservation);
        let outcome = match refused {
            Poll::Ready(Err(PoolError::ConnectFailed { .. })) => "connect failed".to_owned(),
            Poll::Ready(Err(error)) => format!("{error:?}"),
            Poll::Ready(Ok(_)) => "served before the stream was freed".to_owned(),
            Poll::Pending => {
                let second = second.await.expect("the freed stream serves the waiter");
                let session = if local(&first.session) == local(&second.session) {
                    "the first acquisition's session"
                } else {
                    "another session"
                };
                let body = exchange(second.reservation, "/").await;
                format!("{} on {session}", String::from_utf8_lossy(&body))
            }
        };
        (
            outcome,
            (
                stats.connect_attempts,
                stats.connect_failures,
                stats.waiters,
            ),
        )
    });
    assert_eq!(
        observed,
        (
            "ok on the first acquisition's session".to_owned(),
            (4, 3, 1)
        ),
        "(the acquisition's outcome, (dials, failed dials, waiters) after the refused dials)"
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

/// A reservation reports whether the pool dialed its connection for the acquisition that
/// returned it, as hyper-util distinguishes a new connection from a reused one. The first
/// acquisition, handed its own dial's session, reports `true`. An acquisition that took that
/// session idle, a reservation taken on the session's handle, and an acquisition that waited at
/// `max_connections` for a session dialed for an acquisition dropped before its dial completed
/// report `false`.
#[test]
fn a_reservation_reports_whether_its_connection_was_dialed_for_its_acquisition() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let reported = run_on_worker(move |context| async move {
            let pool = pool_with(&context, address, protocol, params(1), one_stream(protocol));
            let first = acquire(&pool).await;
            let dialed = first.reservation.dialed_for_acquisition();
            let session = first.session.clone();
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            settle(&session).await;
            let idle = acquire(&pool).await;
            let taken_idle = idle.reservation.dialed_for_acquisition();
            drop(idle);
            settle(&session).await;
            let direct = session
                .reserve_request()
                .expect("the idle session admits a reservation")
                .dialed_for_acquisition();

            let capped = pool_with(&context, address, protocol, params(1), one_stream(protocol));
            let mut dropped =
                Box::pin(capped.get_session_with_reservation(CancellationToken::never()));
            let mut waiting = pin!(capped.get_session_with_reservation(CancellationToken::never()));
            assert!(
                poll_once(dropped.as_mut()).await.is_pending(),
                "the empty pool dials"
            );
            assert!(
                poll_once(waiting.as_mut()).await.is_pending(),
                "the pending dial holds the pool's one connection"
            );
            drop(dropped);
            let other = waiting
                .await
                .expect("the waiter takes the dropped acquisition's session")
                .reservation
                .dialed_for_acquisition();
            (dialed, taken_idle, direct, other)
        });
        assert_eq!(
            (reported, origin.accepted()),
            ((true, false, false, false), 2),
            "{protocol:?}: ((handed its own dial's session, taken idle, taken on the handle, \
             dialed for a dropped acquisition), connections)"
        );
    }
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
/// The connections the origin accepts are not compared. In both pools the aged
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

/// Pool parameters with `max_connections` and idle retirement.
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

/// Whether every value in `items` is distinct.
fn all_distinct<T: PartialEq>(items: &[T]) -> bool {
    items
        .iter()
        .enumerate()
        .all(|(index, item)| !items[..index].contains(item))
}

/// How each of three sequential exchanges, 20 ms apart, ended (served, or its
/// acquisition's error), and whether each was served on a connection that an
/// earlier exchange did not use. The pool is an owned pool or an
/// `HttpSessionPool` with a 1 ns idle timeout and hyper-util's reuse order and
/// sweep, which models reqwest's zero `pool_idle_timeout` ("expire as soon as
/// idle").
///
/// The connections the origin accepts are not compared. In both pools the
/// session that the checkout retires wakes the acquisition's own waiter when
/// its drain closes its admission, and the waiter then starts a second dial
/// beside the first, as in `aged_session_scenario`. Whether the next exchange
/// takes that unused session or dials again depends on task timing.
fn one_nanosecond_idle_scenario(
    owned: bool,
    protocol: CodecProtocol,
    selection: OwnedSessionSelection,
) -> (String, bool) {
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
            pool.set_session_selection(selection)
                .expect("an open pool takes a selection");
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
/// checkout can retire it, however short the idle timeout. "Expire as soon as
/// idle" rules out reusing an idle session, not using a fresh one.
#[test]
fn a_freshly_dialed_session_serves_its_dialing_acquisition_under_a_1_ns_idle_timeout() {
    for protocol in PROTOCOLS {
        let served = || ("[Ok(()), Ok(()), Ok(())]".to_owned(), true);
        assert_eq!(
            [true, false].map(|owned| one_nanosecond_idle_scenario(
                owned,
                protocol,
                OwnedSessionSelection::FirstAvailable
            )),
            [served(), served()],
            "{protocol:?}: every exchange is served, each on a connection no earlier exchange \
             used, through an owned pool and an `HttpSessionPool`"
        );
    }
}

/// Least-loaded selection keeps that behavior. A session the pool dialed has no exchange in
/// flight, like an idle one, but counts as busy, and among equally loaded sessions a busy one
/// is taken first.
#[test]
fn a_freshly_dialed_session_serves_its_dialing_acquisition_under_least_loaded_selection() {
    for protocol in PROTOCOLS {
        assert_eq!(
            one_nanosecond_idle_scenario(true, protocol, OwnedSessionSelection::LeastLoaded),
            ("[Ok(()), Ok(()), Ok(())]".to_owned(), true),
            "{protocol:?}: every exchange is served, each on a connection no earlier exchange \
             used"
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
/// task waiting while the session is busy. The session's next idle period wakes
/// it.
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

/// Like `ClientSession`'s draining flag, the admission that the pool closes on
/// the sessions retired by one idle-retirement pass rejects a reservation that
/// a waiter's waker takes while the pass drains the first of them.
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

/// A waker that, once armed, begins its runtime's quiescence inside the
/// notification that wakes it.
struct QuiesceOnWake {
    runtime: RuntimeHandle,
    armed: AtomicBool,
    wakes: AtomicUsize,
}

impl Wake for QuiesceOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        if self.armed.swap(false, Ordering::SeqCst) {
            drop(self.runtime.quiesce(TIMEOUT));
        }
    }
}

/// Which drain in an acquisition's step wakes the waiter that begins the
/// worker's quiescence.
#[derive(Clone, Copy, Debug)]
enum QuiescingDrain {
    /// An aged session's drain, before the step reserves its selection.
    MaxAge,
    /// An idle-expired session's drain, after the step reserves its selection.
    IdleExpiry,
}

/// Reserves two HTTP/1.1 sessions with acquisitions waiting behind them,
/// releases both so that one acquisition still waits, and makes the first due
/// for retirement through `drain`. A new acquisition then selects the second
/// while the first's drain wakes the last waiter, whose waker begins the
/// worker's quiescence. Returns the new acquisition's error.
fn quiescing_drain_scenario(drain: QuiescingDrain) -> Result<(), Option<HttpErrorCode>> {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let wake = Arc::new(QuiesceOnWake {
        runtime: runtime.clone(),
        armed: AtomicBool::new(false),
        wakes: AtomicUsize::new(0),
    });
    let worker = runtime.worker(0).expect("worker exists");
    let outcome = wait(
        worker
            .call_local(move |context| async move {
                let (params, waiters) = match drain {
                    QuiescingDrain::MaxAge => (params(2), 3),
                    QuiescingDrain::IdleExpiry => (
                        idle_params(
                            2,
                            SessionReuseOrder::MostRecentlyUsed,
                            Duration::from_secs(90),
                            IdleSweep::AtExpiry,
                        ),
                        4,
                    ),
                };
                let pool = pool_with(&context, address, CodecProtocol::Http11, params, untimed());
                let first = acquire(&pool).await;
                if let QuiescingDrain::MaxAge = drain {
                    pool.expire_sessions_for_test();
                }
                let second = acquire(&pool).await;
                let waker = Waker::from(Arc::clone(&wake));
                let mut waiting = Context::from_waker(&waker);
                let mut acquisitions: Vec<_> = (0..waiters)
                    .map(|_| pool.get_session_with_reservation(CancellationToken::never()))
                    .collect();
                for acquisition in &mut acquisitions {
                    assert!(
                        Pin::new(acquisition).poll(&mut waiting).is_pending(),
                        "both sessions are reserved"
                    );
                }
                drop(first.reservation);
                if let QuiescingDrain::IdleExpiry = drain {
                    settle(&first.session).await;
                }
                drop(second.reservation);
                if let QuiescingDrain::IdleExpiry = drain {
                    pool.age_idle_session_for_test(&first.session, Duration::from_secs(180));
                }
                assert_eq!(
                    wake.wakes.load(Ordering::SeqCst),
                    waiters - 1,
                    "one acquisition still waits"
                );
                wake.armed.store(true, Ordering::SeqCst);
                let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
                let outcome = match Pin::new(&mut acquisition).poll(&mut waiting) {
                    Poll::Ready(Ok(_)) => Ok(()),
                    Poll::Ready(Err(PoolError::ReservationFailed { source })) => {
                        Err(Some(source.code()))
                    }
                    Poll::Ready(Err(_)) => Err(None),
                    Poll::Pending => panic!("the step selects the second session"),
                };
                assert!(
                    first.session.is_draining(),
                    "the step retired the first session"
                );
                assert_eq!(
                    wake.wakes.load(Ordering::SeqCst),
                    waiters,
                    "its drain woke the last waiter"
                );
                assert!(
                    !context.handle().is_accepting() && !context.is_cancelled(),
                    "the worker quiesces without cancelling the scenario"
                );
                drop(acquisitions);
                pool.drain_and_join().await.expect("the pool joins");
                outcome
            })
            .expect("scenario admitted"),
    )
    .expect("scenario ends");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    outcome
}

/// An acquisition fails with `DROPPED` when a drain in its own step begins the
/// worker's quiescence. An aged session is drained before the reservation, so
/// the acquisition fails as `ClientSession::reserve_request` does once the
/// worker quiesces. An idle-expired session is drained after the reservation,
/// so the acquisition releases that reservation instead of returning one that
/// the quiescing driver would reject at submit.
#[test]
fn an_acquisition_fails_when_its_own_drain_quiesces_the_worker() {
    assert_eq!(
        [QuiescingDrain::MaxAge, QuiescingDrain::IdleExpiry].map(quiescing_drain_scenario),
        [Err(Some(HttpErrorCode::DROPPED)); 2],
    );
}

/// Reserves an HTTP/1.1 session from an owned pool, or from an `HttpSessionPool`
/// unless `owned`, begins the worker's quiescence and submits a request on the
/// reservation with no yield in between. Returns the error's code and message
/// if the request is refused, or `Ok` once its 200 response is read.
fn submitted_after_quiescence(owned: bool) -> Result<(), (HttpErrorCode, String)> {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let quiescing = runtime.clone();
    let refusal = |error: HttpError| {
        let message = String::from_utf8_lossy(error.message()).into_owned();
        (error.code(), message)
    };
    let worker = runtime.worker(0).expect("worker exists");
    let outcome = wait(
        worker
            .call_local(move |context| async move {
                let (head, body) = request("/").await;
                if owned {
                    let pool = pool(&context, address, CodecProtocol::Http11, 1);
                    let OwnedGetSessionResult { reservation, .. } = acquire(&pool).await;
                    drop(quiescing.quiesce(TIMEOUT));
                    let submission = reservation.submit(head, body, CancellationToken::never());
                    let outcome = match submission.await {
                        Ok(response) => {
                            assert_eq!(read_body(response).await, b"ok");
                            Ok(())
                        }
                        Err(error) => Err(refusal(error)),
                    };
                    pool.drain_and_join().await.expect("the pool joins");
                    return outcome;
                }
                let pool = HttpSessionPool::new(
                    context,
                    socket_address(address),
                    params(1),
                    plaintext(CodecProtocol::Http11),
                    untimed(),
                );
                let GetSessionResult {
                    reservation,
                    session,
                } = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .expect("acquisition succeeds");
                drop(quiescing.quiesce(TIMEOUT));
                let mut response = session
                    .send_reserved_request_with_headers(
                        head,
                        body,
                        reservation,
                        CancellationToken::never(),
                    )
                    .map_err(refusal)?;
                let head = response.source.read_header_event().await.map_err(refusal)?;
                assert_eq!(head.headers().response_status(), Some(200));
                let mut eom = head.eom();
                while !eom {
                    eom = response
                        .source
                        .read_body_event(u32::MAX)
                        .await
                        .map_err(refusal)?
                        .eom();
                }
                Ok(())
            })
            .expect("scenario admitted"),
    )
    .expect("scenario ends");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    outcome
}

/// A request reserved before the worker begins its quiescence and submitted
/// immediately after it. An `HttpSessionPool`'s `ClientSession` serves it,
/// because its run has not observed the quiescence yet. An owned session
/// refuses it with `REFUSED_STREAM`, because its run drains before its driver
/// commits the submission. The acquisition step's admission recheck after an
/// idle-expired drain relies on that refusal: if owned sessions ever commit
/// reservations taken before quiescence, this test fails, and that recheck
/// should be revisited.
#[test]
fn a_request_reserved_before_quiescence_is_served_only_by_the_uniplex_pool() {
    assert_eq!(
        [false, true].map(submitted_after_quiescence),
        [
            Ok(()),
            Err((
                HttpErrorCode::REFUSED_STREAM,
                "owned upstream connection is draining".to_owned(),
            )),
        ],
    );
}

/// The idle timeout, and so the sweep period, of the schedules run against
/// `HyperUtilPool`.
const SWEEP_IDLE: Duration = Duration::from_millis(200);

/// A model of the `client::legacy` pool in hyper-util 0.1.20, for one origin
/// of HTTP/1.1 connections with no waiters, as in the `pool_idle_timeout.rs`
/// tests. `put` pushes a released connection onto the end of the idle list.
/// The first push ever starts sweeps every `max(timeout, 90 ms)`, each evicting
/// every entry idle for longer than the timeout. A checkout pops from the end,
/// dropping expired entries until it reaches one that has not expired, and
/// dials when the list is empty.
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

/// One step of a schedule run against `HyperUtilPool`, `at` milliseconds after
/// `Start`.
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
/// period while the pool is empty.
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

/// A connection that becomes idle after a sweep in which every connection was
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

/// A checkout takes the connection released last while it has not expired and
/// leaves older expired connections to the next sweep. Once that connection has
/// expired, the checkout drops every idle connection and dials.
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

/// Run on exact times, the model keeps open the connections that each schedule
/// expects.
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

/// As with hyper-util's idle interval, sweeps run at most every 90 ms, however
/// short the idle timeout.
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

/// Counts the heap allocations a thread makes while its counter is enabled. The
/// client crate's test build uses it as its global allocator. Some build
/// systems link every other crate as a shared library whose allocations bypass
/// it, so there it counts only the client crate's own allocations: the pools'
/// and the owned session's. A uniplex control checks that it counts them.
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

/// When an idle session is available, the fast acquisition path serves the
/// acquisition on its first poll without a boxed future.
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

/// Responds to every request with an empty 200.
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

/// Submits a GET on `reservation` and reads its response. Returns the status or
/// the error code, and whether the request failed unprocessed.
async fn outcome(reservation: OwnedClientReservation) -> (Result<u16, HttpErrorCode>, bool) {
    let (head, body) = request("/").await;
    let mut submission = pin!(reservation.submit(head, body, CancellationToken::never()));
    let response = match submission.as_mut().await {
        Ok(response) => response,
        Err(error) => return (Err(error.code()), submission.request_unprocessed()),
    };
    let request = response.request.clone();
    let mut source = response.source.into_source_owner().into_source_holder();
    let status = async {
        let head = source.read_header_event().await?;
        let mut eom = head.eom();
        while !eom {
            eom = source.read_body_event(u32::MAX).await?.eom();
        }
        Ok(head.headers().response_status().expect("a response status"))
    }
    .await
    .map_err(|error: roxygen_core::error::HttpError| error.code());
    (status, request.request_unprocessed())
}

/// Four acquisitions on a one-connection owned HTTP/2 pool, sent in one burst
/// to an `owned_http2` server whose header-frame limit is 2 per long interval.
/// The first two get responses. The third header block crosses the limit: its
/// head is not delivered, it is cancelled without a RST_STREAM, and the server
/// drains with GOAWAY(last incoming stream, `NO_ERROR`). The GOAWAY covers it,
/// so it fails when the connection ends and is not unprocessed. Only an
/// unprocessed request would be resent, on a connection the pool dials for it.
#[test]
fn an_owned_http2_burst_past_the_header_limit_resends_only_unprocessed_requests() {
    let mut config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    config.session.owned_http2 = true;
    config.session.http2_rate_limits = Http2RateLimitConfig {
        header_frames: RateLimitConfig::new(2, Duration::from_secs(60), 2),
        ..Http2RateLimitConfig::default()
    };
    let server =
        ScopedHttpServer::start(config, Arc::new(|_| Rc::new(Ok200) as HttpHandlerRef), None)
            .expect("server starts");
    let address = server.address().expect("server address");
    let (outcomes, resent, attempts) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let acquired: Vec<OwnedGetSessionResult> =
            futures::stream::iter((0..4).map(|_| acquire(&pool)))
                .buffered(4)
                .collect()
                .await;
        let first = local(&acquired[0].session);
        assert!(
            acquired
                .iter()
                .all(|acquired| local(&acquired.session) == first),
            "the burst shares one connection"
        );
        let attempts = pool.connection_attempts_for_test();
        let outcomes: Vec<(Result<u16, HttpErrorCode>, bool)> = futures::stream::iter(
            acquired
                .into_iter()
                .map(|acquired| outcome(acquired.reservation)),
        )
        .buffered(4)
        .collect()
        .await;
        let mut resent = Vec::new();
        for _ in outcomes.iter().filter(|(_, unprocessed)| *unprocessed) {
            let next = acquire(&pool).await;
            assert_ne!(
                local(&next.session),
                first,
                "the pool dials a new connection"
            );
            resent.push(outcome(next.reservation).await);
        }
        (
            outcomes,
            resent,
            pool.connection_attempts_for_test() - attempts,
        )
    });
    // The burst reaches the server in one read, and a header breach does not pause the
    // server's parser, so the fourth header block breaches too and is aborted the same way,
    // under the same GOAWAY. The GOAWAY therefore does not exclude a request, and the pool
    // does not resend one. The two aborted requests fail when the drained connection ends:
    // at its EOF, or at a client write that races the close.
    let ended = |outcome: &(Result<u16, HttpErrorCode>, bool)| {
        matches!(
            outcome,
            (
                Err(HttpErrorCode::TRANSPORT_EOF | HttpErrorCode::TRANSPORT_WRITE_ERROR),
                false
            )
        )
    };
    assert_eq!(
        (
            &outcomes[..2],
            ended(&outcomes[2]),
            ended(&outcomes[3]),
            resent,
            attempts
        ),
        (
            &[(Ok(200), false), (Ok(200), false)][..],
            true,
            true,
            Vec::new(),
            0
        ),
        "(answered, crossing request ended, next request ended, resent, new connection \
         attempts): {outcomes:?}"
    );
}

/// A non-hosted worker polls only the cleanups that were woken. The owned
/// pool's cleanups still run on a `RuntimeOwner` worker, each step within a
/// bound. Eight back-to-back HTTP/1.1 acquisitions wait for their returning
/// session, an idle session is retired without an acquisition, and the worker
/// then shuts down with the pool's and a session's cleanups pending and joins
/// without forcing.
#[test]
fn owned_pool_cleanups_run_and_join_under_woken_only_polling() {
    async fn bounded<Work: Future>(what: &str, work: Work) -> Work::Output {
        tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .unwrap_or_else(|_| panic!("{what} does not finish"))
    }
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (same, retired) = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                4,
                SessionReuseOrder::MostRecentlyUsed,
                IDLE,
                IdleSweep::Interval,
            ),
        );
        let first = bounded("the first acquisition", acquire(&pool)).await;
        let session = local(&first.session);
        assert_eq!(exchange(first.reservation, "/").await, b"ok");
        let mut same = Vec::new();
        for _ in 0..8 {
            let next = bounded("a returning-session wait", acquire(&pool)).await;
            same.push(local(&next.session) == session);
            assert_eq!(
                bounded("an exchange", exchange(next.reservation, "/")).await,
                b"ok"
            );
        }
        bounded("idle retirement", until(|| pool.session_count() == 0)).await;
        let retired = pool.session_count() == 0;
        // Leave a fresh session idle in the pool: the scenario's end drops both with their
        // cleanups pending, and the worker's shutdown must join them.
        let last = bounded("a new session", acquire(&pool)).await;
        assert_eq!(exchange(last.reservation, "/").await, b"ok");
        (same, retired)
    });
    assert_eq!(
        (same, retired, origin.accepted()),
        (vec![true; 8], true, 2),
        "(served on the first session, idle session retired, connections)"
    );
}

/// Polls `future` once, with the current task's waker.
async fn poll_once<Work: Future>(mut future: Pin<&mut Work>) -> Poll<Work::Output> {
    std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

/// An inference router's pool parameters (`MostRecentlyUsed`, a 90 s idle timeout swept on an
/// interval), with `max_connections`.
fn router_params(max_connections: u32) -> PoolParams {
    idle_params(
        max_connections,
        SessionReuseOrder::MostRecentlyUsed,
        Duration::from_secs(90),
        IdleSweep::Interval,
    )
}

/// On HTTP/1.1 a stream's next request arrives before the owned driver has returned the slot
/// of the response the stream just read. The acquisition takes that returning slot instead of
/// starting a connection attempt that would never dial. Back-to-back exchanges on one session
/// do not start an attempt or spawn a pool task, and each is served on that session.
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
/// ahead of an earlier waiter whose own dial is still pending, and does not start an attempt.
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
/// claim on it, so the next acquisition waits for that session instead of dialing. An
/// acquisition waits for a returning session only while another acquisition waits, here for
/// its own dial. Without another waiter, it takes the returning session immediately.
#[test]
fn a_cancelled_acquisition_releases_the_returning_session_it_waited_for() {
    // (whether the acquisition is dropped, or cancelled through its token)
    for dropped in [true, false] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (cancelled, attempts, same) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            let one = acquire(&pool).await;
            let two = acquire(&pool).await;
            let first = submit(one.reservation, "/").await;
            let second = submit(two.reservation, "/").await;
            until(|| first.request.capacity_released() && second.request.capacity_released()).await;
            let attempts = pool.connection_attempts_for_test();
            let mut earlier = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            assert!(
                poll_once(earlier.as_mut()).await.is_pending(),
                "no slot is free"
            );
            // Read from the buffer, with no yield: the second session's slot is returning.
            assert_eq!(read_body(second).await, b"ok");
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
            let same = local(&next.session) == local(&two.session);
            let earlier = earlier.await.expect("the earlier waiter's dial serves");
            assert_eq!(exchange(next.reservation, "/").await, b"ok");
            assert_eq!(exchange(earlier.reservation, "/").await, b"ok");
            assert_eq!(read_body(first).await, b"ok");
            (
                cancelled,
                pool.connection_attempts_for_test() - attempts,
                same,
            )
        });
        assert_eq!(
            (cancelled, attempts, same, origin.accepted()),
            (true, 1, true, 3),
            "dropped: {dropped}: (cancelled, attempts (the earlier waiter's alone), served on the \
             returning session, connections)"
        );
    }
}

/// An HTTP/1.1 origin that accepts one connection and then closes its listener, so that a
/// later dial to its address is refused, and responds `ok` to every request on that
/// connection until the client closes it.
struct OneConnectionOrigin {
    address: SocketAddr,
    /// Returns the port's reservation, which the handle keeps until the origin is dropped.
    thread: Option<std::thread::JoinHandle<tokio::net::TcpSocket>>,
}

impl OneConnectionOrigin {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("origin binds");
        let address = listener.local_addr().expect("origin address");
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("origin accepts");
            drop(listener);
            let reserved = reserve(address, true);
            let mut pending = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let read = match std::io::Read::read(&mut stream, &mut buffer) {
                    Ok(0) | Err(_) => return reserved,
                    Ok(read) => read,
                };
                pending.extend_from_slice(&buffer[..read]);
                while let Some(end) = pending.windows(4).position(|part| part == b"\r\n\r\n") {
                    pending.drain(..end + 4);
                    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                    if std::io::Write::write_all(&mut stream, response).is_err() {
                        return reserved;
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
/// acquisition to dial a replacement, like an acquisition that started its own attempt. An
/// acquisition waits for a returning slot only while another acquisition waits, here for its
/// own dial. Without another waiter, a back-to-back acquisition takes the returning session
/// immediately. When the session then fails before its driver commits the request submitted
/// on it, that request is refused unprocessed, and the next acquisition dials a replacement or
/// fails with the dial's error.
#[test]
fn an_acquisition_waiting_for_a_returning_session_that_fails_dials_or_errors_as_before() {
    {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (pending, replaced) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            let one = acquire(&pool).await;
            let two = acquire(&pool).await;
            let first = submit(one.reservation, "/").await;
            let second = submit(two.reservation, "/").await;
            until(|| first.request.capacity_released() && second.request.capacity_released()).await;
            let mut earlier = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            assert!(
                poll_once(earlier.as_mut()).await.is_pending(),
                "no slot is free"
            );
            // Read from the buffer, with no yield: the second session's slot is returning.
            assert_eq!(read_body(second).await, b"ok");
            let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            let pending = poll_once(next.as_mut()).await.is_pending();
            // The session fails before its driver returns the slot.
            two.session.cancel();
            let next = next.await.expect("a replacement serves");
            let replaced = local(&next.session) != local(&two.session);
            let earlier = earlier.await.expect("the earlier waiter's dial serves");
            assert_eq!(exchange(next.reservation, "/").await, b"ok");
            assert_eq!(exchange(earlier.reservation, "/").await, b"ok");
            assert_eq!(read_body(first).await, b"ok");
            (pending, replaced)
        });
        assert_eq!(
            (pending, replaced, origin.accepted()),
            (true, true, 4),
            "while another acquisition waits: (the acquisition waited, it was served on a \
             replacement, connections)"
        );
    }
    // (whether the origin accepts a replacement connection)
    for accepting in [true, false] {
        let hyper_origin = accepting.then(|| Origin::start(CodecProtocol::Http11));
        let refusing_origin = (!accepting).then(OneConnectionOrigin::start);
        let address = hyper_origin.as_ref().map_or_else(
            || refusing_origin.as_ref().expect("one origin").address,
            |origin| origin.address,
        );
        let (took_returning, unprocessed, outcome) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            let first = acquire(&pool).await;
            let session = first.session.clone();
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            let Poll::Ready(taken) = poll_once(next.as_mut()).await else {
                panic!("the returning session is taken in the first poll");
            };
            let taken = taken.expect("the acquisition succeeds");
            let took_returning = local(&taken.session) == local(&session);
            // The session fails before its driver commits the request.
            session.cancel();
            let (head, body) = request("/").await;
            let mut submission = Box::pin(taken.reservation.submit(
                head,
                body,
                CancellationToken::never(),
            ));
            let refused = submission.as_mut().await.is_err();
            let unprocessed = refused && submission.request_unprocessed();
            let outcome = match pool
                .get_session_with_reservation(CancellationToken::never())
                .await
            {
                Ok(acquired) => {
                    let replaced = local(&acquired.session) != local(&session);
                    assert_eq!(exchange(acquired.reservation, "/").await, b"ok");
                    format!("served, replaced: {replaced}")
                }
                Err(PoolError::ConnectFailed { .. }) => "connect failed".to_owned(),
                Err(error) => format!("{error:?}"),
            };
            (took_returning, unprocessed, outcome)
        });
        let expected = if accepting {
            "served, replaced: true"
        } else {
            "connect failed"
        };
        assert_eq!(
            (took_returning, unprocessed, outcome.as_str()),
            (true, true, expected),
            "accepting: {accepting}: (the returning session was taken, its request was refused \
             unprocessed, the next acquisition)"
        );
        if let Some(origin) = hyper_origin {
            assert_eq!(origin.accepted(), 2, "the replacement was dialed");
        }
    }
}

/// When a session is returning and the pool has no waiter, an acquisition takes that session
/// immediately, whatever the pool's limits. That includes `max_connections` with `max_waiters`
/// set to 0, since the acquisition does not wait, and a 1 ns idle timeout, since a session
/// reserved again while its slot is returning is never idle between exchanges.
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
            "served on the first session",
        ),
        (
            "1 ns idle timeout",
            idle_params(
                4,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::from_nanos(1),
                IdleSweep::Interval,
            ),
            "served on the first session",
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

/// A `HostedWorker` teardown that passes its deadline, as in a router's late
/// shutdown, destroys an owned pool while its session is midway through a
/// streamed response. The session's native I/O cleanup and the pool's task join
/// have not finished, so the destruction is an abandonment and the worker
/// reports a clean overrun instead of a panic.
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

/// `count` acquisitions of `pool`, one after another, each held: on HTTP/1.1, each on its
/// own session.
async fn hold(pool: &OwnedHttpSessionPool, count: usize) -> Vec<OwnedGetSessionResult> {
    let mut held = Vec::new();
    for _ in 0..count {
        held.push(acquire(pool).await);
    }
    held
}

/// `count` sessions of `pool`, each idle after one exchange, in the order their exchanges
/// ended: held together, so each is its own session, then exchanged one after another.
async fn idle_sessions(pool: &OwnedHttpSessionPool, count: usize) -> Vec<OwnedClientSession> {
    let mut sessions = Vec::new();
    for OwnedGetSessionResult {
        reservation,
        session,
    } in hold(pool, count).await
    {
        assert_eq!(exchange(reservation, "/").await, b"ok");
        settle(&session).await;
        sessions.push(session);
    }
    sessions
}

/// Where `session` is in `sessions`, by its connection's local endpoint; `None` for a
/// session dialed since.
fn position_of(sessions: &[OwnedClientSession], session: &OwnedClientSession) -> Option<usize> {
    sessions
        .iter()
        .position(|known| local(known) == local(session))
}

/// A reservation taken on a pooled session's handle is recorded on that session's entry,
/// which the pool locates from the slot recorded on the session, without a pass over the
/// pool's sessions.
#[test]
fn a_direct_reservation_is_recorded_without_a_pool_wide_pass() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (scans, served) = run_on_worker(move |context| async move {
        let pool = idle_pool(&context, address, CodecProtocol::Http11, router_params(8));
        let sessions = idle_sessions(&pool, 8).await;
        let session = &sessions[3];
        let before = pool.pool_scans_for_test();
        let reservation = session
            .reserve_request()
            .expect("an idle session admits a request");
        let scans = pool.pool_scans_for_test() - before;
        let served = exchange(reservation, "/").await;
        settle(session).await;
        (scans, served)
    });
    assert_eq!(
        (scans, served.as_slice()),
        (0, b"ok".as_slice()),
        "(passes over the pool's sessions, body)"
    );
}

/// The size of the large HTTP/1.1 pools in these tests.
const K64: usize = 64;

/// One unary round of `streams` acquisitions, all held together. Their exchanges run
/// concurrently, then the round waits until every session it used is idle again.
async fn unary_round(pool: &OwnedHttpSessionPool, streams: usize) {
    let held = hold(pool, streams).await;
    let sessions: Vec<_> = held
        .iter()
        .map(|acquired| acquired.session.clone())
        .collect();
    let bodies: Vec<_> = futures::stream::iter(
        held.into_iter()
            .map(|acquired| exchange(acquired.reservation, "/")),
    )
    .buffer_unordered(streams)
    .collect()
    .await;
    assert!(
        bodies.iter().all(|body| body == b"ok"),
        "every exchange is served"
    );
    until(|| sessions.iter().all(OwnedClientSession::is_idle)).await;
}

/// Unary rounds on a 64-session HTTP/1.1 pool. Once 64 sessions are open, each round's 64
/// acquisitions select an idle session and its 64 releases return one without a pass over the
/// pool's sessions. Acquisitions do not search for a returning session or dial.
#[test]
fn k64_unary_rounds_select_and_release_with_no_pool_wide_pass() {
    let first = OwnedSessionSelection::FirstAvailable;
    let least_loaded = OwnedSessionSelection::LeastLoaded;
    let configs = [
        ("router", router_params(u32::MAX), first),
        ("least recently used", params(u32::MAX), first),
        (
            "least recently used, retired at expiry",
            idle_params(
                u32::MAX,
                SessionReuseOrder::LeastRecentlyUsed,
                Duration::from_secs(90),
                IdleSweep::AtExpiry,
            ),
            first,
        ),
        (
            "most recently used, retired at expiry",
            idle_params(
                u32::MAX,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::from_secs(90),
                IdleSweep::AtExpiry,
            ),
            first,
        ),
        (
            "router, with a maximum age",
            PoolParams {
                max_age: Duration::from_secs(600),
                ..router_params(u32::MAX)
            },
            first,
        ),
        (
            "router, least loaded",
            router_params(u32::MAX),
            least_loaded,
        ),
        (
            "least recently used, least loaded",
            params(u32::MAX),
            least_loaded,
        ),
    ];
    for (name, params, selection) in configs {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let counts = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            pool.set_session_selection(selection)
                .expect("an open pool takes a selection");
            unary_round(&pool, K64).await;
            let scans = pool.pool_scans_for_test();
            let claims = pool.claim_scans_for_test();
            let attempts = pool.connection_attempts_for_test();
            for _ in 0..3 {
                unary_round(&pool, K64).await;
            }
            (
                pool.pool_scans_for_test() - scans,
                pool.claim_scans_for_test() - claims,
                pool.connection_attempts_for_test() - attempts,
                pool.session_count(),
            )
        });
        assert_eq!(
            (counts, origin.accepted()),
            ((0, 0, 0, K64), K64),
            "{name}: ((passes over the pool's sessions, searches for a returning session, \
             connection attempts, sessions), connections)"
        );
    }
}

/// Idle sessions are reused in the order that `SessionReuseOrder` sets, whatever the pool's
/// size. Most recently used takes the session released last first (LIFO). Least recently used
/// takes the session released first first (FIFO).
#[test]
fn idle_sessions_are_reused_in_release_order_or_its_reverse() {
    const COUNT: usize = 8;
    const RELEASES: [usize; COUNT] = [3, 0, 6, 1, 7, 2, 5, 4];
    let ninety = Duration::from_secs(90);
    let configs = [
        ("router", router_params(8), true),
        (
            "most recently used, retired at expiry",
            idle_params(
                8,
                SessionReuseOrder::MostRecentlyUsed,
                ninety,
                IdleSweep::AtExpiry,
            ),
            true,
        ),
        ("least recently used", params(8), false),
        (
            "least recently used, swept on an interval",
            idle_params(
                8,
                SessionReuseOrder::LeastRecentlyUsed,
                ninety,
                IdleSweep::Interval,
            ),
            false,
        ),
    ];
    for (name, params, last_released_first) in configs {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let picks = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            let held = hold(&pool, COUNT).await;
            let sessions: Vec<_> = held
                .iter()
                .map(|acquired| acquired.session.clone())
                .collect();
            let mut responses = Vec::new();
            for acquired in held {
                responses.push(Some(submit(acquired.reservation, "/").await));
            }
            for index in RELEASES {
                let response = responses[index].take().expect("each response is read once");
                assert_eq!(read_body(response).await, b"ok");
                settle(&sessions[index]).await;
            }
            hold(&pool, COUNT)
                .await
                .iter()
                .map(|acquired| position_of(&sessions, &acquired.session))
                .collect::<Vec<_>>()
        });
        let mut expected: Vec<_> = RELEASES.into_iter().map(Some).collect();
        if last_released_first {
            expected.reverse();
        }
        assert_eq!(picks, expected, "{name}: sessions by release");
    }
}

/// An idle session leaves selection when its connection closes or it is cancelled. The
/// others are still reused in order, and an acquisition beyond them dials.
#[test]
fn closed_and_cancelled_idle_sessions_leave_selection() {
    for (name, params, last_released_first) in [
        ("router", router_params(8), true),
        ("least recently used", params(8), false),
    ] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (open, picks, attempts) = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            let sessions = idle_sessions(&pool, 6).await;
            sessions[4]
                .close_when_idle()
                .expect("an idle session closes");
            sessions[2].cancel();
            until(|| sessions[4].is_finished() && sessions[2].is_finished()).await;
            let open = pool.session_count();
            let held = hold(&pool, 4).await;
            let picks: Vec<_> = held
                .iter()
                .map(|acquired| position_of(&sessions, &acquired.session))
                .collect();
            let attempts = pool.connection_attempts_for_test();
            let beyond = acquire(&pool).await;
            assert_eq!(position_of(&sessions, &beyond.session), None);
            (open, picks, pool.connection_attempts_for_test() - attempts)
        });
        let mut expected = vec![Some(0), Some(1), Some(3), Some(5)];
        if last_released_first {
            expected.reverse();
        }
        assert_eq!(
            (open, picks, attempts),
            (4, expected, 1),
            "{name}: (sessions open, picks by release, attempts)"
        );
    }
}

/// An acquisition retires the idle-expired sessions it reaches, as the pool's own passes do.
/// On an interval it retires the expired candidates in reuse order, and at expiry every expired
/// session. A retired session is never selected again. Schedules that make an acquisition dial
/// are left to the interval schedule tests, because a retired session's drain wakes the dialing
/// acquisition, which may dial twice, as in `one_nanosecond_idle_scenario`.
#[test]
fn sessions_retired_for_idleness_leave_selection() {
    let timeout = Duration::from_secs(90);
    let expired = 2 * timeout;
    let configs = [
        // Least recently used, on an interval: the checkout retires the expired front session
        // and takes the next.
        (
            "least recently used, on an interval",
            SessionReuseOrder::LeastRecentlyUsed,
            IdleSweep::Interval,
            0,
            [Some(1), Some(2)],
        ),
        // At expiry, the step retires the expired session before it selects.
        (
            "least recently used, at expiry",
            SessionReuseOrder::LeastRecentlyUsed,
            IdleSweep::AtExpiry,
            1,
            [Some(0), Some(2)],
        ),
        (
            "most recently used, at expiry",
            SessionReuseOrder::MostRecentlyUsed,
            IdleSweep::AtExpiry,
            2,
            [Some(1), Some(0)],
        ),
    ];
    for (name, order, sweep, aged, expected) in configs {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let outcome = run_on_worker(move |context| async move {
            let pool = idle_pool(
                &context,
                address,
                CodecProtocol::Http11,
                idle_params(8, order, timeout, sweep),
            );
            let sessions = idle_sessions(&pool, 3).await;
            pool.age_idle_session_for_test(&sessions[aged], expired);
            let attempts = pool.connection_attempts_for_test();
            let held = hold(&pool, 2).await;
            let attempts = pool.connection_attempts_for_test() - attempts;
            until(|| sessions[aged].is_finished()).await;
            let picks: Vec<_> = held
                .iter()
                .map(|acquired| position_of(&sessions, &acquired.session))
                .collect();
            (picks, attempts, pool.session_count())
        });
        assert_eq!(
            outcome,
            (expected.to_vec(), 0, 2),
            "{name}: (picks, attempts, sessions open)"
        );
    }
}

/// The index holds only sessions the pool holds: a closed, a cancelled and an idle-retired
/// session leave it with their entries, a reserved one leaves its class list, and a drain
/// empties it.
#[test]
fn the_index_holds_no_session_the_pool_removed() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (counts, picks, open) = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                8,
                SessionReuseOrder::LeastRecentlyUsed,
                Duration::from_secs(90),
                IdleSweep::Interval,
            ),
        );
        let sessions = idle_sessions(&pool, 6).await;
        let mut counts = vec![pool.indexed_sessions_for_test()];
        sessions[1]
            .close_when_idle()
            .expect("an idle session closes");
        sessions[3].cancel();
        until(|| sessions[1].is_finished() && sessions[3].is_finished()).await;
        counts.push(pool.indexed_sessions_for_test());
        let first = acquire(&pool).await;
        counts.push(pool.indexed_sessions_for_test());
        pool.age_idle_session_for_test(&sessions[2], Duration::from_secs(180));
        let second = acquire(&pool).await;
        until(|| sessions[2].is_finished()).await;
        counts.push(pool.indexed_sessions_for_test());
        pool.drain();
        counts.push(pool.indexed_sessions_for_test());
        let picks = [&first, &second].map(|acquired| position_of(&sessions, &acquired.session));
        drop((first, second));
        (counts, picks, pool.session_count())
    });
    assert_eq!(
        (counts, picks, open),
        (
            vec![(0, 6), (0, 4), (0, 3), (0, 1), (0, 0)],
            [Some(0), Some(4)],
            0
        ),
        "((busy, idle) indexed: six idle; less a closed and a cancelled one; less a reserved \
         one; less a retired and a reserved one; drained), picks, sessions open)"
    );
}

/// What one step of `pool_schedule` did, by session ordinal (the order in which the schedule
/// first reached each session).
#[derive(Debug, PartialEq)]
enum ScheduleEvent {
    Pick(usize),
    Release(usize),
    Close(usize),
    Cancel(usize),
    Age(usize),
}

/// A seeded schedule of acquisitions (each submitting a request, then waiting for the dials
/// it started), releases (reading a held response to its end, then waiting for its session to
/// be idle), closes, cancellations and, when `age`, idle aging past the pool's idle timeout, on
/// an HTTP/1.1 pool of at most eight sessions. Returns its events, the connection attempts,
/// and the sessions left open.
///
/// Aging is for sweeps on an interval: at expiry, idle retirement's own task may wake on a
/// release and retire an aged session before an acquisition reaches it.
async fn pool_schedule(
    pool: &OwnedHttpSessionPool,
    timeout: Option<Duration>,
) -> (Vec<ScheduleEvent>, u64, usize) {
    let mut random = 0x2545_f491_4f6c_dd1d_u64;
    let mut below = move |bound: usize| {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        usize::try_from(random % u64::try_from(bound).expect("small bound")).expect("small value")
    };
    let mut known: Vec<OwnedClientSession> = Vec::new();
    let mut held: Vec<(usize, OwnedClientResponse)> = Vec::new();
    // Released, and neither picked, closed, cancelled nor aged since.
    let mut idle: Vec<usize> = Vec::new();
    let mut events = Vec::new();
    let attempts = pool.connection_attempts_for_test();
    for _ in 0..160 {
        let choice = below(20);
        if (choice < 8 || held.is_empty()) && held.len() < 8 {
            let acquired = acquire(pool).await;
            let ordinal = position_of(&known, &acquired.session).unwrap_or_else(|| {
                known.push(acquired.session.clone());
                known.len() - 1
            });
            idle.retain(|&candidate| candidate != ordinal);
            held.push((ordinal, submit(acquired.reservation, "/").await));
            events.push(ScheduleEvent::Pick(ordinal));
            // A retired session's drain may wake a dialing acquisition into a second dial.
            until(|| pool.pending_connects_for_test() == 0).await;
        } else if choice < 15 && !held.is_empty() {
            let (ordinal, response) = held.swap_remove(below(held.len()));
            assert_eq!(read_body(response).await, b"ok");
            settle(&known[ordinal]).await;
            idle.push(ordinal);
            events.push(ScheduleEvent::Release(ordinal));
        } else if !idle.is_empty() {
            let ordinal = idle.swap_remove(below(idle.len()));
            let session = &known[ordinal];
            match choice {
                15 => {
                    session.close_when_idle().expect("an idle session closes");
                    until(|| session.is_finished()).await;
                    events.push(ScheduleEvent::Close(ordinal));
                }
                16 => {
                    session.cancel();
                    until(|| session.is_finished()).await;
                    events.push(ScheduleEvent::Cancel(ordinal));
                }
                _ if let Some(timeout) = timeout => {
                    pool.age_idle_session_for_test(session, 2 * timeout);
                    events.push(ScheduleEvent::Age(ordinal));
                }
                _ => idle.push(ordinal),
            }
        }
    }
    for (_, response) in held {
        assert_eq!(read_body(response).await, b"ok");
    }
    (
        events,
        pool.connection_attempts_for_test() - attempts,
        pool.session_count(),
    )
}

/// The index selects as the pool's passes do: `pool_schedule` makes the same picks, dials as
/// often and leaves as many sessions open on a pool selecting through its index as on one whose
/// passes are forced, under each reuse order and sweep, and under least-loaded selection.
#[test]
fn the_index_selects_as_the_pool_s_passes_do() {
    let ninety = Duration::from_secs(90);
    let first = OwnedSessionSelection::FirstAvailable;
    let least_loaded = OwnedSessionSelection::LeastLoaded;
    let configs = [
        ("router", router_params(8), Some(ninety), first),
        ("least recently used", params(8), None, first),
        (
            "most recently used, at expiry",
            idle_params(
                8,
                SessionReuseOrder::MostRecentlyUsed,
                ninety,
                IdleSweep::AtExpiry,
            ),
            None,
            first,
        ),
        (
            "least recently used, on an interval",
            idle_params(
                8,
                SessionReuseOrder::LeastRecentlyUsed,
                ninety,
                IdleSweep::Interval,
            ),
            Some(ninety),
            first,
        ),
        (
            "router, least loaded",
            router_params(8),
            Some(ninety),
            least_loaded,
        ),
        (
            "least recently used, least loaded",
            params(8),
            None,
            least_loaded,
        ),
    ];
    for (name, params, aged, selection) in configs {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let [(indexed, indexed_scans), (scanned, scans)] = [false, true].map(|forced| {
            run_on_worker(move |context| async move {
                let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                pool.force_pool_scans_for_test(forced);
                let outcome = pool_schedule(&pool, aged).await;
                (outcome, pool.pool_scans_for_test())
            })
        });
        assert!(
            indexed
                .0
                .iter()
                .any(|event| matches!(event, ScheduleEvent::Pick(_))),
            "{name}: the schedule acquires"
        );
        assert!(
            indexed_scans < scans / 2,
            "{name}: the index served most steps and releases ({indexed_scans} passes, {scans} \
             forced)"
        );
        assert_eq!(
            indexed, scanned,
            "{name}: (events, attempts, sessions open)"
        );
    }
}

/// At expiry, once idle time may have passed a session's idle timeout, the step uses the pool's
/// passes, which retire every expired session before they select. Idle time that passes only
/// for the idle sessions, while the index is settled, retires them, and the session released
/// since is taken.
#[test]
fn an_acquisition_at_expiry_retires_the_sessions_whose_idle_time_passed() {
    let timeout = Duration::from_secs(90);
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let outcome = run_on_worker(move |context| async move {
        let pool = idle_pool(
            &context,
            address,
            CodecProtocol::Http11,
            idle_params(
                8,
                SessionReuseOrder::LeastRecentlyUsed,
                timeout,
                IdleSweep::AtExpiry,
            ),
        );
        let sessions = idle_sessions(&pool, 3).await;
        let busy = acquire(&pool).await;
        pool.pass_idle_time_for_test(2 * timeout);
        assert_eq!(exchange(busy.reservation, "/").await, b"ok");
        settle(&busy.session).await;
        let settled = pool.index_settled_for_test();
        let attempts = pool.connection_attempts_for_test();
        let next = acquire(&pool).await;
        let attempts = pool.connection_attempts_for_test() - attempts;
        let retired = [&sessions[1], &sessions[2]].map(OwnedClientSession::is_draining);
        if retired == [true; 2] {
            until(|| sessions[1].is_finished() && sessions[2].is_finished()).await;
        }
        (
            position_of(&sessions, &busy.session),
            settled,
            position_of(&sessions, &next.session),
            retired,
            attempts,
            pool.session_count(),
        )
    });
    assert_eq!(
        outcome,
        (Some(0), true, Some(0), [true; 2], 0, 1),
        "(first pick, index settled, second pick, expired sessions retired, attempts, sessions \
         open)"
    );
}

/// A cancelled session stops admitting before its pool is notified. The cancellation unsettles
/// the index, so a reservation in the same turn and the next release use the pool's passes.
/// Once the session's closed admission reaches the pool, a release settles the index again, and
/// the index then skips the cancelled session.
#[test]
fn a_cancellation_unsettles_the_index_until_a_release_settles_it() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let outcome = run_on_worker(move |context| async move {
        let pool = idle_pool(&context, address, CodecProtocol::Http11, router_params(8));
        let sessions = idle_sessions(&pool, 4).await;
        let before = pool.index_settled_for_test();
        sessions[1].cancel();
        let cancelled = pool.index_settled_for_test();
        let reservation = sessions[3]
            .reserve_request()
            .expect("an idle session admits a request");
        assert_eq!(exchange(reservation, "/").await, b"ok");
        settle(&sessions[3]).await;
        until(|| sessions[1].is_finished()).await;
        let after = pool.index_settled_for_test();
        let picks: Vec<_> = hold(&pool, 3)
            .await
            .iter()
            .map(|acquired| position_of(&sessions, &acquired.session))
            .collect();
        (before, cancelled, after, picks)
    });
    assert_eq!(
        outcome,
        (true, false, true, vec![Some(3), Some(2), Some(0)]),
        "(settled before, after the cancellation, after its close and a release; picks)"
    );
}

/// The passes do not retire a session that reaches its maximum age while it serves an
/// exchange, since it cannot take another. The other sessions keep being selected and released
/// through the index, without a pass over the pool's sessions, until its exchange ends. Then
/// the next acquisition retires it, as the passes do, and takes another session.
#[test]
fn a_session_aged_while_busy_leaves_selection_indexed_until_its_exchange_ends() {
    const ROUNDS: usize = 16;
    let configs = [
        (
            "router, with a maximum age",
            PoolParams {
                max_age: Duration::from_secs(600),
                ..router_params(8)
            },
        ),
        ("least recently used", params(8)),
    ];
    for (name, params) in configs {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let outcome = run_on_worker(move |context| async move {
            let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
            idle_sessions(&pool, 4).await;
            let held = acquire(&pool).await;
            pool.expire_session_for_test(&held.session);
            // The expiry unsettles the index; this exchange's release settles it again.
            one_exchange(&pool).await;
            let before = pool.pool_scans_for_test();
            for _ in 0..ROUNDS {
                let session = one_exchange(&pool).await;
                assert_ne!(local(&session), local(&held.session), "{name}");
            }
            let scans = pool.pool_scans_for_test() - before;
            assert_eq!(exchange(held.reservation, "/").await, b"ok");
            settle(&held.session).await;
            let next = one_exchange(&pool).await;
            let closed = tokio::time::timeout(Duration::from_secs(2), held.session.wait_closed())
                .await
                .is_ok_and(|closed| closed.is_ok());
            (
                scans,
                local(&next) != local(&held.session),
                pool.session_count(),
                closed,
            )
        });
        assert_eq!(
            outcome,
            (0, true, 3, true),
            "{name}: (passes over the pool's sessions while the aged session is busy, the \
             acquisition after its exchange took another session, sessions, the aged session \
             closed)"
        );
    }
}

/// HTTP/2 sessions limited to four streams each.
fn four_streams() -> SessionParams {
    SessionParams {
        max_concurrent_outgoing_streams: Some(4),
        ..untimed()
    }
}

/// The order in which `four_idle_http2_sessions` lets its sessions' exchanges end, by the
/// order in which the sessions first appear among its acquisitions.
const HTTP2_RELEASES: [usize; 4] = [2, 0, 3, 1];

/// Four HTTP/2 sessions of `four_streams`, idle: sixteen acquisitions held together open them,
/// four streams each, then each session's four exchanges run and end, one session after
/// another in `HTTP2_RELEASES` order, so that the pool's order and the sessions' idle stamps
/// follow it. Returns the sessions in that order.
async fn four_idle_http2_sessions(pool: &OwnedHttpSessionPool) -> Vec<OwnedClientSession> {
    let held: Vec<OwnedGetSessionResult> = futures::stream::iter((0..16).map(|_| acquire(pool)))
        .buffered(16)
        .collect()
        .await;
    let mut sessions: Vec<OwnedClientSession> = Vec::new();
    let mut reservations: Vec<Vec<OwnedClientReservation>> = Vec::new();
    for OwnedGetSessionResult {
        reservation,
        session,
    } in held
    {
        let index = position_of(&sessions, &session).unwrap_or_else(|| {
            sessions.push(session);
            reservations.push(Vec::new());
            sessions.len() - 1
        });
        reservations[index].push(reservation);
    }
    assert_eq!(
        reservations.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![4; 4],
        "sixteen held acquisitions open four sessions of four streams"
    );
    for index in HTTP2_RELEASES {
        for reservation in std::mem::take(&mut reservations[index]) {
            assert_eq!(exchange(reservation, "/").await, b"ok");
        }
        settle(&sessions[index]).await;
    }
    HTTP2_RELEASES
        .iter()
        .map(|&index| sessions[index].clone())
        .collect()
}

/// A least-loaded acquisition takes the session with the fewest streams in flight. Among
/// equally loaded sessions it takes an idle one in reuse order, or a busy one in the pool's
/// order. Eight acquisitions held one after another on four idle HTTP/2 sessions of four
/// streams therefore take each session twice: in release order twice under least recently
/// used, and in reverse release order twice under most recently used. `FirstAvailable`, the
/// default, packs them onto the first session with room: four on the first idle session the
/// reuse order takes, then four on the next.
#[test]
fn least_loaded_selection_spreads_held_http2_reservations_in_reuse_order() {
    let least = SessionReuseOrder::LeastRecentlyUsed;
    let most = SessionReuseOrder::MostRecentlyUsed;
    let configs = [
        (
            "first available, least recently used",
            OwnedSessionSelection::FirstAvailable,
            least,
            [0, 0, 0, 0, 1, 1, 1, 1],
        ),
        (
            "first available, most recently used",
            OwnedSessionSelection::FirstAvailable,
            most,
            [3, 3, 3, 3, 2, 2, 2, 2],
        ),
        (
            "least loaded, least recently used",
            OwnedSessionSelection::LeastLoaded,
            least,
            [0, 1, 2, 3, 0, 1, 2, 3],
        ),
        (
            "least loaded, most recently used",
            OwnedSessionSelection::LeastLoaded,
            most,
            [3, 2, 1, 0, 3, 2, 1, 0],
        ),
    ];
    for (name, selection, reuse_order, expected) in configs {
        let origin = Origin::start(CodecProtocol::Http2);
        let address = origin.address;
        let (picks, sessions) = run_on_worker(move |context| async move {
            let pool = pool_with(
                &context,
                address,
                CodecProtocol::Http2,
                PoolParams {
                    reuse_order,
                    ..params(4)
                },
                four_streams(),
            );
            let released = four_idle_http2_sessions(&pool).await;
            pool.set_session_selection(selection)
                .expect("an open pool takes a selection");
            let held = hold(&pool, 8).await;
            let picks: Vec<Option<usize>> = held
                .iter()
                .map(|acquired| position_of(&released, &acquired.session))
                .collect();
            drop(held);
            (picks, pool.session_count())
        });
        assert_eq!(
            (picks, sessions, origin.accepted()),
            (expected.map(Some).to_vec(), 4, 4),
            "{name}: (sessions taken, by release order; sessions; connections)"
        );
    }
}

/// Under least-loaded selection, loops of exchanges run concurrently over four HTTP/2 sessions
/// of four streams. Each acquisition takes a session at most one stream above every other, as
/// read from the sessions' own capacity right after its reservation, with no other code running
/// between the reservation and the read. Every session serves and every exchange completes,
/// also with twenty loops for sixteen streams, so that acquisitions wait for a stream. The pool
/// does not add a connection.
#[test]
fn least_loaded_selection_keeps_concurrent_http2_load_within_one_stream_and_starves_nothing() {
    const ROUNDS: usize = 16;
    let configs = [
        (8, SessionReuseOrder::LeastRecentlyUsed),
        (8, SessionReuseOrder::MostRecentlyUsed),
        (20, SessionReuseOrder::LeastRecentlyUsed),
        (20, SessionReuseOrder::MostRecentlyUsed),
    ];
    for (loops, reuse_order) in configs {
        let origin = Origin::start(CodecProtocol::Http2);
        let address = origin.address;
        let (uneven, served, completed, sessions) = run_on_worker(move |context| async move {
            let pool = pool_with(
                &context,
                address,
                CodecProtocol::Http2,
                PoolParams {
                    reuse_order,
                    ..params(4)
                },
                four_streams(),
            );
            let known = four_idle_http2_sessions(&pool).await;
            pool.set_session_selection(OwnedSessionSelection::LeastLoaded)
                .expect("an open pool takes a selection");
            let uneven = Cell::new(0_usize);
            let served = RefCell::new([0_usize; 4]);
            let completed = Cell::new(0_usize);
            let (pool, known, uneven, served, completed) =
                (&pool, &known, &uneven, &served, &completed);
            futures::stream::iter(0..loops)
                .for_each_concurrent(None, |_| async move {
                    for _ in 0..ROUNDS {
                        let OwnedGetSessionResult {
                            reservation,
                            session,
                        } = acquire(pool).await;
                        let chosen = position_of(known, &session).expect("an established session");
                        let in_flight: Vec<u32> = known
                            .iter()
                            .map(|known| 4 - known.available_capacity())
                            .collect();
                        if in_flight.iter().any(|&other| in_flight[chosen] > other + 1) {
                            uneven.set(uneven.get() + 1);
                        }
                        served.borrow_mut()[chosen] += 1;
                        if exchange(reservation, "/").await == b"ok" {
                            completed.set(completed.get() + 1);
                        }
                    }
                })
                .await;
            (
                uneven.get(),
                *served.borrow(),
                completed.get(),
                pool.session_count(),
            )
        });
        assert_eq!(
            (
                uneven,
                served.iter().all(|&count| count > 0),
                completed,
                sessions,
                origin.accepted()
            ),
            (0, true, loops * ROUNDS, 4, 4),
            "{loops} loops, {reuse_order:?}: (acquisitions that took a session more than one \
             stream above another, every session served ({served:?}), exchanges completed, \
             sessions, connections)"
        );
    }
}

/// On HTTP/1.1, where a session takes one exchange at a time, a least-loaded acquisition
/// prefers an idle session over one with a free slot and a driver that is not idle yet. A
/// reservation dropped unsubmitted leaves a session in that state until the driver's next
/// turn. `FirstAvailable`, the default, takes that session first, as the busy session that
/// can take an exchange.
#[test]
fn least_loaded_selection_takes_an_idle_http1_session_before_one_not_yet_idle() {
    let configs = [
        ("router", router_params(4)),
        ("least recently used", params(4)),
    ];
    for (selection, takes_dropped) in [
        (OwnedSessionSelection::FirstAvailable, true),
        (OwnedSessionSelection::LeastLoaded, false),
    ] {
        for (name, params) in configs {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let (dropped, same) = run_on_worker(move |context| async move {
                let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                idle_sessions(&pool, 2).await;
                let OwnedGetSessionResult {
                    reservation,
                    session,
                } = acquire(&pool).await;
                drop(reservation);
                let dropped = (session.available_capacity(), session.is_idle());
                let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
                let Poll::Ready(next) = poll_once(next.as_mut()).await else {
                    panic!("an established session is taken in the first poll");
                };
                let next = next.expect("the acquisition succeeds");
                (dropped, local(&next.session) == local(&session))
            });
            assert_eq!(
                (dropped, same),
                ((1, false), takes_dropped),
                "{selection:?}, {name}: ((the dropped session's capacity, idle), the next \
                 acquisition took it)"
            );
        }
    }
}

/// Among equally loaded sessions a least-loaded acquisition takes a busy one first, as
/// `FirstAvailable` does. An HTTP/1.1 session that the pool dialed for an acquisition dropped
/// before its dial completed has no exchange in flight, like an idle session, but counts as
/// busy until its first reservation. The next acquisition therefore takes it before the idle
/// session, under either selection. Two acquisitions dial, and the first is dropped before
/// either attempt runs, so the second keeps the attempts dialing and the first's dial completes
/// without a dialer to hand its session to.
#[test]
fn least_loaded_selection_takes_a_dialed_session_no_acquisition_took_before_an_idle_one() {
    let configs = [
        ("router", router_params(4)),
        ("least recently used", params(4)),
    ];
    for selection in [
        OwnedSessionSelection::FirstAvailable,
        OwnedSessionSelection::LeastLoaded,
    ] {
        for (name, params) in configs {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let (dialed, sessions) = run_on_worker(move |context| async move {
                let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                let held = acquire(&pool).await;
                {
                    let mut abandoned =
                        Box::pin(pool.get_session_with_reservation(CancellationToken::never()));
                    let mut keeping =
                        pin!(pool.get_session_with_reservation(CancellationToken::never()));
                    for acquisition in [abandoned.as_mut(), keeping.as_mut()] {
                        assert!(
                            poll_once(acquisition).await.is_pending(),
                            "a reserved HTTP/1.1 session makes the acquisition dial"
                        );
                    }
                    drop(abandoned);
                    until(|| pool.session_count() == 3).await;
                }
                assert_eq!(exchange(held.reservation, "/").await, b"ok");
                settle(&held.session).await;
                let next = acquire(&pool).await;
                (
                    local(&next.session) != local(&held.session),
                    pool.session_count(),
                )
            });
            assert_eq!(
                (dialed, sessions, origin.accepted()),
                (true, 3, 3),
                "{selection:?}, {name}: (a dialed session was taken, sessions, connections)"
            );
        }
    }
}

/// A least-loaded acquisition takes the least loaded session that can take an exchange,
/// however loaded. When every session is busy, an HTTP/1.1 session that has a free slot but
/// whose driver is not idle yet is taken in the acquisition's first poll, without a connection
/// attempt, under either selection.
#[test]
fn least_loaded_selection_takes_a_session_not_yet_idle_rather_than_dial() {
    let configs = [
        ("router", router_params(4)),
        ("least recently used", params(4)),
    ];
    for selection in [
        OwnedSessionSelection::FirstAvailable,
        OwnedSessionSelection::LeastLoaded,
    ] {
        for (name, params) in configs {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let (same, attempts, sessions) = run_on_worker(move |context| async move {
                let pool = idle_pool(&context, address, CodecProtocol::Http11, params);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                idle_sessions(&pool, 1).await;
                let OwnedGetSessionResult {
                    reservation,
                    session,
                } = acquire(&pool).await;
                drop(reservation);
                let attempts = pool.connection_attempts_for_test();
                let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
                let Poll::Ready(next) = poll_once(next.as_mut()).await else {
                    panic!("an established session is taken in the first poll");
                };
                let next = next.expect("the acquisition succeeds");
                (
                    local(&next.session) == local(&session),
                    pool.connection_attempts_for_test() - attempts,
                    pool.session_count(),
                )
            });
            assert_eq!(
                (same, attempts, sessions, origin.accepted()),
                (true, 0, 1, 1),
                "{selection:?}, {name}: (the session not yet idle was taken, connection attempts, \
                 sessions, connections)"
            );
        }
    }
}

/// An owned HTTP/1.1 pool to `address`, whose sessions take pull-through
/// exchanges, with `params` and `session`'s parameters.
fn pull_through_pool(
    context: &WorkerContext,
    address: SocketAddr,
    params: PoolParams,
    session: SessionParams,
) -> OwnedHttpSessionPool {
    OwnedHttpSessionPool::new(
        context.clone(),
        socket_address(address),
        params,
        plaintext(CodecProtocol::Http11),
        session,
    )
    .expect("the pool accepts its parameters")
}

/// Sends `path` on `reservation` as a pull-through request and returns the
/// 200 response's whole body, read through its pull-through body.
async fn pull_through_exchange(reservation: OwnedClientReservation, path: &str) -> Vec<u8> {
    let (head, body) = request(path).await;
    let mut response = reservation
        .submit_pull_through(head, body, CancellationToken::never())
        .await
        .expect("request is committed");
    let head = poll_fn(|context| response.body.poll_header(context))
        .await
        .expect("response head");
    assert_eq!(head.headers().response_status(), Some(200));
    let mut body = Vec::new();
    let mut eom = head.eom();
    while !eom {
        let event = poll_fn(|context| response.body.poll_body(context, u32::MAX))
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

/// Runs one pull-through exchange on the pool's next session and returns that
/// session once it is idle.
async fn pull_through_one_exchange(pool: &OwnedHttpSessionPool) -> OwnedClientSession {
    let OwnedGetSessionResult {
        reservation,
        session,
    } = acquire(pool).await;
    assert_eq!(pull_through_exchange(reservation, "/").await, b"ok");
    settle(&session).await;
    session
}

/// As `two_idle_sessions`, through pull-through exchanges.
async fn two_pull_through_idle_sessions(
    pool: &OwnedHttpSessionPool,
) -> (OwnedClientSession, OwnedClientSession) {
    let first = acquire(pool).await;
    let second = acquire(pool).await;
    assert_ne!(
        local(&first.session),
        local(&second.session),
        "a reserved session makes the pool open another"
    );
    assert_eq!(pull_through_exchange(first.reservation, "/").await, b"ok");
    settle(&first.session).await;
    assert_eq!(pull_through_exchange(second.reservation, "/").await, b"ok");
    settle(&second.session).await;
    (first.session, second.session)
}

/// Idle retirement and most-recently-used order hold on pull-through sessions.
/// The run, to which the drive returns when the response is produced, retires
/// the exchange and publishes idleness, which the session's run scope reports
/// to the pool, as for any owned session. These are the scenarios of
/// `idle_session_is_retired_and_replaced` (both sweeps),
/// `most_recently_used_reuse_picks_the_session_released_last` and
/// `most_recently_used_reuse_lets_the_cold_session_retire`, through
/// pull-through exchanges.
#[test]
fn pull_through_sessions_are_reported_idle_and_retired() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (sessions, closed, replaced, body) = run_on_worker(move |context| async move {
            let pool = pull_through_pool(
                &context,
                address,
                idle_params(1, SessionReuseOrder::LeastRecentlyUsed, IDLE, idle_sweep),
                untimed(),
            );
            let first = pull_through_one_exchange(&pool).await;
            tokio::time::sleep(IDLE * 4).await;
            let sessions = pool.session_count();
            let closed = tokio::time::timeout(Duration::from_secs(1), first.wait_closed()).await;
            let OwnedGetSessionResult {
                reservation,
                session: second,
            } = acquire(&pool).await;
            (
                sessions,
                matches!(closed, Ok(Ok(_))),
                local(&first) != local(&second),
                pull_through_exchange(reservation, "/").await,
            )
        });
        assert_eq!(
            (sessions, closed, replaced, body),
            (0, true, true, b"ok".to_vec()),
            "{idle_sweep:?}: the idle pull-through session is retired without an acquisition, \
             closes, and the next request opens a new connection"
        );
    }

    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let picks = run_on_worker(move |context| async move {
        let pool = pull_through_pool(
            &context,
            address,
            idle_params(
                2,
                SessionReuseOrder::MostRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
            untimed(),
        );
        let (older, newer) = two_pull_through_idle_sessions(&pool).await;
        // Released in the other order, the older session is then the most recent.
        let first = acquire(&pool).await;
        let second = acquire(&pool).await;
        let picks = [
            local(&first.session) == local(&newer),
            local(&second.session) == local(&older),
        ];
        assert_eq!(pull_through_exchange(first.reservation, "/").await, b"ok");
        settle(&first.session).await;
        assert_eq!(pull_through_exchange(second.reservation, "/").await, b"ok");
        settle(&second.session).await;
        let last = local(&pull_through_one_exchange(&pool).await) == local(&older);
        [picks[0], picks[1], last]
    });
    assert_eq!(
        picks,
        [true, true, true],
        "the most recently released pull-through session is reused first"
    );

    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (hot_served, cold_retired, hot_kept, sessions) = run_on_worker(move |context| async move {
        let pool = pull_through_pool(
            &context,
            address,
            idle_params(
                2,
                SessionReuseOrder::MostRecentlyUsed,
                IDLE,
                IdleSweep::AtExpiry,
            ),
            untimed(),
        );
        let (cold, hot) = two_pull_through_idle_sessions(&pool).await;
        let mut hot_served = true;
        for _ in 0..12 {
            hot_served &= local(&pull_through_one_exchange(&pool).await) == local(&hot);
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
        "the most recently used idle pull-through session serves, and the unused one idles out"
    );
}

/// A waiter takes the session that a pull-through exchange released. The drive
/// returns to the run at the response's end, and the run returns the slot. The
/// slot's `capacity_available` wakes the waiter, which reuses the session.
#[test]
fn a_waiter_takes_the_session_a_pull_through_exchange_released() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (pending, first_body, same, body) = run_on_worker(move |context| async move {
        let pool = pull_through_pool(&context, address, params(1), untimed());
        let first = acquire(&pool).await;
        let second = pool.get_session_with_reservation(CancellationToken::never());
        tokio::pin!(second);
        let pending = tokio::select! {
            _ = &mut second => false,
            () = tokio::time::sleep(Duration::from_millis(100)) => true,
        };
        let first_body = pull_through_exchange(first.reservation, "/slow").await;
        let second = second.await.expect("the waiter is served");
        let same = local(&first.session) == local(&second.session);
        (
            pending,
            first_body,
            same,
            pull_through_exchange(second.reservation, "/").await,
        )
    });
    assert_eq!(
        (pending, first_body, same, body, origin.accepted()),
        (true, b"chunk".repeat(SLOW_CHUNKS), true, b"ok".to_vec(), 1),
        "the waiter waits for, then reuses, the session the pull-through exchange released"
    );
}

/// An inference router's HTTP/1.1 parameters: a pool shaped like hyper-util's
/// `client::legacy` pool (MRU reuse, interval sweeps, unlimited connections,
/// one connection attempt), and session timers off with every hyper-parity
/// option on.
fn router_http1() -> (PoolParams, SessionParams) {
    (
        PoolParams {
            max_connections: u32::MAX,
            max_connection_attempts: 1,
            enable_tls_session_caching: false,
            connect_timeout: Duration::from_secs(1),
            max_age: Duration::ZERO,
            max_waiters: u16::MAX,
            idle_timeout: Duration::from_secs(90),
            reuse_order: SessionReuseOrder::MostRecentlyUsed,
            idle_sweep: IdleSweep::Interval,
            acquisition_timeout: None,
        },
        SessionParams {
            deliver_ingress_before_abort: true,
            unframed_bodiless_http1_requests: true,
            non_abortive_close: true,
            ..untimed()
        },
    )
}

/// An owned pool accepts the router's HTTP/1.1 parameters and serves
/// pull-through exchanges on one reused connection.
#[test]
fn the_router_s_session_parameters_are_accepted_with_pull_through() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (same, sessions) = run_on_worker(move |context| async move {
        let (params, session) = router_http1();
        let pool = pull_through_pool(&context, address, params, session);
        let first = pull_through_one_exchange(&pool).await;
        let second = pull_through_one_exchange(&pool).await;
        (local(&first) == local(&second), pool.session_count())
    });
    assert_eq!(
        (same, sessions, origin.accepted()),
        (true, 1, 1),
        "two pull-through exchanges reuse one connection"
    );
}

/// Exchanges each stream of `back_to_back` runs after its first.
const BACK_TO_BACK_REQUESTS: usize = 8;

/// Runs `streams` streams of `BACK_TO_BACK_REQUESTS` exchanges each, through
/// pull-through exchanges when `pull_through`. Every stream first holds a
/// reservation of its own, all together, so the pool opens one session per
/// stream. Each stream then acquires its next reservation as soon as it has
/// read its previous response to its end, without waiting for the session to
/// go idle. Returns this thread's `wait_for_wakeup` calls after the first
/// reservations were all held.
async fn back_to_back(pool: &OwnedHttpSessionPool, streams: usize, pull_through: bool) -> u64 {
    let first: Vec<_> = futures::stream::iter((0..streams).map(|_| acquire(pool)))
        .buffer_unordered(streams)
        .collect()
        .await;
    take_waits_for_wakeup_for_test();
    futures::stream::iter(first.into_iter().map(|first| async move {
        let mut reservation = first.reservation;
        for exchanged in 1..=BACK_TO_BACK_REQUESTS {
            let body = if pull_through {
                pull_through_exchange(reservation, "/").await
            } else {
                exchange(reservation, "/").await
            };
            assert_eq!(body, b"ok");
            if exchanged == BACK_TO_BACK_REQUESTS {
                break;
            }
            reservation = acquire(pool).await.reservation;
        }
    }))
    .buffer_unordered(streams)
    .collect::<Vec<()>>()
    .await;
    take_waits_for_wakeup_for_test()
}

/// A stream that has read its HTTP/1.1 response to its end acquires its next
/// reservation before the driver's turn retires the exchange, and the pool
/// immediately selects that session or another free one. Acquisitions neither
/// wait for a returning session nor dial. K streams run over K sessions, at
/// K = 1, 8 and 64, with this module's pool parameters and with the router's,
/// through plain and pull-through exchanges.
#[test]
fn back_to_back_http1_acquisitions_never_wait_for_a_returning_session() {
    for streams in [1, 8, 64] {
        for router in [false, true] {
            for pull_through in [false, true] {
                let origin = Origin::start(CodecProtocol::Http11);
                let address = origin.address;
                let waits = run_on_worker(move |context| async move {
                    let (params, session) = if router {
                        router_http1()
                    } else {
                        (
                            params(u32::try_from(streams).expect("a small stream count")),
                            untimed(),
                        )
                    };
                    let pool = OwnedHttpSessionPool::new(
                        context.clone(),
                        socket_address(address),
                        params,
                        plaintext(CodecProtocol::Http11),
                        session,
                    )
                    .expect("the pool accepts its parameters");
                    back_to_back(&pool, streams, pull_through).await
                });
                assert_eq!(
                    (waits, origin.accepted()),
                    (0, streams),
                    "K = {streams}, router parameters: {router}, pull-through: {pull_through}: \
                     no acquisition waits and every exchange reuses the stream's sessions"
                );
            }
        }
    }
}

/// While an acquisition waits, the session that a back-to-back acquisition's
/// previous exchange is returning goes to the waiter, not to that acquisition,
/// because the retirement's `capacity_available` wakes the first waiter. Two
/// streams of five exchanges each share one connection and alternate, the
/// second taking the session after the first's first exchange.
#[test]
fn a_waiting_acquisition_takes_a_returning_session_before_its_consumer_acquires_again() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let order = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 1);
        let order = RefCell::new(Vec::new());
        let first = acquire(&pool).await;
        order.borrow_mut().push(0);
        let stream = |id: usize, first: Option<OwnedClientReservation>| {
            let (pool, order) = (&pool, &order);
            async move {
                let mut reservation = first;
                for exchanged in 1..=5 {
                    let held = match reservation.take() {
                        Some(held) => held,
                        None => {
                            let next = acquire(pool).await.reservation;
                            order.borrow_mut().push(id);
                            next
                        }
                    };
                    assert_eq!(exchange(held, "/").await, b"ok");
                    if exchanged == 5 {
                        break;
                    }
                }
            }
        };
        futures::join!(stream(1, None), stream(0, Some(first.reservation)));
        order.into_inner()
    });
    assert_eq!(
        (order, origin.accepted()),
        (vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 1], 1),
        "each stream's next acquisition waits behind the other's, which takes the session first"
    );
}

/// With reuse disabled, a back-to-back acquisition does not take the session
/// that its previous exchange is returning, which the release drains. It dials
/// a new connection instead.
#[test]
fn max_connections_zero_never_reuses_a_returning_session() {
    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let (replaced, body) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http11, 0);
        let first = acquire(&pool).await;
        assert_eq!(exchange(first.reservation, "/").await, b"ok");
        let second = acquire(&pool).await;
        let replaced = local(&first.session) != local(&second.session);
        (replaced, exchange(second.reservation, "/").await)
    });
    assert_eq!(
        (replaced, body, origin.accepted()),
        (true, b"ok".to_vec(), 2),
        "the next exchange dials anew"
    );
}

/// Runs `streams` streams of `BACK_TO_BACK_REQUESTS` exchanges each on `pool`, after making
/// `streams + idle` sessions idle when `idle` is nonzero. Every stream first holds a
/// reservation of its own, all together, then acquires its next one as soon as it has read its
/// previous response to its end. Returns this thread's `wait_for_wakeup` calls after the first
/// reservations were all held, and how many later acquisitions took a session other than
/// those the first reservations held.
async fn back_to_back_sessions(
    pool: &OwnedHttpSessionPool,
    streams: usize,
    idle: usize,
) -> (u64, usize) {
    if idle != 0 {
        idle_sessions(pool, streams + idle).await;
    }
    let first: Vec<_> = futures::stream::iter((0..streams).map(|_| acquire(pool)))
        .buffer_unordered(streams)
        .collect()
        .await;
    take_waits_for_wakeup_for_test();
    let held: Vec<_> = first.iter().map(|first| local(&first.session)).collect();
    let taken: Vec<Vec<TransportEndpoint>> =
        futures::stream::iter(first.into_iter().map(|first| async move {
            let mut taken = Vec::new();
            let mut reservation = first.reservation;
            for exchanged in 1..=BACK_TO_BACK_REQUESTS {
                assert_eq!(exchange(reservation, "/").await, b"ok");
                if exchanged == BACK_TO_BACK_REQUESTS {
                    break;
                }
                let next = acquire(pool).await;
                taken.push(local(&next.session));
                reservation = next.reservation;
            }
            taken
        }))
        .buffer_unordered(streams)
        .collect()
        .await;
    let elsewhere = taken
        .iter()
        .flatten()
        .filter(|session| !held.contains(session))
        .count();
    (take_waits_for_wakeup_for_test(), elsewhere)
}

/// K streams over K HTTP/1.1 sessions acquire their next reservation as soon as they have read
/// their previous response to its end. Under least-loaded selection, as under `FirstAvailable`,
/// acquisitions neither wait for a returning session nor dial. With K more sessions idle, every
/// acquisition takes a returning session (its stream's, or another that the first reservations
/// held) ahead of the idle ones, because a least-loaded acquisition ranks a returning session,
/// which has no exchange in flight and counts as busy, first.
#[test]
fn least_loaded_back_to_back_http1_acquisitions_never_wait_for_a_returning_session() {
    for selection in [
        OwnedSessionSelection::LeastLoaded,
        OwnedSessionSelection::FirstAvailable,
    ] {
        for streams in [8, 64] {
            for idle in [0, streams] {
                for router in [false, true] {
                    let origin = Origin::start(CodecProtocol::Http11);
                    let address = origin.address;
                    let (waits, elsewhere) = run_on_worker(move |context| async move {
                        let (params, session) = if router {
                            router_http1()
                        } else {
                            (
                                params(u32::try_from(2 * streams).expect("a small stream count")),
                                untimed(),
                            )
                        };
                        let pool =
                            pool_with(&context, address, CodecProtocol::Http11, params, session);
                        pool.set_session_selection(selection)
                            .expect("an open pool takes a selection");
                        back_to_back_sessions(&pool, streams, idle).await
                    });
                    assert_eq!(
                        (waits, elsewhere, origin.accepted()),
                        (0, 0, streams + idle),
                        "{selection:?}, K = {streams}, idle: {idle}, router parameters: {router}: \
                         (acquisitions that waited, acquisitions of a session no first \
                         reservation held, connections)"
                    );
                }
            }
        }
    }
}

/// Waiters do not starve under least-loaded selection. K streams share one HTTP/1.1
/// connection, each acquiring its next reservation as soon as it has read its previous response
/// to its end. While acquisitions wait, a stream's back-to-back acquisition does not take the
/// session it is returning, whose retirement wakes the first waiter, so the streams take the
/// connection in turn.
#[test]
fn least_loaded_back_to_back_acquisitions_starve_no_waiter() {
    const ROUNDS: usize = 3;
    for selection in [
        OwnedSessionSelection::LeastLoaded,
        OwnedSessionSelection::FirstAvailable,
    ] {
        for streams in [8, 64] {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let order = run_on_worker(move |context| async move {
                let pool = pool(&context, address, CodecProtocol::Http11, 1);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                let order = RefCell::new(Vec::new());
                let first = acquire(&pool).await;
                order.borrow_mut().push(0);
                let stream = |id: usize, first: Option<OwnedClientReservation>| {
                    let (pool, order) = (&pool, &order);
                    async move {
                        let mut reservation = first;
                        for exchanged in 1..=ROUNDS {
                            let held = match reservation.take() {
                                Some(held) => held,
                                None => {
                                    let next = acquire(pool).await.reservation;
                                    order.borrow_mut().push(id);
                                    next
                                }
                            };
                            assert_eq!(exchange(held, "/").await, b"ok");
                            if exchanged == ROUNDS {
                                break;
                            }
                        }
                    }
                };
                // The waiting streams queue in id order before the first stream's exchange.
                let mut streams_in_order: Vec<_> =
                    (1..streams).map(|id| stream(id, None)).collect();
                streams_in_order.push(stream(0, Some(first.reservation)));
                futures::future::join_all(streams_in_order).await;
                order.into_inner()
            });
            let rotation: Vec<usize> = (0..ROUNDS).flat_map(|_| 0..streams).collect();
            assert_eq!(
                (order, origin.accepted()),
                (rotation, 1),
                "{selection:?}, K = {streams}: the streams take the connection in turn"
            );
        }
    }
}

/// A reservation taken on a returning HTTP/1.1 session and dropped before its driver's turn
/// leaves the session returning without a pool event, and the pool's index lists it again. The
/// next acquisition takes it in its first poll, under either selection.
#[test]
fn a_reservation_dropped_on_a_returning_session_leaves_it_selectable() {
    for selection in [
        OwnedSessionSelection::FirstAvailable,
        OwnedSessionSelection::LeastLoaded,
    ] {
        let origin = Origin::start(CodecProtocol::Http11);
        let address = origin.address;
        let (taken, retaken) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, CodecProtocol::Http11, 4);
            pool.set_session_selection(selection)
                .expect("an open pool takes a selection");
            let first = acquire(&pool).await;
            assert_eq!(exchange(first.reservation, "/").await, b"ok");
            // No yield since the read: the session is returning.
            let taken = acquire(&pool).await;
            let same = local(&taken.session) == local(&first.session);
            drop(taken.reservation);
            let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
            let Poll::Ready(next) = poll_once(next.as_mut()).await else {
                panic!("the returning session is taken in the first poll");
            };
            let next = next.expect("the acquisition succeeds");
            let retaken = local(&next.session) == local(&first.session);
            assert_eq!(exchange(next.reservation, "/").await, b"ok");
            (same, retaken)
        });
        assert_eq!(
            (taken, retaken, origin.accepted()),
            (true, true, 1),
            "{selection:?}: (the back-to-back acquisition took the returning session, the next \
             took it again after the drop, connections)"
        );
    }
}

/// A returning HTTP/1.1 session whose slot a reservation took, through the pool or on the
/// session, is not claimable. Its driver's turn retires the exchange without announcing
/// capacity, since the slot is taken, so a wake would not reach an acquisition waiting for it.
/// While that reservation is held, an acquisition without a free session dials, and so does
/// the next one, and each is served on a connection of its own.
#[test]
fn acquisitions_dial_past_a_returning_session_whose_slot_is_reserved() {
    let cases = [
        OwnedSessionSelection::FirstAvailable,
        OwnedSessionSelection::LeastLoaded,
    ]
    .into_iter()
    .flat_map(|selection| [false, true].map(|router| (selection, router)))
    // (whether the reservation is taken on the session rather than through the pool)
    .flat_map(|(selection, router)| [false, true].map(|direct| (selection, router, direct)));
    let observed: Vec<_> = cases
        .map(|(selection, router, direct)| {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let (attempts, served) = run_on_worker(move |context| async move {
                let (params, session) = if router {
                    router_http1()
                } else {
                    (params(4), untimed())
                };
                let pool = pool_with(&context, address, CodecProtocol::Http11, params, session);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                let first = acquire(&pool).await;
                assert_eq!(exchange(first.reservation, "/").await, b"ok");
                // No yield since the read: the session is returning, and the pool has no waiter.
                let held = if direct {
                    first
                        .session
                        .reserve_request()
                        .expect("a returning slot admits a reservation")
                } else {
                    let taken = acquire(&pool).await;
                    assert!(
                        local(&taken.session) == local(&first.session),
                        "the acquisition takes the returning session"
                    );
                    taken.reservation
                };
                let attempts = pool.connection_attempts_for_test();
                let mut next = pin!(pool.get_session_with_reservation(CancellationToken::never()));
                assert!(poll_once(next.as_mut()).await.is_pending());
                let mut later = pin!(pool.get_session_with_reservation(CancellationToken::never()));
                assert!(poll_once(later.as_mut()).await.is_pending());
                let attempts = pool.connection_attempts_for_test() - attempts;
                // An acquisition that is never woken resolves only when its wait times out or
                // the held reservation is released. Both happen after the two bounds below.
                let bound = TIMEOUT / 4;
                let next = tokio::time::timeout(bound, next)
                    .await
                    .ok()
                    .and_then(Result::ok);
                let later = tokio::time::timeout(bound, later)
                    .await
                    .ok()
                    .and_then(Result::ok);
                let mut served = Vec::new();
                for acquired in [next, later] {
                    let Some(acquired) = acquired else {
                        served.push(false);
                        continue;
                    };
                    served.push(local(&acquired.session) != local(&first.session));
                    assert_eq!(exchange(acquired.reservation, "/").await, b"ok");
                }
                drop(held);
                (attempts, served)
            });
            (
                selection,
                router,
                direct,
                attempts,
                served,
                origin.accepted(),
            )
        })
        .collect();
    let expected: Vec<_> = observed
        .iter()
        .map(|&(selection, router, direct, ..)| (selection, router, direct, 2, vec![true, true], 3))
        .collect();
    assert_eq!(
        observed, expected,
        "(selection, router parameters, reserved on the session rather than through the pool, \
         attempts at the acquisitions' first polls, each served on a connection other than the \
         returning session's, connections)"
    );
}

/// An acquisition waits for a returning HTTP/1.1 session while another waits for its dial. A
/// reservation then taken on that session's handle takes the slot, whose retirement does not
/// announce capacity, so the reservation wakes the waiting acquisition, which dials.
#[test]
fn a_reservation_on_a_claimed_returning_session_wakes_the_acquisition_waiting_for_it() {
    let cases = [
        OwnedSessionSelection::FirstAvailable,
        OwnedSessionSelection::LeastLoaded,
    ]
    .into_iter()
    .flat_map(|selection| [false, true].map(|router| (selection, router)));
    let observed: Vec<_> = cases
        .map(|(selection, router)| {
            let origin = Origin::start(CodecProtocol::Http11);
            let address = origin.address;
            let (attempts, served) = run_on_worker(move |context| async move {
                let (params, session) = if router {
                    router_http1()
                } else {
                    (params(4), untimed())
                };
                let pool = pool_with(&context, address, CodecProtocol::Http11, params, session);
                pool.set_session_selection(selection)
                    .expect("an open pool takes a selection");
                let one = acquire(&pool).await;
                let two = acquire(&pool).await;
                let first = submit(one.reservation, "/").await;
                let second = submit(two.reservation, "/").await;
                until(|| first.request.capacity_released() && second.request.capacity_released())
                    .await;
                let attempts = pool.connection_attempts_for_test();
                let mut earlier =
                    pin!(pool.get_session_with_reservation(CancellationToken::never()));
                assert!(
                    poll_once(earlier.as_mut()).await.is_pending(),
                    "no slot is free"
                );
                // Read from the buffer, with no yield: the second session's slot is returning.
                assert_eq!(read_body(second).await, b"ok");
                let mut waiting =
                    pin!(pool.get_session_with_reservation(CancellationToken::never()));
                assert!(poll_once(waiting.as_mut()).await.is_pending());
                let claimed = pool.connection_attempts_for_test() - attempts;
                let held = two
                    .session
                    .reserve_request()
                    .expect("a returning slot admits a reservation");
                // An acquisition that is never woken resolves only when its wait times out or
                // the held reservation is released. Both happen after the two bounds below.
                let bound = TIMEOUT / 4;
                let earlier = tokio::time::timeout(bound, earlier)
                    .await
                    .ok()
                    .and_then(Result::ok);
                let waiting = tokio::time::timeout(bound, waiting)
                    .await
                    .ok()
                    .and_then(Result::ok);
                let mut served = Vec::new();
                for acquired in [earlier, waiting] {
                    let Some(acquired) = acquired else {
                        served.push(false);
                        continue;
                    };
                    served.push(local(&acquired.session) != local(&two.session));
                    assert_eq!(exchange(acquired.reservation, "/").await, b"ok");
                }
                drop(held);
                assert_eq!(read_body(first).await, b"ok");
                (
                    [claimed, pool.connection_attempts_for_test() - attempts],
                    served,
                )
            });
            (selection, router, attempts, served, origin.accepted())
        })
        .collect();
    let expected: Vec<_> = observed
        .iter()
        .map(|&(selection, router, ..)| (selection, router, [1, 2], vec![true, true], 4))
        .collect();
    assert_eq!(
        observed, expected,
        "(selection, router parameters, attempts once the second acquisition waits and in all, \
         each served on a connection other than the returning session's, connections)"
    );
}

/// Sends `/` on `reservation`, as a pull-through request when
/// `pull_through`, and returns the 200 response's whole body.
async fn exchange_root(reservation: OwnedClientReservation, pull_through: bool) -> Vec<u8> {
    if pull_through {
        pull_through_exchange(reservation, "/").await
    } else {
        exchange(reservation, "/").await
    }
}

/// Runs `streams` streams of `BACK_TO_BACK_REQUESTS` back-to-back exchanges
/// each on sessions the pool already opened, through pull-through exchanges
/// when `pull_through`, and returns this thread's pool batch passes from the
/// streams' first acquisitions to their last exchanges' retirements. Before
/// that, `streams` reservations held together open the sessions, each
/// exchanges once, and every session goes idle.
async fn batch_passes_back_to_back(
    pool: &OwnedHttpSessionPool,
    streams: usize,
    pull_through: bool,
) -> u64 {
    let warm: Vec<_> = futures::stream::iter((0..streams).map(|_| acquire(pool)))
        .buffer_unordered(streams)
        .collect()
        .await;
    let opened: Vec<OwnedClientSession> = futures::stream::iter(warm)
        .then(|acquired| async move {
            assert_eq!(
                exchange_root(acquired.reservation, pull_through).await,
                b"ok"
            );
            acquired.session
        })
        .collect()
        .await;
    for session in &opened {
        settle(session).await;
    }
    take_batch_passes_for_test();
    let last: Vec<OwnedClientSession> = futures::stream::iter((0..streams).map(|_| async move {
        let mut last = None;
        for _ in 0..BACK_TO_BACK_REQUESTS {
            let acquired = acquire(pool).await;
            assert_eq!(
                exchange_root(acquired.reservation, pull_through).await,
                b"ok"
            );
            last = Some(acquired.session);
        }
        last.expect("a stream exchanges at least once")
    }))
    .buffer_unordered(streams)
    .collect()
    .await;
    for session in &last {
        settle(session).await;
    }
    take_batch_passes_for_test()
}

/// Back-to-back exchanges on open sessions neither wake a waiter nor retire a
/// session, so the pool's batch of waiters and sessions to drain stays empty.
/// The pool does not pass over it at an acquisition's step or a session event.
#[test]
fn back_to_back_exchanges_make_no_pass_over_an_empty_pool_batch() {
    let cases = [1, 8].into_iter().flat_map(|streams| {
        [false, true]
            .into_iter()
            .flat_map(move |router| {
                [false, true]
                    .into_iter()
                    .map(move |pull_through| (CodecProtocol::Http11, streams, router, pull_through))
            })
            .chain([(CodecProtocol::Http2, streams, false, false)])
    });
    let mut observed: Vec<_> = cases
        .map(|(protocol, streams, router, pull_through)| {
            let origin = Origin::start(protocol);
            let address = origin.address;
            let passes = run_on_worker(move |context| async move {
                let (params, session) = if router {
                    router_http1()
                } else {
                    (
                        params(u32::try_from(streams).expect("a small stream count")),
                        untimed(),
                    )
                };
                let pool = pool_with(&context, address, protocol, params, session);
                batch_passes_back_to_back(&pool, streams, pull_through).await
            });
            (
                format!(
                    "{protocol:?}, K = {streams}, router parameters: {router}, pull-through: \
                     {pull_through}"
                ),
                passes,
            )
        })
        .collect();

    let origin = Origin::start(CodecProtocol::Http11);
    let address = origin.address;
    let passes = run_on_worker(move |context| async move {
        let pool = HttpSessionPool::new(
            context,
            socket_address(address),
            params(1),
            plaintext(CodecProtocol::Http11),
            untimed(),
        );
        uniplex_one_exchange(&pool).await;
        take_batch_passes_for_test();
        for _ in 0..BACK_TO_BACK_REQUESTS {
            uniplex_one_exchange(&pool).await;
        }
        take_batch_passes_for_test()
    });
    observed.push(("HttpSessionPool, K = 1".to_owned(), passes));
    let expected: Vec<_> = observed.iter().map(|(case, _)| (case.clone(), 0)).collect();
    assert_eq!(observed, expected, "no pass over an empty batch");
}

/// A non-empty waiter batch is signalled in one pass. With one connection of
/// one stream held, a second acquisition waits. Releasing the held reservation
/// wakes it, and its acquisition has no other waiter to signal or session to
/// drain.
#[test]
fn a_released_session_wakes_its_waiter_in_one_pool_batch_pass() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (pending, passes, same) = run_on_worker(move |context| async move {
            let pool = pool_with(&context, address, protocol, params(1), one_stream(protocol));
            let first = acquire(&pool).await;
            let second = pool.get_session_with_reservation(CancellationToken::never());
            tokio::pin!(second);
            let pending = tokio::select! {
                _ = &mut second => false,
                () = tokio::time::sleep(Duration::from_millis(100)) => true,
            };
            take_batch_passes_for_test();
            drop(first.reservation);
            let second = second.await.expect("the waiter is served");
            let passes = take_batch_passes_for_test();
            (
                pending,
                passes,
                local(&first.session) == local(&second.session),
            )
        });
        assert_eq!(
            (pending, passes, same),
            (true, 1, true),
            "{protocol:?}: one pass signals the one waiter, which takes the released session"
        );
    }
}

/// `stats` after one exchange on each protocol's pool, and after the pool's drain:
/// one dial, which the exchange's acquisition waited for, one session opened, and
/// that session closed by the pool's drain, which closed its admission first.
#[test]
fn stats_count_the_sessions_a_pool_opens_and_closes() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (served, drained) = run_on_worker(move |context| async move {
            let pool = pool(&context, address, protocol, 1);
            one_exchange(&pool).await;
            let served = pool.stats();
            pool.drain_and_join().await.expect("pool joins");
            (served, pool.stats())
        });
        let served_expected = OwnedPoolStats {
            sessions_opened: 1,
            connect_attempts: 1,
            peak_waiters: 1,
            ..OwnedPoolStats::default()
        };
        assert_eq!(served, served_expected, "{protocol:?}: after one exchange");
        assert_eq!(
            drained,
            OwnedPoolStats {
                sessions_closed: 1,
                local_drains: 1,
                ..served_expected
            },
            "{protocol:?}: after the drain"
        );
    }
}

/// A dial refused three times, its attempts all retried for the one waiter.
#[test]
fn stats_count_connect_attempts_and_failures() {
    let reserved = reserve(SocketAddr::from(([127, 0, 0, 1], 0)), false);
    let address = reserved.local_addr().expect("address");
    let (error, stats) = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http2,
            PoolParams {
                max_connection_attempts: 3,
                ..params(1)
            },
            untimed(),
        );
        let error = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .err();
        (error, pool.stats())
    });
    assert!(
        matches!(error, Some(PoolError::ConnectFailed { .. })),
        "{error:?}"
    );
    assert_eq!(
        stats,
        OwnedPoolStats {
            connect_attempts: 3,
            connect_failures: 3,
            peak_waiters: 1,
            ..OwnedPoolStats::default()
        }
    );
}

/// Two acquisitions wait for the only stream of a pool with `max_waiters` 2, a third is
/// refused, a step past the deadline times out, and the two waiters time out.
#[test]
fn stats_count_waiters_timeouts_and_max_waiter_rejections() {
    let origin = Origin::start(CodecProtocol::Http2);
    let address = origin.address;
    let (waiting, refused, ended) = run_on_worker(move |context| async move {
        let pool = pool_with(
            &context,
            address,
            CodecProtocol::Http2,
            PoolParams {
                max_waiters: 2,
                connect_timeout: Duration::from_millis(300),
                ..params(1)
            },
            one_stream(CodecProtocol::Http2),
        );
        let _held = acquire(&pool).await;
        let mut first = pin!(pool.get_session_with_reservation(CancellationToken::never()));
        let mut second = pin!(pool.get_session_with_reservation(CancellationToken::never()));
        assert!(poll_once(first.as_mut()).await.is_pending());
        assert!(poll_once(second.as_mut()).await.is_pending());
        let waiting = pool.stats();
        let refused = pool
            .get_session_with_reservation(CancellationToken::never())
            .await
            .err();
        assert!(
            matches!(refused, Some(PoolError::MaxWaiters)),
            "{refused:?}"
        );
        let late = pool.acquisition_step_for_test(Instant::now() - TIMEOUT);
        assert!(matches!(late, Err(PoolError::Timeout)), "{late:?}");
        let refused_stats = pool.stats();
        for waiter in [first, second] {
            let error = waiter.await.err();
            assert!(matches!(error, Some(PoolError::Timeout)), "{error:?}");
        }
        (waiting, refused_stats, pool.stats())
    });
    let waiting_expected = OwnedPoolStats {
        sessions_opened: 1,
        connect_attempts: 1,
        waiters: 2,
        peak_waiters: 2,
        ..OwnedPoolStats::default()
    };
    assert_eq!(waiting, waiting_expected, "two acquisitions wait");
    let refused_expected = OwnedPoolStats {
        acquisition_timeouts: 1,
        max_waiters_rejections: 1,
        ..waiting_expected
    };
    assert_eq!(
        refused, refused_expected,
        "a third is refused, a late step times out"
    );
    assert_eq!(
        ended,
        OwnedPoolStats {
            waiters: 0,
            acquisition_timeouts: 3,
            ..refused_expected
        },
        "both waiters time out"
    );
}

const HEADERS: u8 = 1;
const RST_STREAM: u8 = 3;
const SETTINGS: u8 = 4;
const GOAWAY: u8 = 7;
const ACK: u8 = 1;
/// END_STREAM | END_HEADERS.
const END_STREAM_HEADERS: u8 = 0x5;
/// HPACK: `:status: 200`, static table index 8.
const STATUS_200: [u8; 1] = [0x88];
const NO_ERROR: u32 = 0;
const INTERNAL_ERROR: u32 = 2;
const REFUSED_STREAM: u32 = 7;
const ENHANCE_YOUR_CALM: u32 = 0xb;

fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len())
        .expect("small frame")
        .to_be_bytes();
    let mut frame = vec![length[1], length[2], length[3], kind, flags];
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn goaway(last_stream: u32, code: u32) -> Vec<u8> {
    let mut payload = last_stream.to_be_bytes().to_vec();
    payload.extend_from_slice(&code.to_be_bytes());
    frame(GOAWAY, 0, 0, &payload)
}

/// Reads one frame: its type, flags and stream; `None` at EOF or a reset.
fn read_frame(stream: &mut TcpStream) -> Option<(u8, u8, u32)> {
    let mut header = [0; 9];
    stream.read_exact(&mut header).ok()?;
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
    let mut payload = vec![0; usize::try_from(length).expect("small frame")];
    stream.read_exact(&mut payload).ok()?;
    let id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    Some((header[3], header[4], id))
}

/// Reads the client's frames, acknowledging its SETTINGS, until it has sent `count`
/// HEADERS frames, and returns their streams.
fn read_requests(stream: &mut TcpStream, count: usize) -> Vec<u32> {
    let mut requests = Vec::new();
    while requests.len() < count {
        let (kind, flags, id) = read_frame(stream).expect("a client frame");
        if kind == SETTINGS && flags & ACK == 0 {
            stream
                .write_all(&frame(SETTINGS, ACK, 0, &[]))
                .expect("peer acknowledges");
        } else if kind == HEADERS {
            requests.push(id);
        }
    }
    requests
}

/// A raw HTTP/2 peer for one connection on loopback, on its own thread, whose accept and
/// reads each give up after `TIMEOUT`. Dropping it joins the thread and raises a panic of
/// its script, unless the test already fails.
struct RawPeer {
    address: SocketAddr,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for RawPeer {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take()
            && !std::thread::panicking()
            && let Err(panic) = thread.join()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

/// Starts a [`RawPeer`] that reads the client's preface, sends empty SETTINGS, runs
/// `script`, then reads until the client closes.
fn raw_http2_peer(script: impl FnOnce(&mut TcpStream) + Send + 'static) -> RawPeer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer binds");
    let address = listener.local_addr().expect("peer address");
    listener
        .set_nonblocking(true)
        .expect("peer polls its listener");
    let thread = std::thread::spawn(move || {
        let started = Instant::now();
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && started.elapsed() < TIMEOUT =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("peer accepts: {error}"),
            }
        };
        stream.set_nonblocking(false).expect("peer blocks");
        stream
            .set_read_timeout(Some(TIMEOUT))
            .expect("peer bounds its reads");
        let mut preface = [0; 24];
        stream.read_exact(&mut preface).expect("client preface");
        stream
            .write_all(&frame(SETTINGS, 0, 0, &[]))
            .expect("peer SETTINGS");
        script(&mut stream);
        while read_frame(&mut stream).is_some() {}
    });
    RawPeer {
        address,
        thread: Some(thread),
    }
}

/// A NO_ERROR GOAWAY that covers the one request in flight is counted when it closes the
/// session's admission, while the request waits for its response. The session closes
/// cleanly once the response ends.
#[test]
fn stats_count_a_no_error_goaway_when_it_closes_admission() {
    let (release, released) = mpsc::channel::<()>();
    let peer = raw_http2_peer(move |stream| {
        let requests = read_requests(stream, 1);
        stream
            .write_all(&goaway(requests[0], NO_ERROR))
            .expect("peer drains");
        released.recv().expect("the client releases the response");
        stream
            .write_all(&frame(
                HEADERS,
                END_STREAM_HEADERS,
                requests[0],
                &STATUS_200,
            ))
            .expect("peer responds");
    });
    let address = peer.address;
    let (admission_closed, closed) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let acquired = acquire(&pool).await;
        let response = submit(acquired.reservation, "/").await;
        until(|| pool.stats().peer_goaways != 0).await;
        let admission_closed = pool.stats();
        release.send(()).expect("the peer waits");
        assert_eq!(read_body(response).await, b"");
        acquired
            .session
            .wait_closed()
            .await
            .expect("the session closes cleanly");
        (admission_closed, pool.stats())
    });
    let admission_closed_expected = OwnedPoolStats {
        sessions_opened: 1,
        connect_attempts: 1,
        peak_waiters: 1,
        peer_goaways: 1,
        ..OwnedPoolStats::default()
    };
    assert_eq!(
        admission_closed, admission_closed_expected,
        "counted while the session still has a stream in flight"
    );
    assert_eq!(
        closed,
        OwnedPoolStats {
            sessions_closed: 1,
            ..admission_closed_expected
        }
    );
}

/// A GOAWAY with an error code fails the session. It is counted as a peer GOAWAY and as
/// an error.
#[test]
fn stats_count_a_goaway_with_an_error_code() {
    let peer = raw_http2_peer(|stream| {
        let requests = read_requests(stream, 1);
        stream
            .write_all(&goaway(requests[0], ENHANCE_YOUR_CALM))
            .expect("peer fails the connection");
    });
    let address = peer.address;
    let (status, closed, stats) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let acquired = acquire(&pool).await;
        let (status, _) = outcome(acquired.reservation).await;
        let closed = acquired
            .session
            .wait_closed()
            .await
            .err()
            .map(|error| error.code());
        until(|| pool.stats().sessions_closed != 0).await;
        (status, closed, pool.stats())
    });
    assert_eq!(
        (status.is_err(), closed),
        (true, Some(HttpErrorCode::ENHANCE_YOUR_CALM)),
        "{status:?}"
    );
    assert_eq!(
        stats,
        OwnedPoolStats {
            sessions_opened: 1,
            sessions_closed: 1,
            connect_attempts: 1,
            peak_waiters: 1,
            peer_goaways: 1,
            peer_goaway_errors: 1,
            ..OwnedPoolStats::default()
        }
    );
}

/// A session past `max_age` that an acquisition retires, and the pool's drain of its
/// replacement, are local drains. Each session counts once, under the reason that closed
/// its admission first, so a GOAWAY that the origin sends back is not counted.
#[test]
fn stats_count_local_drains_once_per_session() {
    for protocol in PROTOCOLS {
        let origin = Origin::start(protocol);
        let address = origin.address;
        let (aged, drained) = run_on_worker(move |context| async move {
            let pool = pool_with(
                &context,
                address,
                protocol,
                PoolParams {
                    // Sampled between 70% and 130%.
                    max_age: Duration::from_millis(40),
                    ..params(1)
                },
                untimed(),
            );
            let first = one_exchange(&pool).await;
            tokio::time::sleep(Duration::from_millis(120)).await;
            let second = one_exchange(&pool).await;
            assert_ne!(
                local(&first),
                local(&second),
                "the aged session is replaced"
            );
            first.wait_closed().await.expect("the aged session closes");
            until(|| pool.stats().sessions_closed == 1).await;
            let aged = pool.stats();
            pool.drain_and_join().await.expect("pool joins");
            (aged, pool.stats())
        });
        assert_eq!(
            (
                aged.sessions_opened,
                aged.sessions_closed,
                aged.local_drains,
                aged.peer_goaways
            ),
            (2, 1, 1, 0),
            "{protocol:?}: (opened, closed, local drains, peer GOAWAYs) once the aged session closed"
        );
        assert_eq!(
            (
                drained.sessions_opened,
                drained.sessions_closed,
                drained.local_drains,
                drained.peer_goaways
            ),
            (2, 2, 2, 0),
            "{protocol:?}: (opened, closed, local drains, peer GOAWAYs) after the pool's drain"
        );
    }
}

/// The peer refuses one stream and resets another, which are counted while the session
/// stays open. Then a GOAWAY whose last stream precedes a request in flight refuses it.
#[test]
fn stats_count_the_streams_a_peer_refuses_or_resets() {
    let peer = raw_http2_peer(|stream| {
        let requests = read_requests(stream, 2);
        let mut reply = frame(RST_STREAM, 0, requests[0], &REFUSED_STREAM.to_be_bytes());
        reply.extend(frame(
            RST_STREAM,
            0,
            requests[1],
            &INTERNAL_ERROR.to_be_bytes(),
        ));
        stream.write_all(&reply).expect("peer resets");
        let requests = read_requests(stream, 2);
        let mut reply = frame(HEADERS, END_STREAM_HEADERS, requests[0], &STATUS_200);
        reply.extend(goaway(requests[0], NO_ERROR));
        stream.write_all(&reply).expect("peer answers and drains");
    });
    let address = peer.address;
    let (reset, reset_open, goaway_refused) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let mut rounds = Vec::new();
        for _ in 0..2 {
            let acquired = hold(&pool, 2).await;
            let session = acquired[0].session.clone();
            let mut outcomes: Vec<(Result<u16, HttpErrorCode>, bool)> = futures::stream::iter(
                acquired
                    .into_iter()
                    .map(|acquired| outcome(acquired.reservation)),
            )
            .buffered(2)
            .collect()
            .await;
            outcomes.sort();
            rounds.push((outcomes, pool.stats(), !session.is_finished()));
        }
        let [(first, reset, open), (second, refused, _)] = &rounds[..] else {
            panic!("two rounds ran");
        };
        assert_eq!(
            first,
            &[
                (Err(HttpErrorCode::INTERNAL_ERROR), false),
                (Err(HttpErrorCode::REFUSED_STREAM), true),
            ],
            "the first round's outcomes"
        );
        assert_eq!(
            second,
            &[(Ok(200), false), (Err(HttpErrorCode::REFUSED_STREAM), true)],
            "the second round's outcomes"
        );
        (*reset, *open, *refused)
    });
    assert_eq!(
        (reset.refused_streams, reset.reset_streams, reset_open),
        (1, 1, true),
        "(refused, reset, session open) after the resets"
    );
    assert_eq!(
        (
            goaway_refused.refused_streams,
            goaway_refused.reset_streams,
            goaway_refused.peer_goaways
        ),
        (2, 1, 1),
        "(refused, reset, peer GOAWAYs) after the GOAWAY"
    );
}

/// Resets on a session that the pool dropped when a GOAWAY closed its admission, and whose
/// handles are all gone, are counted: an RST_STREAM on a stream the GOAWAY covered, and a
/// second GOAWAY that refuses another.
#[test]
fn stats_count_resets_on_a_session_the_pool_dropped() {
    let peer = raw_http2_peer(|stream| {
        let requests = read_requests(stream, 2);
        let mut reply = goaway(requests[1], NO_ERROR);
        reply.extend(frame(
            RST_STREAM,
            0,
            requests[0],
            &INTERNAL_ERROR.to_be_bytes(),
        ));
        reply.extend(goaway(requests[0], NO_ERROR));
        stream
            .write_all(&reply)
            .expect("peer drains, resets and refuses");
    });
    let address = peer.address;
    let (outcomes, stats, sessions) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let reservations: Vec<_> = hold(&pool, 2)
            .await
            .into_iter()
            .map(|acquired| acquired.reservation)
            .collect();
        let mut outcomes: Vec<(Result<u16, HttpErrorCode>, bool)> =
            futures::stream::iter(reservations.into_iter().map(outcome))
                .buffered(2)
                .collect()
                .await;
        outcomes.sort();
        (outcomes, pool.stats(), pool.session_count())
    });
    assert_eq!(
        outcomes,
        [
            (Err(HttpErrorCode::INTERNAL_ERROR), false),
            (Err(HttpErrorCode::REFUSED_STREAM), true),
        ]
    );
    assert_eq!(
        (
            stats.refused_streams,
            stats.reset_streams,
            stats.peer_goaways,
            sessions
        ),
        (1, 1, 1, 0),
        "(refused, reset, peer GOAWAYs, pooled sessions)"
    );
}

/// A peer's NO_ERROR reset after a stream's whole response stops only its upload and is not
/// counted. One before the response fails the stream and is counted.
#[test]
fn stats_count_a_no_error_reset_only_before_the_whole_response() {
    let peer = raw_http2_peer(|stream| {
        let requests = read_requests(stream, 2);
        let mut reply = frame(HEADERS, END_STREAM_HEADERS, requests[0], &STATUS_200);
        reply.extend(frame(RST_STREAM, 0, requests[0], &NO_ERROR.to_be_bytes()));
        reply.extend(frame(RST_STREAM, 0, requests[1], &NO_ERROR.to_be_bytes()));
        stream
            .write_all(&reply)
            .expect("peer answers the upload and resets both");
    });
    let address = peer.address;
    let (body, get, stats) = run_on_worker(move |context| async move {
        let pool = pool(&context, address, CodecProtocol::Http2, 1);
        let mut held = hold(&pool, 2).await;
        let get = held.pop().expect("two acquisitions");
        let upload = held.pop().expect("two acquisitions");
        // Larger than the peer's initial windows, which it never opens: the upload stays open.
        let size = 128 * 1024;
        let upload_body =
            BufQueue::copy_from_slice(&vec![0x5a; size], size).expect("bounded upload");
        let mut source = HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(upload_body))
                .expect("valid request"),
        ));
        let mut head = source.read_header_event().await.expect("request head");
        head.headers_mut()
            .headers_mut()
            .add("host", "origin.example");
        let response = upload
            .reservation
            .submit(head, source, CancellationToken::never())
            .await
            .expect("request is committed");
        let (body, get) = tokio::join!(read_body(response), outcome(get.reservation));
        (body, get.0, pool.stats())
    });
    assert_eq!(
        (body, get),
        (Vec::new(), Err(HttpErrorCode::CANCEL)),
        "the upload's response stays whole; the request reset before its response fails"
    );
    assert_eq!(
        (stats.refused_streams, stats.reset_streams),
        (0, 1),
        "(refused, reset)"
    );
}
