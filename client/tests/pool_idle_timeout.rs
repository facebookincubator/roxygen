/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `PoolParams::idle_timeout` retires sessions without an exchange, on the
//! schedule that `PoolParams::idle_sweep` selects, and
//! `PoolParams::reuse_order` selects the idle session that serves next.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::worker::WorkerCall;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::ServerConfig;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexResponse;

use crate::connector::ClientSession;
use crate::connector::ConnectionParams;
use crate::connector::LocalClientFuture;
use crate::connector::SessionParams;
use crate::pool::GetSessionResult;
use crate::pool::HttpSessionPool;
use crate::pool::IdleSessionObserver;
use crate::pool::IdleSweep;
use crate::pool::PoolError;
use crate::pool::PoolParams;
use crate::pool::SessionReuseOrder;

/// The idle timeout, and the scheduling slack that tests reusing a
/// just-released session allow.
const IDLE: Duration = Duration::from_millis(100);
/// `/slow` streams `SLOW_CHUNKS` chunks this far apart, for four idle timeouts in all.
const CHUNK_GAP: Duration = Duration::from_millis(40);
const SLOW_CHUNKS: usize = 10;
/// `/silent` responds after this long without sending anything first.
const SILENCE: Duration = Duration::from_millis(300);
/// `/large` responds with this many bytes, more than the transport buffers.
const LARGE: usize = 8 << 20;

/// A chunked 200 response whose chunks arrive `CHUNK_GAP` apart.
struct SlowBody {
    header: Option<HttpHeaderEvent>,
    remaining: usize,
}

impl SlowBody {
    fn new() -> Self {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("test status is valid");
        message
            .set_status_message(HttpMessage::default_reason(200))
            .expect("test reason is valid");
        message.set_http_version(1, 1);
        Self {
            header: Some(HttpHeaderEvent::new(message, false).expect("header event is valid")),
            remaining: SLOW_CHUNKS,
        }
    }
}

impl HttpSource for SlowBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.remaining = self.remaining.saturating_sub(1);
        let eom = self.remaining == 0;
        Box::pin(async move {
            tokio::time::sleep(CHUNK_GAP).await;
            let chunk = BufQueue::from_bytes(Bytes::from_static(b"chunk"), 5)
                .expect("test chunk is bounded");
            Ok(HttpBodyEvent::body(chunk, eom))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

fn fixed_ok() -> HttpSourceHolder {
    let body =
        BufQueue::from_bytes(Bytes::from_static(b"ok"), 2).expect("test response is bounded");
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_fixed_response(200, Some(body)).expect("test response is valid"),
    ))
}

/// Responds to `/slow` with `SlowBody`, `/silent` with `ok` after `SILENCE`,
/// `/large` with `LARGE` bytes, and anything else with `ok`.
struct IdleTestHandler;

impl HttpHandler for IdleTestHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let mut head = request.read_header_event().await?;
            match head.headers_mut().path() {
                Ok(b"/slow") => Ok(HttpSourceHolder::new(Box::new(SlowBody::new()))),
                Ok(b"/silent") => {
                    tokio::time::sleep(SILENCE).await;
                    Ok(fixed_ok())
                }
                Ok(b"/large") => {
                    let body = BufQueue::from_bytes(Bytes::from(vec![b'x'; LARGE]), LARGE)
                        .expect("test response is bounded");
                    Ok(HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_fixed_response(200, Some(body))
                            .expect("test response is valid"),
                    )))
                }
                _ => Ok(fixed_ok()),
            }
        })
    }
}

fn start_server() -> ScopedHttpServer {
    ScopedHttpServer::start(
        ServerConfig::default(),
        Arc::new(|_| Rc::new(IdleTestHandler) as HttpHandlerRef),
        None,
    )
    .expect("test server starts")
}

fn bounded<T>(call: WorkerCall<T>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), call)
                .await
                .expect("bounded client task")
        })
        .expect("client task completes");
}

/// Runs the client task on a `ManagedWorker` and again on a `HostedWorker`, where
/// pool tasks are independent Tokio tasks polled only by their own wakeups.
fn run_on_worker<F>(factory: F)
where
    F: FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = ()> + 'static>>
        + Clone
        + Send
        + 'static,
{
    let worker = ManagedWorker::new(64).expect("client worker starts");
    bounded(
        worker
            .handle()
            .call_local(factory.clone())
            .expect("client task is admitted"),
    );
    worker.shutdown().expect("client worker shuts down");

    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(64).expect("nonzero capacity"),
        thread_name: "pool-idle".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted client worker starts");
    bounded(
        worker
            .handle()
            .call_local(factory)
            .expect("client task is admitted"),
    );
    let completion = worker.completion();
    drop(worker);
    assert!(
        matches!(*completion.borrow(), Some(Ok(()))),
        "the hosted worker joins cleanly"
    );
}

fn pool(context: WorkerContext, address: SocketAddr, max_connections: u32) -> HttpSessionPool {
    pool_with_order(
        context,
        address,
        max_connections,
        SessionReuseOrder::default(),
    )
}

fn pool_with_order(
    context: WorkerContext,
    address: SocketAddr,
    max_connections: u32,
    reuse_order: SessionReuseOrder,
) -> HttpSessionPool {
    pool_with(context, address, max_connections, reuse_order, IDLE)
}

fn pool_with(
    context: WorkerContext,
    address: SocketAddr,
    max_connections: u32,
    reuse_order: SessionReuseOrder,
    idle_timeout: Duration,
) -> HttpSessionPool {
    pool_with_sweep(
        context,
        address,
        max_connections,
        reuse_order,
        idle_timeout,
        IdleSweep::AtExpiry,
    )
}

fn pool_with_sweep(
    context: WorkerContext,
    address: SocketAddr,
    max_connections: u32,
    reuse_order: SessionReuseOrder,
    idle_timeout: Duration,
    idle_sweep: IdleSweep,
) -> HttpSessionPool {
    new_pool(
        context,
        address,
        PoolParams {
            max_connections,
            idle_timeout,
            reuse_order,
            idle_sweep,
            ..PoolParams::default()
        },
        None,
    )
}

/// A native pool to `address` with the connection limit, idle timeout, reuse
/// order and sweep from `params`. Its sessions report to `lifecycle_observer`.
fn new_pool(
    context: WorkerContext,
    address: SocketAddr,
    params: PoolParams,
    lifecycle_observer: Option<Rc<dyn LifecycleObserver>>,
) -> HttpSessionPool {
    HttpSessionPool::new(
        context,
        SocketAddress::new(address.ip().to_string(), address.port()),
        PoolParams {
            connect_timeout: Duration::from_secs(1),
            max_age: Duration::ZERO,
            ..params
        },
        ConnectionParams::default(),
        // Session timers are off, so they cannot retire a connection.
        SessionParams {
            stream_read_timeout: Some(Duration::ZERO),
            connection_read_timeout: Some(Duration::ZERO),
            write_timeout: Some(Duration::ZERO),
            lifecycle_observer,
            ..SessionParams::default()
        },
    )
}

async fn send(
    session: &ClientSession,
    reservation: RequestReservation,
    path: &[u8],
) -> UniplexResponse {
    let request = HttpFixedSource::make_get_request(path, None).expect("test request is valid");
    session
        .send_reserved_request(
            HttpSourceHolder::new(Box::new(request)),
            reservation,
            CancellationToken::never(),
        )
        .await
        .expect("request is sent")
}

/// Reads a 200 response head, then `pause` before the first body event and
/// `pause_after_first` after it, and returns the whole body.
async fn read_response(
    mut response: UniplexResponse,
    pause: Duration,
    pause_after_first: Duration,
) -> Vec<u8> {
    let head = response
        .source
        .read_header_event()
        .await
        .expect("response head");
    assert_eq!(head.headers().response_status(), Some(200));
    tokio::time::sleep(pause).await;
    let mut body = Vec::new();
    let mut eom = head.eom();
    let mut first = true;
    while !eom {
        let event = response
            .source
            .read_body_event(u32::MAX)
            .await
            .expect("response body");
        if let Some(data) = event.as_body() {
            data.segments()
                .for_each(|segment| body.extend_from_slice(segment));
        }
        eom = event.eom();
        if std::mem::take(&mut first) {
            tokio::time::sleep(pause_after_first).await;
        }
    }
    body
}

/// Sends `path` on a reserved session and returns its whole response body.
async fn exchange(
    session: &ClientSession,
    reservation: RequestReservation,
    path: &[u8],
) -> Vec<u8> {
    read_response(
        send(session, reservation, path).await,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await
}

async fn acquire(pool: &HttpSessionPool) -> GetSessionResult {
    pool.get_session_with_reservation(CancellationToken::never())
        .await
        .expect("acquisition succeeds")
}

/// Waits until the session stops counting the finished exchange as active.
async fn settle(session: &ClientSession) {
    while session.active_transactions() != 0 {
        tokio::task::yield_now().await;
    }
}

/// Runs one exchange on the pool's next session and returns that session.
async fn one_exchange(pool: &HttpSessionPool) -> ClientSession {
    let GetSessionResult {
        reservation,
        session,
    } = acquire(pool).await;
    assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
    settle(&session).await;
    session
}

/// Opens two sessions, then idles them in order: the second is the most
/// recently used.
async fn two_idle_sessions(pool: &HttpSessionPool) -> (ClientSession, ClientSession) {
    let first = acquire(pool).await;
    let second = acquire(pool).await;
    assert_ne!(
        first.session.local_address(),
        second.session.local_address(),
        "a reserved session makes the pool open another"
    );
    assert_eq!(
        exchange(&first.session, first.reservation, b"/").await,
        b"ok"
    );
    settle(&first.session).await;
    assert_eq!(
        exchange(&second.session, second.reservation, b"/").await,
        b"ok"
    );
    settle(&second.session).await;
    (first.session, second.session)
}

async fn sleep_until(deadline: Instant) {
    tokio::time::sleep(deadline.saturating_duration_since(Instant::now())).await;
}

#[test]
fn idle_timeout_is_off_by_default() {
    assert_eq!(PoolParams::default().idle_timeout, Duration::ZERO);
}

#[test]
fn idle_sweep_is_at_expiry_by_default() {
    assert_eq!(PoolParams::default().idle_sweep, IdleSweep::AtExpiry);
}

#[test]
fn reuse_order_is_least_recently_used_by_default() {
    assert_eq!(
        PoolParams::default().reuse_order,
        SessionReuseOrder::LeastRecentlyUsed
    );
}

#[test]
fn idle_session_is_retired_and_replaced() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let first = one_exchange(&pool).await;
            tokio::time::sleep(IDLE * 4).await;
            assert_eq!(
                pool.session_count(),
                0,
                "the idle session is retired without an acquisition"
            );
            tokio::time::timeout(Duration::from_secs(1), first.wait_closed())
                .await
                .expect("the retired session closes")
                .expect("the retired session closes cleanly");
            let GetSessionResult {
                reservation,
                session: second,
            } = acquire(&pool).await;
            assert_ne!(
                first.local_address(),
                second.local_address(),
                "the next request opens a new connection"
            );
            assert_eq!(exchange(&second, reservation, b"/").await, b"ok");
        })
    });
}

#[test]
fn session_streaming_a_long_response_is_not_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session: first,
            } = acquire(&pool).await;
            assert_eq!(
                exchange(&first, reservation, b"/slow").await,
                b"chunk".repeat(SLOW_CHUNKS),
                "the response outlives four idle timeouts"
            );
            assert!(!first.is_draining(), "the streaming session is not retired");
            let GetSessionResult {
                reservation,
                session: second,
            } = acquire(&pool).await;
            assert_eq!(
                first.local_address(),
                second.local_address(),
                "the idle period starts when the response ends"
            );
            assert_eq!(exchange(&second, reservation, b"/").await, b"ok");
        })
    });
}

#[test]
fn silent_exchange_is_not_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session: first,
            } = acquire(&pool).await;
            assert_eq!(
                exchange(&first, reservation, b"/silent").await,
                b"ok",
                "the response arrives after three idle timeouts of silence"
            );
            assert!(!first.is_draining(), "the waiting session is not retired");
            let second = one_exchange(&pool).await;
            assert_eq!(first.local_address(), second.local_address());
        })
    });
}

#[test]
fn backpressured_response_is_not_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session: first,
            } = acquire(&pool).await;
            let response = send(&first, reservation, b"/large").await;
            let body = read_response(response, Duration::ZERO, IDLE * 3).await;
            assert_eq!(
                body.len(),
                LARGE,
                "the response survives a reader that pauses for three idle timeouts"
            );
            assert!(
                !first.is_draining(),
                "the backpressured session is not retired"
            );
            settle(&first).await;
            let second = one_exchange(&pool).await;
            assert_eq!(first.local_address(), second.local_address());
        })
    });
}

#[test]
fn buffered_response_survives_a_slow_consumer() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session,
            } = acquire(&pool).await;
            let response = send(&session, reservation, b"/").await;
            assert_eq!(
                read_response(response, IDLE * 3, Duration::ZERO).await,
                b"ok",
                "a response read three idle timeouts after its head is intact"
            );
        })
    });
}

#[test]
fn reserved_session_is_not_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session,
            } = acquire(&pool).await;
            tokio::time::sleep(IDLE * 4).await;
            assert!(!session.is_draining(), "a reserved session is not idle");
            assert_eq!(pool.session_count(), 1);
            assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
        })
    });
}

#[test]
fn waiter_takes_the_released_session() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let first = acquire(&pool).await;
            let second = pool.get_session_with_reservation(CancellationToken::never());
            tokio::pin!(second);
            tokio::select! {
                _ = &mut second => panic!("the waiter was served while the only session was reserved"),
                () = tokio::time::sleep(IDLE * 2) => {}
            }
            assert!(!first.session.is_draining());
            drop(first.reservation);
            let GetSessionResult {
                reservation,
                session,
            } = second.await.expect("the waiter is served");
            assert_eq!(
                first.session.local_address(),
                session.local_address(),
                "the waiter reuses the released session"
            );
            assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
        })
    });
}

/// Reserves an exchange on each of `sessions` the first time the pool's idle
/// session count changes, as an application callback may while the pool drains
/// a session that it retires.
struct ReserveOnIdleChange {
    sessions: Vec<ClientSession>,
    attempted: Cell<bool>,
    reserved: RefCell<Vec<(ClientSession, RequestReservation)>>,
}

impl IdleSessionObserver for ReserveOnIdleChange {
    fn on_idle_sessions_changed(&self, _pool: &HttpSessionPool) {
        if self.attempted.replace(true) {
            return;
        }
        let reserved = self
            .sessions
            .iter()
            .filter_map(|session| {
                let reservation = session.reserve_request().ok()?;
                Some((session.clone(), reservation))
            })
            .collect();
        *self.reserved.borrow_mut() = reserved;
    }
}

fn reserve_on_idle_change(
    pool: &HttpSessionPool,
    sessions: Vec<ClientSession>,
) -> Rc<ReserveOnIdleChange> {
    let observer = Rc::new(ReserveOnIdleChange {
        sessions,
        attempted: Cell::new(false),
        reserved: RefCell::default(),
    });
    pool.set_idle_session_observer(Some(Rc::clone(&observer) as Rc<dyn IdleSessionObserver>))
        .expect("the pool is open");
    observer
}

/// Every reservation the callback took serves an exchange: retirement never
/// drains a session that a caller could still reserve.
async fn assert_reservations_served(observer: &ReserveOnIdleChange) {
    assert!(
        observer.attempted.get(),
        "the callback ran during retirement"
    );
    let reserved = observer.reserved.take();
    for (session, reservation) in reserved {
        let request = HttpFixedSource::make_get_request(b"/", None).expect("test request is valid");
        let sent = session
            .send_reserved_request(
                HttpSourceHolder::new(Box::new(request)),
                reservation,
                CancellationToken::never(),
            )
            .await;
        let response = match sent {
            Ok(response) => response,
            Err(error) => panic!("a reservation taken during retirement failed: {error}"),
        };
        assert_eq!(
            read_response(response, Duration::ZERO, Duration::ZERO).await,
            b"ok"
        );
    }
}

/// Two sessions expire together; the drain of the first runs a callback that
/// reserves the second before the retirement task reaches it.
#[test]
fn reservation_made_while_idle_retirement_drains_is_served() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 2);
            let (first, second) = two_idle_sessions(&pool).await;
            let observer = reserve_on_idle_change(&pool, vec![first, second]);
            pool.stamp_sessions_idle_for_test(IDLE * 2);
            while !observer.attempted.get() {
                tokio::task::yield_now().await;
            }
            assert_reservations_served(&observer).await;
        })
    });
}

/// As `reservation_made_while_idle_retirement_drains_is_served`, for the
/// sessions that an acquisition retires before it selects one.
#[test]
fn reservation_made_while_an_acquisition_retires_sessions_is_served() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 2);
            let (first, second) = two_idle_sessions(&pool).await;
            let observer = reserve_on_idle_change(&pool, vec![first.clone(), second.clone()]);
            // The retirement task cannot run before the acquisition's first poll.
            pool.age_idle_sessions_for_test(IDLE);
            let GetSessionResult {
                reservation,
                session,
            } = acquire(&pool).await;
            assert_ne!(session.local_address(), first.local_address());
            assert_ne!(session.local_address(), second.local_address());
            assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
            assert_reservations_served(&observer).await;
        })
    });
}

/// Starts one acquisition from the pool, polled once, when a session gains its
/// first reservation, as an application's lifecycle callback may. The session
/// counts that reservation before the pool is notified of it.
#[derive(Default)]
struct AcquireOnActivate {
    pool: RefCell<Weak<HttpSessionPool>>,
    armed: Cell<bool>,
    started: RefCell<Vec<LocalClientFuture<Result<GetSessionResult, PoolError>>>>,
}

impl LifecycleObserver for AcquireOnActivate {
    fn on_event(&self, event: LifecycleEvent) {
        if !matches!(event, LifecycleEvent::ActivateConnection) || !self.armed.replace(false) {
            return;
        }
        let Some(pool) = self.pool.borrow().upgrade() else {
            return;
        };
        let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
        let _ = acquisition
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        self.started.borrow_mut().push(acquisition);
    }
}

/// A reservation taken directly on a session idle past the timeout, before
/// retirement reaches it, keeps the session: an acquisition run by the
/// reservation's own callback must not retire it.
#[test]
fn reservation_on_an_expired_session_survives_an_acquisition_in_its_callback() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                let observer = Rc::new(AcquireOnActivate::default());
                let pool = Rc::new(new_pool(
                    context,
                    address,
                    PoolParams {
                        max_connections: 2,
                        idle_timeout: IDLE,
                        idle_sweep,
                        ..PoolParams::default()
                    },
                    Some(Rc::clone(&observer) as Rc<dyn LifecycleObserver>),
                ));
                *observer.pool.borrow_mut() = Rc::downgrade(&pool);
                let session = one_exchange(&pool).await;
                // The retirement task cannot run before the reservation below.
                pool.age_idle_sessions_for_test(IDLE);
                observer.armed.set(true);
                let reservation = session
                    .reserve_request()
                    .expect("the idle session takes a reservation");
                assert_eq!(
                    observer.started.borrow().len(),
                    1,
                    "{idle_sweep:?}: the callback acquired"
                );
                assert!(
                    !session.is_draining(),
                    "{idle_sweep:?}: a reserved session is not retired"
                );
                assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
                observer.started.take();
            })
        });
    }
}

/// An acquisition that retires an expired session and selects a fresh idle one
/// keeps its selection when a callback that the retirement runs tries to
/// reserve that session first.
#[test]
fn retirement_callback_cannot_take_the_session_an_acquisition_selected() {
    for (idle_sweep, reuse_order) in [
        (IdleSweep::AtExpiry, SessionReuseOrder::LeastRecentlyUsed),
        (IdleSweep::AtExpiry, SessionReuseOrder::MostRecentlyUsed),
        (IdleSweep::Interval, SessionReuseOrder::LeastRecentlyUsed),
    ] {
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                let pool = pool_with_sweep(context, address, 2, reuse_order, IDLE, idle_sweep);
                let (expired, fresh) = two_idle_sessions(&pool).await;
                let observer = reserve_on_idle_change(&pool, vec![fresh.clone()]);
                // The retirement task cannot run before the acquisition's first poll.
                pool.age_idle_session_for_test(&expired, IDLE);
                let GetSessionResult {
                    reservation,
                    session,
                } = pool
                    .get_session_with_reservation(CancellationToken::never())
                    .await
                    .unwrap_or_else(|error| {
                        panic!("{idle_sweep:?}, {reuse_order:?}: the acquisition failed: {error}")
                    });
                assert_eq!(
                    session.local_address(),
                    fresh.local_address(),
                    "{idle_sweep:?}, {reuse_order:?}: the fresh session serves"
                );
                assert!(
                    expired.is_draining(),
                    "{idle_sweep:?}, {reuse_order:?}: the expired session is retired"
                );
                assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
                assert_reservations_served(&observer).await;
            })
        });
    }
}

/// Panics once in the first `ActivateConnection` or `DrainStarted` callback after
/// that event is armed, as an application's lifecycle callback may.
#[derive(Default)]
struct PanicOnArmedEvent {
    activate: Cell<bool>,
    drain: Cell<bool>,
}

impl LifecycleObserver for PanicOnArmedEvent {
    fn on_event(&self, event: LifecycleEvent) {
        let armed = match event {
            LifecycleEvent::ActivateConnection => &self.activate,
            LifecycleEvent::DrainStarted(_) => &self.drain,
            _ => return,
        };
        if armed.replace(false) {
            panic!("injected lifecycle callback panic");
        }
    }
}

/// Polls one acquisition once and reports whether that poll panicked.
fn acquisition_poll_panics(pool: &HttpSessionPool) -> bool {
    let mut acquisition = pool.get_session_with_reservation(CancellationToken::never());
    catch_unwind(AssertUnwindSafe(|| {
        let _ = acquisition
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
    }))
    .is_err()
}

/// `retired` closes and then the pool drains and joins, while the caller still
/// holds a clone of `retired`.
async fn assert_retired_session_closes_and_pool_drains(
    pool: &HttpSessionPool,
    retired: &ClientSession,
    case: &str,
) {
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        while !retired.is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(closed.is_ok(), "{case}: the retired session closes");
    let drained = tokio::time::timeout(Duration::from_secs(2), pool.drain_and_join()).await;
    assert!(
        matches!(drained, Ok(Ok(()))),
        "{case}: the pool drains and joins: {drained:?}"
    );
}

/// A panic in the callback run by reserving the acquisition's selection still
/// drains the expired session that the acquisition retired, so that session
/// closes and the pool can drain while a caller holds a clone of it.
#[test]
fn reservation_callback_panic_still_drains_the_sessions_an_acquisition_retired() {
    for (idle_sweep, reuse_order) in [
        (IdleSweep::AtExpiry, SessionReuseOrder::LeastRecentlyUsed),
        (IdleSweep::AtExpiry, SessionReuseOrder::MostRecentlyUsed),
        (IdleSweep::Interval, SessionReuseOrder::LeastRecentlyUsed),
    ] {
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                let case = format!("{idle_sweep:?}, {reuse_order:?}");
                let observer = Rc::new(PanicOnArmedEvent::default());
                let pool = new_pool(
                    context,
                    address,
                    PoolParams {
                        max_connections: 2,
                        idle_timeout: IDLE,
                        idle_sweep,
                        reuse_order,
                        ..PoolParams::default()
                    },
                    Some(Rc::clone(&observer) as Rc<dyn LifecycleObserver>),
                );
                let (expired, _fresh) = two_idle_sessions(&pool).await;
                // The retirement task cannot run before the acquisition's first poll.
                pool.age_idle_session_for_test(&expired, IDLE);
                observer.activate.set(true);
                assert!(
                    acquisition_poll_panics(&pool),
                    "{case}: the selection's reservation callback panicked"
                );
                assert_retired_session_closes_and_pool_drains(&pool, &expired, &case).await;
            })
        });
    }
}

/// As `reservation_callback_panic_still_drains_the_sessions_an_acquisition_retired`,
/// for a panic raised by the drain of a session the acquisition retired for its
/// age, after it selected another and before it drained the idle-expired one.
#[test]
fn drain_callback_panic_still_drains_the_idle_sessions_an_acquisition_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let observer = Rc::new(PanicOnArmedEvent::default());
            let pool = new_pool(
                context,
                address,
                PoolParams {
                    max_connections: 3,
                    idle_timeout: IDLE,
                    ..PoolParams::default()
                },
                Some(Rc::clone(&observer) as Rc<dyn LifecycleObserver>),
            );
            let mut acquired = Vec::new();
            for _ in 0..3 {
                acquired.push(acquire(&pool).await);
            }
            let mut sessions = Vec::new();
            for GetSessionResult {
                reservation,
                session,
            } in acquired
            {
                assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
                settle(&session).await;
                sessions.push(session);
            }
            let (idle_expired, aged) = (&sessions[0], &sessions[1]);
            // The retirement task cannot run before the acquisition's first poll.
            pool.age_idle_session_for_test(idle_expired, IDLE);
            pool.expire_session_for_test(aged);
            observer.drain.set(true);
            assert!(
                acquisition_poll_panics(&pool),
                "the aged session's drain callback panicked"
            );
            assert!(!observer.drain.get(), "a drain ran the armed callback");
            assert_retired_session_closes_and_pool_drains(&pool, idle_expired, "max age").await;
        })
    });
}

#[test]
fn acquisition_skips_a_session_idle_past_the_timeout() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let first = one_exchange(&pool).await;
            // The retirement task cannot run before the acquisition's first poll.
            pool.age_idle_sessions_for_test(IDLE);
            let GetSessionResult {
                reservation,
                session: second,
            } = acquire(&pool).await;
            assert!(first.is_draining(), "the expired session is retired");
            assert_ne!(first.local_address(), second.local_address());
            assert_eq!(exchange(&second, reservation, b"/").await, b"ok");
        })
    });
}

#[test]
fn staggered_idle_sessions_retire_in_turn() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 2);
            let first = acquire(&pool).await;
            let second = acquire(&pool).await;
            assert_eq!(
                exchange(&first.session, first.reservation, b"/").await,
                b"ok"
            );
            settle(&first.session).await;
            let first_idle = Instant::now();
            tokio::time::sleep(IDLE).await;
            assert_eq!(
                exchange(&second.session, second.reservation, b"/").await,
                b"ok"
            );
            settle(&second.session).await;
            let second_idle = Instant::now();
            sleep_until(first_idle + IDLE * 3 / 2).await;
            assert!(
                first.session.is_draining(),
                "the first idle session expires"
            );
            assert!(
                !second.session.is_draining(),
                "the later idle session is not yet due"
            );
            sleep_until(second_idle + IDLE * 2).await;
            assert!(
                second.session.is_draining(),
                "retirement re-arms for the later idle session"
            );
            assert_eq!(pool.session_count(), 0);
        })
    });
}

/// The idle timeout, and so the sweep period, of the tests that compare the
/// pool with `HyperUtilPool`.
const SWEEP_IDLE: Duration = Duration::from_millis(200);

/// A model of the `client::legacy` pool in hyper-util 0.1.20, the pool behind
/// reqwest 0.12, for one origin of HTTP/1.1 connections with no waiters.
///
/// `put` pushes a released connection onto the end of the idle list. The first
/// push ever spawns `IdleTask`, which sleeps `max(timeout, 90 ms)`
/// (`MIN_CHECK`) from then and again after each `clear_expired`, until the pool
/// drops, and evicts every entry idle for longer than the timeout. A checkout
/// (`IdlePopper::pop`) pops from the end, dropping expired entries until it
/// reaches one that has not expired, and dials when the list is empty. A sweep
/// runs just after its deadline, as a timer fires.
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
    /// Starts the clock: the steps before it run back to back, so that dials
    /// they wait for cannot shift the sweeps that a later release starts.
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
    pool: &HttpSessionPool,
    model: &mut HyperUtilPool,
    connections: &mut Vec<ClientSession>,
    step: Step,
) -> (usize, RequestReservation) {
    let at = Instant::now();
    let GetSessionResult {
        reservation,
        session,
    } = acquire(pool).await;
    let expected = model.checkout(at);
    let taken = connections
        .iter()
        .position(|known| known.is_same_session(&session))
        .unwrap_or_else(|| {
            connections.push(session);
            connections.len() - 1
        });
    assert_eq!(taken, expected, "{step:?}: the connection taken");
    (taken, reservation)
}

/// Runs `steps` on a `MostRecentlyUsed`, `IdleSweep::Interval` pool and on
/// `HyperUtilPool` with the same idle timeout, comparing their open connections
/// at each `Step::Open`.
fn assert_sweeps_as_hyper_util(steps: &'static [Step]) {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool_with_sweep(
                context,
                address,
                8,
                SessionReuseOrder::MostRecentlyUsed,
                SWEEP_IDLE,
                IdleSweep::Interval,
            );
            let mut model = HyperUtilPool::new(SWEEP_IDLE);
            let mut connections = Vec::new();
            let mut held: Vec<Option<(usize, RequestReservation)>> = Vec::new();
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
                        let taken =
                            take_as_hyper_util(&pool, &mut model, &mut connections, step).await;
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
                            tokio::time::timeout(Duration::from_secs(5), async {
                                while connections
                                    .iter()
                                    .any(|session| !session.is_draining() && !session.is_finished())
                                {
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                            })
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
                            "{step:?}: the pool matches hyper-util 0.1.20"
                        );
                        None
                    }
                };
                if let Some((connection, reservation)) = released {
                    let session = &connections[connection];
                    assert_eq!(exchange(session, reservation, b"/").await, b"ok");
                    settle(session).await;
                    model.release(connection, Instant::now());
                }
            }
        })
    });
}

/// The sweep timer runs from the first release, not from a dial, and keeps its
/// period while the pool is empty (0.1.21 stops it on an empty pool).
const FIRST_RELEASE_STARTS_SWEEPS: &[Step] = &[
    Step::Hold { at: 0, slot: 0 },
    // Dialed just before; released at 100, which starts sweeps at 300, 500, 700, ...
    Step::Start,
    Step::Release { at: 100, slot: 0 },
    Step::Open {
        at: 250,
        open: &[0],
    },
    // Sweeps from the dial would wait for 400.
    Step::Open { at: 350, open: &[] },
    // The exchange's connection is idle about 100 ms at the 500 sweep.
    // Sweeps restarted at 400 would close it at 600.
    Step::Exchange { at: 400 },
    Step::Open {
        at: 650,
        open: &[1],
    },
    Step::Open { at: 750, open: &[] },
];

/// A connection that becomes idle after a sweep in which every connection was
/// busy is swept on the old period (0.1.20 does not restart it).
const BUSY_SWEEP_KEEPS_THE_PERIOD: &[Step] = &[
    Step::Hold { at: 0, slot: 0 },
    // The release at 0 starts sweeps at 200, 400, 600, ...
    Step::Start,
    Step::Release { at: 0, slot: 0 },
    Step::Hold { at: 0, slot: 0 },
    // Busy through the 200 sweep, idle from 250.
    Step::Release { at: 250, slot: 0 },
    Step::Open {
        at: 300,
        open: &[0],
    },
    // Kept at 400 (idle 150); a sweep restarted at 250 would close it at 450.
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
    // The release at 0 starts sweeps at 200, 400, 600, 800, ...
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
    // 1 expired at 250, 0 has been idle 175 ms: the checkout takes 0 and keeps 1.
    Step::Exchange { at: 275 },
    Step::Open {
        at: 325,
        open: &[0, 1],
    },
    // The 400 sweep closes 1.
    Step::Open {
        at: 450,
        open: &[0],
    },
    // 0 expired at about 475: the checkout drops it and dials 2.
    Step::Exchange { at: 525 },
    Step::Open {
        at: 550,
        open: &[2],
    },
    // Kept at 600 (idle about 75), closed at 800.
    Step::Open {
        at: 750,
        open: &[2],
    },
    Step::Open { at: 850, open: &[] },
];

/// Run on exact times, the model keeps open the connections that each schedule
/// expects.
#[test]
fn hyper_util_model_follows_the_sweep_schedules() {
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
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool_with_sweep(
                context,
                address,
                1,
                SessionReuseOrder::LeastRecentlyUsed,
                Duration::from_millis(20),
                IdleSweep::Interval,
            );
            let session = one_exchange(&pool).await;
            let idle = Instant::now();
            sleep_until(idle + Duration::from_millis(50)).await;
            assert!(
                !session.is_draining(),
                "past its idle timeout, the session waits for the first sweep"
            );
            sleep_until(idle + Duration::from_millis(150)).await;
            assert!(session.is_draining(), "the first sweep retires the session");
        })
    });
}

#[test]
fn session_reused_just_before_expiry_is_not_retired() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let first = one_exchange(&pool).await;
            let first_idle = Instant::now();
            tokio::time::sleep(IDLE * 7 / 10).await;
            let reused = one_exchange(&pool).await;
            let reused_idle = Instant::now();
            assert_eq!(first.local_address(), reused.local_address());
            sleep_until(first_idle + IDLE * 13 / 10).await;
            assert!(
                !first.is_draining(),
                "reuse restarts the idle period that the first expiry measured"
            );
            sleep_until(reused_idle + IDLE * 2).await;
            assert!(first.is_draining(), "the session expires after the reuse");
        })
    });
}

#[test]
fn least_recently_used_reuse_keeps_every_session_warm() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 2);
            let (first, second) = two_idle_sessions(&pool).await;
            let mut used = Vec::new();
            for _ in 0..12 {
                used.push(one_exchange(&pool).await.local_address().clone());
                tokio::time::sleep(IDLE / 3).await;
            }
            assert!(
                used.contains(first.local_address()) && used.contains(second.local_address()),
                "sequential traffic alternates between both sessions"
            );
            assert!(!first.is_draining() && !second.is_draining());
            assert_eq!(
                pool.session_count(),
                2,
                "a session reused within the idle timeout is never retired"
            );
        })
    });
}

#[test]
fn most_recently_used_reuse_lets_the_cold_session_retire() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool_with_order(context, address, 2, SessionReuseOrder::MostRecentlyUsed);
            let (cold, hot) = two_idle_sessions(&pool).await;
            for _ in 0..12 {
                assert_eq!(
                    one_exchange(&pool).await.local_address(),
                    hot.local_address(),
                    "the most recently used idle session serves"
                );
                tokio::time::sleep(IDLE / 3).await;
            }
            assert!(cold.is_draining(), "the unused session idles out");
            assert!(!hot.is_draining());
            assert_eq!(pool.session_count(), 1);
        })
    });
}

#[test]
fn most_recently_used_reuse_picks_the_session_released_last() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool_with_order(context, address, 2, SessionReuseOrder::MostRecentlyUsed);
            let (older, newer) = two_idle_sessions(&pool).await;
            // Released in the other order, the older session is then the most recent.
            let first = acquire(&pool).await;
            let second = acquire(&pool).await;
            assert_eq!(first.session.local_address(), newer.local_address());
            assert_eq!(second.session.local_address(), older.local_address());
            assert_eq!(
                exchange(&first.session, first.reservation, b"/").await,
                b"ok"
            );
            settle(&first.session).await;
            assert_eq!(
                exchange(&second.session, second.reservation, b"/").await,
                b"ok"
            );
            settle(&second.session).await;
            assert_eq!(
                one_exchange(&pool).await.local_address(),
                older.local_address()
            );
        })
    });
}

#[test]
fn idle_retirement_runs_at_most_twice_per_timeout_under_sequential_traffic() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            one_exchange(&pool).await;
            let started = Instant::now();
            let passes_before = pool.idle_retirement_passes_for_test();
            for _ in 0..40 {
                one_exchange(&pool).await;
            }
            let windows = u64::try_from(started.elapsed().as_millis() / IDLE.as_millis())
                .expect("bounded test duration");
            let passes = pool.idle_retirement_passes_for_test() - passes_before;
            assert!(
                passes <= 2 * (windows + 1),
                "{passes} retirement passes for 40 requests over {windows} full idle timeouts"
            );
        })
    });
}

/// An idle timeout longer than any session stays idle does not retire sessions
/// or wake retirement for each released session.
#[test]
fn unreachable_idle_timeout_does_not_wake_retirement_per_request() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                let pool = pool_with_sweep(
                    context,
                    address,
                    1,
                    SessionReuseOrder::LeastRecentlyUsed,
                    Duration::MAX,
                    idle_sweep,
                );
                let first = one_exchange(&pool).await;
                tokio::task::yield_now().await;
                let passes_before = pool.idle_retirement_passes_for_test();
                for _ in 0..40 {
                    one_exchange(&pool).await;
                }
                let passes = pool.idle_retirement_passes_for_test() - passes_before;
                assert!(
                    passes <= 2,
                    "{idle_sweep:?}: {passes} retirement passes for 40 requests"
                );
                assert!(!first.is_draining(), "{idle_sweep:?}: nothing retires");
                assert_eq!(pool.session_count(), 1);
            })
        });
    }
}

/// Sessions whose idle periods start `IDLE / 4` apart, over four idle timeouts,
/// retire in that order, each only after it has been idle for the timeout. Each
/// distinct expiry may cost its own retiring pass, so the passes scale with the
/// sessions, not with the timeouts. Apart from one recompute after each
/// retiring pass, the number of empty passes is constant.
#[test]
fn many_staggered_idle_sessions_retire_in_release_order() {
    const SESSIONS: usize = 16;
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, SESSIONS as u32);
            let mut held = Vec::new();
            for _ in 0..SESSIONS {
                held.push(acquire(&pool).await);
            }
            let passes_before = pool.idle_retirement_passes_for_test();
            let retiring_before = pool.idle_retirement_retiring_passes_for_test();
            let released = RefCell::new(Vec::<(ClientSession, Instant)>::new());
            let release = async {
                for (index, acquired) in held.into_iter().enumerate() {
                    if index != 0 {
                        tokio::time::sleep(IDLE / 4).await;
                    }
                    let idle_from = Instant::now();
                    assert_eq!(
                        exchange(&acquired.session, acquired.reservation, b"/").await,
                        b"ok"
                    );
                    settle(&acquired.session).await;
                    released.borrow_mut().push((acquired.session, idle_from));
                }
            };
            let observe = async {
                let mut retired_at = vec![None; SESSIONS];
                while retired_at.iter().any(Option::is_none) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let now = Instant::now();
                    for (index, (session, idle_from)) in released.borrow().iter().enumerate() {
                        if retired_at[index].is_none() && session.is_draining() {
                            assert!(
                                now >= *idle_from + IDLE,
                                "session {index} retired after {:?} idle",
                                now - *idle_from
                            );
                            retired_at[index] = Some(now - *idle_from);
                        }
                    }
                    let prefix = retired_at.iter().take_while(|at| at.is_some()).count();
                    assert!(
                        retired_at[prefix..].iter().all(Option::is_none),
                        "sessions retire in the order they became idle: {retired_at:?}"
                    );
                }
                retired_at
            };
            let ((), retired_after) = tokio::join!(release, observe);
            assert!(
                retired_after.iter().flatten().all(|idle| *idle < IDLE * 3),
                "each session retires soon after its own expiry: {retired_after:?}"
            );
            assert_eq!(pool.session_count(), 0);
            let passes = pool.idle_retirement_passes_for_test() - passes_before;
            let retiring = pool.idle_retirement_retiring_passes_for_test() - retiring_before;
            println!("{passes} passes ({retiring} retiring) for {SESSIONS} staggered sessions");
            assert!(
                retiring <= SESSIONS as u64,
                "{retiring} retiring passes for {SESSIONS} sessions"
            );
            assert!(
                passes <= 2 * retiring + 3,
                "{passes} passes, {retiring} of them retiring, for {SESSIONS} staggered sessions"
            );
        })
    });
}

/// Under sustained traffic from many concurrent requesters over several idle
/// timeouts, empty retirement passes stay within about two per elapsed timeout,
/// plus two at the start and one recompute per retiring pass, however many
/// requests run, in each reuse order and sweep mode.
#[test]
fn idle_retirement_passes_under_sustained_concurrent_traffic() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        for reuse_order in [
            SessionReuseOrder::MostRecentlyUsed,
            SessionReuseOrder::LeastRecentlyUsed,
        ] {
            idle_retirement_passes_under_sustained_traffic(reuse_order, idle_sweep);
        }
    }
}

fn idle_retirement_passes_under_sustained_traffic(
    reuse_order: SessionReuseOrder,
    idle_sweep: IdleSweep,
) {
    const REQUESTERS: u32 = 16;
    const TRAFFIC: Duration = Duration::from_millis(500);
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool_with_sweep(
                context,
                address,
                4 * REQUESTERS,
                reuse_order,
                IDLE,
                idle_sweep,
            );
            let passes_before = pool.idle_retirement_passes_for_test();
            let retiring_before = pool.idle_retirement_retiring_passes_for_test();
            let started = Instant::now();
            let requests = Cell::new(0_u64);
            stream::iter(0..REQUESTERS)
                .for_each_concurrent(None, |requester| {
                    let pool = &pool;
                    let requests = &requests;
                    async move {
                        while started.elapsed() < TRAFFIC {
                            one_exchange(pool).await;
                            requests.set(requests.get() + 1);
                            let pause = Duration::from_millis(u64::from(requester % 4));
                            tokio::time::sleep(pause).await;
                        }
                    }
                })
                .await;
            let windows = u64::try_from(started.elapsed().as_millis() / IDLE.as_millis())
                .expect("bounded test duration");
            let passes = pool.idle_retirement_passes_for_test() - passes_before;
            let retiring = pool.idle_retirement_retiring_passes_for_test() - retiring_before;
            let requests = requests.get();
            println!(
                "{reuse_order:?}, {idle_sweep:?}: {requests} requests, {passes} passes \
                 ({retiring} retiring) over {windows} idle timeouts, {} sessions",
                pool.session_count()
            );
            assert!(
                passes <= 2 * (windows + 2) + 2 * retiring,
                "{reuse_order:?}, {idle_sweep:?}: {passes} passes ({retiring} retiring) for \
                 {requests} requests over {windows} idle timeouts"
            );
            // A sweep on an interval retires a session up to a period after it expires.
            tokio::time::sleep(IDLE * 3).await;
            assert_eq!(
                pool.session_count(),
                0,
                "{reuse_order:?}, {idle_sweep:?}: every session idles out after the traffic"
            );
        })
    });
}

/// After a burst opens `BURST` sessions, `REQUESTERS` concurrent requesters need
/// only a few of them. The most-recently-used order keeps reusing those few, so
/// the rest of the burst idles out. The least-recently-used order cycles
/// through every session and so keeps them all. Both hold in either sweep mode.
#[test]
fn most_recently_used_reuse_keeps_fewer_sessions_under_concurrent_traffic() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        sessions_left_after_a_burst(idle_sweep);
    }
}

fn sessions_left_after_a_burst(idle_sweep: IdleSweep) {
    const BURST: usize = 16;
    const REQUESTERS: usize = 4;
    const BURST_IDLE: Duration = Duration::from_secs(1);
    let sessions_left = |reuse_order| {
        let (sender, receiver) = std::sync::mpsc::channel();
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                let pool = pool_with_sweep(
                    context,
                    address,
                    BURST as u32,
                    reuse_order,
                    BURST_IDLE,
                    idle_sweep,
                );
                let mut burst = Vec::new();
                for _ in 0..BURST {
                    burst.push(acquire(&pool).await);
                }
                for acquired in burst {
                    assert_eq!(
                        exchange(&acquired.session, acquired.reservation, b"/").await,
                        b"ok"
                    );
                }
                let started = Instant::now();
                stream::iter(0..REQUESTERS)
                    .for_each_concurrent(None, |requester| {
                        let pool = &pool;
                        async move {
                            while started.elapsed() < BURST_IDLE * 4 {
                                one_exchange(pool).await;
                                let pause = 1 + u64::try_from(requester % 3).expect("small");
                                tokio::time::sleep(Duration::from_millis(pause)).await;
                            }
                        }
                    })
                    .await;
                sender.send(pool.session_count()).expect("count recorded");
            })
        });
        receiver.try_iter().collect::<Vec<_>>()
    };
    let most_recent = sessions_left(SessionReuseOrder::MostRecentlyUsed);
    let least_recent = sessions_left(SessionReuseOrder::LeastRecentlyUsed);
    println!(
        "{idle_sweep:?}: sessions left: MostRecentlyUsed {most_recent:?}, \
         LeastRecentlyUsed {least_recent:?}"
    );
    assert_eq!(
        least_recent,
        [BURST, BURST],
        "{idle_sweep:?}: every burst session stays warm"
    );
    assert!(
        most_recent.iter().all(|left| *left <= REQUESTERS + 2),
        "{idle_sweep:?}: only the sessions the requesters need stay: {most_recent:?}"
    );
}

/// `MostRecentlyUsed` records idle periods even without an idle timeout, but
/// the pool then does not retire sessions or run a retirement task.
#[test]
fn most_recently_used_reuse_without_an_idle_timeout_never_retires() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let baseline = pool_with(
                context.clone(),
                address,
                2,
                SessionReuseOrder::LeastRecentlyUsed,
                Duration::ZERO,
            );
            let pool = pool_with(
                context,
                address,
                2,
                SessionReuseOrder::MostRecentlyUsed,
                Duration::ZERO,
            );
            let (cold, hot) = two_idle_sessions(&pool).await;
            two_idle_sessions(&baseline).await;
            for _ in 0..12 {
                assert_eq!(
                    one_exchange(&pool).await.local_address(),
                    hot.local_address(),
                    "the most recently used idle session serves"
                );
                one_exchange(&baseline).await;
                tokio::time::sleep(IDLE / 3).await;
            }
            assert!(!cold.is_draining() && !hot.is_draining());
            assert_eq!(
                pool.session_count(),
                2,
                "nothing retires without an idle timeout"
            );
            assert_eq!(
                pool.task_count_for_test(),
                baseline.task_count_for_test(),
                "no retirement task runs"
            );
        })
    });
}

#[test]
fn stale_idle_stamp_on_a_reserved_session_does_not_spin_retirement() {
    let server = start_server();
    let address = server.address().expect("server has an address");
    run_on_worker(move |context| {
        Box::pin(async move {
            let pool = pool(context, address, 1);
            let GetSessionResult {
                reservation,
                session,
            } = acquire(&pool).await;
            let passes_before = pool.idle_retirement_passes_for_test();
            // As a missed session event would leave it: a due stamp on an active session.
            pool.stamp_sessions_idle_for_test(IDLE * 2);
            tokio::time::sleep(IDLE * 2).await;
            let passes = pool.idle_retirement_passes_for_test() - passes_before;
            assert!(passes <= 2, "{passes} retirement passes for one wake");
            assert!(!session.is_draining(), "a reserved session is not retired");
            assert_eq!(exchange(&session, reservation, b"/").await, b"ok");
        })
    });
}

#[test]
fn drain_joins_idle_retirement() {
    for idle_sweep in [IdleSweep::AtExpiry, IdleSweep::Interval] {
        let server = start_server();
        let address = server.address().expect("server has an address");
        run_on_worker(move |context| {
            Box::pin(async move {
                // Retirement waits without a deadline in one pool (on an interval, not yet
                // started), with one in the other.
                let pool = |context| {
                    pool_with_sweep(
                        context,
                        address,
                        1,
                        SessionReuseOrder::LeastRecentlyUsed,
                        IDLE,
                        idle_sweep,
                    )
                };
                let empty = pool(context.clone());
                let idle = pool(context);
                let session = one_exchange(&idle).await;
                for pool in [empty, idle] {
                    tokio::time::timeout(Duration::from_secs(1), pool.drain_and_join())
                        .await
                        .expect("the drain is bounded")
                        .expect("the pool drains");
                    assert_eq!(
                        pool.task_count_for_test(),
                        0,
                        "{idle_sweep:?}: no pool task survives"
                    );
                }
                assert!(
                    session.is_draining(),
                    "{idle_sweep:?}: the drain retires the idle session"
                );
            })
        });
    }
}
