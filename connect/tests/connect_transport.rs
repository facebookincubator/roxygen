/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Frozen `HTTPConnectTransportTest` cases and oracles for `HttpConnectTransport`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread::LocalKey;
use std::time::Duration;
use std::time::Instant;

use futures::FutureExt;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_connect::session::ConnectSession;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_connect::stream::RequestHeaderMap;
use roxygen_connect::transport::HttpConnectTransport;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::host::LocalHost;
use roxygen_runtime::host::TokioHost;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_server::connection::ServeTransportOptions;
use roxygen_server::connection::ServerConnection;
use roxygen_server::connection::serve_transport;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_session::uniplex::UniplexSession;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::ByteStreamTransport;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::byte_stream::TransportRead;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::tcp::TcpTransport;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::DuplexStream;
use tokio::net::TcpStream;

use crate::connect_stream::AUTHORITY;
use crate::connect_stream::CONNECT_TIMEOUT;
use crate::connect_stream::EGRESS_BUFFER_SIZE;
use crate::connect_stream::H2cProxy;
use crate::connect_stream::LocalTest;
use crate::connect_stream::WAIT_DEADLINE;
use crate::connect_stream::bytes_of;
use crate::connect_stream::eventually;
use crate::connect_stream::headers;
use crate::connect_stream::queue;
use crate::connect_stream::response;
use crate::connect_stream::run_on_worker;
use crate::connect_stream::socket_address;

const HTTP2_INITIAL_WINDOW_SIZE: u16 = 4;
const PROXY_WINDOW: usize = 64 * 1024;
const SMALL_EGRESS_BUFFER: u32 = 4096;
/// Fills the proxy's stream window and the session's egress body queue, and
/// leaves more than `SMALL_EGRESS_BUFFER` bytes in the egress source, so the
/// egress window stays closed until the proxy reads.
const STALL: usize = 2 * PROXY_WINDOW + 4 * SMALL_EGRESS_BUFFER as usize;
const READ_TIMEOUT: Duration = Duration::from_millis(50);
/// How long an oracle waits to show that something does not happen.
const QUIET: Duration = Duration::from_millis(200);

type ReadFuture = LocalTransportFuture<Result<TransportRead, HttpError>>;
type WriteFuture = LocalTransportFuture<Result<WriteInfo, TransportWriteError>>;

// ---------------------------------------------------------------------------
// Transport calls.

fn read_with(
    transport: &HttpConnectTransport,
    context: &WorkerContext,
    maximum: usize,
    allocation_size: usize,
    timeout: Duration,
    cancellation: CancellationToken,
) -> ReadFuture {
    ByteStreamTransport::read(
        transport,
        context,
        maximum,
        allocation_size,
        timeout,
        cancellation,
    )
}

/// `read(MutableByteRange, timeout)`: the range's size is both the maximum and
/// the allocation size.
fn read(
    transport: &HttpConnectTransport,
    context: &WorkerContext,
    maximum: usize,
    timeout: Duration,
) -> ReadFuture {
    read_with(
        transport,
        context,
        maximum,
        maximum,
        timeout,
        CancellationToken::never(),
    )
}

fn write_with(
    transport: &HttpConnectTransport,
    context: &WorkerContext,
    bytes: &[u8],
    timeout: Duration,
    cancellation: CancellationToken,
) -> WriteFuture {
    ByteStreamTransport::write(transport, context, queue(bytes), timeout, cancellation)
}

fn write(
    transport: &HttpConnectTransport,
    context: &WorkerContext,
    bytes: &[u8],
    timeout: Duration,
) -> WriteFuture {
    write_with(
        transport,
        context,
        bytes,
        timeout,
        CancellationToken::never(),
    )
}

/// `readAll` (HTTPConnectTransportTest.cpp:24-37).
async fn read_all(
    transport: &HttpConnectTransport,
    context: &WorkerContext,
    length: usize,
    timeout: Duration,
) -> Vec<u8> {
    let mut received = Vec::with_capacity(length);
    while received.len() < length {
        let read = read(transport, context, length - received.len(), timeout)
            .await
            .expect("tunnel read");
        assert!(!read.is_eof(), "EOF after {} bytes", received.len());
        received.extend_from_slice(&bytes_of(read.data()));
    }
    received
}

fn assert_eof(read: Result<TransportRead, HttpError>) {
    let read = read.expect("a read after the tunnel's ingress ended");
    assert!(read.is_eof(), "EOF");
    assert_eq!(read.bytes_read(), 0);
}

fn assert_data(read: Result<TransportRead, HttpError>, expected: &[u8]) {
    let read = read.expect("tunnel read");
    assert!(!read.is_eof());
    assert_eq!(bytes_of(read.data()), expected);
}

fn assert_cancelled<T: std::fmt::Debug>(result: Result<T, HttpError>) {
    let error = result.expect_err("the operation is cancelled");
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{error:?}");
}

fn assert_write_error(
    result: Result<WriteInfo, TransportWriteError>,
    code: HttpErrorCode,
    message: &[u8],
) {
    let error = result.expect_err("the write fails");
    assert_eq!(error.code(), code, "{error:?}");
    assert_eq!(
        error.error().message(),
        message,
        "{:?}",
        String::from_utf8_lossy(error.error().message())
    );
    assert_eq!(error.bytes_written(), 0);
}

const WRONG_WORKER: &[u8] = b"HTTPConnectTransport used from a different worker context";

fn assert_wrong_worker<T: std::fmt::Debug>(result: Result<T, HttpError>) {
    let error = result.expect_err("a call from another worker's context fails");
    assert_eq!(
        error.code(),
        HttpErrorCode::INVALID_STATE_TRANSITION,
        "{error:?}"
    );
    assert_eq!(error.message(), WRONG_WORKER);
}

fn assert_write_cancelled(result: Result<WriteInfo, TransportWriteError>) {
    let error = result.expect_err("the write is cancelled");
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{error:?}");
    assert_eq!(error.bytes_written(), 0);
}

fn assert_written(result: Result<WriteInfo, TransportWriteError>, length: usize) {
    let info = result.expect("the write succeeds");
    assert_eq!(info.bytes_written, length);
}

/// The ingress `HttpError` of a `TRANSPORT_READ_ERROR`
/// (`AsyncSocketException::INTERNAL_ERROR` around `describe()`,
/// HTTPConnectTransport.cpp:52-57).
fn read_error_cause(result: Result<TransportRead, HttpError>) -> HttpError {
    let error = result.expect_err("the read fails with the ingress error");
    assert_eq!(
        error.code(),
        HttpErrorCode::TRANSPORT_READ_ERROR,
        "{error:?}"
    );
    let cause = std::error::Error::source(&error)
        .and_then(|cause| cause.downcast_ref::<HttpError>())
        .expect("the ingress HttpError is the cause")
        .clone();
    assert_eq!(error.message(), cause.describe().as_slice());
    cause
}

fn assert_cause(cause: &HttpError, code: HttpErrorCode, message: &[u8]) {
    assert_eq!(cause.code(), code, "{cause:?}");
    assert_eq!(
        cause.message(),
        message,
        "{:?}",
        String::from_utf8_lossy(cause.message())
    );
}

fn assert_read_timeout(result: Result<TransportRead, HttpError>) {
    let error = result.expect_err("the read times out");
    assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT, "{error:?}");
    assert_eq!(error.message(), b"Timed out waiting for body event");
}

// ---------------------------------------------------------------------------
// Polling helpers.

/// Polls `future` once with the current task's waker.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(Pin::new(&mut *future).poll(context))).await
}

fn poll_with<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

/// Polls `future` until it parks, which a test asserts before it changes the
/// tunnel under it.
async fn park<F: Future + Unpin>(future: &mut F, what: &str) {
    assert!(poll_once(future).await.is_pending(), "{what} parks");
}

#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn waker(self: &Arc<Self>) -> Waker {
        Waker::from(Arc::clone(self))
    }

    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

thread_local! {
    static TEARDOWN: RefCell<Option<Rc<HttpConnectTransport>>> = const { RefCell::new(None) };
}

/// A waker that drops the transport handle `hand_to_teardown` gave it when it
/// is woken on the worker, as a waker can run caller code inline.
struct TeardownWaker;

impl TeardownWaker {
    fn waker() -> Waker {
        Waker::from(Arc::new(Self))
    }
}

impl Wake for TeardownWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        teardown();
    }
}

/// A waker that drops the transport handle `hand_to_teardown` gave it when
/// its last reference goes, as releasing a task's last waker can drop the
/// task and the handle it owns.
struct TeardownOnRelease;

impl TeardownOnRelease {
    fn waker() -> Waker {
        Waker::from(Arc::new(Self))
    }
}

#[expect(
    clippy::manual_noop_waker,
    reason = "the waker's Drop is what the tests use, and `Waker::noop` has none"
)]
impl Wake for TeardownOnRelease {
    fn wake(self: Arc<Self>) {}
}

impl Drop for TeardownOnRelease {
    fn drop(&mut self) {
        teardown();
    }
}

fn hand_to_teardown(transport: Rc<HttpConnectTransport>) {
    TEARDOWN.with(|slot| *slot.borrow_mut() = Some(transport));
}

/// Drops the handle `hand_to_teardown` gave, if it is still there.
fn teardown() {
    let transport = TEARDOWN.with(|slot| slot.borrow_mut().take());
    drop(transport);
}

fn teardown_ran() -> bool {
    TEARDOWN.with(|slot| slot.borrow().is_none())
}

type Hook = RefCell<Option<Box<dyn FnOnce()>>>;

thread_local! {
    static ON_WAKE: Hook = const { RefCell::new(None) };
    static ON_CLONE: Hook = const { RefCell::new(None) };
    static ON_DROP: Hook = const { RefCell::new(None) };
}

/// A waker that owns nothing. On the worker, its first wake after
/// `on_next_wake`, its first clone after `on_next_clone`, and its first drop
/// after `on_next_drop` run the caller code those installed, as waking,
/// cloning, or releasing a waker can run caller code inline.
fn hook_waker() -> Waker {
    // SAFETY: every function of `HOOK_WAKER` ignores the null data pointer,
    // so the waker owns no data and is valid on any thread.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &HOOK_WAKER)) }
}

static HOOK_WAKER: RawWakerVTable = RawWakerVTable::new(
    hook_waker_clone,
    hook_waker_wake,
    hook_waker_wake,
    hook_waker_drop,
);

fn hook_waker_clone(_data: *const ()) -> RawWaker {
    run_hook(&ON_CLONE);
    RawWaker::new(std::ptr::null(), &HOOK_WAKER)
}

fn hook_waker_wake(_data: *const ()) {
    run_hook(&ON_WAKE);
}

fn hook_waker_drop(_data: *const ()) {
    run_hook(&ON_DROP);
}

fn run_hook(hook: &'static LocalKey<Hook>) {
    let hook = hook.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

fn on_next_wake(hook: impl FnOnce() + 'static) {
    ON_WAKE.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn on_next_clone(hook: impl FnOnce() + 'static) {
    ON_CLONE.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn on_next_drop(hook: impl FnOnce() + 'static) {
    ON_DROP.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

fn hook_ran(hook: &'static LocalKey<Hook>) -> bool {
    hook.with(|slot| slot.borrow().is_none())
}

thread_local! {
    static TIMER_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static SECOND_CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
}

/// The Tokio host, except that a timer's poll first runs the caller code
/// `on_next_timer_poll` installed, once, as a caller-provided host's timer
/// can.
struct HookedTimerHost;

impl LocalHost for HookedTimerHost {
    fn spawn_local(&self, future: LocalFuture) {
        TokioHost.spawn_local(future);
    }

    fn now(&self) -> Instant {
        TokioHost.now()
    }

    fn sleep_until(&self, deadline: Instant) -> LocalFuture {
        let mut sleep = TokioHost.sleep_until(deadline);
        Box::pin(poll_fn(move |context| {
            let hook = TIMER_HOOK.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
            sleep.as_mut().poll(context)
        }))
    }

    fn yield_to_io(&self) -> LocalFuture {
        TokioHost.yield_to_io()
    }
}

fn on_next_timer_poll(hook: impl FnOnce() + 'static) {
    TIMER_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

/// Runs `section` on a second worker on this thread, whose host is `host`.
async fn on_second_worker(
    host: Host,
    section: impl FnOnce(WorkerContext) -> LocalTest + Send + 'static,
) {
    let (worker, driver) = WorkerDriver::with_host(16, host).expect("second worker driver");
    let driver = tokio::task::spawn_local(driver);
    let call = worker
        .call_local(move |context| AssertUnwindSafe(section(context)).catch_unwind())
        .expect("second worker call");
    let result = call.await.expect("second worker call completed");
    worker.begin_shutdown();
    driver.await.expect("second worker driver task");
    TIMER_HOOK.with(|slot| slot.borrow_mut().take());
    if let Err(payload) = result {
        resume_unwind(payload);
    }
}

/// The context of a second worker on this thread, which has shut down.
async fn second_worker_context() -> WorkerContext {
    on_second_worker(Host::tokio(), |context| {
        Box::pin(async move {
            SECOND_CONTEXT.with(|slot| *slot.borrow_mut() = Some(context));
        })
    })
    .await;
    SECOND_CONTEXT
        .with(|slot| slot.borrow_mut().take())
        .expect("the second worker's context")
}

fn cancelled_source() -> CancellationSource {
    let cancel = CancellationSource::new();
    cancel.request_cancellation();
    cancel
}

fn cancel_from_another_thread(cancel: &Arc<CancellationSource>) {
    let remote = Arc::clone(cancel);
    std::thread::spawn(move || {
        remote.request_cancellation();
    })
    .join()
    .expect("cancelling thread");
}

// ---------------------------------------------------------------------------
// Client-side observers.

/// Counts tunnel bytes that a client session buffered and consumed for a
/// response source, and its closed transactions.
#[derive(Default)]
struct TunnelStats {
    buffered: Cell<i64>,
    consumed: Cell<i64>,
    closed: Cell<usize>,
}

impl Http2SessionStats for TunnelStats {
    fn record_buffered_read_bytes(&self, delta: i64) {
        if delta > 0 {
            self.buffered.set(self.buffered.get() + delta);
        } else {
            self.consumed.set(self.consumed.get() - delta);
        }
    }

    fn record_transaction_closed(&self) {
        self.closed.set(self.closed.get() + 1);
    }
}

#[derive(Default)]
struct ClientEvents {
    drains: Cell<usize>,
    detached: Cell<usize>,
    peer_resets: RefCell<Vec<HttpErrorCode>>,
    destroyed: Cell<bool>,
}

impl LifecycleObserver for ClientEvents {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::DrainStarted(_) => self.drains.set(self.drains.get() + 1),
            LifecycleEvent::TransactionDetached { stream: Some(_) } => {
                self.detached.set(self.detached.get() + 1);
            }
            LifecycleEvent::PeerReset { code, .. } => self.peer_resets.borrow_mut().push(code),
            LifecycleEvent::Destroy => self.destroyed.set(true),
            _ => {}
        }
    }
}

struct Observed {
    stats: Rc<TunnelStats>,
    events: Rc<ClientEvents>,
}

impl Observed {
    fn new() -> Self {
        Self {
            stats: Rc::default(),
            events: Rc::default(),
        }
    }

    fn params(&self) -> SessionParams {
        SessionParams {
            lifecycle_observer: Some(self.events.clone()),
            session_stats: Some(self.stats.clone()),
            ..SessionParams::default()
        }
    }

    fn closed(&self) -> usize {
        self.stats.closed.get()
    }

    /// Waits until the session buffered `bytes` tunnel bytes in all.
    async fn buffered(&self, bytes: i64) {
        let stats = Rc::clone(&self.stats);
        eventually("buffered tunnel bytes", move || {
            stats.buffered.get() >= bytes
        })
        .await;
    }

    /// Waits until the session returned the credit of `bytes` tunnel bytes in all.
    async fn consumed(&self, bytes: i64) {
        let stats = Rc::clone(&self.stats);
        eventually("consumed tunnel bytes", move || {
            stats.consumed.get() >= bytes
        })
        .await;
    }

    async fn transaction_closed(&self) {
        let stats = Rc::clone(&self.stats);
        eventually("the tunnel's transaction close", move || {
            stats.closed.get() > 0
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// `ConnectHandler` proxy tunnels.

/// `HTTPConnectTransportTest::connect()` on a new proxy session.
async fn connect(
    context: &WorkerContext,
    proxy: &H2cProxy,
    session: SessionParams,
    egress_buffer_size: u32,
) -> (ClientSession, Rc<HttpConnectTransport>) {
    let session = proxy.connect(context, session).await;
    let stream = HttpConnectStream::connect_unique(
        context,
        session.clone(),
        session
            .reserve_request()
            .expect("proxy session reservation"),
        AUTHORITY,
        CONNECT_TIMEOUT,
        headers(&[("Foo", "Bar")]),
        egress_buffer_size,
        CancellationToken::never(),
    )
    .await
    .expect("the proxy accepts the CONNECT");
    (session, HttpConnectTransport::new(context, stream))
}

/// `HTTPConnectTransportTest::connect(session)`: a shared CONNECT.
async fn connect_shared(
    context: &WorkerContext,
    session: &ClientSession,
) -> Rc<HttpConnectTransport> {
    let stream = HttpConnectStream::connect(
        context,
        session,
        session
            .reserve_request()
            .expect("proxy session reservation"),
        AUTHORITY,
        CONNECT_TIMEOUT,
        headers(&[("Foo", "Bar")]),
        EGRESS_BUFFER_SIZE,
        CancellationToken::never(),
    )
    .await
    .expect("the proxy accepts the CONNECT");
    HttpConnectTransport::new(context, stream)
}

fn current_resp_source(proxy: &H2cProxy) -> HttpStreamSourceProducer {
    proxy
        .log
        .cur_resp_source
        .borrow()
        .clone()
        .expect("a live tunnel")
}

// ---------------------------------------------------------------------------
// Test-driven proxy tunnels.

/// The proxy side of one CONNECT stream, driven by the test.
struct ProxyTunnel {
    request: Rc<RefCell<HttpSourceHolder>>,
    response: HttpStreamSourceProducer,
}

impl ProxyTunnel {
    fn accept(&self) {
        let mut message = response(200);
        message.headers_mut().add(b"X-Connected-To", "1.2.3.4");
        assert!(
            self.response.headers(message, false),
            "the proxy response head is queued"
        );
    }

    fn send(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, false);
    }

    fn finish(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, true);
    }

    /// Reads the next tunnel bytes or EOM.
    async fn next(&self) -> Result<(Vec<u8>, bool), HttpError> {
        loop {
            let read = self.request.borrow_mut().read_body_event(u32::MAX);
            let event = read.await?;
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Suspend(resume) => {
                    resume.await;
                }
                HttpBodyEventKind::Body(body) => return Ok((bytes_of(&body), eom)),
                _ if eom => return Ok((Vec::new(), true)),
                _ => {}
            }
        }
    }

    /// Reads tunnel bytes until EOM, or returns the request's read error.
    async fn read_to_end(&self) -> Result<Vec<u8>, HttpError> {
        let mut received = Vec::new();
        loop {
            let (bytes, eom) = self.next().await?;
            received.extend_from_slice(&bytes);
            if eom {
                return Ok(received);
            }
        }
    }

    /// Reads exactly `length` tunnel bytes, none of them followed by EOM.
    async fn read_exact(&self, length: usize) -> Vec<u8> {
        let mut received = Vec::new();
        while received.len() < length {
            let (bytes, eom) = self.next().await.expect("tunnel bytes");
            received.extend_from_slice(&bytes);
            assert!(!eom, "the tunnel ended after {} bytes", received.len());
        }
        assert_eq!(received.len(), length);
        received
    }

    /// Asserts that nothing arrives within `QUIET`.
    async fn assert_quiet(&self) {
        let next = tokio::time::timeout(QUIET, self.next()).await;
        assert!(next.is_err(), "nothing reaches the proxy: {next:?}");
    }
}

#[derive(Default)]
struct TunnelSlots {
    tunnels: RefCell<VecDeque<ProxyTunnel>>,
    held: RefCell<Vec<(Rc<RefCell<HttpSourceHolder>>, HttpStreamSourceProducer)>>,
}

struct TunnelHandler {
    slots: Rc<TunnelSlots>,
}

impl HttpHandler for TunnelHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let slots = Rc::clone(&self.slots);
        Box::pin(async move {
            let head = request.read_header_event().await?.into_headers();
            assert_eq!(head.request_method(), Some("CONNECT"));
            let source = HttpStreamSource::new_async(None, None, 1 << 20).map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
            })?;
            let response = source.producer();
            let request = Rc::new(RefCell::new(request));
            slots
                .held
                .borrow_mut()
                .push((Rc::clone(&request), response.clone()));
            slots
                .tunnels
                .borrow_mut()
                .push_back(ProxyTunnel { request, response });
            Ok(HttpSourceHolder::new(Box::new(source)))
        })
    }
}

type PeerResets = Arc<Mutex<Vec<HttpErrorCode>>>;

/// Records the RST_STREAM frames that the proxy session receives.
struct PeerResetRecorder(PeerResets);

impl LifecycleObserver for PeerResetRecorder {
    fn on_event(&self, event: LifecycleEvent) {
        if let LifecycleEvent::PeerReset { code, .. } = event {
            self.0.lock().expect("resets").push(code);
        }
    }
}

/// An h2c `roxygen_server` proxy whose CONNECT streams the test drives.
struct TunnelProxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
    slots: Rc<TunnelSlots>,
    resets: PeerResets,
    connections: RefCell<Vec<ServerConnection>>,
}

impl TunnelProxy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self {
            listener,
            address,
            slots: Rc::default(),
            resets: PeerResets::default(),
            connections: RefCell::default(),
        }
    }

    async fn connect(&self, context: &WorkerContext, session: SessionParams) -> ClientSession {
        let accept = async {
            let (io, remote) = self.listener.accept().await.expect("proxy accept");
            let transport = adopt_local(
                context,
                EstablishedIo::new(
                    io,
                    TransportEndpoint::Socket(socket_address(self.address)),
                    TransportEndpoint::Socket(socket_address(remote)),
                    None,
                ),
                ByteStreamLimits::new(256 * 1024, 256 * 1024, 8).expect("proxy limits"),
            )
            .expect("proxy transport");
            let mut options =
                ServeTransportOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            let mut initial_window = options.session.settings[0];
            initial_window.identifier = HTTP2_INITIAL_WINDOW_SIZE;
            initial_window.value = PROXY_WINDOW as u64;
            options.session.settings = vec![initial_window];
            let resets = Arc::clone(&self.resets);
            options.session.lifecycle_observer_factory = Some(Arc::new(move || {
                Rc::new(PeerResetRecorder(Arc::clone(&resets))) as Rc<dyn LifecycleObserver>
            }));
            let handler: HttpHandlerRef = Rc::new(TunnelHandler {
                slots: Rc::clone(&self.slots),
            });
            let connection =
                serve_transport(context, transport, handler, options).expect("proxy session");
            self.connections.borrow_mut().push(connection);
        };
        let connect = HttpConnector::connect(
            context,
            socket_address(self.address),
            Duration::ZERO,
            ConnectionParams {
                plaintext_protocol: CodecProtocol::Http2,
                ..ConnectionParams::default()
            },
            session,
            CancellationToken::never(),
        );
        let ((), session) = tokio::join!(accept, connect);
        session.expect("client connects to the proxy")
    }

    async fn tunnel(&self) -> ProxyTunnel {
        eventually("a CONNECT at the proxy", || {
            !self.slots.tunnels.borrow().is_empty()
        })
        .await;
        self.slots
            .tunnels
            .borrow_mut()
            .pop_front()
            .expect("a queued tunnel")
    }

    fn peer_resets(&self) -> Vec<HttpErrorCode> {
        self.resets.lock().expect("resets").clone()
    }

    fn release(&self) {
        self.slots.tunnels.borrow_mut().clear();
        let held = std::mem::take(&mut *self.slots.held.borrow_mut());
        for (request, response) in held {
            drop(std::mem::take(&mut *request.borrow_mut()));
            response.abort(HttpErrorCode::CANCEL, b"");
        }
        for connection in self.connections.borrow().iter() {
            connection.cancel();
        }
    }

    async fn shutdown(self) {
        self.release();
        let connections = std::mem::take(&mut *self.connections.borrow_mut());
        for connection in &connections {
            let _ = tokio::time::timeout(WAIT_DEADLINE, connection.wait_closed())
                .await
                .expect("a cancelled proxy session closes");
        }
    }
}

impl Drop for TunnelProxy {
    fn drop(&mut self) {
        self.release();
    }
}

/// A `connect_unique` tunnel that the test-driven proxy accepted.
async fn tunnel_transport(
    context: &WorkerContext,
    proxy: &TunnelProxy,
    session: SessionParams,
    egress_buffer_size: u32,
) -> (ClientSession, Rc<HttpConnectTransport>, ProxyTunnel) {
    let session = proxy.connect(context, session).await;
    let mut connect = HttpConnectStream::connect_unique(
        context,
        session.clone(),
        session
            .reserve_request()
            .expect("proxy session reservation"),
        AUTHORITY,
        CONNECT_TIMEOUT,
        headers(&[("Foo", "Bar")]),
        egress_buffer_size,
        CancellationToken::never(),
    );
    park(&mut connect, "the connect").await;
    let tunnel = proxy.tunnel().await;
    tunnel.accept();
    let stream = connect.await.expect("CONNECT");
    (session, HttpConnectTransport::new(context, stream), tunnel)
}

/// Fills the proxy's stream window with a write that closes the egress window.
async fn stall(transport: &HttpConnectTransport, context: &WorkerContext) -> Vec<u8> {
    let stalled = vec![b's'; STALL];
    assert_written(
        write(transport, context, &stalled, Duration::ZERO).await,
        STALL,
    );
    stalled
}

// ---------------------------------------------------------------------------
// A `ConnectSession` that keeps the egress holder.

/// Keeps the tunnel's egress request holder for the test, which reads it in
/// place of the session, so the egress source's bytes are processed only when
/// the test reads them. The session commits a substitute request with the same
/// head and no `Content-Length`.
#[derive(Clone)]
struct CapturingSession {
    inner: ClientSession,
    egress: Rc<RefCell<Option<HttpSourceHolder>>>,
    response: Rc<RefCell<Option<HttpSourceHolder>>>,
    substitutes: Rc<RefCell<Vec<HttpStreamSourceProducer>>>,
}

impl ConnectSession for CapturingSession {
    fn worker_id(&self) -> WorkerId {
        ConnectSession::worker_id(&self.inner)
    }

    fn local_address(&self) -> SocketAddress {
        ConnectSession::local_address(&self.inner)
    }

    fn peer_address(&self) -> SocketAddress {
        ConnectSession::peer_address(&self.inner)
    }

    fn local_endpoint(&self) -> TransportEndpoint {
        ConnectSession::local_endpoint(&self.inner)
    }

    fn peer_endpoint(&self) -> TransportEndpoint {
        ConnectSession::peer_endpoint(&self.inner)
    }

    fn send_connect_request(
        &self,
        mut request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<UniplexResponse, HttpError>> {
        let session = self.inner.clone();
        let egress = Rc::clone(&self.egress);
        let substitutes = Rc::clone(&self.substitutes);
        let replacement = self.response.borrow_mut().take();
        Box::pin(async move {
            let mut head = request.read_header_event().await?.into_headers();
            head.headers_mut().remove(HttpHeaderCode::ContentLength);
            let substitute = HttpStreamSource::new_async(None, None, 1 << 20)
                .expect("substitute request source");
            let producer = substitute.producer();
            assert!(producer.headers(head, false));
            *egress.borrow_mut() = Some(request);
            substitutes.borrow_mut().push(producer);
            let mut response = ConnectSession::send_connect_request(
                &session,
                HttpSourceHolder::new(Box::new(substitute)),
                reservation,
                cancellation,
            )
            .await?;
            if let Some(source) = replacement {
                response.source = source;
            }
            Ok(response)
        })
    }

    fn initiate_drain(&self) -> Result<bool, HttpError> {
        ConnectSession::initiate_drain(&self.inner)
    }
}

/// A tunnel whose egress holder the test reads. Without `response`, the proxy
/// accepts the CONNECT; with it, `response` answers in place of the proxy.
async fn captured_transport(
    context: &WorkerContext,
    proxy: &TunnelProxy,
    egress_buffer_size: u32,
    connect_headers: RequestHeaderMap,
    response: Option<HttpSourceHolder>,
) -> (Rc<HttpConnectTransport>, HttpSourceHolder) {
    let scripted = response.is_some();
    let session = CapturingSession {
        inner: proxy.connect(context, SessionParams::default()).await,
        egress: Rc::default(),
        response: Rc::new(RefCell::new(response)),
        substitutes: Rc::default(),
    };
    let mut connect = HttpConnectStream::connect_unique(
        context,
        session.clone(),
        session
            .inner
            .reserve_request()
            .expect("proxy session reservation"),
        AUTHORITY,
        CONNECT_TIMEOUT,
        connect_headers,
        egress_buffer_size,
        CancellationToken::never(),
    );
    if !scripted {
        park(&mut connect, "the connect").await;
        proxy.tunnel().await.accept();
    }
    let stream = connect.await.expect("CONNECT");
    let egress = session
        .egress
        .borrow_mut()
        .take()
        .expect("the egress holder was kept");
    (HttpConnectTransport::new(context, stream), egress)
}

/// Reads the next event that carries tunnel bytes or EOM from an egress holder.
async fn next_egress(egress: &mut HttpSourceHolder) -> Result<(Vec<u8>, bool), HttpError> {
    loop {
        let event = egress.read_body_event(u32::MAX).await?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Suspend(_) => {}
            HttpBodyEventKind::Body(body) => return Ok((bytes_of(&body), eom)),
            _ if eom => return Ok((Vec::new(), true)),
            _ => {}
        }
    }
}

async fn next_egress_bytes(egress: &mut HttpSourceHolder) -> (Vec<u8>, bool) {
    next_egress(egress).await.expect("egress event")
}

async fn next_egress_error(egress: &mut HttpSourceHolder) -> HttpError {
    next_egress(egress)
        .await
        .expect_err("the egress source failed")
}

/// A test-driven CONNECT response source and its completion record.
struct ScriptedResponse {
    producer: HttpStreamSourceProducer,
    completions: Rc<RefCell<Vec<Option<HttpError>>>>,
}

struct CompletionRecorder(Rc<RefCell<Vec<Option<HttpError>>>>);

impl HttpStreamSourceCallback for CompletionRecorder {
    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        self.0.borrow_mut().push(error);
    }
}

/// A CONNECT response source that has queued a 200 head, and its producer.
fn scripted_source(
    callback: impl HttpStreamSourceCallback,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let source =
        HttpStreamSource::new_session(StreamId::new(1), callback, 65_535, Duration::from_secs(5))
            .expect("scripted response source");
    let producer = source.producer();
    let mut head = response(200);
    head.headers_mut().set(b"X-Connected-To", b"1.2.3.4");
    assert!(producer.headers(head, false));
    (HttpSourceHolder::new(Box::new(source)), producer)
}

/// A completion callback that queries the transport, as a session's
/// completion callback can run caller code inside the source's stop.
struct PeerQuery {
    transport: Rc<RefCell<Weak<HttpConnectTransport>>>,
    context: WorkerContext,
    peers: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl HttpStreamSourceCallback for PeerQuery {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        let Some(transport) = self.transport.borrow().upgrade() else {
            return;
        };
        let peer = transport.peer_address(&self.context).expect("peer address");
        self.peers.borrow_mut().push(peer.host().to_vec());
    }
}

/// A completion callback that drops the only transport handle.
struct DropTransport(Rc<RefCell<Option<Rc<HttpConnectTransport>>>>);

impl HttpStreamSourceCallback for DropTransport {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        let transport = self.0.borrow_mut().take();
        drop(transport);
    }
}

/// The step that releases a stored body read, which can run caller code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoredReadRelease {
    CancelledStartingReadPolled,
    CancelledStartingReadDropped,
    HandleDrop,
    /// `close` or `close_with_reset` while a read is parked on the stored read.
    ParkedReadClose,
    /// A read's drop of the body read that gave it data.
    ReadyBodyReadDrop,
}

/// A side call that caller code makes on a live handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SideCall {
    ShutdownWrite,
    Close,
    CloseWithReset,
}

impl SideCall {
    fn make(self, transport: &HttpConnectTransport, context: &WorkerContext) {
        match self {
            Self::ShutdownWrite => transport.shutdown_write(context).expect("shutdown_write"),
            Self::Close => transport.close(context).expect("close"),
            Self::CloseWithReset => transport
                .close_with_reset(context)
                .expect("close_with_reset"),
        }
    }
}

/// How a test releases the waker of a parked write, which the baton holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParkedWriteRelease {
    /// A later poll with another waker, while the window stays closed.
    Repoll,
    /// A later poll with another waker, after `window_open` signalled.
    RepollAfterSignal,
    /// Dropping the write.
    Drop,
}

/// The handle that caller code drops, the read and write it then polls, and
/// what those polls returned.
#[derive(Default)]
struct PostDropPolls {
    transport: Option<Rc<HttpConnectTransport>>,
    read: Option<ReadFuture>,
    write: Option<WriteFuture>,
    read_result: Option<Poll<Result<TransportRead, HttpError>>>,
    write_result: Option<Poll<Result<WriteInfo, TransportWriteError>>>,
}

impl PostDropPolls {
    /// Drops the handle, if it holds it, and then polls the read and the write
    /// it holds.
    fn drop_handle_then_poll(polls: &RefCell<Self>) {
        let (transport, read, write) = {
            let mut polls = polls.borrow_mut();
            (
                polls.transport.take(),
                polls.read.take(),
                polls.write.take(),
            )
        };
        drop(transport);
        if let Some(mut read) = read {
            let result = poll_with(&mut read, Waker::noop());
            polls.borrow_mut().read_result = Some(result);
        }
        if let Some(mut write) = write {
            let result = poll_with(&mut write, Waker::noop());
            polls.borrow_mut().write_result = Some(result);
        }
    }

    fn assert_both_cancelled(&mut self, section: impl std::fmt::Debug) {
        assert!(
            self.transport.is_none(),
            "{section:?}: the caller code dropped the handle"
        );
        match self.read_result.take() {
            Some(Poll::Ready(result)) => assert_cancelled(result),
            other => panic!("{section:?}: the caller code's read completes, got {other:?}"),
        }
        match self.write_result.take() {
            Some(Poll::Ready(result)) => assert_write_cancelled(result),
            other => panic!("{section:?}: the caller code's write completes, got {other:?}"),
        }
    }
}

/// A completion callback that drops the transport handle, if it holds it, and
/// then polls a read and a write built before Drop.
struct DropThenPoll(Rc<RefCell<PostDropPolls>>);

impl HttpStreamSourceCallback for DropThenPoll {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        PostDropPolls::drop_handle_then_poll(&self.0);
    }
}

/// The call of a caller-provided response source that runs the caller code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceStep {
    /// `set_read_timeout`, which a read calls before it starts a body read.
    SetReadTimeout,
    /// `read_body_event`, before it builds the body read.
    ReadBodyEvent,
    /// `ingress_completion`, which a read calls after it starts a body read.
    IngressCompletion,
    /// Each poll of the body read, which then gives an empty body event
    /// without EOM.
    BodyReadReady,
    /// Each poll of the body read, which then gives a body event with
    /// `BODY_READ_DATA` and no EOM.
    BodyReadDataReady,
    /// Each poll of the body read, which then gives a `Suspend` event whose
    /// resume future runs the caller code again as it drops.
    BodyReadSuspendReady,
    /// Each poll of the body read, which then gives an empty body event with
    /// EOM.
    BodyReadEomReady,
    /// Each poll of the body read, which then fails with `CANCEL` and
    /// `BODY_READ_ERROR`.
    BodyReadErrorReady,
    /// Each poll of the body read, which then stays pending.
    BodyReadPending,
    /// The drop of the body read, whose polls stay pending.
    BodyReadDrop,
    /// The drop of the body read, whose poll gives a body event with
    /// `BODY_READ_DATA` and no EOM.
    BodyReadDataDrop,
}

const BODY_READ_DATA: &[u8] = b"body read data";
const BODY_READ_ERROR: &[u8] = b"body read error";

impl SourceStep {
    /// Whether the body read's event ends the source, which its holder then
    /// retires without `stop_reading`.
    fn ends_source(self) -> bool {
        matches!(self, Self::BodyReadEomReady | Self::BodyReadErrorReady)
    }
}

/// One `stop_reading` with no error and no live body read, or none when the
/// body read's event at `step` ended the source.
fn assert_stopped_once(
    step: SourceStep,
    stops: &RefCell<Vec<Option<HttpErrorCode>>>,
    live_at_stops: &RefCell<Vec<usize>>,
    section: impl std::fmt::Debug,
) {
    let (expected_stops, expected_live): (&[Option<HttpErrorCode>], &[usize]) =
        if step.ends_source() {
            (&[], &[])
        } else {
            (&[None], &[0])
        };
    assert_eq!(
        *stops.borrow(),
        expected_stops,
        "{section:?}: the source stops once, unless its last event ended it"
    );
    assert_eq!(
        *live_at_stops.borrow(),
        expected_live,
        "{section:?}: the stop comes after the read drops the body read"
    );
}

/// What a read whose step found Drop returns: what its body read gave at
/// `step`, or else `CORO_CANCELLED`.
fn assert_body_read_result(step: SourceStep, result: Result<TransportRead, HttpError>) {
    match step {
        SourceStep::BodyReadDataReady => assert_data(result, BODY_READ_DATA),
        SourceStep::BodyReadEomReady => assert_eof(result),
        SourceStep::BodyReadErrorReady => assert_cause(
            &read_error_cause(result),
            HttpErrorCode::CANCEL,
            BODY_READ_ERROR,
        ),
        _ => assert_cancelled(result),
    }
}

/// A caller-provided CONNECT response source with a 200 head, as a caller's
/// `ConnectSession` can supply. It runs `caller_code` at `step`, which the
/// CONNECT itself also reaches for `set_read_timeout` and
/// `ingress_completion`, and `drop_code`, if any, as each body read drops. It
/// records each `stop_reading`, with how many of its body reads were not yet
/// dropped then.
struct CallerSource {
    step: SourceStep,
    caller_code: Rc<dyn Fn()>,
    drop_code: Option<Rc<dyn Fn()>>,
    stops: Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
    live_reads: Rc<Cell<usize>>,
    live_at_stops: Rc<RefCell<Vec<usize>>>,
}

/// A resume future that never resumes and runs its code as it drops.
struct RunsOnDrop(Rc<dyn Fn()>);

impl Future for RunsOnDrop {
    type Output = ResumeStatus;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for RunsOnDrop {
    fn drop(&mut self) {
        (self.0)();
    }
}

/// Counts a caller-provided source's body read until it drops, which first
/// runs `on_drop`, if any.
struct LiveRead {
    live_reads: Rc<Cell<usize>>,
    on_drop: Option<Rc<dyn Fn()>>,
}

impl LiveRead {
    fn new(live_reads: &Rc<Cell<usize>>, on_drop: Option<Rc<dyn Fn()>>) -> Self {
        live_reads.set(live_reads.get() + 1);
        Self {
            live_reads: Rc::clone(live_reads),
            on_drop,
        }
    }
}

impl Drop for LiveRead {
    fn drop(&mut self) {
        if let Some(on_drop) = &self.on_drop {
            on_drop();
        }
        self.live_reads.set(self.live_reads.get() - 1);
    }
}

impl CallerSource {
    /// The source, and the record of its `stop_reading` calls.
    fn new(
        step: SourceStep,
        caller_code: impl Fn() + 'static,
    ) -> (HttpSourceHolder, Rc<RefCell<Vec<Option<HttpErrorCode>>>>) {
        let (source, stops, _) = Self::counting_live_reads(step, caller_code);
        (source, stops)
    }

    /// The source, the record of its `stop_reading` calls, and how many of
    /// its body reads were live at each.
    fn counting_live_reads(
        step: SourceStep,
        caller_code: impl Fn() + 'static,
    ) -> (
        HttpSourceHolder,
        Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
        Rc<RefCell<Vec<usize>>>,
    ) {
        Self::build(step, Rc::new(caller_code), None)
    }

    /// As `counting_live_reads`, and each body read runs `drop_code` as it
    /// drops.
    fn with_drop_code(
        step: SourceStep,
        caller_code: impl Fn() + 'static,
        drop_code: impl Fn() + 'static,
    ) -> (
        HttpSourceHolder,
        Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
        Rc<RefCell<Vec<usize>>>,
    ) {
        Self::build(step, Rc::new(caller_code), Some(Rc::new(drop_code)))
    }

    fn build(
        step: SourceStep,
        caller_code: Rc<dyn Fn()>,
        drop_code: Option<Rc<dyn Fn()>>,
    ) -> (
        HttpSourceHolder,
        Rc<RefCell<Vec<Option<HttpErrorCode>>>>,
        Rc<RefCell<Vec<usize>>>,
    ) {
        let stops = Rc::new(RefCell::new(Vec::new()));
        let live_at_stops = Rc::new(RefCell::new(Vec::new()));
        let source = Self {
            step,
            caller_code,
            drop_code,
            stops: Rc::clone(&stops),
            live_reads: Rc::default(),
            live_at_stops: Rc::clone(&live_at_stops),
        };
        (
            HttpSourceHolder::new(Box::new(source)),
            stops,
            live_at_stops,
        )
    }

    fn run_at(&self, step: SourceStep) {
        if self.step == step {
            (self.caller_code)();
        }
    }
}

impl HttpSource for CallerSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let head = HttpHeaderEvent::new(response(200), false).expect("200 head");
        Box::pin(std::future::ready(Ok(head)))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.run_at(SourceStep::ReadBodyEvent);
        let step = self.step;
        let caller_code = Rc::clone(&self.caller_code);
        let on_drop = self.drop_code.clone().or_else(|| {
            matches!(
                step,
                SourceStep::BodyReadDrop | SourceStep::BodyReadDataDrop
            )
            .then(|| Rc::clone(&caller_code))
        });
        let live = LiveRead::new(&self.live_reads, on_drop);
        Box::pin(poll_fn(move |_| {
            let _live = &live;
            match step {
                SourceStep::BodyReadReady => {
                    caller_code();
                    Poll::Ready(Ok(HttpBodyEvent::body(BufQueue::new(0), false)))
                }
                SourceStep::BodyReadDataReady => {
                    caller_code();
                    Poll::Ready(Ok(HttpBodyEvent::body(queue(BODY_READ_DATA), false)))
                }
                SourceStep::BodyReadSuspendReady => {
                    caller_code();
                    let resume = RunsOnDrop(Rc::clone(&caller_code));
                    Poll::Ready(Ok(HttpBodyEvent::suspend(Box::pin(resume))))
                }
                SourceStep::BodyReadEomReady => {
                    caller_code();
                    Poll::Ready(Ok(HttpBodyEvent::body(BufQueue::new(0), true)))
                }
                SourceStep::BodyReadErrorReady => {
                    caller_code();
                    Poll::Ready(Err(HttpError::with_message(
                        HttpErrorCode::CANCEL,
                        BODY_READ_ERROR,
                    )))
                }
                SourceStep::BodyReadPending => {
                    caller_code();
                    Poll::Pending
                }
                SourceStep::BodyReadDrop => Poll::Pending,
                SourceStep::BodyReadDataDrop => {
                    Poll::Ready(Ok(HttpBodyEvent::body(queue(BODY_READ_DATA), false)))
                }
                _ => Poll::Ready(Ok(HttpBodyEvent::body(queue(b"never read"), false))),
            }
        }))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.stops.borrow_mut().push(error);
        self.live_at_stops.borrow_mut().push(self.live_reads.get());
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.run_at(SourceStep::IngressCompletion);
        None
    }

    fn set_read_timeout(&mut self, _timeout: Duration) {
        self.run_at(SourceStep::SetReadTimeout);
    }
}

/// A completion callback that polls the read in `read` once with `waker`, in
/// place, and records what the poll gave.
struct PollReadOnComplete {
    read: Rc<RefCell<Option<ReadFuture>>>,
    waker: Waker,
    result: Rc<RefCell<Option<Poll<Result<TransportRead, HttpError>>>>>,
}

impl HttpStreamSourceCallback for PollReadOnComplete {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        let Some(mut read) = self.read.borrow_mut().take() else {
            return;
        };
        let result = poll_with(&mut read, &self.waker);
        if result.is_pending() {
            *self.read.borrow_mut() = Some(read);
        }
        *self.result.borrow_mut() = Some(result);
    }
}

/// A completion callback that closes the transport.
struct CloseTransport {
    transport: Rc<RefCell<Weak<HttpConnectTransport>>>,
    context: WorkerContext,
}

impl HttpStreamSourceCallback for CloseTransport {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        let Some(transport) = self.transport.borrow().upgrade() else {
            return;
        };
        transport.close(&self.context).expect("reentrant close");
    }
}

impl ScriptedResponse {
    fn new() -> (HttpSourceHolder, Self) {
        let completions = Rc::default();
        let (source, producer) = scripted_source(CompletionRecorder(Rc::clone(&completions)));
        (
            source,
            Self {
                producer,
                completions,
            },
        )
    }

    /// Queues a body event with EOM, then one more body event, which the
    /// source rejects with `INVALID_STATE_TRANSITION`
    /// (HTTPStreamSource.h:224-242).
    fn error_after_eom(&self) {
        self.producer.body(queue(b"last"), 0, true);
        self.producer.body(queue(b"extra"), 0, false);
    }

    fn completion_codes(&self) -> Vec<Option<HttpErrorCode>> {
        self.completions
            .borrow()
            .iter()
            .map(|error| error.as_ref().map(HttpError::code))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Scripted HTTP/1.1 proxies.

const H1_CONNECTED: &[u8] =
    b"HTTP/1.1 200 Connection Established\r\nX-Connected-To: 1.2.3.4\r\n\r\n";

/// Reads one HTTP/1.1 head from a scripted proxy's I/O.
async fn read_h1_head(io: &mut (impl AsyncRead + Unpin)) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(
            tokio::time::timeout(WAIT_DEADLINE, io.read_u8())
                .await
                .expect("the CONNECT head arrives")
                .expect("CONNECT head byte"),
        );
    }
    head
}

/// A scripted HTTP/1.1 proxy: the test reads and writes its raw TCP socket.
struct RawH1Proxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
}

impl RawH1Proxy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self { listener, address }
    }

    async fn connect(
        &self,
        context: &WorkerContext,
        session: SessionParams,
    ) -> (ClientSession, TcpStream) {
        let connect = HttpConnector::connect(
            context,
            socket_address(self.address),
            Duration::ZERO,
            ConnectionParams::default(),
            session,
            CancellationToken::never(),
        );
        let (accepted, session) = tokio::join!(self.listener.accept(), connect);
        let (socket, _) = accepted.expect("proxy accept");
        (session.expect("client connects to the proxy"), socket)
    }
}

/// A `connect_unique` tunnel that a scripted HTTP/1.1 proxy accepted.
async fn h1_tunnel(
    context: &WorkerContext,
    session: &ClientSession,
    socket: &mut TcpStream,
    connect_timeout: Duration,
) -> Rc<HttpConnectTransport> {
    let mut connect = HttpConnectStream::connect_unique(
        context,
        session.clone(),
        session
            .reserve_request()
            .expect("proxy session reservation"),
        AUTHORITY,
        connect_timeout,
        headers(&[("Foo", "Bar")]),
        EGRESS_BUFFER_SIZE,
        CancellationToken::never(),
    );
    park(&mut connect, "the connect").await;
    read_h1_head(socket).await;
    socket
        .write_all(H1_CONNECTED)
        .await
        .expect("proxy response");
    HttpConnectTransport::new(context, connect.await.expect("CONNECT"))
}

/// An HTTP/1.1 `ClientSession` over an in-memory duplex whose peer the test
/// scripts, in place of `TestCoroTransport`.
fn duplex_h1_session(
    context: &WorkerContext,
    session: SessionParams,
    local: TransportEndpoint,
    peer: TransportEndpoint,
) -> (ClientSession, DuplexStream) {
    let (client, scripted) = tokio::io::duplex(256 * 1024);
    let transport = adopt_local(
        context,
        EstablishedIo::new(client, local, peer, None),
        ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("duplex limits"),
    )
    .expect("duplex transport");
    let mut options = ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.session = session;
    let session =
        ClientSession::attach_transport(context, transport, options).expect("duplex session");
    (session, scripted)
}

// ---------------------------------------------------------------------------
// A scripted h2c proxy that reads and writes HTTP/2 frames.

const FRAME_DATA: u8 = 0x0;
const FRAME_HEADERS: u8 = 0x1;
const FRAME_RST_STREAM: u8 = 0x3;
const FRAME_SETTINGS: u8 = 0x4;
const FLAG_END_STREAM: u8 = 0x1;
const FLAG_ACK: u8 = 0x1;
const FLAG_END_HEADERS: u8 = 0x4;
const FLAG_PADDED: u8 = 0x8;
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;

struct H2Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

async fn read_frame(socket: &mut TcpStream) -> Option<H2Frame> {
    let mut header = [0; 9];
    let read = tokio::time::timeout(WAIT_DEADLINE, socket.read_exact(&mut header))
        .await
        .expect("the client sends a frame");
    if read.is_err() {
        return None;
    }
    let length =
        usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
    let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let mut payload = vec![0; length];
    socket.read_exact(&mut payload).await.ok()?;
    Some(H2Frame {
        kind: header[3],
        flags: header[4],
        stream,
        payload,
    })
}

async fn write_frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = u32::try_from(payload.len()).expect("frame length");
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes()[1..]);
    frame.push(kind);
    frame.push(flags);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await.expect("proxy frame");
}

/// Reads the client preface, sends `settings`, acknowledges the client's
/// SETTINGS, and returns the stream ID of the client's CONNECT.
async fn h2c_accept_connect(socket: &mut TcpStream, settings: &[(u16, u32)]) -> u32 {
    let mut preface = [0; 24];
    tokio::time::timeout(WAIT_DEADLINE, socket.read_exact(&mut preface))
        .await
        .expect("the client preface arrives")
        .expect("client preface");
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    let mut payload = Vec::new();
    for (identifier, value) in settings {
        payload.extend_from_slice(&identifier.to_be_bytes());
        payload.extend_from_slice(&value.to_be_bytes());
    }
    write_frame(socket, FRAME_SETTINGS, 0, 0, &payload).await;
    loop {
        let frame = read_frame(socket).await.expect("the client's CONNECT");
        match frame.kind {
            FRAME_SETTINGS if frame.flags & FLAG_ACK == 0 => {
                write_frame(socket, FRAME_SETTINGS, FLAG_ACK, 0, &[]).await;
            }
            FRAME_HEADERS => {
                assert_ne!(frame.flags & FLAG_END_HEADERS, 0, "one HEADERS frame");
                return frame.stream;
            }
            _ => {}
        }
    }
}

/// Answers the CONNECT with `status`: a literal `:status` field whose name is
/// HPACK static-table entry 8.
async fn h2c_respond(socket: &mut TcpStream, stream: u32, status: u16) {
    let status = status.to_string();
    let mut block = vec![0x08, u8::try_from(status.len()).expect("status length")];
    block.extend_from_slice(status.as_bytes());
    write_frame(socket, FRAME_HEADERS, FLAG_END_HEADERS, stream, &block).await;
}

/// Reads frames until the client resets `stream`, and returns the DATA bytes
/// sent on it and the reset's error code.
async fn read_until_reset(socket: &mut TcpStream, stream: u32) -> (Vec<u8>, u32) {
    let mut data = Vec::new();
    loop {
        let frame = read_frame(socket)
            .await
            .expect("the client resets the stream");
        if frame.stream != stream {
            continue;
        }
        match frame.kind {
            FRAME_DATA => data.extend_from_slice(&frame.payload),
            FRAME_RST_STREAM => {
                let code = u32::from_be_bytes(frame.payload[..4].try_into().expect("code"));
                return (data, code);
            }
            _ => {}
        }
    }
}

struct RawH2cProxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
}

impl RawH2cProxy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self { listener, address }
    }

    /// A `connect_unique` tunnel that the socket answered with `status`.
    async fn tunnel(
        &self,
        context: &WorkerContext,
        session: SessionParams,
        settings: &[(u16, u32)],
        status: u16,
    ) -> (ClientSession, Rc<HttpConnectTransport>, TcpStream, u32) {
        let connect = HttpConnector::connect(
            context,
            socket_address(self.address),
            Duration::ZERO,
            ConnectionParams {
                plaintext_protocol: CodecProtocol::Http2,
                ..ConnectionParams::default()
            },
            session,
            CancellationToken::never(),
        );
        let (accepted, session) = tokio::join!(self.listener.accept(), connect);
        let (mut socket, _) = accepted.expect("proxy accept");
        let session = session.expect("client connects to the proxy");
        let connect = HttpConnectStream::connect_unique(
            context,
            session.clone(),
            session
                .reserve_request()
                .expect("proxy session reservation"),
            AUTHORITY,
            CONNECT_TIMEOUT,
            headers(&[("Foo", "Bar")]),
            EGRESS_BUFFER_SIZE,
            CancellationToken::never(),
        );
        let (stream, id) = tokio::join!(connect, async {
            let id = h2c_accept_connect(&mut socket, settings).await;
            h2c_respond(&mut socket, id, status).await;
            id
        });
        let transport = HttpConnectTransport::new(context, stream.expect("a 2xx connects"));
        (session, transport, socket, id)
    }
}

/// Records the code of each cancelled byte event.
#[derive(Default)]
struct CancelledByteEvents(RefCell<Vec<HttpErrorCode>>);

impl HttpByteEventCallback for CancelledByteEvents {
    fn on_byte_event(&self, _event: HttpByteEvent) {}

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0.borrow_mut().push(error.code());
    }
}

/// Forwards to a tunnel's transport and records each read an inner session
/// makes: the maximum and allocation size it asks for, and the bytes and EOF
/// it gets.
struct RecordingTransport {
    inner: Rc<HttpConnectTransport>,
    requested: RefCell<Vec<(usize, usize)>>,
    returned: Rc<RefCell<Vec<(usize, bool)>>>,
}

impl ByteStreamTransport for RecordingTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
        self.requested.borrow_mut().push((maximum, allocation_size));
        let read = ByteStreamTransport::read(
            &*self.inner,
            context,
            maximum,
            allocation_size,
            timeout,
            cancellation,
        );
        let returned = Rc::clone(&self.returned);
        Box::pin(async move {
            let result = read.await;
            if let Ok(read) = &result {
                returned
                    .borrow_mut()
                    .push((read.bytes_read(), read.is_eof()));
            }
            result
        })
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        ByteStreamTransport::write(&*self.inner, context, data, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::shutdown_write(&*self.inner, context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::close(&*self.inner, context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::close_with_reset(&*self.inner, context)
    }

    fn local_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        ByteStreamTransport::local_address(&*self.inner, context)
    }

    fn peer_address(&self, context: &WorkerContext) -> Result<SocketAddress, HttpError> {
        ByteStreamTransport::peer_address(&*self.inner, context)
    }

    fn local_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        ByteStreamTransport::local_endpoint(&*self.inner, context)
    }

    fn peer_endpoint(&self, context: &WorkerContext) -> Result<TransportEndpoint, HttpError> {
        ByteStreamTransport::peer_endpoint(&*self.inner, context)
    }

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        ByteStreamTransport::peer_identity(&*self.inner, context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        ByteStreamTransport::byte_event_capability(&*self.inner, context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        ByteStreamTransport::register_byte_event(&*self.inner, context, registration)
    }
}

// ---------------------------------------------------------------------------
// group-134: the frozen cases.

#[test]
fn connect_success_reports_x_connected_to_peer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            // Peer address is the 'X-Connected-To' address.
            let peer = transport.peer_address(&context).expect("peer address");
            assert_eq!(peer.host(), b"1.2.3.4");
            assert_eq!(peer.port(), 443, "the authority's port");
            let local = transport.local_address(&context).expect("local address");
            assert_eq!(
                proxy.log.requests.borrow()[0].peer,
                Some(TransportEndpoint::Socket(local))
            );
            assert_eq!(transport.worker_id(), context.worker_id());
            assert_eq!(
                transport
                    .byte_event_capability(&context)
                    .expect("capability"),
                ByteEventCapability::Unsupported,
                "getTransport() is null"
            );
            assert_eq!(
                transport.peer_identity(&context).expect("peer identity"),
                None,
                "getPeerCertificate() is null"
            );
            let callback = Rc::new(CancelledByteEvents::default());
            let registration = HttpByteEventRegistration::new(
                None,
                ByteEventType::TransportWrite.bit(),
                &callback,
            );
            assert_eq!(
                transport.register_byte_event(&context, registration),
                Err(ByteEventRegistrationError::Unsupported)
            );
            assert_eq!(
                callback.0.borrow().len(),
                1,
                "the registration is cancelled"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn shared_connect_multiplexes_one_h2_session() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let cs1 = connect_shared(&context, &session).await;
            let cs2 = connect_shared(&context, &session).await;
            let port = cs1.local_address(&context).expect("local address").port();
            assert_eq!(
                port,
                cs2.local_address(&context).expect("local address").port()
            );
            drop(cs1);
            drop(cs2);
            let cs1 = connect_shared(&context, &session).await;
            assert_eq!(
                port,
                cs1.local_address(&context).expect("local address").port()
            );
            drop(cs1);
            tokio::task::yield_now().await;
            assert!(
                session.supports_more_transactions(),
                "shared tunnels never drain their session"
            );
            session.initiate_drain().expect("drain");
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancelled_proxy_dial_and_socket_connect() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let address = socket_address(proxy.address());
            let cancel = CancellationSource::new();
            // The token is cancelled while `connect()` waits.
            let mut connect = {
                let context = context.clone();
                let address = address.clone();
                let token = cancel.token();
                Box::pin(async move {
                    let session = HttpConnector::connect(
                        &context,
                        address,
                        Duration::ZERO,
                        ConnectionParams {
                            plaintext_protocol: CodecProtocol::Http2,
                            ..ConnectionParams::default()
                        },
                        SessionParams::default(),
                        token.clone(),
                    )
                    .await
                    .map_err(|error| ("proxy dial", error))?;
                    let reservation = session
                        .reserve_request()
                        .map_err(|error| ("CONNECT", error))?;
                    HttpConnectStream::connect_unique(
                        &context,
                        session.clone(),
                        reservation,
                        AUTHORITY,
                        CONNECT_TIMEOUT,
                        headers(&[("Foo", "Bar")]),
                        EGRESS_BUFFER_SIZE,
                        token,
                    )
                    .await
                    .map(drop)
                    .map_err(|error| ("CONNECT", error))
                })
            };
            park(&mut connect, "the connect").await;
            cancel.request_cancellation();
            let (stage, error) = connect.await.expect_err("the connect is cancelled");
            assert_eq!(stage, "proxy dial", "the CONNECT exchange is not reached");
            assert_cancelled::<()>(Err(error));
            // The token was cancelled before the socket connect was called.
            let connected = TcpTransport::connect(
                &context,
                address,
                ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("limits"),
                Duration::ZERO,
                cancel.token(),
            )
            .await;
            assert_cancelled(connected.map(drop));
            proxy.shutdown().await;
        })
    });
}

#[test]
fn eom_on_connect_is_write_only() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let stream = HttpConnectStream::connect_unique(
                &context,
                session.clone(),
                session.reserve_request().expect("reservation"),
                AUTHORITY,
                CONNECT_TIMEOUT,
                headers(&[("EomOnConnect", "True")]),
                EGRESS_BUFFER_SIZE,
                CancellationToken::never(),
            )
            .await
            .expect("CONNECT");
            assert!(!stream.can_read());
            assert!(stream.can_write());
            let transport = HttpConnectTransport::new(&context, stream);
            assert_eof(read(&transport, &context, 1, Duration::ZERO).await);
            assert_written(
                write(&transport, &context, b"a", Duration::from_millis(100)).await,
                1,
            );
            transport.shutdown_write(&context).expect("shutdown_write");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_scripted_connect_response_with_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let events = Rc::new(ClientEvents::default());
            let (session, mut peer) = duplex_h1_session(
                &context,
                SessionParams {
                    connection_read_timeout: Some(Duration::from_secs(1)),
                    stream_read_timeout: Some(Duration::from_secs(1)),
                    write_timeout: Some(Duration::from_secs(1)),
                    lifecycle_observer: Some(events.clone()),
                    ..SessionParams::default()
                },
                TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 1)),
                TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 2)),
            );
            let client = async {
                let stream = HttpConnectStream::connect_unique(
                    &context,
                    session.clone(),
                    session.reserve_request().expect("reservation"),
                    AUTHORITY,
                    Duration::from_secs(1),
                    RequestHeaderMap::new(),
                    64 * 1024,
                    CancellationToken::never(),
                )
                .await
                .expect("CONNECT");
                assert!(!stream.can_read());
                assert!(stream.can_write());
                let transport = HttpConnectTransport::new(&context, stream);
                assert_eof(read(&transport, &context, 1, Duration::ZERO).await);
                assert_written(
                    write(&transport, &context, b"a", Duration::from_millis(100)).await,
                    1,
                );
                transport.shutdown_write(&context).expect("shutdown_write");
            };
            let scripted = async {
                // `observer.requestWritten`.
                read_h1_head(&mut peer).await;
                peer.write_all(
                    b"HTTP/1.1 200 Connection Established\r\nX-Connected-To: 1.2.3.4\r\n\r\n",
                )
                .await
                .expect("scripted response");
                peer.shutdown().await.expect("scripted EOF");
            };
            tokio::join!(client, scripted);
            drop(session);
            let destroyed = Rc::clone(&events);
            eventually("the session's destruction", move || {
                destroyed.destroyed.get()
            })
            .await;
            drop(peer);
        })
    });
}

#[test]
fn read_write_fills_h2_window() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            // Should fill window.
            const BUFFER_SIZE: usize = 65 * 1024;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let sent = vec![b'a'; BUFFER_SIZE];
            assert_written(
                write(&transport, &context, &sent, Duration::from_millis(100)).await,
                BUFFER_SIZE,
            );
            assert_eq!(
                read_all(&transport, &context, BUFFER_SIZE, Duration::ZERO).await,
                sent
            );
            // A read with 0 length returns at once, without EOF.
            let empty = read(&transport, &context, 0, Duration::ZERO)
                .await
                .expect("zero-length read");
            assert_eq!(empty.bytes_read(), 0);
            assert!(!empty.is_eof());
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_with_allocation_size_then_double_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            const BUFFER_SIZE: usize = 55 * 1184;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let sent = vec![b'a'; BUFFER_SIZE];
            assert_written(
                write(&transport, &context, &sent, Duration::from_millis(100)).await,
                BUFFER_SIZE,
            );
            // `read(rcvBuf, 1000, 1000, 0ms)`: one read buffer of 1_000 bytes.
            let mut received = Vec::new();
            while received.len() < BUFFER_SIZE {
                let read = read_with(
                    &transport,
                    &context,
                    65_536,
                    1_000,
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await
                .expect("tunnel read");
                assert!(!read.is_eof());
                assert!(read.bytes_read() <= 1_000, "{}", read.bytes_read());
                received.extend_from_slice(&bytes_of(read.data()));
            }
            // Initiate close from the peer.
            current_resp_source(&proxy).eom();
            for _ in 0..2 {
                assert_eof(
                    read_with(
                        &transport,
                        &context,
                        65_536,
                        1_000,
                        Duration::from_millis(50),
                        CancellationToken::never(),
                    )
                    .await,
                );
            }
            assert_eq!(received, sent);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            // `collectAll` runs `requestCancellation` before the read starts.
            let cancel = cancelled_source();
            for _ in 0..2 {
                assert_cancelled(
                    read_with(
                        &transport,
                        &context,
                        1024,
                        1024,
                        Duration::ZERO,
                        cancel.token(),
                    )
                    .await,
                );
            }
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_timeout_is_read_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            // The tunnel stays readable (ERR-002).
            assert_written(
                write(&transport, &context, b"ping", Duration::ZERO).await,
                4,
            );
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"ping"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn queued_event_survives_read_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let observed = Observed::new();
            let (session, transport) =
                connect(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            // The proxy echoes only what the tunnel sends, so this read times out.
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            // Instruct the proxy to send the echo and the h2 EOM together.
            let mut sent = vec![0; 1024];
            sent[..9].copy_from_slice(b"write_fin");
            assert_written(
                write(&transport, &context, &sent, Duration::ZERO).await,
                1024,
            );
            // Stands in for MockHTTPSessionStats: the echo is buffered.
            observed.buffered(1024).await;
            let read = read(&transport, &context, 1024, READ_TIMEOUT)
                .await
                .expect("the queued event survives the read timeout");
            assert!(read.bytes_read() > 0);
            assert_eof(
                read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    READ_TIMEOUT,
                    CancellationToken::never(),
                )
                .await,
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn peer_abort_fails_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let response = proxy
                .log
                .cur_resp_source
                .borrow_mut()
                .take()
                .expect("a live tunnel");
            response.abort(HttpErrorCode::CANCEL, b"");
            // Frozen's 50 ms read can time out under load before the abort
            // reaches the client.
            let cause = read_error_cause(read(&transport, &context, 1024, WAIT_DEADLINE).await);
            assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn vectored_write_echo() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            const BUFFER_SIZE: usize = 65_536;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let mut sent = BufQueue::new(2 * BUFFER_SIZE);
            sent.push_vec(vec![b'a'; BUFFER_SIZE])
                .expect("first segment");
            sent.push_vec(vec![b'b'; BUFFER_SIZE])
                .expect("second segment");
            assert_eq!(sent.segment_count(), 2);
            assert_written(
                transport
                    .write(&context, sent, Duration::ZERO, CancellationToken::never())
                    .await,
                2 * BUFFER_SIZE,
            );
            assert_eq!(
                read_all(
                    &transport,
                    &context,
                    BUFFER_SIZE,
                    Duration::from_millis(500)
                )
                .await,
                vec![b'a'; BUFFER_SIZE]
            );
            assert_eq!(
                read_all(
                    &transport,
                    &context,
                    BUFFER_SIZE,
                    Duration::from_millis(500)
                )
                .await,
                vec![b'b'; BUFFER_SIZE]
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn write_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            // Reduce the send buffer so the write would not complete at once.
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let sent = vec![b'a'; 65_536];
            // `collectAll` runs `requestCancellation` before the write starts.
            let cancel = cancelled_source();
            for _ in 0..2 {
                assert_write_cancelled(
                    write_with(&transport, &context, &sent, Duration::ZERO, cancel.token()).await,
                );
            }
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn half_close_then_write_fails() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            transport.shutdown_write(&context).expect("shutdown_write");
            // Wait for the peer to shut down.
            for _ in 0..2 {
                assert_eof(
                    read_with(
                        &transport,
                        &context,
                        65_536,
                        1_000,
                        Duration::from_millis(500),
                        CancellationToken::never(),
                    )
                    .await,
                );
            }
            assert_write_error(
                write(&transport, &context, &vec![b'a'; 65_536], Duration::ZERO).await,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"write after close",
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn close_with_pending_read_and_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let mut pending_read = read(&transport, &context, 1024, Duration::ZERO);
            park(&mut pending_read, "the read").await;
            let sent = vec![b'a'; 256 * 1024];
            let mut pending_write = write(&transport, &context, &sent, Duration::ZERO);
            let first = poll_once(&mut pending_write).await;
            transport.close(&context).expect("close");
            // Writes complete normally.
            let written = match first {
                Poll::Ready(result) => result,
                Poll::Pending => pending_write.await,
            };
            assert_written(written, sent.len());
            // The read fails with the stopped source's error.
            let cause = read_error_cause(pending_read.await);
            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn close_with_reset_aborts_peer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            proxy.expect_exceptions();
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            transport
                .close_with_reset(&context)
                .expect("close_with_reset");
            let log = Rc::clone(&proxy.log);
            eventually("the proxy's request read to fail", move || {
                !log.read_errors.borrow().is_empty()
            })
            .await;
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn window_closing_write_completes_before_a_direct_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            const BUFFER_SIZE: usize = 64 * 3 * 1024 + 1;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let sent = vec![b'a'; BUFFER_SIZE];
            let mut pending_write = write(&transport, &context, &sent, Duration::from_millis(100));
            let first = poll_once(&mut pending_write).await;
            let Poll::Ready(written) = first else {
                panic!("the write finds the baton signalled and completes at once");
            };
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_written(written, BUFFER_SIZE);
            assert_eq!(
                read_all(
                    &transport,
                    &context,
                    BUFFER_SIZE,
                    Duration::from_millis(500)
                )
                .await,
                sent
            );
            assert_eof(
                read_with(
                    &transport,
                    &context,
                    65_536,
                    1_000,
                    Duration::from_millis(500),
                    CancellationToken::never(),
                )
                .await,
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn blocked_write_times_out() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            proxy.log.timeout.set(Duration::from_secs(1));
            const BUFFER_SIZE: usize = 64 * 3 * 1024 + 1;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let sent = vec![b'a'; BUFFER_SIZE];
            assert_written(
                write(&transport, &context, &sent, Duration::from_millis(100)).await,
                BUFFER_SIZE,
            );
            // Times out waiting to send.
            assert_write_error(
                write(&transport, &context, &sent, Duration::from_millis(100)).await,
                HttpErrorCode::WRITE_TIMEOUT,
                b"Write timed out",
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

// ---------------------------------------------------------------------------
// group-135: oracles for frozen behavior that no frozen test covers
// (expectation-319).

#[test]
fn endpoints_are_the_proxy_hop_including_unix_domain() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            assert_eq!(
                transport.local_endpoint(&context).expect("local endpoint"),
                TransportEndpoint::Socket(session.local_address().clone())
            );
            assert_eq!(
                transport.peer_endpoint(&context).expect("peer endpoint"),
                TransportEndpoint::Socket(socket_address(proxy.address()))
            );
            assert_eq!(
                transport
                    .peer_address(&context)
                    .expect("peer address")
                    .host(),
                b"1.2.3.4"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;

            let local = TransportEndpoint::UnixPath(b"/tmp/roxygen-tunnel".to_vec().into());
            let peer = TransportEndpoint::UnixAbstract(b"roxygen-proxy".to_vec().into());
            let (session, mut socket) = duplex_h1_session(
                &context,
                SessionParams::default(),
                local.clone(),
                peer.clone(),
            );
            let connect = HttpConnectStream::connect_unique(
                &context,
                session.clone(),
                session.reserve_request().expect("reservation"),
                AUTHORITY,
                CONNECT_TIMEOUT,
                headers(&[("Foo", "Bar")]),
                EGRESS_BUFFER_SIZE,
                CancellationToken::never(),
            );
            let (stream, ()) = tokio::join!(connect, async {
                read_h1_head(&mut socket).await;
                socket
                    .write_all(H1_CONNECTED)
                    .await
                    .expect("proxy response");
            });
            let transport = HttpConnectTransport::new(&context, stream.expect("CONNECT"));
            assert_eq!(
                transport.local_endpoint(&context).expect("local endpoint"),
                local
            );
            assert_eq!(
                transport.peer_endpoint(&context).expect("peer endpoint"),
                peer
            );
            let connected = transport.peer_address(&context).expect("peer address");
            assert_eq!(connected.host(), b"1.2.3.4");
            assert_eq!(connected.port(), 443);
            drop(transport);
            drop(session);
            drop(socket);
        })
    });
}

#[test]
fn h1_untimed_read_outlives_connect_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let transport =
                h1_tunnel(&context, &session, &mut socket, Duration::from_millis(100)).await;
            let (late, ()) =
                tokio::join!(read_all(&transport, &context, 4, Duration::ZERO), async {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    socket.write_all(b"late").await.expect("proxy write");
                });
            assert_eq!(late, b"late");
            drop(transport);
            drop(session);
        })
    });
}

#[test]
fn h1_read_pending_at_half_close_times_out_after_stream_read_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let params = || SessionParams {
                stream_read_timeout: Some(Duration::from_millis(200)),
                ..SessionParams::default()
            };
            let proxy = RawH1Proxy::start().await;

            // A read pending at the request EOM times out with a silent peer.
            let (session, mut socket) = proxy.connect(&context, params()).await;
            let transport = h1_tunnel(&context, &session, &mut socket, CONNECT_TIMEOUT).await;
            let mut pending = read(&transport, &context, 16, Duration::ZERO);
            park(&mut pending, "the read").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            let result = tokio::time::timeout(WAIT_DEADLINE, pending)
                .await
                .expect("the pending read ends");
            let cause = read_error_cause(result);
            assert_eq!(cause.code(), HttpErrorCode::READ_TIMEOUT, "{cause:?}");
            assert_eof(read(&transport, &context, 16, Duration::ZERO).await);
            drop(transport);
            drop(session);
            drop(socket);

            // Each body read clears the timeout again, so the next read idles.
            let (session, mut socket) = proxy.connect(&context, params()).await;
            let transport = h1_tunnel(&context, &session, &mut socket, CONNECT_TIMEOUT).await;
            let mut pending = read(&transport, &context, 1, Duration::ZERO);
            park(&mut pending, "the read").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            socket.write_all(b"a").await.expect("proxy write");
            assert_data(pending.await, b"a");
            let (late, ()) = tokio::join!(read(&transport, &context, 1, Duration::ZERO), async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                socket.write_all(b"b").await.expect("proxy write");
            });
            assert_data(late, b"b");
            drop(transport);
            drop(session);
            drop(socket);
        })
    });
}

#[test]
fn read_timer_restarts_after_each_event_that_ends_no_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH2cProxy::start().await;
            let (session, transport, mut socket, stream) = proxy
                .tunnel(&context, SessionParams::default(), &[], 200)
                .await;
            let (read, ()) = tokio::join!(
                read(&transport, &context, 16, Duration::from_millis(200)),
                async {
                    for _ in 0..12 {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        // A Pad Length of 0 and no data: one byte of padding.
                        write_frame(&mut socket, FRAME_DATA, FLAG_PADDED, stream, &[0]).await;
                    }
                    write_frame(&mut socket, FRAME_DATA, 0, stream, b"x").await;
                }
            );
            assert_data(read, b"x");
            drop(transport);
            drop(session);
            drop(socket);
        })
    });
}

#[test]
fn cancelling_the_read_that_started_the_body_read_ends_ingress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            // A cancellation from another thread alone wakes the read.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let cancel = Arc::new(CancellationSource::new());
            let started = Rc::new(Cell::new(false));
            let task = tokio::task::spawn_local({
                let mut reading = read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    Duration::ZERO,
                    cancel.token(),
                );
                let started = Rc::clone(&started);
                poll_fn(move |task| {
                    started.set(true);
                    reading.as_mut().poll(task)
                })
            });
            let polled = Rc::clone(&started);
            eventually("the read to start its body read", move || polled.get()).await;
            cancel_from_another_thread(&cancel);
            let result = tokio::time::timeout(WAIT_DEADLINE, task)
                .await
                .expect("the cancellation alone wakes the read")
                .expect("the read task does not panic");
            assert_cancelled(result);
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_written(
                write(&transport, &context, b"ping", Duration::ZERO).await,
                4,
            );
            assert_eq!(tunnel.read_exact(4).await, b"ping");
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");
            drop(transport);
            drop(session);

            // A buffered event that the cancellation supersedes is discarded.
            let observed = Observed::new();
            let (session, transport, tunnel) =
                tunnel_transport(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            let cancel = CancellationSource::new();
            let mut pending = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut pending, "the read").await;
            tunnel.send(b"echo");
            observed.buffered(4).await;
            cancel.request_cancellation();
            match poll_once(&mut pending).await {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the read"),
            }
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);

            // A sampled cancellation wins over a `close` that released the
            // body read, in either order before the next poll, as frozen's
            // cancellation removes the `bodyEvents_` waiter before the close's
            // error is enqueued; that error goes to the next read.
            for cancel_first in [true, false] {
                let (session, transport, _tunnel) = tunnel_transport(
                    &context,
                    &proxy,
                    SessionParams::default(),
                    EGRESS_BUFFER_SIZE,
                )
                .await;
                let cancel = CancellationSource::new();
                let mut pending = read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    Duration::ZERO,
                    cancel.token(),
                );
                park(&mut pending, "the read").await;
                if cancel_first {
                    cancel.request_cancellation();
                    transport.close(&context).expect("close");
                } else {
                    transport.close(&context).expect("close");
                    cancel.request_cancellation();
                }
                match poll_once(&mut pending).await {
                    Poll::Ready(result) => assert_cancelled(result),
                    Poll::Pending => panic!("the sampled cancellation fails the read"),
                }
                let cause =
                    read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
                drop(session);
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_cancelled_after_a_peer_reset_leaves_the_reset_for_the_next_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let observed = Observed::new();
            let (session, transport) =
                connect(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            let cancel = CancellationSource::new();
            let mut pending = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut pending, "the read").await;
            assert_written(
                write(&transport, &context, b"echo", Duration::ZERO).await,
                4,
            );
            observed.buffered(4).await;
            current_resp_source(&proxy).abort(HttpErrorCode::CANCEL, b"");
            // The reset's sink mode credits the echo back.
            observed.consumed(4).await;
            cancel.request_cancellation();
            match poll_once(&mut pending).await {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the read"),
            }
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
            assert_ne!(cause.message(), b"Read cancelled");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn dropping_a_cancelled_starting_read_ends_ingress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let cancel = CancellationSource::new();
            let mut pending = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut pending, "the read").await;
            cancel.request_cancellation();
            drop(pending);
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_and_write_entry_checks_run_at_first_poll() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            // A token cancelled before the first poll never reaches the baton.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let cancel = CancellationSource::new();
            let cancelled = write_with(
                &transport,
                &context,
                &[b'c'; 8192],
                Duration::ZERO,
                cancel.token(),
            );
            cancel.request_cancellation();
            assert_write_cancelled(cancelled.await);
            assert_written(
                write(&transport, &context, b"fresh", Duration::ZERO).await,
                5,
            );
            assert_eq!(tunnel.read_exact(5).await, b"fresh");
            drop(transport);
            drop(session);

            // A shutdown_write before the first poll finds no write in progress.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let first = vec![b'f'; 8192];
            assert_written(
                write(&transport, &context, &first, Duration::ZERO).await,
                first.len(),
            );
            let late = write(&transport, &context, b"late", Duration::ZERO);
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_write_error(
                late.await,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"write after close",
            );
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), first);
            drop(transport);
            drop(session);

            // A read whose token was cancelled before its first poll starts
            // no body read.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let cancel = CancellationSource::new();
            let cancelled = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                cancel.token(),
            );
            cancel.request_cancellation();
            assert_cancelled(cancelled.await);
            tunnel.send(b"echo");
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"echo"
            );
            drop(transport);
            drop(session);

            // The worker check, which has no frozen counterpart, is the one
            // check that runs at the call. A call from another worker's
            // context fails and changes nothing, and a read or write built
            // there fails with it even when first polled after Drop.
            let wrong = second_worker_context().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let wrong_read = read(&transport, &wrong, 1024, Duration::ZERO);
            let wrong_write = write(&transport, &wrong, b"wrong", Duration::ZERO);
            assert_wrong_worker(transport.shutdown_write(&wrong));
            assert_wrong_worker(transport.close(&wrong));
            assert_wrong_worker(transport.close_with_reset(&wrong));
            assert_wrong_worker(transport.local_address(&wrong));
            assert_wrong_worker(transport.peer_address(&wrong));
            assert_wrong_worker(transport.local_endpoint(&wrong));
            assert_wrong_worker(transport.peer_endpoint(&wrong));
            assert_wrong_worker(transport.peer_identity(&wrong));
            assert_wrong_worker(transport.byte_event_capability(&wrong));
            let callback = Rc::new(CancelledByteEvents::default());
            let registration = HttpByteEventRegistration::new(
                None,
                ByteEventType::TransportWrite.bit(),
                &callback,
            );
            assert_eq!(
                transport.register_byte_event(&wrong, registration),
                Err(ByteEventRegistrationError::WrongWorker)
            );
            assert_eq!(
                *callback.0.borrow(),
                [HttpErrorCode::INVALID_STATE_TRANSITION],
                "the registration is cancelled once, with the worker error"
            );
            assert_written(
                write(&transport, &context, b"right", Duration::ZERO).await,
                5,
            );
            assert_eq!(tunnel.read_exact(5).await, b"right");
            tunnel.send(b"echo");
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"echo"
            );
            drop(transport);
            assert_wrong_worker(wrong_read.await);
            assert_write_error(
                wrong_write.await,
                HttpErrorCode::INVALID_STATE_TRANSITION,
                WRONG_WORKER,
            );
            drop(session);

            // The transport must be built on its stream's worker.
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = HttpConnectStream::connect_unique(
                &context,
                session.clone(),
                session
                    .reserve_request()
                    .expect("proxy session reservation"),
                AUTHORITY,
                CONNECT_TIMEOUT,
                headers(&[("Foo", "Bar")]),
                EGRESS_BUFFER_SIZE,
                CancellationToken::never(),
            );
            park(&mut connect, "the connect").await;
            proxy.tunnel().await.accept();
            let stream = connect.await.expect("CONNECT");
            let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
                HttpConnectTransport::new(&wrong, stream)
            })) else {
                panic!("a transport built on another worker panics");
            };
            let message = panic
                .downcast_ref::<String>()
                .expect("a formatted assertion message");
            assert!(
                message.contains("a CONNECT transport must run on its stream's worker"),
                "{message}"
            );
            drop(session);
            drop(wrong);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_error_is_followed_by_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            current_resp_source(&proxy).abort(HttpErrorCode::CANCEL, b"");
            read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn write_after_egress_error_reports_egress_error() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            // The same RST_STREAM fails the ingress and stops the egress.
            read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            let egress_error = HttpError::with_message(HttpErrorCode::CANCEL, b"Abandoned reading");
            assert_write_error(
                write(&transport, &context, b"late", Duration::ZERO).await,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                &egress_error.describe(),
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancelled_parked_write_poisons_later_writes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            let cancel = CancellationSource::new();
            let mut parked = write_with(
                &transport,
                &context,
                b"parked",
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut parked, "the write").await;
            cancel.request_cancellation();
            assert_write_cancelled(parked.await);
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_write_cancelled(write(&transport, &context, b"later", Duration::ZERO).await);
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(
                tunnel.read_to_end().await.expect("END_STREAM"),
                b"",
                "the poisoned writes queued nothing"
            );
            drop(transport);
            drop(session);

            // A pre-cancelled write and an uncancelled dropped parked write
            // leave the baton unchanged; a cancelled one that is dropped
            // poisons it.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            assert_write_cancelled(
                write_with(
                    &transport,
                    &context,
                    b"pre",
                    Duration::ZERO,
                    cancelled_source().token(),
                )
                .await,
            );
            let stalled = stall(&transport, &context).await;
            let mut dropped = write(&transport, &context, b"dropped", Duration::ZERO);
            park(&mut dropped, "the write").await;
            drop(dropped);
            let mut next = write(&transport, &context, b"next", Duration::ZERO);
            park(&mut next, "the next write").await;
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_written(next.await, 4);
            assert_eq!(tunnel.read_exact(4).await, b"next");
            let stalled = stall(&transport, &context).await;
            let cancel = CancellationSource::new();
            let mut dropped = write_with(
                &transport,
                &context,
                b"dropped",
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut dropped, "the write").await;
            cancel.request_cancellation();
            drop(dropped);
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_write_cancelled(write(&transport, &context, b"later", Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn timed_out_write_poisons_later_writes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            assert_write_error(
                write(&transport, &context, b"late", Duration::from_millis(100)).await,
                HttpErrorCode::WRITE_TIMEOUT,
                b"Write timed out",
            );
            let mut again = write(&transport, &context, b"again", Duration::ZERO);
            match poll_once(&mut again).await {
                Poll::Ready(result) => {
                    assert_write_error(result, HttpErrorCode::WRITE_TIMEOUT, b"Write timed out")
                }
                Poll::Pending => panic!("a poisoned baton fails the write at once"),
            }
            // Every stalled byte reached the proxy, so the window opened.
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_write_error(
                write(&transport, &context, b"after", Duration::ZERO).await,
                HttpErrorCode::WRITE_TIMEOUT,
                b"Write timed out",
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn untimed_write_after_timed_write_keeps_write_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, _tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = vec![b's'; STALL];
            assert_written(
                write(&transport, &context, &stalled, Duration::from_millis(100)).await,
                STALL,
            );
            let untimed = tokio::time::timeout(
                WAIT_DEADLINE,
                write(&transport, &context, b"untimed", Duration::ZERO),
            )
            .await
            .expect("the kept timeout ends the untimed write");
            assert_write_error(untimed, HttpErrorCode::WRITE_TIMEOUT, b"Write timed out");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn parked_write_carries_pending_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let mut expected = stall(&transport, &context).await;
            let mut parked = write(&transport, &context, b"tail", Duration::ZERO);
            park(&mut parked, "the write").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            let (received, written) = tokio::join!(tunnel.read_to_end(), parked);
            assert_written(written, 4);
            expected.extend_from_slice(b"tail");
            assert_eq!(
                received.expect("END_STREAM after the parked write"),
                expected
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;

            // On an HTTP/1.1 proxy leg whose peer stops reading, the window
            // stays closed once the session's egress fills. Only the bytes
            // are asserted: the FIN timing is unverified on H1.
            const CHUNK: usize = 64 * 1024;
            let (session, mut peer) = duplex_h1_session(
                &context,
                SessionParams::default(),
                TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 1)),
                TransportEndpoint::Socket(SocketAddress::new("127.0.0.1", 2)),
            );
            let connect = HttpConnectStream::connect_unique(
                &context,
                session.clone(),
                session.reserve_request().expect("reservation"),
                AUTHORITY,
                CONNECT_TIMEOUT,
                headers(&[("Foo", "Bar")]),
                SMALL_EGRESS_BUFFER,
                CancellationToken::never(),
            );
            let (stream, ()) = tokio::join!(connect, async {
                read_h1_head(&mut peer).await;
                peer.write_all(H1_CONNECTED).await.expect("proxy response");
            });
            let transport = HttpConnectTransport::new(&context, stream.expect("CONNECT"));
            let mut expected = Vec::new();
            let mut parked = loop {
                assert!(
                    expected.len() < 64 * CHUNK,
                    "the egress window stays closed"
                );
                let fill = u8::try_from(expected.len() / CHUNK % 251).expect("byte");
                let chunk = vec![fill; CHUNK];
                expected.extend_from_slice(&chunk);
                let mut writing = write(&transport, &context, &chunk, Duration::ZERO);
                if let Poll::Ready(result) = poll_once(&mut writing).await {
                    assert_written(result, CHUNK);
                    continue;
                }
                match tokio::time::timeout(QUIET, &mut writing).await {
                    Ok(result) => assert_written(result, CHUNK),
                    Err(_) => break writing,
                }
            };
            transport.shutdown_write(&context).expect("shutdown_write");
            let mut received = vec![0; expected.len()];
            let (drained, written) = tokio::time::timeout(WAIT_DEADLINE, async {
                tokio::join!(peer.read_exact(&mut received), &mut parked)
            })
            .await
            .expect("the drain opens the window");
            drained.expect("the peer reads every tunnel byte");
            assert_written(written, CHUNK);
            assert!(received == expected, "every byte arrives in order");
            drop(transport);
            drop(session);
            drop(peer);

            // The resumed write's producer call wakes the egress consumer
            // inline, and that consumer's waker can end the egress through the
            // live handle. The write carries the pending EOM as frozen's one
            // `body(buf, 0, pendingEOM_)` does, so that call finds the egress
            // ended: the egress gets the write's bytes and one EOM, and neither
            // a second EOM nor an abort.
            let proxy = TunnelProxy::start().await;
            for call in [
                SideCall::ShutdownWrite,
                SideCall::Close,
                SideCall::CloseWithReset,
            ] {
                let (source, stops) = CallerSource::new(SourceStep::BodyReadPending, || {});
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    4,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                assert_written(
                    write(&transport, &context, b"abcd", Duration::ZERO).await,
                    4,
                );
                let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
                park(&mut parked, "the write").await;
                transport.shutdown_write(&context).expect("shutdown_write");
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (b"abcd".to_vec(), false)
                );
                let mut consumer = Box::pin(next_egress(&mut egress));
                assert!(
                    poll_with(&mut consumer, &hook_waker()).is_pending(),
                    "{call:?}: the egress consumer parks"
                );
                on_next_wake({
                    let transport = Rc::clone(&transport);
                    let context = context.clone();
                    move || call.make(&transport, &context)
                });
                match poll_once(&mut parked).await {
                    Poll::Ready(result) => assert_written(result, 4),
                    Poll::Pending => panic!("{call:?}: window_open resumed the write"),
                }
                assert!(
                    hook_ran(&ON_WAKE),
                    "{call:?}: the write's producer call woke the egress consumer"
                );
                assert_eq!(
                    consumer.await.expect("the egress has no error"),
                    (b"efgh".to_vec(), true),
                    "{call:?}: the write's bytes with one EOM"
                );
                assert_write_error(
                    write(&transport, &context, b"late", Duration::ZERO).await,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    b"write after close",
                );
                let stopped: &[Option<HttpErrorCode>] = match call {
                    SideCall::ShutdownWrite => &[],
                    SideCall::Close | SideCall::CloseWithReset => &[None],
                };
                assert_eq!(
                    *stops.borrow(),
                    stopped,
                    "{call:?}: a close stops the response source"
                );
                drop(transport);
                assert_eq!(
                    *stops.borrow(),
                    [None],
                    "{call:?}: the response source is stopped once"
                );
            }

            // A write with no pending EOM leaves the EOM unseen in its producer
            // call, as frozen `body(buf, 0, false)` does, so a `shutdown_write`
            // or `close` that the egress consumer's waker makes there sends the
            // EOM, and a `close_with_reset` aborts the egress.
            for call in [
                SideCall::ShutdownWrite,
                SideCall::Close,
                SideCall::CloseWithReset,
            ] {
                let (source, stops) = CallerSource::new(SourceStep::BodyReadPending, || {});
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let mut consumer = Box::pin(next_egress(&mut egress));
                assert!(
                    poll_with(&mut consumer, &hook_waker()).is_pending(),
                    "{call:?}: the egress consumer parks"
                );
                on_next_wake({
                    let transport = Rc::clone(&transport);
                    let context = context.clone();
                    move || call.make(&transport, &context)
                });
                assert_written(
                    write(&transport, &context, b"abcd", Duration::ZERO).await,
                    4,
                );
                assert!(
                    hook_ran(&ON_WAKE),
                    "{call:?}: the write's producer call woke the egress consumer"
                );
                let egressed = consumer.await;
                match call {
                    SideCall::ShutdownWrite | SideCall::Close => assert_eq!(
                        egressed.expect("the egress has no error"),
                        (b"abcd".to_vec(), true),
                        "{call:?}: the write's bytes with the side call's EOM"
                    ),
                    SideCall::CloseWithReset => {
                        let error = egressed.expect_err("the side call aborts the egress");
                        assert_eq!(error.code(), HttpErrorCode::CANCEL, "{error:?}");
                        assert_eq!(error.message(), b"HTTP source aborted err=CANCEL");
                    }
                }
                assert_write_error(
                    write(&transport, &context, b"late", Duration::ZERO).await,
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    b"write after close",
                );
                let stopped: &[Option<HttpErrorCode>] = match call {
                    SideCall::ShutdownWrite => &[],
                    SideCall::Close | SideCall::CloseWithReset => &[None],
                };
                assert_eq!(
                    *stops.borrow(),
                    stopped,
                    "{call:?}: a close stops the response source"
                );
                drop(transport);
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn pending_eom_of_a_timed_out_write_waits_for_shutdown_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            let mut parked = write(&transport, &context, b"tail", Duration::from_millis(100));
            park(&mut parked, "the write").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_write_error(
                parked.await,
                HttpErrorCode::WRITE_TIMEOUT,
                b"Write timed out",
            );
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            tunnel.assert_quiet().await;
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn zero_egress_buffer_first_write_waits_on_the_baton() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) =
                tunnel_transport(&context, &proxy, SessionParams::default(), 0).await;
            assert_write_error(
                write(&transport, &context, b"zero", Duration::from_millis(100)).await,
                HttpErrorCode::WRITE_TIMEOUT,
                b"Write timed out",
            );
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(
                tunnel.read_to_end().await.expect("END_STREAM"),
                b"",
                "the write queued nothing"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn parked_write_at_egress_failure() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            stall(&transport, &context).await;
            let mut parked = write(&transport, &context, b"tail", Duration::ZERO);
            park(&mut parked, "the write").await;
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            let result = tokio::time::timeout(WAIT_DEADLINE, parked)
                .await
                .expect("the egress failure wakes the write");
            assert_write_error(
                result,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"HTTPConnectTransport: egress closed",
            );
            let later = write(&transport, &context, b"later", Duration::ZERO)
                .await
                .expect_err("no later write succeeds");
            assert_eq!(later.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn write_resumed_after_a_direct_eom_fails_the_egress_unless_it_completed() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            // The parked write resumes before the source completed.
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            park(&mut parked, "the write").await;
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            // The baton is signalled, so the EOM goes out directly.
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_written(parked.await, 4);
            assert_eq!(
                next_egress_error(&mut egress).await.code(),
                HttpErrorCode::INVALID_STATE_TRANSITION
            );
            drop(transport);
            drop(egress);

            // The parked write resumes after the source completed.
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            park(&mut parked, "the write").await;
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
            assert_write_error(
                parked.await,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"HTTPConnectTransport: egress closed",
            );
            drop(transport);
            drop(egress);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn close_with_reset_with_parked_read_and_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let write_wakes = Arc::new(WakeCounter::default());
            let mut parked_write = write(&transport, &context, b"efgh", Duration::ZERO);
            assert!(poll_with(&mut parked_write, &write_wakes.waker()).is_pending());
            let read_wakes = Arc::new(WakeCounter::default());
            let mut parked_read = read(&transport, &context, 1024, Duration::ZERO);
            assert!(poll_with(&mut parked_read, &read_wakes.waker()).is_pending());
            // The abort credits the queued bytes, which runs `window_open`
            // inside this call.
            transport
                .close_with_reset(&context)
                .expect("close_with_reset");
            assert!(
                write_wakes.wakes() > 0,
                "window_open wakes the parked write"
            );
            assert!(read_wakes.wakes() > 0, "the close wakes the parked read");
            let cause = read_error_cause(parked_read.await);
            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            // No egress read completed the aborted source, so the resumed
            // write's bytes are rejected and it reports success.
            assert_written(parked_write.await, 4);
            assert_write_error(
                write(&transport, &context, b"later", Duration::ZERO).await,
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"write after close",
            );
            assert_eq!(
                next_egress_error(&mut egress).await.code(),
                HttpErrorCode::CANCEL
            );
            drop(transport);
            drop(egress);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn producer_calls_reenter_window_open_without_panic() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                4,
                headers(&[("Foo", "Bar"), ("Content-Length", "100")]),
                None,
            )
            .await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            // The EOM finds the body short and credits the queued bytes, which
            // runs `window_open` inside `shutdown_write`.
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(
                next_egress_error(&mut egress).await.code(),
                HttpErrorCode::CONTENT_LENGTH_MISMATCH
            );
            drop(transport);
            drop(egress);

            // The source stop of `close` and `close_with_reset` runs the
            // response source's completion callback inside the call.
            for reset in [false, true] {
                let slot = Rc::new(RefCell::new(Weak::new()));
                let peers = Rc::new(RefCell::new(Vec::new()));
                let (source, _producer) = scripted_source(PeerQuery {
                    transport: Rc::clone(&slot),
                    context: context.clone(),
                    peers: Rc::clone(&peers),
                });
                let (transport, _egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                *slot.borrow_mut() = Rc::downgrade(&transport);
                if reset {
                    transport
                        .close_with_reset(&context)
                        .expect("close_with_reset");
                } else {
                    transport.close(&context).expect("close");
                }
                assert_eq!(
                    *peers.borrow(),
                    [b"1.2.3.4".to_vec()],
                    "the completion callback reads the peer address"
                );
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
            }

            // Releasing a cancelled starting read's body read completes an
            // aborted source, whose completion callback drops the last
            // transport handle, whether the read is polled or dropped.
            for polled in [true, false] {
                let slot = Rc::new(RefCell::new(None));
                let (source, producer) = scripted_source(DropTransport(Rc::clone(&slot)));
                let (transport, _egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let cancel = CancellationSource::new();
                let mut pending = read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    Duration::ZERO,
                    cancel.token(),
                );
                park(&mut pending, "the read").await;
                *slot.borrow_mut() = Some(transport);
                assert!(producer.abort(HttpErrorCode::CANCEL, b"peer reset"));
                cancel.request_cancellation();
                if polled {
                    match poll_once(&mut pending).await {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => panic!("the sampled cancellation fails the read"),
                    }
                } else {
                    drop(pending);
                }
                assert!(
                    slot.borrow().is_none(),
                    "the completion callback dropped the transport"
                );
            }

            // A completion callback that closes the transport inside the
            // release of the body read, by a cancelled starting read or by
            // `close`, leaves the error the source committed first for the
            // next read.
            for cancelled in [true, false] {
                let slot = Rc::new(RefCell::new(Weak::new()));
                let (source, producer) = scripted_source(CloseTransport {
                    transport: Rc::clone(&slot),
                    context: context.clone(),
                });
                let (transport, _egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                *slot.borrow_mut() = Rc::downgrade(&transport);
                if cancelled {
                    let cancel = CancellationSource::new();
                    let mut pending = read_with(
                        &transport,
                        &context,
                        1024,
                        1024,
                        Duration::ZERO,
                        cancel.token(),
                    );
                    park(&mut pending, "the read").await;
                    assert!(producer.abort(HttpErrorCode::CANCEL, b"before the release"));
                    cancel.request_cancellation();
                    match poll_once(&mut pending).await {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => panic!("the sampled cancellation fails the read"),
                    }
                } else {
                    assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
                    assert!(producer.abort(HttpErrorCode::CANCEL, b"before the release"));
                    transport.close(&context).expect("close");
                }
                let cause =
                    read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
                assert!(
                    cause.message().ends_with(b"details=before the release"),
                    "{:?}",
                    String::from_utf8_lossy(cause.message())
                );
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
            }

            // A caller-provided response source can close the transport inside
            // the calls by which a read starts a body read, or inside the poll
            // of that body read. After a start, or a poll that stays pending,
            // the close then releases that body read and stops the source, so
            // the read fails as a read parked on a stored read that a close
            // released. A poll that gives an event keeps it, as Proxygen's
            // finished body read enqueues its event after `stopReading`: the
            // read releases the body read, applies the close, and returns the
            // data, EOF for an EOM, or the error, or else finds nothing left
            // to read and returns EOF. An EOM or an error ends the source,
            // which its holder retires with no stop. A `Suspend` event's close
            // as it drops comes after that stop.
            for step in [
                SourceStep::SetReadTimeout,
                SourceStep::ReadBodyEvent,
                SourceStep::IngressCompletion,
                SourceStep::BodyReadReady,
                SourceStep::BodyReadDataReady,
                SourceStep::BodyReadSuspendReady,
                SourceStep::BodyReadEomReady,
                SourceStep::BodyReadErrorReady,
                SourceStep::BodyReadPending,
            ] {
                for reset in [false, true] {
                    let slot: Rc<RefCell<Weak<HttpConnectTransport>>> = Rc::default();
                    let (source, stops, live_at_stops) = CallerSource::counting_live_reads(step, {
                        let slot = Rc::clone(&slot);
                        let context = context.clone();
                        move || {
                            let Some(transport) = slot.borrow().upgrade() else {
                                return;
                            };
                            if reset {
                                transport
                                    .close_with_reset(&context)
                                    .expect("reentrant close_with_reset");
                            } else {
                                transport.close(&context).expect("reentrant close");
                            }
                        }
                    });
                    let (transport, mut egress) = captured_transport(
                        &context,
                        &proxy,
                        EGRESS_BUFFER_SIZE,
                        headers(&[("Foo", "Bar")]),
                        Some(source),
                    )
                    .await;
                    *slot.borrow_mut() = Rc::downgrade(&transport);
                    let mut starting = read(&transport, &context, 1024, Duration::ZERO);
                    let result = match poll_once(&mut starting).await {
                        Poll::Ready(result) => result,
                        Poll::Pending => panic!("{step:?} reset={reset}: the read completes"),
                    };
                    match step {
                        SourceStep::BodyReadDataReady => assert_data(result, BODY_READ_DATA),
                        SourceStep::BodyReadReady
                        | SourceStep::BodyReadSuspendReady
                        | SourceStep::BodyReadEomReady => assert_eof(result),
                        SourceStep::BodyReadErrorReady => assert_cause(
                            &read_error_cause(result),
                            HttpErrorCode::CANCEL,
                            BODY_READ_ERROR,
                        ),
                        _ => assert_cause(
                            &read_error_cause(result),
                            HttpErrorCode::NO_ERROR,
                            b"Abandoned reading",
                        ),
                    }
                    assert_stopped_once(step, &stops, &live_at_stops, (step, reset));
                    assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                    if reset {
                        assert_eq!(
                            next_egress_error(&mut egress).await.code(),
                            HttpErrorCode::CANCEL
                        );
                    } else {
                        assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                    }
                    drop(transport);
                }
            }

            // The clone of its waker that a read saves as it parks on the body
            // read can close the transport too, and the read then fails in the
            // same way.
            for call in [SideCall::Close, SideCall::CloseWithReset] {
                let (source, stops) = CallerSource::new(SourceStep::BodyReadPending, || {});
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let mut starting = read(&transport, &context, 1024, Duration::ZERO);
                on_next_clone({
                    let transport = Rc::clone(&transport);
                    let context = context.clone();
                    move || call.make(&transport, &context)
                });
                let cause = match poll_with(&mut starting, &hook_waker()) {
                    Poll::Ready(result) => read_error_cause(result),
                    Poll::Pending => panic!("{call:?}: the read completes"),
                };
                assert!(hook_ran(&ON_CLONE), "{call:?}: the read cloned its waker");
                assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                assert_eq!(
                    *stops.borrow(),
                    [None],
                    "{call:?}: the close stops the source once"
                );
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                if call == SideCall::CloseWithReset {
                    assert_eq!(
                        next_egress_error(&mut egress).await.code(),
                        HttpErrorCode::CANCEL
                    );
                } else {
                    assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                }
                drop(transport);
            }

            // Caller code that a body read's release runs can call `close` or
            // `close_with_reset`. Here a caller-provided source's body read
            // runs it as it drops: when a cancelled starting read is polled
            // again or dropped, when the test's own close of a parked read
            // releases it (the same call twice), and when a read drops the
            // body read that gave it data. The release keeps the source on
            // loan, so the call takes effect once the release returns, and
            // the source stops once, with no body read live. After a
            // cancellation, that stop is the cancellation's own.
            for call in [SideCall::Close, SideCall::CloseWithReset] {
                for release in [
                    StoredReadRelease::CancelledStartingReadPolled,
                    StoredReadRelease::CancelledStartingReadDropped,
                    StoredReadRelease::ParkedReadClose,
                    StoredReadRelease::ReadyBodyReadDrop,
                ] {
                    let step = if release == StoredReadRelease::ReadyBodyReadDrop {
                        SourceStep::BodyReadDataDrop
                    } else {
                        SourceStep::BodyReadDrop
                    };
                    let slot: Rc<RefCell<Weak<HttpConnectTransport>>> = Rc::default();
                    let (source, stops, live_at_stops) = CallerSource::counting_live_reads(step, {
                        let slot = Rc::clone(&slot);
                        let context = context.clone();
                        move || {
                            let transport = slot
                                .borrow()
                                .upgrade()
                                .expect("the handle outlives the release");
                            call.make(&transport, &context);
                        }
                    });
                    let (transport, mut egress) = captured_transport(
                        &context,
                        &proxy,
                        EGRESS_BUFFER_SIZE,
                        headers(&[("Foo", "Bar")]),
                        Some(source),
                    )
                    .await;
                    *slot.borrow_mut() = Rc::downgrade(&transport);
                    let cancel = CancellationSource::new();
                    let mut first = read_with(
                        &transport,
                        &context,
                        1024,
                        1024,
                        Duration::ZERO,
                        cancel.token(),
                    );
                    let stop = match release {
                        StoredReadRelease::ReadyBodyReadDrop => {
                            assert_data(first.await, BODY_READ_DATA);
                            None
                        }
                        StoredReadRelease::ParkedReadClose => {
                            park(&mut first, "the read").await;
                            call.make(&transport, &context);
                            let cause = read_error_cause(first.await);
                            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                            None
                        }
                        _ => {
                            park(&mut first, "the starting read").await;
                            cancel.request_cancellation();
                            if release == StoredReadRelease::CancelledStartingReadPolled {
                                match poll_once(&mut first).await {
                                    Poll::Ready(result) => assert_cancelled(result),
                                    Poll::Pending => {
                                        panic!("{call:?} {release:?}: the read completes")
                                    }
                                }
                            } else {
                                drop(first);
                            }
                            Some(HttpErrorCode::CORO_CANCELLED)
                        }
                    };
                    assert_eq!(
                        *stops.borrow(),
                        [stop],
                        "{call:?} {release:?}: the source stops once"
                    );
                    assert_eq!(
                        *live_at_stops.borrow(),
                        [0],
                        "{call:?} {release:?}: the stop comes after the release"
                    );
                    if stop.is_some() {
                        let cause = read_error_cause(
                            read(&transport, &context, 1024, Duration::ZERO).await,
                        );
                        assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
                    }
                    assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                    if call == SideCall::CloseWithReset {
                        assert_eq!(
                            next_egress_error(&mut egress).await.code(),
                            HttpErrorCode::CANCEL
                        );
                    } else {
                        assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                    }
                    drop(transport);
                }
            }

            // The session's own source releases the waker its body read saved
            // while it borrows its event baton's state. The same calls made in
            // that release give the same results, with no panic.
            for call in [SideCall::Close, SideCall::CloseWithReset] {
                for release in [
                    StoredReadRelease::CancelledStartingReadPolled,
                    StoredReadRelease::CancelledStartingReadDropped,
                    StoredReadRelease::ParkedReadClose,
                ] {
                    let (transport, mut egress) = captured_transport(
                        &context,
                        &proxy,
                        EGRESS_BUFFER_SIZE,
                        headers(&[("Foo", "Bar")]),
                        None,
                    )
                    .await;
                    let cancel = CancellationSource::new();
                    let mut first = read_with(
                        &transport,
                        &context,
                        1024,
                        1024,
                        Duration::ZERO,
                        cancel.token(),
                    );
                    assert!(
                        poll_with(&mut first, &hook_waker()).is_pending(),
                        "{call:?} {release:?}: the read parks"
                    );
                    let side_call = {
                        let transport = Rc::downgrade(&transport);
                        let context = context.clone();
                        move || {
                            let transport = transport
                                .upgrade()
                                .expect("the handle outlives the release");
                            call.make(&transport, &context);
                        }
                    };
                    let cancelled = match release {
                        StoredReadRelease::ParkedReadClose => {
                            on_next_drop(side_call);
                            call.make(&transport, &context);
                            let cause = read_error_cause(first.await);
                            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                            false
                        }
                        StoredReadRelease::CancelledStartingReadPolled => {
                            cancel.request_cancellation();
                            // The poll's cancellation registration releases
                            // its clone of the waker first.
                            on_next_drop(|| on_next_drop(side_call));
                            match poll_with(&mut first, &hook_waker()) {
                                Poll::Ready(result) => assert_cancelled(result),
                                Poll::Pending => {
                                    panic!("{call:?} {release:?}: the read completes")
                                }
                            }
                            true
                        }
                        _ => {
                            cancel.request_cancellation();
                            on_next_drop(side_call);
                            drop(first);
                            true
                        }
                    };
                    assert!(
                        hook_ran(&ON_DROP),
                        "{call:?} {release:?}: the source's release of the body read's waker \
                         made the call"
                    );
                    if cancelled {
                        let cause = read_error_cause(
                            read(&transport, &context, 1024, Duration::ZERO).await,
                        );
                        assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
                    }
                    assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                    if call == SideCall::CloseWithReset {
                        assert_eq!(
                            next_egress_error(&mut egress).await.code(),
                            HttpErrorCode::CANCEL
                        );
                    } else {
                        assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                    }
                    drop(transport);
                }
            }
            proxy.shutdown().await;

            // A caller-provided host's timer can close the transport inside
            // the poll of a read's timer, while the read holds the stored read.
            // The read then fails in the same way.
            for reset in [false, true] {
                on_second_worker(Host::new(Rc::new(HookedTimerHost)), move |context| {
                    Box::pin(async move {
                        let proxy = TunnelProxy::start().await;
                        let (source, stops) = CallerSource::new(SourceStep::BodyReadPending, || {});
                        let (transport, mut egress) = captured_transport(
                            &context,
                            &proxy,
                            EGRESS_BUFFER_SIZE,
                            headers(&[("Foo", "Bar")]),
                            Some(source),
                        )
                        .await;
                        let mut pending =
                            read(&transport, &context, 1024, Duration::from_secs(3600));
                        on_next_timer_poll({
                            let transport = Rc::downgrade(&transport);
                            let context = context.clone();
                            move || {
                                let transport = transport.upgrade().expect("the handle");
                                if reset {
                                    transport
                                        .close_with_reset(&context)
                                        .expect("close_with_reset in the timer");
                                } else {
                                    transport.close(&context).expect("close in the timer");
                                }
                            }
                        });
                        let cause = match poll_once(&mut pending).await {
                            Poll::Ready(result) => read_error_cause(result),
                            Poll::Pending => panic!("reset={reset}: the read completes"),
                        };
                        assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                        assert_eq!(
                            *stops.borrow(),
                            [None],
                            "reset={reset}: the close stops the source once"
                        );
                        assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                        if reset {
                            assert_eq!(
                                next_egress_error(&mut egress).await.code(),
                                HttpErrorCode::CANCEL
                            );
                        } else {
                            assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                        }
                        drop(transport);
                        proxy.shutdown().await;
                    })
                })
                .await;
            }
        })
    });
}

const CYCLE_WRITES: usize = 64;
const CYCLE_CHUNK: usize = 16 * 1024;

/// Makes `CYCLE_WRITES` writes of `CYCLE_CHUNK` bytes, each filled with its
/// index, and returns how many waited for `window_open`.
async fn cycle_writes(
    transport: Rc<HttpConnectTransport>,
    context: WorkerContext,
    timeout: Duration,
) -> usize {
    let mut parked = 0;
    for index in 0..CYCLE_WRITES {
        let chunk = vec![u8::try_from(index).expect("small index"); CYCLE_CHUNK];
        let mut writing = write(&transport, &context, &chunk, timeout);
        let written = match poll_once(&mut writing).await {
            Poll::Ready(result) => result,
            Poll::Pending => {
                assert_ne!(index, 0, "the first write finds the baton signalled");
                parked += 1;
                writing.await
            }
        };
        assert_written(written, CYCLE_CHUNK);
    }
    parked
}

fn cycle_bytes() -> Vec<u8> {
    (0..CYCLE_WRITES)
        .flat_map(|index| vec![u8::try_from(index).expect("small index"); CYCLE_CHUNK])
        .collect()
}

#[test]
fn repeated_window_cycles_complete_every_write_with_a_concurrent_reader() {
    run_on_worker(|context| {
        Box::pin(async move {
            const WRITE_TIMEOUT: Duration = Duration::from_millis(200);
            const READ_PACE: Duration = Duration::from_millis(10);
            let proxy = H2cProxy::start().await;
            let (session, transport) = connect(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let writer = tokio::task::spawn_local(cycle_writes(
                Rc::clone(&transport),
                context.clone(),
                Duration::ZERO,
            ));
            let reader = tokio::task::spawn_local({
                let transport = Rc::clone(&transport);
                let context = context.clone();
                async move {
                    read_all(
                        &transport,
                        &context,
                        CYCLE_WRITES * CYCLE_CHUNK,
                        Duration::ZERO,
                    )
                    .await
                }
            });
            let (parked_writes, received) = tokio::join!(writer, reader);
            let parked_writes = parked_writes.expect("the writer does not panic");
            let received = received.expect("the reader does not panic");
            assert!(received == cycle_bytes(), "every byte arrives in order");
            // Each 16 KiB write closes the 4_096-byte egress window, and the
            // next write's first poll runs before any session task can open it.
            assert_eq!(
                parked_writes,
                CYCLE_WRITES - 1,
                "every write after the first waits for window_open"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;

            // Every write has a timeout, as an inner session's writes do
            // (`UniplexConfig::write_timeout`), and the reader drains the
            // egress source itself, one write's bytes per read after a pause.
            // Each write waits for that reader far less than the timeout, and
            // the run, at least 64 pauses, lasts more than two timeouts.
            let proxy = TunnelProxy::start().await;
            let started = Instant::now();
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                SMALL_EGRESS_BUFFER,
                headers(&[("Foo", "Bar")]),
                None,
            )
            .await;
            let writer = tokio::task::spawn_local(cycle_writes(
                Rc::clone(&transport),
                context.clone(),
                WRITE_TIMEOUT,
            ));
            let reader = tokio::task::spawn_local(async move {
                let mut received = Vec::with_capacity(CYCLE_WRITES * CYCLE_CHUNK);
                while received.len() < CYCLE_WRITES * CYCLE_CHUNK {
                    tokio::time::sleep(READ_PACE).await;
                    let (bytes, eom) = next_egress_bytes(&mut egress).await;
                    assert!(!eom, "EOM after {} bytes", received.len());
                    received.extend(bytes);
                }
                (received, egress)
            });
            let (parked_writes, (received, egress)) = tokio::try_join!(writer, reader)
                .expect("neither the timed writer nor the egress reader panics");
            assert!(
                received == cycle_bytes(),
                "every timed write's bytes reach the egress in order"
            );
            assert_eq!(
                parked_writes,
                CYCLE_WRITES - 1,
                "every timed write after the first waits for window_open"
            );
            assert!(
                started.elapsed() > 2 * WRITE_TIMEOUT,
                "the timed writes outlive two write timeouts"
            );
            drop(transport);
            drop(egress);

            // A window that opens while no write is in progress leaves the
            // baton signalled until a write resets it, so the next write
            // completes at its first poll (TimedBaton.h:29, 43-56).
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            assert!(
                poll_once(&mut parked).await.is_pending(),
                "the first write closed the window"
            );
            drop(parked);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            let mut next = write(&transport, &context, b"ijkl", Duration::ZERO);
            match poll_once(&mut next).await {
                Poll::Ready(result) => assert_written(result, 4),
                Poll::Pending => panic!("the window opened with no write in progress"),
            }
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"ijkl".to_vec(), false)
            );
            drop(transport);
            drop(egress);

            // An egress consumer can poll inside its own waker, which a
            // write's `egress_body` wakes. A read of every byte there runs
            // `window_open` inside that call, whose flow result already counts
            // the read, so the next write completes at its first poll. A read
            // that leaves the window closed lets the reset stand, so the next
            // write waits for a later read.
            for (bytes, maximum) in [(&b"abcd"[..], u32::MAX), (&b"abcdefgh"[..], 2)] {
                let opens = maximum == u32::MAX;
                let (transport, mut egress) =
                    captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
                let first = poll_with(&mut egress.read_body_event(maximum), Waker::noop());
                let suspended = match &first {
                    Poll::Ready(Ok(event)) => {
                        matches!(event.kind(), HttpBodyEventKind::Suspend(_))
                    }
                    _ => false,
                };
                assert!(
                    suspended,
                    "opens={opens}: the idle egress source suspends its first read"
                );
                drop(first);
                let mut consumer = egress.read_body_event(maximum);
                assert!(
                    poll_with(&mut consumer, &hook_waker()).is_pending(),
                    "opens={opens}: the egress consumer waits"
                );
                let inline = Rc::new(RefCell::new(None));
                on_next_wake({
                    let inline = Rc::clone(&inline);
                    move || {
                        let read = match poll_with(&mut consumer, Waker::noop()) {
                            Poll::Ready(Ok(event)) => match (event.eom(), event.into_kind()) {
                                (false, HttpBodyEventKind::Body(body)) => Some(bytes_of(&body)),
                                _ => None,
                            },
                            _ => None,
                        };
                        *inline.borrow_mut() = Some(read);
                    }
                });
                assert_written(
                    write(&transport, &context, bytes, Duration::ZERO).await,
                    bytes.len(),
                );
                let taken = &bytes[..bytes.len().min(maximum as usize)];
                assert_eq!(
                    inline.borrow_mut().take(),
                    Some(Some(taken.to_vec())),
                    "opens={opens}: the consumer reads inside the write's `egress_body`"
                );
                let mut next = write(&transport, &context, b"ijkl", Duration::ZERO);
                match (opens, poll_once(&mut next).await) {
                    (true, Poll::Ready(result)) => assert_written(result, 4),
                    (false, Poll::Pending) => {
                        assert_eq!(
                            next_egress_bytes(&mut egress).await,
                            (bytes[taken.len()..].to_vec(), false)
                        );
                        assert_written(next.await, 4);
                    }
                    (_, first_poll) => panic!(
                        "opens={opens}: the next write's first poll gives {:?}",
                        first_poll.map(|result| result.map(|info| info.bytes_written))
                    ),
                }
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (b"ijkl".to_vec(), false)
                );
                drop(transport);
                drop(egress);
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn inner_h1_session_over_the_tunnel_reads_allocation_sized_pieces() {
    run_on_worker(|context| {
        Box::pin(async move {
            const BODY: usize = 16 * 1024;
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let returned = Rc::new(RefCell::new(Vec::new()));
            let recording = Rc::new(RecordingTransport {
                inner: Rc::clone(&transport),
                requested: RefCell::default(),
                returned: Rc::clone(&returned),
            });
            let config = UniplexConfig::default();
            let (maximum, allocation_size) =
                (config.maximum_read_size, config.read_allocation_size);
            let (inner, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                recording.clone(),
                None,
                config,
            )
            .expect("inner session");
            let run = tokio::task::spawn_local(inner.run());
            let mut request = HttpFixedSource::make_get_request("/", None).expect("GET");
            request
                .message_mut()
                .expect("request head")
                .headers_mut()
                .set(HttpHeaderCode::Host, b"example.com");
            let exchange = tokio::task::spawn_local(handle.send_request(
                HttpSourceHolder::new(Box::new(request)),
                CancellationToken::never(),
            ));

            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let (bytes, eom) = tunnel.next().await.expect("the inner request");
                assert!(!eom, "the inner session keeps the tunnel open");
                head.extend_from_slice(&bytes);
            }
            assert!(
                head.starts_with(b"GET / HTTP/1.1\r\n"),
                "{:?}",
                String::from_utf8_lossy(&head)
            );
            // One tunnel event carries the whole response.
            let body: Vec<u8> = (0..BODY)
                .map(|index| u8::try_from(index % 251).expect("below 251"))
                .collect();
            let mut response =
                format!("HTTP/1.1 200 OK\r\nContent-Length: {BODY}\r\n\r\n").into_bytes();
            response.extend_from_slice(&body);
            tunnel.send(&response);

            let mut exchange = exchange
                .await
                .expect("the exchange task does not panic")
                .expect("the inner response");
            let mut status = exchange
                .source
                .read_header_event()
                .await
                .expect("the response head")
                .into_headers();
            assert_eq!(status.status_code().expect("a response status"), 200);
            let mut received = Vec::new();
            loop {
                let event = exchange
                    .source
                    .read_body_event(u32::MAX)
                    .await
                    .expect("the response body");
                let eom = event.eom();
                if let HttpBodyEventKind::Body(data) = event.into_kind() {
                    received.extend_from_slice(&bytes_of(&data));
                }
                if eom {
                    break;
                }
            }
            assert!(received == body, "the inner session receives every byte");

            // The session reads with its own sizes; the transport returns at
            // most the allocation size per read, and never an empty result
            // without EOF.
            assert!(
                recording
                    .requested
                    .borrow()
                    .iter()
                    .all(|requested| *requested == (maximum, allocation_size)),
                "{:?}",
                recording.requested.borrow()
            );
            let delivered: usize = returned.borrow().iter().map(|(bytes, _)| bytes).sum();
            assert_eq!(delivered, response.len());
            assert!(
                returned
                    .borrow()
                    .iter()
                    .all(|(bytes, eof)| (1..=allocation_size).contains(bytes) && !eof),
                "{:?}",
                returned.borrow()
            );
            assert!(
                returned.borrow().len() >= response.len().div_ceil(allocation_size),
                "{:?}",
                returned.borrow()
            );

            // Closing the idle inner session releases its stored body read and
            // ends the tunnel's egress with no reset.
            handle.close_when_idle().expect("close when idle");
            tokio::time::timeout(WAIT_DEADLINE, run)
                .await
                .expect("the inner session closes")
                .expect("the inner session task does not panic")
                .expect("the inner session closes cleanly");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");
            drop(recording);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

// ---------------------------------------------------------------------------
// group-135: oracles for the `DIV-012` differences (expectation-320).

#[test]
fn smaller_reads_after_timeout_keep_the_remainder_before_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            assert_read_timeout(read(&transport, &context, 64 * 1024, READ_TIMEOUT).await);
            let echo: Vec<u8> = (0..8192_u32)
                .map(|index| u8::try_from(index % 251).expect("byte"))
                .collect();
            tunnel.finish(&echo);
            let mut received = Vec::new();
            loop {
                let read = read(&transport, &context, 1024, Duration::ZERO)
                    .await
                    .expect("tunnel read");
                if read.is_eof() {
                    break;
                }
                assert!((1..=1024).contains(&read.bytes_read()));
                received.extend_from_slice(&bytes_of(read.data()));
            }
            assert_eq!(received, echo);
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);

            // `close` and `close_with_reset` keep the remainder of an event
            // without EOM, and EOF follows its last byte.
            for reset in [false, true] {
                let observed = Observed::new();
                let (session, transport, tunnel) =
                    tunnel_transport(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
                assert_read_timeout(read(&transport, &context, 64 * 1024, READ_TIMEOUT).await);
                tunnel.send(&echo);
                observed.buffered(8192).await;
                assert_data(
                    read(&transport, &context, 1024, Duration::ZERO).await,
                    &echo[..1024],
                );
                if reset {
                    transport
                        .close_with_reset(&context)
                        .expect("close_with_reset");
                } else {
                    transport.close(&context).expect("close");
                }
                let mut received = echo[..1024].to_vec();
                loop {
                    let read = read(&transport, &context, 1024, Duration::ZERO)
                        .await
                        .expect("a read of the kept remainder");
                    if read.is_eof() {
                        break;
                    }
                    received.extend_from_slice(&bytes_of(read.data()));
                }
                assert_eq!(received, echo);
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
                drop(session);
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancelling_a_later_or_returned_read_keeps_the_body_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let returned = CancellationSource::new();
            assert_read_timeout(
                read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    READ_TIMEOUT,
                    returned.token(),
                )
                .await,
            );
            let later = CancellationSource::new();
            let mut waiting = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                later.token(),
            );
            park(&mut waiting, "the later read").await;
            later.request_cancellation();
            match poll_once(&mut waiting).await {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the later read's cancellation fails it"),
            }
            returned.request_cancellation();
            tunnel.send(b"data");
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"data"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_maximum_above_u32_is_clamped() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            tunnel.send(b"data");
            let huge = 1_usize << 32;
            assert_data(
                read_with(
                    &transport,
                    &context,
                    huge,
                    huge,
                    Duration::ZERO,
                    CancellationToken::never(),
                )
                .await,
                b"data",
            );
            let error = read_with(
                &transport,
                &context,
                1024,
                0,
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("a zero allocation size fails");
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            tunnel.send(b"more");
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"more",
                "the rejected read left the tunnel readable"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn concurrent_read_or_write_fails_invalid_state_transition() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            // Of two reads built in turn, the one polled first is pending.
            let built_first = read(&transport, &context, 16, Duration::ZERO);
            let mut polled_first = read(&transport, &context, 16, Duration::ZERO);
            park(&mut polled_first, "the read polled first").await;
            let error = built_first.await.expect_err("a second pending read fails");
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            // Neither that rejected read nor an entry-cancelled, zero-maximum,
            // or zero-allocation-size read ends the pending read's marking.
            assert_cancelled(
                read_with(
                    &transport,
                    &context,
                    16,
                    16,
                    Duration::ZERO,
                    cancelled_source().token(),
                )
                .await,
            );
            let empty = read_with(
                &transport,
                &context,
                0,
                16,
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect("a zero maximum returns at once");
            assert!(!empty.is_eof());
            assert_eq!(empty.bytes_read(), 0);
            let error = read_with(
                &transport,
                &context,
                16,
                0,
                Duration::ZERO,
                CancellationToken::never(),
            )
            .await
            .expect_err("a zero allocation size fails");
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            let mut still_rejected = read(&transport, &context, 16, Duration::ZERO);
            match poll_once(&mut still_rejected).await {
                Poll::Ready(result) => assert_eq!(
                    result.expect_err("a read is still pending").code(),
                    HttpErrorCode::INVALID_STATE_TRANSITION
                ),
                Poll::Pending => panic!("the read polled first is still pending"),
            }
            tunnel.send(b"data");
            assert_data(polled_first.await, b"data");

            let stalled = stall(&transport, &context).await;
            let mut parked = write(&transport, &context, b"parked", Duration::ZERO);
            park(&mut parked, "the write").await;
            let rejected = write(&transport, &context, b"rejected", Duration::ZERO)
                .await
                .expect_err("a second write in progress fails");
            assert_eq!(rejected.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            assert_eq!(rejected.bytes_written(), 0);
            // Neither the rejected write nor an entry-cancelled one ends the
            // parked write's marking, so `shutdown_write` sets the pending
            // EOM, which the parked write carries.
            assert_write_cancelled(
                write_with(
                    &transport,
                    &context,
                    b"cancelled",
                    Duration::ZERO,
                    cancelled_source().token(),
                )
                .await,
            );
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_written(parked.await, 6);
            assert_eq!(
                tunnel.read_to_end().await.expect("END_STREAM"),
                b"parked",
                "the parked write carries the pending EOM"
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn empty_write_returns_zero_without_a_body_event() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(write(&transport, &context, b"", Duration::ZERO).await, 0);
            assert_written(write(&transport, &context, b"ab", Duration::ZERO).await, 2);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"ab".to_vec(), false),
                "no empty body event precedes the write"
            );

            // An empty write while a write is parked leaves it in progress.
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            park(&mut parked, "the write").await;
            let mut empty = write(&transport, &context, b"", Duration::ZERO);
            match poll_once(&mut empty).await {
                Poll::Ready(result) => assert_written(result, 0),
                Poll::Pending => panic!("an empty write returns at once"),
            }
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            assert_written(parked.await, 4);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"efgh".to_vec(), true),
                "the parked write carries the pending EOM"
            );
            drop(transport);
            drop(egress);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_returns_a_ready_event_over_an_unobserved_expiry() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            let observed = Observed::new();
            let (session, transport, tunnel) =
                tunnel_transport(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            let wakes = Arc::new(WakeCounter::default());
            let mut timed = read(&transport, &context, 16, READ_TIMEOUT);
            assert!(poll_with(&mut timed, &wakes.waker()).is_pending());
            let expired = Arc::clone(&wakes);
            eventually("the read's deadline", move || expired.wakes() > 0).await;
            tunnel.send(b"echo");
            observed.buffered(4).await;
            match poll_with(&mut timed, &wakes.waker()) {
                Poll::Ready(result) => assert_data(result, b"echo"),
                Poll::Pending => panic!("the ready event completes the read"),
            }
            drop(transport);
            drop(session);

            // A sampled cancellation wins over the unobserved expiry.
            let (session, transport, _tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let cancel = CancellationSource::new();
            let wakes = Arc::new(WakeCounter::default());
            let mut timed = read_with(&transport, &context, 16, 16, READ_TIMEOUT, cancel.token());
            assert!(poll_with(&mut timed, &wakes.waker()).is_pending());
            let expired = Arc::clone(&wakes);
            eventually("the read's deadline", move || expired.wakes() > 0).await;
            cancel.request_cancellation();
            match poll_with(&mut timed, &wakes.waker()) {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the read"),
            }
            let cause = read_error_cause(read(&transport, &context, 16, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn event_buffered_after_a_timeout_is_lost_to_a_reset_before_the_next_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let observed = Observed::new();
            let (session, transport) =
                connect(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            assert_written(
                write(&transport, &context, &[b'e'; 1024], Duration::ZERO).await,
                1024,
            );
            observed.buffered(1024).await;
            current_resp_source(&proxy).abort(HttpErrorCode::CANCEL, b"");
            observed.consumed(1024).await;
            assert_eq!(
                observed.closed(),
                0,
                "the idle stored read holds the stream"
            );
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
            observed.transaction_closed().await;
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn idle_stored_read_defers_source_completion_after_a_reset_or_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;

            // A reset while the stored read awaits its first event.
            let observed = Observed::new();
            let (session, transport) =
                connect(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            current_resp_source(&proxy).abort(HttpErrorCode::CANCEL, b"");
            let events = Rc::clone(&observed.events);
            eventually("the client's peer reset", move || {
                !events.peer_resets.borrow().is_empty()
            })
            .await;
            tokio::time::sleep(QUIET).await;
            assert_eq!(
                observed.closed(),
                0,
                "the idle stored read holds the stream"
            );
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
            observed.transaction_closed().await;
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            drop(transport);
            drop(session);

            // A peer EOM after the egress ended.
            let observed = Observed::new();
            let (session, transport) =
                connect(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            transport.shutdown_write(&context).expect("shutdown_write");
            let log = Rc::clone(&proxy.log);
            eventually("the proxy's request EOM", move || {
                *log.request_eoms.borrow() > 0
            })
            .await;
            tokio::time::sleep(QUIET).await;
            assert_eq!(
                observed.closed(),
                0,
                "the idle stored read holds the stream"
            );
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            observed.transaction_closed().await;
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn idle_stored_read_returns_no_ingress_credit_until_the_next_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            const WINDOW: usize = 16 * 1024;
            let proxy = H2cProxy::start().await;
            let observed = Observed::new();
            let mut initial_window = SessionParams::http_client_defaults(None).settings[0];
            initial_window.value = WINDOW as u64;
            let params = SessionParams {
                settings: vec![initial_window],
                ..observed.params()
            };
            let (session, transport) = connect(&context, &proxy, params, EGRESS_BUFFER_SIZE).await;
            assert_read_timeout(read(&transport, &context, 64 * 1024, READ_TIMEOUT).await);
            let sent: Vec<u8> = (0..4 * WINDOW)
                .map(|index| u8::try_from(index % 253).expect("byte"))
                .collect();
            assert_written(
                write(&transport, &context, &sent, Duration::ZERO).await,
                sent.len(),
            );
            observed.buffered(1).await;
            tokio::time::sleep(QUIET).await;
            let buffered = observed.stats.buffered.get();
            assert!(
                buffered <= WINDOW as i64,
                "at most one receive window is buffered: {buffered}"
            );
            assert_eq!(observed.stats.consumed.get(), 0, "no credit is returned");
            tokio::time::sleep(QUIET).await;
            assert_eq!(
                observed.stats.buffered.get(),
                buffered,
                "no further echo arrives"
            );
            assert_eq!(
                read_all(&transport, &context, sent.len(), Duration::ZERO).await,
                sent
            );
            let stats = Rc::clone(&observed.stats);
            let total = sent.len() as i64;
            eventually("every echoed byte consumed", move || {
                stats.consumed.get() == total
            })
            .await;
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn read_after_close_with_an_idle_stored_read_fails_then_reads_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            // close and close_with_reset on silent tunnels.
            for reset in [false, true] {
                let (session, transport, _tunnel) = tunnel_transport(
                    &context,
                    &proxy,
                    SessionParams::default(),
                    EGRESS_BUFFER_SIZE,
                )
                .await;
                assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
                if reset {
                    transport
                        .close_with_reset(&context)
                        .expect("close_with_reset");
                } else {
                    transport.close(&context).expect("close");
                }
                let cause =
                    read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
                drop(session);
            }

            proxy.shutdown().await;

            // An event that no read took is discarded, with or without EOM.
            let proxy = TunnelProxy::start().await;
            for eom in [false, true] {
                let observed = Observed::new();
                let (session, transport, tunnel) =
                    tunnel_transport(&context, &proxy, observed.params(), EGRESS_BUFFER_SIZE).await;
                assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
                if eom {
                    tunnel.finish(&[b'e'; 1024]);
                } else {
                    tunnel.send(&[b'e'; 1024]);
                }
                observed.buffered(1024).await;
                transport.close(&context).expect("close");
                let cause =
                    read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                if eom {
                    assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
                    let events = Rc::clone(&observed.events);
                    eventually("the upgraded stream to detach", move || {
                        events.detached.get() > 0
                    })
                    .await;
                    assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");
                }
                drop(transport);
                drop(session);
            }

            // A source error committed before the response EOM and the close
            // goes to the next read in place of 'Abandoned reading'.
            for reset in [false, true] {
                let (source, scripted) = ScriptedResponse::new();
                let (transport, _egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
                assert!(
                    scripted
                        .producer
                        .abort(HttpErrorCode::CANCEL, b"before the close")
                );
                if reset {
                    transport
                        .close_with_reset(&context)
                        .expect("close_with_reset");
                } else {
                    transport.close(&context).expect("close");
                }
                let cause =
                    read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
                assert!(
                    cause.message().ends_with(b"details=before the close"),
                    "{:?}",
                    String::from_utf8_lossy(cause.message())
                );
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
            }

            // A close's stop, or its release of the stored read when the
            // source was aborted first, can run the source's completion
            // callback, which can poll the read parked on that stored read.
            // That read gets the close's error before any EOF.
            for (call, aborted) in [
                (SideCall::Close, false),
                (SideCall::CloseWithReset, false),
                (SideCall::Close, true),
            ] {
                let parked = Rc::new(RefCell::new(None));
                let polled = Rc::new(RefCell::new(None));
                let callback_wakes = Arc::new(WakeCounter::default());
                let (source, producer) = scripted_source(PollReadOnComplete {
                    read: Rc::clone(&parked),
                    waker: callback_wakes.waker(),
                    result: Rc::clone(&polled),
                });
                let (transport, _egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let mut pending = read(&transport, &context, 1024, Duration::ZERO);
                park(&mut pending, "the read").await;
                *parked.borrow_mut() = Some(pending);
                if aborted {
                    assert!(
                        producer.abort(HttpErrorCode::CANCEL, b"before the close"),
                        "the producer aborts"
                    );
                }
                call.make(&transport, &context);
                let result = polled.borrow_mut().take();
                if aborted {
                    assert!(
                        matches!(result, Some(Poll::Pending)),
                        "the release ran the callback, whose poll parks, got {result:?}"
                    );
                    assert_eq!(
                        callback_wakes.wakes(),
                        1,
                        "the close wakes the read once it has published the error"
                    );
                    let mut pending = parked.borrow_mut().take().expect("the parked read");
                    let cause = match poll_with(&mut pending, Waker::noop()) {
                        Poll::Ready(result) => read_error_cause(result),
                        Poll::Pending => panic!("the woken read completes"),
                    };
                    assert_eq!(cause.code(), HttpErrorCode::CANCEL, "{cause:?}");
                    assert!(
                        cause.message().ends_with(b"details=before the close"),
                        "{:?}",
                        String::from_utf8_lossy(cause.message())
                    );
                } else {
                    let cause = match result {
                        Some(Poll::Ready(result)) => read_error_cause(result),
                        other => panic!("{call:?}: the stop ran the callback, got {other:?}"),
                    };
                    assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
                    assert!(
                        parked.borrow().is_none(),
                        "{call:?}: the callback's poll completed the read"
                    );
                }
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
                drop(transport);
            }
            proxy.shutdown().await;

            // A 201 tunnel is not upgraded: with its egress incomplete, the
            // stop's NO_ERROR completion resets the stream.
            let raw = RawH2cProxy::start().await;
            let observed = Observed::new();
            let (session, transport, mut socket, stream) = raw
                .tunnel(
                    &context,
                    observed.params(),
                    &[(SETTINGS_INITIAL_WINDOW_SIZE, 0)],
                    201,
                )
                .await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            write_frame(
                &mut socket,
                FRAME_DATA,
                FLAG_END_STREAM,
                stream,
                &[b'e'; 1024],
            )
            .await;
            observed.buffered(1024).await;
            assert_written(
                write(&transport, &context, &[b'w'; 1024], Duration::ZERO).await,
                1024,
            );
            transport.close(&context).expect("close");
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            let (data, code) = read_until_reset(&mut socket, stream).await;
            assert_eq!(code, 0x8, "RST_STREAM CANCEL");
            assert!(data.is_empty(), "no DATA crossed the zero window");
            drop(transport);
            drop(session);
            drop(socket);
        })
    });
}

#[test]
fn error_installed_after_eom_is_hidden_from_a_cancelled_or_closed_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            // A cancelled starting read.
            let (source, scripted) = ScriptedResponse::new();
            let (transport, _egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            let cancel = CancellationSource::new();
            let mut pending = read_with(
                &transport,
                &context,
                1024,
                1024,
                Duration::ZERO,
                cancel.token(),
            );
            park(&mut pending, "the read").await;
            scripted.error_after_eom();
            cancel.request_cancellation();
            match poll_once(&mut pending).await {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the read"),
            }
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eq!(
                scripted.completion_codes(),
                [Some(HttpErrorCode::INVALID_STATE_TRANSITION)]
            );
            drop(transport);

            // A close after a read timeout.
            let (source, scripted) = ScriptedResponse::new();
            let (transport, _egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
            scripted.error_after_eom();
            transport.close(&context).expect("close");
            let cause = read_error_cause(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::NO_ERROR, b"Abandoned reading");
            assert_eof(read(&transport, &context, 1024, Duration::ZERO).await);
            assert_eq!(
                scripted.completion_codes(),
                [Some(HttpErrorCode::INVALID_STATE_TRANSITION)]
            );
            drop(transport);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_stream_read_timeout_commits_only_at_a_poll_of_the_stored_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;

            // A peer event before the next read disarms the expiry.
            let observed = Observed::new();
            let params = SessionParams {
                stream_read_timeout: Some(Duration::from_millis(200)),
                ..observed.params()
            };
            let (session, mut socket) = proxy.connect(&context, params).await;
            let transport = h1_tunnel(&context, &session, &mut socket, CONNECT_TIMEOUT).await;
            assert_read_timeout(read(&transport, &context, 16, READ_TIMEOUT).await);
            transport.shutdown_write(&context).expect("shutdown_write");
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert_eq!(
                observed.closed(),
                0,
                "the source did not fail at the deadline"
            );
            socket.write_all(b"data").await.expect("proxy write");
            observed.buffered(4).await;
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"data"
            );
            assert_eq!(observed.closed(), 0);
            drop(transport);
            drop(session);
            drop(socket);

            // A sampled cancellation wins over the unobserved expiry.
            let (session, mut socket) = proxy
                .connect(
                    &context,
                    SessionParams {
                        stream_read_timeout: Some(Duration::from_millis(200)),
                        ..SessionParams::default()
                    },
                )
                .await;
            let transport = h1_tunnel(&context, &session, &mut socket, CONNECT_TIMEOUT).await;
            let cancel = CancellationSource::new();
            let mut pending =
                read_with(&transport, &context, 16, 16, Duration::ZERO, cancel.token());
            park(&mut pending, "the read").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            tokio::time::sleep(Duration::from_millis(400)).await;
            cancel.request_cancellation();
            match poll_once(&mut pending).await {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the read"),
            }
            let cause = read_error_cause(read(&transport, &context, 16, Duration::ZERO).await);
            assert_cause(&cause, HttpErrorCode::CORO_CANCELLED, b"Read cancelled");
            assert_eof(read(&transport, &context, 16, Duration::ZERO).await);
            drop(transport);
            drop(session);
            drop(socket);
        })
    });
}

#[test]
fn write_baton_commits_expiry_and_cancellation_at_poll() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let timed = Duration::from_millis(50);

            // window_open before the poll that would observe the expiry.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            let wakes = Arc::new(WakeCounter::default());
            let mut parked = write(&transport, &context, b"late", timed);
            assert!(poll_with(&mut parked, &wakes.waker()).is_pending());
            let expired = Arc::clone(&wakes);
            eventually("the write's deadline", move || expired.wakes() > 0).await;
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            let signalled = Arc::clone(&wakes);
            eventually("window_open", move || signalled.wakes() > 1).await;
            match poll_with(&mut parked, &wakes.waker()) {
                Poll::Ready(result) => assert_written(result, 4),
                Poll::Pending => panic!("the signalled baton completes the write"),
            }
            assert_eq!(tunnel.read_exact(4).await, b"late");
            assert_written(
                write(&transport, &context, b"after", Duration::ZERO).await,
                5,
            );
            assert_eq!(tunnel.read_exact(5).await, b"after");
            drop(transport);
            drop(session);

            // shutdown_write after an unobserved deadline, or after a
            // cancellation on the worker, sets the pending EOM.
            for cancelled in [false, true] {
                let (session, transport, tunnel) = tunnel_transport(
                    &context,
                    &proxy,
                    SessionParams::default(),
                    SMALL_EGRESS_BUFFER,
                )
                .await;
                let stalled = stall(&transport, &context).await;
                let cancel = CancellationSource::new();
                let wakes = Arc::new(WakeCounter::default());
                let mut parked = if cancelled {
                    write_with(
                        &transport,
                        &context,
                        b"late",
                        Duration::ZERO,
                        cancel.token(),
                    )
                } else {
                    write(&transport, &context, b"late", timed)
                };
                assert!(poll_with(&mut parked, &wakes.waker()).is_pending());
                if cancelled {
                    cancel.request_cancellation();
                } else {
                    let expired = Arc::clone(&wakes);
                    eventually("the write's deadline", move || expired.wakes() > 0).await;
                }
                transport.shutdown_write(&context).expect("shutdown_write");
                let result = parked.await;
                if cancelled {
                    assert_write_cancelled(result);
                } else {
                    assert_write_error(result, HttpErrorCode::WRITE_TIMEOUT, b"Write timed out");
                }
                assert_eq!(tunnel.read_exact(STALL).await, stalled);
                tunnel.assert_quiet().await;
                if cancelled {
                    transport.close(&context).expect("close");
                } else {
                    transport.shutdown_write(&context).expect("shutdown_write");
                }
                assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
                drop(transport);
                drop(session);
            }

            // A parked write dropped after an unobserved deadline commits
            // nothing.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            let wakes = Arc::new(WakeCounter::default());
            let mut dropped = write(&transport, &context, b"dropped", timed);
            assert!(poll_with(&mut dropped, &wakes.waker()).is_pending());
            let expired = Arc::clone(&wakes);
            eventually("the write's deadline", move || expired.wakes() > 0).await;
            drop(dropped);
            let mut next = write(&transport, &context, b"next", WAIT_DEADLINE);
            park(&mut next, "the next write").await;
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_written(next.await, 4);
            assert_eq!(tunnel.read_exact(4).await, b"next");
            drop(transport);
            drop(session);

            // A cancellation after an unobserved deadline commits Cancelled.
            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                SMALL_EGRESS_BUFFER,
            )
            .await;
            let stalled = stall(&transport, &context).await;
            let cancel = CancellationSource::new();
            let wakes = Arc::new(WakeCounter::default());
            let mut parked = write_with(&transport, &context, b"late", timed, cancel.token());
            assert!(poll_with(&mut parked, &wakes.waker()).is_pending());
            let expired = Arc::clone(&wakes);
            eventually("the write's deadline", move || expired.wakes() > 0).await;
            cancel.request_cancellation();
            match poll_with(&mut parked, &wakes.waker()) {
                Poll::Ready(result) => assert_write_cancelled(result),
                Poll::Pending => panic!("the sampled cancellation fails the write"),
            }
            assert_eq!(tunnel.read_exact(STALL).await, stalled);
            assert_write_cancelled(write(&transport, &context, b"later", Duration::ZERO).await);
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn pending_eom_write_with_a_short_content_length_reenters_window_open() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                4,
                headers(&[("Foo", "Bar"), ("Content-Length", "100")]),
                None,
            )
            .await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            park(&mut parked, "the write").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            // Its egress_eom finds the body short and credits the bytes, which
            // runs `window_open` inside this poll.
            assert_written(parked.await, 4);
            assert_eq!(
                next_egress_error(&mut egress).await.code(),
                HttpErrorCode::CONTENT_LENGTH_MISMATCH
            );
            drop(transport);
            drop(egress);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn dropped_read_and_write_end_their_operation() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;

            let (session, transport, tunnel) = tunnel_transport(
                &context,
                &proxy,
                SessionParams::default(),
                EGRESS_BUFFER_SIZE,
            )
            .await;
            let mut dropped = read(&transport, &context, 16, Duration::ZERO);
            park(&mut dropped, "the read").await;
            drop(dropped);
            tunnel.send(b"echo");
            assert_eq!(
                read_all(&transport, &context, 4, Duration::ZERO).await,
                b"echo",
                "the next read takes over the stored body read"
            );
            drop(transport);
            drop(session);

            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut dropped = write(&transport, &context, b"efgh", Duration::ZERO);
            park(&mut dropped, "the write").await;
            transport.shutdown_write(&context).expect("shutdown_write");
            drop(dropped);
            let mut next = write(&transport, &context, b"ijkl", Duration::ZERO);
            park(&mut next, "the next write").await;
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            assert_written(next.await, 4);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"ijkl".to_vec(), false),
                "the dropped write took its pending EOM"
            );
            transport.shutdown_write(&context).expect("shutdown_write");
            assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
            drop(transport);
            drop(egress);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn drop_with_parked_read_and_write_cancels_both_and_closes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start().await;
            let observed = Observed::new();
            let (session, transport, tunnel) =
                tunnel_transport(&context, &proxy, observed.params(), SMALL_EGRESS_BUFFER).await;
            let stalled = stall(&transport, &context).await;
            let write_wakes = Arc::new(WakeCounter::default());
            let mut parked_write = write(&transport, &context, b"parked", Duration::ZERO);
            assert!(poll_with(&mut parked_write, &write_wakes.waker()).is_pending());
            let read_wakes = Arc::new(WakeCounter::default());
            let mut parked_read = read(&transport, &context, 16, Duration::ZERO);
            assert!(poll_with(&mut parked_read, &read_wakes.waker()).is_pending());
            let unpolled_read = read(&transport, &context, 16, Duration::ZERO);
            let unpolled_write = write(&transport, &context, b"unpolled", Duration::ZERO);
            drop(transport);
            assert!(write_wakes.wakes() > 0, "Drop wakes the parked write");
            assert!(read_wakes.wakes() > 0, "Drop wakes the parked read");
            assert_eq!(
                observed.events.drains.get(),
                1,
                "the unique tunnel drains its session inside Drop"
            );
            assert_write_cancelled(parked_write.await);
            assert_cancelled(parked_read.await);
            assert_cancelled(unpolled_read.await);
            assert_write_cancelled(unpolled_write.await);
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), stalled);
            assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");
            drop(session);

            // Releasing a stored body read can complete an aborted source and
            // run its completion callback while a cancelled starting read's
            // poll or Drop, or the handle's own Drop, still holds the
            // transport's state.
            for release in [
                StoredReadRelease::CancelledStartingReadPolled,
                StoredReadRelease::CancelledStartingReadDropped,
                StoredReadRelease::HandleDrop,
            ] {
                let polls = Rc::new(RefCell::new(PostDropPolls::default()));
                let (source, producer) = scripted_source(DropThenPoll(Rc::clone(&polls)));
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let read_after = read(&transport, &context, 16, Duration::ZERO);
                let write_after = write(&transport, &context, b"after drop", Duration::ZERO);
                if release == StoredReadRelease::HandleDrop {
                    assert_read_timeout(read(&transport, &context, 1024, READ_TIMEOUT).await);
                    {
                        let mut polls = polls.borrow_mut();
                        polls.read = Some(read_after);
                        polls.write = Some(write_after);
                    }
                    assert!(producer.abort(HttpErrorCode::CANCEL, b"peer reset"));
                    drop(transport);
                } else {
                    let cancel = CancellationSource::new();
                    let mut starting = read_with(
                        &transport,
                        &context,
                        1024,
                        1024,
                        Duration::ZERO,
                        cancel.token(),
                    );
                    park(&mut starting, "the starting read").await;
                    {
                        let mut polls = polls.borrow_mut();
                        polls.transport = Some(transport);
                        polls.read = Some(read_after);
                        polls.write = Some(write_after);
                    }
                    assert!(producer.abort(HttpErrorCode::CANCEL, b"peer reset"));
                    cancel.request_cancellation();
                    if release == StoredReadRelease::CancelledStartingReadPolled {
                        match poll_once(&mut starting).await {
                            Poll::Ready(result) => assert_cancelled(result),
                            Poll::Pending => panic!("the sampled cancellation fails the read"),
                        }
                    } else {
                        drop(starting);
                    }
                }
                polls.borrow_mut().assert_both_cancelled(release);
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (Vec::new(), true),
                    "{release:?}: the stream's close inside Drop sends the EOM, and no write \
                     bytes precede it"
                );
            }

            // Releasing a cancelled starting read's body read can drop the
            // only handle with no completion callback: a caller-provided
            // source's body read can run caller code as it drops, and the
            // session's own source releases the waker its body read saved
            // while it borrows its event baton. The read, in its poll or its
            // Drop, keeps the stream until the release returns, so the stream's
            // close stops the source once, after the release, and never
            // signals that baton while it is borrowed.
            for release in [
                StoredReadRelease::CancelledStartingReadPolled,
                StoredReadRelease::CancelledStartingReadDropped,
            ] {
                let (source, stops, live_at_stops) =
                    CallerSource::counting_live_reads(SourceStep::BodyReadDrop, teardown);
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let cancel = CancellationSource::new();
                let mut starting = read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    Duration::ZERO,
                    cancel.token(),
                );
                park(&mut starting, "the starting read").await;
                hand_to_teardown(transport);
                cancel.request_cancellation();
                if release == StoredReadRelease::CancelledStartingReadPolled {
                    match poll_once(&mut starting).await {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => panic!("the sampled cancellation fails the read"),
                    }
                } else {
                    drop(starting);
                }
                assert!(
                    teardown_ran(),
                    "{release:?}: the body read's drop dropped the handle"
                );
                assert_eq!(
                    *stops.borrow(),
                    [None],
                    "{release:?}: the stream's close stops the source once"
                );
                assert_eq!(
                    *live_at_stops.borrow(),
                    [0],
                    "{release:?}: the stop comes after the read drops the body read"
                );
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (Vec::new(), true),
                    "{release:?}: the stream's close inside Drop sends the EOM"
                );

                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    None,
                )
                .await;
                let cancel = CancellationSource::new();
                let mut starting = read_with(
                    &transport,
                    &context,
                    1024,
                    1024,
                    Duration::ZERO,
                    cancel.token(),
                );
                assert!(
                    poll_with(&mut starting, &hook_waker()).is_pending(),
                    "{release:?}: the starting read parks"
                );
                hand_to_teardown(transport);
                cancel.request_cancellation();
                if release == StoredReadRelease::CancelledStartingReadPolled {
                    // The poll's cancellation registration releases its clone
                    // of the waker first.
                    on_next_drop(|| on_next_drop(teardown));
                    match poll_with(&mut starting, &hook_waker()) {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => panic!("the sampled cancellation fails the read"),
                    }
                } else {
                    on_next_drop(teardown);
                    drop(starting);
                }
                assert!(
                    teardown_ran(),
                    "{release:?}: the source's release of the body read's waker dropped the \
                     handle"
                );
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (Vec::new(), true),
                    "{release:?}: the stream's close inside Drop sends the EOM"
                );
            }

            // A read's own release of a caller-provided body read can drop the
            // only handle too: after the body read gave data, and after a close
            // made in a start or a poll took effect when that call returned.
            // The read keeps the stream until the release returns, so the
            // stream's close stops the source once, after the release, unless
            // an EOM or an error ended it. The read returns the data, EOF for
            // an EOM, or the error its body read gave, and otherwise fails
            // with `CORO_CANCELLED`.
            let (source, stops, live_at_stops) =
                CallerSource::counting_live_reads(SourceStep::BodyReadDataDrop, teardown);
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            let first = read(&transport, &context, 1024, Duration::ZERO);
            hand_to_teardown(transport);
            assert_data(first.await, BODY_READ_DATA);
            assert!(teardown_ran(), "the body read's drop dropped the handle");
            assert_eq!(
                *stops.borrow(),
                [None],
                "the stream's close stops the source once"
            );
            assert_eq!(
                *live_at_stops.borrow(),
                [0],
                "the stop comes after the read drops the body read"
            );
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM"
            );
            drop(egress);
            for step in [
                SourceStep::SetReadTimeout,
                SourceStep::ReadBodyEvent,
                SourceStep::IngressCompletion,
                SourceStep::BodyReadDataReady,
                SourceStep::BodyReadEomReady,
                SourceStep::BodyReadErrorReady,
                SourceStep::BodyReadPending,
            ] {
                for call in [SideCall::Close, SideCall::CloseWithReset] {
                    let slot: Rc<RefCell<Weak<HttpConnectTransport>>> = Rc::default();
                    let (source, stops, live_at_stops) = CallerSource::with_drop_code(
                        step,
                        {
                            let slot = Rc::clone(&slot);
                            let context = context.clone();
                            move || {
                                let Some(transport) = slot.borrow().upgrade() else {
                                    return;
                                };
                                call.make(&transport, &context);
                            }
                        },
                        teardown,
                    );
                    let (transport, mut egress) = captured_transport(
                        &context,
                        &proxy,
                        EGRESS_BUFFER_SIZE,
                        headers(&[("Foo", "Bar")]),
                        Some(source),
                    )
                    .await;
                    *slot.borrow_mut() = Rc::downgrade(&transport);
                    let mut starting = read(&transport, &context, 1024, Duration::ZERO);
                    hand_to_teardown(transport);
                    let result = match poll_once(&mut starting).await {
                        Poll::Ready(result) => result,
                        Poll::Pending => panic!("{step:?} {call:?}: the read completes"),
                    };
                    assert_body_read_result(step, result);
                    assert!(
                        teardown_ran(),
                        "{step:?} {call:?}: the body read's drop dropped the handle"
                    );
                    assert_stopped_once(step, &stops, &live_at_stops, (step, call));
                    if call == SideCall::CloseWithReset {
                        assert_eq!(
                            next_egress_error(&mut egress).await.code(),
                            HttpErrorCode::CANCEL
                        );
                    } else {
                        assert_eq!(next_egress_bytes(&mut egress).await, (Vec::new(), true));
                    }
                }
            }

            // With no stored read, the stream's close inside the handle's Drop
            // stops the live response source, whose completion callback runs
            // inline.
            let polls = Rc::new(RefCell::new(PostDropPolls::default()));
            let (source, _producer) = scripted_source(DropThenPoll(Rc::clone(&polls)));
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            {
                let mut polls = polls.borrow_mut();
                polls.read = Some(read(&transport, &context, 16, Duration::ZERO));
                polls.write = Some(write(&transport, &context, b"after drop", Duration::ZERO));
            }
            drop(transport);
            polls
                .borrow_mut()
                .assert_both_cancelled("the stream's close inside Drop");
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM, and no write bytes precede it"
            );

            // A caller-provided response source can drop the handle inside the
            // calls by which a read starts a body read, or inside the poll of
            // that body read. That read fails too when the body read then
            // gives an event that ends no read or stays pending, and otherwise
            // returns EOF for an EOM or the error the body read gave. The
            // stream's close stops the source once, after the read drops the
            // body read, unless that EOM or error ended it.
            for step in [
                SourceStep::SetReadTimeout,
                SourceStep::ReadBodyEvent,
                SourceStep::IngressCompletion,
                SourceStep::BodyReadReady,
                SourceStep::BodyReadEomReady,
                SourceStep::BodyReadErrorReady,
                SourceStep::BodyReadPending,
            ] {
                let polls = Rc::new(RefCell::new(PostDropPolls::default()));
                let (source, stops, live_at_stops) = CallerSource::counting_live_reads(step, {
                    let polls = Rc::clone(&polls);
                    move || PostDropPolls::drop_handle_then_poll(&polls)
                });
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let mut starting = read(&transport, &context, 1024, Duration::ZERO);
                {
                    let mut polls = polls.borrow_mut();
                    polls.read = Some(read(&transport, &context, 16, Duration::ZERO));
                    polls.write = Some(write(&transport, &context, b"after drop", Duration::ZERO));
                    polls.transport = Some(transport);
                }
                match poll_once(&mut starting).await {
                    Poll::Ready(result) => assert_body_read_result(step, result),
                    Poll::Pending => panic!("{step:?}: the starting read completes"),
                }
                polls.borrow_mut().assert_both_cancelled(step);
                assert_stopped_once(step, &stops, &live_at_stops, step);
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (Vec::new(), true),
                    "{step:?}: the stream's close inside Drop sends the EOM, and no write bytes \
                     precede it"
                );
            }

            // When `window_open` signals the baton, the parked write's waker
            // runs inline and can drop the only handle.
            let (transport, mut egress) =
                captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
            assert_written(
                write(&transport, &context, b"abcd", Duration::ZERO).await,
                4,
            );
            let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
            assert!(
                poll_with(&mut parked, &TeardownWaker::waker()).is_pending(),
                "the write parks on the closed window"
            );
            hand_to_teardown(transport);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (b"abcd".to_vec(), false)
            );
            assert!(
                teardown_ran(),
                "window_open woke the parked write, whose waker dropped the handle"
            );
            assert_write_cancelled(parked.await);
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM, and the parked write's bytes \
                 never reach the egress"
            );

            // A write's commit queues its bytes in the egress source, which
            // wakes the egress consumer inline, and that consumer's waker can
            // drop the only handle. The write keeps its result, and the stream
            // closes once the commit's producer call returns.
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                None,
            )
            .await;
            let mut consumer = Box::pin(next_egress(&mut egress));
            assert!(
                poll_with(&mut consumer, &TeardownWaker::waker()).is_pending(),
                "the egress consumer parks"
            );
            let mut writing = write(&transport, &context, b"abcd", Duration::ZERO);
            hand_to_teardown(transport);
            match poll_once(&mut writing).await {
                Poll::Ready(result) => assert_written(result, 4),
                Poll::Pending => panic!("the write finds the window open"),
            }
            assert!(
                teardown_ran(),
                "the commit woke the egress consumer, whose waker dropped the handle"
            );
            let (mut received, mut eom) = consumer.await.expect("egress event");
            while !eom {
                let (bytes, end) = next_egress_bytes(&mut egress).await;
                received.extend(bytes);
                eom = end;
            }
            assert_eq!(
                received, b"abcd",
                "the write's bytes, then the EOM from the stream's close"
            );

            // A parked read keeps its waker. Releasing it can drop the task
            // that owns the only handle: when the read completes, when a later
            // poll replaces it, and when the read is dropped.
            let (source, response) = ScriptedResponse::new();
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            let mut completing = read(&transport, &context, 1024, Duration::ZERO);
            assert!(
                poll_with(&mut completing, &TeardownOnRelease::waker()).is_pending(),
                "the read parks"
            );
            hand_to_teardown(transport);
            response.producer.body(queue(b"done"), 0, false);
            match poll_with(&mut completing, Waker::noop()) {
                Poll::Ready(result) => assert_data(result, b"done"),
                Poll::Pending => panic!("the read takes the body event"),
            }
            assert!(
                teardown_ran(),
                "the completed read released the waker, which dropped the handle"
            );
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM"
            );

            for dropped in [false, true] {
                let (source, stops) = CallerSource::new(SourceStep::BodyReadPending, || {});
                let (transport, mut egress) = captured_transport(
                    &context,
                    &proxy,
                    EGRESS_BUFFER_SIZE,
                    headers(&[("Foo", "Bar")]),
                    Some(source),
                )
                .await;
                let mut pending = read(&transport, &context, 1024, Duration::ZERO);
                assert!(
                    poll_with(&mut pending, &TeardownOnRelease::waker()).is_pending(),
                    "dropped={dropped}: the read parks"
                );
                hand_to_teardown(transport);
                if dropped {
                    drop(pending);
                } else {
                    let wakes = Arc::new(WakeCounter::default());
                    assert!(
                        poll_with(&mut pending, &wakes.waker()).is_pending(),
                        "the read stays parked"
                    );
                    assert!(wakes.wakes() > 0, "Drop wakes the read's new waker");
                    match poll_with(&mut pending, Waker::noop()) {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => panic!("the read polled after Drop completes"),
                    }
                }
                assert!(
                    teardown_ran(),
                    "dropped={dropped}: releasing the read's waker dropped the handle"
                );
                assert_eq!(*stops.borrow(), [None], "dropped={dropped}: one stop");
                assert_eq!(
                    next_egress_bytes(&mut egress).await,
                    (Vec::new(), true),
                    "dropped={dropped}: the stream's close inside Drop sends the EOM"
                );
            }

            // The baton keeps a parked write's waker. Releasing it can drop
            // the task that owns the only handle, inside the baton's wait.
            for release in [
                ParkedWriteRelease::Repoll,
                ParkedWriteRelease::RepollAfterSignal,
                ParkedWriteRelease::Drop,
            ] {
                let (transport, mut egress) =
                    captured_transport(&context, &proxy, 4, headers(&[("Foo", "Bar")]), None).await;
                assert_written(
                    write(&transport, &context, b"abcd", Duration::ZERO).await,
                    4,
                );
                let mut parked = write(&transport, &context, b"efgh", Duration::ZERO);
                assert!(
                    poll_with(&mut parked, &TeardownOnRelease::waker()).is_pending(),
                    "{release:?}: the write parks on the closed window"
                );
                hand_to_teardown(transport);
                let mut received = Vec::new();
                if release == ParkedWriteRelease::RepollAfterSignal {
                    let (bytes, eom) = next_egress_bytes(&mut egress).await;
                    assert!(!eom);
                    received = bytes;
                    assert!(!teardown_ran(), "window_open's signal wakes the write");
                }
                if release == ParkedWriteRelease::Drop {
                    drop(parked);
                } else {
                    let wakes = Arc::new(WakeCounter::default());
                    match poll_with(&mut parked, &wakes.waker()) {
                        Poll::Ready(result) => assert_write_cancelled(result),
                        Poll::Pending => {
                            assert!(
                                wakes.wakes() > 0,
                                "{release:?}: Drop wakes the write's new waker"
                            );
                            assert_write_cancelled(parked.await);
                        }
                    }
                }
                assert!(
                    teardown_ran(),
                    "{release:?}: releasing the write's waker dropped the handle"
                );
                loop {
                    let (bytes, eom) = next_egress_bytes(&mut egress).await;
                    received.extend(bytes);
                    if eom {
                        break;
                    }
                }
                assert_eq!(
                    received, b"abcd",
                    "{release:?}: the first write's bytes, then the EOM from the stream's close; \
                     the parked write's bytes never reach the egress"
                );
            }

            // Cloning a waker can run caller code that drops the only handle:
            // the clone that a read saves as it parks on its body read, and the
            // clone that a cancellable token's registration takes before the
            // read's entry checks.
            let (source, stops, live_at_stops) =
                CallerSource::counting_live_reads(SourceStep::BodyReadPending, || {});
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                Some(source),
            )
            .await;
            let mut pending = read(&transport, &context, 1024, Duration::ZERO);
            hand_to_teardown(transport);
            on_next_clone(teardown);
            match poll_with(&mut pending, &hook_waker()) {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => {
                    panic!("the read whose waker's clone dropped the handle completes")
                }
            }
            assert!(
                teardown_ran(),
                "the parking read's clone dropped the handle"
            );
            assert_eq!(
                *stops.borrow(),
                [None],
                "the stream's close stops the source once"
            );
            assert_eq!(
                *live_at_stops.borrow(),
                [0],
                "Drop releases the stored read before the stream's close stops the source"
            );
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM"
            );

            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                None,
            )
            .await;
            let cancel = CancellationSource::new();
            let mut empty = read_with(&transport, &context, 0, 1, Duration::ZERO, cancel.token());
            hand_to_teardown(transport);
            on_next_clone(teardown);
            match poll_with(&mut empty, &hook_waker()) {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => {
                    panic!("the read whose waker's clone dropped the handle completes")
                }
            }
            assert!(
                teardown_ran(),
                "the token registration's clone dropped the handle"
            );
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM"
            );

            // The session's own response source clones the waker in its body
            // read's poll while it borrows its event baton, and that clone can
            // drop the only handle. The read keeps the stream until it drops
            // the body read, so the stream's close, which stops that source and
            // signals that baton, runs after the poll.
            let (transport, mut egress) = captured_transport(
                &context,
                &proxy,
                EGRESS_BUFFER_SIZE,
                headers(&[("Foo", "Bar")]),
                None,
            )
            .await;
            let mut pending = read(&transport, &context, 1024, Duration::ZERO);
            hand_to_teardown(transport);
            on_next_clone(teardown);
            match poll_with(&mut pending, &hook_waker()) {
                Poll::Ready(result) => assert_cancelled(result),
                Poll::Pending => {
                    panic!("the read whose body read's clone dropped the handle completes")
                }
            }
            assert!(
                teardown_ran(),
                "the session source's clone of the waker dropped the handle"
            );
            assert_eq!(
                next_egress_bytes(&mut egress).await,
                (Vec::new(), true),
                "the stream's close inside Drop sends the EOM"
            );
            proxy.shutdown().await;

            // A caller-provided host's timer can drop the only handle inside
            // the poll of a read's timer, while the read holds the stored read.
            on_second_worker(Host::new(Rc::new(HookedTimerHost)), |context| {
                Box::pin(async move {
                    let proxy = TunnelProxy::start().await;
                    let (source, stops, live_at_stops) =
                        CallerSource::counting_live_reads(SourceStep::BodyReadPending, || {});
                    let (transport, mut egress) = captured_transport(
                        &context,
                        &proxy,
                        EGRESS_BUFFER_SIZE,
                        headers(&[("Foo", "Bar")]),
                        Some(source),
                    )
                    .await;
                    let mut pending = read(&transport, &context, 1024, Duration::from_secs(3600));
                    hand_to_teardown(transport);
                    on_next_timer_poll(teardown);
                    match poll_once(&mut pending).await {
                        Poll::Ready(result) => assert_cancelled(result),
                        Poll::Pending => {
                            panic!("the read whose timer dropped the handle completes")
                        }
                    }
                    assert!(teardown_ran(), "the timer's poll dropped the handle");
                    assert_eq!(
                        *stops.borrow(),
                        [None],
                        "the stream's close stops the source"
                    );
                    assert_eq!(
                        *live_at_stops.borrow(),
                        [0],
                        "the stop comes after the read drops the body read"
                    );
                    assert_eq!(
                        next_egress_bytes(&mut egress).await,
                        (Vec::new(), true),
                        "the stream's close inside Drop sends the EOM"
                    );
                    proxy.shutdown().await;
                })
            })
            .await;
        })
    });
}
