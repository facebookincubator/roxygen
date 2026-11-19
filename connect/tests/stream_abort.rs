/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for the tunnel abort in `roxygen_connect::experimental`.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use roxygen_client::connector::ClientSession;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::experimental::ConnectAbortExt;
use roxygen_connect::experimental::connect_rejection;
use roxygen_connect::experimental::connect_unique_with_reset_on_cancel;
use roxygen_connect::experimental::connect_with_reset_on_cancel;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::connection::ServeTransportOptions;
use roxygen_server::connection::ServerConnection;
use roxygen_server::connection::serve_transport;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::shutdown::DrainTrigger;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;

use crate::connect_stream::AUTHORITY;
use crate::connect_stream::CONNECT_TIMEOUT;
use crate::connect_stream::EGRESS_BUFFER_SIZE;
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
const STREAM_1: StreamId = StreamId::new(1);
const STREAM_3: StreamId = StreamId::new(3);
const STREAM_5: StreamId = StreamId::new(5);
const CANCEL: HttpErrorCode = HttpErrorCode::CANCEL;

type ConnectFuture = LocalConnectFuture<Result<HttpConnectStream, HttpError>>;

/// The proxy side of one CONNECT stream, driven by the test.
struct ProxyTunnel {
    request: Rc<RefCell<HttpSourceHolder>>,
    response: HttpStreamSourceProducer,
}

impl ProxyTunnel {
    fn accept(&self) {
        assert!(self.response.headers(response(200), false));
    }

    fn reject(&self) {
        assert!(self.response.headers(response(403), false));
    }

    /// Resets the stream from the proxy's side.
    fn reset(&self, code: HttpErrorCode) {
        assert!(self.response.abort(code, b"proxy reset"));
    }

    fn send(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, false);
    }

    fn finish(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, true);
    }

    /// Reads tunnel bytes until EOM, or returns the request's read error.
    async fn read_to_end(&mut self) -> Result<Vec<u8>, HttpError> {
        let mut request = std::mem::take(&mut *self.request.borrow_mut());
        let mut received = Vec::new();
        let result = loop {
            let event = match request.read_body_event(u32::MAX).await {
                Ok(event) => event,
                Err(error) => break Err(error),
            };
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Suspend(resume) => {
                    resume.await;
                }
                HttpBodyEventKind::Body(body) => received.extend_from_slice(&bytes_of(&body)),
                _ => {}
            }
            if eom {
                break Ok(received);
            }
        };
        *self.request.borrow_mut() = request;
        result
    }

    /// Reads exactly `length` tunnel bytes.
    async fn read_exact(&mut self, length: usize) -> Vec<u8> {
        let mut request = std::mem::take(&mut *self.request.borrow_mut());
        let mut received = Vec::new();
        while received.len() < length {
            let event = request
                .read_body_event(u32::try_from(length - received.len()).expect("small read"))
                .await
                .expect("tunnel bytes");
            match event.into_kind() {
                HttpBodyEventKind::Suspend(resume) => {
                    resume.await;
                }
                HttpBodyEventKind::Body(body) => received.extend_from_slice(&bytes_of(&body)),
                _ => {}
            }
        }
        *self.request.borrow_mut() = request;
        received
    }
}

#[derive(Default)]
struct TunnelSlots {
    tunnels: RefCell<VecDeque<ProxyTunnel>>,
    held: RefCell<Vec<(Rc<RefCell<HttpSourceHolder>>, HttpStreamSourceProducer)>>,
}

/// Accepts every CONNECT and leaves both directions to the test.
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

type PeerResets = Arc<Mutex<Vec<(StreamId, HttpErrorCode)>>>;

/// Records the RST_STREAM frames that the proxy session receives.
struct PeerResetRecorder(PeerResets);

impl LifecycleObserver for PeerResetRecorder {
    fn on_event(&self, event: LifecycleEvent) {
        if let LifecycleEvent::PeerReset { stream, code } = event {
            self.0.lock().expect("resets").push((stream, code));
        }
    }
}

/// An h2c `roxygen_server` proxy on the test worker.
struct Proxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
    slots: Rc<TunnelSlots>,
    resets: PeerResets,
    connections: RefCell<Vec<ServerConnection>>,
}

impl Proxy {
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

    /// Waits for the next CONNECT stream that reached the proxy handler.
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

    fn peer_resets(&self) -> Vec<(StreamId, HttpErrorCode)> {
        self.resets.lock().expect("resets").clone()
    }

    async fn resets_reach(&self, count: usize) {
        eventually("RST_STREAM at the proxy", || {
            self.peer_resets().len() >= count
        })
        .await;
    }

    /// Releases every tunnel and cancels every proxy session.
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

impl Drop for Proxy {
    fn drop(&mut self) {
        self.release();
    }
}

/// Records drains, peer resets, and detached streams of a client session.
#[derive(Default)]
struct ClientEvents {
    drains: RefCell<Vec<DrainTrigger>>,
    peer_resets: RefCell<Vec<(StreamId, HttpErrorCode)>>,
    detached: RefCell<Vec<StreamId>>,
}

impl LifecycleObserver for ClientEvents {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::DrainStarted(trigger) => self.drains.borrow_mut().push(trigger),
            LifecycleEvent::PeerReset { stream, code } => {
                self.peer_resets.borrow_mut().push((stream, code));
            }
            LifecycleEvent::TransactionDetached {
                stream: Some(stream),
            } => self.detached.borrow_mut().push(stream),
            _ => {}
        }
    }
}

/// Panics on the drain that an HTTP/1.1 stream reset starts.
struct PanicOnStreamReset;

impl LifecycleObserver for PanicOnStreamReset {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(
            event,
            LifecycleEvent::DrainStarted(DrainTrigger::StreamReset)
        ) {
            panic!("lifecycle observer panic");
        }
    }
}

/// The panic of a caller that drops a connect future while it unwinds.
struct CallerPanic;

/// Counts tunnel bytes that a client session buffered for a response source.
#[derive(Default)]
struct BufferedReads(std::cell::Cell<i64>);

impl Http2SessionStats for BufferedReads {
    fn record_buffered_read_bytes(&self, delta: i64) {
        if delta > 0 {
            self.0.set(self.0.get() + delta);
        }
    }
}

#[derive(Clone, Default)]
struct Observed {
    events: Rc<ClientEvents>,
    reads: Rc<BufferedReads>,
}

impl Observed {
    fn params(&self) -> SessionParams {
        SessionParams {
            lifecycle_observer: Some(self.events.clone()),
            session_stats: Some(self.reads.clone()),
            ..SessionParams::default()
        }
    }

    async fn buffered(&self, bytes: i64) {
        let reads = Rc::clone(&self.reads);
        eventually("buffered tunnel bytes", move || reads.0.get() >= bytes).await;
    }

    async fn detached(&self, count: usize) {
        let events = Rc::clone(&self.events);
        eventually("a detached CONNECT stream", move || {
            events.detached.borrow().len() >= count
        })
        .await;
    }

    fn drains(&self) -> Vec<DrainTrigger> {
        self.events.drains.borrow().clone()
    }

    async fn peer_reset(&self, stream: StreamId) {
        let events = Rc::clone(&self.events);
        eventually("the proxy's RST_STREAM at the client", move || {
            events
                .peer_resets
                .borrow()
                .iter()
                .any(|(id, _)| *id == stream)
        })
        .await;
    }
}

/// Counts wakes.
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

/// A waker that panics when woken.
struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("waker panic");
    }
}

async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(Pin::new(&mut *future).poll(context))).await
}

/// Polls a connect future until its CONNECT is committed and its head read parks.
async fn poll_until_parked(connect: &mut ConnectFuture) {
    assert!(
        poll_once(connect).await.is_pending(),
        "the connect waits for the CONNECT response head"
    );
}

fn poll_read_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
) -> Poll<io::Result<Vec<u8>>> {
    let mut buffer = [0; 64];
    let mut read = ReadBuf::new(&mut buffer);
    tokio::io::AsyncRead::poll_read(
        Pin::new(transport),
        &mut Context::from_waker(waker),
        &mut read,
    )
    .map_ok(|()| read.filled().to_vec())
}

fn poll_write_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
    bytes: &[u8],
) -> Poll<io::Result<usize>> {
    Pin::new(transport).poll_write(&mut Context::from_waker(waker), bytes)
}

fn poll_flush_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
) -> Poll<io::Result<()>> {
    Pin::new(transport).poll_flush(&mut Context::from_waker(waker))
}

fn poll_shutdown_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
) -> Poll<io::Result<()>> {
    Pin::new(transport).poll_shutdown(&mut Context::from_waker(waker))
}

fn noop_waker() -> Waker {
    Arc::new(WakeCounter::default()).waker()
}

/// Asserts that a read failed with an `io::Error` carrying `expected`.
fn assert_read_error(result: Poll<io::Result<Vec<u8>>>, expected: HttpErrorCode) {
    let Poll::Ready(Err(error)) = result else {
        panic!("expected a failed read, got {result:?}");
    };
    let code = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<HttpError>())
        .map(HttpError::code);
    assert_eq!(code, Some(expected), "{error}");
}

fn assert_cancelled_read(result: Poll<io::Result<Vec<u8>>>) {
    assert_read_error(result, CANCEL);
}

fn io_kind<T: std::fmt::Debug>(result: Poll<io::Result<T>>) -> io::ErrorKind {
    let Poll::Ready(Err(error)) = result else {
        panic!("expected an I/O error, got {result:?}");
    };
    error.kind()
}

/// Returns once the proxy has processed every frame that the client wrote
/// before the call. A round trip on a new tunnel of the same session follows
/// those frames on the connection. The caller keeps the returned tunnel until
/// it is done.
async fn barrier(
    context: &WorkerContext,
    proxy: &Proxy,
    session: &ClientSession,
) -> (HttpConnectAsyncTransport, ProxyTunnel) {
    let (mut transport, mut tunnel) = establish_adapter(context, proxy, session).await;
    transport.write_all(b"?").await.expect("barrier write");
    transport.flush().await.expect("barrier flush");
    assert_eq!(tunnel.read_exact(1).await, b"?");
    tunnel.send(b"!");
    let mut reply = [0; 1];
    transport
        .read_exact(&mut reply)
        .await
        .expect("barrier read");
    (transport, tunnel)
}

/// A raw TCP proxy socket and an HTTP/1.1 client session connected to it.
async fn h1_proxy(
    context: &WorkerContext,
    session: SessionParams,
) -> (tokio::net::TcpStream, ClientSession) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener");
    let address = listener.local_addr().expect("proxy address");
    let connect = HttpConnector::connect(
        context,
        socket_address(address),
        Duration::ZERO,
        ConnectionParams::default(),
        session,
        CancellationToken::never(),
    );
    let (accepted, session) = tokio::join!(listener.accept(), connect);
    let (socket, _) = accepted.expect("proxy accept");
    (socket, session.expect("client connects to the proxy"))
}

/// Reads the CONNECT request head at the raw proxy.
async fn read_connect_head(socket: &mut tokio::net::TcpStream) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(socket.read_u8().await.expect("CONNECT head byte"));
    }
    assert!(head.starts_with(b"CONNECT example.com:443 HTTP/1.1\r\n"));
}

fn shared_connect(context: &WorkerContext, session: &ClientSession) -> ConnectFuture {
    HttpConnectStream::connect(
        context,
        session,
        session.reserve_request().expect("proxy reservation"),
        AUTHORITY,
        CONNECT_TIMEOUT,
        headers(&[("Foo", "Bar")]),
        EGRESS_BUFFER_SIZE,
        CancellationToken::never(),
    )
}

/// Establishes a tunnel on `session` that the proxy accepts.
async fn establish(
    context: &WorkerContext,
    proxy: &Proxy,
    session: &ClientSession,
) -> (HttpConnectStream, ProxyTunnel) {
    let mut connect = shared_connect(context, session);
    poll_until_parked(&mut connect).await;
    let tunnel = proxy.tunnel().await;
    tunnel.accept();
    (connect.await.expect("CONNECT"), tunnel)
}

async fn establish_adapter(
    context: &WorkerContext,
    proxy: &Proxy,
    session: &ClientSession,
) -> (HttpConnectAsyncTransport, ProxyTunnel) {
    let (stream, tunnel) = establish(context, proxy, session).await;
    (HttpConnectAsyncTransport::new(context, stream), tunnel)
}

/// Fills the proxy's stream window, then leaves a write whose completion is
/// deferred because the egress source holds more than its buffer.
async fn defer_write(transport: &mut HttpConnectAsyncTransport) {
    transport
        .write_all(&vec![b'w'; PROXY_WINDOW])
        .await
        .expect("window-filling write");
    transport
        .flush()
        .await
        .expect("the session takes the window's bytes");
    transport
        .write_all(&vec![b'p'; EGRESS_BUFFER_SIZE as usize + 1])
        .await
        .expect("the write that closes the egress window");
}

#[test]
fn h2_abort_after_upload_half_close_resets_streaming_response() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let observed = Observed::default();
            let session = proxy.connect(&context, observed.params()).await;
            let (mut transport, mut tunnel) = establish_adapter(&context, &proxy, &session).await;
            tunnel.send(b"streaming");
            let mut received = vec![0; 9];
            transport
                .read_exact(&mut received)
                .await
                .expect("tunnel bytes");
            assert_eq!(received, b"streaming");
            transport.shutdown().await.expect("upload half-close");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            tunnel.send(b"still streaming");
            observed.buffered(9 + 15).await;
            transport.abort();
            proxy.resets_reach(1).await;
            assert_eq!(proxy.peer_resets(), [(STREAM_1, CANCEL)]);
            assert_cancelled_read(poll_read_with(&mut transport, &noop_waker()));
            observed.detached(1).await;

            // The proxy's END_STREAM arrived unread while the upload is open.
            let (mut half_closed, tunnel) = establish_adapter(&context, &proxy, &session).await;
            tunnel.finish(b"unread");
            observed.buffered(9 + 15 + 6).await;
            half_closed.abort();
            proxy.resets_reach(2).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(
                proxy.peer_resets(),
                [(STREAM_1, CANCEL), (STREAM_3, CANCEL)]
            );
            assert_cancelled_read(poll_read_with(&mut half_closed, &noop_waker()));
            observed.detached(2).await;
            drop((transport, half_closed, fence));
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn abort_fails_parked_read_and_deferred_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let (mut transport, _tunnel) = establish_adapter(&context, &proxy, &session).await;
            defer_write(&mut transport).await;
            let writer = Arc::new(WakeCounter::default());
            assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
            let reader = Arc::new(WakeCounter::default());
            assert!(poll_read_with(&mut transport, &reader.waker()).is_pending());
            transport.abort();
            assert!(reader.wakes() > 0, "the abort wakes the parked reader");
            assert!(writer.wakes() > 0, "the abort wakes the parked writer");
            assert_cancelled_read(poll_read_with(&mut transport, &reader.waker()));
            assert_eq!(
                io_kind(poll_flush_with(&mut transport, &writer.waker())),
                io::ErrorKind::ConnectionAborted,
                "the deferred write fails"
            );
            assert_eq!(
                io_kind(poll_write_with(&mut transport, &writer.waker(), b"later")),
                io::ErrorKind::NotConnected,
                "later writes fail"
            );
            proxy.resets_reach(1).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(proxy.peer_resets(), [(STREAM_1, CANCEL)]);

            // A parked writer whose waker panics wakes after the reset and the
            // reader's wake. Its panic then reaches the caller.
            let (mut panicking, _tunnel) = establish_adapter(&context, &proxy, &session).await;
            defer_write(&mut panicking).await;
            let writer = Waker::from(Arc::new(PanicOnWake));
            assert!(poll_flush_with(&mut panicking, &writer).is_pending());
            let reader = Arc::new(WakeCounter::default());
            assert!(poll_read_with(&mut panicking, &reader.waker()).is_pending());
            assert!(
                catch_unwind(AssertUnwindSafe(|| panicking.abort())).is_err(),
                "the writer waker's panic reaches the caller"
            );
            assert!(reader.wakes() > 0, "the abort wakes the parked reader");
            assert_cancelled_read(poll_read_with(&mut panicking, &reader.waker()));
            assert_eq!(
                io_kind(poll_flush_with(&mut panicking, &noop_waker())),
                io::ErrorKind::ConnectionAborted,
                "the deferred write fails"
            );
            proxy.resets_reach(2).await;
            let second_fence = barrier(&context, &proxy, &session).await;
            assert_eq!(
                proxy.peer_resets(),
                [(STREAM_1, CANCEL), (STREAM_5, CANCEL)]
            );
            drop((transport, panicking, fence, second_fence));
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn abort_closes_both_directions_and_wakes_waiters() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let (mut stream, _tunnel) = establish(&context, &proxy, &session).await;
            assert!(stream.can_read() && stream.can_write());
            stream.abort();
            assert!(!stream.can_read(), "the abort closes the read direction");
            assert!(!stream.can_write(), "the abort closes the write direction");
            proxy.resets_reach(1).await;

            // A reader and a shutdown that waits for a deferred write both wake.
            let (mut transport, _tunnel) = establish_adapter(&context, &proxy, &session).await;
            defer_write(&mut transport).await;
            let reader = Arc::new(WakeCounter::default());
            let closer = Arc::new(WakeCounter::default());
            assert!(poll_read_with(&mut transport, &reader.waker()).is_pending());
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            transport.abort();
            assert!(reader.wakes() > 0 && closer.wakes() > 0);
            assert_cancelled_read(poll_read_with(&mut transport, &reader.waker()));
            assert_cancelled_read(poll_read_with(&mut transport, &reader.waker()));
            assert_eq!(
                io_kind(poll_shutdown_with(&mut transport, &closer.waker())),
                io::ErrorKind::ConnectionAborted
            );
            assert_eq!(
                io_kind(poll_write_with(&mut transport, &closer.waker(), b"x")),
                io::ErrorKind::NotConnected
            );
            proxy.resets_reach(2).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(
                proxy.peer_resets(),
                [(STREAM_1, CANCEL), (STREAM_3, CANCEL)]
            );
            drop((transport, fence));
            drop(stream);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn abort_keeps_sibling_tunnel_and_session_usable() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let observed = Observed::default();
            let session = proxy.connect(&context, observed.params()).await;
            let (mut aborted, _aborted_tunnel) =
                establish_adapter(&context, &proxy, &session).await;
            let (mut sibling, mut sibling_tunnel) =
                establish_adapter(&context, &proxy, &session).await;
            // The sibling's upload waits behind the proxy's window, and its
            // response bytes wait unread.
            defer_write(&mut sibling).await;
            sibling_tunnel.send(b"early");
            observed.buffered(5).await;
            aborted.abort();
            proxy.resets_reach(1).await;
            let queued = PROXY_WINDOW + EGRESS_BUFFER_SIZE as usize + 1;
            let mut expected = vec![b'w'; PROXY_WINDOW];
            expected.resize(queued, b'p');
            assert!(
                sibling_tunnel.read_exact(queued).await == expected,
                "the sibling's queued upload arrives intact"
            );
            sibling.flush().await.expect("the deferred write completes");
            let mut early = [0; 5];
            sibling.read_exact(&mut early).await.expect("sibling read");
            assert_eq!(&early, b"early");
            sibling.write_all(b"ping").await.expect("sibling write");
            sibling.flush().await.expect("sibling flush");
            assert_eq!(sibling_tunnel.read_exact(4).await, b"ping");
            sibling_tunnel.send(b"pong");
            let mut received = [0; 4];
            sibling
                .read_exact(&mut received)
                .await
                .expect("sibling read");
            assert_eq!(&received, b"pong");
            assert!(session.supports_more_transactions());
            let (mut third, mut third_tunnel) = establish_adapter(&context, &proxy, &session).await;
            third.write_all(b"third").await.expect("third write");
            third.flush().await.expect("third flush");
            assert_eq!(third_tunnel.read_exact(5).await, b"third");
            assert_eq!(proxy.peer_resets(), [(STREAM_1, CANCEL)]);
            drop((aborted, sibling, third));
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn repeated_abort_sends_one_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let (mut transport, _tunnel) = establish_adapter(&context, &proxy, &session).await;
            transport.abort();
            transport.abort();
            transport.close_now();
            transport.close();
            drop(transport);
            let (mut stream, _tunnel) = establish(&context, &proxy, &session).await;
            stream.abort();
            stream.abort();
            stream.close();
            drop(stream);
            // `close_now` resets through the failed upload, then the abort runs.
            let (mut transport, _tunnel) = establish_adapter(&context, &proxy, &session).await;
            defer_write(&mut transport).await;
            transport.close_now();
            transport.abort();
            drop(transport);
            proxy.resets_reach(3).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(
                proxy.peer_resets(),
                [(STREAM_1, CANCEL), (STREAM_3, CANCEL), (STREAM_5, CANCEL)],
                "one RST_STREAM per tunnel"
            );
            drop(fence);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn abort_after_tunnel_completion_sends_no_frame() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let observed = Observed::default();
            let session = proxy.connect(&context, observed.params()).await;

            // The proxy's END_STREAM arrived unread after the upload's END_STREAM.
            let (mut transport, mut tunnel) = establish_adapter(&context, &proxy, &session).await;
            transport.shutdown().await.expect("upload half-close");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            tunnel.finish(b"unread");
            observed.buffered(6).await;
            transport.abort();
            assert_cancelled_read(poll_read_with(&mut transport, &noop_waker()));
            observed.detached(1).await;

            // A read parked before the proxy's END_STREAM fails at its next poll.
            let (mut transport, mut tunnel) = establish_adapter(&context, &proxy, &session).await;
            transport.shutdown().await.expect("upload half-close");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            let reader = Arc::new(WakeCounter::default());
            assert!(poll_read_with(&mut transport, &reader.waker()).is_pending());
            tunnel.finish(b"tail");
            observed.buffered(6 + 4).await;
            transport.abort();
            assert_cancelled_read(poll_read_with(&mut transport, &reader.waker()));
            observed.detached(2).await;

            // A proxy reset that a read has not reported yet keeps its code.
            let (mut transport, tunnel) = establish_adapter(&context, &proxy, &session).await;
            tunnel.reset(HttpErrorCode::CONNECT_ERROR);
            observed.peer_reset(STREAM_5).await;
            transport.abort();
            assert_read_error(
                poll_read_with(&mut transport, &noop_waker()),
                HttpErrorCode::CONNECT_ERROR,
            );
            observed.detached(3).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(proxy.peer_resets(), [], "no frame for a complete tunnel");
            drop((transport, fence));
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_abort_drains_the_proxy_session() {
    run_on_worker(|context| {
        Box::pin(async move {
            let observed = Observed::default();
            let (mut socket, session) = h1_proxy(&context, observed.params()).await;
            let mut connect = shared_connect(&context, &session);
            poll_until_parked(&mut connect).await;
            read_connect_head(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\nunread")
                .await
                .expect("proxy response");
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            transport.write_all(b"up").await.expect("tunnel write");
            transport.flush().await.expect("tunnel flush");
            let mut received = [0; 2];
            socket
                .read_exact(&mut received)
                .await
                .expect("tunnel bytes");
            assert_eq!(&received, b"up");
            // The upload's EOM sends the client's FIN; the tunnel keeps reading.
            transport.shutdown().await.expect("upload half-close");
            assert_eq!(socket.read(&mut received).await.expect("FIN"), 0);
            socket.write_all(b"streaming").await.expect("proxy write");
            let mut downloaded = [0; 15];
            transport
                .read_exact(&mut downloaded)
                .await
                .expect("tunnel bytes");
            assert_eq!(&downloaded, b"unreadstreaming");
            assert!(socket.take_error().expect("SO_ERROR").is_none());
            transport.abort();
            assert_cancelled_read(poll_read_with(&mut transport, &noop_waker()));
            // A reset that follows the client's FIN is pending as EPIPE.
            let reset = tokio::time::timeout(WAIT_DEADLINE, async {
                loop {
                    if let Some(error) = socket.take_error().expect("SO_ERROR") {
                        return error;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the client resets the proxy connection");
            assert!(
                matches!(
                    reset.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ),
                "{reset}"
            );
            drop(transport);
        })
    });
}

#[test]
fn connect_stream_abort_resets_an_established_tunnel() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let observed = Observed::default();
            let session = proxy.connect(&context, observed.params()).await;
            let (mut stream, mut tunnel) = establish(&context, &proxy, &session).await;
            tunnel.send(b"unread");
            observed.buffered(6).await;
            stream.abort();
            assert!(!stream.can_read() && !stream.can_write());
            proxy.resets_reach(1).await;
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(proxy.peer_resets(), [(STREAM_1, CANCEL)]);
            let error = tunnel
                .read_to_end()
                .await
                .expect_err("the proxy's request read sees the reset");
            assert_eq!(error.code(), HttpErrorCode::CANCEL, "{error:?}");
            observed.detached(1).await;
            assert!(session.supports_more_transactions());
            drop((stream, fence));
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancelled_setup_after_upgrade_resets_through_the_experimental_path() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let mut resets = 0;
            for unique in [false, true] {
                for cancel in [true, false] {
                    let observed = Observed::default();
                    let session = proxy.connect(&context, observed.params()).await;
                    let token = CancellationSource::new();
                    let reservation = session.reserve_request().expect("proxy reservation");
                    let mut connect = if unique {
                        connect_unique_with_reset_on_cancel(
                            &context,
                            session.clone(),
                            reservation,
                            AUTHORITY,
                            CONNECT_TIMEOUT,
                            headers(&[("Foo", "Bar")]),
                            EGRESS_BUFFER_SIZE,
                            token.token(),
                        )
                    } else {
                        connect_with_reset_on_cancel(
                            &context,
                            &session,
                            reservation,
                            AUTHORITY,
                            CONNECT_TIMEOUT,
                            headers(&[("Foo", "Bar")]),
                            EGRESS_BUFFER_SIZE,
                            token.token(),
                        )
                    };
                    poll_until_parked(&mut connect).await;
                    let tunnel = proxy.tunnel().await;
                    tunnel.accept();
                    tunnel.send(b"x");
                    // The session parsed the 200 and marked the stream upgraded.
                    observed.buffered(1).await;
                    if cancel {
                        token.request_cancellation();
                        let error = connect.await.err().expect("the cancellation fails it");
                        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                    } else {
                        drop(connect);
                    }
                    resets += 1;
                    proxy.resets_reach(resets).await;
                    assert_eq!(
                        proxy.peer_resets().last(),
                        Some(&(STREAM_1, CANCEL)),
                        "unique {unique}, cancel {cancel}"
                    );
                    let drained = observed.drains().contains(&DrainTrigger::Local);
                    assert_eq!(drained, unique, "only a unique stream drains its session");
                    drop(tunnel);
                    drop(session);
                }
            }

            // A setup cancelled or dropped before the proxy responds resets once.
            for cancel in [true, false] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let token = CancellationSource::new();
                let mut connect = connect_with_reset_on_cancel(
                    &context,
                    &session,
                    session.reserve_request().expect("proxy reservation"),
                    AUTHORITY,
                    CONNECT_TIMEOUT,
                    headers(&[("Foo", "Bar")]),
                    EGRESS_BUFFER_SIZE,
                    token.token(),
                );
                poll_until_parked(&mut connect).await;
                let tunnel = proxy.tunnel().await;
                if cancel {
                    token.request_cancellation();
                    let error = connect.await.err().expect("the cancellation fails it");
                    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                } else {
                    drop(connect);
                }
                let fence = barrier(&context, &proxy, &session).await;
                assert_eq!(
                    proxy.peer_resets()[resets..],
                    [(STREAM_1, CANCEL)],
                    "cancel {cancel}"
                );
                resets += 1;
                drop((fence, tunnel));
                drop(session);
            }

            // Once the final head is read, the opt-in leaves the default teardown.
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = connect_with_reset_on_cancel(
                &context,
                &session,
                session.reserve_request().expect("proxy reservation"),
                AUTHORITY,
                CONNECT_TIMEOUT,
                headers(&[("Foo", "Bar")]),
                EGRESS_BUFFER_SIZE,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.accept();
            drop(connect.await.expect("CONNECT"));
            assert_eq!(
                tunnel
                    .read_to_end()
                    .await
                    .expect("END_STREAM, not RST_STREAM"),
                b""
            );
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(proxy.peer_resets().len(), resets);
            drop(fence);
            drop(session);

            // A rejection keeps the default teardown with the opt-in.
            let mut teardowns = Vec::new();
            for opt_in in [false, true] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let reservation = session.reserve_request().expect("proxy reservation");
                let mut connect = if opt_in {
                    connect_with_reset_on_cancel(
                        &context,
                        &session,
                        reservation,
                        AUTHORITY,
                        CONNECT_TIMEOUT,
                        headers(&[("Foo", "Bar")]),
                        EGRESS_BUFFER_SIZE,
                        CancellationToken::never(),
                    )
                } else {
                    HttpConnectStream::connect(
                        &context,
                        &session,
                        reservation,
                        AUTHORITY,
                        CONNECT_TIMEOUT,
                        headers(&[("Foo", "Bar")]),
                        EGRESS_BUFFER_SIZE,
                        CancellationToken::never(),
                    )
                };
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                tunnel.reject();
                let error = connect.await.err().expect("the 403 fails the connect");
                assert_eq!(
                    connect_rejection(&error).map(|rejected| rejected.status_code()),
                    Some(403)
                );
                let request = tunnel.read_to_end().await.map_err(|error| error.code());
                let fence = barrier(&context, &proxy, &session).await;
                teardowns.push((request, proxy.peer_resets().len() - resets));
                resets = proxy.peer_resets().len();
                drop(fence);
                drop(session);
            }
            assert_eq!(teardowns[0], teardowns[1], "default, then opt-in");
            proxy.shutdown().await;

            // HTTP/1.1: an opt-in connect dropped before the head resets the
            // connection although a lifecycle observer panics inside the
            // abort. The panic reaches the caller once the teardown finished,
            // or is discarded when the caller drops the future while unwinding.
            for unwinding in [false, true] {
                let (mut socket, session) = h1_proxy(
                    &context,
                    SessionParams {
                        lifecycle_observer: Some(Rc::new(PanicOnStreamReset)),
                        ..SessionParams::default()
                    },
                )
                .await;
                let mut connect = connect_unique_with_reset_on_cancel(
                    &context,
                    session.clone(),
                    session.reserve_request().expect("proxy reservation"),
                    AUTHORITY,
                    CONNECT_TIMEOUT,
                    headers(&[("Foo", "Bar")]),
                    EGRESS_BUFFER_SIZE,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                read_connect_head(&mut socket).await;
                let dropped = catch_unwind(AssertUnwindSafe(move || {
                    let _connect = connect;
                    if unwinding {
                        panic_any(CallerPanic);
                    }
                }));
                let payload = dropped.expect_err("a panic reaches the caller");
                if unwinding {
                    assert!(payload.is::<CallerPanic>(), "the caller's panic survives");
                } else {
                    assert_eq!(
                        payload.downcast_ref::<&str>(),
                        Some(&"lifecycle observer panic")
                    );
                }
                let mut rest = Vec::new();
                let reset = tokio::time::timeout(WAIT_DEADLINE, socket.read_to_end(&mut rest))
                    .await
                    .expect("the client resets the proxy connection")
                    .expect_err("a TCP reset, not a FIN");
                assert_eq!(reset.kind(), io::ErrorKind::ConnectionReset, "{reset}");
                let _ = tokio::time::timeout(WAIT_DEADLINE, session.wait_closed())
                    .await
                    .expect("the reset session closes");
            }
        })
    });
}

#[test]
fn default_teardown_paths_send_no_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = Proxy::start().await;
            let observed = Observed::default();
            let session = proxy.connect(&context, observed.params()).await;
            let mut tunnels = Vec::new();

            let (mut transport, tunnel) = establish_adapter(&context, &proxy, &session).await;
            transport.close();
            tunnels.push(tunnel);
            let (mut transport, tunnel) = establish_adapter(&context, &proxy, &session).await;
            transport.close_now();
            tunnels.push(tunnel);
            let (transport, tunnel) = establish_adapter(&context, &proxy, &session).await;
            drop(transport);
            tunnels.push(tunnel);
            let (mut stream, tunnel) = establish(&context, &proxy, &session).await;
            stream.shutdown_read();
            stream.shutdown_write();
            tunnels.push(tunnel);
            let (stream, tunnel) = establish(&context, &proxy, &session).await;
            drop(stream);
            tunnels.push(tunnel);

            // Default connect cancellation and drop after the session parsed the 200.
            for cancel in [true, false] {
                let token = CancellationSource::new();
                let mut connect = HttpConnectStream::connect(
                    &context,
                    &session,
                    session.reserve_request().expect("proxy reservation"),
                    AUTHORITY,
                    CONNECT_TIMEOUT,
                    headers(&[("Foo", "Bar")]),
                    EGRESS_BUFFER_SIZE,
                    token.token(),
                );
                poll_until_parked(&mut connect).await;
                let tunnel = proxy.tunnel().await;
                tunnel.accept();
                tunnel.send(b"x");
                tunnels.push(tunnel);
                let buffered = observed.reads.0.get() + 1;
                observed.buffered(buffered).await;
                if cancel {
                    token.request_cancellation();
                    assert!(connect.await.is_err());
                } else {
                    drop(connect);
                }
            }

            for tunnel in &mut tunnels {
                assert_eq!(
                    tunnel
                        .read_to_end()
                        .await
                        .expect("END_STREAM, not RST_STREAM"),
                    b""
                );
            }
            let fence = barrier(&context, &proxy, &session).await;
            assert_eq!(proxy.peer_resets(), [], "no default teardown resets");
            drop(fence);
            drop(session);
            proxy.shutdown().await;
        })
    });
}
