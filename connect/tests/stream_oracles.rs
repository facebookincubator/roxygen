/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for `HttpConnectStream` against scripted sessions and proxies, and
//! for `HttpConnectAsyncTransport`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
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

use roxygen_client::HttpClient;
use roxygen_client::RequestSpec;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::session::ConnectSession;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_connect::stream::RequestHeaderMap;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_server::connection::ServeTransportOptions;
use roxygen_server::connection::ServerConnection;
use roxygen_server::connection::serve_transport;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::RequestReservation;
use roxygen_session::shutdown::DrainTrigger;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::ServerName;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;

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

// Proxy fixtures.

/// The proxy side of one CONNECT stream, driven by the test.
struct ProxyTunnel {
    head: HttpMessage,
    /// Shared with `TunnelSlots`, so `TunnelProxy::shutdown` can release it.
    request: Rc<RefCell<HttpSourceHolder>>,
    response: HttpStreamSourceProducer,
}

impl ProxyTunnel {
    fn respond(&self, status: u16, fields: &[(&str, &str)], eom: bool) {
        let mut message = response(status);
        for (name, value) in fields {
            message.headers_mut().add(name.as_bytes(), value);
        }
        assert!(
            self.response.headers(message, eom),
            "the proxy response head is queued"
        );
    }

    fn accept(&self) {
        self.respond(200, &[("X-Connected-To", "1.2.3.4")], false);
    }

    fn send(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, false);
    }

    fn end(&self) {
        self.response.eom();
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
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Suspend(resume) => {
                    resume.await;
                }
                HttpBodyEventKind::Body(body) => received.extend_from_slice(&bytes_of(&body)),
                _ => {}
            }
            assert!(
                !eom || received.len() == length,
                "the tunnel ended after {} bytes",
                received.len()
            );
        }
        *self.request.borrow_mut() = request;
        received
    }
}

#[derive(Default)]
struct TunnelSlots {
    tunnels: RefCell<VecDeque<ProxyTunnel>>,
    /// Every tunnel's request holder and response producer, which
    /// `TunnelProxy::shutdown` releases.
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
            let source = HttpStreamSource::new_async(None, None, 1 << 20).map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
            })?;
            let response = source.producer();
            let request = Rc::new(RefCell::new(request));
            slots
                .held
                .borrow_mut()
                .push((Rc::clone(&request), response.clone()));
            slots.tunnels.borrow_mut().push_back(ProxyTunnel {
                head,
                request,
                response,
            });
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

/// A `roxygen_server` proxy on the test worker whose CONNECT streams the test
/// drives through `ProxyTunnel`.
struct TunnelProxy {
    protocol: CodecProtocol,
    listener: tokio::net::TcpListener,
    address: SocketAddr,
    slots: Rc<TunnelSlots>,
    resets: PeerResets,
    connections: RefCell<Vec<ServerConnection>>,
}

impl TunnelProxy {
    async fn start(protocol: CodecProtocol) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self {
            protocol,
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
                ServeTransportOptions::new(self.protocol, TransportSecurity::Plaintext);
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
                plaintext_protocol: self.protocol,
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
        let tunnel = self
            .slots
            .tunnels
            .borrow_mut()
            .pop_front()
            .expect("a queued tunnel");
        assert_eq!(tunnel.head.request_method(), Some("CONNECT"));
        assert_eq!(
            tunnel
                .head
                .headers()
                .get_single_or_empty(HttpHeaderCode::Host),
            AUTHORITY.as_bytes()
        );
        tunnel
    }

    fn pending_tunnels(&self) -> usize {
        self.slots.tunnels.borrow().len()
    }

    fn peer_resets(&self) -> Vec<HttpErrorCode> {
        self.resets
            .lock()
            .expect("resets")
            .iter()
            .map(|(_, code)| *code)
            .collect()
    }

    /// Releases every tunnel's request holder and aborts its unfinished
    /// response, also for tunnels a test still owns, so that handler sources
    /// do not hold a proxy stream open, and cancels every proxy session.
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

    /// Releases the proxy and waits for every proxy session to close.
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
    // A test that fails before `shutdown` still lets its proxy sessions end,
    // so the worker's shutdown can join them.
    fn drop(&mut self) {
        self.release();
    }
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

const H1_CONNECTED: &[u8] =
    b"HTTP/1.1 200 Connection Established\r\nX-Connected-To: 1.2.3.4\r\n\r\n";

/// Reads one HTTP/1.1 head from the raw proxy socket.
async fn read_h1_head(socket: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(
            tokio::time::timeout(WAIT_DEADLINE, socket.read_u8())
                .await
                .expect("the CONNECT head arrives")
                .expect("CONNECT head byte"),
        );
    }
    head
}

/// How the client ended the raw proxy's TCP connection.
#[derive(Debug, Eq, PartialEq)]
enum PeerClose {
    Fin(Vec<u8>),
    Reset(Vec<u8>),
}

async fn read_until_close(socket: &mut TcpStream) -> PeerClose {
    let mut received = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let read = tokio::time::timeout(WAIT_DEADLINE, socket.read(&mut buffer))
            .await
            .expect("the client ends the proxy connection");
        match read {
            Ok(0) => return PeerClose::Fin(received),
            Ok(length) => received.extend_from_slice(&buffer[..length]),
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {
                return PeerClose::Reset(received);
            }
            Err(error) => panic!("unexpected proxy read error: {error}"),
        }
    }
}

// Client-side observers.

/// Records explicit drains and detached streams of a client session.
#[derive(Default)]
struct ClientEvents {
    drains: Cell<usize>,
    detached: RefCell<Vec<StreamId>>,
}

impl LifecycleObserver for ClientEvents {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::DrainStarted(DrainTrigger::Local) => {
                self.drains.set(self.drains.get() + 1);
            }
            LifecycleEvent::TransactionDetached {
                stream: Some(stream),
            } => self.detached.borrow_mut().push(stream),
            _ => {}
        }
    }
}

/// Counts tunnel bytes that a client session buffered for a response source.
#[derive(Default)]
struct BufferedReads(Cell<i64>);

impl Http2SessionStats for BufferedReads {
    fn record_buffered_read_bytes(&self, delta: i64) {
        if delta > 0 {
            self.0.set(self.0.get() + delta);
        }
    }
}

struct Observed {
    events: Rc<ClientEvents>,
    reads: Rc<BufferedReads>,
}

impl Observed {
    fn new() -> Self {
        Self {
            events: Rc::default(),
            reads: Rc::default(),
        }
    }

    fn params(&self) -> SessionParams {
        SessionParams {
            lifecycle_observer: Some(self.events.clone()),
            session_stats: Some(self.reads.clone()),
            ..SessionParams::default()
        }
    }

    fn drains(&self) -> usize {
        self.events.drains.get()
    }

    /// Waits until the session buffered `bytes` tunnel bytes, which it parses
    /// after the response head that precedes them.
    async fn buffered(&self, bytes: i64) {
        let reads = Rc::clone(&self.reads);
        eventually("buffered tunnel bytes", move || reads.0.get() >= bytes).await;
    }
}

// Scripted `ConnectSession`.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Call {
    WorkerId,
    LocalAddress,
    PeerAddress,
    LocalEndpoint,
    PeerEndpoint,
    SendConnectRequest,
    InitiateDrain,
}

const CONSTRUCTION_READS: [Call; 5] = [
    Call::WorkerId,
    Call::LocalAddress,
    Call::PeerAddress,
    Call::LocalEndpoint,
    Call::PeerEndpoint,
];

enum SendMode {
    /// Commits through the wrapped `ClientSession`.
    Delegate,
    /// Fails immediately and keeps the egress request holder.
    Fail(HttpError),
    /// Never resolves; dropping it releases the reservation.
    Hold,
    /// Cancels the token inside the synchronous `send_connect_request` call,
    /// then returns the delegated `ClientSession` future.
    CancelInCall(Arc<CancellationSource>),
    /// Cancels the token on its first poll, then polls the delegated
    /// `ClientSession` future in that same poll.
    CancelOnFirstPoll(Arc<CancellationSource>),
    /// Keeps the egress request holder and commits a substitute request that
    /// carries the same head without any `Content-Length`, so the test reads
    /// the tunnel's egress in place of the session. `source`, when present,
    /// replaces the committed response's source. `cancel`, when present, is
    /// cancelled after the commit.
    Capture {
        source: Option<HttpSourceHolder>,
        cancel: Option<Arc<CancellationSource>>,
    },
}

impl SendMode {
    fn respond(source: HttpSourceHolder) -> Self {
        Self::Capture {
            source: Some(source),
            cancel: None,
        }
    }
}

#[derive(Default)]
struct Script {
    calls: RefCell<Vec<Call>>,
    modes: RefCell<VecDeque<SendMode>>,
    tokens: RefCell<Vec<CancellationToken>>,
    send_polls: Cell<usize>,
    committed: RefCell<Vec<StreamId>>,
    completion: RefCell<Option<IngressCompletion>>,
    egress: RefCell<Option<HttpSourceHolder>>,
    substitutes: RefCell<Vec<HttpStreamSourceProducer>>,
}

impl Script {
    fn calls(&self) -> Vec<Call> {
        self.calls.borrow().clone()
    }

    fn count(&self, call: Call) -> usize {
        self.calls
            .borrow()
            .iter()
            .filter(|&&made| made == call)
            .count()
    }

    fn push_mode(&self, mode: SendMode) {
        self.modes.borrow_mut().push_back(mode);
    }

    fn take_egress(&self) -> HttpSourceHolder {
        self.egress
            .borrow_mut()
            .take()
            .expect("the egress holder was kept")
    }

    fn completion(&self) -> IngressCompletion {
        self.completion
            .borrow()
            .clone()
            .expect("the response completion was kept")
    }
}

#[derive(Clone)]
struct ScriptedSession {
    inner: ClientSession,
    script: Rc<Script>,
}

impl ScriptedSession {
    fn new(inner: ClientSession) -> Self {
        Self {
            inner,
            script: Rc::default(),
        }
    }

    fn record(&self, call: Call) {
        self.script.calls.borrow_mut().push(call);
    }
}

/// Counts polls of the send future it wraps.
struct CountedSend {
    script: Rc<Script>,
    inner: LocalConnectFuture<Result<UniplexResponse, HttpError>>,
}

impl Future for CountedSend {
    type Output = Result<UniplexResponse, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.script.send_polls.set(self.script.send_polls.get() + 1);
        self.inner.as_mut().poll(context)
    }
}

impl ConnectSession for ScriptedSession {
    fn worker_id(&self) -> WorkerId {
        self.record(Call::WorkerId);
        ConnectSession::worker_id(&self.inner)
    }

    fn local_address(&self) -> SocketAddress {
        self.record(Call::LocalAddress);
        ConnectSession::local_address(&self.inner)
    }

    fn peer_address(&self) -> SocketAddress {
        self.record(Call::PeerAddress);
        ConnectSession::peer_address(&self.inner)
    }

    fn local_endpoint(&self) -> TransportEndpoint {
        self.record(Call::LocalEndpoint);
        ConnectSession::local_endpoint(&self.inner)
    }

    fn peer_endpoint(&self) -> TransportEndpoint {
        self.record(Call::PeerEndpoint);
        ConnectSession::peer_endpoint(&self.inner)
    }

    fn send_connect_request(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<UniplexResponse, HttpError>> {
        self.record(Call::SendConnectRequest);
        self.script.tokens.borrow_mut().push(cancellation.clone());
        let script = Rc::clone(&self.script);
        let mode = script
            .modes
            .borrow_mut()
            .pop_front()
            .unwrap_or(SendMode::Delegate);
        let inner: LocalConnectFuture<Result<UniplexResponse, HttpError>> = match mode {
            SendMode::Delegate => {
                let send = ConnectSession::send_connect_request(
                    &self.inner,
                    request,
                    reservation,
                    cancellation,
                );
                let script = Rc::clone(&script);
                Box::pin(async move {
                    let response = send.await?;
                    script.committed.borrow_mut().push(response.stream);
                    *script.completion.borrow_mut() = response.source.ingress_completion();
                    Ok(response)
                })
            }
            SendMode::Fail(error) => {
                *script.egress.borrow_mut() = Some(request);
                Box::pin(std::future::ready(Err(error)))
            }
            SendMode::Hold => Box::pin(async move {
                let _held = (request, reservation);
                std::future::pending().await
            }),
            SendMode::CancelInCall(cancel) => {
                cancel.request_cancellation();
                ConnectSession::send_connect_request(
                    &self.inner,
                    request,
                    reservation,
                    cancellation,
                )
            }
            SendMode::CancelOnFirstPoll(cancel) => {
                let mut send = ConnectSession::send_connect_request(
                    &self.inner,
                    request,
                    reservation,
                    cancellation,
                );
                let mut first = true;
                Box::pin(poll_fn(move |context| {
                    if std::mem::take(&mut first) {
                        cancel.request_cancellation();
                    }
                    send.as_mut().poll(context)
                }))
            }
            SendMode::Capture { source, cancel } => {
                let session = self.inner.clone();
                let script = Rc::clone(&script);
                let mut request = request;
                Box::pin(async move {
                    let mut head = request.read_header_event().await?.into_headers();
                    head.headers_mut().remove(HttpHeaderCode::ContentLength);
                    let substitute = HttpStreamSource::new_async(None, None, 1 << 20)
                        .expect("substitute request source");
                    let producer = substitute.producer();
                    assert!(producer.headers(head, false));
                    *script.egress.borrow_mut() = Some(request);
                    script.substitutes.borrow_mut().push(producer);
                    let mut response = ConnectSession::send_connect_request(
                        &session,
                        HttpSourceHolder::new(Box::new(substitute)),
                        reservation,
                        cancellation,
                    )
                    .await?;
                    script.committed.borrow_mut().push(response.stream);
                    if let Some(source) = source {
                        response.source = source;
                    }
                    if let Some(cancel) = cancel {
                        cancel.request_cancellation();
                    }
                    Ok(response)
                })
            }
        };
        Box::pin(CountedSend { script, inner })
    }

    fn initiate_drain(&self) -> Result<bool, HttpError> {
        self.record(Call::InitiateDrain);
        ConnectSession::initiate_drain(&self.inner)
    }
}

/// A test-driven CONNECT response source and the observations it allows.
struct ScriptedResponse {
    producer: HttpStreamSourceProducer,
    completion: IngressCompletion,
    completions: Rc<RefCell<Vec<Option<HttpError>>>>,
}

struct CompletionRecorder(Rc<RefCell<Vec<Option<HttpError>>>>);

impl HttpStreamSourceCallback for CompletionRecorder {
    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        self.0.borrow_mut().push(error);
    }
}

impl ScriptedResponse {
    fn new() -> (HttpSourceHolder, Self) {
        let completions = Rc::default();
        let source = HttpStreamSource::new_session(
            StreamId::new(1),
            CompletionRecorder(Rc::clone(&completions)),
            65_535,
            Duration::from_secs(5),
        )
        .expect("scripted response source");
        let producer = source.producer();
        let completion = HttpSource::ingress_completion(&source).expect("source completion");
        (
            HttpSourceHolder::new(Box::new(source)),
            Self {
                producer,
                completion,
                completions,
            },
        )
    }

    fn completion_codes(&self) -> Vec<Option<HttpErrorCode>> {
        self.completions
            .borrow()
            .iter()
            .map(|error| error.as_ref().map(HttpError::code))
            .collect()
    }
}

// Polling helpers.

type ConnectFuture = LocalConnectFuture<Result<HttpConnectStream, HttpError>>;

/// Polls `future` once with the current task's waker.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(Pin::new(&mut *future).poll(context))).await
}

/// Polls a connect future until its header read has parked.
async fn poll_until_parked(connect: &mut ConnectFuture) {
    assert!(
        poll_once(connect).await.is_pending(),
        "the connect waits for the CONNECT response head"
    );
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

thread_local! {
    static RELEASED_BY_WAKER_DROP: RefCell<Option<HttpSourceHolder>> =
        const { RefCell::new(None) };
}

/// Counts wakes, and its drop releases the holder parked in
/// `RELEASED_BY_WAKER_DROP`.
#[derive(Default)]
struct ReleaseOnDrop(AtomicUsize);

impl Wake for ReleaseOnDrop {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        drop(RELEASED_BY_WAKER_DROP.take());
    }
}

fn poll_read_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
    buffer: &mut [u8],
) -> Poll<io::Result<usize>> {
    let mut context = Context::from_waker(waker);
    let mut read = ReadBuf::new(buffer);
    Pin::new(transport)
        .poll_read(&mut context, &mut read)
        .map_ok(|()| read.filled().len())
}

fn poll_write_with(
    transport: &mut HttpConnectAsyncTransport,
    waker: &Waker,
    buffer: &[u8],
) -> Poll<io::Result<usize>> {
    Pin::new(transport).poll_write(&mut Context::from_waker(waker), buffer)
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

fn connect_reservation(session: &ClientSession) -> RequestReservation {
    session
        .reserve_request()
        .expect("proxy session reservation")
}

fn shared_connect<S: ConnectSession>(
    context: &WorkerContext,
    session: &S,
    reservation: RequestReservation,
    connect_headers: RequestHeaderMap,
    timeout: Duration,
    cancellation: CancellationToken,
) -> ConnectFuture {
    HttpConnectStream::connect(
        context,
        session,
        reservation,
        AUTHORITY,
        timeout,
        connect_headers,
        EGRESS_BUFFER_SIZE,
        cancellation,
    )
}

fn unique_connect<S: ConnectSession>(
    context: &WorkerContext,
    session: S,
    reservation: RequestReservation,
    connect_headers: RequestHeaderMap,
    timeout: Duration,
    cancellation: CancellationToken,
) -> ConnectFuture {
    HttpConnectStream::connect_unique(
        context,
        session,
        reservation,
        AUTHORITY,
        timeout,
        connect_headers,
        EGRESS_BUFFER_SIZE,
        cancellation,
    )
}

#[derive(Clone, Copy, Debug)]
enum Ownership {
    Shared,
    Unique,
}

fn connect_with<S: ConnectSession + Clone>(
    ownership: Ownership,
    context: &WorkerContext,
    session: &S,
    reservation: RequestReservation,
    connect_headers: RequestHeaderMap,
    timeout: Duration,
    cancellation: CancellationToken,
) -> ConnectFuture {
    match ownership {
        Ownership::Shared => shared_connect(
            context,
            session,
            reservation,
            connect_headers,
            timeout,
            cancellation,
        ),
        Ownership::Unique => unique_connect(
            context,
            session.clone(),
            reservation,
            connect_headers,
            timeout,
            cancellation,
        ),
    }
}

fn scripted_reservation(session: &ScriptedSession) -> RequestReservation {
    connect_reservation(&session.inner)
}

/// Reads tunnel body events from `source` until EOM, or returns its error.
async fn read_body_to_end(source: &mut HttpSourceHolder) -> Result<Vec<u8>, HttpError> {
    let mut received = Vec::new();
    loop {
        let event = source.read_body_event(u32::MAX).await?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Suspend(resume) => {
                resume.await;
            }
            HttpBodyEventKind::Body(body) => received.extend_from_slice(&bytes_of(&body)),
            _ => {}
        }
        if eom {
            return Ok(received);
        }
    }
}

/// Reads a kept egress holder from its CONNECT head to its end: the tunnel
/// bytes after EOM, or the error that an abort installed.
async fn read_egress_to_end(egress: &mut HttpSourceHolder) -> Result<Vec<u8>, HttpError> {
    let head = egress.read_header_event().await?;
    assert_eq!(head.headers().request_method(), Some("CONNECT"));
    if head.eom() {
        return Ok(Vec::new());
    }
    read_body_to_end(egress).await
}

// Scripted-session tests.

#[derive(Clone, Copy, Debug)]
enum UniqueFailure {
    PreCancelled,
    SendError,
    Non2xx,
    ConnectTimeout,
    CancelledAwaitingHead,
}

#[test]
fn scripted_session_sees_construction_reads_one_token_scoped_send_and_unique_drains() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;

            // Shared connect makes its session calls during the call.
            let scripted = ScriptedSession::new(session.clone());
            let caller = CancellationSource::new();
            let mut connect = shared_connect(
                &context,
                &scripted,
                scripted_reservation(&scripted),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                caller.token(),
            );
            let mut expected = CONSTRUCTION_READS.to_vec();
            expected.push(Call::SendConnectRequest);
            assert_eq!(scripted.script.calls(), expected);
            assert_eq!(scripted.script.send_polls.get(), 0);
            poll_until_parked(&mut connect).await;
            proxy.tunnel().await.accept();
            let stream = connect.await.expect("shared CONNECT");
            drop(stream);
            assert_eq!(
                scripted.script.calls(),
                expected,
                "shared connect never drains"
            );
            let tokens = scripted.script.tokens.borrow().clone();
            assert_eq!(tokens.len(), 1);
            assert!(!tokens[0].is_cancellation_requested());
            caller.request_cancellation();
            assert!(
                tokens[0].is_cancellation_requested(),
                "send_connect_request receives the caller's token"
            );

            // connect_unique makes them on its first poll.
            let scripted =
                ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
            let mut connect = unique_connect(
                &context,
                scripted.clone(),
                scripted_reservation(&scripted),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            assert!(scripted.script.calls().is_empty());
            poll_until_parked(&mut connect).await;
            assert_eq!(scripted.script.calls(), expected);
            proxy.tunnel().await.accept();
            let stream = connect.await.expect("unique CONNECT");
            assert_eq!(scripted.script.count(Call::InitiateDrain), 0);
            drop(stream);
            let mut drained = expected.clone();
            drained.push(Call::InitiateDrain);
            assert_eq!(scripted.script.calls(), drained);

            // connect_unique drains once after every failure.
            for failure in [
                UniqueFailure::PreCancelled,
                UniqueFailure::SendError,
                UniqueFailure::Non2xx,
                UniqueFailure::ConnectTimeout,
                UniqueFailure::CancelledAwaitingHead,
            ] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let cancel = CancellationSource::new();
                let mut timeout = CONNECT_TIMEOUT;
                match failure {
                    UniqueFailure::PreCancelled => {
                        cancel.request_cancellation();
                    }
                    UniqueFailure::SendError => {
                        scripted
                            .script
                            .push_mode(SendMode::Fail(HttpError::with_message(
                                HttpErrorCode::REFUSED_STREAM,
                                b"scripted send failure",
                            )));
                    }
                    UniqueFailure::Non2xx => {
                        let (source, scripted_response) = ScriptedResponse::new();
                        scripted_response.producer.headers(response(500), true);
                        scripted.script.push_mode(SendMode::respond(source));
                    }
                    UniqueFailure::ConnectTimeout | UniqueFailure::CancelledAwaitingHead => {
                        let (source, _silent) = ScriptedResponse::new();
                        scripted.script.push_mode(SendMode::respond(source));
                        if matches!(failure, UniqueFailure::ConnectTimeout) {
                            timeout = Duration::from_millis(50);
                        }
                    }
                }
                let mut connect = unique_connect(
                    &context,
                    scripted.clone(),
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    timeout,
                    cancel.token(),
                );
                if matches!(failure, UniqueFailure::CancelledAwaitingHead) {
                    poll_until_parked(&mut connect).await;
                    cancel.request_cancellation();
                }
                let error = connect.await.err().expect("the connect fails");
                let expected: (HttpErrorCode, &[u8]) = match failure {
                    UniqueFailure::PreCancelled => (HttpErrorCode::CORO_CANCELLED, b"Cancelled"),
                    UniqueFailure::SendError => {
                        (HttpErrorCode::REFUSED_STREAM, b"scripted send failure")
                    }
                    UniqueFailure::Non2xx => (
                        HttpErrorCode::CONNECT_ERROR,
                        b"CONNECT to example.com:443 failed status=500",
                    ),
                    UniqueFailure::ConnectTimeout => {
                        (HttpErrorCode::READ_TIMEOUT, b"Read timeout after 50")
                    }
                    UniqueFailure::CancelledAwaitingHead => {
                        (HttpErrorCode::CORO_CANCELLED, b"Read cancelled")
                    }
                };
                assert_eq!((error.code(), error.message()), expected, "{failure:?}");
                let sends = usize::from(!matches!(failure, UniqueFailure::PreCancelled));
                assert_eq!(
                    scripted.script.count(Call::SendConnectRequest),
                    sends,
                    "{failure:?}: a pre-cancelled token calls no send"
                );
                assert_eq!(
                    scripted.script.count(Call::InitiateDrain),
                    1,
                    "{failure:?} drains the unique session once"
                );
                assert_eq!(
                    scripted.script.calls().last(),
                    Some(&Call::InitiateDrain),
                    "{failure:?} drains last"
                );
            }
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn scripted_send_header_and_timeout_errors_propagate_without_abort() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                // A send error propagates unchanged; Drop then ends the egress with EOM.
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session.clone());
                scripted
                    .script
                    .push_mode(SendMode::Fail(HttpError::with_message(
                        HttpErrorCode::REFUSED_STREAM,
                        b"scripted send failure",
                    )));
                let error = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                )
                .await
                .err()
                .expect("the send error fails the connect");
                assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM, "{ownership:?}");
                assert_eq!(error.message(), b"scripted send failure");
                let mut egress = scripted.script.take_egress();
                assert_eq!(
                    read_egress_to_end(&mut egress)
                        .await
                        .expect("the egress ends with EOM, not an abort"),
                    b""
                );

                // A header-read error propagates unchanged without an abort.
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session.clone());
                let (source, scripted_response) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                scripted_response
                    .producer
                    .abort(HttpErrorCode::PROTOCOL_ERROR, b"peer failure");
                let error = connect
                    .await
                    .err()
                    .expect("the head read error fails the connect");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR);
                assert_eq!(
                    error.message(),
                    b"HTTP source aborted err=PROTOCOL_ERROR, details=peer failure"
                );
                let mut egress = scripted.script.take_egress();
                assert_eq!(
                    read_body_to_end(&mut egress)
                        .await
                        .expect("the egress ends with EOM, not an abort"),
                    b""
                );

                // The CONNECT timeout bounds a silent head wait.
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session.clone());
                let (source, _silent) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let error = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    Duration::from_millis(50),
                    CancellationToken::never(),
                )
                .await
                .err()
                .expect("a silent head wait times out");
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
                assert_eq!(error.message(), b"Read timeout after 50");
                let mut egress = scripted.script.take_egress();
                assert_eq!(
                    read_body_to_end(&mut egress)
                        .await
                        .expect("the egress ends with EOM, not an abort"),
                    b""
                );
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn final_head_queued_before_its_read_starts_wins_over_cancellation() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let cancel = Arc::new(CancellationSource::new());
                let (source, scripted_response) = ScriptedResponse::new();
                scripted_response.producer.headers(response(107), false);
                let mut connected = response(200);
                connected.headers_mut().set(b"X-Connected-To", b"1.2.3.4");
                scripted_response.producer.headers(connected, false);
                scripted.script.push_mode(SendMode::Capture {
                    source: Some(source),
                    cancel: Some(Arc::clone(&cancel)),
                });
                let stream = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("{ownership:?}: a queued final head wins over cancellation: {error}")
                });
                assert!(cancel.is_cancellation_requested());
                assert_eq!(stream.peer_address().host(), b"1.2.3.4");
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancellation_before_a_parked_head_read_resumes_fails_the_connect_without_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let session = proxy.connect(&context, observed.params()).await;
                let scripted = ScriptedSession::new(session);
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                tunnel.accept();
                tunnel.send(b"x");
                observed.buffered(1).await;
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert_eq!(
                    error.http_message().and_then(HttpMessage::response_status),
                    Some(200),
                    "the queued 200 rides on the error"
                );
                assert_eq!(
                    tunnel
                        .read_to_end()
                        .await
                        .expect("END_STREAM, not RST_STREAM"),
                    b""
                );
                assert!(proxy.peer_resets().is_empty());
                let drains = match ownership {
                    Ownership::Shared => 0,
                    Ownership::Unique => 1,
                };
                assert_eq!(
                    scripted.script.count(Call::InitiateDrain),
                    drains,
                    "{ownership:?}"
                );
            }
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let (session, mut socket) = proxy.connect(&context, observed.params()).await;
                let scripted = ScriptedSession::new(session);
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                let head = read_h1_head(&mut socket).await;
                assert!(head.starts_with(b"CONNECT example.com:443 HTTP/1.1\r\n"));
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\n\r\nx")
                    .await
                    .expect("proxy response");
                observed.buffered(1).await;
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert_eq!(
                    error.http_message().and_then(HttpMessage::response_status),
                    Some(200)
                );
                assert_eq!(
                    read_until_close(&mut socket).await,
                    PeerClose::Fin(Vec::new()),
                    "{ownership:?}: the write loop ends with a FIN, not a reset"
                );
                let drains = match ownership {
                    Ownership::Shared => 0,
                    Ownership::Unique => 1,
                };
                assert_eq!(
                    scripted.script.count(Call::InitiateDrain),
                    drains,
                    "{ownership:?}"
                );
            }
        })
    });
}

#[test]
fn cancellation_before_a_parked_head_read_resumes_resets_a_non_200_eom_head() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let session = proxy.connect(&context, observed.params()).await;
                let scripted = ScriptedSession::new(session.clone());
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                tunnel.respond(500, &[], true);
                scripted
                    .script
                    .completion()
                    .wait()
                    .await
                    .expect("the session admits the 500 with END_STREAM");
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert!(error.http_message().is_none());
                let read = tunnel.read_to_end().await;
                assert!(read.is_err(), "no END_STREAM reaches the proxy: {read:?}");
                eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
                assert_eq!(proxy.peer_resets(), [HttpErrorCode::CANCEL]);
                proxy.resets.lock().expect("resets").clear();
                match ownership {
                    Ownership::Shared => {
                        assert_eq!(observed.drains(), 0);
                        let mut connect = shared_connect(
                            &context,
                            &session,
                            connect_reservation(&session),
                            headers(&[("Foo", "Bar")]),
                            CONNECT_TIMEOUT,
                            CancellationToken::never(),
                        );
                        poll_until_parked(&mut connect).await;
                        proxy.tunnel().await.accept();
                        connect.await.expect("the shared session stays usable");
                    }
                    Ownership::Unique => assert_eq!(observed.drains(), 1),
                }
            }
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let (session, mut socket) = proxy.connect(&context, observed.params()).await;
                let scripted = ScriptedSession::new(session.clone());
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                read_h1_head(&mut socket).await;
                socket
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .expect("proxy response");
                scripted
                    .script
                    .completion()
                    .wait()
                    .await
                    .expect("the session admits the 500 with Content-Length: 0");
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert!(error.http_message().is_none());
                assert_eq!(
                    read_until_close(&mut socket).await,
                    PeerClose::Reset(Vec::new()),
                    "{ownership:?}: the proxy connection is reset"
                );
                let drains = match ownership {
                    Ownership::Shared => 0,
                    Ownership::Unique => 1,
                };
                assert_eq!(scripted.script.count(Call::InitiateDrain), drains);
            }
        })
    });
}

#[test]
fn head_read_error_wins_over_cancellation() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                // A peer error committed while the head read is parked.
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let (source, scripted_response) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                scripted_response
                    .producer
                    .abort(HttpErrorCode::PROTOCOL_ERROR, b"peer reset");
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the peer error fails the connect");
                assert_eq!(error.code(), HttpErrorCode::PROTOCOL_ERROR, "{ownership:?}");
                assert_eq!(
                    error.message(),
                    b"HTTP source aborted err=PROTOCOL_ERROR, details=peer reset"
                );

                // A CONNECT timeout whose expiry a poll has recorded.
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let (source, _silent) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    Duration::from_millis(50),
                    cancel.token(),
                );
                tokio::time::pause();
                poll_until_parked(&mut connect).await;
                tokio::time::advance(Duration::from_millis(60)).await;
                assert!(
                    poll_once(&mut connect).await.is_pending(),
                    "the poll that observes the expiry records it"
                );
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the recorded timeout fails the connect");
                tokio::time::resume();
                assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT, "{ownership:?}");
                assert_eq!(error.message(), b"Read timeout after 50");
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn recorded_connect_timeout_then_eom_head_then_cancellation_fails_with_coro_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let (source, scripted_response) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    Duration::from_millis(50),
                    cancel.token(),
                );
                tokio::time::pause();
                poll_until_parked(&mut connect).await;
                tokio::time::advance(Duration::from_millis(60)).await;
                assert!(poll_once(&mut connect).await.is_pending());
                assert!(scripted_response.producer.headers(response(200), true));
                assert!(matches!(
                    scripted_response.completion.snapshot(),
                    Some(Ok(_))
                ));
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                tokio::time::resume();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert!(error.http_message().is_none());
                assert_eq!(
                    scripted_response.completion_codes(),
                    [Some(HttpErrorCode::READ_TIMEOUT)]
                );
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn source_error_after_response_eom_then_cancellation_fails_with_coro_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let (source, scripted_response) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                assert!(scripted_response.producer.headers(response(200), true));
                scripted_response.producer.body(queue(b"x"), 0, false);
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert!(error.http_message().is_none());
                assert_eq!(
                    scripted_response.completion_codes(),
                    [Some(HttpErrorCode::INVALID_STATE_TRANSITION)]
                );
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn unobserved_connect_timeout_then_cancellation_fails_with_coro_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let (source, scripted_response) = ScriptedResponse::new();
                scripted.script.push_mode(SendMode::respond(source));
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    Duration::from_millis(50),
                    cancel.token(),
                );
                tokio::time::pause();
                poll_until_parked(&mut connect).await;
                tokio::time::advance(Duration::from_millis(60)).await;
                cancel.request_cancellation();
                let error = connect
                    .await
                    .err()
                    .expect("the cancellation fails the connect");
                tokio::time::resume();
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"Read cancelled");
                assert_eq!(
                    scripted_response.completion_codes(),
                    [Some(HttpErrorCode::CORO_CANCELLED)]
                );
            }
            proxy.shutdown().await;
        })
    });
}

fn single_stream_params(observed: &Observed) -> SessionParams {
    SessionParams {
        max_concurrent_outgoing_streams: Some(1),
        ..observed.params()
    }
}

#[test]
fn token_cancelled_after_connect_check_fails_in_session_send_without_commit() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let session = proxy
                    .connect(&context, single_stream_params(&observed))
                    .await;
                let scripted = ScriptedSession::new(session.clone());
                let cancel = Arc::new(CancellationSource::new());
                scripted
                    .script
                    .push_mode(SendMode::CancelOnFirstPoll(Arc::clone(&cancel)));
                let error = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                )
                .await
                .err()
                .expect("the cancellation inside the send fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                assert_eq!(error.message(), b"request header read cancelled");
                assert!(scripted.script.committed.borrow().is_empty());
                match ownership {
                    Ownership::Shared => {
                        assert_eq!(observed.drains(), 0);
                        let mut connect = shared_connect(
                            &context,
                            &scripted,
                            scripted_reservation(&scripted),
                            headers(&[("Foo", "Bar")]),
                            CONNECT_TIMEOUT,
                            CancellationToken::never(),
                        );
                        poll_until_parked(&mut connect).await;
                        proxy.tunnel().await.accept();
                        connect
                            .await
                            .expect("the released reservation admits a new CONNECT");
                        assert_eq!(
                            *scripted.script.committed.borrow(),
                            [StreamId::new(1)],
                            "the cancelled CONNECT created no stream"
                        );
                    }
                    Ownership::Unique => {
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 1);
                        assert_eq!(observed.drains(), 1);
                    }
                }
                assert_eq!(proxy.pending_tunnels(), 0);
            }

            // A cancellation inside the synchronous send_connect_request call
            // reaches shared connect's first-poll check, while connect_unique
            // polls the send future in the same poll as the call.
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session);
                let cancel = Arc::new(CancellationSource::new());
                scripted
                    .script
                    .push_mode(SendMode::CancelInCall(Arc::clone(&cancel)));
                let error = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                )
                .await
                .err()
                .expect("the cancellation inside the call fails the connect");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{ownership:?}");
                let (message, polls): (&[u8], usize) = match ownership {
                    Ownership::Shared => (b"Cancelled", 0),
                    Ownership::Unique => (b"request header read cancelled", 1),
                };
                assert_eq!(error.message(), message, "{ownership:?}");
                assert_eq!(scripted.script.send_polls.get(), polls, "{ownership:?}");
                assert!(scripted.script.committed.borrow().is_empty());
            }
            assert_eq!(proxy.pending_tunnels(), 0);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn shared_connect_token_cancelled_before_first_poll_fails_cancelled_without_polling_send() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let session = proxy
                .connect(&context, single_stream_params(&observed))
                .await;
            let scripted = ScriptedSession::new(session.clone());
            let cancel = CancellationSource::new();
            let connect = shared_connect(
                &context,
                &scripted,
                scripted_reservation(&scripted),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                cancel.token(),
            );
            assert_eq!(scripted.script.count(Call::SendConnectRequest), 1);
            cancel.request_cancellation();
            let error = connect
                .await
                .err()
                .expect("the cancelled token fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.message(), b"Cancelled");
            assert_eq!(
                scripted.script.send_polls.get(),
                0,
                "the send future is never polled"
            );
            assert!(scripted.script.committed.borrow().is_empty());
            session
                .reserve_request()
                .expect("dropping the send future released its reservation");
            assert_eq!(observed.drains(), 0);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn egress_buffer_above_signed_window_range_fails_before_send() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let oversized = u32::try_from(i32::MAX).expect("positive") + 1;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let scripted =
                    ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
                let reservation = scripted_reservation(&scripted);
                let connect = match ownership {
                    Ownership::Shared => HttpConnectStream::connect(
                        &context,
                        &scripted,
                        reservation,
                        AUTHORITY,
                        CONNECT_TIMEOUT,
                        headers(&[("Foo", "Bar")]),
                        oversized,
                        CancellationToken::never(),
                    ),
                    Ownership::Unique => HttpConnectStream::connect_unique(
                        &context,
                        scripted.clone(),
                        reservation,
                        AUTHORITY,
                        CONNECT_TIMEOUT,
                        headers(&[("Foo", "Bar")]),
                        oversized,
                        CancellationToken::never(),
                    ),
                };
                let error = connect
                    .await
                    .err()
                    .expect("the egress source cannot be created");
                assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR, "{ownership:?}");
                assert_eq!(
                    error.message(),
                    b"stream flow-control capacity exceeds the signed window range"
                );
                let mut expected = CONSTRUCTION_READS.to_vec();
                if matches!(ownership, Ownership::Unique) {
                    expected.push(Call::InitiateDrain);
                }
                assert_eq!(scripted.script.calls(), expected, "{ownership:?}");
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn dropping_connect_future_before_commit_releases_reservation_and_drains_only_a_built_unique_stream()
 {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;

            // An unpolled connect_unique does not call the session. Another clone
            // keeps the session open.
            let observed = Observed::new();
            let session = proxy.connect(&context, observed.params()).await;
            let scripted = ScriptedSession::new(session.clone());
            let connect = unique_connect(
                &context,
                scripted.clone(),
                scripted_reservation(&scripted),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            drop(connect);
            drop(scripted.inner);
            assert!(scripted.script.calls().is_empty());
            assert_eq!(observed.drains(), 0);
            assert!(session.supports_more_transactions());

            // When the dropped future held the last clone, that clone's Drop drains.
            let observed = Observed::new();
            let scripted = ScriptedSession::new(proxy.connect(&context, observed.params()).await);
            let script = Rc::clone(&scripted.script);
            let reservation = scripted_reservation(&scripted);
            let connect = unique_connect(
                &context,
                scripted,
                reservation,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            drop(connect);
            assert!(script.calls().is_empty());
            assert_eq!(
                observed.drains(),
                1,
                "the last ClientSession drains on drop"
            );

            // Dropping either future while send_connect_request is pending.
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let observed = Observed::new();
                let session = proxy
                    .connect(&context, single_stream_params(&observed))
                    .await;
                let scripted = ScriptedSession::new(session.clone());
                scripted.script.push_mode(SendMode::Hold);
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                assert!(poll_once(&mut connect).await.is_pending());
                assert_eq!(scripted.script.send_polls.get(), 1);
                drop(connect);
                match ownership {
                    Ownership::Shared => {
                        assert_eq!(observed.drains(), 0);
                        session
                            .reserve_request()
                            .expect("the dropped send released its reservation");
                    }
                    Ownership::Unique => {
                        assert_eq!(scripted.script.calls().last(), Some(&Call::InitiateDrain));
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 1);
                    }
                }
                assert_eq!(
                    proxy.pending_tunnels(),
                    0,
                    "{ownership:?}: nothing was committed"
                );
            }
            proxy.shutdown().await;
        })
    });
}

/// Connects a shared tunnel through a scripted session whose response source
/// the test drives, without committing a stream.
async fn scripted_head_connect(
    context: &WorkerContext,
    scripted: &ScriptedSession,
    fields: &[(&str, &[u8])],
) -> HttpConnectStream {
    let (source, scripted_response) = ScriptedResponse::new();
    let mut head = response(200);
    for (name, value) in fields {
        head.headers_mut().add(name.as_bytes(), value);
    }
    assert!(scripted_response.producer.headers(head, false));
    scripted.script.push_mode(SendMode::respond(source));
    shared_connect(
        context,
        scripted,
        scripted_reservation(scripted),
        headers(&[("Foo", "Bar")]),
        CONNECT_TIMEOUT,
        CancellationToken::never(),
    )
    .await
    .expect("the scripted 200 connects")
}

#[test]
fn user_session_id_is_the_single_valued_fwdproxy_request_id() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let scripted =
                ScriptedSession::new(proxy.connect(&context, SessionParams::default()).await);
            let cases: [(&[(&str, &[u8])], &[u8]); 4] = [
                (&[("X-FB-Fwdproxy-Request-ID", b"test-id")], b"test-id"),
                (&[("X-FB-Fwdproxy-Request-ID", b"id-\xff")], b"id-\xff"),
                (
                    &[
                        ("X-FB-Fwdproxy-Request-ID", b"first"),
                        ("X-FB-Fwdproxy-Request-ID", b"second"),
                    ],
                    b"",
                ),
                (&[], b""),
            ];
            for (fields, expected) in cases {
                let stream = scripted_head_connect(&context, &scripted, fields).await;
                assert_eq!(stream.extract_user_session_id(), expected, "{fields:?}");
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn ipv6_hostname_and_repeated_x_connected_to() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let proxy_peer = session.peer_address().clone();
            let scripted = ScriptedSession::new(session);
            let connected = |host: &str| SocketAddress::new(host, 443);
            // Parsing follows `getaddrinfo(AI_NUMERICHOST)`, which accepts
            // every `inet_aton` IPv4 form. Printing follows `inet_ntop`.
            let cases: [(&[(&str, &[u8])], SocketAddress); 24] = [
                (&[("X-Connected-To", b"::1")], connected("::1")),
                (&[("X-Connected-To", b"0:0:0:0:0:0:0:1")], connected("::1")),
                // Of two equally long zero runs, the first is compressed.
                (
                    &[("X-Connected-To", b"1:0:0:2:0:0:3:4")],
                    connected("1::2:0:0:3:4"),
                ),
                (&[("X-Connected-To", b"1:0:0:0:0:0:0:0")], connected("1::")),
                (&[("X-Connected-To", b"0:0:0:0:0:0:0:0")], connected("::")),
                // Five leading zero words end in dotted IPv4 only before `ffff`.
                (
                    &[("X-Connected-To", b"0:0:0:0:0:1:102:304")],
                    connected("::1:102:304"),
                ),
                (
                    &[("X-Connected-To", b"::0102:0304")],
                    connected("::1.2.3.4"),
                ),
                (
                    &[("X-Connected-To", b"::FFFF:1.2.3.4")],
                    connected("::ffff:1.2.3.4"),
                ),
                (&[("X-Connected-To", b"127.1")], connected("127.0.0.1")),
                (&[("X-Connected-To", b"0x7f.0.0.1")], connected("127.0.0.1")),
                (&[("X-Connected-To", b"0X7F000001")], connected("127.0.0.1")),
                (&[("X-Connected-To", b"010.0.0.1")], connected("8.0.0.1")),
                (
                    &[("X-Connected-To", b"1.2.65535")],
                    connected("1.2.255.255"),
                ),
                (&[("X-Connected-To", b"1.2.3.04")], connected("1.2.3.4")),
                (&[("X-Connected-To", b"localhost")], proxy_peer.clone()),
                (&[("X-Connected-To", b"not an address")], proxy_peer.clone()),
                (&[("X-Connected-To", b"08.1.2.3")], proxy_peer.clone()),
                (&[("X-Connected-To", b"1.256.3.4")], proxy_peer.clone()),
                (&[("X-Connected-To", b"1.2.3.4.")], proxy_peer.clone()),
                (&[("X-Connected-To", b"0x")], proxy_peer.clone()),
                (&[("X-Connected-To", b"::01.2.3.4")], proxy_peer.clone()),
                // A zone index cannot be carried by `SocketAddress`.
                (&[("X-Connected-To", b"fe80::1%2")], proxy_peer.clone()),
                (
                    &[
                        ("X-Connected-To", b"1.2.3.4"),
                        ("X-Connected-To", b"5.6.7.8"),
                    ],
                    proxy_peer.clone(),
                ),
                (&[], proxy_peer.clone()),
            ];
            for (fields, expected) in cases {
                let stream = scripted_head_connect(&context, &scripted, fields).await;
                assert_eq!(stream.peer_address(), &expected, "{fields:?}");
                assert_eq!(stream.local_address(), scripted.inner.local_address());
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn shared_keeps_session_unique_drains() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let session = proxy.connect(&context, observed.params()).await;
            let scripted = ScriptedSession::new(session.clone());
            for _ in 0..2 {
                let mut connect = shared_connect(
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                proxy.tunnel().await.accept();
                drop(connect.await.expect("shared CONNECT"));
            }
            assert_eq!(scripted.script.count(Call::InitiateDrain), 0);
            assert_eq!(observed.drains(), 0);
            assert!(session.supports_more_transactions());

            let mut connect = unique_connect(
                &context,
                scripted.clone(),
                scripted_reservation(&scripted),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            proxy.tunnel().await.accept();
            let stream = connect.await.expect("unique CONNECT");
            assert_eq!(observed.drains(), 0);
            drop(stream);
            assert_eq!(scripted.script.count(Call::InitiateDrain), 1);
            assert_eq!(observed.drains(), 1);
            assert!(!session.supports_more_transactions());
            proxy.shutdown().await;
        })
    });
}

async fn h1_accept(socket: &mut TcpStream) -> Vec<u8> {
    let head = read_h1_head(socket).await;
    socket
        .write_all(H1_CONNECTED)
        .await
        .expect("proxy response");
    head
}

/// Echoes `ping` as `pong` across an established tunnel.
async fn h2c_echo(transport: &mut HttpConnectAsyncTransport, tunnel: &mut ProxyTunnel) {
    transport.write_all(b"ping").await.expect("tunnel write");
    assert_eq!(tunnel.read_exact(4).await, b"ping");
    tunnel.send(b"pong");
    let mut reply = [0; 4];
    transport.read_exact(&mut reply).await.expect("tunnel read");
    assert_eq!(&reply, b"pong");
}

async fn h1_echo(transport: &mut HttpConnectAsyncTransport, socket: &mut TcpStream) {
    transport.write_all(b"ping").await.expect("tunnel write");
    let mut request = [0; 4];
    socket.read_exact(&mut request).await.expect("proxy read");
    assert_eq!(&request, b"ping");
    socket.write_all(b"pong").await.expect("proxy write");
    let mut reply = [0; 4];
    transport.read_exact(&mut reply).await.expect("tunnel read");
    assert_eq!(&reply, b"pong");
}

#[test]
fn shared_connect_commits_after_caller_drops_last_session_clone() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let session = proxy.connect(&context, observed.params()).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            drop(session);
            assert_eq!(
                observed.drains(),
                0,
                "the send future holds a session clone"
            );
            poll_until_parked(&mut connect).await;
            assert_eq!(
                observed.drains(),
                1,
                "the send future's clone drains after commit"
            );
            let mut tunnel = proxy.tunnel().await;
            tunnel.accept();
            let stream = connect.await.expect("the committed CONNECT connects");
            let mut transport = HttpConnectAsyncTransport::new(&context, stream);
            h2c_echo(&mut transport, &mut tunnel).await;
            drop(transport);
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let observed = Observed::new();
            let (session, mut socket) = proxy.connect(&context, observed.params()).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            drop(session);
            assert_eq!(observed.drains(), 0);
            poll_until_parked(&mut connect).await;
            assert_eq!(observed.drains(), 1);
            let head = h1_accept(&mut socket).await.to_ascii_lowercase();
            assert!(
                head.windows(19)
                    .any(|line| line == b"connection: close\r\n"),
                "the drained H1 session's CONNECT head: {}",
                String::from_utf8_lossy(&head)
            );
            assert!(!head.windows(10).any(|word| word == b"keep-alive"));
            let stream = connect.await.expect("the committed CONNECT connects");
            let mut transport = HttpConnectAsyncTransport::new(&context, stream);
            h1_echo(&mut transport, &mut socket).await;
        })
    });
}

#[test]
fn dropping_connect_future_awaiting_head_resets_stream_then_unique_drains() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session.clone());
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                drop(connect);
                assert!(tunnel.read_to_end().await.is_err(), "no END_STREAM");
                eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
                assert_eq!(
                    proxy.peer_resets(),
                    [HttpErrorCode::CANCEL],
                    "{ownership:?}"
                );
                proxy.resets.lock().expect("resets").clear();
                match ownership {
                    Ownership::Shared => {
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 0);
                        let mut connect = shared_connect(
                            &context,
                            &session,
                            connect_reservation(&session),
                            headers(&[("Foo", "Bar")]),
                            CONNECT_TIMEOUT,
                            CancellationToken::never(),
                        );
                        poll_until_parked(&mut connect).await;
                        proxy.tunnel().await.accept();
                        connect.await.expect("the shared session stays usable");
                    }
                    Ownership::Unique => {
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 1);
                    }
                }
            }
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session);
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                read_h1_head(&mut socket).await;
                drop(connect);
                assert_eq!(
                    read_until_close(&mut socket).await,
                    PeerClose::Reset(Vec::new()),
                    "{ownership:?}"
                );
                let drains = match ownership {
                    Ownership::Shared => 0,
                    Ownership::Unique => 1,
                };
                assert_eq!(scripted.script.count(Call::InitiateDrain), drains);
            }
        })
    });
}

#[test]
fn non_200_2xx_h1_h2() {
    run_on_worker(|context| {
        Box::pin(async move {
            // HTTP/2 marks only a 200 upgraded, so abandoning a 201 tunnel resets it.
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.respond(201, &[("X-Connected-To", "1.2.3.4")], false);
            let stream = connect.await.expect("any 2xx connects");
            assert_eq!(stream.peer_address().host(), b"1.2.3.4");
            assert!(stream.can_read() && stream.can_write());
            drop(stream);
            assert!(tunnel.read_to_end().await.is_err());
            eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
            assert_eq!(proxy.peer_resets(), [HttpErrorCode::CANCEL]);
            proxy.shutdown().await;

            // The HTTP/1.1 codec tunnels after any 2xx, but the session marks only
            // a 200 upgraded, so abandoning a 201 tunnel resets the connection.
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 201 Created\r\nX-Connected-To: 1.2.3.4\r\n\r\n")
                .await
                .expect("proxy response");
            let stream = connect.await.expect("any 2xx connects");
            assert_eq!(stream.peer_address().host(), b"1.2.3.4");
            drop(stream);
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new())
            );
        })
    });
}

#[test]
fn connect_101_response_is_final_and_rejected_h1_h2() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.respond(101, &[], true);
            let error = connect.await.err().expect("a final 101 is rejected");
            assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR);
            assert_eq!(
                error.message(),
                b"CONNECT to example.com:443 failed status=101"
            );
            let read = tunnel.read_to_end().await.expect_err("the egress abort");
            assert_eq!(read.code(), HttpErrorCode::CONNECT_ERROR);
            eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
            assert_eq!(proxy.peer_resets(), [HttpErrorCode::CONNECT_ERROR]);
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
                .await
                .expect("proxy response");
            let error = connect
                .await
                .err()
                .expect("a 101 without an upgrade request fails");
            // The codec rejects a 101 to a CONNECT that did not request an
            // upgrade, so the header read fails before the connect checks the
            // status.
            assert_eq!(error.code(), HttpErrorCode::HEADER_PARSE_ERROR);
            assert_eq!(
                error.message(),
                b"HTTP source aborted err=HEADER_PARSE_ERROR, details=connection error"
            );
            assert_eq!(
                error.http_message().and_then(HttpMessage::response_status),
                Some(101)
            );
        })
    });
}

#[test]
fn h2c_non_2xx_head_without_end_stream_resets_with_cancel() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let scripted = ScriptedSession::new(session.clone());
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &scripted,
                    scripted_reservation(&scripted),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                tunnel.respond(
                    503,
                    &[("Proxy-Status", "fwdproxy; error=destination_unavailable")],
                    false,
                );
                tunnel.send(b"fwdproxy error body");
                let error = connect.await.err().expect("a 503 is rejected");
                assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR, "{ownership:?}");
                assert_eq!(
                    error.message(),
                    b"CONNECT to example.com:443 failed status=503"
                );
                assert!(tunnel.read_to_end().await.is_err(), "no END_STREAM");
                eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
                assert_eq!(
                    proxy.peer_resets(),
                    [HttpErrorCode::CANCEL],
                    "{ownership:?}"
                );
                proxy.resets.lock().expect("resets").clear();
                match ownership {
                    Ownership::Shared => {
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 0);
                        let mut connect = shared_connect(
                            &context,
                            &session,
                            connect_reservation(&session),
                            headers(&[("Foo", "Bar")]),
                            CONNECT_TIMEOUT,
                            CancellationToken::never(),
                        );
                        poll_until_parked(&mut connect).await;
                        proxy.tunnel().await.accept();
                        connect.await.expect("the shared session stays usable");
                    }
                    Ownership::Unique => {
                        assert_eq!(scripted.script.count(Call::InitiateDrain), 1);
                    }
                }
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_non_2xx_resets_proxy_connection() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("proxy response");
            let error = connect.await.err().expect("a 500 is rejected");
            assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR);
            assert_eq!(
                error.message(),
                b"CONNECT to example.com:443 failed status=500"
            );
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new())
            );
        })
    });
}

#[test]
fn abandon_upgraded_stream_no_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let session = proxy.connect(&context, observed.params()).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.accept();
            tunnel.send(b"unread tunnel bytes");
            let stream = connect.await.expect("CONNECT");
            observed.buffered(19).await;
            drop(stream);
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            tunnel.send(b"more");
            tunnel.end();
            let events = Rc::clone(&observed.events);
            eventually("the abandoned stream to detach", move || {
                !events.detached.borrow().is_empty()
            })
            .await;
            assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");
            assert!(session.supports_more_transactions());
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            socket.write_all(b"unread").await.expect("proxy write");
            let stream = connect.await.expect("CONNECT");
            drop(stream);
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Fin(Vec::new())
            );
        })
    });
}

#[test]
fn caller_host_header_duplicates() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Host", "other.example"), ("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let head = String::from_utf8(h1_accept(&mut socket).await).expect("ASCII head");
            let hosts: Vec<&str> = head
                .split("\r\n")
                .filter(|line| line.to_ascii_lowercase().starts_with("host:"))
                .collect();
            assert_eq!(
                hosts,
                ["Host: example.com:443", "Host: other.example"],
                "{head}"
            );
            connect.await.expect("CONNECT");

            let proxy = RawH2cProxy::start().await;
            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = shared_connect(
                &context,
                &session,
                connect_reservation(&session),
                headers(&[("Host", "other.example"), ("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let names = hpack_static_names(&h2c_request_header_block(&mut socket).await);
            assert_eq!(
                names
                    .iter()
                    .filter(|name| matches!(name, Some(2 | 3)))
                    .count(),
                1,
                "one :method: {names:?}"
            );
            // With two `Host` values there is not a single authority, so the H2
            // encoder omits `:authority`.
            assert!(
                !names
                    .iter()
                    .any(|name| matches!(name, Some(1 | 4..=7 | 38))),
                "no :authority, :path, :scheme, or host field: {names:?}"
            );
        })
    });
}

/// A scripted h2c proxy: the test reads the client's frames from its raw TCP
/// socket.
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

    async fn connect(&self, context: &WorkerContext) -> (ClientSession, TcpStream) {
        let connect = HttpConnector::connect(
            context,
            socket_address(self.address),
            Duration::ZERO,
            ConnectionParams {
                plaintext_protocol: CodecProtocol::Http2,
                ..ConnectionParams::default()
            },
            SessionParams::default(),
            CancellationToken::never(),
        );
        let (accepted, session) = tokio::join!(self.listener.accept(), connect);
        let (socket, _) = accepted.expect("proxy accept");
        (session.expect("client connects to the proxy"), socket)
    }
}

/// Sends an empty SETTINGS frame and returns the header block fragments of
/// the client's first HEADERS frame and its CONTINUATION frames.
async fn h2c_request_header_block(socket: &mut TcpStream) -> Vec<u8> {
    const HEADERS: u8 = 0x1;
    const CONTINUATION: u8 = 0x9;
    const END_HEADERS: u8 = 0x4;
    const PADDED: u8 = 0x8;
    const PRIORITY: u8 = 0x20;
    let mut preface = [0; 24];
    tokio::time::timeout(WAIT_DEADLINE, socket.read_exact(&mut preface))
        .await
        .expect("the client preface arrives")
        .expect("client preface");
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    socket
        .write_all(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0])
        .await
        .expect("proxy SETTINGS");
    let mut block = Vec::new();
    let mut in_headers = false;
    loop {
        let mut frame = [0; 9];
        tokio::time::timeout(WAIT_DEADLINE, socket.read_exact(&mut frame))
            .await
            .expect("the client's HEADERS arrive")
            .expect("frame header");
        let length =
            usize::from(frame[0]) << 16 | usize::from(frame[1]) << 8 | usize::from(frame[2]);
        let (kind, flags) = (frame[3], frame[4]);
        let mut payload = vec![0; length];
        socket
            .read_exact(&mut payload)
            .await
            .expect("frame payload");
        let fragment = match kind {
            HEADERS if !in_headers => {
                let padded = flags & PADDED != 0;
                let padding = if padded { usize::from(payload[0]) } else { 0 };
                let start = usize::from(padded) + if flags & PRIORITY != 0 { 5 } else { 0 };
                &payload[start..length - padding]
            }
            CONTINUATION if in_headers => &payload[..],
            _ => continue,
        };
        in_headers = true;
        block.extend_from_slice(fragment);
        if flags & END_HEADERS != 0 {
            return block;
        }
    }
}

/// The RFC 7541 static-table name index of each field in a connection's
/// first header block, or `None` for a literal name.
fn hpack_static_names(block: &[u8]) -> Vec<Option<u64>> {
    fn integer(block: &[u8], at: &mut usize, prefix_bits: u32) -> u64 {
        let mask = (1 << prefix_bits) - 1;
        let mut value = u64::from(block[*at]) & mask;
        *at += 1;
        if value == mask {
            let mut shift = 0;
            loop {
                let byte = block[*at];
                *at += 1;
                value += u64::from(byte & 0x7f) << shift;
                shift += 7;
                if byte & 0x80 == 0 {
                    break;
                }
            }
        }
        value
    }
    fn skip_string(block: &[u8], at: &mut usize) {
        *at += usize::try_from(integer(block, at, 7)).expect("string length");
    }
    const STATIC_ENTRIES: u64 = 61;
    // The names of the entries this block inserted, newest first.
    let mut inserted: VecDeque<Option<u64>> = VecDeque::new();
    let resolve = |index: u64, inserted: &VecDeque<Option<u64>>| {
        if index <= STATIC_ENTRIES {
            Some(index)
        } else {
            inserted[usize::try_from(index - STATIC_ENTRIES - 1).expect("dynamic index")]
        }
    };
    let mut names = Vec::new();
    let mut at = 0;
    while at < block.len() {
        let first = block[at];
        if first & 0x80 != 0 {
            names.push(resolve(integer(block, &mut at, 7), &inserted));
            continue;
        }
        if first & 0xe0 == 0x20 {
            integer(block, &mut at, 5);
            continue;
        }
        let indexing = first & 0x40 != 0;
        let index = integer(block, &mut at, if indexing { 6 } else { 4 });
        let name = if index == 0 {
            skip_string(block, &mut at);
            None
        } else {
            resolve(index, &inserted)
        };
        skip_string(block, &mut at);
        if indexing {
            inserted.push_front(name);
        }
        names.push(name);
    }
    names
}

/// A shared h2c tunnel that the proxy accepted.
async fn accepted_h2c_stream(
    context: &WorkerContext,
    proxy: &TunnelProxy,
    session: &ClientSession,
) -> (HttpConnectStream, ProxyTunnel) {
    let mut connect = shared_connect(
        context,
        session,
        connect_reservation(session),
        headers(&[("Foo", "Bar")]),
        CONNECT_TIMEOUT,
        CancellationToken::never(),
    );
    poll_until_parked(&mut connect).await;
    let tunnel = proxy.tunnel().await;
    tunnel.accept();
    (connect.await.expect("CONNECT"), tunnel)
}

#[test]
fn stream_shutdown_close_and_egress_error_follow_the_tunnel() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;

            let (mut stream, mut tunnel) = accepted_h2c_stream(&context, &proxy, &session).await;
            stream.shutdown_read();
            assert!(
                !stream.can_read(),
                "shutdown_read stops the response source"
            );
            assert!(stream.can_write(), "shutdown_read leaves the egress open");
            stream.shutdown_write();
            assert!(!stream.can_write(), "shutdown_write ends the egress");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            assert!(
                stream.egress_error().is_none(),
                "the egress completed cleanly"
            );
            drop(stream);

            let (mut stream, mut tunnel) = accepted_h2c_stream(&context, &proxy, &session).await;
            stream.close();
            assert!(!stream.can_read() && !stream.can_write());
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            assert!(
                stream.egress_error().is_none(),
                "the egress completed cleanly"
            );
            drop(stream);
            assert!(proxy.peer_resets().is_empty(), "no RST_STREAM");

            let (stream, tunnel) = accepted_h2c_stream(&context, &proxy, &session).await;
            assert!(stream.egress_error().is_none());
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            eventually("the peer reset to fail the egress", || {
                stream.egress_error().is_some()
            })
            .await;
            // The session stops reading the egress source with the peer's
            // RST_STREAM code.
            let error = stream.egress_error().expect("egress error");
            assert_eq!(error.code(), HttpErrorCode::CANCEL);
            assert_eq!(error.message(), b"Abandoned reading");
            assert!(!stream.can_write(), "a failed egress takes no more bytes");
            drop(stream);
            drop(tunnel);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn cancel_connect_token_after_success_keeps_tunnel() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &session,
                    connect_reservation(&session),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                let mut tunnel = proxy.tunnel().await;
                tunnel.accept();
                let stream = connect.await.expect("CONNECT");
                cancel.request_cancellation();
                let mut transport = HttpConnectAsyncTransport::new(&context, stream);
                h2c_echo(&mut transport, &mut tunnel).await;
                transport.shutdown().await.expect("tunnel EOM");
                assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
                assert!(proxy.peer_resets().is_empty(), "{ownership:?}");
            }
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
                let cancel = CancellationSource::new();
                let mut connect = connect_with(
                    ownership,
                    &context,
                    &session,
                    connect_reservation(&session),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    cancel.token(),
                );
                poll_until_parked(&mut connect).await;
                h1_accept(&mut socket).await;
                let stream = connect.await.expect("CONNECT");
                cancel.request_cancellation();
                let mut transport = HttpConnectAsyncTransport::new(&context, stream);
                h1_echo(&mut transport, &mut socket).await;
                transport.shutdown().await.expect("tunnel EOM");
                assert_eq!(
                    read_until_close(&mut socket).await,
                    PeerClose::Fin(Vec::new()),
                    "{ownership:?}"
                );
            }
        })
    });
}

#[test]
fn cancel_while_awaiting_connect_head_fails_and_resets_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            fn cancel_from_another_thread(cancel: &Arc<CancellationSource>) {
                let remote = Arc::clone(cancel);
                std::thread::spawn(move || {
                    remote.request_cancellation();
                })
                .join()
                .expect("cancelling thread");
            }

            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let cancel = Arc::new(CancellationSource::new());
            let task = tokio::task::spawn_local(unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                Duration::from_secs(600),
                cancel.token(),
            ));
            let mut tunnel = proxy.tunnel().await;
            cancel_from_another_thread(&cancel);
            let error = tokio::time::timeout(WAIT_DEADLINE, task)
                .await
                .expect("the cancellation alone wakes the head wait")
                .expect("the connect task does not panic")
                .err()
                .expect("the cancellation fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.message(), b"Read cancelled");
            assert!(error.http_message().is_none());
            assert!(tunnel.read_to_end().await.is_err(), "no END_STREAM");
            eventually("the proxy's RST_STREAM", || !proxy.peer_resets().is_empty()).await;
            assert_eq!(proxy.peer_resets(), [HttpErrorCode::CANCEL]);
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let cancel = Arc::new(CancellationSource::new());
            let task = tokio::task::spawn_local(unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                Duration::from_secs(600),
                cancel.token(),
            ));
            read_h1_head(&mut socket).await;
            cancel_from_another_thread(&cancel);
            let error = tokio::time::timeout(WAIT_DEADLINE, task)
                .await
                .expect("the cancellation alone wakes the head wait")
                .expect("the connect task does not panic")
                .err()
                .expect("the cancellation fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.message(), b"Read cancelled");
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new())
            );
        })
    });
}

#[test]
fn h1_connect_timeout_expiry_fails_with_read_timeout_and_resets_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                Duration::from_millis(100),
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            let error = connect.await.err().expect("a silent proxy times out");
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert_eq!(error.message(), b"Read timeout after 100");
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new())
            );
        })
    });
}

#[test]
fn h1_async_idle_read_outlives_connect_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                Duration::from_millis(100),
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let stream = connect.await.expect("CONNECT");
            let mut transport = HttpConnectAsyncTransport::new(&context, stream);
            let mut late = [0; 4];
            let (read, ()) = tokio::join!(transport.read_exact(&mut late), async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                socket.write_all(b"late").await.expect("proxy write");
            });
            read.expect("an idle tunnel read outlives the CONNECT timeout");
            assert_eq!(&late, b"late");
        })
    });
}

#[test]
fn unique_tunnel_used_and_dropped_after_session_terminates_is_safe() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.accept();
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            h2c_echo(&mut transport, &mut tunnel).await;
            session.cancel();
            let mut buffer = [0; 8];
            let read = tokio::time::timeout(WAIT_DEADLINE, transport.read(&mut buffer))
                .await
                .expect("a read after termination does not hang");
            assert!(
                read.is_err(),
                "the terminated session fails the tunnel read: {read:?}"
            );
            let write = tokio::time::timeout(WAIT_DEADLINE, transport.write_all(b"after"))
                .await
                .expect("a write after termination does not hang");
            drop(write);
            drop(transport);
            let _ = tokio::time::timeout(WAIT_DEADLINE, session.wait_closed())
                .await
                .expect("the cancelled session finishes once its tunnel is dropped");
            drop(session);
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            h1_echo(&mut transport, &mut socket).await;
            drop(socket);
            let mut buffer = [0; 8];
            let read = tokio::time::timeout(WAIT_DEADLINE, transport.read(&mut buffer))
                .await
                .expect("a read after the proxy closed does not hang");
            assert!(matches!(read, Ok(0)) || read.is_err(), "{read:?}");
            let write = tokio::time::timeout(WAIT_DEADLINE, transport.write_all(b"after")).await;
            assert!(write.is_ok(), "a write after termination does not hang");
            drop(transport);
            let _ = tokio::time::timeout(WAIT_DEADLINE, session.wait_closed())
                .await
                .expect("the session ends once its tunnel is dropped");
        })
    });
}

/// Fills the proxy's stream window and then queues `parked` bytes that the
/// session cannot send.
async fn stall_h2c_egress(transport: &mut HttpConnectAsyncTransport, parked: usize) {
    transport
        .write_all(&vec![b'w'; PROXY_WINDOW])
        .await
        .expect("window-filling write");
    transport
        .flush()
        .await
        .expect("the session takes the window's bytes");
    transport
        .write_all(&vec![b'p'; parked])
        .await
        .expect("parked write");
}

/// Writes until a write's completion stays deferred because the proxy stopped
/// reading its socket and the session cannot write.
async fn stall_h1_egress(transport: &mut HttpConnectAsyncTransport) {
    let chunk = vec![b's'; 64 * 1024];
    for _ in 0..4096 {
        let write = tokio::time::timeout(Duration::from_millis(200), async {
            transport.write_all(&chunk).await?;
            transport.flush().await
        })
        .await;
        // A write that stays pending, or fails because the session already
        // timed out its transport write, ends the stall.
        if !matches!(write, Ok(Ok(()))) {
            return;
        }
    }
    panic!("the proxy socket never pushed back");
}

#[test]
fn dropped_unique_tunnel_with_stalled_egress_is_released_by_write_timeout_or_session_cancel() {
    run_on_worker(|context| {
        Box::pin(async move {
            // HTTP/2: the write timeout resets a stream blocked on its closed window.
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let session = proxy
                .connect(
                    &context,
                    SessionParams {
                        write_timeout: Some(Duration::from_millis(200)),
                        ..observed.params()
                    },
                )
                .await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let tunnel = proxy.tunnel().await;
            tunnel.accept();
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            stall_h2c_egress(&mut transport, 1024).await;
            drop(transport);
            eventually("the stalled stream's reset", || {
                !proxy.peer_resets().is_empty()
            })
            .await;
            assert_eq!(proxy.peer_resets(), [HttpErrorCode::FLOW_CONTROL_ERROR]);
            tokio::time::timeout(WAIT_DEADLINE, session.wait_closed())
                .await
                .expect("the drained session closes once the stream is reset")
                .expect("the session closes cleanly");
            drop(tunnel);
            proxy.shutdown().await;

            // HTTP/2: cancelling a retained session clone releases it sooner.
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.accept();
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            stall_h2c_egress(&mut transport, 1024).await;
            drop(transport);
            session.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), session.wait_closed())
                .await
                .expect("the cancelled session closes before the default write timeout");
            assert!(
                tunnel.read_to_end().await.is_err(),
                "the tunnel did not end cleanly"
            );
            proxy.shutdown().await;

            // HTTP/1.1: a transport write that exceeds the write timeout fails the session.
            for cancel in [false, true] {
                let proxy = RawH1Proxy::start().await;
                let write_timeout = if cancel {
                    None
                } else {
                    Some(Duration::from_millis(200))
                };
                let (session, mut socket) = proxy
                    .connect(
                        &context,
                        SessionParams {
                            write_timeout,
                            ..SessionParams::default()
                        },
                    )
                    .await;
                let mut connect = unique_connect(
                    &context,
                    session.clone(),
                    connect_reservation(&session),
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                h1_accept(&mut socket).await;
                let mut transport =
                    HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
                stall_h1_egress(&mut transport).await;
                drop(transport);
                if cancel {
                    session.cancel();
                }
                let _ = tokio::time::timeout(Duration::from_secs(2), session.wait_closed())
                    .await
                    .expect("the stalled proxy connection is released");
                drop(socket);
            }
        })
    });
}

// tokio-rustls over the tunnel.

const ORIGIN_BODY_LENGTH: usize = 70_000;

fn tls_configs() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let generated =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("certificate");
    let certificate = CertificateDer::from(generated.cert.der().to_vec());
    let private_key = PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec())
        .expect("private key");
    let mut server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], private_key)
        .expect("server configuration");
    server.alpn_protocols = vec![b"http/1.1".to_vec()];
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("test root");
    let mut client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    (Arc::new(server), Arc::new(client))
}

fn origin_body() -> Vec<u8> {
    (0..ORIGIN_BODY_LENGTH)
        .map(|index| u8::try_from(index % 251).expect("byte"))
        .collect()
}

/// A TLS origin that responds to one GET with `origin_body()`.
async fn serve_origin(listener: tokio::net::TcpListener, config: Arc<rustls::ServerConfig>) {
    let (socket, _) = listener.accept().await.expect("origin accept");
    let mut tls = TlsAcceptor::from(config)
        .accept(socket)
        .await
        .expect("origin TLS handshake");
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(tls.read_u8().await.expect("GET head byte"));
    }
    assert!(head.starts_with(b"GET /resource HTTP/1.1\r\n"), "{head:?}");
    let body = origin_body();
    tls.write_all(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )
    .await
    .expect("origin head");
    tls.write_all(&body).await.expect("origin body");
    tls.flush().await.expect("origin flush");
    let mut rest = Vec::new();
    let _ = tls.read_to_end(&mut rest).await;
}

/// Runs TLS over the tunnel, attaches an HTTP/1.1 session to it, and GETs the
/// origin's resource.
async fn tunneled_get(
    context: &WorkerContext,
    transport: HttpConnectAsyncTransport,
    proxy_address: SocketAddr,
    config: Arc<rustls::ClientConfig>,
) -> Vec<u8> {
    let local = transport.local_address().clone();
    let tls = TlsConnector::from(config)
        .connect(
            ServerName::try_from("localhost").expect("server name"),
            transport,
        )
        .await
        .expect("TLS over the tunnel");
    let established = adopt_local(
        context,
        EstablishedIo::new(
            tls,
            TransportEndpoint::Socket(local),
            TransportEndpoint::Socket(socket_address(proxy_address)),
            None,
        ),
        ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("tunnel limits"),
    )
    .expect("adopted tunnel");
    let session = ClientSession::attach_transport(
        context,
        established,
        ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Tls),
    )
    .expect("tunneled session");
    let url = Url::parse(b"https://localhost/resource", true, UrlMode::StrictCompat);
    let response = HttpClient::request(&session, None, RequestSpec::get(url))
        .await
        .expect("tunneled GET");
    let mut head = response.headers;
    assert_eq!(head.status_code().expect("status"), 200);
    bytes_of(&response.body)
}

/// Copies one tunnel's bytes between a proxy CONNECT stream and the origin.
async fn splice_tunnel(tunnel: ProxyTunnel, origin: TcpStream) {
    let ProxyTunnel {
        request, response, ..
    } = tunnel;
    let mut request = std::mem::take(&mut *request.borrow_mut());
    let (mut origin_read, mut origin_write) = origin.into_split();
    let upstream = async move {
        loop {
            let event = request
                .read_body_event(u32::MAX)
                .await
                .expect("tunnel bytes");
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Suspend(resume) => {
                    resume.await;
                }
                HttpBodyEventKind::Body(body) => {
                    origin_write
                        .write_all(&bytes_of(&body))
                        .await
                        .expect("origin write");
                }
                _ => {}
            }
            if eom {
                let _ = origin_write.shutdown().await;
                return;
            }
        }
    };
    let downstream = async move {
        let mut buffer = vec![0; 16 * 1024];
        loop {
            let read = origin_read.read(&mut buffer).await.expect("origin read");
            if read == 0 {
                response.eom();
                return;
            }
            response.body(queue(&buffer[..read]), 0, false);
        }
    };
    tokio::join!(upstream, downstream);
}

#[test]
fn h1_proxy_tokio_rustls_get() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (server_config, client_config) = tls_configs();
            let origin = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("origin listener");
            let origin_address = origin.local_addr().expect("origin address");
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                RequestHeaderMap::new(),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let stream = connect.await.expect("CONNECT");
            let transport = HttpConnectAsyncTransport::new(&context, stream);
            let splice = async {
                let mut upstream = TcpStream::connect(origin_address).await.expect("origin");
                let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
            };
            let (body, (), ()) = tokio::join!(
                async {
                    let body =
                        tunneled_get(&context, transport, proxy.address, client_config).await;
                    drop(session);
                    body
                },
                serve_origin(origin, server_config),
                splice,
            );
            assert_eq!(body.len(), ORIGIN_BODY_LENGTH);
            assert_eq!(body, origin_body());
        })
    });
}

#[test]
fn h2c_proxy_tokio_rustls_get() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (server_config, client_config) = tls_configs();
            let origin = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("origin listener");
            let origin_address = origin.local_addr().expect("origin address");
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                RequestHeaderMap::new(),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let tunnel = proxy.tunnel().await;
            tunnel.accept();
            let stream = connect.await.expect("CONNECT");
            let transport = HttpConnectAsyncTransport::new(&context, stream);
            let splice = async {
                let upstream = TcpStream::connect(origin_address).await.expect("origin");
                splice_tunnel(tunnel, upstream).await;
            };
            let (body, (), ()) = tokio::join!(
                async {
                    let body =
                        tunneled_get(&context, transport, proxy.address, client_config).await;
                    drop(session);
                    body
                },
                serve_origin(origin, server_config),
                splice,
            );
            assert_eq!(body.len(), ORIGIN_BODY_LENGTH);
            assert_eq!(body, origin_body());
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_half_close_sends_fin_after_close_notify_and_keeps_reading() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (server_config, client_config) = tls_configs();
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                RequestHeaderMap::new(),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let stream = connect.await.expect("CONNECT");
            let transport = HttpConnectAsyncTransport::new(&context, stream);
            let peer = async move {
                let mut tls = TlsAcceptor::from(server_config)
                    .accept(socket)
                    .await
                    .expect("TLS through the tunnel");
                let mut received = Vec::new();
                tls.read_to_end(&mut received)
                    .await
                    .expect("close_notify arrives before the FIN");
                assert_eq!(received, b"hello");
                let mut after = [0; 16];
                let tcp = tls.get_mut().0;
                let fin = tokio::time::timeout(WAIT_DEADLINE, tcp.read(&mut after))
                    .await
                    .expect("the client ends its write side");
                assert_eq!(
                    fin.expect("a FIN, not a reset"),
                    0,
                    "bytes followed close_notify"
                );
                tls.write_all(b"remaining")
                    .await
                    .expect("peer write after the FIN");
                tls.shutdown().await.expect("peer close_notify and FIN");
            };
            let client = async move {
                let mut tls = TlsConnector::from(client_config)
                    .connect(
                        ServerName::try_from("localhost").expect("server name"),
                        transport,
                    )
                    .await
                    .expect("TLS over the tunnel");
                tls.write_all(b"hello").await.expect("tunnel write");
                tls.shutdown()
                    .await
                    .expect("close_notify and the tunnel's EOM");
                let mut rest = Vec::new();
                tls.read_to_end(&mut rest)
                    .await
                    .expect("the tunnel keeps reading after its half-close");
                assert_eq!(rest, b"remaining");
            };
            tokio::join!(peer, client);
            drop(session);
        })
    });
}

// `HttpConnectAsyncTransport` tests.

impl ProxyTunnel {
    fn finish(&self, bytes: &[u8]) {
        self.response.body(queue(bytes), 0, true);
    }
}

/// An h2c tunnel whose proxy side the test drives.
async fn h2c_adapter(
    context: &WorkerContext,
    proxy: &TunnelProxy,
    session: SessionParams,
) -> (ClientSession, HttpConnectAsyncTransport, ProxyTunnel) {
    let session = proxy.connect(context, session).await;
    let mut connect = unique_connect(
        context,
        session.clone(),
        connect_reservation(&session),
        headers(&[("Foo", "Bar")]),
        CONNECT_TIMEOUT,
        CancellationToken::never(),
    );
    poll_until_parked(&mut connect).await;
    let tunnel = proxy.tunnel().await;
    tunnel.accept();
    let stream = connect.await.expect("CONNECT");
    (
        session,
        HttpConnectAsyncTransport::new(context, stream),
        tunnel,
    )
}

/// An h2c tunnel whose egress holder the test reads in place of the session,
/// so that an egress read is not active and the egress source's bytes are
/// processed only when the test reads them.
async fn captured_adapter(
    context: &WorkerContext,
    proxy: &TunnelProxy,
    egress_buffer_size: u32,
    connect_headers: RequestHeaderMap,
) -> (
    ScriptedSession,
    HttpConnectAsyncTransport,
    ProxyTunnel,
    HttpSourceHolder,
) {
    let scripted = ScriptedSession::new(proxy.connect(context, SessionParams::default()).await);
    scripted.script.push_mode(SendMode::Capture {
        source: None,
        cancel: None,
    });
    let mut connect = HttpConnectStream::connect_unique(
        context,
        scripted.clone(),
        scripted_reservation(&scripted),
        AUTHORITY,
        CONNECT_TIMEOUT,
        connect_headers,
        egress_buffer_size,
        CancellationToken::never(),
    );
    poll_until_parked(&mut connect).await;
    let tunnel = proxy.tunnel().await;
    tunnel.accept();
    let stream = connect.await.expect("CONNECT");
    let egress = scripted.script.take_egress();
    (
        scripted,
        HttpConnectAsyncTransport::new(context, stream),
        tunnel,
        egress,
    )
}

/// Reads the next event that carries tunnel bytes or EOM from an egress holder.
async fn next_egress(
    egress: &mut HttpSourceHolder,
    maximum: u32,
) -> Result<(BufQueue, bool), HttpError> {
    loop {
        let event = egress.read_body_event(maximum).await?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Suspend(_) => {}
            HttpBodyEventKind::Body(body) => return Ok((body, eom)),
            _ if eom => return Ok((BufQueue::new(0), true)),
            _ => {}
        }
    }
}

async fn next_egress_bytes(egress: &mut HttpSourceHolder, maximum: u32) -> (Vec<u8>, bool) {
    let (body, eom) = next_egress(egress, maximum).await.expect("egress event");
    (bytes_of(&body), eom)
}

fn kind_and_message<T: std::fmt::Debug>(result: Poll<io::Result<T>>) -> (io::ErrorKind, String) {
    let Poll::Ready(Err(error)) = result else {
        panic!("expected an I/O error, got {result:?}");
    };
    (error.kind(), error.to_string())
}

fn not_connected() -> (io::ErrorKind, String) {
    (
        io::ErrorKind::NotConnected,
        "write on a closed or failed transport".to_owned(),
    )
}

fn socket_error() -> (io::ErrorKind, String) {
    (io::ErrorKind::ConnectionAborted, "Socket error".to_owned())
}

#[test]
fn async_small_readbuf_retains_remainder_before_repeated_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            let reader = Arc::new(WakeCounter::default());
            let mut large = [0; 64];
            assert!(poll_read_with(&mut transport, &reader.waker(), &mut large).is_pending());
            tunnel.finish(b"hello world");
            eventually("the stored read's wake", || reader.wakes() > 0).await;
            let mut small = [0; 4];
            let mut received = Vec::new();
            for expected in [4, 4, 3] {
                let Poll::Ready(Ok(read)) =
                    poll_read_with(&mut transport, &reader.waker(), &mut small)
                else {
                    panic!("the remainder is ready");
                };
                assert_eq!(read, expected);
                received.extend_from_slice(&small[..read]);
            }
            assert_eq!(received, b"hello world");
            for _ in 0..3 {
                assert!(
                    matches!(
                        poll_read_with(&mut transport, &reader.waker(), &mut small),
                        Poll::Ready(Ok(0))
                    ),
                    "EOF repeats after the last remainder byte"
                );
            }
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_zero_capacity_read_returns_at_once_and_keeps_stored_read_and_remainder() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            let reader = Arc::new(WakeCounter::default());
            let waker = reader.waker();
            let mut empty = [0; 0];
            assert!(matches!(
                poll_read_with(&mut transport, &waker, &mut empty),
                Poll::Ready(Ok(0))
            ));
            let mut buffer = [0; 64];
            assert!(poll_read_with(&mut transport, &waker, &mut buffer).is_pending());
            assert!(
                matches!(
                    poll_read_with(&mut transport, &waker, &mut empty),
                    Poll::Ready(Ok(0))
                ),
                "a zero-capacity read leaves the stored read in place"
            );
            tunnel.send(b"abcdef");
            eventually("the stored read's wake", || reader.wakes() > 0).await;
            let mut small = [0; 2];
            assert!(matches!(
                poll_read_with(&mut transport, &waker, &mut small),
                Poll::Ready(Ok(2))
            ));
            assert_eq!(&small, b"ab");
            assert!(
                matches!(
                    poll_read_with(&mut transport, &waker, &mut empty),
                    Poll::Ready(Ok(0))
                ),
                "a zero-capacity read leaves the remainder in place"
            );
            let Poll::Ready(Ok(read)) = poll_read_with(&mut transport, &waker, &mut buffer) else {
                panic!("the remainder is ready");
            };
            assert_eq!(&buffer[..read], b"cdef");
            tunnel.send(b"gh");
            let mut later = [0; 2];
            transport
                .read_exact(&mut later)
                .await
                .expect("no zero-maximum read poisoned the source");
            assert_eq!(&later, b"gh");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_empty_write_returns_zero_under_write_gating() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            let waker = writer.waker();
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b""),
                Poll::Ready(Ok(0))
            ));
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(
                poll_write_with(&mut transport, &waker, b"").is_pending(),
                "an empty write waits for the deferred completion"
            );
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), false)
            );
            assert!(writer.wakes() > 0);
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b""),
                Poll::Ready(Ok(0))
            ));
            assert!(matches!(
                poll_shutdown_with(&mut transport, &waker),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &waker, b"")),
                not_connected()
            );
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (Vec::new(), true)
            );

            let (_scripted, mut transport, _tunnel, _egress) =
                captured_adapter(&context, &proxy, 0, headers(&[("Foo", "Bar")])).await;
            assert!(
                matches!(
                    poll_write_with(&mut transport, &waker, b""),
                    Poll::Ready(Ok(0))
                ),
                "an empty write completes at once with an egress buffer of 0"
            );
            assert!(matches!(
                poll_flush_with(&mut transport, &waker),
                Poll::Ready(Ok(()))
            ));
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_write_backpressure_flush_after_bytes_processed() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 8, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            let waker = writer.waker();
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(
                matches!(poll_flush_with(&mut transport, &waker), Poll::Ready(Ok(()))),
                "a write that leaves the window open flushes at once"
            );
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b"efgh"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_flush_with(&mut transport, &waker).is_pending());
            assert_eq!(
                next_egress_bytes(&mut egress, 4).await,
                (b"abcd".to_vec(), false)
            );
            assert_eq!(
                writer.wakes(),
                0,
                "the window-closing write is not yet processed"
            );
            assert!(poll_flush_with(&mut transport, &waker).is_pending());
            assert_eq!(
                next_egress_bytes(&mut egress, 4).await,
                (b"efgh".to_vec(), false)
            );
            assert_eq!(writer.wakes(), 1);
            assert!(matches!(
                poll_flush_with(&mut transport, &waker),
                Poll::Ready(Ok(()))
            ));
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_write_larger_than_egress_buffer_is_accepted_whole_then_next_write_waits() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            let waker = writer.waker();
            let large = b"0123456789abcdef";
            assert!(matches!(
                poll_write_with(&mut transport, &waker, large),
                Poll::Ready(Ok(16))
            ));
            assert!(poll_write_with(&mut transport, &waker, b"x").is_pending());
            assert!(poll_flush_with(&mut transport, &waker).is_pending());
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (large.to_vec(), false)
            );
            assert!(writer.wakes() > 0);
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b"x"),
                Poll::Ready(Ok(1))
            ));
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"x".to_vec(), false)
            );
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_zero_egress_buffer_accepts_each_write_whole_and_defers_completion() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 0, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            let waker = writer.waker();
            for chunk in [b"ab", b"cd"] {
                assert!(matches!(
                    poll_write_with(&mut transport, &waker, chunk),
                    Poll::Ready(Ok(2))
                ));
                assert!(poll_write_with(&mut transport, &waker, b"next").is_pending());
                assert!(poll_flush_with(&mut transport, &waker).is_pending());
                let wakes = writer.wakes();
                assert_eq!(
                    next_egress_bytes(&mut egress, u32::MAX).await,
                    (chunk.to_vec(), false)
                );
                assert_eq!(writer.wakes(), wakes + 1);
                assert!(matches!(
                    poll_flush_with(&mut transport, &waker),
                    Poll::Ready(Ok(()))
                ));
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_vectored_write_is_one_ordered_submission_with_one_completion() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            assert!(transport.is_write_vectored());
            let writer = Arc::new(WakeCounter::default());
            let waker = writer.waker();
            let slices = [
                io::IoSlice::new(b"ab"),
                io::IoSlice::new(b""),
                io::IoSlice::new(b"cd"),
                io::IoSlice::new(b"ef"),
            ];
            let written = Pin::new(&mut transport)
                .poll_write_vectored(&mut Context::from_waker(&waker), &slices);
            assert!(matches!(written, Poll::Ready(Ok(6))));
            assert!(poll_flush_with(&mut transport, &waker).is_pending());
            let (body, eom) = next_egress(&mut egress, u32::MAX)
                .await
                .expect("egress body");
            assert!(!eom);
            assert_eq!(bytes_of(&body), b"abcdef");
            assert_eq!(body.segment_count(), 3, "one segment per non-empty slice");
            assert_eq!(writer.wakes(), 1, "one completion for the submission");
            assert!(matches!(
                poll_flush_with(&mut transport, &waker),
                Poll::Ready(Ok(()))
            ));
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_read_from_new_waker_replaces_stored_waker_without_second_body_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            let first = Arc::new(WakeCounter::default());
            let second = Arc::new(WakeCounter::default());
            let mut buffer = [0; 16];
            assert!(poll_read_with(&mut transport, &first.waker(), &mut buffer).is_pending());
            assert!(poll_read_with(&mut transport, &second.waker(), &mut buffer).is_pending());
            tunnel.send(b"data");
            eventually("the replaced waker's wake", || second.wakes() > 0).await;
            assert_eq!(first.wakes(), 0, "the replaced waker is not woken");
            let Poll::Ready(Ok(read)) =
                poll_read_with(&mut transport, &second.waker(), &mut buffer)
            else {
                panic!("the stored read completed");
            };
            assert_eq!(&buffer[..read], b"data");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_split_halves_on_separate_tasks_wake_independently() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, transport, tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let (mut reader, mut writer) = tokio::io::split(transport);
            let read = tokio::task::spawn_local(async move {
                let mut buffer = [0; 4];
                reader.read_exact(&mut buffer).await.expect("tunnel read");
                (reader, buffer)
            });
            let write = tokio::task::spawn_local(async move {
                writer.write_all(b"abcd").await.expect("tunnel write");
                writer.flush().await.expect("tunnel flush");
                writer
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!read.is_finished() && !write.is_finished());
            tunnel.send(b"data");
            let (reader, buffer) = tokio::time::timeout(WAIT_DEADLINE, read)
                .await
                .expect("the reader wakes on tunnel bytes")
                .expect("reader task");
            assert_eq!(&buffer, b"data");
            assert!(!write.is_finished(), "the writer waits for its own event");
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), false)
            );
            let writer = tokio::time::timeout(WAIT_DEADLINE, write)
                .await
                .expect("the writer wakes once its bytes are processed")
                .expect("writer task");
            drop(reader.unsplit(writer));
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_ingress_error_fails_pending_and_later_reads() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, observed.params()).await;
            let reader = Arc::new(WakeCounter::default());
            let mut buffer = [0; 16];
            assert!(poll_read_with(&mut transport, &reader.waker(), &mut buffer).is_pending());
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            eventually("the failed read's wake", || reader.wakes() > 0).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(
                observed.events.detached.borrow().is_empty(),
                "the unpolled stored read keeps the response source from completing"
            );
            let mut failures = Vec::new();
            for _ in 0..2 {
                let Poll::Ready(Err(error)) =
                    poll_read_with(&mut transport, &reader.waker(), &mut buffer)
                else {
                    panic!("the ingress error fails the read");
                };
                assert_eq!(error.kind(), io::ErrorKind::Other);
                let inner = error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<HttpError>())
                    .expect("the ingress HttpError is the inner error");
                assert_eq!(error.to_string(), inner.to_string());
                failures.push((inner.code(), inner.message().to_vec()));
            }
            assert_eq!(
                failures[0], failures[1],
                "later reads return the same error"
            );
            assert_eq!(failures[0].0, HttpErrorCode::CANCEL);
            let events = Rc::clone(&observed.events);
            eventually("the CONNECT stream to detach", move || {
                !events.detached.borrow().is_empty()
            })
            .await;
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[derive(Clone, Copy, Debug)]
enum ReadShutdown {
    Close,
    CloseNow,
    ShutdownReadThenShutdown,
}

#[test]
fn async_shutdown_read_and_close_wake_a_parked_reader_with_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            for case in [
                ReadShutdown::Close,
                ReadShutdown::CloseNow,
                ReadShutdown::ShutdownReadThenShutdown,
            ] {
                let observed = Observed::new();
                let (session, mut transport, mut tunnel) =
                    h2c_adapter(&context, &proxy, observed.params()).await;
                let reader = Arc::new(WakeCounter::default());
                let mut buffer = [0; 16];
                assert!(poll_read_with(&mut transport, &reader.waker(), &mut buffer).is_pending());
                match case {
                    ReadShutdown::Close => transport.close(),
                    ReadShutdown::CloseNow => transport.close_now(),
                    ReadShutdown::ShutdownReadThenShutdown => {
                        transport.shutdown_read();
                        assert!(matches!(
                            poll_shutdown_with(&mut transport, &reader.waker()),
                            Poll::Ready(Ok(()))
                        ));
                    }
                }
                assert!(reader.wakes() > 0, "{case:?} wakes the parked reader");
                assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
                let events = Rc::clone(&observed.events);
                eventually("the CONNECT stream to detach", move || {
                    !events.detached.borrow().is_empty()
                })
                .await;
                for _ in 0..2 {
                    assert!(
                        matches!(
                            poll_read_with(&mut transport, &reader.waker(), &mut buffer),
                            Poll::Ready(Ok(0))
                        ),
                        "{case:?}: reads end with EOF"
                    );
                }
                assert!(proxy.peer_resets().is_empty(), "{case:?}");
                drop(transport);
                drop(session);
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h2c_async_tunnel_after_connect_head_with_eom_reads_eof_at_once_and_keeps_writes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let mut tunnel = proxy.tunnel().await;
            tunnel.respond(200, &[("X-Connected-To", "1.2.3.4")], true);
            let stream = connect.await.expect("a 200 with END_STREAM connects");
            assert!(!stream.can_read());
            assert!(stream.can_write());
            let mut transport = HttpConnectAsyncTransport::new(&context, stream);
            let waker = noop_waker();
            let mut buffer = [0; 16];
            for _ in 0..2 {
                assert!(matches!(
                    poll_read_with(&mut transport, &waker, &mut buffer),
                    Poll::Ready(Ok(0))
                ));
            }
            assert!(matches!(
                poll_write_with(&mut transport, &waker, b"x"),
                Poll::Ready(Ok(1))
            ));
            transport.shutdown().await.expect("tunnel EOM");
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"x");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn h1_async_read_pending_at_half_close_times_out_after_stream_read_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let params = || SessionParams {
                stream_read_timeout: Some(Duration::from_millis(200)),
                ..SessionParams::default()
            };
            let proxy = RawH1Proxy::start().await;

            // A read pending at the request EOM times out with a silent peer.
            let (session, mut socket) = proxy.connect(&context, params()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            let mut buffer = [0; 16];
            assert!(poll_read_with(&mut transport, &noop_waker(), &mut buffer).is_pending());
            transport.shutdown().await.expect("tunnel EOM");
            let error = tokio::time::timeout(WAIT_DEADLINE, transport.read(&mut buffer))
                .await
                .expect("the pending read ends")
                .expect_err("the stream read timeout fails the pending read");
            assert_eq!(error.kind(), io::ErrorKind::Other);
            let inner = error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<HttpError>())
                .expect("HttpError");
            assert_eq!(inner.code(), HttpErrorCode::READ_TIMEOUT);
            drop(transport);

            // Each stored read clears the timeout again, so the next read idles.
            let (session, mut socket) = proxy.connect(&context, params()).await;
            let mut connect = unique_connect(
                &context,
                session.clone(),
                connect_reservation(&session),
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            h1_accept(&mut socket).await;
            let mut transport =
                HttpConnectAsyncTransport::new(&context, connect.await.expect("CONNECT"));
            assert!(poll_read_with(&mut transport, &noop_waker(), &mut buffer).is_pending());
            transport.shutdown().await.expect("tunnel EOM");
            socket.write_all(b"a").await.expect("proxy write");
            let mut early = [0; 1];
            transport
                .read_exact(&mut early)
                .await
                .expect("the pending read gets data");
            assert_eq!(&early, b"a");
            let mut late = [0; 1];
            let (read, ()) = tokio::join!(transport.read_exact(&mut late), async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                socket.write_all(b"b").await.expect("proxy write");
            });
            read.expect("the next read outlives the stream read timeout");
            assert_eq!(&late, b"b");
            drop(transport);
            drop(session);
        })
    });
}

#[test]
fn async_shutdown_with_deferred_write_sends_eom_then_completes_with_that_write() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let flusher = Arc::new(WakeCounter::default());
            let closer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &flusher.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_flush_with(&mut transport, &flusher.waker()).is_pending());
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), true),
                "poll_shutdown queued the EOM at once"
            );
            assert_eq!(
                closer.wakes(),
                1,
                "the processed bytes wake the latest writer"
            );
            assert_eq!(flusher.wakes(), 0);
            assert!(matches!(
                poll_shutdown_with(&mut transport, &closer.waker()),
                Poll::Ready(Ok(()))
            ));

            // A peer reset discards the deferred write's bytes, which completes it.
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            stall_h2c_egress(&mut transport, PROXY_WINDOW).await;
            let closer = Arc::new(WakeCounter::default());
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            eventually("the discarded bytes complete the write", || {
                closer.wakes() > 0
            })
            .await;
            assert!(matches!(
                poll_shutdown_with(&mut transport, &closer.waker()),
                Poll::Ready(Ok(()))
            ));
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_write_after_shutdown_fails_not_connected() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, mut tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            transport.shutdown().await.expect("tunnel EOM");
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &noop_waker(), b"x")),
                not_connected()
            );
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_drop_with_deferred_write_sends_queued_bytes_then_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, mut tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            stall_h2c_egress(&mut transport, PROXY_WINDOW).await;
            assert!(poll_flush_with(&mut transport, &noop_waker()).is_pending());
            drop(transport);
            let received = tunnel
                .read_to_end()
                .await
                .expect("END_STREAM after the queued bytes");
            assert_eq!(received.len(), 2 * PROXY_WINDOW);
            assert!(received[..PROXY_WINDOW].iter().all(|&byte| byte == b'w'));
            assert!(received[PROXY_WINDOW..].iter().all(|&byte| byte == b'p'));
            assert!(proxy.peer_resets().is_empty());
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_drop_with_stored_read_releases_it_and_closes_without_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let observed = Observed::new();
            let (session, mut transport, mut tunnel) =
                h2c_adapter(&context, &proxy, observed.params()).await;
            let mut buffer = [0; 16];
            assert!(poll_read_with(&mut transport, &noop_waker(), &mut buffer).is_pending());
            drop(transport);
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"");
            tunnel.end();
            let events = Rc::clone(&observed.events);
            eventually("the CONNECT stream to detach", move || {
                !events.detached.borrow().is_empty()
            })
            .await;
            assert!(proxy.peer_resets().is_empty());
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_shutdown_write_now_errors_pending_writes_and_aborts() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &writer.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
            transport.shutdown_write_now();
            assert_eq!(writer.wakes(), 1);
            assert_eq!(
                kind_and_message(poll_flush_with(&mut transport, &writer.waker())),
                socket_error()
            );
            let aborted = next_egress(&mut egress, u32::MAX)
                .await
                .expect_err("the egress is aborted");
            assert_eq!(aborted.code(), HttpErrorCode::CANCEL);
            assert_eq!(aborted.message(), b"HTTP source aborted err=CANCEL");
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &writer.waker(), b"x")),
                not_connected()
            );

            // After poll_shutdown queued the EOM, the abort is a no-op.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let closer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &closer.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            transport.shutdown_write_now();
            assert_eq!(
                kind_and_message(poll_shutdown_with(&mut transport, &closer.waker())),
                socket_error()
            );
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), true),
                "the queued bytes and the EOM still reach the peer"
            );
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_shutdown_write_now_without_pending_writes_sends_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, mut tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            transport.write_all(b"last").await.expect("tunnel write");
            transport.flush().await.expect("tunnel flush");
            transport.shutdown_write_now();
            assert_eq!(tunnel.read_to_end().await.expect("END_STREAM"), b"last");
            assert!(proxy.peer_resets().is_empty());
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &noop_waker(), b"x")),
                not_connected()
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_close_now_fails_deferred_writes_aborts_and_ends_reads() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let writer = Arc::new(WakeCounter::default());
            let reader = Arc::new(WakeCounter::default());
            let mut buffer = [0; 16];
            assert!(poll_read_with(&mut transport, &reader.waker(), &mut buffer).is_pending());
            assert!(matches!(
                poll_write_with(&mut transport, &writer.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
            transport.close_now();
            assert_eq!(
                kind_and_message(poll_flush_with(&mut transport, &writer.waker())),
                socket_error()
            );
            assert!(reader.wakes() > 0);
            for _ in 0..2 {
                assert!(matches!(
                    poll_read_with(&mut transport, &reader.waker(), &mut buffer),
                    Poll::Ready(Ok(0))
                ));
            }
            let aborted = next_egress(&mut egress, u32::MAX)
                .await
                .expect_err("the egress is aborted");
            assert_eq!(aborted.code(), HttpErrorCode::CANCEL);

            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let closer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &closer.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            transport.close_now();
            assert_eq!(
                kind_and_message(poll_shutdown_with(&mut transport, &closer.waker())),
                socket_error()
            );
            assert!(matches!(
                poll_read_with(&mut transport, &closer.waker(), &mut buffer),
                Poll::Ready(Ok(0))
            ));
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), true),
                "the queued bytes and the EOM still reach the peer"
            );
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_peer_reset_completes_deferred_write_from_discarded_bytes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let (session, mut transport, tunnel) =
                h2c_adapter(&context, &proxy, SessionParams::default()).await;
            stall_h2c_egress(&mut transport, PROXY_WINDOW).await;
            let writer = Arc::new(WakeCounter::default());
            assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
            eventually("the discarded bytes complete the write", || {
                writer.wakes() > 0
            })
            .await;
            assert!(
                matches!(
                    poll_flush_with(&mut transport, &writer.waker()),
                    Poll::Ready(Ok(()))
                ),
                "the deferred write completes, not fails"
            );
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &writer.waker(), b"x")),
                not_connected()
            );
            drop(transport);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn async_producer_calls_with_no_egress_read_active_reenter_callbacks_without_panic() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = TunnelProxy::start(CodecProtocol::Http2).await;
            let content_length = || headers(&[("Content-Length", "3"), ("Foo", "Bar")]);

            // shutdown_write_now and close_now with a deferred write.
            for close_now in [false, true] {
                let (_scripted, mut transport, _tunnel, mut egress) =
                    captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
                let writer = Arc::new(WakeCounter::default());
                assert!(matches!(
                    poll_write_with(&mut transport, &writer.waker(), b"abcd"),
                    Poll::Ready(Ok(4))
                ));
                assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
                if close_now {
                    transport.close_now();
                } else {
                    transport.shutdown_write_now();
                }
                assert_eq!(
                    kind_and_message(poll_flush_with(&mut transport, &writer.waker())),
                    socket_error()
                );
                let aborted = next_egress(&mut egress, u32::MAX)
                    .await
                    .expect_err("reading the holder delivers the abort");
                assert_eq!(aborted.code(), HttpErrorCode::CANCEL);
                assert_eq!(
                    kind_and_message(poll_write_with(&mut transport, &writer.waker(), b"x")),
                    not_connected()
                );
            }

            // poll_shutdown waits until the holder's reads process the bytes.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            let closer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &closer.waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_shutdown_with(&mut transport, &closer.waker()).is_pending());
            assert_eq!(closer.wakes(), 0, "queuing the EOM runs no callback");
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), true)
            );
            assert_eq!(closer.wakes(), 1);
            assert!(matches!(
                poll_shutdown_with(&mut transport, &closer.waker()),
                Poll::Ready(Ok(()))
            ));

            // Drop leaves the queued bytes and the EOM. Their callbacks run
            // after the adapter's state is gone and have no effect.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            assert!(matches!(
                poll_write_with(&mut transport, &noop_waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            drop(transport);
            assert_eq!(
                next_egress_bytes(&mut egress, u32::MAX).await,
                (b"abcd".to_vec(), true)
            );

            // body re-enters through sink mode after a Content-Length overflow.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, EGRESS_BUFFER_SIZE, content_length()).await;
            let writer = Arc::new(WakeCounter::default());
            for chunk in [b"wxyz".as_slice(), b"z"] {
                assert!(matches!(
                    poll_write_with(&mut transport, &writer.waker(), chunk),
                    Poll::Ready(Ok(length)) if length == chunk.len()
                ));
                assert!(matches!(
                    poll_flush_with(&mut transport, &writer.waker()),
                    Poll::Ready(Ok(()))
                ));
            }
            let mismatch = next_egress(&mut egress, u32::MAX)
                .await
                .expect_err("the overflow installed a content-length error");
            assert_eq!(mismatch.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &writer.waker(), b"x")),
                not_connected()
            );

            // With an egress buffer of 0 the credited overflow still defers, and
            // the source's completion fails it.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 0, content_length()).await;
            let writer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &writer.waker(), b"wxyz"),
                Poll::Ready(Ok(4))
            ));
            assert!(poll_flush_with(&mut transport, &writer.waker()).is_pending());
            let mismatch = next_egress(&mut egress, u32::MAX)
                .await
                .expect_err("the overflow installed a content-length error");
            assert_eq!(mismatch.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
            assert_eq!(writer.wakes(), 1, "errorWrites wakes the flush");
            assert_eq!(
                kind_and_message(poll_flush_with(&mut transport, &writer.waker())),
                socket_error()
            );
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &writer.waker(), b"x")),
                not_connected()
            );

            // eom re-enters through sink mode when the body is short.
            let (_scripted, mut transport, _tunnel, mut egress) =
                captured_adapter(&context, &proxy, 2, content_length()).await;
            let closer = Arc::new(WakeCounter::default());
            assert!(matches!(
                poll_write_with(&mut transport, &closer.waker(), b"ab"),
                Poll::Ready(Ok(2))
            ));
            assert!(
                matches!(
                    poll_shutdown_with(&mut transport, &closer.waker()),
                    Poll::Ready(Ok(()))
                ),
                "the short body's credit inside eom completes the write"
            );
            let mismatch = next_egress(&mut egress, u32::MAX)
                .await
                .expect_err("the short body installed a content-length error");
            assert_eq!(mismatch.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &closer.waker(), b"x")),
                not_connected()
            );

            // Replacing a flush waker whose drop releases the egress holder
            // re-enters bytes_processed and source_complete.
            let (_scripted, mut transport, _tunnel, egress) =
                captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
            assert!(matches!(
                poll_write_with(&mut transport, &noop_waker(), b"abcd"),
                Poll::Ready(Ok(4))
            ));
            RELEASED_BY_WAKER_DROP.set(Some(egress));
            let releasing = Waker::from(Arc::new(ReleaseOnDrop::default()));
            assert!(poll_flush_with(&mut transport, &releasing).is_pending());
            drop(releasing);
            let flusher = Arc::new(WakeCounter::default());
            assert!(
                matches!(
                    poll_flush_with(&mut transport, &flusher.waker()),
                    Poll::Ready(Ok(()))
                ),
                "the released holder credits the discarded bytes, which completes the write"
            );
            assert_eq!(
                kind_and_message(poll_write_with(&mut transport, &flusher.waker(), b"x")),
                not_connected()
            );

            // A write whose waker replacement completes the egress source in the
            // same way fails instead of reporting bytes the source cannot take.
            for vectored in [false, true] {
                let (_scripted, mut transport, _tunnel, egress) =
                    captured_adapter(&context, &proxy, 4, headers(&[("Foo", "Bar")])).await;
                assert!(matches!(
                    poll_write_with(&mut transport, &noop_waker(), b"abcd"),
                    Poll::Ready(Ok(4))
                ));
                RELEASED_BY_WAKER_DROP.set(Some(egress));
                let releasing = Waker::from(Arc::new(ReleaseOnDrop::default()));
                assert!(poll_flush_with(&mut transport, &releasing).is_pending());
                drop(releasing);
                let writer = noop_waker();
                let written = if vectored {
                    let slices = [io::IoSlice::new(b"lo"), io::IoSlice::new(b"st")];
                    Pin::new(&mut transport)
                        .poll_write_vectored(&mut Context::from_waker(&writer), &slices)
                } else {
                    poll_write_with(&mut transport, &writer, b"lost")
                };
                assert_eq!(
                    kind_and_message(written),
                    not_connected(),
                    "vectored: {vectored}"
                );
                assert!(
                    matches!(
                        poll_flush_with(&mut transport, &writer),
                        Poll::Ready(Ok(()))
                    ),
                    "the credited bytes completed the earlier write"
                );
            }
            proxy.shutdown().await;
        })
    });
}
