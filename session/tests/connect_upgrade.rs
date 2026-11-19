/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Tests for CONNECT upgrade state and for resets of abandoned upgraded streams.
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::net::Ipv4Addr;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_http1::Http1xCodec;
use roxygen_http2::Http2Codec;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsClientOptions;
use roxygen_transport::tls::TlsTransport;
use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tokio_rustls::TlsAcceptor;

use crate::http2::Http2SessionConfig;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

const TEST_DEADLINE: Duration = Duration::from_secs(20);
const WAIT_DEADLINE: Duration = Duration::from_secs(5);
const CONNECTION_WINDOW: usize = 65_535;
const STREAM_1: StreamId = StreamId::new(1);
const STREAM_3: StreamId = StreamId::new(3);

fn run_on_worker(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static,
) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    let result = runtime.block_on(LocalSet::new().run_until(async move {
        let (worker, driver) = WorkerDriver::new(16).expect("worker driver");
        let call = worker
            .call_local(move |context| {
                let worker = context.handle().clone();
                async move {
                    let result =
                        AssertUnwindSafe(tokio::time::timeout(TEST_DEADLINE, test(context)))
                            .catch_unwind()
                            .await;
                    worker.begin_shutdown();
                    result
                }
            })
            .expect("worker call");
        let (_, result) = tokio::join!(driver, call);
        result.expect("worker call completed")
    }));
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("the test exceeded its deadline"),
        Err(payload) => resume_unwind(payload),
    }
}

fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).expect("queue capacity")
}

fn bytes_of(queue: &BufQueue) -> Vec<u8> {
    queue
        .coalesce(queue.chain_length())
        .expect("bounded queue")
        .to_vec()
}

async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let reached = tokio::time::timeout(WAIT_DEADLINE, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for {what}");
}

/// With the clock paused, returns once no task on the worker can make progress,
/// because Tokio advances a paused clock only while the runtime is idle.
async fn until_idle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

/// Returns once no task on the worker can make progress.
async fn settle() {
    tokio::time::pause();
    until_idle().await;
    tokio::time::resume();
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Close {
    Open,
    ShutdownWrite,
    Closed,
    Reset,
}

enum Read {
    Data(Vec<u8>),
    Eof,
    /// Runs inside the session's read, before the next queued read completes.
    Run(Box<dyn FnOnce()>),
}

struct WireState {
    reads: VecDeque<Read>,
    written: Vec<u8>,
    close: Close,
    wrote_after_close: bool,
    stall_writes: bool,
    pending_writes: usize,
}

/// The peer's view of one byte-stream connection. A local reset fails pending
/// and later reads and writes, as it does on a socket.
#[derive(Clone)]
struct Wire {
    state: Rc<RefCell<WireState>>,
    readable: Rc<tokio::sync::Notify>,
    writable: Rc<tokio::sync::Notify>,
}

impl Wire {
    fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(WireState {
                reads: VecDeque::new(),
                written: Vec::new(),
                close: Close::Open,
                wrote_after_close: false,
                stall_writes: false,
                pending_writes: 0,
            })),
            readable: Rc::new(tokio::sync::Notify::new()),
            writable: Rc::new(tokio::sync::Notify::new()),
        }
    }

    /// Leaves later writes pending, as when the peer stops reading.
    fn stall_writes(&self) {
        self.state.borrow_mut().stall_writes = true;
    }

    fn pending_writes(&self) -> usize {
        self.state.borrow().pending_writes
    }

    fn transport(&self) -> Rc<dyn UniplexTransport> {
        Rc::new(WireTransport { wire: self.clone() })
    }

    fn deliver(&self, bytes: Vec<u8>) {
        self.push(Read::Data(bytes));
    }

    fn end(&self) {
        self.push(Read::Eof);
    }

    /// Runs `action` in the session's own read pass, so the session handles the
    /// next queued read before it drives any callback that `action` queues.
    fn run_in_read(&self, action: impl FnOnce() + 'static) {
        self.push(Read::Run(Box::new(action)));
    }

    fn push(&self, read: Read) {
        self.state.borrow_mut().reads.push_back(read);
        self.readable.notify_one();
    }

    fn written(&self) -> Vec<u8> {
        self.state.borrow().written.clone()
    }

    fn close(&self) -> Close {
        self.state.borrow().close
    }

    fn reads_drained(&self) -> bool {
        self.state.borrow().reads.is_empty()
    }

    fn queued_reads(&self) -> usize {
        self.state.borrow().reads.len()
    }

    /// Asserts the session ended the connection with a FIN after its last write.
    fn assert_fin_after_data(&self) {
        let state = self.state.borrow();
        assert_eq!(state.close, Close::ShutdownWrite);
        assert!(
            !state.wrote_after_close,
            "the session wrote after closing the connection"
        );
    }
}

struct WireTransport {
    wire: Wire,
}

impl UniplexTransport for WireTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let wire = self.wire.clone();
        Box::pin(async move {
            let baton = CancellableBaton::new();
            let mut cancelled = std::pin::pin!(baton.wait(cancellation));
            loop {
                if wire.state.borrow().close == Close::Reset {
                    return Err(HttpError::with_message(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"connection reset locally",
                    ));
                }
                let next = wire.state.borrow_mut().reads.pop_front();
                match next {
                    Some(Read::Data(mut bytes)) => {
                        if bytes.len() > maximum {
                            let rest = bytes.split_off(maximum);
                            wire.state.borrow_mut().reads.push_front(Read::Data(rest));
                        }
                        return Ok(UniplexRead::new(queue(&bytes), false));
                    }
                    Some(Read::Eof) => return Ok(UniplexRead::new(BufQueue::new(0), true)),
                    Some(Read::Run(action)) => {
                        action();
                        continue;
                    }
                    None => {}
                }
                tokio::select! {
                    () = wire.readable.notified() => {}
                    _ = &mut cancelled => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"peer read cancelled",
                        ));
                    }
                }
            }
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let wire = self.wire.clone();
        let failed = |code, message: &[u8]| {
            Err(TransportWriteError::new(
                HttpError::with_message(code, message),
                0,
            ))
        };
        Box::pin(async move {
            let baton = CancellableBaton::new();
            let mut cancelled = std::pin::pin!(baton.wait(cancellation));
            let mut expired = std::pin::pin!(async {
                if timeout.is_zero() {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(timeout).await;
            });
            wire.state.borrow_mut().pending_writes += 1;
            let result = loop {
                let (reset, stalled) = {
                    let state = wire.state.borrow();
                    (state.close == Close::Reset, state.stall_writes)
                };
                if reset {
                    break failed(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"connection reset locally",
                    );
                }
                if !stalled {
                    break Ok(());
                }
                tokio::select! {
                    () = wire.writable.notified() => {}
                    _ = &mut cancelled => {
                        break failed(HttpErrorCode::CORO_CANCELLED, b"write cancelled");
                    }
                    () = &mut expired => {
                        break failed(HttpErrorCode::WRITE_TIMEOUT, b"write timed out");
                    }
                }
            };
            let mut state = wire.state.borrow_mut();
            state.pending_writes -= 1;
            result?;
            let length = data.chain_length();
            state.wrote_after_close |= length != 0 && state.close != Close::Open;
            state.written.extend_from_slice(&bytes_of(&data));
            Ok(WriteInfo {
                bytes_written: length,
            })
        })
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.wire.state.borrow_mut();
        if state.close == Close::Open {
            state.close = Close::ShutdownWrite;
        }
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.wire.state.borrow_mut();
        if state.close != Close::Reset {
            state.close = Close::Closed;
        }
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.wire.state.borrow_mut().close = Close::Reset;
        self.wire.readable.notify_one();
        self.wire.writable.notify_waiters();
        Ok(())
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        Err(ByteEventRegistrationError::Unsupported)
    }
}

struct Silent;

impl HttpCodecCallback for Silent {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

/// The remote HTTP/2 endpoint, which frames its own bytes.
struct H2Peer {
    codec: Http2Codec,
}

impl H2Peer {
    fn server() -> (Self, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut Silent)
            .expect("server SETTINGS");
        (Self { codec }, bytes_of(&output))
    }

    fn client() -> (Self, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut Silent)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut Silent)
            .expect("client SETTINGS");
        (Self { codec }, bytes_of(&output))
    }

    fn create_stream(&mut self) -> StreamId {
        self.codec.create_stream()
    }

    fn frame(&mut self, frame: EgressFrame<'_>) -> Vec<u8> {
        let mut output = BufQueue::new(1 << 20);
        self.codec
            .generate(&mut output, frame, &mut Silent)
            .expect("peer frame");
        bytes_of(&output)
    }

    fn headers(&mut self, stream: StreamId, message: &HttpMessage, end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Headers {
            stream,
            message,
            end_stream,
            extra_headers: None,
        })
    }

    fn data(&mut self, stream: StreamId, bytes: &[u8], end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Body {
            stream,
            body: queue(bytes),
            padding: None,
            end_stream,
        })
    }

    fn trailers(&mut self, stream: StreamId) -> Vec<u8> {
        let mut trailers = HttpHeaders::new();
        trailers.set(b"x-late", b"trailer");
        self.frame(EgressFrame::Trailers {
            stream,
            trailers: &trailers,
        })
    }

    /// An empty DATA frame whose padding, with its pad length octet, consumes
    /// `flow_controlled` bytes of receive window.
    fn padding(&mut self, stream: StreamId, flow_controlled: usize) -> Vec<u8> {
        let padding = u8::try_from(flow_controlled - 1).expect("padding fits one frame");
        self.frame(EgressFrame::Body {
            stream,
            body: BufQueue::new(0),
            padding: Some(padding),
            end_stream: false,
        })
    }
}

/// HTTP/2 frames a session wrote, as its peer decodes them.
#[derive(Default)]
struct Frames {
    resets: Vec<(StreamId, H2ErrorCode)>,
    window_updates: Vec<(StreamId, u32)>,
    statuses: Vec<(StreamId, u16)>,
    data: Vec<(StreamId, Vec<u8>)>,
    ended: Vec<StreamId>,
    goaway: bool,
}

impl Frames {
    fn data_on(&self, stream: StreamId) -> Vec<u8> {
        self.data
            .iter()
            .filter(|(id, _)| *id == stream)
            .flat_map(|(_, bytes)| bytes.iter().copied())
            .collect()
    }

    fn credit_for(&self, stream: StreamId) -> usize {
        self.window_updates
            .iter()
            .filter(|(id, _)| *id == stream)
            .map(|(_, amount)| *amount as usize)
            .sum()
    }
}

impl HttpCodecCallback for Frames {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::Abort { stream, code } => self.resets.push((stream, code)),
            CodecEvent::WindowUpdate { stream, amount } => {
                self.window_updates.push((stream, amount));
            }
            CodecEvent::HeadersComplete { stream, message } => {
                if let Some(status) = message.response_status() {
                    self.statuses.push((stream, status));
                }
            }
            CodecEvent::Body { stream, body, .. } => self.data.push((stream, bytes_of(&body))),
            CodecEvent::MessageComplete { stream, .. } => self.ended.push(stream),
            CodecEvent::Goaway { .. } => self.goaway = true,
            _ => {}
        }
    }
}

fn decode(mut codec: Http2Codec, wire: &[u8]) -> Frames {
    let mut frames = Frames::default();
    let mut output = BufQueue::new(1 << 20);
    let input = BufQueue::copy_from_slice(wire, wire.len()).expect("decoded wire fits");
    let _ = codec.on_ingress(&input, &mut frames, &mut output);
    frames
}

/// Frames an upstream session wrote, decoded by its server.
fn upstream_frames(wire: &Wire) -> Frames {
    decode(
        Http2Codec::new(TransportDirection::Downstream),
        &wire.written(),
    )
}

/// Frames a downstream session wrote, decoded by its client.
fn downstream_frames(wire: &Wire) -> Frames {
    let mut codec = Http2Codec::new(TransportDirection::Upstream);
    assert_eq!(codec.create_stream(), STREAM_1);
    decode(codec, &wire.written())
}

/// Stream lifecycle transitions observed on the session owner.
#[derive(Default)]
struct Lifecycle {
    egress_ended: RefCell<Vec<StreamId>>,
    detached: RefCell<Vec<StreamId>>,
    ingress_errors: RefCell<Vec<(StreamId, HttpErrorCode)>>,
}

impl Lifecycle {
    fn attach(session: &UniplexSession) -> Rc<Self> {
        let lifecycle = Rc::new(Self::default());
        session
            .add_lifecycle_observer(Rc::clone(&lifecycle) as Rc<dyn LifecycleObserver>)
            .expect("lifecycle observer");
        lifecycle
    }

    fn egress_ended(&self, stream: StreamId) -> bool {
        self.egress_ended.borrow().contains(&stream)
    }

    fn detached(&self, stream: StreamId) -> bool {
        self.detached.borrow().contains(&stream)
    }

    fn assert_no_ingress_errors(&self) {
        assert_eq!(
            *self.ingress_errors.borrow(),
            [],
            "frames for an erased stream raised an HTTP/2 ingress error"
        );
    }
}

impl LifecycleObserver for Lifecycle {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::RequestEnd { stream } => self.egress_ended.borrow_mut().push(stream),
            LifecycleEvent::TransactionDetached {
                stream: Some(stream),
            } => self.detached.borrow_mut().push(stream),
            LifecycleEvent::Http2IngressError { stream, code } => {
                self.ingress_errors.borrow_mut().push((stream, code));
            }
            _ => {}
        }
    }
}

fn connect_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Connect).expect("CONNECT");
    message.set_url(b"example:443", false).expect("authority");
    message.set_http_version(1, 1);
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example:443");
    message
}

fn h1_websocket_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).expect("GET");
    message.set_url(b"/chat", false).expect("path");
    message.set_http_version(1, 1);
    message.set_egress_websocket_upgrade();
    message.headers_mut().set(HttpHeaderCode::Host, b"foo.com");
    message
}

fn h2_websocket_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message
        .set_method(HttpMethod::Connect)
        .expect("extended CONNECT");
    message.set_url(b"/chat", false).expect("path");
    message.set_http_version(1, 1);
    message.set_upgrade_protocol(b"websocket");
    message.set_egress_websocket_upgrade();
    message.headers_mut().set(HttpHeaderCode::Host, b"foo.com");
    message
}

fn status(code: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(code).expect("status");
    message.set_http_version(1, 1);
    message
}

fn websocket_accept() -> HttpMessage {
    let mut message = status(200);
    message.set_egress_websocket_upgrade();
    message
}

/// A source with its header already queued. The test writes the body later.
fn streaming_source(message: HttpMessage) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let source = HttpStreamSource::new_async(None, None, 1 << 20).expect("streaming source");
    let producer = source.producer();
    assert!(producer.headers(message, false));
    (HttpSourceHolder::new(Box::new(source)), producer)
}

fn send(producer: &HttpStreamSourceProducer, bytes: &[u8]) {
    assert!(producer.body(queue(bytes), 0, false) != FlowControlState::Error);
}

fn h1_head_len(wire: &[u8]) -> Option<usize> {
    wire.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

/// The 101 that a real HTTP/1.1 server derives from the client's upgrade request.
fn h1_websocket_101(request: &[u8]) -> Vec<u8> {
    let mut server = Http1xCodec::new(TransportDirection::Downstream);
    let mut output = BufQueue::new(1 << 20);
    let input = BufQueue::copy_from_slice(request, request.len()).expect("request fits");
    let parsed = server.on_ingress(&input, &mut Silent, &mut output);
    assert_eq!(parsed.consumed, request.len());
    let response = websocket_accept();
    server
        .generate(
            &mut output,
            EgressFrame::Headers {
                stream: STREAM_1,
                message: &response,
                end_stream: false,
                extra_headers: None,
            },
            &mut Silent,
        )
        .expect("101 response");
    bytes_of(&output)
}

async fn read_body(source: &mut HttpSourceHolder, expected: usize) -> Vec<u8> {
    let mut received = Vec::new();
    while received.len() < expected {
        let event = source.read_body_event(u32::MAX).await.expect("tunnel body");
        if let Some(body) = event.as_body() {
            received.extend_from_slice(&bytes_of(body));
        }
        assert!(
            !event.eom(),
            "the tunnel ended after {} bytes",
            received.len()
        );
    }
    received
}

async fn read_to_eom(source: &mut HttpSourceHolder) -> Vec<u8> {
    let mut received = Vec::new();
    loop {
        let event = source.read_body_event(u32::MAX).await.expect("tunnel body");
        if let Some(body) = event.as_body() {
            received.extend_from_slice(&bytes_of(body));
        }
        if event.eom() {
            return received;
        }
    }
}

async fn open_tunnel(
    handle: &UniplexHandle,
    request: HttpMessage,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let (request, upload) = streaming_source(request);
    let response = handle
        .send_request(request, CancellationToken::never())
        .await
        .expect("tunnel request admitted");
    (response.source, upload)
}

fn h1_session(
    context: WorkerContext,
    direction: TransportDirection,
    handler: Option<HttpHandlerRef>,
) -> (UniplexSession, UniplexHandle, Wire, Rc<Lifecycle>) {
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new(
        context,
        direction,
        wire.transport(),
        handler,
        UniplexConfig::default(),
    )
    .expect("HTTP/1.1 session");
    let lifecycle = Lifecycle::attach(&session);
    (session, handle, wire, lifecycle)
}

fn h2_upstream(
    context: WorkerContext,
    config: Http2SessionConfig,
) -> (UniplexSession, UniplexHandle, Wire, H2Peer, Rc<Lifecycle>) {
    h2_upstream_with(context, UniplexConfig::default(), config)
}

fn h2_upstream_with(
    context: WorkerContext,
    uniplex: UniplexConfig,
    config: Http2SessionConfig,
) -> (UniplexSession, UniplexHandle, Wire, H2Peer, Rc<Lifecycle>) {
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new_http2(
        context,
        TransportDirection::Upstream,
        wire.transport(),
        None,
        uniplex,
        config,
    )
    .expect("upstream HTTP/2 session");
    let lifecycle = Lifecycle::attach(&session);
    let (peer, settings) = H2Peer::server();
    wire.deliver(settings);
    (session, handle, wire, peer, lifecycle)
}

/// Opens an H1 CONNECT tunnel and reads its 200.
async fn h1_established_tunnel(
    handle: &UniplexHandle,
    wire: &Wire,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let (mut response, upload) = open_tunnel(handle, connect_request()).await;
    eventually("the CONNECT head", || {
        h1_head_len(&wire.written()).is_some()
    })
    .await;
    wire.deliver(b"HTTP/1.1 200 OK\r\n\r\n".to_vec());
    let mut head = response
        .read_header_event()
        .await
        .expect("CONNECT response");
    assert_eq!(head.headers_mut().status_code().expect("status"), 200);
    assert!(!head.eom());
    (response, upload)
}

/// Opens an H2 tunnel that the peer accepts with `accept`, its framed 200.
async fn h2_established_tunnel(
    handle: &UniplexHandle,
    wire: &Wire,
    accept: Vec<u8>,
    request: HttpMessage,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let (mut response, upload) = open_tunnel(handle, request).await;
    wire.deliver(accept);
    let mut head = response.read_header_event().await.expect("tunnel response");
    assert_eq!(head.headers_mut().status_code().expect("status"), 200);
    assert!(!head.eom());
    (response, upload)
}

/// Waits until `bytes` follow abandonment on an H2 stream, or the stream resets.
async fn h2_data_or_reset(wire: &Wire, stream: StreamId, bytes: &[u8]) {
    eventually("DATA after abandonment, or a RST_STREAM", || {
        let frames = upstream_frames(wire);
        frames.data_on(stream).ends_with(bytes) || !frames.resets.is_empty()
    })
    .await;
}

/// Waits until `bytes` follow abandonment on an H1 tunnel, or the TCP reset.
async fn h1_bytes_or_reset(wire: &Wire, bytes: &[u8]) {
    eventually("tunnel bytes after abandonment, or a TCP reset", || {
        wire.written().ends_with(bytes) || wire.close() == Close::Reset
    })
    .await;
}

/// Ends the tunnel's upload, then waits for its stream to detach.
async fn finish_upload(upload: &HttpStreamSourceProducer, lifecycle: &Lifecycle, stream: StreamId) {
    upload.eom();
    eventually("the stream to detach", || lifecycle.detached(stream)).await;
}

/// Waits until the session has read and parsed every byte the peer sent, or ended.
async fn ingress_consumed(wire: &Wire, handle: &UniplexHandle) {
    eventually("the session to consume the peer's bytes", || {
        wire.reads_drained()
            && !matches!(handle.buffered_ingress_for_test(), Ok(bytes) if !bytes.is_empty())
    })
    .await;
}

fn ingress_meta_retained(handle: &UniplexHandle, stream: StreamId) -> bool {
    handle
        .h1_close_snapshot_for_test(stream)
        .expect("the session is running")
        .0
}

#[derive(Default)]
struct TunnelSlots {
    request: RefCell<Option<HttpSourceHolder>>,
    response: RefCell<Option<HttpStreamSourceProducer>>,
}

/// Accepts a tunnel with `response` and leaves both directions to the test.
struct TunnelHandler {
    response: HttpMessage,
    slots: Rc<TunnelSlots>,
}

impl HttpHandler for TunnelHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let response = self.response.clone();
        let slots = Rc::clone(&self.slots);
        Box::pin(async move {
            let headers = request.read_header_event().await?;
            assert!(!headers.eom());
            let (source, producer) = streaming_source(response);
            slots.request.replace(Some(request));
            slots.response.replace(Some(producer));
            Ok(source)
        })
    }
}

/// An H1 downstream tunnel. After the upgrade response, the handler abandons
/// the request source with `CANCEL` while the peer keeps sending.
fn h1_downstream_abandon(
    request: &'static [u8],
    response: HttpMessage,
    status_line: &'static [u8],
) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let slots = Rc::new(TunnelSlots::default());
            let handler: HttpHandlerRef = Rc::new(TunnelHandler {
                response,
                slots: Rc::clone(&slots),
            });
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Downstream, Some(handler));
            wire.deliver(request.to_vec());
            let peer = wire.clone();
            let exchange = async move {
                eventually("the upgrade response", || {
                    peer.written().starts_with(status_line)
                })
                .await;
                let mut request = slots.request.take().expect("handler kept the request");
                request.stop_reading(Some(HttpErrorCode::CANCEL));
                drop(request);
                peer.deliver(b"late".to_vec());
                let upload = slots.response.take().expect("handler kept the response");
                send(&upload, b"after");
                h1_bytes_or_reset(&peer, b"after").await;
                ingress_consumed(&peer, &handle).await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "abandoning an upgraded stream reset the connection"
                );
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            wire.assert_fin_after_data();
        })
    });
}

/// Abandons an upgraded H2 CONNECT stream with `code`, then checks the frames
/// written once its later egress is written or the stream resets, and again once
/// the upload has ended, the stream has detached, and the session has ended.
fn h2_upstream_abandon_with(code: HttpErrorCode, check: fn(HttpErrorCode, &Frames)) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (mut response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                response.stop_reading(Some(code));
                send(&upload, b"after");
                h2_data_or_reset(&observed, STREAM_1, b"after").await;
                check(code, &upstream_frames(&observed));
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            check(code, &upstream_frames(&wire));
        })
    });
}

/// Abandons an upgraded H1 CONNECT tunnel with `code`, then checks that the
/// session resets the connection exactly when `resets` is set.
fn h1_upstream_abandon_with(code: HttpErrorCode, resets: bool) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (mut response, upload) = h1_established_tunnel(&handle, &peer).await;
                response.stop_reading(Some(code));
                drop(response);
                if resets {
                    eventually("a TCP reset", || peer.close() == Close::Reset).await;
                    return;
                }
                send(&upload, b"after");
                h1_bytes_or_reset(&peer, b"after").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "{code:?} reset an upgraded tunnel"
                );
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            if resets {
                assert_eq!(wire.close(), Close::Reset);
            } else {
                wire.assert_fin_after_data();
            }
        })
    });
}

/// Abandons an upgraded H2 tunnel after, or before, the peer's END_STREAM. The
/// source rejects the peer's later DATA, which only returns connection credit,
/// and the tunnel's egress still finishes.
fn h2_late_data_after_peer_end_stream(end_before_abandon: bool) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                stream_receive_window: 4 * CONNECTION_WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let (session, handle, wire, peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                let accept = peer.borrow_mut().headers(STREAM_1, &status(200), false);
                let (response, upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                let end_stream = peer.borrow_mut().data(STREAM_1, b"", true);
                if end_before_abandon {
                    observed.deliver(end_stream);
                    ingress_consumed(&observed, &handle).await;
                    drop(response);
                    send(&upload, b"after");
                    h2_data_or_reset(&observed, STREAM_1, b"after").await;
                } else {
                    drop(response);
                    send(&upload, b"after");
                    h2_data_or_reset(&observed, STREAM_1, b"after").await;
                    observed.deliver(end_stream);
                }
                // A second window of DATA past END_STREAM needs returned connection credit.
                window.send(STREAM_1, 2 * CONNECTION_WINDOW, false).await;
                ingress_consumed(&observed, &handle).await;
                assert_eq!(
                    upstream_frames(&observed).resets,
                    [],
                    "DATA after the peer's END_STREAM reset the abandoned tunnel"
                );
                lifecycle.assert_no_ingress_errors();
                send(&upload, b"final");
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                let frames = upstream_frames(&observed);
                assert!(frames.data_on(STREAM_1).ends_with(b"afterfinal"));
                assert!(frames.ended.contains(&STREAM_1));
                assert_eq!(frames.credit_for(STREAM_1), 0);
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
}

/// Abandonment leaves an upgraded H2 tunnel unreset, but a malformed header
/// block on the still-attached tunnel resets it.
fn h2_abandoned_tunnel_peer_protocol_error_resets() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                drop(response);
                send(&upload, b"after");
                h2_data_or_reset(&observed, STREAM_1, b"after").await;
                assert_eq!(upstream_frames(&observed).resets, []);
                assert!(!lifecycle.detached(STREAM_1));

                observed.deliver(duplicate_status_headers(STREAM_1));
                eventually("a RST_STREAM", || {
                    !upstream_frames(&observed).resets.is_empty()
                })
                .await;
                assert_eq!(
                    *lifecycle.ingress_errors.borrow(),
                    [(STREAM_1, HttpErrorCode::PROTOCOL_ERROR)]
                );
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                assert_eq!(
                    upload.body(queue(b"late"), 0, false),
                    FlowControlState::Error,
                    "the upload outlived its reset stream"
                );
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            let frames = upstream_frames(&wire);
            assert_eq!(frames.resets, [(STREAM_1, H2ErrorCode::PROTOCOL_ERROR)]);
            assert_eq!(frames.data_on(STREAM_1), b"after");
            assert!(!frames.ended.contains(&STREAM_1));
        })
    });
}

/// The reader abandons an upgraded H2 tunnel in the session's read pass that then
/// reads the peer's EOF, before the session drives the abandonment's callbacks.
/// `stop_reading` completes the source synchronously and the EOF only aborts
/// ingress, so the tunnel's egress finishes without a RST_STREAM or a connection
/// reset.
fn h2_abandoned_tunnel_survives_peer_eof_in_the_same_read() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                observed.run_in_read(move || drop(response));
                observed.end();
                eventually("the session to read the peer's EOF", || {
                    observed.reads_drained()
                })
                .await;
                let accepted = upload.body(queue(b"after"), 0, false);
                eventually("DATA after the peer's EOF, or a reset", || {
                    let frames = upstream_frames(&observed);
                    frames.data_on(STREAM_1).ends_with(b"after")
                        || !frames.resets.is_empty()
                        || observed.close() == Close::Reset
                })
                .await;
                assert_eq!(
                    observed.close(),
                    Close::Open,
                    "the peer's EOF reset the connection under an abandoned tunnel"
                );
                assert_ne!(accepted, FlowControlState::Error);
                assert_eq!(upstream_frames(&observed).resets, []);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                assert!(upstream_frames(&observed).ended.contains(&STREAM_1));
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the abandoned tunnel closes cleanly after the peer's EOF");
            assert_ne!(wire.close(), Close::Reset);
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
}

/// The peer ends an upgraded H2 tunnel, then sends DATA or trailers before the
/// reader has consumed that END_STREAM. The source rejects the frame and fails
/// with INVALID_STATE_TRANSITION, which does not reset an upgraded stream, and
/// the DATA only returns connection credit. The tunnel's egress still finishes.
fn h2_late_frame_before_end_stream_is_read(trailers: bool) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                stream_receive_window: 4 * CONNECTION_WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let (session, handle, wire, peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                let accept = peer.borrow_mut().headers(STREAM_1, &status(200), false);
                let (mut response, upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                send(&upload, b"before");
                eventually("tunnel egress", || {
                    upstream_frames(&observed).data_on(STREAM_1) == b"before"
                })
                .await;

                observed.deliver(peer.borrow_mut().data(STREAM_1, b"", true));
                if trailers {
                    observed.deliver(peer.borrow_mut().trailers(STREAM_1));
                } else {
                    // A second window of DATA past END_STREAM needs returned connection credit.
                    window.send(STREAM_1, 2 * CONNECTION_WINDOW, false).await;
                }
                ingress_consumed(&observed, &handle).await;
                assert_eq!(
                    upstream_frames(&observed).resets,
                    [],
                    "a frame after the peer's END_STREAM reset the upgraded tunnel"
                );
                lifecycle.assert_no_ingress_errors();
                let error = match response.read_body_event(u32::MAX).await {
                    Ok(event) => panic!(
                        "the rejected frame left the source open, eom={}",
                        event.eom()
                    ),
                    Err(error) => error,
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);

                send(&upload, b"after");
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                let frames = upstream_frames(&observed);
                assert_eq!(frames.data_on(STREAM_1), b"beforeafter");
                assert!(frames.ended.contains(&STREAM_1));
                assert_eq!(frames.credit_for(STREAM_1), 0);
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
}

/// The peer ends an upgraded H2 tunnel first, and the reader consumes that
/// END_STREAM while the tunnel's egress keeps streaming. The source rejects the
/// peer's later DATA, padding-only DATA, and trailers, which only return
/// connection credit, and the egress finishes.
fn h2_peer_ends_tunnel_before_egress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                stream_receive_window: 4 * CONNECTION_WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let (session, handle, wire, peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                let accept = peer.borrow_mut().headers(STREAM_1, &status(200), false);
                let (mut response, upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                observed.deliver(peer.borrow_mut().data(STREAM_1, b"reply", true));
                assert_eq!(read_to_eom(&mut response).await, b"reply");
                send(&upload, b"ping");
                eventually("tunnel egress after the peer's END_STREAM", || {
                    upstream_frames(&observed).data_on(STREAM_1) == b"ping"
                })
                .await;

                window.send(STREAM_1, 2 * CONNECTION_WINDOW, false).await;
                window.send(STREAM_1, CONNECTION_WINDOW, true).await;
                observed.deliver(peer.borrow_mut().trailers(STREAM_1));
                ingress_consumed(&observed, &handle).await;
                assert_eq!(
                    upstream_frames(&observed).resets,
                    [],
                    "a frame after the peer's END_STREAM reset the upgraded tunnel"
                );
                lifecycle.assert_no_ingress_errors();
                assert!(!lifecycle.detached(STREAM_1));

                send(&upload, b"pong");
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                let frames = upstream_frames(&observed);
                assert_eq!(frames.data_on(STREAM_1), b"pingpong");
                assert!(frames.ended.contains(&STREAM_1));
                assert_eq!(frames.credit_for(STREAM_1), 0);
                observed.end();
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the tunnel closes cleanly");
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
}

#[test]
fn h1_upstream_connect_streams_egress_after_200() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (mut response, upload) = h1_established_tunnel(&handle, &peer).await;
                send(&upload, b"ping");
                eventually("the first tunnel write", || {
                    peer.written().ends_with(b"ping")
                })
                .await;
                send(&upload, b"pong");
                eventually("the second tunnel write", || {
                    peer.written().ends_with(b"pingpong")
                })
                .await;
                peer.deliver(b"reply".to_vec());
                assert_eq!(read_body(&mut response, 5).await, b"reply");
                peer.end();
                assert!(read_to_eom(&mut response).await.is_empty());
                send(&upload, b"done");
                h1_bytes_or_reset(&peer, b"pingpongdone").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "the peer's EOF reset a tunnel whose egress was open"
                );
                assert!(upload.eom());
                eventually("the end of tunnel egress", || {
                    lifecycle.egress_ended(STREAM_1)
                })
                .await;
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the tunnel closes cleanly");
            let written = wire.written();
            assert!(written.starts_with(b"CONNECT example:443 HTTP/1.1\r\n"));
            let head = h1_head_len(&written).expect("CONNECT head");
            assert_eq!(&written[head..], b"pingpongdone");
            wire.assert_fin_after_data();
        })
    });
}

#[test]
fn h2_upstream_connect_streams_egress_after_200() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (mut response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                send(&upload, b"ping");
                eventually("the first DATA frame", || {
                    upstream_frames(&observed).data_on(STREAM_1) == b"ping"
                })
                .await;
                send(&upload, b"pong");
                eventually("the second DATA frame", || {
                    upstream_frames(&observed).data_on(STREAM_1) == b"pingpong"
                })
                .await;
                observed.deliver(peer.data(STREAM_1, b"reply", false));
                assert_eq!(read_body(&mut response, 5).await, b"reply");
                assert!(upload.eom());
                eventually("END_STREAM", || {
                    upstream_frames(&observed).ended.contains(&STREAM_1)
                })
                .await;
                observed.deliver(peer.data(STREAM_1, b"", true));
                assert!(read_to_eom(&mut response).await.is_empty());
                eventually("the stream to detach", || lifecycle.detached(STREAM_1)).await;
                observed.end();
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the tunnel closes cleanly");
            let frames = upstream_frames(&wire);
            assert_eq!(frames.data_on(STREAM_1), b"pingpong");
            assert!(frames.ended.contains(&STREAM_1));
            assert_eq!(frames.resets, []);
        })
    });
    h2_peer_ends_tunnel_before_egress();
    for trailers in [false, true] {
        h2_late_frame_before_end_stream_is_read(trailers);
    }
}

#[test]
fn h1_abandoned_upgraded_stream_sends_no_tcp_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (response, upload) = h1_established_tunnel(&handle, &peer).await;
                send(&upload, b"before");
                eventually("the first tunnel write", || {
                    peer.written().ends_with(b"before")
                })
                .await;
                drop(response);
                peer.deliver(b"late".to_vec());
                send(&upload, b"after");
                h1_bytes_or_reset(&peer, b"after").await;
                ingress_consumed(&peer, &handle).await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "abandoning an upgraded stream reset the connection"
                );
                peer.end();
                send(&upload, b"final");
                h1_bytes_or_reset(&peer, b"final").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "the peer's EOF reset an abandoned tunnel whose egress was open"
                );
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the abandoned tunnel closes cleanly");
            assert!(wire.written().ends_with(b"beforeafterfinal"));
            wire.assert_fin_after_data();
        })
    });
}

#[test]
fn h2_abandoned_upgraded_stream_sends_no_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                drop(response);
                send(&upload, b"after");
                h2_data_or_reset(&observed, STREAM_1, b"after").await;
                assert_eq!(upstream_frames(&observed).resets, []);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                assert!(upstream_frames(&observed).ended.contains(&STREAM_1));
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
    for end_before_abandon in [false, true] {
        h2_late_data_after_peer_end_stream(end_before_abandon);
    }
    h2_abandoned_tunnel_peer_protocol_error_resets();
    h2_abandoned_tunnel_survives_peer_eof_in_the_same_read();
    h2_push_on_abandoned_tunnel_is_cancelled();
}

/// The peer promises a push on an abandoned tunnel with its egress still open.
/// The abandoned source stops the pushed source, so only the pushed stream
/// resets, with `CANCEL` and no ingress error.
fn h2_push_on_abandoned_tunnel_is_cancelled() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let events = Rc::clone(&lifecycle);
            let exchange = async move {
                let (response, upload) = h2_established_tunnel(
                    &handle,
                    &observed,
                    peer.headers(STREAM_1, &status(200), false),
                    connect_request(),
                )
                .await;
                drop(response);
                send(&upload, b"after");
                h2_data_or_reset(&observed, STREAM_1, b"after").await;
                assert!(!events.detached(STREAM_1));

                let pushed = peer.create_stream();
                let mut promise = HttpMessage::new();
                promise.set_method(HttpMethod::Get).expect("push method");
                promise.set_url(b"/push", false).expect("push path");
                promise.set_http_version(1, 1);
                promise.headers_mut().set(HttpHeaderCode::Host, b"example");
                observed.deliver(peer.frame(EgressFrame::PushPromise {
                    stream: pushed,
                    associated_stream: STREAM_1,
                    message: &promise,
                    end_stream: false,
                }));
                eventually("the pushed stream to reset", || {
                    !upstream_frames(&observed).resets.is_empty()
                })
                .await;
                assert_eq!(
                    upstream_frames(&observed).resets,
                    [(pushed, H2ErrorCode::CANCEL)]
                );
                assert_eq!(
                    *events.ingress_errors.borrow(),
                    [],
                    "the rejected push raised an HTTP/2 ingress error"
                );
                finish_upload(&upload, &events, STREAM_1).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                upstream_frames(&wire).resets,
                [(StreamId::new(2), H2ErrorCode::CANCEL)]
            );
        })
    });
}

/// A downstream HTTP/2 session whose handler accepts the peer's `request`, which
/// is already on the wire, with a 200 and leaves both directions to the test.
fn h2_downstream_tunnel(
    context: WorkerContext,
    request: &HttpMessage,
) -> (
    UniplexSession,
    UniplexHandle,
    Wire,
    H2Peer,
    Rc<Lifecycle>,
    Rc<TunnelSlots>,
) {
    let slots = Rc::new(TunnelSlots::default());
    let handler: HttpHandlerRef = Rc::new(TunnelHandler {
        response: status(200),
        slots: Rc::clone(&slots),
    });
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new_http2(
        context,
        TransportDirection::Downstream,
        wire.transport(),
        Some(handler),
        UniplexConfig::default(),
        Http2SessionConfig::default(),
    )
    .expect("downstream HTTP/2 session");
    let lifecycle = Lifecycle::attach(&session);
    let (mut peer, mut preface) = H2Peer::client();
    let stream = peer.create_stream();
    preface.extend(peer.headers(stream, request, false));
    wire.deliver(preface);
    (session, handle, wire, peer, lifecycle, slots)
}

#[test]
fn h2_downstream_abandoned_upgraded_stream_sends_no_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, peer, lifecycle, slots) =
                h2_downstream_tunnel(context, &connect_request());
            let observed = wire.clone();
            let running = &handle;
            let exchange = async move {
                eventually("the 200 response", || {
                    downstream_frames(&observed)
                        .statuses
                        .contains(&(STREAM_1, 200))
                })
                .await;
                drop(slots.request.take().expect("handler kept the request"));
                let upload = slots.response.take().expect("handler kept the response");
                send(&upload, b"after");
                eventually("DATA after abandonment, or a RST_STREAM", || {
                    let frames = downstream_frames(&observed);
                    frames.data_on(STREAM_1) == b"after" || !frames.resets.is_empty()
                })
                .await;
                assert_eq!(downstream_frames(&observed).resets, []);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                let frames = downstream_frames(&observed);
                assert!(frames.ended.contains(&STREAM_1));
                assert_eq!(frames.resets, []);

                // The peer's half of the detached stream is still open.
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: downstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                window.send(STREAM_1, 2 * CONNECTION_WINDOW, false).await;
                window.send(STREAM_1, CONNECTION_WINDOW, true).await;
                observed.deliver(peer.borrow_mut().trailers(STREAM_1));
                observed.deliver(peer.borrow_mut().frame(EgressFrame::Reset {
                    stream: STREAM_1,
                    code: H2ErrorCode::CANCEL,
                }));
                ingress_consumed(&observed, running).await;
                assert_eq!(downstream_frames(&observed).resets, []);
                lifecycle.assert_no_ingress_errors();
                assert!(!ingress_meta_retained(running, STREAM_1));
                assert!(!running.is_finished());
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            drop(handle);
            assert_eq!(downstream_frames(&wire).resets, []);
        })
    });
}

#[test]
fn h2_downstream_content_length_mismatch_sends_no_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut request = connect_request();
            request
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1");
            let (session, handle, wire, mut peer, lifecycle, slots) =
                h2_downstream_tunnel(context, &request);
            let observed = wire.clone();
            let running = &handle;
            let exchange = async move {
                eventually("the 200 response", || {
                    downstream_frames(&observed)
                        .statuses
                        .contains(&(STREAM_1, 200))
                })
                .await;
                let mut request = slots.request.take().expect("handler kept the request");
                let upload = slots.response.take().expect("handler kept the response");
                observed.deliver(peer.data(STREAM_1, b"", true));
                let error = loop {
                    match request.read_body_event(u32::MAX).await {
                        Ok(event) => assert!(!event.eom(), "the short body ended the source"),
                        Err(error) => break error,
                    }
                };
                assert_eq!(error.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
                drop(request);
                ingress_consumed(&observed, running).await;

                let accepted = upload.body(queue(b"after"), 0, false);
                eventually("DATA after the mismatch, or a RST_STREAM", || {
                    let frames = downstream_frames(&observed);
                    frames.data_on(STREAM_1) == b"after" || !frames.resets.is_empty()
                })
                .await;
                assert_eq!(
                    downstream_frames(&observed).resets,
                    [],
                    "the content-length mismatch reset the upgraded tunnel"
                );
                assert_ne!(accepted, FlowControlState::Error);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                let frames = downstream_frames(&observed);
                assert_eq!(frames.data_on(STREAM_1), b"after");
                assert!(frames.ended.contains(&STREAM_1));
                assert!(!running.is_finished());
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            drop(handle);
            assert_eq!(downstream_frames(&wire).resets, []);
        })
    });
}

/// The peer's EOF ends a downstream H1 tunnel short of its `Content-Length`. The
/// source fails with `CONTENT_LENGTH_MISMATCH` and the egress still finishes.
fn h1_downstream_content_length_mismatch_at_peer_eof_sends_no_tcp_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let slots = Rc::new(TunnelSlots::default());
            let handler: HttpHandlerRef = Rc::new(TunnelHandler {
                response: status(200),
                slots: Rc::clone(&slots),
            });
            let (session, _handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Downstream, Some(handler));
            wire.deliver(
                b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\nContent-Length: 10\r\n\r\n"
                    .to_vec(),
            );
            let peer = wire.clone();
            let exchange = async move {
                eventually("the 200 response", || {
                    peer.written().starts_with(b"HTTP/1.1 200")
                })
                .await;
                let mut request = slots.request.take().expect("handler kept the request");
                let upload = slots.response.take().expect("handler kept the response");
                peer.deliver(b"short".to_vec());
                peer.end();
                let error = loop {
                    match request.read_body_event(u32::MAX).await {
                        Ok(event) => assert!(!event.eom(), "the short body ended the source"),
                        Err(error) => break error,
                    }
                };
                assert_eq!(error.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
                drop(request);
                let accepted = upload.body(queue(b"after"), 0, false);
                h1_bytes_or_reset(&peer, b"after").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "the content-length mismatch reset the upgraded tunnel"
                );
                assert_ne!(accepted, FlowControlState::Error);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            wire.assert_fin_after_data();
        })
    });
}

#[test]
fn h1_downstream_abandoned_upgraded_stream_sends_no_tcp_reset() {
    h1_downstream_abandon(
        b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n",
        status(200),
        b"HTTP/1.1 200",
    );
    h1_downstream_content_length_mismatch_at_peer_eof_sends_no_tcp_reset();
}

#[test]
fn pending_connect_stream_abandoned_before_200_still_resets() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (mut response, _upload) = open_tunnel(&handle, connect_request()).await;
                observed.deliver(peer.headers(STREAM_1, &status(100), false));
                let mut head = response
                    .read_header_event()
                    .await
                    .expect("informational response");
                assert_eq!(head.headers_mut().status_code().expect("status"), 100);
                drop(response);
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;

                // Frames the peer sent before it read the RST_STREAM.
                let peer = RefCell::new(peer);
                observed.deliver(peer.borrow_mut().headers(STREAM_1, &status(200), false));
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                window.send(STREAM_1, 2 * CONNECTION_WINDOW, false).await;
                window.send(STREAM_1, CONNECTION_WINDOW, true).await;
                observed.deliver(peer.borrow_mut().trailers(STREAM_1));
                ingress_consumed(&observed, &handle).await;
                lifecycle.assert_no_ingress_errors();
                assert!(!ingress_meta_retained(&handle, STREAM_1));
                assert!(!handle.is_finished());
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                upstream_frames(&wire).resets,
                [(STREAM_1, H2ErrorCode::CANCEL)]
            );
        })
    });
}

#[test]
fn upgraded_stream_flow_control_error_still_resets() {
    h2_upstream_abandon_with(HttpErrorCode::FLOW_CONTROL_ERROR, |_, frames| {
        assert_eq!(frames.resets, [(STREAM_1, H2ErrorCode::FLOW_CONTROL_ERROR)]);
    });
    h1_upstream_abandon_with(HttpErrorCode::FLOW_CONTROL_ERROR, true);
}

#[test]
fn upgraded_stream_mapped_source_errors_send_no_reset() {
    for code in [
        HttpErrorCode::READ_TIMEOUT,
        HttpErrorCode::CORO_CANCELLED,
        HttpErrorCode::INVALID_STATE_TRANSITION,
        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
    ] {
        h2_upstream_abandon_with(code, |code, frames| {
            assert_eq!(frames.resets, [], "{code:?} reset an upgraded stream");
            assert!(frames.data_on(STREAM_1).ends_with(b"after"));
        });
        h1_upstream_abandon_with(code, false);
    }
}

#[test]
fn upgraded_stream_unmapped_source_error_still_resets() {
    h2_upstream_abandon_with(HttpErrorCode::INTERNAL_ERROR, |_, frames| {
        assert_eq!(frames.resets, [(STREAM_1, H2ErrorCode::INTERNAL_ERROR)]);
    });
    h1_upstream_abandon_with(HttpErrorCode::INTERNAL_ERROR, true);
}

#[test]
fn h1_downstream_websocket_101_abandon_sends_no_tcp_reset() {
    h1_downstream_abandon(
        b"GET / HTTP/1.1\r\nHost: foo.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        websocket_accept(),
        b"HTTP/1.1 101",
    );
}

#[test]
fn h1_upstream_websocket_101_abandon_sends_no_tcp_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (mut response, upload) = open_tunnel(&handle, h1_websocket_request()).await;
                eventually("the upgrade request", || {
                    h1_head_len(&peer.written()).is_some()
                })
                .await;
                peer.deliver(h1_websocket_101(&peer.written()));
                let mut head = response.read_header_event().await.expect("101 response");
                assert_eq!(head.headers_mut().status_code().expect("status"), 101);
                assert!(head.headers().is_ingress_websocket_upgrade());
                drop(response);
                peer.deliver(b"late".to_vec());
                send(&upload, b"after");
                h1_bytes_or_reset(&peer, b"after").await;
                ingress_consumed(&peer, &handle).await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "abandoning an upgraded stream reset the connection"
                );
                // The codec upgraded on the 101, so EOF completes its ingress.
                peer.end();
                send(&upload, b"final");
                h1_bytes_or_reset(&peer, b"final").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "the peer's EOF reset a WebSocket tunnel whose egress was open"
                );
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the abandoned WebSocket tunnel closes cleanly");
            assert!(wire.written().ends_with(b"afterfinal"));
            wire.assert_fin_after_data();
        })
    });
}

#[test]
fn h2_upstream_websocket_200_abandon_sends_no_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (mut response, upload) = open_tunnel(&handle, h2_websocket_request()).await;
                observed.deliver(peer.headers(STREAM_1, &status(200), false));
                let head = response.read_header_event().await.expect("200 response");
                assert!(head.headers().is_ingress_websocket_upgrade());
                drop(response);
                send(&upload, b"after");
                h2_data_or_reset(&observed, STREAM_1, b"after").await;
                assert_eq!(upstream_frames(&observed).resets, []);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(upstream_frames(&wire).resets, []);
        })
    });
}

#[test]
fn h1_non_200_2xx_connect_upgrade_state_is_pinned() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (mut response, _upload) = open_tunnel(&handle, connect_request()).await;
                eventually("the CONNECT head", || {
                    h1_head_len(&peer.written()).is_some()
                })
                .await;
                peer.deliver(b"HTTP/1.1 201 Created\r\n\r\nabc".to_vec());
                let mut head = response.read_header_event().await.expect("201 response");
                assert_eq!(head.headers_mut().status_code().expect("status"), 201);
                // The HTTP/1.1 codec tunnels after any 2xx; the session upgrades only on 200.
                assert!(head.headers().is_upgraded());
                assert_eq!(read_body(&mut response, 3).await, b"abc");
                drop(response);
                eventually("a TCP reset", || peer.close() == Close::Reset).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });
    // The codec's 2xx upgrade completes ingress at the peer's EOF, so the
    // tunnel's open egress still finishes.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (mut response, upload) = open_tunnel(&handle, connect_request()).await;
                eventually("the CONNECT head", || {
                    h1_head_len(&peer.written()).is_some()
                })
                .await;
                peer.deliver(b"HTTP/1.1 201 Created\r\n\r\nabc".to_vec());
                let mut head = response.read_header_event().await.expect("201 response");
                assert_eq!(head.headers_mut().status_code().expect("status"), 201);
                assert_eq!(read_body(&mut response, 3).await, b"abc");
                peer.end();
                assert!(read_to_eom(&mut response).await.is_empty());
                send(&upload, b"done");
                h1_bytes_or_reset(&peer, b"done").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "the peer's EOF reset a 201 tunnel whose egress was open"
                );
                assert!(upload.eom());
                eventually("the end of tunnel egress", || {
                    lifecycle.egress_ended(STREAM_1)
                })
                .await;
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the 201 tunnel closes cleanly");
            wire.assert_fin_after_data();
        })
    });
}

#[test]
fn h2_non_200_2xx_connect_upgrade_state_is_pinned() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_upstream(context, Http2SessionConfig::default());
            let observed = wire.clone();
            let exchange = async move {
                let (mut response, _upload) = open_tunnel(&handle, connect_request()).await;
                observed.deliver(peer.headers(STREAM_1, &status(201), false));
                let mut head = response.read_header_event().await.expect("201 response");
                assert_eq!(head.headers_mut().status_code().expect("status"), 201);
                drop(response);
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                upstream_frames(&wire).resets,
                [(STREAM_1, H2ErrorCode::CANCEL)]
            );
        })
    });
}

/// The peer's view of the session's connection receive window.
struct PeerWindow<'a> {
    wire: &'a Wire,
    written: fn(&Wire) -> Frames,
    peer: &'a RefCell<H2Peer>,
    sent: RefCell<usize>,
}

impl PeerWindow<'_> {
    fn available(&self) -> usize {
        (CONNECTION_WINDOW + (self.written)(self.wire).credit_for(StreamId::UNSPECIFIED))
            .saturating_sub(*self.sent.borrow())
    }

    /// Sends `total` flow-controlled bytes on `stream` without exceeding the
    /// window, as DATA or, with `padding_only`, as empty padded DATA frames.
    async fn send(&self, stream: StreamId, total: usize, padding_only: bool) {
        let mut remaining = total;
        while remaining != 0 {
            eventually("connection credit", || self.available() != 0).await;
            let mut peer = self.peer.borrow_mut();
            let (chunk, frame) = if padding_only {
                let chunk = self.available().min(remaining).min(256);
                (chunk, peer.padding(stream, chunk))
            } else {
                let chunk = self.available().min(remaining);
                (chunk, peer.data(stream, &vec![b'x'; chunk], false))
            };
            remaining -= chunk;
            *self.sent.borrow_mut() += chunk;
            self.wire.deliver(frame);
        }
    }
}

/// Abandons an upgraded H2 tunnel while unread DATA is still buffered in its
/// source. `stop_reading` returns that DATA's connection credit. It acknowledges
/// no stream credit, because the source's input counts as finished once
/// `stop_reading` sets the error.
fn h2_abandoned_tunnel_with_unread_data_returns_connection_credit() {
    run_on_worker(|context| {
        Box::pin(async move {
            // The connection window admits more unread DATA than half the stream
            // window, the threshold at which stream credit would be acknowledged.
            let config = Http2SessionConfig {
                connection_receive_window: 4 * CONNECTION_WINDOW as u32,
                stream_receive_window: 4 * CONNECTION_WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let unread = 3 * CONNECTION_WINDOW;
            let (session, handle, wire, peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                let accept = peer.borrow_mut().headers(STREAM_1, &status(200), false);
                let (abandoned, upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                window.send(STREAM_1, unread, false).await;
                ingress_consumed(&observed, &handle).await;
                let connection_credit =
                    || upstream_frames(&observed).credit_for(StreamId::UNSPECIFIED);
                let before = connection_credit();
                drop(abandoned);
                eventually("connection credit for the unread DATA", || {
                    connection_credit() >= before + unread
                })
                .await;
                let frames = upstream_frames(&observed);
                assert_eq!(frames.credit_for(StreamId::UNSPECIFIED), before + unread);
                assert_eq!(
                    frames.credit_for(STREAM_1),
                    0,
                    "the abandoned stream acknowledged its unread DATA"
                );
                assert_eq!(frames.resets, []);
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            let frames = upstream_frames(&wire);
            assert_eq!(frames.credit_for(STREAM_1), 0);
            assert_eq!(frames.resets, []);
        })
    });
}

#[test]
fn h2_abandoned_upgraded_stream_returns_credit_and_second_tunnel_progresses() {
    run_on_worker(|context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                stream_receive_window: 4 * CONNECTION_WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            assert_eq!(config.connection_receive_window as usize, CONNECTION_WINDOW);
            let (session, handle, wire, peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let peer = RefCell::new(peer);
                let window = PeerWindow {
                    wire: &observed,
                    written: upstream_frames,
                    peer: &peer,
                    sent: RefCell::new(0),
                };
                let accept = peer.borrow_mut().headers(STREAM_1, &status(200), false);
                let (abandoned, upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                drop(abandoned);
                // A third window needs two windows of returned connection credit, so the
                // attached stream has processed half its receive window, the threshold at
                // which stream credit would be acknowledged.
                window.send(STREAM_1, 3 * CONNECTION_WINDOW, false).await;
                // Padding is connection credit too, although these frames carry no body.
                window.send(STREAM_1, CONNECTION_WINDOW, true).await;
                assert!(!lifecycle.detached(STREAM_1));
                finish_upload(&upload, &lifecycle, STREAM_1).await;
                window.send(STREAM_1, CONNECTION_WINDOW, false).await;
                window.send(STREAM_1, CONNECTION_WINDOW, true).await;

                let accept = peer.borrow_mut().headers(STREAM_3, &status(200), false);

                let (mut second, second_upload) =
                    h2_established_tunnel(&handle, &observed, accept, connect_request()).await;
                let (received, ()) = tokio::join!(
                    read_body(&mut second, CONNECTION_WINDOW),
                    window.send(STREAM_3, CONNECTION_WINDOW, false),
                );
                assert_eq!(received.len(), CONNECTION_WINDOW);
                let frames = upstream_frames(&observed);
                assert_eq!(
                    frames.credit_for(STREAM_1),
                    0,
                    "the abandoned stream received stream credit"
                );
                assert_eq!(frames.resets, []);

                observed.deliver(peer.borrow_mut().data(STREAM_3, b"", true));
                assert!(read_to_eom(&mut second).await.is_empty());
                finish_upload(&second_upload, &lifecycle, STREAM_3).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
        })
    });
    h2_abandoned_tunnel_with_unread_data_returns_connection_credit();
}

#[test]
fn abandoned_upgraded_stream_detaches_once_egress_completes() {
    run_on_worker(|context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                max_concurrent_outgoing_streams: 1,
                ..Http2SessionConfig::default()
            };
            let (session, handle, wire, mut peer, lifecycle) = h2_upstream(context, config);
            let observed = wire.clone();
            let exchange = async move {
                let tunnels = [1, 3, 5, 7].map(StreamId::new);
                for stream in tunnels {
                    let (response, upload) = h2_established_tunnel(
                        &handle,
                        &observed,
                        peer.headers(stream, &status(200), false),
                        connect_request(),
                    )
                    .await;
                    drop(response);
                    send(&upload, b"after");
                    h2_data_or_reset(&observed, stream, b"after").await;
                    assert!(
                        !lifecycle.detached(stream),
                        "the abandoned stream detached before its egress completed"
                    );
                    finish_upload(&upload, &lifecycle, stream).await;
                    assert!(upstream_frames(&observed).ended.contains(&stream));
                    assert!(
                        !ingress_meta_retained(&handle, stream),
                        "the detached stream kept its ingress record for the peer's open half"
                    );
                    observed.deliver(peer.data(stream, b"late", false));
                }
                observed.deliver(peer.frame(EgressFrame::Reset {
                    stream: tunnels[0],
                    code: H2ErrorCode::CANCEL,
                }));
                observed.deliver(peer.data(tunnels[1], b"", true));
                observed.deliver(peer.headers(tunnels[2], &status(200), false));
                observed.deliver(peer.trailers(tunnels[3]));
                ingress_consumed(&observed, &handle).await;
                assert_eq!(upstream_frames(&observed).resets, []);
                lifecycle.assert_no_ingress_errors();
                assert!(!handle.is_finished());
                assert!(
                    tunnels
                        .iter()
                        .all(|stream| !ingress_meta_retained(&handle, *stream))
                );

                // A push promised on an erased stream is ignored.
                let pushed = peer.create_stream();
                let mut promise = HttpMessage::new();
                promise.set_method(HttpMethod::Get).expect("push method");
                promise.set_url(b"/push", false).expect("push path");
                promise.set_http_version(1, 1);
                promise.headers_mut().set(HttpHeaderCode::Host, b"example");
                observed.deliver(peer.frame(EgressFrame::PushPromise {
                    stream: pushed,
                    associated_stream: tunnels[0],
                    message: &promise,
                    end_stream: false,
                }));
                observed.deliver(peer.headers(pushed, &status(200), false));
                observed.deliver(peer.data(pushed, b"pushed", true));
                ingress_consumed(&observed, &handle).await;
                assert_eq!(upstream_frames(&observed).resets, []);
                lifecycle.assert_no_ingress_errors();
                assert!(!ingress_meta_retained(&handle, pushed));

                let next_stream = StreamId::new(9);
                let request = HttpFixedSource::make_get_request(b"/next", None).expect("GET");
                let mut next = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("the session admits another request")
                    .source;
                observed.deliver(peer.headers(next_stream, &status(200), true));
                let head = next.read_header_event().await.expect("next response");
                assert!(head.eom());
                eventually("the next stream to detach", || {
                    lifecycle.detached(next_stream)
                })
                .await;

                // A malformed header block on an erased stream still gets a RST_STREAM.
                observed.deliver(duplicate_status_headers(tunnels[1]));
                ingress_consumed(&observed, &handle).await;
                eventually("a RST_STREAM", || {
                    !upstream_frames(&observed).resets.is_empty()
                })
                .await;
                assert_eq!(
                    *lifecycle.ingress_errors.borrow(),
                    [(tunnels[1], HttpErrorCode::PROTOCOL_ERROR)]
                );
                assert!(!ingress_meta_retained(&handle, tunnels[1]));
                assert!(!handle.is_finished());
                observed.end();
            };
            let (run, ()) = tokio::join!(session.run(), exchange);
            run.expect("the session closes cleanly");
            assert_eq!(
                upstream_frames(&wire).resets,
                [(StreamId::new(3), H2ErrorCode::PROTOCOL_ERROR)]
            );
        })
    });
}

/// A malformed HEADERS frame that repeats `:status`.
fn duplicate_status_headers(stream: StreamId) -> Vec<u8> {
    const HEADERS: u8 = 0x1;
    const END_HEADERS: u8 = 0x4;
    const INDEXED_STATUS_200: u8 = 0x88;
    let id = u32::try_from(stream.get()).expect("stream ID fits 31 bits");
    let mut frame = vec![0, 0, 2, HEADERS, END_HEADERS];
    frame.extend_from_slice(&id.to_be_bytes());
    frame.extend_from_slice(&[INDEXED_STATUS_200, INDEXED_STATUS_200]);
    frame
}

/// The upstream session that carries a stalled abandoned tunnel.
enum Tunnel {
    H1,
    H2(H2Peer),
}

/// Abandons a tunnel whose later egress write stalls while the peer stays
/// silent, then returns once `end`, which runs on a paused clock, has ended the
/// session.
async fn abandoned_tunnel_with_stalled_egress(
    session: UniplexSession,
    handle: UniplexHandle,
    wire: Wire,
    tunnel: Tunnel,
    lifecycle: Rc<Lifecycle>,
    end: impl AsyncFnOnce(&UniplexHandle, &Wire),
) -> Result<crate::uniplex::UniplexRunSummary, HttpError> {
    let h2 = matches!(tunnel, Tunnel::H2(_));
    let observed = wire.clone();
    let exchange = async move {
        let (response, upload) = match tunnel {
            Tunnel::H1 => h1_established_tunnel(&handle, &observed).await,
            Tunnel::H2(mut peer) => {
                let accept = peer.headers(STREAM_1, &status(200), false);
                h2_established_tunnel(&handle, &observed, accept, connect_request()).await
            }
        };
        drop(response);
        send(&upload, b"after");
        if h2 {
            h2_data_or_reset(&observed, STREAM_1, b"after").await;
        } else {
            h1_bytes_or_reset(&observed, b"after").await;
        }
        // `end` runs on a paused clock, so the stalled write's deadline expires only
        // when `end` advances the clock.
        tokio::time::pause();
        observed.stall_writes();
        send(&upload, b"stalled");
        until_idle().await;
        assert_ne!(
            observed.pending_writes(),
            0,
            "the tunnel write did not stall"
        );
        end(&handle, &observed).await;
        tokio::time::resume();
        eventually("the stream to detach", || lifecycle.detached(STREAM_1)).await;
        assert_eq!(
            upload.body(queue(b"late"), 0, false),
            FlowControlState::Error,
            "the upload outlived its session"
        );
    };
    let finished = tokio::time::timeout(WAIT_DEADLINE, async {
        tokio::join!(session.run(), exchange)
    })
    .await;
    let (run, ()) = finished.expect("the session outlived its stalled tunnel");
    assert_eq!(wire.pending_writes(), 0);
    assert_eq!(wire.close(), Close::Reset);
    if h2 {
        assert!(!upstream_frames(&wire).ended.contains(&STREAM_1));
    } else {
        assert!(wire.written().ends_with(b"after"));
    }
    run
}

/// An upstream session for a stalled abandoned tunnel, on H1 or H2.
fn stalled_tunnel_session(
    context: WorkerContext,
    h2: bool,
    uniplex: UniplexConfig,
) -> (UniplexSession, UniplexHandle, Wire, Tunnel, Rc<Lifecycle>) {
    if h2 {
        let (session, handle, wire, peer, lifecycle) =
            h2_upstream_with(context, uniplex, Http2SessionConfig::default());
        return (session, handle, wire, Tunnel::H2(peer), lifecycle);
    }
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new(
        context,
        TransportDirection::Upstream,
        wire.transport(),
        None,
        uniplex,
    )
    .expect("HTTP/1.1 session");
    let lifecycle = Lifecycle::attach(&session);
    (session, handle, wire, Tunnel::H1, lifecycle)
}

#[test]
fn abandoned_upgraded_stream_with_open_egress_ends_with_session() {
    for h2 in [true, false] {
        // Local cancellation drains, then resets the transport, as
        // `ClientSession::cancel` does.
        run_on_worker(move |context| {
            Box::pin(async move {
                let (session, handle, wire, tunnel, lifecycle) =
                    stalled_tunnel_session(context.clone(), h2, UniplexConfig::default());
                let run = abandoned_tunnel_with_stalled_egress(
                    session,
                    handle,
                    wire,
                    tunnel,
                    lifecycle,
                    async |handle, wire| {
                        // The H1 codec is not reusable after the upgrade, so that
                        // session is already draining.
                        assert_eq!(handle.initiate_drain().expect("drain"), h2);
                        until_idle().await;
                        assert!(
                            !handle.is_finished(),
                            "draining retired an abandoned stream whose egress was open"
                        );
                        wire.transport()
                            .close_with_reset(&context)
                            .expect("local reset");
                    },
                )
                .await;
                run.expect_err("a locally reset connection fails the session");
            })
        });
        // The write deadline ends the session without any local action.
        run_on_worker(move |context| {
            Box::pin(async move {
                const WRITE_DEADLINE: Duration = Duration::from_millis(50);
                let uniplex = UniplexConfig {
                    write_timeout: WRITE_DEADLINE,
                    ..UniplexConfig::default()
                };
                let (session, handle, wire, tunnel, lifecycle) =
                    stalled_tunnel_session(context, h2, uniplex);
                let run = abandoned_tunnel_with_stalled_egress(
                    session,
                    handle,
                    wire,
                    tunnel,
                    lifecycle,
                    async |_, _| tokio::time::advance(WRITE_DEADLINE).await,
                )
                .await;
                assert_eq!(
                    run.expect_err("a stalled write fails the session").code(),
                    HttpErrorCode::WRITE_TIMEOUT
                );
            })
        });
    }
}

#[test]
fn h1_abandoned_upgraded_stream_detach_closes_connection_without_reset() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) =
                h1_session(context, TransportDirection::Upstream, None);
            let peer = wire.clone();
            let exchange = async move {
                let (response, upload) = h1_established_tunnel(&handle, &peer).await;
                drop(response);
                send(&upload, b"last");
                assert!(upload.eom());
            };
            let finished = tokio::time::timeout(WAIT_DEADLINE, async {
                tokio::join!(session.run(), exchange)
            })
            .await;
            assert!(
                finished.is_ok(),
                "the session waited for the peer after the upgraded stream detached"
            );
            assert!(wire.written().ends_with(b"last"));
            wire.assert_fin_after_data();
        })
    });
    for tls in [false, true] {
        run_on_worker(move |context| Box::pin(h1_socket_teardown(context, tls)));
    }
}

#[test]
fn h1_abandoned_upgraded_stream_paused_at_ingress_limit_resumes_reading() {
    const INGRESS_LIMIT: usize = 1024;
    run_on_worker(|context| {
        Box::pin(async move {
            let wire = Wire::new();
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                wire.transport(),
                None,
                UniplexConfig {
                    ingress_buffer_limit: INGRESS_LIMIT as u32,
                    ..UniplexConfig::default()
                },
            )
            .expect("HTTP/1.1 session");
            let lifecycle = Lifecycle::attach(&session);
            let peer = wire.clone();
            let exchange = async move {
                let (response, upload) = h1_established_tunnel(&handle, &peer).await;
                peer.deliver(vec![b'x'; INGRESS_LIMIT]);
                peer.deliver(b"queued".to_vec());
                eventually("the read of the first tunnel bytes", || {
                    peer.queued_reads() <= 1
                })
                .await;
                settle().await;
                assert_eq!(
                    peer.queued_reads(),
                    1,
                    "the session read past its ingress buffer limit"
                );

                drop(response);
                ingress_consumed(&peer, &handle).await;
                peer.deliver(b"late".to_vec());
                ingress_consumed(&peer, &handle).await;
                send(&upload, b"after");
                h1_bytes_or_reset(&peer, b"after").await;
                assert_eq!(
                    peer.close(),
                    Close::Open,
                    "abandoning a paused tunnel reset the connection"
                );
                finish_upload(&upload, &lifecycle, STREAM_1).await;
            };
            let finished = tokio::time::timeout(2 * WAIT_DEADLINE, async {
                tokio::join!(session.run(), exchange)
            })
            .await;
            let (run, ()) = finished.expect("the session outlived its abandoned tunnel");
            run.expect("the abandoned tunnel closes cleanly");
            assert!(wire.written().ends_with(b"after"));
            wire.assert_fin_after_data();
        })
    });
}

fn tls_fixture() -> (Vec<u8>, Arc<ServerConfig>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let generated =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("certificate");
    let certificate = generated.cert.der().to_vec();
    let private_key = PrivateKeyDer::try_from(generated.signing_key.serialized_der().to_vec())
        .expect("private key");
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(certificate.clone())], private_key)
        .expect("server configuration");
    (certificate, Arc::new(config))
}

/// A proxy that accepts the CONNECT and returns every tunnel byte it reads
/// through a clean end of stream. A TCP reset fails the read, and so does a
/// TLS end of stream without a preceding close_notify.
async fn h1_connect_proxy(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
) -> std::io::Result<Vec<u8>> {
    let mut head = Vec::new();
    while h1_head_len(&head).is_none() {
        head.push(stream.read_u8().await?);
    }
    assert!(head.starts_with(b"CONNECT example:443 HTTP/1.1\r\n"));
    stream.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await?;
    stream.flush().await?;
    let mut tunnel = Vec::new();
    stream.read_to_end(&mut tunnel).await?;
    Ok(tunnel)
}

/// Abandons an H1 CONNECT tunnel over loopback TCP, or TLS over TCP, after its
/// last upload write, and checks what the proxy reads.
async fn h1_socket_teardown(context: WorkerContext, tls: bool) {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("loopback listener");
    let local = listener.local_addr().expect("listener address");
    let address = SocketAddress::new(local.ip().to_string(), local.port());
    let limits = ByteStreamLimits::new(4096, 4096, 16).expect("limits");
    let (certificate, server_config) = tls_fixture();
    let proxy = async move {
        let (socket, _) = listener.accept().await?;
        let (tunnel, mut socket) = if tls {
            let mut stream = TlsAcceptor::from(server_config).accept(socket).await?;
            let tunnel = h1_connect_proxy(&mut stream).await?;
            (tunnel, stream.into_inner().0)
        } else {
            let mut socket = socket;
            (h1_connect_proxy(&mut socket).await?, socket)
        };
        // A TLS end of stream is close_notify alone, so the FIN is read from TCP.
        let mut trailing = Vec::new();
        tokio::time::timeout(WAIT_DEADLINE, socket.read_to_end(&mut trailing))
            .await
            .expect("a FIN while the client still holds its socket")?;
        assert_eq!(trailing, b"", "bytes followed the end of the tunnel");
        Ok::<_, std::io::Error>(tunnel)
    };
    let client = async {
        let transport: Rc<dyn UniplexTransport> = if tls {
            let mut options = TlsClientOptions::new("localhost");
            options.add_root_certificate(certificate);
            Rc::new(
                TlsTransport::connect(
                    &context,
                    address,
                    options,
                    limits,
                    WAIT_DEADLINE,
                    CancellationToken::never(),
                )
                .await
                .expect("TLS connection"),
            )
        } else {
            Rc::new(
                TcpTransport::connect(
                    &context,
                    address,
                    limits,
                    WAIT_DEADLINE,
                    CancellationToken::never(),
                )
                .await
                .expect("TCP connection"),
            )
        };
        // Holding the transport past the session keeps its socket from closing on drop.
        let held = Rc::clone(&transport);
        let (session, handle) = UniplexSession::new(
            context.clone(),
            TransportDirection::Upstream,
            transport,
            None,
            UniplexConfig::default(),
        )
        .expect("HTTP/1.1 session");
        let exchange = async move {
            let (mut response, upload) = open_tunnel(&handle, connect_request()).await;
            let mut head = response
                .read_header_event()
                .await
                .expect("CONNECT response");
            assert_eq!(head.headers_mut().status_code().expect("status"), 200);
            drop(response);
            send(&upload, b"last");
            assert!(upload.eom());
        };
        let (run, ()) = tokio::join!(session.run(), exchange);
        run.expect("the abandoned tunnel closes cleanly");
        held
    };
    let (tunnel, held) = tokio::join!(proxy, client);
    assert_eq!(
        tunnel.expect("the proxy read a FIN, after close_notify on TLS"),
        b"last"
    );
    drop(held);
}
