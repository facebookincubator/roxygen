//! Opt-in abort of open exchanges on a downstream HTTP/1.1 peer's EOF
//! (`ServerSessionConfig::abort_exchanges_on_eof`, `DIV-053`).
//!
//! Each HTTP/1.1 case runs in every server mode in `HTTP1_MODES`, on both
//! workers. HTTP/2 connections are unchanged by the option, on `UniplexSession`
//! and on the owned driver (`ServerSessionConfig::owned_http2`) alike.

use std::cell::Cell;
use std::future::Future;
use std::io::Write;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::tcp::TcpTransport;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::Notify;

use crate::acceptor::ServerSessionConfig;
use crate::connection::ServeTransportOptions;
use crate::connection::ServerConnection;
use crate::connection::serve_transport;

const TIMEOUT: Duration = Duration::from_secs(10);
/// How long a half-closed connection must keep serving a held exchange.
const SETTLE: Duration = Duration::from_millis(100);
const REQUEST: &[u8] = b"GET /held HTTP/1.1\r\nHost: localhost\r\n\r\n";
const CLOSING_REQUEST: &[u8] =
    b"GET /held HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
const HTTP10_REQUEST: &[u8] = b"GET /held HTTP/1.0\r\nHost: localhost\r\n\r\n";

/// The HTTP/1.1 server modes each case runs in, as `owned_http1` values:
/// `UniplexSession` only, since the owned driver refuses `abort_exchanges_on_eof`.
const HTTP1_MODES: &[bool] = &[false];

fn wait<Work: Future>(future: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, future)
                .await
                .expect("bounded test completion")
        })
}

/// Runs `test` on a fresh `RuntimeOwner` worker, which must then quiesce without
/// forcing, and again on a `HostedWorker`, whose commands and cleanups are
/// independent Tokio tasks that only their own wakeups poll.
pub(super) fn on_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Clone + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    on_runtime_owner(test.clone());
    on_hosted_worker(test);
}

fn on_hosted_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "eof-abort".to_owned(),
        on_owner_thread_stop: None,
    })
    .expect("hosted worker starts");
    wait(worker.handle().call_local(test).expect("test admitted")).expect("test finishes");
    let completion = worker.completion();
    drop(worker);
    assert!(
        matches!(*completion.borrow(), Some(Ok(()))),
        "the hosted worker joins cleanly"
    );
}

fn on_runtime_owner<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(worker.call_local(test).expect("test admitted")).expect("test finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// Runs `test` in each HTTP/1.1 server mode in `HTTP1_MODES` (`false` serves
/// on `UniplexSession`, `true` on the owned driver), each on both workers. Each
/// mode is printed before it runs, so a failure's output names it.
fn in_both_modes<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext, bool) -> Work + Clone + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    for &owned_http1 in HTTP1_MODES {
        eprintln!("owned_http1: {owned_http1}");
        let test = test.clone();
        on_worker(move |context| test(context, owned_http1));
    }
}

/// An untimed server, serving HTTP/1.1 on the owned driver when `owned_http1`.
fn session(owned_http1: bool, abort_exchanges_on_eof: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1,
        abort_exchanges_on_eof,
        ..ServerSessionConfig::default()
    }
}

/// Serves one accepted loopback connection on this worker and returns its client.
async fn serve_loopback(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
    handler: HttpHandlerRef,
) -> (ServerConnection, TcpStream) {
    serve_loopback_with_buffers(context, protocol, session, handler, None).await
}

/// As `serve_loopback`, optionally limiting the server's send buffer and the
/// client's receive buffer to `socket_buffer` bytes.
async fn serve_loopback_with_buffers(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
    handler: HttpHandlerRef,
    socket_buffer: Option<u32>,
) -> (ServerConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let socket = TcpSocket::new_v4().expect("client socket");
    if let Some(size) = socket_buffer {
        socket
            .set_recv_buffer_size(size)
            .expect("client receive buffer");
    }
    let client = socket
        .connect(listener.local_addr().expect("listener address"))
        .await
        .expect("client connects");
    let (accepted, _) = listener.accept().await.expect("connection accepted");
    if let Some(size) = socket_buffer {
        socket2::SockRef::from(&accepted)
            .set_send_buffer_size(size as usize)
            .expect("server send buffer");
    }
    let transport = TcpTransport::from_accepted(
        context,
        accepted.into_std().expect("standard stream"),
        session.byte_stream_limits,
    )
    .expect("transport adopted");
    let mut options = ServeTransportOptions::new(protocol, TransportSecurity::Plaintext);
    options.session = session;
    let connection = serve_transport(context, transport, handler, options).expect("attached");
    (connection, client)
}

async fn until(condition: impl Fn() -> bool) {
    while !condition() {
        tokio::task::yield_now().await;
    }
}

#[derive(Default)]
struct Held {
    invoked: Cell<bool>,
    dropped: Cell<bool>,
    release: Notify,
}

/// Sets `dropped` unless disarmed by completion.
struct DropProbe(Option<Rc<Held>>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        if let Some(held) = self.0.take() {
            held.dropped.set(true);
        }
    }
}

/// Holds each exchange until released, then answers 200 with `held`.
struct HeldHandler(Rc<Held>);

impl HttpHandler for HeldHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.0);
        held.invoked.set(true);
        Box::pin(async move {
            let _request = request;
            let mut probe = DropProbe(Some(Rc::clone(&held)));
            held.release.notified().await;
            probe.0 = None;
            let body = BufQueue::from_bytes(Bytes::from_static(b"held"), 4).expect("body");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body)).expect("response"),
            )))
        })
    }
}

/// What happened to a streamed response source.
#[derive(Default)]
struct SourceProbe {
    stopped: Cell<bool>,
    dropped: Cell<bool>,
    produced: Cell<usize>,
}

/// A streamed response whose body never ends: with `endless`, each read yields
/// a chunk at once, so the response fills a peer that stops reading; otherwise
/// no body ever arrives.
struct StreamingBody {
    header: Option<HttpHeaderEvent>,
    probe: Rc<SourceProbe>,
    endless: bool,
}

static CHUNK: [u8; 16384] = [b'x'; 16384];

impl HttpSource for StreamingBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if !self.endless {
            return Box::pin(std::future::pending());
        }
        let length = usize::try_from(maximum)
            .unwrap_or(usize::MAX)
            .clamp(1, CHUNK.len());
        self.probe.produced.set(self.probe.produced.get() + length);
        let chunk = BufQueue::from_bytes(Bytes::from_static(&CHUNK[..length]), length)
            .expect("bounded chunk");
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(chunk, false))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.probe.stopped.set(true);
    }
}

impl Drop for StreamingBody {
    fn drop(&mut self) {
        self.probe.dropped.set(true);
    }
}

struct StreamingHandler {
    probe: Rc<SourceProbe>,
    endless: bool,
}

impl StreamingHandler {
    fn new(probe: &Rc<SourceProbe>, endless: bool) -> Rc<Self> {
        Rc::new(Self {
            probe: Rc::clone(probe),
            endless,
        })
    }
}

impl HttpHandler for StreamingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = StreamingBody {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            probe: Rc::clone(&self.probe),
            endless: self.endless,
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Waits until an endless response stops producing, which it does once the
/// transport stops taking its bytes.
async fn until_stalled(probe: &SourceProbe) {
    loop {
        let before = probe.produced.get();
        tokio::time::sleep(Duration::from_millis(50)).await;
        if before != 0 && probe.produced.get() == before {
            return;
        }
    }
}

/// Socket buffers too small to take a whole `FINAL_BATCH` response.
const SMALL_SOCKET_BUFFER: u32 = 4096;
/// A response body that the session serializes in one batch with its end, as it
/// reads up to 64 KiB per body event, and that the small socket buffers cannot hold.
const FINAL_BATCH: usize = 32 << 10;

/// A finished response of `FINAL_BATCH` bytes, served in as few body events as
/// the session allows; `produced` counts the bytes the session has taken.
struct FinalBatchBody {
    header: Option<HttpHeaderEvent>,
    probe: Rc<SourceProbe>,
}

impl HttpSource for FinalBatchBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let produced = self.probe.produced.get();
        let length = usize::try_from(maximum)
            .unwrap_or(usize::MAX)
            .clamp(1, FINAL_BATCH - produced);
        self.probe.produced.set(produced + length);
        let chunk =
            BufQueue::from_bytes(Bytes::from(vec![b'x'; length]), length).expect("bounded chunk");
        let eom = produced + length == FINAL_BATCH;
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(chunk, eom))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.probe.stopped.set(true);
    }
}

struct FinalBatchHandler(Rc<SourceProbe>);

impl HttpHandler for FinalBatchHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = FinalBatchBody {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            probe: Rc::clone(&self.0),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Waits until the session has serialized the whole `FINAL_BATCH` response and
/// the bytes reaching `client`, which does not read, stop growing; returns how
/// many arrived.
async fn until_final_batch_blocked(probe: &SourceProbe, client: &TcpStream) -> usize {
    until(|| probe.produced.get() == FINAL_BATCH).await;
    let mut wire = vec![0; 2 * FINAL_BATCH];
    let mut arrived = 0;
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = client.peek(&mut wire).await.expect("client peeks");
        if now != 0 && now == arrived {
            return now;
        }
        arrived = now;
    }
}

/// Reads the whole request body and drops the request before holding the
/// exchange, as a proxy that forwarded the request does while it waits.
struct ConsumingHandler(Rc<Held>);

impl HttpHandler for ConsumingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.0);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let mut eom = head.eom();
            while !eom {
                eom = request.read_body_event(u32::MAX).await?.eom();
            }
            drop(request);
            let mut probe = DropProbe(Some(Rc::clone(&held)));
            held.invoked.set(true);
            held.release.notified().await;
            probe.0 = None;
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// Reads the request head, stops the upload with `NO_ERROR` and holds the
/// exchange until released, as a handler that answers without the body does.
struct StoppingHandler(Rc<Held>);

impl HttpHandler for StoppingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.0);
        Box::pin(async move {
            request.read_header_event().await?;
            request.stop_reading(Some(HttpErrorCode::NO_ERROR));
            let mut probe = DropProbe(Some(Rc::clone(&held)));
            held.invoked.set(true);
            held.release.notified().await;
            probe.0 = None;
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// Reads the request body and answers 413 when it fails, as an upload limit does.
struct UploadHandler;

impl HttpHandler for UploadHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let mut eom = head.eom();
            while !eom {
                match request.read_body_event(u32::MAX).await {
                    Ok(event) => eom = event.eom(),
                    Err(_) => {
                        return Ok(HttpSourceHolder::new(Box::new(
                            HttpFixedSource::make_fixed_response(413, None).expect("response"),
                        )));
                    }
                }
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// Hands each request, its head read, to the handler registered for its path.
struct RouteHandler(Vec<(&'static [u8], HttpHandlerRef)>);

impl HttpHandler for RouteHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let routes = self.0.clone();
        Box::pin(async move {
            let mut head = request.read_header_event().await?;
            let path = head.headers_mut().path().expect("request path").to_vec();
            let (_, handler) = routes
                .iter()
                .find(|(route, _)| *route == path.as_slice())
                .expect("a handler serves the path");
            handler.handle_request(context, request).await
        })
    }
}

/// Answers `/held` as `ConsumingHandler` does and anything else at once with `ok`.
struct KeepAliveHandler(Rc<Held>);

impl HttpHandler for KeepAliveHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.0);
        Box::pin(async move {
            let mut head = request.read_header_event().await?;
            if head.headers_mut().path().expect("request path") != b"/held" {
                let body = BufQueue::from_bytes(Bytes::from_static(b"ok"), 2).expect("body");
                return Ok(HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_fixed_response(200, Some(body)).expect("response"),
                )));
            }
            let mut eom = head.eom();
            while !eom {
                eom = request.read_body_event(u32::MAX).await?.eom();
            }
            drop(request);
            let mut probe = DropProbe(Some(Rc::clone(&held)));
            held.invoked.set(true);
            held.release.notified().await;
            probe.0 = None;
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// Runs `count` keep-alive exchanges with `KeepAliveHandler`, one at a time.
async fn keep_alive_exchanges(client: &mut TcpStream, count: usize) {
    for _ in 0..count {
        client
            .write_all(b"GET /fast HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("request written");
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\nok") {
            let mut chunk = [0; 512];
            let read = client.read(&mut chunk).await.expect("response read");
            assert_ne!(read, 0, "the response arrives before close");
            response.extend_from_slice(&chunk[..read]);
        }
        assert!(response.starts_with(b"HTTP/1.1 200"), "{response:?}");
    }
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small frame");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// The client preface, empty SETTINGS, and a complete GET on stream 1:
/// GET / on localhost.
fn h2_request() -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend(h2_frame(0x4, 0, 0, &[]));
    // :method GET, :scheme http, :path /, then :authority localhost.
    let mut block = vec![0x82, 0x86, 0x84, 0x41, 9];
    block.extend_from_slice(b"localhost");
    wire.extend(h2_frame(0x1, 0x5, 1, &block));
    wire
}

const H2_DATA: u8 = 0x0;
const H2_HEADERS: u8 = 0x1;
const H2_RST_STREAM: u8 = 0x3;
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;
const PRIORITY: u8 = 0x20;
/// HPACK static table index 8, `:status: 200`.
const STATUS_200: u8 = 0x88;

/// One HTTP/2 frame on a stream.
#[derive(Debug, PartialEq)]
struct H2Frame<'a> {
    kind: u8,
    flags: u8,
    payload: &'a [u8],
}

/// The frames on `stream` in `wire`, which must hold whole frames.
fn h2_stream_frames(mut wire: &[u8], stream: u32) -> Result<Vec<H2Frame<'_>>, &'static str> {
    let mut frames = Vec::new();
    while !wire.is_empty() {
        let header = wire.get(..9).ok_or("truncated frame header")?;
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let payload = wire.get(9..9 + length).ok_or("truncated frame payload")?;
        if id == stream {
            frames.push(H2Frame {
                kind: header[3],
                flags: header[4],
                payload,
            });
        }
        wire = &wire[9 + length..];
    }
    Ok(frames)
}

/// Asserts that `wire` holds whole frames and carries on `stream` one complete
/// response: a HEADERS frame whose block starts with `:status: 200`, then only
/// DATA frames, which hold exactly `body`, the last ending the stream. After
/// the end, a RST_STREAM with `NO_ERROR` may follow, as `UniplexSession` sends
/// in these tests and the owned driver does not; any other frame on the
/// stream, a RST_STREAM before the end included, fails.
fn assert_whole_h2_response(wire: &[u8], stream: u32, body: &[u8]) {
    let frames = h2_stream_frames(wire, stream).expect("the server writes whole frames");
    let Some((head, rest)) = frames.split_first() else {
        panic!("no frame on stream {stream}");
    };
    assert_eq!(
        (
            head.kind,
            head.flags & (END_STREAM | END_HEADERS | PADDED | PRIORITY)
        ),
        (H2_HEADERS, END_HEADERS),
        "one HEADERS frame opens the response: {frames:?}"
    );
    assert_eq!(
        head.payload.first(),
        Some(&STATUS_200),
        "the status is 200: {frames:?}"
    );
    let end = rest
        .iter()
        .position(|frame| frame.flags & END_STREAM != 0)
        .map_or(rest.len(), |end| end + 1);
    let (data, after) = rest.split_at(end);
    assert!(
        data.iter()
            .all(|frame| frame.kind == H2_DATA && frame.flags & PADDED == 0),
        "only unpadded DATA frames carry the body: {frames:?}"
    );
    let written: Vec<u8> = data
        .iter()
        .flat_map(|frame| frame.payload.iter().copied())
        .collect();
    assert_eq!(written, body, "the whole body is written: {frames:?}");
    assert!(
        data.last()
            .is_some_and(|frame| frame.flags & END_STREAM != 0),
        "a DATA frame ends the stream: {frames:?}"
    );
    assert!(
        after.len() <= 1
            && after
                .iter()
                .all(|frame| frame.kind == H2_RST_STREAM && frame.payload == [0; 4]),
        "after the end, at most a RST_STREAM with NO_ERROR follows: {frames:?}"
    );
}

#[test]
fn h2_stream_frames_rejects_truncated_frames() {
    let frame = h2_frame(H2_DATA, END_STREAM, 1, b"held");
    assert_eq!(
        h2_stream_frames(&frame[..5], 1),
        Err("truncated frame header")
    );
    assert_eq!(
        h2_stream_frames(&frame[..frame.len() - 1], 1),
        Err("truncated frame payload")
    );
}

/// A 200 head, then `held` in two DATA frames, the last ending the stream,
/// with a frame on another stream between them and a RST_STREAM with
/// `NO_ERROR` after the end.
#[test]
fn whole_h2_response_check_accepts_a_head_and_its_body() {
    let wire = [
        h2_frame(H2_HEADERS, END_HEADERS, 1, &[STATUS_200]),
        h2_frame(H2_DATA, 0, 3, b"zz"),
        h2_frame(H2_DATA, 0, 1, b"he"),
        h2_frame(H2_DATA, END_STREAM, 1, b"ld"),
        h2_frame(H2_RST_STREAM, 0, 1, &[0; 4]),
    ]
    .concat();
    assert_whole_h2_response(&wire, 1, b"held");
}

#[test]
#[should_panic(expected = "only unpadded DATA frames carry the body")]
fn whole_h2_response_check_rejects_a_reset_after_the_head() {
    let wire = [
        h2_frame(H2_HEADERS, END_HEADERS, 1, &[STATUS_200]),
        h2_frame(H2_RST_STREAM, 0, 1, &8_u32.to_be_bytes()),
    ]
    .concat();
    assert_whole_h2_response(&wire, 1, b"");
}

#[test]
#[should_panic(expected = "a DATA frame ends the stream")]
fn whole_h2_response_check_rejects_a_body_without_its_end() {
    let wire = [
        h2_frame(H2_HEADERS, END_HEADERS, 1, &[STATUS_200]),
        h2_frame(H2_DATA, 0, 1, b"held"),
    ]
    .concat();
    assert_whole_h2_response(&wire, 1, b"held");
}

#[test]
#[should_panic(expected = "after the end, at most a RST_STREAM with NO_ERROR follows")]
fn whole_h2_response_check_rejects_two_resets_after_the_end() {
    let wire = [
        h2_frame(H2_HEADERS, END_HEADERS, 1, &[STATUS_200]),
        h2_frame(H2_DATA, END_STREAM, 1, b"held"),
        h2_frame(H2_RST_STREAM, 0, 1, &[0; 4]),
        h2_frame(H2_RST_STREAM, 0, 1, &[0; 4]),
    ]
    .concat();
    assert_whole_h2_response(&wire, 1, b"held");
}

/// Reads until EOF or an error, returning the bytes and whether the peer reset.
async fn read_until_closed(client: &mut TcpStream) -> (Vec<u8>, bool) {
    let mut wire = Vec::new();
    loop {
        let mut chunk = [0; 512];
        match client.read(&mut chunk).await {
            Ok(0) => return (wire, false),
            Ok(read) => wire.extend_from_slice(&chunk[..read]),
            Err(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                return (wire, true);
            }
        }
    }
}

/// Reads until the end of an HTTP/1.1 response head.
async fn read_http1_response_head(client: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut chunk = [0; 512];
        let read = client.read(&mut chunk).await.expect("response head read");
        assert_ne!(read, 0, "the response head arrives before close");
        head.extend_from_slice(&chunk[..read]);
    }
    head
}

/// Closes `client` abortively, so that the server reads a reset.
fn reset(client: TcpStream) {
    socket2::SockRef::from(&client)
        .set_linger(Some(Duration::ZERO))
        .expect("abortive close");
    drop(client);
}

#[test]
fn abort_exchanges_on_eof_is_off_by_default() {
    assert!(!ServerSessionConfig::default().abort_exchanges_on_eof);
}

#[test]
fn http1_eof_aborts_a_held_exchange_when_enabled() {
    http1_eof_aborts_a_held_exchange(REQUEST, false);
}

#[test]
fn http1_eof_aborts_a_held_closing_exchange_when_enabled() {
    http1_eof_aborts_a_held_exchange(CLOSING_REQUEST, false);
}

#[test]
fn http1_eof_aborts_a_held_http10_exchange_when_enabled() {
    http1_eof_aborts_a_held_exchange(HTTP10_REQUEST, false);
}

/// With `non_abortive_close` (`DIV-038`), the abort's reset closes the socket
/// without `SO_LINGER` zero, so the client reads EOF.
#[test]
fn http1_eof_aborts_a_held_exchange_with_a_fin_under_non_abortive_close() {
    http1_eof_aborts_a_held_exchange(REQUEST, true);
}

/// The EOF fails the connection as a reset does (`DIV-053`): the handler future
/// is cancelled, nothing is written, and the transport is reset, which
/// `non_abortive_close` turns into a FIN.
fn http1_eof_aborts_a_held_exchange(request: &'static [u8], non_abortive_close: bool) {
    in_both_modes(move |context, owned| async move {
        let held = Rc::new(Held::default());
        let session = ServerSessionConfig {
            non_abortive_close,
            ..session(owned, true)
        };
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session,
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client.write_all(request).await.expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.dropped.get(), "the handler future is cancelled");
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(wire.is_empty(), "no response is written: {wire:?}");
        assert_eq!(reset, !non_abortive_close, "the abort resets the transport");
    });
}

#[test]
fn http1_reset_aborts_a_held_closing_exchange_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(CLOSING_REQUEST)
            .await
            .expect("request written");
        until(|| held.invoked.get()).await;
        reset(client);
        connection
            .wait_closed()
            .await
            .expect_err("the reset fails the connection");
        assert!(held.dropped.get(), "the handler future is cancelled");
    });
}

#[test]
fn http1_eof_aborts_a_handler_that_read_its_body_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(ConsumingHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nabc")
            .await
            .expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.dropped.get(), "the handler future is cancelled");
    });
}

/// A request whose reader stopped its upload with `NO_ERROR` still owes its
/// response, so the EOF aborts it.
#[test]
fn http1_eof_aborts_a_no_error_stopped_upload_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(StoppingHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nabc")
            .await
            .expect("partial request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.dropped.get(), "the handler future is cancelled");
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(wire.is_empty(), "no response is written: {wire:?}");
        assert!(reset, "the abort resets the transport");
    });
}

/// The EOF waits behind the pipelined request: the first exchange is answered,
/// then the second, open when the EOF is read, is aborted.
#[test]
fn http1_eof_behind_a_pipelined_request_answers_the_first_and_aborts_the_second_when_enabled() {
    in_both_modes(|context, owned| async move {
        let first = Rc::new(Held::default());
        let second = Rc::new(Held::default());
        let handler = RouteHandler(vec![
            (
                b"/first",
                Rc::new(HeldHandler(Rc::clone(&first))) as HttpHandlerRef,
            ),
            (b"/second", Rc::new(HeldHandler(Rc::clone(&second)))),
        ]);
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(handler),
        )
        .await;
        client
            .write_all(
                b"GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n\
                  GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .expect("requests written");
        until(|| first.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        tokio::time::sleep(SETTLE).await;
        assert!(
            !first.dropped.get() && !second.invoked.get(),
            "the EOF waits behind the pipelined request"
        );
        first.release.notify_one();
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(!first.dropped.get(), "the first exchange completes");
        assert!(
            second.invoked.get(),
            "the second request reaches its handler"
        );
        assert!(
            second.dropped.get(),
            "the second handler future is cancelled"
        );
        let (wire, reset) = read_until_closed(&mut client).await;
        assert_eq!(
            wire.windows(12)
                .filter(|window| *window == b"HTTP/1.1 200")
                .count(),
            1,
            "only the first response is written: {wire:?}"
        );
        assert!(wire.ends_with(b"held"), "{wire:?}");
        assert!(reset, "the abort resets the transport");
    });
}

#[test]
fn http1_eof_keeps_a_held_closing_exchange_by_default() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, false),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(CLOSING_REQUEST)
            .await
            .expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        tokio::time::sleep(SETTLE).await;
        assert!(
            !held.dropped.get(),
            "the half-closed exchange keeps running"
        );
        held.release.notify_one();
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("the response is delivered");
        assert!(
            response.starts_with(b"HTTP/1.1 200") && response.ends_with(b"held"),
            "the held response is written: {response:?}"
        );
        connection
            .wait_closed()
            .await
            .expect("the half-closed connection completes");
    });
}

#[test]
fn http1_closing_exchange_completes_cleanly_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        held.release.notify_one();
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(CLOSING_REQUEST)
            .await
            .expect("request written");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("the server closes after the response without a reset");
        assert!(
            response.starts_with(b"HTTP/1.1 200") && response.ends_with(b"held"),
            "the response is written: {response:?}"
        );
        connection
            .wait_closed()
            .await
            .expect("the closing exchange completes cleanly");
        assert!(!held.dropped.get());
    });
}

/// Reads go on past a request that closes the connection, so its EOF is seen:
/// the bytes behind it, those read with it and more than the session buffers
/// (`connection_flow_control`) read later, are discarded unparsed, and the EOF
/// aborts it.
#[test]
fn http1_bytes_after_a_closing_request_are_discarded_and_its_eof_aborts_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let session = session(owned, true);
        let trailer = vec![b'z'; session.connection_flow_control as usize + (64 << 10)];
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session,
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        let mut wire = CLOSING_REQUEST.to_vec();
        wire.extend_from_slice(b"extra");
        client.write_all(&wire).await.expect("request written");
        until(|| held.invoked.get()).await;
        client
            .write_all(&trailer)
            .await
            .expect("the session reads the trailing bytes");
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.dropped.get(), "the handler future is cancelled");
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(wire.is_empty(), "no response is written: {wire:?}");
        assert!(reset, "the abort resets the transport");
    });
}

#[test]
fn http1_eof_keeps_a_held_exchange_by_default() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, false),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client.write_all(REQUEST).await.expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        tokio::time::sleep(SETTLE).await;
        assert!(
            !held.dropped.get(),
            "the half-closed exchange keeps running"
        );
        held.release.notify_one();
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("the response is delivered");
        assert!(
            response.starts_with(b"HTTP/1.1 200") && response.ends_with(b"held"),
            "the held response is written: {response:?}"
        );
        connection
            .wait_closed()
            .await
            .expect("the half-closed connection completes");
    });
}

#[test]
fn http1_eof_stops_a_streaming_response_when_enabled() {
    http1_eof_stops_a_streaming_response(REQUEST);
}

#[test]
fn http1_eof_stops_a_closing_streaming_response_when_enabled() {
    http1_eof_stops_a_streaming_response(CLOSING_REQUEST);
}

fn http1_eof_stops_a_streaming_response(request: &'static [u8]) {
    in_both_modes(move |context, owned| async move {
        let probe = Rc::new(SourceProbe::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            StreamingHandler::new(&probe, false),
        )
        .await;
        client.write_all(request).await.expect("request written");
        let head = read_http1_response_head(&mut client).await;
        assert!(head.starts_with(b"HTTP/1.1 200"));
        assert!(!probe.stopped.get());
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(probe.stopped.get(), "the response source is stopped");
        assert!(probe.dropped.get(), "the response source is dropped");
        let (rest, reset) = read_until_closed(&mut client).await;
        assert!(
            !rest.ends_with(b"0\r\n\r\n"),
            "the aborted body has no terminal chunk: {rest:?}"
        );
        assert!(reset, "the abort resets the transport");
    });
}

#[test]
fn http1_reset_stops_a_closing_streaming_response_when_enabled() {
    in_both_modes(|context, owned| async move {
        let probe = Rc::new(SourceProbe::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            StreamingHandler::new(&probe, false),
        )
        .await;
        client
            .write_all(CLOSING_REQUEST)
            .await
            .expect("request written");
        read_http1_response_head(&mut client).await;
        reset(client);
        connection
            .wait_closed()
            .await
            .expect_err("the reset fails the connection");
        assert!(probe.stopped.get(), "the response source is stopped");
        assert!(probe.dropped.get(), "the response source is dropped");
    });
}

/// The abort cancels the exchange at once even while a write is blocked on a
/// peer that stopped reading; once the client goes away, the session ends with
/// the EOF's error.
#[test]
fn http1_eof_with_a_blocked_write_cancels_the_exchange_at_once_when_enabled() {
    in_both_modes(|context, owned| async move {
        let probe = Rc::new(SourceProbe::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            StreamingHandler::new(&probe, true),
        )
        .await;
        client.write_all(REQUEST).await.expect("request written");
        until_stalled(&probe).await;
        client.shutdown().await.expect("client half-closes");
        tokio::time::timeout(
            Duration::from_secs(1),
            until(|| probe.stopped.get() && probe.dropped.get()),
        )
        .await
        .expect("the response source is stopped and dropped at once");
        drop(client);
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
    });
}

#[test]
fn http1_eof_keeps_flushing_a_serialized_response_when_enabled() {
    http1_eof_keeps_flushing_a_serialized_response(REQUEST);
}

#[test]
fn http1_eof_keeps_flushing_a_serialized_closing_response_when_enabled() {
    http1_eof_keeps_flushing_a_serialized_response(CLOSING_REQUEST);
}

/// A response whose end the session serialized is no longer open: its
/// transaction ended at serialization, so the EOF does not abort it, and it is
/// written whole to the half-closed peer once that reads. Had every byte reached
/// the kernel before the EOF, the test could not tell, so it requires a blocked
/// final write.
fn http1_eof_keeps_flushing_a_serialized_response(request: &'static [u8]) {
    in_both_modes(move |context, owned| async move {
        let probe = Rc::new(SourceProbe::default());
        let (connection, mut client) = serve_loopback_with_buffers(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(FinalBatchHandler(Rc::clone(&probe))),
            Some(SMALL_SOCKET_BUFFER),
        )
        .await;
        client.write_all(request).await.expect("request written");
        let arrived = until_final_batch_blocked(&probe, &client).await;
        assert!(
            arrived < FINAL_BATCH,
            "the transport holds back part of the response: {arrived} bytes arrived"
        );
        client.shutdown().await.expect("client half-closes");
        assert!(
            tokio::time::timeout(SETTLE, connection.wait_closed())
                .await
                .is_err(),
            "the session keeps the serialized response"
        );
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(!reset, "the connection closes without a reset");
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        assert_eq!(
            wire.iter().filter(|byte| **byte == b'x').count(),
            FINAL_BATCH,
            "the whole response body is written"
        );
        assert!(
            wire.ends_with(b"0\r\n\r\n"),
            "the response's end is written"
        );
        connection
            .wait_closed()
            .await
            .expect("the serialized response completes the connection");
        assert!(!probe.stopped.get(), "the response source is not stopped");
    });
}

#[test]
fn http1_request_and_eof_in_one_write_abort_when_enabled() {
    http1_request_and_eof_in_one_write_abort(REQUEST);
}

#[test]
fn http1_closing_request_and_eof_in_one_write_abort_when_enabled() {
    http1_request_and_eof_in_one_write_abort(CLOSING_REQUEST);
}

/// The request and the EOF reach the session together, before it parses the
/// request.
fn http1_request_and_eof_in_one_write_abort(request: &'static [u8]) {
    in_both_modes(move |context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        let mut client = client.into_std().expect("standard client");
        client.set_nonblocking(false).expect("blocking client");
        client.write_all(request).expect("request written");
        client
            .shutdown(std::net::Shutdown::Write)
            .expect("client half-closes");
        client.set_nonblocking(true).expect("nonblocking client");
        let mut client = TcpStream::from_std(client).expect("client adopted");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.invoked.get(), "the request reaches the handler");
        assert!(held.dropped.get(), "the handler future is cancelled");
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(wire.is_empty(), "no response is written: {wire:?}");
        assert!(reset, "the abort resets the transport");
    });
}

#[test]
fn http1_eof_mid_upload_aborts_without_the_codec_response_when_enabled() {
    http1_eof_mid_upload_aborts_without_the_codec_response();
}

/// An EOF inside a request body: by default the codec rejects the incomplete
/// request with a 400 and resets; with the option the exchange is open, so the
/// EOF aborts it with no response.
fn http1_eof_mid_upload_aborts_without_the_codec_response() {
    let outcomes: Vec<_> = HTTP1_MODES
        .iter()
        .flat_map(|&owned| [(owned, false), (owned, true)])
        .flat_map(|(owned, abort_exchanges_on_eof)| {
            let (sender, receiver) = std::sync::mpsc::channel();
            on_worker(move |context| async move {
                let (connection, mut client) = serve_loopback(
                    &context,
                    CodecProtocol::Http11,
                    session(owned, abort_exchanges_on_eof),
                    Rc::new(UploadHandler),
                )
                .await;
                client
                    .write_all(
                        b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nabc",
                    )
                    .await
                    .expect("partial request written");
                client.shutdown().await.expect("client half-closes");
                let (wire, reset) = read_until_closed(&mut client).await;
                let error = connection.wait_closed().await.err().map(|error| error.code());
                let status = wire.split(|byte| *byte == b'\r').next().map(<[u8]>::to_vec);
                sender
                    .send((owned, abort_exchanges_on_eof, status, reset, error))
                    .expect("outcome recorded");
            });
            receiver.try_iter().collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        outcomes.len(),
        4 * HTTP1_MODES.len(),
        "each setting ran in each mode on both executors"
    );
    assert!(
        outcomes
            .iter()
            .all(|(_, abort_exchanges_on_eof, status, reset, error)| {
                let expected: &[u8] = if *abort_exchanges_on_eof {
                    b""
                } else {
                    b"HTTP/1.1 400 Bad Request"
                };
                status.as_deref() == Some(expected)
                    && *reset
                    && *error == Some(HttpErrorCode::TRANSPORT_EOF)
            }),
        "by default the codec answers 400 and resets; with the option the EOF aborts \
         the open exchange without a response: {outcomes:?}"
    );
}

/// A mid-upload EOF leaves a response whose end is serialized to be written.
/// The reset can discard bytes the kernel has not yet sent; the abort policy
/// leaves this completed response alone.
#[test]
fn http1_eof_mid_upload_with_an_early_response_is_unchanged_when_enabled() {
    for &owned in HTTP1_MODES {
        assert_writes_the_response_then_resets(&http1_eof_mid_upload_with_an_early_response(
            owned, false,
        ));
    }
}

fn assert_writes_the_response_then_resets(outcomes: &[(bool, usize, bool, Option<HttpErrorCode>)]) {
    assert!(
        outcomes
            .iter()
            .all(|(open, body_written, reset, error)| *open
                && *body_written <= FINAL_BATCH
                && *reset
                && *error == Some(HttpErrorCode::TRANSPORT_EOF)),
        "the half-closed session writes the response, then resets: {outcomes:?}"
    );
}

/// The owned HTTP/1.1 driver, with the option off: a mid-upload EOF after the
/// response started writes the serialized bytes before the reset, as
/// `UniplexSession` does, so under `non_abortive_close` the whole response
/// arrives.
#[test]
fn owned_http1_eof_mid_upload_with_an_early_response_writes_it_by_default() {
    assert_writes_the_response_then_resets(&http1_eof_mid_upload_with_an_early_response_for(
        true,
        false,
        &[false],
    ));
    let outcomes = http1_eof_mid_upload_with_an_early_response_for(true, true, &[false]);
    assert!(
        outcomes
            .iter()
            .all(|outcome| *outcome
                == (true, FINAL_BATCH, false, Some(HttpErrorCode::TRANSPORT_EOF))),
        "the whole response, then EOF: {outcomes:?}"
    );
}

/// With `non_abortive_close` (`DIV-038`), the close that follows a mid-upload EOF
/// no longer discards the response's tail:
/// the session writes the whole response, then the client reads all of it and
/// EOF, since the session read everything the client sent.
#[test]
fn http1_eof_mid_upload_with_an_early_response_arrives_whole_when_non_abortive() {
    for &owned in HTTP1_MODES {
        let outcomes = http1_eof_mid_upload_with_an_early_response(owned, true);
        assert!(
            outcomes.iter().all(|outcome| *outcome
                == (true, FINAL_BATCH, false, Some(HttpErrorCode::TRANSPORT_EOF))),
            "owned_http1 {owned}: the whole response, then EOF: {outcomes:?}"
        );
    }
}

/// Whether the session stayed open after a mid-upload EOF, the response body
/// bytes the client read, whether the connection was reset and the session's
/// error, for each `abort_exchanges_on_eof` setting on each executor, with
/// HTTP/1.1 on the owned driver when `owned_http1`.
fn http1_eof_mid_upload_with_an_early_response(
    owned_http1: bool,
    non_abortive_close: bool,
) -> Vec<(bool, usize, bool, Option<HttpErrorCode>)> {
    http1_eof_mid_upload_with_an_early_response_for(owned_http1, non_abortive_close, &[false, true])
}

/// As `http1_eof_mid_upload_with_an_early_response`, for the
/// `abort_exchanges_on_eof` settings in `settings`.
fn http1_eof_mid_upload_with_an_early_response_for(
    owned_http1: bool,
    non_abortive_close: bool,
    settings: &'static [bool],
) -> Vec<(bool, usize, bool, Option<HttpErrorCode>)> {
    let outcomes: Vec<_> = settings
        .iter()
        .copied()
        .flat_map(|abort_exchanges_on_eof| {
            let (sender, receiver) = std::sync::mpsc::channel();
            on_worker(move |context| async move {
                let probe = Rc::new(SourceProbe::default());
                let session = ServerSessionConfig {
                    non_abortive_close,
                    ..session(owned_http1, abort_exchanges_on_eof)
                };
                let (connection, mut client) = serve_loopback_with_buffers(
                    &context,
                    CodecProtocol::Http11,
                    session,
                    Rc::new(FinalBatchHandler(Rc::clone(&probe))),
                    Some(SMALL_SOCKET_BUFFER),
                )
                .await;
                client
                    .write_all(
                        b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\nabc",
                    )
                    .await
                    .expect("partial request written");
                until_final_batch_blocked(&probe, &client).await;
                client.shutdown().await.expect("client half-closes");
                let open = tokio::time::timeout(SETTLE, connection.wait_closed())
                    .await
                    .is_err();
                let (wire, reset) = read_until_closed(&mut client).await;
                let body_written = wire.iter().filter(|byte| **byte == b'x').count();
                let error = connection.wait_closed().await.err().map(|error| error.code());
                sender
                    .send((open, body_written, reset, error))
                    .expect("outcome recorded");
            });
            receiver.try_iter().collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        outcomes.len(),
        2 * settings.len(),
        "each setting ran on both executors"
    );
    outcomes
}

#[test]
fn http1_eof_after_the_response_closes_cleanly_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        held.release.notify_one();
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client.write_all(REQUEST).await.expect("request written");
        let mut response = Vec::new();
        while !response.ends_with(b"held") {
            let mut chunk = [0; 512];
            let read = client.read(&mut chunk).await.expect("response read");
            assert_ne!(read, 0, "the response arrives before close");
            response.extend_from_slice(&chunk[..read]);
        }
        client
            .shutdown()
            .await
            .expect("client closes the idle connection");
        connection
            .wait_closed()
            .await
            .expect("an EOF on an idle connection is a clean close");
        assert!(!held.dropped.get());
    });
}

/// The exchanges a keep-alive connection completed before the held one leave
/// nothing behind that changes how its EOF is judged, and a chunked request
/// body counts as complete once its last chunk arrives.
#[test]
fn http1_eof_aborts_a_held_chunked_exchange_after_keep_alive_exchanges_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(KeepAliveHandler(Rc::clone(&held))),
        )
        .await;
        keep_alive_exchanges(&mut client, 5).await;
        client
            .write_all(
                b"POST /held HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n\
                  3\r\nabc\r\n0\r\n\r\n",
            )
            .await
            .expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        let error = connection
            .wait_closed()
            .await
            .expect_err("the EOF fails the connection");
        assert_eq!(error.code(), HttpErrorCode::TRANSPORT_EOF);
        assert!(held.dropped.get(), "the handler future is cancelled");
        let (wire, reset) = read_until_closed(&mut client).await;
        assert!(wire.is_empty(), "no response is written: {wire:?}");
        assert!(reset, "the abort resets the transport");
    });
}

#[test]
fn http1_eof_after_many_keep_alive_exchanges_closes_cleanly_when_enabled() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, true),
            Rc::new(KeepAliveHandler(Rc::clone(&held))),
        )
        .await;
        keep_alive_exchanges(&mut client, 20).await;
        client
            .shutdown()
            .await
            .expect("client closes the idle connection");
        connection
            .wait_closed()
            .await
            .expect("an EOF after completed exchanges is a clean close");
    });
}

#[test]
fn http1_reset_aborts_a_held_exchange_by_default() {
    in_both_modes(|context, owned| async move {
        let held = Rc::new(Held::default());
        let (connection, client) = serve_loopback(
            &context,
            CodecProtocol::Http11,
            session(owned, false),
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        let mut client = client;
        client.write_all(REQUEST).await.expect("request written");
        until(|| held.invoked.get()).await;
        reset(client);
        connection
            .wait_closed()
            .await
            .expect_err("the reset fails the connection");
        assert!(held.dropped.get(), "the handler future is cancelled");
    });
}

#[test]
fn http2_eof_keeps_open_streams_by_default() {
    http2_eof_keeps_an_open_stream(session(false, false));
}

/// `DIV-053` leaves HTTP/2 unchanged: the half-closed stream keeps running.
#[test]
fn http2_eof_keeps_open_streams_when_enabled() {
    http2_eof_keeps_an_open_stream(session(false, true));
}

/// The owned HTTP/2 driver accepts `abort_exchanges_on_eof` and ignores it, as
/// API Proxy's owned engine runs it.
#[test]
fn owned_http2_eof_keeps_open_streams_when_enabled() {
    http2_eof_keeps_an_open_stream(ServerSessionConfig {
        owned_http2: true,
        ..session(false, true)
    });
}

/// A held h2c stream survives the peer's half-close, and its whole response is
/// written once released.
fn http2_eof_keeps_an_open_stream(session: ServerSessionConfig) {
    on_worker(move |context| async move {
        let held = Rc::new(Held::default());
        let (connection, mut client) = serve_loopback(
            &context,
            CodecProtocol::Http2,
            session,
            Rc::new(HeldHandler(Rc::clone(&held))),
        )
        .await;
        client
            .write_all(&h2_request())
            .await
            .expect("request written");
        until(|| held.invoked.get()).await;
        client.shutdown().await.expect("client half-closes");
        tokio::time::sleep(SETTLE).await;
        assert!(!held.dropped.get(), "the half-closed stream keeps running");
        held.release.notify_one();
        let (wire, reset) = read_until_closed(&mut client).await;
        assert_whole_h2_response(&wire, 1, b"held");
        assert!(!reset, "the connection closes without a reset");
        connection
            .wait_closed()
            .await
            .expect("the half-closed connection completes");
    });
}
