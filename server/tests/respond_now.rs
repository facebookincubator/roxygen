/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpHandler::respond_now` on the owned HTTP/1.1 and HTTP/2 drivers, over an
//! in-memory transport attached with `serve_transport`.

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::FixedResponse;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::DuplexStream;

use crate::acceptor::ServerSessionConfig;
use crate::allocation_count::COUNTS_EVERY_CRATE;
use crate::allocation_count::start_counting;
use crate::allocation_count::stop_counting;
use crate::connection::ServeTransportOptions;
use crate::connection::serve_transport;

const TIMEOUT: Duration = Duration::from_secs(10);
const BODY: &[u8] = b"fixed body";
/// Longer than the stream window that the reset test grants, and than the
/// bytes that the close test's transport and writes hold.
const LARGE: usize = 64 * 1024;

fn wait<Work: Future>(future: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, future)
                .await
                .expect("bounded test")
        })
}

/// Runs `work` on the worker of a fresh one-worker runtime, then joins the runtime.
fn on_worker<Work, Run, Output>(work: Work) -> Output
where
    Work: FnOnce(WorkerContext) -> Run + Send + 'static,
    Run: Future<Output = Output> + 'static,
    Output: Send + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let output = wait(worker.call_local(work).expect("test admitted")).expect("test finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    output
}

/// Serves one connection with `handler` on the owned driver for `protocol`, runs
/// `client` on its peer end, and returns what `client` returns once the session
/// has closed.
async fn serve<Client, Run, Output>(
    context: &WorkerContext,
    protocol: CodecProtocol,
    handler: HttpHandlerRef,
    client: Client,
) -> Output
where
    Client: FnOnce(DuplexStream) -> Run,
    Run: Future<Output = Output>,
{
    serve_buffered(context, protocol, handler, 256 * 1024, client).await
}

/// [`serve`] over an in-memory transport that buffers at most `buffer` bytes in
/// each direction.
async fn serve_buffered<Client, Run, Output>(
    context: &WorkerContext,
    protocol: CodecProtocol,
    handler: HttpHandlerRef,
    buffer: usize,
    client: Client,
) -> Output
where
    Client: FnOnce(DuplexStream) -> Run,
    Run: Future<Output = Output>,
{
    let (io, peer) = tokio::io::duplex(buffer);
    let transport = adopt_local(
        context,
        EstablishedIo::new(
            io,
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport"),
    )
    .expect("I/O adopted");
    let mut options = ServeTransportOptions::new(protocol, TransportSecurity::Plaintext);
    options.session = ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1: true,
        owned_http2: true,
        add_date_header_to_response: false,
        ..ServerSessionConfig::default()
    };
    let connection =
        serve_transport(context, transport, handler, options).expect("transport attached");
    let output = client(peer).await;
    let _ = connection.wait_closed().await;
    output
}

#[derive(Default)]
struct Calls {
    respond_now: Cell<usize>,
    handle_request: Cell<usize>,
}

/// Counts the drops of a body's backing, which `Bytes::from_owner` keeps.
struct Tracked {
    bytes: Vec<u8>,
    dropped: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Answers every request with one whole response: [`LARGE`] bytes for `/large`,
/// otherwise [`BODY`]. `respond_now` answers only when `now` is set, and
/// `handle_request` answers with the same parts in an `HttpFixedSource` after
/// reading the request. Each adds `Content-Length`.
struct Fixed {
    now: bool,
    calls: Rc<Calls>,
    request_bodies: Rc<Cell<usize>>,
    large_dropped: Arc<AtomicUsize>,
}

impl Fixed {
    fn new(now: bool) -> Self {
        Self {
            now,
            calls: Rc::default(),
            request_bodies: Rc::default(),
            large_dropped: Arc::default(),
        }
    }
}

/// The response [`Fixed`] answers `request` with.
fn fixed_response(
    request: &HttpMessage,
    large_dropped: &Arc<AtomicUsize>,
) -> (HttpMessage, BufQueue) {
    let body = if request.request_url() == Some(b"/large") {
        Bytes::from_owner(Tracked {
            bytes: vec![b'l'; LARGE],
            dropped: Arc::clone(large_dropped),
        })
    } else {
        Bytes::from_static(BODY)
    };
    let mut head = HttpMessage::new();
    head.set_status_code(200).expect("a response head");
    head.set_status_message(HttpMessage::default_reason(200))
        .expect("a response head");
    head.set_http_version(1, 1);
    let headers = head.headers_mut();
    headers.add(HttpHeaderCode::ContentType, b"text/plain");
    headers.add("X-Fixed", b"one");
    headers.add("X-Fixed", b"two");
    let length = body.len();
    (
        head,
        BufQueue::from_bytes(body, length).expect("the body fits its queue"),
    )
}

impl HttpHandler for Fixed {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.calls
            .handle_request
            .set(self.calls.handle_request.get() + 1);
        let request_bodies = Rc::clone(&self.request_bodies);
        let large_dropped = Arc::clone(&self.large_dropped);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let mut eom = head.eom();
            while !eom {
                let event = request.read_body_event(u32::MAX).await?;
                if event.as_body().is_some_and(|body| !body.is_empty()) {
                    request_bodies.set(request_bodies.get() + 1);
                }
                eom = event.eom();
            }
            let (head, body) = fixed_response(head.headers(), &large_dropped);
            Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                head,
                Some(body),
            ))))
        })
    }

    fn respond_now(&self, request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        self.calls.respond_now.set(self.calls.respond_now.get() + 1);
        if !self.now {
            return None;
        }
        let (head, body) = fixed_response(request, &self.large_dropped);
        Some(Ok(FixedResponse::new(head, body)))
    }
}

/// Panics in `respond_now` when `now` is set, and otherwise in `handle_request`.
struct Panicking {
    now: bool,
    calls: Rc<Calls>,
}

impl HttpHandler for Panicking {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.calls
            .handle_request
            .set(self.calls.handle_request.get() + 1);
        if !self.now {
            panic!("injected handle_request panic");
        }
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            HttpFixedSource::make_fixed_response(200, None).expect("a response"),
        )))))
    }

    fn respond_now(&self, _request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        self.calls.respond_now.set(self.calls.respond_now.get() + 1);
        if self.now {
            panic!("injected respond_now panic");
        }
        None
    }
}

/// Writes `requests`, then returns everything the server writes until it closes
/// the connection.
async fn exchange_http1(mut peer: DuplexStream, requests: &[u8]) -> Vec<u8> {
    peer.write_all(requests).await.expect("requests written");
    let mut wire = Vec::new();
    peer.read_to_end(&mut wire).await.expect("responses read");
    wire
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small frame");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
/// The RST_STREAM code `CANCEL`.
const CANCEL: u32 = 0x8;

/// The client preface and a SETTINGS frame that sets the initial stream window
/// to `window`, when given.
fn h2_preface(window: Option<u32>) -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    let mut settings = Vec::new();
    if let Some(window) = window {
        // SETTINGS_INITIAL_WINDOW_SIZE.
        settings.extend_from_slice(&4_u16.to_be_bytes());
        settings.extend_from_slice(&window.to_be_bytes());
    }
    wire.extend(h2_frame(SETTINGS, 0, 0, &settings));
    wire
}

/// A request head on `stream` for `path` on localhost, encoded without the
/// dynamic table, which ends the stream unless `body` follows it.
fn h2_request(stream: u32, method: u8, path: &[u8], body: Option<&[u8]>) -> Vec<u8> {
    // The static-table method, `:scheme: http`, then `:path` and `:authority`
    // as literals without indexing.
    let mut block = vec![
        method,
        0x86,
        0x04,
        u8::try_from(path.len()).expect("short path"),
    ];
    block.extend_from_slice(path);
    block.extend_from_slice(&[0x01, 9]);
    block.extend_from_slice(b"localhost");
    let flags = if body.is_some() {
        END_HEADERS
    } else {
        END_HEADERS | END_STREAM
    };
    let mut wire = h2_frame(HEADERS, flags, stream, &block);
    if let Some(body) = body {
        wire.extend(h2_frame(DATA, END_STREAM, stream, body));
    }
    wire
}

/// HPACK static-table `:method: GET` and `:method: POST`.
const GET: u8 = 0x82;
const POST: u8 = 0x83;

/// One frame on a request stream.
#[derive(Debug, PartialEq)]
struct Frame {
    stream: u32,
    kind: u8,
    flags: u8,
    payload: Vec<u8>,
}

/// The whole frames on request streams at the start of `wire`.
fn stream_frames(mut wire: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    while let Some(header) = wire.get(..9) {
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let Some(payload) = wire.get(9..9 + length) else {
            break;
        };
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        if stream != 0 {
            frames.push(Frame {
                stream,
                kind: header[3],
                flags: header[4],
                payload: payload.to_vec(),
            });
        }
        wire = &wire[9 + length..];
    }
    frames
}

fn ended(frames: &[Frame], stream: u32) -> bool {
    frames.iter().any(|frame| {
        frame.stream == stream
            && (frame.kind == DATA || frame.kind == HEADERS)
            && frame.flags & END_STREAM != 0
    })
}

fn data(frames: &[Frame], stream: u32) -> Vec<u8> {
    frames
        .iter()
        .filter(|frame| frame.stream == stream && frame.kind == DATA)
        .flat_map(|frame| frame.payload.iter().copied())
        .collect()
}

/// Reads from `peer` into `wire` until `done` holds for its request-stream frames.
async fn read_until(peer: &mut DuplexStream, wire: &mut Vec<u8>, done: impl Fn(&[Frame]) -> bool) {
    let mut buffer = vec![0; 64 * 1024];
    while !done(&stream_frames(wire)) {
        let read = peer.read(&mut buffer).await.expect("response read");
        assert_ne!(read, 0, "the server closed before the responses ended");
        wire.extend_from_slice(&buffer[..read]);
    }
}

/// Writes `requests` and returns the frames on request streams once each of
/// `streams` has ended.
async fn exchange_http2(mut peer: DuplexStream, requests: Vec<u8>, streams: &[u32]) -> Vec<Frame> {
    peer.write_all(&requests).await.expect("requests written");
    let mut wire = Vec::new();
    read_until(&mut peer, &mut wire, |frames| {
        streams.iter().all(|&stream| ended(frames, stream))
    })
    .await;
    stream_frames(&wire)
}

const HTTP1_GETS: &[u8] = b"GET /fixed HTTP/1.1\r\nHost: localhost\r\n\r\n\
    GET /fixed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

/// The HTTP/1.1 response [`Fixed`] writes for `/fixed`, with the `Connection`
/// header that the codec adds.
fn http1_response(connection: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Fixed: one\r\nX-Fixed: two\r\n\
         Connection: {connection}\r\nContent-Length: 10\r\n\r\nfixed body"
    )
}

fn http2_gets() -> Vec<u8> {
    let mut requests = h2_preface(None);
    requests.extend(h2_request(1, GET, b"/fixed", None));
    requests.extend(h2_request(3, GET, b"/fixed", None));
    requests
}

/// On both owned drivers, a response from `respond_now` writes the same bytes as
/// the same response from `handle_request`, and a handler whose `respond_now`
/// returns `None` is answered by `handle_request`.
#[test]
fn a_fixed_response_writes_what_handle_request_writes() {
    let (http1, http2) = on_worker(|context| async move {
        let mut http1 = Vec::new();
        let mut http2 = Vec::new();
        for now in [true, false] {
            let handler = Rc::new(Fixed::new(now));
            let calls = Rc::clone(&handler.calls);
            let wire = serve(&context, CodecProtocol::Http11, handler, |peer| {
                exchange_http1(peer, HTTP1_GETS)
            })
            .await;
            http1.push((wire, calls.respond_now.get(), calls.handle_request.get()));

            let handler = Rc::new(Fixed::new(now));
            let calls = Rc::clone(&handler.calls);
            let frames = serve(&context, CodecProtocol::Http2, handler, |peer| {
                exchange_http2(peer, http2_gets(), &[1, 3])
            })
            .await;
            http2.push((frames, calls.respond_now.get(), calls.handle_request.get()));
        }
        (http1, http2)
    });

    let [(fast, 2, 0), (regular, 2, 2)] = http1.as_slice() else {
        panic!("respond_now answers both requests or declines both: {http1:?}");
    };
    assert_eq!(
        String::from_utf8_lossy(fast),
        String::from_utf8_lossy(regular)
    );
    assert_eq!(
        String::from_utf8_lossy(fast),
        http1_response("keep-alive") + &http1_response("close")
    );

    let [(fast, 2, 0), (regular, 2, 2)] = http2.as_slice() else {
        panic!("respond_now answers both streams or declines both: {http2:?}");
    };
    assert_eq!(fast, regular);
    for stream in [1, 3] {
        assert!(ended(fast, stream), "stream {stream} ends");
        assert_eq!(data(fast, stream), BODY, "stream {stream}");
    }
}

/// A panic in `respond_now` is contained and answered as a panic in
/// `handle_request` is, and `handle_request` is not called.
#[test]
fn a_panicking_respond_now_is_answered_as_a_panicking_handler() {
    let (http1, http2) = on_worker(|context| async move {
        let mut http1 = Vec::new();
        let mut http2 = Vec::new();
        for now in [true, false] {
            let calls = Rc::new(Calls::default());
            let handler = Rc::new(Panicking {
                now,
                calls: Rc::clone(&calls),
            });
            let wire = serve(&context, CodecProtocol::Http11, handler, |peer| {
                exchange_http1(peer, b"GET /panic HTTP/1.1\r\nHost: localhost\r\n\r\n")
            })
            .await;
            http1.push((wire, calls.respond_now.get(), calls.handle_request.get()));

            let calls = Rc::new(Calls::default());
            let handler = Rc::new(Panicking {
                now,
                calls: Rc::clone(&calls),
            });
            let mut requests = h2_preface(None);
            requests.extend(h2_request(1, GET, b"/panic", None));
            let frames = serve(&context, CodecProtocol::Http2, handler, |peer| {
                exchange_http2(peer, requests, &[1])
            })
            .await;
            http2.push((frames, calls.respond_now.get(), calls.handle_request.get()));
        }
        (http1, http2)
    });

    let [(fast, 1, 0), (regular, 1, 1)] = http1.as_slice() else {
        panic!("only the panicking method runs: {http1:?}");
    };
    assert_eq!(
        String::from_utf8_lossy(fast),
        String::from_utf8_lossy(regular)
    );
    assert!(fast.starts_with(b"HTTP/1.1 500 "), "{fast:?}");
    assert!(fast.ends_with(b"request handler panicked"), "{fast:?}");

    let [(fast, 1, 0), (regular, 1, 1)] = http2.as_slice() else {
        panic!("only the panicking method runs: {http2:?}");
    };
    assert_eq!(fast, regular);
    assert_eq!(data(fast, 1), b"request handler panicked");
}

/// A request with a body is not offered to `respond_now`; `handle_request`
/// reads its body and answers it.
#[test]
fn a_request_with_a_body_is_left_to_handle_request() {
    let results = on_worker(|context| async move {
        let handler = Rc::new(Fixed::new(true));
        let calls = Rc::clone(&handler.calls);
        let bodies = Rc::clone(&handler.request_bodies);
        let http1 = serve(&context, CodecProtocol::Http11, handler, |peer| {
            exchange_http1(
                peer,
                b"POST /fixed HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\
                  Connection: close\r\n\r\nhello",
            )
        })
        .await;
        let http1_counts = (
            calls.respond_now.get(),
            calls.handle_request.get(),
            bodies.get(),
        );

        let handler = Rc::new(Fixed::new(true));
        let calls = Rc::clone(&handler.calls);
        let bodies = Rc::clone(&handler.request_bodies);
        let mut requests = h2_preface(None);
        requests.extend(h2_request(1, POST, b"/fixed", Some(b"hello")));
        let http2 = serve(&context, CodecProtocol::Http2, handler, |peer| {
            exchange_http2(peer, requests, &[1])
        })
        .await;
        let http2_counts = (
            calls.respond_now.get(),
            calls.handle_request.get(),
            bodies.get(),
        );
        (http1, http1_counts, http2, http2_counts)
    });
    let (http1, http1_counts, http2, http2_counts) = results;
    assert_eq!(String::from_utf8_lossy(&http1), http1_response("close"));
    assert_eq!(http1_counts, (0, 1, 1));
    assert!(ended(&http2, 1));
    assert_eq!(data(&http2, 1), BODY);
    assert_eq!(http2_counts, (0, 1, 1));
}

/// Resetting a stream whose fixed response is stalled on flow-control credit
/// ends that response: its body is released, the rest of it is not sent, and
/// the connection still answers another stream from `respond_now`.
#[test]
fn a_reset_stream_releases_its_fixed_response() {
    let ((frames, released), calls, dropped) = on_worker(|context| async move {
        let handler = Rc::new(Fixed::new(true));
        let calls = Rc::clone(&handler.calls);
        let dropped = Arc::clone(&handler.large_dropped);
        let observed = Arc::clone(&dropped);
        let frames = serve(
            &context,
            CodecProtocol::Http2,
            handler,
            |mut peer| async move {
                let mut requests = h2_preface(Some(100));
                requests.extend(h2_request(1, GET, b"/large", None));
                peer.write_all(&requests).await.expect("request written");
                let mut wire = Vec::new();
                read_until(&mut peer, &mut wire, |frames| data(frames, 1).len() == 100).await;
                let mut reset = h2_frame(RST_STREAM, 0, 1, &CANCEL.to_be_bytes());
                reset.extend(h2_request(3, GET, b"/fixed", None));
                peer.write_all(&reset).await.expect("reset written");
                read_until(&mut peer, &mut wire, |frames| ended(frames, 3)).await;
                (stream_frames(&wire), observed.load(Ordering::SeqCst))
            },
        )
        .await;
        let calls = (calls.respond_now.get(), calls.handle_request.get());
        (frames, calls, dropped.load(Ordering::SeqCst))
    });
    assert_eq!(calls, (2, 0));
    assert_eq!(data(&frames, 1), vec![b'l'; 100]);
    assert!(!ended(&frames, 1), "the reset response does not end");
    assert_eq!(data(&frames, 3), BODY);
    assert_eq!(
        released, 1,
        "the reset response's body is released before the next stream is answered"
    );
    assert_eq!(dropped, 1, "the body is released once");
}

/// A peer that closes an HTTP/1.1 connection partway through a fixed response
/// ends that response, and its body is released once.
#[test]
fn a_closed_connection_releases_its_fixed_response() {
    let (seen, calls, dropped) = on_worker(|context| async move {
        let handler = Rc::new(Fixed::new(true));
        let calls = Rc::clone(&handler.calls);
        let dropped = Arc::clone(&handler.large_dropped);
        // The transport and the driver's writes hold far less than the body, so
        // the response is still being written when the peer closes.
        let seen = serve_buffered(
            &context,
            CodecProtocol::Http11,
            handler,
            4096,
            |mut peer| async move {
                peer.write_all(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .expect("request written");
                let mut wire = Vec::new();
                let mut buffer = [0; 1024];
                while !wire.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = peer.read(&mut buffer).await.expect("response read");
                    assert_ne!(read, 0, "the server closed before the response head");
                    wire.extend_from_slice(&buffer[..read]);
                }
                wire
            },
        )
        .await;
        let calls = (calls.respond_now.get(), calls.handle_request.get());
        (seen, calls, dropped.load(Ordering::SeqCst))
    });
    assert_eq!(calls, (1, 0));
    assert!(seen.starts_with(b"HTTP/1.1 200 OK\r\n"), "{seen:?}");
    assert!(
        seen.len() < LARGE,
        "the peer closes partway through the body"
    );
    assert_eq!(dropped, 1, "the body is released once");
}

/// A whole response in memory whose body is polled in place.
struct InPlace {
    head: Option<Box<HttpMessage>>,
    body: BufQueue,
}

impl InPlace {
    fn next_body(&mut self, maximum: u32) -> HttpBodyEvent {
        let body = self.body.split_at_most(maximum as usize);
        HttpBodyEvent::body(body, self.body.is_empty())
    }
}

impl HttpSource for InPlace {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let head = self.head.take().expect("the head is read once");
        let event =
            HttpHeaderEvent::from_boxed_headers(head, self.body.is_empty()).map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
            });
        Box::pin(std::future::ready(event))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Ok(self.next_body(maximum))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        _context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        Poll::Ready(Ok(self.next_body(maximum)))
    }
}

/// Answers each request with a copy of one head, whose header entries its
/// copies share, and a copied body: through `respond_now` when `now` is set,
/// and otherwise from `handle_request`, with an [`InPlace`] source installed in
/// the request's holder.
struct Shared {
    now: bool,
    head: Box<HttpMessage>,
}

impl Shared {
    fn new(now: bool) -> Self {
        let mut head = Box::new(HttpMessage::new());
        head.set_status_code(200).expect("a response head");
        head.set_status_message(HttpMessage::default_reason(200))
            .expect("a response head");
        head.set_http_version(1, 1);
        head.headers_mut()
            .add(HttpHeaderCode::ContentType, b"text/plain");
        head.headers_mut().share_entries();
        Self { now, head }
    }
}

fn copied_body() -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(b"2"), 1).expect("the body fits its queue")
}

impl HttpHandler for Shared {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let head = self.head.clone();
        Box::pin(async move {
            request.read_header_event_unboxed().await?;
            request.set_source(Box::new(InPlace {
                head: Some(head),
                body: copied_body(),
            }));
            Ok(request)
        })
    }

    fn respond_now(&self, _request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        self.now.then(|| {
            Ok(FixedResponse::from_parts(
                HttpMessage::clone(&self.head),
                copied_body(),
            ))
        })
    }
}

/// How many request streams in `wire` have ended, counted without allocating.
fn ended_streams(mut wire: &[u8]) -> usize {
    let mut ended = 0;
    while let Some(header) = wire.get(..9) {
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        if wire.len() < 9 + length {
            break;
        }
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        if stream != 0 && matches!(header[3], DATA | HEADERS) && header[4] & END_STREAM != 0 {
            ended += 1;
        }
        wire = &wire[9 + length..];
    }
    ended
}

/// The worker thread's heap allocations while one HTTP/2 connection answers
/// `streams` GETs with [`Shared`].
async fn shared_allocations(context: &WorkerContext, now: bool, streams: u32) -> usize {
    serve(
        context,
        CodecProtocol::Http2,
        Rc::new(Shared::new(now)),
        |mut peer| async move {
            let mut requests = h2_preface(None);
            for index in 0..streams {
                requests.extend(h2_request(1 + 2 * index, GET, b"/shared", None));
            }
            peer.write_all(&requests).await.expect("requests written");
            let mut wire = Vec::with_capacity(1 << 20);
            let mut buffer = vec![0; 64 * 1024];
            start_counting();
            while ended_streams(&wire) < streams as usize {
                let read = peer.read(&mut buffer).await.expect("response read");
                assert_ne!(read, 0, "the server closed before the responses ended");
                wire.extend_from_slice(&buffer[..read]);
            }
            stop_counting()
        },
    )
    .await
}

/// On the owned HTTP/2 driver, answering through `respond_now` saves seven heap
/// allocations per request over answering from `handle_request` in the
/// request's holder with a source polled in place: the request's hybrid source
/// and holder, the handler future, the boxed header reads of the request and
/// of the response, the response source, and the box of the response head,
/// whose allocation the fixed response reuses from the request head. The
/// per-request counts are differenced over two numbers of streams. The saving
/// follows the regular path's count, so a change to that path's allocations
/// changes it too. Answering through `respond_now` itself stays within seven
/// allocations per request.
#[test]
fn respond_now_saves_seven_allocations_per_request() {
    if !COUNTS_EVERY_CRATE {
        return;
    }
    let counts = on_worker(|context| async move {
        shared_allocations(&context, false, 1).await;
        let mut counts = Vec::new();
        for streams in [20, 40] {
            let regular = shared_allocations(&context, false, streams).await;
            let fixed = shared_allocations(&context, true, streams).await;
            counts.push((regular, fixed));
        }
        counts
    });
    let [(regular_20, fixed_20), (regular_40, fixed_40)] = counts[..] else {
        unreachable!("two stream counts");
    };
    let saved_20 = regular_20 - fixed_20;
    let saved_40 = regular_40 - fixed_40;
    assert_eq!(
        saved_40 - saved_20,
        7 * 20,
        "{saved_20} allocations saved over 20 requests, {saved_40} over 40 ({counts:?})"
    );
    let fixed = fixed_40 - fixed_20;
    assert!(
        fixed <= 7 * 20,
        "{fixed} allocations for 20 requests answered by respond_now ({counts:?})"
    );
}
