//! The router prototype's ingress combination: `serve_transport` with
//! `abort_exchanges_on_eof` (`DIV-053`), `abort_http2_streams_on_eof`
//! (`DIV-055`), `non_abortive_close` (`DIV-038`), `exact_method_tokens`
//! (`DIV-040`) and `verbatim_http2_header_values` (`DIV-041`), serving a
//! response source that polls its body events in place
//! (`DIV-039`) under a head whose values the handler inserts with
//! `HttpHeaders::add_verbatim`, over HTTP/1.1 and h2c on loopback TCP, on a
//! `RuntimeOwner` worker and a `HostedWorker`.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::tcp::TcpTransport;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::acceptor::ServerSessionConfig;
use crate::connection::ServeTransportOptions;
use crate::connection::ServerConnection;
use crate::connection::serve_transport;
use crate::eof_abort_tests::on_worker;

const REQUEST: &[u8] = b"POST /generate HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}";
const BOUND: Duration = Duration::from_secs(2);
/// A request id with surrounding whitespace, as a client may send it.
const PADDED_REQUEST_ID: &[u8] = b" rid-7\t";
/// A value the router generates with surrounding whitespace.
const PADDED_VALUE: &[u8] = b" v\t";

/// The response's body events, fed by the test, and what the session did to
/// the source.
#[derive(Default)]
struct Script {
    events: RefCell<VecDeque<Result<HttpBodyEvent, HttpError>>>,
    waker: RefCell<Option<Waker>>,
    reads: Cell<usize>,
    stopped: Cell<bool>,
    dropped: Cell<bool>,
    /// Each request's method token and `x-request-id`, as the handler read them.
    requests: RefCell<Vec<(String, Option<Vec<u8>>)>>,
}

impl Script {
    fn push(&self, event: Result<HttpBodyEvent, HttpError>) {
        self.events.borrow_mut().push_back(event);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn push_body(&self, bytes: &'static [u8]) {
        let body =
            BufQueue::from_bytes(Bytes::from_static(bytes), bytes.len()).expect("bounded body");
        self.push(Ok(HttpBodyEvent::body(body, false)));
    }
}

/// A streamed 200 that serves its body only through `poll_body_event`, as the
/// router's generation response source does.
struct PollingResponse {
    header: Option<HttpHeaderEvent>,
    script: Rc<Script>,
}

impl HttpSource for PollingResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.script.reads.set(self.script.reads.get() + 1);
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.script.stopped.set(true);
    }

    fn polls_body_events(&self) -> bool {
        true
    }

    fn poll_body_event(
        &mut self,
        context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        if let Some(event) = self.script.events.borrow_mut().pop_front() {
            return Poll::Ready(event);
        }
        *self.script.waker.borrow_mut() = Some(context.waker().clone());
        Poll::Pending
    }
}

impl Drop for PollingResponse {
    fn drop(&mut self) {
        self.script.dropped.set(true);
    }
}

/// Reads the request head, as the router does, and answers with a streamed 200
/// whose head echoes the request's `x-request-id` and carries `x-padded`, each
/// inserted with `add_verbatim`, as the router copies `http::HeaderValue` bytes.
struct PollingHandler(Rc<Script>);

impl HttpHandler for PollingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let script = Rc::clone(&self.0);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let request_id = head
                .headers()
                .headers()
                .get_single(b"x-request-id")
                .value
                .map(<[u8]>::to_vec);
            script.requests.borrow_mut().push((
                head.headers()
                    .request_method()
                    .unwrap_or_default()
                    .to_owned(),
                request_id.clone(),
            ));
            let mut message = HttpMessage::new();
            message.set_status_code(200).expect("status");
            message
                .set_status_message(HttpMessage::default_reason(200))
                .expect("reason");
            message.set_http_version(1, 1);
            if let Some(request_id) = request_id {
                message
                    .headers_mut()
                    .add_verbatim(b"x-request-id", request_id);
            }
            message
                .headers_mut()
                .add_verbatim(b"x-padded", PADDED_VALUE);
            let source = PollingResponse {
                header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
                script,
            };
            Ok(HttpSourceHolder::new(Box::new(source)))
        })
    }
}

/// The router's server settings, round 8.
fn router_session() -> ServerSessionConfig {
    ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        abort_exchanges_on_eof: true,
        abort_http2_streams_on_eof: true,
        non_abortive_close: true,
        exact_method_tokens: true,
        verbatim_http2_header_values: true,
        ..ServerSessionConfig::default()
    }
}

/// Serves one loopback connection with the router's server settings.
async fn serve_router_connection(
    context: &WorkerContext,
    script: &Rc<Script>,
    protocol: CodecProtocol,
) -> (ServerConnection, TcpStream) {
    serve_router_connection_with(context, script, protocol, router_session()).await
}

/// Serves one loopback connection with `session`.
async fn serve_router_connection_with(
    context: &WorkerContext,
    script: &Rc<Script>,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
) -> (ServerConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().expect("listener address")),
        listener.accept()
    );
    let (accepted, _) = accepted.expect("connection accepted");
    let transport = TcpTransport::from_accepted(
        context,
        accepted.into_std().expect("standard stream"),
        session.byte_stream_limits,
    )
    .expect("transport adopted");
    let mut options = ServeTransportOptions::new(protocol, TransportSecurity::Plaintext);
    options.session = session;
    let connection = serve_transport(
        context,
        transport,
        Rc::new(PollingHandler(Rc::clone(script))),
        options,
    )
    .expect("attached");
    (connection, client.expect("client connects"))
}

/// Reads into `wire` until it ends with `end`.
async fn read_until(client: &mut TcpStream, wire: &mut Vec<u8>, end: &[u8]) {
    while !wire.ends_with(end) {
        let mut chunk = [0; 1024];
        let read = client.read(&mut chunk).await.expect("response read");
        assert_ne!(read, 0, "the connection ends before {end:?}");
        wire.extend_from_slice(&chunk[..read]);
    }
}

/// Reads until the connection ends; returns whether it ended with a reset
/// rather than EOF.
async fn read_until_closed(client: &mut TcpStream, wire: &mut Vec<u8>) -> bool {
    loop {
        let mut chunk = [0; 1024];
        match client.read(&mut chunk).await {
            Ok(0) => return false,
            Ok(read) => wire.extend_from_slice(&chunk[..read]),
            Err(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
                return true;
            }
        }
    }
}

async fn within<Work: Future>(what: &str, work: Work) -> Work::Output {
    tokio::time::timeout(BOUND, work)
        .await
        .unwrap_or_else(|_| panic!("{what} within {BOUND:?}"))
}

const HEAD_END: &[u8] = b"\r\n\r\n";

/// A client that half-closes mid-stream (hyper's `half_close(false)` 499 path):
/// the polling source chain is stopped and dropped within a bound and the client
/// sees EOF, without the response's last chunk; `read_body_event` is never used.
#[test]
fn router_ingress_eof_mid_stream_drops_the_polling_source_and_ends_with_eof() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        let (connection, mut client) =
            serve_router_connection(&context, &script, CodecProtocol::Http11).await;
        client.write_all(REQUEST).await.expect("request written");
        let mut wire = Vec::new();
        read_until(&mut client, &mut wire, HEAD_END).await;
        script.push_body(b"first");
        read_until(&mut client, &mut wire, b"5\r\nfirst\r\n").await;

        client.shutdown().await.expect("client half-closes");
        within("the source is dropped", async {
            while !script.dropped.get() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let reset = within(
            "the connection ends",
            read_until_closed(&mut client, &mut wire),
        )
        .await;
        assert!(!reset, "the client sees EOF, not a reset");
        assert!(
            wire.ends_with(b"5\r\nfirst\r\n"),
            "nothing follows the written body, no last chunk: {:?}",
            String::from_utf8_lossy(&wire)
        );
        assert!(script.stopped.get(), "the source was stopped");
        assert_eq!(script.reads.get(), 0, "read_body_event is never called");
        let _ = within("the session closes", connection.wait_closed()).await;
    });
}

/// A response body error after some events: the client receives every byte
/// written, no last chunk, then FIN; `read_body_event` is never used.
#[test]
fn router_ingress_body_error_delivers_the_written_body_then_fin() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        let (connection, mut client) =
            serve_router_connection(&context, &script, CodecProtocol::Http11).await;
        client.write_all(REQUEST).await.expect("request written");
        let mut wire = Vec::new();
        read_until(&mut client, &mut wire, HEAD_END).await;
        script.push_body(b"first");
        read_until(&mut client, &mut wire, b"5\r\nfirst\r\n").await;
        script.push_body(b"second");
        read_until(&mut client, &mut wire, b"6\r\nsecond\r\n").await;

        script.push(Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"upstream body failed",
        )));
        let reset = within(
            "the connection ends",
            read_until_closed(&mut client, &mut wire),
        )
        .await;
        assert!(!reset, "the client sees FIN, not a reset");
        let head_end = wire
            .windows(HEAD_END.len())
            .position(|window| window == HEAD_END)
            .expect("a response head")
            + HEAD_END.len();
        assert_eq!(
            &wire[head_end..],
            b"5\r\nfirst\r\n6\r\nsecond\r\n",
            "the written body, without a last chunk"
        );
        assert!(script.dropped.get(), "the failed source is dropped");
        assert_eq!(script.reads.get(), 0, "read_body_event is never called");
        let _ = within("the session closes", connection.wait_closed()).await;
    });
}

fn contains(wire: &[u8], needle: &[u8]) -> bool {
    wire.windows(needle.len()).any(|window| window == needle)
}

/// An HTTP/1.1 POST with `method`, carrying `PADDED_REQUEST_ID`.
fn http1_request(method: &[u8]) -> Vec<u8> {
    [
        method,
        b" /generate HTTP/1.1\r\nHost: localhost\r\nx-request-id:",
        PADDED_REQUEST_ID,
        b"\r\nContent-Length: 2\r\n\r\n{}",
    ]
    .concat()
}

/// HTTP/1.1, one connection per recognized method (`POST`, `PURGE`): the handler
/// reads each method as its exact token and the request id trimmed (as
/// `httparse` trims it), and the response head carries each value byte for byte
/// as inserted, as hyper writes a `HeaderValue` (`name: value\r\n`).
#[test]
fn router_ingress_reads_llhttp_methods_and_writes_verbatim_response_values_over_http1() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        for method in [b"POST".as_slice(), b"PURGE"] {
            let (_connection, mut client) =
                serve_router_connection(&context, &script, CodecProtocol::Http11).await;
            client
                .write_all(&http1_request(method))
                .await
                .expect("request written");
            let mut wire = Vec::new();
            within(
                "the response head",
                read_until(&mut client, &mut wire, HEAD_END),
            )
            .await;
            let head = String::from_utf8_lossy(&wire);
            assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head:?}");
            assert!(contains(&wire, b"\r\nx-request-id: rid-7\r\n"), "{head:?}");
            assert!(
                contains(&wire, b"\r\nx-padded:  v\t\r\n"),
                "the padded value is written as inserted: {head:?}"
            );
        }
        assert_eq!(
            *script.requests.borrow(),
            [
                ("POST".to_owned(), Some(b"rid-7".to_vec())),
                ("PURGE".to_owned(), Some(b"rid-7".to_vec())),
            ],
        );
    });
}

/// h2c through h2's client, one stream per method (`POST`, `post`, `PURGE`): the
/// handler reads each method as its exact token and the padded request id as
/// sent, and h2 decodes the echoed request id and the padded value byte for
/// byte.
#[test]
fn router_ingress_reads_exact_methods_and_writes_verbatim_response_values_over_h2c() {
    on_worker(move |context| async move {
        exact_methods_and_verbatim_values_over_h2c(&context, router_session()).await;
    });
}

/// As `router_ingress_reads_exact_methods_and_writes_verbatim_response_values_over_h2c`,
/// with h2c on the owned HTTP/2 driver (`owned_http2`), which applies both options as
/// `UniplexSession` does.
#[test]
fn router_ingress_reads_exact_methods_and_writes_verbatim_response_values_over_owned_h2c() {
    on_worker(move |context| async move {
        let session = ServerSessionConfig {
            owned_http2: true,
            ..router_session()
        };
        exact_methods_and_verbatim_values_over_h2c(&context, session).await;
    });
}

/// Sends the three requests of the h2c method and value test through h2's client to a
/// connection served with `session`, and checks what the handler read and what h2 decoded.
async fn exact_methods_and_verbatim_values_over_h2c(
    context: &WorkerContext,
    session: ServerSessionConfig,
) {
    let script = Rc::new(Script::default());
    let (_connection, client) =
        serve_router_connection_with(context, &script, CodecProtocol::Http2, session).await;
    let (sender, connection) = h2::client::handshake(client).await.expect("h2 handshake");
    let mut connection = std::pin::pin!(connection);
    let exchanges = async move {
        let mut sender = sender;
        let mut heads = Vec::new();
        for method in ["POST", "post", "PURGE"] {
            let request = http::Request::builder()
                .method(method)
                .uri("http://localhost/generate")
                .header("x-request-id", PADDED_REQUEST_ID)
                .body(())
                .expect("request");
            sender = sender.ready().await.expect("stream capacity");
            let (response, _) = sender.send_request(request, true).expect("request sent");
            let response = response.await.expect("response head");
            assert_eq!(response.status(), 200);
            let value = |name| {
                response
                    .headers()
                    .get(name)
                    .map(|value: &http::HeaderValue| value.as_bytes().to_vec())
            };
            heads.push((value("x-request-id"), value("x-padded")));
        }
        heads
    };
    let heads = tokio::select! {
        heads = within("the responses", exchanges) => heads,
        ended = &mut connection => panic!("the h2 connection ended first: {ended:?}"),
    };
    assert_eq!(
        heads,
        vec![
            (
                Some(PADDED_REQUEST_ID.to_vec()),
                Some(PADDED_VALUE.to_vec())
            );
            3
        ]
    );
    assert_eq!(
        *script.requests.borrow(),
        [
            ("POST".to_owned(), Some(PADDED_REQUEST_ID.to_vec())),
            ("post".to_owned(), Some(PADDED_REQUEST_ID.to_vec())),
            ("PURGE".to_owned(), Some(PADDED_REQUEST_ID.to_vec())),
        ],
    );
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small frame");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// The client preface, SETTINGS opening the stream window to the maximum, a
/// connection WINDOW_UPDATE to the maximum, and a bodiless POST /generate on
/// stream 1.
fn h2c_request() -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend(h2_frame(0x4, 0, 0, &[0x00, 0x04, 0x7f, 0xff, 0xff, 0xff]));
    let increment: u32 = 0x7fff_ffff - 65_535;
    wire.extend(h2_frame(0x8, 0, 0, &increment.to_be_bytes()));
    // :method POST, :scheme http, then :path and :authority without indexing.
    let mut block = vec![0x83, 0x86, 0x04, 9];
    block.extend_from_slice(b"/generate");
    block.extend_from_slice(&[0x01, 9]);
    block.extend_from_slice(b"localhost");
    wire.extend(h2_frame(0x1, 0x5, 1, &block));
    wire
}

/// One frame the server sent.
#[derive(Debug)]
struct Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

/// The whole frames at the start of `wire`, after the server's connection
/// preface (none: a server sends no preface beyond its SETTINGS frame).
fn h2_frames(mut wire: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    while wire.len() >= 9 {
        let length = usize::from(wire[0]) << 16 | usize::from(wire[1]) << 8 | usize::from(wire[2]);
        let Some(payload) = wire.get(9..9 + length) else {
            break;
        };
        frames.push(Frame {
            kind: wire[3],
            flags: wire[4],
            stream: u32::from_be_bytes([wire[5], wire[6], wire[7], wire[8]]) & 0x7fff_ffff,
            payload: payload.to_vec(),
        });
        wire = &wire[9 + length..];
    }
    frames
}

/// Stream 1's DATA payload so far, and whether a frame ended the stream.
fn stream_1_body(wire: &[u8]) -> (Vec<u8>, bool) {
    h2_frames(wire)
        .iter()
        .filter(|frame| frame.stream == 1)
        .fold((Vec::new(), false), |(mut body, ended), frame| {
            if frame.kind == 0x0 {
                body.extend_from_slice(&frame.payload);
            }
            (body, ended || frame.flags & 0x1 != 0 || frame.kind == 0x3)
        })
}

/// Reads into `wire` until `done` holds for the whole frames read.
async fn read_h2_until(client: &mut TcpStream, wire: &mut Vec<u8>, done: impl Fn(&[u8]) -> bool) {
    while !done(wire) {
        let mut chunk = [0; 1024];
        let read = client.read(&mut chunk).await.expect("response read");
        assert_ne!(read, 0, "the connection ends early");
        wire.extend_from_slice(&chunk[..read]);
    }
}

fn has_stream_1_headers(wire: &[u8]) -> bool {
    h2_frames(wire)
        .iter()
        .any(|frame| frame.stream == 1 && frame.kind == 0x1)
}

/// A client that half-closes an h2c connection mid-stream: a transport EOF
/// aborts every open stream (`DIV-055`), as hyper's h2 server does, so the
/// polling source chain is stopped and dropped within a bound and the
/// connection ends with EOF (`non_abortive_close`), the stream never ended;
/// `read_body_event` is never used.
#[test]
fn router_ingress_h2c_eof_mid_stream_drops_the_polling_source_and_ends_with_eof() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        let (connection, mut client) =
            serve_router_connection(&context, &script, CodecProtocol::Http2).await;
        client
            .write_all(&h2c_request())
            .await
            .expect("request written");
        let mut wire = Vec::new();
        within(
            "the response head",
            read_h2_until(&mut client, &mut wire, has_stream_1_headers),
        )
        .await;
        script.push_body(b"first");
        within(
            "the first event",
            read_h2_until(&mut client, &mut wire, |wire| {
                stream_1_body(wire).0 == b"first"
            }),
        )
        .await;

        client.shutdown().await.expect("client half-closes");
        within("the source is dropped", async {
            while !script.dropped.get() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let reset = within(
            "the connection ends",
            read_until_closed(&mut client, &mut wire),
        )
        .await;
        assert!(!reset, "the client sees EOF, not a reset");
        assert_eq!(
            stream_1_body(&wire),
            (b"first".to_vec(), false),
            "nothing follows the written event and the stream never ends"
        );
        assert!(script.stopped.get(), "the source was stopped");
        assert_eq!(script.reads.get(), 0, "read_body_event is never called");
        let _ = within("the session closes", connection.wait_closed()).await;
    });
}

/// Without `abort_http2_streams_on_eof`, a client that half-closes an h2c
/// connection mid-stream keeps the stream: `abort_exchanges_on_eof` alone
/// leaves HTTP/2 unchanged (`DIV-053`), so the stream and its polling source
/// stay open, the rest of the response reaches the client, and the connection
/// then closes cleanly; `read_body_event` is never used.
#[test]
fn router_ingress_h2c_half_close_mid_stream_keeps_the_stream_without_abort_http2_streams_on_eof() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        let session = ServerSessionConfig {
            abort_http2_streams_on_eof: false,
            ..router_session()
        };
        let (connection, mut client) =
            serve_router_connection_with(&context, &script, CodecProtocol::Http2, session).await;
        client
            .write_all(&h2c_request())
            .await
            .expect("request written");
        let mut wire = Vec::new();
        within(
            "the response head",
            read_h2_until(&mut client, &mut wire, has_stream_1_headers),
        )
        .await;
        script.push_body(b"first");
        within(
            "the first event",
            read_h2_until(&mut client, &mut wire, |wire| {
                stream_1_body(wire).0 == b"first"
            }),
        )
        .await;

        client.shutdown().await.expect("client half-closes");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !script.stopped.get() && !script.dropped.get(),
            "the half-close neither stops nor drops the source"
        );
        let second = BufQueue::from_bytes(Bytes::from_static(b"second"), 6).expect("bounded body");
        script.push(Ok(HttpBodyEvent::body(second, true)));
        within(
            "the rest of the response",
            read_h2_until(&mut client, &mut wire, |wire| stream_1_body(wire).1),
        )
        .await;
        assert_eq!(
            stream_1_body(&wire),
            (b"firstsecond".to_vec(), true),
            "the whole body, then the stream's end"
        );
        let reset = within(
            "the connection ends",
            read_until_closed(&mut client, &mut wire),
        )
        .await;
        assert!(!reset, "the connection closes cleanly");
        assert!(!script.stopped.get(), "the source was never stopped");
        assert_eq!(script.reads.get(), 0, "read_body_event is never called");
        let _ = within("the session closes", connection.wait_closed()).await;
    });
}

/// A response body error after some events on h2c: the stream is reset with
/// INTERNAL_ERROR, as hyper's h2 server resets it, after every event written,
/// and the connection stays open (it answers a PING); `read_body_event` is never
/// used.
#[test]
fn router_ingress_h2c_body_error_resets_the_stream_and_keeps_the_connection() {
    on_worker(move |context| async move {
        let script = Rc::new(Script::default());
        let (_connection, mut client) =
            serve_router_connection(&context, &script, CodecProtocol::Http2).await;
        client
            .write_all(&h2c_request())
            .await
            .expect("request written");
        let mut wire = Vec::new();
        within(
            "the response head",
            read_h2_until(&mut client, &mut wire, has_stream_1_headers),
        )
        .await;
        script.push_body(b"first");
        within(
            "the first event",
            read_h2_until(&mut client, &mut wire, |wire| {
                stream_1_body(wire).0 == b"first"
            }),
        )
        .await;
        script.push_body(b"second");
        within(
            "the second event",
            read_h2_until(&mut client, &mut wire, |wire| {
                stream_1_body(wire).0 == b"firstsecond"
            }),
        )
        .await;

        script.push(Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"upstream body failed",
        )));
        within(
            "the stream reset",
            read_h2_until(&mut client, &mut wire, |wire| stream_1_body(wire).1),
        )
        .await;
        let stream_1: Vec<_> = h2_frames(&wire)
            .into_iter()
            .filter(|frame| frame.stream == 1)
            .map(|frame| (frame.kind, frame.flags & 0x1, frame.payload))
            .collect();
        assert_eq!(
            stream_1,
            [
                (0x1, 0, stream_1[0].2.clone()),
                (0x0, 0, b"first".to_vec()),
                (0x0, 0, b"second".to_vec()),
                (0x3, 0, 0x2_u32.to_be_bytes().to_vec()),
            ],
            "HEADERS, both events, then RST_STREAM(INTERNAL_ERROR), never END_STREAM"
        );
        within("the source is dropped", async {
            while !script.dropped.get() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;

        client
            .write_all(&h2_frame(0x6, 0, 0, b"liveness"))
            .await
            .expect("ping written");
        within(
            "the PING ACK",
            read_h2_until(&mut client, &mut wire, |wire| {
                h2_frames(wire).iter().any(|frame| {
                    frame.kind == 0x6 && frame.flags & 0x1 != 0 && frame.payload == b"liveness"
                })
            }),
        )
        .await;
        assert!(
            !h2_frames(&wire).iter().any(|frame| frame.kind == 0x7),
            "no GOAWAY: the connection stays open"
        );
        assert_eq!(script.reads.get(), 0, "read_body_event is never called");
    });
}
