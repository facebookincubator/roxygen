//! llhttp HTTP/1 methods, opt-in exact HTTP/2 method tokens (`DIV-040`), and
//! verbatim HTTP/2 field values (`DIV-041`) over `serve_transport` and loopback TCP, on a `RuntimeOwner`
//! worker and a `HostedWorker`, for HTTP/1.1 and h2c, next to hyper's server
//! answering the same bytes.

use std::cell::Cell;
use std::cell::RefCell;
use std::convert::Infallible;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use http_body::Body as _;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::source::HttpSource;
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

const METHODS: [&[u8]; 4] = [b"POST", b"post", b"Post", b"PURGE"];

/// Header fields whose values have surrounding whitespace, are empty, or repeat.
const PADDED_FIELDS: [(&[u8], &[u8]); 8] = [
    (b"user-agent", b" ua\t"),
    (b"x-lead", b"  leading"),
    (b"x-trail", b"trailing\t"),
    (b"x-both", b"\t both "),
    (b"x-empty", b""),
    (b"x-only-space", b" \t "),
    (b"x-dup", b" one"),
    (b"x-dup", b"two "),
];

/// `PADDED_FIELDS` with optional whitespace around each value removed.
const TRIMMED_FIELDS: [(&[u8], &[u8]); 8] = [
    (b"user-agent", b"ua"),
    (b"x-lead", b"leading"),
    (b"x-trail", b"trailing"),
    (b"x-both", b"both"),
    (b"x-empty", b""),
    (b"x-only-space", b""),
    (b"x-dup", b"one"),
    (b"x-dup", b"two"),
];

/// What the application saw of one request: its method, and every header but
/// `host`, in order, names in lowercase.
#[derive(Debug, PartialEq)]
struct Seen {
    method: String,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
}

fn fields(fields: &[(&[u8], &[u8])]) -> Vec<(Vec<u8>, Vec<u8>)> {
    fields
        .iter()
        .map(|(name, value)| (name.to_vec(), value.to_vec()))
        .collect()
}

/// Records what each request carried as the application reads it, and answers 200.
struct RecordingHandler;

impl HttpHandler for RecordingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let message = head.headers();
            let mut headers = Vec::new();
            message.headers().for_each(|name, value| {
                if !name.eq_ignore_ascii_case(b"host") {
                    headers.push((name.to_ascii_lowercase(), value.to_vec()));
                }
            });
            let seen = Seen {
                method: message.request_method().unwrap_or_default().to_owned(),
                headers,
            };
            RECORDED.with(|recorded| recorded.borrow_mut().push(seen));
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

thread_local! {
    static RECORDED: RefCell<Vec<Seen>> = const { RefCell::new(Vec::new()) };
}

fn session(exact_method_tokens: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        exact_method_tokens,
        ..ServerSessionConfig::default()
    }
}

fn verbatim_session(verbatim_http2_header_values: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        verbatim_http2_header_values,
        ..session(false)
    }
}

async fn connect_served(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
) -> (ServerConnection, TcpStream) {
    connect_served_with(context, protocol, session, Rc::new(RecordingHandler)).await
}

async fn connect_served_with(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
    handler: HttpHandlerRef,
) -> (ServerConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let client = TcpStream::connect(listener.local_addr().expect("address"))
        .await
        .expect("client connects");
    let (accepted, _) = listener.accept().await.expect("accepted");
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

fn http1_request(method: &[u8]) -> Vec<u8> {
    [
        method,
        b" /generate HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
    ]
    .concat()
}

/// A GET carrying `fields`, each as `name: value` with no other whitespace.
fn http1_request_with_fields(fields: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut wire = b"GET /generate HTTP/1.1\r\nHost: localhost\r\n".to_vec();
    for (name, value) in fields {
        wire.extend_from_slice(name);
        wire.extend_from_slice(b":");
        wire.extend_from_slice(value);
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(b"\r\n");
    wire
}

/// Reads one HTTP/1.1 response head and returns its status code.
async fn http1_status(client: &mut TcpStream) -> u16 {
    let mut head = Vec::new();
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut chunk = [0; 512];
        let read = client.read(&mut chunk).await.expect("response read");
        assert_ne!(read, 0, "a response arrives before close: {head:?}");
        head.extend_from_slice(&chunk[..read]);
    }
    std::str::from_utf8(&head[9..12])
        .expect("status digits")
        .parse()
        .expect("status code")
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).expect("small frame");
    let mut frame = length.to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// HPACK literal fields without indexing, each with an indexed name when the
/// static table has it (`:method` 2, `:path` 4, `:authority` 1) and a literal
/// name otherwise; `:scheme http` is static entry 6.
fn h2_request_block(method: &[u8], fields: &[(&[u8], &[u8])]) -> Vec<u8> {
    let literal = |block: &mut Vec<u8>, value: &[u8]| {
        block.push(u8::try_from(value.len()).expect("short value"));
        block.extend_from_slice(value);
    };
    let mut block = vec![0x02];
    literal(&mut block, method);
    block.push(0x86);
    block.push(0x04);
    literal(&mut block, b"/generate");
    block.push(0x01);
    literal(&mut block, b"localhost");
    for (name, value) in fields {
        block.push(0x00);
        literal(&mut block, name);
        literal(&mut block, value);
    }
    block
}

/// The client preface, empty SETTINGS, and one bodiless request per method on
/// streams 1, 3, 5, ...
fn h2c_requests(methods: &[&[u8]]) -> Vec<u8> {
    h2c_requests_with_fields(methods, &[])
}

/// `h2c_requests`, each request carrying `fields`.
fn h2c_requests_with_fields(methods: &[&[u8]], fields: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend(h2_frame(0x4, 0, 0, &[]));
    for (index, method) in methods.iter().enumerate() {
        let stream = 1 + 2 * u32::try_from(index).expect("few streams");
        wire.extend(h2_frame(
            0x1,
            0x5,
            stream,
            &h2_request_block(method, fields),
        ));
    }
    wire
}

/// Reads until a HEADERS frame arrived on each of `streams`.
async fn h2_until_headers(client: &mut TcpStream, streams: usize) {
    let mut wire = Vec::new();
    loop {
        let mut seen = 0;
        let mut rest = wire.as_slice();
        while rest.len() >= 9 {
            let length =
                usize::from(rest[0]) << 16 | usize::from(rest[1]) << 8 | usize::from(rest[2]);
            if rest.len() < 9 + length {
                break;
            }
            seen += usize::from(rest[3] == 0x1);
            rest = &rest[9 + length..];
        }
        if seen == streams {
            return;
        }
        let mut chunk = [0; 1024];
        let read = client.read(&mut chunk).await.expect("response read");
        assert_ne!(read, 0, "the responses arrive before close");
        wire.extend_from_slice(&chunk[..read]);
    }
}

/// Sends each of `requests` and returns what the application saw, and each
/// response's status over HTTP/1.1 (one connection per request); over h2c the
/// one request is a whole connection's bytes carrying `streams` requests.
async fn serve(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
    requests: &[Vec<u8>],
    streams: usize,
) -> (Vec<Seen>, Vec<u16>) {
    RECORDED.with(|recorded| recorded.borrow_mut().clear());
    let mut statuses = Vec::new();
    for request in requests {
        let (_connection, mut client) = connect_served(context, protocol, session.clone()).await;
        client.write_all(request).await.expect("request written");
        if protocol == CodecProtocol::Http11 {
            statuses.push(http1_status(&mut client).await);
        } else {
            h2_until_headers(&mut client, streams).await;
        }
    }
    (RECORDED.with(|recorded| recorded.take()), statuses)
}

/// The methods the application saw, and each HTTP/1.1 request's status (one
/// connection per request), or the h2c responses (one connection).
async fn serve_methods(
    context: WorkerContext,
    protocol: CodecProtocol,
    exact_method_tokens: bool,
) -> (Vec<String>, Vec<u16>) {
    serve_methods_with(context, protocol, session(exact_method_tokens)).await
}

/// As `serve_methods`, with the session configuration `session`.
async fn serve_methods_with(
    context: WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
) -> (Vec<String>, Vec<u16>) {
    let (seen, statuses) = serve(
        &context,
        protocol,
        session,
        &method_requests(protocol),
        METHODS.len(),
    )
    .await;
    (seen.into_iter().map(|seen| seen.method).collect(), statuses)
}

/// `session(exact_method_tokens)` with `owned_http1`.
fn owned_session(exact_method_tokens: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        owned_http1: true,
        ..session(exact_method_tokens)
    }
}

fn method_requests(protocol: CodecProtocol) -> Vec<Vec<u8>> {
    if protocol == CodecProtocol::Http11 {
        METHODS.iter().map(|method| http1_request(method)).collect()
    } else {
        vec![h2c_requests(&METHODS)]
    }
}

fn field_requests(protocol: CodecProtocol) -> Vec<Vec<u8>> {
    if protocol == CodecProtocol::Http11 {
        vec![http1_request_with_fields(&PADDED_FIELDS)]
    } else {
        vec![h2c_requests_with_fields(&[b"GET"], &PADDED_FIELDS)]
    }
}

/// The header fields the application saw on the one request of `field_requests`.
async fn serve_fields(
    context: WorkerContext,
    protocol: CodecProtocol,
    verbatim_http2_header_values: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    serve_fields_with(
        context,
        protocol,
        verbatim_session(verbatim_http2_header_values),
    )
    .await
}

/// As `serve_fields`, with the session configuration `session`.
async fn serve_fields_with(
    context: WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut seen, _) = serve(&context, protocol, session, &field_requests(protocol), 1).await;
    assert_eq!(seen.len(), 1);
    seen.remove(0).headers
}

/// `verbatim_session(verbatim_http2_header_values)` with `owned_http2`.
fn owned_h2_verbatim_session(verbatim_http2_header_values: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        owned_http2: true,
        ..verbatim_session(verbatim_http2_header_values)
    }
}

/// What hyper's server (hyper-util's `auto` builder, as `axum::serve` uses it)
/// shows its service for the same `requests` (as `serve` sends them).
fn hyper_seen(protocol: CodecProtocol, requests: &[Vec<u8>], streams: usize) -> Vec<Seen> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("hyper runtime");
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let seen = Rc::new(RefCell::new(Vec::new()));
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let serve_seen = Rc::clone(&seen);
                tokio::task::spawn_local(async move {
                    loop {
                        let (stream, _) = listener.accept().await.expect("accepted");
                        let seen = Rc::clone(&serve_seen);
                        tokio::task::spawn_local(async move {
                            let service = hyper::service::service_fn(
                                move |request: hyper::Request<hyper::body::Incoming>| {
                                    let headers = request
                                        .headers()
                                        .iter()
                                        .filter(|(name, _)| *name != "host")
                                        .map(|(name, value)| {
                                            (
                                                name.as_str().as_bytes().to_vec(),
                                                value.as_bytes().to_vec(),
                                            )
                                        })
                                        .collect();
                                    seen.borrow_mut().push(Seen {
                                        method: request.method().as_str().to_owned(),
                                        headers,
                                    });
                                    std::future::ready(Ok::<_, Infallible>(hyper::Response::new(
                                        http_body_util_empty(),
                                    )))
                                },
                            );
                            let _ = hyper_util::server::conn::auto::Builder::new(
                                hyper_util::rt::TokioExecutor::new(),
                            )
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .await;
                        });
                    }
                });
                for request in requests {
                    let mut client = TcpStream::connect(address).await.expect("connects");
                    client.write_all(request).await.expect("request written");
                    if protocol == CodecProtocol::Http11 {
                        http1_status(&mut client).await;
                    } else {
                        h2_until_headers(&mut client, streams).await;
                    }
                }
            })
            .await;
        seen.take()
    })
}

fn hyper_methods(protocol: CodecProtocol) -> Vec<String> {
    hyper_seen(protocol, &method_requests(protocol), METHODS.len())
        .into_iter()
        .map(|seen| seen.method)
        .collect()
}

fn hyper_fields(protocol: CodecProtocol) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut seen = hyper_seen(protocol, &field_requests(protocol), 1);
    assert_eq!(seen.len(), 1);
    seen.remove(0).headers
}

/// An empty body for hyper's responses, without an extra crate.
fn http_body_util_empty() -> String {
    String::new()
}

#[test]
fn exact_method_tokens_are_off_by_default() {
    assert!(!ServerSessionConfig::default().exact_method_tokens);
}

#[test]
fn http2_method_option_does_not_broaden_http1_methods() {
    on_worker(|context| async move {
        let (methods, statuses) = serve_methods(context, CodecProtocol::Http11, true).await;
        assert_eq!(methods, ["POST", "PURGE"]);
        assert_eq!(statuses, [200, 400, 400, 200]);
    });
    assert_eq!(
        hyper_methods(CodecProtocol::Http11),
        ["POST", "post", "Post", "PURGE"]
    );
}

#[test]
fn exact_method_tokens_reach_the_application_byte_for_byte_over_h2c_when_enabled() {
    on_worker(|context| async move {
        let (methods, _) = serve_methods(context, CodecProtocol::Http2, true).await;
        assert_eq!(methods, ["POST", "post", "Post", "PURGE"]);
    });
    assert_eq!(
        hyper_methods(CodecProtocol::Http2),
        ["POST", "post", "Post", "PURGE"]
    );
}

#[test]
fn http1_requests_with_a_method_outside_llhttp_table_get_400_by_default() {
    on_worker(|context| async move {
        let (methods, statuses) = serve_methods(context, CodecProtocol::Http11, false).await;
        assert_eq!(methods, ["POST", "PURGE"]);
        assert_eq!(statuses, [200, 400, 400, 200]);
    });
}

#[test]
fn h2c_request_methods_are_canonicalized_by_default() {
    on_worker(|context| async move {
        let (methods, _) = serve_methods(context, CodecProtocol::Http2, false).await;
        assert_eq!(methods, ["POST", "POST", "POST", "PURGE"]);
    });
}

#[test]
fn owned_http1_configuration_accepts_the_http2_method_option() {
    assert_eq!(owned_session(true).check_owned_http1(), Ok(()));
}

#[test]
fn http2_method_option_does_not_broaden_owned_http1_methods() {
    on_worker(|context| async move {
        let (methods, statuses) =
            serve_methods_with(context, CodecProtocol::Http11, owned_session(true)).await;
        assert_eq!(methods, ["POST", "PURGE"]);
        assert_eq!(statuses, [200, 400, 400, 200]);
    });
}

#[test]
fn owned_http1_requests_with_a_method_outside_llhttp_table_get_400_by_default() {
    on_worker(|context| async move {
        let (methods, statuses) =
            serve_methods_with(context, CodecProtocol::Http11, owned_session(false)).await;
        assert_eq!(methods, ["POST", "PURGE"]);
        assert_eq!(statuses, [200, 400, 400, 200]);
    });
}

#[test]
fn exact_method_tokens_still_refuse_a_method_that_is_not_a_token_over_owned_http1() {
    on_worker(|context| async move {
        RECORDED.with(|recorded| recorded.borrow_mut().clear());
        let (_connection, mut client) =
            connect_served(&context, CodecProtocol::Http11, owned_session(true)).await;
        client
            .write_all(&http1_request(b"P@ST"))
            .await
            .expect("request written");
        assert_eq!(http1_status(&mut client).await, 400);
        assert!(RECORDED.with(|recorded| recorded.borrow().is_empty()));
    });
}

/// The frame types (and RST_STREAM error codes) the server sent on stream 1,
/// until the stream or the connection ends.
async fn h2_stream_1_frames(client: &mut TcpStream) -> Vec<(u8, u32)> {
    h2_stream_1_frame_bytes(client)
        .await
        .into_iter()
        .map(|(kind, _, payload)| {
            let code = if kind == 0x3 {
                u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
            } else {
                0
            };
            (kind, code)
        })
        .collect()
}

/// Each frame the server sent on stream 1 as its type, flags and payload, until the
/// stream or the connection ends.
async fn h2_stream_1_frame_bytes(client: &mut TcpStream) -> Vec<(u8, u8, Vec<u8>)> {
    let mut wire = Vec::new();
    loop {
        let mut frames = Vec::new();
        let mut rest = wire.as_slice();
        let mut ended = false;
        while rest.len() >= 9 {
            let length =
                usize::from(rest[0]) << 16 | usize::from(rest[1]) << 8 | usize::from(rest[2]);
            if rest.len() < 9 + length {
                break;
            }
            let stream = u32::from_be_bytes([rest[5], rest[6], rest[7], rest[8]]) & 0x7fff_ffff;
            if stream == 1 {
                frames.push((rest[3], rest[4], rest[9..9 + length].to_vec()));
                ended |= rest[3] == 0x3 || rest[4] & 0x1 != 0;
            }
            ended |= rest[3] == 0x7;
            rest = &rest[9 + length..];
        }
        if ended {
            return frames;
        }
        let mut chunk = [0; 1024];
        let read = client.read(&mut chunk).await.expect("response read");
        if read == 0 {
            return frames;
        }
        wire.extend_from_slice(&chunk[..read]);
    }
}

#[test]
fn exact_method_tokens_still_refuse_a_method_that_is_not_a_token() {
    on_worker(|context| async move {
        RECORDED.with(|recorded| recorded.borrow_mut().clear());
        let (_connection, mut client) =
            connect_served(&context, CodecProtocol::Http11, session(true)).await;
        client
            .write_all(&http1_request(b"P@ST"))
            .await
            .expect("request written");
        assert_eq!(http1_status(&mut client).await, 400);
        let (_connection, mut client) =
            connect_served(&context, CodecProtocol::Http2, session(true)).await;
        client
            .write_all(&h2c_requests(&[b"P@ST"]))
            .await
            .expect("request written");
        // One HEADERS frame ending the stream: the session's 400 for a malformed
        // request (h2 instead treats such a `:method` as a connection error).
        assert_eq!(h2_stream_1_frames(&mut client).await, [(0x1, 0)]);
        assert!(RECORDED.with(|recorded| recorded.borrow().is_empty()));
    });
}

#[test]
fn verbatim_http2_header_values_are_off_by_default() {
    assert!(!ServerSessionConfig::default().verbatim_http2_header_values);
}

#[test]
fn verbatim_http2_header_values_reach_the_application_as_sent_over_h2c_when_enabled() {
    on_worker(|context| async move {
        assert_eq!(
            serve_fields(context, CodecProtocol::Http2, true).await,
            fields(&PADDED_FIELDS)
        );
    });
    assert_eq!(hyper_fields(CodecProtocol::Http2), fields(&PADDED_FIELDS));
}

#[test]
fn h2c_field_values_are_trimmed_by_default() {
    on_worker(|context| async move {
        assert_eq!(
            serve_fields(context, CodecProtocol::Http2, false).await,
            fields(&TRIMMED_FIELDS)
        );
    });
}

#[test]
fn owned_http2_accepts_verbatim_http2_header_values() {
    assert_eq!(owned_h2_verbatim_session(true).check_owned_http2(), Ok(()));
}

#[test]
fn verbatim_http2_header_values_reach_the_application_as_sent_over_owned_h2c_when_enabled() {
    on_worker(|context| async move {
        assert_eq!(
            serve_fields_with(
                context,
                CodecProtocol::Http2,
                owned_h2_verbatim_session(true)
            )
            .await,
            fields(&PADDED_FIELDS)
        );
    });
}

#[test]
fn owned_h2c_field_values_are_trimmed_by_default() {
    on_worker(|context| async move {
        assert_eq!(
            serve_fields_with(
                context,
                CodecProtocol::Http2,
                owned_h2_verbatim_session(false)
            )
            .await,
            fields(&TRIMMED_FIELDS)
        );
    });
}

#[test]
fn http1_field_values_keep_their_whitespace_trimmed_with_verbatim_http2_header_values() {
    on_worker(|context| async move {
        assert_eq!(
            serve_fields(context, CodecProtocol::Http11, true).await,
            fields(&TRIMMED_FIELDS)
        );
    });
    assert_eq!(hyper_fields(CodecProtocol::Http11), fields(&TRIMMED_FIELDS));
}

/// What the application saw of one request.
#[derive(Clone, Debug, PartialEq)]
enum RequestSeen {
    /// Reading the head failed.
    HeadFailed,
    /// The head's `content-length` values, and the body, or `None` if reading it
    /// failed.
    Read {
        content_length: Vec<Vec<u8>>,
        body: Option<Vec<u8>>,
    },
}

thread_local! {
    static REQUESTS: RefCell<Vec<RequestSeen>> = const { RefCell::new(Vec::new()) };
    /// The request heads `BodyRecordingHandler` has read.
    static HEADS_READ: Cell<usize> = const { Cell::new(0) };
}

/// Reads each request to its end and records what it saw; answers 200, or fails
/// the exchange if a read failed.
struct BodyRecordingHandler;

impl HttpHandler for BodyRecordingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = match request.read_header_event().await {
                Ok(head) => head,
                Err(error) => {
                    REQUESTS.with(|seen| seen.borrow_mut().push(RequestSeen::HeadFailed));
                    return Err(error);
                }
            };
            HEADS_READ.with(|heads| heads.set(heads.get() + 1));
            let mut content_length = Vec::new();
            head.headers().headers().for_each(|name, value| {
                if name.eq_ignore_ascii_case(b"content-length") {
                    content_length.push(value.to_vec());
                }
            });
            let mut eom = head.eom();
            let mut body = Some(Vec::new());
            while !eom {
                let Ok(event) = request.read_body_event(u32::MAX).await else {
                    body = None;
                    break;
                };
                eom = event.eom();
                if let (Some(body), Some(bytes)) = (body.as_mut(), event.as_body()) {
                    body.extend_from_slice(&bytes.coalesce(usize::MAX).expect("body bytes"));
                }
            }
            let failed = body.is_none();
            REQUESTS.with(|seen| {
                seen.borrow_mut().push(RequestSeen::Read {
                    content_length,
                    body,
                })
            });
            if failed {
                return Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"request body failed",
                ));
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

/// An h2c connection carrying one POST with `content-length: <content_length>`,
/// ended by its HEADERS frame or, with `data`, by one DATA frame carrying it.
fn h2c_content_length_request(content_length: &[u8], data: Option<&[u8]>) -> Vec<u8> {
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend(h2_frame(0x4, 0, 0, &[]));
    let block = h2_request_block(b"POST", &[(b"content-length", content_length)]);
    match data {
        None => wire.extend(h2_frame(0x1, 0x5, 1, &block)),
        Some(data) => {
            wire.extend(h2_frame(0x1, 0x4, 1, &block));
            wire.extend(h2_frame(0x0, 0x1, 1, data));
        }
    }
    wire
}

/// Waits, within a bound, until `recorded` returns an entry, and returns the one.
async fn one_recorded(mut recorded: impl FnMut() -> Vec<RequestSeen>) -> RequestSeen {
    let mut seen = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while seen.is_empty() {
            seen = recorded();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the application read the request within the bound");
    assert_eq!(seen.len(), 1);
    seen.remove(0)
}

/// What the application saw of `request` over h2c, and the frames (with
/// RST_STREAM codes) the server sent on stream 1.
async fn serve_body(
    context: &WorkerContext,
    session: ServerSessionConfig,
    request: &[u8],
) -> (RequestSeen, Vec<(u8, u32)>) {
    REQUESTS.with(|seen| seen.borrow_mut().clear());
    let (_connection, mut client) = connect_served_with(
        context,
        CodecProtocol::Http2,
        session,
        Rc::new(BodyRecordingHandler),
    )
    .await;
    client.write_all(request).await.expect("request written");
    let frames = h2_stream_1_frames(&mut client).await;
    let seen = one_recorded(|| REQUESTS.with(|seen| seen.take())).await;
    (seen, frames)
}

/// What hyper's service sees of `request` over h2c, reading its body to its end,
/// and the frames (with RST_STREAM codes) hyper's server sent on stream 1.
fn hyper_body_seen(request: &[u8]) -> (RequestSeen, Vec<(u8, u32)>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("hyper runtime");
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let serve_seen = Arc::clone(&seen);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accepted");
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let seen = Arc::clone(&serve_seen);
                    async move {
                        let content_length = request
                            .headers()
                            .get_all("content-length")
                            .iter()
                            .map(|value| value.as_bytes().to_vec())
                            .collect();
                        let mut incoming = request.into_body();
                        let mut body = Some(Vec::new());
                        while let Some(frame) =
                            std::future::poll_fn(|cx| Pin::new(&mut incoming).poll_frame(cx)).await
                        {
                            let Ok(frame) = frame else {
                                body = None;
                                break;
                            };
                            if let (Some(body), Ok(data)) = (body.as_mut(), frame.into_data()) {
                                body.extend_from_slice(&data);
                            }
                        }
                        seen.lock().expect("seen").push(RequestSeen::Read {
                            content_length,
                            body,
                        });
                        Ok::<_, Infallible>(hyper::Response::new(http_body_util_empty()))
                    }
                },
            );
            let _ =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await;
        });
        let mut client = TcpStream::connect(address).await.expect("connects");
        client.write_all(request).await.expect("request written");
        let frames = h2_stream_1_frames(&mut client).await;
        let seen = one_recorded(|| std::mem::take(&mut *seen.lock().expect("seen"))).await;
        (seen, frames)
    })
}

#[test]
fn an_empty_h2c_content_length_ending_the_request_reads_as_zero_as_h2_reads_it() {
    let request = h2c_content_length_request(b"", None);
    let expected = (
        RequestSeen::Read {
            content_length: vec![Vec::new()],
            body: Some(Vec::new()),
        },
        vec![(0x1, 0)],
    );
    assert_eq!(
        hyper_body_seen(&request),
        expected,
        "hyper's service reads the empty field and an empty body, and its 200 is one \
         HEADERS frame"
    );
    on_worker(move |context| async move {
        assert_eq!(
            serve_body(&context, verbatim_session(true), &request).await,
            expected
        );
    });
}

#[test]
fn an_empty_h2c_content_length_followed_by_data_fails_the_request_as_a_zero_one_does() {
    let empty = h2c_content_length_request(b"", Some(b"abc"));
    let zero = h2c_content_length_request(b"0", Some(b"abc"));
    // h2 reads both lengths as zero: hyper's service reads the head, the body read
    // fails, and h2 resets the stream with PROTOCOL_ERROR.
    for (request, value) in [(&empty, b"".as_slice()), (&zero, b"0")] {
        assert_eq!(
            hyper_body_seen(request),
            (
                RequestSeen::Read {
                    content_length: vec![value.to_vec()],
                    body: None,
                },
                vec![(0x3, 0x1)],
            ),
            "{value:?}"
        );
    }
    // Roxygen fails both as it fails any overrun of a declared length that arrives
    // with the head: the error discards the head before the handler reads it, and
    // the stream is reset with INTERNAL_ERROR (`DIV-041`).
    on_worker(move |context| async move {
        for request in [&empty, &zero] {
            assert_eq!(
                serve_body(&context, verbatim_session(true), request).await,
                (RequestSeen::HeadFailed, vec![(0x3, 0x2)])
            );
        }
    });
}

/// What the application recorded of `request` over h2c, if it read anything, and the frames
/// (with RST_STREAM codes) the server sent on stream 1. A request the session refuses before
/// its head reaches the handler records nothing.
async fn serve_body_recorded(
    context: &WorkerContext,
    session: ServerSessionConfig,
    request: &[u8],
) -> (Vec<RequestSeen>, Vec<(u8, u32)>) {
    REQUESTS.with(|seen| seen.borrow_mut().clear());
    let (_connection, mut client) = connect_served_with(
        context,
        CodecProtocol::Http2,
        session,
        Rc::new(BodyRecordingHandler),
    )
    .await;
    client.write_all(request).await.expect("request written");
    let frames = h2_stream_1_frames(&mut client).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    (REQUESTS.with(|seen| seen.take()), frames)
}

#[test]
fn an_empty_h2c_content_length_ending_the_request_reads_as_zero_over_owned_http2() {
    let request = h2c_content_length_request(b"", None);
    on_worker(move |context| async move {
        let (seen, frames) =
            serve_body_recorded(&context, owned_h2_verbatim_session(true), &request).await;
        assert_eq!(
            seen,
            [RequestSeen::Read {
                content_length: vec![Vec::new()],
                body: Some(Vec::new()),
            }]
        );
        assert!(
            frames.iter().all(|(kind, _)| *kind != 0x3),
            "the 200 ends the stream without a reset: {frames:?}"
        );
        assert_eq!(frames.first(), Some(&(0x1, 0)));
    });
}

#[test]
fn an_empty_h2c_content_length_followed_by_data_fails_the_request_as_a_zero_one_does_over_owned_http2()
 {
    let empty = h2c_content_length_request(b"", Some(b"abc"));
    let zero = h2c_content_length_request(b"0", Some(b"abc"));
    on_worker(move |context| async move {
        let (zero_seen, zero_frames) =
            serve_body_recorded(&context, owned_h2_verbatim_session(true), &zero).await;
        let (empty_seen, empty_frames) =
            serve_body_recorded(&context, owned_h2_verbatim_session(true), &empty).await;
        // As over the managed session
        // (`an_empty_h2c_content_length_followed_by_data_fails_the_request_as_a_zero_one_does`),
        // the overrun fails the request before the application reads its head, and the stream
        // is reset with INTERNAL_ERROR.
        assert_eq!(zero_frames, [(0x3, 0x2)]);
        assert!(
            zero_seen.is_empty(),
            "the owned driver fails the stream before the head reaches the handler: \
             {zero_seen:?}"
        );
        assert_eq!(empty_frames, zero_frames);
        assert_eq!(empty_seen, zero_seen);
    });
}

/// What the application saw of an h2c POST with `content-length: <content_length>` whose
/// HEADERS frame leaves the stream open and whose one DATA frame, `abc` without END_STREAM,
/// is sent once the application has read the head; and each frame (type, flags, payload)
/// the server sent on stream 1.
async fn serve_split_overrun(
    context: &WorkerContext,
    session: ServerSessionConfig,
    content_length: &[u8],
) -> (RequestSeen, Vec<(u8, u8, Vec<u8>)>) {
    REQUESTS.with(|seen| seen.borrow_mut().clear());
    HEADS_READ.with(|heads| heads.set(0));
    let (_connection, mut client) = connect_served_with(
        context,
        CodecProtocol::Http2,
        session,
        Rc::new(BodyRecordingHandler),
    )
    .await;
    let mut head = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    head.extend(h2_frame(0x4, 0, 0, &[]));
    head.extend(h2_frame(
        0x1,
        0x4,
        1,
        &h2_request_block(b"POST", &[(b"content-length", content_length)]),
    ));
    client.write_all(&head).await.expect("head written");
    tokio::time::timeout(Duration::from_secs(5), async {
        while HEADS_READ.with(Cell::get) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the application read the head within the bound");
    client
        .write_all(&h2_frame(0x0, 0, 1, b"abc"))
        .await
        .expect("data written");
    let frames = tokio::time::timeout(Duration::from_secs(5), h2_stream_1_frame_bytes(&mut client))
        .await
        .expect("stream 1 ended within the bound");
    let seen = one_recorded(|| REQUESTS.with(|seen| seen.take())).await;
    (seen, frames)
}

/// A body read that fails while the request is still open, after the application has read
/// the head, here on DATA overrunning the declared length: the handler's failure becomes a
/// 500 on both drivers, then the managed session sends that 500's body with END_STREAM and
/// `owned_http2` resets the stream with INTERNAL_ERROR (`DIV-028`). This holds for any
/// declared length, and an empty one with `verbatim_http2_header_values` fails on each
/// driver as `0` does (`DIV-041`).
#[test]
fn an_h2c_overrun_after_the_head_gets_a_500_then_its_body_or_over_owned_http2_a_reset() {
    on_worker(|context| async move {
        for (verbatim, content_length) in [
            (false, b"0".as_slice()),
            (false, b"2"),
            (true, b"0"),
            (true, b""),
        ] {
            for owned_http2 in [false, true] {
                let case = format!(
                    "verbatim {verbatim}, content-length {:?}, owned_http2 {owned_http2}",
                    String::from_utf8_lossy(content_length)
                );
                let session = ServerSessionConfig {
                    owned_http2,
                    ..verbatim_session(verbatim)
                };
                let (seen, frames) = serve_split_overrun(&context, session, content_length).await;
                assert_eq!(
                    seen,
                    RequestSeen::Read {
                        content_length: vec![content_length.to_vec()],
                        body: None,
                    },
                    "{case}: the body read fails before any overrunning byte"
                );
                let [(head_kind, head_flags, head), (kind, flags, payload)] = frames.as_slice()
                else {
                    panic!("{case}: {frames:?}");
                };
                // A HEADERS frame that ends its block but not the stream, led by `:status 500`
                // (HPACK static entry 14).
                assert_eq!(
                    (*head_kind, *head_flags, head.first()),
                    (0x1, 0x4, Some(&0x8e)),
                    "{case}"
                );
                let expected: (u8, u8, &[u8]) = if owned_http2 {
                    (0x3, 0, &[0, 0, 0, 0x2])
                } else {
                    (0x0, 0x1, b"request body failed")
                };
                assert_eq!((*kind, *flags, payload.as_slice()), expected, "{case}");
            }
        }
    });
}

/// Without `verbatim_http2_header_values` an empty `content-length` leaves the body unchecked
/// on both drivers (`DIV-041`): the application reads the DATA and answers 200.
#[test]
fn an_empty_h2c_content_length_leaves_the_body_unchecked_by_default_on_both_drivers() {
    let request = h2c_content_length_request(b"", Some(b"abc"));
    on_worker(move |context| async move {
        for owned_http2 in [false, true] {
            let session = ServerSessionConfig {
                owned_http2,
                ..verbatim_session(false)
            };
            assert_eq!(
                serve_body_recorded(&context, session, &request).await,
                (
                    vec![RequestSeen::Read {
                        content_length: vec![Vec::new()],
                        body: Some(b"abc".to_vec()),
                    }],
                    vec![(0x1, 0)],
                ),
                "owned_http2 {owned_http2}"
            );
        }
    });
}
