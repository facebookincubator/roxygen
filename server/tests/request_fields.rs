//! Opt-in exact method tokens (`DIV-040`) over `serve_transport` and loopback
//! TCP, on a `RuntimeOwner` worker and a `HostedWorker`, for HTTP/1.1 and h2c,
//! next to hyper's server answering the same bytes.

use std::cell::RefCell;
use std::convert::Infallible;
use std::rc::Rc;
use std::time::Duration;

use roxygen_core::connection::TransportSecurity;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
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

/// Records each request's method as the application reads it, and answers 200.
struct RecordingHandler;

impl HttpHandler for RecordingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            let method = head
                .headers()
                .request_method()
                .unwrap_or_default()
                .to_owned();
            RECORDED.with(|recorded| recorded.borrow_mut().push(method));
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, None).expect("response"),
            )))
        })
    }
}

thread_local! {
    static RECORDED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
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

async fn connect_served(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
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
    let connection =
        serve_transport(context, transport, Rc::new(RecordingHandler), options).expect("attached");
    (connection, client)
}

fn http1_request(method: &[u8]) -> Vec<u8> {
    [
        method,
        b" /generate HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
    ]
    .concat()
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
    let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
    wire.extend(h2_frame(0x4, 0, 0, &[]));
    for (index, method) in methods.iter().enumerate() {
        let stream = 1 + 2 * u32::try_from(index).expect("few streams");
        wire.extend(h2_frame(0x1, 0x5, stream, &h2_request_block(method, &[])));
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

/// The methods the application saw, and each HTTP/1.1 request's status (one
/// connection per request), or the h2c responses (one connection).
async fn serve_methods(
    context: WorkerContext,
    protocol: CodecProtocol,
    exact_method_tokens: bool,
) -> (Vec<String>, Vec<u16>) {
    RECORDED.with(|recorded| recorded.borrow_mut().clear());
    let mut statuses = Vec::new();
    if protocol == CodecProtocol::Http11 {
        for method in METHODS {
            let (_connection, mut client) =
                connect_served(&context, protocol, session(exact_method_tokens)).await;
            client
                .write_all(&http1_request(method))
                .await
                .expect("request written");
            statuses.push(http1_status(&mut client).await);
        }
    } else {
        let (_connection, mut client) =
            connect_served(&context, protocol, session(exact_method_tokens)).await;
        client
            .write_all(&h2c_requests(&METHODS))
            .await
            .expect("requests written");
        h2_until_headers(&mut client, METHODS.len()).await;
    }
    (RECORDED.with(|recorded| recorded.take()), statuses)
}

/// What hyper's server (hyper-util's `auto` builder, as `axum::serve` uses it)
/// shows its service for the same bytes.
fn hyper_methods(protocol: CodecProtocol) -> Vec<String> {
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
                                    seen.borrow_mut().push(request.method().as_str().to_owned());
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
                if protocol == CodecProtocol::Http11 {
                    for method in METHODS {
                        let mut client = TcpStream::connect(address).await.expect("connects");
                        client
                            .write_all(&http1_request(method))
                            .await
                            .expect("request written");
                        http1_status(&mut client).await;
                    }
                } else {
                    let mut client = TcpStream::connect(address).await.expect("connects");
                    client
                        .write_all(&h2c_requests(&METHODS))
                        .await
                        .expect("requests written");
                    h2_until_headers(&mut client, METHODS.len()).await;
                }
            })
            .await;
        seen.take()
    })
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
fn exact_method_tokens_reach_the_application_byte_for_byte_over_http1_when_enabled() {
    on_worker(|context| async move {
        let (methods, statuses) = serve_methods(context, CodecProtocol::Http11, true).await;
        assert_eq!(methods, ["POST", "post", "Post", "PURGE"]);
        assert_eq!(statuses, [200, 200, 200, 200]);
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
fn http1_requests_with_a_method_outside_the_frozen_table_get_400_by_default() {
    on_worker(|context| async move {
        let (methods, statuses) = serve_methods(context, CodecProtocol::Http11, false).await;
        assert_eq!(methods, ["POST"]);
        assert_eq!(statuses, [200, 400, 400, 400]);
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
fn owned_http1_refuses_exact_method_tokens() {
    let config = ServerSessionConfig {
        owned_http1: true,
        exact_method_tokens: true,
        ..session(true)
    };
    assert_eq!(
        config.check_owned_http1(),
        Err("owned_http1 does not support exact_method_tokens")
    );
}

/// The frame types (and RST_STREAM error codes) the server sent on stream 1,
/// until the stream or the connection ends.
async fn h2_stream_1_frames(client: &mut TcpStream) -> Vec<(u8, u32)> {
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
            let payload = &rest[9..9 + length];
            if stream == 1 {
                let code = if rest[3] == 0x3 {
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                } else {
                    0
                };
                frames.push((rest[3], code));
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
