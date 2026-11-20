/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `ServerSessionConfig::non_abortive_close` over loopback TCP.

use std::cell::Cell;
use std::future::Future;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::DeadlineExpiry;
use roxygen_session::OwnedConnectionDeadlines;
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::rate_limit::RateLimitConfig;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_transport::tcp::TcpTransport;
use roxygen_transport::tls::TlsServerOptions;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use socket2::Domain;
use socket2::SockRef;
use socket2::Socket;
use socket2::Type;
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
use crate::lifecycle_tests::CHILD_SPAWN;
use crate::owned_application::OwnedApplicationFactory;
use crate::owned_application::OwnedAttached;
use crate::owned_application::OwnedConnectionAttach;
use crate::owned_application::OwnedConnectionControl;
use crate::owned_application::OwnedConnectionFactory;
use crate::server::HttpServer;
use crate::server::ServerConfig;
use crate::server::StaticTlsCredentials;

const TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST: &[u8] = b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n";
/// The response body sent before the failure: more than the client's receive
/// buffer holds, less than the server's send buffer takes in one write.
const BODY: usize = 48 << 10;
const CLIENT_RECEIVE_BUFFER: u32 = 4096;
const SERVER_SEND_BUFFER: u32 = 256 << 10;

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

/// Runs `test` on a `RuntimeOwner` worker, which must then quiesce without
/// forcing, and on a `HostedWorker`.
fn on_worker<Test, Work>(test: Test)
where
    Test: FnOnce(WorkerContext) -> Work + Clone + Send + 'static,
    Work: Future<Output = ()> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(worker.call_local(test.clone()).expect("test admitted")).expect("test finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);

    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(4).expect("nonzero capacity"),
        thread_name: "non-abortive".to_owned(),
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

fn session(non_abortive_close: bool, owned_http1: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1,
        non_abortive_close,
        ..ServerSessionConfig::default()
    }
}

/// Serves one accepted HTTP/1.1 loopback connection with a large server send
/// buffer and a small client receive buffer, and returns its client.
async fn serve_loopback(
    context: &WorkerContext,
    session: ServerSessionConfig,
    handler: HttpHandlerRef,
) -> (ServerConnection, TcpStream) {
    serve_loopback_with(context, CodecProtocol::Http11, session, handler).await
}

/// `serve_loopback` for a connection of `protocol`.
async fn serve_loopback_with(
    context: &WorkerContext,
    protocol: CodecProtocol,
    session: ServerSessionConfig,
    handler: HttpHandlerRef,
) -> (ServerConnection, TcpStream) {
    let listener = TcpSocket::new_v4().expect("listener socket");
    // Accepted sockets inherit the listener's send buffer.
    listener
        .set_send_buffer_size(SERVER_SEND_BUFFER)
        .expect("server send buffer");
    listener
        .bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("listener binds");
    let listener = listener.listen(8).expect("listener listens");
    let client = TcpSocket::new_v4().expect("client socket");
    client
        .set_recv_buffer_size(CLIENT_RECEIVE_BUFFER)
        .expect("client receive buffer");
    let client = client
        .connect(listener.local_addr().expect("listener address"))
        .await
        .expect("client connects");
    let (accepted, _) = listener.accept().await.expect("connection accepted");
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

/// Reads until the connection ends; returns the bytes and whether it ended
/// with a reset rather than EOF.
async fn read_until_closed(client: &mut TcpStream) -> (Vec<u8>, bool) {
    let mut wire = Vec::new();
    loop {
        let mut chunk = [0; 4096];
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

/// The body of a chunked HTTP/1.1 response and whether it ended with the last chunk.
fn chunked_body(wire: &[u8]) -> (Vec<u8>, bool) {
    let head_end = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a whole response head");
    let mut rest = &wire[head_end + 4..];
    let mut body = Vec::new();
    loop {
        let Some(line_end) = rest.windows(2).position(|window| window == b"\r\n") else {
            body.extend_from_slice(rest);
            return (body, false);
        };
        let size = usize::from_str_radix(
            std::str::from_utf8(&rest[..line_end]).expect("chunk size"),
            16,
        )
        .expect("hexadecimal chunk size");
        rest = &rest[line_end + 2..];
        if size == 0 {
            return (body, rest == b"\r\n");
        }
        let taken = size.min(rest.len());
        body.extend_from_slice(&rest[..taken]);
        rest = &rest[taken..];
        if taken < size {
            return (body, false);
        }
        rest = rest.strip_prefix(b"\r\n").unwrap_or(rest);
    }
}

#[derive(Default)]
struct Failure {
    /// The session started reading the event after the body.
    awaited: Cell<bool>,
    fail: Notify,
}

/// A streamed 200 whose body is `BODY` bytes in one event, followed by an
/// error once `fail` is notified.
struct FailingBody {
    header: Option<HttpHeaderEvent>,
    body_sent: bool,
    failure: Rc<Failure>,
}

impl HttpSource for FailingBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if !std::mem::replace(&mut self.body_sent, true) {
            let body = BufQueue::from_bytes(Bytes::from(vec![b'x'; BODY]), BODY).expect("body");
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(body, false))));
        }
        let failure = Rc::clone(&self.failure);
        Box::pin(async move {
            failure.awaited.set(true);
            failure.fail.notified().await;
            Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"response body failed",
            ))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct FailingBodyHandler(Rc<Failure>);

impl HttpHandler for FailingBodyHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = FailingBody {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            body_sent: false,
            failure: Rc::clone(&self.0),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Waits until body bytes reach `client`, which shows that the session wrote
/// the body; the server's send buffer takes it in one write.
async fn until_body_arrives(client: &TcpStream) {
    let mut peeked = [0; 1024];
    loop {
        let length = client.peek(&mut peeked).await.expect("client peeks");
        let wire = &peeked[..length];
        if let Some(head) = wire.windows(4).position(|window| window == b"\r\n\r\n")
            && let rest = &wire[head + 4..]
            && let Some(line) = rest.windows(2).position(|window| window == b"\r\n")
            && rest.len() > line + 2
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Serves a response whose body fails after `BODY` bytes while the client is
/// not reading, waits until the session has closed the connection, then reads
/// it. Returns the response body the client received, whether it ended with the
/// last chunk, and whether the connection ended with a reset.
async fn body_failure(context: WorkerContext, session: ServerSessionConfig) -> (usize, bool, bool) {
    let failure = Rc::new(Failure::default());
    let (connection, mut client) = serve_loopback(
        &context,
        session,
        Rc::new(FailingBodyHandler(Rc::clone(&failure))),
    )
    .await;
    client.write_all(REQUEST).await.expect("request written");
    until(|| failure.awaited.get()).await;
    until_body_arrives(&client).await;
    failure.fail.notify_one();
    let _ = connection.wait_closed().await;
    let (wire, reset) = read_until_closed(&mut client).await;
    let (body, terminated) = chunked_body(&wire);
    assert!(
        body.iter().all(|&byte| byte == b'x'),
        "the body is the source's bytes"
    );
    (body.len(), terminated, reset)
}

#[test]
fn non_abortive_close_is_off_by_default() {
    assert!(!ServerSessionConfig::default().non_abortive_close);
}

/// `serve_transport` rejects the option for a transport that the caller
/// declares TLS, as `HttpServer` does with TLS credentials. It is rejected
/// because the TLS adapter's own failure paths still reset.
#[test]
fn non_abortive_close_refuses_an_attached_tls_transport() {
    on_worker(|context| async move {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let (client, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().expect("listener address")),
            listener.accept()
        );
        let _client = client.expect("client connects");
        let (accepted, _) = accepted.expect("connection accepted");
        let session = session(true, false);
        let transport = TcpTransport::from_accepted(
            &context,
            accepted.into_std().expect("standard stream"),
            session.byte_stream_limits,
        )
        .expect("transport adopted");
        let mut options = ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Tls);
        options.session = session;
        let refused = serve_transport(
            &context,
            transport,
            Rc::new(HeldHandler(Rc::default())),
            options,
        )
        .err()
        .expect("the attachment is refused");
        assert_eq!(refused.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
    });
}

/// Checks the default: the session resets the connection after a body error,
/// and the abortive close discards what the client had not received yet.
#[test]
fn http1_body_error_resets_by_default() {
    let _spawn = CHILD_SPAWN.lock().unwrap_or_else(PoisonError::into_inner);
    for owned in [false, true] {
        on_worker(move |context| async move {
            let (received, terminated, reset) = body_failure(context, session(false, owned)).await;
            assert!(reset, "owned {owned}: the connection ends with a reset");
            assert!(
                !terminated,
                "owned {owned}: the aborted body has no last chunk"
            );
            assert!(
                received < BODY,
                "owned {owned}: unsent bytes are discarded, {received} arrived"
            );
        });
    }
}

/// As hyper, which drops a connection whose response body failed: the client
/// receives every byte the session wrote, then EOF, and no last chunk.
#[test]
fn http1_body_error_closes_after_the_written_bytes_when_enabled() {
    for owned in [false, true] {
        on_worker(move |context| async move {
            assert_eq!(
                body_failure(context, session(true, owned)).await,
                (BODY, false, false),
                "owned {owned}: the whole body, no last chunk, EOF"
            );
        });
    }
}

#[derive(Default)]
struct Held {
    invoked: Cell<bool>,
}

/// Holds every exchange forever.
struct HeldHandler(Rc<Held>);

impl HttpHandler for HeldHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.0.invoked.set(true);
        Box::pin(async move {
            let _request = request;
            std::future::pending().await
        })
    }
}

/// Cancels a connection whose exchange is held; returns what the client read
/// and whether the connection ended with a reset.
async fn cancelled(context: WorkerContext, session: ServerSessionConfig) -> (Vec<u8>, bool) {
    let held = Rc::new(Held::default());
    let (connection, mut client) =
        serve_loopback(&context, session, Rc::new(HeldHandler(Rc::clone(&held)))).await;
    client.write_all(REQUEST).await.expect("request written");
    until(|| held.invoked.get()).await;
    connection.cancel();
    let _ = connection.wait_closed().await;
    read_until_closed(&mut client).await
}

/// Checks the default: cancelling a connection resets it.
#[test]
fn http1_cancelled_connection_resets_by_default() {
    on_worker(|context| async move {
        assert_eq!(
            cancelled(context, session(false, false)).await,
            (Vec::new(), true)
        );
    });
}

/// As when a hyper server drops its connection future, the client sees EOF.
#[test]
fn http1_cancelled_connection_closes_with_eof_when_enabled() {
    on_worker(|context| async move {
        assert_eq!(
            cancelled(context, session(true, false)).await,
            (Vec::new(), false)
        );
    });
}

/// The HTTP/2 client preface, an empty SETTINGS frame, and a HEADERS frame
/// (END_STREAM, END_HEADERS) on stream 1 for `GET http://localhost/`.
const H2_REQUEST: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\
    \x00\x00\x00\x04\x00\x00\x00\x00\x00\
    \x00\x00\x0e\x01\x05\x00\x00\x00\x01\x82\x86\x84\x41\x09localhost";

/// Cancels an `owned_http2` connection whose exchange is held, which resets
/// its transport; returns whether the connection ended with a reset.
async fn owned_http2_cancelled(context: WorkerContext, non_abortive_close: bool) -> bool {
    let held = Rc::new(Held::default());
    let session = ServerSessionConfig {
        owned_http2: true,
        ..session(non_abortive_close, false)
    };
    let (connection, mut client) = serve_loopback_with(
        &context,
        CodecProtocol::Http2,
        session,
        Rc::new(HeldHandler(Rc::clone(&held))),
    )
    .await;
    client.write_all(H2_REQUEST).await.expect("request written");
    until(|| held.invoked.get()).await;
    connection.cancel();
    let _ = connection.wait_closed().await;
    read_until_closed(&mut client).await.1
}

/// `owned_http2` applies the option as `owned_http1` does. The acceptor sets
/// the transport flag before it picks the driver, so the reset that ends a
/// cancelled owned HTTP/2 connection (RST by default) ends it with EOF.
#[test]
fn owned_http2_cancelled_connection_closes_with_eof_when_enabled() {
    on_worker(|context| async move {
        assert!(
            owned_http2_cancelled(context.clone(), false).await,
            "a cancelled owned HTTP/2 connection resets by default"
        );
        assert!(
            !owned_http2_cancelled(context, true).await,
            "a cancelled owned HTTP/2 connection ends with EOF when enabled"
        );
    });
}

/// A HEADERS frame (END_STREAM, END_HEADERS) for `GET http://localhost/` on
/// `stream`, then RST_STREAM(`CANCEL`) on it.
fn h2_request_then_reset(stream: u32) -> Vec<u8> {
    let id = stream.to_be_bytes();
    let mut frames = vec![0, 0, 0x0e, 0x01, 0x05];
    frames.extend_from_slice(&id);
    frames.extend_from_slice(b"\x82\x86\x84\x41\x09localhost");
    frames.extend_from_slice(&[0, 0, 4, 0x03, 0]);
    frames.extend_from_slice(&id);
    frames.extend_from_slice(&8_u32.to_be_bytes());
    frames
}

/// The error codes of the GOAWAY frames in `wire`, which starts with the
/// server's first frame.
fn goaway_codes(wire: &[u8]) -> Vec<u32> {
    let mut codes = Vec::new();
    let mut rest = wire;
    while rest.len() >= 9 {
        let length = usize::from(rest[0]) << 16 | usize::from(rest[1]) << 8 | usize::from(rest[2]);
        let Some(payload) = rest.get(9..9 + length) else {
            break;
        };
        if rest[3] == 0x07 && payload.len() >= 8 {
            codes.push(u32::from_be_bytes(
                payload[4..8].try_into().expect("4 bytes"),
            ));
        }
        rest = &rest[9 + length..];
    }
    codes
}

/// An `owned_http2` connection whose reset limit is 1 receives three requests,
/// each reset by the client; returns the GOAWAY codes it wrote and whether it
/// ended with a reset.
async fn owned_http2_reset_flood(
    context: WorkerContext,
    non_abortive_close: bool,
) -> (Vec<u32>, bool) {
    let held = Rc::new(Held::default());
    let one = RateLimitConfig::new(1, Duration::from_secs(60), 1);
    let session = ServerSessionConfig {
        owned_http2: true,
        http2_rate_limits: Http2RateLimitConfig {
            resets: one,
            ..Http2RateLimitConfig::default()
        },
        ..session(non_abortive_close, false)
    };
    let (_connection, mut client) = serve_loopback_with(
        &context,
        CodecProtocol::Http2,
        session,
        Rc::new(HeldHandler(Rc::clone(&held))),
    )
    .await;
    let mut flood = H2_REQUEST[..33].to_vec();
    for stream in [1, 3, 5] {
        flood.extend_from_slice(&h2_request_then_reset(stream));
    }
    client.write_all(&flood).await.expect("flood written");
    let (wire, reset) = read_until_closed(&mut client).await;
    (goaway_codes(&wire), reset)
}

/// A reset-flood breach on `owned_http2` fails the connection with
/// GOAWAY(`CANCEL`) and then a FIN, with or without `non_abortive_close`.
#[test]
fn owned_http2_reset_flood_closes_with_eof() {
    on_worker(|context| async move {
        assert_eq!(
            owned_http2_reset_flood(context.clone(), false).await,
            (vec![0x8], false),
            "by default the breach sends GOAWAY(CANCEL), then a FIN"
        );
        assert_eq!(
            owned_http2_reset_flood(context, true).await,
            (vec![0x8], false),
            "with non_abortive_close the breach sends GOAWAY(CANCEL), then a FIN"
        );
    });
}

/// A streamed 200 whose body is `READY_BODY` bytes in one event, then an
/// error, all ready together, so that HTTP/1.1 egress batching serializes the
/// body and takes the error before any output is flushed.
struct ReadyFailingBody {
    header: Option<HttpHeaderEvent>,
    body_sent: bool,
}

const READY_BODY: usize = 32 << 10;
const SMALL_BUFFER: u32 = 4096;

impl HttpSource for ReadyFailingBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if !std::mem::replace(&mut self.body_sent, true) {
            let body = BufQueue::from_bytes(Bytes::from(vec![b'x'; READY_BODY]), READY_BODY)
                .expect("body");
            return Box::pin(std::future::ready(Ok(HttpBodyEvent::body(body, false))));
        }
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"response body failed",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct ReadyFailingBodyHandler;

impl HttpHandler for ReadyFailingBodyHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        let source = ReadyFailingBody {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            body_sent: false,
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Serves `ReadyFailingBodyHandler` on a connection whose server send buffer
/// and client receive buffer are small, to a client that neither reads nor
/// closes. Returns whether the session closed the connection within `within`,
/// then what the client reads afterwards and whether it ended with a reset.
async fn ready_body_failure(
    context: WorkerContext,
    session: ServerSessionConfig,
    within: Duration,
) -> (bool, Vec<u8>, bool) {
    let listener = TcpSocket::new_v4().expect("listener socket");
    listener
        .set_send_buffer_size(SMALL_BUFFER)
        .expect("server send buffer");
    listener
        .bind("127.0.0.1:0".parse().expect("loopback address"))
        .expect("listener binds");
    let listener = listener.listen(8).expect("listener listens");
    let client = TcpSocket::new_v4().expect("client socket");
    client
        .set_recv_buffer_size(SMALL_BUFFER)
        .expect("client receive buffer");
    let mut client = client
        .connect(listener.local_addr().expect("listener address"))
        .await
        .expect("client connects");
    let (accepted, _) = listener.accept().await.expect("connection accepted");
    let transport = TcpTransport::from_accepted(
        &context,
        accepted.into_std().expect("standard stream"),
        session.byte_stream_limits,
    )
    .expect("transport adopted");
    let mut options =
        ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.session = session;
    let connection = serve_transport(
        &context,
        transport,
        Rc::new(ReadyFailingBodyHandler),
        options,
    )
    .expect("attached");
    client.write_all(REQUEST).await.expect("request written");
    let closed = tokio::time::timeout(within, connection.wait_closed())
        .await
        .is_ok();
    let (wire, reset) = read_until_closed(&mut client).await;
    if !closed {
        let _ = connection.wait_closed().await;
    }
    (closed, wire, reset)
}

/// Matches hyper, which drops its unflushed output and closes the connection
/// when a response body fails. With batching, a body and its error that are
/// ready together discard the serialized response, which the peer is not
/// reading, and the connection closes immediately with FIN (no last chunk, no
/// wait on a peer that stopped reading, despite the zero write timeout). The
/// owned HTTP/1.1 driver, which writes a turn's output together, does the same.
#[test]
fn http1_ready_body_error_abandons_unwritten_output_when_non_abortive() {
    for owned in [false, true] {
        on_worker(move |context| async move {
            let session = session(true, owned);
            let (closed, wire, reset) =
                ready_body_failure(context, session, Duration::from_secs(2)).await;
            assert!(
                closed,
                "owned {owned}: the session closes without the peer reading"
            );
            assert!(!reset, "owned {owned}: the connection ends with FIN");
            assert_eq!(
                wire, b"",
                "owned {owned}: the head and body serialized with the error are never written"
            );
        });
    }
}

/// Checks the default: the session writes what it serialized before resetting,
/// so with a zero write timeout it holds the connection while the peer does
/// not read; once the peer reads, the body ends without a last chunk, then
/// the abortive reset.
#[test]
fn http1_ready_body_error_waits_for_the_peer_by_default() {
    on_worker(|context| async move {
        let (closed, wire, reset) =
            ready_body_failure(context, session(false, false), Duration::from_millis(300)).await;
        assert!(!closed, "the session waits for the peer to read");
        assert!(reset, "the connection ends with a reset");
        assert!(
            !wire.ends_with(b"0\r\n\r\n"),
            "the failed body has no last chunk"
        );
    });
}

/// Waits until `done` holds, within `TIMEOUT`.
fn wait_until(what: &str, done: impl Fn() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(
            started.elapsed() < TIMEOUT,
            "timed out waiting until {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A one-shot release that a worker's future can wait for.
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().expect("gate waker").take() {
            waker.wake();
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.open.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *self.waker.lock().expect("gate waker") = Some(context.waker().clone());
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// The body of `/big-then-hard-close`: more than the server's send buffer, its ciphertext
/// staging and the client's receive buffer hold together.
const BIG_BODY: usize = 1 << 20;

/// What the TLS tests' application shares with the test thread.
#[derive(Default)]
struct TlsProbe {
    /// Requests delivered to exchanges.
    requests: AtomicUsize,
    /// Hard-closes the connections of `/big-then-hard-close` requests once opened.
    hard_close: Gate,
}

impl TlsProbe {
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

/// Serves each connection on the owned driver its ALPN protocol names.
struct TlsConnections(Arc<TlsProbe>);

impl OwnedConnectionFactory for TlsConnections {
    fn attach(&self, connection: OwnedConnectionAttach<'_>) -> Result<OwnedAttached, HttpError> {
        let probe = Arc::clone(&self.0);
        let make = move |control| TlsApp { probe, control };
        match connection.protocol() {
            CodecProtocol::Http2 => connection.serve_http2(make),
            _ => connection.serve_http1(Http1DriverConfig::default(), make),
        }
    }
}

struct TlsApp {
    probe: Arc<TlsProbe>,
    control: OwnedConnectionControl,
}

impl OwnedApplication for TlsApp {
    type Exchange = TlsExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> TlsExchange {
        TlsExchange {
            probe: Arc::clone(&self.probe),
            control: self.control.clone(),
            head: None,
            body: None,
            body_ends: true,
            _request: None,
            _tasks: None,
        }
    }
}

/// An exchange that answers by its request's path:
/// - `/hard-close` hard-closes its connection and never answers;
/// - `/ok` is answered `200` with the body `ok`;
/// - `/big-then-hard-close` is answered `200` with `BIG_BODY` bytes of a longer body, and its
///   connection is hard-closed once the probe's `hard_close` opens;
/// - `/skip-close-notify` calls `skip_tls_close_notify` and is answered `200` with
///   `BIG_BODY` bytes;
/// - `/drain` drains its connection and is answered `200` with the body `ok`;
/// - anything else is never answered.
struct TlsExchange {
    probe: Arc<TlsProbe>,
    control: OwnedConnectionControl,
    head: Option<HttpHeaderEvent>,
    body: Option<BufQueue>,
    /// Whether `body` ends the response; if not, the rest never comes.
    body_ends: bool,
    _request: Option<HttpStreamSourceOwner>,
    _tasks: Option<ExchangeTasks>,
}

/// An exchange's tasks; its `Drop` cancels and reaps them.
struct ExchangeTasks(TaskGroup);

impl Drop for ExchangeTasks {
    fn drop(&mut self) {
        self.0.abort_all();
        while let Some(completion) = self.0.try_join_next() {
            completion.discard();
        }
    }
}

impl TlsExchange {
    /// Answers `200` with `body`.
    fn respond(&mut self, body: Vec<u8>) {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message.set_http_version(1, 1);
        message.headers_mut().set(
            HttpHeaderCode::ContentLength,
            (body.len() + usize::from(!self.body_ends))
                .to_string()
                .as_bytes(),
        );
        self.head = Some(HttpHeaderEvent::new(message, false).expect("response head"));
        let length = body.len();
        self.body = Some(BufQueue::from_bytes(Bytes::from(body), length).expect("body"));
    }
}

impl OwnedExchange for TlsExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self._request = Some(body);
        match head.request_url().unwrap_or_default() {
            b"/hard-close" => self.control.hard_close(),
            b"/ok" => self.respond(b"ok".to_vec()),
            b"/drain" => {
                self.control.drain().expect("the connection drains");
                self.respond(b"ok".to_vec());
            }
            b"/skip-close-notify" => {
                self.control.skip_tls_close_notify();
                self.respond(vec![b'x'; BIG_BODY]);
            }
            b"/big-then-hard-close" => {
                self.body_ends = false;
                self.respond(vec![b'x'; BIG_BODY]);
                let probe = Arc::clone(&self.probe);
                let control = self.control.clone();
                let mut tasks = TaskGroup::new();
                tasks
                    .spawn_local(move |_| async move {
                        std::future::poll_fn(|context| probe.hard_close.poll(context)).await;
                        control.hard_close();
                    })
                    .expect("the hard-close task spawns");
                self._tasks = Some(ExchangeTasks(tasks));
            }
            _ => {}
        }
        self.probe.requests.fetch_add(1, Ordering::SeqCst);
    }

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        match self.head.take() {
            Some(head) => Poll::Ready(Ok(head)),
            None => Poll::Pending,
        }
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        match self.body.take() {
            Some(body) => Poll::Ready(Ok(HttpBodyEvent::body(body, self.body_ends))),
            None => Poll::Pending,
        }
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

/// A `localhost` certificate and key, with `h2` and `http/1.1` offered over ALPN.
fn tls_options() -> (TlsServerOptions, Vec<u8>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("certificate generated");
    let certificate = generated.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![certificate.clone().into_boxed_slice()],
        generated
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        vec![
            b"h2".to_vec().into_boxed_slice(),
            b"http/1.1".to_vec().into_boxed_slice(),
        ],
    )
    .expect("TLS options");
    (options, certificate)
}

/// The untimed owned session of the TLS tests.
fn tls_session(non_abortive_close: bool) -> ServerSessionConfig {
    ServerSessionConfig {
        owned_http2: true,
        ..session(non_abortive_close, true)
    }
}

/// The server configuration of the TLS tests, and its certificate. Each accepted socket gets a
/// small send buffer, so that a large response blocks.
fn tls_server_config(mut session: ServerSessionConfig) -> (ServerConfig, Vec<u8>) {
    session.accepted_socket_profile = Some(Arc::new(|socket: &std::net::TcpStream| {
        SockRef::from(socket)
            .set_send_buffer_size(32 << 10)
            .map_err(|error| {
                HttpError::with_message(HttpErrorCode::CONNECT_ERROR, error.to_string().as_bytes())
            })
    }));
    let (options, certificate) = tls_options();
    let config = ServerConfig {
        session,
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(options))),
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    (config, certificate)
}

/// An `HttpServer` with TLS credentials; dropping it force-stops it.
struct TlsServer {
    server: HttpServer,
    address: SocketAddr,
    certificate: Vec<u8>,
    probe: Arc<TlsProbe>,
}

impl TlsServer {
    /// Starts the server with `session`, hosting `TlsConnections`.
    fn start(session: ServerSessionConfig) -> Self {
        let (config, certificate) = tls_server_config(session);
        let probe = Arc::new(TlsProbe::default());
        let factory_probe = Arc::clone(&probe);
        let factory: OwnedApplicationFactory = Arc::new(move |_, _| {
            Ok(Rc::new(TlsConnections(Arc::clone(&factory_probe)))
                as Rc<dyn OwnedConnectionFactory>)
        });
        let server = HttpServer::start_owned(config, factory, None).expect("the TLS server starts");
        Self::new(server, certificate, probe)
    }

    /// Starts the server with `session`, serving `HeldHandler` on the built-in owned drivers.
    fn start_with_handler(session: ServerSessionConfig) -> Self {
        let (config, certificate) = tls_server_config(session);
        let server = HttpServer::start(
            config,
            Arc::new(|_| Rc::new(HeldHandler(Rc::default())) as HttpHandlerRef),
            None,
        )
        .expect("the TLS server starts");
        Self::new(server, certificate, Arc::default())
    }

    fn new(server: HttpServer, certificate: Vec<u8>, probe: Arc<TlsProbe>) -> Self {
        let address = server.address().expect("server address");
        Self {
            server,
            address,
            certificate,
            probe,
        }
    }

    /// Connects a TLS client that offers only `protocol` over ALPN.
    fn connect(&self, protocol: &[u8]) -> TlsClient {
        self.connect_with(protocol, None)
    }

    /// `connect`, with the client socket's receive buffer set to `receive_buffer`.
    fn connect_with(&self, protocol: &[u8], receive_buffer: Option<usize>) -> TlsClient {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(self.certificate.clone()))
            .expect("root certificate");
        let mut config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![protocol.to_vec()];
        let connection = ClientConnection::new(
            Arc::new(config),
            ServerName::try_from("localhost".to_owned()).expect("server name"),
        )
        .expect("TLS client");
        let socket = Socket::new(Domain::for_address(self.address), Type::STREAM, None)
            .expect("client socket");
        if let Some(size) = receive_buffer {
            socket
                .set_recv_buffer_size(size)
                .expect("client receive buffer");
        }
        socket
            .connect_timeout(&self.address.into(), TIMEOUT)
            .expect("client connects");
        let socket = std::net::TcpStream::from(socket);
        socket
            .set_read_timeout(Some(TIMEOUT))
            .expect("read timeout");
        socket
            .set_write_timeout(Some(TIMEOUT))
            .expect("write timeout");
        StreamOwned::new(connection, socket)
    }
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        let _ = self.server.force_stop();
    }
}

type TlsClient = StreamOwned<ClientConnection, std::net::TcpStream>;

/// How a TLS connection ended, as its client reads it.
#[derive(Debug, PartialEq)]
enum TlsEnd {
    /// close_notify, then FIN.
    CloseNotify,
    /// close_notify, and then the TCP stream does not end with FIN: more bytes, a reset, or
    /// nothing within `TIMEOUT`.
    CloseNotifyWithoutFin,
    /// FIN with no close_notify.
    Fin,
    /// A TCP reset.
    Reset,
}

/// Reads the plaintext until the connection ends, and how it ended. rustls stops reading the
/// socket at close_notify, so the socket is then read directly for the FIN.
fn read_until_tls_end(client: &mut TlsClient) -> (Vec<u8>, TlsEnd) {
    let mut plaintext = Vec::new();
    let mut chunk = [0; 16 << 10];
    loop {
        match client.read(&mut chunk) {
            Ok(0) => {
                let end = match client.sock.read(&mut [0; 1]) {
                    Ok(0) => TlsEnd::CloseNotify,
                    _ => TlsEnd::CloseNotifyWithoutFin,
                };
                return (plaintext, end);
            }
            Ok(read) => plaintext.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return (plaintext, TlsEnd::Fin);
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {
                return (plaintext, TlsEnd::Reset);
            }
            Err(error) => panic!("the TLS client read failed: {error}"),
        }
    }
}

/// Hard-closes an HTTP/1.1 TLS connection whose exchange is held; returns how it ended.
fn tls_hard_close_held(non_abortive_close: bool) -> TlsEnd {
    let server = TlsServer::start(tls_session(non_abortive_close));
    let mut client = server.connect(b"http/1.1");
    client
        .write_all(b"GET /hard-close HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || server.probe.requests() == 1);
    let (plaintext, end) = read_until_tls_end(&mut client);
    assert_eq!(plaintext, b"", "a hard close writes no response");
    end
}

/// Hard-closes an HTTP/1.1 TLS connection while its response write is blocked on a client that
/// does not read; returns how the connection ended once the client reads.
fn tls_hard_close_writing(non_abortive_close: bool) -> TlsEnd {
    let server = TlsServer::start(tls_session(non_abortive_close));
    let mut client = server.connect_with(b"http/1.1", Some(CLIENT_RECEIVE_BUFFER as usize));
    client
        .write_all(b"GET /big-then-hard-close HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || server.probe.requests() == 1);
    client
        .sock
        .peek(&mut [0; 1])
        .expect("response bytes arrive");
    // The response write fills the client's receive buffer, the server's send buffer and the
    // ciphertext staging queue, and then stays blocked.
    std::thread::sleep(Duration::from_millis(200));
    server.probe.hard_close.open();
    let (plaintext, end) = read_until_tls_end(&mut client);
    assert!(
        plaintext.len() < BIG_BODY,
        "the blocked response is cut short: {} bytes",
        plaintext.len()
    );
    end
}

/// With `non_abortive_close`, a hard-closed TLS connection ends as dropping the TCP stream
/// does: FIN, with no close_notify and no reset, also while a response write is blocked.
#[test]
fn tls_cancel_with_non_abortive_close_yields_eof() {
    assert_eq!(tls_hard_close_held(true), TlsEnd::Fin, "a held exchange");
    assert_eq!(
        tls_hard_close_writing(true),
        TlsEnd::Fin,
        "a blocked response write"
    );
}

/// Serves `/ok` on an HTTP/1.1 TLS connection whose request asks to close it; returns the
/// response and how the connection ended.
fn tls_graceful_close(non_abortive_close: bool) -> (Vec<u8>, TlsEnd) {
    let server = TlsServer::start(tls_session(non_abortive_close));
    let mut client = server.connect(b"http/1.1");
    client
        .write_all(b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request written");
    read_until_tls_end(&mut client)
}

/// A graceful close sends close_notify and then FIN, with or without `non_abortive_close`.
#[test]
fn graceful_close_sends_close_notify_by_default() {
    for non_abortive_close in [false, true] {
        let (response, end) = tls_graceful_close(non_abortive_close);
        assert!(
            response.starts_with(b"HTTP/1.1 200 ") && response.ends_with(b"\r\n\r\nok"),
            "non_abortive_close {non_abortive_close}: the whole response arrives: {:?}",
            String::from_utf8_lossy(&response)
        );
        assert_eq!(
            end,
            TlsEnd::CloseNotify,
            "non_abortive_close {non_abortive_close}: close_notify, then FIN"
        );
    }
}

/// Checks the default: a hard-closed TLS connection is reset, also while a response write is
/// blocked.
#[test]
fn tls_cancel_default_yields_reset() {
    assert_eq!(tls_hard_close_held(false), TlsEnd::Reset, "a held exchange");
    assert_eq!(
        tls_hard_close_writing(false),
        TlsEnd::Reset,
        "a blocked response write"
    );
}

/// `skip_tls_close_notify` makes the graceful close write the whole response, staged
/// ciphertext included, and then FIN with no close_notify.
#[test]
fn skip_tls_close_notify_ends_with_fin_after_flush() {
    let server = TlsServer::start(tls_session(false));
    let mut client = server.connect_with(b"http/1.1", Some(CLIENT_RECEIVE_BUFFER as usize));
    client
        .write_all(
            b"GET /skip-close-notify HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .expect("request written");
    // The response fills the buffers and the ciphertext staging queue before the client reads.
    std::thread::sleep(Duration::from_millis(200));
    let (response, end) = read_until_tls_end(&mut client);
    let head_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a whole response head")
        + 4;
    assert!(response.starts_with(b"HTTP/1.1 200 "));
    assert_eq!(
        response.len() - head_end,
        BIG_BODY,
        "the whole body arrives"
    );
    assert!(response[head_end..].iter().all(|&byte| byte == b'x'));
    assert_eq!(end, TlsEnd::Fin, "FIN with no close_notify");
}

/// An HTTP/2 frame.
fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len())
        .expect("frame length")
        .to_be_bytes();
    let mut frame = length[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// The client preface and an empty SETTINGS frame.
fn h2_preface() -> Vec<u8> {
    H2_REQUEST[..33].to_vec()
}

/// A HEADERS frame (END_STREAM, END_HEADERS) for `GET https://localhost<path>` on `stream`.
fn h2_get(stream: u32, path: &[u8]) -> Vec<u8> {
    let mut block = vec![
        0x82,
        0x87,
        0x04,
        u8::try_from(path.len()).expect("short path"),
    ];
    block.extend_from_slice(path);
    block.extend_from_slice(b"\x41\x09localhost");
    h2_frame(0x01, 0x05, stream, &block)
}

/// Starts a TLS server with `session` and an HTTP/2 client that writes `wire`; returns the
/// plaintext the client read and how the connection ended.
fn h2_exchange(session: ServerSessionConfig, wire: &[u8]) -> (Vec<u8>, TlsEnd) {
    let server = TlsServer::start(session);
    let mut client = server.connect(b"h2");
    client.write_all(wire).expect("frames written");
    read_until_tls_end(&mut client)
}

/// The final GOAWAY is sent and the last stream is done: a local drain during a request.
fn h2_graceful_end() -> (Vec<u8>, TlsEnd) {
    let session = ServerSessionConfig {
        goaway_timeout: Duration::from_millis(100),
        ..tls_session(true)
    };
    let mut wire = h2_preface();
    wire.extend(h2_get(1, b"/drain"));
    let (plaintext, end) = h2_exchange(session, &wire);
    let codes = goaway_codes(&plaintext);
    assert!(
        !codes.is_empty() && codes.iter().all(|&code| code == 0),
        "the drain sends GOAWAY(NO_ERROR): {codes:?}"
    );
    (plaintext, end)
}

/// A DATA frame on stream 0, a connection error that the session answers with
/// GOAWAY(PROTOCOL_ERROR).
fn h2_protocol_error() -> (Vec<u8>, TlsEnd) {
    let mut wire = h2_preface();
    wire.extend(h2_frame(0x00, 0, 0, b"x"));
    let (plaintext, end) = h2_exchange(tls_session(true), &wire);
    assert_eq!(
        goaway_codes(&plaintext),
        vec![0x1],
        "GOAWAY(PROTOCOL_ERROR)"
    );
    (plaintext, end)
}

/// Three requests, each reset by the client, on a connection whose reset limit is 1.
fn h2_rate_limit_breach() -> (Vec<u8>, TlsEnd) {
    let session = ServerSessionConfig {
        http2_rate_limits: Http2RateLimitConfig {
            resets: RateLimitConfig::new(1, Duration::from_secs(60), 1),
            ..Http2RateLimitConfig::default()
        },
        ..tls_session(true)
    };
    let mut wire = h2_preface();
    for stream in [1, 3, 5] {
        wire.extend(h2_request_then_reset(stream));
    }
    let (plaintext, end) = h2_exchange(session, &wire);
    assert_eq!(goaway_codes(&plaintext), vec![0x8], "GOAWAY(CANCEL)");
    (plaintext, end)
}

/// The client sends close_notify and FIN once its handshake is done and it has written `wire`.
fn peer_eof(protocol: &[u8], wire: &[u8]) -> (Vec<u8>, TlsEnd) {
    let server = TlsServer::start(tls_session(true));
    let mut client = server.connect(protocol);
    while client.conn.is_handshaking() {
        client
            .conn
            .complete_io(&mut client.sock)
            .expect("TLS handshake");
    }
    client.write_all(wire).expect("written");
    client.conn.send_close_notify();
    client.flush().expect("close_notify written");
    client
        .sock
        .shutdown(std::net::Shutdown::Write)
        .expect("client FIN");
    read_until_tls_end(&mut client)
}

/// `/hard-close` on HTTP/2.
fn h2_hard_close() -> (Vec<u8>, TlsEnd) {
    let mut wire = h2_preface();
    wire.extend(h2_get(1, b"/hard-close"));
    h2_exchange(tls_session(true), &wire)
}

/// The connection deadline's idle expiry, with `DeadlineExpiry::HardClose`, after the HTTP/2
/// handshake.
fn h2_idle_expiry() -> (Vec<u8>, TlsEnd) {
    let session = ServerSessionConfig {
        owned_deadlines: OwnedConnectionDeadlines::new(
            Duration::ZERO,
            Duration::from_millis(200),
            DeadlineExpiry::HardClose,
        ),
        ..tls_session(true)
    };
    h2_exchange(session, &h2_preface())
}

/// `HttpServer::force_stop` while an HTTP/1.1 exchange is held.
fn h1_forced_stop() -> (Vec<u8>, TlsEnd) {
    let server = TlsServer::start(tls_session(true));
    let mut client = server.connect(b"http/1.1");
    client
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || server.probe.requests() == 1);
    server.server.force_stop().expect("the server stops");
    read_until_tls_end(&mut client)
}

/// A connection that negotiated `h2` whose client sends HTTP/1.1 instead of the HTTP/2
/// preface, on the hosted or the built-in owned HTTP/2 path.
fn h2_handshake_failure(hosted: bool) -> (Vec<u8>, TlsEnd) {
    let session = tls_session(true);
    let server = if hosted {
        TlsServer::start(session)
    } else {
        TlsServer::start_with_handler(session)
    };
    let mut client = server.connect(b"h2");
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    read_until_tls_end(&mut client)
}

/// An HTTP/1.1 request head that does not parse, which the session answers with 400 and a
/// reset.
fn h1_parse_error() -> (Vec<u8>, TlsEnd) {
    let server = TlsServer::start(tls_session(true));
    let mut client = server.connect(b"http/1.1");
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nBad Header: x\r\n\r\n")
        .expect("request written");
    let (response, end) = read_until_tls_end(&mut client);
    assert!(
        response.starts_with(b"HTTP/1.1 400 "),
        "the error response arrives: {:?}",
        String::from_utf8_lossy(&response)
    );
    (response, end)
}

/// Under `non_abortive_close`, the TLS close kind follows the close cause: a graceful end, a
/// connection error the session reports with GOAWAY, and a peer EOF send close_notify and then
/// FIN; a cancel, a deadline's hard close, a forced stop, a failed HTTP/2 handshake and the
/// reset after an HTTP/1.1 parse-error response end with FIN and no close_notify.
#[test]
fn tls_close_kind_per_close_cause() {
    let peer_eof_h2 = || peer_eof(b"h2", &h2_preface());
    let peer_eof_h1 = || peer_eof(b"http/1.1", b"");
    let causes: [(&str, &dyn Fn() -> (Vec<u8>, TlsEnd), TlsEnd); 11] = [
        ("H2 graceful end", &h2_graceful_end, TlsEnd::CloseNotify),
        ("H2 protocol error", &h2_protocol_error, TlsEnd::CloseNotify),
        (
            "H2 rate-limit breach",
            &h2_rate_limit_breach,
            TlsEnd::CloseNotify,
        ),
        ("H2 peer EOF", &peer_eof_h2, TlsEnd::CloseNotify),
        ("H1 peer EOF", &peer_eof_h1, TlsEnd::CloseNotify),
        ("H2 hard close", &h2_hard_close, TlsEnd::Fin),
        ("H2 idle expiry", &h2_idle_expiry, TlsEnd::Fin),
        ("H1 forced stop", &h1_forced_stop, TlsEnd::Fin),
        (
            "H2 handshake failure, hosted",
            &|| h2_handshake_failure(true),
            TlsEnd::Fin,
        ),
        (
            "H2 handshake failure, built-in",
            &|| h2_handshake_failure(false),
            TlsEnd::Fin,
        ),
        ("H1 parse-error response", &h1_parse_error, TlsEnd::Fin),
    ];
    let mismatches = causes
        .into_iter()
        .filter_map(|(cause, run, expected)| {
            let (_, end) = run();
            (end != expected).then(|| format!("{cause}: {end:?}, expected {expected:?}"))
        })
        .collect::<Vec<_>>();
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
