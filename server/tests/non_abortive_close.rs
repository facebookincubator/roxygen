//! `ServerSessionConfig::non_abortive_close` (`DIV-038`) over loopback TCP.

use std::cell::Cell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::PoisonError;
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
use roxygen_session::http2::rate_limit::Http2RateLimitConfig;
use roxygen_session::http2::rate_limit::RateLimitConfig;
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
use crate::lifecycle_tests::CHILD_SPAWN;

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

/// Serves one accepted HTTP/1.1 loopback connection whose server send buffer
/// is large and whose client receive buffer is small, and returns its client.
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
    /// The session asked for the event after the body.
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

/// Serves a response whose body fails after `BODY` bytes while the client reads
/// nothing, waits until the session has closed the connection, then reads it:
/// returns the response body the client received, whether it ended with the
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

/// A transport that `serve_transport`'s caller declares TLS refuses the option,
/// as `HttpServer` refuses it with TLS credentials: the TLS adapter's own
/// failure paths still reset.
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

/// Pins the default: the session resets the connection after a body error, and
/// the abortive close discards what the client had not received yet.
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

/// Pins the default: cancelling a connection resets it.
#[test]
fn http1_cancelled_connection_resets_by_default() {
    on_worker(|context| async move {
        assert_eq!(
            cancelled(context, session(false, false)).await,
            (Vec::new(), true)
        );
    });
}

/// As hyper, whose connection future a server drops: the client sees EOF.
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

/// `owned_http2` honors the option as `owned_http1` does: the acceptor sets
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

/// `DIV-052` with `DIV-038`: a reset-flood breach on `owned_http2` fails the
/// connection with GOAWAY(`CANCEL`), and `non_abortive_close` turns the reset
/// that follows into a FIN.
#[test]
fn owned_http2_reset_flood_closes_with_eof_when_enabled() {
    on_worker(|context| async move {
        assert_eq!(
            owned_http2_reset_flood(context.clone(), false).await,
            (vec![0x8], true),
            "by default the breach sends GOAWAY(CANCEL), then resets"
        );
        assert_eq!(
            owned_http2_reset_flood(context, true).await,
            (vec![0x8], false),
            "with non_abortive_close the breach sends GOAWAY(CANCEL), then a FIN"
        );
    });
}

/// A streamed 200 whose body is `READY_BODY` bytes in one event, then an
/// error, all ready at once, so that batching (`DIV-034`) serializes the body
/// and takes the error before anything is flushed.
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

/// As hyper, which drops the output it has not flushed when a response body
/// fails and closes the connection: with batching, a body and its error that
/// are ready together abandon the serialized response, which the peer is not
/// taking, and the connection closes at once with FIN (no last chunk, no wait
/// on a peer that stopped reading, despite the zero write timeout). The owned
/// HTTP/1.1 driver, which writes a turn's output together, does the same.
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

/// Pins the default: the session writes what it serialized before resetting,
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
