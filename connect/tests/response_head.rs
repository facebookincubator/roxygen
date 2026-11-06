//! `DIV-011` oracles for `roxygen_connect::experimental`.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use roxygen_client::connector::ClientSession;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_connect::experimental::ConnectResponseHeadExt;
use roxygen_connect::experimental::connect_rejection;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_connect::stream::RequestHeaderMap;
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
use roxygen_core::message::HttpMessage;
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
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::connect_stream::AUTHORITY;
use crate::connect_stream::CONNECT_TIMEOUT;
use crate::connect_stream::EGRESS_BUFFER_SIZE;
use crate::connect_stream::H2cProxy;
use crate::connect_stream::WAIT_DEADLINE;
use crate::connect_stream::connect_unique;
use crate::connect_stream::eventually;
use crate::connect_stream::headers;
use crate::connect_stream::queue;
use crate::connect_stream::response;
use crate::connect_stream::run_on_worker;
use crate::connect_stream::socket_address;

// ---------------------------------------------------------------------------
// Fixtures.

/// The proxy side of one h2c CONNECT stream, which the test drives.
struct Tunnel {
    request: RefCell<HttpSourceHolder>,
    response: HttpStreamSourceProducer,
}

impl Tunnel {
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

    /// Reads the tunnel's egress until EOM, or returns the request read's error.
    async fn read_to_end(&self) -> Result<(), HttpError> {
        let mut request = std::mem::take(&mut *self.request.borrow_mut());
        loop {
            let event = request.read_body_event(u32::MAX).await?;
            let eom = event.eom();
            if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                resume.await;
            }
            if eom {
                return Ok(());
            }
        }
    }
}

type Tunnels = Rc<RefCell<Vec<Rc<Tunnel>>>>;

/// Accepts every CONNECT and leaves its response to the test.
struct TunnelHandler {
    tunnels: Tunnels,
}

impl HttpHandler for TunnelHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let tunnels = Rc::clone(&self.tunnels);
        Box::pin(async move {
            request.read_header_event().await?;
            let source = HttpStreamSource::new_async(None, None, 1 << 20).map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
            })?;
            tunnels.borrow_mut().push(Rc::new(Tunnel {
                request: RefCell::new(request),
                response: source.producer(),
            }));
            Ok(HttpSourceHolder::new(Box::new(source)))
        })
    }
}

/// An h2c `roxygen_server` proxy whose CONNECT responses the test scripts.
struct ScriptedH2cProxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
    tunnels: Tunnels,
    taken: Cell<usize>,
    connections: RefCell<Vec<ServerConnection>>,
}

impl ScriptedH2cProxy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self {
            listener,
            address,
            tunnels: Tunnels::default(),
            taken: Cell::new(0),
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
            let handler: HttpHandlerRef = Rc::new(TunnelHandler {
                tunnels: Rc::clone(&self.tunnels),
            });
            let options =
                ServeTransportOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
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
    async fn tunnel(&self) -> Rc<Tunnel> {
        let next = self.taken.get();
        eventually("a CONNECT at the proxy", || {
            self.tunnels.borrow().len() > next
        })
        .await;
        self.taken.set(next + 1);
        Rc::clone(&self.tunnels.borrow()[next])
    }

    /// Releases every tunnel's request and unfinished response, so no handler
    /// source holds a proxy stream open, and cancels every proxy session.
    fn release(&self) {
        for tunnel in self.tunnels.borrow().iter() {
            drop(std::mem::take(&mut *tunnel.request.borrow_mut()));
            tunnel.response.abort(HttpErrorCode::CANCEL, b"");
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

impl Drop for ScriptedH2cProxy {
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

    async fn connect(&self, context: &WorkerContext) -> (ClientSession, TcpStream) {
        let connect = HttpConnector::connect(
            context,
            socket_address(self.address),
            Duration::ZERO,
            ConnectionParams::default(),
            SessionParams::default(),
            CancellationToken::never(),
        );
        let (accepted, session) = tokio::join!(self.listener.accept(), connect);
        let (socket, _) = accepted.expect("proxy accept");
        (session.expect("client connects to the proxy"), socket)
    }
}

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

/// How the client ended the raw proxy's TCP connection, and what it sent first.
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

/// Counts tunnel bytes that a client session buffered for a response source,
/// which it parses after the response head that precedes them.
#[derive(Default)]
struct BufferedReads(Cell<i64>);

impl Http2SessionStats for BufferedReads {
    fn record_buffered_read_bytes(&self, delta: i64) {
        if delta > 0 {
            self.0.set(self.0.get() + delta);
        }
    }
}

fn observed_params(reads: &Rc<BufferedReads>) -> SessionParams {
    SessionParams {
        session_stats: Some(reads.clone()),
        ..SessionParams::default()
    }
}

async fn buffered(reads: &BufferedReads, bytes: i64) {
    eventually("buffered tunnel bytes", || reads.0.get() >= bytes).await;
}

type ConnectFuture = LocalConnectFuture<Result<HttpConnectStream, HttpError>>;

#[derive(Clone, Copy, Debug)]
enum Ownership {
    Shared,
    Unique,
}

fn start_connect(
    ownership: Ownership,
    context: &WorkerContext,
    session: &ClientSession,
    connect_headers: RequestHeaderMap,
    timeout: Duration,
    cancellation: CancellationToken,
) -> ConnectFuture {
    let reservation = session
        .reserve_request()
        .expect("proxy session reservation");
    match ownership {
        Ownership::Shared => HttpConnectStream::connect(
            context,
            session,
            reservation,
            AUTHORITY,
            timeout,
            connect_headers,
            EGRESS_BUFFER_SIZE,
            cancellation,
        ),
        Ownership::Unique => HttpConnectStream::connect_unique(
            context,
            session.clone(),
            reservation,
            AUTHORITY,
            timeout,
            connect_headers,
            EGRESS_BUFFER_SIZE,
            cancellation,
        ),
    }
}

/// Polls `connect` once, which commits the CONNECT and parks its head wait.
async fn poll_until_parked(connect: &mut ConnectFuture) {
    let polled = poll_fn(|context| Poll::Ready(connect.as_mut().poll(context))).await;
    assert!(
        polled.is_pending(),
        "the connect waits for the CONNECT response head"
    );
}

/// The fields of `message` in order, each name as the message stores it.
fn fields(message: &HttpMessage) -> Vec<(String, String)> {
    message
        .headers()
        .iter()
        .map(|(_, name, value)| {
            (
                String::from_utf8_lossy(name).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            )
        })
        .collect()
}

/// The fields of a head from a `roxygen_server` proxy, which appends a `Date`
/// field to each response, without that field.
fn server_fields(message: &HttpMessage) -> Vec<(String, String)> {
    let mut fields = fields(message);
    let date = fields.pop().expect("the server's Date field");
    assert_eq!(date.0, "Date", "{fields:?}");
    fields
}

fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

// ---------------------------------------------------------------------------
// Oracles.

/// A fwdproxy-style success head: two `Proxy-Status` and two
/// `X-FB-Fwdproxy-Debug` lines, one `Proxy-Status` name in lowercase.
const FWDPROXY_SUCCESS_FIELDS: [(&str, &str); 6] = [
    ("X-Connected-To", "1.2.3.4"),
    ("Proxy-Status", "fwdproxy-a; next-hop=\"origin\""),
    ("X-FB-Fwdproxy-Debug", "zone=\"a\", host=1"),
    ("X-FB-Fwdproxy-Request-ID", "request-1"),
    ("proxy-status", "fwdproxy-b"),
    ("X-FB-Fwdproxy-Debug", "zone=\"b\""),
];

const H1_FWDPROXY_SUCCESS: &[u8] = b"HTTP/1.1 200 Connection Established\r\n\
X-Connected-To: 1.2.3.4\r\n\
Proxy-Status: fwdproxy-a; next-hop=\"origin\"\r\n\
X-FB-Fwdproxy-Debug: zone=\"a\", host=1\r\n\
X-FB-Fwdproxy-Request-ID: request-1\r\n\
proxy-status: fwdproxy-b\r\n\
X-FB-Fwdproxy-Debug: zone=\"b\"\r\n\
\r\n";

fn assert_success_head(
    stream: &HttpConnectStream,
    head_fields: fn(&HttpMessage) -> Vec<(String, String)>,
    expected: &[(&str, &str)],
    case: &str,
) {
    let head = stream
        .connect_response()
        .unwrap_or_else(|| panic!("{case}: the final 2xx head is retained"));
    assert_eq!(head.response_status(), Some(200), "{case}");
    assert_eq!(head_fields(head), owned(expected), "{case}");
    assert_eq!(stream.peer_address().host(), b"1.2.3.4", "{case}");
    assert_eq!(stream.extract_user_session_id(), b"request-1", "{case}");
}

#[test]
fn success_head_is_retained_with_ordered_duplicate_fields_h1_h2() {
    run_on_worker(|context| {
        Box::pin(async move {
            // Common names take the table's spelling; other names keep the wire case.
            let h1_expected = [
                ("X-Connected-To", "1.2.3.4"),
                ("Proxy-Status", "fwdproxy-a; next-hop=\"origin\""),
                ("X-FB-Fwdproxy-Debug", "zone=\"a\", host=1"),
                ("X-FB-Fwdproxy-Request-ID", "request-1"),
                ("Proxy-Status", "fwdproxy-b"),
                ("X-FB-Fwdproxy-Debug", "zone=\"b\""),
            ];
            let proxy = RawH1Proxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let (session, mut socket) = proxy.connect(&context).await;
                let mut connect = start_connect(
                    ownership,
                    &context,
                    &session,
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                read_h1_head(&mut socket).await;
                socket
                    .write_all(H1_FWDPROXY_SUCCESS)
                    .await
                    .expect("proxy response");
                let stream = connect.await.expect("CONNECT");
                assert_success_head(&stream, fields, &h1_expected, &format!("H1 {ownership:?}"));
            }

            // Common names take the table's spelling; HPACK lowercases the others.
            let h2c_expected = [
                ("x-connected-to", "1.2.3.4"),
                ("Proxy-Status", "fwdproxy-a; next-hop=\"origin\""),
                ("x-fb-fwdproxy-debug", "zone=\"a\", host=1"),
                ("x-fb-fwdproxy-request-id", "request-1"),
                ("Proxy-Status", "fwdproxy-b"),
                ("x-fb-fwdproxy-debug", "zone=\"b\""),
            ];
            let proxy = ScriptedH2cProxy::start().await;
            for ownership in [Ownership::Shared, Ownership::Unique] {
                let session = proxy.connect(&context, SessionParams::default()).await;
                let mut connect = start_connect(
                    ownership,
                    &context,
                    &session,
                    headers(&[("Foo", "Bar")]),
                    CONNECT_TIMEOUT,
                    CancellationToken::never(),
                );
                poll_until_parked(&mut connect).await;
                proxy
                    .tunnel()
                    .await
                    .respond(200, &FWDPROXY_SUCCESS_FIELDS, false);
                let stream = connect.await.expect("CONNECT");
                assert_success_head(
                    &stream,
                    server_fields,
                    &h2c_expected,
                    &format!("h2c {ownership:?}"),
                );
            }
            proxy.shutdown().await;
        })
    });
}

#[test]
fn take_connect_response_empties_the_head() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut stream = connect_unique(
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CancellationToken::never(),
            )
            .await
            .expect("CONNECT");
            assert!(stream.connect_response().is_some());
            let head = stream
                .take_connect_response()
                .expect("the first take returns the head");
            assert_eq!(head.response_status(), Some(200));
            assert_eq!(
                server_fields(&head),
                owned(&[("x-connected-to", "1.2.3.4")])
            );
            assert!(stream.connect_response().is_none());
            assert!(stream.take_connect_response().is_none());
            assert_eq!(stream.peer_address().host(), b"1.2.3.4");
            drop(stream);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn informational_head_is_not_retained() {
    run_on_worker(|context| {
        Box::pin(async move {
            // The frozen `ConnectHandler` sends a 107 before its 200.
            let proxy = H2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let stream = connect_unique(
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CancellationToken::never(),
            )
            .await
            .expect("CONNECT");
            let head = stream.connect_response().expect("the final head");
            assert_eq!(head.response_status(), Some(200), "not the 107");
            assert_eq!(server_fields(head), owned(&[("x-connected-to", "1.2.3.4")]));
            drop(stream);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn connect_head_with_eom_is_retained() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = ScriptedH2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            proxy
                .tunnel()
                .await
                .respond(200, &[("X-Connected-To", "1.2.3.4")], true);
            let stream = connect.await.expect("CONNECT");
            assert!(!stream.can_read(), "the 200 carried EOM");
            assert!(stream.can_write());
            let head = stream.connect_response().expect("the EOM head is retained");
            assert_eq!(head.response_status(), Some(200));
            assert_eq!(server_fields(head), owned(&[("x-connected-to", "1.2.3.4")]));
            drop(stream);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

const REJECTION_MESSAGE: &[u8] = b"CONNECT to example.com:443 failed status=500";

/// Checks the core error and its rejection cause, and returns the rejected head.
fn rejected_head<'a>(error: &'a HttpError, case: &str) -> &'a HttpMessage {
    assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR, "{case}");
    assert_eq!(error.message(), REJECTION_MESSAGE, "{case}");
    assert!(error.http_message().is_none(), "{case}");
    let rejected =
        connect_rejection(error).unwrap_or_else(|| panic!("{case}: the rejection cause"));
    assert_eq!(rejected.authority(), AUTHORITY, "{case}");
    assert_eq!(rejected.status_code(), 500, "{case}");
    assert_eq!(rejected.to_string().as_bytes(), REJECTION_MESSAGE, "{case}");
    assert_eq!(rejected.response().response_status(), Some(500), "{case}");
    rejected.response()
}

/// A fwdproxy-style error head.
const FWDPROXY_ERROR_FIELDS: [(&str, &str); 3] = [
    ("Proxy-Status", "fwdproxy; error=destination_ip_unroutable"),
    ("X-FB-Error-From", "fwdproxy"),
    ("Cache-Control", "no-cache"),
];

/// An HTTP/1.1 500 status line and [`FWDPROXY_ERROR_FIELDS`], before the
/// framing field and the end of the head.
const H1_FWDPROXY_ERROR: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\n\
Proxy-Status: fwdproxy; error=destination_ip_unroutable\r\n\
X-FB-Error-From: fwdproxy\r\n\
Cache-Control: no-cache\r\n";

/// Rejects a unique h2c CONNECT with a 500 that carries
/// [`FWDPROXY_ERROR_FIELDS`] and, when `body` is not empty, that body, and
/// returns the error and the proxy's request read result.
async fn h2c_rejection(
    context: &WorkerContext,
    proxy: &ScriptedH2cProxy,
    body: &[u8],
) -> (HttpError, Result<(), HttpError>) {
    let session = proxy.connect(context, SessionParams::default()).await;
    let mut connect = start_connect(
        Ownership::Unique,
        context,
        &session,
        headers(&[("Foo", "Bar")]),
        CONNECT_TIMEOUT,
        CancellationToken::never(),
    );
    poll_until_parked(&mut connect).await;
    let tunnel = proxy.tunnel().await;
    tunnel.respond(500, &FWDPROXY_ERROR_FIELDS, body.is_empty());
    if !body.is_empty() {
        tunnel.response.body(queue(body), 0, true);
    }
    let error = connect.await.err().expect("a 500 is rejected");
    (error, tunnel.read_to_end().await)
}

#[test]
fn rejection_exposes_status_head_and_core_message() {
    run_on_worker(|context| {
        Box::pin(async move {
            let h2c_expected = owned(&[
                ("Proxy-Status", "fwdproxy; error=destination_ip_unroutable"),
                ("x-fb-error-from", "fwdproxy"),
                ("Cache-Control", "no-cache"),
            ]);
            let proxy = ScriptedH2cProxy::start().await;

            // With END_STREAM on the head, the proxy receives the egress abort.
            let (error, read) = h2c_rejection(&context, &proxy, b"").await;
            assert_eq!(
                server_fields(rejected_head(&error, "h2c EOM")),
                h2c_expected
            );
            let read = read.expect_err("the proxy observes the egress abort");
            assert_eq!(read.code(), HttpErrorCode::CONNECT_ERROR);

            // With a body, Drop's close() resets the unfinished response with
            // CANCEL before the session sees the abort (expectation-303).
            let (error, read) = h2c_rejection(&context, &proxy, b"fwdproxy error body").await;
            assert_eq!(
                server_fields(rejected_head(&error, "h2c body")),
                h2c_expected
            );
            let read = read.expect_err("no END_STREAM reaches the proxy");
            assert_eq!(read.code(), HttpErrorCode::CANCEL);
            proxy.shutdown().await;

            // The frozen `ConnectHandler` answers `Fail` with a 107, then a 500
            // with END_STREAM.
            let proxy = H2cProxy::start().await;
            proxy.expect_exceptions();
            let session = proxy.connect(&context, SessionParams::default()).await;
            let error = start_connect(
                Ownership::Shared,
                &context,
                &session,
                headers(&[("Fail", "True")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            )
            .await
            .err()
            .expect("a 500 is rejected");
            let head = rejected_head(&error, "h2c shared after a 107");
            assert!(server_fields(head).is_empty(), "the final 500, not the 107");
            let log = Rc::clone(&proxy.log);
            eventually(
                "the proxy's request read to observe the egress abort",
                || !log.read_errors.borrow().is_empty(),
            )
            .await;
            assert_eq!(log.read_error_codes(), [HttpErrorCode::CONNECT_ERROR]);
            drop(session);
            proxy.shutdown().await;

            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            socket
                .write_all(&[H1_FWDPROXY_ERROR, b"Content-Length: 0\r\n\r\n"].concat())
                .await
                .expect("proxy response");
            let error = connect.await.err().expect("a 500 is rejected");
            let mut h1_expected = owned(&FWDPROXY_ERROR_FIELDS);
            h1_expected.push(("Content-Length".to_owned(), "0".to_owned()));
            assert_eq!(fields(rejected_head(&error, "H1")), h1_expected);
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new()),
                "the egress abort resets the proxy connection"
            );
        })
    });
}

#[test]
fn timeout_cancellation_and_peer_reset_carry_no_rejection() {
    run_on_worker(|context| {
        Box::pin(async move {
            // An H1 CONNECT timeout.
            let proxy = RawH1Proxy::start().await;
            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                Duration::from_millis(100),
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            let error = connect.await.err().expect("a silent proxy times out");
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            assert!(connect_rejection(&error).is_none());

            // An H1 non-2xx with a non-zero `Content-Length` fails before the
            // status check, because the codec ignores every CONNECT response
            // body (`DIV-011`'s limit).
            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            read_h1_head(&mut socket).await;
            socket
                .write_all(
                    &[
                        H1_FWDPROXY_ERROR,
                        b"Content-Length: 19\r\n\r\nfwdproxy error body",
                    ]
                    .concat(),
                )
                .await
                .expect("proxy response");
            let error = connect
                .await
                .err()
                .expect("the ignored body mismatches its length");
            assert_eq!(error.code(), HttpErrorCode::CONTENT_LENGTH_MISMATCH);
            assert_eq!(
                error.http_message().and_then(HttpMessage::response_status),
                Some(500),
                "the queued head rides on the error (`SRC-010`)"
            );
            assert!(connect_rejection(&error).is_none());

            // A cancellation before any head.
            let proxy = ScriptedH2cProxy::start().await;
            let session = proxy.connect(&context, SessionParams::default()).await;
            let cancel = CancellationSource::new();
            let mut connect = start_connect(
                Ownership::Shared,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                cancel.token(),
            );
            poll_until_parked(&mut connect).await;
            proxy.tunnel().await;
            cancel.request_cancellation();
            let error = connect
                .await
                .err()
                .expect("the cancellation fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert!(error.http_message().is_none());
            assert!(connect_rejection(&error).is_none());

            // A cancellation that carries a queued 503 head (`DIV-019`).
            let reads = Rc::new(BufferedReads::default());
            let session = proxy.connect(&context, observed_params(&reads)).await;
            let cancel = CancellationSource::new();
            let mut connect = start_connect(
                Ownership::Shared,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                cancel.token(),
            );
            poll_until_parked(&mut connect).await;
            let tunnel = proxy.tunnel().await;
            tunnel.respond(
                503,
                &[("Proxy-Status", "fwdproxy; error=destination_unavailable")],
                false,
            );
            tunnel.response.body(queue(b"x"), 0, false);
            buffered(&reads, 1).await;
            cancel.request_cancellation();
            let error = connect
                .await
                .err()
                .expect("the cancellation fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(
                error.http_message().and_then(HttpMessage::response_status),
                Some(503),
                "the queued head rides on the error"
            );
            assert!(connect_rejection(&error).is_none());

            // A peer RST_STREAM(CONNECT_ERROR) before the head has the
            // rejection's code but no rejection cause.
            let session = proxy.connect(&context, SessionParams::default()).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            proxy
                .tunnel()
                .await
                .response
                .abort(HttpErrorCode::CONNECT_ERROR, b"");
            let error = connect
                .await
                .err()
                .expect("the peer reset fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR, "{error:?}");
            assert!(connect_rejection(&error).is_none());
            proxy.shutdown().await;
        })
    });
}

/// The CONNECT request for [`AUTHORITY`] and [`wire_connect_headers`], as
/// `connect/src/stream.rs` sent it before the `DIV-011` extension, which must
/// leave it unchanged.
const RECORDED_CONNECT_REQUEST: &[u8] = b"CONNECT example.com:443 HTTP/1.1\r\n\
Host: example.com:443\r\n\
Foo: Bar\r\n\
X-FB-Fwdproxy-Debug: True\r\n\
Connection: keep-alive\r\n\
\r\n";

fn wire_connect_headers() -> RequestHeaderMap {
    headers(&[("X-FB-Fwdproxy-Debug", "True"), ("Foo", "Bar")])
}

#[test]
fn connect_wire_bytes_are_unchanged_by_the_extension() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = RawH1Proxy::start().await;

            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                wire_connect_headers(),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let head = read_h1_head(&mut socket).await;
            assert_eq!(
                String::from_utf8_lossy(&head),
                String::from_utf8_lossy(RECORDED_CONNECT_REQUEST)
            );
            socket
                .write_all(
                    b"HTTP/1.1 200 Connection Established\r\nX-Connected-To: 1.2.3.4\r\n\r\n",
                )
                .await
                .expect("proxy response");
            let mut stream = connect.await.expect("CONNECT");
            assert!(stream.connect_response().is_some());
            assert!(stream.take_connect_response().is_some());
            drop(stream);
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Fin(Vec::new())
            );

            let (session, mut socket) = proxy.connect(&context).await;
            let mut connect = start_connect(
                Ownership::Unique,
                &context,
                &session,
                wire_connect_headers(),
                CONNECT_TIMEOUT,
                CancellationToken::never(),
            );
            poll_until_parked(&mut connect).await;
            let head = read_h1_head(&mut socket).await;
            assert_eq!(
                String::from_utf8_lossy(&head),
                String::from_utf8_lossy(RECORDED_CONNECT_REQUEST)
            );
            socket
                .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("proxy response");
            let error = connect.await.err().expect("a 500 is rejected");
            assert!(connect_rejection(&error).is_some());
            assert_eq!(
                read_until_close(&mut socket).await,
                PeerClose::Reset(Vec::new())
            );
        })
    });
}
