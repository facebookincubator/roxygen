//! The router prototype's ingress combination: `serve_transport` with
//! `abort_exchanges_on_eof` (`DIV-053`) and `non_abortive_close` (`DIV-038`),
//! serving a response source that polls its body events in place (`DIV-039`),
//! over loopback TCP on a `RuntimeOwner` worker and a `HostedWorker`.

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

/// The response's body events, fed by the test, and what the session did to
/// the source.
#[derive(Default)]
struct Script {
    events: RefCell<VecDeque<Result<HttpBodyEvent, HttpError>>>,
    waker: RefCell<Option<Waker>>,
    reads: Cell<usize>,
    stopped: Cell<bool>,
    dropped: Cell<bool>,
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

struct PollingHandler(Rc<Script>);

impl HttpHandler for PollingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mut message = HttpMessage::new();
        message.set_status_code(200).expect("status");
        message
            .set_status_message(HttpMessage::default_reason(200))
            .expect("reason");
        message.set_http_version(1, 1);
        let source = PollingResponse {
            header: Some(HttpHeaderEvent::new(message, false).expect("response head")),
            script: Rc::clone(&self.0),
        };
        Box::pin(std::future::ready(Ok(HttpSourceHolder::new(Box::new(
            source,
        )))))
    }
}

/// Serves one loopback connection with the router's server settings.
async fn serve_router_connection(
    context: &WorkerContext,
    script: &Rc<Script>,
) -> (ServerConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().expect("listener address")),
        listener.accept()
    );
    let (accepted, _) = accepted.expect("connection accepted");
    let session = ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        abort_exchanges_on_eof: true,
        non_abortive_close: true,
        ..ServerSessionConfig::default()
    };
    let transport = TcpTransport::from_accepted(
        context,
        accepted.into_std().expect("standard stream"),
        session.byte_stream_limits,
    )
    .expect("transport adopted");
    let mut options =
        ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
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
        let (connection, mut client) = serve_router_connection(&context, &script).await;
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
        let (connection, mut client) = serve_router_connection(&context, &script).await;
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
