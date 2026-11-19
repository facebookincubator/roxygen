/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Connect-phase tests for `HttpConnectStream`, and the h2c proxy fixture that
//! the CONNECT tests share.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use futures::FutureExt;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_connect::stream::HttpConnectStream;
use roxygen_connect::stream::RequestHeaderMap;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::worker::WorkerContext;
use roxygen_server::connection::ServeTransportOptions;
use roxygen_server::connection::ServerConnection;
use roxygen_server::connection::serve_transport;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::task::LocalSet;

pub(crate) const AUTHORITY: &str = "example.com:443";
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const EGRESS_BUFFER_SIZE: u32 = 64 * 1024;
pub(crate) const WAIT_DEADLINE: Duration = Duration::from_secs(5);
const TEST_DEADLINE: Duration = Duration::from_secs(30);
const HTTP2_INITIAL_WINDOW_SIZE: u16 = 4;

pub(crate) type LocalTest = Pin<Box<dyn Future<Output = ()>>>;

/// Runs `test` on one Roxygen worker whose Tokio runtime also drives the proxy.
pub(crate) fn run_on_worker(test: impl FnOnce(WorkerContext) -> LocalTest + Send + 'static) {
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

/// Polls `condition` on the worker until it holds.
pub(crate) async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let reached = tokio::time::timeout(WAIT_DEADLINE, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for {what}");
}

pub(crate) fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

pub(crate) fn headers(pairs: &[(&str, &str)]) -> RequestHeaderMap {
    pairs
        .iter()
        .map(|(name, value)| (name.as_bytes().into(), value.as_bytes().into()))
        .collect()
}

pub(crate) fn response(status: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(status).expect("response status");
    message.set_http_version(1, 1);
    message
}

pub(crate) fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::copy_from_slice(bytes, bytes.len()).expect("bounded queue")
}

pub(crate) fn bytes_of(queue: &BufQueue) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(queue.chain_length());
    for segment in queue.segments() {
        bytes.extend_from_slice(segment);
    }
    bytes
}

/// What the proxy saw on one CONNECT request.
pub(crate) struct ProxyRequest {
    pub(crate) head: HttpMessage,
    pub(crate) peer: Option<TransportEndpoint>,
}

#[derive(Default)]
pub(crate) struct ProxyLog {
    pub(crate) requests: RefCell<Vec<ProxyRequest>>,
    /// Errors that a tunnel's request read returned to `handle_connect`.
    pub(crate) read_errors: RefCell<Vec<HttpError>>,
    /// Tunnels whose request read ended with EOM.
    pub(crate) request_eoms: RefCell<usize>,
    /// Set by `expect_exceptions`. While it is `false`, a checked request read
    /// must not fail.
    exceptions_expected: Cell<bool>,
    /// The request read errors checked against `exceptions_expected`: those
    /// that arrive while the response source is live or after a CONNECT head
    /// with EOM.
    checked_read_errors: RefCell<Vec<HttpError>>,
    tasks: RefCell<Vec<JoinHandle<()>>>,
    /// The latest tunnel's response producer, cleared when any tunnel's
    /// `handle_connect` ends.
    pub(crate) cur_resp_source: RefCell<Option<HttpStreamSourceProducer>>,
    /// The delay before a tunnel starts echoing.
    pub(crate) timeout: Cell<Duration>,
}

impl ProxyLog {
    pub(crate) fn read_error_codes(&self) -> Vec<HttpErrorCode> {
        self.read_errors
            .borrow()
            .iter()
            .map(HttpError::code)
            .collect()
    }
}

/// The proxy's CONNECT handler. It sends a 1xx head, then a 200 with
/// `X-Connected-To: 1.2.3.4`, and echoes the tunnel. The `Slow`, `Fail` and
/// `EomOnConnect` request headers delay the final head, make it a 500, or end
/// the response with it.
pub(crate) struct ConnectHandler {
    pub(crate) log: Rc<ProxyLog>,
}

impl HttpHandler for ConnectHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let log = Rc::clone(&self.log);
        let peer = context
            .connection_context()
            .map(|connection| connection.peer_endpoint().clone());
        Box::pin(async move {
            let head = request.read_header_event().await?.into_headers();
            let mut connected = response(200);
            connected.headers_mut().set(b"X-Connected-To", b"1.2.3.4");
            let source = HttpStreamSource::new_async(None, None, 65_535).map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
            })?;
            let producer = source.producer();
            // A 1xx head first, which the client must skip.
            producer.headers(response(107), false);
            let request_headers = head.headers();
            if request_headers.exists(b"Slow") {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let eom_on_connect = request_headers.exists(b"EomOnConnect");
            if request_headers.exists(b"Fail") {
                producer.headers(response(500), true);
            } else if eom_on_connect {
                producer.headers(connected, true);
            } else {
                producer.headers(connected, false);
            }
            log.requests.borrow_mut().push(ProxyRequest { head, peer });
            *log.cur_resp_source.borrow_mut() = Some(producer.clone());
            let task = tokio::task::spawn_local(handle_connect(
                request,
                producer,
                eom_on_connect,
                Rc::clone(&log),
            ));
            log.tasks.borrow_mut().push(task);
            Ok(HttpSourceHolder::new(Box::new(source)))
        })
    }
}

/// Echoes the tunnel until EOM or `write_fin`, after the configured delay.
async fn handle_connect(
    request: HttpSourceHolder,
    response: HttpStreamSourceProducer,
    response_complete: bool,
    log: Rc<ProxyLog>,
) {
    let timeout = log.timeout.get();
    if !timeout.is_zero() {
        tokio::time::sleep(timeout).await;
    }
    echo_tunnel(request, response, response_complete, &log).await;
    *log.cur_resp_source.borrow_mut() = None;
}

async fn echo_tunnel(
    mut request: HttpSourceHolder,
    response: HttpStreamSourceProducer,
    response_complete: bool,
    log: &ProxyLog,
) {
    loop {
        let event = request.read_body_event(u32::MAX).await;
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                // An error after the response source is gone and incomplete
                // is not checked against `exceptions_expected`.
                if response.is_live() || response_complete {
                    log.checked_read_errors.borrow_mut().push(error.clone());
                }
                log.read_errors.borrow_mut().push(error);
                return;
            }
        };
        let eom = event.eom();
        let kind = event.into_kind();
        let body = match kind {
            HttpBodyEventKind::Suspend(resume) => {
                resume.await;
                continue;
            }
            HttpBodyEventKind::Body(body) => Some(body),
            _ => None,
        };
        if !response.is_live() && !response_complete {
            break;
        }
        let write_fin = body
            .as_ref()
            .and_then(BufQueue::front)
            .is_some_and(|front| front.starts_with(b"write_fin"));
        let finished = eom || write_fin;
        if let Some(body) = body {
            if response_complete {
                assert_eq!(bytes_of(&body), b"a");
            } else {
                response.body(body, 0, finished);
            }
        }
        if finished {
            if eom {
                *log.request_eoms.borrow_mut() += 1;
            }
            break;
        }
    }
}

/// An h2c `roxygen_server` proxy on the test worker that serves
/// `ConnectHandler`.
pub(crate) struct H2cProxy {
    listener: tokio::net::TcpListener,
    address: SocketAddr,
    pub(crate) log: Rc<ProxyLog>,
    connections: RefCell<Vec<ServerConnection>>,
}

impl H2cProxy {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy listener");
        let address = listener.local_addr().expect("proxy address");
        Self {
            listener,
            address,
            log: Rc::default(),
            connections: RefCell::default(),
        }
    }

    /// Connects a client session to the proxy over h2c.
    pub(crate) async fn connect(
        &self,
        context: &WorkerContext,
        session: SessionParams,
    ) -> ClientSession {
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
            let mut options =
                ServeTransportOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
            // To help the write timeout test, use the default stream flow control.
            let mut initial_window = options.session.settings[0];
            initial_window.identifier = HTTP2_INITIAL_WINDOW_SIZE;
            initial_window.value = 64 * 1024;
            options.session.settings = vec![initial_window];
            let handler: HttpHandlerRef = Rc::new(ConnectHandler {
                log: Rc::clone(&self.log),
            });
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

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// Allows every request read to fail.
    pub(crate) fn expect_exceptions(&self) {
        self.log.exceptions_expected.set(true);
    }

    /// Joins the tunnel tasks, which end at the client's EOM or a request read
    /// error, and checks that request reads did not fail unless failures were
    /// expected. Then cancels every proxy session and checks each CONNECT
    /// head's `Host`, and its `Foo` unless it carried `Fail` or `EomOnConnect`.
    pub(crate) async fn shutdown(self) {
        let tasks = std::mem::take(&mut *self.log.tasks.borrow_mut());
        for task in tasks {
            tokio::time::timeout(WAIT_DEADLINE, task)
                .await
                .expect("a tunnel task finishes once its client ends the tunnel")
                .expect("a tunnel task does not panic");
        }
        if !self.log.exceptions_expected.get() {
            assert!(
                self.log.checked_read_errors.borrow().is_empty(),
                "no tunnel request read fails: {:?}",
                self.log.checked_read_errors.borrow()
            );
        }
        let connections = std::mem::take(&mut *self.connections.borrow_mut());
        for connection in &connections {
            connection.cancel();
        }
        for connection in &connections {
            let _ = tokio::time::timeout(WAIT_DEADLINE, connection.wait_closed())
                .await
                .expect("a cancelled proxy session closes");
        }
        drop(connections);
        for request in self.log.requests.borrow().iter() {
            let fields = request.head.headers();
            assert_eq!(
                fields.get_single_or_empty(HttpHeaderCode::Host),
                AUTHORITY.as_bytes()
            );
            if !fields.exists(b"Fail") && !fields.exists(b"EomOnConnect") {
                assert_eq!(fields.get_single_or_empty(b"Foo"), b"Bar");
            }
        }
    }
}

/// Records whether a client session started draining.
#[derive(Default)]
pub(crate) struct DrainObserver {
    pub(crate) drains: RefCell<usize>,
}

impl LifecycleObserver for DrainObserver {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::DrainStarted(_)) {
            *self.drains.borrow_mut() += 1;
        }
    }
}

pub(crate) async fn connect_unique(
    context: &WorkerContext,
    session: &ClientSession,
    connect_headers: RequestHeaderMap,
    cancellation: CancellationToken,
) -> Result<HttpConnectStream, HttpError> {
    let reservation = session
        .reserve_request()
        .expect("proxy session reservation");
    HttpConnectStream::connect_unique(
        context,
        session.clone(),
        reservation,
        AUTHORITY,
        CONNECT_TIMEOUT,
        connect_headers,
        EGRESS_BUFFER_SIZE,
        cancellation,
    )
    .await
}

#[test]
fn connect_non_2xx_fails_with_connect_error() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            proxy.expect_exceptions();
            let session = proxy.connect(&context, SessionParams::default()).await;
            let error = connect_unique(
                &context,
                &session,
                headers(&[("Fail", "True")]),
                CancellationToken::never(),
            )
            .await
            .err()
            .expect("a 500 CONNECT response fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CONNECT_ERROR);
            assert_eq!(
                error.message(),
                b"CONNECT to example.com:443 failed status=500"
            );
            let log = Rc::clone(&proxy.log);
            eventually(
                "the proxy's request read to observe the egress abort",
                || !log.read_errors.borrow().is_empty(),
            )
            .await;
            assert_eq!(log.read_error_codes(), [HttpErrorCode::CONNECT_ERROR]);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn connect_timeout_overrides_stream_read_timeout() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let session = proxy
                .connect(
                    &context,
                    SessionParams {
                        stream_read_timeout: Some(Duration::from_millis(100)),
                        ..SessionParams::default()
                    },
                )
                .await;
            let stream = connect_unique(
                &context,
                &session,
                headers(&[("Slow", "True"), ("Foo", "Bar")]),
                CancellationToken::never(),
            )
            .await
            .expect("a 1 s CONNECT timeout outlasts the 500 ms proxy delay");
            assert!(stream.can_read());
            assert!(stream.can_write());
            drop(stream);
            drop(session);
            proxy.shutdown().await;
        })
    });
}

#[test]
fn precancelled_connect_unique_drops_safely() {
    run_on_worker(|context| {
        Box::pin(async move {
            let proxy = H2cProxy::start().await;
            let observer = Rc::new(DrainObserver::default());
            let session = proxy
                .connect(
                    &context,
                    SessionParams {
                        lifecycle_observer: Some(observer.clone()),
                        ..SessionParams::default()
                    },
                )
                .await;
            // Cancel after the session exists so connect_unique constructs the stream.
            let cancellation = CancellationSource::new();
            cancellation.request_cancellation();
            let error = connect_unique(
                &context,
                &session,
                headers(&[("Foo", "Bar")]),
                cancellation.token(),
            )
            .await
            .err()
            .expect("a cancelled token fails the connect");
            assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
            assert_eq!(error.message(), b"Cancelled");
            assert_eq!(
                *observer.drains.borrow(),
                1,
                "the unique stream drains its session"
            );
            assert!(!session.supports_more_transactions());
            assert!(
                proxy.log.requests.borrow().is_empty(),
                "no CONNECT was sent"
            );
            drop(session);
            proxy.shutdown().await;
        })
    });
}
