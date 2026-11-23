/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `HttpServer::start_owned`: an embedder's own `OwnedApplication` hosted on the server's
//! workers, with the server's accept, drain and worker model.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::filter_factory::FilterFactory;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::worker::WorkerContext;
use roxygen_session::DeadlineExpiry;
use roxygen_session::OwnedConnectionDeadlines;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::owned::OwnedApplication;
use roxygen_session::owned::OwnedExchange;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::session::LifecycleObserver;
use roxygen_transport::tls::TlsServerOptions;
use roxygen_transport::tls::builtin_crypto_provider;
use rustls::ClientConfig;
use rustls::ClientConnection;
use rustls::RootCertStore;
use rustls::StreamOwned;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::pki_types::ServerName;

use crate::ConnectionId;
use crate::ConnectionLifecycleObserver;
use crate::ConnectionOutcome;
use crate::acceptor::ConnectionInfo;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::fault_next_http2_run;
use crate::owned_application::OwnedApplicationFactory;
use crate::owned_application::OwnedAttached;
use crate::owned_application::OwnedConnectionAttach;
use crate::owned_application::OwnedConnectionControl;
use crate::owned_application::OwnedConnectionFactory;
use crate::owned_application::StopAbandon;
use crate::owned_application::WorkerView;
use crate::server::AcceptDistribution;
use crate::server::DequeueHoldForTest;
use crate::server::HttpServer;
use crate::server::HttpServerError;
use crate::server::ServerCallbackStage;
use crate::server::ServerConfig;
use crate::server::ServerFilterFactory;
use crate::server::ServerObserver;
use crate::server::StaticTlsCredentials;

const TIMEOUT: Duration = Duration::from_secs(10);

/// What the test application's connections and exchanges report to the test thread.
#[derive(Default)]
struct Probe {
    attaches: AtomicUsize,
    seen: Mutex<Vec<SeenConnection>>,
    /// Each factory call's worker index, its thread and its context's owner thread.
    factory_calls: Mutex<Vec<(usize, ThreadId, ThreadId)>>,
    /// The error of each exchange's `cancel`.
    cancels: Mutex<Vec<HttpErrorCode>>,
    /// For each `/drain` request: the result of `drain()`, and whether the connection's
    /// handshake had completed and its session had finished.
    drains: Mutex<Vec<(Result<bool, HttpErrorCode>, bool, bool)>>,
    /// Exchange completions and `worker_stopping` calls and polls, in order.
    events: Mutex<Vec<String>>,
    /// Requests delivered to exchanges.
    requests: AtomicUsize,
    /// Releases the responses of `/hold` requests.
    release: Gate,
    /// Polls of `worker_stopping` futures.
    stop_polls: AtomicUsize,
    /// Resolves gated `worker_stopping` futures.
    stop_release: Gate,
    /// How the next attaches fail, in order; once empty, attaches serve.
    attach_failures: Mutex<VecDeque<AttachFailure>>,
}

/// How an attach fails.
#[derive(Clone, Copy)]
enum AttachFailure {
    Error,
    Panic,
    /// The attach serves, and the session's HTTP/2 run unwinds out of its first poll.
    RunPanic,
}

/// What `attach` saw of one connection.
#[derive(Debug, PartialEq)]
struct SeenConnection {
    on_worker_thread: bool,
    worker_index: usize,
    id: ConnectionId,
    protocol: CodecProtocol,
    secure: bool,
    info_peer_identity: Option<Vec<u8>>,
    context_peer_identity: Option<Vec<u8>>,
    context_is_info_context: bool,
}

impl Probe {
    fn attaches(&self) -> usize {
        self.attaches.load(Ordering::SeqCst)
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }

    fn stop_polls(&self) -> usize {
        self.stop_polls.load(Ordering::SeqCst)
    }

    fn event(&self, event: String) {
        self.events.lock().unwrap().push(event);
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
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
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.open.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *self.waker.lock().unwrap() = Some(context.waker().clone());
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
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

/// The application factory: each worker gets connections that report to `probe`, and no
/// `worker_stopping` future.
fn factory(probe: &Arc<Probe>) -> OwnedApplicationFactory {
    factory_with(probe, StopBehavior::None)
}

/// What each worker's `worker_stopping` returns.
#[derive(Clone, Copy)]
enum StopBehavior {
    /// No future.
    None,
    /// A future that resolves when first polled.
    Ready,
    /// A future that resolves once the probe's `stop_release` opens.
    Gated,
    /// A future that waits, for up to 2 s, until every session of the worker has finished.
    WaitFinished,
    /// A future that never resolves and owns a task group with a pending task.
    Forever,
    /// `worker_stopping` itself panics.
    PanicOnCall,
    /// A future that panics when polled.
    PanicOnPoll,
    /// No future; `worker_stopping` drains every attached session and records the result.
    DrainEach,
}

/// The tasks of a `worker_stopping` future. Its `Drop` cancels and reaps them synchronously.
struct StopTasks {
    probe: Arc<Probe>,
    tasks: TaskGroup,
}

impl StopTasks {
    /// A task group with one pending task, whose drop the probe records.
    fn new(probe: &Arc<Probe>) -> Self {
        let mut tasks = TaskGroup::new();
        let dropped = DropEvent {
            probe: Arc::clone(probe),
            event: "stop task dropped",
        };
        tasks
            .spawn_local(move |_| async move {
                let _dropped = dropped;
                std::future::pending::<()>().await;
            })
            .expect("the stop task spawns");
        Self {
            probe: Arc::clone(probe),
            tasks,
        }
    }
}

impl Drop for StopTasks {
    fn drop(&mut self) {
        self.tasks.abort_all();
        let mut reaped = 0;
        while let Some(completion) = self.tasks.try_join_next() {
            completion.discard();
            reaped += 1;
        }
        self.probe.event(format!(
            "stop tasks reaped: {reaped}, empty: {}",
            self.tasks.is_empty()
        ));
    }
}

/// Records `event` when dropped.
struct DropEvent {
    probe: Arc<Probe>,
    event: &'static str,
}

impl Drop for DropEvent {
    fn drop(&mut self) {
        self.probe.event(self.event.to_owned());
    }
}

/// The application factory, whose workers' `worker_stopping` follows `stopping`.
fn factory_with(probe: &Arc<Probe>, stopping: StopBehavior) -> OwnedApplicationFactory {
    let probe = Arc::clone(probe);
    Arc::new(move |worker: WorkerContext, index: usize| {
        probe.factory_calls.lock().unwrap().push((
            index,
            std::thread::current().id(),
            worker.owner_thread(),
        ));
        Ok(Rc::new(TestConnections {
            probe: Arc::clone(&probe),
            stopping,
            controls: Rc::default(),
        }) as Rc<dyn OwnedConnectionFactory>)
    })
}

/// One worker's connection factory.
struct TestConnections {
    probe: Arc<Probe>,
    stopping: StopBehavior,
    /// The control of every connection this worker attached.
    controls: Rc<RefCell<Vec<OwnedConnectionControl>>>,
}

impl TestConnections {
    /// How many of this worker's attached sessions have finished, of how many.
    fn finished_sessions(&self) -> String {
        let controls = self.controls.borrow();
        let finished = controls
            .iter()
            .filter(|control| control.is_finished())
            .count();
        format!("{finished} of {} sessions finished", controls.len())
    }
}

impl OwnedConnectionFactory for TestConnections {
    fn attach(&self, connection: OwnedConnectionAttach<'_>) -> Result<OwnedAttached, HttpError> {
        self.probe.attaches.fetch_add(1, Ordering::SeqCst);
        self.probe.seen.lock().unwrap().push(SeenConnection {
            on_worker_thread: connection.worker().is_owner_thread(),
            worker_index: connection.worker_index(),
            id: connection.id(),
            protocol: connection.protocol(),
            secure: connection.info().secure,
            info_peer_identity: connection
                .info()
                .peer_identity
                .as_deref()
                .map(<[u8]>::to_vec),
            context_peer_identity: connection.context().peer_identity().map(<[u8]>::to_vec),
            context_is_info_context: Arc::ptr_eq(connection.context(), &connection.info().context),
        });
        let failure = self.probe.attach_failures.lock().unwrap().pop_front();
        match failure {
            Some(AttachFailure::Error) => {
                return Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"attach refused",
                ));
            }
            Some(AttachFailure::Panic) => panic!("attach panics"),
            Some(AttachFailure::RunPanic) => fault_next_http2_run(),
            None => {}
        }
        let probe = Arc::clone(&self.probe);
        let controls = Rc::clone(&self.controls);
        let make = move |control: OwnedConnectionControl| {
            controls.borrow_mut().push(control.clone());
            TestApp { probe, control }
        };
        match connection.protocol() {
            CodecProtocol::Http2 => connection.serve_http2(make),
            _ => connection.serve_http1(Http1DriverConfig::default(), make),
        }
    }

    fn worker_stopping(&self, abandon: StopAbandon) -> Option<Pin<Box<dyn Future<Output = ()>>>> {
        let probe = Arc::clone(&self.probe);
        probe.event(format!("stopping: {}", self.finished_sessions()));
        match self.stopping {
            StopBehavior::None => None,
            StopBehavior::DrainEach => {
                for control in self.controls.borrow().iter() {
                    let drained = control.drain().map_err(|error| error.code());
                    probe.event(format!("drain: {drained:?}"));
                }
                None
            }
            StopBehavior::PanicOnCall => panic!("worker_stopping panics"),
            StopBehavior::PanicOnPoll => Some(Box::pin(async {
                panic!("the stop future panics");
            })),
            StopBehavior::Ready => Some(Box::pin(async move {
                probe.event("stopping polled".to_owned());
            })),
            StopBehavior::Gated => Some(Box::pin(std::future::poll_fn(move |context| {
                probe.stop_polls.fetch_add(1, Ordering::SeqCst);
                probe.stop_release.poll(context)
            }))),
            StopBehavior::Forever => {
                probe.event(format!("abandoned at creation: {}", abandon.is_abandoned()));
                let tasks = StopTasks::new(&probe);
                Some(Box::pin(std::future::poll_fn(move |_| {
                    tasks.probe.stop_polls.fetch_add(1, Ordering::SeqCst);
                    Poll::Pending
                })))
            }
            StopBehavior::WaitFinished => {
                let controls = Rc::clone(&self.controls);
                Some(Box::pin(async move {
                    let started = Instant::now();
                    let all_finished = || controls.borrow().iter().all(|c| c.is_finished());
                    while !all_finished() && started.elapsed() < Duration::from_secs(2) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    probe.event(format!("all sessions finished: {}", all_finished()));
                }))
            }
        }
    }
}

/// One connection's application.
struct TestApp {
    probe: Arc<Probe>,
    control: OwnedConnectionControl,
}

impl OwnedApplication for TestApp {
    type Exchange = TestExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> TestExchange {
        TestExchange {
            probe: Arc::clone(&self.probe),
            control: self.control.clone(),
            state: ExchangeState::AwaitingRequest,
        }
    }
}

enum ExchangeState {
    AwaitingRequest,
    /// Answers `200 ok`: the head, then the body.
    Ok {
        body: bool,
        _request: HttpStreamSourceOwner,
    },
    /// Never answers.
    Held {
        _request: HttpStreamSourceOwner,
    },
    /// Panics when polled.
    Panic {
        _request: HttpStreamSourceOwner,
    },
    /// Answers `200 ok` once the probe's `release` opens.
    HoldUntilReleased(HttpStreamSourceOwner),
    Done,
}

/// An exchange that answers by its request's path:
/// - `/hard-close` hard-closes its connection and never answers;
/// - `/drain` drains its connection and never answers;
/// - `/panic` panics as its response head is polled;
/// - `/hold` is answered `200 ok` once the probe's `release` opens;
/// - anything else is answered `200 ok`.
struct TestExchange {
    probe: Arc<Probe>,
    control: OwnedConnectionControl,
    state: ExchangeState,
}

/// The `200 ok` response head, with its length.
fn ok_head() -> HttpHeaderEvent {
    let mut message = HttpMessage::new();
    message.set_status_code(200).expect("status");
    message.set_http_version(1, 1);
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"2");
    HttpHeaderEvent::new(message, false).expect("response head")
}

impl OwnedExchange for TestExchange {
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, _eom: bool) {
        self.probe.requests.fetch_add(1, Ordering::SeqCst);
        self.state = match head.request_url().unwrap_or_default() {
            b"/hard-close" => {
                self.control.hard_close();
                ExchangeState::Held { _request: body }
            }
            b"/drain" => {
                let drained = self.control.drain().map_err(|error| error.code());
                self.probe.drains.lock().unwrap().push((
                    drained,
                    self.control.handshake_complete(),
                    self.control.is_finished(),
                ));
                ExchangeState::Held { _request: body }
            }
            b"/panic" => ExchangeState::Panic { _request: body },
            b"/hold" => ExchangeState::HoldUntilReleased(body),
            _ => ExchangeState::Ok {
                body: false,
                _request: body,
            },
        };
    }

    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        if let ExchangeState::HoldUntilReleased(_) = &self.state {
            std::task::ready!(self.probe.release.poll(context));
            let ExchangeState::HoldUntilReleased(request) =
                std::mem::replace(&mut self.state, ExchangeState::Done)
            else {
                unreachable!("the exchange is held");
            };
            self.state = ExchangeState::Ok {
                body: false,
                _request: request,
            };
        }
        match &mut self.state {
            ExchangeState::Ok { body, .. } if !*body => {
                *body = true;
                Poll::Ready(Ok(ok_head()))
            }
            ExchangeState::Held { .. } => Poll::Pending,
            ExchangeState::Panic { .. } => panic!("owned exchange panicked"),
            _ => Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"no response head to send",
            ))),
        }
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.state = ExchangeState::Done;
        Poll::Ready(Ok(HttpBodyEvent::body(
            BufQueue::from_bytes(Bytes::from_static(b"ok"), 2).expect("body"),
            true,
        )))
    }

    fn cancel(&mut self, error: &HttpError, _peer_reset: bool) {
        self.probe.cancels.lock().unwrap().push(error.code());
        self.state = ExchangeState::Done;
    }

    fn response_complete(&mut self) {}

    fn complete(self) {
        self.probe.event("complete".to_owned());
    }
}

/// Plaintext connections run `protocol` on the owned drivers, untimed.
fn owned_config(protocol: CodecProtocol) -> ServerConfig {
    ServerConfig {
        plaintext_protocol: protocol,
        session: ServerSessionConfig {
            stream_read_timeout: Duration::ZERO,
            connection_idle_timeout: Duration::ZERO,
            write_timeout: Duration::ZERO,
            owned_http1: true,
            owned_http2: true,
            ..ServerSessionConfig::default()
        },
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    }
}

fn connect(address: SocketAddr) -> TcpStream {
    let stream = TcpStream::connect_timeout(&address, TIMEOUT).expect("client connects");
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const H2_DATA: u8 = 0x0;
const H2_HEADERS: u8 = 0x1;
const H2_SETTINGS: u8 = 0x4;
const H2_PING: u8 = 0x6;
const H2_GOAWAY: u8 = 0x7;
const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
/// HPACK static table index 8, `:status: 200`.
const STATUS_200: u8 = 0x88;

struct H2Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).unwrap().to_be_bytes();
    let mut frame = length[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Writes the client preface and an empty SETTINGS frame.
fn start_h2(stream: &mut TcpStream) {
    let mut wire = H2_PREFACE.to_vec();
    wire.extend(h2_frame(H2_SETTINGS, 0, 0, &[]));
    stream.write_all(&wire).unwrap();
}

/// Reads frames, acknowledging SETTINGS and PING, until `done` holds for one, and returns it.
fn read_h2_until(
    stream: &mut TcpStream,
    done: impl Fn(&H2Frame) -> bool,
) -> std::io::Result<H2Frame> {
    loop {
        let mut header = [0_u8; 9];
        stream.read_exact(&mut header)?;
        let length =
            usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload)?;
        let frame = H2Frame {
            kind: header[3],
            flags: header[4],
            stream: u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
            payload,
        };
        if done(&frame) {
            return Ok(frame);
        }
        if frame.kind == H2_SETTINGS && frame.flags & ACK == 0 {
            stream.write_all(&h2_frame(H2_SETTINGS, ACK, 0, &[]))?;
        } else if frame.kind == H2_PING && frame.flags & ACK == 0 {
            stream.write_all(&h2_frame(H2_PING, ACK, 0, &frame.payload))?;
        }
    }
}

/// Sends a GET for `path` on stream `id`.
fn h2_send_get(stream: &mut TcpStream, id: u32, path: &[u8]) {
    // :method GET, :scheme http, then :path and :authority without indexing.
    let length = u8::try_from(path.len()).expect("short path");
    let mut block = vec![0x82, 0x86, 0x04, length];
    block.extend_from_slice(path);
    block.extend_from_slice(&[0x01, 9]);
    block.extend_from_slice(b"localhost");
    stream
        .write_all(&h2_frame(H2_HEADERS, END_STREAM | END_HEADERS, id, &block))
        .unwrap();
}

/// Sends a GET for `path` on stream `id` and reads its response: whether its status is 200, and
/// its body.
fn h2_get(stream: &mut TcpStream, id: u32, path: &[u8]) -> (bool, Vec<u8>) {
    h2_send_get(stream, id, path);
    let mut status_200 = None;
    let mut body = Vec::new();
    loop {
        let frame = read_h2_until(stream, |frame| frame.stream == id)
            .expect("the response arrives on its stream");
        if frame.kind == H2_HEADERS && status_200.is_none() {
            status_200 = Some(frame.payload.first() == Some(&STATUS_200));
        }
        if frame.kind == H2_DATA {
            body.extend_from_slice(&frame.payload);
        }
        if frame.flags & END_STREAM != 0 {
            return (status_200 == Some(true), body);
        }
    }
}

/// Writes `request` and reads the response until the server closes the connection.
fn h1_exchange(address: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut client = connect(address);
    client.write_all(request).expect("request written");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .expect("the response ends with the connection");
    response
}

/// Writes `request` and reads until the server closes the connection or resets it.
fn h1_exchange_until_closed(address: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut client = connect(address);
    client.write_all(request).expect("request written");
    read_until_closed(&mut client)
}

/// Reads until the server closes the connection or resets it, and returns the bytes read. A
/// read timeout fails the test: the connection stayed open.
fn read_until_closed(client: &mut TcpStream) -> Vec<u8> {
    let mut received = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        match client.read(&mut chunk) {
            Ok(0) => return received,
            Ok(read) => received.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == ErrorKind::ConnectionReset => return received,
            Err(error) => panic!(
                "the connection did not close: {error:?} after {:?}",
                String::from_utf8_lossy(&received)
            ),
        }
    }
}

/// The sum of a worker's contained-panic counters, and its connection-scope counter.
fn contained_panics(server: &HttpServer) -> (u64, u64) {
    let stats = server.control().workers()[0].poll_stats();
    (
        stats.contained_exchange_panics
            + stats.contained_connection_panics
            + stats.contained_driver_panics,
        stats.contained_connection_panics,
    )
}

/// An owned application serves an HTTP/2 request: its connection factory attaches the
/// connection, and its exchange's response reaches the client.
#[test]
fn owned_app_serves_h2_request() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(owned_config(CodecProtocol::Http2), factory(&probe), None)
        .expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    start_h2(&mut client);
    assert_eq!(h2_get(&mut client, 1, b"/"), (true, b"ok".to_vec()));
    assert_eq!(probe.attaches(), 1, "one connection was attached");
    drop(client);
    assert!(server.force_stop().expect("server stops"));
}

/// An owned application serves an HTTP/1.1 request through `serve_http1`.
#[test]
fn owned_app_serves_h1_request() {
    let probe = Arc::new(Probe::default());
    let server =
        HttpServer::start_owned(owned_config(CodecProtocol::Http11), factory(&probe), None)
            .expect("server starts");
    let response = h1_exchange(
        server.address().expect("one address"),
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(
        response.starts_with(b"HTTP/1.1 200 ") && response.ends_with(b"\r\n\r\nok"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(probe.attaches(), 1, "one connection was attached");
    assert!(server.force_stop().expect("server stops"));
}

/// A server certificate for `localhost`, a client CA and a client certificate it issued.
struct MutualTls {
    server: TlsServerOptions,
    server_certificate: Vec<u8>,
    client_certificate: Vec<u8>,
    client_key: Vec<u8>,
}

fn mutual_tls() -> MutualTls {
    let server = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "client CA");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client = rcgen::CertificateParams::new(vec!["client".to_owned()])
        .unwrap()
        .signed_by(&client_key, &rcgen::Issuer::new(ca_params, &ca_key))
        .unwrap();
    MutualTls {
        server: TlsServerOptions::new_with_client_auth(
            vec![server.cert.der().to_vec().into_boxed_slice()],
            server
                .signing_key
                .serialized_der()
                .to_vec()
                .into_boxed_slice(),
            vec![b"http/1.1".to_vec().into_boxed_slice()],
            vec![ca.der().to_vec().into_boxed_slice()],
        )
        .unwrap(),
        server_certificate: server.cert.der().to_vec(),
        client_certificate: client.der().to_vec(),
        client_key: client_key.serialized_der().to_vec(),
    }
}

/// The application sees its connection's worker, identity, protocol and metadata, including the
/// DER of the client's TLS certificate, in `attach`, on the worker's thread.
#[test]
fn owned_app_sees_context_and_peer_der() {
    let tls = mutual_tls();
    let probe = Arc::new(Probe::default());
    let config = ServerConfig {
        tls_credentials: Some(Arc::new(StaticTlsCredentials::new(tls.server))),
        ..owned_config(CodecProtocol::Http11)
    };
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(tls.server_certificate))
        .unwrap();
    let mut client_config =
        ClientConfig::builder_with_provider(Arc::new(builtin_crypto_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![CertificateDer::from(tls.client_certificate.clone())],
                PrivateKeyDer::try_from(tls.client_key).unwrap(),
            )
            .unwrap();
    client_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connection = ClientConnection::new(
        Arc::new(client_config),
        ServerName::try_from("localhost".to_owned()).unwrap(),
    )
    .unwrap();
    let mut client = StreamOwned::new(connection, connect(server.address().expect("one address")));
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request written");
    let mut response = Vec::new();
    let mut chunk = [0; 1024];
    // The server may close without close_notify after the response.
    while let Ok(read @ 1..) = client.read(&mut chunk) {
        response.extend_from_slice(&chunk[..read]);
    }
    assert!(
        response.ends_with(b"\r\n\r\nok"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(
        *probe.seen.lock().unwrap(),
        [SeenConnection {
            on_worker_thread: true,
            worker_index: 0,
            id: ConnectionId {
                worker: 0,
                serial: 1,
            },
            protocol: CodecProtocol::Http11,
            secure: true,
            info_peer_identity: Some(tls.client_certificate.clone()),
            context_peer_identity: Some(tls.client_certificate),
            context_is_info_context: true,
        }]
    );
    assert!(server.force_stop().expect("server stops"));
}

/// The factory runs once on each worker, on that worker's thread, whatever the number of
/// connections, and `HttpServerControl::workers()` describes each worker read-only.
#[test]
fn owned_app_factory_runs_once_per_worker() {
    const WORKERS: usize = 3;
    let probe = Arc::new(Probe::default());
    let config = ServerConfig {
        num_io_threads: WORKERS,
        ..owned_config(CodecProtocol::Http11)
    };
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");
    for _ in 0..2 * WORKERS {
        let response = h1_exchange(
            server.address().expect("one address"),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert!(response.ends_with(b"\r\n\r\nok"));
    }
    assert_eq!(probe.attaches(), 2 * WORKERS);

    let workers = server.control().workers();
    assert_eq!(
        workers.iter().map(WorkerView::index).collect::<Vec<_>>(),
        (0..WORKERS).collect::<Vec<_>>()
    );
    let mut calls = probe.factory_calls.lock().unwrap().clone();
    calls.sort_by_key(|(index, _, _)| *index);
    assert_eq!(
        calls,
        workers
            .iter()
            .map(|worker| (worker.index(), worker.owner_thread(), worker.owner_thread()))
            .collect::<Vec<_>>(),
        "one call per worker, on the worker's own thread"
    );
    assert!(
        workers
            .iter()
            .all(|worker| worker.poll_stats().driver_polls > 0),
        "each worker's poll counters"
    );
    let mut closed: Vec<_> = workers.iter().map(WorkerView::closed).collect();
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        closed
            .iter_mut()
            .all(|closed| std::pin::Pin::new(closed).poll(&mut context).is_pending()),
        "the workers run until the server stops"
    );
    assert!(server.force_stop().expect("server stops"));
    assert!(
        closed
            .iter_mut()
            .all(|closed| std::pin::Pin::new(closed).poll(&mut context).is_ready()),
        "the workers have closed"
    );
    assert_eq!(probe.factory_calls.lock().unwrap().len(), WORKERS);
}

/// A server filter, which `start_owned` must reject before any worker builds it.
struct UnusedFilters;

impl ServerFilterFactory for UnusedFilters {
    fn make_filter_factory(&self) -> Rc<dyn FilterFactory> {
        unreachable!("start_owned rejects filter factories")
    }
}

/// Counts the connections it sees close.
#[derive(Default)]
struct ConnectionCounts {
    accepted: AtomicUsize,
    closed: AtomicUsize,
}

impl ConnectionCounts {
    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn closed(&self) -> usize {
        self.closed.load(Ordering::SeqCst)
    }
}

impl ConnectionLifecycleObserver for ConnectionCounts {
    fn on_socket_accepted(&self, _id: ConnectionId) {
        self.accepted.fetch_add(1, Ordering::SeqCst);
    }

    fn on_socket_closed(&self, _id: ConnectionId, _outcome: &ConnectionOutcome<'_>) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
}

/// Counts worker thread stops.
#[derive(Default)]
struct ThreadStops(AtomicUsize);

impl ServerObserver for ThreadStops {
    fn on_thread_stop(&self, _worker_index: usize) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Whether `worker` has closed.
fn is_closed(worker: &WorkerView) -> bool {
    let mut closed = std::pin::pin!(worker.closed());
    closed
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_ready()
}

/// `start_owned` requires both owned modes and rejects the classic per-session options, the
/// filters, the statistics recorder and the session lifecycle observer, naming each. The
/// server-level `lifecycle_observer` is allowed.
#[test]
fn owned_app_rejects_classic_options() {
    let probe = Arc::new(Probe::default());
    let rejected = |option: &str, configure: &dyn Fn(&mut ServerConfig)| {
        let mut config = owned_config(CodecProtocol::Http11);
        configure(&mut config);
        match HttpServer::start_owned(config, factory(&probe), None) {
            Err(HttpServerError::InvalidConfig(message)) => assert!(
                message.contains("start_owned") && message.contains(option),
                "{option}: {message}"
            ),
            Err(error) => panic!("{option}: {error}"),
            Ok(_) => panic!("{option}: the server started"),
        }
    };
    rejected("owned_http1", &|config| config.session.owned_http1 = false);
    rejected("owned_http2", &|config| config.session.owned_http2 = false);
    rejected("filter_factories", &|config| {
        config.filter_factories = vec![Arc::new(UnusedFilters)];
    });
    rejected("stats_factory", &|config| {
        config.session.stats_factory = Some(Arc::new(
            |_: &ConnectionInfo| -> Rc<dyn Http2SessionStats> { unreachable!("rejected") },
        ));
    });
    rejected("lifecycle_observer_factory", &|config| {
        config.session.lifecycle_observer_factory =
            Some(Arc::new(|| -> Rc<dyn LifecycleObserver> {
                unreachable!("rejected")
            }));
    });
    assert!(
        probe.factory_calls.lock().unwrap().is_empty(),
        "no worker was started"
    );

    let connections = Arc::new(ConnectionCounts::default());
    let config = ServerConfig {
        lifecycle_observer: Some(Arc::clone(&connections) as Arc<dyn ConnectionLifecycleObserver>),
        ..owned_config(CodecProtocol::Http11)
    };
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");
    let response = h1_exchange(
        server.address().expect("one address"),
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert!(response.ends_with(b"\r\n\r\nok"));
    assert!(server.force_stop().expect("server stops"));
    assert_eq!(connections.closed(), 1, "the observer saw the connection");
}

/// `hard_close` cancels the connection: the exchange sees `CORO_CANCELLED` and nothing is
/// written. With `non_abortive_close` the client then reads EOF, not a reset.
#[test]
fn owned_app_hard_close_writes_no_response() {
    let probe = Arc::new(Probe::default());
    let mut config = owned_config(CodecProtocol::Http11);
    config.session.non_abortive_close = true;
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");
    let response = h1_exchange(
        server.address().expect("one address"),
        b"GET /hard-close HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    assert!(
        response.is_empty(),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(server.force_stop().expect("server stops"));
    assert_eq!(
        *probe.cancels.lock().unwrap(),
        [HttpErrorCode::CORO_CANCELLED]
    );
}

/// The application drains its own HTTP/2 connection through its control: the client gets a
/// GOAWAY while the drained stream stays open, and a forced stop then cancels the exchange.
#[test]
fn owned_app_drain_sends_goaway_and_force_cancels_exchanges() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(owned_config(CodecProtocol::Http2), factory(&probe), None)
        .expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    start_h2(&mut client);
    h2_send_get(&mut client, 1, b"/drain");
    let goaway = read_h2_until(&mut client, |frame| {
        frame.kind == H2_GOAWAY || frame.stream == 1
    })
    .expect("the server sends GOAWAY");
    assert_eq!(goaway.kind, H2_GOAWAY, "no response on the drained stream");
    assert_eq!(
        *probe.drains.lock().unwrap(),
        [(Ok(true), true, false)],
        "the drain changed the session, after its handshake, while it ran"
    );
    assert!(
        probe.cancels.lock().unwrap().is_empty(),
        "the drain does not cancel the open exchange"
    );
    assert!(server.force_stop().expect("server stops"));
    assert_eq!(
        probe.cancels.lock().unwrap().len(),
        1,
        "the forced stop cancels the exchange"
    );
}

/// A panic in an owned application's exchange closes only its connection, and the worker then
/// serves another connection. The HTTP/1.1 driver resumes the panic as its run completes; the
/// server's run wrapper contains it and counts it in `contained_connection_panics`. The
/// HTTP/2 driver contains it itself. Either way the client sees the connection close or reset
/// with no response, not a stalled connection.
#[test]
fn owned_app_panic_is_contained_per_connection() {
    const OK_REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let probe = Arc::new(Probe::default());

    let server =
        HttpServer::start_owned(owned_config(CodecProtocol::Http11), factory(&probe), None)
            .expect("server starts");
    let address = server.address().expect("one address");
    let response =
        h1_exchange_until_closed(address, b"GET /panic HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert!(
        response.is_empty(),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(h1_exchange(address, OK_REQUEST).ends_with(b"\r\n\r\nok"));
    let (all, connection) = contained_panics(&server);
    assert_eq!(
        connection, 1,
        "the run's resumed panic is contained per connection"
    );
    assert_eq!(
        all, 2,
        "the driver's turn and the run wrapper each count it once"
    );
    assert!(
        server
            .force_stop()
            .expect("the panic is not a server failure")
    );

    let server = HttpServer::start_owned(owned_config(CodecProtocol::Http2), factory(&probe), None)
        .expect("server starts");
    let address = server.address().expect("one address");
    let mut client = connect(address);
    start_h2(&mut client);
    h2_send_get(&mut client, 1, b"/panic");
    let end = read_h2_until(&mut client, |frame| frame.stream == 1)
        .map(|frame| (frame.kind, frame.flags))
        .expect_err("the panicked stream gets no frame");
    // The reset can surface on the write that acknowledges the server's SETTINGS.
    assert!(
        matches!(
            end.kind(),
            ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::UnexpectedEof
        ),
        "the panic closes the connection before the read timeout: {end:?}"
    );
    let mut next = connect(address);
    start_h2(&mut next);
    assert_eq!(h2_get(&mut next, 1, b"/"), (true, b"ok".to_vec()));
    assert_eq!(
        contained_panics(&server).0,
        1,
        "the driver counts the panic once"
    );
    assert!(
        server
            .force_stop()
            .expect("the panic is not a server failure")
    );
}

/// A hosted HTTP/2 run that unwinds out of a poll fails only its connection. Its connection's
/// control then reports the session finished, and a drain no longer changes it, as
/// `worker_stopping` sees once the worker has no session left. The worker is force-stopped,
/// which drains no session itself.
#[test]
fn owned_app_h2_run_panic_finishes_its_control() {
    let probe = Arc::new(Probe::default());
    probe
        .attach_failures
        .lock()
        .unwrap()
        .push_back(AttachFailure::RunPanic);
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http2),
        factory_with(&probe, StopBehavior::DrainEach),
        None,
    )
    .expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    read_until_closed(&mut client);
    wait_until("the run's panic is contained per connection", || {
        contained_panics(&server) == (1, 1)
    });
    assert!(server.control().force_stop());
    server.join().expect("the panic is not a server failure");
    assert_eq!(
        probe.events(),
        ["stopping: 1 of 1 sessions finished", "drain: Ok(false)"]
    );
}

/// `worker_stopping` runs on a drained worker once its last session has finished, and the
/// worker waits for its future before it stops.
#[test]
fn worker_stopping_runs_after_sessions_join() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::Ready),
        None,
    )
    .expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    client
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || probe.requests() == 1);
    assert!(server.drain());
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        probe.events().is_empty(),
        "a running session defers worker_stopping: {:?}",
        probe.events()
    );
    probe.release.open();
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .expect("the drained connection closes after its response");
    assert!(
        response.ends_with(b"\r\n\r\nok"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(server.wait_for_stop().expect("server stops"));
    assert_eq!(
        probe.events(),
        [
            "complete",
            "stopping: 1 of 1 sessions finished",
            "stopping polled"
        ]
    );
}

/// `HttpServerControl::force_stop` aborts and joins every session before the worker calls
/// `worker_stopping`, then waits for its pending future, and exits once that future resolves.
#[test]
fn force_stop_aborts_sessions_before_worker_stopping() {
    let probe = Arc::new(Probe::default());
    let connections = Arc::new(ConnectionCounts::default());
    let config = ServerConfig {
        lifecycle_observer: Some(Arc::clone(&connections) as Arc<dyn ConnectionLifecycleObserver>),
        ..owned_config(CodecProtocol::Http11)
    };
    let server = HttpServer::start_owned(config, factory_with(&probe, StopBehavior::Gated), None)
        .expect("server starts");
    let worker = server.control().workers().remove(0);
    let mut client = connect(server.address().expect("one address"));
    client
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || probe.requests() == 1);
    let stopping = Instant::now();
    assert!(server.control().force_stop());
    wait_until("the stop future is polled", || probe.stop_polls() > 0);
    assert!(
        stopping.elapsed() < Duration::from_secs(1),
        "sessions were joined in {:?}",
        stopping.elapsed()
    );
    assert_eq!(
        (connections.accepted(), connections.closed()),
        (1, 1),
        "every accepted connection closed before the stop future ran"
    );
    assert_eq!(
        *probe.cancels.lock().unwrap(),
        [HttpErrorCode::CORO_CANCELLED]
    );
    std::thread::sleep(Duration::from_millis(50));
    assert!(!is_closed(&worker), "the worker waits for its stop future");
    probe.stop_release.open();
    server.join().expect("the workers join");
    assert!(is_closed(&worker));
}

/// A forced stop drops a session's started run; the server cancels it and finishes it in the
/// worker's cleanup, so the exchange sees `CORO_CANCELLED` and the connection's control
/// reports the session finished before the worker stops.
#[test]
fn abandoned_run_is_finished_by_cleanup() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::WaitFinished),
        None,
    )
    .expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    client
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the request is delivered", || probe.requests() == 1);
    assert!(server.control().force_stop());
    server.join().expect("the workers join");
    assert_eq!(
        *probe.cancels.lock().unwrap(),
        [HttpErrorCode::CORO_CANCELLED]
    );
    assert_eq!(
        probe.events().last().map(String::as_str),
        Some("all sessions finished: true")
    );
}

/// `serve_http1` takes the connection deadlines from `ServerSessionConfig::owned_deadlines`,
/// even with the default `Http1DriverConfig`: a request head that has not ended by the
/// handshake deadline closes the connection then, with no response.
#[test]
fn serve_http1_applies_session_deadlines_with_default_driver_config() {
    const HANDSHAKE: Duration = Duration::from_millis(200);
    let probe = Arc::new(Probe::default());
    let mut config = owned_config(CodecProtocol::Http11);
    config.session.owned_deadlines =
        OwnedConnectionDeadlines::new(HANDSHAKE, Duration::ZERO, DeadlineExpiry::HardClose);
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let started = Instant::now();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .expect("partial head written");
    let mut received = Vec::new();
    let read = client.read_to_end(&mut received);
    let elapsed = started.elapsed();
    assert!(
        match &read {
            Ok(_) => true,
            Err(error) => error.kind() == ErrorKind::ConnectionReset,
        },
        "the connection closes by the handshake deadline: {read:?} after {elapsed:?}"
    );
    assert!(
        received.is_empty(),
        "no response: {:?}",
        String::from_utf8_lossy(&received)
    );
    assert!(
        elapsed >= HANDSHAKE,
        "the close came before the deadline: {elapsed:?}"
    );
    assert_eq!(probe.attaches(), 1);
    assert!(server.force_stop().expect("server stops"));
}

/// `abandon_stopping` makes the worker drop a `worker_stopping` future that never resolves, on
/// the worker thread; the future's `Drop` cancels and reaps its tasks, and `join` returns with
/// the worker thread stopped.
#[test]
fn abandon_drops_pending_worker_stopping_and_join_returns() {
    let probe = Arc::new(Probe::default());
    let stops = Arc::new(ThreadStops::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::Forever),
        Some(Arc::clone(&stops) as Arc<dyn ServerObserver>),
    )
    .expect("server starts");
    let control = server.control();
    assert!(server.drain());
    wait_until("the stop future is polled", || probe.stop_polls() > 0);
    let abandoning = Instant::now();
    control.abandon_stopping();
    control.abandon_stopping();
    server.join().expect("the workers join");
    assert!(
        abandoning.elapsed() < Duration::from_secs(1),
        "joined in {:?}",
        abandoning.elapsed()
    );
    assert_eq!(
        probe.events(),
        [
            "stopping: 0 of 0 sessions finished",
            "abandoned at creation: false",
            "stop task dropped",
            "stop tasks reaped: 1, empty: true",
        ]
    );
    assert_eq!(
        stops.0.load(Ordering::SeqCst),
        1,
        "the worker thread stopped"
    );
}

/// Dropping `HttpServer` while a `worker_stopping` future is pending returns promptly: the drop
/// abandons graceful worker stop, so the worker drops the future and the join completes.
#[test]
fn drop_with_pending_worker_stopping_returns_within_bound() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::Forever),
        None,
    )
    .expect("server starts");
    let control = server.control();
    assert!(server.drain());
    wait_until("the stop future is polled", || probe.stop_polls() > 0);
    // Bounds a drop that would otherwise wait for the stop future forever.
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        control.abandon_stopping();
    });
    let dropping = Instant::now();
    drop(server);
    let elapsed = dropping.elapsed();
    watchdog.join().expect("the watchdog finishes");
    assert!(
        elapsed < Duration::from_secs(1),
        "the drop returned in {elapsed:?}"
    );
    assert_eq!(
        probe.events().last().map(String::as_str),
        Some("stop tasks reaped: 1, empty: true")
    );
}

/// `HttpServer::force_stop()` returns promptly while a `worker_stopping` future is pending
/// forever, and that future's tasks are cancelled and reaped.
#[test]
fn owner_force_stop_returns_with_pending_worker_stopping() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::Forever),
        None,
    )
    .expect("server starts");
    let control = server.control();
    assert!(server.drain());
    wait_until("the stop future is polled", || probe.stop_polls() > 0);
    // Bounds a stop that would otherwise wait for the stop future forever.
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        control.abandon_stopping();
    });
    let stopping = Instant::now();
    assert!(server.force_stop().expect("the server stops"));
    let elapsed = stopping.elapsed();
    watchdog.join().expect("the watchdog finishes");
    assert!(
        elapsed < Duration::from_secs(1),
        "force_stop returned in {elapsed:?}"
    );
    assert_eq!(
        probe.events()[2..],
        ["stop task dropped", "stop tasks reaped: 1, empty: true"]
    );
}

/// A connection that a draining worker dequeues is drained once `attach` has returned its
/// session: the HTTP/1.1 session starts drained with no exchange in progress, so it closes the
/// connection without reading the request queued before the drain.
#[test]
fn admit_session_runs_after_attach_while_draining() {
    let probe = Arc::new(Probe::default());
    let hold = DequeueHoldForTest::new();
    let mut config = owned_config(CodecProtocol::Http11);
    config.socket.accept_distribution = AcceptDistribution::PerWorkerReusePort;
    config.socket.reuse_port = true;
    config.dequeue_hold_for_test = Some(Arc::clone(&hold));
    let server = HttpServer::start_owned(config, factory(&probe), None).expect("server starts");
    let mut client = connect(server.address().expect("one address"));
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .expect("request written");
    wait_until("the socket is accepted", || {
        server.control().accept_stats()[0].accepted == 1
    });
    assert!(server.drain());
    hold.release();
    let mut response = Vec::new();
    let read = client.read_to_end(&mut response);
    assert!(
        read.is_ok(),
        "the drained connection closes: {read:?} after {:?}",
        String::from_utf8_lossy(&response)
    );
    assert!(
        response.is_empty(),
        "{:?}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(
        probe.attaches(),
        1,
        "the connection dequeued during the drain was attached"
    );
    assert!(server.wait_for_stop().expect("server stops"));
}

/// A `worker_stopping` future created after `abandon_stopping` has fired is dropped without
/// being polled; its `Drop` still cancels and reaps its tasks, and `join` returns.
#[test]
fn abandon_before_first_poll_drops_unpolled_stop_future() {
    let probe = Arc::new(Probe::default());
    let server = HttpServer::start_owned(
        owned_config(CodecProtocol::Http11),
        factory_with(&probe, StopBehavior::Forever),
        None,
    )
    .expect("server starts");
    server.control().abandon_stopping();
    let stopping = Instant::now();
    assert!(server.drain());
    server.join().expect("the workers join");
    assert!(
        stopping.elapsed() < Duration::from_secs(1),
        "joined in {:?}",
        stopping.elapsed()
    );
    assert_eq!(probe.stop_polls(), 0, "the stop future is dropped unpolled");
    assert_eq!(
        probe.events(),
        [
            "stopping: 0 of 0 sessions finished",
            "abandoned at creation: true",
            "stop task dropped",
            "stop tasks reaped: 1, empty: true",
        ]
    );
}

/// The application factory runs under the server's startup checks: an error or a panic fails
/// `start_owned` with `WorkerStartup`.
#[test]
fn owned_app_factory_failure_fails_start() {
    let failing: OwnedApplicationFactory = Arc::new(|_, _| {
        Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"no application",
        ))
    });
    match HttpServer::start_owned(owned_config(CodecProtocol::Http11), failing, None) {
        Err(HttpServerError::WorkerStartup(message)) => {
            assert!(message.contains("no application"), "{message}")
        }
        Err(error) => panic!("{error}"),
        Ok(_) => panic!("the server started"),
    }
    let panicking: OwnedApplicationFactory = Arc::new(|_, _| panic!("the factory panics"));
    match HttpServer::start_owned(owned_config(CodecProtocol::Http11), panicking, None) {
        Err(HttpServerError::WorkerStartup(message)) => {
            assert_eq!(message, "owned application factory panicked")
        }
        Err(error) => panic!("{error}"),
        Ok(_) => panic!("the server started"),
    }
}

/// A failed `attach` closes only its connection and the worker keeps serving: an error records
/// no failure, and a panic is recorded as the worker's callback failure, which the stop reports.
#[test]
fn owned_app_attach_failure_closes_only_that_connection() {
    for failure in [AttachFailure::Error, AttachFailure::Panic] {
        let probe = Arc::new(Probe::default());
        probe.attach_failures.lock().unwrap().push_back(failure);
        let server =
            HttpServer::start_owned(owned_config(CodecProtocol::Http11), factory(&probe), None)
                .expect("server starts");
        let address = server.address().expect("one address");
        let mut refused = connect(address);
        refused
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .expect("request written");
        let response = read_until_closed(&mut refused);
        assert!(
            response.is_empty(),
            "{:?}",
            String::from_utf8_lossy(&response)
        );
        let served = h1_exchange(
            address,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        assert!(
            served.ends_with(b"\r\n\r\nok"),
            "{:?}",
            String::from_utf8_lossy(&served)
        );
        assert_eq!(probe.attaches(), 2);
        match (failure, server.force_stop()) {
            (AttachFailure::Error, Ok(true)) => {}
            (
                AttachFailure::Panic,
                Err(HttpServerError::CallbackPanic {
                    worker_index: 0,
                    stage: ServerCallbackStage::HandlerFactory,
                }),
            ) => {}
            (_, result) => panic!("unexpected stop result: {result:?}"),
        }
    }
}

/// A panic in `worker_stopping` or in its future is recorded as the worker's callback failure,
/// and the worker still stops.
#[test]
fn worker_stopping_panic_is_recorded_and_the_worker_stops() {
    for stopping in [StopBehavior::PanicOnCall, StopBehavior::PanicOnPoll] {
        let probe = Arc::new(Probe::default());
        let server = HttpServer::start_owned(
            owned_config(CodecProtocol::Http11),
            factory_with(&probe, stopping),
            None,
        )
        .expect("server starts");
        let worker = server.control().workers().remove(0);
        assert!(server.drain());
        let started = Instant::now();
        while !is_closed(&worker) && started.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(1));
        }
        if !is_closed(&worker) {
            // Joining a worker that never stops would hang the test.
            std::mem::forget(server);
            panic!("the worker did not stop after the panic");
        }
        match server.wait_for_stop() {
            Err(HttpServerError::CallbackPanic {
                worker_index: 0,
                stage: ServerCallbackStage::HandlerFactory,
            }) => {}
            result => panic!("unexpected stop result: {result:?}"),
        }
    }
}
