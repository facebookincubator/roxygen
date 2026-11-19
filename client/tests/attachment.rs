/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::OriginSecurity;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_filter::HttpSourceFilterHooks;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerState;
use roxygen_server::scoped_server::ScopedHttpServer;
use roxygen_server::server::ServerConfig;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::established::adopt_local_with_completion;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::DuplexStream;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;

use crate::HttpClient;
use crate::RequestSpec;
use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::SessionRequestCancellation;
use crate::connector::completion_waiters_for_test;
use crate::connector::head_wait_probe;
use crate::connector::session_completion_for_test;
use crate::connector::wait_session_completion;

const TIMEOUT: Duration = Duration::from_secs(10);

fn limits() -> ByteStreamLimits {
    ByteStreamLimits::new(64 * 1024, 64 * 1024, 8).expect("bounded transport")
}

fn endpoint(address: std::net::SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}

fn wait<Work: Future>(future: Work) -> Work::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("application runtime")
        .block_on(async {
            tokio::time::timeout(TIMEOUT, future)
                .await
                .expect("bounded attachment completion")
        })
}

fn run_on_worker<Output, Factory, Work>(factory: Factory) -> Output
where
    Output: Send + 'static,
    Factory: FnOnce(WorkerContext) -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
{
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let result = wait(worker.call_local(factory).expect("initializer admitted"))
        .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    result
}

struct EchoHandler;

impl HttpHandler for EchoHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let headers = request.read_header_event().await?;
            let mut body = BufQueue::new(256 * 1024);
            if !headers.eom() {
                loop {
                    let event = request.read_body_event(1024).await?;
                    if let Some(data) = event.as_body() {
                        body.append(&mut data.clone_shared())
                            .expect("bounded upload");
                    }
                    if event.eom() {
                        break;
                    }
                }
            }
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body)).expect("valid response"),
            )))
        })
    }
}

struct OrderedCallbacks(Rc<RefCell<Vec<&'static str>>>);

impl LifecycleObserver for OrderedCallbacks {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::TransactionAttached => self.0.borrow_mut().push("attached"),
            LifecycleEvent::TransactionDetached { .. } => self.0.borrow_mut().push("detached"),
            _ => {}
        }
    }
}

impl Http2SessionStats for OrderedCallbacks {
    fn record_transaction_opened(&self) {
        self.0.borrow_mut().push("opened");
    }

    fn record_transaction_closed(&self) {
        self.0.borrow_mut().push("closed");
    }
}

fn start_server(protocol: CodecProtocol) -> ScopedHttpServer {
    ScopedHttpServer::start(
        ServerConfig {
            plaintext_protocol: protocol,
            shutdown_on_signals: Vec::new(),
            ..ServerConfig::default()
        },
        Arc::new(|_| Rc::new(EchoHandler)),
        None,
    )
    .expect("native peer starts")
}

#[test]
fn client_attachment() {
    fn transferable<Value: Send + Sync>() {}
    transferable::<ConnectionContext>();
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let server = start_server(protocol);
        let address = server.address().expect("peer address");
        run_on_worker(move |context| async move {
            let stream = tokio::net::TcpStream::connect(address)
                .await
                .expect("application connector");
            let local = endpoint(stream.local_addr().expect("local address"));
            let peer = endpoint(address);
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    stream,
                    local.clone(),
                    peer.clone(),
                    Some(PeerIdentity::new(b"connector-identity")),
                ),
                ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let callbacks = Rc::new(RefCell::new(Vec::new()));
            let mut options = ClientAttachOptions::new(protocol, TransportSecurity::Plaintext);
            options.upstream_origin = Some(UpstreamOrigin::new(
                "logical-origin.example:443",
                OriginSecurity::Https,
            ));
            options.session.lifecycle_observer =
                Some(Rc::new(OrderedCallbacks(Rc::clone(&callbacks))));
            options.session.session_stats = Some(Rc::new(OrderedCallbacks(Rc::clone(&callbacks))));
            let session = ClientSession::attach_transport(&context, transport, options)
                .expect("client attached");
            let metadata = Arc::clone(session.connection_context());
            assert_eq!(metadata.local_endpoint(), &local);
            assert_eq!(metadata.peer_endpoint(), &peer);
            assert_eq!(metadata.security(), TransportSecurity::Plaintext);
            assert_eq!(
                metadata.peer_identity(),
                Some(b"connector-identity".as_slice())
            );
            assert_eq!(
                metadata
                    .upstream_origin()
                    .expect("explicit origin")
                    .security(),
                OriginSecurity::Https
            );
            let payload = vec![0x71; 128 * 1024];
            let url = Url::parse(
                "http://header-authority.example/upload",
                false,
                UrlMode::StrictCompat,
            );
            let mut response =
                HttpClient::request(&session, None, RequestSpec::post(url, &payload))
                    .await
                    .expect("exchange succeeds");
            assert_eq!(response.headers.status_code().expect("status"), 200);
            assert_eq!(
                response
                    .body
                    .coalesce(payload.len())
                    .expect("bounded response"),
                payload.as_slice()
            );
            assert_eq!(
                session.connection_context(),
                &metadata,
                "request authority cannot rewrite connection context"
            );
            session.close_when_idle().expect("graceful drain");
            session.wait_closed().await.expect("session tasks finish");
            assert_eq!(
                *callbacks.borrow(),
                ["attached", "opened", "closed", "detached"]
            );
        });
    }
}

struct Gate {
    started: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

impl Gate {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        Pin::new(&mut self.release)
            .poll(context)
            .map(|result| result.expect("root releases gate"))
    }
}

#[derive(Clone, Debug, Default)]
struct CloseTrace(Arc<Mutex<VecDeque<String>>>);

impl CloseTrace {
    fn record(&self, event: String) {
        let mut events = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if events.len() == 64 {
            let _ = events.pop_front();
        }
        events.push_back(event);
    }
}

impl LifecycleObserver for CloseTrace {
    fn on_event(&self, event: LifecycleEvent) {
        self.record(format!("lifecycle={event:?}"));
    }
}

struct ProbeIo<Io> {
    io: Io,
    owner: ThreadId,
    drops: Arc<AtomicUsize>,
    gate_enabled: Rc<Cell<bool>>,
    flush: Option<Gate>,
    shutdown: Option<Gate>,
    trace: Option<CloseTrace>,
}

impl<Io> ProbeIo<Io> {
    fn record_close(&self, event: impl FnOnce() -> String) {
        if let Some(trace) = &self.trace {
            trace.record(format!(
                "{}; gate_enabled={}",
                event(),
                self.gate_enabled.get()
            ));
        }
    }
}

impl<Io> Drop for ProbeIo<Io> {
    fn drop(&mut self) {
        self.record_close(|| {
            format!(
                "I/O drop: current={:?}, owner={:?}, flush_gate={}, shutdown_gate={}",
                std::thread::current().id(),
                self.owner,
                self.flush.is_some(),
                self.shutdown.is_some()
            )
        });
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for ProbeIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled = buffer.filled().len();
        let remaining = buffer.remaining();
        let result = Pin::new(&mut self.io).poll_read(context, buffer);
        match &result {
            Poll::Ready(Err(error)) => self.record_close(|| {
                format!(
                    "read error: kind={:?}, os={:?}",
                    error.kind(),
                    error.raw_os_error()
                )
            }),
            Poll::Ready(Ok(())) if remaining != 0 && buffer.filled().len() == filled => {
                self.record_close(|| "read EOF".to_owned());
            }
            _ => {}
        }
        result
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for ProbeIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(context, buffer);
        if let Poll::Ready(Err(error)) = &result {
            self.record_close(|| {
                format!(
                    "write error: kind={:?}, os={:?}",
                    error.kind(),
                    error.raw_os_error()
                )
            });
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.record_close(|| format!("flush poll: gate_present={}", self.flush.is_some()));
        if self.gate_enabled.get()
            && let Some(gate) = self.flush.as_mut()
        {
            if gate.poll(context).is_pending() {
                self.record_close(|| "flush gate pending".to_owned());
                return Poll::Pending;
            }
            let _ = self.flush.take();
        }
        let result = Pin::new(&mut self.io).poll_flush(context);
        self.record_close(|| format!("flush I/O result={result:?}"));
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.record_close(|| format!("shutdown poll: gate_present={}", self.shutdown.is_some()));
        if let Some(gate) = self.shutdown.as_mut() {
            if gate.poll(context).is_pending() {
                self.record_close(|| "shutdown gate pending".to_owned());
                return Poll::Pending;
            }
            let _ = self.shutdown.take();
        }
        let result = Pin::new(&mut self.io).poll_shutdown(context);
        self.record_close(|| format!("shutdown I/O result={result:?}"));
        result
    }
}

#[test]
fn client_attachment_rejects_protocol_and_precancellation() {
    run_on_worker(|context| async move {
        for cancelled in [false, true] {
            let (io, _peer) = tokio::io::duplex(64);
            let drops = Arc::new(AtomicUsize::new(0));
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    ProbeIo {
                        io,
                        owner: std::thread::current().id(),
                        drops: Arc::clone(&drops),
                        gate_enabled: Rc::new(Cell::new(false)),
                        flush: None,
                        shutdown: None,
                        trace: None,
                    },
                    TransportEndpoint::UnnamedUnix,
                    TransportEndpoint::UnixAbstract(Box::from(b"peer".as_slice())),
                    None,
                ),
                limits(),
            )
            .expect("adopted fixture");
            let cancellation = CancellationSource::new();
            let mut options = ClientAttachOptions::new(
                if cancelled {
                    CodecProtocol::Http11
                } else {
                    CodecProtocol::Http3
                },
                TransportSecurity::Unknown,
            );
            if cancelled {
                cancellation.request_cancellation();
                options.cancellation = cancellation.token();
            }
            let error = ClientSession::attach_transport(&context, transport, options)
                .err()
                .expect("attachment rejected");
            assert_eq!(
                error.code(),
                if cancelled {
                    HttpErrorCode::CORO_CANCELLED
                } else {
                    HttpErrorCode::CONNECT_ERROR
                }
            );
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "rejection disposes owned I/O immediately"
            );
        }
    });
}

#[test]
fn client_attachment_cancellation_closes_admission_before_driver_poll() {
    run_on_worker(|context| async move {
        let (io, _peer) = tokio::io::duplex(64);
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                io,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("adopted fixture");
        let cancellation = CancellationSource::new();
        let mut options =
            ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
        options.cancellation = cancellation.token();
        let session =
            ClientSession::attach_transport(&context, transport, options).expect("attached");
        cancellation.request_cancellation();
        assert!(session.reserve_request().is_err());
        assert!(!session.supports_more_transactions());
        drop(session.wait_closed());
        assert_eq!(
            session
                .wait_closed()
                .await
                .expect_err("cancelled session")
                .code(),
            HttpErrorCode::CORO_CANCELLED
        );
        assert!(session.is_finished());
    });
}

struct HeaderProbeState {
    owner: ThreadId,
    polls: Cell<usize>,
    stops: Cell<usize>,
    drops: Cell<usize>,
}

struct HeldRequestHeaders {
    inner: HttpFixedSource,
    release: Option<oneshot::Receiver<()>>,
    on_ready: Option<Box<dyn FnOnce()>>,
    state: Rc<HeaderProbeState>,
}

impl HttpSource for HeldRequestHeaders {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let mut release = self.release.take().expect("one header read");
        let on_ready = self.on_ready.take();
        let state = Rc::clone(&self.state);
        let headers = self.inner.read_header_event();
        Box::pin(async move {
            poll_fn(|context| {
                assert_eq!(std::thread::current().id(), state.owner);
                state.polls.set(state.polls.get() + 1);
                Pin::new(&mut release).poll(context)
            })
            .await
            .expect("header gate released");
            if let Some(on_ready) = on_ready {
                on_ready();
            }
            headers.await
        })
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        assert_eq!(std::thread::current().id(), self.state.owner);
        self.state.stops.set(self.state.stops.get() + 1);
        self.inner.stop_reading(error);
    }
}

impl Drop for HeldRequestHeaders {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.state.owner);
        self.state.drops.set(self.state.drops.get() + 1);
    }
}

fn held_request_headers(
    on_ready: Option<Box<dyn FnOnce()>>,
) -> (HttpSourceHolder, oneshot::Sender<()>, Rc<HeaderProbeState>) {
    let (release, release_rx) = oneshot::channel();
    let state = Rc::new(HeaderProbeState {
        owner: std::thread::current().id(),
        polls: Cell::new(0),
        stops: Cell::new(0),
        drops: Cell::new(0),
    });
    let body = BufQueue::from_bytes(bytes::Bytes::from_static(b"request-body"), 12)
        .expect("bounded request body");
    let source = HeldRequestHeaders {
        inner: HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(body))
            .expect("valid request"),
        release: Some(release_rx),
        on_ready,
        state: Rc::clone(&state),
    };
    (HttpSourceHolder::new(Box::new(source)), release, state)
}

#[derive(Default)]
struct RequestWake(AtomicUsize);

impl Wake for RequestWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn check_header_cancellation(cancel_from_header_poll: bool) {
    run_on_worker(move |context| async move {
        for reserved in [false, true] {
            for attachment_cancellation in [false, true] {
                let (io, _peer) = tokio::io::duplex(4096);
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(
                        io,
                        TransportEndpoint::Unknown,
                        TransportEndpoint::Unknown,
                        None,
                    ),
                    limits(),
                )
                .expect("adopted fixture");
                let cancellation = Rc::new(CancellationSource::new());
                let events = Rc::new(RefCell::new(Vec::new()));
                let mut options =
                    ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
                options.cancellation = cancellation.token();
                options.session.lifecycle_observer =
                    Some(Rc::new(OrderedCallbacks(Rc::clone(&events))));
                let session = ClientSession::attach_transport(&context, transport, options)
                    .expect("attached session");
                let cancel: Rc<dyn Fn()> = if attachment_cancellation {
                    let cancellation = Rc::clone(&cancellation);
                    Rc::new(move || {
                        cancellation.request_cancellation();
                    })
                } else {
                    let session = session.clone();
                    Rc::new(move || session.cancel())
                };
                let on_ready = cancel_from_header_poll.then(|| {
                    let cancel = Rc::clone(&cancel);
                    Box::new(move || cancel()) as Box<dyn FnOnce()>
                });
                let (source, release, probe) = held_request_headers(on_ready);
                let mut request = if reserved {
                    let reservation = session.reserve_request().expect("request reserved");
                    session.send_reserved_request(source, reservation, CancellationToken::never())
                } else {
                    session.send_request(source, CancellationToken::never())
                };
                let wake = Arc::new(RequestWake::default());
                let waker = Waker::from(Arc::clone(&wake));
                assert!(
                    request
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(probe.polls.get(), 1);
                assert_eq!(*events.borrow(), ["attached"]);
                if cancel_from_header_poll {
                    release
                        .send(())
                        .expect("complete headers and cancel while polling");
                } else {
                    cancel();
                    assert!(
                        wake.0.load(Ordering::SeqCst) > 0,
                        "cancellation wakes held request"
                    );
                    assert_eq!(probe.polls.get(), 1, "source is still pending");
                }
                let error = request
                    .await
                    .err()
                    .expect("session cancellation aborts preparation");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(probe.stops.get(), 1, "unfinished source stopped once");
                assert_eq!(
                    probe.drops.get(),
                    1,
                    "source disposed before request completes"
                );
                assert_eq!(
                    *events.borrow(),
                    ["attached", "detached"],
                    "reservation released"
                );
                assert_eq!(
                    session
                        .wait_closed()
                        .await
                        .expect_err("cancelled session")
                        .code(),
                    HttpErrorCode::CORO_CANCELLED
                );
                assert!(session.is_finished());
            }
        }
    });
}

#[test]
fn client_attachment_cancels_pending_request_headers() {
    check_header_cancellation(false);
}

#[test]
fn client_attachment_rechecks_cancellation_before_request_commit() {
    check_header_cancellation(true);
}

struct HeldRequestBody {
    inner: HttpFixedSource,
    state: Rc<HeaderProbeState>,
}

impl HttpSource for HeldRequestBody {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let state = Rc::clone(&self.state);
        Box::pin(poll_fn(move |_| {
            assert_eq!(std::thread::current().id(), state.owner);
            state.polls.set(state.polls.get() + 1);
            Poll::Pending
        }))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        assert_eq!(std::thread::current().id(), self.state.owner);
        self.state.stops.set(self.state.stops.get() + 1);
        self.inner.stop_reading(error);
    }
}

impl Drop for HeldRequestBody {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.state.owner);
        self.state.drops.set(self.state.drops.get() + 1);
    }
}

#[test]
fn reserved_request_cancellation_after_response_eom_stops_upload_and_reuses_h2() {
    run_on_worker(|context| async move {
        let (io, peer) = tokio::io::duplex(64 * 1024);
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                io,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("adopted fixture");
        let session = ClientSession::attach_transport(
            &context,
            transport,
            ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext),
        )
        .expect("attached session");
        let client = async {
            let state = Rc::new(HeaderProbeState {
                owner: std::thread::current().id(),
                polls: Cell::new(0),
                stops: Cell::new(0),
                drops: Cell::new(0),
            });
            let body = BufQueue::from_bytes(bytes::Bytes::from_static(b"x"), 1).unwrap();
            let request = HttpSourceHolder::new(Box::new(HeldRequestBody {
                inner: HttpFixedSource::make_fixed_request("/early", HttpMethod::Post, Some(body))
                    .unwrap(),
                state: Rc::clone(&state),
            }));
            let cancellation = CancellationSource::new();
            let reservation = session.reserve_request().unwrap();
            let mut response = session
                .send_reserved_request(request, reservation, cancellation.token())
                .await
                .unwrap();
            assert!(response.source.read_header_event().await.unwrap().eom());
            assert!(
                state.polls.get() > 0,
                "upload is pending after full response"
            );
            assert_eq!(state.stops.get(), 0, "response EOM must not cancel upload");
            let mut completion = response.completion.wait();
            poll_fn(|context| {
                assert!(completion.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            cancellation.request_cancellation();
            tokio::time::timeout(Duration::from_secs(2), completion)
                .await
                .expect("caller cancellation must retire the reserved upload")
                .expect("exchange cleanup confirmed");
            assert_eq!(state.stops.get(), 1);
            assert_eq!(state.drops.get(), 1);
            assert_eq!(Rc::strong_count(&state), 1, "pending read also retired");

            let reservation = session
                .reserve_request()
                .expect("same H2 session is reusable");
            let request = HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_get_request(b"/later", None).unwrap(),
            ));
            let mut response = session
                .send_reserved_request(request, reservation, CancellationToken::never())
                .await
                .unwrap();
            let headers = response.source.read_header_event().await.unwrap();
            assert_eq!(headers.headers().response_status(), Some(204));
            assert!(headers.eom());
            response.completion.wait().await.unwrap();
            session.cancel();
            let _ = session.wait_closed().await;
        };
        let server = async {
            let mut connection = h2::server::handshake(peer).await.unwrap();
            let (first_request, mut first_response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(first_request.uri().path(), "/early");
            assert!(!first_request.body().is_end_stream());
            let first_stream = first_response
                .send_response(
                    http::Response::builder().status(200).body(()).unwrap(),
                    true,
                )
                .unwrap();
            let (later_request, mut later_response) = connection.accept().await.unwrap().unwrap();
            assert_eq!(later_request.uri().path(), "/later");
            let later_stream = later_response
                .send_response(
                    http::Response::builder().status(204).body(()).unwrap(),
                    true,
                )
                .unwrap();
            assert!(!matches!(connection.accept().await, Some(Ok(_))));
            drop((first_request, first_stream, later_request, later_stream));
        };
        tokio::join!(client, server);
    });
}

#[test]
fn client_attachment_preserves_reserved_drain_policy() {
    run_on_worker(|context| async move {
        let (io, _peer) = tokio::io::duplex(4096);
        let transport = adopt_local(
            &context,
            EstablishedIo::new(
                io,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("adopted fixture");
        let session = ClientSession::attach_transport(
            &context,
            transport,
            ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
        )
        .expect("attached session");
        let reservation = session.reserve_request().expect("request reserved");
        session.initiate_drain().expect("graceful drain");
        let (source, release, probe) = held_request_headers(None);
        let mut request =
            session.send_reserved_request(source, reservation, CancellationToken::never());
        assert!(
            request
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "graceful drain does not cancel pending header preparation"
        );
        release.send(()).expect("finish header preparation");
        assert_eq!(
            request
                .await
                .err()
                .expect("existing post-drain commit policy")
                .code(),
            HttpErrorCode::REFUSED_STREAM
        );
        assert_eq!(probe.stops.get(), 1);
        assert_eq!(probe.drops.get(), 1);
        session
            .wait_closed()
            .await
            .expect("reservation and session joined");
    });
}

#[test]
fn client_attachment_preserves_reserved_request_during_quiescence() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let server = start_server(protocol);
        let address = server.address().expect("peer address");
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("owner starts");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker exists");
        let quiesce_handle = runtime.clone();
        let initialized = worker
            .call_local(move |context| async move {
                let io = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("connected I/O");
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(io, TransportEndpoint::Unknown, endpoint(address), None),
                    limits(),
                )
                .expect("adopted fixture");
                let session = ClientSession::attach_transport(
                    &context,
                    transport,
                    ClientAttachOptions::new(protocol, TransportSecurity::Plaintext),
                )
                .expect("attached session");
                let reservation = session
                    .reserve_request()
                    .expect("request reserved before drain");
                drop(quiesce_handle.quiesce(TIMEOUT));
                assert!(!context.handle().is_accepting());
                assert_eq!(context.handle().state(), WorkerState::Open);
                assert!(session.reserve_request().is_err(), "new requests rejected");
                let (source, release, probe) = held_request_headers(None);
                let mut request =
                    session.send_reserved_request(source, reservation, CancellationToken::never());
                assert!(
                    request
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                release
                    .send(())
                    .expect("finish reserved request preparation");
                let mut response = request
                    .await
                    .expect("reserved request admitted during grace");
                let mut headers = response
                    .source
                    .read_header_event()
                    .await
                    .expect("response headers");
                assert_eq!(
                    headers
                        .headers_mut()
                        .status_code()
                        .expect("response status"),
                    200
                );
                let mut body = BufQueue::new(64);
                if !headers.eom() {
                    loop {
                        let event = response
                            .source
                            .read_body_event(64)
                            .await
                            .expect("response body");
                        if let Some(data) = event.as_body() {
                            body.append(&mut data.clone_shared())
                                .expect("bounded response");
                        }
                        if event.eom() {
                            break;
                        }
                    }
                }
                assert_eq!(
                    body.coalesce(64).expect("response bytes").as_ref(),
                    b"request-body"
                );
                session
                    .wait_closed()
                    .await
                    .expect("reserved request and session joined");
                assert_eq!(
                    probe.stops.get(),
                    0,
                    "grace does not cancel reserved source"
                );
                assert_eq!(probe.drops.get(), 1);
            })
            .expect("initializer admitted");
        wait(initialized).expect("reserved request finishes during grace");
        wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
        assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    }
}

#[test]
fn client_attachment_rejects_quiescing_open_worker() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let quiescing = runtime.clone();
    wait(
        worker
            .call_local(move |context| async move {
                let (io, _peer) = tokio::io::duplex(64);
                let drops = Arc::new(AtomicUsize::new(0));
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(
                        ProbeIo {
                            io,
                            owner: std::thread::current().id(),
                            drops: Arc::clone(&drops),
                            gate_enabled: Rc::new(Cell::new(false)),
                            flush: None,
                            shutdown: None,
                            trace: None,
                        },
                        TransportEndpoint::Unknown,
                        TransportEndpoint::Unknown,
                        None,
                    ),
                    limits(),
                )
                .expect("I/O adopted before admission closes");
                drop(quiescing.quiesce(TIMEOUT));
                assert_eq!(context.handle().state(), WorkerState::Open);
                let error = ClientSession::attach_transport(
                    &context,
                    transport,
                    ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
                )
                .err()
                .expect("new session rejected during grace");
                assert_eq!(error.code(), HttpErrorCode::DROPPED);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("quiescence completes");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

#[test]
fn client_attachment_handle_stays_local() {
    struct Invalid;
    trait AmbiguousIfSend<Marker> {
        fn marker() {}
    }
    impl<Value: ?Sized> AmbiguousIfSend<()> for Value {}
    impl<Value: ?Sized + Send> AmbiguousIfSend<Invalid> for Value {}
    let _ = <ClientSession as AmbiguousIfSend<_>>::marker;
}

#[test]
fn client_attachment_quiescence_joins_pending_close_after_initializer() {
    let server = start_server(CodecProtocol::Http11);
    let address = server.address().expect("peer address");
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let drops = Arc::new(AtomicUsize::new(0));
    let owner_drops = Arc::clone(&drops);
    let trace = CloseTrace::default();
    let owner_trace = trace.clone();
    let (flush_started, flush_started_rx) = oneshot::channel();
    let (flush_release, flush_release_rx) = oneshot::channel();
    let (shutdown_started, shutdown_started_rx) = oneshot::channel();
    let (shutdown_release, shutdown_release_rx) = oneshot::channel();
    let quiesce_handle = runtime.clone();
    let initialized = worker
        .call_local(move |context| async move {
            let io = tokio::net::TcpStream::connect(address)
                .await
                .expect("connected I/O");
            let gate_enabled = Rc::new(Cell::new(false));
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    ProbeIo {
                        io,
                        owner: std::thread::current().id(),
                        drops: owner_drops,
                        gate_enabled: Rc::clone(&gate_enabled),
                        flush: Some(Gate {
                            started: Some(flush_started),
                            release: flush_release_rx,
                        }),
                        shutdown: Some(Gate {
                            started: Some(shutdown_started),
                            release: shutdown_release_rx,
                        }),
                        trace: Some(owner_trace.clone()),
                    },
                    TransportEndpoint::Unknown,
                    endpoint(address),
                    None,
                ),
                limits(),
            )
            .expect("adopted I/O");
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            options.session.lifecycle_observer = Some(Rc::new(owner_trace.clone()));
            let session = ClientSession::attach_transport(&context, transport, options)
                .expect("attached session");
            let url = Url::parse("http://peer.example/", false, UrlMode::StrictCompat);
            let _response = HttpClient::request(&session, None, RequestSpec::post(url, b"bytes"))
                .await
                .expect("exchange");
            owner_trace.record(format!(
                "response received: finished={}, draining={}, active_transactions={}",
                session.is_finished(),
                session.is_draining(),
                session.active_transactions()
            ));
            gate_enabled.set(true);
            owner_trace.record("flush gate enabled".to_owned());
            context.handle().submit(|_| {}).expect("fill command queue");
            assert_eq!(
                context.handle().submit(|_| {}),
                Err(SubmitError::Overloaded)
            );
            owner_trace.record("quiesce requested".to_owned());
            drop(quiesce_handle.quiesce(TIMEOUT));
            assert!(
                session.reserve_request().is_err(),
                "quiesce closes new request admission synchronously"
            );
            drop(session.wait_closed());
            drop(session);
            owner_trace
                .record("initializer completion observation and session handle dropped".to_owned());
        })
        .expect("initializer admitted");
    wait(async {
        initialized.await.unwrap_or_else(|error| {
            panic!("initializer finishes: {error:?}; last_64_events={trace:?}")
        });
        trace.record("initializer joined".to_owned());
        flush_started_rx.await.unwrap_or_else(|error| {
            panic!(
                "close flush starts: {error:?}; worker_state={:?}; drops={}; last_64_events={trace:?}",
                worker.state(),
                drops.load(Ordering::SeqCst)
            )
        });
        let mut quiesced = Box::pin(runtime.quiesce(TIMEOUT));
        assert!(
            quiesced
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "quiescence stays pending before flush release; last_64_events={trace:?}"
        );
        assert_eq!(worker.state(), WorkerState::Open);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        trace.record("release flush".to_owned());
        flush_release
            .send(())
            .unwrap_or_else(|error| panic!("allow flush: {error:?}; last_64_events={trace:?}"));
        shutdown_started_rx.await.unwrap_or_else(|error| {
            panic!(
                "shutdown starts: {error:?}; worker_state={:?}; drops={}; last_64_events={trace:?}",
                worker.state(),
                drops.load(Ordering::SeqCst)
            )
        });
        assert!(
            quiesced
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "quiescence stays pending before shutdown release; last_64_events={trace:?}"
        );
        trace.record("release shutdown".to_owned());
        shutdown_release
            .send(())
            .unwrap_or_else(|error| panic!("allow shutdown: {error:?}; last_64_events={trace:?}"));
        quiesced.await.unwrap_or_else(|error| {
            panic!("session and transport scopes finish: {error:?}; last_64_events={trace:?}")
        });
    });
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let report = owner.join().expect("physical owner join");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

struct ReadFailureControl {
    fail: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    observed: RefCell<Option<oneshot::Sender<()>>>,
}

impl ReadFailureControl {
    fn fail(&self) {
        self.fail.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

struct ReadFailureIo {
    io: tokio::net::TcpStream,
    failure: Rc<ReadFailureControl>,
}

impl AsyncRead for ReadFailureIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.failure.fail.get() {
            if let Some(observed) = self.failure.observed.borrow_mut().take() {
                observed.send(()).expect("native read error observed");
            }
            return Poll::Ready(Err(io::Error::other("read failed after repeated grace")));
        }
        let result = Pin::new(&mut self.io).poll_read(context, buffer);
        if result.is_pending() {
            self.failure.waker.replace(Some(context.waker().clone()));
        }
        result
    }
}

impl AsyncWrite for ReadFailureIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }
}

enum PendingCloseRelease {
    Flush {
        flush: oneshot::Sender<()>,
        shutdown_started: oneshot::Receiver<()>,
        shutdown: oneshot::Sender<()>,
    },
    Shutdown(oneshot::Sender<()>),
}

impl PendingCloseRelease {
    fn is_closed(&self) -> bool {
        match self {
            Self::Flush {
                flush, shutdown, ..
            } => flush.is_closed() && shutdown.is_closed(),
            Self::Shutdown(shutdown) => shutdown.is_closed(),
        }
    }
}

#[derive(Clone, Copy)]
enum AfterRepeatedGrace {
    Complete,
    CancelLocal,
    CancelAttachment,
    FailRead,
}

fn check_repeated_grace(repeat_at_shutdown: bool, after_grace: AfterRepeatedGrace) {
    let server = start_server(CodecProtocol::Http11);
    let address = server.address().expect("peer address");
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let quiesce_handle = runtime.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let worker_drops = Arc::clone(&drops);
    let trace = CloseTrace::default();
    let owner_trace = trace.clone();
    let initialized = worker
        .call_local(move |context| async move {
            let io = tokio::net::TcpStream::connect(address)
                .await
                .expect("connected I/O");
            let (read_failed, read_failed_rx) = oneshot::channel();
            let read_failure = Rc::new(ReadFailureControl {
                fail: Cell::new(false),
                waker: RefCell::new(None),
                observed: RefCell::new(Some(read_failed)),
            });
            let gate_enabled = Rc::new(Cell::new(false));
            let (flush_started, flush_started_rx) = oneshot::channel();
            let (flush_release, flush_release_rx) = oneshot::channel();
            let (shutdown_started, shutdown_started_rx) = oneshot::channel();
            let (shutdown_release, shutdown_release_rx) = oneshot::channel();
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    ProbeIo {
                        io: ReadFailureIo {
                            io,
                            failure: Rc::clone(&read_failure),
                        },
                        owner: std::thread::current().id(),
                        drops: Arc::clone(&worker_drops),
                        gate_enabled: Rc::clone(&gate_enabled),
                        flush: Some(Gate {
                            started: Some(flush_started),
                            release: flush_release_rx,
                        }),
                        shutdown: Some(Gate {
                            started: Some(shutdown_started),
                            release: shutdown_release_rx,
                        }),
                        trace: Some(owner_trace.clone()),
                    },
                    TransportEndpoint::Unknown,
                    endpoint(address),
                    None,
                ),
                limits(),
            )
            .expect("adopted I/O");
            let cancellation = CancellationSource::new();
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            options.cancellation = cancellation.token();
            options.session.lifecycle_observer = Some(Rc::new(owner_trace.clone()));
            let session = ClientSession::attach_transport(&context, transport, options)
                .expect("attached session");
            let body = BufQueue::from_bytes(bytes::Bytes::from_static(b"request-body"), 12)
                .expect("bounded request body");
            let request = HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(body))
                .expect("valid request");
            let mut response = session
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationToken::never(),
                )
                .await
                .expect("request admitted");
            let mut headers = response
                .source
                .read_header_event()
                .await
                .expect("response headers");
            assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
            assert!(!headers.eom(), "retain the unconsumed response body");
            gate_enabled.set(true);
            session.initiate_drain().expect("first graceful drain");
            flush_started_rx
                .await
                .expect("graceful close entered flush");
            let release = if repeat_at_shutdown {
                flush_release.send(()).expect("finish close flush");
                shutdown_started_rx
                    .await
                    .expect("graceful close entered shutdown");
                PendingCloseRelease::Shutdown(shutdown_release)
            } else {
                PendingCloseRelease::Flush {
                    flush: flush_release,
                    shutdown_started: shutdown_started_rx,
                    shutdown: shutdown_release,
                }
            };
            assert_eq!(
                session.active_transactions(),
                1,
                "body still owns its stream"
            );
            assert!(
                !session.is_finished(),
                "the native driver must still be joining"
            );
            let mut completed = session.wait_closed();
            assert!(
                completed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(!session.initiate_drain().expect("repeated graceful drain"));
            let rejected = session
                .close_when_idle()
                .expect_err("native command surface is closing");
            assert_eq!(rejected.code(), HttpErrorCode::DROPPED);
            let supervisor_turn = context
                .register_scope(async {})
                .expect("reserve ordered owner barrier");
            context.handle().submit(|_| {}).expect("fill command queue");
            assert_eq!(
                context.handle().submit(|_| {}),
                Err(SubmitError::Overloaded)
            );
            drop(quiesce_handle.quiesce(TIMEOUT));
            assert!(
                session.reserve_request().is_err(),
                "new admission is closed"
            );
            supervisor_turn
                .activate()
                .expect("activate reserved barrier")
                .await;
            owner_trace
                .record("supervisor observed repeated grace while close was pending".to_owned());
            assert_eq!(
                worker_drops.load(Ordering::SeqCst),
                0,
                "repeat grace must not reset the pending close; last_64_events={owner_trace:?}"
            );
            assert!(
                !release.is_closed(),
                "close gates still belong to the transport"
            );
            assert!(
                !session.is_finished(),
                "held response still blocks native completion"
            );
            match after_grace {
                AfterRepeatedGrace::Complete => {
                    let mut body = BufQueue::new(64);
                    loop {
                        let event = response
                            .source
                            .read_body_event(64)
                            .await
                            .expect("response body");
                        if let Some(data) = event.as_body() {
                            body.append(&mut data.clone_shared())
                                .expect("bounded response");
                        }
                        if event.eom() {
                            break;
                        }
                    }
                    assert_eq!(
                        body.coalesce(64).expect("echo bytes").as_ref(),
                        b"request-body"
                    );
                    drop(response);
                    let summary = completed
                        .await
                        .expect("native driver joins without repeat-drain error");
                    assert!(!summary.reset, "graceful close never reset the session");
                    drop(session);
                    release
                }
                AfterRepeatedGrace::CancelLocal
                | AfterRepeatedGrace::CancelAttachment
                | AfterRepeatedGrace::FailRead => {
                    let expected = match after_grace {
                        AfterRepeatedGrace::CancelLocal => {
                            session.cancel();
                            HttpErrorCode::CORO_CANCELLED
                        }
                        AfterRepeatedGrace::CancelAttachment => {
                            cancellation.request_cancellation();
                            HttpErrorCode::CORO_CANCELLED
                        }
                        AfterRepeatedGrace::FailRead => {
                            read_failure.fail();
                            read_failed_rx
                                .await
                                .expect("native read returned its error");
                            HttpErrorCode::TRANSPORT_READ_ERROR
                        }
                        AfterRepeatedGrace::Complete => unreachable!("success handled separately"),
                    };
                    drop(response);
                    let error = completed
                        .await
                        .expect_err("terminal native failure remains observable");
                    assert_eq!(
                        error.code(),
                        expected,
                        "original terminal failure: {error:?}"
                    );
                    if matches!(after_grace, AfterRepeatedGrace::FailRead) {
                        assert!(
                            error
                                .to_string()
                                .contains("read failed after repeated grace")
                        );
                    }
                    drop(session);
                    release
                }
            }
        })
        .expect("initializer admitted");
    wait(async {
        let release = initialized
            .await
            .expect("initializer and native session finish");
        let (release, terminal_release) = if matches!(after_grace, AfterRepeatedGrace::Complete) {
            (Some(release), None)
        } else {
            (None, Some(release))
        };
        let mut quiesced = Box::pin(runtime.quiesce(TIMEOUT));
        if let Some(release) = release {
            assert!(
                quiesced
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending(),
                "only the transport close remains; last_64_events={trace:?}"
            );
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(worker.state(), WorkerState::Open);
            assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
            let shutdown = match release {
                PendingCloseRelease::Flush {
                    flush,
                    shutdown_started,
                    shutdown,
                } => {
                    flush.send(()).expect("release close flush");
                    shutdown_started.await.expect("close enters shutdown");
                    assert!(
                        quiesced
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending(),
                        "shutdown is still held after flush completes; last_64_events={trace:?}"
                    );
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    shutdown
                }
                PendingCloseRelease::Shutdown(shutdown) => shutdown,
            };
            shutdown.send(()).expect("release close shutdown");
        }
        quiesced
            .await
            .expect("all session and transport scopes join");
        if let Some(release) = terminal_release {
            assert!(
                release.is_closed(),
                "terminal cleanup retires both gate receivers without releasing their senders"
            );
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "runtime quiescence joins terminal I/O disposal"
            );
        }
    });
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let report = owner.join().expect("physical owner join");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

#[test]
fn client_attachment_repeated_grace_keeps_pending_close() {
    for repeat_at_shutdown in [false, true] {
        check_repeated_grace(repeat_at_shutdown, AfterRepeatedGrace::Complete);
    }
}

#[test]
fn client_attachment_repeated_grace_preserves_explicit_cancellation() {
    for cancellation in [
        AfterRepeatedGrace::CancelLocal,
        AfterRepeatedGrace::CancelAttachment,
    ] {
        check_repeated_grace(false, cancellation);
    }
}

#[test]
fn client_attachment_repeated_grace_preserves_native_error() {
    check_repeated_grace(false, AfterRepeatedGrace::FailRead);
}

struct PanicOnDrain;

impl LifecycleObserver for PanicOnDrain {
    fn on_event(&self, event: LifecycleEvent) {
        assert!(
            !matches!(event, LifecycleEvent::DrainStarted(_)),
            "genuine drain callback failure"
        );
    }
}

#[test]
fn client_attachment_graceful_drain_failure_remains_observable() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let quiesce_handle = runtime.clone();
    let initialized = worker
        .call_local(move |context| async move {
            let (io, _peer) = tokio::io::duplex(64);
            let drops = Arc::new(AtomicUsize::new(0));
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    ProbeIo {
                        io,
                        owner: std::thread::current().id(),
                        drops: Arc::clone(&drops),
                        gate_enabled: Rc::new(Cell::new(false)),
                        flush: None,
                        shutdown: None,
                        trace: None,
                    },
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                limits(),
            )
            .expect("adopted fixture");
            let mut options =
                ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
            options.session.lifecycle_observer = Some(Rc::new(PanicOnDrain));
            let session = ClientSession::attach_transport(&context, transport, options)
                .expect("attached session");
            drop(quiesce_handle.quiesce(TIMEOUT));
            let error = session
                .wait_closed()
                .await
                .expect_err("drain callback failure survives");
            assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
            assert!(
                error
                    .to_string()
                    .contains("session drain callback panicked")
            );
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "genuine drain failure resets I/O"
            );
        })
        .expect("initializer admitted");
    wait(initialized).expect("failed session is still joined");
    wait(runtime.quiesce(TIMEOUT)).expect("all scopes joined");
    assert_eq!(owner.join().expect("physical owner join").workers_forced, 0);
}

struct CancelOnNativeDestroy {
    owner: ThreadId,
    cancel: RefCell<Option<Box<dyn FnOnce()>>>,
    calls: Cell<usize>,
    drops: Arc<AtomicUsize>,
    trace: CloseTrace,
}

impl LifecycleObserver for CancelOnNativeDestroy {
    fn on_event(&self, event: LifecycleEvent) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.trace.on_event(event);
        if matches!(event, LifecycleEvent::Destroy) {
            assert_eq!(self.calls.replace(1), 0, "native destruction occurs once");
            assert_eq!(
                self.drops.load(Ordering::SeqCst),
                0,
                "native destruction precedes the gated transport close"
            );
            let cancel = self.cancel.borrow_mut().take().expect("one cancellation");
            cancel();
            self.trace
                .record("cancellation requested during native destruction".to_owned());
        }
    }
}

#[test]
fn client_attachment_cancelled_native_completion_resets_pending_close() {
    for attachment_cancellation in [false, true] {
        let server = start_server(CodecProtocol::Http11);
        let address = server.address().expect("peer address");
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("owner starts");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker exists");
        let drops = Arc::new(AtomicUsize::new(0));
        let worker_drops = Arc::clone(&drops);
        let trace = CloseTrace::default();
        let owner_trace = trace.clone();
        let initialized = worker
            .call_local(move |context| async move {
                let io = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("connected I/O");
                let gate_enabled = Rc::new(Cell::new(false));
                let (flush_started, flush_started_rx) = oneshot::channel();
                let (flush_release, flush_release_rx) = oneshot::channel();
                let (shutdown_started, shutdown_started_rx) = oneshot::channel();
                let (shutdown_release, shutdown_release_rx) = oneshot::channel();
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(
                        ProbeIo {
                            io,
                            owner: std::thread::current().id(),
                            drops: Arc::clone(&worker_drops),
                            gate_enabled: Rc::clone(&gate_enabled),
                            flush: Some(Gate {
                                started: Some(flush_started),
                                release: flush_release_rx,
                            }),
                            shutdown: Some(Gate {
                                started: Some(shutdown_started),
                                release: shutdown_release_rx,
                            }),
                            trace: Some(owner_trace.clone()),
                        },
                        TransportEndpoint::Unknown,
                        endpoint(address),
                        None,
                    ),
                    limits(),
                )
                .expect("adopted I/O");
                let cancellation = Rc::new(CancellationSource::new());
                let retained_session = Rc::new(RefCell::new(None::<ClientSession>));
                let cancel: Box<dyn FnOnce()> = if attachment_cancellation {
                    let cancellation = Rc::clone(&cancellation);
                    Box::new(move || {
                        cancellation.request_cancellation();
                    })
                } else {
                    let retained_session = Rc::downgrade(&retained_session);
                    Box::new(move || {
                        retained_session
                            .upgrade()
                            .expect("initializer retains local session")
                            .borrow()
                            .as_ref()
                            .expect("attached local session")
                            .cancel();
                    })
                };
                let observer = Rc::new(CancelOnNativeDestroy {
                    owner: std::thread::current().id(),
                    cancel: RefCell::new(Some(cancel)),
                    calls: Cell::new(0),
                    drops: Arc::clone(&worker_drops),
                    trace: owner_trace.clone(),
                });
                let mut options =
                    ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
                options.cancellation = cancellation.token();
                options.session.lifecycle_observer = Some(observer.clone());
                let session = ClientSession::attach_transport(&context, transport, options)
                    .expect("attached session");
                assert!(retained_session.replace(Some(session.clone())).is_none());
                let request_body =
                    BufQueue::from_bytes(bytes::Bytes::from_static(b"request-body"), 12)
                        .expect("bounded request body");
                let request =
                    HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(request_body))
                        .expect("valid request");
                let mut response = session
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("request admitted");
                let mut headers = response
                    .source
                    .read_header_event()
                    .await
                    .expect("response headers");
                assert_eq!(headers.headers_mut().status_code().expect("status"), 200);
                assert!(
                    !headers.eom(),
                    "retain response consumption until close starts"
                );
                gate_enabled.set(true);
                session.initiate_drain().expect("graceful drain");
                flush_started_rx
                    .await
                    .expect("actual close flush is pending");
                assert_eq!(session.active_transactions(), 1);
                assert!(!session.is_finished());
                assert_eq!(
                    observer.calls.get(),
                    0,
                    "native destruction is still blocked"
                );
                let mut completed = session.wait_closed();
                assert!(
                    completed
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert!(!session.initiate_drain().expect("repeated graceful drain"));
                assert_eq!(
                    session
                        .close_when_idle()
                        .expect_err("native writer is already closing")
                        .code(),
                    HttpErrorCode::DROPPED
                );
                context.handle().submit(|_| {}).expect("fill command queue");
                assert_eq!(
                    context.handle().submit(|_| {}),
                    Err(SubmitError::Overloaded)
                );
                let mut body = BufQueue::new(64);
                loop {
                    let event = response
                        .source
                        .read_body_event(64)
                        .await
                        .expect("successful response body");
                    if let Some(data) = event.as_body() {
                        body.append(&mut data.clone_shared())
                            .expect("bounded response");
                    }
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(
                    body.coalesce(64).expect("echo bytes").as_ref(),
                    b"request-body"
                );
                drop(response);
                let error = completed
                    .await
                    .expect_err("cancellation wins native-completion arbitration");
                assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
                assert_eq!(
                    error.message(),
                    if attachment_cancellation {
                        b"attached session cancelled".as_slice()
                    } else {
                        b"client session cancelled".as_slice()
                    }
                );
                assert_eq!(observer.calls.get(), 1);
                assert!(session.is_finished());
                let repeated = session
                    .wait_closed()
                    .await
                    .expect_err("native completion receipt stays terminal");
                assert_eq!(repeated.code(), error.code());
                assert_eq!(repeated.message(), error.message());
                drop(retained_session.borrow_mut().take());
                drop(session);
                PendingCloseRelease::Flush {
                    flush: flush_release,
                    shutdown_started: shutdown_started_rx,
                    shutdown: shutdown_release,
                }
            })
            .expect("initializer admitted");
        wait(async {
            let release = initialized
                .await
                .expect("initializer observes cancelled native completion");
            runtime.quiesce(TIMEOUT).await.unwrap_or_else(|error| {
                panic!("all cleanup scopes join: {error:?}; trace={trace:?}")
            });
            assert!(
                release.is_closed(),
                "reset retires both receivers while their unreleased senders remain live"
            );
            assert_eq!(
                drops.load(Ordering::SeqCst),
                1,
                "one owner-local I/O disposal"
            );
        });
        let report = owner.join().expect("physical owner join");
        assert_eq!(report.workers_joined, 1);
        assert_eq!(report.workers_forced, 0);
    }
}

#[test]
fn client_attachment_io_control_cancels_after_wait_closed() {
    for hold_shutdown in [false, true] {
        let server = start_server(CodecProtocol::Http11);
        let address = server.address().expect("peer address");
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("owner starts");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker exists");
        let drops = Arc::new(AtomicUsize::new(0));
        let worker_drops = Arc::clone(&drops);
        let trace = CloseTrace::default();
        let owner_trace = trace.clone();
        let initialized = worker
            .call_local(move |context| async move {
                let io = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("connected I/O");
                let gate_enabled = Rc::new(Cell::new(false));
                let (flush_started, flush_started_rx) = oneshot::channel();
                let (flush_release, flush_release_rx) = oneshot::channel();
                let (shutdown_started, shutdown_started_rx) = oneshot::channel();
                let (shutdown_release, shutdown_release_rx) = oneshot::channel();
                let (transport, control) = adopt_local_with_completion(
                    &context,
                    EstablishedIo::new(
                        ProbeIo {
                            io,
                            owner: std::thread::current().id(),
                            drops: Arc::clone(&worker_drops),
                            gate_enabled: Rc::clone(&gate_enabled),
                            flush: Some(Gate {
                                started: Some(flush_started),
                                release: flush_release_rx,
                            }),
                            shutdown: Some(Gate {
                                started: Some(shutdown_started),
                                release: shutdown_release_rx,
                            }),
                            trace: Some(owner_trace.clone()),
                        },
                        TransportEndpoint::Unknown,
                        endpoint(address),
                        None,
                    ),
                    limits(),
                )
                .expect("completion-aware adopted I/O");
                let mut options =
                    ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
                options.session.lifecycle_observer = Some(Rc::new(owner_trace));
                let session = ClientSession::attach_transport(&context, transport, options)
                    .expect("attached session");
                let request_body =
                    BufQueue::from_bytes(bytes::Bytes::from_static(b"request-body"), 12)
                        .expect("request body");
                let request =
                    HttpFixedSource::make_fixed_request("/", HttpMethod::Post, Some(request_body))
                        .expect("request");
                let mut response = session
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("request admitted");
                let mut header = response
                    .source
                    .read_header_event()
                    .await
                    .expect("response headers");
                assert_eq!(
                    header.headers_mut().status_code().expect("response status"),
                    200
                );
                assert!(!header.eom());
                gate_enabled.set(true);
                session.initiate_drain().expect("start graceful drain");
                flush_started_rx.await.expect("actual close flush pending");
                let release = if hold_shutdown {
                    flush_release.send(()).expect("allow flush only");
                    shutdown_started_rx
                        .await
                        .expect("actual close shutdown pending");
                    PendingCloseRelease::Shutdown(shutdown_release)
                } else {
                    PendingCloseRelease::Flush {
                        flush: flush_release,
                        shutdown_started: shutdown_started_rx,
                        shutdown: shutdown_release,
                    }
                };
                let mut body = BufQueue::new(64);
                loop {
                    let event = response
                        .source
                        .read_body_event(64)
                        .await
                        .expect("response body");
                    if let Some(data) = event.as_body() {
                        body.append(&mut data.clone_shared())
                            .expect("bounded response");
                    }
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(body.coalesce(64).expect("echo").as_ref(), b"request-body");
                drop(response);
                session
                    .wait_closed()
                    .await
                    .expect("native session succeeds before transport cancellation");
                session
                    .wait_closed()
                    .await
                    .expect("native success stays sticky");
                assert!(session.is_finished());
                assert_eq!(worker_drops.load(Ordering::SeqCst), 0);
                assert!(!release.is_closed());
                assert!(
                    Box::pin(control.completion().wait_retired())
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                drop(session);
                (control, release)
            })
            .expect("initializer admitted");
        wait(async {
            let (control, release) = initialized
                .await
                .expect("native owner has returned and dropped");
            let receipt = control.completion();
            assert!(
                Box::pin(receipt.wait_retired())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let mut quiesced = Box::pin(runtime.quiesce(TIMEOUT));
            assert!(
                quiesced
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
            assert_eq!(drops.load(Ordering::SeqCst), 0, "trace={trace:?}");
            control.cancel();
            control.cancel();
            receipt
                .wait_retired()
                .await
                .expect("transport and close scope retire");
            receipt
                .wait_retired()
                .await
                .expect("sticky physical retirement");
            assert!(
                release.is_closed(),
                "unreleased gate receivers are destroyed; trace={trace:?}"
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1, "trace={trace:?}");
            quiesced.await.expect("cleanup within grace");
        });
        let report = owner.join().expect("physical join");
        assert_eq!(report.workers_joined, 1);
        assert_eq!(report.workers_forced, 0);
    }
}

#[derive(Default)]
struct OfferedReads {
    offers: Vec<usize>,
    inbound: VecDeque<u8>,
    outbound: Vec<u8>,
    eof: bool,
    reader: Option<Waker>,
}

/// In-memory peer that records the capacity each transport read offers it.
#[derive(Clone, Default)]
struct OfferRecordingIo(Rc<RefCell<OfferedReads>>);

impl OfferRecordingIo {
    fn take_offers(&self) -> Vec<usize> {
        std::mem::take(&mut self.0.borrow_mut().offers)
    }

    fn deliver(&self, bytes: &[u8]) {
        let reader = {
            let mut reads = self.0.borrow_mut();
            reads.inbound.extend(bytes);
            reads.reader.take()
        };
        if let Some(reader) = reader {
            reader.wake();
        }
    }

    async fn until(&self, ready: impl Fn(&OfferedReads) -> bool) {
        while !ready(&self.0.borrow()) {
            tokio::task::yield_now().await;
        }
    }
}

impl AsyncRead for OfferRecordingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut reads = self.0.borrow_mut();
        reads.offers.push(buffer.remaining());
        if reads.inbound.is_empty() && !reads.eof {
            reads.reader = Some(context.waker().clone());
            return Poll::Pending;
        }
        let length = buffer.remaining().min(reads.inbound.len());
        let bytes = reads.inbound.drain(..length).collect::<Vec<_>>();
        buffer.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for OfferRecordingIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().outbound.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let reader = {
            let mut reads = self.0.borrow_mut();
            reads.eof = true;
            reads.reader.take()
        };
        if let Some(reader) = reader {
            reader.wake();
        }
        Poll::Ready(Ok(()))
    }
}

#[test]
fn idle_client_read_offers_proxygen_read_allocation_default() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_on_worker(move |context| async move {
            let peer = OfferRecordingIo::default();
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    peer.clone(),
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                ByteStreamLimits::new(1 << 20, 64 * 1024, 8).expect("bounded transport"),
            )
            .expect("I/O adopted");
            let session = ClientSession::attach_transport(
                &context,
                transport,
                ClientAttachOptions::new(protocol, TransportSecurity::Plaintext),
            )
            .expect("client attached");
            peer.until(|reads| !reads.offers.is_empty()).await;
            let idle = peer.take_offers();
            assert!(
                idle.iter().all(|offer| *offer == 4_000),
                "{protocol:?}: an idle client read offers readBufNewAllocSize_ (4_000 bytes): \
                 {idle:?}",
            );

            if protocol == CodecProtocol::Http11 {
                let request = HttpFixedSource::make_fixed_request("/", HttpMethod::Get, None)
                    .expect("request");
                let mut response = session
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("request admitted");
                peer.until(|reads| reads.outbound.ends_with(b"\r\n\r\n"))
                    .await;
                let mut burst = b"HTTP/1.1 200 OK\r\nContent-Length: 70000\r\n\r\n".to_vec();
                burst.resize(burst.len() + 70_000, b'x');
                peer.take_offers();
                peer.deliver(&burst);
                let mut header = response
                    .source
                    .read_header_event()
                    .await
                    .expect("response headers");
                assert_eq!(
                    header.headers_mut().status_code().expect("response status"),
                    200
                );
                let mut body = 0;
                while !header.eom() {
                    let event = response
                        .source
                        .read_body_event(128 * 1024)
                        .await
                        .expect("response body");
                    body += event.as_body().map_or(0, BufQueue::chain_length);
                    if event.eom() {
                        break;
                    }
                }
                assert_eq!(body, 70_000);
                let offers = peer.take_offers();
                assert_eq!(
                    offers.get(..6),
                    Some(&[4_000, 4_000, 8_000, 16_000, 32_000, 1_536][..]),
                    "the woken idle read fills its 4_000-byte buffer and continues up to \
                     maximum_read_size (65_536 bytes): {offers:?}",
                );
            } else {
                peer.deliver(&[0, 0, 0, 4, 0, 0, 0, 0, 0]);
            }
            session.close_when_idle().expect("graceful close");
            session.wait_closed().await.expect("session finishes");
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadMode {
    Ready,
    Pending,
    Failing,
}

#[derive(Default)]
struct HeadProbe {
    polls: Cell<usize>,
    future_drops: Cell<usize>,
    source_drops: Cell<usize>,
    live_waits_at_future_drop: Cell<Option<usize>>,
    released: Cell<bool>,
    waker: RefCell<Option<Waker>>,
    on_poll: RefCell<Option<Box<dyn FnOnce()>>>,
    on_future_drop: RefCell<Option<Box<dyn FnOnce()>>>,
    log: RefCell<Option<Rc<RefCell<Vec<&'static str>>>>>,
}

impl HeadProbe {
    fn release(&self) {
        self.released.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    fn record(&self, entry: &'static str) {
        if let Some(log) = self.log.borrow().as_ref() {
            log.borrow_mut().push(entry);
        }
    }
}

fn head_failure() -> HttpError {
    HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, b"probe head failure")
}

fn probe_header() -> HttpHeaderEvent {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).unwrap();
    message.set_url(b"/", false).unwrap();
    message.set_http_version(1, 1);
    HttpHeaderEvent::new(message, true).unwrap()
}

struct ProbeHeadFuture {
    probe: Rc<HeadProbe>,
    mode: HeadMode,
}

impl Future for ProbeHeadFuture {
    type Output = Result<HttpHeaderEvent, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.probe.polls.set(self.probe.polls.get() + 1);
        let hook = self.probe.on_poll.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
        match self.mode {
            HeadMode::Ready => Poll::Ready(Ok(probe_header())),
            HeadMode::Failing => Poll::Ready(Err(head_failure())),
            HeadMode::Pending if self.probe.released.get() => Poll::Ready(Ok(probe_header())),
            HeadMode::Pending => {
                *self.probe.waker.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for ProbeHeadFuture {
    fn drop(&mut self) {
        self.probe
            .future_drops
            .set(self.probe.future_drops.get() + 1);
        self.probe
            .live_waits_at_future_drop
            .set(Some(head_wait_probe::snapshot().live));
        let hook = self.probe.on_future_drop.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
    }
}

struct ProbeHeadSource {
    probe: Rc<HeadProbe>,
    mode: HeadMode,
}

impl HttpSource for ProbeHeadSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(ProbeHeadFuture {
            probe: Rc::clone(&self.probe),
            mode: self.mode,
        })
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        assert_eq!(error, None);
        self.probe.record("source-stopped");
    }
}

impl Drop for ProbeHeadSource {
    fn drop(&mut self) {
        self.probe
            .source_drops
            .set(self.probe.source_drops.get() + 1);
        self.probe.record("source-dropped");
    }
}

fn probe_source(mode: HeadMode) -> (HttpSourceHolder, Rc<HeadProbe>) {
    let probe = Rc::new(HeadProbe::default());
    (
        HttpSourceHolder::new(Box::new(ProbeHeadSource {
            probe: Rc::clone(&probe),
            mode,
        })),
        probe,
    )
}

struct Attached {
    session: ClientSession,
    attachment: Arc<CancellationSource>,
    events: Rc<RefCell<Vec<&'static str>>>,
    _peer: DuplexStream,
}

impl Attached {
    fn new(context: &WorkerContext) -> Self {
        let (io, peer) = tokio::io::duplex(4096);
        let transport = adopt_local(
            context,
            EstablishedIo::new(
                io,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("adopted fixture");
        let attachment = Arc::new(CancellationSource::new());
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut options =
            ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
        options.cancellation = attachment.token();
        options.session.lifecycle_observer = Some(Rc::new(OrderedCallbacks(Rc::clone(&events))));
        let session =
            ClientSession::attach_transport(context, transport, options).expect("attached session");
        Self {
            session,
            attachment,
            events,
            _peer: peer,
        }
    }

    fn send(
        &self,
        source: HttpSourceHolder,
        reserved: bool,
    ) -> Pin<Box<dyn Future<Output = Result<roxygen_session::uniplex::UniplexResponse, HttpError>>>>
    {
        if reserved {
            let reservation = self.session.reserve_request().expect("request reserved");
            self.session
                .send_reserved_request(source, reservation, CancellationToken::never())
        } else {
            self.session
                .send_request(source, CancellationToken::never())
        }
    }

    fn cancel(&self, attachment: bool) {
        if attachment {
            self.attachment.request_cancellation();
        } else {
            self.session.cancel();
        }
    }

    async fn finish_cancelled(self) {
        self.session.cancel();
        let _ = self.session.wait_closed().await;
    }
}

#[derive(Default)]
struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_session_cancelled(error: &HttpError) {
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
    assert_eq!(error.message(), b"client session cancelled");
}

fn assert_filter_cancelled(result: Result<HttpHeaderEvent, HttpError>) {
    assert_session_cancelled(&result.err().expect("the head read is cancelled"));
}

fn filter(
    local: &CancellationSource,
    attachment: &CancellationSource,
) -> SessionRequestCancellation {
    SessionRequestCancellation::for_test(local.token(), attachment.token())
}

#[test]
fn request_head_read_fails_cancelled_before_polling_the_source() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                for mode in [HeadMode::Ready, HeadMode::Pending, HeadMode::Failing] {
                    let attached = Attached::new(&context);
                    let (source, probe) = probe_source(mode);
                    let request = attached.send(source, reserved);
                    attached.cancel(attachment);
                    let error = request.await.err().expect("pre-cancelled head read");
                    assert_session_cancelled(&error);
                    assert_eq!(probe.polls.get(), 0, "the header future is never polled");
                    assert_eq!(probe.future_drops.get(), 1);
                    attached.finish_cancelled().await;
                }
            }
        }
    });

    let local = CancellationSource::new();
    let attachment = CancellationSource::new();
    for cancel_local in [false, true] {
        let (mut source, probe) = probe_source(HeadMode::Ready);
        let mut read = filter(&local, &attachment).read_header_event(source.read_header_event());
        if cancel_local {
            local.request_cancellation();
        } else {
            attachment.request_cancellation();
        }
        assert_filter_cancelled(futures::executor::block_on(read.as_mut()));
        assert_eq!(probe.polls.get(), 0);
        assert_eq!(
            probe.future_drops.get(),
            1,
            "the unpolled source drops before return"
        );
        drop(read);
    }
}

/// A session-cancelled head read stops and drops the request source before the
/// send returns and its reservation is released, ready or pending.
#[test]
fn cancelled_request_head_retires_the_source_before_the_reservation() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                for polled in [false, true] {
                    let attached = Attached::new(&context);
                    let (source, probe) = probe_source(HeadMode::Pending);
                    *probe.log.borrow_mut() = Some(Rc::clone(&attached.events));
                    let mut request = attached.send(source, reserved);
                    if polled {
                        assert!(
                            request
                                .as_mut()
                                .poll(&mut Context::from_waker(Waker::noop()))
                                .is_pending()
                        );
                    }
                    attached.cancel(attachment);
                    let error = request.await.err().expect("cancelled head read");
                    assert_session_cancelled(&error);
                    assert_eq!(
                        *attached.events.borrow(),
                        ["attached", "source-stopped", "source-dropped", "detached"]
                    );
                    attached.finish_cancelled().await;
                }
            }
        }
    });
}

#[test]
fn cancellation_inside_the_header_poll_wins_over_its_result() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                for mode in [HeadMode::Ready, HeadMode::Failing] {
                    let attached = Rc::new(Attached::new(&context));
                    let (source, probe) = probe_source(mode);
                    let cancel = Rc::clone(&attached);
                    *probe.on_poll.borrow_mut() = Some(Box::new(move || cancel.cancel(attachment)));
                    let request = attached.send(source, reserved);
                    let error = request.await.err().expect("cancelled in the head poll");
                    assert_session_cancelled(&error);
                    assert_eq!(probe.polls.get(), 1);
                    assert_eq!(probe.future_drops.get(), 1);
                    let attached = Rc::try_unwrap(attached).ok().expect("sole owner");
                    attached.finish_cancelled().await;
                }
            }
        }
    });
}

#[test]
fn pending_request_head_wakes_for_cancellation_or_headers() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                let attached = Attached::new(&context);
                let (source, probe) = probe_source(HeadMode::Pending);
                let mut request = attached.send(source, reserved);
                let wake = Arc::new(CountingWake::default());
                let waker = Waker::from(Arc::clone(&wake));
                assert!(
                    request
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(probe.polls.get(), 1);
                assert_eq!(head_wait_probe::snapshot().live, 2);
                attached.cancel(attachment);
                assert!(
                    wake.0.load(Ordering::SeqCst) > 0,
                    "cancellation wakes the read"
                );
                let error = request.await.err().expect("cancelled pending head");
                assert_session_cancelled(&error);
                assert_eq!(probe.polls.get(), 1, "the source is not repolled");
                assert_eq!(
                    probe.live_waits_at_future_drop.get(),
                    Some(2),
                    "the source future drops before the waits"
                );
                assert_eq!(head_wait_probe::snapshot().live, 0);
                attached.finish_cancelled().await;
            }

            let attached = Attached::new(&context);
            let (source, probe) = probe_source(HeadMode::Pending);
            let mut request = attached.send(source, reserved);
            let wake = Arc::new(CountingWake::default());
            let waker = Waker::from(Arc::clone(&wake));
            assert!(
                request
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            probe.release();
            assert!(wake.0.load(Ordering::SeqCst) > 0);
            let response = request.await.expect("released headers start the request");
            assert_eq!(probe.polls.get(), 2);
            assert_eq!(head_wait_probe::snapshot().live, 0);
            drop(response);
            attached.finish_cancelled().await;
        }
    });
}

#[test]
fn cross_thread_cancellation_racing_the_first_head_poll_wins() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            let attached = Attached::new(&context);
            let (source, probe) = probe_source(HeadMode::Pending);
            let remote = Arc::clone(&attached.attachment);
            *probe.on_poll.borrow_mut() = Some(Box::new(move || {
                std::thread::spawn(move || {
                    remote.request_cancellation();
                })
                .join()
                .expect("remote cancellation");
            }));
            let request = attached.send(source, reserved);
            let error = request
                .await
                .err()
                .expect("remote cancellation during the head poll wins");
            assert_session_cancelled(&error);
            assert!(
                probe.waker.borrow().is_some(),
                "the source never woke the read"
            );
            assert_eq!(probe.polls.get(), 1);
            attached.finish_cancelled().await;
        }
    });

    for iteration in 0..1_000_usize {
        let local = Arc::new(CancellationSource::new());
        let attachment = Arc::new(CancellationSource::new());
        let read = filter(&local, &attachment).read_header_event(Box::pin(std::future::pending()));
        let remote = if iteration % 2 == 0 {
            Arc::clone(&local)
        } else {
            Arc::clone(&attachment)
        };
        let canceller = std::thread::spawn(move || {
            remote.request_cancellation();
        });
        assert_filter_cancelled(wait(read));
        canceller.join().expect("remote cancellation");
    }
}

#[test]
fn request_head_source_error_is_returned_unchanged() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            let attached = Attached::new(&context);
            let (source, probe) = probe_source(HeadMode::Failing);
            let error = attached
                .send(source, reserved)
                .await
                .err()
                .expect("the source failure is returned");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
            assert_eq!(error.message(), b"probe head failure");
            assert_eq!(probe.polls.get(), 1);
            attached.finish_cancelled().await;
        }
    });

    let local = CancellationSource::new();
    let attachment = CancellationSource::new();
    let (mut source, _probe) = probe_source(HeadMode::Failing);
    let error = futures::executor::block_on(
        filter(&local, &attachment).read_header_event(source.read_header_event()),
    )
    .err()
    .expect("the source failure is returned");
    assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
    assert_eq!(error.message(), b"probe head failure");
}

#[test]
fn cancellation_from_the_source_destructor_wins_over_ready_headers() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                let attached = Rc::new(Attached::new(&context));
                let (source, probe) = probe_source(HeadMode::Ready);
                let cancel = Rc::clone(&attached);
                *probe.on_future_drop.borrow_mut() =
                    Some(Box::new(move || cancel.cancel(attachment)));
                let error = attached
                    .send(source, reserved)
                    .await
                    .err()
                    .expect("destructor cancellation wins");
                assert_session_cancelled(&error);
                assert_eq!(probe.polls.get(), 1);
                let attached = Rc::try_unwrap(attached).ok().expect("sole owner");
                attached.finish_cancelled().await;
            }
        }
    });

    let local = CancellationSource::new();
    let attachment = Rc::new(CancellationSource::new());
    let (mut source, probe) = probe_source(HeadMode::Ready);
    let cancel = Rc::clone(&attachment);
    *probe.on_future_drop.borrow_mut() = Some(Box::new(move || {
        cancel.request_cancellation();
    }));
    assert_filter_cancelled(futures::executor::block_on(
        filter(&local, &attachment).read_header_event(source.read_header_event()),
    ));
}

#[test]
fn abandoned_pending_head_read_releases_registrations_and_reservation() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            for attachment in [false, true] {
                let attached = Attached::new(&context);
                let (source, probe) = probe_source(HeadMode::Pending);
                let mut request = attached.send(source, reserved);
                let wake = Arc::new(CountingWake::default());
                let waker = Waker::from(Arc::clone(&wake));
                assert!(
                    request
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(head_wait_probe::snapshot().live, 2);
                assert_eq!(*attached.events.borrow(), ["attached"]);
                drop(request);
                assert_eq!(probe.future_drops.get(), 1);
                assert_eq!(probe.source_drops.get(), 1, "the source drops once");
                assert_eq!(head_wait_probe::snapshot().live, 0);
                assert_eq!(
                    *attached.events.borrow(),
                    ["attached", "detached"],
                    "the reservation was released"
                );
                let wakes = wake.0.load(Ordering::SeqCst);
                attached.cancel(attachment);
                assert_eq!(
                    wake.0.load(Ordering::SeqCst),
                    wakes,
                    "no token registration outlived the read"
                );
                attached.finish_cancelled().await;
            }
        }
    });
}

#[test]
fn wait_closed_before_and_after_completion_returns_equal_results() {
    run_on_worker(|context| async move {
        for cancelled in [false, true] {
            let attached = Attached::new(&context);
            let mut before = attached.session.wait_closed();
            assert!(
                before
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if cancelled {
                attached.session.cancel();
            } else {
                attached.session.close_when_idle().unwrap();
            }
            let first = before.await;
            let second = attached.session.wait_closed().await;
            match (first, second) {
                (Ok(first), Ok(second)) => {
                    assert!(!cancelled);
                    assert_eq!(first, second);
                }
                (Err(first), Err(second)) => {
                    assert!(cancelled);
                    assert_eq!(first.code(), second.code());
                    assert_eq!(first.message(), second.message());
                }
                _ => panic!("completion observations differ"),
            }
        }
    });
}

#[test]
fn concurrent_wait_closed_futures_both_resolve() {
    run_on_worker(|context| async move {
        let attached = Attached::new(&context);
        let first = attached.session.wait_closed();
        let second = attached.session.wait_closed();
        let close = async {
            tokio::task::yield_now().await;
            attached.session.close_when_idle().unwrap();
        };
        let (first, second, ()) = tokio::join!(first, second, close);
        assert_eq!(first.unwrap(), second.unwrap());
    });
}

fn summary() -> UniplexRunSummary {
    UniplexRunSummary {
        bytes_read: 3,
        bytes_scheduled: 5,
        bytes_written: 5,
        reset: false,
    }
}

#[test]
fn wait_closed_after_an_unpublished_scope_drop_fails_session_scope_failed() {
    let (publisher, completion) = session_completion_for_test();
    let wake = Arc::new(CountingWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut pending = wait_session_completion(&completion);
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(publisher);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    for result in [
        futures::executor::block_on(pending),
        futures::executor::block_on(wait_session_completion(&completion)),
    ] {
        let error = result.expect_err("an unpublished scope fails");
        assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);
        assert_eq!(error.message(), b"session scope failed");
    }
}

#[test]
fn published_result_wins_over_a_later_scope_drop() {
    let (publisher, completion) = session_completion_for_test();
    let mut pending = wait_session_completion(&completion);
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    publisher.publish(Ok(summary()));
    assert_eq!(futures::executor::block_on(pending).unwrap(), summary());
    assert_eq!(
        futures::executor::block_on(wait_session_completion(&completion)).unwrap(),
        summary()
    );

    let (publisher, completion) = session_completion_for_test();
    publisher.publish(Err(HttpError::with_message(
        HttpErrorCode::CORO_CANCELLED,
        b"published failure",
    )));
    let error = futures::executor::block_on(wait_session_completion(&completion))
        .expect_err("the published failure");
    assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED);
    assert_eq!(error.message(), b"published failure");
}

#[test]
fn dropping_every_session_clone_with_a_live_wait_closed_still_drains() {
    run_on_worker(|context| async move {
        let Attached {
            session,
            attachment: _attachment,
            events: _events,
            _peer,
        } = Attached::new(&context);
        let clone = session.clone();
        let mut closed = session.wait_closed();
        assert!(
            closed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(session);
        drop(clone);
        tokio::time::timeout(Duration::from_secs(5), closed)
            .await
            .expect("dropping the last session handle drains the session")
            .expect("the idle session closes cleanly");
    });
}

#[test]
fn ready_request_head_builds_no_cancellation_waits() {
    run_on_worker(|context| async move {
        for reserved in [false, true] {
            let attached = Attached::new(&context);
            let (source, probe) = probe_source(HeadMode::Ready);
            let before = head_wait_probe::snapshot();
            let response = attached
                .send(source, reserved)
                .await
                .expect("ready headers start the request");
            assert_eq!(head_wait_probe::snapshot().built, before.built);
            assert_eq!(probe.polls.get(), 1);
            drop(response);
            attached.finish_cancelled().await;

            let attached = Attached::new(&context);
            let (source, probe) = probe_source(HeadMode::Pending);
            let before = head_wait_probe::snapshot();
            let mut request = attached.send(source, reserved);
            assert!(
                request
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(
                probe.polls.get(),
                1,
                "the source is polled once in that poll"
            );
            assert_eq!(head_wait_probe::snapshot().built, before.built + 2);
            probe.release();
            let response = request.await.expect("released headers start the request");
            assert_eq!(head_wait_probe::snapshot().built, before.built + 2);
            drop(response);
            attached.finish_cancelled().await;
        }
    });

    let local = CancellationSource::new();
    let attachment = CancellationSource::new();
    let (mut source, _probe) = probe_source(HeadMode::Ready);
    let before = head_wait_probe::snapshot();
    futures::executor::block_on(
        filter(&local, &attachment).read_header_event(source.read_header_event()),
    )
    .expect("ready headers");
    assert_eq!(head_wait_probe::snapshot(), before);
}

#[test]
fn dropped_wait_closed_future_leaves_no_live_waiter() {
    let (publisher, completion) = session_completion_for_test();
    let mut dropped = wait_session_completion(&completion);
    assert!(
        dropped
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(completion_waiters_for_test(&completion), 1);
    drop(dropped);
    let live = wait_session_completion(&completion);
    assert_eq!(
        completion_waiters_for_test(&completion),
        1,
        "registration pruned the dropped waiter"
    );
    publisher.publish(Ok(summary()));
    assert_eq!(futures::executor::block_on(live).unwrap(), summary());

    run_on_worker(|context| async move {
        let attached = Attached::new(&context);
        let mut dropped = attached.session.wait_closed();
        assert!(
            dropped
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(dropped);
        let live = attached.session.wait_closed();
        assert_eq!(attached.session.completion_waiters_for_test(), 1);
        attached.session.close_when_idle().unwrap();
        live.await.expect("the idle session closes cleanly");
    });
}

struct PanickingWake;

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("wait_closed waker panic");
    }
}

#[test]
fn panicking_wait_closed_waker_does_not_skip_the_others() {
    let (publisher, completion) = session_completion_for_test();
    let panicking = Waker::from(Arc::new(PanickingWake));
    let counting = Arc::new(CountingWake::default());
    let counted = Waker::from(Arc::clone(&counting));
    let mut first = wait_session_completion(&completion);
    let mut second = wait_session_completion(&completion);
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&panicking))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&counted))
            .is_pending()
    );
    let payload: Box<dyn Any + Send> =
        catch_unwind(AssertUnwindSafe(|| publisher.publish(Ok(summary()))))
            .expect_err("the first waker panic leaves the publisher");
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"wait_closed waker panic")
    );
    assert_eq!(
        counting.0.load(Ordering::SeqCst),
        1,
        "later waiters are still woken"
    );
    for pending in [first, second] {
        assert_eq!(futures::executor::block_on(pending).unwrap(), summary());
    }
}
