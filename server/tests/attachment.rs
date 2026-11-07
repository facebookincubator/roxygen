use std::future::Future;
use std::io;
use std::net::TcpListener;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use roxygen_client::HttpClient;
use roxygen_client::RequestSpec;
use roxygen_client::connector::ConnectionParams;
use roxygen_client::connector::HttpConnector;
use roxygen_client::connector::SessionParams;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::OriginSecurity;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::connection::UpstreamOrigin;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::url::Url;
use roxygen_core::url::UrlMode;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::established::adopt_local_with_completion;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::oneshot;

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionInfo;
use crate::connection::CompletionPauseForTest;
use crate::connection::ServeTransportOptions;
use crate::connection::ServerConnection;
use crate::connection::serve_transport;

const TIMEOUT: Duration = Duration::from_secs(10);

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

fn limits() -> ByteStreamLimits {
    ByteStreamLimits::new(4096, 4096, 8).expect("bounded transport")
}

fn socket_address(address: std::net::SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

#[derive(Default)]
struct Evidence {
    events: Mutex<Vec<&'static str>>,
    contexts: Mutex<Vec<Arc<ConnectionContext>>>,
    handler_calls: AtomicUsize,
}

struct RecordingHandler(Arc<Evidence>);

impl HttpHandler for RecordingHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.0.handler_calls.fetch_add(1, Ordering::SeqCst);
        let connection = Arc::clone(
            context
                .connection_context()
                .expect("trusted connection metadata"),
        );
        assert_eq!(context.peer_identity(), connection.peer_identity());
        self.0.contexts.lock().expect("contexts").push(connection);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            assert_eq!(
                head.headers()
                    .headers()
                    .get_single_or_empty(b"x-peer-identity"),
                b"header-forgery"
            );
            let mut body = BufQueue::new(128 * 1024);
            if !head.eom() {
                loop {
                    let event = request.read_body_event(1024).await?;
                    if let Some(data) = event.as_body() {
                        body.append(&mut data.clone_shared()).expect("bounded body");
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

struct AcceptedObserver(Arc<Evidence>);

impl AcceptedConnectionObserver for AcceptedObserver {
    fn on_accepted(&self, connection: &ConnectionInfo) {
        assert_eq!(
            connection.peer_identity.as_deref(),
            Some(b"transport-identity".as_slice())
        );
        assert!(
            !connection.zero_copy_enabled,
            "adoption cannot invent socket capabilities"
        );
        self.0.events.lock().expect("events").push("accepted");
    }

    fn on_registered(&self) {
        self.0.events.lock().expect("events").push("registered");
    }
}

struct SessionObserver(Arc<Evidence>);

impl LifecycleObserver for SessionObserver {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::Destroy) {
            self.0.events.lock().expect("events").push("destroy");
        }
    }
}

#[test]
fn server_attachment() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("application listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let address = listener.local_addr().expect("listener address");
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 4,
        })
        .expect("owner starts");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker exists");
        let evidence = Arc::new(Evidence::default());
        let server_evidence = Arc::clone(&evidence);
        let accepted = worker
            .call_local(move |context| async move {
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("application reactor");
                let (io, remote) = listener.accept().await.expect("application accept");
                let transport = adopt_local(
                    &context,
                    EstablishedIo::new(
                        io,
                        TransportEndpoint::Socket(socket_address(address)),
                        TransportEndpoint::Socket(socket_address(remote)),
                        Some(PeerIdentity::new(b"transport-identity")),
                    ),
                    limits(),
                )
                .expect("accepted I/O adopted");
                let mut options =
                    ServeTransportOptions::new(protocol, TransportSecurity::Plaintext);
                options.upstream_origin = Some(UpstreamOrigin::new(
                    "configured-origin:443",
                    OriginSecurity::Https,
                ));
                let filter_evidence = Arc::clone(&server_evidence);
                options.new_connection_filter =
                    Some(Arc::new(move |connection: &ConnectionInfo| {
                        assert_eq!(connection.context.security(), TransportSecurity::Plaintext);
                        assert_eq!(connection.context.http_protocol(), Some(protocol));
                        assert_eq!(connection.context.requested_server_name(), None);
                        assert_eq!(connection.context.negotiated_application_protocol(), None);
                        assert_eq!(
                            connection
                                .context
                                .upstream_origin()
                                .expect("configured origin")
                                .authority(),
                            "configured-origin:443"
                        );
                        filter_evidence
                            .events
                            .lock()
                            .expect("events")
                            .push("filter");
                        true
                    }));
                options.connection_observer =
                    Some(Arc::new(AcceptedObserver(Arc::clone(&server_evidence))));
                options.zero_copy_enable_threshold = 1;
                let lifecycle_evidence = Arc::clone(&server_evidence);
                options.session.lifecycle_observer_factory = Some(Arc::new(move || {
                    lifecycle_evidence
                        .events
                        .lock()
                        .expect("events")
                        .push("lifecycle");
                    Rc::new(SessionObserver(Arc::clone(&lifecycle_evidence)))
                }));
                let connection = serve_transport(
                    &context,
                    transport,
                    Rc::new(RecordingHandler(server_evidence)),
                    options,
                )
                .expect("downstream attachment");
                connection
                    .wait_closed()
                    .await
                    .expect("server session joins");
            })
            .expect("accept initializer admitted");
        let request = worker
            .call_local(move |context| async move {
                let client = HttpConnector::connect(
                    &context,
                    socket_address(address),
                    TIMEOUT,
                    ConnectionParams {
                        plaintext_protocol: protocol,
                        ..ConnectionParams::default()
                    },
                    SessionParams::default(),
                    CancellationToken::never(),
                )
                .await
                .expect("native client connects");
                let payload = vec![0x57; 64 * 1024];
                let url = Url::parse(
                    "http://untrusted-authority.example/upload",
                    false,
                    UrlMode::StrictCompat,
                );
                let mut request = RequestSpec::post(url, &payload);
                request.headers.insert(
                    Box::from(b"x-peer-identity".as_slice()),
                    Box::from(b"header-forgery".as_slice()),
                );
                request.headers.insert(
                    Box::from(b"forwarded".as_slice()),
                    Box::from(b"proto=https;host=header-origin".as_slice()),
                );
                let mut response = HttpClient::request(&client, None, request)
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
                client.close_when_idle().expect("native client drains");
                client.wait_closed().await.expect("native client joins");
            })
            .expect("client initializer admitted");
        wait(async {
            request.await.expect("client command finishes");
            accepted.await.expect("server command finishes");
            runtime.quiesce(TIMEOUT).await.expect("all scopes finish");
        });
        assert_eq!(owner.join().expect("physical join").workers_forced, 0);
        assert_eq!(evidence.handler_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *evidence.events.lock().expect("events"),
            ["filter", "accepted", "lifecycle", "destroy"]
        );
        let contexts = evidence.contexts.lock().expect("contexts");
        let connection = &contexts[0];
        assert_eq!(
            connection.peer_identity(),
            Some(b"transport-identity".as_slice())
        );
        assert_eq!(connection.security(), TransportSecurity::Plaintext);
        assert_eq!(
            connection.upstream_origin().expect("origin").security(),
            OriginSecurity::Https
        );
        assert_eq!(
            connection.local_endpoint(),
            &TransportEndpoint::Socket(socket_address(address))
        );
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

struct MemoryIo {
    request: &'static [u8],
    written: Arc<Mutex<Vec<u8>>>,
    drops: Arc<Mutex<Vec<ThreadId>>>,
    owner: ThreadId,
    flush: Option<Gate>,
    shutdown: Option<Gate>,
}

impl Drop for MemoryIo {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.lock().expect("drops").push(self.owner);
    }
}

impl AsyncRead for MemoryIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.request.is_empty() {
            return Poll::Pending;
        }
        let count = self.request.len().min(output.remaining());
        output.put_slice(&self.request[..count]);
        self.request = &self.request[count..];
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MemoryIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.written
            .lock()
            .expect("wire bytes")
            .extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(gate) = self.flush.as_mut() {
            if gate.poll(context).is_pending() {
                return Poll::Pending;
            }
            let _ = self.flush.take();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(gate) = self.shutdown.as_mut() {
            if gate.poll(context).is_pending() {
                return Poll::Pending;
            }
            let _ = self.shutdown.take();
        }
        Poll::Ready(Ok(()))
    }
}

#[test]
fn server_attachment_rejection_and_early_drop_dispose_on_owner() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let quiescing = runtime.clone();
    wait(
        worker
            .call_local(move |context| async move {
                for case in 0..5 {
                    let drops = Arc::new(Mutex::new(Vec::new()));
                    let evidence = Arc::new(Evidence::default());
                    let transport = adopt_local(
                        &context,
                        EstablishedIo::new(
                            MemoryIo {
                                request: b"",
                                written: Arc::new(Mutex::new(Vec::new())),
                                drops: Arc::clone(&drops),
                                owner: std::thread::current().id(),
                                flush: None,
                                shutdown: None,
                            },
                            TransportEndpoint::UnnamedUnix,
                            TransportEndpoint::UnixPath(Box::from(b"/trusted/socket".as_slice())),
                            None,
                        ),
                        limits(),
                    )
                    .expect("I/O adopted");
                    let mut options = ServeTransportOptions::new(
                        if case == 0 {
                            CodecProtocol::Http3
                        } else {
                            CodecProtocol::Http11
                        },
                        TransportSecurity::Plaintext,
                    );
                    let cancellation = CancellationSource::new();
                    if case == 1 {
                        cancellation.request_cancellation();
                        options.cancellation = cancellation.token();
                    }
                    if case == 2 || case == 3 {
                        options.new_connection_filter =
                            Some(Arc::new(move |_: &ConnectionInfo| {
                                assert_ne!(case, 3, "injected admission panic");
                                false
                            }));
                    }
                    let result = serve_transport(
                        &context,
                        transport,
                        Rc::new(RecordingHandler(Arc::clone(&evidence))),
                        options,
                    );
                    if case == 4 {
                        let connection = result.expect("connection attaches");
                        let closed = connection.wait_closed();
                        drop(connection.wait_closed());
                        drop(connection);
                        assert_eq!(
                            closed.await.expect_err("early drop cancels").code(),
                            HttpErrorCode::CORO_CANCELLED
                        );
                    } else {
                        let error = result.err().expect("admission rejected");
                        assert_eq!(
                            error.code(),
                            if case == 1 {
                                HttpErrorCode::CORO_CANCELLED
                            } else {
                                HttpErrorCode::CONNECT_ERROR
                            }
                        );
                    }
                    assert_eq!(evidence.handler_calls.load(Ordering::SeqCst), 0);
                    assert_eq!(*drops.lock().expect("drops"), [std::thread::current().id()]);
                }
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
                .expect("I/O adopted while accepting");
                drop(quiescing.quiesce(TIMEOUT));
                assert_eq!(context.handle().state(), WorkerState::Open);
                let error = serve_transport(
                    &context,
                    transport,
                    Rc::new(RecordingHandler(Arc::new(Evidence::default()))),
                    ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
                )
                .err()
                .expect("new server session rejected during grace");
                assert_eq!(error.code(), HttpErrorCode::DROPPED);
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

#[test]
fn server_attachment_handle_stays_local() {
    struct Invalid;
    trait AmbiguousIfSend<Marker> {
        fn marker() {}
    }
    impl<Value: ?Sized> AmbiguousIfSend<()> for Value {}
    impl<Value: ?Sized + Send> AmbiguousIfSend<Invalid> for Value {}
    let _ = <ServerConnection as AmbiguousIfSend<_>>::marker;
}

#[test]
fn handler_connection_context_preserves_identity_and_liveness() {
    let mut owner = KeepAliveOwner::new(());
    let original = HandlerSessionContext::acquire_with_peer_identity(
        &owner,
        Some(Rc::from(b"real-peer".as_slice())),
    )
    .expect("live context");
    let metadata = Arc::new(
        ConnectionContext::new(
            TransportEndpoint::UnnamedUnix,
            TransportEndpoint::UnixAbstract(Box::from(b"peer".as_slice())),
            TransportSecurity::Plaintext,
            Some(Arc::from(b"real-peer".as_slice())),
        )
        .with_upstream_origin(Some(UpstreamOrigin::new(
            "upstream:443",
            OriginSecurity::Https,
        ))),
    );
    let enriched = original
        .clone()
        .with_connection_context(Arc::clone(&metadata))
        .expect("matching metadata");
    assert_eq!(enriched.peer_identity(), original.peer_identity());
    assert!(Arc::ptr_eq(
        enriched.connection_context().expect("snapshot"),
        &metadata
    ));
    let impostor = Arc::new(ConnectionContext::new(
        TransportEndpoint::Unknown,
        TransportEndpoint::Unknown,
        TransportSecurity::Tls,
        Some(Arc::from(b"different-peer".as_slice())),
    ));
    assert_eq!(
        original
            .clone()
            .with_connection_context(impostor)
            .err()
            .expect("identity mismatch rejected")
            .code(),
        HttpErrorCode::INADEQUATE_SECURITY
    );
    let changed_connection = Arc::new(ConnectionContext::new(
        TransportEndpoint::Unknown,
        TransportEndpoint::Unknown,
        TransportSecurity::Tls,
        Some(Arc::from(b"real-peer".as_slice())),
    ));
    assert!(
        enriched
            .clone()
            .with_connection_context(changed_connection)
            .is_err(),
        "enrichment cannot overwrite trusted endpoints/security"
    );
    let mut finalized = Box::pin(owner.finalize());
    assert!(
        finalized
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(original);
    assert!(
        finalized
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(enriched);
    assert!(
        finalized
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert_eq!(
        metadata.peer_identity(),
        Some(b"real-peer".as_slice()),
        "metadata alone is not a keep-alive"
    );
}

struct QuiescingHandler {
    worker: WorkerHandle,
    runtime: RuntimeHandle,
    response: std::cell::RefCell<Option<oneshot::Receiver<()>>>,
    started: std::cell::RefCell<Option<oneshot::Sender<()>>>,
}

impl HttpHandler for QuiescingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.worker.submit(|_| {}).expect("fill command queue");
        assert_eq!(self.worker.submit(|_| {}), Err(SubmitError::Overloaded));
        drop(self.runtime.quiesce(TIMEOUT));
        assert!(!self.worker.is_accepting());
        let response = self.response.borrow_mut().take().expect("one request");
        self.started
            .borrow_mut()
            .take()
            .expect("one start")
            .send(())
            .expect("root observes handler");
        Box::pin(async move {
            request.read_header_event().await?;
            response.await.expect("root releases response");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(
                    200,
                    Some(BufQueue::from_bytes(Bytes::from_static(b"response"), 8).expect("body")),
                )
                .expect("response"),
            )))
        })
    }
}

#[test]
fn server_attachment_quiescence_joins_response_flush_and_shutdown() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 1,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let worker_runtime = runtime.clone();
    let (handler_started, handler_started_rx) = oneshot::channel();
    let (response_release, response_release_rx) = oneshot::channel();
    let (flush_started, flush_started_rx) = oneshot::channel();
    let (flush_release, flush_release_rx) = oneshot::channel();
    let (shutdown_started, shutdown_started_rx) = oneshot::channel();
    let (shutdown_release, shutdown_release_rx) = oneshot::channel();
    let drops = Arc::new(Mutex::new(Vec::new()));
    let worker_drops = Arc::clone(&drops);
    let written = Arc::new(Mutex::new(Vec::new()));
    let worker_written = Arc::clone(&written);
    let initialized = worker
        .call_local(move |context| async move {
            let transport = adopt_local(
                &context,
                EstablishedIo::new(
                    MemoryIo {
                        request: b"GET / HTTP/1.1\r\nHost: header.example\r\n\r\n",
                        written: worker_written,
                        drops: worker_drops,
                        owner: std::thread::current().id(),
                        flush: Some(Gate {
                            started: Some(flush_started),
                            release: flush_release_rx,
                        }),
                        shutdown: Some(Gate {
                            started: Some(shutdown_started),
                            release: shutdown_release_rx,
                        }),
                    },
                    TransportEndpoint::UnnamedUnix,
                    TransportEndpoint::UnixPath(Box::from(b"/trusted/peer".as_slice())),
                    None,
                ),
                limits(),
            )
            .expect("I/O adopted");
            let handler = Rc::new(QuiescingHandler {
                worker: context.handle().clone(),
                runtime: worker_runtime,
                response: std::cell::RefCell::new(Some(response_release_rx)),
                started: std::cell::RefCell::new(Some(handler_started)),
            });
            let connection = serve_transport(
                &context,
                transport,
                handler,
                ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext),
            )
            .expect("connection attaches");
            context
                .register_scope(async move {
                    connection
                        .wait_closed()
                        .await
                        .expect("server session receipt");
                })
                .expect("connection owner scope")
                .activate()
                .expect("owner scope activates")
                .detach();
        })
        .expect("initializer admitted");
    wait(async {
        initialized
            .await
            .expect("initializer ends before exchange cleanup");
        handler_started_rx
            .await
            .expect("accepted request closes admission");
        let mut quiesced = Box::pin(runtime.quiesce(TIMEOUT));
        assert!(
            quiesced
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
        response_release.send(()).expect("allow response");
        flush_started_rx.await.expect("response flush pending");
        assert!(
            quiesced
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(worker.state(), WorkerState::Open);
        assert!(drops.lock().expect("drops").is_empty());
        flush_release.send(()).expect("allow flush");
        shutdown_started_rx.await.expect("shutdown pending");
        assert!(
            quiesced
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        shutdown_release.send(()).expect("allow shutdown");
        quiesced.await.expect("session and transport scopes join");
    });
    let report = owner.join().expect("physical join");
    assert_eq!(report.workers_forced, 0);
    assert_eq!(report.workers_joined, 1);
    let drops = drops.lock().expect("drops");
    assert_eq!(drops.len(), 1);
    assert_ne!(drops[0], std::thread::current().id());
    let wire = written.lock().expect("wire bytes");
    assert!(wire.starts_with(b"HTTP/1.1 200"));
    assert!(wire.ends_with(b"response"));
}

struct CompletionServerIo {
    inner: MemoryIo,
    write_pending: bool,
}

impl AsyncRead for CompletionServerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, output)
    }
}

impl AsyncWrite for CompletionServerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_pending = true;
        Pin::new(&mut self.inner).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if std::mem::take(&mut self.write_pending) {
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.inner).poll_flush(context)
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct CompletionResponseHandler;

impl HttpHandler for CompletionResponseHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            assert!(request.read_header_event().await?.eom());
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(
                    200,
                    Some(BufQueue::from_bytes(Bytes::from_static(b"response"), 8).expect("body")),
                )
                .expect("response"),
            )))
        })
    }
}

#[test]
fn server_attachment_io_control_cancels_after_wait_closed() {
    for hold_shutdown in [false, true] {
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("owner");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("worker");
        let drops = Arc::new(Mutex::new(Vec::new()));
        let worker_drops = Arc::clone(&drops);
        let written = Arc::new(Mutex::new(Vec::new()));
        let worker_written = Arc::clone(&written);
        let initialized = worker.call_local(move |context| async move {
            let (flush_started, flush_started_rx) = oneshot::channel();
            let (flush_release, flush_release_rx) = oneshot::channel();
            let (shutdown_started, shutdown_started_rx) = oneshot::channel();
            let (shutdown_release, shutdown_release_rx) = oneshot::channel();
            let (transport, control) = adopt_local_with_completion(
                &context,
                EstablishedIo::new(CompletionServerIo {
                    inner: MemoryIo {
                        request: b"GET / HTTP/1.1\r\nHost: server.example\r\nConnection: close\r\n\r\n",
                        written: worker_written, drops: Arc::clone(&worker_drops), owner: std::thread::current().id(),
                        flush: Some(Gate { started: Some(flush_started), release: flush_release_rx }),
                        shutdown: Some(Gate { started: Some(shutdown_started), release: shutdown_release_rx }),
                    },
                    write_pending: false,
                }, TransportEndpoint::UnnamedUnix, TransportEndpoint::Unknown, None), limits(),
            ).expect("completion-aware adoption");
            let connection = serve_transport(&context, transport, Rc::new(CompletionResponseHandler), ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext)).expect("server connection");
            flush_started_rx.await.expect("actual close flush pending after response writes");
            let flush_release = if hold_shutdown {
                flush_release.send(()).expect("allow flush only");
                shutdown_started_rx.await.expect("actual close shutdown pending");
                None
            } else {
                Some(flush_release)
            };
            connection.wait_closed().await.expect("native server succeeds before transport cancellation");
            connection.wait_closed().await.expect("sticky native success");
            assert!(worker_drops.lock().expect("drops").is_empty());
            assert!(!shutdown_release.is_closed());
            assert!(Box::pin(control.completion().wait_retired()).as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            drop(connection);
            (control, flush_release, shutdown_release)
        }).expect("initializer admitted");
        wait(async {
            let (control, flush_release, shutdown_release) = initialized
                .await
                .expect("native server owner returned and dropped");
            let receipt = control.completion();
            let mut quiesced = Box::pin(runtime.quiesce(TIMEOUT));
            assert!(
                quiesced
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert_eq!(worker.submit(|_| {}), Err(SubmitError::Closed));
            assert!(drops.lock().expect("drops").is_empty());
            assert!(
                Box::pin(receipt.wait_retired())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            control.cancel();
            receipt
                .wait_retired()
                .await
                .expect("I/O and drain scope retire");
            receipt.wait_retired().await.expect("sticky retirement");
            assert!(shutdown_release.is_closed());
            if let Some(release) = flush_release.as_ref() {
                assert!(release.is_closed());
            }
            assert_eq!(
                drops.lock().expect("drops").as_slice(),
                &[worker.owner_thread()]
            );
            quiesced.await.expect("cooperative cleanup within grace");
        });
        let report = owner.join().expect("physical join");
        assert_eq!(report.workers_forced, 0);
        assert_eq!(report.workers_joined, 1);
        let wire = written.lock().expect("response wire");
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        assert!(wire.ends_with(b"response"));
    }
}

struct TerminalDrainIo {
    inner: CompletionServerIo,
    fail_write: bool,
    write_attempts: Arc<AtomicUsize>,
}

impl AsyncRead for TerminalDrainIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, output)
    }
}

impl AsyncWrite for TerminalDrainIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail_write {
            Poll::Ready(Err(io::Error::other("terminal drain native write failure")))
        } else {
            Pin::new(&mut self.inner).poll_write(context, bytes)
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

struct TerminalDrainGates {
    publisher: Option<oneshot::Sender<()>>,
    flush: Option<oneshot::Sender<()>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for TerminalDrainGates {
    fn drop(&mut self) {
        drop(self.publisher.take());
        for release in [self.flush.take(), self.shutdown.take()]
            .into_iter()
            .flatten()
        {
            let _ = release.send(());
        }
    }
}

async fn terminal_drain_catch_unwind<Work: Future>(
    work: Work,
) -> std::thread::Result<Work::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|context| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(context)))
        {
            Ok(result) => result.map(Ok),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

#[test]
fn server_terminal_drain_before_publication_is_idempotent() {
    for fail_write in [false, true] {
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 1,
        })
        .expect("terminal-drain runtime");
        let runtime = owner.handle();
        let worker = runtime.worker(0).expect("native worker");
        let drops = Arc::new(Mutex::new(Vec::new()));
        let worker_drops = Arc::clone(&drops);
        let written = Arc::new(Mutex::new(Vec::new()));
        let worker_written = Arc::clone(&written);
        let write_attempts = Arc::new(AtomicUsize::new(0));
        let worker_write_attempts = Arc::clone(&write_attempts);
        let initialized = worker
            .call_local(move |context| async move {
                let (publisher_entered, publisher_entered_rx) = oneshot::channel();
                let (publisher_release, publisher_release_rx) = oneshot::channel();
                let (flush_started, flush_started_rx) = oneshot::channel();
                let (flush_release, flush_release_rx) = oneshot::channel();
                let (shutdown_started, shutdown_started_rx) = oneshot::channel();
                let (shutdown_release, shutdown_release_rx) = oneshot::channel();
                let mut gates = TerminalDrainGates {
                    publisher: Some(publisher_release),
                    flush: Some(flush_release),
                    shutdown: Some(shutdown_release),
                };
                let outcome =
                    terminal_drain_catch_unwind(tokio::time::timeout(TIMEOUT / 2, async {
                        let request =
                            b"GET / HTTP/1.1\r\nHost: server.example\r\nConnection: close\r\n\r\n";
                        let (transport, io_control) = adopt_local_with_completion(
                            &context,
                            EstablishedIo::new(
                                TerminalDrainIo {
                                    inner: CompletionServerIo {
                                        inner: MemoryIo {
                                            request,
                                            written: Arc::clone(&worker_written),
                                            drops: Arc::clone(&worker_drops),
                                            owner: std::thread::current().id(),
                                            flush: Some(Gate {
                                                started: Some(flush_started),
                                                release: flush_release_rx,
                                            }),
                                            shutdown: Some(Gate {
                                                started: Some(shutdown_started),
                                                release: shutdown_release_rx,
                                            }),
                                        },
                                        write_pending: false,
                                    },
                                    fail_write,
                                    write_attempts: Arc::clone(&worker_write_attempts),
                                },
                                TransportEndpoint::UnnamedUnix,
                                TransportEndpoint::Unknown,
                                None,
                            ),
                            limits(),
                        )
                        .expect("actual adopted I/O");
                        let io_completion = io_control.completion();
                        let mut options = ServeTransportOptions::new(
                            CodecProtocol::Http11,
                            TransportSecurity::Plaintext,
                        );
                        options.completion_pause_for_test = Some(CompletionPauseForTest {
                            entered: publisher_entered,
                            release: publisher_release_rx,
                        });
                        let connection = serve_transport(
                            &context,
                            transport,
                            Rc::new(CompletionResponseHandler),
                            options,
                        )
                        .expect("actual native server attachment");
                        publisher_entered_rx
                            .await
                            .expect("actual native run returned before publication");
                        assert_eq!(connection.terminal_state_for_test(), (true, false));
                        if fail_write {
                            assert_eq!(worker_write_attempts.load(Ordering::SeqCst), 1);
                            assert!(worker_written.lock().expect("wire").is_empty());
                            assert_eq!(
                                worker_drops.lock().expect("I/O drops").as_slice(),
                                &[std::thread::current().id()]
                            );
                        } else {
                            flush_started_rx
                                .await
                                .expect("actual graceful-close flush is pending");
                            assert!(worker_write_attempts.load(Ordering::SeqCst) > 0);
                            assert!(worker_drops.lock().expect("I/O drops").is_empty());
                        }
                        let attempts_before = worker_write_attempts.load(Ordering::SeqCst);
                        let drops_before = worker_drops.lock().expect("I/O drops").clone();
                        let wire_before = worker_written.lock().expect("wire").clone();
                        let mut closed = connection.wait_closed();
                        for _ in 0..3 {
                            assert!(
                                closed
                                    .as_mut()
                                    .poll(&mut Context::from_waker(Waker::noop()))
                                    .is_pending()
                            );
                            assert!(!gates.publisher.as_ref().unwrap().is_closed());
                            assert!(
                                !connection
                                    .initiate_drain()
                                    .expect("terminal drain is a no-op")
                            );
                            assert_eq!(connection.terminal_state_for_test(), (true, false));
                            assert!(
                                closed
                                    .as_mut()
                                    .poll(&mut Context::from_waker(Waker::noop()))
                                    .is_pending()
                            );
                            drop(connection.wait_closed());
                            assert_eq!(
                                worker_write_attempts.load(Ordering::SeqCst),
                                attempts_before
                            );
                            assert_eq!(*worker_written.lock().expect("wire"), wire_before);
                            assert_eq!(*worker_drops.lock().expect("I/O drops"), drops_before);
                            if !fail_write {
                                assert!(!gates.flush.as_ref().unwrap().is_closed());
                                assert!(!gates.shutdown.as_ref().unwrap().is_closed());
                                assert!(
                                    Box::pin(io_completion.wait_retired())
                                        .as_mut()
                                        .poll(&mut Context::from_waker(Waker::noop()))
                                        .is_pending()
                                );
                            }
                        }
                        gates
                            .publisher
                            .take()
                            .expect("publisher release")
                            .send(())
                            .expect("release the actual publisher");
                        let first = closed.await;
                        let repeated = connection.wait_closed().await;
                        assert_eq!(connection.terminal_state_for_test(), (true, false));
                        if fail_write {
                            let first = first.expect_err("retain the actual native write failure");
                            let repeated = repeated.expect_err("sticky native write failure");
                            assert_eq!(first.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                            assert_eq!(first.message(), b"terminal drain native write failure");
                            assert_eq!(repeated.code(), first.code());
                            assert_eq!(repeated.message(), first.message());
                        } else {
                            let summary = first.expect("actual clean native session");
                            assert_eq!(repeated.expect("sticky clean native session"), summary);
                            assert_eq!(summary.bytes_read, request.len() as u64);
                            assert_eq!(summary.bytes_written, wire_before.len() as u64);
                            assert_eq!(summary.bytes_scheduled, summary.bytes_written);
                            assert!(!summary.reset);
                            assert!(wire_before.starts_with(b"HTTP/1.1 200"));
                            assert!(wire_before.ends_with(b"response"));
                            assert!(worker_drops.lock().expect("I/O drops").is_empty());
                            gates
                                .flush
                                .take()
                                .unwrap()
                                .send(())
                                .expect("allow real flush");
                            shutdown_started_rx
                                .await
                                .expect("real shutdown reached after flush");
                            assert!(worker_drops.lock().expect("I/O drops").is_empty());
                            assert!(
                                Box::pin(io_completion.wait_retired())
                                    .as_mut()
                                    .poll(&mut Context::from_waker(Waker::noop()))
                                    .is_pending()
                            );
                            gates
                                .shutdown
                                .take()
                                .unwrap()
                                .send(())
                                .expect("allow real shutdown");
                        }
                        drop(connection);
                        io_completion
                            .wait_retired()
                            .await
                            .expect("actual I/O and drain scope retire");
                        io_completion
                            .wait_retired()
                            .await
                            .expect("sticky physical retirement");
                        assert_eq!(
                            worker_drops.lock().expect("I/O drops").as_slice(),
                            &[std::thread::current().id()]
                        );
                    }))
                    .await;
                drop(gates);
                outcome
            })
            .expect("native test owner admitted");
        let execution =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wait(initialized)));
        let quiesced = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wait(runtime.quiesce(TIMEOUT))
        }));
        let joined = owner.join();
        let outcome = execution
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            .expect("native test owner joins");
        outcome
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            .expect("terminal drain phases finish before their bound");
        quiesced
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            .expect("all native registered scopes retire");
        let report = joined.expect("physical worker join");
        assert_eq!(report.workers_joined, 1);
        assert_eq!(report.workers_forced, 0);
        assert_eq!(worker.state(), WorkerState::Closed);
        assert_eq!(
            drops.lock().expect("I/O drops").as_slice(),
            &[worker.owner_thread()]
        );
    }
}

/// Attaches an unrun HTTP/1.1 session on `context` and reports its kind.
fn attached_session_kind(
    context: &roxygen_runtime::worker::WorkerContext,
    protocol: CodecProtocol,
    session: crate::acceptor::ServerSessionConfig,
) -> crate::acceptor::ServerSessionKind {
    let (io, _peer) = tokio::io::duplex(64);
    attach_for_test(
        context,
        protocol,
        session,
        io,
        Rc::new(RecordingHandler(Arc::new(Evidence::default()))),
    )
    .session
    .kind()
}

/// Attaches an unrun session on `context` over `io`, as the server's acceptor does.
fn attach_for_test(
    context: &roxygen_runtime::worker::WorkerContext,
    protocol: CodecProtocol,
    session: crate::acceptor::ServerSessionConfig,
    io: tokio::io::DuplexStream,
    handler: roxygen_core::handler::HttpHandlerRef,
) -> crate::acceptor::AcceptedSession {
    let transport = crate::acceptor::AcceptedTransport::ByteStream(
        adopt_local(
            context,
            EstablishedIo::new(
                io,
                TransportEndpoint::Unknown,
                TransportEndpoint::Unknown,
                None,
            ),
            limits(),
        )
        .expect("I/O adopted"),
    );
    let connection = Arc::new(
        transport
            .connection_context(context, TransportSecurity::Plaintext, None)
            .expect("connection metadata")
            .with_http_protocol(protocol),
    );
    let config = crate::acceptor::AcceptorConfig {
        plaintext_protocol: protocol,
        session,
        connection_filter: None,
        tls: None,
        tls_observer: None,
        connection_observer: None,
        zero_copy_threshold: 0,
    };
    crate::acceptor::attach_session(
        transport,
        crate::acceptor::SessionAttachment {
            context,
            handler,
            config: &config,
            connection: crate::acceptor::connection_info(protocol, connection),
            draining: None,
            transport_limits: None,
            worker_index: 0,
            callback_failure: &Arc::new(Mutex::new(None)),
        },
    )
    .expect("session attaches")
}

#[test]
fn owned_http1_is_off_by_default() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                let session = crate::acceptor::ServerSessionConfig::default();
                assert!(!session.owned_http1);
                assert_eq!(
                    attached_session_kind(&context, CodecProtocol::Http11, session),
                    crate::acceptor::ServerSessionKind::Uniplex
                );
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// Attaches an unrun session on a fresh worker and reports its kind.
pub(crate) fn session_kind_on_worker(
    protocol: CodecProtocol,
    session: crate::acceptor::ServerSessionConfig,
) -> crate::acceptor::ServerSessionKind {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    let kind = wait(
        worker
            .call_local(
                move |context| async move { attached_session_kind(&context, protocol, session) },
            )
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
    kind
}

/// Records its invocation and never answers.
struct PendingHandler(Rc<std::cell::Cell<bool>>);

impl HttpHandler for PendingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.0.set(true);
        Box::pin(std::future::pending())
    }
}

#[test]
fn owned_http1_abandoned_run_finishes_on_the_worker() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                use tokio::io::AsyncReadExt;
                use tokio::io::AsyncWriteExt;
                let (io, mut peer) = tokio::io::duplex(4096);
                let invoked = Rc::new(std::cell::Cell::new(false));
                let session = crate::acceptor::ServerSessionConfig {
                    stream_read_timeout: Duration::ZERO,
                    connection_idle_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    owned_http1: true,
                    ..crate::acceptor::ServerSessionConfig::default()
                };
                let accepted = attach_for_test(
                    &context,
                    CodecProtocol::Http11,
                    session,
                    io,
                    Rc::new(PendingHandler(Rc::clone(&invoked))),
                );
                let control = accepted.control;
                let mut run = accepted.session.run();
                peer.write_all(b"GET /pending HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .expect("request written");
                std::future::poll_fn(|poll_context| {
                    assert!(
                        run.as_mut().poll(poll_context).is_pending(),
                        "the run waits for its handler"
                    );
                    if invoked.get() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                // As the server's force stop aborts its session task.
                let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(run)));
                assert!(
                    dropped.is_ok(),
                    "an abandoned owned run drops without a panic"
                );
                let mut rest = Vec::new();
                let _ = peer.read_to_end(&mut rest).await;
                assert!(rest.is_empty(), "the cancelled request gets no response");
                while !control.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

#[test]
fn owned_http1_attachment_rejects_unsupported_configuration() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                let untimed = || crate::acceptor::ServerSessionConfig {
                    stream_read_timeout: Duration::ZERO,
                    connection_idle_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    owned_http1: true,
                    ..crate::acceptor::ServerSessionConfig::default()
                };
                let mut stats = untimed();
                stats.stats_factory = Some(Arc::new(|_: &ConnectionInfo| {
                    unreachable!("a refused configuration builds no recorder")
                }));
                let mut lifecycle = untimed();
                lifecycle.lifecycle_observer_factory = Some(Arc::new(|| {
                    unreachable!("a refused configuration builds no observer")
                }));
                let mut timed = untimed();
                timed.write_timeout = Duration::from_secs(1);
                for (session, message) in [
                    (stats, &b"owned_http1 does not support stats_factory"[..]),
                    (
                        lifecycle,
                        &b"owned_http1 does not support lifecycle_observer_factory"[..],
                    ),
                    (
                        timed,
                        &b"owned_http1 is untimed: write_timeout must be zero"[..],
                    ),
                ] {
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
                    .expect("I/O adopted");
                    let mut options = ServeTransportOptions::new(
                        CodecProtocol::Http11,
                        TransportSecurity::Plaintext,
                    );
                    options.session = session;
                    let error = serve_transport(
                        &context,
                        transport,
                        Rc::new(RecordingHandler(Arc::new(Evidence::default()))),
                        options,
                    )
                    .err()
                    .expect("the unsupported configuration is refused");
                    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                    assert_eq!(error.message(), message);
                }
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

#[test]
fn owned_http1_attachment_cancel_ends_a_pending_exchange() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                use tokio::io::AsyncWriteExt;
                let (io, mut peer) = tokio::io::duplex(4096);
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
                .expect("I/O adopted");
                let mut options =
                    ServeTransportOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
                options.session = crate::acceptor::ServerSessionConfig {
                    stream_read_timeout: Duration::ZERO,
                    connection_idle_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    owned_http1: true,
                    ..crate::acceptor::ServerSessionConfig::default()
                };
                let invoked = Rc::new(std::cell::Cell::new(false));
                let connection = serve_transport(
                    &context,
                    transport,
                    Rc::new(PendingHandler(Rc::clone(&invoked))),
                    options,
                )
                .expect("owned attachment");
                peer.write_all(b"GET /pending HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .expect("request written");
                while !invoked.get() {
                    tokio::task::yield_now().await;
                }
                connection.cancel();
                assert_eq!(
                    connection
                        .wait_closed()
                        .await
                        .expect_err("cancel fails the connection")
                        .code(),
                    HttpErrorCode::CORO_CANCELLED
                );
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// A handler future that never finishes and panics when dropped.
struct PendingPanicsOnDrop;

impl Future for PendingPanicsOnDrop {
    type Output = Result<HttpSourceHolder, roxygen_core::error::HttpError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingPanicsOnDrop {
    fn drop(&mut self) {
        panic!("pending handler future destructor panicked");
    }
}

/// Records its invocation and never answers, from a future whose destructor panics.
struct PendingDropPanickingHandler(Rc<std::cell::Cell<bool>>);

impl HttpHandler for PendingDropPanickingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        self.0.set(true);
        Box::pin(PendingPanicsOnDrop)
    }
}

#[test]
fn owned_http1_cancel_contains_a_handler_future_drop_panic() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                use tokio::io::AsyncWriteExt;
                let (io, mut peer) = tokio::io::duplex(4096);
                let invoked = Rc::new(std::cell::Cell::new(false));
                let session = crate::acceptor::ServerSessionConfig {
                    stream_read_timeout: Duration::ZERO,
                    connection_idle_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    owned_http1: true,
                    ..crate::acceptor::ServerSessionConfig::default()
                };
                let accepted = attach_for_test(
                    &context,
                    CodecProtocol::Http11,
                    session,
                    io,
                    Rc::new(PendingDropPanickingHandler(Rc::clone(&invoked))),
                );
                let control = accepted.control;
                let mut run = accepted.session.run();
                peer.write_all(b"GET /pending HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .expect("request written");
                std::future::poll_fn(|poll_context| {
                    assert!(
                        run.as_mut().poll(poll_context).is_pending(),
                        "the run waits for its handler"
                    );
                    if invoked.get() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                control.force_stop();
                let ended = std::future::poll_fn(|poll_context| {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run.as_mut().poll(poll_context)
                    })) {
                        Ok(Poll::Pending) => Poll::Pending,
                        Ok(Poll::Ready(_)) => Poll::Ready(true),
                        Err(_) => Poll::Ready(false),
                    }
                })
                .await;
                assert!(ended, "the cancelled run ends without a panic");
                assert!(control.is_finished());
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// Records how far its response source got.
#[derive(Default)]
struct ResponseProgress {
    reading: std::cell::Cell<bool>,
    stopped: std::cell::Cell<bool>,
}

/// A response source whose header read never finishes and whose `stop_reading` panics.
struct PendingStopPanicsResponse(Rc<ResponseProgress>);

impl HttpSource for PendingStopPanicsResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<
        Result<roxygen_core::events::HttpHeaderEvent, roxygen_core::error::HttpError>,
    > {
        self.0.reading.set(true);
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<
        Result<roxygen_core::events::HttpBodyEvent, roxygen_core::error::HttpError>,
    > {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {
        self.0.stopped.set(true);
        panic!("response source stop panicked");
    }
}

/// Answers with a [`PendingStopPanicsResponse`].
struct PendingStopPanicsHandler(Rc<ResponseProgress>);

impl HttpHandler for PendingStopPanicsHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let progress = Rc::clone(&self.0);
        Box::pin(async move {
            Ok(HttpSourceHolder::new(Box::new(PendingStopPanicsResponse(
                progress,
            ))))
        })
    }
}

#[test]
fn owned_http1_abandoned_run_contains_a_retained_callback_panic() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                use tokio::io::AsyncReadExt;
                use tokio::io::AsyncWriteExt;
                let (io, mut peer) = tokio::io::duplex(4096);
                let progress = Rc::new(ResponseProgress::default());
                let session = crate::acceptor::ServerSessionConfig {
                    stream_read_timeout: Duration::ZERO,
                    connection_idle_timeout: Duration::ZERO,
                    write_timeout: Duration::ZERO,
                    owned_http1: true,
                    ..crate::acceptor::ServerSessionConfig::default()
                };
                let accepted = attach_for_test(
                    &context,
                    CodecProtocol::Http11,
                    session,
                    io,
                    Rc::new(PendingStopPanicsHandler(Rc::clone(&progress))),
                );
                let control = accepted.control;
                let mut run = accepted.session.run();
                peer.write_all(b"GET /pending HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .expect("request written");
                std::future::poll_fn(|poll_context| {
                    assert!(
                        run.as_mut().poll(poll_context).is_pending(),
                        "the run waits for its response head"
                    );
                    if progress.reading.get() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                // As the server's force stop aborts its session task. Cancelling the
                // exchange then panics in the response source's `stop_reading`, which the
                // driver retains and resumes when the abandoned run completes.
                let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(run)));
                assert!(
                    dropped.is_ok(),
                    "an abandoned owned run drops without a panic"
                );
                let mut rest = Vec::new();
                let _ = peer.read_to_end(&mut rest).await;
                assert!(rest.is_empty(), "the cancelled request gets no response");
                while !control.is_finished() {
                    tokio::task::yield_now().await;
                }
                assert!(progress.stopped.get(), "the response source was stopped");
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(
        worker
            .call_local(|_context| async {})
            .expect("the worker admits more work"),
    )
    .expect("the worker keeps serving");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// An untimed session configuration with the given owned modes on.
fn untimed_owned(owned_http1: bool, owned_http2: bool) -> crate::acceptor::ServerSessionConfig {
    crate::acceptor::ServerSessionConfig {
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http1,
        owned_http2,
        ..crate::acceptor::ServerSessionConfig::default()
    }
}

#[test]
fn owned_http2_dispatches_by_protocol() {
    use crate::acceptor::ServerSessionKind;
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http2, untimed_owned(false, true)),
        ServerSessionKind::OwnedHttp2
    );
    // Each owned flag leaves the other protocol on the managed session.
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http11, untimed_owned(false, true)),
        ServerSessionKind::Uniplex
    );
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http2, untimed_owned(true, false)),
        ServerSessionKind::Uniplex
    );
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http11, untimed_owned(true, true)),
        ServerSessionKind::OwnedHttp1
    );
    assert_eq!(
        session_kind_on_worker(CodecProtocol::Http2, untimed_owned(true, true)),
        ServerSessionKind::OwnedHttp2
    );
}

#[test]
fn owned_http2_attachment_rejects_unsupported_configuration() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                let mut stats = untimed_owned(false, true);
                stats.stats_factory = Some(Arc::new(|_: &ConnectionInfo| {
                    unreachable!("a refused configuration builds no recorder")
                }));
                let mut timed = untimed_owned(false, true);
                timed.stream_read_timeout = Duration::from_secs(1);
                for (session, message) in [
                    (stats, &b"owned_http2 does not support stats_factory"[..]),
                    (
                        timed,
                        &b"owned_http2 is untimed: stream_read_timeout must be zero"[..],
                    ),
                ] {
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
                    .expect("I/O adopted");
                    let mut options = ServeTransportOptions::new(
                        CodecProtocol::Http2,
                        TransportSecurity::Plaintext,
                    );
                    options.session = session;
                    let error = serve_transport(
                        &context,
                        transport,
                        Rc::new(RecordingHandler(Arc::new(Evidence::default()))),
                        options,
                    )
                    .err()
                    .expect("the unsupported configuration is refused");
                    assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                    assert_eq!(error.message(), message);
                }
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}

/// Answers every request with a fixed body, after draining it.
struct FixedHandler;

impl HttpHandler for FixedHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if !head.eom() {
                loop {
                    let event = request.read_body_event(1024).await?;
                    if event.eom() {
                        break;
                    }
                }
            }
            let body = BufQueue::copy_from_slice(b"owned-http2", 11).expect("fixed body fits");
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(200, Some(body)).expect("valid response"),
            )))
        })
    }
}

#[test]
fn owned_http2_serves_a_fixed_get_and_drains() {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 4,
    })
    .expect("owner starts");
    let runtime = owner.handle();
    let worker = runtime.worker(0).expect("worker exists");
    wait(
        worker
            .call_local(move |context| async move {
                let (io, peer) = tokio::io::duplex(64 * 1024);
                let accepted = attach_for_test(
                    &context,
                    CodecProtocol::Http2,
                    untimed_owned(false, true),
                    io,
                    Rc::new(FixedHandler),
                );
                assert_eq!(
                    accepted.session.kind(),
                    crate::acceptor::ServerSessionKind::OwnedHttp2
                );
                let control = accepted.control;
                let run = tokio::task::spawn_local(accepted.session.run());
                let (client, connection) = h2::client::handshake(peer).await.expect("H2 handshake");
                let connection = tokio::task::spawn_local(connection);
                let mut client = client.ready().await.expect("H2 client ready");
                let request = http::Request::builder()
                    .method("GET")
                    .uri("http://localhost/fixed")
                    .body(())
                    .expect("request builds");
                let (response, _stream) = client.send_request(request, true).expect("request sent");
                let response = response.await.expect("response head");
                assert_eq!(response.status(), http::StatusCode::OK);
                let mut body = response.into_body();
                let mut bytes = Vec::new();
                while let Some(chunk) = body.data().await {
                    let chunk = chunk.expect("response chunk");
                    bytes.extend_from_slice(&chunk);
                    body.flow_control()
                        .release_capacity(chunk.len())
                        .expect("capacity released");
                }
                assert_eq!(bytes, b"owned-http2");
                control.drain();
                drop(client);
                connection.abort();
                let summary = run
                    .await
                    .expect("owned run task joins")
                    .expect("owned run completes");
                assert!(summary.bytes_written > 0);
                assert!(control.is_finished());
            })
            .expect("initializer admitted"),
    )
    .expect("initializer finishes");
    wait(runtime.quiesce(TIMEOUT)).expect("cleanup joins");
    assert_eq!(owner.join().expect("physical join").workers_forced, 0);
}
