/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use http::Request;
use http::Response;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use http_body_util::BodyExt;
use http_body_util::Full;
use roxygen_client::pool::AttachedPoolParams;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_hyper_adapter::body::LocalBodyOptions;
use roxygen_hyper_adapter::source::ApplicationBodyOptions;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::source_proxy::RemoteSourceError;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use tokio::runtime::Handle;
use tokio::task::JoinSet;
use tower::ServiceExt;
use tower_service::Service;

use crate::client::Client;
use crate::client::ClientError;
use crate::client::ClientOptions;
use crate::client::ClientOwner;
use crate::client::ClientShutdownError;
use crate::client::ClientStartError;
use crate::client::SessionSettings;
use crate::client::connector::ConnectedIo;
use crate::client::connector::ConnectorPolicy;
use crate::client::owner::TestApplicationJoin;
use crate::client::owner::TestLatch;
use crate::client_tests::DropGate;
use crate::client_tests::Gate;
use crate::client_tests::Handler;
use crate::client_tests::Io;
use crate::client_tests::ProbedIo;
use crate::client_tests::ReleaseGate;
use crate::client_tests::TestBody;
use crate::client_tests::TestConnector;
use crate::client_tests::WorkerControl;
use crate::client_tests::body_bytes;
use crate::client_tests::case;
use crate::client_tests::forced_run;
use crate::client_tests::full;
use crate::client_tests::options;
use crate::client_tests::panicked_run;
use crate::client_tests::probe_connector;
use crate::client_tests::producer_reaped;
use crate::client_tests::route;
use crate::route::ProxyEndpoint;

fn simple_handler() -> Handler {
    Arc::new(|request| {
        Box::pin(async move {
            let _ = request.into_body().collect().await;
            Response::new(full("ok"))
        })
    })
}

struct LiveUploadState {
    pending: Gate,
    drops: AtomicUsize,
    frames: AtomicUsize,
    ended: AtomicBool,
}

type LiveUploadResult = Result<(), crate::client_tests::OriginalError>;
type LiveUploadSender = tokio::sync::oneshot::Sender<LiveUploadResult>;

struct LiveUpload {
    stage: Cell<u8>,
    release: RefCell<Pin<Box<tokio::sync::oneshot::Receiver<LiveUploadResult>>>>,
    state: Arc<LiveUploadState>,
    app: tokio::runtime::Id,
    _pinned: std::marker::PhantomPinned,
}

impl Body for LiveUpload {
    type Data = Bytes;
    type Error = crate::client_tests::OriginalError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let body = self.as_ref().get_ref();
        assert_eq!(Handle::current().id(), body.app);
        if body.stage.get() == 0 {
            body.stage.set(1);
            body.state.frames.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"upload-")))));
        }
        if body.stage.get() == 1 {
            let result = match body.release.borrow_mut().as_mut().poll(context) {
                Poll::Pending => {
                    body.state.pending.open();
                    return Poll::Pending;
                }
                Poll::Ready(result) => {
                    result.expect("upload sender must remain owned until cancellation retires it")
                }
            };
            body.stage.set(2);
            if let Err(error) = result {
                return Poll::Ready(Some(Err(error)));
            }
            body.state.frames.fetch_add(1, Ordering::SeqCst);
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(
                b"forbidden-suffix",
            )))));
        }
        body.state.ended.store(true, Ordering::SeqCst);
        Poll::Ready(None)
    }
}

impl Drop for LiveUpload {
    fn drop(&mut self) {
        assert_eq!(Handle::current().id(), self.app);
        self.state.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn live_upload() -> (LiveUpload, LiveUploadSender, Arc<LiveUploadState>) {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let state = Arc::new(LiveUploadState {
        pending: Gate::new(),
        drops: AtomicUsize::new(0),
        frames: AtomicUsize::new(0),
        ended: AtomicBool::new(false),
    });
    (
        LiveUpload {
            stage: Cell::new(0),
            release: RefCell::new(Box::pin(receiver)),
            state: Arc::clone(&state),
            app: Handle::current().id(),
            _pinned: std::marker::PhantomPinned,
        },
        sender,
        state,
    )
}

struct LivePeerBody {
    prefix: Option<Bytes>,
    _request: hyper::body::Incoming,
    sent: Gate,
}

impl Body for LivePeerBody {
    type Data = Bytes;
    type Error = crate::client::BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if let Some(prefix) = self.prefix.take() {
            self.sent.open();
            Poll::Ready(Some(Ok(Frame::data(prefix))))
        } else {
            Poll::Pending
        }
    }
}

struct LiveRelease {
    peer: Gate,
    latches: Vec<Arc<TestLatch>>,
}

impl Drop for LiveRelease {
    fn drop(&mut self) {
        for latch in &self.latches {
            latch.release();
        }
        self.peer.open();
    }
}

async fn live_phase<T>(phase: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap_or_else(|_| panic!("live cancellation phase timed out: {phase}"))
}

async fn live_h1_peer(stream: tokio::net::TcpStream, sent: Gate, release: Gate) {
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let mut stream = tokio::io::BufReader::new(stream);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
        head.push_str(&line);
        assert!(head.len() <= 8192);
        if line == "\r\n" {
            break;
        }
    }
    assert!(head.starts_with("POST /held HTTP/1.1\r\n"));
    assert!(
        head.to_ascii_lowercase()
            .contains("transfer-encoding: chunked\r\n")
    );
    let mut prefix = Vec::new();
    while prefix.len() < 7 {
        let mut line = String::new();
        assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
        let size = usize::from_str_radix(line.trim(), 16).unwrap();
        assert!(size != 0 && size <= 7 - prefix.len());
        let mut chunk = vec![0; size];
        stream.read_exact(&mut chunk).await.unwrap();
        let mut ending = [0; 2];
        stream.read_exact(&mut ending).await.unwrap();
        assert_eq!(&ending, b"\r\n");
        prefix.extend(chunk);
    }
    assert_eq!(prefix, b"upload-");
    stream
        .get_mut()
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nprefix-")
        .await
        .unwrap();
    sent.open();
    release.wait().await;
}

async fn live_connector(
    fixture: &mut crate::client_tests::Fixture,
    protocol: CodecProtocol,
    sent: Gate,
    release: Gate,
) -> TestConnector {
    let listener = Arc::new(
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap(),
    );
    let address = listener.local_addr().unwrap();
    let peers = Arc::clone(&fixture.peers);
    let executor = fixture.executor.clone();
    let stop = fixture.stop.clone();
    TestConnector::new(move |request| {
        let listener = Arc::clone(&listener);
        let sent = sent.clone();
        let release = release.clone();
        let executor = executor.clone();
        let stop = stop.clone();
        peers.lock().unwrap().spawn(async move {
            let stream = tokio::select! {
                accepted = listener.accept() => accepted.unwrap().0,
                _ = stop.wait() => return,
            };
            if protocol == CodecProtocol::Http11 {
                live_h1_peer(stream, sent, release).await;
            } else {
                let handler: Handler = Arc::new(move |request| {
                    let sent = sent.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        if request.uri().path() == "/sibling" {
                            assert_eq!(
                                request.into_body().collect().await.unwrap().to_bytes(),
                                b"healthy-upload"[..]
                            );
                            let mut trailers = http::HeaderMap::new();
                            trailers
                                .insert("x-live-sibling", http::HeaderValue::from_static("done"));
                            let first = futures::stream::once(std::future::ready(Ok::<
                                _,
                                crate::client::BoxError,
                            >(
                                Frame::data(Bytes::from_static(b"healthy-")),
                            )));
                            let last = futures::stream::once(async move {
                                release.wait().await;
                                Ok(Frame::data(Bytes::from_static(b"response")))
                            })
                            .chain(futures::stream::once(
                                std::future::ready(Ok(Frame::trailers(trailers))),
                            ));
                            return Response::new(
                                http_body_util::StreamBody::new(first.chain(last)).boxed_unsync(),
                            );
                        }
                        let mut incoming = request.into_body();
                        let mut prefix = Vec::new();
                        while prefix.len() < 7 {
                            prefix.extend_from_slice(
                                &incoming
                                    .frame()
                                    .await
                                    .unwrap()
                                    .unwrap()
                                    .into_data()
                                    .unwrap(),
                            );
                        }
                        assert_eq!(prefix, b"upload-");
                        Response::new(
                            LivePeerBody {
                                prefix: Some(Bytes::from_static(b"prefix-")),
                                _request: incoming,
                                sent,
                            }
                            .boxed_unsync(),
                        )
                    })
                });
                crate::client_tests::serve_peer(stream, protocol, handler, executor).await;
            }
        });
        Box::pin(async move {
            let stream = tokio::net::TcpStream::connect(address).await?;
            Ok(ConnectedIo::new(
                request.route().clone(),
                EstablishedIo::new(
                    Box::pin(stream) as Io,
                    TransportEndpoint::Unknown,
                    TransportEndpoint::Unknown,
                    None,
                ),
                protocol,
                TransportSecurity::Plaintext,
            ))
        })
    })
}

fn live_h1_upload_cancel(error: &ClientError) -> bool {
    matches!(error, ClientError::Pool(roxygen_client::pool::PoolError::SessionJoin { source })
        if source.code() == HttpErrorCode::CORO_CANCELLED
            && source.message() == b"request body production cancelled"
            && std::error::Error::source(source).is_none())
}

async fn live_prefix(body: &mut crate::client::ClientBody) {
    let mut prefix = Vec::new();
    while prefix.len() < 7 {
        let frame = live_phase("complete response prefix", body.frame())
            .await
            .unwrap()
            .unwrap();
        prefix.extend_from_slice(&frame.into_data().unwrap());
    }
    assert_eq!(prefix, b"prefix-");
}

fn live_stopped(frame: Option<Result<Frame<Bytes>, crate::client::BoxError>>) {
    let error = frame
        .expect("stopped source emits a real error")
        .expect_err("cancelled retained read must not produce data or trailers");
    assert!(
        matches!(
            error.downcast_ref::<RemoteSourceError<HttpError>>(),
            Some(RemoteSourceError::Stopped)
        ),
        "actual body error: {error:?}"
    );
}

async fn live_cancel_read(
    body: &mut crate::client::ClientBody,
    exchange: &crate::client::ClientExchange,
    control: &WorkerControl,
    application: &TestApplicationJoin,
    repeat: bool,
) {
    let wakes = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&wakes);
    let waker = Waker::from(Arc::new(ObservingWake {
        forward: Waker::noop().clone(),
        action: Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    }));
    let mut read = Box::pin(body.frame());
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    control.pause().await;
    let before = wakes.load(Ordering::SeqCst);
    exchange.cancel();
    if repeat {
        exchange.cancel();
    }
    let forwarded = tokio::time::timeout(Duration::from_secs(5), async {
        while application.forwards.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    control.resume().await;
    forwarded.expect("response stop is accepted before native cancellation runs");
    live_phase("retained read wake after cancellation", async {
        while wakes.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await;
    live_stopped(live_phase("same retained read", read).await);
    assert!(body.is_end_stream());
    assert!(body.frame().await.is_none());
}

async fn live_cancelled(exchange: &crate::client::ClientExchange) {
    let error = live_phase("confirmed cancellation receipt", exchange.wait())
        .await
        .expect_err("explicit live exchange cancellation");
    assert!(
        matches!(error.cause(), ClientError::Cancelled),
        "selected cause: {error:?}"
    );
    assert!(!error.cleanup_unconfirmed());
}

async fn live_upload_joined(probe: &TestApplicationJoin, upload: &LiveUploadState) {
    assert!(probe.reaped.load(Ordering::Acquire));
    assert_eq!(*probe.outcome.lock().unwrap(), Some("cancelled"));
    let application = probe
        .cleanup
        .lock()
        .unwrap()
        .clone()
        .expect("actual application source observer");
    live_phase("actual application/source join", application.wait())
        .await
        .unwrap();
    let response = probe
        .response_cleanup
        .lock()
        .unwrap()
        .clone()
        .expect("actual response source observer");
    live_phase("actual response export join", response.wait())
        .await
        .unwrap();
    assert_eq!(upload.drops.load(Ordering::SeqCst), 1);
    assert_eq!(upload.frames.load(Ordering::SeqCst), 1);
    assert!(!upload.ended.load(Ordering::SeqCst));
}

#[test]
fn live_exchange_cancel_wakes_retained_body_and_reaps_upload() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        for phase in 0..3 {
            case(move |fixture| {
                Box::pin(async move {
                    if protocol == CodecProtocol::Http11 {
                        fixture.expected_run_error = Some(live_h1_upload_cancel);
                    }
                    let sent = Gate::new();
                    let release = LiveRelease {
                        peer: Gate::new(),
                        latches: Vec::new(),
                    };
                    let mut connector =
                        live_connector(fixture, protocol, sent.clone(), release.peer.clone()).await;
                    let calls = Arc::clone(&connector.calls);
                    let io_drops = Arc::new(AtomicUsize::new(0));
                    let io_release = Gate::new();
                    io_release.open();
                    probe_connector(
                        &mut connector,
                        &fixture.worker,
                        io_release,
                        Gate::new(),
                        Arc::clone(&io_drops),
                    );
                    let application = Arc::new(TestApplicationJoin::default());
                    let io = Arc::new(crate::client::owner::TestIoJoin::default());
                    let mut settings = options();
                    settings.test_application = Some(Arc::clone(&application));
                    settings.test_io = Some(Arc::clone(&io));
                    let client = fixture.start(connector, settings).await;
                    let bound = client.route(route(protocol)).unwrap();
                    let (upload, sender, upload_state) = live_upload();
                    let request = client
                        .try_request(
                            &bound,
                            Request::builder()
                                .method("POST")
                                .uri("/held")
                                .body(upload)
                                .unwrap(),
                        )
                        .unwrap();
                    let exchange = request.exchange();
                    let mut body = live_phase("published response", request)
                        .await
                        .unwrap()
                        .into_body();
                    live_phase("peer prefix produced", sent.wait()).await;
                    live_phase(
                        "upload poll held with sender alive",
                        upload_state.pending.wait(),
                    )
                    .await;
                    assert!(!sender.is_closed());
                    assert_eq!(upload_state.drops.load(Ordering::SeqCst), 0);
                    if phase == 0 {
                        exchange.cancel();
                        live_phase("abort accepted before first read", async {
                            while application.forwards.load(Ordering::Acquire) == 0 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await;
                        live_stopped(live_phase("cancel before first read", body.frame()).await);
                        assert!(body.is_end_stream());
                        assert!(body.frame().await.is_none());
                    } else {
                        live_prefix(&mut body).await;
                        live_cancel_read(
                            &mut body,
                            &exchange,
                            &fixture.control,
                            &application,
                            phase == 2,
                        )
                        .await;
                    }
                    live_cancelled(&exchange).await;
                    live_upload_joined(&application, &upload_state).await;
                    assert!(sender.is_closed());
                    assert_eq!(application.forwards.load(Ordering::Acquire), 1);
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    fixture.close_clients().await;
                    let physical = io
                        .adopted
                        .lock()
                        .unwrap()
                        .clone()
                        .expect("actual I/O adoption");
                    live_phase("actual adopted I/O retirement", physical.wait_retired())
                        .await
                        .unwrap();
                    assert!(io.retired.load(Ordering::Acquire));
                    assert_eq!(io_drops.load(Ordering::SeqCst), 1);
                    assert_eq!(fixture.run_results.len(), 1);
                    assert_eq!(fixture.control.forced.load(Ordering::SeqCst), 0);
                    drop((body, sender, release, client));
                })
            });
        }
    }
}

fn live_credit_pending(
    client: &Client,
    bound: &crate::client::Route,
    exchange: &crate::client::ClientExchange,
) {
    assert_eq!(client.shared.admission.available_permits(), 0);
    let mut service = client.service::<Full<Bytes>>(bound).unwrap();
    assert!(
        service
            .poll_ready(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let mut completion = Box::pin(exchange.wait());
    assert!(
        completion
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

fn live_run_pending(fixture: &mut crate::client_tests::Fixture) {
    if let Some(result) = fixture.runs.try_join_next() {
        fixture
            .run_results
            .push(result.expect("the actual execution parent must not panic or be aborted"));
        panic!("the execution parent returned before retained joins were released");
    }
}

fn live_join_credit_case(protocol: CodecProtocol, native_first: bool) {
    case(move |fixture| {
        Box::pin(async move {
            if protocol == CodecProtocol::Http11 {
                fixture.expected_run_error = Some(live_h1_upload_cancel);
            }
            let app_gate = TestLatch::new();
            let native_gate = TestLatch::new();
            let release = LiveRelease {
                peer: Gate::new(),
                latches: vec![Arc::clone(&app_gate), Arc::clone(&native_gate)],
            };
            let connector =
                live_connector(fixture, protocol, Gate::new(), release.peer.clone()).await;
            let calls = Arc::clone(&connector.calls);
            let application = Arc::new(TestApplicationJoin::default());
            let io = Arc::new(crate::client::owner::TestIoJoin::default());
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.maximum_physical_connections = 1;
            settings.pool.max_connections = 1;
            settings.test_application = Some(Arc::clone(&application));
            settings.test_io = Some(Arc::clone(&io));
            settings.test_app_return = Some(Arc::clone(&app_gate));
            settings.test_native_return = Some(Arc::clone(&native_gate));
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(protocol)).unwrap();
            let (upload, sender, upload_state) = live_upload();
            let request = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/held")
                        .body(upload)
                        .unwrap(),
                )
                .unwrap();
            let exchange = request.exchange();
            let mut body = live_phase("credit case response", request)
                .await
                .unwrap()
                .into_body();
            live_prefix(&mut body).await;
            live_phase("credit case upload pending", upload_state.pending.wait()).await;
            live_cancel_read(&mut body, &exchange, &fixture.control, &application, false).await;
            live_phase("actual app-return gate", app_gate.wait_entered()).await;
            live_phase("actual native-return gate", native_gate.wait_entered()).await;
            live_upload_joined(&application, &upload_state).await;
            assert!(sender.is_closed());
            live_credit_pending(&client, &bound, &exchange);
            if native_first {
                native_gate.release();
                live_phase(
                    "released native scope actually retired",
                    application.native_retired(),
                )
                .await
                .unwrap();
            } else {
                app_gate.release();
                live_phase(
                    "application job actually handed off to receipt phase",
                    async {
                        while !application.retirement_entered.load(Ordering::Acquire) {
                            tokio::task::yield_now().await;
                        }
                    },
                )
                .await;
            }
            live_credit_pending(&client, &bound, &exchange);
            live_run_pending(fixture);
            if native_first {
                app_gate.release();
            } else {
                native_gate.release();
            }
            live_cancelled(&exchange).await;
            assert_eq!(client.shared.admission.available_permits(), 1);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            fixture.close_clients().await;
            let physical = io
                .adopted
                .lock()
                .unwrap()
                .clone()
                .expect("actual physical owner");
            live_phase("credit case physical join", physical.wait_retired())
                .await
                .unwrap();
            assert!(io.retired.load(Ordering::Acquire));
            assert_eq!(fixture.run_results.len(), 1);
            assert_eq!(fixture.control.forced.load(Ordering::SeqCst), 0);
            drop((body, sender, release, client));
        })
    });
}

fn live_forward_panic(
    probe: &TestApplicationJoin,
    pause: Option<Arc<DropGate>>,
) -> Arc<AtomicUsize> {
    let panics = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&panics);
    *probe.forward_wake.lock().unwrap() = Some(Waker::from(Arc::new(ObservingWake {
        forward: Waker::noop().clone(),
        action: Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            if let Some(pause) = &pause {
                pause.block();
            }
            panic!("accepted response-stop notification panic");
        }),
    })));
    panics
}

fn live_forward_panic_case() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(panicked_run);
            let release = LiveRelease {
                peer: Gate::new(),
                latches: Vec::new(),
            };
            let connector = live_connector(
                fixture,
                CodecProtocol::Http2,
                Gate::new(),
                release.peer.clone(),
            )
            .await;
            let calls = Arc::clone(&connector.calls);
            let application = Arc::new(TestApplicationJoin::default());
            let io = Arc::new(crate::client::owner::TestIoJoin::default());
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.test_application = Some(Arc::clone(&application));
            settings.test_io = Some(Arc::clone(&io));
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let (upload, sender, upload_state) = live_upload();
            let request = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/held")
                        .body(upload)
                        .unwrap(),
                )
                .unwrap();
            let exchange = request.exchange();
            let mut body = live_phase("panic case response", request)
                .await
                .unwrap()
                .into_body();
            live_prefix(&mut body).await;
            live_phase("panic case retained upload", upload_state.pending.wait()).await;
            let mut read = Box::pin(body.frame());
            assert!(
                read.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            fixture.control.pause().await;
            let panics = live_forward_panic(&application, None);
            exchange.cancel();
            live_phase("new live forwarding notification", async {
                while !client.shared.destruction_failed.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert_eq!(panics.load(Ordering::SeqCst), 1);
            assert_eq!(application.forwards.load(Ordering::Acquire), 1);
            assert!(!application.reaped.load(Ordering::Acquire));
            assert_eq!(upload_state.drops.load(Ordering::SeqCst), 0);
            assert!(!sender.is_closed());
            assert_eq!(client.shared.admission.available_permits(), 0);
            let mut completion = Box::pin(exchange.wait());
            assert!(
                completion
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let source = application
                .response_cleanup
                .lock()
                .unwrap()
                .clone()
                .unwrap();
            assert!(futures::poll!(Box::pin(source.wait())).is_pending());
            live_run_pending(fixture);
            fixture.control.resume().await;
            live_stopped(live_phase("same read after contained notification", read).await);
            assert!(body.frame().await.is_none());
            let error = live_phase("same joins after contained notification", completion)
                .await
                .expect_err("notification uncertainty remains local to this exchange");
            assert!(
                matches!(error.cause(), ClientError::Cancelled),
                "selected failure: {error:?}"
            );
            assert!(error.cleanup_unconfirmed());
            live_upload_joined(&application, &upload_state).await;
            assert!(sender.is_closed());
            assert!(
                client.shared.admission.is_closed(),
                "uncertain cancellation cannot readmit work"
            );
            fixture.close_clients().await;
            let physical = io.adopted.lock().unwrap().clone().unwrap();
            physical.wait_retired().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(panics.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.run_results.len(), 1);
            assert_eq!(fixture.control.forced.load(Ordering::SeqCst), 0);
            drop((body, sender, release, client));
        })
    });
}

#[test]
fn live_exchange_cancel_keeps_native_source_and_app_join_credits() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        for native_first in [false, true] {
            live_join_credit_case(protocol, native_first);
        }
    }
    live_forward_panic_case();
    live_original_failure_case(true, false);
}

#[test]
fn ready_receipt_selects_caught_notification_panic_without_owner_repoll() {
    live_original_failure_case(true, true);
}

fn live_original_failure_case(notification_panic: bool, ready_receipt: bool) {
    case(move |fixture| {
        Box::pin(async move {
            if notification_panic {
                fixture.expected_run_error = Some(panicked_run);
            }
            let app_gate = TestLatch::new();
            let native_gate = ready_receipt.then(TestLatch::new);
            let forward_gate = ready_receipt.then(DropGate::new);
            let forward_release = forward_gate
                .as_ref()
                .map(|gate| ReleaseGate(Arc::clone(gate)));
            let mut release = LiveRelease {
                peer: Gate::new(),
                latches: vec![Arc::clone(&app_gate)],
            };
            release.latches.extend(native_gate.iter().cloned());
            let connector = live_connector(
                fixture,
                CodecProtocol::Http2,
                Gate::new(),
                release.peer.clone(),
            )
            .await;
            let calls = Arc::clone(&connector.calls);
            let application = Arc::new(TestApplicationJoin::default());
            let io = Arc::new(crate::client::owner::TestIoJoin::default());
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.test_application = Some(Arc::clone(&application));
            settings.test_app_return = Some(Arc::clone(&app_gate));
            settings.test_io = Some(Arc::clone(&io));
            settings.test_shutdown_return = native_gate.clone();
            let client = if ready_receipt {
                let (startup, run) =
                    Client::prepare(fixture.worker.clone(), connector, settings).unwrap();
                fixture.runs.spawn(tokio::task::unconstrained(run));
                let (client, owner) = startup.start().await.unwrap();
                fixture.owners.push(owner);
                client
            } else {
                fixture.start(connector, settings).await
            };
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let (upload, sender, upload_state) = live_upload();
            let request = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/held")
                        .body(upload)
                        .unwrap(),
                )
                .unwrap();
            let exchange = request.exchange();
            let mut body = live_phase("original-error response head", request)
                .await
                .unwrap()
                .into_body();
            live_prefix(&mut body).await;
            live_phase("original-error upload pending", upload_state.pending.wait()).await;
            let identity = Arc::new(());
            sender
                .send(Err(crate::client_tests::OriginalError(Arc::clone(
                    &identity,
                ))))
                .expect("the original error reaches the still-owned body");
            live_phase("actual selected application error", app_gate.wait_entered()).await;
            assert!(application.reaped.load(Ordering::Acquire));
            assert_eq!(*application.outcome.lock().unwrap(), Some("application"));
            assert_eq!(upload_state.drops.load(Ordering::SeqCst), 1);
            assert_eq!(upload_state.frames.load(Ordering::SeqCst), 1);
            assert!(!upload_state.ended.load(Ordering::SeqCst));
            assert!(!application.retirement_entered.load(Ordering::Acquire));
            assert_eq!(application.forwards.load(Ordering::Acquire), 0);
            live_credit_pending(&client, &bound, &exchange);
            fixture.control.pause().await;
            let panics =
                notification_panic.then(|| live_forward_panic(&application, forward_gate.clone()));
            exchange.cancel();
            assert_eq!(
                application.forwards.load(Ordering::Acquire),
                0,
                "cancel is sticky while the completed application job is still parented"
            );
            app_gate.release();
            if let Some(gate) = &forward_gate {
                live_phase(
                    "late forwarding held inside owner poll",
                    gate.entered.wait(),
                )
                .await;
                assert!(!client.shared.destruction_failed.load(Ordering::Acquire));
                assert!(!client.shared.stop.is_cancelled());
            } else {
                live_phase("late receipt-phase forwarding", async {
                    while application.forwards.load(Ordering::Acquire) == 0
                        || (notification_panic
                            && !client.shared.destruction_failed.load(Ordering::Acquire))
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
            }
            assert!(application.retirement_entered.load(Ordering::Acquire));
            assert_eq!(application.forwards.load(Ordering::Acquire), 1);
            let mut completion = Box::pin(exchange.wait());
            assert!(
                completion
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(futures::poll!(Box::pin(application.native_retired())).is_pending());
            let response_cleanup = application
                .response_cleanup
                .lock()
                .unwrap()
                .clone()
                .unwrap();
            assert!(futures::poll!(Box::pin(response_cleanup.wait())).is_pending());
            assert_eq!(client.shared.admission.available_permits(), 0);
            live_run_pending(fixture);
            fixture.control.resume().await;
            if let Some(gate) = &forward_gate {
                live_phase(
                    "native receipt ready before notification panic",
                    application.native_retired(),
                )
                .await
                .unwrap();
                live_phase(
                    "response receipt ready before notification panic",
                    response_cleanup.wait(),
                )
                .await
                .unwrap();
                let upload_cleanup = application.cleanup.lock().unwrap().clone().unwrap();
                live_phase(
                    "upload receipt ready before notification panic",
                    upload_cleanup.wait(),
                )
                .await
                .unwrap();
                assert!(!client.shared.destruction_failed.load(Ordering::Acquire));
                assert!(!client.shared.stop.is_cancelled());
                assert!(!client.shared.admission.is_closed());
                assert!(futures::poll!(&mut completion).is_pending());
                live_run_pending(fixture);
                gate.release();
            }
            let error = live_phase("original cause after actual receipt joins", completion)
                .await
                .expect_err("late cancellation must not replace a selected application error");
            assert_eq!(error.cleanup_unconfirmed(), notification_panic);
            let ClientError::Application(cause) = error.cause() else {
                panic!("expected original application failure, not cancellation: {error:?}");
            };
            let original = cause
                .external_cause()
                .unwrap()
                .downcast_ref::<crate::client_tests::OriginalError>()
                .unwrap();
            assert!(Arc::ptr_eq(&original.0, &identity));
            application.native_retired().await.unwrap();
            response_cleanup.wait().await.unwrap();
            let upload_cleanup = application.cleanup.lock().unwrap().clone().unwrap();
            upload_cleanup.wait().await.unwrap();
            if notification_panic {
                assert!(client.shared.admission.is_closed());
            } else {
                assert_eq!(client.shared.admission.available_permits(), 1);
            }
            if let Some(gate) = &native_gate {
                live_phase(
                    "native parent held after ready receipt",
                    gate.wait_entered(),
                )
                .await;
                assert_eq!(client.shared.admission.available_permits(), 0);
                live_run_pending(fixture);
                gate.release();
            }
            fixture.close_clients().await;
            let physical = io.adopted.lock().unwrap().clone().unwrap();
            physical.wait_retired().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.run_results.len(), 1);
            assert_eq!(fixture.control.forced.load(Ordering::SeqCst), 0);
            if let Some(panics) = panics {
                assert_eq!(panics.load(Ordering::SeqCst), 1);
            }
            drop((body, forward_release, release, client));
        })
    });
}

fn live_h2_sibling_case() {
    case(|fixture| {
        Box::pin(async move {
            let release = LiveRelease {
                peer: Gate::new(),
                latches: Vec::new(),
            };
            let connector = live_connector(
                fixture,
                CodecProtocol::Http2,
                Gate::new(),
                release.peer.clone(),
            )
            .await;
            let calls = Arc::clone(&connector.calls);
            let application = Arc::new(TestApplicationJoin::default());
            let io = Arc::new(crate::client::owner::TestIoJoin::default());
            let mut settings = options();
            settings.maximum_physical_connections = 1;
            settings.pool.max_connections = 1;
            settings.test_application = Some(Arc::clone(&application));
            settings.test_io = Some(Arc::clone(&io));
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let (upload, sender, upload_state) = live_upload();
            let request = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/held")
                        .body(upload)
                        .unwrap(),
                )
                .unwrap();
            let exchange = request.exchange();
            let mut body = live_phase("multiplexed held response", request)
                .await
                .unwrap()
                .into_body();
            live_prefix(&mut body).await;
            live_phase("multiplexed held upload", upload_state.pending.wait()).await;
            let sibling = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/sibling")
                        .body(Full::new(Bytes::from_static(b"healthy-upload")))
                        .unwrap(),
                )
                .unwrap();
            let sibling_exchange = sibling.exchange();
            let sibling = live_phase("sibling on the existing connection", sibling)
                .await
                .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let mut sibling_body = sibling.into_body();
            let first = live_phase("first sibling frame", sibling_body.frame())
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap();
            assert_eq!(first, b"healthy-"[..]);
            assert!(!release.peer.is_open());
            live_cancel_read(&mut body, &exchange, &fixture.control, &application, true).await;
            live_cancelled(&exchange).await;
            let mut next = Box::pin(sibling_body.frame());
            assert!(
                next.as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            release.peer.open();
            let second = live_phase("peer suffix produced after other stream retired", next)
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap();
            assert_eq!(second, b"response"[..]);
            let remaining = live_phase(
                "healthy sibling after other-stream cancel",
                sibling_body.collect(),
            )
            .await
            .unwrap();
            assert_eq!(
                remaining.trailers().unwrap().get("x-live-sibling").unwrap(),
                "done"
            );
            let mut exact = first.to_vec();
            exact.extend_from_slice(&second);
            exact.extend_from_slice(&remaining.to_bytes());
            assert_eq!(exact, b"healthy-response");
            live_phase("healthy sibling exchange joins", sibling_exchange.wait())
                .await
                .unwrap();
            live_cancelled(&exchange).await;
            live_upload_joined(&application, &upload_state).await;
            assert!(sender.is_closed());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(!client.shared.stop.is_cancelled());
            assert!(!client.shared.admission.is_closed());
            let physical = io.adopted.lock().unwrap().clone().unwrap();
            assert!(
                futures::poll!(Box::pin(physical.wait_retired())).is_pending(),
                "healthy shared I/O outlives both exchanges"
            );
            fixture.close_clients().await;
            physical.wait_retired().await.unwrap();
            assert!(io.retired.load(Ordering::Acquire));
            assert_eq!(fixture.run_results.len(), 1);
            assert!(fixture.run_results[0].is_ok());
            assert_eq!(fixture.control.forced.load(Ordering::SeqCst), 0);
            drop((body, sender, release, client));
        })
    });
}

#[test]
fn live_exchange_cancel_preserves_selected_failure_and_other_exchange() {
    live_original_failure_case(false, false);
    live_h2_sibling_case();
}

struct ObservingWake {
    forward: Waker,
    action: Arc<dyn Fn() + Send + Sync>,
}
impl Wake for ObservingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        (self.action)();
        self.forward.wake_by_ref();
    }
}
struct ObservedRun {
    run: crate::client::ClientRun,
    action: Arc<dyn Fn() + Send + Sync>,
    parked: Gate,
}

#[derive(Clone, Debug)]
struct TerminalWakeAllocation {
    poll: usize,
    parent: Option<usize>,
}

#[derive(Clone, Debug)]
struct TerminalWakeDrop {
    allocation: usize,
    armed: bool,
}

#[derive(Clone, Debug, Default)]
struct TerminalWakeLedger {
    polls: usize,
    allocations: Vec<TerminalWakeAllocation>,
    drops: Vec<TerminalWakeDrop>,
    panic_attempts: Vec<usize>,
    completed: Vec<usize>,
    ready_polls: Vec<usize>,
}

impl TerminalWakeLedger {
    fn snapshot(ledger: &Mutex<Self>) -> Self {
        ledger.lock().unwrap().clone()
    }

    fn assert_preterminal(&self) {
        assert!(self.ready_polls.is_empty(), "{self:?}");
        assert!(self.drops.iter().all(|entry| !entry.armed), "{self:?}");
        assert!(self.panic_attempts.is_empty(), "{self:?}");
    }

    fn assert_terminal(&self, panics: bool) {
        assert_eq!(self.ready_polls.len(), 1, "{self:?}");
        let ready_poll = self.ready_polls[0];
        assert_eq!(ready_poll + 1, self.polls, "{self:?}");
        let clones = self
            .allocations
            .iter()
            .enumerate()
            .filter_map(|(identity, allocation)| {
                (allocation.poll == ready_poll && allocation.parent.is_some()).then_some(identity)
            })
            .collect::<Vec<_>>();
        assert_eq!(clones.len(), 1, "{self:?}");
        let terminal_clone = clones[0];
        let drops = self
            .drops
            .iter()
            .filter(|entry| entry.allocation == terminal_clone)
            .collect::<Vec<_>>();
        assert_eq!(drops.len(), 1, "{self:?}");
        assert!(drops[0].armed, "{self:?}");
        assert_eq!(
            self.completed
                .iter()
                .filter(|identity| **identity == terminal_clone)
                .count(),
            1,
            "{self:?}"
        );
        assert_eq!(
            self.panic_attempts
                .iter()
                .filter(|identity| **identity == terminal_clone)
                .count(),
            usize::from(panics),
            "{self:?}"
        );
    }

    fn assert_all_retired(&self, panics: bool) {
        self.assert_terminal(panics);
        let created = (0..self.allocations.len()).collect::<BTreeSet<_>>();
        let entered = self
            .drops
            .iter()
            .map(|entry| entry.allocation)
            .collect::<BTreeSet<_>>();
        let completed = self.completed.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(entered, created, "{self:?}");
        assert_eq!(completed, created, "{self:?}");
        assert_eq!(self.drops.len(), created.len(), "{self:?}");
        assert_eq!(self.completed.len(), created.len(), "{self:?}");
        let roots = self
            .allocations
            .iter()
            .filter_map(|allocation| allocation.parent.is_none().then_some(allocation.poll))
            .collect::<Vec<_>>();
        assert_eq!(roots, (0..self.polls).collect::<Vec<_>>(), "{self:?}");
        for (identity, allocation) in self.allocations.iter().enumerate() {
            if let Some(parent) = allocation.parent {
                assert!(parent < identity, "{self:?}");
                assert_eq!(allocation.poll, self.allocations[parent].poll, "{self:?}");
            }
        }
        let expected_panics = self
            .drops
            .iter()
            .filter_map(|entry| {
                (panics && entry.armed && self.allocations[entry.allocation].parent.is_some())
                    .then_some(entry.allocation)
            })
            .collect::<Vec<_>>();
        assert_eq!(self.panic_attempts, expected_panics, "{self:?}");
    }
}

struct TerminalWakeRetirement {
    ledger: Arc<Mutex<TerminalWakeLedger>>,
    allocation: usize,
    poll: usize,
}

impl Drop for TerminalWakeRetirement {
    fn drop(&mut self) {
        self.ledger.lock().unwrap().completed.push(self.allocation);
    }
}

struct TerminalWake {
    forward: Waker,
    armed: Arc<AtomicBool>,
    panics: bool,
    retirement: TerminalWakeRetirement,
}

impl Drop for TerminalWake {
    fn drop(&mut self) {
        let armed = self.armed.load(Ordering::Acquire);
        let panics = {
            let mut ledger = self.retirement.ledger.lock().unwrap();
            let allocation = self.retirement.allocation;
            let panics = armed && self.panics && ledger.allocations[allocation].parent.is_some();
            ledger.drops.push(TerminalWakeDrop { allocation, armed });
            if panics {
                ledger.panic_attempts.push(allocation);
            }
            panics
        };
        assert!(!panics, "terminal clone-owned waker destructor");
    }
}

impl TerminalWake {
    fn new(
        forward: Waker,
        armed: Arc<AtomicBool>,
        panics: bool,
        ledger: Arc<Mutex<TerminalWakeLedger>>,
        parent: Option<usize>,
    ) -> Self {
        let (allocation, poll) = {
            let mut state = ledger.lock().unwrap();
            let poll = if let Some(parent) = parent {
                state.allocations[parent].poll
            } else {
                let poll = state.polls;
                state.polls = state
                    .polls
                    .checked_add(1)
                    .expect("unique terminal poll identity");
                poll
            };
            let allocation = state.allocations.len();
            state
                .allocations
                .push(TerminalWakeAllocation { poll, parent });
            (allocation, poll)
        };
        Self {
            forward,
            armed,
            panics,
            retirement: TerminalWakeRetirement {
                ledger,
                allocation,
                poll,
            },
        }
    }

    const VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
        Self::clone_raw,
        Self::wake_raw,
        Self::wake_ref_raw,
        Self::drop_raw,
    );

    fn into_raw(self) -> std::task::RawWaker {
        std::task::RawWaker::new(Box::into_raw(Box::new(self)).cast(), &Self::VTABLE)
    }

    unsafe fn clone_raw(pointer: *const ()) -> std::task::RawWaker {
        let owned = unsafe { &*pointer.cast::<Self>() };
        Self::new(
            owned.forward.clone(),
            Arc::clone(&owned.armed),
            owned.panics,
            Arc::clone(&owned.retirement.ledger),
            Some(owned.retirement.allocation),
        )
        .into_raw()
    }

    unsafe fn wake_raw(pointer: *const ()) {
        let owned = unsafe { Box::from_raw(pointer.cast_mut().cast::<Self>()) };
        owned.forward.wake_by_ref();
    }

    unsafe fn wake_ref_raw(pointer: *const ()) {
        let owned = unsafe { &*pointer.cast::<Self>() };
        owned.forward.wake_by_ref();
    }

    unsafe fn drop_raw(pointer: *const ()) {
        drop(unsafe { Box::from_raw(pointer.cast_mut().cast::<Self>()) });
    }
}

struct TerminalRun {
    run: crate::client::ClientRun,
    armed: Arc<AtomicBool>,
    ledger: Arc<Mutex<TerminalWakeLedger>>,
    panics: bool,
}

impl Future for TerminalRun {
    type Output = Result<(), ClientShutdownError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let owner = TerminalWake::new(
            context.waker().clone(),
            Arc::clone(&self.armed),
            self.panics,
            Arc::clone(&self.ledger),
            None,
        );
        let poll = owner.retirement.poll;
        let waker = unsafe { Waker::from_raw(owner.into_raw()) };
        let result = Pin::new(&mut self.run).poll(&mut Context::from_waker(&waker));
        if result.is_ready() {
            self.ledger.lock().unwrap().ready_polls.push(poll);
        }
        drop(waker);
        result
    }
}

fn terminal_waker_case(mode: u8) {
    let ledger = Arc::new(Mutex::new(TerminalWakeLedger::default()));
    let observed = Arc::clone(&ledger);
    case(move |fixture| {
        Box::pin(async move {
            fixture.expected_run_error = match mode {
                0 => None,
                1 => Some(panicked_run),
                _ => Some(forced_run),
            };
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let (startup, mut run) =
                Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
            let armed = Arc::new(AtomicBool::new(false));
            run.arm_terminal_drop(Arc::clone(&armed));
            fixture.runs.spawn(TerminalRun {
                run,
                armed: Arc::clone(&armed),
                ledger: Arc::clone(&observed),
                panics: mode != 0,
            });
            let (client, owner) = startup.start().await.unwrap();
            fixture.owners.push(owner);
            assert!(!armed.load(Ordering::Acquire));
            TerminalWakeLedger::snapshot(&observed).assert_preterminal();
            if mode == 2 {
                fixture.control.force_drop().await;
            }
            fixture.close_clients().await;
            assert!(armed.load(Ordering::Acquire));
            TerminalWakeLedger::snapshot(&observed).assert_terminal(mode != 0);
            assert_eq!(
                client.shared.destruction_failed.load(Ordering::Acquire),
                mode != 0
            );
            assert_eq!(fixture.run_results.len(), 1);
            match (&fixture.run_results[0], mode) {
                (Ok(()), 0)
                | (Err(ClientShutdownError(ClientError::Panicked)), 1)
                | (Err(ClientShutdownError(ClientError::CleanupUnconfirmed)), 2) => {}
                (result, _) => {
                    panic!("terminal run result must include owned-waker retirement: {result:?}")
                }
            }
        })
    });
    TerminalWakeLedger::snapshot(&ledger).assert_all_retired(mode != 0);
}
impl Future for ObservedRun {
    type Output = Result<(), ClientShutdownError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = Waker::from(Arc::new(ObservingWake {
            forward: context.waker().clone(),
            action: Arc::clone(&self.action),
        }));
        let result = Pin::new(&mut self.run).poll(&mut Context::from_waker(&waker));
        if result.is_pending() {
            self.parked.open();
        }
        result
    }
}

#[test]
fn admission_publication_does_not_call_wakers_under_lock() {
    case(|fixture| {
        Box::pin(async move {
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let calls = Arc::clone(&connector.calls);
            let owner_slot: Arc<Mutex<Option<std::sync::Weak<crate::client::Shared>>>> =
                Arc::new(Mutex::new(None));
            let probe_slot = Arc::clone(&owner_slot);
            let armed = Arc::new(AtomicBool::new(false));
            let trigger = Arc::clone(&armed);
            let unlocked = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&unlocked);
            let called = Gate::new();
            let callback = called.clone();
            let action = Arc::new(move || {
                if !trigger.swap(false, Ordering::AcqRel) {
                    return;
                }
                let shared = probe_slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                    .unwrap();
                let available = shared.admission_gate.try_lock().is_ok();
                observed.store(available, Ordering::Release);
                if available {
                    shared.close();
                }
                callback.open();
            });
            let (startup, run) =
                Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
            let parked = Gate::new();
            fixture.runs.spawn(ObservedRun {
                run,
                action,
                parked: parked.clone(),
            });
            let (client, owner) = startup.start().await.unwrap();
            fixture.owners.push(owner);
            parked.wait().await;
            *owner_slot.lock().unwrap() = Some(Arc::downgrade(&client.shared));
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let body = TestBody::new(vec![]);
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            armed.store(true, Ordering::Release);
            let response = client
                .try_request(&route, Request::builder().uri("/").body(body).unwrap())
                .unwrap();
            called.wait().await;
            assert!(
                unlocked.load(Ordering::Acquire),
                "negative control records the locked gate instead of deadlocking"
            );
            assert!(matches!(response.await, Err(ClientError::Cancelled)));
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(client.shared.publishing.load(Ordering::Acquire), 0);
            assert!(fixture.runs.is_empty());
        })
    });
}

async fn notification_phase<T>(kind: u8, phase: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .unwrap_or_else(|_| panic!("notification kind={kind} phase={phase} timed out"))
}

fn notification_panic_case(kind: u8) {
    case(|fixture| {
        Box::pin(async move {
            use hyper::rt::Executor;
            fixture.expected_run_error = Some(panicked_run);
            let cleanup_gate = DropGate::new();
            let release = ReleaseGate(Arc::clone(&cleanup_gate));
            let reply = Gate::new();
            if kind != 0 {
                reply.open();
            }
            let response_ready = reply.clone();
            let executor = fixture.executor.clone();
            let handler: Handler = Arc::new(move |request| {
                let ready = response_ready.clone();
                let executor = executor.clone();
                Box::pin(async move {
                    executor.execute(async move {
                        let _ = request.into_body().collect().await;
                    });
                    ready.wait().await;
                    Response::new(full("ready"))
                })
            });
            let first_probe = Arc::new(TestApplicationJoin::default());
            let second_probe = Arc::new(TestApplicationJoin::default());
            let mut settings = options();
            settings.test_application = Some(Arc::clone(&first_probe));
            settings
                .test_application_following
                .push(Arc::clone(&second_probe));
            if kind == 2 {
                settings.maximum_exchanges = 1;
            }
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let client = fixture.start(connector, settings).await;
            let route = client.route(route(CodecProtocol::Http2)).unwrap();
            let mut body =
                TestBody::new(vec![Frame::data(Bytes::from_static(b"pending"))]).gated(Gate::new());
            body.drop_gate = Some(Arc::clone(&cleanup_gate));
            let polls = Arc::clone(&body.polls);
            let drops = Arc::clone(&body.drops);
            let mut pending = Box::pin(
                client
                    .try_request(
                        &route,
                        Request::builder()
                            .method("POST")
                            .uri("/")
                            .body(body)
                            .unwrap(),
                    )
                    .unwrap(),
            );
            notification_phase(kind, "first upload's second body poll", async {
                while polls.load(Ordering::SeqCst) < 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let wakes = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&wakes);
            let waker = Waker::from(Arc::new(ObservingWake {
                forward: Waker::noop().clone(),
                action: Arc::new(move || {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        panic!("foreign notification");
                    }
                }),
            }));
            let mut second_pending = None;
            if kind == 0 {
                let mut body =
                    TestBody::new(vec![Frame::data(Bytes::from_static(b"also pending"))])
                        .gated(Gate::new());
                body.drop_gate = Some(Arc::clone(&cleanup_gate));
                let polls = Arc::clone(&body.polls);
                second_pending = Some(
                    client
                        .try_request(
                            &route,
                            Request::builder()
                                .method("POST")
                                .uri("/other")
                                .body(body)
                                .unwrap(),
                        )
                        .unwrap(),
                );
                notification_phase(kind, "second upload's second body poll", async {
                    while polls.load(Ordering::SeqCst) < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                assert!(
                    pending
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                reply.open();
                notification_phase(kind, "foreign response notification", async {
                    while wakes.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
            } else if kind == 1 {
                let target = client
                    .try_request(
                        &route,
                        Request::builder()
                            .uri("/complete")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap()
                    .await
                    .unwrap();
                let exchange = target.body().exchange();
                let mut completion = Box::pin(exchange.wait());
                assert!(
                    completion
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                assert_eq!(body_bytes(target.into_body()).await, "ready");
                notification_phase(kind, "foreign exchange notification", async {
                    while wakes.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
            } else {
                let mut service = client.service::<Full<Bytes>>(&route).unwrap();
                let mut other = service.clone();
                assert!(
                    service
                        .poll_ready(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                let notified = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&notified);
                let ordinary = Waker::from(Arc::new(ObservingWake {
                    forward: Waker::noop().clone(),
                    action: Arc::new(move || {
                        observed.fetch_add(1, Ordering::SeqCst);
                    }),
                }));
                assert!(
                    other
                        .poll_ready(&mut Context::from_waker(&ordinary))
                        .is_pending()
                );
                fixture.owners.last().unwrap().shutdown();
                assert!(
                    notified.load(Ordering::Acquire) > 0,
                    "later readiness waiters still notified"
                );
                assert!(matches!(
                    futures::poll!(Box::pin(service.ready())),
                    Poll::Ready(Err(ClientError::Closed))
                ));
                assert!(matches!(
                    futures::poll!(Box::pin(other.ready())),
                    Poll::Ready(Err(ClientError::Closed))
                ));
                assert!(client.shared.stop.is_cancelled());
            }
            notification_phase(
                kind,
                "actual producer destruction entry",
                cleanup_gate.entered.wait(),
            )
            .await;
            assert!(wakes.load(Ordering::Acquire) > 0);
            assert!(!first_probe.reaped.load(Ordering::Acquire));
            assert!(futures::poll!(Box::pin(fixture.runs.join_next())).is_pending());
            let cleanup = first_probe.cleanup.lock().unwrap().clone().unwrap();
            assert!(futures::poll!(Box::pin(cleanup.wait())).is_pending());
            cleanup_gate.release();
            drop(pending);
            drop(second_pending);
            fixture.close_clients().await;
            producer_reaped(&first_probe).await;
            cleanup.wait().await.unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            if kind == 0 {
                producer_reaped(&second_probe).await;
            }
            assert!(client.shared.admission.is_closed());
            assert!(client.shared.physical.is_closed());
            drop(release);
        })
    });
}

#[test]
fn external_timeouts_disables_only_selected_timeouts() {
    let original = ClientOptions {
        maximum_exchanges: 17,
        command_capacity: 19,
        maximum_connector_tasks: 3,
        maximum_physical_connections: 5,
        maximum_routes: 7,
        connector_policy: ConnectorPolicy::Forward(
            ProxyEndpoint::tcp("http://127.0.0.1:3128".parse().unwrap()).unwrap(),
        ),
        pool: AttachedPoolParams {
            max_connections: 4,
            max_connection_attempts: 2,
            connect_timeout: Duration::from_secs(5),
            max_age: Duration::from_secs(13),
            max_waiters: 11,
            max_retiring: None,
            pending_connect_credit: 1,
        },
        session: SessionSettings {
            max_concurrent_outgoing_streams: Some(23),
            connection_flow_control: Some(65535),
            stream_read_timeout: Some(Duration::from_secs(2)),
            connection_read_timeout: Some(Duration::from_secs(3)),
            write_timeout: Some(Duration::from_secs(4)),
        },
        upload: ApplicationBodyOptions {
            maximum_frame_bytes: NonZeroUsize::new(1024).unwrap(),
            maximum_demand_bytes: NonZeroU32::new(257).unwrap(),
        },
        response: LocalBodyOptions {
            command_capacity: 9,
            maximum_read: NonZeroU32::new(123).unwrap(),
            size_hint: SizeHint::with_exact(321),
        },
        transport: ByteStreamLimits::new(1024, 2048, 8).unwrap(),
        test_acquisitions: Some(Arc::new(AtomicUsize::new(29))),
        ..ClientOptions::default()
    };
    let configured = original.clone().with_external_timeouts();

    assert_eq!(configured.pool.connect_timeout, Duration::ZERO);
    assert_eq!(
        configured.session.connection_read_timeout,
        Some(Duration::ZERO)
    );
    assert_eq!(configured.session.stream_read_timeout, Some(Duration::ZERO));
    assert_eq!(configured.session.write_timeout, Some(Duration::ZERO));
    assert_eq!(configured.maximum_exchanges, original.maximum_exchanges);
    assert_eq!(configured.command_capacity, original.command_capacity);
    assert_eq!(
        configured.maximum_connector_tasks,
        original.maximum_connector_tasks
    );
    assert_eq!(
        configured.maximum_physical_connections,
        original.maximum_physical_connections
    );
    assert_eq!(configured.maximum_routes, original.maximum_routes);
    assert_eq!(configured.connector_policy, original.connector_policy);
    assert_eq!(
        configured.pool,
        AttachedPoolParams {
            connect_timeout: Duration::ZERO,
            ..original.pool
        }
    );
    assert_eq!(
        configured.session.max_concurrent_outgoing_streams,
        original.session.max_concurrent_outgoing_streams
    );
    assert_eq!(
        configured.session.connection_flow_control,
        original.session.connection_flow_control
    );
    assert_eq!(
        configured.upload.maximum_frame_bytes,
        original.upload.maximum_frame_bytes
    );
    assert_eq!(
        configured.upload.maximum_demand_bytes,
        original.upload.maximum_demand_bytes
    );
    assert_eq!(
        configured.response.command_capacity,
        original.response.command_capacity
    );
    assert_eq!(
        configured.response.maximum_read,
        original.response.maximum_read
    );
    assert_eq!(
        configured.response.size_hint.lower(),
        original.response.size_hint.lower()
    );
    assert_eq!(
        configured.response.size_hint.upper(),
        original.response.size_hint.upper()
    );
    assert_eq!(configured.transport, original.transport);
    assert!(Arc::ptr_eq(
        configured.test_acquisitions.as_ref().unwrap(),
        original.test_acquisitions.as_ref().unwrap(),
    ));
}

#[test]
fn external_timeouts_is_opt_in_and_idempotent() {
    let defaults = ClientOptions::default();
    let once = defaults.clone().with_external_timeouts();
    let twice = once.clone().with_external_timeouts();

    assert_eq!(defaults.pool.connect_timeout, Duration::from_secs(2));
    assert_eq!(defaults.session.connection_read_timeout, None);
    assert_eq!(defaults.session.stream_read_timeout, None);
    assert_eq!(defaults.session.write_timeout, None);
    assert_eq!(once.pool.connect_timeout, Duration::ZERO);
    assert_eq!(once.session.connection_read_timeout, Some(Duration::ZERO));
    assert_eq!(once.session.stream_read_timeout, Some(Duration::ZERO));
    assert_eq!(once.session.write_timeout, Some(Duration::ZERO));
    assert_eq!(twice.pool, once.pool);
    assert_eq!(
        twice.session.connection_read_timeout,
        once.session.connection_read_timeout
    );
    assert_eq!(
        twice.session.stream_read_timeout,
        once.session.stream_read_timeout
    );
    assert_eq!(twice.session.write_timeout, once.session.write_timeout);
}

#[test]
fn runtime_identity_rejects_other_or_missing_context_before_admission() {
    case(|fixture| {
        Box::pin(async move {
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let calls = Arc::clone(&connector.calls);
            let client = fixture.start(connector, options()).await;
            let route = client.route(route(CodecProtocol::Http11)).unwrap();
            let native_runtime = fixture
                .worker
                .call(|_| Handle::current())
                .unwrap()
                .await
                .unwrap();
            let body = TestBody::new(vec![]);
            let drops = Arc::clone(&body.drops);
            let rejected = {
                let _other = native_runtime.enter();
                client
                    .try_request(&route, Request::builder().uri("/").body(body).unwrap())
                    .err()
                    .unwrap()
            };
            assert!(matches!(rejected.error, ClientError::WrongRuntime));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(rejected);
            let body = TestBody::new(vec![]);
            let drops = Arc::clone(&body.drops);
            let copy = client.clone();
            let bound = route.clone();
            let thread = std::thread::spawn(move || {
                copy.try_request(&bound, Request::builder().uri("/").body(body).unwrap())
                    .err()
                    .unwrap()
            });
            let rejected = thread.join().unwrap();
            assert!(matches!(rejected.error, ClientError::WrongRuntime));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(rejected);
            let mut service = client.service::<Full<Bytes>>(&route).unwrap();
            service.ready().await.unwrap();
            let failed = {
                let _other = native_runtime.enter();
                service.call(
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            };
            assert!(matches!(failed.await, Err(ClientError::WrongRuntime)));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        })
    });
}

#[test]
fn prepare_captures_runtime_and_run_waits_for_parented_activation() {
    case(|fixture| {
        Box::pin(async move {
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let events = Arc::clone(&connector.events);
            let (startup, run) =
                Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
            assert!(events.lock().unwrap().is_empty());
            fixture.runs.spawn(run);
            tokio::task::yield_now().await;
            assert!(events.lock().unwrap().is_empty());
            let (client, owner) = startup.start().await.unwrap();
            fixture.owners.push(owner);
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            assert_eq!(body_bytes(response.into_body()).await, "ok");
            exchange.wait().await.unwrap();
            assert!(
                events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(phase, _)| *phase == "call")
            );
        })
    });
}

#[test]
fn unpolled_response_future_and_preparation_rejection_drop_body_on_app() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            fixture.control.pause().await;
            let body = TestBody::new(vec![]);
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            let future = client
                .try_request(&bound, Request::builder().uri("/").body(body).unwrap())
                .unwrap();
            let exchange = future.exchange();
            drop(future);
            fixture.control.force_drop().await;
            exchange.wait().await.unwrap_err();
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            let body = TestBody::new(vec![]);
            let drops = Arc::clone(&body.drops);
            let rejected = client
                .try_request(&bound, Request::builder().uri("/").body(body).unwrap())
                .err()
                .unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(rejected);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[test]
fn full_queues_do_not_block_exchange_cancel_or_owner_shutdown() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let mut settings = options();
            settings.maximum_exchanges = 2;
            settings.command_capacity = 1;
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            fixture.control.pause().await;
            let mut service = client.service::<TestBody>(&bound).unwrap();
            let mut futures = Vec::new();
            let mut drops = Vec::new();
            for _request in 0..2 {
                service.ready().await.unwrap();
                let body = TestBody::new(vec![]);
                drops.push(Arc::clone(&body.drops));
                futures.push(service.call(Request::builder().uri("/").body(body).unwrap()));
            }
            assert!(matches!(
                client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap()
                    )
                    .err()
                    .unwrap()
                    .error,
                ClientError::Capacity
            ));
            for future in &futures {
                future.exchange().cancel();
            }
            fixture.owners.last().unwrap().shutdown();
            drop(futures);
            fixture.control.force_drop().await;
            fixture.close_clients().await;
            assert!(drops.iter().all(|drops| drops.load(Ordering::SeqCst) == 1));
        })
    });
}

#[test]
fn source_native_and_application_joins_each_gate_exchange_completion() {
    case(|fixture| {
        Box::pin(async move {
            for application in [true, false] {
                let latch = TestLatch::new();
                let mut settings = options();
                if application {
                    settings.test_app_return = Some(Arc::clone(&latch));
                } else {
                    settings.test_native_return = Some(Arc::clone(&latch));
                }
                let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
                let client = fixture.start(connector, settings).await;
                let bound = client.route(route(CodecProtocol::Http11)).unwrap();
                let body = TestBody::new(vec![]);
                let drops = Arc::clone(&body.drops);
                let future = client
                    .try_request(&bound, Request::builder().uri("/").body(body).unwrap())
                    .unwrap();
                let exchange = future.exchange();
                body_bytes(future.await.unwrap().into_body()).await;
                latch.wait_entered().await;
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                let completion = exchange.wait();
                tokio::pin!(completion);
                assert!(futures::poll!(&mut completion).is_pending());
                latch.release();
                completion.await.unwrap();
            }
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            let completion = exchange.wait();
            tokio::pin!(completion);
            assert!(futures::poll!(&mut completion).is_pending());
            fixture.control.pause().await;
            drop(response);
            assert!(futures::poll!(&mut completion).is_pending());
            fixture.control.resume().await;
            if let Err(error) = completion.await {
                assert!(!error.cleanup_unconfirmed());
            }
        })
    });
}

#[test]
fn native_completion_does_not_substitute_for_adopted_io_retirement() {
    case(|fixture| {
        Box::pin(async move {
            let handler: Handler = Arc::new(|_| {
                Box::pin(async {
                    Response::builder()
                        .header("connection", "close")
                        .body(full("done"))
                        .unwrap()
                })
            });
            let mut connector = fixture.connector(CodecProtocol::Http11, handler);
            let gate = Gate::new();
            let drops = Arc::new(AtomicUsize::new(0));
            probe_connector(
                &mut connector,
                &fixture.worker,
                gate.clone(),
                Gate::new(),
                Arc::clone(&drops),
            );
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let response = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap()
                .await
                .unwrap();
            let exchange = response.body().exchange();
            body_bytes(response.into_body()).await;
            exchange.wait().await.unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(client.shared.physical.available_permits(), 1);
            gate.open();
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        })
    });
}

#[test]
fn partial_startup_and_late_handoff_join_before_parent_completion() {
    case(|fixture| {
        Box::pin(async move {
            fixture.control.pause().await;
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let calls = Arc::clone(&connector.calls);
            let events = Arc::clone(&connector.events);
            let (startup, run) =
                Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
            fixture.runs.spawn(run);
            {
                let startup = startup.start();
                tokio::pin!(startup);
                assert!(futures::poll!(&mut startup).is_pending());
            }
            fixture.control.resume().await;
            fixture.close_clients().await;
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(
                events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(phase, _)| *phase == "connector-drop")
            );
            assert!(fixture.runs.is_empty());
        })
    });
}

struct ManagedCase {
    runtime: RuntimeHandle,
    runs: JoinSet<Result<(), ClientShutdownError>>,
    owners: Vec<ClientOwner>,
    peers: Arc<Mutex<JoinSet<()>>>,
    stop: Gate,
    run_results: Vec<Result<(), ClientShutdownError>>,
    outer_failed: bool,
}

impl ManagedCase {
    async fn join_runs(&mut self) {
        while let Some(result) = self.runs.join_next().await {
            match result {
                Ok(result) => self.run_results.push(result),
                Err(_) => self.outer_failed = true,
            }
        }
    }
}

fn managed_case(
    test: impl for<'case> FnOnce(
        &'case mut ManagedCase,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'case>>,
) {
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 128,
    })
    .unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        let mut case = ManagedCase {
            runtime: owner.handle(),
            runs: JoinSet::new(),
            owners: Vec::new(),
            peers: Arc::new(Mutex::new(JoinSet::new())),
            stop: Gate::new(),
            run_results: Vec::new(),
            outer_failed: false,
        };
        let result = std::panic::AssertUnwindSafe(tokio::time::timeout(
            Duration::from_secs(10),
            test(&mut case),
        ))
        .catch_unwind()
        .await;
        for guard in &case.owners {
            guard.shutdown();
        }
        let quiesce = case.runtime.quiesce(Duration::from_secs(1));
        let (_, quiescence) = futures::join!(case.join_runs(), quiesce);
        case.stop.open();
        let mut peers = std::mem::replace(&mut *case.peers.lock().unwrap(), JoinSet::new());
        while let Some(result) = peers.join_next().await {
            case.outer_failed |= result.is_err();
        }
        (result, case.outer_failed, case.run_results, quiescence)
    });
    let joined = owner.join().unwrap();
    assert_eq!(joined.workers_joined, 1);
    assert_eq!(joined.workers_forced, 0);
    drop(runtime);
    match result.0 {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("managed client case timed out"),
        Err(payload) => std::panic::resume_unwind(payload),
    }
    assert!(
        !result.1,
        "all actual run/peer parents joined without task panic"
    );
    assert!(
        result.2.iter().all(Result::is_ok),
        "unexpected inner run results: {:?}",
        result.2
    );
    result.3.unwrap();
}

#[test]
fn adoption_rejected_during_grace_joins_transit_io_without_session_receipt() {
    managed_case(|case| {
        Box::pin(async move {
            let worker = case.runtime.worker(0).unwrap();
            let owner_thread = worker.owner_thread();
            let app = Handle::current().id();
            let drops = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::clone(&drops);
            let claimed = Gate::new();
            let probe = claimed.clone();
            let connector = TestConnector::new(move |request| {
                let (client, peer) = tokio::io::duplex(1024);
                drop(peer);
                let io: Io = Box::pin(ProbedIo {
                    inner: Box::pin(client),
                    worker_claimed: Arc::clone(&request.claimed),
                    shutdown_complete: false,
                    shutdown: Box::pin(async {}),
                    claimed: probe.clone(),
                    dropped: Arc::clone(&dropped),
                    worker: owner_thread,
                    app,
                });
                Box::pin(async move {
                    Ok(ConnectedIo::new(
                        request.route().clone(),
                        EstablishedIo::new(
                            io,
                            TransportEndpoint::Unknown,
                            TransportEndpoint::Unknown,
                            None,
                        ),
                        CodecProtocol::Http11,
                        TransportSecurity::Plaintext,
                    ))
                })
            });
            let latch = TestLatch::new();
            let mut settings = options();
            settings.test_handoff = Some(Arc::clone(&latch));
            let (startup, run) = Client::prepare(worker.clone(), connector, settings).unwrap();
            case.runs.spawn(run);
            let (client, guard) = startup.start().await.unwrap();
            case.owners.push(guard);
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            latch.wait_entered().await;
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            let quiescence = case.runtime.quiesce(Duration::from_secs(1));
            assert!(!worker.is_accepting());
            latch.release();
            assert!(future.await.is_err());
            quiescence.await.unwrap();
            case.join_runs().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(
                !claimed.is_open(),
                "adoption rejects before I/O polling or session creation"
            );
        })
    });
}

#[test]
fn abandoned_observers_and_user_drop_panics_do_not_abandon_other_joins() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(panicked_run);
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let client = fixture.start(connector, options()).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            fixture.control.pause().await;
            let mut first = TestBody::new(vec![]);
            first.panic_drop = true;
            let first_drops = Arc::clone(&first.drops);
            let second = TestBody::new(vec![]);
            let second_drops = Arc::clone(&second.drops);
            let mut service = client.service::<TestBody>(&bound).unwrap();
            let mut admitted = Vec::new();
            for body in [first, second] {
                service.ready().await.unwrap();
                admitted.push(service.call(Request::builder().uri("/").body(body).unwrap()));
            }
            for future in admitted {
                drop(future.exchange());
                drop(future);
            }
            fixture.control.resume().await;
            fixture.close_clients().await;
            assert_eq!(first_drops.load(Ordering::SeqCst), 1);
            assert_eq!(second_drops.load(Ordering::SeqCst), 1);
            assert!(fixture.run_results.iter().any(Result::is_err));
            assert!(client.shared.admission.is_closed());
        })
    });
    for kind in 0..3 {
        notification_panic_case(kind);
    }
}

#[test]
fn client_shutdown_then_physical_worker_join_keeps_app_reactor_progressing() {
    managed_case(|case| {
        Box::pin(async move {
            let worker = case.runtime.worker(0).unwrap();
            let app = Handle::current();
            let ticks = Arc::new(AtomicUsize::new(0));
            let recorded = Arc::clone(&ticks);
            let close_entered = Gate::new();
            let entered = close_entered.clone();
            let close_gate = Gate::new();
            let release = close_gate.clone();
            struct OpenOnDrop(Gate);
            impl Drop for OpenOnDrop {
                fn drop(&mut self) {
                    self.0.open();
                }
            }
            let release_on_drop = OpenOnDrop(close_gate.clone());
            let peers = Arc::clone(&case.peers);
            let stop = case.stop.clone();
            let reactor = app.clone();
            let owner_thread = worker.owner_thread();
            let drops = Arc::new(AtomicUsize::new(0));
            let destroyed = Arc::clone(&drops);
            let connector = TestConnector::new(move |request| {
                use tokio::io::AsyncReadExt;
                use tokio::io::AsyncWriteExt;
                let (client, mut peer) = tokio::io::duplex(4096);
                let stop = stop.clone();
                peers.lock().unwrap().spawn(async move {
                    let mut head = Vec::new();
                    loop {
                        let byte = tokio::select! { byte = peer.read_u8() => byte.unwrap(), _ = stop.wait() => return }; head.push(byte);
                        assert!(head.len() <= 4096); if head.ends_with(b"\r\n\r\n") { break; }
                    }
                    assert!(head.starts_with(b"GET / HTTP/1.1\r\n"));
                    peer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").await.unwrap(); peer.shutdown().await.unwrap();
                });
                let entered = entered.clone();
                let release = release.clone();
                let reactor = reactor.clone();
                let recorded = Arc::clone(&recorded);
                let io: Io = Box::pin(ProbedIo {
                    inner: Box::pin(client),
                    worker_claimed: Arc::clone(&request.claimed),
                    shutdown_complete: false,
                    shutdown: Box::pin(async move {
                        entered.open();
                        release.wait().await;
                        let timer = {
                            let _entered = reactor.enter();
                            tokio::time::sleep(Duration::from_millis(20))
                        };
                        timer.await;
                        recorded.fetch_add(1, Ordering::SeqCst);
                    }),
                    claimed: Gate::new(),
                    dropped: Arc::clone(&destroyed),
                    worker: owner_thread,
                    app: Handle::current().id(),
                });
                Box::pin(async move {
                    Ok(ConnectedIo::new(
                        request.route().clone(),
                        EstablishedIo::new(
                            io,
                            TransportEndpoint::Unknown,
                            TransportEndpoint::Unknown,
                            None,
                        ),
                        CodecProtocol::Http11,
                        TransportSecurity::Plaintext,
                    ))
                })
            });
            let io_probe = Arc::new(crate::client::owner::TestIoJoin::default());
            let mut settings = options();
            settings.test_io = Some(Arc::clone(&io_probe));
            settings.maximum_physical_connections = 1;
            let (startup, run) = Client::prepare(worker, connector, settings).unwrap();
            case.runs.spawn(run);
            let (client, guard) = startup.start().await.unwrap();
            case.owners.push(guard);
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            assert_eq!(body_bytes(future.await.unwrap().into_body()).await, "ok");
            exchange.wait().await.unwrap();
            close_entered.wait().await;
            let receipt = io_probe
                .adopted
                .lock()
                .unwrap()
                .clone()
                .expect("actual adoption");
            assert!(futures::poll!(Box::pin(receipt.wait_retired())).is_pending());
            assert_eq!(ticks.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(client.shared.physical.available_permits(), 0);
            close_gate.open();
            receipt.wait_retired().await.unwrap();
            case.owners.last().unwrap().shutdown();
            case.owners
                .last()
                .unwrap()
                .wait_app_leaves_drained()
                .await
                .unwrap();
            case.join_runs().await;
            assert!(io_probe.retired.load(Ordering::Acquire));
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(ticks.load(Ordering::SeqCst), 1);
            assert_eq!(Handle::current().id(), app.id());
            case.runtime.quiesce(Duration::from_secs(1)).await.unwrap();
            drop(release_on_drop);
        })
    });
}

#[test]
fn forced_native_driver_drop_keeps_actual_body_join_in_application_parent() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let probe = Arc::new(TestApplicationJoin::default());
            let mut settings = options();
            settings.test_application = Some(Arc::clone(&probe));
            settings.test_shutdown_return = Some(TestLatch::new());
            let cleanup_gate = DropGate::new();
            let release = ReleaseGate(Arc::clone(&cleanup_gate));
            let connector = fixture.connector(CodecProtocol::Http2, simple_handler());
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let gate = Gate::new();
            let mut body =
                TestBody::new(vec![Frame::data(Bytes::from_static(b"prefix"))]).gated(gate);
            body.drop_gate = Some(Arc::clone(&cleanup_gate));
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            while polls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            fixture.force_drop_live_session().await;
            cleanup_gate.entered.wait().await;
            assert!(!probe.reaped.load(Ordering::Acquire));
            assert!(futures::poll!(Box::pin(fixture.runs.join_next())).is_pending());
            let cleanup = probe.cleanup.lock().unwrap().clone().unwrap();
            assert!(futures::poll!(Box::pin(cleanup.wait())).is_pending());
            cleanup_gate.release();
            let exchange = future.exchange();
            assert!(future.await.is_err());
            assert!(exchange.wait().await.is_err());
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(fixture.runs.is_empty());
            assert!(fixture.run_results.iter().any(Result::is_err));
            producer_reaped(&probe).await;
            drop(release);
        })
    });
}

#[test]
fn forced_native_driver_drop_with_all_public_handles_gone_still_reaps_run() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let probe = Arc::new(TestApplicationJoin::default());
            let mut settings = options();
            settings.test_application = Some(Arc::clone(&probe));
            settings.test_shutdown_return = Some(TestLatch::new());
            let cleanup_gate = DropGate::new();
            let release = ReleaseGate(Arc::clone(&cleanup_gate));
            let connector = fixture.connector(CodecProtocol::Http2, simple_handler());
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http2)).unwrap();
            let mut body =
                TestBody::new(vec![Frame::data(Bytes::from_static(b"prefix"))]).gated(Gate::new());
            body.drop_gate = Some(Arc::clone(&cleanup_gate));
            let drops = Arc::clone(&body.drops);
            let polls = Arc::clone(&body.polls);
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            while polls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            fixture.control.pause().await;
            drop(future.exchange());
            drop(future);
            drop(bound);
            drop(client);
            fixture.owners.clear();
            fixture.force_drop_live_session().await;
            cleanup_gate.entered.wait().await;
            assert!(!probe.reaped.load(Ordering::Acquire));
            assert!(futures::poll!(Box::pin(fixture.runs.join_next())).is_pending());
            cleanup_gate.release();
            fixture.close_clients().await;
            producer_reaped(&probe).await;
            drop(release);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(fixture.runs.is_empty());
        })
    });
}

#[test]
fn forced_native_drop_during_startup_and_io_claim_retains_disposition() {
    for after_adoption in [false, true] {
        case(|fixture| {
            Box::pin(async move {
                fixture.expected_run_error = Some(forced_run);
                let mut connector = fixture.connector(CodecProtocol::Http11, simple_handler());
                let drops = Arc::new(AtomicUsize::new(0));
                let io_used = Gate::new();
                let shutdown = Gate::new();
                shutdown.open();
                probe_connector(
                    &mut connector,
                    &fixture.worker,
                    shutdown,
                    io_used.clone(),
                    Arc::clone(&drops),
                );
                let latch = TestLatch::new();
                let mut settings = options();
                let io_probe = Arc::new(crate::client::owner::TestIoJoin::default());
                settings.test_io = Some(Arc::clone(&io_probe));
                if after_adoption {
                    settings.test_after_adoption = Some(Arc::clone(&latch));
                } else {
                    settings.test_handoff = Some(Arc::clone(&latch));
                }
                let client = fixture.start(connector, settings).await;
                let bound = client.route(route(CodecProtocol::Http11)).unwrap();
                let future = client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .unwrap();
                latch.wait_entered().await;
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                let adopted = io_probe.adopted.lock().unwrap().clone();
                assert_eq!(adopted.is_some(), after_adoption);
                if let Some(receipt) = &adopted {
                    assert!(futures::poll!(Box::pin(receipt.wait_retired())).is_pending());
                }
                fixture.stop.open();
                fixture.control.force_drop().await;
                latch.release();
                assert!(future.await.is_err());
                fixture.close_clients().await;
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert!(io_probe.retired.load(Ordering::Acquire));
                if let Some(receipt) = adopted {
                    receipt.wait_retired().await.unwrap();
                }
                if !after_adoption {
                    assert!(!io_used.is_open());
                }
                assert!(fixture.run_results.iter().any(Result::is_err));
            })
        });
    }
    for native_call_enqueued in [false, true] {
        case(|fixture| {
            Box::pin(async move {
                fixture.expected_run_error = if native_call_enqueued {
                    Some(forced_run)
                } else {
                    Some(|error| matches!(error, ClientError::Closed))
                };
                fixture.control.pause().await;
                let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
                let events = Arc::clone(&connector.events);
                let calls = Arc::clone(&connector.calls);
                let (startup, run) =
                    Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
                let release = Gate::new();
                struct ReleaseOnDrop(Gate);
                impl Drop for ReleaseOnDrop {
                    fn drop(&mut self) {
                        self.0.open();
                    }
                }
                let release_on_drop = ReleaseOnDrop(release.clone());
                let admitted = release.clone();
                let native_wait = Gate::new();
                let parked = native_wait.clone();
                fixture.runs.spawn(async move {
                    admitted.wait().await;
                    tokio::pin!(run);
                    futures::future::poll_fn(|context| {
                        let result = run.as_mut().poll(context);
                        if result.is_pending() {
                            parked.open();
                        }
                        result
                    })
                    .await
                });
                let startup = startup.start();
                tokio::pin!(startup);
                assert!(futures::poll!(&mut startup).is_pending());
                if native_call_enqueued {
                    release.open();
                    native_wait.wait().await;
                }
                fixture.control.force_drop().await;
                release.open();
                let error = startup.await.err().expect("startup loses the worker");
                if native_call_enqueued {
                    assert!(matches!(
                        error,
                        ClientStartError::Client(ClientError::CleanupUnconfirmed)
                    ));
                } else {
                    assert!(matches!(
                        error,
                        ClientStartError::Client(ClientError::Closed)
                    ));
                }
                fixture.close_clients().await;
                assert_eq!(fixture.run_results.len(), 1);
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                let dropped: Vec<_> = events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(phase, _)| *phase == "connector-drop")
                    .copied()
                    .collect();
                assert_eq!(dropped.len(), 1);
                assert_eq!(dropped[0].1, Handle::current().id());
                drop(release_on_drop);
            })
        });
    }
}

#[test]
fn app_leaf_drain_does_not_complete_parent_join() {
    for mode in 0..3 {
        terminal_waker_case(mode);
    }
    case(|fixture| {
        Box::pin(async move {
            let connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let (startup, run) =
                Client::prepare(fixture.worker.clone(), connector, options()).unwrap();
            let returned = Gate::new();
            let hold = Gate::new();
            let observed = returned.clone();
            let release = hold.clone();
            struct ReleaseOnDrop(Gate);
            impl Drop for ReleaseOnDrop {
                fn drop(&mut self) {
                    self.0.open();
                }
            }
            let release_on_drop = ReleaseOnDrop(hold.clone());
            fixture.runs.spawn(async move {
                let result = run.await;
                observed.open();
                release.wait().await;
                result
            });
            let (client, owner) = startup.start().await.unwrap();
            fixture.owners.push(owner);
            drop(client);
            fixture
                .owners
                .last()
                .unwrap()
                .wait_app_leaves_drained()
                .await
                .unwrap();
            returned.wait().await;
            let parent_join = fixture.runs.join_next();
            tokio::pin!(parent_join);
            assert!(futures::poll!(&mut parent_join).is_pending());
            hold.open();
            parent_join.await.unwrap().unwrap().unwrap();
            drop(release_on_drop);
        })
    });
}

#[test]
fn forced_native_drop_does_not_recycle_app_parent_ledger_credits() {
    producer_credit_case();
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let mut connector = fixture.connector(CodecProtocol::Http11, simple_handler());
            let drops = Arc::new(AtomicUsize::new(0));
            let shutdown = Gate::new();
            shutdown.open();
            probe_connector(
                &mut connector,
                &fixture.worker,
                shutdown,
                Gate::new(),
                Arc::clone(&drops),
            );
            let latch = TestLatch::new();
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.maximum_physical_connections = 1;
            settings.test_handoff = Some(Arc::clone(&latch));
            let client = fixture.start(connector, settings).await;
            let bound = client.route(route(CodecProtocol::Http11)).unwrap();
            let future = client
                .try_request(
                    &bound,
                    Request::builder()
                        .uri("/")
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
                .unwrap();
            latch.wait_entered().await;
            assert_eq!(client.shared.admission.available_permits(), 0);
            assert_eq!(client.shared.physical.available_permits(), 0);
            fixture.control.force_drop().await;
            for _attempt in 0..3 {
                let rejected = client
                    .try_request(
                        &bound,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .err()
                    .unwrap();
                assert!(matches!(rejected.error, ClientError::Closed));
            }
            latch.release();
            assert!(future.await.is_err());
            fixture.close_clients().await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(client.shared.admission.is_closed());
            assert!(client.shared.physical.is_closed());
        })
    });
}

fn producer_credit_case() {
    case(|fixture| {
        Box::pin(async move {
            fixture.expected_run_error = Some(forced_run);
            let probe = Arc::new(TestApplicationJoin::default());
            let cleanup_gate = DropGate::new();
            let release = ReleaseGate(Arc::clone(&cleanup_gate));
            let mut settings = options();
            settings.maximum_exchanges = 1;
            settings.maximum_physical_connections = 1;
            settings.test_application = Some(Arc::clone(&probe));
            settings.test_shutdown_return = Some(TestLatch::new());
            let peer_pending = Gate::new();
            let pending = peer_pending.clone();
            let handler: Handler = Arc::new(move |request| {
                let pending = pending.clone();
                Box::pin(async move {
                    let body = request.into_body().collect();
                    tokio::pin!(body);
                    let _ = std::future::poll_fn(|context| {
                        let result = body.as_mut().poll(context);
                        if result.is_pending() {
                            pending.open();
                        }
                        result
                    })
                    .await;
                    Response::new(full("ok"))
                })
            });
            let connector = fixture.connector(CodecProtocol::Http2, handler);
            let client = fixture.start(connector, settings).await;
            let route = client.route(route(CodecProtocol::Http2)).unwrap();
            let mut body =
                TestBody::new(vec![Frame::data(Bytes::from_static(b"pending"))]).gated(Gate::new());
            body.drop_gate = Some(Arc::clone(&cleanup_gate));
            let polls = Arc::clone(&body.polls);
            let drops = Arc::clone(&body.drops);
            let future = client
                .try_request(
                    &route,
                    Request::builder()
                        .method("POST")
                        .uri("/")
                        .body(body)
                        .unwrap(),
                )
                .unwrap();
            let exchange = future.exchange();
            while polls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            peer_pending.wait().await;
            fixture.cancel_and_join_pending_peer().await;
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert!(!probe.reaped.load(Ordering::Acquire));
            fixture.force_drop_live_session().await;
            cleanup_gate.entered.wait().await;
            assert!(!probe.reaped.load(Ordering::Acquire));
            assert_eq!(client.shared.admission.available_permits(), 0);
            assert!(futures::poll!(Box::pin(exchange.wait())).is_pending());
            assert!(futures::poll!(Box::pin(fixture.runs.join_next())).is_pending());
            for _attempt in 0..3 {
                let rejected = client
                    .try_request(
                        &route,
                        Request::builder()
                            .uri("/")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                    .err()
                    .unwrap();
                assert!(matches!(rejected.error, ClientError::Closed));
                assert_eq!(client.shared.admission.available_permits(), 0);
            }
            let cleanup = probe.cleanup.lock().unwrap().clone().unwrap();
            assert!(futures::poll!(Box::pin(cleanup.wait())).is_pending());
            cleanup_gate.release();
            assert!(future.await.is_err());
            assert!(exchange.wait().await.unwrap_err().cleanup_unconfirmed());
            fixture.close_clients().await;
            producer_reaped(&probe).await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            drop(release);
        })
    });
}
