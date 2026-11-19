/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc as std_mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Wake;
use std::task::Waker;
use std::thread::JoinHandle;

use http::Response;
use http_body_util::BodyExt;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::CodecProtocol;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http_adapter::HttpAdapterError;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerCleanupObservationError;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::LocalSet;

use crate::extensions::RetiringExtensions;
use crate::server::Http1ConnectionSwitchRejection;
use crate::server::ServerError;
use crate::server::ServerExchange;
use crate::server::ServerFailure;
use crate::server::ServerOptions;
use crate::server::ServerRun;
use crate::server::prepare_connection;
use crate::server_network_tests::fixture::Failure;
use crate::server_network_tests::fixture::FixtureOwners;
use crate::server_network_tests::fixture::FixtureTasks as JoinSet;
use crate::server_network_tests::fixture::Gate;
use crate::server_network_tests::fixture::Outcome;
use crate::server_network_tests::fixture::Parent;
use crate::server_network_tests::fixture::PeerConnection;
use crate::server_network_tests::fixture::PeerTasks;
use crate::server_network_tests::fixture::Probe;
use crate::server_network_tests::fixture::RejectionIoControl;
use crate::server_network_tests::fixture::TIMEOUT;
use crate::server_network_tests::fixture::TestBody;
use crate::server_network_tests::fixture::TestError;
use crate::server_network_tests::fixture::assert_native_error_response;
use crate::server_network_tests::fixture::assert_pending;
use crate::server_network_tests::fixture::observe_rejection;
use crate::server_network_tests::fixture::observe_source;
use crate::server_network_tests::fixture::pending_rejection_read;
use crate::server_network_tests::fixture::read_rejection;
use crate::server_network_tests::fixture::rejection_sockets;
use crate::server_network_tests::fixture::request;
use crate::server_network_tests::fixture::retirement_gate;
use crate::server_network_tests::fixture::run_test;
use crate::server_network_tests::fixture::service;
use crate::server_network_tests::fixture::sockets;
use crate::server_network_tests::fixture::start;
use crate::server_network_tests::fixture::unfinished_switch_request;

fn options(protocol: CodecProtocol, capacity: usize) -> ServerOptions {
    let mut options = ServerOptions::new(protocol, TransportSecurity::Plaintext);
    options.max_exchanges = NonZeroUsize::new(capacity).unwrap();
    options.dispatch_capacity = NonZeroUsize::new(capacity).unwrap();
    options
}

#[test]
fn server_connection_switch_rejection_retires_ingress_before_flush() {
    for tls in [false, true] {
        run_test(|worker, _| async move {
            let io = RejectionIoControl::new();
            io.shutdown.release();
            let probe = Probe::new();
            let (ingress, watcher) = observe_rejection(worker.clone(), probe.clone(), None).await;
            let (established, mut peer) = rejection_sockets(tls, io.clone(), None).await;
            let mut options = options(CodecProtocol::Http11, 1);
            options.security = if tls {
                TransportSecurity::Tls
            } else {
                TransportSecurity::Plaintext
            };
            options.http1_connection_switch_rejection =
                Some(Http1ConnectionSwitchRejection::forbidden());
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service(probe.clone(), |_| async {
                    panic!("rejected ingress must not reach the service")
                }),
                options,
            )
            .unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            peer.write_all(&unfinished_switch_request()).await.unwrap();
            peer.flush().await.unwrap();
            io.write.entered().await;
            ingress.await.unwrap();
            assert_eq!(probe.count("native request eom"), 0);
            assert_eq!(probe.count("native websocket flag"), 1);
            assert_eq!(probe.count("request source stop"), 1);
            assert_eq!(probe.count("request source error"), 0);
            assert_eq!(probe.count("request body read"), 0);
            assert_eq!(probe.count("request source drop"), 1);
            probe.assert_native("request source stop");
            probe.assert_native("request source drop");
            assert_eq!(probe.count("service ready"), 0);
            assert_eq!(probe.count("service call"), 0);
            assert_eq!(io.probe.count("adopted io drop"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            assert_eq!(parent.tasks.len(), 1);
            {
                let response = read_rejection(&mut peer, 403, "1.1");
                tokio::pin!(response);
                assert_pending(response.as_mut()).await;
                io.write.release();
                response.await;
            }
            watcher.await.unwrap();
            connection.wait_app_leaves_drained().await.unwrap();
            parent.reap().await.unwrap();
            assert!(parent.tasks.is_empty());
            assert_eq!(parent.slots.available_permits(), 1);
            assert_eq!(probe.count("service drop"), 1);
            probe.assert_app("service drop");
            assert_eq!(io.probe.count("adopted io drop"), 1);
            assert_eq!(io.probe.count("io drop"), 1);
            io.probe.assert_native("adopted io drop");
            drop(peer);
        });
    }
}

#[test]
fn server_connection_switch_rejection_retains_io_and_parent_credit() {
    for controller_exit in ["release", "unwind", "cancel"] {
        run_test(|worker, _| async move {
            let io = RejectionIoControl::new();
            io.write.release();
            let probe = Probe::new();
            let (ingress, watcher) = observe_rejection(worker.clone(), probe.clone(), None).await;
            let (retirement, release) = retirement_gate();
            let (established, mut peer) =
                rejection_sockets(false, io.clone(), Some(retirement)).await;
            let mut options = options(CodecProtocol::Http11, 1);
            options.http1_connection_switch_rejection =
                Some(Http1ConnectionSwitchRejection::forbidden());
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service(probe.clone(), |_| async {
                    panic!("rejection must not invoke the service")
                }),
                options,
            )
            .unwrap();
            let (produced, production) = oneshot::channel();
            let reap_gate = Gate::default();
            let reap_release = ReapGateRelease(reap_gate.clone());
            parent.tasks.track_run(&run);
            let run = GatedRun {
                run: Box::pin(run),
                result: None,
                produced: Some(produced),
                release: reap_gate.clone(),
            };
            parent.tasks.spawn(async move { (run.await, permit) });
            let connection = startup.start().await.unwrap();
            peer.write_all(&unfinished_switch_request()).await.unwrap();
            peer.flush().await.unwrap();
            io.shutdown.entered().await;
            ingress.await.unwrap();
            watcher.await.unwrap();
            connection.wait_app_leaves_drained().await.unwrap();
            assert_eq!(probe.count("service ready"), 0);
            assert_eq!(probe.count("service call"), 0);
            assert_eq!(probe.count("service drop"), 1);
            assert_eq!(probe.count("request source drop"), 1);
            assert_eq!(io.probe.count("adopted io drop"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            tokio::pin!(production);
            assert_pending(production.as_mut()).await;
            io.shutdown.release();
            release.entered().await;
            assert_eq!(io.probe.count("adopted io drop"), 0);
            assert_eq!(io.probe.count("io drop"), 0);
            assert_pending(production.as_mut()).await;
            assert_eq!(parent.slots.available_permits(), 0);
            release.release();
            production.await.unwrap();
            assert_eq!(io.probe.count("adopted io drop"), 1);
            assert_eq!(io.probe.count("io drop"), 1);
            assert_eq!(parent.slots.available_permits(), 0);
            assert_eq!(parent.tasks.len(), 1);
            read_rejection(&mut peer, 403, "1.1").await;
            match controller_exit {
                "release" => {
                    reap_gate.release();
                    drop(reap_release);
                }
                "unwind" => {
                    let first = Arc::new(PanicMarker);
                    let original = Arc::clone(&first);
                    let payload = catch_unwind(AssertUnwindSafe(move || {
                        let _release = reap_release;
                        panic_any(original);
                    }))
                    .unwrap_err();
                    assert!(Arc::ptr_eq(
                        payload.downcast_ref::<Arc<PanicMarker>>().unwrap(),
                        &first
                    ));
                }
                "cancel" => {
                    let mut controller = Box::pin(async move {
                        let _release = reap_release;
                        std::future::pending::<()>().await;
                    });
                    assert_pending(controller.as_mut()).await;
                    drop(controller);
                }
                _ => unreachable!(),
            }
            assert!(reap_gate.is_released());
            assert_eq!(parent.slots.available_permits(), 0);
            parent.reap().await.unwrap();
            assert!(parent.tasks.is_empty());
            assert_eq!(parent.slots.available_permits(), 1);
            probe.assert_app("service drop");
            probe.assert_native("request source drop");
            io.probe.assert_native("adopted io drop");
            drop(peer);
        });
    }
}

/// Releases only the artificial gate; the registered parent still owns its join and permit.
struct ReapGateRelease(Gate);

impl Drop for ReapGateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct RejectionDropService {
    first: Arc<PanicMarker>,
    probe: Probe,
    not_sync: std::cell::Cell<()>,
}

impl tower_service::Service<http::Request<roxygen_hyper_adapter::body::RoxygenBody>>
    for RejectionDropService
{
    type Response = Response<TestBody>;
    type Error = TestError;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.probe.record("service ready");
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Request<roxygen_hyper_adapter::body::RoxygenBody>) -> Self::Future {
        self.not_sync.set(());
        self.probe.record("service call");
        std::future::ready(Ok(Response::new(TestBody::empty())))
    }
}

impl Drop for RejectionDropService {
    fn drop(&mut self) {
        self.probe.record("service drop");
        panic_any(Arc::clone(&self.first));
    }
}

struct NativeRetirementCapture(Probe);

impl Drop for NativeRetirementCapture {
    fn drop(&mut self) {
        self.0.record("native source capture drop");
    }
}

struct RejectionRetirementSource {
    source: HttpSourceHolder,
    entered: Option<oneshot::Sender<()>>,
    release: std_mpsc::Receiver<()>,
    first: Arc<PanicMarker>,
    probe: Probe,
    _capture: NativeRetirementCapture,
}

impl HttpSource for RejectionRetirementSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.source.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.record("native retirement stop");
        self.source.stop_reading(error);
    }
}

impl Drop for RejectionRetirementSource {
    fn drop(&mut self) {
        self.probe.record("native retirement drop attempt");
        let _ = self.entered.take().unwrap().send(());
        if self.release.recv().is_ok() {
            panic_any(Arc::clone(&self.first));
        }
    }
}

fn rejection_native_source_retirement_panic() {
    let mut native = ManualWorkerHarness::new();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let owners = FixtureOwners::new();
    let first = Arc::new(PanicMarker);
    runtime.block_on(async {
        owners
            .run_case(async {
                let io = RejectionIoControl::new();
                io.shutdown.release();
                let probe = Probe::new();
                let (ingress, watcher) =
                    observe_rejection(native.worker.clone(), probe.clone(), None).await;
                let (entered, dropped) = oneshot::channel();
                let (resume, release) = std_mpsc::sync_channel(1);
                let source_probe = probe.clone();
                let original = Arc::clone(&first);
                native
                    .worker
                    .call(move |_| {
                        let mut fault = Some((entered, release, original));
                        crate::server::install_head_hook(move |source| {
                            let (entered, release, first) = fault.take().unwrap();
                            HttpSourceHolder::new(Box::new(RejectionRetirementSource {
                                source: observe_source(source, source_probe.clone(), None),
                                entered: Some(entered),
                                release,
                                first,
                                probe: source_probe.clone(),
                                _capture: NativeRetirementCapture(source_probe.clone()),
                            }))
                        });
                    })
                    .unwrap()
                    .await
                    .unwrap();
                let (retirement, release_io) = retirement_gate();
                let (established, mut peer) =
                    rejection_sockets(false, io.clone(), Some(retirement)).await;
                let mut options = options(CodecProtocol::Http11, 1);
                options.http1_connection_switch_rejection =
                    Some(Http1ConnectionSwitchRejection::forbidden());
                let mut parent = Parent::new(1);
                let permit = parent.reserve();
                let (startup, run) = prepare_connection(
                    native.worker.clone(),
                    established,
                    service(probe.clone(), |_| async {
                        panic!("source retirement failure must not invoke the service")
                    }),
                    options,
                )
                .unwrap();
                parent.spawn(run, permit);
                let connection = startup.start().await.unwrap();
                peer.write_all(&unfinished_switch_request()).await.unwrap();
                peer.flush().await.unwrap();
                let peer = Arc::new(tokio::sync::Mutex::new(peer));
                let reader_peer = Arc::clone(&peer);
                let pending = Gate::default();
                let observed = pending.clone();
                let mut readers = JoinSet::new();
                readers.spawn(async move {
                    let mut peer = reader_peer.lock().await;
                    let mut byte = [0; 1];
                    let read = peer.read(&mut byte);
                    tokio::pin!(read);
                    assert_pending(read.as_mut()).await;
                    observed.release();
                    read.await
                });
                pending.wait().await;
                dropped.await.unwrap();
                connection.wait_app_leaves_drained().await.unwrap();
                assert_eq!(probe.count("native request eom"), 0);
                assert_eq!(probe.count("native websocket flag"), 1);
                assert_eq!(probe.count("request source stop"), 1);
                assert_eq!(probe.count("request source error"), 0);
                assert_eq!(probe.count("request source drop"), 1);
                assert_eq!(probe.count("native retirement stop"), 1);
                assert_eq!(probe.count("native retirement drop attempt"), 1);
                assert_eq!(probe.count("native source capture drop"), 0);
                assert_eq!(probe.count("service ready"), 0);
                assert_eq!(probe.count("service call"), 0);
                assert_eq!(probe.count("service drop"), 1);
                assert_eq!(io.probe.count("adopted io drop"), 0);
                assert_eq!(parent.slots.available_permits(), 0);
                let mut ingress = Box::pin(ingress);
                let mut watcher = Box::pin(watcher);
                assert_pending(ingress.as_mut()).await;
                assert_pending(watcher.as_mut()).await;
                readers.abort_all();
                assert!(
                    readers
                        .join_next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .is_cancelled()
                );
                assert!(readers.is_empty());
                resume.send(()).unwrap();
                let source_result = ingress.await;
                assert_eq!(
                    source_result,
                    Err(WorkerCleanupObservationError::Unconfirmed)
                );
                assert_eq!(probe.count("native source capture drop"), 1);
                release_io.entered().await;
                assert_eq!(io.probe.count("adopted io drop"), 0);
                assert_eq!(parent.tasks.len(), 1);
                assert_eq!(parent.slots.available_permits(), 0);
                release_io.release();
                let result = parent.reap().await.unwrap_err();
                assert!(!result.cleanup_confirmed());
                watcher.await.unwrap();
                native.stop(false).await;
                assert!(!native.worker.is_accepting());
                assert!(parent.tasks.is_empty());
                assert!(parent.slots.is_closed());
                assert!(parent.slots.try_acquire().is_err());
                assert_eq!(probe.count("native retirement drop attempt"), 1);
                assert_eq!(probe.count("native source capture drop"), 1);
                assert_eq!(io.probe.count("adopted io drop"), 1);
                assert_eq!(io.probe.count("io drop"), 1);
                probe.assert_app("service drop");
                probe.assert_native("native retirement stop");
                probe.assert_native("native retirement drop attempt");
                probe.assert_native("native source capture drop");
                probe.assert_native("request source drop");
                io.probe.assert_native("adopted io drop");
                drop(peer);
            })
            .await;
        owners.finish().await;
        if native.exited.is_some() {
            native.stop(true).await;
        }
    });
    owners
        .failures()
        .capture(|| native.join_source_retirement_panic(&first));
    drop(runtime);
    owners.report();
}

#[test]
fn server_connection_switch_rejection_failure_preserves_first_cause() {
    run_test(|worker, _| async move {
        let io = RejectionIoControl::new();
        io.shutdown.release();
        io.fail_write.store(true, Ordering::SeqCst);
        let probe = Probe::new();
        let (ingress, watcher) = observe_rejection(worker.clone(), probe.clone(), None).await;
        let (established, mut peer) = rejection_sockets(false, io.clone(), None).await;
        let mut options = options(CodecProtocol::Http11, 1);
        options.http1_connection_switch_rejection =
            Some(Http1ConnectionSwitchRejection::forbidden());
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let (startup, run) = prepare_connection(
            worker,
            established,
            service(probe.clone(), |_| async {
                panic!("rejection failure must not invoke the service")
            }),
            options,
        )
        .unwrap();
        parent.spawn(run, permit);
        let connection = startup.start().await.unwrap();
        peer.write_all(&unfinished_switch_request()).await.unwrap();
        peer.flush().await.unwrap();
        io.write.entered().await;
        ingress.await.unwrap();
        {
            let mut byte = [0; 1];
            let read = peer.read(&mut byte);
            tokio::pin!(read);
            assert_pending(read.as_mut()).await;
        }
        assert_eq!(parent.slots.available_permits(), 0);
        io.write.release();
        let error = parent.reap().await.unwrap_err();
        let ServerFailure::Native(cause) = error.cause() else {
            panic!("lost initiating write failure: {error:?}");
        };
        assert_eq!(cause.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
        assert_eq!(cause.message(), b"rejection write failure");
        watcher.await.unwrap();
        assert_eq!(probe.count("service ready"), 0);
        assert_eq!(probe.count("service call"), 0);
        assert_eq!(probe.count("service drop"), 1);
        assert_eq!(probe.count("request source drop"), 1);
        assert_eq!(io.probe.count("adopted io drop"), 1);
        assert_eq!(io.probe.count("io drop"), 1);
        assert!(parent.tasks.is_empty());
        probe.assert_app("service drop");
        probe.assert_native("request source drop");
        io.probe.assert_native("adopted io drop");
        drop(connection);
        drop(peer);
    });
    run_test(|worker, _| async move {
        let io = RejectionIoControl::new();
        io.shutdown.release();
        let probe = Probe::new();
        let first = Arc::new(PanicMarker);
        let (ingress, watcher) = observe_rejection(worker.clone(), probe.clone(), None).await;
        let (retirement, release) = retirement_gate();
        let (established, mut peer) = rejection_sockets(false, io.clone(), Some(retirement)).await;
        let mut options = options(CodecProtocol::Http11, 1);
        options.http1_connection_switch_rejection =
            Some(Http1ConnectionSwitchRejection::forbidden());
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let service = RejectionDropService {
            first: Arc::clone(&first),
            probe: probe.clone(),
            not_sync: std::cell::Cell::new(()),
        };
        let (startup, run) = prepare_connection(worker, established, service, options).unwrap();
        parent.spawn(run, permit);
        let connection = startup.start().await.unwrap();
        peer.write_all(&unfinished_switch_request()).await.unwrap();
        peer.flush().await.unwrap();
        io.write.entered().await;
        ingress.await.unwrap();
        let mut readers = pending_rejection_read(peer).await;
        let leaf = connection.wait_app_leaves_drained().await.unwrap_err();
        assert_panic(&leaf, &first);
        assert_eq!(probe.count("service ready"), 0);
        assert_eq!(probe.count("service call"), 0);
        assert_eq!(probe.count("service drop"), 1);
        probe.assert_app("service drop");
        readers.abort_all();
        assert!(
            readers
                .join_next()
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(readers.is_empty());
        connection.cancel();
        release.entered().await;
        assert_eq!(io.probe.count("adopted io drop"), 0);
        assert_eq!(parent.slots.available_permits(), 0);
        assert_eq!(parent.tasks.len(), 1);
        release.release();
        let error = parent.reap().await.unwrap_err();
        assert_panic(&error, &first);
        assert!(!error.cleanup_confirmed());
        watcher.await.unwrap();
        assert_eq!(probe.count("request source stop"), 1);
        assert_eq!(probe.count("request source drop"), 1);
        assert_eq!(io.probe.count("adopted io drop"), 1);
        assert_eq!(io.probe.count("io drop"), 1);
        assert!(parent.tasks.is_empty());
        assert!(parent.slots.is_closed());
        assert!(parent.slots.try_acquire().is_err());
        probe.assert_native("request source drop");
        io.probe.assert_native("adopted io drop");
    });
    rejection_native_source_retirement_panic();
}

#[test]
fn server_grace_delivers_real_late_head() {
    for protocol in [CodecProtocol::Http11, CodecProtocol::Http2] {
        run_test(|worker, runtime| async move {
            let gate = Gate::default();
            let ready = gate.clone();
            let probe = Probe::new();
            let response_probe = probe.clone();
            let service = service(probe.clone(), move |request| {
                drop(request);
                let gate = ready.clone();
                let body = TestBody::data(b"late-real-body").probed(response_probe.clone());
                async move {
                    gate.wait().await;
                    Ok(Response::builder()
                        .status(201)
                        .header("x-real-head", "arrived-during-grace")
                        .body(body)
                        .unwrap())
                }
            });
            let io = Probe::new();
            let (established, stream) = sockets(io.clone(), None).await;
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) =
                prepare_connection(worker.clone(), established, service, options(protocol, 1))
                    .unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let peer_shutdown = Gate::default();
            let peer_io = Probe::new();
            let (mut peer, peer_finished) = PeerConnection::connect_gated(
                stream,
                protocol,
                &peers,
                peer_shutdown.clone(),
                peer_io.clone(),
            )
            .await;
            let mut requests = JoinSet::new();
            requests.spawn(peer.send(request("/late", TestBody::empty())));
            gate.entered().await;
            assert!(connection.begin_drain());
            let quiescence = runtime.quiesce(TIMEOUT);
            assert!(!worker.is_accepting());
            gate.release();
            let response = requests.join_next().await.unwrap().unwrap().unwrap();
            assert_eq!(response.status(), 201);
            assert_eq!(response.headers()["x-real-head"], "arrived-during-grace");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                b"late-real-body".as_slice()
            );
            let result = parent.reap().await;
            assert!(result.is_ok(), "late grace response cleanup: {result:?}");
            assert_eq!(io.count("io drop"), 1);
            assert_eq!(peer_io.count("io drop"), 0);
            peer_shutdown.entered().await;
            peer_shutdown.release();
            peer_finished.await.unwrap().unwrap();
            drop(peer);
            peers.finish().await;
            assert_eq!(peer_io.count("io drop"), 1);
            peer_io.assert_app("io drop");
            quiescence.await.unwrap();
            probe.assert_app("body poll");
            probe.assert_app("body drop");
            assert!(requests.is_empty());
        });
    }
}

#[test]
fn server_permit_waits_for_all_exchange_joins() {
    run_test(|worker, _| async move {
        let gate = Gate::default();
        let body_gate = gate.clone();
        let probe = Probe::new();
        let response_probe = probe.clone();
        let (sent, mut received) = mpsc::channel(2);
        let service = service(probe.clone(), move |request| {
            sent.try_send(
                request
                    .extensions()
                    .get::<ServerExchange>()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
            let body = TestBody::data(b"charged-until-joined")
                .gated(body_gate.clone())
                .probed(response_probe.clone());
            async move { Ok(Response::new(body)) }
        });
        let mut running = start(worker, service, options(CodecProtocol::Http2, 1), None).await;
        let response = running
            .peer
            .send(request("/first", TestBody::empty()))
            .await
            .unwrap();
        let exchange = received.recv().await.unwrap();
        gate.entered().await;
        let completed = exchange.wait_completed();
        tokio::pin!(completed);
        assert_pending(completed.as_mut()).await;
        eprintln!("permit fixture: requesting the rejected over-capacity exchange");
        assert_native_error_response(
            running
                .peer
                .send(request("/no-credit", TestBody::empty()))
                .await
                .unwrap(),
            b"application server exchange cancelled",
        )
        .await;
        assert_eq!(probe.count("service call"), 1);
        assert_eq!(probe.count("body drop"), 0);
        gate.release();
        eprintln!("permit fixture: collecting the original gated response");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"charged-until-joined".as_slice()
        );
        eprintln!("permit fixture: waiting for all original exchange receipts");
        completed.await.unwrap();
        assert_eq!(probe.count("body drop"), 1);
        assert_eq!(probe.count("service future drop"), 1);
        let response = running
            .peer
            .send(request("/reused-after-receipts", TestBody::empty()))
            .await
            .unwrap();
        response.into_body().collect().await.unwrap();
        received
            .recv()
            .await
            .unwrap()
            .wait_completed()
            .await
            .unwrap();
        assert_eq!(probe.count("service call"), 2);
        let _ = running.finish().await;
        probe.assert_app("body drop");
    });
}

#[test]
fn server_connection_waits_for_adopted_io_retirement() {
    run_test(|worker, _| async move {
        let shutdown = Gate::default();
        let probe = Probe::new();
        let service = service(probe.clone(), |_| async {
            Ok(Response::new(TestBody::empty()))
        });
        let mut running = start(
            worker,
            service,
            options(CodecProtocol::Http11, 1),
            Some(shutdown.clone()),
        )
        .await;
        running.connection.begin_drain();
        running.connection.wait_app_leaves_drained().await.unwrap();
        shutdown.entered().await;
        assert_eq!(probe.count("service drop"), 1);
        assert_eq!(running.io.count("io drop"), 0);
        assert_eq!(running.parent.slots.available_permits(), 0);
        let reap = running.parent.tasks.join_next();
        tokio::pin!(reap);
        assert_pending(reap.as_mut()).await;
        shutdown.release();
        let (result, permit) = reap.await.unwrap().unwrap();
        assert!(result.is_ok(), "I/O retirement outcome: {result:?}");
        assert_eq!(running.io.count("io drop"), 1);
        running.io.assert_native("io drop");
        drop(permit);
        assert_eq!(running.parent.slots.available_permits(), 1);
        drop(running.peer);
        running.peers.finish().await;
    });
}

#[derive(Debug)]
enum NativeCommand {
    Pause {
        entered: oneshot::Sender<()>,
        release: std_mpsc::Receiver<()>,
    },
    DropDriver,
    Finish,
}

struct ManualWorkerHarness {
    worker: WorkerHandle,
    commands: mpsc::Sender<NativeCommand>,
    exited: Option<oneshot::Receiver<()>>,
    exit_result: Option<Result<(), oneshot::error::RecvError>>,
    thread: Option<JoinHandle<()>>,
}

impl ManualWorkerHarness {
    fn new() -> Self {
        let (ready, initialized) = std_mpsc::sync_channel(1);
        let (commands, mut receiver) = mpsc::channel(4);
        let (exited, exit) = oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = Builder::new_current_thread().enable_all().build().unwrap();
            let local = LocalSet::new();
            runtime.block_on(local.run_until(async move {
                let (worker, driver) = WorkerDriver::new(4).unwrap();
                ready.send(worker.clone()).unwrap();
                let mut driver = Box::pin(driver);
                loop {
                    tokio::select! {
                        biased;
                        command = receiver.recv() => match command {
                            Some(NativeCommand::Pause { entered, release }) => {
                                let _ = entered.send(());
                                if release.recv().is_err() { worker.begin_shutdown(); }
                            }
                            Some(NativeCommand::DropDriver) | None => { drop(driver); break; }
                            Some(NativeCommand::Finish) => { worker.begin_shutdown(); }
                        },
                        () = &mut driver => break,
                    }
                }
            }));
            drop(local);
            drop(runtime);
            let _ = exited.send(());
        });
        let worker = match initialized.recv() {
            Ok(worker) => worker,
            Err(error) => match thread.join() {
                Ok(()) => panic!("native fixture exited before initialization: {error}"),
                Err(payload) => std::panic::resume_unwind(payload),
            },
        };
        Self {
            worker,
            commands,
            exited: Some(exit),
            exit_result: None,
            thread: Some(thread),
        }
    }

    async fn pause(&self) -> std_mpsc::SyncSender<()> {
        let (entered, ready) = oneshot::channel();
        let (release, wait) = std_mpsc::sync_channel(1);
        self.commands
            .send(NativeCommand::Pause {
                entered,
                release: wait,
            })
            .await
            .unwrap();
        ready.await.unwrap();
        release
    }

    async fn stop(&mut self, forced: bool) {
        let _ = self
            .commands
            .send(if forced {
                NativeCommand::DropDriver
            } else {
                NativeCommand::Finish
            })
            .await;
        self.exit_result = Some(self.exited.take().unwrap().await);
    }

    fn join_thread(&mut self) -> std::thread::Result<()> {
        assert!(tokio::runtime::Handle::try_current().is_err());
        self.thread.take().unwrap().join()
    }

    fn join(&mut self) {
        if let Err(payload) = self.join_thread() {
            std::panic::resume_unwind(payload);
        }
        assert!(
            matches!(self.exit_result.take(), Some(Ok(()))),
            "normally joined native worker must publish its successful exit",
        );
    }

    fn join_failed_session(&mut self, phase: &str) {
        let expected = match phase {
            "readiness" | "service" => "session handler tasks must be joined",
            "body" => "session streams must be detached",
            _ => panic!("unknown forced session phase: {phase}"),
        };
        let outcome = self.join_thread();
        let payload = match outcome {
            Err(payload) => payload,
            Ok(()) => panic!("forced {phase} worker unexpectedly completed successfully"),
        };
        if payload.downcast_ref::<&'static str>() != Some(&expected) {
            std::panic::resume_unwind(payload);
        }
        assert!(
            matches!(self.exit_result.take(), Some(Err(_))),
            "the failed worker must not publish a successful native exit",
        );
        drop(payload);
    }

    fn join_source_retirement_panic(&mut self, first: &Arc<PanicMarker>) {
        let payload = match self.join_thread() {
            Err(payload) => payload,
            Ok(()) => panic!("native source retirement panic was not retained by the worker"),
        };
        if !payload
            .downcast_ref::<Arc<PanicMarker>>()
            .is_some_and(|payload| Arc::ptr_eq(payload, first))
        {
            std::panic::resume_unwind(payload);
        }
        assert!(matches!(self.exit_result.take(), Some(Err(_))));
        drop(payload);
    }
}

impl Drop for ManualWorkerHarness {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                self.commands.try_send(NativeCommand::DropDriver)
            })) {
                if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    roxygen_runtime::panic::discard_panic(secondary);
                }
            }
            if let Err(payload) = thread.join() {
                if let Err(secondary) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                {
                    roxygen_runtime::panic::discard_panic(secondary);
                }
            }
        }
    }
}

fn run_manual(
    work: impl for<'owner> FnOnce(
        &'owner mut ManualWorkerHarness,
    ) -> Pin<Box<dyn Future<Output = ()> + 'owner>>,
) {
    let mut native = ManualWorkerHarness::new();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let owners = FixtureOwners::new();
    runtime.block_on(async {
        owners.run_case(async { work(&mut native).await }).await;
        owners.finish().await;
        if native.exited.is_some() {
            native.stop(false).await;
        }
    });
    owners.failures().capture(|| native.join());
    drop(runtime);
    owners.report();
}

#[derive(Debug)]
struct PanicMarker;

struct LeafDrop(Probe);
impl Drop for LeafDrop {
    fn drop(&mut self) {
        self.0.record("pending leaf drop");
    }
}

struct ThrowingPayload(Probe);
impl Drop for ThrowingPayload {
    fn drop(&mut self) {
        self.0.record("secondary payload drop");
        panic_any("secondary payload destruction");
    }
}

#[derive(Clone)]
struct ThrowingExtension {
    _drop: Arc<ExtensionDrop>,
}
struct ExtensionDrop {
    marker: Arc<PanicMarker>,
    probe: Probe,
}
impl Drop for ExtensionDrop {
    fn drop(&mut self) {
        self.probe.record("extension drop");
        panic_any(Arc::clone(&self.marker));
    }
}

struct ReadyDrop {
    output: Option<Box<Result<Response<TestBody>, TestError>>>,
    retired: Box<dyn Fn() + Send>,
}
impl Future for ReadyDrop {
    type Output = Result<Response<TestBody>, TestError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(*self.output.take().expect("ready output consumed once"))
    }
}
impl Drop for ReadyDrop {
    fn drop(&mut self) {
        if let Some(output) = self.output.take() {
            let failures = crate::server_network_tests::fixture::Failures::default();
            match *output {
                Ok(response) => {
                    let (mut parts, body) = response.into_parts();
                    failures.capture(|| {
                        drop(RetiringExtensions(std::mem::take(&mut parts.extensions)))
                    });
                    failures.capture(|| drop(parts));
                    failures.capture(|| drop(body));
                }
                Err(error) => {
                    failures.capture(|| drop(error));
                }
            }
            failures.capture(|| (self.retired)());
            if let Some(payload) = failures.take() {
                std::panic::resume_unwind(payload);
            }
        } else {
            (self.retired)();
        }
    }
}

struct GatedReady {
    gate: Gate,
    ready: ReadyDrop,
}
impl Future for GatedReady {
    type Output = Result<Response<TestBody>, TestError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.gate.poll(context).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.ready).poll(context)
    }
}

fn assert_panic(error: &ServerError, marker: &Arc<PanicMarker>) {
    let actual = error
        .first_panic()
        .expect("original panic retained")
        .cloned::<Arc<PanicMarker>>()
        .expect("typed original marker");
    assert!(Arc::ptr_eq(&actual, marker));
    assert!(!error.cleanup_confirmed());
}

fn retirement_case(phase: &'static str, future_panics: bool) {
    run_manual(move |native| {
        Box::pin(async move {
            let first = Arc::new(PanicMarker);
            let extension_marker = Arc::new(PanicMarker);
            let probe = Probe::new();
            let sibling_probe = Probe::new();
            let sibling_gate = Gate::default();
            let service_gate = Gate::default();
            let (send, mut receive) = mpsc::channel(2);
            let response_probe = probe.clone();
            let child_probe = sibling_probe.clone();
            let body_gate = sibling_gate.clone();
            let wait = service_gate.clone();
            let future_marker = Arc::clone(&first);
            let drop_marker = if future_panics {
                Arc::clone(&extension_marker)
            } else {
                Arc::clone(&first)
            };
            let service = service(probe.clone(), move |request| {
                let exchange = request
                    .extensions()
                    .get::<ServerExchange>()
                    .unwrap()
                    .clone();
                send.try_send(exchange.clone()).unwrap();
                if request.uri().path() == "/sibling" {
                    let gate = body_gate.clone();
                    let guard = LeafDrop(child_probe.clone());
                    return Box::pin(async move {
                        let _guard = guard;
                        gate.wait().await;
                        Ok(Response::new(TestBody::empty()))
                    })
                        as crate::server_network_tests::fixture::ServiceFuture;
                }
                let body_probe = response_probe.clone();
                let body = TestBody::data(b"not-delivered")
                    .probed(response_probe.clone())
                    .on_drop(move || panic_any(ThrowingPayload(body_probe.clone())));
                let mut response = Response::new(body);
                if phase == "projection" {
                    *response.version_mut() = http::Version::HTTP_3;
                }
                response.extensions_mut().insert(ThrowingExtension {
                    _drop: Arc::new(ExtensionDrop {
                        marker: Arc::clone(&drop_marker),
                        probe: response_probe.clone(),
                    }),
                });
                let marker = Arc::clone(&future_marker);
                let probe = response_probe.clone();
                Box::pin(GatedReady {
                    gate: wait.clone(),
                    ready: ReadyDrop {
                        output: Some(Box::new(Ok(response))),
                        retired: Box::new(move || {
                            probe.record("ready future drop");
                            if phase == "late cancel" {
                                exchange.cancel();
                            }
                            if future_panics {
                                panic_any(Arc::clone(&marker));
                            }
                        }),
                    },
                })
            });
            let mut running = start(
                native.worker.clone(),
                service,
                options(CodecProtocol::Http2, 2),
                None,
            )
            .await;
            let mut requests = JoinSet::new();
            requests.spawn(running.peer.send(request("/sibling", TestBody::empty())));
            let sibling_exchange = receive.recv().await.unwrap();
            sibling_gate.entered().await;
            requests.spawn(running.peer.send(request("/failing", TestBody::empty())));
            let exchange = receive.recv().await.unwrap();
            service_gate.entered().await;
            let resume = native.pause().await;
            service_gate.release();
            let leaf = running
                .connection
                .wait_app_leaves_drained()
                .await
                .unwrap_err();
            assert_panic(&leaf, &first);
            if phase == "projection" && !future_panics {
                assert!(matches!(
                    leaf.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedHttpVersion(
                        http::Version::HTTP_3
                    ))
                ));
            } else {
                assert!(matches!(leaf.cause(), ServerFailure::Panic(_)));
            }
            assert_eq!(probe.count("ready future drop"), 1);
            assert_eq!(probe.count("extension drop"), 1);
            assert_eq!(probe.count("body drop"), 1);
            assert_eq!(probe.count("secondary payload drop"), 1);
            for event in [
                "ready future drop",
                "extension drop",
                "body drop",
                "secondary payload drop",
            ] {
                probe.assert_app(event);
            }
            assert_eq!(sibling_probe.count("pending leaf drop"), 1);
            sibling_probe.assert_app("pending leaf drop");
            assert_eq!(running.io.count("io drop"), 0);
            assert_eq!(running.parent.slots.available_permits(), 0);
            let completed = exchange.wait_completed();
            tokio::pin!(completed);
            assert_pending(completed.as_mut()).await;
            let sibling_completed = sibling_exchange.wait_completed();
            tokio::pin!(sibling_completed);
            assert_pending(sibling_completed.as_mut()).await;
            assert_eq!(requests.len(), 2);
            {
                let pending = requests.join_next();
                tokio::pin!(pending);
                assert_pending(pending.as_mut()).await;
            }
            requests.abort_all();
            for _ in 0..2 {
                assert!(
                    requests
                        .join_next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .is_cancelled()
                );
            }
            assert!(requests.is_empty());
            resume.send(()).unwrap();
            let error = running.parent.reap().await.unwrap_err();
            assert_panic(&error, &first);
            if phase == "projection" && !future_panics {
                assert!(matches!(
                    error.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedHttpVersion(
                        http::Version::HTTP_3
                    ))
                ));
            } else {
                assert!(matches!(error.cause(), ServerFailure::Panic(_)));
            }
            assert!(running.parent.slots.is_closed());
            completed.await.unwrap_err();
            let _ = sibling_completed.await;
            assert_eq!(running.io.count("io drop"), 1);
            running.io.assert_native("io drop");
            drop(running.peer);
            running.peers.finish().await;
            assert!(requests.is_empty());
        })
    });
}

#[test]
fn server_response_retirement_preserves_first_failure() {
    for phase in ["success", "projection", "late cancel"] {
        retirement_case(phase, false);
    }
}

#[test]
fn server_ready_service_output_retirement_preserves_first_failure() {
    for phase in ["success", "late cancel"] {
        retirement_case(phase, true);
    }
    run_test(|worker, _| async move {
        let marker = Arc::new(());
        let original = Arc::clone(&marker);
        let panic = Arc::new(PanicMarker);
        let retained = Arc::clone(&panic);
        let probe = Probe::new();
        let dropped = probe.clone();
        let ready = Gate::default();
        let wait = ready.clone();
        let service = service(probe.clone(), move |_| {
            let panic = Arc::clone(&retained);
            let dropped = dropped.clone();
            GatedReady {
                gate: wait.clone(),
                ready: ReadyDrop {
                    output: Some(Box::new(Err(TestError(Arc::clone(&original))))),
                    retired: Box::new(move || {
                        dropped.record("ready error future drop");
                        panic_any(Arc::clone(&panic));
                    }),
                },
            }
        });
        let mut running = start(
            worker.clone(),
            service,
            options(CodecProtocol::Http2, 1),
            None,
        )
        .await;
        let mut requests = JoinSet::new();
        requests.spawn(running.peer.send(request("/error", TestBody::empty())));
        ready.entered().await;
        let (entered, stopped) = oneshot::channel();
        let (resume, blocked) = std_mpsc::sync_channel(1);
        let paused = worker
            .call(move |_| {
                let _ = entered.send(());
                let _ = blocked.recv();
            })
            .unwrap();
        stopped.await.unwrap();
        ready.release();
        let leaf = running
            .connection
            .wait_app_leaves_drained()
            .await
            .unwrap_err();
        assert_panic(&leaf, &panic);
        let ServerFailure::Service(original) = leaf.cause() else {
            panic!("original service error lost before request cancellation");
        };
        assert!(Arc::ptr_eq(
            &original.downcast_ref::<TestError>().unwrap().0,
            &marker
        ));
        assert_eq!(probe.count("ready error future drop"), 1);
        assert_eq!(running.io.count("io drop"), 0);
        assert_eq!(running.parent.slots.available_permits(), 0);
        {
            let pending = requests.join_next();
            tokio::pin!(pending);
            assert_pending(pending.as_mut()).await;
        }
        requests.abort_all();
        assert!(
            requests
                .join_next()
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(requests.is_empty());
        resume.send(()).unwrap();
        paused.await.unwrap();
        let error = running.parent.reap().await.unwrap_err();
        assert_panic(&error, &panic);
        let ServerFailure::Service(error) = error.cause() else {
            panic!("original service error lost");
        };
        assert!(Arc::ptr_eq(
            &error.downcast_ref::<TestError>().unwrap().0,
            &marker
        ));
        assert_eq!(probe.count("ready error future drop"), 1);
        probe.assert_app("ready error future drop");
        assert_eq!(running.io.count("io drop"), 1);
        running.io.assert_native("io drop");
        assert!(running.parent.slots.is_closed());
        drop(running.peer);
        running.peers.finish().await;
    });
    let probe = Probe::new();
    let retired = probe.clone();
    run_test(move |worker, runtime| async move {
        let marker = Arc::new(());
        let panic = Arc::new(PanicMarker);
        let ready = Gate::default();
        let service = ErrorRetirementService {
            marker: Arc::clone(&marker),
            panic: Arc::clone(&panic),
            probe: retired.clone(),
            ready: ready.clone(),
        };
        let io = Probe::new();
        let (established, stream) = sockets(io.clone(), None).await;
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let (startup, run) = prepare_connection(
            worker.clone(),
            established,
            service,
            options(CodecProtocol::Http2, 1),
        )
        .unwrap();
        parent.spawn(run, permit);
        let connection = startup.start().await.unwrap();
        let mut peers = PeerTasks::new();
        let mut peer = PeerConnection::connect(stream, CodecProtocol::Http2, &peers).await;
        let mut requests = JoinSet::new();
        requests.spawn(peer.send(request("/throwing-error", TestBody::empty())));
        ready.entered().await;
        let (entered, stopped) = oneshot::channel();
        let (resume, blocked) = std_mpsc::sync_channel(1);
        let paused = worker
            .call(move |_| {
                let _ = entered.send(());
                let _ = blocked.recv();
            })
            .unwrap();
        stopped.await.unwrap();
        ready.release();
        let leaf = connection.wait_app_leaves_drained().await.unwrap_err();
        assert_panic(&leaf, &panic);
        let ServerFailure::Service(original) = leaf.cause() else {
            panic!("ready typed error lost before request cancellation");
        };
        assert!(Arc::ptr_eq(
            &original.downcast_ref::<ErrorRetirement>().unwrap().marker,
            &marker
        ));
        assert_eq!(retired.count("ready throwing-error future drop"), 1);
        assert_eq!(retired.count("error drop"), 0);
        assert_eq!(io.count("io drop"), 0);
        assert_eq!(parent.slots.available_permits(), 0);
        {
            let pending = requests.join_next();
            tokio::pin!(pending);
            assert_pending(pending.as_mut()).await;
        }
        requests.abort_all();
        assert!(
            requests
                .join_next()
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(requests.is_empty());
        resume.send(()).unwrap();
        paused.await.unwrap();
        let error = parent.reap().await.unwrap_err();
        assert_panic(&error, &panic);
        let ServerFailure::Service(original) = error.cause() else {
            panic!("ready typed error lost");
        };
        assert!(Arc::ptr_eq(
            &original.downcast_ref::<ErrorRetirement>().unwrap().marker,
            &marker
        ));
        assert_eq!(retired.count("ready throwing-error future drop"), 1);
        assert_eq!(retired.count("error drop"), 0);
        assert_eq!(io.count("io drop"), 1);
        assert!(parent.slots.is_closed());
        drop(connection);
        drop(peer);
        peers.finish().await;
        runtime.quiesce(TIMEOUT).await.unwrap();
        drop(error);
    });
    assert_eq!(probe.count("error drop"), 1);
    assert_eq!(probe.count("secondary payload drop"), 1);
    probe.assert_app("ready throwing-error future drop");
    probe.assert_app("error drop");
}

struct ErrorRetirement {
    marker: Arc<()>,
    probe: Probe,
}
impl std::fmt::Debug for ErrorRetirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ErrorRetirement")
    }
}
impl std::fmt::Display for ErrorRetirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("typed error retirement")
    }
}
impl std::error::Error for ErrorRetirement {}
impl Drop for ErrorRetirement {
    fn drop(&mut self) {
        self.probe.record("error drop");
        panic_any(ThrowingPayload(self.probe.clone()));
    }
}
struct ReadyErrorRetirement {
    error: Option<roxygen_hyper_adapter::body::BoxError>,
    panic: Arc<PanicMarker>,
    probe: Probe,
    ready: Gate,
}
impl Future for ReadyErrorRetirement {
    type Output = Result<Response<TestBody>, roxygen_hyper_adapter::body::BoxError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ready.poll(context).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(Err(self.error.take().expect("ready error consumed once")))
    }
}
impl Drop for ReadyErrorRetirement {
    fn drop(&mut self) {
        let pending = self.error.take();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(pending))) {
            roxygen_runtime::panic::discard_panic(payload);
        }
        self.probe.record("ready throwing-error future drop");
        panic_any(Arc::clone(&self.panic));
    }
}
struct ErrorRetirementService {
    marker: Arc<()>,
    panic: Arc<PanicMarker>,
    probe: Probe,
    ready: Gate,
}
impl tower_service::Service<http::Request<roxygen_hyper_adapter::body::RoxygenBody>>
    for ErrorRetirementService
{
    type Response = Response<TestBody>;
    type Error = roxygen_hyper_adapter::body::BoxError;
    type Future = ReadyErrorRetirement;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: http::Request<roxygen_hyper_adapter::body::RoxygenBody>) -> Self::Future {
        ReadyErrorRetirement {
            error: Some(Box::new(ErrorRetirement {
                marker: Arc::clone(&self.marker),
                probe: self.probe.clone(),
            })),
            panic: Arc::clone(&self.panic),
            probe: self.probe.clone(),
            ready: self.ready.clone(),
        }
    }
}

struct HeadOverride {
    source: HttpSourceHolder,
    projection: bool,
}
impl HttpSource for HeadOverride {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let header = self.source.read_header_event();
        let projection = self.projection;
        Box::pin(async move {
            let mut header = header.await?;
            if projection {
                header.headers_mut().set_upgraded(true);
                Ok(header)
            } else {
                Err(HttpError::with_message(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                    b"initiating native header marker",
                ))
            }
        })
    }
    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.source.read_body_event(maximum)
    }
    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
    }
}

struct DrainFailure(Arc<AtomicUsize>);
impl LifecycleObserver for DrainFailure {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(event, LifecycleEvent::DrainStarted(_)) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic_any("initiating drain failure");
        }
    }
}

#[test]
fn server_initiating_errors_survive_cancellation() {
    for phase in [
        "native",
        "header",
        "projection",
        "drain",
        "earlier application",
    ] {
        run_test(move |worker, _| async move {
            if matches!(phase, "header" | "projection") {
                worker
                    .call(move |_| {
                        crate::server::install_head_hook(move |source| {
                            HttpSourceHolder::new(Box::new(HeadOverride {
                                source,
                                projection: phase == "projection",
                            }))
                        })
                    })
                    .unwrap()
                    .await
                    .unwrap();
            }
            let mut configured = options(CodecProtocol::Http2, 2);
            let drains = Arc::new(AtomicUsize::new(0));
            if phase == "drain" {
                let calls = Arc::clone(&drains);
                configured.session.lifecycle_observer_factory =
                    Some(Arc::new(move || Rc::new(DrainFailure(Arc::clone(&calls)))));
            }
            let io = Probe::new();
            let probe = Probe::new();
            let (mut established, stream) = sockets(io.clone(), None).await;
            let (retirement, release) = retirement_gate();
            established.io.retirement = Some(retirement);
            let native_error = Gate::default();
            if matches!(phase, "native" | "earlier application") {
                established.io.read_failure = Some(native_error.clone());
            }
            let pending = Gate::default();
            let wait = pending.clone();
            let marker = Arc::new(());
            let original = Arc::clone(&marker);
            let (send, mut receive) = mpsc::channel(2);
            let service = service(probe.clone(), move |request| {
                let failed = request.uri().path() == "/first-error";
                let marker = Arc::clone(&original);
                let wait = wait.clone();
                send.try_send(
                    request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                async move {
                    if failed {
                        return Err(TestError(marker));
                    }
                    wait.wait().await;
                    Ok(Response::new(TestBody::empty()))
                }
            });
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) =
                prepare_connection(worker, established, service, configured).unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let mut peer = PeerConnection::connect(stream, CodecProtocol::Http2, &peers).await;
            if phase == "earlier application" {
                assert_native_error_response(
                    peer.send(request("/first-error", TestBody::empty()))
                        .await
                        .unwrap(),
                    b"application server exchange cancelled",
                )
                .await;
                let error = receive
                    .recv()
                    .await
                    .unwrap()
                    .wait_completed()
                    .await
                    .unwrap_err();
                assert!(matches!(error.cause(), ServerFailure::Service(_)));
            }
            let mut requests = JoinSet::new();
            requests.spawn(peer.send(request("/pending", TestBody::empty())));
            if matches!(phase, "header" | "projection") {
                assert_native_error_response(
                    requests.join_next().await.unwrap().unwrap().unwrap(),
                    if phase == "header" {
                        b"initiating native header marker"
                    } else {
                        b"application server exchange cancelled"
                    },
                )
                .await;
                assert_eq!(probe.count("service call"), 0);
                connection.cancel();
            } else {
                pending.entered().await;
                if phase == "drain" {
                    connection.begin_drain();
                } else {
                    native_error.release();
                }
            }
            release.entered().await;
            assert_eq!(io.count("io drop"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            let (result, permit) = {
                let reaped = parent.tasks.join_next();
                tokio::pin!(reaped);
                assert_pending(reaped.as_mut()).await;
                release.release();
                reaped.await.unwrap().unwrap()
            };
            let error = result.unwrap_err();
            match phase {
                "projection" => assert!(matches!(
                    error.cause(),
                    ServerFailure::Projection(HttpAdapterError::UnsupportedMessageMetadata(
                        roxygen_http_adapter::UnsupportedMessageMetadata::Upgraded
                    ))
                )),
                "earlier application" => {
                    let ServerFailure::Service(error) = error.cause() else {
                        panic!("earlier application cause was replaced");
                    };
                    assert!(Arc::ptr_eq(
                        &error.downcast_ref::<TestError>().unwrap().0,
                        &marker
                    ));
                }
                _ => {
                    let ServerFailure::Native(error) = error.cause() else {
                        panic!("initiating native cause was replaced: {error:?}");
                    };
                    let (code, message): (_, &[u8]) = match phase {
                        "native" => (
                            HttpErrorCode::TRANSPORT_READ_ERROR,
                            b"initiating native read marker",
                        ),
                        "header" => (
                            HttpErrorCode::TRANSPORT_READ_ERROR,
                            b"initiating native header marker",
                        ),
                        "drain" => (
                            HttpErrorCode::INTERNAL_ERROR,
                            b"session drain callback panicked",
                        ),
                        _ => unreachable!(),
                    };
                    assert_eq!(error.code(), code);
                    assert_eq!(error.message(), message);
                }
            }
            if !error.cleanup_confirmed() {
                parent.slots.close();
            }
            drop(permit);
            assert!(parent.tasks.is_empty());
            assert_eq!(io.count("io drop"), 1);
            io.assert_native("io drop");
            if phase == "drain" {
                assert_eq!(drains.load(Ordering::SeqCst), 1);
            }
            if !matches!(phase, "header" | "projection") {
                assert_eq!(
                    probe.count("service future drop"),
                    if phase == "earlier application" { 2 } else { 1 }
                );
                probe.assert_app("service future drop");
                requests.join_next().await.unwrap().unwrap().unwrap_err();
            }
            drop(peer);
            peers.finish().await;
            assert!(requests.is_empty());
        });
    }
}

#[derive(Default)]
struct ThrowingWake {
    payload: Mutex<Option<Failure>>,
    target: Mutex<Option<Waker>>,
    calls: AtomicUsize,
}
impl Wake for ThrowingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let target = self.target.lock().unwrap().clone();
        if let Some(target) = target {
            target.wake();
        }
        let payload = self.payload.lock().unwrap().take();
        if let Some(payload) = payload {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::panic::resume_unwind(payload);
        }
    }
}

fn register_throwing<Work: Future>(future: Pin<&mut Work>, payload: Failure) -> Arc<ThrowingWake> {
    let wake = Arc::new(ThrowingWake::default());
    *wake.payload.lock().unwrap() = Some(payload);
    let waker = Waker::from(Arc::clone(&wake));
    assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
    wake
}

struct WakeRun {
    run: Pin<Box<ServerRun>>,
    wake: Arc<ThrowingWake>,
}
impl Future for WakeRun {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Outcome> {
        *self.wake.target.lock().unwrap() = Some(context.waker().clone());
        let waker = Waker::from(Arc::clone(&self.wake));
        self.run.as_mut().poll(&mut Context::from_waker(&waker))
    }
}

#[test]
fn server_notifications_preserve_join_ownership() {
    for phase in ["completion", "leaf"] {
        run_test(move |worker, _| async move {
            let first = Arc::new(PanicMarker);
            let probe = Probe::new();
            let body_probe = Probe::new();
            let response_probe = body_probe.clone();
            let first_gate = Gate::default();
            let first_wait = first_gate.clone();
            let sibling_gate = Gate::default();
            let sibling_wait = sibling_gate.clone();
            let (send, mut receive) = mpsc::channel(2);
            let service = service(probe.clone(), move |request| {
                send.try_send(
                    request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                let gate = first_wait.clone();
                let body = if request.uri().path() == "/sibling" {
                    TestBody::data(b"sibling-producer")
                        .gated(sibling_wait.clone())
                        .probed(response_probe.clone())
                } else {
                    TestBody::empty()
                };
                let sibling = request.uri().path() == "/sibling";
                async move {
                    if !sibling {
                        gate.wait().await;
                    }
                    Ok(Response::new(body))
                }
            });
            let io = Probe::new();
            let (mut established, stream) = sockets(io.clone(), None).await;
            let (retirement, release) = retirement_gate();
            established.io.retirement = Some(retirement);
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service,
                options(CodecProtocol::Http2, 2),
            )
            .unwrap();
            parent.spawn(run, permit);
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let mut peer = PeerConnection::connect(stream, CodecProtocol::Http2, &peers).await;
            let sibling = peer
                .send(request("/sibling", TestBody::empty()))
                .await
                .unwrap();
            let sibling_exchange = receive.recv().await.unwrap();
            sibling_gate.entered().await;
            let mut requests = JoinSet::new();
            requests.spawn(peer.send(request("/first", TestBody::empty())));
            let exchange = receive.recv().await.unwrap();
            first_gate.entered().await;
            let mut completed = Box::pin(exchange.wait_completed());
            let mut leaves = Box::pin(connection.wait_app_leaves_drained());
            let first_wake = if phase == "completion" {
                register_throwing(completed.as_mut(), Box::new(Arc::clone(&first)))
            } else {
                register_throwing(leaves.as_mut(), Box::new(Arc::clone(&first)))
            };
            let mut independent = Box::pin(exchange.wait_completed());
            assert_pending(independent.as_mut()).await;
            let mut independent_leaf = Box::pin(connection.wait_app_leaves_drained());
            assert_pending(independent_leaf.as_mut()).await;
            let secondary = if phase == "completion" {
                Some(register_throwing(
                    leaves.as_mut(),
                    Box::new(ThrowingPayload(probe.clone())),
                ))
            } else {
                None
            };
            first_gate.release();
            eprintln!("notification fixture {phase}: waiting for first exchange completion");
            independent.await.unwrap();
            if phase == "leaf" {
                assert_eq!(body_probe.count("body drop"), 0);
                connection.cancel();
            }
            eprintln!("notification fixture {phase}: waiting for native I/O retirement entry");
            release.entered().await;
            assert_eq!(io.count("io drop"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            {
                let reaped = parent.tasks.join_next();
                tokio::pin!(reaped);
                assert_pending(reaped.as_mut()).await;
            }
            release.release();
            eprintln!("notification fixture {phase}: waiting for application leaves");
            let leaf = independent_leaf.await;
            let _ = leaves.await;
            let _ = completed.await;
            assert_eq!(first_wake.calls.load(Ordering::SeqCst), 1);
            if let Some(secondary) = secondary {
                assert_eq!(secondary.calls.load(Ordering::SeqCst), 1);
                assert_eq!(probe.count("secondary payload drop"), 1);
                assert_panic(leaf.as_ref().unwrap_err(), &first);
            }
            assert_eq!(body_probe.count("body drop"), 1);
            body_probe.assert_app("body drop");
            let (outcome, permit) = parent.tasks.join_next().await.unwrap().unwrap();
            assert_panic(&outcome.unwrap_err(), &first);
            parent.slots.close();
            drop(permit);
            assert!(parent.tasks.is_empty());
            let _ = sibling_exchange.wait_completed().await;
            assert_eq!(io.count("io drop"), 1);
            if let Ok(response) = requests.join_next().await.unwrap().unwrap() {
                let _ = response.into_body().collect().await;
            }
            drop(sibling);
            drop(peer);
            peers.finish().await;
        });
    }
    let disposed = Arc::new(AtomicUsize::new(0));
    let disposal = Arc::clone(&disposed);
    run_test(move |worker, _| async move {
        struct ActivePayload(Arc<AtomicUsize>);
        impl Drop for ActivePayload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic_any("active-unwind secondary destruction");
            }
        }
        let probe = Probe::new();
        let gate = Gate::default();
        let wait = gate.clone();
        let service = service(probe.clone(), move |_| {
            let gate = wait.clone();
            async move {
                gate.wait().await;
                Ok(Response::new(TestBody::empty()))
            }
        });
        let io = Probe::new();
        let (established, stream) = sockets(io.clone(), None).await;
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let (startup, run) = prepare_connection(
            worker.clone(),
            established,
            service,
            options(CodecProtocol::Http2, 1),
        )
        .unwrap();
        parent.tasks.track_run(&run);
        let wake = Arc::new(ThrowingWake::default());
        let driven = WakeRun {
            run: Box::pin(run),
            wake: Arc::clone(&wake),
        };
        parent.tasks.spawn(async move { (driven.await, permit) });
        let connection = startup.start().await.unwrap();
        let mut peers = PeerTasks::new();
        let mut peer = PeerConnection::connect(stream, CodecProtocol::Http2, &peers).await;
        let mut requests = JoinSet::new();
        requests.spawn(peer.send(request("/active-unwind", TestBody::empty())));
        gate.entered().await;
        let (entered, stopped) = oneshot::channel();
        let (resume, blocked) = std_mpsc::sync_channel(1);
        let paused = worker
            .call(move |_| {
                let _ = entered.send(());
                let _ = blocked.recv();
            })
            .unwrap();
        stopped.await.unwrap();
        *wake.payload.lock().unwrap() = Some(Box::new(ActivePayload(disposal)));
        let marker = Arc::new(PanicMarker);
        let original = Arc::clone(&marker);
        let caught = catch_unwind(AssertUnwindSafe(move || {
            let _connection = connection;
            panic_any(original);
        }))
        .unwrap_err();
        assert!(Arc::ptr_eq(
            caught.downcast_ref::<Arc<PanicMarker>>().unwrap(),
            &marker
        ));
        assert_eq!(io.count("io drop"), 0);
        assert_eq!(parent.slots.available_permits(), 0);
        {
            let pending = requests.join_next();
            tokio::pin!(pending);
            assert_pending(pending.as_mut()).await;
        }
        requests.abort_all();
        assert!(
            requests
                .join_next()
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert!(requests.is_empty());
        resume.send(()).unwrap();
        paused.await.unwrap();
        let error = parent.reap().await.unwrap_err();
        assert!(matches!(error.cause(), ServerFailure::Panic(_)));
        assert!(!error.cleanup_confirmed());
        assert_eq!(wake.calls.load(Ordering::SeqCst), 1);
        assert_eq!(probe.count("service future drop"), 1);
        probe.assert_app("service future drop");
        assert_eq!(io.count("io drop"), 1);
        io.assert_native("io drop");
        drop(peer);
        peers.finish().await;
    });
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
}

#[test]
fn server_partial_setup_panic_transfers_cleanup() {
    for phase in ["watcher activation", "ingress prepared"] {
        run_manual(move |native| {
            Box::pin(async move {
                let marker = Arc::new(PanicMarker);
                let original = Arc::clone(&marker);
                let (send, mut receive) = mpsc::channel(2);
                let (resume, paused) = std_mpsc::sync_channel(1);
                let attempts = Arc::new(AtomicUsize::new(0));
                let called = Arc::clone(&attempts);
                native
                    .worker
                    .call(move |_| {
                        crate::server::install_setup_hook(move |boundary, receipt| {
                            assert!(send.try_send(receipt).is_ok());
                            if phase == boundary {
                                called.fetch_add(1, Ordering::SeqCst);
                                let _ = paused.recv();
                                panic_any(Arc::clone(&original));
                            }
                        })
                    })
                    .unwrap()
                    .await
                    .unwrap();
                let probe = Probe::new();
                let io = Probe::new();
                let (mut established, stream) = sockets(io.clone(), None).await;
                let (retirement, release) = retirement_gate();
                established.io.retirement = Some(retirement);
                let service = service(probe.clone(), |_| async {
                    Ok(Response::new(TestBody::empty()))
                });
                let mut parent = Parent::new(1);
                let permit = parent.reserve();
                let (startup, run) = prepare_connection(
                    native.worker.clone(),
                    established,
                    service,
                    options(CodecProtocol::Http2, 1),
                )
                .unwrap();
                parent.spawn(run, permit);
                let connection = startup.start().await.unwrap();
                let mut peers = PeerTasks::new();
                let mut peer = PeerConnection::connect(stream, CodecProtocol::Http2, &peers).await;
                let mut requests = JoinSet::new();
                requests.spawn(peer.send(request("/partial", TestBody::empty())));
                let watcher = receive.recv().await.unwrap();
                let ingress = if phase == "ingress prepared" {
                    Some(receive.recv().await.unwrap())
                } else {
                    None
                };
                assert_eq!(probe.count("service call"), 0);
                assert_eq!(io.count("io drop"), 0);
                assert_eq!(parent.slots.available_permits(), 0);
                {
                    let pending = requests.join_next();
                    tokio::pin!(pending);
                    assert_pending(pending.as_mut()).await;
                }
                requests.abort_all();
                assert!(
                    requests
                        .join_next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .is_cancelled()
                );
                assert!(requests.is_empty());
                resume.send(()).unwrap();
                release.entered().await;
                assert_eq!(parent.slots.available_permits(), 0);
                assert_eq!(io.count("io drop"), 0);
                {
                    let reaped = parent.tasks.join_next();
                    tokio::pin!(reaped);
                    assert_pending(reaped.as_mut()).await;
                }
                release.release();
                let error = parent.reap().await.unwrap_err();
                assert_panic(&error, &marker);
                assert!(matches!(error.cause(), ServerFailure::Panic(_)));
                assert!(parent.slots.is_closed());
                assert!(parent.slots.clone().try_acquire_owned().is_err());
                assert_eq!(attempts.load(Ordering::SeqCst), 1);
                watcher.wait().await.unwrap();
                if let Some(ingress) = ingress {
                    ingress.wait().await.unwrap();
                }
                assert_eq!(probe.count("service call"), 0);
                assert_eq!(probe.count("service drop"), 1);
                probe.assert_app("service drop");
                assert_eq!(io.count("io drop"), 1);
                io.assert_native("io drop");
                drop(connection);
                drop(peer);
                peers.finish().await;
                assert!(requests.is_empty());
            })
        });
    }
}

#[test]
fn server_run_loss_finishes_escaped_observers() {
    run_manual(|native| {
        Box::pin(async move {
            let gate = Gate::default();
            let wait = gate.clone();
            let probe = Probe::new();
            let response_probe = probe.clone();
            let (send, mut receive) = mpsc::channel(1);
            let service = service(probe.clone(), move |request| {
                send.try_send(
                    request
                        .extensions()
                        .get::<ServerExchange>()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                let wait = wait.clone();
                let probe = response_probe.clone();
                async move {
                    wait.wait().await;
                    Ok(Response::new(TestBody::empty().probed(probe)))
                }
            });
            let mut running = start(
                native.worker.clone(),
                service,
                options(CodecProtocol::Http2, 1),
                None,
            )
            .await;
            let mut requests = JoinSet::new();
            requests.spawn(running.peer.send(request("/lost-run", TestBody::empty())));
            let exchange = receive.recv().await.unwrap();
            gate.entered().await;
            let mut first = Box::pin(exchange.wait_completed());
            let mut second = Box::pin(exchange.wait_completed());
            assert_pending(first.as_mut()).await;
            assert_pending(second.as_mut()).await;
            let resume = native.pause().await;
            running.connection.begin_drain();
            gate.release();
            running.connection.wait_app_leaves_drained().await.unwrap();
            assert_eq!(probe.count("service future drop"), 1);
            assert_eq!(probe.count("body drop"), 1);
            probe.assert_app("service future drop");
            probe.assert_app("body drop");
            assert_pending(first.as_mut()).await;
            assert_eq!(running.io.count("io drop"), 0);
            running.parent.slots.close();
            running.parent.tasks.abort_all();
            let joined = running.parent.tasks.join_next().await.unwrap().unwrap_err();
            assert!(joined.is_cancelled());
            assert!(running.parent.tasks.is_empty());
            let first = first.await.unwrap_err();
            let second = second.await.unwrap_err();
            let late = exchange.wait_completed().await.unwrap_err();
            assert!(!first.cleanup_confirmed());
            assert!(matches!(first.cause(), ServerFailure::Cleanup(_)));
            assert!(Arc::ptr_eq(&first, &second));
            assert!(Arc::ptr_eq(&first, &late));
            assert!(running.parent.slots.clone().try_acquire_owned().is_err());
            assert_eq!(running.io.count("io drop"), 0);
            resume.send(()).unwrap();
            drop(running.peer);
            running.peers.finish().await;
            let _ = requests.join_next().await.unwrap().unwrap();
            native.stop(false).await;
            assert_eq!(running.io.count("io drop"), 1);
            running.io.assert_native("io drop");
        })
    });
}

#[test]
fn server_fixture_failures_reap_all_children() {
    use hyper::rt::Executor;
    for phase in ["peer child", "case"] {
        let marker = Arc::new(PanicMarker);
        let expected = Arc::clone(&marker);
        let dropped = Arc::new(AtomicUsize::new(0));
        let retired = Arc::clone(&dropped);
        let failure = catch_unwind(AssertUnwindSafe(move || {
            run_test(move |_, _| async move {
                struct CountDrop(Arc<AtomicUsize>);
                impl Drop for CountDrop {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let mut peers = PeerTasks::new();
                let pending = Gate::default();
                let wait = pending.clone();
                let guard = CountDrop(retired);
                peers.executor().execute(async move {
                    let _guard = guard;
                    wait.wait().await;
                });
                pending.entered().await;
                if phase == "case" {
                    panic_any(marker);
                }
                let failing = Gate::default();
                let ready = failing.clone();
                peers.executor().execute(async move {
                    ready.release();
                    let _ = ready.poll(&mut Context::from_waker(Waker::noop()));
                    panic_any(marker);
                });
                failing.entered().await;
                peers.finish().await;
                assert_eq!(peers.len(), 0);
            })
        }))
        .unwrap_err();
        assert!(Arc::ptr_eq(
            failure.downcast_ref::<Arc<PanicMarker>>().unwrap(),
            &expected
        ));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    run_test(|_, _| async move {
        let owners = FixtureOwners::new();
        let mut tasks = JoinSet::new();
        let probe = Probe::new();
        let guard = LeafDrop(probe.clone());
        let gate = Gate::default();
        let wait = gate.clone();
        tasks.spawn(async move {
            let _guard = guard;
            wait.wait().await;
        });
        gate.entered().await;
        owners
            .run_case_until(std::time::Duration::ZERO, std::future::pending())
            .await;
        owners.finish().await;
        assert!(tasks.is_empty());
        assert_eq!(probe.count("pending leaf drop"), 1);
        probe.assert_app("pending leaf drop");
        let error = owners.failures().take().unwrap();
        assert_eq!(error.downcast_ref::<&str>(), Some(&"fixture case timeout"));
    });
    let marker = Arc::new(PanicMarker);
    let expected = Arc::clone(&marker);
    let dropped = Arc::new(AtomicUsize::new(0));
    let retired = Arc::clone(&dropped);
    let error = catch_unwind(AssertUnwindSafe(move || {
        run_manual(move |native| {
            Box::pin(async move {
                struct PauseLeaf(Arc<AtomicUsize>);
                impl Drop for PauseLeaf {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let gate = Gate::default();
                let wait = gate.clone();
                let service = service(Probe::new(), move |_| {
                    let wait = wait.clone();
                    let guard = PauseLeaf(Arc::clone(&retired));
                    async move {
                        let _guard = guard;
                        wait.wait().await;
                        Ok(Response::new(TestBody::empty()))
                    }
                });
                let mut running = start(
                    native.worker.clone(),
                    service,
                    options(CodecProtocol::Http2, 1),
                    None,
                )
                .await;
                let mut requests = JoinSet::new();
                requests.spawn(
                    running
                        .peer
                        .send(request("/pause-failure", TestBody::empty())),
                );
                gate.entered().await;
                let _release = native.pause().await;
                panic_any(marker);
            })
        })
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        error.downcast_ref::<Arc<PanicMarker>>().unwrap(),
        &expected
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn server_cancel_bypasses_saturated_admission() {
    let mut native = ManualWorkerHarness::new();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let owners = FixtureOwners::new();
    runtime.block_on(async {
        owners
            .run_case(async {
                let gate = Gate::default();
                let service_gate = gate.clone();
                let probe = Probe::new();
                let service = service(probe.clone(), move |_| {
                    let gate = service_gate.clone();
                    async move {
                        gate.wait().await;
                        Ok(Response::new(TestBody::empty()))
                    }
                });
                let mut running = start(
                    native.worker.clone(),
                    service,
                    options(CodecProtocol::Http2, 1),
                    None,
                )
                .await;
                let mut requests = JoinSet::new();
                requests.spawn(running.peer.send(request("/pending", TestBody::empty())));
                gate.entered().await;
                let resume = native.pause().await;
                for _ in 0..4 {
                    native.worker.submit(|_| {}).unwrap();
                }
                assert_eq!(native.worker.submit(|_| {}), Err(SubmitError::Overloaded));
                running.connection.cancel();
                let _ = running.connection.wait_app_leaves_drained().await;
                assert_eq!(probe.count("service future drop"), 1);
                probe.assert_app("service future drop");
                assert_eq!(running.parent.slots.available_permits(), 0);
                assert_eq!(running.io.count("io drop"), 0);
                resume.send(()).unwrap();
                let result = running.finish().await;
                assert!(result.is_err());
                requests.join_next().await.unwrap().unwrap().unwrap_err();
                assert!(requests.is_empty());
                native.stop(false).await;
            })
            .await;
        owners.finish().await;
        if native.exited.is_some() {
            native.stop(true).await;
        }
    });
    owners.failures().capture(|| native.join());
    drop(runtime);
    owners.report();
}

#[test]
fn server_abandoned_startup_and_forced_shutdown_join() {
    run_test(|worker, _| async move {
        let probe = Probe::new();
        let io_probe = Probe::new();
        let (established, stream) = sockets(io_probe.clone(), None).await;
        let mut parent = Parent::new(1);
        let permit = parent.reserve();
        let (startup, run) = prepare_connection(
            worker.clone(),
            established,
            service(probe.clone(), |_| async {
                Ok(Response::new(TestBody::empty()))
            }),
            options(CodecProtocol::Http11, 1),
        )
        .unwrap();
        parent.spawn(run, permit);
        drop(startup);
        assert!(parent.reap().await.is_err());
        assert_eq!(probe.count("service call"), 0);
        assert_eq!(io_probe.count("io drop"), 1);
        io_probe.assert_app("io drop");
        drop(stream);
        let gate = Gate::default();
        let body_gate = gate.clone();
        let probe = Probe::new();
        let body_probe = probe.clone();
        let service = service(probe.clone(), move |_| {
            let body = TestBody::data(b"never-released")
                .gated(body_gate.clone())
                .probed(body_probe.clone());
            async move { Ok(Response::new(body)) }
        });
        let mut running = start(worker, service, options(CodecProtocol::Http2, 1), None).await;
        let response = running
            .peer
            .send(request("/force", TestBody::empty()))
            .await
            .unwrap();
        gate.entered().await;
        running.connection.cancel();
        let _ = running.finish().await;
        drop(response);
        assert_eq!(probe.count("body drop"), 1);
        probe.assert_app("body drop");
    });
}

#[test]
fn server_native_driver_drop_keeps_application_joins_rooted() {
    for phase in ["readiness", "service", "body"] {
        let mut native = ManualWorkerHarness::new();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let owners = FixtureOwners::new();
        runtime.block_on(async {
            owners
                .run_case(async {
                    let gate = Gate::default();
                    let wait = gate.clone();
                    let probe = Probe::new();
                    let body_probe = probe.clone();
                    let mut service = service(probe.clone(), move |_| {
                        let gate = wait.clone();
                        let body_probe = body_probe.clone();
                        async move {
                            if phase == "service" {
                                gate.wait().await;
                            }
                            let body = TestBody::data(b"pending-body").probed(body_probe);
                            Ok(Response::new(if phase == "body" {
                                body.gated(gate)
                            } else {
                                body
                            }))
                        }
                    });
                    if phase == "readiness" {
                        service.readiness = Some(gate.clone());
                    }
                    let mut running = start(
                        native.worker.clone(),
                        service,
                        options(CodecProtocol::Http2, 1),
                        None,
                    )
                    .await;
                    let mut requests = JoinSet::new();
                    requests.spawn(async move {
                        match running
                            .peer
                            .send(request("/native-loss", TestBody::empty()))
                            .await
                        {
                            Ok(response) => response.into_body().collect().await.map(|_| ()),
                            Err(error) => Err(error),
                        }
                    });
                    gate.entered().await;
                    native.stop(true).await;
                    let result = running.parent.reap().await.unwrap_err();
                    assert!(!result.cleanup_confirmed());
                    assert!(running.parent.slots.is_closed());
                    assert_eq!(probe.count("service drop"), 1);
                    probe.assert_app("service drop");
                    if phase != "readiness" {
                        assert_eq!(probe.count("service future drop"), 1);
                        probe.assert_app("service future drop");
                    }
                    if phase == "body" {
                        assert_eq!(probe.count("body drop"), 1);
                        probe.assert_app("body drop");
                    }
                    running.peers.finish().await;
                    assert!(requests.join_next().await.unwrap().unwrap().is_err());
                    assert!(requests.is_empty());
                    assert_eq!(running.io.count("io drop"), 1);
                })
                .await;
            owners.finish().await;
            if native.exited.is_some() {
                native.stop(true).await;
            }
        });
        owners
            .failures()
            .capture(|| native.join_failed_session(phase));
        drop(runtime);
        owners.report();
    }
}

#[test]
fn server_public_handle_abandonment_preserves_living_parent() {
    run_test(|worker, _| async move {
        let gate = Gate::default();
        let pending = gate.clone();
        let (sent, mut received) = mpsc::channel(1);
        let probe = Probe::new();
        let service = service(probe.clone(), move |request| {
            sent.try_send(
                request
                    .extensions()
                    .get::<ServerExchange>()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
            let pending = pending.clone();
            async move {
                pending.wait().await;
                Ok(Response::new(TestBody::empty()))
            }
        });
        let mut running = start(worker, service, options(CodecProtocol::Http2, 1), None).await;
        let mut requests = JoinSet::new();
        requests.spawn(running.peer.send(request("/abandon", TestBody::empty())));
        gate.entered().await;
        let exchange = received.recv().await.unwrap();
        drop(exchange.wait_completed());
        drop(exchange);
        let leaves = running.connection.wait_app_leaves_drained();
        drop(running.connection);
        let _ = leaves.await;
        assert_eq!(probe.count("service future drop"), 1);
        assert_eq!(running.parent.slots.available_permits(), 0);
        drop(running.peer);
        running.peers.finish().await;
        assert!(running.parent.reap().await.is_err());
        assert!(requests.join_next().await.unwrap().unwrap().is_err());
        assert!(requests.is_empty());
        probe.assert_app("service future drop");
    });
}

#[test]
fn server_startup_requires_bounded_parent_admission() {
    let mut native = ManualWorkerHarness::new();
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let owners = FixtureOwners::new();
    runtime.block_on(async {
        owners
            .run_case(async {
                let mut parent = Parent::new(1);
                let reserved = parent.reserve();
                assert!(Arc::clone(&parent.slots).try_acquire_owned().is_err());
                assert_eq!(native.worker.queued_commands(), 0);
                let probe = Probe::new();
                let (established, stream) = sockets(probe.clone(), None).await;
                let (startup, run) = prepare_connection(
                    native.worker.clone(),
                    established,
                    service(probe.clone(), |_| async {
                        Ok(Response::new(TestBody::empty()))
                    }),
                    options(CodecProtocol::Http11, 1),
                )
                .unwrap();
                assert_eq!(native.worker.queued_commands(), 0);
                let mut startup = Box::pin(startup.start());
                assert_pending(startup.as_mut()).await;
                assert_eq!(native.worker.queued_commands(), 0);
                assert_eq!(probe.count("io read"), 0);
                assert_eq!(probe.count("service ready"), 0);
                let resume = native.pause().await;
                parent.spawn(run, reserved);
                futures::future::poll_fn(|context| {
                    if native.worker.queued_commands() == 1 {
                        Poll::Ready(())
                    } else {
                        context.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
                drop(startup);
                assert_eq!(parent.slots.available_permits(), 0);
                assert_eq!(probe.count("io drop"), 0);
                resume.send(()).unwrap();
                let result = parent.reap().await.unwrap_err();
                assert!(result.cleanup_confirmed());
                assert_eq!(parent.slots.available_permits(), 1);
                assert_eq!(probe.count("io drop"), 1);
                assert_eq!(probe.count("service ready"), 0);
                probe.assert_app("io drop");
                drop(stream);
                let (established, stream) = sockets(probe.clone(), None).await;
                let (startup, run) = prepare_connection(
                    native.worker.clone(),
                    established,
                    service(probe.clone(), |_| async {
                        Ok(Response::new(TestBody::empty()))
                    }),
                    options(CodecProtocol::Http11, 1),
                )
                .unwrap();
                drop(startup);
                drop(run);
                assert_eq!(probe.count("io drop"), 2);
                assert_eq!(native.worker.queued_commands(), 0);
                drop(stream);
                native.stop(false).await;
            })
            .await;
        owners.finish().await;
        if native.exited.is_some() {
            native.stop(true).await;
        }
    });
    owners.failures().capture(|| native.join());
    drop(runtime);
    owners.report();
}

struct GatedRun {
    run: Pin<Box<ServerRun>>,
    result: Option<Outcome>,
    produced: Option<oneshot::Sender<()>>,
    release: Gate,
}

impl Future for GatedRun {
    type Output = Outcome;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Outcome> {
        if self.result.is_none() {
            match self.run.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.result = Some(result);
                    let _ = self.produced.take().unwrap().send(());
                }
            }
        }
        if self.release.poll(context).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(self.result.take().unwrap())
    }
}

#[test]
fn server_app_leaf_drain_is_not_server_run_reaping() {
    for failed_leaf in [false, true] {
        run_test(|worker, _| async move {
            let shutdown = Gate::default();
            let reap_gate = Gate::default();
            let io = Probe::new();
            let probe = Probe::new();
            let (exchange, mut exchanges) = mpsc::channel(1);
            let service = service(probe.clone(), move |request| {
                exchange
                    .try_send(
                        request
                            .extensions()
                            .get::<ServerExchange>()
                            .unwrap()
                            .clone(),
                    )
                    .unwrap();
                async move {
                    if failed_leaf {
                        Err(TestError(Arc::new(())))
                    } else {
                        Ok(Response::new(TestBody::empty()))
                    }
                }
            });
            let (established, stream) = sockets(io.clone(), Some(shutdown.clone())).await;
            let mut parent = Parent::new(1);
            let permit = parent.reserve();
            let (startup, run) = prepare_connection(
                worker,
                established,
                service,
                options(CodecProtocol::Http2, 1),
            )
            .unwrap();
            let (produced, production) = oneshot::channel();
            parent.tasks.track_run(&run);
            let run = GatedRun {
                run: Box::pin(run),
                result: None,
                produced: Some(produced),
                release: reap_gate.clone(),
            };
            parent.tasks.spawn(async move { (run.await, permit) });
            let connection = startup.start().await.unwrap();
            let mut peers = PeerTasks::new();
            let peer_shutdown = Gate::default();
            let peer_io = Probe::new();
            let (mut peer, peer_finished) = PeerConnection::connect_gated(
                stream,
                CodecProtocol::Http2,
                &peers,
                peer_shutdown.clone(),
                peer_io.clone(),
            )
            .await;
            let response = peer.send(request("/leaf", TestBody::empty())).await;
            if failed_leaf {
                assert_native_error_response(
                    response.unwrap(),
                    b"application server exchange cancelled",
                )
                .await;
            } else {
                response.unwrap().into_body().collect().await.unwrap();
            }
            let exchange = exchanges.recv().await.unwrap();
            eprintln!("leaf fixture failed={failed_leaf}: waiting for exchange receipts");
            let exchange_result = exchange.wait_completed().await;
            assert_eq!(exchange_result.is_err(), failed_leaf);
            connection.begin_drain();
            eprintln!("leaf fixture failed={failed_leaf}: waiting for application leaves");
            let leaf = connection.wait_app_leaves_drained().await;
            assert_eq!(leaf.is_err(), failed_leaf);
            eprintln!("leaf fixture failed={failed_leaf}: waiting for transport shutdown entry");
            if tokio::time::timeout(TIMEOUT / 2, shutdown.entered())
                .await
                .is_err()
            {
                panic!(
                    "leaf fixture failed={failed_leaf}: transport shutdown entry timed out; \
                     state={:?}; io_shutdown={}; io_drop={}; service_drop={}; \
                     parent_tasks={}; available_permits={}",
                    crate::server::connection_diagnostics(&connection),
                    io.count("io shutdown"),
                    io.count("io drop"),
                    probe.count("service drop"),
                    parent.tasks.len(),
                    parent.slots.available_permits(),
                );
            }
            assert_eq!(probe.count("service drop"), 1);
            assert_eq!(io.count("io drop"), 0);
            assert_eq!(parent.slots.available_permits(), 0);
            let mut production = Box::pin(production);
            assert_pending(production.as_mut()).await;
            shutdown.release();
            eprintln!("leaf fixture failed={failed_leaf}: waiting for run result production");
            production.await.unwrap();
            assert_eq!(io.count("io drop"), 1);
            assert_eq!(parent.slots.available_permits(), 0);
            reap_gate.release();
            let result = parent.reap().await;
            assert_eq!(result.is_err(), failed_leaf);
            assert_eq!(parent.slots.available_permits(), 1);
            assert_eq!(peer_io.count("io drop"), 0);
            peer_shutdown.entered().await;
            peer_shutdown.release();
            peer_finished.await.unwrap().unwrap();
            drop(peer);
            peers.finish().await;
            assert_eq!(peer_io.count("io drop"), 1);
            peer_io.assert_app("io drop");
        });
    }
}

struct CloneFailureWake {
    target: Mutex<Option<Waker>>,
    marker: Arc<PanicMarker>,
    clone_calls: AtomicUsize,
    borrowed_wakes: AtomicUsize,
    run_polls: AtomicUsize,
    first_pending: AtomicUsize,
    wake_panics: bool,
    probe: Probe,
}

impl CloneFailureWake {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake_owned,
        Self::wake_borrowed,
        Self::drop_waker,
    );

    /// The raw handle owns the transferred Arc reference. Its vtable preserves
    /// that ownership on clone failure and consumes it exactly once on drop/wake.
    fn into_waker(owner: Arc<Self>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(owner).cast(), &Self::VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    /// # Safety
    /// `data` denotes a live Arc reference created by `into_waker` or this
    /// vtable. A throwing clone leaves that reference untouched; success adds one.
    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let state = unsafe { &*data.cast::<Self>() };
        if state.clone_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic_any(Arc::clone(&state.marker));
        }
        unsafe { Arc::increment_strong_count(data.cast::<Self>()) };
        RawWaker::new(data, &Self::VTABLE)
    }

    /// # Safety
    /// `data` transfers exactly one vtable-owned Arc reference. The local Arc
    /// releases it even if the forwarded notification throws.
    unsafe fn wake_owned(data: *const ()) {
        let state = unsafe { Arc::from_raw(data.cast::<Self>()) };
        state.forward();
    }

    /// # Safety
    /// The calling Waker retains its Arc reference for this entire borrowed
    /// callback; neither forwarding nor a panic consumes that reference.
    unsafe fn wake_borrowed(data: *const ()) {
        let state = unsafe { &*data.cast::<Self>() };
        state.borrowed_wakes.fetch_add(1, Ordering::SeqCst);
        state.forward();
    }

    /// # Safety
    /// `data` transfers exactly one still-live reference from this vtable.
    unsafe fn drop_waker(data: *const ()) {
        drop(unsafe { Arc::from_raw(data.cast::<Self>()) });
    }

    fn forward(&self) {
        let target = self
            .target
            .lock()
            .unwrap()
            .clone()
            .expect("armed Tokio waker");
        target.wake_by_ref();
        if self.wake_panics {
            self.probe.record("clone fallback wake panic");
            panic_any(ThrowingPayload(self.probe.clone()));
        }
    }
}

struct CloneFailureRun {
    run: Pin<Box<ServerRun>>,
    wake: Arc<CloneFailureWake>,
    result: Option<Outcome>,
    produced: Option<oneshot::Sender<()>>,
    release: Gate,
}

impl Future for CloneFailureRun {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Outcome> {
        if self.result.is_none() {
            let first = self.wake.run_polls.fetch_add(1, Ordering::SeqCst) == 0;
            let result = if first {
                *self.wake.target.lock().unwrap() = Some(context.waker().clone());
                let waker = CloneFailureWake::into_waker(Arc::clone(&self.wake));
                let result = self.run.as_mut().poll(&mut Context::from_waker(&waker));
                assert!(
                    result.is_pending(),
                    "clone failure returns before inner polling"
                );
                assert_eq!(self.wake.clone_calls.load(Ordering::SeqCst), 1);
                self.wake.first_pending.fetch_add(1, Ordering::SeqCst);
                result
            } else {
                self.run.as_mut().poll(context)
            };
            match result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.result = Some(result);
                    let _ = self.produced.take().unwrap().send(());
                }
            }
        }
        if self.release.poll(context).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(self.result.take().unwrap())
    }
}

#[test]
fn server_waker_clone_failure_reschedules_cleanup() {
    for wake_panics in [false, true] {
        run_manual(move |native| {
            Box::pin(async move {
                let marker = Arc::new(PanicMarker);
                let probe = Probe::new();
                let io = Probe::new();
                let (established, stream) = sockets(io.clone(), None).await;
                let resume = native.pause().await;
                assert_eq!(native.worker.queued_commands(), 0);
                let mut parent = Parent::new(1);
                let permit = parent.reserve();
                let (startup, run) = prepare_connection(
                    native.worker.clone(),
                    established,
                    service(probe.clone(), |_| async {
                        Ok(Response::new(TestBody::empty()))
                    }),
                    options(CodecProtocol::Http2, 1),
                )
                .unwrap();
                let wake = Arc::new(CloneFailureWake {
                    target: Mutex::new(None),
                    marker: Arc::clone(&marker),
                    clone_calls: AtomicUsize::new(0),
                    borrowed_wakes: AtomicUsize::new(0),
                    run_polls: AtomicUsize::new(0),
                    first_pending: AtomicUsize::new(0),
                    wake_panics,
                    probe: probe.clone(),
                });
                let release = Gate::default();
                let (produced, production) = oneshot::channel();
                parent.tasks.track_run(&run);
                let run = CloneFailureRun {
                    run: Box::pin(run),
                    wake: Arc::clone(&wake),
                    result: None,
                    produced: Some(produced),
                    release: release.clone(),
                };
                parent.tasks.spawn(async move { (run.await, permit) });
                tokio::time::timeout(TIMEOUT, production)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(wake.clone_calls.load(Ordering::SeqCst), 1);
                assert_eq!(wake.borrowed_wakes.load(Ordering::SeqCst), 1);
                assert_eq!(wake.first_pending.load(Ordering::SeqCst), 1);
                assert_eq!(wake.run_polls.load(Ordering::SeqCst), 2);
                assert_eq!(native.worker.queued_commands(), 0);
                assert_eq!(io.count("io read"), 0);
                assert_eq!(io.count("io shutdown"), 0);
                assert_eq!(io.count("io drop"), 1);
                io.assert_app("io drop");
                assert_eq!(probe.count("service ready"), 0);
                assert_eq!(probe.count("service call"), 0);
                assert_eq!(probe.count("service future drop"), 0);
                assert_eq!(probe.count("service drop"), 1);
                probe.assert_app("service drop");
                assert_eq!(
                    probe.count("clone fallback wake panic"),
                    usize::from(wake_panics)
                );
                assert_eq!(
                    probe.count("secondary payload drop"),
                    usize::from(wake_panics)
                );
                if wake_panics {
                    probe.assert_app("secondary payload drop");
                }
                assert_eq!(parent.slots.available_permits(), 0);
                assert_eq!(parent.tasks.len(), 1);
                {
                    let reaped = parent.tasks.join_next();
                    tokio::pin!(reaped);
                    assert_pending(reaped.as_mut()).await;
                }
                release.release();
                let error = tokio::time::timeout(TIMEOUT, parent.reap())
                    .await
                    .unwrap()
                    .unwrap_err();
                assert_panic(&error, &marker);
                assert!(matches!(error.cause(), ServerFailure::Panic(_)));
                assert!(parent.tasks.is_empty());
                assert_eq!(parent.slots.available_permits(), 1);
                assert!(parent.slots.is_closed());
                assert!(parent.slots.clone().try_acquire_owned().is_err());
                assert_eq!(native.worker.queued_commands(), 0);
                drop(startup);
                drop(stream);
                resume.send(()).unwrap();
            })
        });
    }
}
