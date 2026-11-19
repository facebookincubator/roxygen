/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomPinned;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use http::HeaderMap;
use http::HeaderValue;
use http::Request;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use http_body_util::BodyExt;
use roxygen_client::pool::PoolError;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower_service::Service;

use crate::client::Client;
use crate::client::ClientAppDrainError;
use crate::client::ClientBody;
use crate::client::ClientError;
use crate::client::ClientOptions;
use crate::client::ClientOwner;
use crate::client::ClientShutdownError;
use crate::client::connector::ConnectRequest;
use crate::client::connector::ConnectedIo;
use crate::client::owner::TestAppLeafSnapshot;
use crate::client::owner::TestLatch;
use crate::route::Origin;
use crate::route::ProtocolPolicy;
use crate::route::RouteSpec;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotOrder {
    Healthy,
    AppFirst,
    NativeFirst,
}

struct SnapshotControls {
    order: SnapshotOrder,
    native_return: Option<Arc<TestLatch>>,
    publication: Option<Arc<TestAppLeafSnapshot>>,
}

impl SnapshotControls {
    fn new(order: SnapshotOrder) -> Self {
        Self {
            order,
            native_return: (order == SnapshotOrder::AppFirst).then(TestLatch::new),
            publication: (order == SnapshotOrder::NativeFirst).then(TestAppLeafSnapshot::new),
        }
    }

    fn cancelled(&self) -> bool {
        self.order != SnapshotOrder::Healthy
    }

    fn release(&self) {
        if let Some(latch) = &self.native_return {
            latch.release();
        }
        if let Some(probe) = &self.publication {
            probe.release();
        }
    }
}

impl Drop for SnapshotControls {
    fn drop(&mut self) {
        self.release();
    }
}

fn is_exact_upload_cancel(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Pool(PoolError::SessionJoin { source })
            if source.code() == HttpErrorCode::CORO_CANCELLED
                && source.message() == b"request body production cancelled"
                && std::error::Error::source(source).is_none()
    )
}

#[derive(Default)]
struct Probe {
    method: &'static str,
    body_drops: AtomicUsize,
    body_polls: AtomicUsize,
    body_wrong_thread: AtomicBool,
    upload_eom: AtomicBool,
    io_drops: AtomicUsize,
    connects: AtomicUsize,
    peer_prefix: AtomicBool,
    peer_early_write: AtomicBool,
    peer_upload_eom: AtomicBool,
}

struct Upload {
    stage: Cell<u8>,
    release: RefCell<Pin<Box<oneshot::Receiver<()>>>>,
    owner: ThreadId,
    probe: Arc<Probe>,
    _pinned: PhantomPinned,
}

impl Body for Upload {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let body = self.as_ref().get_ref();
        body.probe.body_polls.fetch_add(1, Ordering::SeqCst);
        if std::thread::current().id() != body.owner {
            body.probe.body_wrong_thread.store(true, Ordering::SeqCst);
        }
        match body.stage.get() {
            0 => {
                body.stage.set(1);
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"upload-")))))
            }
            1 => {
                let Poll::Ready(released) = body.release.borrow_mut().as_mut().poll(context) else {
                    return Poll::Pending;
                };
                body.stage.set(2);
                Poll::Ready(Some(
                    released
                        .map(|()| Frame::data(Bytes::from_static(b"done")))
                        .map_err(io::Error::other),
                ))
            }
            2 => {
                body.stage.set(3);
                body.probe.upload_eom.store(true, Ordering::SeqCst);
                let mut trailers = HeaderMap::new();
                trailers.insert("x-upload", HeaderValue::from_static("sent"));
                Poll::Ready(Some(Ok(Frame::trailers(trailers))))
            }
            _ => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.stage.get() == 3
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::new()
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if std::thread::current().id() != self.owner {
            self.probe.body_wrong_thread.store(true, Ordering::SeqCst);
        }
        self.probe.body_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct TrackedIo {
    stream: TcpStream,
    probe: Arc<Probe>,
}

impl AsyncRead for TrackedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for TrackedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

impl Drop for TrackedIo {
    fn drop(&mut self) {
        self.probe.io_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct Connector {
    address: SocketAddr,
    probe: Arc<Probe>,
}

impl Service<ConnectRequest> for Connector {
    type Response = ConnectedIo<TrackedIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ConnectRequest) -> Self::Future {
        self.probe.connects.fetch_add(1, Ordering::SeqCst);
        let address = self.address;
        let probe = Arc::clone(&self.probe);
        Box::pin(async move {
            let stream = TcpStream::connect(address).await?;
            let local = stream.local_addr()?;
            let peer = stream.peer_addr()?;
            let local =
                TransportEndpoint::Socket(SocketAddress::new(local.ip().to_string(), local.port()));
            let peer =
                TransportEndpoint::Socket(SocketAddress::new(peer.ip().to_string(), peer.port()));
            Ok(ConnectedIo::new(
                request.route().clone(),
                EstablishedIo::new(TrackedIo { stream, probe }, local, peer, None),
                CodecProtocol::Http11,
                TransportSecurity::Plaintext,
            ))
        })
    }
}

fn require(condition: bool, reason: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(reason.to_owned()))
    }
}

async fn line(reader: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut line = String::new();
    let count = reader.read_line(&mut line).await?;
    require(
        count != 0 && count <= 4096 && line.ends_with("\r\n"),
        "bounded CRLF line required",
    )?;
    Ok(line)
}

async fn chunk(reader: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let size = usize::from_str_radix(line(reader).await?.trim(), 16).map_err(io::Error::other)?;
    require(size <= 64, "request chunk exceeds fixture bound")?;
    let mut data = vec![0; size];
    if size != 0 {
        reader.read_exact(&mut data).await?;
        let mut ending = [0; 2];
        reader.read_exact(&mut ending).await?;
        require(ending == *b"\r\n", "request chunk delimiter")?;
    }
    Ok(data)
}

#[derive(Debug, Eq, PartialEq)]
enum PeerOutcome {
    Complete,
    CancelledAfterEarlyWrite,
    InterruptedForCleanup,
}

async fn peer_exchange(
    listener: TcpListener,
    probe: Arc<Probe>,
    cancel: bool,
    retired: oneshot::Receiver<()>,
) -> io::Result<PeerOutcome> {
    let address = listener.local_addr()?;
    let (stream, _) = listener.accept().await?;
    let mut reader = BufReader::new(stream);
    require(
        line(&mut reader).await? == format!("{} /duplex HTTP/1.1\r\n", probe.method),
        "actual H1 request line",
    )?;
    let mut headers = String::new();
    loop {
        let next = line(&mut reader).await?;
        if next == "\r\n" {
            break;
        }
        headers.push_str(&next.to_ascii_lowercase());
        require(headers.len() <= 8192, "request head bound")?;
    }
    require(
        headers.contains(&format!("host: {address}\r\n")),
        "actual route authority",
    )?;
    require(
        headers.contains("transfer-encoding: chunked\r\n"),
        "real chunked upload",
    )?;
    require(
        headers.contains("te: trailers\r\n"),
        "response trailer negotiation",
    )?;
    require(
        chunk(&mut reader).await? == b"upload-",
        "actual request prefix",
    )?;
    probe.peer_prefix.store(true, Ordering::SeqCst);
    reader.get_mut().write_all(
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-response\r\n\r\n6\r\nearly-\r\n",
    ).await?;
    probe.peer_early_write.store(true, Ordering::SeqCst);
    if cancel {
        retired.await.map_err(io::Error::other)?;
        return Ok(PeerOutcome::CancelledAfterEarlyWrite);
    }
    require(
        chunk(&mut reader).await? == b"done",
        "remaining upload bytes",
    )?;
    require(
        chunk(&mut reader).await?.is_empty(),
        "request terminal chunk",
    )?;
    require(
        line(&mut reader).await? == "x-upload: sent\r\n",
        "request trailers",
    )?;
    require(
        line(&mut reader).await? == "\r\n",
        "request trailer terminator",
    )?;
    probe.peer_upload_eom.store(true, Ordering::SeqCst);
    reader
        .get_mut()
        .write_all(b"4\r\nlate\r\n0\r\nx-response: verified\r\n\r\n")
        .await?;
    reader.get_mut().shutdown().await?;
    Ok(PeerOutcome::Complete)
}

async fn stopped(mut stop: watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

struct Parents {
    runs: JoinSet<Result<(), ClientShutdownError>>,
    peers: JoinSet<io::Result<PeerOutcome>>,
    owners: Vec<ClientOwner>,
}

async fn receive_bytes(body: &mut Pin<Box<ClientBody>>, expected: &[u8]) -> io::Result<()> {
    let mut bytes = Vec::new();
    while bytes.len() < expected.len() {
        let frame = body
            .frame()
            .await
            .ok_or_else(|| io::Error::other("response ended before expected data"))?
            .map_err(io::Error::other)?;
        let data = frame
            .data_ref()
            .ok_or_else(|| io::Error::other("trailers before expected data"))?;
        bytes.extend_from_slice(data);
        require(
            bytes.len() <= expected.len(),
            "unexpected additional response data",
        )?;
    }
    require(bytes == expected, "ordered response data changed")
}

async fn exercise(
    parents: &mut Parents,
    runtime: &RuntimeHandle,
    probe: &Arc<Probe>,
    controls: &SnapshotControls,
    release_upload: &mut Option<oneshot::Sender<()>>,
    upload_gate: oneshot::Receiver<()>,
    stop_peer: watch::Receiver<bool>,
) -> io::Result<()> {
    let cancel = controls.cancelled();
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let (retire_peer, peer_retired) = oneshot::channel();
    let peer_probe = Arc::clone(probe);
    parents.peers.spawn(async move {
        tokio::select! {
            result = peer_exchange(listener, peer_probe, cancel, peer_retired) => result,
            () = stopped(stop_peer) => Ok(PeerOutcome::InterruptedForCleanup),
        }
    });
    let (startup, run) = Client::prepare(
        runtime
            .worker(0)
            .ok_or_else(|| io::Error::other("missing native worker"))?,
        Connector {
            address,
            probe: Arc::clone(probe),
        },
        ClientOptions {
            maximum_exchanges: 1,
            maximum_connector_tasks: 1,
            maximum_physical_connections: 1,
            maximum_routes: 1,
            test_shutdown_return: controls.native_return.clone(),
            test_app_leaf_snapshot: controls.publication.clone(),
            ..ClientOptions::default()
        },
    )
    .map_err(io::Error::other)?;
    parents.runs.spawn(run);
    let (client, owner) = startup.start().await.map_err(io::Error::other)?;
    parents.owners.push(owner);
    let origin = format!("http://{address}")
        .parse::<Origin>()
        .map_err(io::Error::other)?;
    let route = client
        .route(RouteSpec::direct(origin, ProtocolPolicy::Http1Only))
        .map_err(io::Error::other)?;
    let mut service = client.service::<Upload>(&route).map_err(io::Error::other)?;
    poll_fn(|context| service.poll_ready(context))
        .await
        .map_err(io::Error::other)?;
    let response = service.call(
        Request::builder()
            .method(probe.method)
            .uri("/duplex")
            .header("te", "trailers")
            .header("trailer", "x-upload")
            .body(Upload {
                stage: Cell::new(0),
                release: RefCell::new(Box::pin(upload_gate)),
                owner: std::thread::current().id(),
                probe: Arc::clone(probe),
                _pinned: PhantomPinned,
            })
            .map_err(io::Error::other)?,
    );
    let exchange = response.exchange();
    let response = response.await.map_err(io::Error::other)?;
    require(
        response.status() == http::StatusCode::OK,
        "early response status",
    )?;
    require(
        response.version() == http::Version::HTTP_11,
        "actual H1 response version",
    )?;
    let mut body = Box::pin(response.into_body());
    receive_bytes(&mut body, b"early-").await?;
    require(
        probe.peer_prefix.load(Ordering::SeqCst) && probe.peer_early_write.load(Ordering::SeqCst),
        "actual peer consumed prefix and wrote early response",
    )?;
    require(
        !probe.upload_eom.load(Ordering::SeqCst) && probe.body_drops.load(Ordering::SeqCst) == 0,
        "upload remains admitted and pending",
    )?;
    let mut completion = Box::pin(exchange.wait());
    require(
        poll_fn(|context| Poll::Ready(completion.as_mut().poll(context).is_pending())).await,
        "early response cannot certify pending upload",
    )?;
    if cancel {
        require(
            !body.is_end_stream(),
            "cancel requires nonterminal response",
        )?;
        exchange.cancel();
        drop(body);
        let error = completion
            .await
            .expect_err("cancelled pending upload must fail its exchange");
        require(
            matches!(error.cause(), ClientError::Cancelled) && !error.cleanup_unconfirmed(),
            "exact cancellation with confirmed composed cleanup",
        )?;
        require(
            probe.body_drops.load(Ordering::SeqCst) == 1,
            "producer destruction precedes exchange confirmation",
        )?;
        require(
            !probe.upload_eom.load(Ordering::SeqCst),
            "cancellation must not manufacture upload EOM",
        )?;
        retire_peer
            .send(())
            .map_err(|()| io::Error::other("peer exited before cancellation retirement"))?;
    } else {
        release_upload
            .take()
            .ok_or_else(|| io::Error::other("missing upload release"))?
            .send(())
            .map_err(|()| io::Error::other("upload disappeared before release"))?;
        receive_bytes(&mut body, b"late").await?;
        let trailers = body
            .frame()
            .await
            .ok_or_else(|| io::Error::other("missing response trailers"))?
            .map_err(io::Error::other)?;
        require(
            trailers.trailers_ref().is_some_and(|headers| {
                headers.len() == 1
                    && headers.get("x-response") == Some(&HeaderValue::from_static("verified"))
            }),
            "exact ordered response trailers",
        )?;
        require(
            body.frame().await.is_none(),
            "response must end cleanly after trailers",
        )?;
        completion.await.map_err(io::Error::other)?;
        require(
            probe.peer_upload_eom.load(Ordering::SeqCst),
            "peer consumed complete upload and trailers",
        )?;
    }
    Ok(())
}

fn network_case(order: SnapshotOrder) {
    network_case_with_method(order, "POST");
}

fn network_case_with_method(order: SnapshotOrder, method: &'static str) {
    let mut native = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 16,
    })
    .unwrap();
    let runtime = native.handle();
    let app = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let probe = Arc::new(Probe {
        method,
        ..Probe::default()
    });
    let controls = SnapshotControls::new(order);
    let cancel = controls.cancelled();
    let (
        operation,
        phase,
        runs,
        peers,
        leaves,
        after_run_leaves,
        selected_native,
        cleanup_timed_out,
        quiescence,
    ) = app.block_on(async {
        let mut parents = Parents {
            runs: JoinSet::new(),
            peers: JoinSet::new(),
            owners: Vec::new(),
        };
        let (release, upload_gate) = oneshot::channel();
        let mut release = Some(release);
        let (stop_peer, peer_stopped) = watch::channel(false);
        let operation = tokio::time::timeout(
            DEADLINE,
            AssertUnwindSafe(exercise(
                &mut parents,
                &runtime,
                &probe,
                &controls,
                &mut release,
                upload_gate,
                peer_stopped,
            ))
            .catch_unwind(),
        )
        .await;
        if !matches!(&operation, Ok(Ok(Ok(())))) {
            controls.release();
            stop_peer.send_replace(true);
        }
        for owner in &parents.owners {
            owner.shutdown();
        }
        let mut runs = Vec::new();
        let mut peers = Vec::new();
        let mut leaves = Vec::new();
        let mut after_run_leaves = Vec::new();
        let mut selected_native = None;
        let phase = tokio::time::timeout(
            DEADLINE,
            AssertUnwindSafe(async {
                if !matches!(&operation, Ok(Ok(Ok(())))) {
                    return Ok(());
                }
                require(parents.owners.len() == 1, "one actual client generation")?;
                let owner = &parents.owners[0];
                match order {
                    SnapshotOrder::Healthy => {}
                    SnapshotOrder::AppFirst => {
                        controls
                            .native_return
                            .as_ref()
                            .expect("app-first native return gate")
                            .wait_entered()
                            .await;
                        leaves.push(owner.wait_app_leaves_drained().await);
                    }
                    SnapshotOrder::NativeFirst => {
                        let publication = controls
                            .publication
                            .as_ref()
                            .expect("native-first publication gate");
                        publication.wait_eligible().await;
                        selected_native = Some(publication.native_error_selected().await);
                        let mut leaf = Box::pin(owner.wait_app_leaves_drained());
                        if let Poll::Ready(result) =
                            poll_fn(|context| Poll::Ready(leaf.as_mut().poll(context))).await
                        {
                            leaves.push(result);
                            return Err(io::Error::other(
                                "application snapshot published before its retained release",
                            ));
                        }
                    }
                }
                if cancel {
                    match poll_fn(|context| Poll::Ready(parents.runs.poll_join_next(context))).await
                    {
                        Poll::Pending => {}
                        Poll::Ready(result) => {
                            if let Some(result) = result {
                                runs.push(result);
                            }
                            return Err(io::Error::other(
                                "actual ClientRun completed before the ordering gate released",
                            ));
                        }
                    }
                }
                controls.release();
                if leaves.is_empty() {
                    leaves.push(owner.wait_app_leaves_drained().await);
                }
                Ok(())
            })
            .catch_unwind(),
        )
        .await;
        controls.release();
        if !matches!(&phase, Ok(Ok(Ok(())))) {
            stop_peer.send_replace(true);
        }
        let cleanup = async {
            if leaves.is_empty() {
                for owner in &parents.owners {
                    leaves.push(owner.wait_app_leaves_drained().await);
                }
            }
            while let Some(result) = parents.runs.join_next().await {
                runs.push(result);
            }
            for owner in &parents.owners {
                after_run_leaves.push(owner.wait_app_leaves_drained().await);
            }
            while let Some(result) = parents.peers.join_next().await {
                peers.push(result);
            }
        };
        let cleanup_timed_out = tokio::time::timeout(DEADLINE, cleanup).await.is_err();
        if cleanup_timed_out {
            stop_peer.send_replace(true);
        }
        let quiescence = runtime
            .quiesce(if cleanup_timed_out {
                Duration::ZERO
            } else {
                DEADLINE
            })
            .await;
        while let Some(result) = parents.runs.join_next().await {
            runs.push(result);
        }
        while let Some(result) = parents.peers.join_next().await {
            peers.push(result);
        }
        drop(release);
        (
            operation,
            phase,
            runs,
            peers,
            leaves,
            after_run_leaves,
            selected_native,
            cleanup_timed_out,
            quiescence,
        )
    });
    let physical = native.join();
    drop(app);
    assert!(!cleanup_timed_out, "cleanup exceeded its bounded grace");
    assert_eq!(quiescence, Ok(()), "native quiescence");
    let physical = physical.expect("actual physical worker join");
    assert_eq!(physical.workers_joined, 1);
    assert_eq!(physical.workers_forced, 0);
    let operation = operation.expect("bounded actual-wire operation");
    let operation = match operation {
        Ok(operation) => operation,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    if let Err(error) = operation {
        panic!(
            "actual-wire H1 duplex contract: {error:?}; order={order:?}; phase={phase:?}; runs={runs:?}; peers={peers:?}; leaves={leaves:?}"
        );
    }
    let phase = phase.expect("bounded causal snapshot observation");
    let phase = match phase {
        Ok(phase) => phase,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    assert!(
        phase.is_ok(),
        "snapshot order={order:?}; phase={phase:?}; runs={runs:?}; leaves={leaves:?}; after_run_leaves={after_run_leaves:?}; selected_native={selected_native:?}"
    );
    assert_eq!(peers.len(), 1);
    let peer = peers
        .into_iter()
        .next()
        .unwrap()
        .expect("actual peer parent join")
        .expect("actual peer I/O result");
    assert_eq!(
        peer,
        if cancel {
            PeerOutcome::CancelledAfterEarlyWrite
        } else {
            PeerOutcome::Complete
        }
    );
    assert_eq!(runs.len(), 1);
    let run = runs
        .into_iter()
        .next()
        .unwrap()
        .expect("actual ClientRun parent join");
    if cancel {
        let ClientShutdownError(error) =
            run.expect_err("cancelled H1 connection retains its pool failure");
        assert!(
            is_exact_upload_cancel(&error),
            "exact joined H1 upload cancellation cause: {error:?}; order={order:?}; application exchange already confirmed Cancelled; peer retired only after that receipt; leaves={leaves:?}; after_run_leaves={after_run_leaves:?}; selected_native={selected_native:?}; body_drops={}; io_drops={}; upload_eom={}",
            probe.body_drops.load(Ordering::SeqCst),
            probe.io_drops.load(Ordering::SeqCst),
            probe.upload_eom.load(Ordering::SeqCst),
        );
    } else {
        run.expect("healthy duplex client run");
    }
    assert_eq!(leaves.len(), 1);
    assert_eq!(after_run_leaves.len(), 1);
    match order {
        SnapshotOrder::Healthy | SnapshotOrder::AppFirst => {
            assert!(selected_native.is_none());
            assert!(leaves[0].is_ok(), "initial {order:?} snapshot: {leaves:?}");
            assert!(
                after_run_leaves[0].is_ok(),
                "sticky {order:?} snapshot after actual Run join: {after_run_leaves:?}"
            );
        }
        SnapshotOrder::NativeFirst => {
            let selected = selected_native.expect("actual owner native-result selection ACK");
            assert!(
                is_exact_upload_cancel(&selected),
                "actual selected cause: {selected:?}"
            );
            for (boundary, result) in [
                ("initial", &leaves[0]),
                ("after actual Run join", &after_run_leaves[0]),
            ] {
                let ClientAppDrainError(error) = result
                    .as_ref()
                    .expect_err("native-first snapshot must retain its cause");
                assert!(
                    is_exact_upload_cancel(error),
                    "{boundary} native-first snapshot: {error:?}; actual selected cause: {selected:?}"
                );
            }
        }
    }
    assert_eq!(probe.connects.load(Ordering::SeqCst), 1);
    assert_eq!(probe.body_drops.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_drops.load(Ordering::SeqCst), 1);
    assert!(probe.body_polls.load(Ordering::SeqCst) >= 2);
    assert!(!probe.body_wrong_thread.load(Ordering::SeqCst));
    assert_eq!(probe.upload_eom.load(Ordering::SeqCst), !cancel);
}

#[test]
fn h1_early_head_and_data_do_not_wait_for_upload_eom() {
    network_case(SnapshotOrder::Healthy);
}

#[test]
fn h1_non_post_streaming_requests_frame_data_and_trailers() {
    for method in ["PUT", "PATCH", "GET"] {
        network_case_with_method(SnapshotOrder::Healthy, method);
    }
}

#[test]
fn h1_early_response_cancellation_joins_pending_upload() {
    network_case(SnapshotOrder::AppFirst);
    network_case(SnapshotOrder::NativeFirst);
}

#[test]
fn h1_cancelled_app_leaves_before_native_failure_is_sticky() {
    network_case(SnapshotOrder::AppFirst);
}

#[test]
fn h1_cancelled_app_leaves_after_native_failure_retains_cause() {
    network_case(SnapshotOrder::NativeFirst);
}

#[test]
fn h1_cancelled_app_leaf_rejects_unrelated_failure() {
    let wrong_failures = [
        ClientError::Cancelled,
        ClientError::Closed,
        ClientError::CleanupUnconfirmed,
        ClientError::Panicked,
        ClientError::Pool(PoolError::CleanupPanicked),
        ClientError::Native(HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"request body production cancelled",
        )),
        ClientError::Pool(PoolError::SessionJoin {
            source: HttpError::with_message(
                HttpErrorCode::TRANSPORT_READ_ERROR,
                b"byte-stream was reset",
            ),
        }),
        ClientError::Pool(PoolError::SessionJoin {
            source: HttpError::with_message(
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"request body production cancelled",
            ),
        }),
        ClientError::Pool(PoolError::SessionJoin {
            source: HttpError::with_message(HttpErrorCode::CORO_CANCELLED, b"different failure"),
        }),
        ClientError::Pool(PoolError::SessionJoin {
            source: HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"request body production cancelled",
            )
            .with_cause(Arc::new(io::Error::other("unexpected nested cause"))),
        }),
    ];
    for error in wrong_failures {
        assert!(
            !is_exact_upload_cancel(&error),
            "unrelated failure accepted as the causal upload cancellation: {error:?}"
        );
    }
}
