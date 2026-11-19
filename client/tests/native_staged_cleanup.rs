/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt::Display;
use std::fs;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
#[cfg(fbcode_build)]
use std::os::linux::process::ChildExt as LinuxChildExt;
#[cfg(fbcode_build)]
use std::os::linux::process::CommandExt as LinuxCommandExt;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt as UnixCommandExt;
use std::os::unix::process::ExitStatusExt;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::panic::resume_unwind;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Child;
use std::process::Command;
use std::process::ExitStatus;
use std::process::Stdio;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc as thread_channel;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use futures::FutureExt;
use h2::Reason;
use h2::RecvStream;
use h2::SendStream;
use http::HeaderMap;
use http::HeaderValue;
use http::Response;
use roxygen_codec::CodecSetting;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::HttpMethod;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_http2::settings::HttpSettings;
use roxygen_http2::settings::SettingsId;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::context::RuntimeContext;
use roxygen_runtime::managed::QuiesceFuture;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeError;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::managed::ShutdownReport;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::byte_stream::TransportRetirementError;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use roxygen_transport::established::adopt_local_with_completion;
use roxygen_transport::tls::TlsClientOptions;
use rustls::ServerConfig;
use rustls::pki_types::PrivatePkcs8KeyDer;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::Interest;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::Timeout;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_rustls::TlsAcceptor;

use crate::HttpClient;
use crate::SourceRequest;
use crate::connector::ClientAttachOptions;
use crate::connector::ClientSession;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::HttpConnector;
use crate::connector::IdentityValidation;
use crate::connector::SessionParams;
use crate::connector::uniplex_config_for_session_params;
use crate::native_readiness_tests::CheckedNativeOutcome;
use crate::native_readiness_tests::NativeReleaseError;
use crate::native_readiness_tests::publish_checked_native;
use crate::native_readiness_tests::release_checked_peer_io;

const CHUNK: usize = 16 * 1024;
const UPLOAD: usize = 2 * CHUNK;
const DOWNLOAD: usize = 16 * 1024 * 1024;
const STREAM_WINDOW: u32 = 32 * 1024;
const CONNECTION_WINDOW: u32 = 65_535;
const STAGE_BOUND: Duration = Duration::from_secs(15);
const ROUTE_BOUND: Duration = Duration::from_secs(120);
const QUIESCE_BOUND: Duration = Duration::from_secs(10);
const WATCHDOG_BOUND: Duration = Duration::from_secs(900);
const PROC_BOUND: u64 = 16 * 1024;
static SERIAL: Mutex<()> = Mutex::new(());

fn failure(error: impl Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn require(condition: bool, message: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(failure(message))
    }
}

fn retain(errors: &mut Vec<String>, stage: &str, result: io::Result<()>) {
    if let Err(error) = result {
        errors.push(format!("{stage}: {error}"));
    }
}

fn finish(errors: Vec<String>) -> io::Result<()> {
    require(errors.is_empty(), "fixture failure").map_err(|_| failure(errors.join("; ")))
}

fn source_error(error: impl Display) -> HttpError {
    HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Route {
    h2: bool,
    tls: bool,
}

const ROUTES: [Route; 4] = [
    Route {
        h2: false,
        tls: false,
    },
    Route {
        h2: true,
        tls: false,
    },
    Route {
        h2: false,
        tls: true,
    },
    Route {
        h2: true,
        tls: true,
    },
];

impl Route {
    fn protocol(self) -> CodecProtocol {
        if self.h2 {
            CodecProtocol::Http2
        } else {
            CodecProtocol::Http11
        }
    }

    fn alpn(self) -> &'static [u8] {
        if self.h2 { b"h2" } else { b"http/1.1" }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    UploadCancel,
    FutureDrop,
    BodyDrop,
    Release,
}

impl Stage {
    fn authority(self) -> &'static str {
        match self {
            Self::UploadCancel => "untrusted-upload.invalid",
            Self::FutureDrop => "untrusted-future.invalid",
            Self::BodyDrop => "untrusted-body.invalid",
            Self::Release => "untrusted-release.invalid",
        }
    }
}

const STAGES: [Stage; 4] = [
    Stage::UploadCancel,
    Stage::FutureDrop,
    Stage::BodyDrop,
    Stage::Release,
];

#[derive(Clone)]
struct RouteFixture {
    route: Route,
    certificate: Option<Vec<u8>>,
    tls: Option<Arc<ServerConfig>>,
}

impl RouteFixture {
    fn new(route: Route) -> io::Result<Self> {
        if !route.tls {
            return Ok(Self {
                route,
                certificate: None,
                tls: None,
            });
        }
        let generated =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).map_err(failure)?;
        let certificate = generated.cert.der().to_vec();
        let key = PrivatePkcs8KeyDer::from(generated.signing_key.serialized_der().to_vec());
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(failure)?
                .with_no_client_auth()
                .with_single_cert(vec![generated.cert.der().clone()], key.into())
                .map_err(failure)?;
        config.alpn_protocols = vec![route.alpn().to_vec()];
        Ok(Self {
            route,
            certificate: Some(certificate),
            tls: Some(Arc::new(config)),
        })
    }

    fn connection(&self) -> ConnectionParams {
        let transport = match &self.certificate {
            None => ClientTransportOptions::Plaintext,
            Some(certificate) => {
                let mut options = TlsClientOptions::new("localhost");
                options.add_root_certificate(certificate);
                options.set_alpn_protocols([self.route.alpn()]);
                ClientTransportOptions::Tls(options)
            }
        };
        ConnectionParams {
            transport,
            plaintext_protocol: self.route.protocol(),
            identity_validation: IdentityValidation::Enforcing,
            ..ConnectionParams::default()
        }
    }
}

#[derive(Default)]
struct Signal {
    set: AtomicBool,
    wake: Notify,
}

impl Signal {
    fn mark(&self) {
        self.set.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    fn marked(&self) -> bool {
        self.set.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.marked() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Default)]
struct Signals {
    connected: Signal,
    accepted_published: Signal,
    upload: Signal,
    upload_pending: Signal,
    held: Signal,
    pressure: Signal,
    action: Signal,
    failed: Signal,
    peer_done: Signal,
    native_operation_done: Signal,
    peer_acknowledged: Signal,
    native_drain: Signal,
    retirement_published: Signal,
    retirement: Mutex<Option<TransportRetirement>>,
    checked_native: Mutex<Option<oneshot::Receiver<CheckedNativeOutcome>>>,
    accepted: Mutex<Option<(SocketAddr, SocketAddr)>>,
    wire_written: AtomicUsize,
    data_sent: AtomicUsize,
    read_balance: AtomicI64,
    read_received: AtomicUsize,
    ingress_changed: Notify,
    pressure_count: AtomicUsize,
    observe_h2: bool,
    progress: Mutex<ProgressRecords>,
    diagnostic: Option<Arc<ReferenceDiagnostic>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReferenceClient {
    Native,
    Independent,
}

#[derive(Clone, Copy, Debug)]
struct ReferenceCase {
    client: ReferenceClient,
    peer_no_delay: bool,
}

const REFERENCE_CASES: [ReferenceCase; 4] = [
    ReferenceCase {
        client: ReferenceClient::Native,
        peer_no_delay: false,
    },
    ReferenceCase {
        client: ReferenceClient::Independent,
        peer_no_delay: false,
    },
    ReferenceCase {
        client: ReferenceClient::Native,
        peer_no_delay: true,
    },
    ReferenceCase {
        client: ReferenceClient::Independent,
        peer_no_delay: true,
    },
];

#[derive(Clone, Copy, Debug, Default)]
struct ReferenceProgress {
    first: Option<(usize, Duration)>,
    last: Option<(usize, Duration)>,
    count: usize,
}

impl ReferenceProgress {
    fn record(&mut self, amount: usize, elapsed: Duration) {
        self.first.get_or_insert((amount, elapsed));
        self.last = Some((amount, elapsed));
        self.count += 1;
    }
}

#[derive(Clone, Copy, Debug)]
enum ReferenceEndpoint {
    Client,
    Peer,
}

impl ReferenceEndpoint {
    fn index(self) -> usize {
        match self {
            Self::Client => 0,
            Self::Peer => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ReferenceIoState {
    read: ReferenceProgress,
    write: ReferenceProgress,
    eof: Option<Duration>,
    error: Option<(bool, io::ErrorKind, Option<i32>, Duration)>,
    shutdown: Option<Duration>,
    shutdown_error: Option<(io::ErrorKind, Option<i32>, Duration)>,
    dropped: Option<Duration>,
    drops: usize,
    owner: Option<ThreadId>,
    wrong_owner: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct ReferenceState {
    worker: Option<ThreadId>,
    peer_option: Option<(bool, bool, Duration)>,
    outgoing_option: Option<(bool, Duration)>,
    accepted_endpoints: Option<(SocketAddr, SocketAddr)>,
    outgoing_endpoints: Option<(SocketAddr, SocketAddr)>,
    operation: Option<(Duration, Duration)>,
    held: Option<Duration>,
    checkpoint: Option<Duration>,
    released: Option<Duration>,
    resumed: Option<Duration>,
    verified: ReferenceProgress,
    credit_released: usize,
    used_capacity: usize,
    used_peak: usize,
    capacity: ReferenceProgress,
    reservations: ReferenceProgress,
    grants: ReferenceProgress,
    capacity_pending: ReferenceProgress,
    capacity_terminal: Option<(CapacityPoll, Duration)>,
    data: ReferenceProgress,
    trailer_sent: Option<Duration>,
    trailer_received: Option<Duration>,
    body_complete: Option<Duration>,
    handles_released: Option<Duration>,
    driver: Option<(ChildOutcome, Duration)>,
    driver_polls: ReferenceProgress,
    peer_children: [ChildOutcome; 2],
    native_finished: Option<(bool, u32)>,
    io: [ReferenceIoState; 2],
    peer_vectored_capability: Option<bool>,
    peer_vectored_calls: usize,
    peer_vectored_written: usize,
    peer_vectored_cross_slice: Option<(usize, usize, usize, Duration)>,
}

struct ReferenceDiagnostic {
    case: ReferenceCase,
    started: Instant,
    state: Mutex<ReferenceState>,
    before_cancel: Mutex<Option<ReferenceState>>,
    changed: Notify,
}

impl ReferenceDiagnostic {
    fn new(case: ReferenceCase) -> Self {
        Self {
            case,
            started: Instant::now(),
            state: Mutex::new(ReferenceState::default()),
            before_cancel: Mutex::new(None),
            changed: Notify::new(),
        }
    }

    fn update(&self, update: impl FnOnce(&mut ReferenceState, Duration)) {
        let elapsed = self.started.elapsed();
        update(
            &mut self.state.lock().expect("bounded reference state"),
            elapsed,
        );
    }

    fn snapshot(&self) -> ReferenceState {
        *self.state.lock().expect("bounded reference state")
    }

    fn capture_before_cancel(&self) {
        let snapshot = self.snapshot();
        self.before_cancel
            .lock()
            .expect("first pre-cancel snapshot")
            .get_or_insert(snapshot);
    }

    fn sample_used(&self, used: usize) -> usize {
        let changed = {
            let mut state = self.state.lock().expect("reference flow observation");
            let changed = state.used_capacity != used;
            state.used_capacity = used;
            state.used_peak = state.used_peak.max(used);
            changed
        };
        if changed {
            self.changed.notify_waiters();
        }
        used
    }

    fn log(&self, boundary: &str) {
        eprintln!(
            "native_h2_reference case={:?} boundary={boundary} elapsed_from_case_start={:?} state={:?} before_cancel={:?}; raw writes, DATA submission, credit release and consumer verification are distinct, not TCP ACKs",
            self.case,
            self.started.elapsed(),
            self.snapshot(),
            *self.before_cancel.lock().expect("pre-cancel observation"),
        );
    }
}

async fn reference_timeout<Work: Future>(
    bounded: Timeout<Work>,
    diagnostic: &ReferenceDiagnostic,
) -> Result<Work::Output, Elapsed> {
    let mut bounded = Box::pin(bounded);
    let result = bounded.as_mut().await;
    if result.is_err() {
        diagnostic.capture_before_cancel();
    }
    drop(bounded);
    result
}

struct ReferenceIo<Io> {
    inner: Option<Io>,
    diagnostic: Arc<ReferenceDiagnostic>,
    endpoint: ReferenceEndpoint,
    owner: ThreadId,
}

impl<Io> ReferenceIo<Io> {
    fn new(inner: Io, diagnostic: Arc<ReferenceDiagnostic>, endpoint: ReferenceEndpoint) -> Self {
        let owner = std::thread::current().id();
        diagnostic.update(|state, _| state.io[endpoint.index()].owner = Some(owner));
        Self {
            inner: Some(inner),
            diagnostic,
            endpoint,
            owner,
        }
    }

    fn observe(&self, read: bool, result: &Poll<io::Result<usize>>) {
        self.diagnostic.update(|state, elapsed| {
            let state = &mut state.io[self.endpoint.index()];
            state.wrong_owner |= self.owner != std::thread::current().id();
            match result {
                Poll::Ready(Ok(0)) if read => {
                    state.eof.get_or_insert(elapsed);
                }
                Poll::Ready(Ok(amount)) if *amount > 0 => {
                    let progress = if read {
                        &mut state.read
                    } else {
                        &mut state.write
                    };
                    let offset = progress.last.map_or(0, |(offset, _)| offset) + *amount;
                    progress.record(offset, elapsed);
                }
                Poll::Ready(Err(error)) => {
                    state
                        .error
                        .get_or_insert((read, error.kind(), error.raw_os_error(), elapsed));
                }
                _ => {}
            }
        });
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for ReferenceIo<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let nonempty = buffer.remaining() > 0;
        let result =
            Pin::new(self.inner.as_mut().expect("owned observed I/O")).poll_read(context, buffer);
        match &result {
            Poll::Ready(Ok(())) if nonempty => {
                self.observe(true, &Poll::Ready(Ok(buffer.filled().len() - before)))
            }
            Poll::Ready(Err(error)) => self.diagnostic.update(|state, elapsed| {
                let state = &mut state.io[self.endpoint.index()];
                state.wrong_owner |= self.owner != std::thread::current().id();
                state
                    .error
                    .get_or_insert((true, error.kind(), error.raw_os_error(), elapsed));
            }),
            _ => {}
        }
        result
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for ReferenceIo<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result =
            Pin::new(self.inner.as_mut().expect("owned observed I/O")).poll_write(context, bytes);
        self.observe(false, &result);
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(self.inner.as_mut().expect("owned observed I/O"))
            .poll_write_vectored(context, buffers);
        self.observe(false, &result);
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner
            .as_ref()
            .expect("owned observed I/O")
            .is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().expect("owned observed I/O")).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result =
            Pin::new(self.inner.as_mut().expect("owned observed I/O")).poll_shutdown(context);
        if let Poll::Ready(Ok(())) = result {
            self.diagnostic.update(|state, elapsed| {
                state.io[self.endpoint.index()]
                    .shutdown
                    .get_or_insert(elapsed);
            });
        }
        if let Poll::Ready(Err(error)) = &result {
            self.diagnostic.update(|state, elapsed| {
                state.io[self.endpoint.index()]
                    .shutdown_error
                    .get_or_insert((error.kind(), error.raw_os_error(), elapsed));
            });
        }
        result
    }
}

impl<Io> Drop for ReferenceIo<Io> {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.diagnostic.update(|state, elapsed| {
            let state = &mut state.io[self.endpoint.index()];
            state.wrong_owner |= self.owner != std::thread::current().id();
            state.drops += 1;
            state.dropped.get_or_insert(elapsed);
        });
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum NativePhase {
    #[default]
    Initial,
    Header,
    Body,
    Held,
    Trailers,
    Completion,
    Complete,
}

#[derive(Clone, Copy, Debug, Default)]
enum CapacityPoll {
    #[default]
    Unpolled,
    Pending,
    Granted,
    Closed,
    Error,
}

#[derive(Clone, Copy, Debug, Default)]
enum ChildOutcome {
    #[default]
    Unpolled,
    Pending,
    ReturnedOk,
    ReturnedError,
    Aborted,
    FutureDropped,
    ControllerRetired,
}

#[derive(Clone, Copy)]
enum PeerChild {
    Exchange,
    Driver,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerWriteOp {
    Write,
    Flush,
    Shutdown,
}

impl PeerChild {
    fn index(self) -> usize {
        match self {
            Self::Exchange => 0,
            Self::Driver => 1,
        }
    }
}

#[derive(Clone, Copy)]
struct ProgressPoint {
    offset: usize,
    at: Instant,
}

impl ProgressPoint {
    fn observation(
        self,
        release: Option<Instant>,
        captured: Instant,
    ) -> (usize, Option<Duration>, Duration) {
        (
            self.offset,
            release.and_then(|release| self.at.checked_duration_since(release)),
            captured.saturating_duration_since(self.at),
        )
    }
}

#[derive(Clone, Copy, Default)]
struct ProgressState {
    root_release: Option<Instant>,
    native_action: Option<Instant>,
    native_phase: NativePhase,
    native_verified: Option<ProgressPoint>,
    last_granted: Option<usize>,
    current_capacity: Option<usize>,
    capacity_poll: CapacityPoll,
    peer_data: Option<ProgressPoint>,
    peer_wire: Option<ProgressPoint>,
    children: [ChildOutcome; 2],
    peer_first_write_error: Option<PeerWriteOp>,
}

#[derive(Clone, Copy)]
struct ProgressSnapshot {
    state: ProgressState,
    captured: Instant,
    data_sent: usize,
    wire_written: usize,
    read_received: usize,
    read_balance: i64,
    native_ledger: Option<ByteLedger>,
}

impl ProgressSnapshot {
    fn log(self, boundary: &str, position: &ResourcePoint) {
        let state = self.state;
        eprintln!(
            "native_staged H2 cross-thread observation (not atomic wire receipt) boundary={boundary} route={:?} round={} stage={:?} root_release_accepted={} release_age={:?} native_action_observed={} action_age={:?} native_phase={:?} point_format=(offset, after_release, age_at_capture) native_verified={:?} last_granted={:?} last_sampled_capacity={:?} capacity_poll={:?} peer_DATA={:?} peer_wire={:?} children(exchange,driver)={:?} peer_first_write_error={:?} sampled_DATA={} sampled_wire={} sampled_HTTP_received={} sampled_HTTP_balance={} exact_owner_HTTP_ledger={:?}",
            position.route,
            position.round,
            position.stage,
            state.root_release.is_some(),
            state
                .root_release
                .map(|at| self.captured.saturating_duration_since(at)),
            state.native_action.is_some(),
            state
                .native_action
                .map(|at| self.captured.saturating_duration_since(at)),
            state.native_phase,
            state
                .native_verified
                .map(|point| point.observation(state.root_release, self.captured)),
            state.last_granted,
            state.current_capacity,
            state.capacity_poll,
            state
                .peer_data
                .map(|point| point.observation(state.root_release, self.captured)),
            state
                .peer_wire
                .map(|point| point.observation(state.root_release, self.captured)),
            state.children,
            state.peer_first_write_error,
            self.data_sent,
            self.wire_written,
            self.read_received,
            self.read_balance,
            self.native_ledger,
        );
    }
}

#[derive(Default)]
struct ProgressRecords {
    current: ProgressState,
    native_failure: Option<ProgressSnapshot>,
    peer_before_drop: [Option<ProgressSnapshot>; 2],
}

impl Signals {
    fn update_progress(&self, update: impl FnOnce(&mut ProgressState)) {
        if self.observe_h2 {
            update(&mut self.progress.lock().expect("fixed progress state").current);
        }
    }

    fn peer_outcome(&self, child: PeerChild, outcome: ChildOutcome) {
        self.update_progress(|state| state.children[child.index()] = outcome);
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.update(|state, _| state.peer_children[child.index()] = outcome);
        }
    }

    fn verified(&self, offset: usize) {
        let point = ProgressPoint {
            offset,
            at: Instant::now(),
        };
        self.update_progress(|state| state.native_verified = Some(point));
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.update(|state, elapsed| state.verified.record(offset, elapsed));
        }
    }

    fn phase(&self, phase: NativePhase) {
        self.update_progress(|state| state.native_phase = phase);
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.update(|state, elapsed| match phase {
                NativePhase::Held => {
                    state.held.get_or_insert(elapsed);
                }
                NativePhase::Complete => {
                    state.body_complete.get_or_insert(elapsed);
                }
                _ => {}
            });
        }
    }

    fn snapshot(&self, native_ledger: Option<ByteLedger>) -> ProgressSnapshot {
        ProgressSnapshot {
            state: self.progress.lock().expect("fixed progress state").current,
            captured: Instant::now(),
            data_sent: self.data_sent.load(Ordering::SeqCst),
            wire_written: self.wire_written.load(Ordering::SeqCst),
            read_received: self.read_received.load(Ordering::SeqCst),
            read_balance: self.read_balance.load(Ordering::SeqCst),
            native_ledger,
        }
    }

    fn capture_native_failure(&self, ledger: ByteLedger) {
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.capture_before_cancel();
        }
        if self.observe_h2 {
            let snapshot = self.snapshot(Some(ledger));
            self.progress
                .lock()
                .expect("fixed progress state")
                .native_failure
                .get_or_insert(snapshot);
        }
    }

    fn capture_peer_before_drop(&self, child: PeerChild) {
        if self.observe_h2 {
            self.update_progress(|state| {
                if matches!(state.children[child.index()], ChildOutcome::Pending) {
                    state.children[child.index()] = ChildOutcome::FutureDropped;
                }
            });
            let snapshot = self.snapshot(None);
            if let Some(diagnostic) = &self.diagnostic {
                diagnostic.update(|state, _| {
                    state.peer_children[child.index()] = snapshot.state.children[child.index()]
                });
            }
            self.progress
                .lock()
                .expect("fixed progress state")
                .peer_before_drop[child.index()]
            .get_or_insert(snapshot);
        }
    }

    fn log_progress(&self, position: ResourcePoint) {
        if !self.observe_h2 {
            return;
        }
        let (native, peer) = {
            let progress = self.progress.lock().expect("fixed progress state");
            (progress.native_failure, progress.peer_before_drop)
        };
        for (boundary, snapshot) in [
            ("native-before-local-failure-publication-and-cancel", native),
            ("peer-exchange-before-future-drop", peer[0]),
            ("peer-driver-before-future-drop", peer[1]),
        ] {
            if let Some(snapshot) = snapshot {
                snapshot.log(boundary, &position);
            }
        }
    }

    async fn wait(&self, signal: &Signal) -> io::Result<()> {
        tokio::select! {
            biased;
            _ = self.failed.wait() => Err(failure("another owned operation failed")),
            _ = signal.wait() => Ok(()),
        }
    }

    fn pressure(&self) {
        if self.held.marked() && !self.action.marked() {
            self.pressure_count.fetch_add(1, Ordering::SeqCst);
            self.pressure.mark();
        }
    }

    fn held_ingress_ready(&self, route: Route, consumed: usize) -> bool {
        if route.h2 {
            self.read_balance.load(Ordering::SeqCst) > 0
        } else {
            self.read_received
                .load(Ordering::SeqCst)
                .checked_sub(consumed)
                .is_some_and(|buffered| {
                    buffered >= UniplexConfig::default().ingress_buffer_limit as usize
                })
        }
    }

    async fn wait_for_buffered_ingress(&self, route: Route, consumed: usize) -> io::Result<()> {
        loop {
            let changed = self.ingress_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.held_ingress_ready(route, consumed) {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = self.failed.wait() => return Err(failure("ingress observation cancelled after fixture failure")),
                _ = changed => {}
            }
        }
    }
}

struct PeerIo<'a, Io> {
    inner: Io,
    signals: &'a Signals,
}

impl<'a, Io> PeerIo<'a, Io> {
    fn new(inner: Io, signals: &'a Signals) -> Self {
        Self { inner, signals }
    }

    fn observe<Output>(&self, operation: PeerWriteOp, result: &Poll<io::Result<Output>>) {
        if matches!(result, Poll::Ready(Err(_))) {
            self.signals.update_progress(|state| {
                state.peer_first_write_error.get_or_insert(operation);
            });
        }
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for PeerIo<'_, Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for PeerIo<'_, Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(context, bytes);
        self.observe(PeerWriteOp::Write, &result);
        result
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(context, buffers);
        self.observe(PeerWriteOp::Write, &result);
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(context);
        self.observe(PeerWriteOp::Flush, &result);
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_shutdown(context);
        self.observe(PeerWriteOp::Shutdown, &result);
        result
    }
}

struct WireIo {
    socket: TcpStream,
    writer: std::net::TcpStream,
    signals: Arc<Signals>,
    h2: bool,
}

impl AsyncRead for WireIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(context, buffer)
    }
}

impl WireIo {
    fn poll_write_with(
        &self,
        context: &mut Context<'_>,
        mut write: impl FnMut() -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.socket.try_io(Interest::WRITABLE, || {
                let result = write();
                if result
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
                    && !self.h2
                    && self.signals.wire_written.load(Ordering::SeqCst) > 0
                {
                    self.signals.pressure();
                }
                result
            }) {
                Ok(count) => {
                    let previous = self.signals.wire_written.fetch_add(count, Ordering::SeqCst);
                    if self.h2 && count > 0 {
                        let point = ProgressPoint {
                            offset: previous + count,
                            at: Instant::now(),
                        };
                        self.signals
                            .update_progress(|state| state.peer_wire = Some(point));
                    }
                    return Poll::Ready(Ok(count));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    match self.socket.poll_write_ready(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(())) => {}
                    }
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}

impl AsyncWrite for WireIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(context, || Write::write(&mut &self.writer, bytes))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(context, || {
            let result = Write::write_vectored(&mut &self.writer, buffers);
            if let Some(diagnostic) = &self.signals.diagnostic {
                diagnostic.update(|state, elapsed| {
                    state.peer_vectored_calls += 1;
                    if let Ok(count) = &result {
                        state.peer_vectored_written += *count;
                        let first = buffers
                            .iter()
                            .find(|buffer| !buffer.is_empty())
                            .map_or(0, |buffer| buffer.len());
                        if *count > first {
                            state.peer_vectored_cross_slice.get_or_insert((
                                buffers.iter().filter(|buffer| !buffer.is_empty()).count(),
                                first,
                                *count,
                                elapsed,
                            ));
                        }
                    }
                });
            }
            result
        })
    }

    fn is_write_vectored(&self) -> bool {
        let capable = self.socket.is_write_vectored();
        if let Some(diagnostic) = &self.signals.diagnostic {
            diagnostic.update(|state, _| state.peer_vectored_capability = Some(capable));
        }
        capable
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(context)
    }
}

fn pattern(offset: usize, length: usize) -> Bytes {
    Bytes::from(
        (offset..offset + length)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

fn verify_bytes(bytes: &[u8], offset: usize, limit: usize) -> io::Result<usize> {
    require(
        !bytes.is_empty() && offset + bytes.len() <= limit,
        "bounded positive payload",
    )?;
    require(
        bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| *byte == ((offset + index) % 251) as u8),
        "exact deterministic payload",
    )?;
    Ok(offset + bytes.len())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct H1IngressBound {
    flow: i64,
    segment: i64,
    ceiling: i64,
}

impl H1IngressBound {
    fn selected(route: Route, stage: Stage) -> bool {
        !route.h2 && matches!(stage, Stage::BodyDrop | Stage::Release)
    }

    fn from_session(session: &SessionParams) -> io::Result<Self> {
        let config = uniplex_config_for_session_params(session);
        require(
            session.max_concurrent_outgoing_streams == Some(1)
                && config.ingress_buffer_limit == 65_535
                && config.maximum_read_size == 65_536
                && config.read_allocation_size == 4_000,
            "unchanged single-transaction H1 BODY-credit configuration",
        )?;
        let flow = i64::from(config.ingress_buffer_limit);
        let segment = i64::try_from(config.maximum_read_size).map_err(failure)?;
        let ceiling = flow
            .checked_sub(1)
            .and_then(|prefix| prefix.checked_add(segment))
            .ok_or_else(|| failure("checked H1 BODY-credit ceiling"))?;
        let bound = Self {
            flow,
            segment,
            ceiling,
        };
        require(bound.valid(), "fixed source-derived H1 BODY-credit bound")?;
        Ok(bound)
    }

    fn valid(self) -> bool {
        self.flow == 65_535 && self.segment == 65_536 && self.ceiling == 131_070
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct H2IngressBound {
    connection: i64,
    stream: i64,
    frame: i64,
}

impl H2IngressBound {
    fn from_session(session: &SessionParams) -> io::Result<Self> {
        require(
            session.max_concurrent_outgoing_streams == Some(1),
            "unchanged single-transaction H2 configuration",
        )?;
        let connection = session
            .connection_flow_control
            .ok_or_else(|| failure("explicit H2 connection window"))?;
        let stream = session
            .settings
            .iter()
            .rev()
            .find(|setting| setting.identifier == 4)
            .ok_or_else(|| failure("explicit H2 stream receive window"))?
            .value;
        let defaults = HttpSettings::new();
        let frame = session
            .settings
            .iter()
            .rev()
            .find(|setting| setting.identifier == 5)
            .map(|setting| setting.value)
            .or_else(|| {
                defaults
                    .get(SettingsId::MaxFrameSize)
                    .map(|setting| setting.value)
            })
            .ok_or_else(|| failure("actual codec default receive frame limit"))?;
        let bound = Self {
            connection: i64::from(connection),
            stream: i64::try_from(stream).map_err(failure)?,
            frame: i64::try_from(frame).map_err(failure)?,
        };
        require(bound.valid(), "fixed source-derived H2 BODY-credit bounds")?;
        Ok(bound)
    }

    fn valid(self) -> bool {
        self.connection == 65_535 && self.stream == 32_768 && self.frame == 16_384
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IngressBound {
    H1(H1IngressBound),
    H2(H2IngressBound),
}

impl IngressBound {
    fn selected(stage: Stage) -> bool {
        matches!(stage, Stage::BodyDrop | Stage::Release)
    }

    fn from_configuration(
        session: &SessionParams,
        connection: &ConnectionParams,
        route: Route,
    ) -> io::Result<Self> {
        require(
            matches!(&connection.transport, ClientTransportOptions::Tls(_)) == route.tls
                && connection.plaintext_protocol == route.protocol(),
            "final native connection configuration matches the selected route",
        )?;
        if route.h2 {
            H2IngressBound::from_session(session).map(Self::H2)
        } else {
            H1IngressBound::from_session(session).map(Self::H1)
        }
    }

    fn valid(self, route: Route) -> bool {
        match self {
            Self::H1(bound) => !route.h2 && bound.valid(),
            Self::H2(bound) => route.h2 && bound.valid(),
        }
    }

    fn segment(self) -> i64 {
        match self {
            Self::H1(bound) => bound.segment,
            Self::H2(bound) => bound.frame,
        }
    }

    fn ceiling(self) -> i64 {
        match self {
            Self::H1(bound) => bound.ceiling,
            Self::H2(bound) => bound.connection,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IngressCredit {
    bound: IngressBound,
    route: Route,
    stage: Stage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1IngressViolation {
    MissingBinding,
    BindingMismatch,
    NotFresh,
    InvalidLedger,
    WrongOwner,
    Balance { read: bool, before: i64, delta: i64 },
    Segment { delta: i64, maximum: i64 },
    Ceiling { peak: i64, maximum: i64 },
    ReceivedOverflow { before: usize, delta: i64 },
    Transactions { opened: usize, closed: usize },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ByteLedger {
    read: i64,
    write: i64,
    read_peak: i64,
    write_peak: i64,
    read_positive: usize,
    read_received: usize,
    write_positive: usize,
    opened: usize,
    closed: usize,
    invalid: bool,
    ingress: Option<IngressCredit>,
    ingress_violation: Option<H1IngressViolation>,
}

impl ByteLedger {
    fn reject_ingress(&mut self, violation: H1IngressViolation) {
        self.invalid = true;
        if self.ingress_violation.is_none() {
            self.ingress_violation = Some(violation);
        }
    }

    fn ingress_result(&self) -> io::Result<()> {
        match self.ingress_violation {
            Some(violation) => Err(failure(format!(
                "source-bound native BODY-credit violation: {violation:?}"
            ))),
            None => Ok(()),
        }
    }

    fn bind_ingress(
        &mut self,
        bound: H1IngressBound,
        route: Route,
        stage: Stage,
    ) -> io::Result<()> {
        self.bind_native_ingress(IngressBound::H1(bound), route, stage)
    }

    fn bind_native_ingress(
        &mut self,
        bound: IngressBound,
        route: Route,
        stage: Stage,
    ) -> io::Result<()> {
        if !IngressBound::selected(stage) || !bound.valid(route) {
            self.reject_ingress(H1IngressViolation::BindingMismatch);
        } else if *self != Self::default() {
            self.reject_ingress(H1IngressViolation::NotFresh);
        } else {
            self.ingress = Some(IngressCredit {
                bound,
                route,
                stage,
            });
        }
        self.ingress_result()
    }

    fn require_ingress(&mut self, route: Route, stage: Stage) -> io::Result<()> {
        match (IngressBound::selected(stage), self.ingress) {
            (false, None) => return self.ingress_result(),
            (true, None) => self.reject_ingress(H1IngressViolation::MissingBinding),
            (true, Some(credit))
                if credit.stage == stage && credit.route == route && credit.bound.valid(route) => {}
            _ => self.reject_ingress(H1IngressViolation::BindingMismatch),
        }
        if self.invalid && self.ingress_violation.is_none() {
            self.reject_ingress(H1IngressViolation::InvalidLedger);
        }
        self.ingress_result()
    }

    fn check_owner(&mut self, matches: bool) {
        self.invalid |= !matches;
        if !matches && self.ingress.is_some() {
            self.reject_ingress(H1IngressViolation::WrongOwner);
        }
    }

    fn check_ingress_transactions(&mut self) {
        if self.ingress.is_some() && (self.opened != 1 || self.closed > 1) {
            self.reject_ingress(H1IngressViolation::Transactions {
                opened: self.opened,
                closed: self.closed,
            });
        }
    }

    fn observe_ingress(&mut self, delta: i64) {
        let Some(credit) = self.ingress else {
            return;
        };
        self.check_ingress_transactions();
        if delta > credit.bound.segment() {
            self.reject_ingress(H1IngressViolation::Segment {
                delta,
                maximum: credit.bound.segment(),
            });
        }
        if self.read_peak > credit.bound.ceiling() {
            self.reject_ingress(H1IngressViolation::Ceiling {
                peak: self.read_peak,
                maximum: credit.bound.ceiling(),
            });
        }
    }
}

struct Stats {
    owner: ThreadId,
    ledger: RefCell<ByteLedger>,
    signals: Arc<Signals>,
}

impl Stats {
    fn delta(&self, read: bool, delta: i64) {
        let mut ledger = self.ledger.borrow_mut();
        ledger.check_owner(self.owner == std::thread::current().id());
        let current = if read { ledger.read } else { ledger.write };
        let Some(next) = current.checked_add(delta) else {
            ledger.invalid = true;
            if ledger.ingress.is_some() {
                ledger.reject_ingress(H1IngressViolation::Balance {
                    read,
                    before: current,
                    delta,
                });
            }
            return;
        };
        ledger.invalid |= next < 0;
        if next < 0 && ledger.ingress.is_some() {
            ledger.reject_ingress(H1IngressViolation::Balance {
                read,
                before: current,
                delta,
            });
        }
        if read {
            ledger.read = next;
            ledger.read_peak = ledger.read_peak.max(next);
            ledger.observe_ingress(delta);
            ledger.read_positive += usize::from(delta > 0);
            if delta > 0 {
                let Some(received) = usize::try_from(delta)
                    .ok()
                    .and_then(|amount| ledger.read_received.checked_add(amount))
                else {
                    ledger.invalid = true;
                    if ledger.ingress.is_some() {
                        let before = ledger.read_received;
                        ledger
                            .reject_ingress(H1IngressViolation::ReceivedOverflow { before, delta });
                    }
                    return;
                };
                ledger.read_received = received;
                self.signals.read_received.store(received, Ordering::SeqCst);
            }
            self.signals.read_balance.store(next, Ordering::SeqCst);
            self.signals.ingress_changed.notify_waiters();
        } else {
            ledger.write = next;
            ledger.write_peak = ledger.write_peak.max(next);
            ledger.write_positive += usize::from(delta > 0);
        }
    }
}

impl Http2SessionStats for Stats {
    fn record_buffered_read_bytes(&self, delta: i64) {
        self.delta(true, delta);
    }
    fn record_buffered_write_bytes(&self, delta: i64) {
        self.delta(false, delta);
    }
    fn record_transaction_opened(&self) {
        let mut ledger = self.ledger.borrow_mut();
        ledger.check_owner(self.owner == std::thread::current().id());
        ledger.opened += 1;
        ledger.check_ingress_transactions();
    }
    fn record_transaction_closed(&self) {
        let mut ledger = self.ledger.borrow_mut();
        ledger.check_owner(self.owner == std::thread::current().id());
        ledger.closed += 1;
        ledger.check_ingress_transactions();
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SourceProof {
    constructed: usize,
    polled: usize,
    stops: usize,
    drops: usize,
    state_drops: usize,
    live_reads: usize,
    read_drops: usize,
    terminal: usize,
    wrong_owner: bool,
    premature: bool,
    stop_error: Option<HttpErrorCode>,
}

struct UploadState {
    owner: ThreadId,
    proof: Rc<RefCell<SourceProof>>,
    signals: Arc<Signals>,
    suffix: Option<oneshot::Receiver<()>>,
    offset: usize,
    stopped: bool,
}

impl Drop for UploadState {
    fn drop(&mut self) {
        let mut proof = self.proof.borrow_mut();
        proof.wrong_owner |= self.owner != std::thread::current().id();
        proof.state_drops += 1;
    }
}

struct ReadOwner {
    owner: ThreadId,
    proof: Rc<RefCell<SourceProof>>,
}

impl ReadOwner {
    fn new(owner: ThreadId, proof: &Rc<RefCell<SourceProof>>) -> Self {
        proof.borrow_mut().live_reads += 1;
        Self {
            owner,
            proof: Rc::clone(proof),
        }
    }

    fn poll(&self) {
        let mut proof = self.proof.borrow_mut();
        proof.polled += 1;
        proof.wrong_owner |= self.owner != std::thread::current().id();
    }
}

impl Drop for ReadOwner {
    fn drop(&mut self) {
        let mut proof = self.proof.borrow_mut();
        proof.live_reads -= 1;
        proof.read_drops += 1;
        proof.wrong_owner |= self.owner != std::thread::current().id();
    }
}

struct UploadSource {
    header: Option<HttpHeaderEvent>,
    state: Rc<RefCell<UploadState>>,
}

impl HttpSource for UploadSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let mut header = self.header.take();
        let state = self.state.borrow();
        let owner = ReadOwner::new(state.owner, &state.proof);
        Box::pin(poll_fn(move |_| {
            owner.poll();
            Poll::Ready(
                header
                    .take()
                    .ok_or_else(|| source_error("header read twice")),
            )
        }))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let state = Rc::clone(&self.state);
        let owner = ReadOwner::new(state.borrow().owner, &state.borrow().proof);
        Box::pin(poll_fn(move |context| {
            owner.poll();
            let mut state = state.borrow_mut();
            if state.stopped || maximum == 0 || state.offset == UPLOAD {
                return Poll::Ready(Err(source_error("invalid upload read")));
            }
            if state.offset == CHUNK {
                if let Some(suffix) = &mut state.suffix {
                    match Pin::new(suffix).poll(context) {
                        Poll::Pending => {
                            state.signals.upload_pending.mark();
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(source_error(error))),
                        Poll::Ready(Ok(())) => state.suffix = None,
                    }
                }
            }
            let amount = (maximum as usize).min(CHUNK - state.offset % CHUNK);
            let data = pattern(state.offset, amount);
            state.offset += amount;
            let eom = state.offset == UPLOAD;
            if eom {
                state.proof.borrow_mut().terminal += 1;
            }
            Poll::Ready(
                BufQueue::from_bytes(data, CHUNK)
                    .map(|body| HttpBodyEvent::body(body, eom))
                    .map_err(source_error),
            )
        }))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        let mut state = self.state.borrow_mut();
        state.stopped = true;
        let mut proof = state.proof.borrow_mut();
        proof.stops += 1;
        proof.stop_error = error;
        proof.wrong_owner |= state.owner != std::thread::current().id();
        proof.premature |= state.suffix.is_some() && !state.signals.action.marked();
    }
}

impl Drop for UploadSource {
    fn drop(&mut self) {
        let state = self.state.borrow();
        let mut proof = state.proof.borrow_mut();
        proof.drops += 1;
        proof.wrong_owner |= state.owner != std::thread::current().id();
        proof.premature |= state.suffix.is_some() && !state.signals.action.marked();
    }
}

fn upload_source(
    stage: Stage,
    route: Route,
    proof: Rc<RefCell<SourceProof>>,
    signals: Arc<Signals>,
) -> io::Result<(HttpSourceHolder, Option<oneshot::Sender<()>>)> {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Post).map_err(failure)?;
    message.set_url(b"/native-staged", false).map_err(failure)?;
    message.set_http_version(1, 1);
    message.set_secure(route.tls);
    message.set_wants_keepalive(true);
    message
        .headers_mut()
        .set(b"host", stage.authority().as_bytes());
    message
        .headers_mut()
        .set(b"content-length", UPLOAD.to_string().as_bytes());
    message.headers_mut().set(
        b"forwarded",
        b"for=192.0.2.9;proto=https;host=untrusted.invalid",
    );
    message.headers_mut().set(b"x-forwarded-proto", b"http");
    message
        .headers_mut()
        .set(b"x-client-cert", b"not-a-transport-identity");
    let (sender, suffix) = if stage == Stage::UploadCancel {
        let (sender, receiver) = oneshot::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };
    proof.borrow_mut().constructed += 1;
    Ok((
        HttpSourceHolder::new(Box::new(UploadSource {
            header: Some(HttpHeaderEvent::new(message, false).map_err(failure)?),
            state: Rc::new(RefCell::new(UploadState {
                owner: std::thread::current().id(),
                proof,
                signals,
                suffix,
                offset: 0,
                stopped: false,
            })),
        })),
        sender,
    ))
}

async fn h1_head(stream: &mut (impl AsyncRead + Unpin), stage: Stage) -> io::Result<()> {
    let mut header = Vec::with_capacity(512);
    loop {
        require(header.len() < CHUNK, "bounded independent request head")?;
        header.push(stream.read_u8().await?);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = std::str::from_utf8(&header).map_err(failure)?;
    let mut lines = text.split("\r\n");
    require(
        lines.next() == Some("POST /native-staged HTTP/1.1"),
        "actual H1 request line",
    )?;
    let mut lengths = 0;
    let mut hosts = 0;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| failure("malformed request header"))?;
        if name.eq_ignore_ascii_case("content-length") {
            lengths += 1;
            require(
                value.trim() == UPLOAD.to_string(),
                "exact H1 upload content length",
            )?;
        }
        if name.eq_ignore_ascii_case("host") {
            hosts += 1;
            require(
                value.trim() == stage.authority(),
                "synthetic authority is sent, not trusted",
            )?;
        }
        require(
            !name.eq_ignore_ascii_case("transfer-encoding"),
            "fixed-size upload is not chunked",
        )?;
    }
    require(
        lengths == 1 && hosts == 1,
        "single upload length and authority",
    )
}

fn abortive_read_error(error: &io::Error, route: Route) -> bool {
    error.kind() == io::ErrorKind::ConnectionReset
        || (route.tls && error.kind() == io::ErrorKind::UnexpectedEof)
}

async fn h1_end(
    stream: &mut (impl AsyncRead + Unpin),
    route: Route,
    cancelled: bool,
) -> io::Result<()> {
    let mut byte = [0];
    match stream.read(&mut byte).await {
        Ok(0) => Ok(()),
        Err(error) if cancelled && abortive_read_error(&error, route) => Ok(()),
        Err(error) => Err(error),
        Ok(_) => Err(failure(
            "unexpected byte after controlled request or response",
        )),
    }
}

async fn h1_response(stream: &mut (impl AsyncWrite + Unpin), signals: &Signals) -> io::Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-native-terminal\r\n\r\n",
        )
        .await?;
    for offset in (0..DOWNLOAD).step_by(CHUNK) {
        let data = pattern(offset, CHUNK);
        stream.write_all(b"4000\r\n").await?;
        stream.write_all(&data).await?;
        stream.write_all(b"\r\n").await?;
        signals.data_sent.fetch_add(CHUNK, Ordering::SeqCst);
        if offset == 0 {
            stream.flush().await?;
            signals.wait(&signals.held).await?;
        }
    }
    stream
        .write_all(b"0\r\nx-native-terminal: exact\r\n\r\n")
        .await?;
    stream.flush().await
}

async fn h1_peer(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    route: Route,
    stage: Stage,
    signals: &Signals,
) -> io::Result<()> {
    h1_head(&mut stream, stage).await?;
    let mut chunk = vec![0; CHUNK];
    stream.read_exact(&mut chunk).await?;
    verify_bytes(&chunk, 0, UPLOAD)?;
    if stage == Stage::UploadCancel {
        signals.upload.mark();
        signals.wait(&signals.action).await?;
        return h1_end(&mut stream, route, true).await;
    }
    stream.read_exact(&mut chunk).await?;
    verify_bytes(&chunk, CHUNK, UPLOAD)?;
    signals.upload.mark();
    if stage == Stage::FutureDrop {
        signals.wait(&signals.action).await?;
        return h1_end(&mut stream, route, true).await;
    }
    let result = h1_response(&mut stream, signals).await;
    if stage == Stage::BodyDrop {
        require(
            signals.pressure.marked() && signals.action.marked(),
            "write failure follows pressured body drop",
        )?;
        return match result {
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
            Ok(()) => Err(failure(
                "abandoned pressured H1 response unexpectedly completed",
            )),
        };
    }
    result?;
    require(
        signals.data_sent.load(Ordering::SeqCst) == DOWNLOAD,
        "complete H1 producer bytes",
    )?;
    h1_end(&mut stream, route, false).await?;
    stream.shutdown().await
}

async fn h2_upload(body: &mut RecvStream, count: usize) -> io::Result<()> {
    let mut consumed = 0;
    while consumed < count {
        let data = body
            .data()
            .await
            .ok_or_else(|| failure("premature H2 request EOM"))?
            .map_err(io::Error::other)?;
        consumed = verify_bytes(&data, consumed, count)?;
        body.flow_control()
            .release_capacity(data.len())
            .map_err(io::Error::other)?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum ChunkOutcome {
    Sent,
    CapacityClosed,
}

async fn h2_chunk(
    stream: &mut SendStream<Bytes>,
    offset: usize,
    signals: &Signals,
) -> io::Result<ChunkOutcome> {
    let data = pattern(offset, CHUNK);
    let mut sent = 0;
    while sent < data.len() {
        if let Some(diagnostic) = &signals.diagnostic {
            diagnostic
                .update(|state, elapsed| state.reservations.record(data.len() - sent, elapsed));
        }
        stream.reserve_capacity(data.len() - sent);
        let capacity = poll_fn(|context| {
            let capacity = stream.poll_capacity(context);
            let current = stream.capacity();
            if let Some(diagnostic) = &signals.diagnostic {
                diagnostic.update(|state, elapsed| {
                    state.capacity.record(current, elapsed);
                    if capacity.is_pending() {
                        state
                            .capacity_pending
                            .record(signals.data_sent.load(Ordering::SeqCst), elapsed);
                    }
                    match &capacity {
                        Poll::Ready(Some(Ok(granted))) => state.grants.record(*granted, elapsed),
                        Poll::Ready(Some(Err(_))) => {
                            state
                                .capacity_terminal
                                .get_or_insert((CapacityPoll::Error, elapsed));
                        }
                        Poll::Ready(None) => {
                            state
                                .capacity_terminal
                                .get_or_insert((CapacityPoll::Closed, elapsed));
                        }
                        Poll::Pending => {}
                    }
                });
            }
            signals.update_progress(|state| {
                state.current_capacity = Some(current);
                state.capacity_poll = match &capacity {
                    Poll::Pending => CapacityPoll::Pending,
                    Poll::Ready(Some(Ok(granted))) => {
                        state.last_granted = Some(*granted);
                        CapacityPoll::Granted
                    }
                    Poll::Ready(Some(Err(_))) => CapacityPoll::Error,
                    Poll::Ready(None) => CapacityPoll::Closed,
                };
            });
            if capacity.is_pending() && signals.data_sent.load(Ordering::SeqCst) > 0 {
                signals.pressure();
            }
            capacity
        })
        .await;
        let Some(capacity) = capacity else {
            return Ok(ChunkOutcome::CapacityClosed);
        };
        let capacity = capacity.map_err(io::Error::other)?;
        require(capacity > 0, "positive granted H2 response capacity")?;
        let amount = capacity.min(data.len() - sent);
        stream
            .send_data(data.slice(sent..sent + amount), false)
            .map_err(io::Error::other)?;
        sent += amount;
        let previous = signals.data_sent.fetch_add(amount, Ordering::SeqCst);
        if let Some(diagnostic) = &signals.diagnostic {
            diagnostic.update(|state, elapsed| state.data.record(previous + amount, elapsed));
        }
        let point = ProgressPoint {
            offset: previous + amount,
            at: Instant::now(),
        };
        let current = stream.capacity();
        signals.update_progress(|state| {
            state.peer_data = Some(point);
            state.current_capacity = Some(current);
        });
    }
    Ok(ChunkOutcome::Sent)
}

async fn require_h2_cancel(stream: &mut SendStream<Bytes>) -> io::Result<()> {
    let reason = poll_fn(|context| stream.poll_reset(context))
        .await
        .map_err(io::Error::other)?;
    require(
        reason == Reason::CANCEL,
        "actual retained H2 stream reset is CANCEL",
    )
}

async fn h2_exchange(
    request: http::Request<RecvStream>,
    mut response: h2::server::SendResponse<Bytes>,
    stage: Stage,
    signals: &Signals,
) -> io::Result<()> {
    require(
        request.method() == http::Method::POST && request.uri().path() == "/native-staged",
        "actual H2 request method/path",
    )?;
    require(
        request
            .headers()
            .get("content-length")
            .is_some_and(|value| value == UPLOAD.to_string().as_str()),
        "exact H2 upload length",
    )?;
    require(
        request
            .uri()
            .authority()
            .is_some_and(|authority| authority.as_str() == stage.authority()),
        "untrusted H2 authority is transmitted",
    )?;
    let mut body = request.into_body();
    h2_upload(
        &mut body,
        if stage == Stage::UploadCancel {
            CHUNK
        } else {
            UPLOAD
        },
    )
    .await?;
    if stage == Stage::UploadCancel {
        signals.upload.mark();
        signals.wait(&signals.action).await?;
        let error = body
            .data()
            .await
            .ok_or_else(|| failure("cancelled H2 upload ended successfully"))?
            .err()
            .ok_or_else(|| failure("held upload suffix reached peer"))?;
        return require(
            error.reason() == Some(Reason::CANCEL),
            "H2 request cancellation is RST_STREAM CANCEL",
        );
    }
    require(body.data().await.is_none(), "complete H2 upload EOM")?;
    require(
        body.trailers().await.map_err(io::Error::other)?.is_none(),
        "no invented upload trailers",
    )?;
    drop(body);
    signals.upload.mark();
    if stage == Stage::FutureDrop {
        signals.wait(&signals.action).await?;
        let reason = poll_fn(|context| response.poll_reset(context))
            .await
            .map_err(io::Error::other)?;
        return require(
            reason == Reason::CANCEL,
            "pending response-future drop is RST_STREAM CANCEL",
        );
    }
    let mut stream = response
        .send_response(
            Response::builder()
                .status(200)
                .header("trailer", "x-native-terminal")
                .body(())
                .map_err(failure)?,
            false,
        )
        .map_err(io::Error::other)?;
    let result = async {
        for offset in (0..DOWNLOAD).step_by(CHUNK) {
            if h2_chunk(&mut stream, offset, signals).await? == ChunkOutcome::CapacityClosed {
                return Ok(false);
            }
            if offset == 0 {
                signals.wait(&signals.held).await?;
            }
        }
        let mut trailers = HeaderMap::new();
        trailers.insert("x-native-terminal", HeaderValue::from_static("exact"));
        stream.send_trailers(trailers).map_err(io::Error::other)?;
        if let Some(diagnostic) = &signals.diagnostic {
            diagnostic.update(|state, elapsed| state.trailer_sent = Some(elapsed));
        }
        Ok::<bool, io::Error>(true)
    }
    .await;
    if stage == Stage::BodyDrop {
        require(
            signals.pressure.marked() && signals.action.marked(),
            "H2 reset follows pressured body drop",
        )?;
        require(
            !result?,
            "abandoned pressured H2 response unexpectedly completed",
        )?;
        return require_h2_cancel(&mut stream).await;
    }
    require(
        result?,
        "released H2 response capacity closed before completion",
    )?;
    require(
        signals.data_sent.load(Ordering::SeqCst) == DOWNLOAD,
        "complete H2 producer bytes",
    )
}

#[derive(Debug, Default, thiserror::Error)]
#[error(
    "inline H2 children: first={first:?}; second={second:?}; exchange_aborted={exchange_aborted}; driver_aborted={driver_aborted}"
)]
struct PeerChildrenFailure {
    #[source]
    first: Option<io::Error>,
    second: Option<io::Error>,
    exchange_aborted: bool,
    driver_aborted: bool,
}

struct PeerProgressGuard<'a> {
    signals: &'a Signals,
    child: PeerChild,
}

impl Drop for PeerProgressGuard<'_> {
    fn drop(&mut self) {
        self.signals.capture_peer_before_drop(self.child);
    }
}

async fn observe_peer_child(
    work: impl Future<Output = io::Result<()>>,
    signals: &Signals,
    failures: &RefCell<PeerChildrenFailure>,
    child: PeerChild,
) -> bool {
    let mut work = Box::pin(work);
    let observation = PeerProgressGuard { signals, child };
    signals.peer_outcome(child, ChildOutcome::Pending);
    let result = tokio::select! {
        biased;
        result = &mut work => Some(result),
        _ = signals.failed.wait() => None,
    };
    signals.peer_outcome(
        child,
        match &result {
            Some(Ok(())) => ChildOutcome::ReturnedOk,
            Some(Err(_)) => ChildOutcome::ReturnedError,
            None => ChildOutcome::Aborted,
        },
    );
    drop(observation);
    drop(work);
    match result {
        Some(Ok(())) => false,
        Some(Err(error)) => {
            {
                let mut failures = failures.borrow_mut();
                if failures.first.is_none() {
                    failures.first = Some(error);
                } else {
                    failures.second = Some(error);
                }
            }
            if let Some(diagnostic) = &signals.diagnostic {
                diagnostic.capture_before_cancel();
            }
            signals.failed.mark();
            false
        }
        None => true,
    }
}

async fn join_peer_children(
    exchange: impl Future<Output = io::Result<()>>,
    driver: impl Future<Output = io::Result<()>>,
    signals: &Signals,
    driver_first: bool,
) -> io::Result<()> {
    let failures = RefCell::new(PeerChildrenFailure::default());
    let exchange = observe_peer_child(exchange, signals, &failures, PeerChild::Exchange);
    let driver = observe_peer_child(driver, signals, &failures, PeerChild::Driver);
    let (exchange_aborted, driver_aborted) = if driver_first {
        let (driver, exchange) = tokio::join!(biased; driver, exchange);
        (exchange, driver)
    } else {
        tokio::join!(biased; exchange, driver)
    };
    let mut failures = failures.into_inner();
    failures.exchange_aborted = exchange_aborted;
    failures.driver_aborted = driver_aborted;
    if failures.first.is_some() || exchange_aborted || driver_aborted {
        Err(io::Error::other(failures))
    } else {
        Ok(())
    }
}

async fn until_failed<Output>(
    work: impl Future<Output = io::Result<Output>>,
    signals: &Signals,
) -> io::Result<Output> {
    tokio::select! {
        biased;
        result = work => result,
        _ = signals.failed.wait() => Err(failure("peer setup cancelled after native fixture failure")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerDriverRetirement {
    Terminal,
    Controller,
    Aborted,
}

async fn join_controlled_peer_children(
    exchange: impl Future<Output = io::Result<()>>,
    driver: impl Future<Output = io::Result<()>>,
    signals: &Signals,
    driver_first: bool,
) -> io::Result<PeerDriverRetirement> {
    let failures = RefCell::new(PeerChildrenFailure::default());
    let exchange_done = Signal::default();
    let exchange = observe_peer_child(
        async {
            let result = exchange.await;
            if result.is_ok() {
                exchange_done.mark();
            }
            result
        },
        signals,
        &failures,
        PeerChild::Exchange,
    );
    let driver = async {
        let mut driver = Box::pin(driver);
        let observation = PeerProgressGuard {
            signals,
            child: PeerChild::Driver,
        };
        signals.peer_outcome(PeerChild::Driver, ChildOutcome::Pending);
        let result = tokio::select! {
            biased;
            result = &mut driver => Some(result),
            _ = signals.failed.wait() => None,
            _ = async {
                exchange_done.wait().await;
                signals.native_operation_done.wait().await;
            } => {
                signals.peer_outcome(PeerChild::Driver, ChildOutcome::ControllerRetired);
                drop(observation);
                drop(driver);
                return PeerDriverRetirement::Controller;
            },
        };
        signals.peer_outcome(
            PeerChild::Driver,
            match &result {
                Some(Ok(())) => ChildOutcome::ReturnedOk,
                Some(Err(_)) => ChildOutcome::ReturnedError,
                None => ChildOutcome::Aborted,
            },
        );
        drop(observation);
        drop(driver);
        match result {
            Some(Ok(())) => PeerDriverRetirement::Terminal,
            Some(Err(error)) => {
                {
                    let mut failures = failures.borrow_mut();
                    if failures.first.is_none() {
                        failures.first = Some(error);
                    } else {
                        failures.second = Some(error);
                    }
                }
                signals.failed.mark();
                PeerDriverRetirement::Terminal
            }
            None => PeerDriverRetirement::Aborted,
        }
    };
    let (exchange_aborted, retirement) = if driver_first {
        let (retirement, exchange_aborted) = tokio::join!(biased; driver, exchange);
        (exchange_aborted, retirement)
    } else {
        tokio::join!(biased; exchange, driver)
    };
    let mut failures = failures.into_inner();
    failures.exchange_aborted = exchange_aborted;
    failures.driver_aborted = retirement == PeerDriverRetirement::Aborted;
    if failures.first.is_some() || failures.exchange_aborted || failures.driver_aborted {
        Err(io::Error::other(failures))
    } else {
        signals.wait(&signals.native_operation_done).await?;
        Ok(retirement)
    }
}

fn acknowledge_peer(
    signals: &Signals,
    retirement: PeerDriverRetirement,
) -> io::Result<PeerDriverRetirement> {
    require(
        !signals.failed.marked(),
        "prior peer/native failure prevents acknowledgement",
    )?;
    require(
        signals.native_operation_done.marked(),
        "native operation precedes peer acknowledgement",
    )?;
    require(
        !signals.native_drain.marked(),
        "peer retires before native drain",
    )?;
    require(
        retirement != PeerDriverRetirement::Aborted,
        "aborted driver is not controller retirement",
    )?;
    signals.peer_acknowledged.mark();
    Ok(retirement)
}

async fn published_retirement(signals: &Signals) -> io::Result<TransportRetirement> {
    signals.retirement_published.wait().await;
    signals
        .retirement
        .lock()
        .map_err(failure)?
        .clone()
        .ok_or_else(|| failure("native transport does not support retirement observation"))
}

#[derive(Clone, Copy)]
enum H2PeerPurpose {
    Staged,
    GatedAccounting,
}

impl H2PeerPurpose {
    fn requires_pong(self, stage: Stage) -> bool {
        matches!(self, Self::Staged) && matches!(stage, Stage::UploadCancel | Stage::FutureDrop)
    }
}

async fn h2_input_barrier(
    purpose: H2PeerPurpose,
    stage: Stage,
    ping: Option<h2::PingPong>,
    exchange: impl Future<Output = io::Result<()>>,
) -> io::Result<()> {
    if matches!(purpose, H2PeerPurpose::GatedAccounting) {
        require(
            matches!(stage, Stage::UploadCancel),
            "gated accounting peer only exercises UploadCancel",
        )?;
    }
    if purpose.requires_pong(stage) {
        let mut ping =
            ping.ok_or_else(|| failure("original H2 input barrier handle unavailable"))?;
        ping.ping(h2::Ping::opaque())
            .await
            .map_err(io::Error::other)?;
    }
    exchange.await
}

async fn controlled_h2_peer(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    stage: Stage,
    signals: &Signals,
) -> io::Result<()> {
    controlled_h2_peer_for(stream, stage, signals, H2PeerPurpose::Staged).await
}

async fn gated_accounting_h2_peer(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    signals: &Signals,
) -> io::Result<()> {
    controlled_h2_peer_for(
        stream,
        Stage::UploadCancel,
        signals,
        H2PeerPurpose::GatedAccounting,
    )
    .await
}

async fn controlled_h2_peer_for(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    stage: Stage,
    signals: &Signals,
    purpose: H2PeerPurpose,
) -> io::Result<()> {
    let operation = AssertUnwindSafe(async {
        let mut connection = until_failed(
            async {
                h2::server::handshake(&mut stream)
                    .await
                    .map_err(io::Error::other)
            },
            signals,
        )
        .await?;
        let (request, response) = until_failed(
            async {
                connection
                    .accept()
                    .await
                    .ok_or_else(|| failure("H2 peer never accepted a request"))?
                    .map_err(io::Error::other)
            },
            signals,
        )
        .await?;
        let ping = if purpose.requires_pong(stage) {
            connection.ping_pong()
        } else {
            None
        };
        let result = join_controlled_peer_children(
            h2_input_barrier(
                purpose,
                stage,
                ping,
                h2_exchange(request, response, stage, signals),
            ),
            async move {
                match connection.accept().await {
                    None => Ok(()),
                    Some(Err(error)) => Err(io::Error::other(error)),
                    Some(Ok(_)) => Err(failure("unexpected second H2 request")),
                }
            },
            signals,
            false,
        )
        .await;
        result.and_then(|retirement| acknowledge_peer(signals, retirement))
    })
    .catch_unwind()
    .await;
    if !matches!(&operation, Ok(Ok(_))) {
        signals.failed.mark();
    }
    let checked = signals
        .checked_native
        .lock()
        .map(|mut checked| checked.take())
        .map_err(failure);
    let retired = match checked {
        Ok(Some(checked)) => {
            release_checked_peer_io(stream, published_retirement(signals), checked, STAGE_BOUND)
                .await
        }
        Ok(None) => {
            drop(stream);
            Err(failure("missing final checked native outcome receiver"))
        }
        Err(error) => {
            drop(stream);
            Err(error)
        }
    };
    match operation {
        Ok(Ok(_)) => retired,
        Ok(Err(error)) => {
            if let Err(cleanup) = retired {
                eprintln!("peer transport retirement after original failure: {cleanup}");
            }
            Err(error)
        }
        Err(payload) => {
            eprintln!("peer transport retirement after panic: {retired:?}");
            resume_unwind(payload)
        }
    }
}

async fn h2_peer(
    stream: impl AsyncRead + AsyncWrite + Unpin,
    stage: Stage,
    signals: &Signals,
) -> io::Result<()> {
    let mut connection = until_failed(
        async {
            h2::server::handshake(stream)
                .await
                .map_err(io::Error::other)
        },
        signals,
    )
    .await?;
    let (request, response) = until_failed(
        async {
            connection
                .accept()
                .await
                .ok_or_else(|| failure("H2 peer never accepted a request"))?
                .map_err(io::Error::other)
        },
        signals,
    )
    .await?;
    let exchange = h2_exchange(request, response, stage, signals);
    let driver = async {
        match connection.accept().await {
            None => Ok(()),
            Some(Err(error)) => Err(io::Error::other(error)),
            Some(Ok(_)) => Err(failure("unexpected second H2 request")),
        }
    };
    join_peer_children(exchange, driver, signals, false).await
}

async fn peer(
    listener: &TcpListener,
    fixture: &RouteFixture,
    stage: Stage,
    shared: Arc<Signals>,
) -> io::Result<()> {
    let signals = shared.as_ref();
    let operation = async {
        let (socket, remote) = until_failed(listener.accept(), signals).await?;
        if let Some(diagnostic) = &signals.diagnostic {
            require(
                fixture.route.h2 && !fixture.route.tls && stage == Stage::Release,
                "diagnostic peer is plaintext H2 release only",
            )?;
            let before = socket.nodelay()?;
            socket.set_nodelay(diagnostic.case.peer_no_delay)?;
            let after = socket.nodelay()?;
            diagnostic.update(|state, elapsed| state.peer_option = Some((before, after, elapsed)));
            require(
                after == diagnostic.case.peer_no_delay,
                "actual accepted peer NODELAY matches fixed case",
            )?;
            let local = socket.local_addr()?;
            diagnostic.update(|state, _| state.accepted_endpoints = Some((local, remote)));
        } else {
            socket.set_nodelay(true)?;
            require(socket.nodelay()?, "healthy fixture peer NODELAY is enabled")?;
        }
        *signals.accepted.lock().map_err(failure)? = Some((socket.local_addr()?, remote));
        signals.accepted_published.mark();
        let socket = socket.into_std()?;
        let writer = socket.try_clone()?;
        let stream = WireIo {
            socket: TcpStream::from_std(socket)?,
            writer,
            signals: Arc::clone(&shared),
            h2: fixture.route.h2,
        };
        if let Some(diagnostic) = &signals.diagnostic {
            return h2_peer(
                ReferenceIo::new(stream, Arc::clone(diagnostic), ReferenceEndpoint::Peer),
                stage,
                signals,
            )
            .await;
        }
        match &fixture.tls {
            Some(config) => {
                let stream = until_failed(
                    TlsAcceptor::from(Arc::clone(config)).accept(stream),
                    signals,
                )
                .await?;
                require(
                    stream.get_ref().1.alpn_protocol() == Some(fixture.route.alpn()),
                    "actual enforcing fixture ALPN",
                )?;
                if fixture.route.h2 {
                    controlled_h2_peer(PeerIo::new(stream, signals), stage, signals).await
                } else {
                    until_failed(h1_peer(stream, fixture.route, stage, signals), signals).await
                }
            }
            None => {
                if fixture.route.h2 {
                    controlled_h2_peer(PeerIo::new(stream, signals), stage, signals).await
                } else {
                    until_failed(h1_peer(stream, fixture.route, stage, signals), signals).await
                }
            }
        }
    };
    let result = if let Some(diagnostic) = &signals.diagnostic {
        reference_timeout(timeout(ROUTE_BOUND, operation), diagnostic)
            .await
            .map_err(io::Error::other)
            .and_then(|result| result)
    } else {
        timeout(ROUTE_BOUND, operation)
            .await
            .map_err(failure)
            .and_then(|result| result)
    };
    if result.is_err() {
        if let Some(diagnostic) = &signals.diagnostic {
            diagnostic.capture_before_cancel();
        }
        signals.failed.mark();
    }
    signals.peer_done.mark();
    result
}

#[derive(Clone, Copy, Debug)]
struct StagePoint {
    stage: Stage,
    ledger: ByteLedger,
    source: SourceProof,
    data_sent: usize,
    wire_written: usize,
    pressure_count: usize,
}

enum StageSender {
    Original(oneshot::Sender<StagePoint>),
    Diagnostic(oneshot::Sender<ReferenceCheckpoint>),
}

impl StageSender {
    fn send(self, point: StagePoint) -> io::Result<()> {
        match self {
            Self::Original(sender) => sender
                .send(point)
                .map_err(|_| failure("synchronous stage observer disappeared")),
            Self::Diagnostic(sender) => sender
                .send(ReferenceCheckpoint::Native(point))
                .map_err(|_| failure("diagnostic stage observer disappeared")),
        }
    }
}

enum ReferenceCheckpoint {
    Native(StagePoint),
    Independent {
        consumed: usize,
        released: usize,
        used: usize,
    },
}

struct NativeSetup {
    fixture: RouteFixture,
    stage: Stage,
    address: SocketAddr,
    signals: Arc<Signals>,
    reached: StageSender,
    release: oneshot::Receiver<()>,
    command_context: RuntimeContext,
}

struct NativeObservation {
    connection: Arc<ConnectionContext>,
    ledger: ByteLedger,
    source: SourceProof,
    owner: ThreadId,
    response_bytes: usize,
    command_context: RuntimeContext,
}

fn verify_connection(
    connection: &ConnectionContext,
    fixture: &RouteFixture,
    signals: &Signals,
) -> io::Result<()> {
    let (server, client) = signals
        .accepted
        .lock()
        .map_err(failure)?
        .ok_or_else(|| failure("independent accept endpoints missing"))?;
    require(
        connection.local_endpoint().socket_address()
            == Some(&SocketAddress::new(client.ip().to_string(), client.port()))
            && connection.peer_endpoint().socket_address()
                == Some(&SocketAddress::new(server.ip().to_string(), server.port())),
        "trusted endpoints agree with actual independent accept",
    )?;
    require(
        connection.upstream_origin().is_none(),
        "native direct connection has no invented upstream origin",
    )?;
    require(
        connection.security()
            == if fixture.route.tls {
                TransportSecurity::Tls
            } else {
                TransportSecurity::Plaintext
            },
        "actual immediate transport security",
    )?;
    require(
        connection.peer_identity() == fixture.certificate.as_deref(),
        "exact default native certificate identity or plaintext absence",
    )
}

async fn still_pending<F: Future + ?Sized>(mut future: Pin<&mut F>) -> io::Result<()> {
    poll_fn(|context| {
        Poll::Ready(match future.as_mut().poll(context) {
            Poll::Pending => Ok(()),
            Poll::Ready(_) => Err(failure(
                "native future completed before its positive cancellation/drop stage",
            )),
        })
    })
    .await
}

struct Checkpoint {
    reached: StageSender,
    release: oneshot::Receiver<()>,
    stage: Stage,
}

async fn checkpoint(
    checkpoint: Checkpoint,
    signals: &Signals,
    stats: &Stats,
    source: &RefCell<SourceProof>,
) -> io::Result<()> {
    let Checkpoint {
        reached,
        release,
        stage,
    } = checkpoint;
    reached.send(StagePoint {
        stage,
        ledger: *stats.ledger.borrow(),
        source: *source.borrow(),
        data_sent: signals.data_sent.load(Ordering::SeqCst),
        wire_written: signals.wire_written.load(Ordering::SeqCst),
        pressure_count: signals.pressure_count.load(Ordering::SeqCst),
    })?;
    release.await.map_err(failure)
}

async fn response_prefix(
    source: &mut HttpSourceHolder,
    route: Route,
    signals: &Signals,
) -> io::Result<usize> {
    signals.phase(NativePhase::Header);
    let mut header = source.read_header_event().await.map_err(failure)?;
    require(
        !header.eom() && header.headers_mut().status_code().map_err(failure)? == 200,
        "real nonterminal final response head",
    )?;
    require(
        header.headers().http_version() == (1, 1)
            && header.headers().advanced_protocol_string()
                == if route.h2 {
                    Some(b"h2".as_slice())
                } else {
                    None
                },
        "native response protocol metadata",
    )?;
    let mut consumed = 0;
    signals.phase(NativePhase::Body);
    while consumed < CHUNK {
        let event = source
            .read_body_event((CHUNK - consumed) as u32)
            .await
            .map_err(failure)?;
        require(!event.eom(), "response prefix precedes EOM")?;
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                for segment in body.segments() {
                    consumed = verify_bytes(segment, consumed, CHUNK)?;
                    signals.verified(consumed);
                }
            }
            HttpBodyEventKind::Suspend(resume) => {
                require(
                    resume.await == ResumeStatus::Signalled,
                    "prefix suspension resumes normally",
                )?;
            }
            _ => return Err(failure("unexpected event before exact response prefix")),
        }
    }
    require(consumed == CHUNK, "complete 16-KiB native-consumed prefix")?;
    Ok(consumed)
}

async fn response_suffix(
    source: &mut HttpSourceHolder,
    mut consumed: usize,
    signals: &Signals,
) -> io::Result<usize> {
    loop {
        signals.phase(if consumed == DOWNLOAD {
            NativePhase::Trailers
        } else {
            NativePhase::Body
        });
        let event = source
            .read_body_event(CHUNK as u32)
            .await
            .map_err(failure)?;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                require(
                    !eom && consumed < DOWNLOAD,
                    "body cannot replace the terminal trailers",
                )?;
                for segment in body.segments() {
                    consumed = verify_bytes(segment, consumed, DOWNLOAD)?;
                    signals.verified(consumed);
                }
            }
            HttpBodyEventKind::Trailers(trailers) => {
                require(
                    eom && consumed == DOWNLOAD
                        && trailers.len() == 1
                        && trailers.get_single_or_empty(b"x-native-terminal") == b"exact",
                    "complete 16-MiB body followed by exact terminal trailer",
                )?;
                if let Some(diagnostic) = &signals.diagnostic {
                    diagnostic.update(|state, elapsed| state.trailer_received = Some(elapsed));
                }
                return Ok(consumed);
            }
            HttpBodyEventKind::Suspend(resume) => {
                require(
                    resume.await == ResumeStatus::Signalled,
                    "body suspension resumes normally",
                )?;
            }
            _ => return Err(failure("unexpected response body event")),
        }
    }
}

struct NativeSourceRefs<'a> {
    proof: &'a Rc<RefCell<SourceProof>>,
    retained_suffix: &'a mut Option<oneshot::Sender<()>>,
}

async fn native_operation(
    session: &ClientSession,
    setup: NativeSetup,
    stats: &Stats,
    source_refs: NativeSourceRefs<'_>,
) -> io::Result<usize> {
    let NativeSourceRefs {
        proof,
        retained_suffix,
    } = source_refs;
    let NativeSetup {
        fixture,
        stage,
        signals,
        reached,
        release,
        ..
    } = setup;
    let (source, suffix) =
        upload_source(stage, fixture.route, Rc::clone(proof), Arc::clone(&signals))?;
    *retained_suffix = suffix;
    if matches!(stage, Stage::UploadCancel | Stage::FutureDrop) {
        let cancellation = CancellationSource::new();
        let mut request = SourceRequest::new(source);
        request.cancellation = cancellation.token();
        request.maximum_response_body = CHUNK;
        let mut response = HttpClient::request_source(session, request);
        let positive_stage = async {
            signals.wait(&signals.upload).await?;
            if stage == Stage::UploadCancel {
                signals.wait(&signals.upload_pending).await?;
            }
            Ok::<(), io::Error>(())
        };
        tokio::select! {
            biased;
            result = &mut response => return Err(failure(format!("native response completed before stage: {}", match result { Ok(_) => "success".to_owned(), Err(error) => error.to_string() }))),
            result = positive_stage => result?,
        }
        still_pending(response.as_mut()).await?;
        if stage == Stage::UploadCancel {
            require(
                proof.borrow().drops == 0 && proof.borrow().stops == 0,
                "unfinished upload ownership retained before action",
            )?;
            require(
                proof.borrow().live_reads > 0
                    && retained_suffix
                        .as_ref()
                        .is_some_and(|sender| !sender.is_closed()),
                "actual pending upload read retains unreleased suffix",
            )?;
        } else {
            require(
                proof.borrow().terminal == 1,
                "complete upload before pending response-future drop",
            )?;
        }
        checkpoint(
            Checkpoint {
                reached,
                release,
                stage,
            },
            &signals,
            stats,
            proof,
        )
        .await?;
        let action = Instant::now();
        signals.update_progress(|state| state.native_action = Some(action));
        signals.action.mark();
        if stage == Stage::UploadCancel {
            cancellation.request_cancellation();
            let error = response
                .await
                .err()
                .ok_or_else(|| failure("cancelled request succeeded"))?;
            require(
                error.code() == HttpErrorCode::CORO_CANCELLED,
                "real request token returns CORO_CANCELLED",
            )?;
        } else {
            drop(response);
        }
        return Ok(0);
    }
    require(
        retained_suffix.is_none(),
        "response-pressure upload has no suffix gate",
    )?;
    let mut response = session
        .send_request(source, CancellationToken::never())
        .await
        .map_err(failure)?;
    let completion = response.completion.clone();
    let mut consumed = response_prefix(&mut response.source, fixture.route, &signals).await?;
    signals.phase(NativePhase::Held);
    signals.held.mark();
    signals.wait(&signals.pressure).await?;
    signals
        .wait_for_buffered_ingress(fixture.route, consumed)
        .await?;
    stats
        .ledger
        .borrow_mut()
        .require_ingress(fixture.route, stage)?;
    require(
        stats.ledger.borrow().read > 0 && stats.ledger.borrow().read_positive > 0,
        "pressure includes actual native HTTP ingress, not merely peer application blocking",
    )?;
    require(
        signals.data_sent.load(Ordering::SeqCst) < DOWNLOAD,
        "genuine incomplete bounded response at pressure",
    )?;
    checkpoint(
        Checkpoint {
            reached,
            release,
            stage,
        },
        &signals,
        stats,
        proof,
    )
    .await?;
    let action = Instant::now();
    signals.update_progress(|state| state.native_action = Some(action));
    if let Some(diagnostic) = &signals.diagnostic {
        diagnostic.update(|state, elapsed| state.resumed = Some(elapsed));
    }
    signals.action.mark();
    if stage == Stage::Release {
        consumed = response_suffix(&mut response.source, consumed, &signals).await?;
    }
    drop(response);
    signals.phase(NativePhase::Completion);
    completion.wait().await.map_err(failure)?;
    completion.wait().await.map_err(failure)?;
    signals.phase(NativePhase::Complete);
    Ok(consumed)
}

fn h1_action_error(stage: Stage, error: &HttpError) -> bool {
    let (code, message): (_, &[u8]) = match stage {
        Stage::UploadCancel => (
            HttpErrorCode::CORO_CANCELLED,
            b"request body production cancelled",
        ),
        Stage::FutureDrop => (
            HttpErrorCode::TRANSPORT_READ_ERROR,
            b"byte-stream was reset",
        ),
        Stage::BodyDrop | Stage::Release => return false,
    };
    error.code() == code && error.message() == message && std::error::Error::source(error).is_none()
}

fn native_terminal(
    result: Result<roxygen_session::uniplex::UniplexRunSummary, HttpError>,
    route: Route,
    stage: Stage,
) -> io::Result<()> {
    if !route.h2 && matches!(stage, Stage::UploadCancel | Stage::FutureDrop) {
        let error = result.err().ok_or_else(|| {
            failure("H1 cancellation action did not retain its original native failure")
        })?;
        return require(
            h1_action_error(stage, &error),
            "H1 action retains its exact original native error",
        )
        .map_err(|mismatch| {
            failure(format!(
                "{mismatch}: stage={stage:?}; code={:?}; {error}; cause_present={}",
                error.code(),
                std::error::Error::source(&error).is_some()
            ))
        });
    }
    let summary = result.map_err(failure)?;
    require(
        summary.reset == (!route.h2 && stage == Stage::BodyDrop),
        "actual session reset policy",
    )?;
    require(
        summary.bytes_read > 0
            && summary.bytes_written > 0
            && summary.bytes_scheduled == summary.bytes_written,
        "native final scheduled/written byte accounting",
    )
    .map_err(|mismatch| {
        failure(format!(
            "{mismatch}: route={route:?}; stage={stage:?}; bytes_read={}; bytes_written={}; bytes_scheduled={}; reset={}; read_positive={}; written_positive={}; scheduled_matches_written={}",
            summary.bytes_read,
            summary.bytes_written,
            summary.bytes_scheduled,
            summary.reset,
            summary.bytes_read > 0,
            summary.bytes_written > 0,
            summary.bytes_scheduled == summary.bytes_written,
        ))
    })
}

async fn native_operation_bound(
    signals: &Signals,
    operation: impl Future<Output = io::Result<usize>>,
) -> io::Result<usize> {
    if let Some(diagnostic) = &signals.diagnostic {
        let deadline = tokio::time::Instant::now() + STAGE_BOUND;
        diagnostic
            .update(|state, elapsed| state.operation = Some((elapsed, elapsed + STAGE_BOUND)));
        let mut bounded = Box::pin(timeout_at(deadline, operation));
        let result = bounded.as_mut().await;
        if !matches!(&result, Ok(Ok(_))) {
            diagnostic.capture_before_cancel();
        }
        drop(bounded);
        result.map_err(failure)?
    } else {
        timeout(STAGE_BOUND, operation).await.map_err(failure)?
    }
}

fn native_session_params() -> SessionParams {
    SessionParams {
        settings: vec![CodecSetting {
            identifier: 4,
            value: STREAM_WINDOW.into(),
        }],
        connection_flow_control: Some(CONNECTION_WINDOW),
        max_concurrent_outgoing_streams: Some(1),
        stream_read_timeout: Some(STAGE_BOUND),
        connection_read_timeout: Some(STAGE_BOUND),
        write_timeout: Some(STAGE_BOUND),
        ..SessionParams::default()
    }
}

type NativeSessionClosed = Result<Result<UniplexRunSummary, HttpError>, Elapsed>;
type NativeTransportRetired = Result<Result<(), TransportRetirementError>, Elapsed>;

fn native_retirement_result(retired: Option<NativeTransportRetired>) -> io::Result<()> {
    retired
        .ok_or_else(|| failure("native connector retirement observation is required"))?
        .map_err(failure)?
        .map_err(failure)
}

fn retain_native_retirement(errors: &mut Vec<String>, retired: io::Result<()>) {
    retain(
        errors,
        "actual native transport value and close-scope retirement",
        retired,
    );
}

async fn native_finalization(
    session: &ClientSession,
    retirement: Option<&TransportRetirement>,
) -> (NativeSessionClosed, io::Result<()>) {
    let deadline = tokio::time::Instant::now() + STAGE_BOUND;
    tokio::join!(timeout_at(deadline, session.wait_closed()), async {
        let retired = match retirement {
            Some(retirement) => Some(timeout_at(deadline, retirement.wait_retired()).await),
            None => None,
        };
        native_retirement_result(retired)
    },)
}

async fn native(context: WorkerContext, setup: NativeSetup) -> io::Result<NativeObservation> {
    let owner = std::thread::current().id();
    let expected_context = setup.command_context.clone();
    require(
        context.is_owner_thread() && RuntimeContext::current() == Some(expected_context.clone()),
        "command factory/poll uses actual captured context and owner",
    )?;
    let signals = Arc::clone(&setup.signals);
    let fixture = setup.fixture.clone();
    let stage = setup.stage;
    let mut session_params = native_session_params();
    let connection_params = fixture.connection();
    let stats = Rc::new(Stats {
        owner,
        ledger: RefCell::new(ByteLedger::default()),
        signals: Arc::clone(&signals),
    });
    if IngressBound::selected(stage) {
        let bound =
            IngressBound::from_configuration(&session_params, &connection_params, fixture.route)?;
        stats
            .ledger
            .borrow_mut()
            .bind_native_ingress(bound, fixture.route, stage)?;
    }
    stats
        .ledger
        .borrow_mut()
        .require_ingress(fixture.route, stage)?;
    session_params.session_stats = Some(stats.clone());
    let session = HttpConnector::connect(
        &context,
        SocketAddress::new(setup.address.ip().to_string(), setup.address.port()),
        STAGE_BOUND,
        connection_params,
        session_params,
        CancellationToken::never(),
    )
    .await
    .map_err(failure)?;
    let controlled_peer = fixture.route.h2 && signals.diagnostic.is_none();
    let retirement = if signals.diagnostic.is_none() {
        session.transport_retirement()
    } else {
        None
    };
    if controlled_peer {
        let retirement = retirement
            .as_ref()
            .ok_or_else(|| failure("native connector retirement observation is required"))?;
        *signals.retirement.lock().map_err(failure)? = Some(retirement.clone());
        signals.retirement_published.mark();
    }
    signals.connected.mark();
    let connection = Arc::clone(session.connection_context());
    let proof = Rc::new(RefCell::new(SourceProof::default()));
    let mut retained_suffix = None;
    let operation = AssertUnwindSafe(async {
        require(
            session.protocol() == fixture.route.protocol()
                && session.worker_id() == context.worker_id(),
            "native connector protocol and worker identity",
        )?;
        require(
            connection.local_endpoint().socket_address() == Some(session.local_address())
                && connection.peer_endpoint().socket_address() == Some(session.peer_address()),
            "public session endpoint compatibility",
        )?;
        native_operation_bound(&signals, async {
            signals.wait(&signals.accepted_published).await?;
            verify_connection(&connection, &fixture, &signals)?;
            let bytes = native_operation(
                &session,
                setup,
                &stats,
                NativeSourceRefs {
                    proof: &proof,
                    retained_suffix: &mut retained_suffix,
                },
            )
            .await?;
            if controlled_peer {
                signals.native_operation_done.mark();
                signals.wait(&signals.peer_acknowledged).await?;
            }
            Ok(bytes)
        })
        .await
    })
    .catch_unwind()
    .await;
    let mut errors = Vec::new();
    let (response_bytes, panic) = match operation {
        Ok(Ok(bytes)) => (bytes, None),
        Ok(Err(error)) => {
            retain(&mut errors, "native operation", Err(error));
            (0, None)
        }
        Err(payload) => (0, Some(payload)),
    };
    let failed = !errors.is_empty() || panic.is_some();
    if failed {
        signals.capture_native_failure(*stats.ledger.borrow());
        signals.failed.mark();
        session.cancel();
    } else {
        if controlled_peer {
            signals.native_drain.mark();
        }
        retain(
            &mut errors,
            "native drain admission",
            session.initiate_drain().map(|_| ()).map_err(failure),
        );
    }
    let (closed, retired) = if signals.diagnostic.is_none() {
        let (closed, retired) = native_finalization(&session, retirement.as_ref()).await;
        (closed, Some(retired))
    } else {
        (timeout(STAGE_BOUND, session.wait_closed()).await, None)
    };
    match closed {
        Ok(result) if !failed => retain(
            &mut errors,
            "actual native session result",
            native_terminal(result, fixture.route, stage),
        ),
        Ok(result) => retain(
            &mut errors,
            "failure-path native session result",
            result.map(|_| ()).map_err(failure),
        ),
        Err(error) => {
            retain(
                &mut errors,
                "native session finalization deadline",
                Err(failure(error)),
            );
            signals.capture_native_failure(*stats.ledger.borrow());
            signals.failed.mark();
            session.cancel();
        }
    }
    retain(
        &mut errors,
        "finished native transactions",
        require(
            session.is_finished() && session.active_transactions() == 0,
            "session and transaction finalization",
        ),
    );
    if let Some(diagnostic) = &signals.diagnostic {
        diagnostic.update(|state, _| {
            state.native_finished = Some((session.is_finished(), session.active_transactions()))
        });
    }
    retain(
        &mut errors,
        "retained trusted context",
        verify_connection(&connection, &fixture, &signals),
    );
    retain(
        &mut errors,
        "immutable session snapshot",
        require(
            session.connection_context().as_ref() == connection.as_ref(),
            "headers/actions cannot mutate trusted connection metadata",
        ),
    );
    drop(session);
    if let Some(retired) = retired {
        retain_native_retirement(&mut errors, retired);
    }
    if let Some(suffix) = retained_suffix {
        retain(
            &mut errors,
            "unreleased upload gate",
            require(
                suffix.is_closed(),
                "suffix receiver retired without releasing its sender",
            ),
        );
        drop(suffix);
    }
    retain(
        &mut errors,
        "command context after suspensions and cleanup",
        require(
            RuntimeContext::current() == Some(expected_context.clone())
                && std::thread::current().id() == owner,
            "command context restored on each worker poll",
        ),
    );
    let source = *proof.borrow();
    retain(
        &mut errors,
        "actual source/read retirement",
        require(
            source.constructed == 1
                && source.polled > 0
                && source.drops == 1
                && source.state_drops == 1
                && source.live_reads == 0
                && source.read_drops > 0
                && !source.wrong_owner
                && !source.premature,
            "source/read cleanup occurred exactly once on native owner after action",
        ),
    );
    retain(
        &mut errors,
        "source STOP and terminal policy",
        require(
            source.stops == usize::from(stage == Stage::UploadCancel)
                && source.terminal == usize::from(stage != Stage::UploadCancel),
            "unfinished upload STOP once; completed uploads have no STOP",
        ),
    );
    retain(
        &mut errors,
        "source-bound native BODY-credit retirement",
        stats
            .ledger
            .borrow_mut()
            .require_ingress(fixture.route, stage),
    );
    let ledger = *stats.ledger.borrow();
    retain(
        &mut errors,
        "actual logical HTTP ledger retirement",
        require(
            !ledger.invalid
                && ledger.read == 0
                && ledger.write == 0
                && ledger.opened == 1
                && ledger.closed == 1
                && ledger.write_positive > 0,
            "checked HTTP byte deltas and transaction balances retire",
        ),
    );
    if !errors.is_empty() {
        signals.failed.mark();
    }
    if let Some(payload) = panic {
        eprintln!("native staged cleanup after panic: {errors:?}");
        resume_unwind(payload);
    }
    finish(errors)?;
    Ok(NativeObservation {
        connection,
        ledger,
        source,
        owner,
        response_bytes,
        command_context: expected_context,
    })
}

#[derive(Clone, Copy, Debug)]
struct Resources {
    descriptors: usize,
    threads: usize,
    rss_bytes: u64,
    process_hwm_bytes: u64,
}

fn directory_count(path: &'static str) -> io::Result<usize> {
    fs::read_dir(path)?.try_fold(0, |count, entry| {
        let entry = entry?;
        drop(entry);
        require(count < 4096, "bounded process resource enumeration")?;
        Ok(count + 1)
    })
}

fn resources() -> io::Result<Resources> {
    require(
        tokio::runtime::Handle::try_current().is_err(),
        "process sampling occurs outside async execution",
    )?;
    let mut status = String::new();
    Read::take(fs::File::open("/proc/self/status")?, PROC_BOUND + 1).read_to_string(&mut status)?;
    require(
        status.len() as u64 <= PROC_BOUND,
        "bounded numeric process status",
    )?;
    let field = |name: &str| -> io::Result<u64> {
        let line = status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or_else(|| failure("required actual RSS field is missing"))?;
        let mut parts = line.split_whitespace();
        let value: u64 = parts
            .next()
            .ok_or_else(|| failure("missing RSS value"))?
            .parse()
            .map_err(failure)?;
        require(
            parts.next() == Some("kB") && parts.next().is_none(),
            "actual Linux RSS units",
        )?;
        value
            .checked_mul(1024)
            .ok_or_else(|| failure("RSS conversion overflow"))
    };
    let rss_bytes = field("VmRSS:")?;
    let process_hwm_bytes = field("VmHWM:")?;
    require(
        rss_bytes > 0 && process_hwm_bytes >= rss_bytes,
        "positive current RSS and process-lifetime high water",
    )?;
    drop(status);
    let threads = directory_count("/proc/self/task")?;
    let descriptors = directory_count("/proc/self/fd")?
        .checked_sub(1)
        .ok_or_else(|| failure("missing sampler descriptor"))?;
    require(
        threads > 0 && descriptors > 0,
        "positive alive-process resource counts",
    )?;
    Ok(Resources {
        descriptors,
        threads,
        rss_bytes,
        process_hwm_bytes,
    })
}

fn resource_bound(actual: Resources, baseline: Resources) -> io::Result<()> {
    require(
        actual.descriptors <= baseline.descriptors && actual.threads <= baseline.threads,
        "actual post-cleanup FD/thread counts exceed the fixed warmed baseline",
    )
}

fn resources_after_join(baseline: Resources) -> io::Result<Resources> {
    let deadline = Instant::now() + STAGE_BOUND;
    loop {
        let sample = resources()?;
        if sample.descriptors > baseline.descriptors
            || sample.threads <= baseline.threads
            || Instant::now() >= deadline
        {
            return Ok(sample);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

struct ResourcePoint {
    route: Route,
    round: usize,
    stage: Stage,
}

fn log_resources(label: &str, point: ResourcePoint, sample: Resources) {
    let ResourcePoint {
        route,
        round,
        stage,
    } = point;
    eprintln!(
        "native_staged {label} route={route:?} round={round} stage={stage:?} fd={} threads={} rss_bytes={} process_lifetime_hwm_bytes={}",
        sample.descriptors, sample.threads, sample.rss_bytes, sample.process_hwm_bytes
    );
}

struct Environment {
    runtime: Runtime,
    listener: TcpListener,
    address: SocketAddr,
    fixtures: Vec<RouteFixture>,
    started: Instant,
}

impl Environment {
    fn new() -> io::Result<Self> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0"))?;
        let address = listener.local_addr()?;
        let fixtures = ROUTES
            .into_iter()
            .map(RouteFixture::new)
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            runtime,
            listener,
            address,
            fixtures,
            started: Instant::now(),
        })
    }

    fn for_route(route: Route) -> io::Result<Self> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0"))?;
        let address = listener.local_addr()?;
        Ok(Self {
            runtime,
            listener,
            address,
            fixtures: vec![RouteFixture::new(route)?],
            started: Instant::now(),
        })
    }
}

struct CaseObservation {
    native: NativeObservation,
    stage: StagePoint,
    pressure_resources: Resources,
    retired_resources: Resources,
}

fn verify_stage(mut point: StagePoint, route: Route) -> io::Result<()> {
    point.ledger.require_ingress(route, point.stage)?;
    require(
        !point.ledger.invalid && !point.source.wrong_owner && !point.source.premature,
        "owner-local stage ledger is valid before action",
    )?;
    match point.stage {
        Stage::UploadCancel => require(
            point.source.live_reads > 0
                && point.source.terminal == 0
                && point.source.stops == 0
                && point.source.drops == 0,
            "positive pending unfinished upload stage",
        ),
        Stage::FutureDrop => require(
            point.source.terminal == 1,
            "positive completed-upload/pending-response stage",
        ),
        Stage::BodyDrop | Stage::Release => {
            require(
                point.ledger.read > 0
                    && point.ledger.read_positive > 0
                    && point.data_sent >= CHUNK
                    && point.data_sent < DOWNLOAD
                    && point.wire_written > 0
                    && point.pressure_count > 0,
                "positive actual I/O pressure and logical native ingress",
            )?;
            if !route.h2 {
                require(
                    point
                        .ledger
                        .read_received
                        .checked_sub(CHUNK)
                        .is_some_and(|buffered| {
                            buffered >= UniplexConfig::default().ingress_buffer_limit as usize
                        }),
                    "H1 held body reached the actual native ingress-pause threshold after consumed prefix",
                )?;
            }
            eprintln!(
                "native_staged pressure={}; configured_h2_stream_window={:?}; configured_h2_connection_window={:?}; native_connector_read_write_limits=262144; HTTP_callback_ledger={:?}; peer_data_sent={}; peer_wire_written={}",
                if route.h2 {
                    "h2_poll_capacity_Pending_after_DATA"
                } else {
                    "socket_write_WouldBlock_after_progress"
                },
                route.h2.then_some(STREAM_WINDOW),
                route.h2.then_some(CONNECTION_WINDOW),
                point.ledger,
                point.data_sent,
                point.wire_written
            );
            Ok(())
        }
    }
}

struct CaseSpec {
    stage: Stage,
    round: usize,
    remaining: Duration,
}

fn run_case(
    environment: &Environment,
    worker: &WorkerHandle,
    fixture: &RouteFixture,
    spec: CaseSpec,
) -> io::Result<CaseObservation> {
    let CaseSpec {
        stage,
        round,
        remaining,
    } = spec;
    require(
        environment.started.elapsed() < WATCHDOG_BOUND,
        "fixed whole-fixture watchdog budget",
    )?;
    let caller_context = RuntimeContext::current();
    let command_context = RuntimeContext::new();
    let signals = Arc::new(Signals {
        observe_h2: fixture.route.h2,
        ..Signals::default()
    });
    let (reached, mut observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let setup = NativeSetup {
        fixture: fixture.clone(),
        stage,
        address: environment.address,
        signals: Arc::clone(&signals),
        reached: StageSender::Original(reached),
        release: released,
        command_context: command_context.clone(),
    };
    let checked_native = if fixture.route.h2 {
        let (publication, checked) = oneshot::channel();
        *signals.checked_native.lock().map_err(failure)? = Some(checked);
        Some(publication)
    } else {
        None
    };
    let call = {
        let _command = command_context.enter();
        let failed = Arc::clone(&signals);
        worker
            .call_local(move |context| async move {
                let result = match checked_native {
                    Some(publication) => {
                        match AssertUnwindSafe(publish_checked_native(
                            native(context, setup),
                            publication,
                        ))
                        .catch_unwind()
                        .await
                        {
                            Ok(result) => result,
                            Err(payload) => {
                                failed.failed.mark();
                                resume_unwind(payload)
                            }
                        }
                    }
                    None => native(context, setup).await,
                };
                if result.is_err() {
                    failed.failed.mark();
                }
                result
            })
            .map_err(failure)?
    };
    require(
        RuntimeContext::current() == caller_context,
        "command submission restores caller context",
    )?;
    let mut call = Box::pin(call);
    if !fixture.route.tls {
        environment.runtime.block_on(async {
            timeout(STAGE_BOUND, signals.wait(&signals.connected))
                .await
                .map_err(failure)??;
            require(
                !signals.accepted_published.marked()
                    && signals.accepted.lock().map_err(failure)?.is_none(),
                "actual native TCP connect precedes deliberately unpolled independent accept",
            )?;
            still_pending(call.as_mut()).await
        })?;
    }
    let peer = peer(&environment.listener, fixture, stage, Arc::clone(&signals));
    let mut both = Box::pin(async { timeout(remaining, async { tokio::join!(call, peer) }).await });
    let first = environment.runtime.block_on(async {
        tokio::select! {
            biased;
            result = &mut both => Err(result),
            point = &mut observed => Ok(point),
        }
    });
    let mut errors = Vec::new();
    let mut measured = None;
    let completed = match first {
        Ok(Ok(point)) => {
            retain(
                &mut errors,
                "stage evidence",
                verify_stage(point, fixture.route),
            );
            match resources() {
                Ok(sample) => {
                    log_resources(
                        "R2-positive",
                        ResourcePoint {
                            route: fixture.route,
                            round,
                            stage,
                        },
                        sample,
                    );
                    measured = Some((point, sample));
                }
                Err(error) => retain(&mut errors, "positive-stage resources", Err(error)),
            }
            if !errors.is_empty() {
                signals.failed.mark();
            }
            let released_at = Instant::now();
            let released = release.send(());
            if released.is_ok() {
                signals.update_progress(|state| state.root_release = Some(released_at));
            }
            retain(
                &mut errors,
                "root releases already-observed action",
                released.map_err(|_| failure("native stage owner disappeared")),
            );
            environment.runtime.block_on(both)
        }
        Ok(Err(error)) => {
            retain(
                &mut errors,
                "missing native positive stage",
                Err(failure(error)),
            );
            signals.failed.mark();
            drop(release);
            environment.runtime.block_on(both)
        }
        Err(completed) => {
            retain(
                &mut errors,
                "native stage",
                Err(failure("operation ended without positive stage")),
            );
            drop(release);
            completed
        }
    };
    let completed = match completed {
        Ok(completed) => completed,
        Err(error) => {
            signals.log_progress(ResourcePoint {
                route: fixture.route,
                round,
                stage,
            });
            signals.failed.mark();
            retain(
                &mut errors,
                "absolute route deadline; runtime owner still must quiesce and physically join",
                Err(failure(error)),
            );
            return Err(failure(errors.join("; ")));
        }
    };
    let observation = match completed.0 {
        Ok(Ok(observation)) => Some(observation),
        Ok(Err(error)) => {
            retain(&mut errors, "native operation and cleanup", Err(error));
            None
        }
        Err(error) => {
            retain(
                &mut errors,
                "actual worker command completion",
                Err(failure(error)),
            );
            None
        }
    };
    retain(
        &mut errors,
        "actual independent peer and H2 driver retirement",
        completed.1,
    );
    retain(
        &mut errors,
        "peer owner terminal marker",
        require(signals.peer_done.marked(), "peer future actually retired"),
    );
    retain(
        &mut errors,
        "caller context after native/peer polling",
        require(
            RuntimeContext::current() == caller_context,
            "worker command does not overwrite caller ambient context",
        ),
    );
    let retired = resources();
    if let Ok(sample) = retired.as_ref() {
        log_resources(
            "R3-retired",
            ResourcePoint {
                route: fixture.route,
                round,
                stage,
            },
            *sample,
        );
    }
    if let Err(error) = retired.as_ref() {
        retain(&mut errors, "actual retired resources", Err(failure(error)));
    }
    if !errors.is_empty() {
        signals.log_progress(ResourcePoint {
            route: fixture.route,
            round,
            stage,
        });
    }
    finish(errors)?;
    let retired_resources = retired?;
    let native = observation.ok_or_else(|| failure("missing native result"))?;
    let (point, pressure_resources) =
        measured.ok_or_else(|| failure("missing actual stage resource sample"))?;
    require(
        native.owner == worker.owner_thread(),
        "actual source worker identity",
    )?;
    require(
        native.response_bytes
            == match stage {
                Stage::UploadCancel | Stage::FutureDrop => 0,
                Stage::BodyDrop => CHUNK,
                Stage::Release => DOWNLOAD,
            },
        "exact consumed body at each stage",
    )?;
    verify_connection(&native.connection, fixture, &signals)?;
    eprintln!(
        "native_staged retired route={:?} round={round} stage={stage:?} source={:?} stop_error={:?} HTTP_callback_ledger={:?} consumed={}",
        fixture.route,
        native.source,
        native.source.stop_error,
        native.ledger,
        native.response_bytes
    );
    Ok(CaseObservation {
        native,
        stage: point,
        pressure_resources,
        retired_resources,
    })
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Schedule {
    Stages,
    Context,
    InfrastructureWarmup,
    Resources,
}

#[derive(Default)]
struct RootRetirement {
    errors: Vec<String>,
    first_panic: Option<Box<dyn Any + Send>>,
    cleanup_panics: usize,
}

impl RootRetirement {
    fn record_cleanup_panic(&mut self, payload: Box<dyn Any + Send>) {
        self.cleanup_panics += 1;
        if self.first_panic.is_none() {
            self.first_panic = Some(payload);
        } else if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    }
}

fn retire_runtime<Observe, Observed>(
    runtime: &Runtime,
    owner: &mut RuntimeOwner,
    retirement: &mut RootRetirement,
    mut observe: Observe,
) -> Option<ShutdownReport>
where
    Observe: FnMut(QuiesceFuture) -> Observed,
    Observed: Future<Output = Result<(), RuntimeError>>,
{
    let handle = owner.handle();
    for _attempt in 0..2 {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let quiescence = handle.quiesce(QUIESCE_BOUND);
            runtime.block_on(observe(quiescence))
        }));
        match result {
            Ok(result) => {
                retain(
                    &mut retirement.errors,
                    "actual async worker quiescence",
                    result.map_err(failure),
                );
                break;
            }
            Err(payload) => retirement.record_cleanup_panic(payload),
        }
    }
    match catch_unwind(AssertUnwindSafe(|| owner.join())) {
        Ok(Ok(report)) => {
            retain(
                &mut retirement.errors,
                "physical native worker join report",
                require(
                    report.workers_joined == 1 && report.workers_forced == 0,
                    "one actual worker joined with zero forced cleanup",
                ),
            );
            Some(report)
        }
        Ok(Err(error)) => {
            retain(
                &mut retirement.errors,
                "physical native worker join",
                Err(failure(error)),
            );
            None
        }
        Err(payload) => {
            retirement.record_cleanup_panic(payload);
            None
        }
    }
}

fn run_route(
    environment: &Environment,
    fixture: &RouteFixture,
    schedule: Schedule,
) -> io::Result<()> {
    run_route_observed(environment, fixture, schedule, |_| Ok(()))
}

fn run_route_observed<Observed>(
    environment: &Environment,
    fixture: &RouteFixture,
    schedule: Schedule,
    observe: impl FnOnce(&[CaseObservation]) -> io::Result<Observed>,
) -> io::Result<Observed> {
    let baseline = resources()?;
    log_resources(
        "R0-before-native-worker",
        ResourcePoint {
            route: fixture.route,
            round: 0,
            stage: Stage::Release,
        },
        baseline,
    );
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 64,
    })
    .map_err(failure)?;
    let handle = owner.handle();
    let Some(worker) = handle.worker(0) else {
        let mut retirement = RootRetirement {
            errors: vec!["configured native worker missing".to_owned()],
            ..RootRetirement::default()
        };
        retire_runtime(
            &environment.runtime,
            &mut owner,
            &mut retirement,
            |observer| observer,
        );
        if let Some(payload) = retirement.first_panic {
            eprintln!("missing-worker cleanup: {:?}", retirement.errors);
            resume_unwind(payload);
        }
        finish(retirement.errors)?;
        return Err(failure("configured native worker missing after retirement"));
    };
    let ambient = RuntimeContext::new();
    let _ambient = ambient.enter();
    let mut observations = Vec::with_capacity(40);
    let mut errors = Vec::new();
    let operation = catch_unwind(AssertUnwindSafe(|| -> io::Result<()> {
        let stages: &[Stage] = match schedule {
            Schedule::Stages | Schedule::Resources => &STAGES,
            Schedule::Context => &[Stage::UploadCancel, Stage::Release],
            Schedule::InfrastructureWarmup => &[Stage::Release],
        };
        let rounds = if schedule == Schedule::Resources {
            10
        } else {
            1
        };
        let started = Instant::now();
        let mut warmed = None;
        for round in 0..rounds {
            for stage in stages.iter().copied() {
                let remaining = ROUTE_BOUND
                    .checked_sub(started.elapsed())
                    .ok_or_else(|| failure("fixed whole-route budget"))?;
                let observation = run_case(
                    environment,
                    &worker,
                    fixture,
                    CaseSpec {
                        stage,
                        round,
                        remaining,
                    },
                )?;
                let retired = observation.retired_resources;
                observations.push(observation);
                if let Some(baseline) = warmed {
                    resource_bound(retired, baseline)?;
                }
            }
            if schedule == Schedule::Resources && round == 1 {
                let baseline = resources()?;
                log_resources(
                    "R1-fixed-two-round-warmup",
                    ResourcePoint {
                        route: fixture.route,
                        round,
                        stage: Stage::Release,
                    },
                    baseline,
                );
                for observation in &observations {
                    resource_bound(observation.retired_resources, baseline)?;
                }
                warmed = Some(baseline);
            }
        }
        require(
            started.elapsed() < ROUTE_BOUND,
            "fixed whole-route budget includes synchronous checkpoints",
        )?;
        require(
            RuntimeContext::current() == Some(ambient.clone()),
            "distinct command contexts restore caller ambient context",
        )?;
        require(
            observations.len() == rounds * stages.len(),
            "entire predeclared schedule retained",
        )?;
        require(
            observations
                .windows(2)
                .all(|pair| pair[0].native.command_context != pair[1].native.command_context),
            "successive native commands use distinct public runtime contexts",
        )?;
        for observation in &observations {
            require(
                observation.native.command_context != ambient,
                "command context is distinct from caller context",
            )?;
            require(
                observation.native.connection.security()
                    == if fixture.route.tls {
                        TransportSecurity::Tls
                    } else {
                        TransportSecurity::Plaintext
                    },
                "retained connection snapshot after later healthy control",
            )?;
            require(
                observation.native.connection.peer_identity() == fixture.certificate.as_deref()
                    && observation.native.connection.upstream_origin().is_none(),
                "retained identity remains trusted after close and later command",
            )?;
            require(
                observation.stage.stage != Stage::Release
                    || observation.native.response_bytes == DOWNLOAD,
                "each retained release control completed",
            )?;
            require(
                observation.pressure_resources.rss_bytes > 0,
                "every positive stage retains actual RSS",
            )?;
        }
        require(
            worker.queued_commands() == 0,
            "no queued command envelopes after completed schedule",
        )
    }));
    let panic = match operation {
        Ok(result) => {
            retain(&mut errors, "native route", result);
            None
        }
        Err(payload) => Some(payload),
    };
    let mut retirement = RootRetirement {
        errors,
        first_panic: panic,
        cleanup_panics: 0,
    };
    retire_runtime(
        &environment.runtime,
        &mut owner,
        &mut retirement,
        |observer| observer,
    );
    let RootRetirement {
        mut errors,
        first_panic: panic,
        ..
    } = retirement;
    let joined_resources = if schedule == Schedule::Resources {
        resources_after_join(baseline)
    } else {
        resources()
    };
    match joined_resources {
        Ok(sample) => {
            log_resources(
                "R4-physical-join-handles-alive",
                ResourcePoint {
                    route: fixture.route,
                    round: 0,
                    stage: Stage::Release,
                },
                sample,
            );
            if schedule == Schedule::Resources {
                retain(
                    &mut errors,
                    "post-join resources",
                    resource_bound(sample, baseline),
                );
            }
        }
        Err(error) => retain(&mut errors, "actual post-join resources", Err(error)),
    }
    drop(worker);
    drop(handle);
    drop(owner);
    match resources() {
        Ok(sample) => {
            log_resources(
                "R5-root-and-snapshots-alive",
                ResourcePoint {
                    route: fixture.route,
                    round: 0,
                    stage: Stage::Release,
                },
                sample,
            );
            if schedule == Schedule::Resources {
                retain(
                    &mut errors,
                    "post-handle resources",
                    resource_bound(sample, baseline),
                );
            }
        }
        Err(error) => retain(&mut errors, "actual post-handle resources", Err(error)),
    }
    require(observations.len() <= 40, "bounded retained cycle ledger")?;
    if let Some(payload) = panic {
        eprintln!("native route cleanup after panic: {errors:?}");
        resume_unwind(payload);
    }
    finish(errors)?;
    observe(&observations)
}

fn run(schedule: Schedule) -> io::Result<()> {
    let _serial = SERIAL.lock().map_err(failure)?;
    let environment = Environment::new()?;
    if schedule == Schedule::Resources {
        for fixture in &environment.fixtures {
            run_route(&environment, fixture, Schedule::InfrastructureWarmup)?;
        }
    }
    for fixture in &environment.fixtures {
        run_route(&environment, fixture, schedule)?;
    }
    Ok(())
}

struct ReferenceSetup {
    address: SocketAddr,
    signals: Arc<Signals>,
    diagnostic: Arc<ReferenceDiagnostic>,
    reached: oneshot::Sender<ReferenceCheckpoint>,
    release: oneshot::Receiver<()>,
    command_context: RuntimeContext,
}

async fn reference_data(
    body: &mut RecvStream,
    consumed: usize,
    limit: usize,
    signals: &Signals,
) -> io::Result<usize> {
    let diagnostic = signals
        .diagnostic
        .as_ref()
        .expect("reference case observation");
    let data = body
        .data()
        .await
        .ok_or_else(|| failure("reference response ended before exact payload"))?
        .map_err(io::Error::other)?;
    let consumed = verify_bytes(&data, consumed, limit)?;
    let amount = data.len();
    drop(data);
    signals.verified(consumed);
    body.flow_control()
        .release_capacity(amount)
        .map_err(io::Error::other)?;
    diagnostic.update(|state, _| state.credit_released += amount);
    diagnostic.sample_used(body.flow_control().used_capacity());
    Ok(consumed)
}

async fn reference_request(
    mut sender: h2::client::SendRequest<Bytes>,
    flow: Rc<RefCell<Option<h2::FlowControl>>>,
    setup: ReferenceSetup,
) -> io::Result<()> {
    let ReferenceSetup {
        signals,
        diagnostic,
        reached,
        release,
        ..
    } = setup;
    signals.wait(&signals.accepted_published).await?;
    let endpoints = diagnostic.snapshot();
    require(
        endpoints
            .outgoing_endpoints
            .is_some_and(|(local, remote)| endpoints.accepted_endpoints == Some((remote, local))),
        "reference actual endpoints match independent accept, not request authority",
    )?;
    poll_fn(|context| sender.poll_ready(context))
        .await
        .map_err(io::Error::other)?;
    let request = http::Request::builder()
        .method(http::Method::POST)
        .version(http::Version::HTTP_2)
        .uri(format!(
            "http://{}/native-staged",
            Stage::Release.authority()
        ))
        .header("host", Stage::Release.authority())
        .header("content-length", UPLOAD.to_string())
        .header(
            "forwarded",
            "for=192.0.2.9;proto=https;host=untrusted.invalid",
        )
        .header("x-forwarded-proto", "http")
        .header("x-client-cert", "not-a-transport-identity")
        .body(())
        .map_err(failure)?;
    let (response, mut upload) = sender
        .send_request(request, false)
        .map_err(io::Error::other)?;
    for offset in (0..UPLOAD).step_by(CHUNK) {
        upload
            .send_data(pattern(offset, CHUNK), offset + CHUNK == UPLOAD)
            .map_err(io::Error::other)?;
    }
    drop(upload);
    drop(sender);
    let response = response.await.map_err(io::Error::other)?;
    require(
        response.status() == 200 && response.version() == http::Version::HTTP_2,
        "actual reference H2 200 response",
    )?;
    let mut body = response.into_body();
    require(
        !body.is_end_stream(),
        "reference response is nonterminal before prefix",
    )?;
    *flow.borrow_mut() = Some(body.flow_control().clone());
    let mut consumed = 0;
    while consumed < CHUNK {
        consumed = reference_data(&mut body, consumed, CHUNK, &signals).await?;
    }
    signals.phase(NativePhase::Held);
    signals.held.mark();
    signals.wait(&signals.pressure).await?;
    let used = loop {
        let changed = diagnostic.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let used = diagnostic.sample_used(body.flow_control().used_capacity());
        if used > 0 {
            break used;
        }
        tokio::select! {
            biased;
            _ = signals.failed.wait() => return Err(failure("reference held stage cancelled after owned failure")),
            _ = changed => {}
        }
    };
    require(
        signals.data_sent.load(Ordering::SeqCst) < DOWNLOAD,
        "reference pressure precedes complete producer",
    )?;
    let released = diagnostic.snapshot().credit_released;
    reached
        .send(ReferenceCheckpoint::Independent {
            consumed,
            released,
            used,
        })
        .map_err(|_| failure("reference checkpoint observer disappeared"))?;
    release.await.map_err(failure)?;
    diagnostic.update(|state, elapsed| state.resumed = Some(elapsed));
    signals.action.mark();
    while consumed < DOWNLOAD {
        consumed = reference_data(&mut body, consumed, DOWNLOAD, &signals).await?;
    }
    match body.data().await {
        None => {}
        Some(Ok(_)) => {
            return Err(failure(
                "reference body has no extra DATA after exact length",
            ));
        }
        Some(Err(error)) => return Err(io::Error::other(error)),
    }
    let trailers = body
        .trailers()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| failure("reference response missing terminal trailers"))?;
    require(
        trailers.len() == 1
            && trailers.get("x-native-terminal") == Some(&HeaderValue::from_static("exact")),
        "exact reference terminal trailer",
    )?;
    require(body.is_end_stream(), "reference stream really reached EOM")?;
    diagnostic.update(|state, elapsed| {
        state.trailer_received = Some(elapsed);
        state.body_complete = Some(elapsed);
    });
    drop(body);
    Ok(())
}

async fn reference_client(context: WorkerContext, setup: ReferenceSetup) -> io::Result<()> {
    let expected_context = setup.command_context.clone();
    require(
        context.is_owner_thread() && RuntimeContext::current() == Some(expected_context.clone()),
        "reference driver remains on actual command owner/context",
    )?;
    let owner = std::thread::current().id();
    let signals = Arc::clone(&setup.signals);
    let diagnostic = Arc::clone(&setup.diagnostic);
    let socket = timeout(STAGE_BOUND, TcpStream::connect(setup.address))
        .await
        .map_err(failure)??;
    let deadline = tokio::time::Instant::now() + STAGE_BOUND;
    diagnostic.update(|state, elapsed| state.operation = Some((elapsed, elapsed + STAGE_BOUND)));
    socket.set_nodelay(true)?;
    let no_delay = socket.nodelay()?;
    diagnostic.update(|state, elapsed| state.outgoing_option = Some((no_delay, elapsed)));
    let endpoints = (socket.local_addr()?, socket.peer_addr()?);
    diagnostic.update(|state, _| state.outgoing_endpoints = Some(endpoints));
    require(no_delay, "actual reference outgoing NODELAY true")?;
    signals.connected.mark();
    let socket = ReferenceIo::new(socket, Arc::clone(&diagnostic), ReferenceEndpoint::Client);
    let mut builder = h2::client::Builder::new();
    builder
        .initial_window_size(STREAM_WINDOW)
        .initial_connection_window_size(CONNECTION_WINDOW);
    let (sender, connection) = reference_timeout(
        timeout_at(deadline, builder.handshake::<_, Bytes>(socket)),
        &diagnostic,
    )
    .await
    .map_err(io::Error::other)?
    .map_err(io::Error::other)?;
    let flow = Rc::new(RefCell::new(None::<h2::FlowControl>));
    let mut connection = Box::pin(connection);
    let mut driver = Box::pin(
        AssertUnwindSafe(poll_fn(|context| {
            let result = connection.as_mut().poll(context);
            diagnostic.update(|state, elapsed| {
                state
                    .driver_polls
                    .record(state.driver_polls.count + 1, elapsed)
            });
            let used = flow.borrow().as_ref().map(h2::FlowControl::used_capacity);
            if let Some(used) = used {
                diagnostic.sample_used(used);
            }
            result
        }))
        .catch_unwind(),
    );
    let mut operation = Box::pin(AssertUnwindSafe(timeout_at(deadline, async {
        tokio::select! {
            biased;
            result = reference_request(sender, Rc::clone(&flow), setup) => result,
            _ = signals.failed.wait() => Err(failure("reference operation cancelled after owned failure")),
        }
    })).catch_unwind());
    let mut driver_result = None;
    let operation_result = tokio::select! {
        biased;
        result = &mut driver => {
            if !matches!(&result, Ok(Ok(()))) {
                diagnostic.capture_before_cancel();
                signals.failed.mark();
            }
            driver_result = Some(result);
            operation.as_mut().await
        }
        result = &mut operation => result,
    };
    let driver_first = driver_result.is_some();
    if !matches!(&operation_result, Ok(Ok(Ok(())))) {
        diagnostic.capture_before_cancel();
        signals.failed.mark();
    }
    drop(operation);
    drop(flow.borrow_mut().take());
    diagnostic.update(|state, elapsed| state.handles_released = Some(elapsed));
    if driver_result.is_none() {
        match timeout(STAGE_BOUND, &mut driver).await {
            Ok(result) => driver_result = Some(result),
            Err(_) => {
                diagnostic.capture_before_cancel();
                signals.failed.mark();
            }
        }
    }
    drop(driver);
    drop(connection);
    let (terminal, driver_error, driver_panic) = match driver_result {
        Some(Ok(Ok(()))) => (ChildOutcome::ReturnedOk, None, None),
        Some(Ok(Err(error))) => (
            ChildOutcome::ReturnedError,
            Some(io::Error::other(error)),
            None,
        ),
        Some(Err(payload)) => (ChildOutcome::FutureDropped, None, Some(payload)),
        None => (
            ChildOutcome::FutureDropped,
            Some(failure(
                "reference driver retirement deadline; future disposed, not joined success",
            )),
            None,
        ),
    };
    diagnostic.update(|state, elapsed| state.driver = Some((terminal, elapsed)));
    let (operation_error, operation_panic) = match operation_result {
        Ok(Ok(Ok(()))) => (None, None),
        Ok(Ok(Err(error))) => (Some(error), None),
        Ok(Err(error)) => (Some(failure(error)), None),
        Err(payload) => (None, Some(payload)),
    };
    let ordered = if driver_first {
        [
            (driver_error, driver_panic),
            (operation_error, operation_panic),
        ]
    } else {
        [
            (operation_error, operation_panic),
            (driver_error, driver_panic),
        ]
    };
    let mut failures = ReferenceFailures::default();
    let mut panic = None;
    for (error, payload) in ordered {
        if let Some(error) = error {
            failures.errors.push(error);
        }
        if let Some(payload) = payload {
            retain_reference_panic(&mut panic, payload);
        }
    }
    if let Err(error) = require(
        std::thread::current().id() == owner
            && context.is_owner_thread()
            && RuntimeContext::current() == Some(expected_context),
        "reference handles and actual driver retired on owner/context",
    ) {
        failures.errors.push(error);
    }
    if let Some(payload) = panic {
        eprintln!("reference driver cleanup before original panic: {failures:?}");
        resume_unwind(payload);
    }
    if !failures.errors.is_empty() {
        return Err(io::Error::other(failures));
    }
    Ok(())
}

#[derive(Debug, Default, thiserror::Error)]
#[error("fixed reference-control failures: {errors:?}; incomplete_cleanup={incomplete}")]
struct ReferenceFailures {
    errors: Vec<io::Error>,
    incomplete: bool,
}

fn retain_reference_panic(first: &mut Option<Box<dyn Any + Send>>, payload: Box<dyn Any + Send>) {
    if first.is_none() {
        *first = Some(payload);
    } else if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

struct ReferenceCaseResult {
    failures: ReferenceFailures,
    panic: Option<Box<dyn Any + Send>>,
    cleanup_confirmed: bool,
}

fn verify_reference_checkpoint(point: ReferenceCheckpoint, signals: &Signals) -> io::Result<()> {
    match point {
        ReferenceCheckpoint::Native(point) => verify_stage(point, ROUTES[1])?,
        ReferenceCheckpoint::Independent {
            consumed,
            released,
            used,
        } => {
            require(
                consumed == CHUNK && released == CHUNK && used > 0,
                "reference consumed prefix, released exact consumed credit and retains real used capacity",
            )?;
            require(
                signals.held.marked() && signals.pressure_count.load(Ordering::SeqCst) > 0,
                "reference checkpoint follows held consumer and actual peer capacity Pending",
            )?;
        }
    }
    require(
        signals.data_sent.load(Ordering::SeqCst) >= CHUNK
            && signals.data_sent.load(Ordering::SeqCst) < DOWNLOAD
            && signals.wire_written.load(Ordering::SeqCst) > 0,
        "reference-control checkpoint requires incomplete actual producer/wire progress",
    )
}

fn run_reference_case(case: ReferenceCase) -> io::Result<ReferenceCaseResult> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0"))?;
    let address = listener.local_addr()?;
    let fixture = RouteFixture::new(ROUTES[1])?;
    let diagnostic = Arc::new(ReferenceDiagnostic::new(case));
    let signals = Arc::new(Signals {
        observe_h2: true,
        diagnostic: Some(Arc::clone(&diagnostic)),
        ..Signals::default()
    });
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 64,
    })
    .map_err(failure)?;
    let mut failures = ReferenceFailures::default();
    let mut retirement = RootRetirement::default();
    let mut both_completed = false;
    let operation = catch_unwind(AssertUnwindSafe(|| -> io::Result<()> {
        let worker = owner
            .handle()
            .worker(0)
            .ok_or_else(|| failure("configured diagnostic worker missing"))?;
        let owner_thread = worker.owner_thread();
        diagnostic.update(|state, _| state.worker = Some(owner_thread));
        let command_context = RuntimeContext::new();
        let caller_context = RuntimeContext::current();
        let (reached, mut observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let local_signals = Arc::clone(&signals);
        let local_diagnostic = Arc::clone(&diagnostic);
        let local_fixture = fixture.clone();
        let call = {
            let _entered = command_context.enter();
            worker
                .call_local(move |context| async move {
                    let result = AssertUnwindSafe(async {
                        match case.client {
                            ReferenceClient::Native => {
                                let observation = native(
                                    context,
                                    NativeSetup {
                                        fixture: local_fixture,
                                        stage: Stage::Release,
                                        address,
                                        signals: Arc::clone(&local_signals),
                                        reached: StageSender::Diagnostic(reached),
                                        release: released,
                                        command_context,
                                    },
                                )
                                .await?;
                                require(
                                    observation.owner == owner_thread
                                        && observation.response_bytes == DOWNLOAD,
                                    "native diagnostic retains exact body and actual owner",
                                )
                            }
                            ReferenceClient::Independent => {
                                reference_client(
                                    context,
                                    ReferenceSetup {
                                        address,
                                        signals: Arc::clone(&local_signals),
                                        diagnostic: Arc::clone(&local_diagnostic),
                                        reached,
                                        release: released,
                                        command_context,
                                    },
                                )
                                .await
                            }
                        }
                    })
                    .catch_unwind()
                    .await;
                    if !matches!(&result, Ok(Ok(()))) {
                        local_diagnostic.capture_before_cancel();
                        local_signals.failed.mark();
                    }
                    result
                })
                .map_err(failure)?
        };
        require(
            RuntimeContext::current() == caller_context,
            "diagnostic submission restores root context",
        )?;
        let peer = async {
            let result = AssertUnwindSafe(peer(
                &listener,
                &fixture,
                Stage::Release,
                Arc::clone(&signals),
            ))
            .catch_unwind()
            .await;
            if result.is_err() {
                diagnostic.capture_before_cancel();
                signals.failed.mark();
            }
            result
        };
        let mut both = {
            let _entered = runtime.enter();
            Box::pin(reference_timeout(
                timeout(ROUTE_BOUND, async { tokio::join!(call, peer) }),
                &diagnostic,
            ))
        };
        let first = runtime.block_on(async {
            tokio::select! {
                biased;
                result = &mut both => Err(result),
                point = &mut observed => Ok(point),
            }
        });
        let completed = match first {
            Ok(point) => {
                let result = point
                    .map_err(failure)
                    .and_then(|point| verify_reference_checkpoint(point, &signals));
                if let Err(error) = result {
                    failures.errors.push(error);
                    diagnostic.capture_before_cancel();
                    signals.failed.mark();
                } else {
                    diagnostic.update(|state, elapsed| state.checkpoint = Some(elapsed));
                }
                diagnostic.log("held-before-root-release");
                let at = diagnostic.started.elapsed();
                if release.send(()).is_err() {
                    failures.errors.push(failure(
                        "diagnostic checkpoint release receiver disappeared",
                    ));
                    diagnostic.capture_before_cancel();
                    signals.failed.mark();
                } else {
                    diagnostic.update(|state, _| state.released = Some(at));
                    let at = Instant::now();
                    signals.update_progress(|state| state.root_release = Some(at));
                }
                runtime.block_on(both.as_mut())
            }
            Err(completed) => {
                failures.errors.push(failure(
                    "diagnostic ended without positive release checkpoint",
                ));
                drop(release);
                completed
            }
        };
        let (client, peer) = completed.map_err(io::Error::other)?;
        both_completed = true;
        match client {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => failures.errors.push(error),
            Ok(Err(payload)) => retain_reference_panic(&mut retirement.first_panic, payload),
            Err(error) => failures.errors.push(failure(error)),
        }
        match peer {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.errors.push(error),
            Err(payload) => retain_reference_panic(&mut retirement.first_panic, payload),
        }
        require(
            RuntimeContext::current() == caller_context,
            "diagnostic polling preserves root context",
        )?;
        Ok(())
    }));
    match operation {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.errors.push(error),
        Err(payload) => retain_reference_panic(&mut retirement.first_panic, payload),
    }
    if !failures.errors.is_empty() || retirement.first_panic.is_some() {
        diagnostic.capture_before_cancel();
        signals.failed.mark();
    }
    let report = retire_runtime(&runtime, &mut owner, &mut retirement, |observer| observer);
    let state = diagnostic.snapshot();
    if failures.errors.is_empty() && retirement.first_panic.is_none() {
        if let Err(error) = require(
            state
                .verified
                .last
                .is_some_and(|(offset, _)| offset == DOWNLOAD)
                && state.trailer_sent.is_some()
                && state.trailer_received.is_some()
                && state.body_complete.is_some()
                && state.checkpoint.is_some()
                && state.released.is_some()
                && state.resumed.is_some()
                && state
                    .peer_option
                    .is_some_and(|(_, actual, _)| actual == case.peer_no_delay)
                && (case.client == ReferenceClient::Native
                    || (state.credit_released == DOWNLOAD
                        && state.used_capacity == 0
                        && state.outgoing_option.is_some_and(|(actual, _)| actual)
                        && state.driver.is_some_and(|(outcome, _)| {
                            matches!(outcome, ChildOutcome::ReturnedOk)
                        }))),
            "successful diagnostic requires complete body/control/option/driver evidence",
        ) {
            failures.errors.push(error);
        }
    }
    let peer_retired = state.io[ReferenceEndpoint::Peer.index()].drops == 1
        && state.io[ReferenceEndpoint::Peer.index()].owner == Some(std::thread::current().id());
    let client_retired = match case.client {
        ReferenceClient::Native => state.native_finished == Some((true, 0)),
        ReferenceClient::Independent => {
            state.io[ReferenceEndpoint::Client.index()].drops == 1
                && state.io[ReferenceEndpoint::Client.index()].owner == state.worker
                && state.handles_released.is_some()
                && state.driver.is_some_and(|(outcome, _)| {
                    matches!(
                        outcome,
                        ChildOutcome::ReturnedOk | ChildOutcome::ReturnedError
                    )
                })
        }
    };
    let children = signals.snapshot(None).state.children;
    let cleanup_confirmed = both_completed
        && signals.peer_done.marked()
        && peer_retired
        && client_retired
        && children.iter().all(|outcome| {
            matches!(
                outcome,
                ChildOutcome::ReturnedOk | ChildOutcome::ReturnedError | ChildOutcome::Aborted
            )
        })
        && state.io.iter().all(|state| !state.wrong_owner)
        && retirement.errors.is_empty()
        && retirement.cleanup_panics == 0
        && report.is_some_and(|report| report.workers_joined == 1 && report.workers_forced == 0);
    failures
        .errors
        .extend(retirement.errors.into_iter().map(failure));
    if !cleanup_confirmed {
        failures.incomplete = true;
        failures.errors.push(failure(
            "diagnostic cleanup not confirmed; remaining fixed cases must not start",
        ));
    }
    diagnostic.log("after-actual-worker-join");
    if case.client == ReferenceClient::Native {
        signals.log_progress(ResourcePoint {
            route: ROUTES[1],
            round: 0,
            stage: Stage::Release,
        });
    }
    eprintln!(
        "native_h2_reference case={case:?} actual_join={report:?} peer_children={children:?} cleanup_confirmed={cleanup_confirmed} failures={failures:?} panic={}",
        retirement.first_panic.is_some()
    );
    drop(owner);
    drop(listener);
    drop(runtime);
    Ok(ReferenceCaseResult {
        failures,
        panic: retirement.first_panic,
        cleanup_confirmed,
    })
}

#[test]
fn native_h2_release_reference_control() {
    let serial = SERIAL.lock().expect("serialized fixed reference control");
    eprintln!(
        "native_h2_reference predeclared_cases={REFERENCE_CASES:?}; each invocation is independent; do not pool or replace failed primary evidence"
    );
    let mut outcomes = Vec::with_capacity(REFERENCE_CASES.len());
    for case in REFERENCE_CASES {
        match run_reference_case(case) {
            Ok(result) => {
                let confirmed = result.cleanup_confirmed;
                outcomes.push(result);
                if !confirmed {
                    break;
                }
            }
            Err(error) => {
                outcomes.push(ReferenceCaseResult {
                    failures: ReferenceFailures {
                        errors: vec![error],
                        incomplete: true,
                    },
                    panic: None,
                    cleanup_confirmed: false,
                });
                break;
            }
        }
    }
    drop(serial);
    let complete = outcomes.len() == REFERENCE_CASES.len();
    let mut failures = ReferenceFailures {
        incomplete: !complete,
        ..ReferenceFailures::default()
    };
    let mut panic = None;
    for outcome in outcomes {
        failures.incomplete |= outcome.failures.incomplete;
        failures.errors.extend(outcome.failures.errors);
        if let Some(payload) = outcome.panic {
            retain_reference_panic(&mut panic, payload);
        }
    }
    if let Some(payload) = panic {
        eprintln!("fixed reference-control retained errors before original panic: {failures:?}");
        resume_unwind(payload);
    }
    assert!(
        complete && failures.errors.is_empty() && !failures.incomplete,
        "{failures:?}"
    );
}

struct LocalDrop {
    owner: ThreadId,
    dropped: Arc<AtomicUsize>,
    wrong_owner: Arc<AtomicBool>,
    local: Rc<()>,
}

impl Drop for LocalDrop {
    fn drop(&mut self) {
        self.wrong_owner
            .fetch_or(self.owner != std::thread::current().id(), Ordering::SeqCst);
        assert_eq!(Rc::strong_count(&self.local), 1);
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn local_drop(dropped: &Arc<AtomicUsize>, wrong_owner: &Arc<AtomicBool>) -> LocalDrop {
    LocalDrop {
        owner: std::thread::current().id(),
        dropped: Arc::clone(dropped),
        wrong_owner: Arc::clone(wrong_owner),
        local: Rc::new(()),
    }
}

struct ControlIo {
    stream: tokio::io::DuplexStream,
    _owned: LocalDrop,
}

impl AsyncRead for ControlIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for ControlIo {
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

struct H1CloseControlGate {
    entered: Signal,
    release: Mutex<Option<oneshot::Sender<()>>>,
    shutdown_errors: AtomicUsize,
}

impl H1CloseControlGate {
    fn release(&self) {
        let release = self
            .release
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(release) = release {
            let _ = release.send(());
        }
    }
}

struct H1CloseRelease(Arc<H1CloseControlGate>);

impl Drop for H1CloseRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct H1CloseControlIo {
    io: ControlIo,
    gate: Arc<H1CloseControlGate>,
    release: oneshot::Receiver<()>,
    released: bool,
    shutdown_error: bool,
}

impl AsyncRead for H1CloseControlIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for H1CloseControlIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.released {
            match Pin::new(&mut self.release).poll(context) {
                Poll::Pending => {
                    self.gate.entered.mark();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => self.released = true,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            }
        }
        if self.shutdown_error {
            self.gate.shutdown_errors.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original held H1 shutdown I/O error",
            )))
        } else {
            Pin::new(&mut self.io).poll_shutdown(context)
        }
    }
}

fn native_retirement_reduction_controls() {
    let cases = [
        (None, "native connector retirement observation is required"),
        (
            Some(Ok(Err(TransportRetirementError::IoDestructionUnconfirmed))),
            "transport I/O value destruction was not confirmed",
        ),
        (
            Some(Ok(Err(
                TransportRetirementError::DrainRetirementUnconfirmed,
            ))),
            "transport close-scope retirement was not confirmed",
        ),
        (
            Some(Ok(Err(TransportRetirementError::BothUnconfirmed))),
            "transport I/O value and close-scope retirement were not confirmed",
        ),
    ];
    for (retired, expected) in cases {
        let mut errors = Vec::new();
        retain(
            &mut errors,
            "native operation",
            Err(failure("original operation failure")),
        );
        retain(
            &mut errors,
            "actual native session result",
            Err(failure("original session failure")),
        );
        retain_native_retirement(&mut errors, native_retirement_result(retired));
        assert_eq!(
            errors,
            [
                "native operation: original operation failure".to_owned(),
                "actual native session result: original session failure".to_owned(),
                format!("actual native transport value and close-scope retirement: {expected}"),
            ],
        );
        let ordered = errors.join("; ");
        assert_eq!(
            finish(errors)
                .expect_err("injected reducer failures are not retirement success")
                .to_string(),
            ordered,
        );
    }
    let mut errors = Vec::new();
    retain(
        &mut errors,
        "native operation",
        Err(failure("original operation failure")),
    );
    retain_native_retirement(&mut errors, native_retirement_result(Some(Ok(Ok(())))));
    assert_eq!(errors, ["native operation: original operation failure"]);
    assert!(finish(errors).is_err());
}

async fn h1_finalization_control(
    context: WorkerContext,
    gate: Arc<H1CloseControlGate>,
    release: oneshot::Receiver<()>,
    shutdown_error: bool,
    drops: Arc<AtomicUsize>,
    wrong_owner: Arc<AtomicBool>,
) -> io::Result<()> {
    require(context.is_owner_thread(), "H1 close-control native owner")?;
    let (native_io, peer_io) = tokio::io::duplex(CHUNK);
    let peer_io = ControlIo {
        stream: peer_io,
        _owned: local_drop(&drops, &wrong_owner),
    };
    let native_io = H1CloseControlIo {
        io: ControlIo {
            stream: native_io,
            _owned: local_drop(&drops, &wrong_owner),
        },
        gate: Arc::clone(&gate),
        release,
        released: false,
        shutdown_error,
    };
    let transport = adopt_local(
        &context,
        EstablishedIo::new(
            native_io,
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        ByteStreamLimits::new(CHUNK, CHUNK, 8).map_err(failure)?,
    )
    .map_err(failure)?;
    let mut options = ClientAttachOptions::new(CodecProtocol::Http11, TransportSecurity::Plaintext);
    options.session = native_session_params();
    let session = ClientSession::attach_transport(&context, transport, options).map_err(failure)?;
    let retirement = session
        .transport_retirement()
        .ok_or_else(|| failure("actual adopted H1 session retirement receipt"))?;
    let operation_deadline = tokio::time::Instant::now() + STAGE_BOUND;
    let cleanup_deadline = operation_deadline + STAGE_BOUND;
    let operation = AssertUnwindSafe(async {
        require(
            session.protocol() == CodecProtocol::Http11 && session.active_transactions() == 0,
            "actual idle H1 session, not a fabricated completed future",
        )?;
        session.initiate_drain().map_err(failure)?;
        let mut finalized = Box::pin(native_finalization(&session, Some(&retirement)));
        tokio::select! {
            biased;
            result = &mut finalized => return Err(failure(format!(
                "H1 finalization completed before original held shutdown: {result:?}",
            ))),
            _ = gate.entered.wait() => {},
        }
        let summary = timeout_at(operation_deadline, session.wait_closed())
            .await
            .map_err(failure)?
            .map_err(failure)?;
        require(
            session.is_finished()
                && session.active_transactions() == 0
                && gate.entered.marked()
                && drops.load(Ordering::SeqCst) == 0
                && !wrong_owner.load(Ordering::SeqCst),
            "actual H1 completion precedes held original transport destruction",
        )?;
        still_pending(finalized.as_mut()).await?;
        gate.release();
        let (closed, retired) = finalized.await;
        require(
            closed.map_err(failure)?.map_err(failure)? == summary,
            "shared finalization retains the actual original H1 summary",
        )?;
        let mut errors = Vec::new();
        retain_native_retirement(&mut errors, retired);
        finish(errors)?;
        require(
            drops.load(Ordering::SeqCst) == 1 && !wrong_owner.load(Ordering::SeqCst),
            "original native I/O destroyed while original peer I/O remains held",
        )?;
        require(
            gate.shutdown_errors.load(Ordering::SeqCst) == usize::from(shutdown_error),
            "actual shutdown I/O error is distinct from unconfirmed destruction",
        )?;
        let (repeated, missing) = native_finalization(&session, None).await;
        require(
            repeated.map_err(failure)?.map_err(failure)? == summary,
            "missing-receipt control still consumes the actual session result",
        )?;
        let mut errors = Vec::new();
        retain_native_retirement(&mut errors, missing);
        require(
            errors == [
                "actual native transport value and close-scope retirement: native connector retirement observation is required",
            ] && finish(errors).is_err(),
            "shared finalization rejects an explicitly withheld actual receipt",
        )
    })
    .catch_unwind()
    .await;
    gate.release();
    if !matches!(&operation, Ok(Ok(()))) {
        session.cancel();
    }
    let (closed, retired) = tokio::join!(
        timeout_at(cleanup_deadline, session.wait_closed()),
        timeout_at(cleanup_deadline, retirement.wait_retired()),
    );
    drop(session);
    drop(peer_io);
    let mut errors = Vec::new();
    let panic = match operation {
        Ok(result) => {
            retain(&mut errors, "original H1 finalization control", result);
            None
        }
        Err(payload) => Some(payload),
    };
    retain(
        &mut errors,
        "H1 control cleanup session",
        closed
            .map_err(failure)
            .and_then(|result| result.map(|_| ()).map_err(failure)),
    );
    retain_native_retirement(&mut errors, native_retirement_result(Some(retired)));
    retain(
        &mut errors,
        "H1 control original I/O owners",
        require(
            drops.load(Ordering::SeqCst) == 2 && !wrong_owner.load(Ordering::SeqCst),
            "both actual original I/O owners destroyed on their worker",
        ),
    );
    if let Some(payload) = panic {
        eprintln!("H1 finalization control cleanup after original panic: {errors:?}");
        resume_unwind(payload);
    }
    finish(errors)
}

fn h1_finalization_controls(runtime: &Runtime) {
    native_retirement_reduction_controls();
    for shutdown_error in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let wrong_owner = Arc::new(AtomicBool::new(false));
        let (release, released) = oneshot::channel();
        let gate = Arc::new(H1CloseControlGate {
            entered: Signal::default(),
            release: Mutex::new(Some(release)),
            shutdown_errors: AtomicUsize::new(0),
        });
        let _release = H1CloseRelease(Arc::clone(&gate));
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 64,
        })
        .expect("H1 close-control worker starts");
        let operation = catch_unwind(AssertUnwindSafe(|| -> io::Result<()> {
            let worker = owner
                .handle()
                .worker(0)
                .ok_or_else(|| failure("H1 close-control worker missing"))?;
            let gate = Arc::clone(&gate);
            let drops = Arc::clone(&drops);
            let wrong_owner = Arc::clone(&wrong_owner);
            let call = worker
                .call_local(move |context| {
                    h1_finalization_control(
                        context,
                        gate,
                        released,
                        shutdown_error,
                        drops,
                        wrong_owner,
                    )
                })
                .map_err(failure)?;
            runtime.block_on(async {
                timeout(ROUTE_BOUND, call)
                    .await
                    .map_err(failure)?
                    .map_err(failure)?
            })
        }));
        gate.release();
        let mut retirement = RootRetirement::default();
        match operation {
            Ok(result) => retain(
                &mut retirement.errors,
                "H1 finalization control arm",
                result,
            ),
            Err(payload) => retirement.first_panic = Some(payload),
        }
        let joined = retire_runtime(runtime, &mut owner, &mut retirement, |observer| observer);
        if joined.is_some() {
            retain(
                &mut retirement.errors,
                "H1 control post-join original I/O owners",
                require(
                    drops.load(Ordering::SeqCst) == 2 && !wrong_owner.load(Ordering::SeqCst),
                    "both owner-local I/O destructions survive original worker join",
                ),
            );
        }
        if let Some(payload) = retirement.first_panic {
            eprintln!(
                "H1 close-control physical cleanup after panic: {:?}",
                retirement.errors
            );
            resume_unwind(payload);
        }
        finish(retirement.errors).expect("shared H1 finalization and original physical cleanup");
    }
}

struct HandshakeWriteGate {
    io: ControlIo,
    release: oneshot::Receiver<()>,
    pending: Arc<AtomicUsize>,
}

impl AsyncRead for HandshakeWriteGate {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for HandshakeWriteGate {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match Pin::new(&mut self.release).poll(context) {
            Poll::Pending => {
                self.pending.fetch_add(1, Ordering::SeqCst);
                Poll::Pending
            }
            Poll::Ready(_) => {
                Poll::Ready(Err(failure("initial handshake gate unexpectedly released")))
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReferenceTimeoutControl {
    Handshake,
    PeerRoute,
    ParentJoin,
}

fn reference_timeout_snapshot_control(
    control: ReferenceTimeoutControl,
) -> impl Future<Output = ()> {
    let owner = std::thread::current().id();
    let diagnostic = Arc::new(ReferenceDiagnostic::new(ReferenceCase {
        client: ReferenceClient::Independent,
        peer_no_delay: false,
    }));
    let endpoint = if control == ReferenceTimeoutControl::PeerRoute {
        ReferenceEndpoint::Peer
    } else {
        ReferenceEndpoint::Client
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let wrong_owner = Arc::new(AtomicBool::new(false));
    let pending = Arc::new(AtomicUsize::new(0));
    let sibling_drops = Arc::new(AtomicUsize::new(0));
    let sibling_started = Arc::new(AtomicBool::new(false));
    let route_returned = Arc::new(AtomicBool::new(false));
    let sibling_owner = (control == ReferenceTimeoutControl::ParentJoin)
        .then(|| local_drop(&sibling_drops, &wrong_owner));
    let (stream, mut peer) = tokio::io::duplex(CHUNK);
    let (release, held) = oneshot::channel();
    let io = ReferenceIo::new(
        HandshakeWriteGate {
            io: ControlIo {
                stream,
                _owned: local_drop(&drops, &wrong_owner),
            },
            release: held,
            pending: Arc::clone(&pending),
        },
        Arc::clone(&diagnostic),
        endpoint,
    );
    let handshake = h2::client::handshake(io);
    let joined_started = Arc::clone(&sibling_started);
    let route_completed = Arc::clone(&route_returned);
    let operation = async move {
        match control {
            ReferenceTimeoutControl::Handshake => handshake.await,
            ReferenceTimeoutControl::PeerRoute => {
                let route = async move {
                    let result = handshake.await;
                    route_completed.store(true, Ordering::SeqCst);
                    result
                };
                route.await
            }
            ReferenceTimeoutControl::ParentJoin => {
                let sibling = async move {
                    let _owned = sibling_owner.expect("parent retains its second inline child");
                    joined_started.store(true, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                };
                let (result, ()) = tokio::join!(handshake, sibling);
                result
            }
        }
    };
    let bounded = timeout_at(tokio::time::Instant::now(), operation);
    async move {
        let elapsed = reference_timeout(bounded, &diagnostic)
            .await
            .err()
            .expect("held actual handshake selects its original timeout");
        assert!(
            pending.load(Ordering::SeqCst) > 0,
            "actual h2 initial write returned Pending before Elapsed: {control:?}"
        );
        let before = diagnostic
            .before_cancel
            .lock()
            .expect("retained pre-disposal state")
            .expect("selected timeout captured its still-owned operation");
        let before_io = before.io[endpoint.index()];
        assert_eq!(before_io.owner, Some(owner));
        assert_eq!(
            before_io.drops, 0,
            "snapshot precedes actual handshake I/O disposal: {control:?}"
        );
        assert!(before_io.dropped.is_none());
        assert_eq!(before_io.write.count, 0);
        assert!(!before_io.wrong_owner);
        let after = diagnostic.snapshot().io[endpoint.index()];
        assert_eq!(after.owner, Some(owner));
        assert_eq!(after.drops, 1, "one post-inner-Drop receipt: {control:?}");
        assert!(after.dropped.is_some());
        assert!(!after.wrong_owner);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!wrong_owner.load(Ordering::SeqCst));
        assert!(
            release.is_closed(),
            "sender is retained; only operation disposal closes its gate receiver"
        );
        assert!(
            !route_returned.load(Ordering::SeqCst),
            "the pending route did not fabricate an H2 return"
        );
        assert_eq!(
            sibling_started.load(Ordering::SeqCst),
            control == ReferenceTimeoutControl::ParentJoin
        );
        assert_eq!(
            sibling_drops.load(Ordering::SeqCst),
            usize::from(control == ReferenceTimeoutControl::ParentJoin)
        );
        diagnostic.capture_before_cancel();
        assert_eq!(
            diagnostic
                .before_cancel
                .lock()
                .expect("sticky original snapshot")
                .expect("original snapshot remains")
                .io[endpoint.index()]
            .drops,
            0
        );
        let error = io::Error::other(elapsed);
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<Elapsed>())
                .is_some(),
            "the original Tokio timeout is retained, not a synthesized protocol error"
        );
        let mut byte = [0];
        assert_eq!(
            peer.read(&mut byte).await.expect("actual gate-side EOF"),
            0,
            "no preface byte escaped the held write, and the actual I/O retired"
        );
        drop(release);
        drop(peer);
    }
}

#[test]
fn native_h2_reference_retains_timeout_snapshots() {
    let serial = SERIAL.lock().expect("serialized retained-timeout control");
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("control root reactor");
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 64,
    })
    .expect("actual control worker owner");
    let mut retirement = RootRetirement::default();
    let operation = catch_unwind(AssertUnwindSafe(|| {
        let worker = owner.handle().worker(0).expect("actual control worker");
        let owner_thread = worker.owner_thread();
        let call = worker
            .call_local(move |context| async move {
                AssertUnwindSafe(async move {
                    assert!(context.is_owner_thread());
                    assert_eq!(std::thread::current().id(), owner_thread);
                    for control in [
                        ReferenceTimeoutControl::Handshake,
                        ReferenceTimeoutControl::PeerRoute,
                    ] {
                        reference_timeout_snapshot_control(control).await;
                    }
                })
                .catch_unwind()
                .await
            })
            .expect("admitted owner-local timeout controls");
        let result = runtime
            .block_on(async { timeout(ROUTE_BOUND, call).await })
            .expect("bounded actual command completion")
            .expect("actual worker returned its owned control outcome");
        if let Err(payload) = result {
            resume_unwind(payload);
        }
        let parent = {
            let _entered = runtime.enter();
            reference_timeout_snapshot_control(ReferenceTimeoutControl::ParentJoin)
        };
        runtime
            .block_on(async { timeout(ROUTE_BOUND, parent).await })
            .expect("bounded root retained-parent-timeout control");
    }));
    if let Err(payload) = operation {
        retirement.first_panic = Some(payload);
    }
    let report = retire_runtime(&runtime, &mut owner, &mut retirement, |quiescence| {
        quiescence
    });
    drop(owner);
    drop(runtime);
    drop(serial);
    if let Some(payload) = retirement.first_panic {
        eprintln!(
            "retained-timeout control cleanup before original panic: {:?}",
            retirement.errors
        );
        resume_unwind(payload);
    }
    assert!(retirement.errors.is_empty(), "{:?}", retirement.errors);
    assert_eq!(retirement.cleanup_panics, 0);
    let report = report.expect("actual physical worker join receipt");
    assert_eq!(report.workers_joined, 1);
    assert_eq!(report.workers_forced, 0);
}

struct ControlDrivers {
    client: Option<Pin<Box<h2::client::Connection<ControlIo, Bytes>>>>,
    server: h2::server::Connection<ControlIo, Bytes>,
    client_result: Option<Result<(), h2::Error>>,
    server_result: Option<Result<(), h2::Error>>,
}

impl ControlDrivers {
    fn poll(&mut self, context: &mut Context<'_>) {
        if self.client_result.is_none()
            && let Some(client) = self.client.as_mut()
            && let Poll::Ready(result) = client.as_mut().poll(context)
        {
            self.client_result = Some(result);
        }
        if self.server_result.is_none() {
            match self.server.poll_accept(context) {
                Poll::Ready(None) => self.server_result = Some(Ok(())),
                Poll::Ready(Some(Err(error))) => self.server_result = Some(Err(error)),
                Poll::Ready(Some(Ok(_))) => panic!("no second request in reset control"),
                Poll::Pending => {}
            }
        }
    }

    async fn until<Work: Future>(&mut self, work: Work) -> Work::Output {
        let mut work = Box::pin(work);
        poll_fn(|context| {
            if let Poll::Ready(result) = work.as_mut().poll(context) {
                return Poll::Ready(result);
            }
            self.poll(context);
            work.as_mut().poll(context)
        })
        .await
    }

    async fn finish(&mut self) {
        poll_fn(|context| {
            self.poll(context);
            if (self.client.is_none() || self.client_result.is_some())
                && self.server_result.is_some()
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResetControl {
    Cancel,
    OtherReason,
    Reasonless,
    ConnectionError,
}

async fn h2_reset_control(control: ResetControl) {
    let drops = Arc::new(AtomicUsize::new(0));
    let wrong_owner = Arc::new(AtomicBool::new(false));
    let (client_io, server_io) = tokio::io::duplex(4 * CHUNK);
    let client_io = ControlIo {
        stream: client_io,
        _owned: local_drop(&drops, &wrong_owner),
    };
    let server_io = ControlIo {
        stream: server_io,
        _owned: local_drop(&drops, &wrong_owner),
    };
    let mut builder = h2::client::Builder::new();
    builder.initial_window_size(CHUNK as u32);
    let (client, server) = tokio::join!(
        builder.handshake::<_, Bytes>(client_io),
        h2::server::handshake(server_io)
    );
    let (mut client, client_driver) = client.expect("real reset-control client handshake");
    let mut client_driver = Box::pin(client_driver);
    let mut server = server.expect("real reset-control server handshake");
    let (response, mut request_stream) = client
        .send_request(
            http::Request::builder()
                .uri("https://localhost/native-reset-control")
                .body(())
                .expect("control request"),
            true,
        )
        .expect("actual control request stream");
    drop(client);
    let (request, mut responder) = poll_fn(|context| {
        assert!(
            client_driver.as_mut().poll(context).is_pending(),
            "live original client driver"
        );
        server.poll_accept(context)
    })
    .await
    .expect("actual control server request")
    .expect("valid H2 request");
    drop(request);
    let mut stream = responder
        .send_response(Response::new(()), false)
        .expect("real control response head");
    drop(responder);
    let mut drivers = ControlDrivers {
        client: Some(client_driver),
        server,
        client_result: None,
        server_result: None,
    };
    let mut body = drivers
        .until(response)
        .await
        .expect("actual response headers")
        .into_body();
    let signals = Signals::default();
    assert_eq!(
        drivers
            .until(h2_chunk(&mut stream, 0, &signals))
            .await
            .expect("first real DATA"),
        ChunkOutcome::Sent
    );
    let prefix = drivers
        .until(body.data())
        .await
        .expect("real response DATA")
        .expect("valid response DATA");
    assert_eq!(
        verify_bytes(&prefix, 0, CHUNK).expect("exact first DATA"),
        CHUNK
    );
    signals.held.mark();
    let mut pending = Box::pin(h2_chunk(&mut stream, CHUNK, &signals));
    still_pending(pending.as_mut())
        .await
        .expect("second DATA genuinely capacity-blocked");
    assert!(signals.pressure.marked());
    assert_eq!(signals.data_sent.load(Ordering::SeqCst), CHUNK);
    drop(pending);
    match control {
        ResetControl::Cancel | ResetControl::OtherReason => {
            let reason = if control == ResetControl::Cancel {
                Reason::CANCEL
            } else {
                Reason::PROTOCOL_ERROR
            };
            request_stream.send_reset(reason);
            assert_eq!(
                drivers
                    .until(h2_chunk(&mut stream, CHUNK, &signals))
                    .await
                    .expect("closed capacity observed"),
                ChunkOutcome::CapacityClosed
            );
            let observed = drivers.until(require_h2_cancel(&mut stream)).await;
            if control == ResetControl::Cancel {
                observed.expect("actual CANCEL alone satisfies the pressured reset oracle");
            } else {
                assert_eq!(
                    observed.expect_err("other reset rejected").to_string(),
                    "actual retained H2 stream reset is CANCEL"
                );
            }
            assert_eq!(
                poll_fn(|context| stream.poll_reset(context))
                    .await
                    .expect("same retained actual reset"),
                reason
            );
        }
        ResetControl::Reasonless => {
            stream
                .send_trailers(HeaderMap::new())
                .expect("local normal EOM without reset");
            assert_eq!(
                h2_chunk(&mut stream, CHUNK, &signals)
                    .await
                    .expect("normal closed capacity"),
                ChunkOutcome::CapacityClosed
            );
            let mut reset = Box::pin(require_h2_cancel(&mut stream));
            still_pending(reset.as_mut())
                .await
                .expect("capacity None alone is not reset evidence");
            drop(reset);
            let trailers = drivers
                .until(body.trailers())
                .await
                .expect("actual ordinary terminal trailers");
            assert!(trailers.is_some());
            let mut reset = Box::pin(require_h2_cancel(&mut stream));
            still_pending(reset.as_mut())
                .await
                .expect("delivered reasonless EOM is not CANCEL");
            drop(reset);
        }
        ResetControl::ConnectionError => {
            drop(
                drivers
                    .client
                    .take()
                    .expect("actual client driver deliberately aborted"),
            );
            let error = drivers
                .until(require_h2_cancel(&mut stream))
                .await
                .expect_err("connection loss is not stream CANCEL");
            let error = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<h2::Error>())
                .expect("actual connection error retained");
            assert_eq!(
                error.get_io().map(io::Error::kind),
                Some(io::ErrorKind::BrokenPipe)
            );
            assert_eq!(error.reason(), None);
        }
    }
    assert_eq!(
        signals.data_sent.load(Ordering::SeqCst),
        CHUNK,
        "no negative sends a second DATA chunk"
    );
    drop(prefix);
    drop(body);
    drop(request_stream);
    drop(stream);
    drivers.finish().await;
    if control == ResetControl::ConnectionError {
        assert!(
            drivers.client.is_none() && drivers.client_result.is_none(),
            "abort is not a fabricated client-driver result"
        );
        assert!(
            drivers.server_result.as_ref().is_some_and(Result::is_ok),
            "h2 connection EOF is distinct from its active stream's BrokenPipe"
        );
    } else {
        assert!(drivers.client_result.as_ref().is_some_and(Result::is_ok));
        assert!(drivers.server_result.as_ref().is_some_and(Result::is_ok));
    }
    drop(drivers);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "both actual inline driver I/O owners retired"
    );
    assert!(!wrong_owner.load(Ordering::SeqCst));
}

struct H2InputControlWire {
    bytes: RefCell<Vec<u8>>,
    parsed: Cell<usize>,
    pings: Cell<usize>,
    sent: Signal,
}

impl H2InputControlWire {
    fn new() -> Self {
        Self {
            bytes: RefCell::new(Vec::with_capacity(CHUNK)),
            parsed: Cell::new(0),
            pings: Cell::new(0),
            sent: Signal::default(),
        }
    }

    fn observe(&self, written: &[u8]) -> io::Result<()> {
        let mut bytes = self.bytes.borrow_mut();
        require(
            bytes
                .len()
                .checked_add(written.len())
                .is_some_and(|size| size <= CHUNK),
            "bounded actual H2 input-control wire capture",
        )?;
        bytes.extend_from_slice(written);
        loop {
            let parsed = self.parsed.get();
            let frame = &bytes[parsed..];
            if frame.len() < 9 {
                return Ok(());
            }
            let payload = (usize::from(frame[0]) << 16)
                | (usize::from(frame[1]) << 8)
                | usize::from(frame[2]);
            let size = 9 + payload;
            require(size <= CHUNK, "bounded input-control frame")?;
            if frame.len() < size {
                return Ok(());
            }
            if frame[3] == 6 {
                require(
                    payload == 8 && frame[4] == 0 && frame[5..9] == [0; 4],
                    "actual connection-level non-ACK PING",
                )?;
                require(
                    self.pings.get() == 0,
                    "exactly one actual input-control PING",
                )?;
                self.pings.set(1);
                self.sent.mark();
            }
            self.parsed.set(parsed + size);
        }
    }
}

struct H2InputControlIo {
    io: ControlIo,
    wire: Rc<H2InputControlWire>,
}

impl AsyncRead for H2InputControlIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for H2InputControlIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.io).poll_write(context, bytes) {
            Poll::Ready(Ok(written)) => match self.wire.observe(&bytes[..written]) {
                Ok(()) => Poll::Ready(Ok(written)),
                Err(error) => Poll::Ready(Err(error)),
            },
            result => result,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H2InputControl {
    Pong,
    ConnectionLoss,
    MissingHandle,
    ExternalFailure,
}

async fn h2_input_barrier_control(stage: Stage, control: H2InputControl, driver_first: bool) {
    let drops = Arc::new(AtomicUsize::new(0));
    let wrong_owner = Arc::new(AtomicBool::new(false));
    let wire = Rc::new(H2InputControlWire::new());
    let (client_io, server_io) = tokio::io::duplex(4 * CHUNK);
    let client_io = ControlIo {
        stream: client_io,
        _owned: local_drop(&drops, &wrong_owner),
    };
    let server_io = H2InputControlIo {
        io: ControlIo {
            stream: server_io,
            _owned: local_drop(&drops, &wrong_owner),
        },
        wire: Rc::clone(&wire),
    };
    let builder = h2::client::Builder::new();
    let (client, server) = tokio::join!(
        builder.handshake::<_, Bytes>(client_io),
        h2::server::handshake(server_io),
    );
    let (client, client_driver) = client.expect("actual input-control client handshake");
    let mut server = server.expect("actual input-control server handshake");
    let original_ping = server
        .ping_pong()
        .expect("original fresh connection has its owned ping handle");
    let ping = if control == H2InputControl::MissingHandle {
        let missing = server.ping_pong();
        assert!(missing.is_none(), "actual second take is unavailable");
        drop(original_ping);
        missing
    } else {
        Some(original_ping)
    };
    let signals = Signals::default();
    signals.native_operation_done.mark();
    let release = Signal::default();
    let exchange_error = Cell::new(None::<*const h2::Error>);
    let driver_error = Cell::new(None::<*const h2::Error>);
    let exchange = async {
        let result = h2_input_barrier(H2PeerPurpose::Staged, stage, ping, async {
            signals.upload.mark();
            Ok(())
        })
        .await;
        if let Err(error) = &result {
            exchange_error.set(
                error
                    .get_ref()
                    .and_then(|error| error.downcast_ref::<h2::Error>())
                    .map(|error| error as *const h2::Error),
            );
        }
        result
    };
    let driver_capture = &driver_error;
    let driver = async move {
        let result = match server.accept().await {
            None => Ok(()),
            Some(Err(error)) => Err(io::Error::other(error)),
            Some(Ok(_)) => Err(failure("input control has no application request")),
        };
        if let Err(error) = &result {
            driver_capture.set(
                error
                    .get_ref()
                    .and_then(|error| error.downcast_ref::<h2::Error>())
                    .map(|error| error as *const h2::Error),
            );
        }
        result
    };
    let peer = async {
        let result = join_controlled_peer_children(exchange, driver, &signals, driver_first).await;
        if matches!(
            control,
            H2InputControl::MissingHandle | H2InputControl::ExternalFailure
        ) {
            release.mark();
        }
        result.and_then(|retirement| acknowledge_peer(&signals, retirement))
    };
    let remote = async {
        release.wait().await;
        if control == H2InputControl::Pong {
            let result = client_driver.await;
            drop(client);
            Some(result)
        } else {
            drop(client);
            drop(client_driver);
            None
        }
    };
    let mut joined = Box::pin(async { tokio::join!(peer, remote) });
    if control != H2InputControl::MissingHandle {
        tokio::select! {
            biased;
            result = &mut joined => panic!("input barrier completed before remote release: {result:?}"),
            _ = wire.sent.wait() => {},
        }
        assert_eq!(wire.pings.get(), 1);
        assert!(
            !signals.upload.marked(),
            "real PING without Pong is not stage publication"
        );
        assert!(!signals.peer_acknowledged.marked() && !signals.failed.marked());
        assert!(!signals.action.marked());
        assert!(exchange_error.get().is_none() && driver_error.get().is_none());
        assert_eq!(signals.data_sent.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    match control {
        H2InputControl::Pong | H2InputControl::ConnectionLoss => release.mark(),
        H2InputControl::ExternalFailure => signals.failed.mark(),
        H2InputControl::MissingHandle => {}
    }
    let (peer, remote) = joined.await;
    match control {
        H2InputControl::Pong => {
            assert_eq!(
                peer.expect("actual Pong releases the original exchange"),
                PeerDriverRetirement::Controller,
            );
            remote
                .expect("original remote driver was polled")
                .expect("original remote driver completed");
            assert!(signals.upload.marked() && signals.peer_acknowledged.marked());
            assert!(!signals.failed.marked());
            assert!(exchange_error.get().is_none() && driver_error.get().is_none());
            assert_eq!(wire.pings.get(), 1);
        }
        H2InputControl::MissingHandle => {
            assert!(
                remote.is_none(),
                "deliberate original driver drop is not an exit result"
            );
            let error = peer.expect_err("missing original handle prevents publication");
            let detail = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<PeerChildrenFailure>())
                .expect("original joined child failure");
            let original = detail
                .first
                .as_ref()
                .expect("original missing-handle error");
            assert_eq!(
                original.to_string(),
                "original H2 input barrier handle unavailable"
            );
            assert!(detail.second.is_none());
            assert!(!detail.exchange_aborted && detail.driver_aborted);
            assert!(!signals.upload.marked() && !signals.peer_acknowledged.marked());
            assert!(signals.failed.marked());
            assert_eq!(wire.pings.get(), 0);
        }
        H2InputControl::ConnectionLoss | H2InputControl::ExternalFailure => {
            assert!(
                remote.is_none(),
                "deliberate original driver drop is not an exit result"
            );
            assert!(!signals.upload.marked() && !signals.peer_acknowledged.marked());
            assert!(signals.failed.marked());
            let error = peer.expect_err("failed input barrier prevents publication");
            let detail = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<PeerChildrenFailure>())
                .expect("original joined child failure");
            let retained = [detail.first.as_ref(), detail.second.as_ref()].map(|error| {
                error.map(|error| {
                    let actual = error
                        .get_ref()
                        .and_then(|error| error.downcast_ref::<h2::Error>())
                        .expect("original typed H2 I/O failure retained");
                    assert!(actual.get_io().is_some());
                    actual as *const h2::Error
                })
            });
            assert_eq!(
                retained,
                [
                    driver_error.get().or(exchange_error.get()),
                    driver_error.get().and(exchange_error.get()),
                ],
                "original errors retain their allocations and observed order",
            );
            if control == H2InputControl::ConnectionLoss {
                assert!(exchange_error.get().is_some());
                assert!(!detail.exchange_aborted && !detail.driver_aborted);
            } else {
                assert!(detail.driver_aborted);
                assert_eq!(detail.exchange_aborted, !driver_first);
                assert_eq!(exchange_error.get().is_some(), driver_first);
                assert!(driver_error.get().is_none());
            }
            assert_eq!(wire.pings.get(), 1);
        }
    }
    assert!(!signals.action.marked() && !signals.native_drain.marked());
    assert_eq!(signals.data_sent.load(Ordering::SeqCst), 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "both original control I/O owners dropped"
    );
    assert!(!wrong_owner.load(Ordering::SeqCst));
}

async fn h2_input_barrier_controls() {
    for stage in [Stage::UploadCancel, Stage::FutureDrop] {
        for driver_first in [false, true] {
            for control in [
                H2InputControl::Pong,
                H2InputControl::ConnectionLoss,
                H2InputControl::MissingHandle,
                H2InputControl::ExternalFailure,
            ] {
                h2_input_barrier_control(stage, control, driver_first).await;
            }
        }
    }
}

async fn peer_failure_controls() {
    for driver_first in [false, true] {
        for exchange_fails in [false, true] {
            let signals = Signals::default();
            let drops = Arc::new(AtomicUsize::new(0));
            let wrong_owner = Arc::new(AtomicBool::new(false));
            let exchange_owner = local_drop(&drops, &wrong_owner);
            let driver_owner = local_drop(&drops, &wrong_owner);
            let work = |owned: LocalDrop, fails: bool| async move {
                let _owned = owned;
                if fails {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "original inline peer failure",
                    ))
                } else {
                    std::future::pending::<io::Result<()>>().await
                }
            };
            let error = join_peer_children(
                work(exchange_owner, exchange_fails),
                work(driver_owner, !exchange_fails),
                &signals,
                driver_first,
            )
            .await
            .expect_err("actual peer failure survives both inline child poll orders");
            let detail = error
                .get_ref()
                .and_then(|error| error.downcast_ref::<PeerChildrenFailure>())
                .expect("owned typed child outcomes");
            let original = detail
                .first
                .as_ref()
                .expect("actual original child error retained");
            assert_eq!(original.kind(), io::ErrorKind::InvalidData);
            assert_eq!(original.to_string(), "original inline peer failure");
            assert!(detail.second.is_none());
            assert_eq!(detail.exchange_aborted, !exchange_fails);
            assert_eq!(detail.driver_aborted, exchange_fails);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert!(!wrong_owner.load(Ordering::SeqCst));
        }
    }
    let signals = Signals::default();
    signals.failed.mark();
    let error = join_peer_children(
        std::future::pending(),
        std::future::pending(),
        &signals,
        false,
    )
    .await
    .expect_err("external native abort is not peer success");
    let detail = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<PeerChildrenFailure>())
        .expect("external abort retains explicit child outcomes");
    assert!(detail.first.is_none() && detail.second.is_none());
    assert!(detail.exchange_aborted && detail.driver_aborted);
    h2_input_barrier_controls().await;
}

#[derive(Debug, thiserror::Error)]
#[error("owned injected peer-child failure")]
struct InjectedPeerFailure {
    drops: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl Drop for InjectedPeerFailure {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        assert_eq!(self.drops.fetch_add(1, Ordering::SeqCst), 0);
    }
}

async fn controlled_peer_failure_controls() {
    for driver_first in [false, true] {
        for exchange_fails in [false, true] {
            for control_ready in [false, true] {
                let signals = Signals::default();
                if control_ready {
                    signals.native_operation_done.mark();
                }
                let drops = Arc::new(AtomicUsize::new(0));
                let wrong_owner = Arc::new(AtomicBool::new(false));
                let error_drops = Arc::new(AtomicUsize::new(0));
                let error = io::Error::other(InjectedPeerFailure {
                    drops: Arc::clone(&error_drops),
                    owner: std::thread::current().id(),
                });
                let address = error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<InjectedPeerFailure>()
                    .unwrap() as *const InjectedPeerFailure;
                let mut error = Some(error);
                let mut work = |owned: LocalDrop, fails: bool, succeeds: bool| {
                    let error = if fails { error.take() } else { None };
                    async move {
                        let _owned = owned;
                        if let Some(error) = error {
                            Err(error)
                        } else if succeeds {
                            Ok(())
                        } else {
                            std::future::pending::<io::Result<()>>().await
                        }
                    }
                };
                let exchange = work(
                    local_drop(&drops, &wrong_owner),
                    exchange_fails,
                    control_ready,
                );
                let driver = work(local_drop(&drops, &wrong_owner), !exchange_fails, false);
                let result =
                    join_controlled_peer_children(exchange, driver, &signals, driver_first)
                        .await
                        .and_then(|retirement| acknowledge_peer(&signals, retirement));
                let error = result
                    .expect_err("original failure beats cancellation/control in either poll order");
                let detail = error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<PeerChildrenFailure>()
                    .unwrap();
                let original = detail.first.as_ref().expect("owned original failure");
                assert_eq!(original.kind(), io::ErrorKind::Other);
                assert!(std::ptr::eq(
                    original
                        .get_ref()
                        .unwrap()
                        .downcast_ref::<InjectedPeerFailure>()
                        .unwrap(),
                    address,
                ));
                assert!(detail.second.is_none());
                assert_eq!(drops.load(Ordering::SeqCst), 2);
                assert!(!wrong_owner.load(Ordering::SeqCst));
                assert_eq!(error_drops.load(Ordering::SeqCst), 0);
                assert!(!signals.peer_acknowledged.marked());
                assert!(!signals.native_drain.marked());
                drop(error);
                assert_eq!(error_drops.load(Ordering::SeqCst), 1);
            }
        }
        let signals = Signals::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let wrong_owner = Arc::new(AtomicBool::new(false));
        let exchange_owner = local_drop(&drops, &wrong_owner);
        let driver_owner = local_drop(&drops, &wrong_owner);
        let mut joined = Box::pin(join_controlled_peer_children(
            async move {
                drop(exchange_owner);
                Ok(())
            },
            async move {
                let _owned = driver_owner;
                std::future::pending().await
            },
            &signals,
            driver_first,
        ));
        assert!(
            poll_fn(|context| Poll::Ready(joined.as_mut().poll(context)))
                .await
                .is_pending()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!signals.peer_acknowledged.marked());
        signals.native_operation_done.mark();
        assert_eq!(joined.await.unwrap(), PeerDriverRetirement::Controller);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(!wrong_owner.load(Ordering::SeqCst));
        assert!(!signals.peer_acknowledged.marked());
        acknowledge_peer(&signals, PeerDriverRetirement::Controller).unwrap();
        assert!(signals.peer_acknowledged.marked());
        assert!(!signals.native_drain.marked());
    }
    let signals = Signals::default();
    signals.native_operation_done.mark();
    signals.failed.mark();
    let error = join_controlled_peer_children(
        std::future::pending(),
        std::future::pending(),
        &signals,
        false,
    )
    .await
    .expect_err("external abort never becomes controller retirement");
    let detail = error
        .get_ref()
        .unwrap()
        .downcast_ref::<PeerChildrenFailure>()
        .unwrap();
    assert!(detail.first.is_none() && detail.second.is_none());
    assert!(detail.exchange_aborted && detail.driver_aborted);
    assert!(!signals.peer_acknowledged.marked() && !signals.native_drain.marked());
}

struct CleanupWake {
    release: Arc<Signal>,
    count: Arc<AtomicUsize>,
}

impl Wake for CleanupWake {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.release.mark();
        panic_any("original cleanup notification panic");
    }
}

fn cleanup_panic_controls(runtime: &Runtime) {
    for notification in [false, true] {
        for operation_panics in [false, true] {
            let mut owner = RuntimeOwner::start(RuntimeConfig {
                workers: 1,
                command_capacity: 64,
            })
            .expect("real cleanup-control worker");
            let handle = owner.handle();
            let worker = handle.worker(0).expect("cleanup-control worker exists");
            let release = Arc::new(Signal::default());
            let drops = Arc::new(AtomicUsize::new(0));
            let wrong_owner = Arc::new(AtomicBool::new(false));
            let (entered, admitted) = oneshot::channel();
            let call = worker
                .call_local({
                    let release = Arc::clone(&release);
                    let drops = Arc::clone(&drops);
                    let wrong_owner = Arc::clone(&wrong_owner);
                    move |_context| async move {
                        let _owned = local_drop(&drops, &wrong_owner);
                        entered
                            .send(())
                            .expect("synchronous cleanup-control root retained");
                        release.wait().await;
                    }
                })
                .expect("actual command admission");
            runtime
                .block_on(admitted)
                .expect("actual local owner installed");
            let wakes = Arc::new(AtomicUsize::new(0));
            let mut observer = Box::pin(worker.quiescing());
            if notification {
                let waker = Waker::from(Arc::new(CleanupWake {
                    release: Arc::clone(&release),
                    count: Arc::clone(&wakes),
                }));
                assert!(
                    observer
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
            }
            let mut retirement = RootRetirement::default();
            if operation_panics {
                retirement.first_panic =
                    catch_unwind(|| panic_any("original operation panic")).err();
            }
            let mut inject_poll_panic = !notification;
            let report = retire_runtime(runtime, &mut owner, &mut retirement, |quiescence| {
                let inject = std::mem::take(&mut inject_poll_panic);
                let release = Arc::clone(&release);
                async move {
                    let mut quiescence = Box::pin(quiescence);
                    let mut inject = inject;
                    poll_fn(|context| {
                        let result = quiescence.as_mut().poll(context);
                        if std::mem::take(&mut inject) {
                            assert!(
                                result.is_pending(),
                                "real cleanup held by admitted local owner"
                            );
                            release.mark();
                            panic_any("original cleanup poll panic");
                        }
                        result
                    })
                    .await
                }
            })
            .expect("explicit physical join survives cleanup panic");
            assert_eq!(report.workers_joined, 1);
            assert_eq!(report.workers_forced, 0);
            assert!(retirement.errors.is_empty(), "{:?}", retirement.errors);
            assert_eq!(retirement.cleanup_panics, 1);
            assert_eq!(
                retirement
                    .first_panic
                    .as_ref()
                    .and_then(|payload| payload.downcast_ref::<&str>()),
                Some(&if operation_panics {
                    "original operation panic"
                } else if notification {
                    "original cleanup notification panic"
                } else {
                    "original cleanup poll panic"
                })
            );
            runtime
                .block_on(call)
                .expect("actual admitted command completion retained");
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(!wrong_owner.load(Ordering::SeqCst));
            assert_eq!(wakes.load(Ordering::SeqCst), usize::from(notification));
            assert!(!worker.is_accepting());
            let repeated = owner.join().expect("sticky actual physical join");
            assert_eq!(repeated.workers_joined, 1);
            assert_eq!(repeated.workers_forced, 0);
            drop(observer);
        }
    }
}

const PEER_IO_CONTROL_BYTES: &[u8] = b"peer-control";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeerIoControlMethod {
    Read,
    Write,
    Vectored,
    Flush,
    Shutdown,
}

impl PeerIoControlMethod {
    fn operation(self) -> Option<PeerWriteOp> {
        match self {
            Self::Read => None,
            Self::Write | Self::Vectored => Some(PeerWriteOp::Write),
            Self::Flush => Some(PeerWriteOp::Flush),
            Self::Shutdown => Some(PeerWriteOp::Shutdown),
        }
    }
}

#[derive(Default)]
struct PeerIoControlWake {
    wakes: AtomicUsize,
}

impl Wake for PeerIoControlWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("original peer I/O control error")]
struct PeerIoControlError {
    drops: Arc<AtomicUsize>,
}

impl Drop for PeerIoControlError {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn peer_io_control_error(drops: &Arc<AtomicUsize>) -> (io::Error, *const PeerIoControlError) {
    let error = io::Error::new(
        io::ErrorKind::ConnectionReset,
        PeerIoControlError {
            drops: Arc::clone(drops),
        },
    );
    let identity = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<PeerIoControlError>())
        .expect("original boxed control error") as *const PeerIoControlError;
    (error, identity)
}

fn check_peer_io_control_error(
    result: Poll<io::Result<usize>>,
    identity: *const PeerIoControlError,
    drops: &AtomicUsize,
) {
    let Poll::Ready(Err(error)) = result else {
        panic!("original delegated error must remain Ready(Err)");
    };
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(error.raw_os_error(), None);
    assert!(std::ptr::eq(
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<PeerIoControlError>())
            .expect("no error wrapping or replacement"),
        identity,
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(error);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct PeerIoControl<'a> {
    method: PeerIoControlMethod,
    result: Option<Poll<io::Result<usize>>>,
    buffer_address: usize,
    vectored: bool,
    waker: Waker,
    signals: &'a Signals,
    polls: usize,
    drops: Arc<AtomicUsize>,
}

impl PeerIoControl<'_> {
    fn poll(
        &mut self,
        method: PeerIoControlMethod,
        context: &Context<'_>,
    ) -> Poll<io::Result<usize>> {
        assert_eq!(method, self.method, "exact delegated operation");
        assert!(context.waker().will_wake(&self.waker));
        drop(
            self.signals
                .progress
                .try_lock()
                .expect("delegate runs outside observation lock"),
        );
        self.polls += 1;
        let result = self
            .result
            .take()
            .expect("one delegate poll per outer poll");
        if result.is_pending() {
            context.waker().wake_by_ref();
        }
        result
    }
}

impl AsyncRead for PeerIoControl<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert_eq!(buffer as *mut ReadBuf<'_> as usize, self.buffer_address);
        assert_eq!(buffer.filled(), b"kept");
        self.poll(PeerIoControlMethod::Read, context)
            .map(|result| result.map(|amount| buffer.put_slice(&PEER_IO_CONTROL_BYTES[..amount])))
    }
}

impl AsyncWrite for PeerIoControl<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        assert_eq!(bytes.as_ptr() as usize, self.buffer_address);
        assert_eq!(bytes, PEER_IO_CONTROL_BYTES);
        self.poll(PeerIoControlMethod::Write, context)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        assert_eq!(buffers.as_ptr() as usize, self.buffer_address);
        assert_eq!(buffers.len(), 2);
        assert_eq!(&*buffers[0], &PEER_IO_CONTROL_BYTES[..2]);
        assert_eq!(&*buffers[1], &PEER_IO_CONTROL_BYTES[2..]);
        self.poll(PeerIoControlMethod::Vectored, context)
    }

    fn is_write_vectored(&self) -> bool {
        assert!(self.signals.progress.try_lock().is_ok());
        self.vectored
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll(PeerIoControlMethod::Flush, context)
            .map(|result| result.map(|_| ()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll(PeerIoControlMethod::Shutdown, context)
            .map(|result| result.map(|_| ()))
    }
}

impl Drop for PeerIoControl<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_peer_io_control(
    stream: &mut PeerIo<'_, PeerIoControl<'_>>,
    context: &mut Context<'_>,
) -> Poll<io::Result<usize>> {
    match stream.inner.method {
        PeerIoControlMethod::Read => {
            let mut bytes = [0; 32];
            let mut buffer = ReadBuf::new(&mut bytes);
            buffer.put_slice(b"kept");
            stream.inner.buffer_address = &mut buffer as *mut ReadBuf<'_> as usize;
            let amount = match stream.inner.result.as_ref() {
                Some(Poll::Ready(Ok(amount))) => *amount,
                _ => 0,
            };
            let result = Pin::new(stream).poll_read(context, &mut buffer);
            assert_eq!(&buffer.filled()[..4], b"kept");
            assert_eq!(&buffer.filled()[4..], &PEER_IO_CONTROL_BYTES[..amount]);
            result.map(|result| result.map(|()| buffer.filled().len() - 4))
        }
        PeerIoControlMethod::Write => {
            stream.inner.buffer_address = PEER_IO_CONTROL_BYTES.as_ptr() as usize;
            Pin::new(stream).poll_write(context, PEER_IO_CONTROL_BYTES)
        }
        PeerIoControlMethod::Vectored => {
            let buffers = [
                IoSlice::new(&PEER_IO_CONTROL_BYTES[..2]),
                IoSlice::new(&PEER_IO_CONTROL_BYTES[2..]),
            ];
            stream.inner.buffer_address = buffers.as_ptr() as usize;
            Pin::new(stream).poll_write_vectored(context, &buffers)
        }
        PeerIoControlMethod::Flush => Pin::new(stream)
            .poll_flush(context)
            .map(|result| result.map(|()| 0)),
        PeerIoControlMethod::Shutdown => Pin::new(stream)
            .poll_shutdown(context)
            .map(|result| result.map(|()| 0)),
    }
}

struct PeerShutdownViaWrite<'a>(PeerIoControl<'a>);

impl AsyncWrite for PeerShutdownViaWrite<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0)
            .poll_write(context, PEER_IO_CONTROL_BYTES)
            .map(|result| result.map(|_| ()))
    }
}

fn peer_io_observation_controls() {
    let methods = [
        PeerIoControlMethod::Read,
        PeerIoControlMethod::Write,
        PeerIoControlMethod::Vectored,
        PeerIoControlMethod::Flush,
        PeerIoControlMethod::Shutdown,
    ];
    for method in methods {
        let signals = Signals {
            observe_h2: true,
            ..Signals::default()
        };
        let wake = Arc::new(PeerIoControlWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let io_drops = Arc::new(AtomicUsize::new(0));
        let mut stream = PeerIo::new(
            PeerIoControl {
                method,
                result: Some(Poll::Pending),
                buffer_address: 0,
                vectored: false,
                waker: waker.clone(),
                signals: &signals,
                polls: 0,
                drops: Arc::clone(&io_drops),
            },
            &signals,
        );
        assert!(!stream.is_write_vectored());
        stream.inner.vectored = true;
        assert!(stream.is_write_vectored());
        assert!(poll_peer_io_control(&mut stream, &mut context).is_pending());
        assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(signals.snapshot(None).state.peer_first_write_error, None);
        let amounts: &[usize] = match method {
            PeerIoControlMethod::Read
            | PeerIoControlMethod::Write
            | PeerIoControlMethod::Vectored => &[0, 3],
            PeerIoControlMethod::Flush | PeerIoControlMethod::Shutdown => &[0],
        };
        for amount in amounts {
            stream.inner.result = Some(Poll::Ready(Ok(*amount)));
            assert!(matches!(
                poll_peer_io_control(&mut stream, &mut context),
                Poll::Ready(Ok(actual)) if actual == *amount
            ));
            assert_eq!(signals.snapshot(None).state.peer_first_write_error, None);
        }
        let error_drops = Arc::new(AtomicUsize::new(0));
        let (error, identity) = peer_io_control_error(&error_drops);
        stream.inner.result = Some(Poll::Ready(Err(error)));
        check_peer_io_control_error(
            poll_peer_io_control(&mut stream, &mut context),
            identity,
            &error_drops,
        );
        assert_eq!(
            signals.snapshot(None).state.peer_first_write_error,
            method.operation()
        );
        assert_eq!(stream.inner.polls, amounts.len() + 2);
        if method.operation().is_some() {
            for later in methods {
                let error_drops = Arc::new(AtomicUsize::new(0));
                let (error, identity) = peer_io_control_error(&error_drops);
                stream.inner.method = later;
                stream.inner.result = Some(Poll::Ready(Err(error)));
                check_peer_io_control_error(
                    poll_peer_io_control(&mut stream, &mut context),
                    identity,
                    &error_drops,
                );
                assert_eq!(
                    signals.snapshot(None).state.peer_first_write_error,
                    method.operation(),
                    "later {later:?} cannot replace first {method:?}"
                );
            }
            assert_eq!(stream.inner.polls, amounts.len() + 2 + methods.len());
        }
        assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
        assert_eq!(io_drops.load(Ordering::SeqCst), 0);
        drop(stream);
        assert_eq!(io_drops.load(Ordering::SeqCst), 1);
    }

    let signals = Signals {
        observe_h2: true,
        ..Signals::default()
    };
    let wake = Arc::new(PeerIoControlWake::default());
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let io_drops = Arc::new(AtomicUsize::new(0));
    let mut stream = PeerIo::new(
        PeerShutdownViaWrite(PeerIoControl {
            method: PeerIoControlMethod::Write,
            result: Some(Poll::Pending),
            buffer_address: PEER_IO_CONTROL_BYTES.as_ptr() as usize,
            vectored: false,
            waker: waker.clone(),
            signals: &signals,
            polls: 0,
            drops: Arc::clone(&io_drops),
        }),
        &signals,
    );
    assert!(
        Pin::new(&mut stream)
            .poll_shutdown(&mut context)
            .is_pending()
    );
    assert_eq!(signals.snapshot(None).state.peer_first_write_error, None);
    stream.inner.0.result = Some(Poll::Ready(Ok(PEER_IO_CONTROL_BYTES.len())));
    assert!(matches!(
        Pin::new(&mut stream).poll_shutdown(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(signals.snapshot(None).state.peer_first_write_error, None);
    let error_drops = Arc::new(AtomicUsize::new(0));
    let (error, identity) = peer_io_control_error(&error_drops);
    stream.inner.0.result = Some(Poll::Ready(Err(error)));
    check_peer_io_control_error(
        Pin::new(&mut stream)
            .poll_shutdown(&mut context)
            .map(|result| result.map(|()| 0)),
        identity,
        &error_drops,
    );
    assert_eq!(
        signals.snapshot(None).state.peer_first_write_error,
        Some(PeerWriteOp::Shutdown),
        "inner write failure belongs to the h2-facing shutdown operation"
    );
    assert_eq!(stream.inner.0.polls, 3);
    assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(io_drops.load(Ordering::SeqCst), 0);
    drop(stream);
    assert_eq!(io_drops.load(Ordering::SeqCst), 1);
}

fn ingress_control_stats(stage: Stage) -> Stats {
    let stats = Stats {
        owner: std::thread::current().id(),
        ledger: RefCell::new(ByteLedger::default()),
        signals: Arc::new(Signals::default()),
    };
    let bound = H1IngressBound::from_session(&native_session_params())
        .expect("actual native session configuration supplies the control bound");
    stats
        .ledger
        .borrow_mut()
        .bind_ingress(
            bound,
            Route {
                h2: false,
                tls: false,
            },
            stage,
        )
        .expect("fresh selected control recorder");
    stats.record_transaction_opened();
    stats
}

fn expect_ingress_violation(stats: &Stats, expected: H1IngressViolation) {
    let mut ledger = stats.ledger.borrow_mut();
    assert!(
        ledger
            .require_ingress(
                Route {
                    h2: false,
                    tls: false
                },
                Stage::BodyDrop
            )
            .is_err()
    );
    assert!(ledger.invalid);
    assert_eq!(ledger.ingress_violation, Some(expected));
}

fn ingress_bound_controls() {
    let route = Route {
        h2: false,
        tls: false,
    };
    let bound = H1IngressBound::from_session(&native_session_params())
        .expect("unchanged actual session parameters");
    assert_eq!(
        bound,
        H1IngressBound {
            flow: 65_535,
            segment: 65_536,
            ceiling: 131_070,
        }
    );
    for candidate_route in ROUTES {
        for stage in STAGES {
            let expected = !candidate_route.h2 && IngressBound::selected(stage);
            assert_eq!(H1IngressBound::selected(candidate_route, stage), expected);
            if !expected {
                let mut ledger = ByteLedger::default();
                if IngressBound::selected(stage) {
                    assert!(ledger.require_ingress(candidate_route, stage).is_err());
                    assert_eq!(
                        ledger.ingress_violation,
                        Some(H1IngressViolation::MissingBinding)
                    );
                } else {
                    ledger
                        .require_ingress(candidate_route, stage)
                        .expect("unselected stages retain the original recorder");
                }
                assert!(ledger.ingress.is_none());
                let mut ledger = ByteLedger::default();
                assert!(ledger.bind_ingress(bound, candidate_route, stage).is_err());
                assert_eq!(
                    ledger.ingress_violation,
                    Some(H1IngressViolation::BindingMismatch)
                );
            }
        }
    }
    for stage in [Stage::BodyDrop, Stage::Release] {
        let stats = ingress_control_stats(stage);
        stats.record_buffered_read_bytes(65_534);
        stats.record_buffered_read_bytes(65_536);
        {
            let mut ledger = stats.ledger.borrow_mut();
            ledger
                .require_ingress(route, stage)
                .expect("exact ceiling is allowed");
            assert_eq!(ledger.read, 131_070);
            assert_eq!(ledger.read_peak, 131_070);
        }
        stats.record_buffered_read_bytes(-131_070);
        stats.record_transaction_closed();
        let mut ledger = stats.ledger.borrow_mut();
        ledger
            .require_ingress(route, stage)
            .expect("large negative credit is not a BODY segment");
        assert_eq!(
            (ledger.read, ledger.read_peak, ledger.opened, ledger.closed),
            (0, 131_070, 1, 1)
        );
        assert!(ledger.bind_ingress(bound, route, stage).is_err());
        assert_eq!(ledger.ingress_violation, Some(H1IngressViolation::NotFresh));
    }

    let excessive = ingress_control_stats(Stage::BodyDrop);
    for delta in [65_534, 65_536, 1] {
        excessive.record_buffered_read_bytes(delta);
    }
    let ceiling = H1IngressViolation::Ceiling {
        peak: 131_071,
        maximum: 131_070,
    };
    expect_ingress_violation(&excessive, ceiling);
    excessive.record_buffered_read_bytes(-131_071);
    assert_eq!(excessive.ledger.borrow().read, 0);
    expect_ingress_violation(&excessive, ceiling);
    excessive.record_buffered_read_bytes(65_537);
    excessive.record_buffered_read_bytes(-65_537);
    excessive.record_transaction_closed();
    expect_ingress_violation(&excessive, ceiling);
    assert_eq!(excessive.ledger.borrow().read_peak, 131_071);

    let segment = ingress_control_stats(Stage::BodyDrop);
    segment.record_buffered_read_bytes(65_537);
    assert!(segment.ledger.borrow().read < 131_070);
    segment.record_buffered_read_bytes(-65_537);
    expect_ingress_violation(
        &segment,
        H1IngressViolation::Segment {
            delta: 65_537,
            maximum: 65_536,
        },
    );

    let underflow = ingress_control_stats(Stage::BodyDrop);
    underflow.record_buffered_read_bytes(-1);
    underflow.record_buffered_read_bytes(1);
    assert_eq!(underflow.ledger.borrow().read, 0);
    expect_ingress_violation(
        &underflow,
        H1IngressViolation::Balance {
            read: true,
            before: 0,
            delta: -1,
        },
    );
    let overflow = ingress_control_stats(Stage::BodyDrop);
    overflow.ledger.borrow_mut().read = i64::MAX;
    overflow.record_buffered_read_bytes(1);
    overflow.record_buffered_read_bytes(-i64::MAX);
    assert_eq!(overflow.ledger.borrow().read, 0);
    expect_ingress_violation(
        &overflow,
        H1IngressViolation::Balance {
            read: true,
            before: i64::MAX,
            delta: 1,
        },
    );
    let received = ingress_control_stats(Stage::BodyDrop);
    received.ledger.borrow_mut().read_received = usize::MAX;
    received.record_buffered_read_bytes(1);
    received.record_buffered_read_bytes(-1);
    expect_ingress_violation(
        &received,
        H1IngressViolation::ReceivedOverflow {
            before: usize::MAX,
            delta: 1,
        },
    );

    let owner = ingress_control_stats(Stage::BodyDrop);
    owner.ledger.borrow_mut().check_owner(false);
    owner.record_buffered_read_bytes(1);
    owner.record_buffered_read_bytes(-1);
    expect_ingress_violation(&owner, H1IngressViolation::WrongOwner);
    let transactions = ingress_control_stats(Stage::BodyDrop);
    transactions.record_transaction_opened();
    transactions.record_transaction_closed();
    transactions.record_transaction_closed();
    expect_ingress_violation(
        &transactions,
        H1IngressViolation::Transactions {
            opened: 2,
            closed: 0,
        },
    );
    let closed = ingress_control_stats(Stage::BodyDrop);
    closed.record_transaction_closed();
    closed.record_transaction_closed();
    expect_ingress_violation(
        &closed,
        H1IngressViolation::Transactions {
            opened: 1,
            closed: 2,
        },
    );

    let mut missing = ByteLedger::default();
    assert!(missing.require_ingress(route, Stage::BodyDrop).is_err());
    assert!(missing.bind_ingress(bound, route, Stage::BodyDrop).is_err());
    assert_eq!(
        missing.ingress_violation,
        Some(H1IngressViolation::MissingBinding)
    );
    let mut stale = ByteLedger {
        read_positive: 1,
        ..ByteLedger::default()
    };
    assert!(stale.bind_ingress(bound, route, Stage::BodyDrop).is_err());
    assert_eq!(stale.ingress_violation, Some(H1IngressViolation::NotFresh));
    for (wrong_route, wrong_stage) in [
        (
            Route {
                h2: true,
                tls: false,
            },
            Stage::BodyDrop,
        ),
        (
            Route {
                h2: false,
                tls: true,
            },
            Stage::BodyDrop,
        ),
        (route, Stage::Release),
        (route, Stage::UploadCancel),
    ] {
        let stats = ingress_control_stats(Stage::BodyDrop);
        assert!(
            stats
                .ledger
                .borrow_mut()
                .require_ingress(wrong_route, wrong_stage)
                .is_err()
        );
        expect_ingress_violation(&stats, H1IngressViolation::BindingMismatch);
    }
    for wrong_bound in [
        H1IngressBound {
            flow: 65_536,
            ..bound
        },
        H1IngressBound {
            segment: 65_537,
            ..bound
        },
        H1IngressBound {
            ceiling: 131_071,
            ..bound
        },
    ] {
        let mut ledger = ByteLedger::default();
        assert!(
            ledger
                .bind_ingress(wrong_bound, route, Stage::BodyDrop)
                .is_err()
        );
        assert_eq!(
            ledger.ingress_violation,
            Some(H1IngressViolation::BindingMismatch)
        );
    }
    let mut wrong_session = native_session_params();
    wrong_session.max_concurrent_outgoing_streams = Some(2);
    assert!(H1IngressBound::from_session(&wrong_session).is_err());

    let idle = ingress_control_stats(Stage::BodyDrop);
    let point = StagePoint {
        stage: Stage::BodyDrop,
        ledger: *idle.ledger.borrow(),
        source: SourceProof::default(),
        data_sent: 0,
        wire_written: 0,
        pressure_count: 0,
    };
    assert!(
        verify_stage(point, route).is_err(),
        "valid zero credit is not pressure"
    );
}

fn ingress_phase_controls() {
    let signals = Signals::default();
    let route = ROUTES[0];
    let limit = UniplexConfig::default().ingress_buffer_limit as usize;
    signals.read_balance.store(limit as i64, Ordering::SeqCst);
    signals
        .read_received
        .store(limit + CHUNK - 1, Ordering::SeqCst);
    assert!(
        !signals.held_ingress_ready(route, CHUNK),
        "positive stale credit cannot certify pause"
    );
    signals.read_received.store(limit + CHUNK, Ordering::SeqCst);
    assert!(
        signals.held_ingress_ready(route, CHUNK),
        "exact received-minus-consumed threshold"
    );
}

fn native_ingress_control_stats(route: Route, stage: Stage) -> Stats {
    let fixture = RouteFixture::new(route).expect("selected control route configuration");
    let bound =
        IngressBound::from_configuration(&native_session_params(), &fixture.connection(), route)
            .expect("actual final constructor inputs");
    let stats = Stats {
        owner: std::thread::current().id(),
        ledger: RefCell::new(ByteLedger::default()),
        signals: Arc::new(Signals::default()),
    };
    stats
        .ledger
        .borrow_mut()
        .bind_native_ingress(bound, route, stage)
        .expect("fresh route-bound control recorder");
    stats.record_transaction_opened();
    stats
}

fn native_ingress_bound_controls() {
    for route in ROUTES {
        let fixture = RouteFixture::new(route).expect("control fixture");
        let session = native_session_params();
        let connection = fixture.connection();
        let bound = IngressBound::from_configuration(&session, &connection, route)
            .expect("fixed effective configuration");
        assert_eq!(bound.ceiling(), if route.h2 { 65_535 } else { 131_070 });
        assert_eq!(bound.segment(), if route.h2 { 16_384 } else { 65_536 });
        for wrong_route in ROUTES.into_iter().filter(|candidate| *candidate != route) {
            assert!(IngressBound::from_configuration(&session, &connection, wrong_route).is_err());
            let stats = native_ingress_control_stats(route, Stage::BodyDrop);
            assert!(
                stats
                    .ledger
                    .borrow_mut()
                    .require_ingress(wrong_route, Stage::BodyDrop)
                    .is_err()
            );
            assert_eq!(
                stats.ledger.borrow().ingress_violation,
                Some(H1IngressViolation::BindingMismatch)
            );
        }
        for stage in [Stage::BodyDrop, Stage::Release] {
            let stats = native_ingress_control_stats(route, stage);
            let mut remaining = bound.ceiling();
            while remaining > 0 {
                let delta = remaining.min(bound.segment());
                stats.record_buffered_read_bytes(delta);
                remaining -= delta;
            }
            stats
                .ledger
                .borrow_mut()
                .require_ingress(route, stage)
                .expect("exact connection ceiling");
            assert_eq!(stats.ledger.borrow().read_peak, bound.ceiling());
            stats.record_buffered_read_bytes(1);
            let original = Some(H1IngressViolation::Ceiling {
                peak: bound.ceiling() + 1,
                maximum: bound.ceiling(),
            });
            assert_eq!(stats.ledger.borrow().ingress_violation, original);
            stats.record_buffered_read_bytes(-bound.ceiling() - 1);
            stats.record_buffered_read_bytes(bound.segment() + 1);
            stats.record_buffered_read_bytes(-bound.segment() - 1);
            stats.record_transaction_closed();
            let mut ledger = stats.ledger.borrow_mut();
            assert_eq!(ledger.read, 0);
            assert_eq!(ledger.ingress_violation, original);
            assert!(ledger.require_ingress(route, stage).is_err());
        }
        let oversized = native_ingress_control_stats(route, Stage::BodyDrop);
        oversized.record_buffered_read_bytes(bound.segment() + 1);
        oversized.record_buffered_read_bytes(-bound.segment() - 1);
        assert_eq!(
            oversized.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::Segment {
                delta: bound.segment() + 1,
                maximum: bound.segment(),
            })
        );
        let underflow = native_ingress_control_stats(route, Stage::BodyDrop);
        underflow.record_buffered_read_bytes(-1);
        underflow.record_buffered_read_bytes(1);
        assert_eq!(
            underflow.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::Balance {
                read: true,
                before: 0,
                delta: -1,
            })
        );
        let overflow = native_ingress_control_stats(route, Stage::BodyDrop);
        overflow.ledger.borrow_mut().read = i64::MAX;
        overflow.record_buffered_read_bytes(1);
        assert_eq!(
            overflow.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::Balance {
                read: true,
                before: i64::MAX,
                delta: 1,
            })
        );
        let received = native_ingress_control_stats(route, Stage::BodyDrop);
        received.ledger.borrow_mut().read_received = usize::MAX;
        received.record_buffered_read_bytes(1);
        assert_eq!(
            received.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::ReceivedOverflow {
                before: usize::MAX,
                delta: 1,
            })
        );
        let owner = native_ingress_control_stats(route, Stage::BodyDrop);
        owner.ledger.borrow_mut().check_owner(false);
        owner.record_buffered_read_bytes(1);
        assert_eq!(
            owner.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::WrongOwner)
        );
        let transactions = native_ingress_control_stats(route, Stage::BodyDrop);
        transactions.record_transaction_opened();
        assert_eq!(
            transactions.ledger.borrow().ingress_violation,
            Some(H1IngressViolation::Transactions {
                opened: 2,
                closed: 0,
            })
        );
        let mut missing = ByteLedger::default();
        assert!(missing.require_ingress(route, Stage::BodyDrop).is_err());
        assert_eq!(
            missing.ingress_violation,
            Some(H1IngressViolation::MissingBinding)
        );
        let mut stale = ByteLedger {
            read_positive: 1,
            ..ByteLedger::default()
        };
        assert!(
            stale
                .bind_native_ingress(bound, route, Stage::BodyDrop)
                .is_err()
        );
        assert_eq!(stale.ingress_violation, Some(H1IngressViolation::NotFresh));
        let idle = native_ingress_control_stats(route, Stage::BodyDrop);
        assert!(
            verify_stage(
                StagePoint {
                    stage: Stage::BodyDrop,
                    ledger: *idle.ledger.borrow(),
                    source: SourceProof::default(),
                    data_sent: 0,
                    wire_written: 0,
                    pressure_count: 0,
                },
                route
            )
            .is_err()
        );
        let mut changed = native_session_params();
        changed.max_concurrent_outgoing_streams = Some(2);
        assert!(IngressBound::from_configuration(&changed, &connection, route).is_err());
        if route.h2 {
            for (identifier, value) in [(4, 32_769), (5, 16_385)] {
                let mut changed = native_session_params();
                changed.settings.push(CodecSetting { identifier, value });
                assert!(IngressBound::from_configuration(&changed, &connection, route).is_err());
            }
            let mut changed = native_session_params();
            changed.connection_flow_control = Some(65_536);
            assert!(IngressBound::from_configuration(&changed, &connection, route).is_err());
        }
    }
}

struct IngressRejectedCredit(Rc<Stats>);

impl HttpStreamSourceCallback for IngressRejectedCredit {
    fn bytes_processed(&mut self, stream: StreamId, amount: usize, _acknowledge: usize) {
        assert_eq!(stream, StreamId::new(1));
        self.0
            .record_buffered_read_bytes(-i64::try_from(amount).expect("bounded discarded credit"));
    }
}

async fn h2_ingress_reject_balance_control() {
    for route in ROUTES.into_iter().filter(|route| route.h2) {
        let stats = Rc::new(native_ingress_control_stats(route, Stage::BodyDrop));
        let mut source = HttpStreamSource::new_session(
            StreamId::new(1),
            IngressRejectedCredit(Rc::clone(&stats)),
            STREAM_WINDOW,
            STAGE_BOUND,
        )
        .expect("real strict stream source");
        let producer = source.producer();
        let mut headers = HttpMessage::default();
        headers.set_status_code(200).expect("response status");
        assert!(producer.headers(headers, false));
        source
            .read_header_event()
            .await
            .expect("actual response head");
        for (count, expected) in [
            (16_384, FlowControlState::Open),
            (16_384, FlowControlState::Closed),
            (1, FlowControlState::Error),
        ] {
            let mut body = BufQueue::new(count);
            body.push_bytes(pattern(0, count))
                .expect("bounded control DATA");
            stats.record_buffered_read_bytes(i64::try_from(count).expect("DATA length"));
            assert_eq!(producer.body(body, 0, false), expected);
            if expected == FlowControlState::Error {
                stats.record_buffered_read_bytes(
                    -i64::try_from(count).expect("rejected DATA length"),
                );
            }
        }
        let error = match source.read_body_event(CHUNK as u32).await {
            Err(error) => error,
            Ok(_) => panic!("stream window violation must remain a real source error"),
        };
        assert_eq!(error.code(), HttpErrorCode::FLOW_CONTROL_ERROR);
        stats.record_transaction_closed();
        let mut ledger = stats.ledger.borrow_mut();
        ledger
            .require_ingress(route, Stage::BodyDrop)
            .expect("raw stream rejection is below connection ceiling");
        assert_eq!(
            (ledger.read_peak, ledger.read, ledger.read_received),
            (32_769, 0, 32_769)
        );
        assert_eq!((ledger.opened, ledger.closed), (1, 1));
        assert_eq!(ledger.ingress_violation, None);
    }
}

fn h1_action_error_controls() {
    for (stage, other, code, message) in [
        (
            Stage::UploadCancel,
            Stage::FutureDrop,
            HttpErrorCode::CORO_CANCELLED,
            "request body production cancelled",
        ),
        (
            Stage::FutureDrop,
            Stage::UploadCancel,
            HttpErrorCode::TRANSPORT_READ_ERROR,
            "byte-stream was reset",
        ),
    ] {
        let original = HttpError::with_message(code, message);
        assert!(h1_action_error(stage, &original));
        native_terminal(Err(original.clone()), ROUTES[0], stage)
            .expect("actual action-specific Run predicate accepts its original error");
        assert!(!h1_action_error(other, &original));
        assert!(!h1_action_error(Stage::BodyDrop, &original));
        assert!(!h1_action_error(Stage::Release, &original));
        let mismatch = native_terminal(Err(original.clone()), ROUTES[0], other)
            .expect_err("swapped U/F Run result must fail");
        assert!(mismatch.to_string().contains(&format!("code={code:?}")));
        let mut wrong_code = original.clone();
        wrong_code.set_code(HttpErrorCode::READ_TIMEOUT);
        assert!(!h1_action_error(stage, &wrong_code));
        let mut wrong_message = original.clone();
        wrong_message.set_message(b"different native error");
        assert!(!h1_action_error(stage, &wrong_message));
        let unexpected_cause = original.with_cause(Arc::new(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "unexpected retained I/O cause",
        )));
        assert!(!h1_action_error(stage, &unexpected_cause));
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AccountingIngressBytes {
    read: u64,
    written: u64,
}

struct AccountingIngressTrace {
    owner: ThreadId,
    confirmed: bool,
    released: Cell<bool>,
    gated: Signal,
    ingress: Signal,
    read_waker: RefCell<Option<Waker>>,
    io: RefCell<[AccountingIngressBytes; 2]>,
    lifecycle: RefCell<AccountingIngressBytes>,
    io_errors: Cell<usize>,
    summary: Cell<Option<UniplexRunSummary>>,
    oracle_passed: Cell<Option<bool>>,
}

impl AccountingIngressTrace {
    fn observe<Output>(&self, result: &Poll<io::Result<Output>>) {
        assert_eq!(self.owner, std::thread::current().id());
        if matches!(result, Poll::Ready(Err(_))) {
            self.io_errors.set(self.io_errors.get() + 1);
        }
    }
}

impl LifecycleObserver for AccountingIngressTrace {
    fn on_event(&self, event: LifecycleEvent) {
        assert_eq!(self.owner, std::thread::current().id());
        match event {
            LifecycleEvent::Http2Read { bytes, .. } => {
                self.lifecycle.borrow_mut().read += bytes as u64;
                if bytes > 0 {
                    self.ingress.mark();
                }
            }
            LifecycleEvent::Http2Write { bytes } => {
                self.lifecycle.borrow_mut().written += bytes as u64;
            }
            _ => {}
        }
    }
}

struct AccountingIngressIo {
    inner: ControlIo,
    trace: Rc<AccountingIngressTrace>,
    native: bool,
}

impl AsyncRead for AccountingIngressIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        assert_eq!(self.trace.owner, std::thread::current().id());
        if self.native && !self.trace.released.get() {
            *self.trace.read_waker.borrow_mut() = Some(context.waker().clone());
            self.trace.gated.mark();
            return Poll::Pending;
        }
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        self.trace.observe(&result);
        if matches!(&result, Poll::Ready(Ok(()))) {
            self.trace.io.borrow_mut()[usize::from(!self.native)].read +=
                (buffer.filled().len() - before) as u64;
        }
        result
    }
}

impl AsyncWrite for AccountingIngressIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(context, bytes);
        self.trace.observe(&result);
        if let Poll::Ready(Ok(amount)) = &result {
            self.trace.io.borrow_mut()[usize::from(!self.native)].written += *amount as u64;
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(context);
        self.trace.observe(&result);
        result
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_shutdown(context);
        self.trace.observe(&result);
        result
    }
}

async fn accounting_ingress_native(
    session: ClientSession,
    signals: Arc<Signals>,
    trace: Rc<AccountingIngressTrace>,
    stats: Rc<Stats>,
) -> io::Result<()> {
    let route = Route {
        h2: true,
        tls: false,
    };
    let proof = Rc::new(RefCell::new(SourceProof::default()));
    let mut suffix = None;
    let operation_deadline = tokio::time::Instant::now() + STAGE_BOUND;
    let cleanup_deadline = operation_deadline + STAGE_BOUND;
    let operation = async {
        let (source, held) = upload_source(
            Stage::UploadCancel,
            route,
            Rc::clone(&proof),
            Arc::clone(&signals),
        )?;
        suffix = held;
        let cancellation = CancellationSource::new();
        let mut request = SourceRequest::new(source);
        request.cancellation = cancellation.token();
        request.maximum_response_body = CHUNK;
        let mut response = HttpClient::request_source(&session, request);
        let checkpoint = async {
            signals.wait(&signals.upload).await?;
            signals.wait(&signals.upload_pending).await?;
            signals.wait(&trace.gated).await
        };
        tokio::select! {
            biased;
            result = &mut response => return Err(failure(format!(
                "accounting response completed before checkpoint: {:?}",
                result.map(|_| ()),
            ))),
            result = checkpoint => result?,
        }
        still_pending(response.as_mut()).await?;
        let source = *proof.borrow();
        require(
            source.constructed == 1
                && source.live_reads > 0
                && source.stops == 0
                && source.drops == 0
                && source.state_drops == 0
                && source.terminal == 0
                && !source.wrong_owner
                && !source.premature
                && suffix.as_ref().is_some_and(|held| !held.is_closed()),
            "actual unfinished upload ownership at accounting checkpoint",
        )?;
        require(
            trace.io.borrow()[0].read == 0
                && trace.lifecycle.borrow().read == 0
                && trace.io.borrow()[1].written > 0
                && !trace.released.get(),
            "peer SETTINGS writes precede any delivered native HTTP read",
        )?;
        if trace.confirmed {
            trace.released.set(true);
            let waker = trace.read_waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
            signals.wait(&trace.ingress).await?;
            require(
                trace.io.borrow()[0].read > 0 && trace.lifecycle.borrow().read > 0,
                "positive delegated and native lifecycle ingress before cancellation",
            )?;
            still_pending(response.as_mut()).await?;
        }
        require(
            proof.borrow().live_reads > 0
                && proof.borrow().stops == 0
                && proof.borrow().drops == 0
                && proof.borrow().terminal == 0
                && suffix.as_ref().is_some_and(|held| !held.is_closed()),
            "unreleased suffix and live source read remain held at the actual action",
        )?;
        eprintln!(
            "native_accounting_control confirmed={} before_action io={:?} lifecycle={:?} source={:?}",
            trace.confirmed,
            trace.io.borrow(),
            trace.lifecycle.borrow(),
            proof.borrow(),
        );
        signals.action.mark();
        cancellation.request_cancellation();
        let error = response
            .await
            .err()
            .ok_or_else(|| failure("accounting control cancellation succeeded"))?;
        eprintln!(
            "native_accounting_control confirmed={} actual_request_error={error:?}",
            trace.confirmed,
        );
        require(
            error.code() == HttpErrorCode::CORO_CANCELLED,
            "actual accounting control request returns CORO_CANCELLED",
        )?;
        signals.native_operation_done.mark();
        signals.wait(&signals.peer_acknowledged).await
    };
    let result = AssertUnwindSafe(timeout_at(operation_deadline, operation))
        .catch_unwind()
        .await;
    let mut errors = Vec::new();
    let mut panic = None;
    match result {
        Ok(Ok(result)) => retain(&mut errors, "accounting operation", result),
        Ok(Err(error)) => {
            retain(
                &mut errors,
                "accounting operation deadline",
                Err(failure(error)),
            );
        }
        Err(payload) => panic = Some(payload),
    }
    if errors.is_empty() && panic.is_none() {
        signals.native_drain.mark();
        retain(
            &mut errors,
            "accounting drain admission",
            session.initiate_drain().map(|_| ()).map_err(failure),
        );
    }
    if !errors.is_empty() || panic.is_some() {
        signals.failed.mark();
        session.cancel();
    }
    let (closed, retired) = tokio::join!(
        timeout_at(cleanup_deadline, session.wait_closed()),
        timeout_at(cleanup_deadline, async {
            published_retirement(&signals)
                .await?
                .wait_retired()
                .await
                .map_err(failure)
        }),
    );
    let summary = match closed {
        Ok(Ok(summary)) => Some(summary),
        Ok(Err(error)) => {
            retain(
                &mut errors,
                "accounting session result",
                Err(failure(error)),
            );
            None
        }
        Err(error) => {
            session.cancel();
            retain(
                &mut errors,
                "accounting session deadline",
                Err(failure(error)),
            );
            None
        }
    };
    trace.summary.set(summary);
    retain(
        &mut errors,
        "accounting transport retirement",
        retired.map_err(failure).and_then(|result| result),
    );
    if let Some(summary) = summary {
        let repeated = timeout_at(cleanup_deadline, session.wait_closed())
            .await
            .map_err(failure)
            .and_then(|result| result.map_err(failure));
        retain(
            &mut errors,
            "sticky actual accounting summary",
            repeated.and_then(|repeated| require(repeated == summary, "identical published tuple")),
        );
        let bytes = trace.io.borrow()[0];
        retain(
            &mut errors,
            "independent accounting totals",
            require(
                summary.bytes_read == bytes.read
                    && summary.bytes_written == bytes.written
                    && summary.bytes_scheduled == bytes.written
                    && bytes.written > 0
                    && (bytes.read > 0) == trace.confirmed
                    && *trace.lifecycle.borrow() == bytes
                    && !summary.reset
                    && trace.io_errors.get() == 0,
                "actual native tuple equals delegated I/O and lifecycle totals",
            ),
        );
    }
    retain(
        &mut errors,
        "accounting session completion",
        require(
            session.is_finished() && session.active_transactions() == 0,
            "accounting session and all transactions finished",
        ),
    );
    drop(session);
    retain(
        &mut errors,
        "held accounting suffix retirement",
        require(
            suffix.as_ref().is_some_and(oneshot::Sender::is_closed),
            "suffix receiver retired without sender release",
        ),
    );
    drop(suffix);
    let source = *proof.borrow();
    let ledger = *stats.ledger.borrow();
    eprintln!(
        "native_accounting_control confirmed={} summary={summary:?} io={:?} lifecycle={:?} source={source:?} ledger={ledger:?} cleanup_errors={errors:?}",
        trace.confirmed,
        trace.io.borrow(),
        trace.lifecycle.borrow(),
    );
    retain(
        &mut errors,
        "accounting source retirement",
        require(
            source.constructed == 1
                && source.polled > 0
                && source.stops == 1
                && source.terminal == 0
                && source.drops == 1
                && source.state_drops == 1
                && source.live_reads == 0
                && source.read_drops > 0
                && !source.wrong_owner
                && !source.premature,
            "unfinished source STOP and owner-local source/read destruction exactly once",
        ),
    );
    retain(
        &mut errors,
        "accounting logical ledger retirement",
        require(
            !ledger.invalid
                && ledger.read == 0
                && ledger.write == 0
                && ledger.opened == 1
                && ledger.closed == 1
                && ledger.write_positive > 0,
            "logical buffer and transaction ledger retires independently of transport totals",
        ),
    );
    if let Some(payload) = panic {
        eprintln!("accounting control cleanup after panic: {errors:?}");
        resume_unwind(payload);
    }
    finish(errors)?;
    let summary = summary.ok_or_else(|| failure("missing actual accounting summary"))?;
    let terminal = native_terminal(Ok(summary), route, Stage::UploadCancel);
    trace.oracle_passed.set(Some(terminal.is_ok()));
    eprintln!(
        "native_accounting_control confirmed={} actual_tuple={summary:?} unchanged_native_oracle={terminal:?}",
        trace.confirmed,
    );
    terminal
}

async fn accounting_ingress_case(
    context: WorkerContext,
    confirmed: bool,
    drops: Arc<AtomicUsize>,
    wrong_owner: Arc<AtomicBool>,
) -> io::Result<()> {
    require(
        context.is_owner_thread(),
        "accounting control native worker",
    )?;
    let retirement_deadline = tokio::time::Instant::now() + 3 * STAGE_BOUND;
    let signals = Arc::new(Signals {
        observe_h2: true,
        ..Signals::default()
    });
    let trace = Rc::new(AccountingIngressTrace {
        owner: std::thread::current().id(),
        confirmed,
        released: Cell::new(false),
        gated: Signal::default(),
        ingress: Signal::default(),
        read_waker: RefCell::new(None),
        io: RefCell::new([AccountingIngressBytes::default(); 2]),
        lifecycle: RefCell::new(AccountingIngressBytes::default()),
        io_errors: Cell::new(0),
        summary: Cell::new(None),
        oracle_passed: Cell::new(None),
    });
    let (client_io, peer_io) = tokio::io::duplex(4 * CHUNK);
    let client_io = AccountingIngressIo {
        inner: ControlIo {
            stream: client_io,
            _owned: local_drop(&drops, &wrong_owner),
        },
        trace: Rc::clone(&trace),
        native: true,
    };
    let peer_io = AccountingIngressIo {
        inner: ControlIo {
            stream: peer_io,
            _owned: local_drop(&drops, &wrong_owner),
        },
        trace: Rc::clone(&trace),
        native: false,
    };
    let (transport, adopted) = adopt_local_with_completion(
        &context,
        EstablishedIo::new(
            client_io,
            TransportEndpoint::Unknown,
            TransportEndpoint::Unknown,
            None,
        ),
        ByteStreamLimits::new(4 * CHUNK, 4 * CHUNK, 8).map_err(failure)?,
    )
    .map_err(failure)?;
    let stats = Rc::new(Stats {
        owner: std::thread::current().id(),
        ledger: RefCell::new(ByteLedger::default()),
        signals: Arc::clone(&signals),
    });
    let mut options = ClientAttachOptions::new(CodecProtocol::Http2, TransportSecurity::Plaintext);
    options.session = SessionParams {
        settings: vec![CodecSetting {
            identifier: 4,
            value: STREAM_WINDOW.into(),
        }],
        connection_flow_control: Some(CONNECTION_WINDOW),
        max_concurrent_outgoing_streams: Some(1),
        stream_read_timeout: Some(STAGE_BOUND),
        connection_read_timeout: Some(STAGE_BOUND),
        write_timeout: Some(STAGE_BOUND),
        session_stats: Some(stats.clone()),
        lifecycle_observer: Some(trace.clone()),
        ..SessionParams::default()
    };
    let session = ClientSession::attach_transport(&context, transport, options).map_err(failure)?;
    *signals.retirement.lock().map_err(failure)? = Some(
        session
            .transport_retirement()
            .ok_or_else(|| failure("attached native transport retirement receipt"))?,
    );
    signals.retirement_published.mark();
    let (publication, checked) = oneshot::channel();
    *signals.checked_native.lock().map_err(failure)? = Some(checked);
    let native = async {
        let result = AssertUnwindSafe(publish_checked_native(
            accounting_ingress_native(session, Arc::clone(&signals), Rc::clone(&trace), stats),
            publication,
        ))
        .catch_unwind()
        .await;
        if !matches!(&result, Ok(Ok(()))) {
            signals.failed.mark();
        }
        result
    };
    let peer = AssertUnwindSafe(gated_accounting_h2_peer(peer_io, &signals)).catch_unwind();
    let (native, peer) = tokio::join!(native, peer);
    let mut panics = RootRetirement::default();
    let native = match native {
        Ok(result) => Some(result),
        Err(payload) => {
            panics.record_cleanup_panic(payload);
            None
        }
    };
    let peer = match peer {
        Ok(result) => Some(result),
        Err(payload) => {
            panics.record_cleanup_panic(payload);
            None
        }
    };
    if panics.first_panic.is_some() || trace.oracle_passed.get().is_none() {
        adopted.cancel();
    }
    let retired = timeout_at(retirement_deadline, adopted.completion().wait_retired())
        .await
        .map_err(failure)
        .and_then(|result| result.map_err(failure));
    eprintln!(
        "native_accounting_control confirmed={confirmed} tuple={:?} native={native:?} checked_peer={peer:?} retirement={retired:?} io_drops={} wrong_owner={}",
        trace.summary.get(),
        drops.load(Ordering::SeqCst),
        wrong_owner.load(Ordering::SeqCst),
    );
    if let Some(payload) = panics.first_panic {
        resume_unwind(payload);
    }
    retired?;
    require(
        drops.load(Ordering::SeqCst) == 2 && !wrong_owner.load(Ordering::SeqCst),
        "both actual I/O owners destroyed once on their native worker",
    )?;
    require(
        trace.oracle_passed.get() == Some(confirmed)
            && trace.released.get() == confirmed
            && signals.peer_acknowledged.marked()
            && signals.native_operation_done.marked()
            && signals.native_drain.marked(),
        "exact accounting arm and real cancellation/acknowledgement/drain milestones",
    )?;
    if confirmed {
        native.ok_or_else(|| failure("missing positive native result"))??;
        peer.ok_or_else(|| failure("missing positive checked peer result"))??;
    } else {
        require(
            matches!(native, Some(Err(_)))
                && peer.as_ref().is_some_and(|result| {
                    result.as_ref().err().is_some_and(|error| {
                        matches!(
                            error
                                .get_ref()
                                .and_then(|error| error.downcast_ref::<NativeReleaseError>()),
                            Some(NativeReleaseError::Rejected(CheckedNativeOutcome::Failed)),
                        )
                    })
                }),
            "negative control retains native accounting failure and actual Rejected(Failed)",
        )?;
    }
    Ok(())
}

#[test]
fn native_staged_accounting_ingress_controls() {
    let _serial = SERIAL
        .lock()
        .expect("serialized accounting ingress controls");
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("accounting control root reactor");
    for confirmed in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let wrong_owner = Arc::new(AtomicBool::new(false));
        let mut owner = RuntimeOwner::start(RuntimeConfig {
            workers: 1,
            command_capacity: 64,
        })
        .expect("accounting control worker starts");
        let operation = catch_unwind(AssertUnwindSafe(|| -> io::Result<()> {
            let worker = owner
                .handle()
                .worker(0)
                .ok_or_else(|| failure("accounting control worker missing"))?;
            let io_drops = Arc::clone(&drops);
            let io_wrong_owner = Arc::clone(&wrong_owner);
            let call = worker
                .call_local(move |context| {
                    accounting_ingress_case(context, confirmed, io_drops, io_wrong_owner)
                })
                .map_err(failure)?;
            runtime.block_on(async {
                timeout(ROUTE_BOUND, call)
                    .await
                    .map_err(failure)?
                    .map_err(failure)?
            })
        }));
        let mut retirement = RootRetirement::default();
        match operation {
            Ok(result) => retain(&mut retirement.errors, "accounting control arm", result),
            Err(payload) => retirement.first_panic = Some(payload),
        }
        let quiesced = catch_unwind(AssertUnwindSafe(|| {
            let quiescence = owner.handle().quiesce(QUIESCE_BOUND);
            runtime.block_on(quiescence)
        }));
        match quiesced {
            Ok(result) => retain(
                &mut retirement.errors,
                "accounting worker quiescence",
                result.map_err(failure),
            ),
            Err(payload) => retirement.record_cleanup_panic(payload),
        }
        let joined = match catch_unwind(AssertUnwindSafe(|| owner.shutdown_and_join())) {
            Ok(Ok(receipt)) => {
                retain(
                    &mut retirement.errors,
                    "accounting physical worker join",
                    require(
                        receipt.report.workers_joined == 1 && receipt.report.workers_forced == 0,
                        "one actual joined worker and zero forced workers",
                    ),
                );
                for worker_failure in &receipt.failures {
                    retain(
                        &mut retirement.errors,
                        "accounting worker retirement failure",
                        Err(failure(format!(
                            "worker {}: {:?}",
                            worker_failure.worker, worker_failure.error,
                        ))),
                    );
                }
                Some(receipt)
            }
            Ok(Err(error)) => {
                retain(
                    &mut retirement.errors,
                    "accounting explicit retirement error",
                    Err(failure(error)),
                );
                None
            }
            Err(payload) => {
                retirement.record_cleanup_panic(payload);
                None
            }
        };
        if joined.is_some() {
            retain(
                &mut retirement.errors,
                "post-join accounting I/O owners",
                require(
                    drops.load(Ordering::SeqCst) == 2 && !wrong_owner.load(Ordering::SeqCst),
                    "two owner-local I/O destructions still confirmed after actual worker join",
                ),
            );
        }
        eprintln!(
            "native_accounting_control confirmed={confirmed} physical_retirement_report={joined:?} errors={:?}",
            retirement.errors,
        );
        if let Some(payload) = retirement.first_panic {
            resume_unwind(payload);
        }
        finish(retirement.errors).expect("fixed accounting arm and physical ownership retirement");
    }
}

#[test]
fn native_staged_fixture_failure_controls() {
    let _serial = SERIAL.lock().expect("serialized fixture failure controls");
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("control reactor");
    ingress_bound_controls();
    ingress_phase_controls();
    native_ingress_bound_controls();
    retention_literal_controls()
        .expect("fixed retention admission, slots and first-cause controls");
    runtime.block_on(async {
        timeout(STAGE_BOUND, h2_ingress_reject_balance_control())
            .await
            .expect("bounded actual stream rejection and ordered callback contrast");
    });
    h1_action_error_controls();
    h1_finalization_controls(&runtime);
    peer_io_observation_controls();
    runtime
        .block_on(async { timeout(STAGE_BOUND, peer_failure_controls()).await })
        .expect("bounded inline failure controls");
    runtime
        .block_on(async { timeout(STAGE_BOUND, controlled_peer_failure_controls()).await })
        .expect("bounded controller retirement and original-error controls");
    for control in [
        ResetControl::Cancel,
        ResetControl::OtherReason,
        ResetControl::Reasonless,
        ResetControl::ConnectionError,
    ] {
        runtime
            .block_on(async { timeout(STAGE_BOUND, h2_reset_control(control)).await })
            .expect("bounded actual H2 reset controls");
    }
    cleanup_panic_controls(&runtime);
}

#[test]
fn native_staged_cancellation_and_pressure() {
    run(Schedule::Stages)
        .expect("all four native routes retire staged cancellation and real pressure");
}

#[test]
fn native_trusted_context() {
    run(Schedule::Context)
        .expect("actual native connection facts remain separate from command context");
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "process-wide FD/thread checkpoints require per-test process isolation (one test per process)"
)]
fn native_alive_owner_resources() {
    run(Schedule::Resources)
        .expect("fixed warmed native worker schedule retains strict alive-owner FD/thread bounds");
}

const RETENTION_WARMUPS: usize = 2;
const RETENTION_MEASURED: usize = 8;
const RETENTION_BACKING: usize = 8192;
const RETENTION_OUTPUT_BOUND: usize = 256 * 1024;
const RETENTION_CHILD_MARKER: &str = "ROXYGEN_NATIVE_CLIENT_RETENTION_CHILD";
const RETENTION_CHILD_BUDGET: &str = "ROXYGEN_NATIVE_CLIENT_RETENTION_REMAINING_NS";
const RETENTION_CHILD_ARGS: [&str; 4] = [
    "native_staged_cleanup_tests::native_envelope_retention_matrix",
    "--exact",
    "--nocapture",
    "--test-threads=1",
];
type RetentionFailure = Box<dyn std::error::Error + Send + Sync>;
type RetentionResult<T> = Result<T, RetentionFailure>;

#[derive(Debug, Error)]
enum RetentionError {
    #[error("retention allocation growth at capsule {index}: {actual} > {baseline}")]
    Growth {
        index: usize,
        baseline: usize,
        actual: usize,
    },
    #[error("all eight completed retention capsules are required")]
    MissingMeasurements,
    #[error("fixed absolute client retention parent deadline expired")]
    Deadline,
    #[error("retention child exited with {0}")]
    Exit(ExitStatus),
    #[error("retention holder cleanup failed after {primary}")]
    HolderCleanup {
        #[source]
        primary: RetentionFailure,
        cleanup: Vec<RetentionFailure>,
    },
    #[error("retention holder panicked during cleanup")]
    HolderPanic { payload: Mutex<Box<dyn Any + Send>> },
    #[error("retention route panicked")]
    RoutePanic { payload: Mutex<Box<dyn Any + Send>> },
    #[error("retention measurements failed for {route:?}: {source}")]
    Measurements {
        #[source]
        source: RetentionFailure,
        route: Route,
        allocator_description: String,
        sensitivity: RetentionSensitivity,
        baseline: usize,
        warmed: Resources,
        observations: Box<[Option<RetentionMeasurement>; RETENTION_MEASURED]>,
    },
    #[error("retention child {route:?} failed: {primary}")]
    Child {
        #[source]
        primary: RetentionFailure,
        route: Route,
        pid: u32,
        status: Option<ExitStatus>,
        wait_state: RetentionWaitState,
        pidfd_confirmed: bool,
        forced: bool,
        cleanup: Vec<RetentionFailure>,
        stdout: String,
        stderr: String,
    },
    #[error("client retention matrix first failed at route index {first_failure}: {outcomes:?}")]
    Matrix {
        first_failure: usize,
        outcomes: Vec<(Route, RetentionResult<()>)>,
    },
}

#[derive(Clone, Copy, Debug)]
struct RetentionStage {
    stage: Stage,
    owner: ThreadId,
    held: Resources,
    retired: Resources,
    read_peak: i64,
    received: usize,
    response: usize,
}

#[derive(Clone, Copy, Debug)]
struct RetentionHeld {
    route: Route,
    stages: [RetentionStage; 4],
}

impl RetentionHeld {
    fn from_observations(route: Route, observations: &[CaseObservation]) -> io::Result<Self> {
        require(
            observations.len() == STAGES.len(),
            "exactly one four-stage retention capsule",
        )?;
        let mut stages = [None; 4];
        for (index, observation) in observations.iter().enumerate() {
            require(
                observation.stage.stage == STAGES[index]
                    && observation.native.owner == observations[0].native.owner
                    && observation.native.owner != std::thread::current().id(),
                "one actual native worker and original stage order per capsule",
            )?;
            stages[index] = Some(RetentionStage {
                stage: observation.stage.stage,
                owner: observation.native.owner,
                held: observation.pressure_resources,
                retired: observation.retired_resources,
                read_peak: observation.native.ledger.read_peak,
                received: observation.native.ledger.read_received,
                response: observation.native.response_bytes,
            });
        }
        Ok(Self {
            route,
            stages: stages.map(|stage| stage.expect("all four checked observations are present")),
        })
    }
}

fn retention_capsule(route: Route) -> RetentionResult<RetentionHeld> {
    let capsule = std::thread::Builder::new().spawn(move || -> io::Result<_> {
        let kernel_task = fs::read_link("/proc/thread-self")?;
        let environment = Environment::for_route(route)?;
        let observed = run_route_observed(
            &environment,
            &environment.fixtures[0],
            Schedule::Stages,
            |observations| RetentionHeld::from_observations(route, observations),
        );
        drop(environment);
        Ok((kernel_task, observed?))
    })?;
    let (kernel_task, held) = match capsule.join() {
        Ok(result) => result?,
        Err(payload) => resume_unwind(payload),
    };
    // pthread_join can return before the kernel removes the task's proc entry.
    let path = Path::new("/proc").join(kernel_task);
    let started = Instant::now();
    while path.try_exists()? {
        require(
            started.elapsed() < STAGE_BOUND,
            "joined retention capsule leaves the kernel task table",
        )?;
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(held)
}

fn retention_rss_field(status: &str, name: &str) -> RetentionResult<u64> {
    let mut matches = status.lines().filter_map(|line| line.strip_prefix(name));
    let value = matches
        .next()
        .ok_or_else(|| failure("required retention RSS field"))?;
    require(matches.next().is_none(), "unique retention RSS field")?;
    let mut fields = value.split_ascii_whitespace();
    let number = fields
        .next()
        .ok_or_else(|| failure("retention RSS value"))?;
    require(
        !number.is_empty()
            && number.bytes().all(|byte| byte.is_ascii_digit())
            && fields.next() == Some("kB")
            && fields.next().is_none(),
        "unsigned retention RSS with Linux kB units",
    )?;
    Ok(number
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or_else(|| failure("retention RSS conversion overflow"))?)
}

fn retention_resources() -> RetentionResult<Resources> {
    require(
        tokio::runtime::Handle::try_current().is_err(),
        "retention sampling outside async execution",
    )?;
    let mut buffer = [0; PROC_BOUND as usize + 1];
    let mut file = fs::File::open("/proc/self/status")?;
    let mut used = 0;
    loop {
        require(used < buffer.len(), "bounded retention proc status")?;
        match file.read(&mut buffer[used..]) {
            Ok(0) => break,
            Ok(count) => used += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    drop(file);
    let text = std::str::from_utf8(&buffer[..used])?;
    let rss_bytes = retention_rss_field(text, "VmRSS:")?;
    let process_hwm_bytes = retention_rss_field(text, "VmHWM:")?;
    let descriptors = directory_count("/proc/self/fd")?
        .checked_sub(1)
        .ok_or_else(|| failure("retention sampler descriptor"))?;
    let threads = directory_count("/proc/self/task")?;
    require(
        rss_bytes > 0 && process_hwm_bytes >= rss_bytes && descriptors > 0 && threads > 0,
        "positive actual retention resources and same-snapshot high water",
    )?;
    Ok(Resources {
        descriptors,
        threads,
        rss_bytes,
        process_hwm_bytes,
    })
}

type AllocatedBytes = unsafe extern "C" fn() -> usize;
type Mallctl = unsafe extern "C" fn(
    *const libc::c_char,
    *mut libc::c_void,
    *mut usize,
    *mut libc::c_void,
    usize,
) -> libc::c_int;

enum RetentionAllocator {
    Sanitizer(AllocatedBytes),
    Jemalloc(Mallctl),
}

impl RetentionAllocator {
    fn new() -> io::Result<Self> {
        // SAFETY: these NUL-terminated static names identify process-lifetime
        // allocator interfaces. RTLD_DEFAULT does not open or unload a library.
        let allocated = unsafe {
            libc::dlsym(
                libc::RTLD_DEFAULT,
                c"__sanitizer_get_current_allocated_bytes".as_ptr(),
            )
        };
        if !allocated.is_null() {
            // SAFETY: sanitizer/allocator_interface.h defines this exact C ABI;
            // the linked sanitizer runtime remains loaded for the process lifetime.
            return Ok(Self::Sanitizer(unsafe {
                std::mem::transmute::<*mut libc::c_void, AllocatedBytes>(allocated)
            }));
        }
        // SAFETY: the static symbol name and process-lifetime handle satisfy dlsym.
        let mallctl = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"mallctl".as_ptr()) };
        if !mallctl.is_null() {
            // SAFETY: jemalloc/jemalloc.h defines this exact C ABI. The configured
            // process allocator remains loaded until process exit.
            return Ok(Self::Jemalloc(unsafe {
                std::mem::transmute::<*mut libc::c_void, Mallctl>(mallctl)
            }));
        }
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "configured allocator exposes no live allocation counter",
        ))
    }

    fn description(&self) -> &'static str {
        match self {
            Self::Sanitizer(_) => "sanitizer live allocated bytes",
            Self::Jemalloc(_) => "jemalloc stats.allocated",
        }
    }

    fn read(&self) -> io::Result<usize> {
        let allocated = match self {
            Self::Sanitizer(read) => {
                // SAFETY: new resolved the sanitizer's no-argument counter with
                // its declared ABI; the interface has no caller preconditions.
                unsafe { read() }
            }
            Self::Jemalloc(mallctl) => {
                let mut epoch = 1u64;
                // SAFETY: epoch accepts a readable uint64_t new value. The pointer
                // and its exact size remain valid throughout this synchronous call.
                let result = unsafe {
                    mallctl(
                        c"epoch".as_ptr(),
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        std::ptr::from_mut(&mut epoch).cast(),
                        std::mem::size_of_val(&epoch),
                    )
                };
                if result != 0 {
                    return Err(io::Error::from_raw_os_error(result));
                }
                let mut allocated = 0usize;
                let mut length = std::mem::size_of_val(&allocated);
                // SAFETY: stats.allocated writes a size_t into the initialized,
                // correctly sized output and updates the separate size_t length.
                let result = unsafe {
                    mallctl(
                        c"stats.allocated".as_ptr(),
                        std::ptr::from_mut(&mut allocated).cast(),
                        &mut length,
                        std::ptr::null_mut(),
                        0,
                    )
                };
                if result != 0 {
                    return Err(io::Error::from_raw_os_error(result));
                }
                if length != std::mem::size_of_val(&allocated) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "allocator counter has an unexpected size",
                    ));
                }
                allocated
            }
        };
        if allocated == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "allocator counter must observe live allocations",
            ));
        }
        Ok(allocated)
    }
}

fn retention_ceiling(index: usize, baseline: usize, actual: usize) -> RetentionResult<()> {
    if actual > baseline {
        Err(RetentionError::Growth {
            index,
            baseline,
            actual,
        }
        .into())
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct RetentionSensitivity {
    baseline: usize,
    held: usize,
    released: usize,
    capacity: usize,
    visible: usize,
}

fn retention_holder_outcome<Output, Diagnostic: Write>(
    outcome: std::thread::Result<RetentionResult<Output>>,
    joined: std::thread::Result<RetentionResult<()>>,
    diagnostic: impl FnOnce() -> Diagnostic,
) -> std::thread::Result<RetentionResult<Output>> {
    match (outcome, joined) {
        (Err(primary), joined) => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let mut diagnostic = diagnostic();
                match joined {
                    Ok(Err(error)) => {
                        let _ = writeln!(diagnostic, "holder cleanup: {error:?}");
                    }
                    Err(payload) => {
                        let _ = writeln!(diagnostic, "holder cleanup panic: {payload:?}");
                    }
                    Ok(Ok(())) => {}
                }
            }));
            Err(primary)
        }
        (Ok(Err(primary)), Err(payload)) => Ok(Err(RetentionError::HolderCleanup {
            primary,
            cleanup: vec![
                RetentionError::HolderPanic {
                    payload: Mutex::new(payload),
                }
                .into(),
            ],
        }
        .into())),
        (Ok(Ok(_)), Err(payload)) => Err(payload),
        (Ok(Err(primary)), Ok(Err(cleanup))) => Ok(Err(RetentionError::HolderCleanup {
            primary,
            cleanup: vec![cleanup],
        }
        .into())),
        (Ok(Err(error)), Ok(Ok(()))) | (Ok(Ok(_)), Ok(Err(error))) => Ok(Err(error)),
        (Ok(Ok(value)), Ok(Ok(()))) => Ok(Ok(value)),
    }
}

fn retention_sensitivity(allocator: &RetentionAllocator) -> RetentionResult<RetentionSensitivity> {
    let (ready_send, ready) = thread_channel::sync_channel(1);
    let (allocate, allocate_receive) = thread_channel::sync_channel(1);
    let (held_send, held) = thread_channel::sync_channel(1);
    let (release, release_receive) = thread_channel::sync_channel(1);
    let holder = std::thread::Builder::new().spawn(move || -> RetentionResult<()> {
        ready_send.send(())?;
        allocate_receive.recv_timeout(STAGE_BOUND)?;
        let buffer = vec![0x65_u8; RETENTION_BACKING];
        let capacity = buffer.capacity();
        require(
            capacity == RETENTION_BACKING,
            "actual sensitivity backing capacity",
        )?;
        let bytes = Bytes::from(buffer);
        let alias = bytes.slice(..1);
        require(
            alias.as_ptr() == bytes.as_ptr(),
            "actual shared one-byte alias",
        )?;
        drop(bytes);
        require(alias.as_ref() == b"e", "retained touched alias byte")?;
        held_send.send((capacity, alias.len()))?;
        let result = release_receive.recv_timeout(STAGE_BOUND);
        drop(alias);
        result?;
        Ok(())
    })?;
    let outcome = catch_unwind(AssertUnwindSafe(
        || -> RetentionResult<(usize, usize, usize, usize)> {
            ready.recv_timeout(STAGE_BOUND)?;
            let baseline = allocator.read()?;
            allocate.try_send(())?;
            let (capacity, visible) = held.recv_timeout(STAGE_BOUND)?;
            let observed = allocator.read()?;
            require(
                capacity == RETENTION_BACKING
                    && visible == 1
                    && observed
                        .checked_sub(baseline)
                        .is_some_and(|delta| delta >= capacity),
                "same allocator detects retained full backing, not visible alias length",
            )?;
            let rejected = retention_ceiling(RETENTION_MEASURED, baseline, observed)
                .expect_err("same zero-growth predicate rejects the actual alias");
            require(
                matches!(rejected.downcast_ref::<RetentionError>(),
            Some(RetentionError::Growth { index, baseline: actual_baseline, actual })
                if *index == RETENTION_MEASURED && *actual_baseline == baseline && *actual == observed),
                "exact typed alias-growth rejection",
            )?;
            drop(rejected);
            release.try_send(())?;
            Ok((baseline, observed, capacity, visible))
        },
    ));
    drop(allocate);
    drop(release);
    let joined = holder.join();
    let (baseline, held, capacity, visible) =
        match retention_holder_outcome(outcome, joined, io::stderr) {
            Ok(result) => result?,
            Err(payload) => resume_unwind(payload),
        };
    let released = allocator.read()?;
    retention_ceiling(RETENTION_MEASURED + 1, baseline, released)?;
    Ok(RetentionSensitivity {
        baseline,
        held,
        released,
        capacity,
        visible,
    })
}

#[derive(Clone, Copy, Debug)]
struct RetentionMeasurement {
    held: RetentionHeld,
    retired: Resources,
    allocated: usize,
}

fn retention_final_measurement(
    observations: &[Option<RetentionMeasurement>; RETENTION_MEASURED],
) -> RetentionResult<RetentionMeasurement> {
    if !observations.iter().all(Option::is_some) {
        return Err(RetentionError::MissingMeasurements.into());
    }
    observations[RETENTION_MEASURED - 1].ok_or_else(|| RetentionError::MissingMeasurements.into())
}

fn retention_measurement_failure(
    source: RetentionFailure,
    route: Route,
    allocator_description: &str,
    sensitivity: RetentionSensitivity,
    baseline: usize,
    warmed: Resources,
    observations: [Option<RetentionMeasurement>; RETENTION_MEASURED],
) -> RetentionFailure {
    RetentionError::Measurements {
        source,
        route,
        allocator_description: allocator_description.to_owned(),
        sensitivity,
        baseline,
        warmed,
        observations: Box::new(observations),
    }
    .into()
}

fn retention_before_deadline(elapsed: Duration, bound: Duration) -> RetentionResult<()> {
    if elapsed >= bound {
        Err(RetentionError::Deadline.into())
    } else {
        Ok(())
    }
}

fn retention_route_name(route: Route) -> &'static str {
    match (route.h2, route.tls) {
        (false, false) => "tcp-h1",
        (true, false) => "tcp-h2",
        (false, true) => "tls-h1",
        (true, true) => "tls-h2",
    }
}

fn retention_receipt(route: Route) -> String {
    format!(
        "native-client-retention-v1 route={} warmups=2 measured=8 complete",
        retention_route_name(route)
    )
}

fn retention_witness(route: Route, remaining: Duration) -> RetentionResult<()> {
    let started = Instant::now();
    let mut observations = [None; RETENTION_MEASURED];
    let allocator = RetentionAllocator::new()?;
    let allocator_description = allocator.description();
    require(
        !allocator_description.is_empty() && allocator_description.len() <= 256,
        "bounded allocator description",
    )?;
    let sensitivity = retention_sensitivity(&allocator)?;
    let mut warmed = None;
    for _ in 0..RETENTION_WARMUPS {
        retention_before_deadline(started.elapsed(), remaining)?;
        retention_capsule(route)?;
        warmed = Some(retention_resources()?);
        allocator.read()?;
        retention_before_deadline(started.elapsed(), remaining)?;
    }
    let warmed = warmed.ok_or_else(|| failure("two actual warmup capsules"))?;
    let baseline = allocator.read()?;
    let mut first_failure = None;
    for (index, observation) in observations.iter_mut().enumerate() {
        let outcome = (|| -> RetentionResult<()> {
            retention_before_deadline(started.elapsed(), remaining)?;
            let held = retention_capsule(route)?;
            let retired = retention_resources()?;
            let allocated = allocator.read()?;
            *observation = Some(RetentionMeasurement {
                held,
                retired,
                allocated,
            });
            resource_bound(retired, warmed)?;
            retention_ceiling(index, baseline, allocated)?;
            retention_before_deadline(started.elapsed(), remaining)
        })();
        if let Err(source) = outcome {
            first_failure = Some(source);
            break;
        }
    }
    let last = match first_failure.map_or_else(|| retention_final_measurement(&observations), Err) {
        Ok(last) => last,
        Err(source) => {
            return Err(retention_measurement_failure(
                source,
                route,
                allocator_description,
                sensitivity,
                baseline,
                warmed,
                observations,
            ));
        }
    };
    let mut output = io::stdout().lock();
    writeln!(
        output,
        "native-client-retention route={route:?} allocator={allocator_description:?} baseline={baseline} warmed={warmed:?}"
    )?;
    writeln!(
        output,
        "native-client-retention sensitivity baseline={} held={} released={} capacity={} visible={}",
        sensitivity.baseline,
        sensitivity.held,
        sensitivity.released,
        sensitivity.capacity,
        sensitivity.visible
    )?;
    for (index, observation) in observations.into_iter().enumerate() {
        let observation = observation.ok_or(RetentionError::MissingMeasurements)?;
        require(observation.held.route == route, "matched retained route")?;
        writeln!(
            output,
            "native-client-retention capsule={index} allocated={} retired={:?}",
            observation.allocated, observation.retired
        )?;
        for stage in observation.held.stages {
            writeln!(
                output,
                "native-client-retention capsule={index} stage={:?} worker={:?} held={:?} stage_retired={:?} read_peak={} received={} response={}",
                stage.stage,
                stage.owner,
                stage.held,
                stage.retired,
                stage.read_peak,
                stage.received,
                stage.response
            )?;
        }
    }
    writeln!(
        output,
        "native-client-retention selected_live_capsule=7 release_held_rss={} post_owner_rss={} whole_child_lifetime_hwm={} includes_setup_warmup_controls=true",
        last.held.stages[3].held.rss_bytes, last.retired.rss_bytes, last.retired.process_hwm_bytes
    )?;
    retention_before_deadline(started.elapsed(), remaining)?;
    writeln!(output, "{}", retention_receipt(route))?;
    Ok(())
}

struct RetentionDirectory {
    path: PathBuf,
}

impl RetentionDirectory {
    fn create(route: Route) -> RetentionResult<Self> {
        let path = std::env::temp_dir().join(format!(
            "roxygen-native-client-retention-{}-{}",
            std::process::id(),
            retention_route_name(route),
        ));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self { path })
    }

    fn remove(&self, errors: &mut Vec<RetentionFailure>) {
        for name in ["stdout", "stderr"] {
            if let Err(error) = fs::remove_file(self.path.join(name)) {
                errors.push(error.into());
            }
        }
        if let Err(error) = fs::remove_dir(&self.path) {
            errors.push(error.into());
        }
    }
}

impl Drop for RetentionDirectory {
    fn drop(&mut self) {
        for name in ["stdout", "stderr"] {
            let _ = fs::remove_file(self.path.join(name));
        }
        let _ = fs::remove_dir(&self.path);
    }
}

struct RetentionCapture {
    pipe: UnixStream,
    file: fs::File,
    bytes: usize,
    eof: bool,
}

impl RetentionCapture {
    fn create(path: &Path) -> RetentionResult<(Self, Stdio)> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        let (pipe, writer) = UnixStream::pair()?;
        pipe.set_nonblocking(true)?;
        let writer: OwnedFd = writer.into();
        Ok((
            Self {
                pipe,
                file,
                bytes: 0,
                eof: false,
            },
            Stdio::from(writer),
        ))
    }

    fn pump(&mut self) -> RetentionResult<()> {
        let mut buffer = [0; 4096];
        for _ in 0..16 {
            if self.eof {
                break;
            }
            match self.pipe.read(&mut buffer) {
                Ok(0) => self.eof = true,
                Ok(count) => {
                    let next = self
                        .bytes
                        .checked_add(count)
                        .ok_or_else(|| failure("retention output count overflow"))?;
                    require(
                        next <= RETENTION_OUTPUT_BOUND,
                        "retention output exceeds256KiB",
                    )?;
                    self.file.write_all(&buffer[..count])?;
                    self.bytes = next;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn drain_reaped(&mut self) -> RetentionResult<()> {
        for _ in 0..=RETENTION_OUTPUT_BOUND / 4096 {
            self.pump()?;
            if self.eof {
                return Ok(());
            }
        }
        Err(failure("reaped retention child left an output writer alive").into())
    }

    fn text(&mut self) -> RetentionResult<String> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut text = String::with_capacity(self.bytes);
        Read::by_ref(&mut self.file)
            .take(RETENTION_OUTPUT_BOUND as u64 + 1)
            .read_to_string(&mut text)?;
        require(
            text.len() == self.bytes,
            "retained output matches bounded capture",
        )?;
        Ok(text)
    }
}

fn retention_wait_policy(handler: libc::sighandler_t, flags: libc::c_int) -> io::Result<()> {
    require(
        handler == libc::SIG_DFL && flags & libc::SA_NOCLDWAIT == 0,
        "retention requires default, non-auto-reaping SIGCHLD without changing signal policy",
    )
}

fn retention_wait_admission() -> io::Result<()> {
    let mut action = std::mem::MaybeUninit::<libc::sigaction>::uninit();
    if unsafe { libc::sigaction(libc::SIGCHLD, std::ptr::null(), action.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let action = unsafe { action.assume_init() };
    retention_wait_policy(action.sa_sigaction, action.sa_flags)
}

trait RetentionChildOperations {
    fn poll_child(&mut self) -> io::Result<Option<ExitStatus>>;
    fn kill_child(&mut self) -> io::Result<()>;
    fn wait_child(&mut self) -> io::Result<ExitStatus>;
}

impl RetentionChildOperations for Child {
    fn poll_child(&mut self) -> io::Result<Option<ExitStatus>> {
        self.try_wait()
    }

    #[cfg(fbcode_build)]
    fn kill_child(&mut self) -> io::Result<()> {
        self.pidfd()?.kill()
    }

    #[cfg(not(fbcode_build))]
    fn kill_child(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn wait_child(&mut self) -> io::Result<ExitStatus> {
        self.wait()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionWaitState {
    Owned,
    Reaped(ExitStatus),
    Lost,
}

struct RetentionChildOwner<Process: RetentionChildOperations = Child> {
    child: Process,
    state: RetentionWaitState,
    pidfd_confirmed: bool,
    forced: bool,
}

impl<Process: RetentionChildOperations> RetentionChildOwner<Process> {
    fn status(&self) -> Option<ExitStatus> {
        match self.state {
            RetentionWaitState::Reaped(status) => Some(status),
            RetentionWaitState::Owned | RetentionWaitState::Lost => None,
        }
    }

    fn poll(&mut self) -> io::Result<Option<ExitStatus>> {
        match self.state {
            RetentionWaitState::Reaped(status) => return Ok(Some(status)),
            RetentionWaitState::Lost => return Err(io::Error::from_raw_os_error(libc::ECHILD)),
            RetentionWaitState::Owned => {}
        }
        loop {
            match self.child.poll_child() {
                Ok(Some(status)) => {
                    self.state = RetentionWaitState::Reaped(status);
                    return Ok(Some(status));
                }
                Ok(None) => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    if error.raw_os_error() == Some(libc::ECHILD) {
                        self.state = RetentionWaitState::Lost;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn retire(&mut self, mut failed: impl FnMut(io::Error)) {
        if self.state != RetentionWaitState::Owned {
            return;
        }
        match self.poll() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => failed(error),
        }
        if self.state != RetentionWaitState::Owned {
            return;
        }
        if self.pidfd_confirmed && !self.forced {
            self.forced = true;
            if let Err(error) = self.child.kill_child() {
                failed(error);
            }
        }
        loop {
            match self.child.wait_child() {
                Ok(status) => {
                    self.state = RetentionWaitState::Reaped(status);
                    break;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    if error.raw_os_error() == Some(libc::ECHILD) {
                        self.state = RetentionWaitState::Lost;
                    }
                    failed(error);
                    break;
                }
            }
        }
    }
}

impl<Process: RetentionChildOperations> Drop for RetentionChildOwner<Process> {
    fn drop(&mut self) {
        self.retire(drop);
    }
}

fn retention_child_route(
    marker: &OsStr,
    arguments: impl Iterator<Item = OsString>,
) -> Option<Route> {
    if !arguments.eq(RETENTION_CHILD_ARGS.into_iter().map(OsString::from)) {
        return None;
    }
    ROUTES
        .into_iter()
        .find(|route| marker == OsStr::new(&format!("v1:{}", retention_route_name(*route))))
}

fn retention_child_budget(value: &OsStr) -> RetentionResult<Duration> {
    let text = value
        .to_str()
        .ok_or_else(|| failure("UTF-8 remaining child budget"))?;
    require(
        !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()),
        "unsigned remaining child budget",
    )?;
    let remaining = Duration::from_nanos(text.parse::<u64>()?);
    require(
        !remaining.is_zero() && remaining <= WATCHDOG_BOUND,
        "child receives only a finite remaining parent budget",
    )?;
    Ok(remaining)
}

fn retention_receipt_valid(route: Route, success: bool, forced: bool, stdout: &str) -> bool {
    let expected = retention_receipt(route);
    success
        && !forced
        && stdout
            .lines()
            .filter(|line| line.starts_with("native-client-retention-v1 "))
            .eq(std::iter::once(expected.as_str()))
}

fn retention_observed_status(
    elapsed: Duration,
    observation: io::Result<Option<ExitStatus>>,
) -> RetentionResult<bool> {
    let status = observation?;
    retention_before_deadline(elapsed, WATCHDOG_BOUND)?;
    match status {
        Some(status) if !status.success() => Err(RetentionError::Exit(status).into()),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

fn retention_parent_failure(
    primary: Option<RetentionFailure>,
    cleanup: &mut Vec<RetentionFailure>,
    accepted: bool,
) -> Option<RetentionFailure> {
    primary
        .or_else(|| {
            if cleanup.is_empty() {
                None
            } else {
                Some(cleanup.remove(0))
            }
        })
        .or_else(|| {
            (!accepted).then(|| {
                failure("unforced exit0 and one route-bound final receipt required").into()
            })
        })
}

fn retention_parent_route(route: Route, started: Instant) -> RetentionResult<()> {
    retention_before_deadline(started.elapsed(), WATCHDOG_BOUND)?;
    let directory = RetentionDirectory::create(route)?;
    let (mut stdout, child_stdout) = RetentionCapture::create(&directory.path.join("stdout"))?;
    let (mut stderr, child_stderr) = RetentionCapture::create(&directory.path.join("stderr"))?;
    let mut cleanup = Vec::with_capacity(12);
    let executable = std::env::current_exe()?;
    retention_wait_admission()?;
    let remaining = WATCHDOG_BOUND
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(RetentionError::Deadline)?;
    let mut command = Command::new(executable);
    #[cfg(fbcode_build)]
    command.create_pidfd(true);
    command
        .args(RETENTION_CHILD_ARGS)
        .env(
            RETENTION_CHILD_MARKER,
            format!("v1:{}", retention_route_name(route)),
        )
        .env(RETENTION_CHILD_BUDGET, remaining.as_nanos().to_string())
        .stdin(Stdio::null())
        .stdout(child_stdout)
        .stderr(child_stderr);
    unsafe {
        command.pre_exec(|| Ok(()));
    }
    let child = command.spawn()?;
    drop(command);
    #[cfg(fbcode_build)]
    let pidfd = child.pidfd().map(|_| ());
    // Without the unstable pidfd API the child is admitted like a kernel
    // without pidfd support: wait-only cleanup and no numeric signalling.
    #[cfg(not(fbcode_build))]
    let pidfd: io::Result<()> = Err(io::ErrorKind::Unsupported.into());
    let mut owner = RetentionChildOwner {
        child,
        state: RetentionWaitState::Owned,
        pidfd_confirmed: pidfd.is_ok(),
        forced: false,
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| -> RetentionResult<()> {
        pidfd?;
        loop {
            retention_before_deadline(started.elapsed(), WATCHDOG_BOUND)?;
            stdout.pump()?;
            stderr.pump()?;
            retention_before_deadline(started.elapsed(), WATCHDOG_BOUND)?;
            let observation = owner.poll();
            if retention_observed_status(started.elapsed(), observation)? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }));
    owner.retire(|error| cleanup.push(error.into()));
    let status = owner.status();
    if status.is_some() {
        for capture in [&mut stdout, &mut stderr] {
            if let Err(error) = capture.drain_reaped() {
                cleanup.push(error);
            }
        }
    } else {
        cleanup.push(failure("original retention child reap was not established").into());
    }
    let mut read_capture = |capture: &mut RetentionCapture| match capture.text() {
        Ok(text) => text,
        Err(error) => {
            cleanup.push(error);
            String::new()
        }
    };
    let stdout_text = read_capture(&mut stdout);
    let stderr_text = read_capture(&mut stderr);
    drop(stdout);
    drop(stderr);
    directory.remove(&mut cleanup);
    let primary = match outcome {
        Ok(result) => result.err(),
        Err(payload) => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                eprintln!(
                    "client retention panic cleanup route={route:?} pid={} status={:?} wait_state={:?} pidfd_confirmed={} forced={} errors={cleanup:?} stdout={stdout_text:?} stderr={stderr_text:?}",
                    owner.child.id(),
                    status,
                    owner.state,
                    owner.pidfd_confirmed,
                    owner.forced,
                )
            }));
            resume_unwind(payload);
        }
    };
    let primary = retention_parent_failure(
        primary,
        &mut cleanup,
        retention_receipt_valid(
            route,
            status.is_some_and(|status| status.success()),
            owner.forced,
            &stdout_text,
        ),
    )
    .or_else(|| retention_before_deadline(started.elapsed(), WATCHDOG_BOUND).err());
    if let Some(primary) = primary {
        return Err(RetentionError::Child {
            primary,
            route,
            pid: owner.child.id(),
            status,
            wait_state: owner.state,
            pidfd_confirmed: owner.pidfd_confirmed,
            forced: owner.forced,
            cleanup,
            stdout: stdout_text,
            stderr: stderr_text,
        }
        .into());
    }
    eprintln!(
        "native-client-retention original_child route={route:?} pid={} raw_wait={:?} wait_state={:?} pidfd_confirmed={} forced={} remaining_at_launch_ns={} parent_elapsed_ms={}",
        owner.child.id(),
        status.map(ExitStatus::into_raw),
        owner.state,
        owner.pidfd_confirmed,
        owner.forced,
        remaining.as_nanos(),
        started.elapsed().as_millis()
    );
    print!("{stdout_text}");
    if !stderr_text.is_empty() {
        eprint!("{stderr_text}");
    }
    Ok(())
}

enum RetentionMatrixOutcome {
    Returned(RetentionResult<()>),
    Panicked {
        route: Route,
        payload: Box<dyn Any + Send>,
        outcomes: Vec<(Route, RetentionResult<()>)>,
    },
}

fn retention_matrix_outcome(
    mut observed: Vec<(Route, std::thread::Result<RetentionResult<()>>)>,
) -> RetentionMatrixOutcome {
    let Some(first_failure) = observed
        .iter()
        .position(|(_, result)| !matches!(result, Ok(Ok(()))))
    else {
        return RetentionMatrixOutcome::Returned(Ok(()));
    };
    let first_panic = if observed[first_failure].1.is_err() {
        let (route, result) = observed.remove(first_failure);
        match result {
            Err(payload) => Some((route, payload)),
            Ok(_) => unreachable!("selected first route outcome was a panic"),
        }
    } else {
        None
    };
    let outcomes = observed
        .into_iter()
        .map(|(route, result)| {
            let result = match result {
                Ok(result) => result,
                Err(payload) => Err(RetentionError::RoutePanic {
                    payload: Mutex::new(payload),
                }
                .into()),
            };
            (route, result)
        })
        .collect();
    match first_panic {
        Some((route, payload)) => RetentionMatrixOutcome::Panicked {
            route,
            payload,
            outcomes,
        },
        None => RetentionMatrixOutcome::Returned(Err(RetentionError::Matrix {
            first_failure,
            outcomes,
        }
        .into())),
    }
}

fn retention_matrix_result<Diagnostic: Write>(
    outcome: RetentionMatrixOutcome,
    diagnostic: impl FnOnce() -> Diagnostic,
) -> std::thread::Result<RetentionResult<()>> {
    match outcome {
        RetentionMatrixOutcome::Returned(result) => Ok(result),
        RetentionMatrixOutcome::Panicked {
            route,
            payload,
            outcomes,
        } => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let mut diagnostic = diagnostic();
                let _ = writeln!(
                    diagnostic,
                    "client retention matrix primary panic route={route:?} secondary outcomes={outcomes:?}"
                );
                drop(outcomes);
            }));
            Err(payload)
        }
    }
}

fn retention_parent() -> RetentionResult<()> {
    let started = Instant::now();
    let mut outcomes = Vec::with_capacity(ROUTES.len());
    for route in ROUTES {
        outcomes.push((
            route,
            catch_unwind(AssertUnwindSafe(|| retention_parent_route(route, started))),
        ));
    }
    require(
        outcomes.len() == ROUTES.len(),
        "all four original route outcomes retained",
    )?;
    match retention_matrix_result(retention_matrix_outcome(outcomes), io::stderr) {
        Ok(result) => result?,
        Err(payload) => resume_unwind(payload),
    }
    retention_before_deadline(started.elapsed(), WATCHDOG_BOUND)?;
    Ok(())
}

fn retention_control_measurement(index: usize) -> RetentionMeasurement {
    let held = Resources {
        descriptors: 8,
        threads: 3,
        rss_bytes: 300 + index as u64,
        process_hwm_bytes: 400 + index as u64,
    };
    let retired = Resources {
        descriptors: 4,
        threads: 1,
        rss_bytes: 40,
        process_hwm_bytes: 100,
    };
    RetentionMeasurement {
        held: RetentionHeld {
            route: ROUTES[0],
            stages: STAGES.map(|stage| RetentionStage {
                stage,
                owner: std::thread::current().id(),
                held,
                retired,
                read_peak: 1,
                received: 1,
                response: 1,
            }),
        },
        retired,
        allocated: 100 + index,
    }
}

fn retention_measurement_controls() -> RetentionResult<()> {
    let complete: [Option<RetentionMeasurement>; RETENTION_MEASURED] =
        std::array::from_fn(|index| Some(retention_control_measurement(index)));
    let last = retention_final_measurement(&complete)?;
    require(
        last.allocated == 107,
        "predeclared last capsule, not selected empirical stability",
    )?;
    require(
        complete[0].expect("control slot").held.stages[3]
            .held
            .rss_bytes
            > last.retired.process_hwm_bytes,
        "earlier raw RSS above later reported HWM is not normalized or rejected",
    )?;
    let sensitivity = RetentionSensitivity {
        baseline: 100,
        held: 8292,
        released: 100,
        capacity: 8192,
        visible: 1,
    };
    for missing in 0..RETENTION_MEASURED {
        let mut observations = complete;
        observations[missing] = None;
        let source = retention_final_measurement(&observations)
            .expect_err("each missing index prevents acceptance");
        let identity = source
            .downcast_ref::<RetentionError>()
            .expect("typed missing measurement") as *const RetentionError;
        let wrapped = retention_measurement_failure(
            source,
            ROUTES[0],
            "synthetic-control",
            sensitivity,
            107,
            last.retired,
            observations,
        );
        match wrapped.downcast_ref::<RetentionError>() {
            Some(RetentionError::Measurements {
                source,
                route,
                allocator_description,
                sensitivity: retained_sensitivity,
                baseline,
                warmed,
                observations: retained,
            }) => {
                require(
                    source
                        .downcast_ref::<RetentionError>()
                        .is_some_and(|source| {
                            std::ptr::eq(source, identity)
                                && matches!(source, RetentionError::MissingMeasurements)
                        }),
                    "missing-slot wrapper retains original typed cause",
                )?;
                require(
                    *route == ROUTES[0]
                        && allocator_description == "synthetic-control"
                        && *baseline == 107
                        && warmed.descriptors == 4
                        && retained_sensitivity.capacity == RETENTION_BACKING,
                    "failure retains original baseline and sensitivity",
                )?;
                for (index, observation) in retained.iter().enumerate() {
                    match observation {
                        None => require(
                            index == missing,
                            "only the genuinely missing slot is absent",
                        )?,
                        Some(observation) => require(
                            index != missing
                                && observation.allocated == 100 + index
                                && observation.held.stages[3].held.rss_bytes == 300 + index as u64,
                            "all observed slots retain their raw values",
                        )?,
                    }
                }
            }
            _ => return Err(failure("typed measurement wrapper required").into()),
        }
    }
    retention_ceiling(0, 100, 100)?;
    retention_ceiling(1, 100, 99)?;
    let growth = retention_ceiling(3, 100, 101).expect_err("one byte of growth is rejected");
    require(
        matches!(
            growth.downcast_ref::<RetentionError>(),
            Some(RetentionError::Growth {
                index: 3,
                baseline: 100,
                actual: 101
            })
        ),
        "strict original allocation discriminator",
    )?;
    require(
        retention_final_measurement(&[None; RETENTION_MEASURED]).is_err(),
        "no empty series",
    )?;
    for text in [
        "",
        "VmRSS: 1 MB\n",
        "VmRSS: -1 kB\n",
        "VmRSS: 1 kB\nVmRSS: 2 kB\n",
        "VmRSS: 18446744073709551615 kB\n",
    ] {
        require(
            retention_rss_field(text, "VmRSS:").is_err(),
            "malformed or overflowing actual RSS is not zero",
        )?;
    }
    require(
        retention_rss_field("VmRSS: 2 kB\n", "VmRSS:")? == 2048,
        "literal Linux RSS units",
    )?;
    let excessive_fds = Resources {
        descriptors: 5,
        ..last.retired
    };
    let excessive_threads = Resources {
        threads: 2,
        ..last.retired
    };
    require(
        resource_bound(excessive_fds, last.retired).is_err()
            && resource_bound(excessive_threads, last.retired).is_err(),
        "independent FD/thread recovery gates",
    )?;
    Ok(())
}

fn retention_holder_controls() -> RetentionResult<()> {
    #[derive(Clone, Copy)]
    enum Outcome {
        Success,
        Error,
        Panic,
    }
    for main in [Outcome::Success, Outcome::Error, Outcome::Panic] {
        for holder in [Outcome::Success, Outcome::Error, Outcome::Panic] {
            let primary = Box::new(io::Error::from(io::ErrorKind::BrokenPipe));
            let primary_identity = primary.as_ref() as *const io::Error;
            let cleanup = Box::new(io::Error::from(io::ErrorKind::ConnectionReset));
            let cleanup_identity = cleanup.as_ref() as *const io::Error;
            let main_panic = Arc::new(());
            let holder_panic = Arc::new(());
            let outcome: std::thread::Result<RetentionResult<usize>> = match main {
                Outcome::Success => Ok(Ok(7)),
                Outcome::Error => Ok(Err(primary)),
                Outcome::Panic => Err(Box::new(Arc::clone(&main_panic))),
            };
            let joined: std::thread::Result<RetentionResult<()>> = match holder {
                Outcome::Success => Ok(Ok(())),
                Outcome::Error => Ok(Err(cleanup)),
                Outcome::Panic => Err(Box::new(Arc::clone(&holder_panic))),
            };
            let mut storage = [];
            let mut diagnostic: &mut [u8] = &mut storage;
            require(
                diagnostic
                    .write_all(b"cleanup")
                    .expect_err("actual bounded diagnostic fails")
                    .kind()
                    == io::ErrorKind::WriteZero,
                "fallible diagnostic boundary",
            )?;
            let mut reported = false;
            let result = retention_holder_outcome(outcome, joined, || {
                reported = true;
                diagnostic
            });
            require(
                reported == matches!(main, Outcome::Panic),
                "only primary panic requests fallback diagnostics",
            )?;
            match (main, holder, result) {
                (Outcome::Panic, _, Err(payload)) => require(
                    payload
                        .downcast_ref::<Arc<()>>()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &main_panic)),
                    "original main panic survives diagnostic failure",
                )?,
                (Outcome::Success, Outcome::Panic, Err(payload)) => require(
                    payload
                        .downcast_ref::<Arc<()>>()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &holder_panic)),
                    "original holder panic remains primary",
                )?,
                (Outcome::Error, Outcome::Success, Ok(Err(error))) => require(
                    error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|actual| std::ptr::eq(actual, primary_identity)),
                    "original main typed error",
                )?,
                (Outcome::Success, Outcome::Error, Ok(Err(error))) => require(
                    error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|actual| std::ptr::eq(actual, cleanup_identity)),
                    "original holder typed error",
                )?,
                (Outcome::Error, Outcome::Error | Outcome::Panic, Ok(Err(error))) => {
                    let Some(RetentionError::HolderCleanup { primary, cleanup }) =
                        error.downcast_ref::<RetentionError>()
                    else {
                        return Err(failure("typed primary and cleanup aggregate").into());
                    };
                    require(
                        primary
                            .downcast_ref::<io::Error>()
                            .is_some_and(|actual| std::ptr::eq(actual, primary_identity))
                            && cleanup.len() == 1,
                        "original main cause and one secondary retained",
                    )?;
                    match holder {
                        Outcome::Error => require(
                            cleanup[0]
                                .downcast_ref::<io::Error>()
                                .is_some_and(|actual| std::ptr::eq(actual, cleanup_identity)),
                            "original secondary error",
                        )?,
                        Outcome::Panic => {
                            let Some(RetentionError::HolderPanic { payload }) =
                                cleanup[0].downcast_ref::<RetentionError>()
                            else {
                                return Err(failure("retained holder panic payload").into());
                            };
                            require(
                                payload
                                    .lock()
                                    .map_err(failure)?
                                    .downcast_ref::<Arc<()>>()
                                    .is_some_and(|actual| Arc::ptr_eq(actual, &holder_panic)),
                                "original secondary panic object",
                            )?;
                        }
                        Outcome::Success => unreachable!(),
                    }
                }
                (Outcome::Success, Outcome::Success, Ok(Ok(7))) => {}
                _ => return Err(failure("holder outcome precedence changed").into()),
            }
        }
    }
    Ok(())
}

fn retention_child_owner_controls() -> RetentionResult<()> {
    #[derive(Debug)]
    enum Step {
        Poll(io::Result<Option<ExitStatus>>),
        Kill(io::Result<()>),
        Wait(io::Result<ExitStatus>),
    }
    struct Script {
        pending: std::collections::VecDeque<Step>,
        calls: Vec<&'static str>,
    }
    struct Process(Arc<Mutex<Script>>);
    impl Process {
        fn next(&mut self, operation: &'static str) -> Step {
            let mut script = self.0.lock().expect("retention owner control script");
            script.calls.push(operation);
            script
                .pending
                .pop_front()
                .expect("no operation after wait authority ends")
        }
    }
    impl RetentionChildOperations for Process {
        fn poll_child(&mut self) -> io::Result<Option<ExitStatus>> {
            match self.next("poll") {
                Step::Poll(result) => result,
                other => panic!("poll substituted for {other:?}"),
            }
        }
        fn kill_child(&mut self) -> io::Result<()> {
            match self.next("kill") {
                Step::Kill(result) => result,
                other => panic!("signal substituted for {other:?}"),
            }
        }
        fn wait_child(&mut self) -> io::Result<ExitStatus> {
            match self.next("wait") {
                Step::Wait(result) => result,
                other => panic!("wait substituted for {other:?}"),
            }
        }
    }
    let owner = |steps: Vec<Step>, pidfd_confirmed| {
        let script = Arc::new(Mutex::new(Script {
            pending: steps.into(),
            calls: Vec::new(),
        }));
        (
            RetentionChildOwner {
                child: Process(Arc::clone(&script)),
                state: RetentionWaitState::Owned,
                pidfd_confirmed,
                forced: false,
            },
            script,
        )
    };
    let observed = |script: &Arc<Mutex<Script>>, expected: &[&str]| -> RetentionResult<()> {
        let script = script.lock().map_err(failure)?;
        require(
            script.pending.is_empty() && script.calls.as_slice() == expected,
            "exact original-owner operation sequence including Drop",
        )?;
        Ok(())
    };
    let interrupted = || io::Error::from_raw_os_error(libc::EINTR);
    let lost = || io::Error::from_raw_os_error(libc::ECHILD);
    let status = ExitStatus::from_raw(7 << 8);
    retention_wait_policy(libc::SIG_DFL, 0)?;
    retention_wait_policy(libc::SIG_DFL, libc::SA_NOCLDSTOP)?;
    for (handler, flags) in [
        (libc::SIG_IGN, 0),
        (libc::SIG_DFL, libc::SA_NOCLDWAIT),
        (usize::MAX, 0),
    ] {
        require(
            retention_wait_policy(handler, flags).is_err(),
            "ignored, auto-reaping or custom SIGCHLD rejects without policy mutation",
        )?;
    }
    for explicit_poll in [false, true] {
        let (mut child, script) = owner(vec![Step::Poll(Err(lost()))], true);
        let mut errors = Vec::new();
        if explicit_poll {
            let original = child.poll().expect_err("lost original wait authority");
            require(
                original.raw_os_error() == Some(libc::ECHILD),
                "original ECHILD retained",
            )?;
            child.retire(|error| errors.push(error));
            require(
                errors.is_empty(),
                "lost authority is not retried during retirement",
            )?;
        } else {
            child.retire(|error| errors.push(error));
            require(
                errors.len() == 1 && errors[0].raw_os_error() == Some(libc::ECHILD),
                "retirement retains its original ECHILD",
            )?;
        }
        require(
            child.state == RetentionWaitState::Lost && !child.forced && child.status().is_none(),
            "ECHILD is neither owned-unreaped nor successfully reaped",
        )?;
        drop(child);
        observed(&script, &["poll"])?;
    }
    let (mut child, script) = owner(
        vec![
            Step::Poll(Err(interrupted())),
            Step::Poll(Ok(None)),
            Step::Poll(Ok(None)),
            Step::Kill(Ok(())),
            Step::Wait(Err(interrupted())),
            Step::Wait(Ok(status)),
        ],
        true,
    );
    require(
        child.poll()?.is_none() && child.state == RetentionWaitState::Owned,
        "EINTR retries the same original poll without losing ownership",
    )?;
    let mut errors = Vec::new();
    child.retire(|error| errors.push(error));
    require(
        errors.is_empty() && child.status() == Some(status) && child.forced,
        "interrupted blocking wait retains actual raw status and forced classification",
    )?;
    child.retire(|error| errors.push(error));
    drop(child);
    observed(&script, &["poll", "poll", "poll", "kill", "wait", "wait"])?;

    let (mut child, script) = owner(
        vec![Step::Poll(Err(interrupted())), Step::Poll(Ok(Some(status)))],
        true,
    );
    require(
        child.poll()? == Some(status) && child.poll()? == Some(status),
        "actual reap is cached",
    )?;
    child.retire(|error| errors.push(error));
    require(!child.forced, "already-reaped child is never signalled")?;
    drop(child);
    observed(&script, &["poll", "poll"])?;

    let (mut child, script) = owner(
        vec![
            Step::Poll(Ok(None)),
            Step::Kill(Ok(())),
            Step::Wait(Err(lost())),
        ],
        true,
    );
    let mut errors = Vec::new();
    child.retire(|error| errors.push(error));
    require(
        errors.len() == 1
            && errors[0].raw_os_error() == Some(libc::ECHILD)
            && child.state == RetentionWaitState::Lost
            && child.status().is_none(),
        "ECHILD from blocking wait permanently removes signal authority",
    )?;
    drop(child);
    observed(&script, &["poll", "kill", "wait"])?;

    let (child, script) = owner(
        vec![
            Step::Poll(Ok(None)),
            Step::Kill(Ok(())),
            Step::Wait(Err(interrupted())),
            Step::Wait(Ok(status)),
        ],
        true,
    );
    drop(child);
    observed(&script, &["poll", "kill", "wait", "wait"])?;

    let (mut child, script) = owner(vec![Step::Poll(Ok(None)), Step::Wait(Ok(status))], false);
    child.retire(|error| errors.push(error));
    require(
        child.status() == Some(status) && !child.forced,
        "missing pidfd permits only original wait cleanup, never numeric signalling",
    )?;
    drop(child);
    observed(&script, &["poll", "wait"])?;

    let (mut child, script) = owner(
        vec![
            Step::Poll(Err(io::Error::from_raw_os_error(libc::EIO))),
            Step::Kill(Err(io::Error::from_raw_os_error(libc::ESRCH))),
            Step::Wait(Ok(status)),
        ],
        true,
    );
    let mut errors = Vec::new();
    child.retire(|error| errors.push(error));
    require(
        errors.len() == 2
            && errors[0].raw_os_error() == Some(libc::EIO)
            && errors[1].raw_os_error() == Some(libc::ESRCH)
            && child.status() == Some(status),
        "original wait failure precedes secondary signal failure and actual later reap",
    )?;
    drop(child);
    observed(&script, &["poll", "kill", "wait"])?;
    Ok(())
}

fn retention_matrix_controls() -> RetentionResult<()> {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Outcome {
        Success,
        Error,
        Panic,
    }
    fn retained(
        result: &RetentionResult<()>,
        expected: Outcome,
        error_identity: *const io::Error,
        panic_identity: *const Arc<()>,
    ) -> RetentionResult<()> {
        match (expected, result) {
            (Outcome::Success, Ok(())) => Ok(()),
            (Outcome::Error, Err(error)) => {
                require(
                    error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|actual| std::ptr::eq(actual, error_identity)),
                    "original route error allocation retained",
                )?;
                Ok(())
            }
            (Outcome::Panic, Err(error)) => {
                let Some(RetentionError::RoutePanic { payload }) =
                    error.downcast_ref::<RetentionError>()
                else {
                    return Err(failure("actual secondary route panic required").into());
                };
                require(
                    payload
                        .lock()
                        .map_err(failure)?
                        .downcast_ref::<Arc<()>>()
                        .is_some_and(|actual| std::ptr::eq(actual, panic_identity)),
                    "actual secondary panic payload allocation retained",
                )?;
                Ok(())
            }
            _ => Err(failure("route outcome was replaced or omitted").into()),
        }
    }
    for earlier in [Outcome::Success, Outcome::Error, Outcome::Panic] {
        for later in [Outcome::Success, Outcome::Error, Outcome::Panic] {
            let earlier_error = Box::new(io::Error::from(io::ErrorKind::BrokenPipe));
            let earlier_error_identity = earlier_error.as_ref() as *const io::Error;
            let later_error = Box::new(io::Error::from(io::ErrorKind::ConnectionReset));
            let later_error_identity = later_error.as_ref() as *const io::Error;
            let earlier_panic = Box::new(Arc::new(()));
            let earlier_panic_identity = earlier_panic.as_ref() as *const Arc<()>;
            let later_panic = Box::new(Arc::new(()));
            let later_panic_identity = later_panic.as_ref() as *const Arc<()>;
            let earlier_result: std::thread::Result<RetentionResult<()>> = match earlier {
                Outcome::Success => Ok(Ok(())),
                Outcome::Error => Ok(Err(earlier_error)),
                Outcome::Panic => Err(earlier_panic),
            };
            let later_result: std::thread::Result<RetentionResult<()>> = match later {
                Outcome::Success => Ok(Ok(())),
                Outcome::Error => Ok(Err(later_error)),
                Outcome::Panic => Err(later_panic),
            };
            let outcome = retention_matrix_outcome(vec![
                (ROUTES[0], Ok(Ok(()))),
                (ROUTES[1], earlier_result),
                (ROUTES[2], later_result),
                (ROUTES[3], Ok(Ok(()))),
            ]);
            let first = if earlier != Outcome::Success {
                earlier
            } else {
                later
            };
            let first_index = if earlier != Outcome::Success { 1 } else { 2 };
            match &outcome {
                RetentionMatrixOutcome::Returned(Ok(())) => {
                    require(first == Outcome::Success, "only all-success matrix passes")?
                }
                RetentionMatrixOutcome::Returned(Err(error)) => {
                    let Some(RetentionError::Matrix {
                        first_failure,
                        outcomes,
                    }) = error.downcast_ref::<RetentionError>()
                    else {
                        return Err(failure("typed matrix retains ordered route results").into());
                    };
                    require(
                        first == Outcome::Error
                            && *first_failure == first_index
                            && outcomes.len() == ROUTES.len(),
                        "earliest typed failure stays primary even before a later panic",
                    )?;
                    require(
                        outcomes.iter().map(|(route, _)| *route).eq(ROUTES),
                        "all four actual route slots retained",
                    )?;
                    retained(
                        &outcomes[1].1,
                        earlier,
                        earlier_error_identity,
                        earlier_panic_identity,
                    )?;
                    retained(
                        &outcomes[2].1,
                        later,
                        later_error_identity,
                        later_panic_identity,
                    )?;
                    require(
                        outcomes[0].1.is_ok() && outcomes[3].1.is_ok(),
                        "genuine successes retained",
                    )?;
                }
                RetentionMatrixOutcome::Panicked {
                    route,
                    payload,
                    outcomes,
                } => {
                    let identity = if earlier == Outcome::Panic {
                        earlier_panic_identity
                    } else {
                        later_panic_identity
                    };
                    require(
                        first == Outcome::Panic
                            && *route == ROUTES[first_index]
                            && payload
                                .downcast_ref::<Arc<()>>()
                                .is_some_and(|actual| std::ptr::eq(actual, identity))
                            && outcomes.len() == ROUTES.len() - 1,
                        "original first panic and all other route outcomes are retained",
                    )?;
                    require(
                        outcomes
                            .iter()
                            .map(|(route, _)| *route)
                            .eq(ROUTES.into_iter().enumerate().filter_map(|(index, route)| {
                                (index != first_index).then_some(route)
                            })),
                        "secondary route order stays unchanged",
                    )?;
                    if earlier == Outcome::Panic {
                        retained(
                            &outcomes[1].1,
                            later,
                            later_error_identity,
                            later_panic_identity,
                        )?;
                    }
                }
            }
            let mut storage = [];
            let mut diagnostic: &mut [u8] = &mut storage;
            require(
                diagnostic
                    .write_all(b"matrix cleanup")
                    .expect_err("actual finite sink fails")
                    .kind()
                    == io::ErrorKind::WriteZero,
                "genuinely fallible matrix diagnostic",
            )?;
            let mut reported = false;
            let result = retention_matrix_result(outcome, || {
                reported = true;
                diagnostic
            });
            require(
                reported == (first == Outcome::Panic),
                "diagnostic failure cannot select the primary cause",
            )?;
            match result {
                Err(payload) => {
                    let identity = if earlier == Outcome::Panic {
                        earlier_panic_identity
                    } else {
                        later_panic_identity
                    };
                    require(
                        first == Outcome::Panic
                            && payload
                                .downcast_ref::<Arc<()>>()
                                .is_some_and(|actual| std::ptr::eq(actual, identity)),
                        "original first panic survives failed reporting",
                    )?;
                }
                Ok(Err(error)) => {
                    let Some(RetentionError::Matrix {
                        first_failure,
                        outcomes,
                    }) = error.downcast_ref::<RetentionError>()
                    else {
                        return Err(
                            failure("returned matrix retains its original route evidence").into(),
                        );
                    };
                    require(
                        first == Outcome::Error
                            && *first_failure == first_index
                            && outcomes.len() == ROUTES.len(),
                        "later panic cannot turn the earlier typed failure into an unwind",
                    )?;
                    retained(
                        &outcomes[1].1,
                        earlier,
                        earlier_error_identity,
                        earlier_panic_identity,
                    )?;
                    retained(
                        &outcomes[2].1,
                        later,
                        later_error_identity,
                        later_panic_identity,
                    )?;
                }
                Ok(Ok(())) => require(first == Outcome::Success, "all-success matrix control")?,
            }
        }
    }
    Ok(())
}

fn retention_literal_controls() -> RetentionResult<()> {
    retention_measurement_controls()?;
    retention_holder_controls()?;
    retention_child_owner_controls()?;
    retention_matrix_controls()?;
    let success = ExitStatus::from_raw(0);
    let before = WATCHDOG_BOUND - Duration::from_nanos(1);
    require(
        retention_observed_status(before, Ok(Some(success)))?
            && !retention_observed_status(before, Ok(None))?,
        "actual-status classifier before deadline",
    )?;
    for elapsed in [WATCHDOG_BOUND, WATCHDOG_BOUND + Duration::from_nanos(1)] {
        let deadline = retention_observed_status(elapsed, Ok(Some(success)))
            .expect_err("late exit0 cannot pass");
        let identity = deadline
            .downcast_ref::<RetentionError>()
            .expect("typed deadline") as *const RetentionError;
        let mut cleanup: Vec<RetentionFailure> =
            vec![io::Error::from(io::ErrorKind::BrokenPipe).into()];
        let retained = retention_parent_failure(Some(deadline), &mut cleanup, true)
            .expect("normal later reap cannot erase the earlier deadline");
        require(
            retained
                .downcast_ref::<RetentionError>()
                .is_some_and(|actual| {
                    std::ptr::eq(actual, identity) && matches!(actual, RetentionError::Deadline)
                })
                && cleanup.len() == 1,
            "original deadline and secondary cleanup retained",
        )?;
        require(
            retention_observed_status(elapsed, Ok(None)).is_err(),
            "pending at deadline also fails",
        )?;
    }
    let nonzero = ExitStatus::from_raw(7 << 8);
    let failed =
        retention_observed_status(before, Ok(Some(nonzero))).expect_err("nonzero actual status");
    require(
        matches!(failed.downcast_ref::<RetentionError>(), Some(RetentionError::Exit(actual)) if *actual == nonzero),
        "original nonzero wait status retained",
    )?;
    let error =
        retention_observed_status(WATCHDOG_BOUND, Err(io::Error::from_raw_os_error(libc::EIO)))
            .expect_err("poll failure precedes later clock classification");
    require(
        error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(libc::EIO)),
        "original typed wait error",
    )?;
    for route in ROUTES {
        let marker = format!("v1:{}", retention_route_name(route));
        let arguments = || RETENTION_CHILD_ARGS.into_iter().map(OsString::from);
        require(
            retention_child_route(OsStr::new(&marker), arguments()) == Some(route),
            "exact selected child",
        )?;
        require(
            retention_child_route(OsStr::new("v1:other"), arguments()).is_none()
                && retention_child_route(OsStr::new(&marker), arguments().skip(1)).is_none()
                && retention_child_route(
                    OsStr::new(&marker),
                    arguments().chain([OsString::from("--ignored")]),
                )
                .is_none(),
            "wrong marker, broad selection and extra child flags reject",
        )?;
        let receipt = retention_receipt(route);
        require(
            retention_receipt_valid(route, true, false, &receipt),
            "one correct route receipt",
        )?;
        for (success, forced, text) in [
            (false, false, receipt.clone()),
            (true, true, receipt.clone()),
            (true, false, String::new()),
            (true, false, format!("{receipt}\n{receipt}\n")),
            (
                true,
                false,
                retention_receipt(Route {
                    tls: !route.tls,
                    ..route
                }),
            ),
        ] {
            require(
                !retention_receipt_valid(route, success, forced, &text),
                "receipt cannot substitute for actual unforced success",
            )?;
        }
    }
    require(
        retention_child_budget(OsStr::new("899999999999"))? == before,
        "remaining budget is not renewed900s",
    )?;
    for value in ["", "0", "-1", "+1", "900000000001", "18446744073709551616"] {
        require(
            retention_child_budget(OsStr::new(value)).is_err(),
            "missing, unbounded or expired child budget",
        )?;
    }
    Ok(())
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "requires the unstable `linux_pidfd` API and a jemalloc `mallctl` or sanitizer allocated-bytes symbol that `dlsym` can resolve in the test process"
)]
fn native_envelope_retention_matrix() -> RetentionResult<()> {
    let _serial = SERIAL.lock().map_err(failure)?;
    match std::env::var_os(RETENTION_CHILD_MARKER) {
        None => {
            require(
                std::env::var_os(RETENTION_CHILD_BUDGET).is_none(),
                "child budget requires exact child admission",
            )?;
            retention_parent()
        }
        Some(marker) => {
            let route =
                retention_child_route(&marker, std::env::args_os().skip(1)).ok_or_else(|| {
                    failure(
                        "exact version, route and selector required; no broad or recursive child",
                    )
                })?;
            let value = std::env::var_os(RETENTION_CHILD_BUDGET)
                .ok_or_else(|| failure("child requires remaining parent budget"))?;
            retention_witness(route, retention_child_budget(&value)?)
        }
    }
}
