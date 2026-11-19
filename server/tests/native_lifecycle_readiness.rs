/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::any::Any;
use std::cell::RefCell;
use std::env;
use std::error::Error;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs;
use std::fs::DirBuilder;
use std::fs::File;
use std::fs::OpenOptions;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::future::ready;
use std::io;
use std::io::IoSlice;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::net::SocketAddr;
use std::os::fd::OwnedFd;
#[cfg(fbcode_build)]
use std::os::linux::process::ChildExt;
#[cfg(fbcode_build)]
use std::os::linux::process::CommandExt;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc as thread_channel;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::ThreadId;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use h2::RecvStream;
use h2::SendStream;
use h2::client::ResponseFuture;
use h2::client::SendRequest;
use http::Request;
use http::Response;
use http::Version;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::exchange::ExchangeCompletionError;
use roxygen_core::exchange::LocalExchangeCompletionFuture;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::message::SocketAddress;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::tls::TlsServerOptions;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use socket2::SockRef;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::Interest;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::ConnectionInfo;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::HttpServerState;
use crate::server::ServerConfig;
use crate::server::ServerObserver;
use crate::server::StaticTlsCredentials;

const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const NATIVE_TIMEOUT: Duration = Duration::from_secs(30);
const SERIES_BUDGET: Duration = Duration::from_secs(120);
const CHUNK: usize = 4096;
const BODY_SIZE: usize = 2 * CHUNK;
const PRESSURE_BYTES: usize = 16 * 1024 * 1024;
const PRESSURE_WINDOW: usize = 256 * 1024;
const PRESSURE_SOCKET_REQUEST: usize = 64 * 1024;
const EGRESS_BYTES: usize = 16 * 1024 * 1024;
const EGRESS_SLOTS: usize = 16;
const EGRESS_STREAM_WINDOW: u32 = 64 * 1024;
const EGRESS_CONNECTION_WINDOW: u32 = 1024 * 1024;
const UPLOAD: [u8; BODY_SIZE] = [b'u'; BODY_SIZE];
const DOWNLOAD: [u8; BODY_SIZE] = [b'r'; BODY_SIZE];
const HEALTH: &[u8] = b"native-lifecycle-ok";
const MAX_TARGETS: usize = 24;
const MAX_REQUESTS: usize = 128;
const MAX_CONNECTIONS: usize = 32;
const MAX_DRIVERS: usize = 4;
const MAX_PROC_ENTRIES: usize = 4096;
const MAX_THREAD_IDENTITIES: usize = 32;
const MAX_THREAD_COMM_BYTES: usize = 64;
const MAX_THREAD_STAT_BYTES: usize = 4096;
const SAMPLE_COUNT: usize = 20;
const WARMUPS: usize = 2;
const MEASURED: usize = 8;
static SERIAL: Mutex<()> = Mutex::new(());

type BoxError = Box<dyn Error + Send + Sync>;
type TestResult<Output> = Result<Output, BoxError>;

#[derive(Debug, Error)]
enum FixtureError {
    #[error("retention observation {index} allocated {actual} bytes above baseline {baseline}")]
    RetentionGrowth {
        index: usize,
        baseline: usize,
        actual: usize,
    },
    #[error(
        "retention allocator={allocator_description:?}, sensitivity={sensitivity:?}, baseline={baseline}, warmed={warmed:?}, observations={observations:?}: {source}"
    )]
    RetentionMeasurements {
        #[source]
        source: BoxError,
        allocator_description: String,
        sensitivity: Box<RetentionSensitivity>,
        baseline: usize,
        warmed: RetentionResources,
        observations: Box<[Option<RetentionMeasurement>; MEASURED]>,
    },
    #[error("retention cleanup after {primary}: {cleanup:?}")]
    RetentionCleanup {
        #[source]
        primary: BoxError,
        cleanup: Vec<BoxError>,
    },
    #[error("holder panicked during cleanup: {message}")]
    RetentionHolderPanic {
        message: String,
        payload: Mutex<Box<dyn Any + Send>>,
    },
    #[error("retention child exited with {0}")]
    RetentionExit(ExitStatus),
    #[error(
        "retention child failure: {primary}; status={status:?}; forced={forced}; cleanup={cleanup:?}; stdout={stdout:?}; stderr={stderr:?}"
    )]
    RetentionChild {
        #[source]
        primary: BoxError,
        status: Option<ExitStatus>,
        forced: bool,
        cleanup: Vec<BoxError>,
        stdout: String,
        stderr: String,
    },
    #[error("target {target} retains response backing capacity {capacity}")]
    BackingRetained { target: usize, capacity: usize },
    #[error("native lifecycle assertion: {0}")]
    Assertion(&'static str),
    #[error("native lifecycle deadline: {0}")]
    Deadline(&'static str),
    #[error("native upload resume returned {0:?}, expected Signalled")]
    Resume(ResumeStatus),
    #[error("native target {target} ({stage:?}) retired before the cancellation marker")]
    PrematureRetirement { target: usize, stage: Stage },
    #[error("thread {tid} identity observation at {phase}: {source}")]
    ThreadIdentityRead {
        tid: i32,
        phase: &'static str,
        #[source]
        source: BoxError,
    },
    #[error("thread {tid} changed identity during observation: {before} -> {after}")]
    ThreadIdentityChanged { tid: i32, before: u64, after: u64 },
    #[error("resource observation at {checkpoint:?}: {errors:?}")]
    ResourceObservation {
        checkpoint: Checkpoint,
        errors: Vec<BoxError>,
    },
    #[error("{route:?}: {errors:?}")]
    Case { route: Route, errors: Vec<BoxError> },
}

fn require(condition: bool, message: &'static str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(FixtureError::Assertion(message).into())
    }
}

async fn within<Work: Future>(phase: &'static str, work: Work) -> TestResult<Work::Output> {
    timeout(STEP_TIMEOUT, work)
        .await
        .map_err(|_| FixtureError::Deadline(phase).into())
}

fn drive<Output>(
    runtime: &Runtime,
    work: impl Future<Output = TestResult<Output>>,
) -> TestResult<Output> {
    runtime.block_on(within("peer operation", work))?
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    H1,
    H2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Route {
    protocol: Protocol,
    tls: bool,
}

const ROUTES: [Route; 4] = [
    Route {
        protocol: Protocol::H1,
        tls: false,
    },
    Route {
        protocol: Protocol::H2,
        tls: false,
    },
    Route {
        protocol: Protocol::H1,
        tls: true,
    },
    Route {
        protocol: Protocol::H2,
        tls: true,
    },
];

impl Route {
    fn codec(self) -> CodecProtocol {
        match self.protocol {
            Protocol::H1 => CodecProtocol::Http11,
            Protocol::H2 => CodecProtocol::Http2,
        }
    }

    fn alpn(self) -> &'static [u8] {
        match self.protocol {
            Protocol::H1 => b"http/1.1",
            Protocol::H2 => b"h2",
        }
    }

    fn security(self) -> TransportSecurity {
        if self.tls {
            TransportSecurity::Tls
        } else {
            TransportSecurity::Plaintext
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Complete,
    Upload,
    Handler,
    Response,
}

const CANCEL_STAGES: [Stage; 3] = [Stage::Upload, Stage::Handler, Stage::Response];

#[derive(Clone, Copy, Debug)]
struct WorkerIdentity {
    rust: ThreadId,
    linux: i32,
}

fn current_identity() -> WorkerIdentity {
    let linux = unsafe { libc::syscall(libc::SYS_gettid) };
    WorkerIdentity {
        rust: std::thread::current().id(),
        linux: linux.try_into().expect("Linux TID fits pid_t"),
    }
}

#[derive(Clone, Copy)]
enum GuardKind {
    Handler = 0,
    Wait = 1,
    Response = 2,
}

struct TargetEvidence {
    id: usize,
    stage: Stage,
    owner: Mutex<Option<WorkerIdentity>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    cancelling: AtomicBool,
    premature_retirement: AtomicBool,
    wrong_thread: AtomicBool,
    created: [AtomicUsize; 3],
    dropped: [AtomicUsize; 3],
    stops: Mutex<Vec<Option<HttpErrorCode>>>,
    ingress_error: Mutex<Option<HttpError>>,
    peer_reset: Mutex<Option<h2::Error>>,
    upload_bytes: AtomicUsize,
    response_bytes: AtomicUsize,
    pending_checks: AtomicUsize,
    completed_checks: AtomicUsize,
    backing: Option<Arc<BackingRecord>>,
}

impl TargetEvidence {
    fn record_retirement(&self, kind: GuardKind) {
        let requires_marker = match kind {
            GuardKind::Wait => self.stage != Stage::Complete,
            GuardKind::Handler => matches!(self.stage, Stage::Upload | Stage::Handler),
            GuardKind::Response => self.stage == Stage::Response,
        };
        if requires_marker && !self.cancelling.load(Ordering::Acquire) {
            self.premature_retirement.store(true, Ordering::Release);
        }
    }

    fn check_retirement_order(&self) -> TestResult<()> {
        if self.premature_retirement.load(Ordering::Acquire) {
            Err(FixtureError::PrematureRetirement {
                target: self.id,
                stage: self.stage,
            }
            .into())
        } else {
            Ok(())
        }
    }

    fn check_owner(&self) {
        if self
            .owner
            .lock()
            .expect("target owner")
            .is_none_or(|owner| {
                let current = current_identity();
                owner.rust != current.rust || owner.linux != current.linux
            })
        {
            self.wrong_thread.store(true, Ordering::Release);
        }
    }

    fn held(&self) -> TestResult<()> {
        self.check_retirement_order()?;
        require(
            !self.wrong_thread.load(Ordering::Acquire),
            "target callbacks remain worker-local",
        )?;
        require(
            self.created[GuardKind::Wait as usize].load(Ordering::Acquire) == 1,
            "actual pending native wait",
        )?;
        require(
            self.dropped[GuardKind::Wait as usize].load(Ordering::Acquire) == 0,
            "native wait remains held",
        )?;
        require(
            self.stops.lock().expect("source stops").is_empty(),
            "source not stopped before peer cancellation",
        )?;
        let upload = if self.stage == Stage::Upload {
            CHUNK
        } else {
            BODY_SIZE
        };
        require(
            self.upload_bytes.load(Ordering::Acquire) == upload,
            "exact upload consumption at the admitted stage",
        )?;
        let response = usize::from(self.stage == Stage::Response);
        require(
            self.response_bytes.load(Ordering::Acquire) == response * CHUNK,
            "exact response prefix production at the held stage",
        )?;
        require(
            self.created[GuardKind::Response as usize].load(Ordering::Acquire) == response,
            "response construction matches stage",
        )?;
        require(
            self.dropped[GuardKind::Handler as usize].load(Ordering::Acquire) == response,
            "handler remains pending at the requested stage",
        )
    }

    fn retired(&self) -> TestResult<()> {
        require(
            !self.wrong_thread.load(Ordering::Acquire),
            "all target drops occur on the native worker",
        )?;
        require(
            self.created[0].load(Ordering::Acquire) == 1
                && self.dropped[0].load(Ordering::Acquire) == 1,
            "one native handler future retired",
        )?;
        let waits = usize::from(self.stage != Stage::Complete);
        require(
            self.created[1].load(Ordering::Acquire) == waits
                && self.dropped[1].load(Ordering::Acquire) == waits,
            "the actual native read/resume/gate future retired",
        )?;
        let sources = usize::from(matches!(self.stage, Stage::Complete | Stage::Response));
        require(
            self.created[2].load(Ordering::Acquire) == sources
                && self.dropped[2].load(Ordering::Acquire) == sources,
            "native response source destruction matches stage",
        )?;
        require(
            self.stops.lock().expect("source stops").len()
                == usize::from(self.stage == Stage::Response),
            "unfinished response stopped exactly once; terminal responses not stopped",
        )?;
        let upload = if self.stage == Stage::Upload {
            CHUNK
        } else {
            BODY_SIZE
        };
        require(
            self.upload_bytes.load(Ordering::Acquire) == upload,
            "no post-cancel upload progress",
        )?;
        let response = match self.stage {
            Stage::Complete => BODY_SIZE,
            Stage::Response => CHUNK,
            Stage::Upload | Stage::Handler => 0,
        };
        require(
            self.response_bytes.load(Ordering::Acquire) == response,
            "exact response production with no post-cancel progress",
        )?;
        require(
            self.completed_checks.load(Ordering::Acquire) == 2,
            "actual native receipt completed twice",
        )?;
        require(
            self.pending_checks.load(Ordering::Acquire) == waits,
            "cancelled target receipt was first observed pending",
        )?;
        self.check_retirement_order()
    }
}

struct DropGuard {
    evidence: Arc<TargetEvidence>,
    kind: GuardKind,
}

impl DropGuard {
    fn new(evidence: Arc<TargetEvidence>, kind: GuardKind) -> Self {
        evidence.check_owner();
        evidence.created[kind as usize].fetch_add(1, Ordering::AcqRel);
        Self { evidence, kind }
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.evidence.record_retirement(self.kind);
        self.evidence.check_owner();
        self.evidence.dropped[self.kind as usize].fetch_add(1, Ordering::AcqRel);
    }
}

async fn observe_pending<Work: Future>(
    work: Work,
    evidence: &Arc<TargetEvidence>,
) -> TestResult<Work::Output> {
    let mut work = Box::pin(work);
    let mut guard = None;
    let output = within(
        "native held future",
        poll_fn(|context| {
            let polled = work.as_mut().poll(context);
            if polled.is_pending() && evidence.created[1].load(Ordering::Acquire) == 0 {
                guard = Some(DropGuard::new(Arc::clone(evidence), GuardKind::Wait));
                let sender = evidence
                    .entered
                    .lock()
                    .expect("native stage notification")
                    .take();
                if sender.is_none_or(|sender| sender.send(()).is_err()) {
                    return Poll::Ready(Err(FixtureError::Assertion(
                        "native stage observer remains alive",
                    )));
                }
            }
            polled.map(Ok)
        }),
    )
    .await?;
    drop(work);
    drop(guard);
    Ok(output?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PressureAction {
    Release = 1,
    NativeCancel = 2,
    PeerCancel = 3,
}

#[derive(Default)]
struct PressureWake {
    waiter: Mutex<Option<Waker>>,
}

impl PressureWake {
    fn register(&self, waker: &Waker) {
        let next = waker.clone();
        let previous = self.waiter.lock().expect("pressure waiter").replace(next);
        drop(previous);
    }

    fn wake(&self) {
        let waiter = self.waiter.lock().expect("pressure waiter").take();
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct H1IngressBound {
    flow: i64,
    segment: i64,
    ceiling: i64,
}

impl H1IngressBound {
    fn from_config(route: Route, config: &ServerConfig) -> TestResult<Self> {
        let session = &config.session;
        require(
            route.protocol == Protocol::H1
                && !route.tls
                && config.plaintext_protocol == CodecProtocol::Http11
                && config.tls_credentials.is_none()
                && config.num_io_threads == 1
                && session.stream_flow_control == 262_144
                && session.connection_flow_control == 5_242_880
                && session.read_allocation_size == 4_000
                && session.byte_stream_limits.maximum_read_size() == 262_144,
            "BODY-credit ceiling binds the unchanged single-worker plaintext H1 configuration",
        )?;
        let flow = i64::from(session.stream_flow_control);
        let maximum = (session.stream_flow_control as usize)
            .min(session.byte_stream_limits.maximum_read_size());
        let allocation = session.read_allocation_size.min(maximum);
        // Read continuations double their buffer, so a TCP read's largest buffer is its first
        // offer or at most half the read.
        let segment = i64::try_from(allocation.max(maximum / 2))?;
        // The body event that crosses the pause at F (`stream_flow_control`) carries the
        // unread body less than one allocation past F.
        let ceiling = flow
            .checked_add(i64::try_from(allocation)?)
            .and_then(|sum| sum.checked_sub(1))
            .ok_or(FixtureError::Assertion("checked BODY-credit ceiling"))?;
        require(
            segment == 131_072 && ceiling == 266_143,
            "prospective F plus one allocation minus one, not an observed allowance",
        )?;
        Ok(Self {
            flow,
            segment,
            ceiling,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct H1IngressCredit {
    bound: H1IngressBound,
    target: usize,
    peak: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EnvelopeIngressBound {
    route: Route,
    stream: i64,
    connection: i64,
    segment: i64,
    ceiling: i64,
}

impl EnvelopeIngressBound {
    fn from_config(route: Route, config: &ServerConfig) -> TestResult<Self> {
        let session = &config.session;
        require(
            config.plaintext_protocol == route.codec()
                && config.tls_credentials.is_some() == route.tls
                && config.num_io_threads == 1
                && session.stream_flow_control == 262_144
                && session.connection_flow_control == 5_242_880
                && session.read_allocation_size == 4_000
                && session.byte_stream_limits.maximum_read_size() == 262_144
                && session.byte_stream_limits.maximum_write_size() == 262_144,
            "ingress envelope binds final native route, worker, queue and transport settings",
        )?;
        let stream = i64::from(session.stream_flow_control);
        let connection = i64::from(session.connection_flow_control);
        let (segment, ceiling) = match route.protocol {
            Protocol::H1 => {
                let maximum = (session.stream_flow_control as usize)
                    .min(session.byte_stream_limits.maximum_read_size());
                let segment = i64::try_from(maximum)?;
                let ceiling = stream
                    .checked_add(i64::try_from(session.read_allocation_size.min(maximum))?)
                    .and_then(|sum| sum.checked_sub(1))
                    .ok_or(FixtureError::Assertion("checked H1 ingress envelope"))?;
                require(
                    segment == 262_144 && ceiling == 266_143,
                    "fixed H1 BODY-credit envelope",
                )?;
                (segment, ceiling)
            }
            Protocol::H2 => {
                let http2 = crate::acceptor::http2_session_config(session);
                let mut frames = http2
                    .settings
                    .iter()
                    .filter(|setting| setting.identifier == 5);
                let mut streams = http2
                    .settings
                    .iter()
                    .filter(|setting| setting.identifier == 4);
                require(
                    http2.connection_receive_window == 5_242_880
                        && http2.stream_receive_window == 262_144
                        && matches!(
                            http2.stream_receive_window_mode,
                            roxygen_session::http2::StreamReceiveWindowMode::PreserveConfigured,
                        )
                        && frames.next().is_some_and(|setting| setting.value == 16_384)
                        && frames.next().is_none()
                        && streams
                            .next()
                            .is_none_or(|setting| setting.value == 262_144)
                        && streams.next().is_none(),
                    "effective H2 connection, stream and advertised frame bounds",
                )?;
                (16_384, i64::from(http2.connection_receive_window))
            }
        };
        Ok(Self {
            route,
            stream,
            connection,
            segment,
            ceiling,
        })
    }

    fn matches(self, connection: &ConnectionInfo) -> bool {
        connection.protocol == self.route.codec()
            && connection.secure == self.route.tls
            && connection.context.security() == self.route.security()
    }

    fn pressure_floor(self) -> i64 {
        match self.route.protocol {
            Protocol::H1 => self.stream,
            Protocol::H2 => self.stream - CHUNK as i64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct EnvelopeIngressCredit {
    bound: EnvelopeIngressBound,
    peak: i64,
    first_failure: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Default)]
struct PressureQueue {
    read: i64,
    write: i64,
    received: usize,
    opened: usize,
    closed: usize,
    drops: usize,
    invalid: bool,
    ingress: Option<H1IngressCredit>,
    envelope: Option<EnvelopeIngressCredit>,
}

impl PressureQueue {
    fn fail(&mut self, reason: &'static str) {
        self.invalid = true;
        if let Some(envelope) = &mut self.envelope {
            envelope.first_failure.get_or_insert(reason);
        }
    }

    fn bind_envelope(&mut self, bound: EnvelopeIngressBound) -> TestResult<()> {
        if self.envelope.is_some()
            || self.invalid
            || self.read != 0
            || self.write != 0
            || self.received != 0
            || self.opened != 0
            || self.closed != 0
            || self.drops != 0
        {
            self.fail("nonfresh connection envelope");
            return Err(
                FixtureError::Assertion("connection envelope precedes all callbacks").into(),
            );
        }
        self.envelope = Some(EnvelopeIngressCredit {
            bound,
            peak: 0,
            first_failure: None,
        });
        Ok(())
    }

    fn envelope_valid(self, bound: EnvelopeIngressBound) -> bool {
        !self.invalid
            && self.read >= 0
            && self.drops <= 1
            && self.closed <= self.opened
            && self.envelope.is_some_and(|envelope| {
                envelope.bound == bound
                    && envelope.first_failure.is_none()
                    && self.read <= envelope.peak
                    && envelope.peak <= bound.ceiling
            })
    }

    fn envelope_pressure(self, bound: EnvelopeIngressBound) -> bool {
        self.envelope_valid(bound)
            && self
                .envelope
                .is_some_and(|envelope| envelope.peak >= bound.pressure_floor())
    }

    fn check_owner(&mut self, expected: WorkerIdentity, current: WorkerIdentity) {
        if current.rust != expected.rust || current.linux != expected.linux {
            self.fail("callback worker identity");
        }
    }

    fn bind_ingress(&mut self, bound: H1IngressBound, target: usize) -> TestResult<()> {
        self.invalid |= self.ingress.is_some()
            || !(1..=2).contains(&target)
            || self.read != 0
            || self.write != 0
            || self.received != 0
            || self.opened != 0
            || self.closed != 0
            || self.drops != 0;
        require(
            !self.invalid,
            "bind a fresh single-upload BODY-credit recorder before request traffic",
        )?;
        self.ingress = Some(H1IngressCredit {
            bound,
            target,
            peak: 0,
        });
        Ok(())
    }

    fn delta(&mut self, read: bool, delta: i64) {
        let balance = if read {
            &mut self.read
        } else {
            &mut self.write
        };
        match balance.checked_add(delta) {
            Some(next) if next >= 0 => *balance = next,
            _ => self.fail("callback balance arithmetic"),
        }
        if read && delta > 0 {
            match usize::try_from(delta)
                .ok()
                .and_then(|delta| self.received.checked_add(delta))
            {
                Some(next) => self.received = next,
                None => self.fail("received-byte arithmetic"),
            }
        }
        if read && let Some(ingress) = &mut self.ingress {
            ingress.peak = ingress.peak.max(self.read);
            self.invalid |= self.opened != 1
                || delta > ingress.bound.segment
                || ingress.peak > ingress.bound.ceiling;
        }
        if read && let Some(envelope) = &mut self.envelope {
            envelope.peak = envelope.peak.max(self.read);
            let failure = if delta > envelope.bound.segment {
                Some("positive callback segment bound")
            } else if envelope.peak > envelope.bound.ceiling {
                Some("connection BODY-credit ceiling")
            } else if self.invalid {
                Some("ordered callback accounting")
            } else {
                None
            };
            if let Some(reason) = failure {
                envelope.first_failure.get_or_insert(reason);
                self.invalid = true;
            }
        }
    }

    fn opened(&mut self) {
        match self.opened.checked_add(1) {
            Some(next) if next <= MAX_REQUESTS => self.opened = next,
            _ => self.invalid = true,
        }
        self.invalid |= self.ingress.is_some() && self.opened != 1;
    }

    fn ingress_valid(self, bound: H1IngressBound, target: usize) -> bool {
        !self.invalid
            && self.opened == 1
            && self.closed <= 1
            && self.drops <= 1
            && self.read >= 0
            && self.ingress.is_some_and(|ingress| {
                ingress.bound == bound
                    && ingress.target == target
                    && self.read <= ingress.peak
                    && ingress.peak <= bound.ceiling
            })
    }

    fn ingress_pressure(self, bound: H1IngressBound, target: usize) -> bool {
        self.ingress_valid(bound, target)
            && self
                .ingress
                .is_some_and(|ingress| ingress.peak >= bound.flow)
    }
}

#[derive(Clone, Copy, Debug)]
struct H1IngressRecovery {
    queue: PressureQueue,
    running: bool,
    starts: usize,
    stops: usize,
}

impl H1IngressRecovery {
    fn admitted(self, bound: H1IngressBound, target: usize) -> bool {
        self.queue.ingress_pressure(bound, target)
            && self.queue.read == 0
            && self.running
            && self.starts == 1
            && self.stops == 0
    }

    fn envelope_admitted(self, bound: EnvelopeIngressBound) -> bool {
        self.queue.envelope_pressure(bound)
            && self.queue.read == 0
            && self.running
            && self.starts == 1
            && self.stops == 0
    }
}

#[derive(Debug, Error)]
#[error("{source}; plaintext-H1 BODY-credit phase={phase} target={target} queue={queue:?}")]
struct H1IngressFailure {
    #[source]
    source: FixtureError,
    phase: &'static str,
    target: usize,
    queue: PressureQueue,
}

#[derive(Debug, Error)]
#[error("{source}; native ingress envelope phase={phase} target={target} queue={queue:?}")]
struct EnvelopeIngressFailure {
    #[source]
    source: FixtureError,
    phase: &'static str,
    target: usize,
    queue: PressureQueue,
}

struct PressureLedger {
    context: Arc<ConnectionContext>,
    socket_profile: Option<SocketBufferReceipt>,
    owner: WorkerIdentity,
    queue: Mutex<PressureQueue>,
    changed: Arc<PressureWake>,
}

impl PressureLedger {
    fn update(&self, update: impl FnOnce(&mut PressureQueue)) {
        let current = current_identity();
        let mut queue = self.queue.lock().expect("pressure queue");
        queue.check_owner(self.owner, current);
        if queue.envelope.is_some() && queue.drops != 0 {
            queue.fail("callback after recorder destruction");
        }
        update(&mut queue);
        if queue.invalid {
            queue.fail("session counter invariant");
        }
        drop(queue);
        self.changed.wake();
    }

    fn delta(&self, read: bool, delta: i64) {
        self.update(|queue| queue.delta(read, delta));
    }

    fn snapshot(&self) -> PressureQueue {
        *self.queue.lock().expect("pressure queue")
    }
}

struct PressureStats(Arc<PressureLedger>);

impl Http2SessionStats for PressureStats {
    fn record_buffered_read_bytes(&self, delta: i64) {
        self.0.delta(true, delta);
    }

    fn record_buffered_write_bytes(&self, delta: i64) {
        self.0.delta(false, delta);
    }

    fn record_transaction_opened(&self) {
        self.0.update(PressureQueue::opened);
    }

    fn record_transaction_closed(&self) {
        self.0.update(|queue| match queue.closed.checked_add(1) {
            Some(next) if next <= queue.opened => queue.closed = next,
            _ => queue.invalid = true,
        });
    }
}

impl Drop for PressureStats {
    fn drop(&mut self) {
        self.0.update(|queue| {
            queue.invalid |= queue.drops != 0 || queue.read != 0 || queue.write != 0;
            queue.drops = 1;
        });
    }
}

struct PressureTarget {
    id: usize,
    baseline: usize,
    ingress_bound: Option<H1IngressBound>,
    envelope_bound: Option<EnvelopeIngressBound>,
    ingress_recovery: Mutex<Option<H1IngressRecovery>>,
    owner: Mutex<Option<WorkerIdentity>>,
    ledger: Mutex<Option<Arc<PressureLedger>>>,
    changed: Arc<PressureWake>,
    consumed: AtomicUsize,
    action: AtomicUsize,
    entered: AtomicBool,
    native_cancelled: AtomicBool,
    eom: AtomicBool,
    invalid: AtomicBool,
    created: [AtomicUsize; 3],
    dropped: [AtomicUsize; 3],
    stops: Mutex<Vec<Option<HttpErrorCode>>>,
    pending: AtomicUsize,
    completed: AtomicUsize,
}

impl PressureTarget {
    fn check_ingress(&self, queue: PressureQueue, phase: &'static str) -> TestResult<()> {
        if let Some(bound) = self.ingress_bound
            && !queue.ingress_valid(bound, self.id)
        {
            return Err(H1IngressFailure {
                source: FixtureError::Assertion("valid target-bound BODY-credit callbacks"),
                phase,
                target: self.id,
                queue,
            }
            .into());
        }
        if let Some(bound) = self.envelope_bound
            && !queue.envelope_valid(bound)
        {
            return Err(EnvelopeIngressFailure {
                source: FixtureError::Assertion("valid route-bound ordered BODY-credit callbacks"),
                phase,
                target: self.id,
                queue,
            }
            .into());
        }
        Ok(())
    }

    fn check_owner(&self) {
        let current = current_identity();
        if self
            .owner
            .lock()
            .expect("pressure owner")
            .is_none_or(|owner| current.rust != owner.rust || current.linux != owner.linux)
        {
            self.invalid.store(true, Ordering::Release);
        }
    }

    fn increment(&self, count: &AtomicUsize) {
        if count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .is_err()
        {
            self.invalid.store(true, Ordering::Release);
        }
        self.changed.wake();
    }

    fn ledger(&self) -> TestResult<Arc<PressureLedger>> {
        self.ledger
            .lock()
            .expect("pressure ledger")
            .clone()
            .ok_or_else(|| {
                FixtureError::Assertion("target has an actual accepted statistics recorder").into()
            })
    }

    fn retired(&self) -> TestResult<()> {
        require(
            !self.invalid.load(Ordering::Acquire),
            "valid worker-local pressure accounting",
        )?;
        for index in 0..3 {
            require(
                self.created[index].load(Ordering::Acquire) == 1
                    && self.dropped[index].load(Ordering::Acquire) == 1,
                "one pressure handler, held wait and request source actually destroyed",
            )?;
        }
        let released = self.action.load(Ordering::Acquire) == PressureAction::Release as usize;
        require(
            self.consumed.load(Ordering::Acquire)
                == (if released { PRESSURE_BYTES } else { CHUNK })
                && self.eom.load(Ordering::Acquire) == released,
            "release consumes exact upload; cancellation does not consume away pressure",
        )?;
        let stops = self.stops.lock().expect("pressure source stops");
        if released {
            require(
                stops.is_empty(),
                "terminal pressure source has no extra stop",
            )?;
        } else {
            require(
                stops.len() == 1,
                "unfinished pressure source stopped exactly once",
            )?;
            if self.action.load(Ordering::Acquire) == PressureAction::NativeCancel as usize {
                require(
                    stops[0] == Some(HttpErrorCode::CANCEL)
                        && self.native_cancelled.load(Ordering::Acquire),
                    "actual worker-owned H1 cancellation",
                )?;
            }
        }
        require(
            self.pending.load(Ordering::Acquire) == 2
                && self.completed.load(Ordering::Acquire) == 2,
            "actual pending and sticky completed pressure receipts",
        )?;
        let queue = self.ledger()?.snapshot();
        self.check_ingress(queue, "retired")?;
        require(
            !queue.invalid && queue.read == 0,
            "target ingress reconciles after actual retirement",
        )
    }
}

struct PressureGuard {
    target: Arc<PressureTarget>,
    kind: usize,
}

impl PressureGuard {
    fn new(target: &Arc<PressureTarget>, kind: usize) -> Self {
        target.check_owner();
        target.increment(&target.created[kind]);
        Self {
            target: Arc::clone(target),
            kind,
        }
    }
}

impl Drop for PressureGuard {
    fn drop(&mut self) {
        self.target.check_owner();
        if self.target.action.load(Ordering::Acquire) == 0 {
            self.target.invalid.store(true, Ordering::Release);
        }
        self.target.increment(&self.target.dropped[self.kind]);
    }
}

struct PressurePlan {
    target: Arc<PressureTarget>,
    action: oneshot::Receiver<PressureAction>,
}

struct PressureState {
    plan: Mutex<Option<PressurePlan>>,
    targets: Mutex<Vec<Arc<PressureTarget>>>,
    ledgers: Mutex<Vec<Arc<PressureLedger>>>,
    changed: Arc<PressureWake>,
    socket_profile_enabled: bool,
    socket_profiles: Mutex<SocketProfileRecords>,
    ingress_bound: Option<H1IngressBound>,
    envelope_bound: Option<EnvelopeIngressBound>,
}

impl PressureState {
    fn new(socket_profile_enabled: bool, ingress_bound: Option<H1IngressBound>) -> Self {
        Self {
            plan: Mutex::new(None),
            targets: Mutex::new(Vec::with_capacity(2)),
            ledgers: Mutex::new(Vec::with_capacity(MAX_CONNECTIONS)),
            changed: Arc::new(PressureWake::default()),
            socket_profile_enabled,
            socket_profiles: Mutex::new(SocketProfileRecords::default()),
            ingress_bound,
            envelope_bound: None,
        }
    }

    fn configured(route: Route, config: &ServerConfig) -> TestResult<Self> {
        let ingress = if route.protocol == Protocol::H1 && !route.tls {
            Some(H1IngressBound::from_config(route, config)?)
        } else {
            None
        };
        let mut state = Self::new(route.protocol == Protocol::H1, ingress);
        state.envelope_bound = Some(EnvelopeIngressBound::from_config(route, config)?);
        Ok(state)
    }

    fn recorder(&self, connection: &ConnectionInfo) -> Rc<dyn Http2SessionStats> {
        let socket_profile = if self.socket_profile_enabled {
            self.socket_profiles
                .lock()
                .expect("pressure socket profiles")
                .receipts
                .iter()
                .flatten()
                .find(|receipt| receipt.matches_connection(connection))
                .copied()
        } else {
            None
        };
        let mut queue = PressureQueue::default();
        if let Some(bound) = self.envelope_bound {
            queue
                .bind_envelope(bound)
                .expect("fresh connection recorder");
            if !bound.matches(connection) {
                queue.fail("actual accepted protocol and security");
            }
        }
        let ledger = Arc::new(PressureLedger {
            context: Arc::clone(&connection.context),
            socket_profile,
            owner: current_identity(),
            queue: Mutex::new(queue),
            changed: Arc::clone(&self.changed),
        });
        let mut ledgers = self.ledgers.lock().expect("pressure ledgers");
        assert!(
            ledgers.len() < MAX_CONNECTIONS,
            "bounded pressure connection recorders"
        );
        ledgers.push(Arc::clone(&ledger));
        drop(ledgers);
        if self.ingress_bound.is_some() || self.envelope_bound.is_some() {
            self.changed.wake();
        }
        Rc::new(PressureStats(ledger))
    }

    async fn bind_ingress_target(
        &self,
        shared: &Shared,
        target: &PressureTarget,
        addresses: PeerAddresses,
    ) -> TestResult<()> {
        if self.ingress_bound.is_none() && self.envelope_bound.is_none() {
            return Ok(());
        }
        require(
            target.ingress_bound == self.ingress_bound
                && target.envelope_bound == self.envelope_bound
                && self
                    .envelope_bound
                    .is_none_or(|bound| bound.route == shared.route)
                && self.socket_profile_enabled == (shared.route.protocol == Protocol::H1),
            "target and connection envelope bind the actual configured route",
        )?;
        let ledger = within(
            "accepted BODY-credit recorder before request traffic",
            poll_fn(|context| {
                self.changed.register(context.waker());
                let ledger = self
                    .ledgers
                    .lock()
                    .expect("pressure ledgers")
                    .iter()
                    .find(|ledger| {
                        ledger.context.local_endpoint().socket_address()
                            == Some(&socket_address(addresses.remote))
                            && ledger.context.peer_endpoint().socket_address()
                                == Some(&socket_address(addresses.local))
                    })
                    .cloned();
                match ledger {
                    Some(ledger) => Poll::Ready(ledger),
                    None => Poll::Pending,
                }
            }),
        )
        .await?;
        require(
            ledger.context.security() == shared.route.security()
                && if shared.route.protocol == Protocol::H1 {
                    ledger.socket_profile.is_some_and(|receipt| {
                        receipt.direction == SocketProfileDirection::Receiver
                            && receipt.addresses.local == addresses.remote
                            && receipt.addresses.remote == addresses.local
                    })
                } else {
                    ledger.socket_profile.is_none()
                },
            "BODY-credit identity is the actual accepted native socket",
        )?;
        let owner =
            shared
                .owner
                .lock()
                .expect("native worker identity")
                .ok_or(FixtureError::Assertion(
                    "BODY-credit recorder has the started worker identity",
                ))?;
        {
            let mut queue = ledger.queue.lock().expect("pressure queue");
            queue.check_owner(owner, ledger.owner);
            require(
                queue.read == 0 && queue.received == target.baseline,
                "target starts at its unchanged connection credit baseline",
            )?;
            if let Some(bound) = self.ingress_bound {
                queue.bind_ingress(bound, target.id)?;
            }
            if let Some(bound) = self.envelope_bound {
                require(
                    queue.envelope_valid(bound),
                    "valid lifetime connection envelope at target binding",
                )?;
            }
        }
        let mut slot = target.ledger.lock().expect("pressure ledger");
        require(slot.is_none(), "BODY-credit target is bound only once")?;
        *slot = Some(ledger);
        Ok(())
    }

    fn record_socket_profile(&self, receipt: SocketBufferReceipt) -> TestResult<()> {
        require(
            self.socket_profile_enabled,
            "socket profile is explicitly enabled",
        )?;
        let mut profiles = self
            .socket_profiles
            .lock()
            .expect("pressure socket profiles");
        let result = profiles.record(receipt);
        profiles.failed |= result.is_err();
        drop(profiles);
        self.changed.wake();
        result
    }

    fn fail_socket_profile(&self) {
        self.socket_profiles
            .lock()
            .expect("pressure socket profiles")
            .failed = true;
        self.changed.wake();
    }

    async fn paired_socket_profile(
        &self,
        target: usize,
        addresses: PeerAddresses,
    ) -> TestResult<SocketProfilePair> {
        require(self.socket_profile_enabled, "H1 socket profile is enabled")?;
        let pair = within(
            "paired socket profiles before body polling",
            poll_fn(|context| {
                self.changed.register(context.waker());
                let pair = self
                    .socket_profiles
                    .lock()
                    .expect("pressure socket profiles")
                    .ready_pair(addresses);
                match pair {
                    Ok(Some(pair)) => Poll::Ready(Ok(pair)),
                    Ok(None) => Poll::Pending,
                    Err(error) => Poll::Ready(Err(error)),
                }
            }),
        )
        .await??;
        let mut profiles = self
            .socket_profiles
            .lock()
            .expect("pressure socket profiles");
        require(!profiles.failed, "socket profile setup remains valid")?;
        profiles.record_pair(target, pair)?;
        Ok(pair)
    }

    fn report_socket_profiles(&self, route: Route) {
        if !self.socket_profile_enabled {
            return;
        }
        let (receipts, pairs, failed) = {
            let profiles = self
                .socket_profiles
                .lock()
                .expect("pressure socket profiles");
            (profiles.receipts, profiles.pairs, profiles.failed)
        };
        for receipt in receipts.into_iter().flatten() {
            eprintln!("native-pressure {route:?} socket-profile {receipt:?} failed={failed}");
        }
        for (index, pair) in pairs.into_iter().enumerate() {
            eprintln!(
                "native-pressure {route:?} socket-pair target={} {pair:?}",
                index + 1
            );
        }
    }

    fn ledger(&self, context: &Arc<ConnectionContext>) -> TestResult<Arc<PressureLedger>> {
        self.ledgers
            .lock()
            .expect("pressure ledgers")
            .iter()
            .find(|ledger| Arc::ptr_eq(&ledger.context, context))
            .cloned()
            .ok_or_else(|| {
                FixtureError::Assertion("source-affine actual connection recorder").into()
            })
    }

    fn install(
        &self,
        baseline: usize,
    ) -> TestResult<(Arc<PressureTarget>, oneshot::Sender<PressureAction>)> {
        let mut targets = self.targets.lock().expect("pressure targets");
        require(targets.len() < 2, "exactly two bounded pressure actions")?;
        let target = Arc::new(PressureTarget {
            id: targets.len() + 1,
            baseline,
            ingress_bound: self.ingress_bound,
            envelope_bound: self.envelope_bound,
            ingress_recovery: Mutex::new(None),
            owner: Mutex::new(None),
            ledger: Mutex::new(None),
            changed: Arc::clone(&self.changed),
            consumed: AtomicUsize::new(0),
            action: AtomicUsize::new(0),
            entered: AtomicBool::new(false),
            native_cancelled: AtomicBool::new(false),
            eom: AtomicBool::new(false),
            invalid: AtomicBool::new(false),
            created: std::array::from_fn(|_| AtomicUsize::new(0)),
            dropped: std::array::from_fn(|_| AtomicUsize::new(0)),
            stops: Mutex::new(Vec::with_capacity(1)),
            pending: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        });
        let (send, action) = oneshot::channel();
        let mut plan = self.plan.lock().expect("pressure plan");
        require(plan.is_none(), "one pressure target admission at a time")?;
        *plan = Some(PressurePlan {
            target: Arc::clone(&target),
            action,
        });
        targets.push(Arc::clone(&target));
        Ok((target, send))
    }

    fn joined(&self, accepted: usize) -> TestResult<()> {
        let targets = self.targets.lock().expect("pressure targets");
        require(targets.len() == 2, "release and cancel both executed")?;
        for target in targets.iter() {
            target.retired()?;
            if let Some(bound) = target.ingress_bound {
                require(
                    target
                        .ingress_recovery
                        .lock()
                        .expect("BODY-credit live recovery")
                        .is_some_and(|recovery| recovery.admitted(bound, target.id)),
                    "each bounded target recorded zero BODY credit while its worker was live",
                )?;
            }
            if let Some(bound) = target.envelope_bound {
                require(
                    target
                        .ingress_recovery
                        .lock()
                        .expect("envelope live recovery")
                        .is_some_and(|recovery| recovery.envelope_admitted(bound)),
                    "each route-bound target recovered on its original live worker",
                )?;
            }
        }
        let ledgers = self.ledgers.lock().expect("pressure ledgers");
        require(
            ledgers.len() == accepted,
            "every accepted session has its actual recorder",
        )?;
        for ledger in ledgers.iter() {
            let queue = ledger.snapshot();
            if let Some(bound) = self.envelope_bound {
                require(
                    queue.envelope_valid(bound),
                    "final connection envelope retains every callback",
                )?;
                eprintln!(
                    "native-ingress-envelope bound={bound:?} worker_tid={} local={:?} peer={:?} queue={queue:?}",
                    ledger.owner.linux,
                    ledger.context.local_endpoint(),
                    ledger.context.peer_endpoint()
                );
            }
            require(
                !queue.invalid
                    && queue.drops == 1
                    && queue.read == 0
                    && queue.write == 0
                    && queue.opened == queue.closed,
                "joined sessions destroy recorders with zero balanced queues and transactions",
            )?;
        }
        let profiles = self
            .socket_profiles
            .lock()
            .expect("pressure socket profiles");
        require(!profiles.failed, "socket profile failures remain failures")?;
        if self.socket_profile_enabled {
            require(
                profiles
                    .receipts
                    .iter()
                    .flatten()
                    .filter(|receipt| receipt.direction == SocketProfileDirection::Receiver)
                    .count()
                    == accepted
                    && profiles
                        .receipts
                        .iter()
                        .flatten()
                        .filter(|receipt| receipt.direction == SocketProfileDirection::Sender)
                        .count()
                        == 2,
                "every H1 accepted socket and both pressure senders have receipts",
            )?;
            for (target, pair) in targets.iter().zip(profiles.pairs) {
                let pair = pair.ok_or(FixtureError::Assertion("both socket pairs recorded"))?;
                require(
                    target.ledger()?.socket_profile == Some(pair.receiver),
                    "frozen receiver profile belongs to the actual target recorder",
                )?;
            }
            require(
                ledgers.iter().all(|ledger| ledger.socket_profile.is_some()),
                "all H1 receiver profiles are bound to accepted contexts",
            )?;
        } else {
            require(
                profiles.receipts.iter().all(Option::is_none)
                    && profiles.pairs.iter().all(Option::is_none)
                    && ledgers.iter().all(|ledger| ledger.socket_profile.is_none()),
                "H2 remains entirely unprofiled",
            )?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct BackingTotals {
    allocated: AtomicUsize,
    destroyed: AtomicUsize,
    retained: AtomicUsize,
    peak: AtomicUsize,
    invalid: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
struct BackingCounts {
    allocated: usize,
    destroyed: usize,
    retained: usize,
    peak: usize,
    invalid: bool,
}

impl BackingTotals {
    fn allocate(&self, capacity: usize) {
        if self
            .allocated
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1).filter(|value| *value <= MAX_TARGETS)
            })
            .is_err()
        {
            self.invalid.store(true, Ordering::Release);
        }
        match self
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(capacity)
            }) {
            Ok(previous) => {
                self.peak.fetch_max(previous + capacity, Ordering::AcqRel);
            }
            Err(_) => self.invalid.store(true, Ordering::Release),
        }
    }

    fn destroy(&self, capacity: usize) {
        let retained = self
            .retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(capacity)
            });
        let destroyed = self
            .destroyed
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1).filter(|value| *value <= MAX_TARGETS)
            });
        if retained.is_err() || destroyed.is_err() {
            self.invalid.store(true, Ordering::Release);
        }
    }

    fn snapshot(&self) -> BackingCounts {
        BackingCounts {
            allocated: self.allocated.load(Ordering::Acquire),
            destroyed: self.destroyed.load(Ordering::Acquire),
            retained: self.retained.load(Ordering::Acquire),
            peak: self.peak.load(Ordering::Acquire),
            invalid: self.invalid.load(Ordering::Acquire),
        }
    }

    fn reconciled(&self, expected: usize) -> TestResult<()> {
        let counts = self.snapshot();
        require(
            !counts.invalid
                && counts.allocated == expected
                && counts.destroyed == expected
                && counts.retained == 0
                && (BODY_SIZE..=2 * BODY_SIZE).contains(&counts.peak),
            "bounded response backing allocations actually destroyed exactly once",
        )
    }
}

struct BackingRecord {
    target: usize,
    keep_alias: bool,
    capacity: AtomicUsize,
    retained: AtomicUsize,
    destroyed: AtomicUsize,
    alias_bytes: AtomicUsize,
    invalid: AtomicBool,
    totals: Arc<BackingTotals>,
}

impl BackingRecord {
    fn zero(&self) -> Result<(), FixtureError> {
        let destroyed = self.destroyed.load(Ordering::Acquire);
        let capacity = self.capacity.load(Ordering::Acquire);
        if self.invalid.load(Ordering::Acquire) || capacity != BODY_SIZE || destroyed > 1 {
            return Err(FixtureError::Assertion(
                "confirmed owned response backing identity/capacity",
            ));
        }
        if destroyed == 0 || self.retained.load(Ordering::Acquire) != 0 {
            return Err(FixtureError::BackingRetained {
                target: self.target,
                capacity,
            });
        }
        Ok(())
    }

    fn held(&self) -> TestResult<()> {
        require(
            self.retained.load(Ordering::Acquire) == self.capacity.load(Ordering::Acquire)
                && self.destroyed.load(Ordering::Acquire) == 0,
            "held response retains its entire actual backing",
        )?;
        match self.zero() {
            Err(FixtureError::BackingRetained { target, capacity })
                if target == self.target && capacity == BODY_SIZE =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
            Ok(()) => Err(FixtureError::Assertion(
                "zero-retention predicate accepted a live backing",
            )
            .into()),
        }
    }
}

struct OwnedResponseBacking {
    buffer: Option<Vec<u8>>,
    owner: ThreadId,
    record: Arc<BackingRecord>,
    complete: Option<oneshot::Sender<()>>,
}

impl AsRef<[u8]> for OwnedResponseBacking {
    fn as_ref(&self) -> &[u8] {
        self.buffer
            .as_deref()
            .expect("Bytes accesses its live owned response backing")
    }
}

impl Drop for OwnedResponseBacking {
    fn drop(&mut self) {
        let buffer = self.buffer.take();
        let capacity = self.record.capacity.load(Ordering::Acquire);
        let valid = buffer
            .as_ref()
            .is_some_and(|buffer| buffer.capacity() == capacity);
        drop(buffer);
        if !valid || std::thread::current().id() != self.owner {
            self.record.invalid.store(true, Ordering::Release);
            self.record.totals.invalid.store(true, Ordering::Release);
        }
        self.record.totals.destroy(capacity);
        let retained =
            self.record
                .retained
                .compare_exchange(capacity, 0, Ordering::AcqRel, Ordering::Acquire);
        let destroyed =
            self.record
                .destroyed
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
        if retained.is_err() || destroyed.is_err() {
            self.record.invalid.store(true, Ordering::Release);
            self.record.totals.invalid.store(true, Ordering::Release);
        }
        if let Some(complete) = self.complete.take() {
            let _ = complete.send(());
        }
    }
}

struct BackingBuild {
    record: Arc<BackingRecord>,
    complete: oneshot::Sender<()>,
}

impl BackingBuild {
    fn bytes(self) -> TestResult<Bytes> {
        let buffer = DOWNLOAD.to_vec();
        let capacity = buffer.capacity();
        require(
            capacity == BODY_SIZE,
            "actual fixed owned response Vec capacity",
        )?;
        require(
            self.record
                .capacity
                .compare_exchange(0, capacity, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "one owned payload allocation per response target",
        )?;
        self.record.retained.store(capacity, Ordering::Release);
        self.record.totals.allocate(capacity);
        Ok(Bytes::from_owner(OwnedResponseBacking {
            buffer: Some(buffer),
            owner: std::thread::current().id(),
            record: self.record,
            complete: Some(self.complete),
        }))
    }
}

struct BackingWait {
    record: Arc<BackingRecord>,
    complete: oneshot::Receiver<()>,
}

struct RetainedBackingAlias {
    record: Arc<BackingRecord>,
    bytes: Bytes,
}

struct TargetPlan {
    evidence: Arc<TargetEvidence>,
    release: oneshot::Receiver<()>,
    backing: Option<BackingBuild>,
}

struct TargetControl {
    evidence: Arc<TargetEvidence>,
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    backing: Option<BackingWait>,
}

struct ObservedRequest {
    path: String,
    context: Arc<ConnectionContext>,
    stream: Option<StreamId>,
}

type EgressProductionResult = Result<(), Arc<mpsc::error::SendError<()>>>;
type EgressProduction = Pin<Box<dyn Future<Output = Result<(), mpsc::error::SendError<()>>>>>;

struct EgressEvidence {
    id: usize,
    changed: Arc<PressureWake>,
    prefix: AtomicUsize,
    peer_bytes: AtomicUsize,
    produced: AtomicUsize,
    delivered: AtomicUsize,
    reads: AtomicUsize,
    read_drops: AtomicUsize,
    source_created: AtomicUsize,
    source_drops: AtomicUsize,
    terminal: AtomicBool,
    action: AtomicUsize,
    pending_receipts: AtomicUsize,
    completed_receipts: AtomicUsize,
    invalid: AtomicBool,
    stops: Mutex<Vec<Option<HttpErrorCode>>>,
    producer_result: Mutex<Option<EgressProductionResult>>,
    peer_reset: Mutex<Option<h2::Error>>,
    socket_pair: Mutex<Option<SocketProfilePair>>,
}

impl EgressEvidence {
    fn add(&self, counter: &AtomicUsize, amount: usize) {
        if counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value
                    .checked_add(amount)
                    .filter(|value| *value <= EGRESS_BYTES)
            })
            .is_err()
        {
            self.invalid.store(true, Ordering::Release);
        }
        self.changed.wake();
    }

    fn retired(&self, route: Route) -> TestResult<()> {
        let released = self.action.load(Ordering::Acquire) == 1;
        require(
            !self.invalid.load(Ordering::Acquire),
            "valid egress observations",
        )?;
        require(
            self.source_created.load(Ordering::Acquire) == 1
                && self.source_drops.load(Ordering::Acquire) == 1
                && self.reads.load(Ordering::Acquire) > 0
                && self.reads.load(Ordering::Acquire) == self.read_drops.load(Ordering::Acquire),
            "actual response source and every owned read future destroyed",
        )?;
        require(
            self.pending_receipts.load(Ordering::Acquire) == 1
                && self.completed_receipts.load(Ordering::Acquire) == 2,
            "pending then sticky completed actual egress exchange receipt",
        )?;
        let result = self.producer_result.lock().expect("egress producer result");
        require(
            if released {
                matches!(&*result, Some(Ok(())))
            } else {
                matches!(&*result, Some(Err(_)))
            },
            "release completes producer; cancellation observes actual closed admission",
        )?;
        require(
            self.terminal.load(Ordering::Acquire) == released
                && self.stops.lock().expect("egress stops").len() == usize::from(!released),
            "only cancelled response requires actual native source stop",
        )?;
        let produced = self.produced.load(Ordering::Acquire);
        let delivered = self.delivered.load(Ordering::Acquire);
        require(
            if released {
                produced == EGRESS_BYTES && delivered == EGRESS_BYTES
            } else {
                self.action.load(Ordering::Acquire) == 2
                    && delivered > self.prefix.load(Ordering::Acquire)
                    && delivered <= produced
                    && produced < EGRESS_BYTES
            },
            "exact completed egress bytes or explicitly incomplete cancellation",
        )?;
        require(
            self.peer_reset.lock().expect("egress peer reset").is_some()
                == (!released && route.protocol == Protocol::H2),
            "actual H2 reset retained separately from H1 transport reset",
        )
    }
}

struct EgressPlan {
    evidence: Arc<EgressEvidence>,
    receiver: mpsc::Receiver<Bytes>,
}

struct EgressState {
    plan: Mutex<Option<EgressPlan>>,
    targets: Mutex<Vec<Arc<EgressEvidence>>>,
    profiles: Mutex<Vec<SocketBufferReceipt>>,
}

impl EgressState {
    fn new() -> Self {
        Self {
            plan: Mutex::new(None),
            targets: Mutex::new(Vec::with_capacity(2)),
            profiles: Mutex::new(Vec::with_capacity(2 * MAX_CONNECTIONS)),
        }
    }

    fn install(&self) -> TestResult<EgressProducer> {
        let mut targets = self.targets.lock().expect("egress targets");
        let mut plan = self.plan.lock().expect("egress plan");
        require(
            targets.len() < 2 && plan.is_none(),
            "two serial egress actions",
        )?;
        let evidence = Arc::new(EgressEvidence {
            id: targets.len() + 1,
            changed: Arc::new(PressureWake::default()),
            prefix: AtomicUsize::new(0),
            peer_bytes: AtomicUsize::new(0),
            produced: AtomicUsize::new(0),
            delivered: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            read_drops: AtomicUsize::new(0),
            source_created: AtomicUsize::new(0),
            source_drops: AtomicUsize::new(0),
            terminal: AtomicBool::new(false),
            action: AtomicUsize::new(0),
            pending_receipts: AtomicUsize::new(0),
            completed_receipts: AtomicUsize::new(0),
            invalid: AtomicBool::new(false),
            stops: Mutex::new(Vec::with_capacity(1)),
            producer_result: Mutex::new(None),
            peer_reset: Mutex::new(None),
            socket_pair: Mutex::new(None),
        });
        let (sender, receiver) = mpsc::channel(EGRESS_SLOTS);
        *plan = Some(EgressPlan {
            evidence: Arc::clone(&evidence),
            receiver,
        });
        targets.push(Arc::clone(&evidence));
        let output = sender.clone();
        let target = Arc::clone(&evidence);
        let future = Box::pin(async move {
            for offset in (CHUNK..EGRESS_BYTES).step_by(CHUNK) {
                let permit = output.reserve().await?;
                let bytes: Vec<_> = (offset..offset + CHUNK)
                    .map(|index| (index % 251) as u8)
                    .collect();
                permit.send(Bytes::from(bytes));
                target.add(&target.produced, CHUNK);
            }
            Ok(())
        });
        Ok(EgressProducer {
            evidence,
            sender,
            future,
            prefix_started: false,
            pending: false,
        })
    }

    fn profile(&self, receipt: SocketBufferReceipt) -> TestResult<()> {
        receipt.validate()?;
        let mut profiles = self.profiles.lock().expect("egress socket profiles");
        require(
            profiles.len() < 2 * MAX_CONNECTIONS
                && !profiles.iter().any(|previous| {
                    previous.direction == receipt.direction
                        && previous.addresses == receipt.addresses
                }),
            "bounded unique egress socket profiles",
        )?;
        profiles.push(receipt);
        Ok(())
    }

    fn pair(&self, context: &ConnectionContext) -> TestResult<SocketProfilePair> {
        let profiles = self.profiles.lock().expect("egress socket profiles");
        let sender = profiles
            .iter()
            .find(|entry| {
                entry.direction == SocketProfileDirection::Sender
                    && context.local_endpoint().socket_address()
                        == Some(&socket_address(entry.addresses.local))
                    && context.peer_endpoint().socket_address()
                        == Some(&socket_address(entry.addresses.remote))
            })
            .copied()
            .ok_or(FixtureError::Assertion(
                "actual accepted egress sender profile",
            ))?;
        let receiver = profiles
            .iter()
            .find(|entry| {
                entry.direction == SocketProfileDirection::Receiver
                    && entry.addresses.local == sender.addresses.remote
                    && entry.addresses.remote == sender.addresses.local
            })
            .copied()
            .ok_or(FixtureError::Assertion(
                "paired independent egress receiver profile",
            ))?;
        Ok(SocketProfilePair { sender, receiver })
    }

    fn joined(&self, route: Route, accepted: usize) -> TestResult<()> {
        require(
            self.plan.lock().expect("egress plan").is_none(),
            "egress plan consumed",
        )?;
        let targets = self.targets.lock().expect("egress targets");
        require(
            targets.len() == 2,
            "egress release and cancel both executed",
        )?;
        for target in targets.iter() {
            target.retired(route)?;
            require(
                target.socket_pair.lock().expect("egress pair").is_some()
                    == (route.protocol == Protocol::H1),
                "egress profiles only on H1",
            )?;
        }
        let profiles = self.profiles.lock().expect("egress socket profiles");
        for direction in [
            SocketProfileDirection::Sender,
            SocketProfileDirection::Receiver,
        ] {
            require(
                profiles
                    .iter()
                    .filter(|entry| entry.direction == direction)
                    .count()
                    == if route.protocol == Protocol::H1 {
                        accepted
                    } else {
                        0
                    },
                "every egress socket has its declared profile",
            )?;
        }
        Ok(())
    }
}

struct EgressProducer {
    evidence: Arc<EgressEvidence>,
    sender: mpsc::Sender<Bytes>,
    future: EgressProduction,
    prefix_started: bool,
    pending: bool,
}

impl EgressProducer {
    async fn send_prefix(&mut self) -> TestResult<()> {
        if let Some(error) = self.unexpected_error() {
            return Err(error);
        }
        require(
            !self.prefix_started
                && self.evidence.produced.load(Ordering::Acquire) == 0
                && self
                    .evidence
                    .producer_result
                    .lock()
                    .expect("egress producer result")
                    .is_none()
                && !self.pending,
            "fresh egress prefix setup",
        )?;
        self.prefix_started = true;
        let permit = match self.sender.reserve().await {
            Ok(permit) => permit,
            Err(error) => {
                let original = Arc::new(error);
                *self
                    .evidence
                    .producer_result
                    .lock()
                    .expect("egress producer result") = Some(Err(Arc::clone(&original)));
                return Err(Box::new(original));
            }
        };
        let bytes: Vec<_> = (0..CHUNK).map(|index| (index % 251) as u8).collect();
        permit.send(Bytes::from(bytes));
        self.evidence.add(&self.evidence.produced, CHUNK);
        Ok(())
    }

    async fn prefix(
        &mut self,
        work: impl Future<Output = TestResult<usize>>,
    ) -> TestResult<(usize, usize)> {
        within("egress producer and independent peer", async {
            self.send_prefix().await?;
            let prefix = work.await?;
            let baseline = self.evidence.produced.load(Ordering::Acquire);
            require(
                prefix > 0 && prefix <= CHUNK && baseline == CHUNK,
                "actual prefix precedes egress continuation",
            )?;
            Ok((prefix, baseline))
        })
        .await?
    }

    fn poll(&mut self, context: &mut Context<'_>) {
        self.pending = false;
        if self
            .evidence
            .producer_result
            .lock()
            .expect("egress producer result")
            .is_some()
        {
            return;
        }
        match self.future.as_mut().poll(context) {
            Poll::Ready(result) => {
                *self
                    .evidence
                    .producer_result
                    .lock()
                    .expect("egress producer result") = Some(result.map_err(Arc::new));
            }
            Poll::Pending => self.pending = self.sender.capacity() == 0,
        }
    }

    fn unexpected_error(&self) -> Option<BoxError> {
        if self.evidence.action.load(Ordering::Acquire) == 2 {
            return None;
        }
        self.evidence
            .producer_result
            .lock()
            .expect("egress producer result")
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(|error| Box::new(Arc::clone(error)) as BoxError)
    }

    async fn during<Output>(
        &mut self,
        work: impl Future<Output = TestResult<Output>>,
    ) -> TestResult<Output> {
        let mut work = Box::pin(work);
        within(
            "egress producer and independent peer",
            poll_fn(|context| {
                self.poll(context);
                match self.unexpected_error() {
                    Some(error) => Poll::Ready(Err(error)),
                    None => work.as_mut().poll(context),
                }
            }),
        )
        .await?
    }

    async fn finish(&mut self) -> TestResult<()> {
        within(
            "actual egress producer return",
            poll_fn(|context| {
                self.poll(context);
                if self
                    .evidence
                    .producer_result
                    .lock()
                    .expect("egress producer result")
                    .is_some()
                {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
    }
}

struct EgressSourceState {
    receiver: Option<mpsc::Receiver<Bytes>>,
    buffered: Bytes,
    offset: usize,
    stop_error: Option<HttpErrorCode>,
}

struct EgressSource {
    state: Rc<RefCell<EgressSourceState>>,
    shared: Arc<Shared>,
    evidence: Arc<EgressEvidence>,
    head: bool,
}

impl EgressSource {
    fn close(&self) {
        let (receiver, buffered) = {
            let mut state = self.state.borrow_mut();
            (state.receiver.take(), std::mem::take(&mut state.buffered))
        };
        drop(receiver);
        drop(buffered);
    }
}

impl Drop for EgressSource {
    fn drop(&mut self) {
        self.shared.check_owner();
        self.close();
        if self.evidence.action.load(Ordering::Acquire) == 0 {
            self.evidence.invalid.store(true, Ordering::Release);
        }
        self.evidence.add(&self.evidence.source_drops, 1);
    }
}

struct EgressReadGuard {
    shared: Arc<Shared>,
    evidence: Arc<EgressEvidence>,
}

impl Drop for EgressReadGuard {
    fn drop(&mut self) {
        self.shared.check_owner();
        self.evidence.add(&self.evidence.read_drops, 1);
    }
}

impl HttpSource for EgressSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.shared.check_owner();
        let result = (|| -> TestResult<HttpHeaderEvent> {
            require(
                std::mem::replace(&mut self.head, false),
                "one egress response header",
            )?;
            let mut headers = HttpMessage::new();
            headers.set_status_code(200)?;
            headers.set_status_message(HttpMessage::default_reason(200))?;
            headers.set_http_version(1, 1);
            headers
                .headers_mut()
                .add(b"content-length", EGRESS_BYTES.to_string());
            Ok(HttpHeaderEvent::new(headers, false)?)
        })()
        .map_err(|error| self.shared.fail(error));
        Box::pin(ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.shared.check_owner();
        self.evidence.add(&self.evidence.reads, 1);
        let state = Rc::clone(&self.state);
        let guard = EgressReadGuard {
            shared: Arc::clone(&self.shared),
            evidence: Arc::clone(&self.evidence),
        };
        Box::pin(async move {
            poll_fn(|context| {
                guard.shared.check_owner();
                let mut state = state.borrow_mut();
                if state.receiver.is_none() {
                    return Poll::Ready(Err(HttpError::with_message(
                        state.stop_error.unwrap_or(HttpErrorCode::DROPPED),
                        b"egress response source retired",
                    )));
                }
                if maximum == 0 || state.offset >= EGRESS_BYTES {
                    return Poll::Ready(Err(guard
                        .shared
                        .fail(FixtureError::Assertion("valid live egress read").into())));
                }
                if state.buffered.is_empty() {
                    match state
                        .receiver
                        .as_mut()
                        .expect("live egress receiver")
                        .poll_recv(context)
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(bytes)) if bytes.len() == CHUNK => state.buffered = bytes,
                        Poll::Ready(Some(_)) => {
                            return Poll::Ready(Err(guard.shared.fail(
                                FixtureError::Assertion("fixed bounded egress channel chunks")
                                    .into(),
                            )));
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(Err(guard.shared.fail(
                                FixtureError::Assertion(
                                    "egress producer cannot end before declared body",
                                )
                                .into(),
                            )));
                        }
                    }
                }
                let amount = (maximum as usize).min(state.buffered.len());
                let bytes = state.buffered.split_to(amount);
                state.offset += amount;
                let terminal = state.offset == EGRESS_BYTES;
                guard
                    .evidence
                    .delivered
                    .store(state.offset, Ordering::Release);
                guard.evidence.terminal.store(terminal, Ordering::Release);
                let event = BufQueue::from_bytes(bytes, amount)
                    .map(|body| HttpBodyEvent::body(body, terminal))
                    .map_err(|error| guard.shared.fail(error.into()));
                drop(state);
                guard.evidence.changed.wake();
                Poll::Ready(event)
            })
            .await
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.shared.check_owner();
        let mut stops = self.evidence.stops.lock().expect("egress stops");
        if !stops.is_empty() || self.evidence.action.load(Ordering::Acquire) != 2 {
            self.evidence.invalid.store(true, Ordering::Release);
        }
        if stops.is_empty() {
            stops.push(error);
        }
        drop(stops);
        self.state.borrow_mut().stop_error = Some(error.unwrap_or(HttpErrorCode::DROPPED));
        self.close();
    }
}

#[derive(Clone)]
struct EgressReceipt {
    receipt: ExchangeCompletion,
    evidence: Arc<EgressEvidence>,
}

struct Shared {
    route: Route,
    owner: Mutex<Option<WorkerIdentity>>,
    starts: AtomicUsize,
    stops: AtomicUsize,
    handler_drops: AtomicUsize,
    lifecycle_drops: AtomicUsize,
    destroyed: AtomicUsize,
    wrong_thread: AtomicBool,
    accepted: Mutex<Vec<ConnectionInfo>>,
    requests: Mutex<Vec<ObservedRequest>>,
    resets: Mutex<Vec<(StreamId, HttpErrorCode)>>,
    targets: Mutex<Vec<Arc<TargetEvidence>>>,
    plan: Mutex<Option<TargetPlan>>,
    errors: Mutex<Vec<BoxError>>,
    pressure: Option<Arc<PressureState>>,
    backing: Option<Arc<BackingTotals>>,
    egress: Option<Arc<EgressState>>,
}

impl Shared {
    fn new(route: Route) -> Arc<Self> {
        Arc::new(Self {
            route,
            owner: Mutex::new(None),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            handler_drops: AtomicUsize::new(0),
            lifecycle_drops: AtomicUsize::new(0),
            destroyed: AtomicUsize::new(0),
            wrong_thread: AtomicBool::new(false),
            accepted: Mutex::new(Vec::with_capacity(MAX_CONNECTIONS)),
            requests: Mutex::new(Vec::with_capacity(MAX_REQUESTS)),
            resets: Mutex::new(Vec::with_capacity(MAX_TARGETS)),
            targets: Mutex::new(Vec::with_capacity(MAX_TARGETS)),
            plan: Mutex::new(None),
            errors: Mutex::new(Vec::with_capacity(MAX_REQUESTS)),
            pressure: None,
            backing: None,
            egress: None,
        })
    }

    fn check_owner(&self) {
        if self
            .owner
            .lock()
            .expect("native worker identity")
            .is_none_or(|owner| {
                let current = current_identity();
                current.rust != owner.rust || current.linux != owner.linux
            })
        {
            self.wrong_thread.store(true, Ordering::Release);
        }
    }

    fn fail(&self, error: BoxError) -> HttpError {
        let native =
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes());
        let mut errors = self.errors.lock().expect("native fixture failures");
        assert!(
            errors.len() < MAX_REQUESTS,
            "native fixture error capacity: {error:?}"
        );
        errors.push(error);
        native
    }

    fn install(&self, stage: Stage) -> TestResult<TargetControl> {
        let mut targets = self.targets.lock().expect("target evidence");
        require(targets.len() < MAX_TARGETS, "bounded target history")?;
        let (entered, receiver) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let backing = self.backing.as_ref().map(|totals| {
            let target = targets.len() + 1;
            let record = Arc::new(BackingRecord {
                target,
                keep_alias: target == 2 * (WARMUPS + 1) && stage == Stage::Response,
                capacity: AtomicUsize::new(0),
                retained: AtomicUsize::new(0),
                destroyed: AtomicUsize::new(0),
                alias_bytes: AtomicUsize::new(0),
                invalid: AtomicBool::new(false),
                totals: Arc::clone(totals),
            });
            let (complete, receiver) = oneshot::channel();
            (
                BackingBuild {
                    record: Arc::clone(&record),
                    complete,
                },
                BackingWait {
                    record,
                    complete: receiver,
                },
            )
        });
        let (backing_build, backing_wait) = match backing {
            Some((build, wait)) => (Some(build), Some(wait)),
            None => (None, None),
        };
        let evidence = Arc::new(TargetEvidence {
            id: targets.len() + 1,
            stage,
            owner: Mutex::new(None),
            entered: Mutex::new(Some(entered)),
            cancelling: AtomicBool::new(false),
            premature_retirement: AtomicBool::new(false),
            wrong_thread: AtomicBool::new(false),
            created: std::array::from_fn(|_| AtomicUsize::new(0)),
            dropped: std::array::from_fn(|_| AtomicUsize::new(0)),
            stops: Mutex::new(Vec::with_capacity(2)),
            ingress_error: Mutex::new(None),
            peer_reset: Mutex::new(None),
            upload_bytes: AtomicUsize::new(0),
            response_bytes: AtomicUsize::new(0),
            pending_checks: AtomicUsize::new(0),
            completed_checks: AtomicUsize::new(0),
            backing: backing_build
                .as_ref()
                .map(|build| Arc::clone(&build.record)),
        });
        let mut plan = self.plan.lock().expect("native target plan");
        require(plan.is_none(), "one admitted target plan at a time")?;
        *plan = Some(TargetPlan {
            evidence: Arc::clone(&evidence),
            release: gate,
            backing: backing_build,
        });
        targets.push(Arc::clone(&evidence));
        Ok(TargetControl {
            evidence,
            entered: receiver,
            release,
            backing: backing_wait,
        })
    }

    fn observe_request(
        &self,
        path: String,
        context: &HandlerSessionContext,
        stream: Option<StreamId>,
    ) -> TestResult<()> {
        self.check_owner();
        let connection = context
            .connection_context()
            .ok_or(FixtureError::Assertion("actual native connection context"))?;
        require(
            context.peer_identity().is_none(),
            "server-auth TLS does not invent a client identity",
        )?;
        require(
            connection.security() == self.route.security(),
            "actual immediate-hop security",
        )?;
        require(
            connection.tls_cipher_suite().is_some() == self.route.tls
                && connection.tls_key_exchange_group().is_some() == self.route.tls,
            "TLS parameters exactly on TLS connections",
        )?;
        require(
            connection.peer_identity().is_none() && connection.upstream_origin().is_none(),
            "direct native connection has no identity or logical origin",
        )?;
        let accepted = self.accepted.lock().expect("accepted metadata");
        require(
            accepted
                .iter()
                .any(|entry| Arc::ptr_eq(&entry.context, connection)),
            "handler context is the actual accepted context",
        )?;
        drop(accepted);
        let mut requests = self.requests.lock().expect("native request observations");
        require(
            requests.len() < MAX_REQUESTS,
            "bounded request observations",
        )?;
        requests.push(ObservedRequest {
            path,
            context: Arc::clone(connection),
            stream,
        });
        Ok(())
    }

    fn request(
        &self,
        path: &str,
        peer: PeerAddresses,
    ) -> TestResult<(Arc<ConnectionContext>, Option<StreamId>)> {
        let requests = self.requests.lock().expect("native request observations");
        let entry = requests
            .iter()
            .find(|entry| entry.path == path)
            .ok_or(FixtureError::Assertion("request reached native handler"))?;
        require(
            entry.context.local_endpoint().socket_address() == Some(&socket_address(peer.remote)),
            "native local endpoint equals independently observed peer remote endpoint",
        )?;
        require(
            entry.context.peer_endpoint().socket_address() == Some(&socket_address(peer.local)),
            "native remote endpoint equals independently observed peer local endpoint",
        )?;
        Ok((Arc::clone(&entry.context), entry.stream))
    }

    fn joined(&self) -> TestResult<()> {
        require(
            self.starts.load(Ordering::Acquire) == 1 && self.stops.load(Ordering::Acquire) == 1,
            "one worker started and stopped",
        )?;
        require(
            !self.wrong_thread.load(Ordering::Acquire),
            "native callbacks and destruction remain worker-local",
        )?;
        require(
            self.handler_drops.load(Ordering::Acquire) == 1,
            "worker-owned handler destroyed",
        )?;
        let accepted = self.accepted.lock().expect("accepted metadata").len();
        require(
            accepted == self.destroyed.load(Ordering::Acquire),
            "all accepted native sessions emitted destruction",
        )?;
        require(
            accepted == self.lifecycle_drops.load(Ordering::Acquire),
            "all native lifecycle observers destroyed",
        )?;
        for target in self.targets.lock().expect("target history").iter() {
            target.retired()?;
        }
        Ok(())
    }

    fn report_targets(&self) {
        for target in self.targets.lock().expect("target history").iter() {
            let created: [usize; 3] =
                std::array::from_fn(|index| target.created[index].load(Ordering::Acquire));
            let dropped: [usize; 3] =
                std::array::from_fn(|index| target.dropped[index].load(Ordering::Acquire));
            eprintln!(
                "native-lifecycle {:?} target={} stage={:?} upload={} response={} created={created:?} dropped={dropped:?} pending_checks={} completed_checks={} premature_retirement={} source_stops={:?} ingress_error={:?} peer_reset={:?}",
                self.route,
                target.id,
                target.stage,
                target.upload_bytes.load(Ordering::Acquire),
                target.response_bytes.load(Ordering::Acquire),
                target.pending_checks.load(Ordering::Acquire),
                target.completed_checks.load(Ordering::Acquire),
                target.premature_retirement.load(Ordering::Acquire),
                target.stops.lock().expect("source stops"),
                target.ingress_error.lock().expect("native ingress error"),
                target.peer_reset.lock().expect("independent reset error"),
            );
        }
    }
}

impl AcceptedConnectionObserver for Shared {
    fn on_accepted(&self, connection: &ConnectionInfo) {
        self.check_owner();
        if connection.protocol != self.route.codec()
            || connection.secure != self.route.tls
            || connection.zero_copy_enabled
            || connection.peer_identity.is_some()
        {
            self.fail(
                FixtureError::Assertion(
                    "actual native protocol/security/default zero-copy capability",
                )
                .into(),
            );
        }
        let mut accepted = self.accepted.lock().expect("accepted metadata");
        assert!(
            accepted.len() < MAX_CONNECTIONS,
            "bounded accepted observations"
        );
        accepted.push(connection.clone());
    }
}

impl ServerObserver for Shared {
    fn on_thread_start(&self, worker: usize) {
        assert_eq!(worker, 0, "one native worker");
        let identity = current_identity();
        assert!(identity.linux > 0, "actual Linux worker TID");
        *self.owner.lock().expect("native worker identity") = Some(identity);
        self.starts.fetch_add(1, Ordering::AcqRel);
    }

    fn on_thread_stop(&self, worker: usize) {
        assert_eq!(worker, 0, "one native worker");
        self.check_owner();
        self.stops.fetch_add(1, Ordering::AcqRel);
    }
}

struct SessionObserver(Arc<Shared>);

impl LifecycleObserver for SessionObserver {
    fn on_event(&self, event: LifecycleEvent) {
        self.0.check_owner();
        match event {
            LifecycleEvent::PeerReset { stream, code } => {
                let mut resets = self.0.resets.lock().expect("native reset observations");
                assert!(resets.len() < MAX_TARGETS, "bounded reset observations");
                resets.push((stream, code));
            }
            LifecycleEvent::Destroy => {
                self.0.destroyed.fetch_add(1, Ordering::AcqRel);
            }
            _ => {}
        }
    }
}

impl Drop for SessionObserver {
    fn drop(&mut self) {
        self.0.check_owner();
        self.0.lifecycle_drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct ReceiptRecord {
    receipt: ExchangeCompletion,
    evidence: Arc<TargetEvidence>,
}

struct NativeHandler {
    shared: Arc<Shared>,
    receipt: Rc<RefCell<Option<ReceiptRecord>>>,
    pressure_receipt: Option<Rc<RefCell<Option<PressureReceipt>>>>,
    backing_alias: Option<Rc<RefCell<Option<RetainedBackingAlias>>>>,
    egress_receipt: Option<Rc<RefCell<Option<EgressReceipt>>>>,
}

impl Drop for NativeHandler {
    fn drop(&mut self) {
        self.shared.check_owner();
        self.shared.handler_drops.fetch_add(1, Ordering::AcqRel);
    }
}

enum HandlerOutcome {
    Response(HttpSourceHolder),
    Cancelled(HttpError),
}

impl HttpHandler for NativeHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let shared = Arc::clone(&self.shared);
        let receipt = Rc::clone(&self.receipt);
        let pressure_receipt = self.pressure_receipt.as_ref().map(Rc::clone);
        let backing_alias = self.backing_alias.as_ref().map(Rc::clone);
        let egress_receipt = self.egress_receipt.as_ref().map(Rc::clone);
        Box::pin(async move {
            match native_request(
                NativeRequestState {
                    shared: &shared,
                    stored: &receipt,
                    pressure_receipt: pressure_receipt.as_ref(),
                    backing_alias: backing_alias.as_ref(),
                    egress_receipt: egress_receipt.as_ref(),
                },
                context,
                request,
            )
            .await
            {
                Ok(HandlerOutcome::Response(source)) => Ok(source),
                Ok(HandlerOutcome::Cancelled(error)) => Err(error),
                Err(error) => Err(shared.fail(error)),
            }
        })
    }
}

#[derive(Clone)]
struct PressureReceipt {
    receipt: ExchangeCompletion,
    target: Arc<PressureTarget>,
}

struct PressureSource {
    inner: HttpSourceHolder,
    guard: PressureGuard,
}

impl HttpSource for PressureSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        let target = &self.guard.target;
        target.check_owner();
        {
            let mut stops = target.stops.lock().expect("pressure source stops");
            if stops.is_empty() {
                stops.push(error);
            } else {
                target.invalid.store(true, Ordering::Release);
            }
        }
        self.inner.stop_reading(error);
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.inner.get_stream_id()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.inner.set_read_timeout(timeout);
    }
}

async fn pressure_read(
    request: &mut HttpSourceHolder,
    target: &Arc<PressureTarget>,
    end: usize,
) -> TestResult<()> {
    loop {
        let offset = target.consumed.load(Ordering::Acquire);
        if offset == end && (end != PRESSURE_BYTES || target.eom.load(Ordering::Acquire)) {
            return Ok(());
        }
        let maximum = (end - offset).clamp(1, CHUNK);
        let event = within(
            "native pressure upload",
            request.read_body_event(maximum as u32),
        )
        .await??;
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                let mut consumed = offset;
                for segment in body.segments() {
                    require(
                        segment.len() <= end - consumed,
                        "bounded streamed native pressure bytes",
                    )?;
                    require(
                        segment
                            .iter()
                            .enumerate()
                            .all(|(index, byte)| *byte == ((consumed + index) % 251) as u8),
                        "byte-exact offset-sensitive pressure upload",
                    )?;
                    consumed += segment.len();
                }
                require(
                    consumed > offset || eom,
                    "positive pressure body progress or EOM",
                )?;
                target.consumed.store(consumed, Ordering::Release);
                if eom {
                    require(
                        consumed == PRESSURE_BYTES,
                        "pressure upload EOM at exact declared length",
                    )?;
                    target.eom.store(true, Ordering::Release);
                }
                target.changed.wake();
            }
            HttpBodyEventKind::Suspend(resume) => {
                let status = within("native pressure upload resume", resume).await?;
                if status != ResumeStatus::Signalled {
                    return Err(FixtureError::Resume(status).into());
                }
            }
            _ => return Err(FixtureError::Assertion("unexpected pressure upload event").into()),
        }
    }
}

async fn native_pressure_request(
    shared: &Arc<Shared>,
    stored: &Rc<RefCell<Option<PressureReceipt>>>,
    kind: &str,
    identifier: usize,
    eom: bool,
    context: HandlerSessionContext,
    request: HttpSourceHolder,
) -> TestResult<HandlerOutcome> {
    let state = shared
        .pressure
        .as_ref()
        .ok_or(FixtureError::Assertion("pressure fixture state"))?;
    if kind == "pressure-pending" || kind == "pressure-retired" {
        require(eom, "pressure receipt probe has no upload")?;
        let record = stored
            .borrow()
            .clone()
            .ok_or(FixtureError::Assertion("actual pressure receipt"))?;
        require(
            record.target.id == identifier,
            "pressure probe targets the stored exchange",
        )?;
        if kind == "pressure-pending" {
            let mut wait = record.receipt.wait();
            require(
                poll_fn(|context| Poll::Ready(wait.as_mut().poll(context).is_pending())).await,
                "pressure receipt remains pending during healthy progress",
            )?;
            record.target.increment(&record.target.pending);
        } else {
            for _ in 0..2 {
                within("actual pressure exchange retirement", record.receipt.wait()).await??;
                record.target.increment(&record.target.completed);
            }
            stored.borrow_mut().take();
        }
        return healthy_response();
    }
    require(
        kind == "pressure" && !eom,
        "pressure target is a streaming upload",
    )?;
    let plan = state
        .plan
        .lock()
        .expect("pressure plan")
        .take()
        .ok_or(FixtureError::Assertion("one admitted pressure plan"))?;
    let target = plan.target;
    require(
        target.id == identifier,
        "pressure source targets its actual plan",
    )?;
    *target.owner.lock().expect("pressure owner") = Some(current_identity());
    let actual_ledger = state.ledger(
        context
            .connection_context()
            .ok_or(FixtureError::Assertion("native pressure context"))?,
    )?;
    if target.ingress_bound.is_some() || target.envelope_bound.is_some() {
        let expected_ledger = target.ledger()?;
        expected_ledger.update(|queue| {
            queue.invalid |= !Arc::ptr_eq(&expected_ledger, &actual_ledger);
        });
        require(
            !expected_ledger.snapshot().invalid,
            "native upload uses its pre-bound accepted BODY-credit recorder and worker",
        )?;
    } else {
        *target.ledger.lock().expect("pressure ledger") = Some(actual_ledger);
    }
    let _handler = PressureGuard::new(&target, 0);
    let mut request = HttpSourceHolder::new(Box::new(PressureSource {
        inner: request,
        guard: PressureGuard::new(&target, 2),
    }));
    let receipt = context
        .exchange_completion()
        .cloned()
        .ok_or(FixtureError::Assertion(
            "actual pressure exchange completion",
        ))?;
    require(
        stored.borrow().is_none(),
        "single worker-owned pressure receipt slot",
    )?;
    *stored.borrow_mut() = Some(PressureReceipt {
        receipt: receipt.clone(),
        target: Arc::clone(&target),
    });
    pressure_read(&mut request, &target, CHUNK).await?;
    let mut observation = receipt.wait();
    require(
        poll_fn(|context| Poll::Ready(observation.as_mut().poll(context).is_pending())).await,
        "native pressure receipt is pending before admission",
    )?;
    drop(observation);
    target.increment(&target.pending);
    let mut action = Box::pin(plan.action);
    let mut held = None;
    let action_result = within(
        "native pressure action wait",
        poll_fn(|context| {
            let result = action.as_mut().poll(context);
            if result.is_pending() && held.is_none() {
                held = Some(PressureGuard::new(&target, 1));
                target.entered.store(true, Ordering::Release);
                target.changed.wake();
            }
            result
        }),
    )
    .await??;
    drop(action);
    drop(held);
    require(
        target.action.load(Ordering::Acquire) == action_result as usize,
        "worker executes the pressure-admitted action",
    )?;
    match action_result {
        PressureAction::Release => {
            pressure_read(&mut request, &target, PRESSURE_BYTES).await?;
            healthy_response()
        }
        PressureAction::NativeCancel => {
            require(
                shared.route.protocol == Protocol::H1,
                "native cancellation is the H1 oracle",
            )?;
            request.stop_reading(Some(HttpErrorCode::CANCEL));
            require(
                !request.readable(),
                "native H1 source ownership actually retired",
            )?;
            target.native_cancelled.store(true, Ordering::Release);
            target.changed.wake();
            Ok(HandlerOutcome::Cancelled(HttpError::with_message(
                HttpErrorCode::CANCEL,
                b"pressure-admitted worker-owned request cancellation",
            )))
        }
        PressureAction::PeerCancel => Err(FixtureError::Assertion(
            "H2 peer cancellation never releases the native gate",
        )
        .into()),
    }
}

fn healthy_response() -> TestResult<HandlerOutcome> {
    let body = BufQueue::from_bytes(Bytes::from_static(HEALTH), HEALTH.len())?;
    let response = HttpFixedSource::make_fixed_response(200, Some(body))?;
    Ok(HandlerOutcome::Response(HttpSourceHolder::new(Box::new(
        response,
    ))))
}

struct NativeRequestState<'a> {
    shared: &'a Arc<Shared>,
    stored: &'a Rc<RefCell<Option<ReceiptRecord>>>,
    pressure_receipt: Option<&'a Rc<RefCell<Option<PressureReceipt>>>>,
    backing_alias: Option<&'a Rc<RefCell<Option<RetainedBackingAlias>>>>,
    egress_receipt: Option<&'a Rc<RefCell<Option<EgressReceipt>>>>,
}

async fn native_request(
    state: NativeRequestState<'_>,
    context: HandlerSessionContext,
    mut request: HttpSourceHolder,
) -> TestResult<HandlerOutcome> {
    let NativeRequestState {
        shared,
        stored,
        pressure_receipt,
        backing_alias,
        egress_receipt,
    } = state;
    let stream = request.get_stream_id();
    let mut head = within("native request head", request.read_header_event()).await??;
    let path = std::str::from_utf8(head.headers_mut().path()?)?.to_owned();
    require(path.len() <= 64, "bounded fixture request path")?;
    require(
        head.headers_mut().method_string()? == "POST",
        "native fixture method",
    )?;
    if path.starts_with("/context/") {
        let variant = if path.ends_with('a') { "a" } else { "b" };
        let headers = head.headers().headers();
        require(
            headers.get_single_or_empty(b"host") == format!("spoof-{variant}.invalid").as_bytes(),
            "forged authority reached the native request",
        )?;
        require(
            headers.get_single_or_empty(b"forwarded") == b"for=192.0.2.1;proto=https",
            "forged Forwarded header reached the handler",
        )?;
        require(
            headers.get_single_or_empty(b"x-forwarded-for") == b"192.0.2.2",
            "forged endpoint header reached the handler",
        )?;
        require(
            headers.get_single_or_empty(b"x-forwarded-proto") == b"https",
            "forged security header reached the handler",
        )?;
        require(
            headers.get_single_or_empty(b"x-client-cert") == b"not-a-transport-identity",
            "forged identity header reached the handler",
        )?;
    }
    shared.observe_request(path.clone(), &context, stream)?;
    let (kind, identifier) = path
        .strip_prefix('/')
        .and_then(|path| path.split_once('/'))
        .ok_or(FixtureError::Assertion("fixture request route"))?;
    if kind == "context" {
        require(head.eom(), "context request has no upload")?;
        return healthy_response();
    }
    let identifier = identifier.parse::<usize>()?;
    if kind.starts_with("egress") {
        require(head.eom(), "egress request has no upload")?;
        return native_egress_request(
            shared,
            egress_receipt.ok_or(FixtureError::Assertion("worker-local egress receipt"))?,
            context,
            (kind, identifier),
        )
        .await;
    }
    if kind == "backing-release" {
        require(head.eom(), "backing release request has no upload")?;
        let slot =
            backing_alias.ok_or(FixtureError::Assertion("worker-local backing alias slot"))?;
        let retained = {
            let mut slot = slot.borrow_mut();
            let retained = slot
                .as_ref()
                .ok_or(FixtureError::Assertion("actual retained backing alias"))?;
            require(
                retained.record.target == identifier && retained.bytes.len() == 1,
                "worker releases the exact target's one-byte alias",
            )?;
            slot.take().expect("validated worker-local alias")
        };
        let RetainedBackingAlias { record, bytes } = retained;
        drop(bytes);
        record.alias_bytes.store(0, Ordering::Release);
        return healthy_response();
    }
    if kind.starts_with("pressure") {
        return native_pressure_request(
            shared,
            pressure_receipt.ok_or(FixtureError::Assertion("pressure receipt owner"))?,
            kind,
            identifier,
            head.eom(),
            context,
            request,
        )
        .await;
    }
    if kind == "pending" || kind == "retired" {
        require(head.eom(), "receipt probe has no upload")?;
        let record = stored
            .borrow()
            .clone()
            .ok_or(FixtureError::Assertion("real stored native receipt"))?;
        require(
            record.evidence.id == identifier,
            "probe targets the intended exchange",
        )?;
        if kind == "pending" {
            let mut observation = record.receipt.wait();
            let pending =
                poll_fn(|task| Poll::Ready(observation.as_mut().poll(task).is_pending())).await;
            require(pending, "held native exchange receipt is pending")?;
            drop(observation);
            record
                .evidence
                .pending_checks
                .fetch_add(1, Ordering::AcqRel);
        } else {
            within("native target cleanup receipt", record.receipt.wait()).await??;
            record
                .evidence
                .completed_checks
                .fetch_add(1, Ordering::AcqRel);
            within("sticky recreated native receipt", record.receipt.wait()).await??;
            record
                .evidence
                .completed_checks
                .fetch_add(1, Ordering::AcqRel);
            stored.borrow_mut().take();
        }
        return healthy_response();
    }
    require(
        kind == "target" && !head.eom(),
        "admitted target has a streaming upload",
    )?;
    let plan = shared
        .plan
        .lock()
        .expect("native target plan")
        .take()
        .ok_or(FixtureError::Assertion("one native target plan"))?;
    let evidence = plan.evidence;
    require(evidence.id == identifier, "target plan association")?;
    *evidence.owner.lock().expect("target owner") = Some(current_identity());
    let _handler = DropGuard::new(Arc::clone(&evidence), GuardKind::Handler);
    let receipt = context
        .exchange_completion()
        .cloned()
        .ok_or(FixtureError::Assertion(
            "admitted target has an actual native receipt",
        ))?;
    require(
        stored.borrow().is_none(),
        "bounded single-slot receipt storage",
    )?;
    *stored.borrow_mut() = Some(ReceiptRecord {
        receipt,
        evidence: Arc::clone(&evidence),
    });
    let mut received = Vec::with_capacity(BODY_SIZE);
    loop {
        let held_upload = evidence.stage == Stage::Upload && received.len() == CHUNK;
        let event = if held_upload {
            observe_pending(request.read_body_event(CHUNK as u32), &evidence).await?
        } else {
            within("native upload", request.read_body_event(CHUNK as u32)).await?
        };
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                *evidence.ingress_error.lock().expect("native ingress error") = Some(error.clone());
                require(
                    held_upload && evidence.cancelling.load(Ordering::Acquire),
                    "native upload error occurs only after staged peer cancellation",
                )?;
                let expected = if shared.route.protocol == Protocol::H2 {
                    HttpErrorCode::CANCEL
                } else {
                    HttpErrorCode::TRANSPORT_READ_ERROR
                };
                if error.code() != expected {
                    return Err(error.into());
                }
                return Ok(HandlerOutcome::Cancelled(error));
            }
        };
        let eom = event.eom();
        match event.into_kind() {
            HttpBodyEventKind::Body(body) => {
                require(!held_upload, "held upload cannot acquire more bytes or EOM")?;
                require(
                    received.len() + body.chain_length() <= BODY_SIZE,
                    "bounded native upload",
                )?;
                for segment in body.segments() {
                    received.extend_from_slice(segment);
                }
                evidence
                    .upload_bytes
                    .store(received.len(), Ordering::Release);
                require(
                    received.as_slice() == &UPLOAD[..received.len()],
                    "exact native upload bytes",
                )?;
                if eom {
                    require(
                        received.as_slice() == UPLOAD.as_slice(),
                        "complete native upload",
                    )?;
                    break;
                }
            }
            HttpBodyEventKind::Suspend(resume) => {
                let status = if held_upload {
                    observe_pending(resume, &evidence).await?
                } else {
                    within("native upload resume", resume).await?
                };
                if status != ResumeStatus::Signalled {
                    return Err(FixtureError::Resume(status).into());
                }
            }
            _ => return Err(FixtureError::Assertion("unexpected native upload event").into()),
        }
    }
    require(
        evidence.stage != Stage::Upload,
        "partial upload never completed",
    )?;
    if evidence.stage == Stage::Handler {
        observe_pending(plan.release, &evidence).await??;
        return Err(FixtureError::Assertion("held handler gate must not be released").into());
    }
    let gate = if evidence.stage == Stage::Response {
        Some(plan.release)
    } else {
        None
    };
    let payload = if let Some(backing) = plan.backing {
        let record = Arc::clone(&backing.record);
        let bytes = backing.bytes()?;
        if record.keep_alias {
            let slot = backing_alias.ok_or(FixtureError::Assertion("owned backing alias owner"))?;
            let mut slot = slot.borrow_mut();
            require(slot.is_none(), "bounded single retained backing alias")?;
            let alias = bytes.slice(..1);
            record.alias_bytes.store(alias.len(), Ordering::Release);
            *slot = Some(RetainedBackingAlias {
                record,
                bytes: alias,
            });
        }
        Some(bytes)
    } else {
        None
    };
    let response = NativeResponse {
        head: true,
        offset: 0,
        gate,
        shared: Arc::clone(shared),
        payload,
        guard: DropGuard::new(evidence, GuardKind::Response),
    };
    Ok(HandlerOutcome::Response(HttpSourceHolder::new(Box::new(
        response,
    ))))
}

async fn require_pending_egress_receipt(mut wait: LocalExchangeCompletionFuture) -> TestResult<()> {
    match poll_fn(|task| Poll::Ready(wait.as_mut().poll(task))).await {
        Poll::Pending => Ok(()),
        Poll::Ready(Ok(())) => {
            Err(FixtureError::Assertion("pressured response receipt remains pending").into())
        }
        Poll::Ready(Err(error)) => Err(error.into()),
    }
}

async fn native_egress_request(
    shared: &Arc<Shared>,
    stored: &Rc<RefCell<Option<EgressReceipt>>>,
    context: HandlerSessionContext,
    route: (&str, usize),
) -> TestResult<HandlerOutcome> {
    let (kind, identifier) = route;
    if kind == "egress-pending" || kind == "egress-retired" {
        let record = stored
            .borrow()
            .clone()
            .ok_or(FixtureError::Assertion("actual egress receipt"))?;
        require(record.evidence.id == identifier, "egress receipt identity")?;
        if kind == "egress-pending" {
            require_pending_egress_receipt(record.receipt.wait()).await?;
            record.evidence.add(&record.evidence.pending_receipts, 1);
        } else {
            within("actual egress retirement", record.receipt.wait()).await??;
            record.evidence.add(&record.evidence.completed_receipts, 1);
            within("sticky egress retirement", record.receipt.wait()).await??;
            record.evidence.add(&record.evidence.completed_receipts, 1);
            stored.borrow_mut().take();
        }
        return healthy_response();
    }
    require(kind == "egress", "fixed egress route")?;
    let state = shared
        .egress
        .as_ref()
        .ok_or(FixtureError::Assertion("egress exercise"))?;
    let plan = state
        .plan
        .lock()
        .expect("egress plan")
        .take()
        .ok_or(FixtureError::Assertion("one egress plan"))?;
    require(
        plan.evidence.id == identifier && stored.borrow().is_none(),
        "egress target and receipt slot",
    )?;
    if shared.route.protocol == Protocol::H1 {
        let connection = context
            .connection_context()
            .ok_or(FixtureError::Assertion("egress accepted context"))?;
        *plan.evidence.socket_pair.lock().expect("egress pair") = Some(state.pair(connection)?);
    }
    *stored.borrow_mut() = Some(EgressReceipt {
        receipt: context
            .exchange_completion()
            .cloned()
            .ok_or(FixtureError::Assertion("actual egress exchange completion"))?,
        evidence: Arc::clone(&plan.evidence),
    });
    plan.evidence.add(&plan.evidence.source_created, 1);
    Ok(HandlerOutcome::Response(HttpSourceHolder::new(Box::new(
        EgressSource {
            state: Rc::new(RefCell::new(EgressSourceState {
                receiver: Some(plan.receiver),
                buffered: Bytes::new(),
                offset: 0,
                stop_error: None,
            })),
            shared: Arc::clone(shared),
            evidence: plan.evidence,
            head: true,
        },
    ))))
}

struct NativeResponse {
    head: bool,
    offset: usize,
    gate: Option<oneshot::Receiver<()>>,
    shared: Arc<Shared>,
    payload: Option<Bytes>,
    guard: DropGuard,
}

impl HttpSource for NativeResponse {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let result = (|| -> TestResult<HttpHeaderEvent> {
            require(
                std::mem::replace(&mut self.head, false),
                "one response header read",
            )?;
            let mut headers = HttpMessage::new();
            headers.set_status_code(200)?;
            headers.set_status_message(HttpMessage::default_reason(200))?;
            headers.set_http_version(1, 1);
            headers
                .headers_mut()
                .add(b"content-length", BODY_SIZE.to_string());
            Ok(HttpHeaderEvent::new(headers, false)?)
        })()
        .map_err(|error| self.shared.fail(error));
        Box::pin(ready(result))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if self.offset == CHUNK
            && let Some(gate) = self.gate.take()
        {
            let evidence = Arc::clone(&self.guard.evidence);
            let shared = Arc::clone(&self.shared);
            return Box::pin(async move {
                let result: TestResult<HttpBodyEvent> = async {
                    observe_pending(gate, &evidence).await??;
                    Err(FixtureError::Assertion("held response gate must not be released").into())
                }
                .await;
                result.map_err(|error| shared.fail(error))
            });
        }
        let result = (|| -> TestResult<HttpBodyEvent> {
            require(
                maximum != 0 && self.offset < BODY_SIZE,
                "valid bounded native response read",
            )?;
            let boundary = if self.offset < CHUNK {
                CHUNK
            } else {
                BODY_SIZE
            };
            let amount = (maximum as usize).min(CHUNK).min(boundary - self.offset);
            let body = match &self.payload {
                Some(payload) => payload.slice(self.offset..self.offset + amount),
                None => Bytes::from_static(&DOWNLOAD).slice(self.offset..self.offset + amount),
            };
            let body = BufQueue::from_bytes(body, amount)?;
            self.offset += amount;
            self.guard
                .evidence
                .response_bytes
                .fetch_add(amount, Ordering::AcqRel);
            Ok(HttpBodyEvent::body(body, self.offset == BODY_SIZE))
        })()
        .map_err(|error| self.shared.fail(error));
        Box::pin(ready(result))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.guard.evidence.record_retirement(GuardKind::Response);
        self.guard.evidence.check_owner();
        let mut stops = self
            .guard
            .evidence
            .stops
            .lock()
            .expect("native source stops");
        assert!(stops.len() < 2, "duplicate native source stop: {error:?}");
        stops.push(error);
        self.gate.take();
        self.offset = BODY_SIZE;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerAddresses {
    local: SocketAddr,
    remote: SocketAddr,
}

fn socket_address(address: SocketAddr) -> SocketAddress {
    SocketAddress::new(address.ip().to_string(), address.port())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketProfileDirection {
    Sender,
    Receiver,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketBufferReceipt {
    direction: SocketProfileDirection,
    addresses: PeerAddresses,
    requested: usize,
    effective: usize,
}

impl SocketBufferReceipt {
    fn validate(self) -> TestResult<()> {
        if self.requested == PRESSURE_SOCKET_REQUEST
            && (PRESSURE_SOCKET_REQUEST..=PRESSURE_WINDOW).contains(&self.effective)
        {
            Ok(())
        } else {
            Err(SocketProfileValidationError { receipt: self }.into())
        }
    }

    fn matches_connection(self, connection: &ConnectionInfo) -> bool {
        self.direction == SocketProfileDirection::Receiver
            && connection.protocol == CodecProtocol::Http11
            && socket_address(self.addresses.local) == connection.local_address
            && socket_address(self.addresses.remote) == connection.peer_address
    }
}

#[derive(Debug, Error)]
#[error("pressure socket profile outside its fixed envelope: {receipt:?}")]
struct SocketProfileValidationError {
    receipt: SocketBufferReceipt,
}

#[derive(Debug, Error)]
#[error("pressure socket {direction:?} {operation}: {source}")]
struct SocketProfileIoError {
    direction: SocketProfileDirection,
    operation: &'static str,
    #[source]
    source: io::Error,
}

fn socket_profile_with(
    direction: SocketProfileDirection,
    addresses: PeerAddresses,
    set: impl FnOnce(usize) -> io::Result<()>,
    get: impl FnOnce() -> io::Result<usize>,
) -> TestResult<SocketBufferReceipt> {
    set(PRESSURE_SOCKET_REQUEST).map_err(|source| SocketProfileIoError {
        direction,
        operation: "set buffer size",
        source,
    })?;
    let effective = get().map_err(|source| SocketProfileIoError {
        direction,
        operation: "get buffer size",
        source,
    })?;
    let receipt = SocketBufferReceipt {
        direction,
        addresses,
        requested: PRESSURE_SOCKET_REQUEST,
        effective,
    };
    receipt.validate()?;
    Ok(receipt)
}

fn socket_profile(
    socket: &std::net::TcpStream,
    direction: SocketProfileDirection,
) -> TestResult<SocketBufferReceipt> {
    let addresses = PeerAddresses {
        local: socket.local_addr().map_err(|source| SocketProfileIoError {
            direction,
            operation: "local address",
            source,
        })?,
        remote: socket.peer_addr().map_err(|source| SocketProfileIoError {
            direction,
            operation: "peer address",
            source,
        })?,
    };
    let socket = SockRef::from(socket);
    socket_profile_with(
        direction,
        addresses,
        |requested| match direction {
            SocketProfileDirection::Sender => socket.set_send_buffer_size(requested),
            SocketProfileDirection::Receiver => socket.set_recv_buffer_size(requested),
        },
        || match direction {
            SocketProfileDirection::Sender => socket.send_buffer_size(),
            SocketProfileDirection::Receiver => socket.recv_buffer_size(),
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketProfilePair {
    sender: SocketBufferReceipt,
    receiver: SocketBufferReceipt,
}

fn paired_socket_profile(
    protocol: Protocol,
    addresses: PeerAddresses,
    sender: Option<SocketBufferReceipt>,
    receiver: Option<SocketBufferReceipt>,
) -> TestResult<Option<SocketProfilePair>> {
    if protocol == Protocol::H2 {
        require(
            sender.is_none() && receiver.is_none(),
            "H2 retains unprofiled socket defaults",
        )?;
        return Ok(None);
    }
    let sender = sender.ok_or(FixtureError::Assertion("actual sender socket profile"))?;
    let receiver = receiver.ok_or(FixtureError::Assertion("actual receiver socket profile"))?;
    sender.validate()?;
    receiver.validate()?;
    require(
        sender.direction == SocketProfileDirection::Sender
            && receiver.direction == SocketProfileDirection::Receiver
            && sender.addresses == addresses
            && receiver.addresses.local == addresses.remote
            && receiver.addresses.remote == addresses.local,
        "paired socket profiles bind the actual connection endpoints",
    )?;
    Ok(Some(SocketProfilePair { sender, receiver }))
}

#[derive(Default)]
struct SocketProfileRecords {
    receipts: [Option<SocketBufferReceipt>; MAX_CONNECTIONS],
    pairs: [Option<SocketProfilePair>; 2],
    failed: bool,
}

impl SocketProfileRecords {
    fn ready_pair(&self, addresses: PeerAddresses) -> TestResult<Option<SocketProfilePair>> {
        require(!self.failed, "socket profile setup failed")?;
        let sender = self
            .receipts
            .iter()
            .flatten()
            .find(|receipt| {
                receipt.direction == SocketProfileDirection::Sender
                    && receipt.addresses == addresses
            })
            .copied();
        require(sender.is_some(), "sender profile precedes the body writer")?;
        let receiver = self
            .receipts
            .iter()
            .flatten()
            .find(|receipt| {
                receipt.direction == SocketProfileDirection::Receiver
                    && receipt.addresses.local == addresses.remote
                    && receipt.addresses.remote == addresses.local
            })
            .copied();
        if receiver.is_none() {
            return Ok(None);
        }
        paired_socket_profile(Protocol::H1, addresses, sender, receiver)
    }

    fn record(&mut self, receipt: SocketBufferReceipt) -> TestResult<()> {
        receipt.validate()?;
        require(
            !self.failed
                && !self.receipts.iter().flatten().any(|previous| {
                    previous.direction == receipt.direction
                        && previous.addresses == receipt.addresses
                }),
            "socket profile publication is valid and unique",
        )?;
        let slot = self
            .receipts
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(FixtureError::Assertion("bounded socket profile receipts"))?;
        *slot = Some(receipt);
        Ok(())
    }

    fn record_pair(&mut self, target: usize, pair: SocketProfilePair) -> TestResult<()> {
        let slot = target
            .checked_sub(1)
            .and_then(|index| self.pairs.get_mut(index))
            .ok_or(FixtureError::Assertion(
                "bounded paired socket profile target",
            ))?;
        require(slot.is_none(), "paired socket profile published once")?;
        *slot = Some(pair);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PressureWireCounts {
    written: usize,
    blocked: bool,
    invalid: bool,
    syscall_would_block: usize,
    readiness_pending: usize,
}

impl PressureWireCounts {
    fn record_pressure(&mut self, would_block: Option<usize>, pending: bool) {
        match would_block.and_then(|count| self.syscall_would_block.checked_add(count)) {
            Some(next) => self.syscall_would_block = next,
            None => self.invalid = true,
        }
        if pending {
            match self.readiness_pending.checked_add(1) {
                Some(next) => self.readiness_pending = next,
                None => self.invalid = true,
            }
        }
    }
}

struct PressureSocket {
    socket: TcpStream,
    writer: std::net::TcpStream,
    counts: Arc<Mutex<PressureWireCounts>>,
    saw_would_block: bool,
}

impl PressureSocket {
    fn poll_write_with(
        &mut self,
        context: &mut Context<'_>,
        mut write: impl FnMut(&std::net::TcpStream) -> io::Result<usize>,
    ) -> Poll<io::Result<usize>> {
        let mut would_block = Some(0usize);
        loop {
            let mut actual_would_block = false;
            let result = self.socket.try_io(Interest::WRITABLE, || {
                let result = write(&self.writer);
                actual_would_block = result
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock);
                result
            });
            if actual_would_block {
                would_block = would_block.and_then(|count| count.checked_add(1));
            }
            self.saw_would_block |= actual_would_block;
            match result {
                Ok(amount) => {
                    self.saw_would_block = false;
                    let mut counts = self.counts.lock().expect("pressure wire counts");
                    counts.blocked = false;
                    counts.record_pressure(would_block, false);
                    match counts.written.checked_add(amount) {
                        Some(next) => counts.written = next,
                        None => counts.invalid = true,
                    }
                    return Poll::Ready(Ok(amount));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    match self.socket.poll_write_ready(context) {
                        Poll::Pending => {
                            let mut counts = self.counts.lock().expect("pressure wire counts");
                            counts.blocked = self.saw_would_block;
                            counts.record_pressure(would_block, true);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(())) => {}
                    }
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}

impl AsyncRead for PressureSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(context, output)
    }
}

impl AsyncWrite for PressureSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(context, |mut writer| Write::write(&mut writer, data))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_with(context, |mut writer| {
            Write::write_vectored(&mut writer, data)
        })
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(context)
    }
}

enum PressureIo {
    Tcp(PressureSocket),
    Tls(Box<TlsStream<PressureSocket>>),
}

impl PressureIo {
    fn reset(self) -> TestResult<()> {
        let socket = match &self {
            Self::Tcp(socket) => &socket.socket,
            Self::Tls(tls) => &tls.get_ref().0.socket,
        };
        SockRef::from(socket).set_linger(Some(Duration::ZERO))?;
        drop(self);
        Ok(())
    }
}

impl AsyncRead for PressureIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_read(context, output),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_read(context, output),
        }
    }
}

impl AsyncWrite for PressureIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_write(context, data),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_write(context, data),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_write_vectored(context, data),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_write_vectored(context, data),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(socket) => socket.is_write_vectored(),
            Self::Tls(tls) => tls.is_write_vectored(),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_flush(context),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_shutdown(context),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_shutdown(context),
        }
    }
}

impl Endpoint {
    async fn pressure_connect(
        &self,
        counts: Arc<Mutex<PressureWireCounts>>,
    ) -> TestResult<(PressureIo, PeerAddresses)> {
        let socket = within("pressure TCP connect", TcpStream::connect(self.address)).await??;
        socket.set_nodelay(true)?;
        let addresses = PeerAddresses {
            local: socket.local_addr()?,
            remote: socket.peer_addr()?,
        };
        let socket = socket.into_std()?;
        if self.route.protocol == Protocol::H1
            && let Some(pressure) = &self.shared.pressure
        {
            let receipt = socket_profile(&socket, SocketProfileDirection::Sender)?;
            pressure.record_socket_profile(receipt)?;
        }
        let writer = socket.try_clone()?;
        let socket = PressureSocket {
            socket: TcpStream::from_std(socket)?,
            writer,
            counts,
            saw_would_block: false,
        };
        let io = if let Some(config) = &self.tls {
            let tls = within(
                "pressure verified TLS handshake",
                TlsConnector::from(Arc::clone(config))
                    .connect(ServerName::try_from("localhost")?, socket),
            )
            .await??;
            require(
                tls.get_ref().1.alpn_protocol() == Some(self.route.alpn()),
                "actual pressure TLS ALPN",
            )?;
            PressureIo::Tls(Box::new(tls))
        } else {
            PressureIo::Tcp(socket)
        };
        Ok((io, addresses))
    }
}

enum PressureWire {
    H1(PressureIo),
    H2(H2Target),
}

struct PressureUpload {
    wire: Option<PressureWire>,
    counts: Arc<Mutex<PressureWireCounts>>,
    wire_baseline: usize,
    offset: usize,
    buffer: Bytes,
    blocked: bool,
    done: bool,
    checkpoints: PressureCheckpoints,
    socket_profile: Option<SocketProfilePair>,
}

impl PressureUpload {
    fn new(
        wire: PressureWire,
        counts: Arc<Mutex<PressureWireCounts>>,
        socket_profile: Option<SocketProfilePair>,
    ) -> Self {
        let wire_baseline = counts.lock().expect("pressure wire counts").written;
        Self {
            wire: Some(wire),
            counts,
            wire_baseline,
            offset: 0,
            buffer: Bytes::new(),
            blocked: false,
            done: false,
            checkpoints: PressureCheckpoints::default(),
            socket_profile,
        }
    }
}

impl Future for PressureUpload {
    type Output = TestResult<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let upload = self.get_mut();
        upload.blocked = false;
        for _ in 0..16 {
            let wire = upload
                .wire
                .as_mut()
                .expect("inline writer retains its real wire");
            if upload.offset == PRESSURE_BYTES {
                if let PressureWire::H1(io) = wire {
                    match Pin::new(io).poll_flush(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                        Poll::Ready(Ok(())) => {}
                    }
                }
                upload.done = true;
                return Poll::Ready(Ok(()));
            }
            if upload.buffer.is_empty() {
                upload.buffer = Bytes::from(
                    (upload.offset..upload.offset + CHUNK.min(PRESSURE_BYTES - upload.offset))
                        .map(|index| (index % 251) as u8)
                        .collect::<Vec<_>>(),
                );
            }
            let amount = match wire {
                PressureWire::H1(io) => {
                    upload.counts.lock().expect("pressure wire counts").blocked = false;
                    match Pin::new(io).poll_write(context, &upload.buffer) {
                        Poll::Pending => {
                            upload.blocked =
                                upload.counts.lock().expect("pressure wire counts").blocked;
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                        Poll::Ready(Ok(amount)) => amount,
                    }
                }
                PressureWire::H2(exchange) => {
                    exchange.stream.reserve_capacity(upload.buffer.len());
                    let capacity = match exchange.stream.poll_capacity(context) {
                        Poll::Pending => {
                            upload.blocked = true;
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(Ok(capacity))) => capacity,
                        Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error.into())),
                        Poll::Ready(None) => {
                            return Poll::Ready(Err(FixtureError::Assertion(
                                "pressure H2 capacity ended early",
                            )
                            .into()));
                        }
                    };
                    let amount = capacity.min(upload.buffer.len());
                    if amount != 0
                        && let Err(error) = exchange.stream.send_data(
                            upload.buffer.slice(..amount),
                            upload.offset + amount == PRESSURE_BYTES,
                        )
                    {
                        return Poll::Ready(Err(error.into()));
                    }
                    amount
                }
            };
            if amount == 0 || amount > upload.buffer.len() {
                return Poll::Ready(Err(FixtureError::Assertion(
                    "positive bounded pressure writer progress",
                )
                .into()));
            }
            upload.offset += amount;
            upload.buffer = upload.buffer.slice(amount..);
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

enum PeerIo {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl PeerIo {
    fn reset(self) -> TestResult<()> {
        let socket = match &self {
            Self::Tcp(socket) => socket,
            Self::Tls(tls) => tls.get_ref().0,
        };
        SockRef::from(socket).set_linger(Some(Duration::ZERO))?;
        drop(self);
        Ok(())
    }
}

impl AsyncRead for PeerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_read(context, output),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_read(context, output),
        }
    }
}

impl AsyncWrite for PeerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_write(context, data),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_write(context, data),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_flush(context),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Tcp(socket) => Pin::new(socket).poll_shutdown(context),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_shutdown(context),
        }
    }
}

struct Endpoint {
    address: SocketAddr,
    route: Route,
    tls: Option<Arc<ClientConfig>>,
    shared: Arc<Shared>,
}

impl Endpoint {
    async fn connect(&self) -> TestResult<(PeerIo, PeerAddresses)> {
        let socket = within("independent TCP connect", TcpStream::connect(self.address)).await??;
        socket.set_nodelay(true)?;
        let addresses = PeerAddresses {
            local: socket.local_addr()?,
            remote: socket.peer_addr()?,
        };
        if self.route.protocol == Protocol::H1
            && let Some(egress) = &self.shared.egress
        {
            let profile = socket_profile_with(
                SocketProfileDirection::Receiver,
                addresses,
                |requested| SockRef::from(&socket).set_recv_buffer_size(requested),
                || SockRef::from(&socket).recv_buffer_size(),
            )?;
            egress.profile(profile)?;
        }
        let io = if let Some(config) = &self.tls {
            let tls = within(
                "independent verified TLS handshake",
                TlsConnector::from(Arc::clone(config))
                    .connect(ServerName::try_from("localhost")?, socket),
            )
            .await??;
            require(
                tls.get_ref().1.alpn_protocol() == Some(self.route.alpn()),
                "actual independent TLS ALPN",
            )?;
            PeerIo::Tls(Box::new(tls))
        } else {
            PeerIo::Tcp(socket)
        };
        Ok((io, addresses))
    }

    async fn peer(&self, drivers: &mut JoinSet<TestResult<()>>) -> TestResult<Peer> {
        let (io, addresses) = self.connect().await?;
        let wire = match self.route.protocol {
            Protocol::H1 => PeerWire::H1(io),
            Protocol::H2 => {
                require(
                    drivers.len() < MAX_DRIVERS,
                    "bounded independent driver ownership",
                )?;
                let (sender, connection) = if self.shared.egress.is_some() {
                    let mut builder = h2::client::Builder::new();
                    builder.initial_window_size(EGRESS_STREAM_WINDOW);
                    builder.initial_connection_window_size(EGRESS_CONNECTION_WINDOW);
                    within("egress h2 handshake", builder.handshake(io)).await??
                } else {
                    within("independent h2 handshake", h2::client::handshake(io)).await??
                };
                drivers.spawn(async move {
                    connection.await?;
                    Ok(())
                });
                PeerWire::H2(sender)
            }
        };
        Ok(Peer { wire, addresses })
    }
}

enum PeerWire {
    H1(PeerIo),
    H2(SendRequest<Bytes>),
}

struct Peer {
    wire: PeerWire,
    addresses: PeerAddresses,
}

fn authority(path: &str) -> &'static str {
    if path.starts_with("/context/") {
        if path.ends_with('a') {
            "spoof-a.invalid"
        } else {
            "spoof-b.invalid"
        }
    } else {
        "localhost"
    }
}

async fn h1_request<Io: AsyncWrite + Unpin>(
    io: &mut Io,
    path: &str,
    length: usize,
    data: &[u8],
) -> TestResult<()> {
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: {length}\r\nForwarded: for=192.0.2.1;proto=https\r\nX-Forwarded-For: 192.0.2.2\r\nX-Forwarded-Proto: https\r\nX-Client-Cert: not-a-transport-identity\r\n\r\n",
        authority(path),
    );
    within("H1 request head", io.write_all(head.as_bytes())).await??;
    for chunk in data.chunks(CHUNK) {
        within("H1 request body", io.write_all(chunk)).await??;
    }
    within("H1 request flush", io.flush()).await??;
    Ok(())
}

async fn h1_head<Io: AsyncRead + Unpin>(io: &mut Io, expected: usize) -> TestResult<()> {
    let mut bytes = Vec::with_capacity(CHUNK);
    while !bytes.ends_with(b"\r\n\r\n") {
        require(bytes.len() < CHUNK, "bounded independent H1 response head")?;
        let mut byte = [0];
        io.read_exact(&mut byte).await?;
        bytes.push(byte[0]);
    }
    let mut lines = std::str::from_utf8(&bytes)?.split("\r\n");
    require(
        lines
            .next()
            .is_some_and(|line| line.starts_with("HTTP/1.1 200 ")),
        "actual H1 response status/version",
    )?;
    let mut content_length = None;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or(FixtureError::Assertion("H1 response header field"))?;
        if name.eq_ignore_ascii_case("content-length") {
            require(content_length.is_none(), "exactly one H1 content length")?;
            content_length = Some(value.trim().parse::<usize>()?);
        }
        require(
            !name.eq_ignore_ascii_case("transfer-encoding"),
            "fixed lifecycle response has no transfer coding",
        )?;
    }
    require(
        content_length == Some(expected),
        "exact independent H1 response length",
    )
}

async fn h2_request(
    sender: &mut SendRequest<Bytes>,
    route: Route,
    path: &str,
    length: usize,
    data: &[u8],
) -> TestResult<H2Target> {
    within(
        "h2 request readiness",
        poll_fn(|context| sender.poll_ready(context)),
    )
    .await??;
    let request = Request::builder()
        .method("POST")
        .version(Version::HTTP_2)
        .uri(format!(
            "{}://{}{path}",
            if route.tls { "https" } else { "http" },
            authority(path)
        ))
        .header("content-length", length)
        .header("forwarded", "for=192.0.2.1;proto=https")
        .header("x-forwarded-for", "192.0.2.2")
        .header("x-forwarded-proto", "https")
        .header("x-client-cert", "not-a-transport-identity")
        .body(())?;
    let (response, mut stream) = sender.send_request(request, length == 0)?;
    let mut offset = 0;
    while offset < data.len() {
        let remaining = (data.len() - offset).min(CHUNK);
        stream.reserve_capacity(remaining);
        let capacity = within(
            "h2 upload capacity",
            poll_fn(|context| stream.poll_capacity(context)),
        )
        .await?
        .ok_or(FixtureError::Assertion(
            "h2 upload capacity remains available",
        ))??;
        let amount = capacity.min(remaining);
        require(amount != 0, "positive h2 upload capacity")?;
        let end = offset + amount;
        stream.send_data(Bytes::copy_from_slice(&data[offset..end]), end == length)?;
        offset = end;
    }
    Ok(H2Target {
        stream,
        response: Some(response),
        body: None,
    })
}

fn h2_head(response: &Response<RecvStream>, expected: usize) -> TestResult<()> {
    require(
        response.status().as_u16() == 200 && response.version() == Version::HTTP_2,
        "actual independent H2 response status/version",
    )?;
    require(
        response.headers().get_all("content-length").iter().count() == 1,
        "exactly one H2 content length",
    )?;
    let length = response
        .headers()
        .get("content-length")
        .ok_or(FixtureError::Assertion("H2 content length"))?
        .to_str()?
        .parse::<usize>()?;
    require(
        length == expected && !response.headers().contains_key("transfer-encoding"),
        "exact H2 response length and framing",
    )
}

async fn h2_body(body: &mut RecvStream, expected: &[u8], terminal: bool) -> TestResult<()> {
    let mut bytes = Vec::with_capacity(expected.len());
    while let Some(data) = body.data().await {
        let data = data?;
        require(
            !data.is_empty() && bytes.len() + data.len() <= expected.len(),
            "bounded positive H2 DATA progress",
        )?;
        bytes.extend_from_slice(&data);
        body.flow_control().release_capacity(data.len())?;
        if !terminal && bytes.len() == expected.len() {
            break;
        }
    }
    require(
        bytes.as_slice() == expected,
        "exact independent H2 response bytes",
    )?;
    if terminal {
        require(
            body.trailers().await?.is_none(),
            "no undeclared lifecycle response trailers",
        )?;
    }
    Ok(())
}

impl Peer {
    async fn exchange(&mut self, route: Route, path: &str) -> TestResult<()> {
        match &mut self.wire {
            PeerWire::H1(io) => {
                h1_request(io, path, 0, b"").await?;
                within("healthy H1 response head", h1_head(io, HEALTH.len())).await??;
                let mut bytes = [0; HEALTH.len()];
                within("healthy H1 response body", io.read_exact(&mut bytes)).await??;
                require(bytes.as_slice() == HEALTH, "exact healthy H1 response")?;
            }
            PeerWire::H2(sender) => {
                let mut exchange = h2_request(sender, route, path, 0, b"").await?;
                let response = within(
                    "healthy H2 response head",
                    exchange.response.take().expect("response future"),
                )
                .await??;
                h2_head(&response, HEALTH.len())?;
                within(
                    "healthy H2 body EOF",
                    h2_body(&mut response.into_body(), HEALTH, true),
                )
                .await??;
            }
        }
        Ok(())
    }

    async fn close(self) -> TestResult<()> {
        match self.wire {
            PeerWire::H1(mut io) => {
                within("healthy H1 transport shutdown", io.shutdown()).await??;
            }
            PeerWire::H2(sender) => drop(sender),
        }
        Ok(())
    }
}

struct H2Target {
    stream: SendStream<Bytes>,
    response: Option<ResponseFuture>,
    body: Option<RecvStream>,
}

enum TargetWire {
    H1(PeerIo),
    H2(H2Target),
}

struct ActiveTarget {
    wire: TargetWire,
    addresses: PeerAddresses,
    control: TargetControl,
}

async fn start_target(
    endpoint: &Endpoint,
    healthy: &mut Peer,
    stage: Stage,
) -> TestResult<ActiveTarget> {
    let control = endpoint.shared.install(stage)?;
    let path = format!("/target/{}", control.evidence.id);
    let data = if stage == Stage::Upload {
        &UPLOAD[..CHUNK]
    } else {
        &UPLOAD[..]
    };
    let (wire, addresses) = match &mut healthy.wire {
        PeerWire::H1(_) => {
            let (mut io, addresses) = endpoint.connect().await?;
            h1_request(&mut io, &path, BODY_SIZE, data).await?;
            (TargetWire::H1(io), addresses)
        }
        PeerWire::H2(sender) => {
            let exchange = h2_request(sender, endpoint.route, &path, BODY_SIZE, data).await?;
            (TargetWire::H2(exchange), healthy.addresses)
        }
    };
    let mut target = ActiveTarget {
        wire,
        addresses,
        control,
    };
    if matches!(stage, Stage::Response | Stage::Complete) {
        match &mut target.wire {
            TargetWire::H1(io) => {
                within("target H1 head", h1_head(io, BODY_SIZE)).await??;
                let expected = if stage == Stage::Response {
                    &DOWNLOAD[..CHUNK]
                } else {
                    &DOWNLOAD[..]
                };
                let mut bytes = vec![0; expected.len()];
                within("target H1 exact body", io.read_exact(&mut bytes)).await??;
                require(
                    bytes.as_slice() == expected,
                    "exact H1 target bytes before cancellation",
                )?;
            }
            TargetWire::H2(exchange) => {
                let response = within(
                    "target H2 head",
                    exchange.response.take().expect("target response future"),
                )
                .await??;
                h2_head(&response, BODY_SIZE)?;
                let mut body = response.into_body();
                let expected = if stage == Stage::Response {
                    &DOWNLOAD[..CHUNK]
                } else {
                    &DOWNLOAD[..]
                };
                within(
                    "target H2 exact body",
                    h2_body(&mut body, expected, stage == Stage::Complete),
                )
                .await??;
                exchange.body = Some(body);
            }
        }
    }
    if stage != Stage::Complete {
        within("native pending stage barrier", &mut target.control.entered).await??;
        target.control.evidence.held()?;
        let probe = format!("/pending/{}", target.control.evidence.id);
        healthy.exchange(endpoint.route, &probe).await?;
        let (target_context, target_stream) = endpoint.shared.request(&path, target.addresses)?;
        let (probe_context, probe_stream) = endpoint.shared.request(&probe, healthy.addresses)?;
        if endpoint.route.protocol == Protocol::H2 {
            require(
                Arc::ptr_eq(&target_context, &probe_context),
                "target and healthy probe share the same native H2 connection",
            )?;
            require(
                target_stream.is_some() && probe_stream.is_some() && target_stream != probe_stream,
                "target and healthy probe use different H2 streams",
            )?;
        } else {
            require(
                !Arc::ptr_eq(&target_context, &probe_context)
                    && target.addresses.local != healthy.addresses.local,
                "H1 target and healthy probe use distinct real connections",
            )?;
        }
        target.control.evidence.held()?;
    }
    Ok(target)
}

async fn finish_target(
    endpoint: &Endpoint,
    healthy: &mut Peer,
    target: ActiveTarget,
) -> TestResult<()> {
    let ActiveTarget {
        wire,
        addresses,
        control,
    } = target;
    let TargetControl {
        evidence,
        entered,
        release,
        backing: _,
    } = control;
    let cancelling = evidence.stage != Stage::Complete;
    if cancelling {
        evidence.cancelling.store(true, Ordering::Release);
    }
    let mut expected_reset_stream = None;
    match wire {
        TargetWire::H1(mut io) => {
            if cancelling {
                io.reset()?;
            } else {
                within("completed H1 target shutdown", io.shutdown()).await??;
            }
        }
        TargetWire::H2(mut exchange) => {
            if cancelling {
                expected_reset_stream = Some(StreamId::new(u64::from(
                    exchange.stream.stream_id().as_u32(),
                )));
                exchange.stream.send_reset(h2::Reason::CANCEL);
                let error = if let Some(response) = exchange.response.take() {
                    match within("cancelled H2 response future", response).await? {
                        Err(error) => error,
                        Ok(_) => {
                            return Err(FixtureError::Assertion(
                                "cancelled pre-header target cannot produce a response",
                            )
                            .into());
                        }
                    }
                } else {
                    let body = exchange.body.as_mut().ok_or(FixtureError::Assertion(
                        "cancelled H2 response body retained",
                    ))?;
                    match within("cancelled H2 response body", body.data()).await? {
                        Some(Err(error)) => error,
                        _ => return Err(FixtureError::Assertion(
                            "cancelled response body reports an explicit reset, not DATA or EOF",
                        )
                        .into()),
                    }
                };
                if !error.is_reset() || error.reason() != Some(h2::Reason::CANCEL) {
                    return Err(error.into());
                }
                *evidence.peer_reset.lock().expect("independent reset error") = Some(error);
            }
        }
    }
    let path = format!("/target/{}", evidence.id);
    endpoint.shared.request(&path, addresses)?;
    healthy
        .exchange(endpoint.route, &format!("/retired/{}", evidence.id))
        .await?;
    if let Some(stream) = expected_reset_stream {
        let resets = endpoint
            .shared
            .resets
            .lock()
            .expect("native reset observations");
        require(
            resets
                .iter()
                .filter(|entry| **entry == (stream, HttpErrorCode::CANCEL))
                .count()
                == 1,
            "native peer reset observed exactly once for the target stream",
        )?;
    }
    evidence.retired()?;
    drop(entered);
    drop(release);
    Ok(())
}

fn require_premature_retirement_rejection(
    result: TestResult<()>,
    target: usize,
    stage: Stage,
) -> TestResult<()> {
    match result {
        Err(error)
            if matches!(
                error.downcast_ref::<FixtureError>(),
                Some(FixtureError::PrematureRetirement { target: actual_target, stage: actual_stage })
                    if *actual_target == target && *actual_stage == stage
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
        Ok(()) => Err(FixtureError::Assertion(
            "positive oracle must reject the deliberately premature target retirement",
        )
        .into()),
    }
}

async fn premature_retirement_negative_control(
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
) -> TestResult<()> {
    require(
        endpoint.route.protocol == Protocol::H1 && !endpoint.route.tls,
        "negative control uses real TCP H1",
    )?;
    let mut healthy = endpoint.peer(drivers).await?;
    let target = start_target(endpoint, &mut healthy, Stage::Response).await?;
    let ActiveTarget {
        wire,
        addresses,
        control,
    } = target;
    let TargetControl {
        evidence,
        entered,
        release,
        backing: _,
    } = control;
    require(
        evidence.id == 1
            && endpoint
                .shared
                .targets
                .lock()
                .expect("target history")
                .len()
                == 1,
        "one negative-control target",
    )?;
    require(
        !evidence.cancelling.load(Ordering::Acquire),
        "negative control has not published cancellation",
    )?;
    evidence.held()?;
    let TargetWire::H1(io) = wire else {
        return Err(
            FixtureError::Assertion("negative control owns the real H1 target socket").into(),
        );
    };
    io.reset()?;
    endpoint
        .shared
        .request(&format!("/target/{}", evidence.id), addresses)?;
    healthy
        .exchange(endpoint.route, &format!("/retired/{}", evidence.id))
        .await?;
    require(
        !evidence.cancelling.load(Ordering::Acquire),
        "actual target receipt completed before cancellation publication",
    )?;
    require(
        evidence.premature_retirement.load(Ordering::Acquire),
        "real unmarked H1 abort records premature retirement",
    )?;
    require_premature_retirement_rejection(evidence.retired(), evidence.id, Stage::Response)?;
    evidence.cancelling.store(true, Ordering::Release);
    require(
        evidence.premature_retirement.load(Ordering::Acquire),
        "later cancellation publication cannot clear premature retirement",
    )?;
    require_premature_retirement_rejection(evidence.retired(), evidence.id, Stage::Response)?;
    drop(entered);
    drop(release);
    healthy.close().await
}

async fn context_matrix(
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
) -> TestResult<()> {
    let mut first = endpoint.peer(drivers).await?;
    first.exchange(endpoint.route, "/context/first-a").await?;
    first.exchange(endpoint.route, "/context/first-b").await?;
    let (first_context, first_stream) = endpoint
        .shared
        .request("/context/first-a", first.addresses)?;
    let (again, next_stream) = endpoint
        .shared
        .request("/context/first-b", first.addresses)?;
    require(
        Arc::ptr_eq(&first_context, &again),
        "one native connection preserves metadata identity across requests",
    )?;
    if endpoint.route.protocol == Protocol::H2 {
        require(
            first_stream.is_some() && next_stream.is_some() && first_stream != next_stream,
            "distinct native H2 streams retain connection metadata",
        )?;
    }
    let mut second = endpoint.peer(drivers).await?;
    second.exchange(endpoint.route, "/context/second-a").await?;
    second.exchange(endpoint.route, "/context/second-b").await?;
    let (second_context, second_stream) = endpoint
        .shared
        .request("/context/second-a", second.addresses)?;
    let (second_again, second_next_stream) = endpoint
        .shared
        .request("/context/second-b", second.addresses)?;
    require(
        Arc::ptr_eq(&second_context, &second_again),
        "fresh connection also preserves metadata identity",
    )?;
    if endpoint.route.protocol == Protocol::H2 {
        require(
            second_stream.is_some()
                && second_next_stream.is_some()
                && second_stream != second_next_stream,
            "fresh native H2 connection has distinct request streams",
        )?;
    }
    require(
        !Arc::ptr_eq(&first_context, &second_context),
        "new native connection has distinct metadata identity",
    )?;
    require(
        first.addresses.local != second.addresses.local,
        "independent new connection endpoint",
    )?;
    second.close().await?;
    first.close().await?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Checkpoint {
    R0,
    R1,
    R2,
    R3,
    R4,
    R5,
}

#[derive(Debug)]
struct ThreadIdentity {
    tid: i32,
    comm: String,
    starttime: u64,
}

impl ThreadIdentity {
    fn same_instance(&self, other: &Self) -> bool {
        self.tid == other.tid && self.starttime == other.starttime
    }
}

#[derive(Debug)]
struct ThreadObservation {
    tid: i32,
    identity: TestResult<ThreadIdentity>,
}

#[derive(Debug)]
struct ThreadSnapshot {
    entries: Vec<ThreadObservation>,
    scan_error: Option<BoxError>,
}

impl ThreadSnapshot {
    fn complete(&self) -> bool {
        self.scan_error.is_none() && self.entries.iter().all(|entry| entry.identity.is_ok())
    }

    fn identities(&self) -> impl Iterator<Item = &ThreadIdentity> {
        self.entries
            .iter()
            .filter_map(|entry| entry.identity.as_ref().ok())
    }

    fn take_errors(&mut self) -> Vec<BoxError> {
        let mut errors = Vec::with_capacity(MAX_THREAD_IDENTITIES + 1);
        for entry in self.entries.drain(..) {
            if let Err(error) = entry.identity {
                errors.push(error);
            }
        }
        if let Some(error) = self.scan_error.take() {
            errors.push(error);
        }
        errors
    }
}

fn read_thread_field(tid: i32, field: &'static str, limit: usize) -> TestResult<String> {
    let mut text = String::with_capacity(limit);
    std::fs::File::open(format!("/proc/self/task/{tid}/{field}"))?
        .take(limit as u64)
        .read_to_string(&mut text)?;
    require(
        text.len() < limit,
        "thread identity field reached its diagnostic read bound",
    )?;
    Ok(text)
}

fn read_thread_starttime(tid: i32) -> TestResult<u64> {
    let stat = read_thread_field(tid, "stat", MAX_THREAD_STAT_BYTES)?;
    let (reported_tid, remainder) = stat
        .split_once(" (")
        .ok_or(FixtureError::Assertion("thread stat numeric TID prefix"))?;
    require(
        reported_tid.parse::<i32>()? == tid,
        "thread stat belongs to the enumerated TID",
    )?;
    let (_, fields) = remainder
        .rsplit_once(") ")
        .ok_or(FixtureError::Assertion("thread stat command delimiter"))?;
    let starttime = fields
        .split_ascii_whitespace()
        .nth(19)
        .ok_or(FixtureError::Assertion("thread stat field 22 starttime"))?;
    require(
        !starttime.is_empty() && starttime.bytes().all(|byte| byte.is_ascii_digit()),
        "thread stat starttime is unsigned decimal",
    )?;
    Ok(starttime.parse()?)
}

fn read_thread_identity(tid: i32) -> TestResult<ThreadIdentity> {
    let before = read_thread_starttime(tid).map_err(|source| FixtureError::ThreadIdentityRead {
        tid,
        phase: "stat-before",
        source,
    })?;
    let comm = (|| -> TestResult<String> {
        let mut comm = read_thread_field(tid, "comm", MAX_THREAD_COMM_BYTES)?;
        require(comm.ends_with('\n'), "thread comm has its kernel newline")?;
        comm.pop();
        Ok(comm)
    })()
    .map_err(|source| FixtureError::ThreadIdentityRead {
        tid,
        phase: "comm",
        source,
    })?;
    let after = read_thread_starttime(tid).map_err(|source| FixtureError::ThreadIdentityRead {
        tid,
        phase: "stat-after",
        source,
    })?;
    if before != after {
        return Err(FixtureError::ThreadIdentityChanged { tid, before, after }.into());
    }
    Ok(ThreadIdentity {
        tid,
        comm,
        starttime: before,
    })
}

fn thread_snapshot() -> ThreadSnapshot {
    let mut snapshot = ThreadSnapshot {
        entries: Vec::with_capacity(MAX_THREAD_IDENTITIES),
        scan_error: None,
    };
    let scanned = (|| -> TestResult<()> {
        for entry in std::fs::read_dir("/proc/self/task")? {
            require(
                snapshot.entries.len() < MAX_THREAD_IDENTITIES,
                "32-TID diagnostic capacity",
            )?;
            let entry = entry?;
            let tid = entry
                .file_name()
                .to_str()
                .ok_or(FixtureError::Assertion("numeric diagnostic TID"))?
                .parse::<i32>()?;
            require(tid > 0, "positive diagnostic TID")?;
            require(
                snapshot.entries.iter().all(|entry| entry.tid != tid),
                "diagnostic enumeration did not repeat a TID",
            )?;
            snapshot.entries.push(ThreadObservation {
                tid,
                identity: read_thread_identity(tid),
            });
        }
        require(
            !snapshot.entries.is_empty(),
            "nonempty live-process thread snapshot",
        )
    })();
    snapshot.scan_error = scanned.err();
    snapshot
}

fn report_thread_snapshot(checkpoint: Checkpoint, snapshot: &ThreadSnapshot) {
    eprintln!(
        "native-thread-snapshot {checkpoint:?} complete={} scan_error={:?}",
        snapshot.complete(),
        snapshot.scan_error
    );
    for entry in &snapshot.entries {
        match &entry.identity {
            Ok(identity) => eprintln!(
                "native-thread {checkpoint:?} tid={} comm={:?} starttime_ticks={}",
                identity.tid, identity.comm, identity.starttime,
            ),
            Err(error) => eprintln!(
                "native-thread {checkpoint:?} tid={} identity_unavailable={error:?}",
                entry.tid
            ),
        }
    }
}

fn report_thread_difference(
    previous: (Checkpoint, &ThreadSnapshot),
    current: (Checkpoint, &ThreadSnapshot),
) {
    let (from, before) = previous;
    let (to, after) = current;
    if !before.complete() || !after.complete() {
        eprintln!("native-thread-difference {from:?}->{to:?} incomplete=true");
        return;
    }
    let added: Vec<_> = after
        .identities()
        .filter(|identity| {
            !before
                .identities()
                .any(|other| identity.same_instance(other))
        })
        .collect();
    let removed: Vec<_> = before
        .identities()
        .filter(|identity| {
            !after
                .identities()
                .any(|other| identity.same_instance(other))
        })
        .collect();
    eprintln!("native-thread-difference {from:?}->{to:?} added={added:?} removed={removed:?}");
}

#[derive(Debug)]
struct ResourceSample {
    checkpoint: Checkpoint,
    cycle: Option<usize>,
    fd_count: usize,
    thread_count: usize,
    rss_bytes: u64,
    worker_tid: Option<i32>,
    worker_present: bool,
    threads: Option<ThreadSnapshot>,
}

fn sample(
    checkpoint: Checkpoint,
    cycle: Option<usize>,
    worker: Option<i32>,
) -> TestResult<ResourceSample> {
    let threads = matches!(
        checkpoint,
        Checkpoint::R0 | Checkpoint::R1 | Checkpoint::R4 | Checkpoint::R5
    )
    .then(thread_snapshot);
    match sample_metrics(checkpoint, cycle, worker) {
        Ok(mut observed) => {
            observed.threads = threads;
            Ok(observed)
        }
        Err(error) => {
            if let Some(mut threads) = threads {
                report_thread_snapshot(checkpoint, &threads);
                let mut errors = threads.take_errors();
                if !errors.is_empty() {
                    errors.insert(0, error);
                    return Err(FixtureError::ResourceObservation { checkpoint, errors }.into());
                }
            }
            Err(error)
        }
    }
}

fn sample_metrics(
    checkpoint: Checkpoint,
    cycle: Option<usize>,
    worker: Option<i32>,
) -> TestResult<ResourceSample> {
    let mut status = String::with_capacity(65536);
    std::fs::File::open("/proc/self/status")?
        .take(65537)
        .read_to_string(&mut status)?;
    require(status.len() <= 65536, "bounded proc status")?;
    let mut rss = None;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            require(rss.is_none(), "one VmRSS sample")?;
            let mut fields = value.split_whitespace();
            let kib = fields
                .next()
                .ok_or(FixtureError::Assertion("VmRSS value"))?
                .parse::<u64>()?;
            require(
                fields.next() == Some("kB") && fields.next().is_none(),
                "explicit Linux VmRSS units",
            )?;
            rss = Some(
                kib.checked_mul(1024)
                    .ok_or(FixtureError::Assertion("VmRSS conversion overflow"))?,
            );
        }
    }
    let rss_bytes = rss.ok_or(FixtureError::Assertion("Linux process VmRSS is available"))?;
    require(rss_bytes != 0, "nonzero observed RSS")?;
    drop(status);
    let mut fd_count = 0usize;
    for entry in std::fs::read_dir("/proc/self/fd")? {
        entry?;
        fd_count += 1;
        require(
            fd_count <= MAX_PROC_ENTRIES,
            "bounded process FD enumeration",
        )?;
    }
    let fd_count = fd_count.checked_sub(1).ok_or(FixtureError::Assertion(
        "FD enumeration includes its own directory descriptor",
    ))?;
    let mut thread_count = 0;
    let mut worker_present = false;
    for entry in std::fs::read_dir("/proc/self/task")? {
        let entry = entry?;
        let tid = entry
            .file_name()
            .to_str()
            .ok_or(FixtureError::Assertion("numeric Linux TID"))?
            .parse::<i32>()?;
        worker_present |= worker == Some(tid);
        thread_count += 1;
        require(
            thread_count <= MAX_PROC_ENTRIES,
            "bounded process TID enumeration",
        )?;
    }
    require(thread_count != 0, "sampler process remains alive")?;
    Ok(ResourceSample {
        checkpoint,
        cycle,
        fd_count,
        thread_count,
        rss_bytes,
        worker_tid: worker,
        worker_present,
        threads: None,
    })
}

fn report_thread_snapshots(samples: &mut [ResourceSample], errors: &mut Vec<BoxError>) {
    let mut baseline = None;
    let mut previous = None;
    for sample in samples.iter() {
        if let Some(threads) = sample.threads.as_ref() {
            report_thread_snapshot(sample.checkpoint, threads);
            let current = (sample.checkpoint, threads);
            if let Some(previous) = previous {
                report_thread_difference(previous, current);
            }
            if matches!(sample.checkpoint, Checkpoint::R0) {
                baseline = Some(current);
            } else if matches!(sample.checkpoint, Checkpoint::R4 | Checkpoint::R5)
                && let Some(baseline) = baseline
            {
                report_thread_difference(baseline, current);
            }
            previous = Some(current);
        }
    }
    for sample in samples {
        if let Some(threads) = sample.threads.as_mut() {
            errors.append(&mut threads.take_errors());
        }
    }
}

fn record_sample(samples: &mut Vec<ResourceSample>, sample: ResourceSample) -> TestResult<()> {
    require(
        samples.len() < SAMPLE_COUNT,
        "bounded R0 through R5 sample series",
    )?;
    samples.push(sample);
    Ok(())
}

fn live_sample(
    server: &HttpServer,
    shared: &Shared,
    checkpoint: Checkpoint,
    cycle: Option<usize>,
) -> TestResult<ResourceSample> {
    require(
        server.state() == HttpServerState::Running && shared.stops.load(Ordering::Acquire) == 0,
        "resource sample retains a running native owner",
    )?;
    let worker = shared
        .owner
        .lock()
        .expect("native worker identity")
        .ok_or(FixtureError::Assertion("native worker started"))?;
    let observed = sample(checkpoint, cycle, Some(worker.linux))?;
    require(
        observed.worker_present,
        "actual native worker TID remains alive at the checkpoint",
    )?;
    Ok(observed)
}

fn backing_worker_alive(server: &HttpServer, shared: &Shared) -> TestResult<()> {
    require(
        server.state() == HttpServerState::Running
            && shared.starts.load(Ordering::Acquire) == 1
            && shared.stops.load(Ordering::Acquire) == 0,
        "R3-B retains the running native worker, not shutdown-assisted release",
    )
}

struct BackingPairContext<'a> {
    runtime: &'a Runtime,
    server: &'a HttpServer,
    endpoint: &'a Endpoint,
    healthy: &'a mut Peer,
}

fn finish_backing_pair(
    context: BackingPairContext<'_>,
    cycle: usize,
    complete: BackingWait,
    mut cancelled: BackingWait,
) -> TestResult<()> {
    let BackingPairContext {
        runtime,
        server,
        endpoint,
        healthy,
    } = context;
    let totals = endpoint
        .shared
        .backing
        .as_ref()
        .expect("owned backing exercise");
    backing_worker_alive(server, &endpoint.shared)?;
    eprintln!(
        "native-backing after-original-checkpoint cycle={cycle} {:?}",
        totals.snapshot()
    );
    drive(runtime, async {
        within("R3-B actual response backing destruction", async {
            if cancelled.record.keep_alias {
                cancelled.record.held()?;
                require(
                    cancelled.record.alias_bytes.load(Ordering::Acquire) == 1,
                    "one worker-local byte retains the full backing after target receipt",
                )?;
                let pending =
                    poll_fn(|context| Poll::Ready(Pin::new(&mut cancelled.complete).poll(context)))
                        .await;
                match pending {
                    Poll::Pending => {}
                    Poll::Ready(Err(error)) => return Err(BoxError::from(error)),
                    Poll::Ready(Ok(())) => {
                        return Err(FixtureError::Assertion(
                            "backing destruction preceded actual alias release",
                        )
                        .into());
                    }
                }
                healthy
                    .exchange(
                        endpoint.route,
                        &format!("/backing-release/{}", cancelled.record.target),
                    )
                    .await?;
            }
            for wait in [complete, cancelled] {
                wait.complete.await?;
                wait.record.zero()?;
                require(
                    wait.record.alias_bytes.load(Ordering::Acquire) == 0,
                    "actual worker-local alias ownership released",
                )?;
            }
            Ok(())
        })
        .await?
    })?;
    backing_worker_alive(server, &endpoint.shared)?;
    totals.reconciled(2 * (cycle + 1))?;
    let checkpoint = if cycle >= WARMUPS {
        "R3-B"
    } else {
        "warmup-backing"
    };
    eprintln!(
        "native-backing {checkpoint} cycle={cycle} {:?}",
        totals.snapshot()
    );
    Ok(())
}

fn resource_series(
    runtime: &Runtime,
    server: &HttpServer,
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
    samples: &mut Vec<ResourceSample>,
) -> TestResult<()> {
    let started = Instant::now();
    let mut healthy = drive(runtime, endpoint.peer(drivers))?;
    for cycle in 0..WARMUPS + MEASURED {
        require(
            started.elapsed() < SERIES_BUDGET,
            "finite native resource series budget",
        )?;
        let complete_backing = drive(runtime, async {
            let mut target = start_target(endpoint, &mut healthy, Stage::Complete).await?;
            let backing = target.control.backing.take();
            finish_target(endpoint, &mut healthy, target).await?;
            Ok(backing)
        })?;
        let mut target = drive(
            runtime,
            start_target(endpoint, &mut healthy, Stage::Response),
        )?;
        let cancelled_backing = target.control.backing.take();
        if let Some(backing) = &target.control.evidence.backing {
            backing.held()?;
            require(
                backing.alias_bytes.load(Ordering::Acquire) == usize::from(backing.keep_alias),
                "retained alias matches the declared measured negative arm",
            )?;
        }
        if cycle >= WARMUPS {
            record_sample(
                samples,
                live_sample(
                    server,
                    &endpoint.shared,
                    Checkpoint::R2,
                    Some(cycle - WARMUPS),
                )?,
            )?;
        }
        drive(runtime, finish_target(endpoint, &mut healthy, target))?;
        if cycle + 1 == WARMUPS {
            record_sample(
                samples,
                live_sample(server, &endpoint.shared, Checkpoint::R1, None)?,
            )?;
        } else if cycle >= WARMUPS {
            record_sample(
                samples,
                live_sample(
                    server,
                    &endpoint.shared,
                    Checkpoint::R3,
                    Some(cycle - WARMUPS),
                )?,
            )?;
        }
        match (complete_backing, cancelled_backing) {
            (Some(complete), Some(cancelled)) => {
                finish_backing_pair(
                    BackingPairContext {
                        runtime,
                        server,
                        endpoint,
                        healthy: &mut healthy,
                    },
                    cycle,
                    complete,
                    cancelled,
                )?;
            }
            (None, None) => {}
            _ => {
                return Err(FixtureError::Assertion(
                    "both actual response backing waits are retained",
                )
                .into());
            }
        }
    }
    drive(runtime, healthy.close())?;
    Ok(())
}

const RETENTION_SELECTOR: &str =
    "native_lifecycle_readiness_tests::native_selected_workload_retention_control";
const RETENTION_CHILD_MARKER: &str = "ROXYGEN_NATIVE_RETENTION_CHILD";
const RETENTION_CHILD_VERSION: &str = "v1";
const RETENTION_RECEIPT: &str = "roxygen-native-retention-v1 complete capsules=8";
const RETENTION_PROCESS_BOUND: Duration = Duration::from_secs(900);
const RETENTION_POLL_INTERVAL: Duration = Duration::from_millis(10);
const RETENTION_OUTPUT_BOUND: usize = 256 * 1024;
const RETENTION_CHILD_ARGS: [&str; 4] = [
    RETENTION_SELECTOR,
    "--exact",
    "--nocapture",
    "--test-threads=1",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionHeld {
    target: usize,
    worker_tid: i32,
    rss_bytes: u64,
    fd_count: usize,
    thread_count: usize,
    pending_receipts: usize,
    upload_bytes: usize,
    response_bytes: usize,
}

fn selected_retention_pair(
    runtime: &Runtime,
    server: &HttpServer,
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
) -> TestResult<RetentionHeld> {
    require(
        ROUTES.contains(&endpoint.route),
        "selected retention capsule uses one declared native route",
    )?;
    let mut healthy = drive(runtime, endpoint.peer(drivers))?;
    let complete = drive(runtime, async {
        let mut target = start_target(endpoint, &mut healthy, Stage::Complete).await?;
        require(
            target.control.evidence.id == 1,
            "first capsule operation completes",
        )?;
        let backing = target
            .control
            .backing
            .take()
            .ok_or(FixtureError::Assertion(
                "complete target has its owned response backing receipt",
            ))?;
        finish_target(endpoint, &mut healthy, target).await?;
        Ok(backing)
    })?;
    let mut target = drive(
        runtime,
        start_target(endpoint, &mut healthy, Stage::Response),
    )?;
    let cancelled = target
        .control
        .backing
        .take()
        .ok_or(FixtureError::Assertion(
            "cancel target has its owned response backing receipt",
        ))?;
    let evidence = &target.control.evidence;
    require(
        evidence.id == 2 && evidence.pending_checks.load(Ordering::Acquire) == 1,
        "second capsule operation has a real pending receipt and independent healthy probe",
    )?;
    evidence.held()?;
    cancelled.record.held()?;
    let observed = live_sample(server, &endpoint.shared, Checkpoint::R2, None)?;
    evidence.held()?;
    let held = RetentionHeld {
        target: evidence.id,
        worker_tid: observed
            .worker_tid
            .ok_or(FixtureError::Assertion("held native worker TID"))?,
        rss_bytes: observed.rss_bytes,
        fd_count: observed.fd_count,
        thread_count: observed.thread_count,
        pending_receipts: evidence.pending_checks.load(Ordering::Acquire),
        upload_bytes: evidence.upload_bytes.load(Ordering::Acquire),
        response_bytes: evidence.response_bytes.load(Ordering::Acquire),
    };
    drive(runtime, finish_target(endpoint, &mut healthy, target))?;
    drive(runtime, async {
        for backing in [complete, cancelled] {
            require(
                !backing.record.keep_alias,
                "no hidden extra capsule operation",
            )?;
            backing.complete.await?;
            backing.record.zero()?;
            require(
                backing.record.alias_bytes.load(Ordering::Acquire) == 0,
                "both capsule backing owners were released",
            )?;
        }
        healthy.close().await
    })?;
    backing_worker_alive(server, &endpoint.shared)?;
    require(
        endpoint
            .shared
            .targets
            .lock()
            .expect("capsule target history")
            .len()
            == 2,
        "one complete and one response-cancel operation per capsule",
    )?;
    endpoint
        .shared
        .backing
        .as_ref()
        .expect("capsule backing totals")
        .reconciled(2)?;
    Ok(held)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionResources {
    rss_bytes: u64,
    process_hwm_bytes: u64,
    fd_count: usize,
    thread_count: usize,
}

fn retention_rss_field(status: &str, name: &str) -> TestResult<u64> {
    let mut matches = status.lines().filter_map(|line| line.strip_prefix(name));
    let value = matches
        .next()
        .ok_or(FixtureError::Assertion("required retention RSS field"))?;
    require(matches.next().is_none(), "unique retention RSS field")?;
    let mut fields = value.split_ascii_whitespace();
    let number = fields
        .next()
        .ok_or(FixtureError::Assertion("retention RSS value"))?;
    require(
        !number.is_empty()
            && number.bytes().all(|byte| byte.is_ascii_digit())
            && fields.next() == Some("kB")
            && fields.next().is_none(),
        "unsigned retention RSS with explicit Linux kB units",
    )?;
    number
        .parse::<u64>()?
        .checked_mul(1024)
        .ok_or_else(|| FixtureError::Assertion("retention RSS conversion overflow").into())
}

fn retention_directory_count(path: &'static str) -> TestResult<usize> {
    std::fs::read_dir(path)?.try_fold(0, |count, entry| {
        drop(entry?);
        require(
            count < MAX_PROC_ENTRIES,
            "bounded retention resource enumeration",
        )?;
        Ok(count + 1)
    })
}

fn retention_resources() -> TestResult<RetentionResources> {
    require(
        tokio::runtime::Handle::try_current().is_err(),
        "retention resource reads occur outside async execution",
    )?;
    let mut buffer = [0; 65537];
    let mut status = File::open("/proc/self/status")?;
    let mut used = 0;
    loop {
        require(used < buffer.len(), "bounded retention proc status")?;
        match status.read(&mut buffer[used..]) {
            Ok(0) => break,
            Ok(count) => used += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    drop(status);
    let text = std::str::from_utf8(&buffer[..used])?;
    let rss_bytes = retention_rss_field(text, "VmRSS:")?;
    let process_hwm_bytes = retention_rss_field(text, "VmHWM:")?;
    let fd_count = retention_directory_count("/proc/self/fd")?
        .checked_sub(1)
        .ok_or(FixtureError::Assertion(
            "retention sampler directory descriptor",
        ))?;
    let thread_count = retention_directory_count("/proc/self/task")?;
    require(
        rss_bytes > 0 && process_hwm_bytes >= rss_bytes && fd_count > 0 && thread_count > 0,
        "positive real retention resources and process-lifetime high water",
    )?;
    Ok(RetentionResources {
        rss_bytes,
        process_hwm_bytes,
        fd_count,
        thread_count,
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

fn retention_ceiling(index: usize, baseline: usize, actual: usize) -> TestResult<()> {
    if actual > baseline {
        Err(FixtureError::RetentionGrowth {
            index,
            baseline,
            actual,
        }
        .into())
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionSensitivity {
    baseline: usize,
    held: usize,
    released: usize,
    capacity: usize,
    visible: usize,
}

fn retention_holder_outcome<Output, Diagnostic: Write>(
    outcome: std::thread::Result<TestResult<Output>>,
    joined: std::thread::Result<TestResult<()>>,
    diagnostic: impl FnOnce() -> Diagnostic,
) -> std::thread::Result<TestResult<Output>> {
    match (outcome, joined) {
        (Err(primary), joined) => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                let mut diagnostic = diagnostic();
                match joined {
                    Ok(Err(error)) => {
                        let _ = writeln!(diagnostic, "holder cleanup: {error:?}");
                    }
                    Err(payload) => {
                        let _ = writeln!(
                            diagnostic,
                            "holder also panicked during cleanup: {payload:?}"
                        );
                    }
                    Ok(Ok(())) => {}
                }
            }));
            Err(primary)
        }
        (Ok(Err(primary)), Err(payload)) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload")
                .chars()
                .take(256)
                .collect();
            let cleanup = FixtureError::RetentionHolderPanic {
                message,
                payload: Mutex::new(payload),
            };
            Ok(Err(FixtureError::RetentionCleanup {
                primary,
                cleanup: vec![cleanup.into()],
            }
            .into()))
        }
        (Ok(Ok(_)), Err(payload)) => Err(payload),
        (Ok(Err(primary)), Ok(Err(cleanup))) => Ok(Err(FixtureError::RetentionCleanup {
            primary,
            cleanup: vec![cleanup],
        }
        .into())),
        (Ok(Err(error)), Ok(Ok(()))) | (Ok(Ok(_)), Ok(Err(error))) => Ok(Err(error)),
        (Ok(Ok(value)), Ok(Ok(()))) => Ok(Ok(value)),
    }
}

fn retention_sensitivity(allocator: &RetentionAllocator) -> TestResult<RetentionSensitivity> {
    let (ready_send, ready) = thread_channel::sync_channel(1);
    let (allocate, allocate_receive) = thread_channel::sync_channel(1);
    let (held_send, held) = thread_channel::sync_channel(1);
    let (release, release_receive) = thread_channel::sync_channel(1);
    let holder = std::thread::Builder::new().spawn(move || -> TestResult<()> {
        ready_send.send(())?;
        allocate_receive.recv_timeout(STEP_TIMEOUT)?;
        let buffer = DOWNLOAD.to_vec();
        let capacity = buffer.capacity();
        require(
            capacity == BODY_SIZE,
            "sensitivity Vec has actual8192-byte capacity",
        )?;
        let bytes = Bytes::from(buffer);
        let alias = bytes.slice(..1);
        require(
            alias.as_ptr() == bytes.as_ptr(),
            "sensitivity alias shares actual Vec storage",
        )?;
        drop(bytes);
        require(
            alias.as_ref() == &DOWNLOAD[..1],
            "one real touched byte remains visible",
        )?;
        held_send.send((capacity, alias.len()))?;
        let released = release_receive.recv_timeout(STEP_TIMEOUT);
        drop(alias);
        released?;
        Ok(())
    })?;
    let outcome = catch_unwind(AssertUnwindSafe(
        || -> TestResult<(usize, usize, usize, usize)> {
            ready.recv_timeout(STEP_TIMEOUT)?;
            let baseline = allocator.read()?;
            allocate.try_send(())?;
            let (capacity, visible) = held.recv_timeout(STEP_TIMEOUT)?;
            let observed = allocator.read()?;
            require(
                capacity == BODY_SIZE
                    && visible == 1
                    && observed
                        .checked_sub(baseline)
                        .is_some_and(|delta| delta >= capacity),
                "allocator sensitivity sees full backing, not just one visible byte",
            )?;
            let rejected = retention_ceiling(MEASURED, baseline, observed).expect_err(
                "the actual retained alias must be rejected by the measured zero-growth predicate",
            );
            require(
                matches!(rejected.downcast_ref::<FixtureError>(),
                Some(FixtureError::RetentionGrowth { index, baseline: actual_baseline, actual })
                    if *index == MEASURED && *actual_baseline == baseline && *actual == observed),
                "sensitivity preserves the exact typed growth rejection",
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
    retention_ceiling(MEASURED + 1, baseline, released)?;
    Ok(RetentionSensitivity {
        baseline,
        held,
        released,
        capacity,
        visible,
    })
}

fn retention_capsule() -> TestResult<RetentionHeld> {
    retention_route_capsule(Route {
        protocol: Protocol::H1,
        tls: false,
    })
}

fn retention_route_capsule(route: Route) -> TestResult<RetentionHeld> {
    let capsule = std::thread::Builder::new().spawn(move || -> TestResult<_> {
        let kernel_task = fs::read_link("/proc/thread-self")?;
        let held = run_case_observed(route, Exercise::SelectedRetention)?.ok_or(
            FixtureError::Assertion("selected capsule returned its held observation"),
        )?;
        Ok((kernel_task, held))
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
            started.elapsed() < STEP_TIMEOUT,
            "joined retention capsule leaves the kernel task table",
        )?;
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(held)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionMeasurement {
    held: RetentionHeld,
    retired: RetentionResources,
    allocated: usize,
}

fn retention_resource_recovery(
    actual: RetentionResources,
    baseline: RetentionResources,
) -> TestResult<()> {
    require(
        actual.fd_count <= baseline.fd_count && actual.thread_count <= baseline.thread_count,
        "selected capsule FD/thread recovery against the fixed warmed baseline",
    )
}

fn retention_final_measurement(
    measurements: &[Option<RetentionMeasurement>; MEASURED],
) -> TestResult<RetentionMeasurement> {
    require(
        measurements.iter().all(Option::is_some),
        "all eight completed capsules",
    )?;
    measurements[MEASURED - 1]
        .ok_or_else(|| FixtureError::Assertion("all eight completed capsules").into())
}

fn retention_witness() -> TestResult<()> {
    retention_route_witness(retention_capsule, RETENTION_RECEIPT)
}

fn retention_route_witness(
    mut capsule: impl FnMut() -> TestResult<RetentionHeld>,
    receipt: &'static str,
) -> TestResult<()> {
    let mut measurements: [Option<RetentionMeasurement>; MEASURED] = [None; MEASURED];
    let allocator = RetentionAllocator::new()?;
    let allocator_description = allocator.description();
    require(
        !allocator_description.is_empty() && allocator_description.len() <= 256,
        "bounded allocator description",
    )?;
    retention_literal_controls()?;
    let sensitivity = retention_sensitivity(&allocator)?;
    let mut warmed = None;
    for _ in 0..WARMUPS {
        capsule()?;
        warmed = Some(retention_resources()?);
        allocator.read()?;
    }
    let warmed = warmed.ok_or(FixtureError::Assertion("two completed warmup capsules"))?;
    let baseline = allocator.read()?;
    let mut failure = None;
    for (index, measurement) in measurements.iter_mut().enumerate() {
        let held = capsule()?;
        let retired = retention_resources()?;
        let allocated = allocator.read()?;
        *measurement = Some(RetentionMeasurement {
            held,
            retired,
            allocated,
        });
        if let Err(source) = retention_resource_recovery(retired, warmed)
            .and_then(|()| retention_ceiling(index, baseline, allocated))
        {
            failure = Some(source);
            break;
        }
    }
    if let Some(source) = failure {
        return Err(FixtureError::RetentionMeasurements {
            source,
            allocator_description: allocator_description.to_owned(),
            sensitivity: Box::new(sensitivity),
            baseline,
            warmed,
            observations: Box::new(measurements),
        }
        .into());
    }
    let last = retention_final_measurement(&measurements).map_err(|source| {
        FixtureError::RetentionMeasurements {
            source,
            allocator_description: allocator_description.to_owned(),
            sensitivity: Box::new(sensitivity),
            baseline,
            warmed,
            observations: Box::new(measurements),
        }
    })?;
    let mut output = io::stdout().lock();
    writeln!(
        output,
        "native-retention allocator={allocator_description:?} baseline={baseline} warmed={warmed:?}"
    )?;
    writeln!(
        output,
        "native-retention sensitivity baseline={} held={} released={} capacity={} visible={}",
        sensitivity.baseline,
        sensitivity.held,
        sensitivity.released,
        sensitivity.capacity,
        sensitivity.visible
    )?;
    for (index, measurement) in measurements.into_iter().enumerate() {
        let measurement =
            measurement.ok_or(FixtureError::Assertion("no omitted capsule receipt"))?;
        let held = measurement.held;
        writeln!(
            output,
            "native-retention capsule={index} target={} worker_tid={} held_rss={} held_fds={} held_threads={} pending_receipts={} upload={} response_prefix={} allocated={} retired={:?}",
            held.target,
            held.worker_tid,
            held.rss_bytes,
            held.fd_count,
            held.thread_count,
            held.pending_receipts,
            held.upload_bytes,
            held.response_bytes,
            measurement.allocated,
            measurement.retired
        )?;
    }
    writeln!(
        output,
        "native-retention selected_live_capsule=7 live_rss={} post_owner_rss={} whole_child_lifetime_hwm={} includes_setup_warmup_controls=true",
        last.held.rss_bytes, last.retired.rss_bytes, last.retired.process_hwm_bytes
    )?;
    writeln!(output, "{receipt}")?;
    Ok(())
}

struct RetentionOutputDirectory {
    path: PathBuf,
}

impl RetentionOutputDirectory {
    fn create() -> TestResult<Self> {
        let path = env::temp_dir().join(format!("roxygen-native-retention-{}", std::process::id()));
        DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self { path })
    }

    fn remove(&self, errors: &mut Vec<BoxError>) {
        for name in ["stdout", "stderr"] {
            if let Err(error) = std::fs::remove_file(self.path.join(name)) {
                errors.push(error.into());
            }
        }
        if let Err(error) = std::fs::remove_dir(&self.path) {
            errors.push(error.into());
        }
    }
}

impl Drop for RetentionOutputDirectory {
    fn drop(&mut self) {
        for name in ["stdout", "stderr"] {
            let _ = std::fs::remove_file(self.path.join(name));
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

struct RetentionCapture {
    pipe: UnixStream,
    file: File,
    bytes: usize,
    eof: bool,
}

impl RetentionCapture {
    fn create(path: &Path) -> TestResult<(Self, Stdio)> {
        let file = OpenOptions::new()
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

    fn pump(&mut self) -> TestResult<()> {
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
                        .ok_or(FixtureError::Assertion("retention output count overflow"))?;
                    require(
                        next <= RETENTION_OUTPUT_BOUND,
                        "retention child output exceeds256KiB",
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

    fn drain_reaped(&mut self) -> TestResult<()> {
        for _ in 0..=RETENTION_OUTPUT_BOUND / 4096 {
            self.pump()?;
            if self.eof {
                return Ok(());
            }
        }
        Err(FixtureError::Assertion("reaped retention child left an output writer alive").into())
    }

    fn text(&mut self) -> TestResult<String> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut text = String::with_capacity(self.bytes);
        Read::by_ref(&mut self.file)
            .take(RETENTION_OUTPUT_BOUND as u64 + 1)
            .read_to_string(&mut text)?;
        require(
            text.len() == self.bytes,
            "retained child output matches bounded capture",
        )?;
        Ok(text)
    }
}

fn retention_sigchld_valid(handler: libc::sighandler_t, flags: libc::c_int) -> bool {
    handler == libc::SIG_DFL && flags & libc::SA_NOCLDWAIT == 0
}

fn retention_sigchld_policy() -> io::Result<()> {
    let mut action = std::mem::MaybeUninit::<libc::sigaction>::uninit();
    let result = unsafe { libc::sigaction(libc::SIGCHLD, std::ptr::null(), action.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let action = unsafe { action.assume_init() };
    if retention_sigchld_valid(action.sa_sigaction, action.sa_flags) {
        Ok(())
    } else {
        Err(io::Error::other(
            "retention requires default SIGCHLD without SA_NOCLDWAIT",
        ))
    }
}

trait RetentionChildOperations {
    fn poll_original(&mut self) -> io::Result<Option<ExitStatus>>;
    fn kill_original(&mut self) -> io::Result<()>;
    fn wait_original(&mut self) -> io::Result<ExitStatus>;
}

impl RetentionChildOperations for Child {
    fn poll_original(&mut self) -> io::Result<Option<ExitStatus>> {
        self.try_wait()
    }

    #[cfg(fbcode_build)]
    fn kill_original(&mut self) -> io::Result<()> {
        self.pidfd()?.kill()
    }

    #[cfg(not(fbcode_build))]
    fn kill_original(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn wait_original(&mut self) -> io::Result<ExitStatus> {
        self.wait()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionWaitAuthority {
    Owned,
    Reaped(ExitStatus),
    Lost,
}

struct RetentionChildOwner<Process: RetentionChildOperations = Child> {
    child: Process,
    authority: RetentionWaitAuthority,
    pidfd_confirmed: bool,
    forced: bool,
}

impl<Process: RetentionChildOperations> RetentionChildOwner<Process> {
    fn new(child: Process, pidfd_confirmed: bool) -> Self {
        Self {
            child,
            authority: RetentionWaitAuthority::Owned,
            pidfd_confirmed,
            forced: false,
        }
    }

    fn status(&self) -> Option<ExitStatus> {
        match self.authority {
            RetentionWaitAuthority::Reaped(status) => Some(status),
            RetentionWaitAuthority::Owned | RetentionWaitAuthority::Lost => None,
        }
    }

    fn observe_wait_error(&mut self, error: &io::Error) {
        if error.raw_os_error() == Some(libc::ECHILD) {
            self.authority = RetentionWaitAuthority::Lost;
        }
    }

    fn poll(&mut self) -> io::Result<Option<ExitStatus>> {
        match self.authority {
            RetentionWaitAuthority::Reaped(status) => return Ok(Some(status)),
            RetentionWaitAuthority::Lost => return Err(io::Error::from_raw_os_error(libc::ECHILD)),
            RetentionWaitAuthority::Owned => {}
        }
        loop {
            match self.child.poll_original() {
                Ok(Some(status)) => {
                    self.authority = RetentionWaitAuthority::Reaped(status);
                    return Ok(Some(status));
                }
                Ok(None) => return Ok(None),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.observe_wait_error(&error);
                    return Err(error);
                }
            }
        }
    }

    fn retire(&mut self, errors: &mut Vec<BoxError>) {
        self.retire_with(|error| errors.push(error.into()));
    }

    fn retire_with(&mut self, mut record: impl FnMut(io::Error)) {
        if self.authority != RetentionWaitAuthority::Owned {
            return;
        }
        match self.poll() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => record(error),
        }
        if self.authority != RetentionWaitAuthority::Owned {
            return;
        }
        if !self.forced && self.pidfd_confirmed {
            self.forced = true;
            if let Err(error) = self.child.kill_original() {
                record(error);
            }
        }
        loop {
            match self.child.wait_original() {
                Ok(status) => {
                    self.authority = RetentionWaitAuthority::Reaped(status);
                    return;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.observe_wait_error(&error);
                    record(error);
                    return;
                }
            }
        }
    }
}

impl<Process: RetentionChildOperations> Drop for RetentionChildOwner<Process> {
    fn drop(&mut self) {
        self.retire_with(|_| {});
    }
}

fn retention_spawn(command: &mut Command) -> TestResult<(RetentionChildOwner, io::Result<()>)> {
    retention_sigchld_policy()?;
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(command, || Ok(()));
    }
    #[cfg(fbcode_build)]
    let (child, pidfd) = {
        let child = command.create_pidfd(true).spawn()?;
        let pidfd = child.pidfd().map(|_| ());
        (child, pidfd)
    };
    // Without the unstable pidfd API the child is admitted like a kernel
    // without pidfd support: wait-only cleanup and no numeric signalling.
    #[cfg(not(fbcode_build))]
    let (child, pidfd) = (command.spawn()?, Err(io::ErrorKind::Unsupported.into()));
    let owner = RetentionChildOwner::new(child, pidfd.is_ok());
    Ok((owner, pidfd))
}

fn retention_owner_controls() -> TestResult<()> {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Operation {
        Poll,
        Kill,
        Wait,
    }

    struct Step {
        operation: Operation,
        result: Result<Option<ExitStatus>, libc::c_int>,
    }

    struct Script {
        steps: std::vec::IntoIter<Step>,
        calls: Vec<Operation>,
    }

    struct ScriptChild(Rc<RefCell<Script>>);

    impl ScriptChild {
        fn next(&self, operation: Operation) -> io::Result<Option<ExitStatus>> {
            let mut script = self.0.borrow_mut();
            script.calls.push(operation);
            let step = script
                .steps
                .next()
                .ok_or_else(|| io::Error::other("unexpected owner operation"))?;
            if step.operation != operation {
                return Err(io::Error::other("owner operation order changed"));
            }
            step.result.map_err(io::Error::from_raw_os_error)
        }
    }

    impl RetentionChildOperations for ScriptChild {
        fn poll_original(&mut self) -> io::Result<Option<ExitStatus>> {
            self.next(Operation::Poll)
        }

        fn kill_original(&mut self) -> io::Result<()> {
            self.next(Operation::Kill).map(|_| ())
        }

        fn wait_original(&mut self) -> io::Result<ExitStatus> {
            self.next(Operation::Wait)?
                .ok_or_else(|| io::Error::other("wait must return an actual status"))
        }
    }

    fn scripted(steps: Vec<Step>) -> (RetentionChildOwner<ScriptChild>, Rc<RefCell<Script>>) {
        let script = Rc::new(RefCell::new(Script {
            steps: steps.into_iter(),
            calls: Vec::new(),
        }));
        (
            RetentionChildOwner::new(ScriptChild(Rc::clone(&script)), true),
            script,
        )
    }

    fn consumed(script: &Rc<RefCell<Script>>, expected: &[Operation]) -> TestResult<()> {
        let script = script.borrow();
        require(
            script.steps.len() == 0 && script.calls.as_slice() == expected,
            "owner consumed exactly the original scripted operations, including Drop",
        )
    }

    let step = |operation, result| Step { operation, result };
    let success = ExitStatus::from_raw(0);
    let killed = ExitStatus::from_raw(libc::SIGKILL);
    require(
        retention_sigchld_valid(libc::SIG_DFL, 0)
            && retention_sigchld_valid(libc::SIG_DFL, libc::SA_NOCLDSTOP)
            && !retention_sigchld_valid(libc::SIG_IGN, 0)
            && !retention_sigchld_valid(libc::SIG_ERR, 0)
            && !retention_sigchld_valid(libc::SIG_DFL, libc::SA_NOCLDWAIT)
            && !retention_sigchld_valid(libc::SIG_DFL, libc::SA_NOCLDWAIT | libc::SA_NOCLDSTOP),
        "default SIGCHLD differs from ignored, custom or auto-reaping policy",
    )?;
    for explicit_poll in [false, true] {
        let (mut owner, script) = scripted(vec![step(Operation::Poll, Err(libc::ECHILD))]);
        let mut errors = Vec::new();
        if explicit_poll {
            errors.push(BoxError::from(
                owner.poll().expect_err("lost original wait authority"),
            ));
        }
        owner.retire(&mut errors);
        require(
            owner.authority == RetentionWaitAuthority::Lost
                && owner.status().is_none()
                && !owner.forced
                && errors.len() == 1
                && errors[0]
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.raw_os_error() == Some(libc::ECHILD)),
            "original ECHILD is retained and never interpreted as a live owned PID",
        )?;
        let identity = errors[0]
            .downcast_ref::<io::Error>()
            .expect("original wait error") as *const io::Error;
        let primary = errors.remove(0);
        let retained = retention_parent_failure(Some(primary), &mut errors, true)
            .expect("lost wait cannot become accepted");
        retention_error_identity(&retained, identity)?;
        require(
            owner
                .poll()
                .expect_err("lost authority remains lost")
                .raw_os_error()
                == Some(libc::ECHILD),
            "later observation cannot restore lost authority",
        )?;
        owner.retire(&mut errors);
        drop(owner);
        consumed(&script, &[Operation::Poll])?;
    }
    let (owner, script) = scripted(vec![
        step(Operation::Poll, Err(libc::EINTR)),
        step(Operation::Poll, Err(libc::ECHILD)),
    ]);
    drop(owner);
    consumed(&script, &[Operation::Poll, Operation::Poll])?;
    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Err(libc::EINTR)),
        step(Operation::Poll, Ok(Some(success))),
    ]);
    require(
        owner.poll()? == Some(success) && owner.poll()? == Some(success),
        "interrupted poll retries the original child and caches only its actual status",
    )?;
    let mut errors = Vec::new();
    owner.retire(&mut errors);
    require(
        owner.authority == RetentionWaitAuthority::Reaped(success)
            && !owner.forced
            && errors.is_empty(),
        "already-reaped child needs no signal or second wait",
    )?;
    drop(owner);
    consumed(&script, &[Operation::Poll, Operation::Poll])?;

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Err(libc::EINTR)),
        step(Operation::Poll, Ok(None)),
        step(Operation::Kill, Ok(None)),
        step(Operation::Wait, Err(libc::EINTR)),
        step(Operation::Wait, Err(libc::EINTR)),
        step(Operation::Wait, Ok(Some(killed))),
    ]);
    owner.retire(&mut errors);
    require(
        owner.status() == Some(killed) && owner.forced && errors.is_empty(),
        "interrupted blocking waits preserve original ownership through actual reap",
    )?;
    owner.retire(&mut errors);
    drop(owner);
    consumed(
        &script,
        &[
            Operation::Poll,
            Operation::Poll,
            Operation::Kill,
            Operation::Wait,
            Operation::Wait,
            Operation::Wait,
        ],
    )?;

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Ok(None)),
        step(Operation::Kill, Ok(None)),
        step(Operation::Wait, Err(libc::ECHILD)),
    ]);
    owner.retire(&mut errors);
    require(
        owner.authority == RetentionWaitAuthority::Lost
            && owner.status().is_none()
            && owner.forced
            && errors.len() == 1
            && errors[0]
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.raw_os_error() == Some(libc::ECHILD)),
        "ECHILD from blocking wait revokes all later wait and pidfd-signal attempts",
    )?;
    owner.retire(&mut errors);
    drop(owner);
    consumed(
        &script,
        &[Operation::Poll, Operation::Kill, Operation::Wait],
    )?;
    errors.clear();

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Ok(None)),
        step(Operation::Kill, Ok(None)),
        step(Operation::Wait, Err(libc::EIO)),
        step(Operation::Poll, Ok(None)),
        step(Operation::Wait, Err(libc::EINTR)),
        step(Operation::Wait, Ok(Some(killed))),
    ]);
    owner.retire(&mut errors);
    require(
        owner.authority == RetentionWaitAuthority::Owned
            && owner.forced
            && errors.len() == 1
            && errors[0]
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.raw_os_error() == Some(libc::EIO)),
        "non-ECHILD wait failure retains ownership and the original error",
    )?;
    drop(owner);
    consumed(
        &script,
        &[
            Operation::Poll,
            Operation::Kill,
            Operation::Wait,
            Operation::Poll,
            Operation::Wait,
            Operation::Wait,
        ],
    )?;
    errors.clear();

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Err(libc::EIO)),
        step(Operation::Kill, Err(libc::EPERM)),
        step(Operation::Wait, Ok(Some(success))),
    ]);
    owner.retire(&mut errors);
    require(
        owner.status() == Some(success)
            && owner.forced
            && errors.len() == 2
            && errors[0]
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.raw_os_error() == Some(libc::EIO))
            && errors[1]
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.raw_os_error() == Some(libc::EPERM)),
        "poll failure precedes failed signal while original wait still completes",
    )?;
    drop(owner);
    consumed(
        &script,
        &[Operation::Poll, Operation::Kill, Operation::Wait],
    )?;
    errors.clear();

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Ok(None)),
        step(Operation::Wait, Err(libc::EIO)),
        step(Operation::Poll, Ok(None)),
        step(Operation::Wait, Ok(Some(success))),
    ]);
    owner.pidfd_confirmed = false;
    owner.retire(&mut errors);
    require(
        owner.authority == RetentionWaitAuthority::Owned
            && !owner.pidfd_confirmed
            && !owner.forced
            && errors.len() == 1,
        "missing pidfd permits only original wait cleanup, including after a wait error",
    )?;
    drop(owner);
    consumed(
        &script,
        &[
            Operation::Poll,
            Operation::Wait,
            Operation::Poll,
            Operation::Wait,
        ],
    )?;
    errors.clear();

    let (mut owner, script) = scripted(vec![
        step(Operation::Poll, Ok(None)),
        step(Operation::Wait, Ok(Some(success))),
    ]);
    owner.pidfd_confirmed = false;
    let unavailable = Box::new(io::Error::other("scripted missing original pidfd"));
    let identity = unavailable.as_ref() as *const io::Error;
    owner.retire(&mut errors);
    let retained = retention_parent_failure(
        Some(unavailable),
        &mut errors,
        retention_receipt_valid(
            owner.status().is_some_and(|status| status.success()),
            owner.forced,
            RETENTION_RECEIPT,
        ),
    )
    .expect("original pidfd admission failure cannot become a passing exit0");
    retention_error_identity(&retained, identity)?;
    require(
        owner.status() == Some(success)
            && !owner.pidfd_confirmed
            && !owner.forced
            && errors.is_empty(),
        "successful wait-only cleanup is not successful pidfd admission",
    )?;
    drop(owner);
    consumed(&script, &[Operation::Poll, Operation::Wait])?;

    let (owner, script) = scripted(vec![
        step(Operation::Poll, Ok(None)),
        step(Operation::Kill, Ok(None)),
        step(Operation::Wait, Err(libc::EINTR)),
        step(Operation::Wait, Ok(Some(killed))),
    ]);
    let original = Arc::new(());
    let payload = catch_unwind(AssertUnwindSafe(|| {
        let _owner = owner;
        std::panic::panic_any(Arc::clone(&original));
    }))
    .expect_err("original panic remains after fallback original wait");
    require(
        payload
            .downcast_ref::<Arc<()>>()
            .is_some_and(|actual| Arc::ptr_eq(actual, &original)),
        "fallback retirement preserves the original panic object",
    )?;
    consumed(
        &script,
        &[
            Operation::Poll,
            Operation::Kill,
            Operation::Wait,
            Operation::Wait,
        ],
    )
}

fn retention_child_arguments(marker: &OsStr, arguments: impl Iterator<Item = OsString>) -> bool {
    marker == OsStr::new(RETENTION_CHILD_VERSION)
        && arguments.eq(RETENTION_CHILD_ARGS.into_iter().map(OsString::from))
}

fn retention_receipt_valid(success: bool, forced: bool, stdout: &str) -> bool {
    success
        && !forced
        && stdout
            .lines()
            .filter(|line| *line == RETENTION_RECEIPT)
            .count()
            == 1
}

fn retention_before_deadline(elapsed: Duration) -> TestResult<()> {
    if elapsed >= RETENTION_PROCESS_BOUND {
        Err(FixtureError::Deadline("900-second isolated retention child").into())
    } else {
        Ok(())
    }
}

fn retention_observed_status(
    elapsed: Duration,
    observation: io::Result<Option<ExitStatus>>,
) -> TestResult<bool> {
    let status = observation?;
    retention_before_deadline(elapsed)?;
    match status {
        Some(status) if !status.success() => Err(FixtureError::RetentionExit(status).into()),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

fn retention_parent_failure(
    primary: Option<BoxError>,
    cleanup: &mut Vec<BoxError>,
    accepted: bool,
) -> Option<BoxError> {
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
                BoxError::from(FixtureError::Assertion(
                    "unforced exit0 and exactly one final retention receipt required",
                ))
            })
        })
}

fn retention_deadline_controls() -> TestResult<()> {
    let before = RETENTION_PROCESS_BOUND - Duration::from_nanos(1);
    let success = ExitStatus::from_raw(0);
    retention_before_deadline(before)?;
    require(
        retention_observed_status(before, Ok(Some(success)))?
            && !retention_observed_status(before, Ok(None))?,
        "before-deadline exit0 is admitted while a live child remains pending",
    )?;
    for elapsed in [
        RETENTION_PROCESS_BOUND,
        RETENTION_PROCESS_BOUND + Duration::from_nanos(1),
    ] {
        require(
            retention_before_deadline(elapsed).is_err()
                && retention_observed_status(elapsed, Ok(None)).is_err(),
            "exact and late boundaries reject continued observation",
        )?;
        let deadline = retention_observed_status(elapsed, Ok(Some(success)))
            .expect_err("exact and late exit0 observations must fail");
        let original = deadline
            .downcast_ref::<FixtureError>()
            .expect("deadline remains a typed fixture error");
        require(
            matches!(
                original,
                FixtureError::Deadline("900-second isolated retention child")
            ),
            "exit0 at or beyond the fixed boundary is a deadline failure",
        )?;
        let identity = original as *const FixtureError;
        let mut cleanup = vec![BoxError::from(io::Error::from(io::ErrorKind::BrokenPipe))];
        let retained = retention_parent_failure(
            Some(deadline),
            &mut cleanup,
            retention_receipt_valid(success.success(), false, RETENTION_RECEIPT),
        )
        .expect("later normal reap and receipt cannot erase the original deadline");
        require(
            retained
                .downcast_ref::<FixtureError>()
                .is_some_and(|actual| std::ptr::eq(actual, identity))
                && cleanup.len() == 1
                && cleanup[0]
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe),
            "deadline identity survives a normal reap and fallible secondary cleanup",
        )?;
    }
    let failed_status = ExitStatus::from_raw(1 << 8);
    let failure = retention_observed_status(before, Ok(Some(failed_status)))
        .expect_err("nonzero child status cannot pass before the deadline");
    require(
        matches!(failure.downcast_ref::<FixtureError>(),
            Some(FixtureError::RetentionExit(status)) if *status == failed_status),
        "nonzero child status retains its typed failure",
    )?;
    let poll_error = retention_observed_status(
        RETENTION_PROCESS_BOUND,
        Err(io::Error::from_raw_os_error(libc::EIO)),
    )
    .expect_err("a failed actual poll cannot pass");
    require(
        poll_error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(libc::EIO)),
        "the original poll error is not replaced by post-poll deadline classification",
    )
}

fn retention_error_identity(error: &BoxError, identity: *const io::Error) -> TestResult<()> {
    require(
        error
            .downcast_ref::<io::Error>()
            .is_some_and(|actual| std::ptr::eq(actual, identity)),
        "holder classification retains the original typed error allocation",
    )
}

fn retention_holder_controls() -> TestResult<()> {
    #[derive(Clone, Copy)]
    enum Outcome {
        Success,
        Error,
        Panic,
    }

    let outcomes = [Outcome::Success, Outcome::Error, Outcome::Panic];
    for main in outcomes {
        for holder in outcomes {
            let primary = Box::new(io::Error::from(io::ErrorKind::BrokenPipe));
            let primary_identity = primary.as_ref() as *const io::Error;
            let cleanup = Box::new(io::Error::from(io::ErrorKind::ConnectionReset));
            let cleanup_identity = cleanup.as_ref() as *const io::Error;
            let primary_panic = Arc::new(());
            let cleanup_panic = Arc::new(());
            let outcome: std::thread::Result<TestResult<usize>> = match main {
                Outcome::Success => Ok(Ok(7)),
                Outcome::Error => Ok(Err(primary)),
                Outcome::Panic => Err(Box::new(primary_panic.clone())),
            };
            let joined: std::thread::Result<TestResult<()>> = match holder {
                Outcome::Success => Ok(Ok(())),
                Outcome::Error => Ok(Err(cleanup)),
                Outcome::Panic => Err(Box::new(cleanup_panic.clone())),
            };
            let mut storage = [];
            let mut diagnostic: &mut [u8] = &mut storage;
            require(
                diagnostic
                    .write_all(b"cleanup")
                    .expect_err("zero-capacity diagnostic writer cannot succeed")
                    .kind()
                    == io::ErrorKind::WriteZero,
                "the cleanup control uses a genuinely fallible writer",
            )?;
            let mut reported = false;
            let observed = retention_holder_outcome(outcome, joined, || {
                reported = true;
                diagnostic
            });
            require(
                reported == matches!(main, Outcome::Panic),
                "diagnostic writer is opened only for primary panic cleanup",
            )?;
            match (main, holder, observed) {
                (Outcome::Panic, _, Err(payload)) => require(
                    payload
                        .downcast_ref::<Arc<()>>()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &primary_panic)),
                    "main panic identity survives every holder outcome and failed reporting",
                )?,
                (Outcome::Success, Outcome::Panic, Err(payload)) => require(
                    payload
                        .downcast_ref::<Arc<()>>()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &cleanup_panic)),
                    "holder panic is primary only after successful main observation",
                )?,
                (Outcome::Error, Outcome::Success, Ok(Err(error))) => {
                    retention_error_identity(&error, primary_identity)?;
                }
                (Outcome::Success, Outcome::Error, Ok(Err(error))) => {
                    retention_error_identity(&error, cleanup_identity)?;
                }
                (Outcome::Error, Outcome::Error | Outcome::Panic, Ok(Err(error))) => {
                    let Some(FixtureError::RetentionCleanup { primary, cleanup }) =
                        error.downcast_ref::<FixtureError>()
                    else {
                        return Err(FixtureError::Assertion("typed combined holder failure").into());
                    };
                    retention_error_identity(primary, primary_identity)?;
                    require(
                        error
                            .source()
                            .and_then(|source| source.downcast_ref::<io::Error>())
                            .is_some_and(|source| std::ptr::eq(source, primary_identity))
                            && cleanup.len() == 1,
                        "original primary remains the error source with one secondary failure",
                    )?;
                    if matches!(holder, Outcome::Error) {
                        retention_error_identity(&cleanup[0], cleanup_identity)?;
                    } else {
                        let Some(FixtureError::RetentionHolderPanic { message, payload }) =
                            cleanup[0].downcast_ref::<FixtureError>()
                        else {
                            return Err(
                                FixtureError::Assertion("typed secondary holder panic").into()
                            );
                        };
                        let payload = payload.lock().unwrap_or_else(|error| error.into_inner());
                        require(
                            message == "non-string panic payload"
                                && payload
                                    .downcast_ref::<Arc<()>>()
                                    .is_some_and(|actual| Arc::ptr_eq(actual, &cleanup_panic)),
                            "secondary panic context retains the actual opaque panic payload",
                        )?;
                    }
                }
                (Outcome::Success, Outcome::Success, Ok(Ok(value))) => {
                    require(
                        value == 7,
                        "successful holder classification retains its value",
                    )?;
                }
                _ => {
                    return Err(FixtureError::Assertion("holder precedence matrix outcome").into());
                }
            }
        }
    }
    Ok(())
}

fn retention_final_measurement_controls() -> TestResult<()> {
    let allocator_description = "synthetic-retention-control";
    let sensitivity = RetentionSensitivity {
        baseline: 4096,
        held: 12288,
        released: 4096,
        capacity: 8192,
        visible: 1,
    };
    let baseline = 16384;
    let warmed = RetentionResources {
        rss_bytes: 2048,
        process_hwm_bytes: 8192,
        fd_count: 4,
        thread_count: 1,
    };
    let measurements: [Option<RetentionMeasurement>; MEASURED] = std::array::from_fn(|index| {
        let offset = u64::try_from(index).expect("eight control indices fit u64");
        Some(RetentionMeasurement {
            held: RetentionHeld {
                target: index + 1,
                worker_tid: i32::try_from(index + 100).expect("bounded control worker TID"),
                rss_bytes: 8192,
                fd_count: index + 5,
                thread_count: index + 2,
                pending_receipts: 1,
                upload_bytes: BODY_SIZE,
                response_bytes: CHUNK,
            },
            retired: RetentionResources {
                rss_bytes: 3072 + offset,
                ..warmed
            },
            allocated: baseline - index,
        })
    });
    require(
        retention_final_measurement(&measurements)?
            == measurements[MEASURED - 1].expect("eight synthetic measurements"),
        "complete measurements retain the predeclared final capsule",
    )?;
    for index in 0..MEASURED {
        for held_rss in [8191, 8192, 8193] {
            let mut observed = measurements;
            observed[index]
                .as_mut()
                .expect("populated synthetic held slot")
                .held
                .rss_bytes = held_rss;
            require(
                retention_final_measurement(&observed)?
                    == observed[MEASURED - 1].expect("complete synthetic measurements"),
                "separate proc snapshots preserve values without cross-snapshot ordering",
            )?;
        }
        let mut missing = measurements;
        missing[index] = None;
        let source = retention_final_measurement(&missing)
            .expect_err("any missing capsule measurement must fail");
        let original = source
            .downcast_ref::<FixtureError>()
            .expect("typed incomplete-measurements assertion");
        require(
            matches!(
                original,
                FixtureError::Assertion("all eight completed capsules")
            ),
            "incomplete measurements preserve the original assertion kind and message",
        )?;
        let identity = original as *const FixtureError;
        let error = FixtureError::RetentionMeasurements {
            source,
            allocator_description: allocator_description.to_owned(),
            sensitivity: Box::new(sensitivity),
            baseline,
            warmed,
            observations: Box::new(missing),
        };
        require(
            error
                .source()
                .and_then(|source| source.downcast_ref::<FixtureError>())
                .is_some_and(|source| std::ptr::eq(source, identity)),
            "diagnostic envelope retains the original typed cause allocation",
        )?;
        let FixtureError::RetentionMeasurements {
            allocator_description: retained_version,
            sensitivity: retained_sensitivity,
            baseline: retained_baseline,
            warmed: retained_warmed,
            observations,
            ..
        } = error
        else {
            return Err(FixtureError::Assertion("typed retention diagnostic envelope").into());
        };
        require(
            retained_version == allocator_description
                && *retained_sensitivity == sensitivity
                && retained_baseline == baseline
                && retained_warmed == warmed
                && *observations == missing
                && observations
                    .iter()
                    .enumerate()
                    .all(|(slot, measurement)| measurement.is_some() == (slot != index)),
            "failure retains allocator, sensitivity, baseline, warmed and each raw slot",
        )?;
    }
    Ok(())
}

fn retention_literal_controls() -> TestResult<()> {
    retention_deadline_controls()?;
    retention_holder_controls()?;
    retention_final_measurement_controls()?;
    retention_ceiling(0, BODY_SIZE, BODY_SIZE)?;
    retention_ceiling(0, BODY_SIZE, BODY_SIZE - 1)?;
    let error = retention_ceiling(0, BODY_SIZE, BODY_SIZE + 1)
        .expect_err("one extra allocated byte must fail");
    require(
        matches!(error.downcast_ref::<FixtureError>(),
        Some(FixtureError::RetentionGrowth { index: 0, baseline, actual })
            if *baseline == BODY_SIZE && *actual == BODY_SIZE + 1),
        "exact typed one-byte excess",
    )?;
    require(
        retention_ceiling(0, usize::MAX - 1, usize::MAX).is_err(),
        "zero-growth predicate does not overflow at the integer boundary",
    )?;
    require(
        retention_receipt_valid(true, false, RETENTION_RECEIPT)
            && !retention_receipt_valid(false, false, RETENTION_RECEIPT)
            && !retention_receipt_valid(true, true, RETENTION_RECEIPT)
            && !retention_receipt_valid(true, false, "")
            && !retention_receipt_valid(
                true,
                false,
                &format!("{RETENTION_RECEIPT}\n{RETENTION_RECEIPT}"),
            ),
        "exit failure, force, absent and duplicate completion receipts are rejected",
    )?;
    require(
        retention_child_arguments(
            OsStr::new(RETENTION_CHILD_VERSION),
            RETENTION_CHILD_ARGS.into_iter().map(OsString::from),
        ) && !retention_child_arguments(
            OsStr::new("v2"),
            RETENTION_CHILD_ARGS.into_iter().map(OsString::from),
        ) && !retention_child_arguments(
            OsStr::new(RETENTION_CHILD_VERSION),
            RETENTION_CHILD_ARGS[..3]
                .iter()
                .copied()
                .map(OsString::from),
        ) && !retention_child_arguments(
            OsStr::new(RETENTION_CHILD_VERSION),
            RETENTION_CHILD_ARGS
                .into_iter()
                .chain(["--ignored"])
                .map(OsString::from),
        ),
        "only the versioned exact single-leaf child invocation is admitted",
    )?;
    require(
        retention_rss_field("VmRSS: 1 kB\n", "VmRSS:")? == 1024
            && retention_rss_field("VmRSS: 1 kB\nVmRSS: 2 kB\n", "VmRSS:").is_err()
            && retention_rss_field("VmRSS: 1 bytes\n", "VmRSS:").is_err()
            && retention_rss_field("VmRSS: 18446744073709551615 kB\n", "VmRSS:").is_err()
            && retention_rss_field("VmHWM: 1 kB\n", "VmRSS:").is_err(),
        "resource parser rejects duplicate, wrong-unit, overflowing and absent observations",
    )?;
    let baseline = RetentionResources {
        rss_bytes: 1024,
        process_hwm_bytes: 2048,
        fd_count: 4,
        thread_count: 2,
    };
    retention_resource_recovery(baseline, baseline)?;
    require(
        retention_resource_recovery(
            RetentionResources {
                fd_count: 5,
                ..baseline
            },
            baseline,
        )
        .is_err()
            && retention_resource_recovery(
                RetentionResources {
                    thread_count: 3,
                    ..baseline
                },
                baseline,
            )
            .is_err(),
        "descriptor and thread growth fail independently of allocated-byte recovery",
    )
}

fn retention_parent() -> TestResult<()> {
    let directory = RetentionOutputDirectory::create()?;
    let (mut stdout, child_stdout) = RetentionCapture::create(&directory.path.join("stdout"))?;
    let (mut stderr, child_stderr) = RetentionCapture::create(&directory.path.join("stderr"))?;
    let mut cleanup = Vec::with_capacity(12);
    let started = Instant::now();
    let (mut owner, pidfd) = retention_spawn(
        Command::new(env::current_exe()?)
            .args(RETENTION_CHILD_ARGS)
            .env(RETENTION_CHILD_MARKER, RETENTION_CHILD_VERSION)
            .stdin(Stdio::null())
            .stdout(child_stdout)
            .stderr(child_stderr),
    )?;
    let outcome = catch_unwind(AssertUnwindSafe(|| -> TestResult<()> {
        pidfd?;
        loop {
            retention_before_deadline(started.elapsed())?;
            stdout.pump()?;
            stderr.pump()?;
            retention_before_deadline(started.elapsed())?;
            let observation = owner.poll();
            if retention_observed_status(started.elapsed(), observation)? {
                return Ok(());
            }
            std::thread::sleep(RETENTION_POLL_INTERVAL);
        }
    }));
    owner.retire(&mut cleanup);
    if owner.status().is_some() {
        for capture in [&mut stdout, &mut stderr] {
            if let Err(error) = capture.drain_reaped() {
                cleanup.push(error);
            }
        }
    } else {
        cleanup.push(FixtureError::Assertion("retention child reap was not established").into());
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
    let stdout = stdout_text;
    let stderr = stderr_text;
    let primary = match outcome {
        Ok(result) => result.err(),
        Err(payload) => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                eprintln!(
                    "retention child panic cleanup: status={:?} forced={} errors={cleanup:?} stdout={stdout:?} stderr={stderr:?}",
                    owner.status(),
                    owner.forced
                );
            }));
            resume_unwind(payload);
        }
    };
    let primary = retention_parent_failure(
        primary,
        &mut cleanup,
        retention_receipt_valid(
            owner.status().is_some_and(|status| status.success()),
            owner.forced,
            &stdout,
        ),
    );
    if let Some(primary) = primary {
        return Err(FixtureError::RetentionChild {
            primary,
            status: owner.status(),
            forced: owner.forced,
            cleanup,
            stdout,
            stderr,
        }
        .into());
    }
    print!("{stdout}");
    if !stderr.is_empty() {
        eprint!("{stderr}");
    }
    Ok(())
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "requires the unstable `linux_pidfd` API and a jemalloc `mallctl` or sanitizer allocated-bytes symbol that `dlsym` can resolve in the test process"
)]
fn native_selected_workload_retention_control() -> TestResult<()> {
    match env::var_os(RETENTION_CHILD_MARKER) {
        None => retention_parent(),
        Some(marker) => {
            require(
                retention_child_arguments(&marker, env::args_os().skip(1)),
                "retention child marker and exact arguments prohibit recursive or broad execution",
            )?;
            retention_witness()
        }
    }
}

const ENVELOPE_RETENTION_SELECTOR: &str =
    "native_lifecycle_readiness_tests::native_envelope_retention_matrix";
const ENVELOPE_RETENTION_MARKER: &str = "ROXYGEN_NATIVE_ENVELOPE_RETENTION_CHILD";
const ENVELOPE_RETENTION_BUDGET: &str = "ROXYGEN_NATIVE_ENVELOPE_RETENTION_REMAINING_NS";
const ENVELOPE_RETENTION_ARGS: [&str; 4] = [
    ENVELOPE_RETENTION_SELECTOR,
    "--exact",
    "--nocapture",
    "--test-threads=1",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EnvelopeRetentionRoute {
    route: Route,
    marker: &'static str,
    receipt: &'static str,
}

const ENVELOPE_RETENTION_ROUTES: [EnvelopeRetentionRoute; 3] = [
    EnvelopeRetentionRoute {
        route: ROUTES[1],
        marker: "v1-plaintext-h2",
        receipt: "roxygen-native-envelope-retention-v1 route=plaintext-h2 complete capsules=8",
    },
    EnvelopeRetentionRoute {
        route: ROUTES[2],
        marker: "v1-tls-h1",
        receipt: "roxygen-native-envelope-retention-v1 route=tls-h1 complete capsules=8",
    },
    EnvelopeRetentionRoute {
        route: ROUTES[3],
        marker: "v1-tls-h2",
        receipt: "roxygen-native-envelope-retention-v1 route=tls-h2 complete capsules=8",
    },
];

fn envelope_retention_remaining(elapsed: Duration, budget: Duration) -> TestResult<Duration> {
    require(
        !budget.is_zero() && budget <= RETENTION_PROCESS_BOUND,
        "envelope child receives only a positive remaining parent budget",
    )?;
    budget
        .checked_sub(elapsed)
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            FixtureError::Deadline("absolute native envelope retention acceptance budget").into()
        })
}

fn envelope_retention_child_arguments(
    marker: &OsStr,
    budget: &OsStr,
    arguments: impl Iterator<Item = OsString>,
) -> TestResult<(EnvelopeRetentionRoute, Duration)> {
    require(
        arguments.eq(ENVELOPE_RETENTION_ARGS.into_iter().map(OsString::from)),
        "envelope child uses the exact same-binary singleton arguments",
    )?;
    let selected = ENVELOPE_RETENTION_ROUTES
        .into_iter()
        .find(|selected| marker == OsStr::new(selected.marker))
        .ok_or(FixtureError::Assertion(
            "exact versioned envelope route marker",
        ))?;
    let value = budget.to_str().ok_or(FixtureError::Assertion(
        "ASCII decimal remaining envelope budget",
    ))?;
    require(
        !value.is_empty()
            && value.len() <= 12
            && !value.starts_with('0')
            && value.bytes().all(|byte| byte.is_ascii_digit()),
        "bounded canonical positive remaining envelope nanoseconds",
    )?;
    let budget = Duration::from_nanos(value.parse::<u64>()?);
    envelope_retention_remaining(Duration::ZERO, budget)?;
    Ok((selected, budget))
}

fn envelope_retention_observed_status(
    elapsed: Duration,
    observation: io::Result<Option<ExitStatus>>,
) -> TestResult<bool> {
    let status = observation?;
    envelope_retention_remaining(elapsed, RETENTION_PROCESS_BOUND)?;
    match status {
        Some(status) if !status.success() => Err(FixtureError::RetentionExit(status).into()),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

fn envelope_retention_receipt_valid(
    selected: EnvelopeRetentionRoute,
    success: bool,
    forced: bool,
    stdout: &str,
) -> bool {
    success
        && !forced
        && stdout
            .lines()
            .filter(|line| line.starts_with("roxygen-native-"))
            .eq([selected.receipt])
}

fn envelope_retention_controls() -> TestResult<()> {
    let arguments = || ENVELOPE_RETENTION_ARGS.into_iter().map(OsString::from);
    for selected in ENVELOPE_RETENTION_ROUTES {
        for (literal, duration) in [
            ("1", Duration::from_nanos(1)),
            ("900000000000", RETENTION_PROCESS_BOUND),
        ] {
            require(
                envelope_retention_child_arguments(
                    OsStr::new(selected.marker),
                    OsStr::new(literal),
                    arguments(),
                )? == (selected, duration),
                "exact route and canonical remaining budget survive child admission",
            )?;
        }
        for literal in [
            "",
            "0",
            "01",
            "-1",
            "+1",
            " 1",
            "1 ",
            "１",
            "900000000001",
            "1000000000000",
        ] {
            require(
                envelope_retention_child_arguments(
                    OsStr::new(selected.marker),
                    OsStr::new(literal),
                    arguments(),
                )
                .is_err(),
                "invalid or expanded remaining budget cannot admit a child",
            )?;
        }
        require(
            envelope_retention_child_arguments(
                OsStr::new(selected.marker),
                OsStr::new("1"),
                RETENTION_CHILD_ARGS.into_iter().map(OsString::from),
            )
            .is_err()
                && envelope_retention_child_arguments(
                    OsStr::new(selected.marker),
                    OsStr::new("1"),
                    arguments().chain([OsString::from("--ignored")]),
                )
                .is_err(),
            "old selector and additional arguments cannot broaden child execution",
        )?;
        require(
            envelope_retention_receipt_valid(selected, true, false, selected.receipt)
                && !envelope_retention_receipt_valid(selected, false, false, selected.receipt)
                && !envelope_retention_receipt_valid(selected, true, true, selected.receipt)
                && !envelope_retention_receipt_valid(selected, true, false, "")
                && !envelope_retention_receipt_valid(selected, true, false, RETENTION_RECEIPT)
                && !envelope_retention_receipt_valid(
                    selected,
                    true,
                    false,
                    &format!("{}\n{}\n", selected.receipt, selected.receipt),
                ),
            "completion requires an unforced successful child and one exact route receipt",
        )?;
        for other in ENVELOPE_RETENTION_ROUTES {
            if other != selected {
                require(
                    !envelope_retention_receipt_valid(selected, true, false, other.receipt)
                        && !envelope_retention_receipt_valid(
                            selected,
                            true,
                            false,
                            &format!("{}\n{}\n", selected.receipt, other.receipt),
                        ),
                    "a different route or extra route receipt cannot substitute",
                )?;
            }
        }
    }
    for marker in ["", "v1", "v1-plaintext-h1", "v2-tls-h2", "v1-tls-h2 "] {
        require(
            envelope_retention_child_arguments(OsStr::new(marker), OsStr::new("1"), arguments())
                .is_err(),
            "undeclared child marker cannot recurse or choose another route",
        )?;
    }
    let first_elapsed = Duration::from_secs(300);
    let second_elapsed = Duration::from_secs(700);
    require(
        envelope_retention_remaining(first_elapsed, RETENTION_PROCESS_BOUND)?
            == Duration::from_secs(600)
            && envelope_retention_remaining(second_elapsed, RETENTION_PROCESS_BOUND)?
                == Duration::from_secs(200)
            && envelope_retention_remaining(Duration::ZERO, Duration::ZERO).is_err()
            && envelope_retention_remaining(
                Duration::ZERO,
                RETENTION_PROCESS_BOUND + Duration::from_nanos(1),
            )
            .is_err(),
        "later routes receive the original matrix remainder, never a new900 seconds",
    )?;
    let before = RETENTION_PROCESS_BOUND - Duration::from_nanos(1);
    require(
        envelope_retention_remaining(before, RETENTION_PROCESS_BOUND)? == Duration::from_nanos(1)
            && envelope_retention_observed_status(before, Ok(Some(ExitStatus::from_raw(0))))?
            && !envelope_retention_observed_status(before, Ok(None))?,
        "only before-deadline original exit0 can finish observation",
    )?;
    for elapsed in [
        RETENTION_PROCESS_BOUND,
        RETENTION_PROCESS_BOUND + Duration::from_nanos(1),
    ] {
        let failure =
            envelope_retention_observed_status(elapsed, Ok(Some(ExitStatus::from_raw(0))))
                .expect_err("late exit0 must fail");
        let original = failure
            .downcast_ref::<FixtureError>()
            .expect("typed deadline");
        let identity = original as *const FixtureError;
        let mut cleanup = vec![BoxError::from(io::Error::from(io::ErrorKind::BrokenPipe))];
        let retained = retention_parent_failure(Some(failure), &mut cleanup, true)
            .expect("normal cleanup cannot erase deadline");
        require(
            retained
                .downcast_ref::<FixtureError>()
                .is_some_and(|error| std::ptr::eq(error, identity))
                && cleanup.len() == 1
                && envelope_retention_remaining(elapsed, RETENTION_PROCESS_BOUND).is_err()
                && envelope_retention_observed_status(elapsed, Ok(None)).is_err(),
            "deadline identity persists through normal reap and secondary cleanup",
        )?;
    }
    let original = envelope_retention_observed_status(
        RETENTION_PROCESS_BOUND,
        Err(io::Error::from_raw_os_error(libc::EIO)),
    )
    .expect_err("failed original poll retains its error");
    require(
        original
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.raw_os_error() == Some(libc::EIO))
            && envelope_retention_observed_status(before, Ok(Some(ExitStatus::from_raw(1 << 8))))
                .is_err(),
        "original poll error and nonzero status cannot become successful deadline observations",
    )
}

fn envelope_retention_parent_route(
    selected: EnvelopeRetentionRoute,
    started: Instant,
) -> TestResult<()> {
    envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
    let path = env::temp_dir().join(format!(
        "roxygen-native-envelope-retention-{}-{}",
        std::process::id(),
        selected.marker,
    ));
    DirBuilder::new().mode(0o700).create(&path)?;
    let directory = RetentionOutputDirectory { path };
    let (mut stdout, child_stdout) = RetentionCapture::create(&directory.path.join("stdout"))?;
    let (mut stderr, child_stderr) = RetentionCapture::create(&directory.path.join("stderr"))?;
    let mut cleanup = Vec::with_capacity(12);
    let executable = env::current_exe()?;
    let remaining = envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
    let (mut owner, pidfd) = retention_spawn(
        Command::new(executable)
            .args(ENVELOPE_RETENTION_ARGS)
            .env_remove(RETENTION_CHILD_MARKER)
            .env(ENVELOPE_RETENTION_MARKER, selected.marker)
            .env(ENVELOPE_RETENTION_BUDGET, remaining.as_nanos().to_string())
            .stdin(Stdio::null())
            .stdout(child_stdout)
            .stderr(child_stderr),
    )?;
    let outcome = catch_unwind(AssertUnwindSafe(|| -> TestResult<()> {
        pidfd?;
        loop {
            envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
            stdout.pump()?;
            stderr.pump()?;
            envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
            let observation = owner.poll();
            if envelope_retention_observed_status(started.elapsed(), observation)? {
                return Ok(());
            }
            std::thread::sleep(RETENTION_POLL_INTERVAL);
        }
    }));
    owner.retire(&mut cleanup);
    if owner.status().is_some() {
        for capture in [&mut stdout, &mut stderr] {
            if let Err(error) = capture.drain_reaped() {
                cleanup.push(error);
            }
        }
    } else {
        cleanup
            .push(FixtureError::Assertion("envelope child actual reap was not established").into());
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
    let stdout = stdout_text;
    let stderr = stderr_text;
    let primary = match outcome {
        Ok(result) => result.err(),
        Err(payload) => {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                eprintln!(
                    "envelope retention panic cleanup: route={selected:?} status={:?} forced={} errors={cleanup:?} stdout={stdout:?} stderr={stderr:?}",
                    owner.status(),
                    owner.forced,
                );
            }));
            resume_unwind(payload);
        }
    };
    if let Err(error) = envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND) {
        cleanup.push(error);
    }
    let primary = retention_parent_failure(
        primary,
        &mut cleanup,
        envelope_retention_receipt_valid(
            selected,
            owner.status().is_some_and(|status| status.success()),
            owner.forced,
            &stdout,
        ),
    );
    if let Some(primary) = primary {
        return Err(FixtureError::RetentionChild {
            primary,
            status: owner.status(),
            forced: owner.forced,
            cleanup,
            stdout,
            stderr,
        }
        .into());
    }
    print!("{stdout}");
    if !stderr.is_empty() {
        eprint!("{stderr}");
    }
    envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
    Ok(())
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "requires the unstable `linux_pidfd` API and a jemalloc `mallctl` or sanitizer allocated-bytes symbol that `dlsym` can resolve in the test process"
)]
fn native_envelope_retention_matrix() -> TestResult<()> {
    require(
        env::var_os(RETENTION_CHILD_MARKER).is_none(),
        "selected-H1 child marker cannot select an envelope route",
    )?;
    match (
        env::var_os(ENVELOPE_RETENTION_MARKER),
        env::var_os(ENVELOPE_RETENTION_BUDGET),
    ) {
        (None, None) => {
            envelope_retention_controls()?;
            let started = Instant::now();
            for selected in ENVELOPE_RETENTION_ROUTES {
                envelope_retention_parent_route(selected, started).map_err(|error| {
                    FixtureError::Case {
                        route: selected.route,
                        errors: vec![error],
                    }
                })?;
            }
            envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
            println!("roxygen-native-envelope-retention-v1 complete routes=3 capsules_per_route=8");
            envelope_retention_remaining(started.elapsed(), RETENTION_PROCESS_BOUND)?;
            Ok(())
        }
        (Some(marker), Some(budget)) => {
            let started = Instant::now();
            let (selected, budget) =
                envelope_retention_child_arguments(&marker, &budget, env::args_os().skip(1))?;
            retention_route_witness(
                || {
                    envelope_retention_remaining(started.elapsed(), budget)?;
                    let held = retention_route_capsule(selected.route)?;
                    envelope_retention_remaining(started.elapsed(), budget)?;
                    Ok(held)
                },
                selected.receipt,
            )?;
            envelope_retention_remaining(started.elapsed(), budget)?;
            Ok(())
        }
        _ => Err(FixtureError::Assertion(
            "envelope child marker and remaining budget occur together",
        )
        .into()),
    }
}

fn tls_fixture(route: Route) -> TestResult<(TlsServerOptions, Arc<ClientConfig>)> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let der = certificate.cert.der().to_vec();
    let options = TlsServerOptions::new(
        vec![der.clone().into_boxed_slice()],
        certificate
            .signing_key
            .serialized_der()
            .to_vec()
            .into_boxed_slice(),
        vec![route.alpn().into()],
    )?;
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(der))?;
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![route.alpn().to_vec()];
    Ok((options, Arc::new(client)))
}

#[derive(Clone, Copy, Debug)]
struct PressureObservation {
    protocol: Protocol,
    target: usize,
    expected: usize,
    consumed: usize,
    received: usize,
    queued: i64,
    sent: usize,
    wire: usize,
    blocked: bool,
    held: bool,
    valid: bool,
    action: usize,
    done: bool,
    pending: usize,
}

impl PressureObservation {
    fn admitted(self) -> bool {
        let Ok(queued) = usize::try_from(self.queued) else {
            return false;
        };
        self.target == self.expected
            && self.target != 0
            && self.valid
            && self.held
            && self.blocked
            && !self.done
            && self.action == 0
            && self.pending != 0
            && self.consumed == CHUNK
            && self.sent > CHUNK
            && self.sent < PRESSURE_BYTES
            && self.wire > 0
            && self.received <= self.sent
            && self.received.checked_sub(self.consumed) == Some(queued)
            && match self.protocol {
                Protocol::H1 => queued >= PRESSURE_WINDOW,
                Protocol::H2 => self.sent >= PRESSURE_WINDOW && queued >= PRESSURE_WINDOW - CHUNK,
            }
    }
}

fn pressure_observation(
    route: Route,
    target: &PressureTarget,
    upload: &PressureUpload,
) -> TestResult<Option<PressureObservation>> {
    if !target.entered.load(Ordering::Acquire) {
        return Ok(None);
    }
    let queue = target.ledger()?.snapshot();
    require(
        target.ingress_bound.is_some() == (route.protocol == Protocol::H1 && !route.tls)
            && target
                .envelope_bound
                .is_some_and(|bound| bound.route == route),
        "preserved plaintext H1 oracle and actual route-bound ingress envelope",
    )?;
    target.check_ingress(queue, "pressure")?;
    let wire = *upload.counts.lock().expect("pressure wire counts");
    require(
        !queue.invalid && !wire.invalid && !target.invalid.load(Ordering::Acquire),
        "pressure counters remain confirmed",
    )?;
    let received = queue
        .received
        .checked_sub(target.baseline)
        .ok_or(FixtureError::Assertion(
            "monotonic target ingress statistics",
        ))?;
    let wire_bytes = wire
        .written
        .checked_sub(upload.wire_baseline)
        .ok_or(FixtureError::Assertion("monotonic actual wire progress"))?;
    Ok(Some(PressureObservation {
        protocol: route.protocol,
        target: target.id,
        expected: target.id,
        consumed: target.consumed.load(Ordering::Acquire),
        received,
        queued: queue.read,
        sent: upload.offset,
        wire: wire_bytes,
        blocked: upload.blocked,
        held: target.created[1].load(Ordering::Acquire) == 1
            && target
                .dropped
                .iter()
                .all(|count| count.load(Ordering::Acquire) == 0)
            && !target.eom.load(Ordering::Acquire),
        valid: target
            .ingress_bound
            .is_none_or(|bound| queue.ingress_pressure(bound, target.id))
            && target
                .envelope_bound
                .is_some_and(|bound| queue.envelope_pressure(bound)),
        action: target.action.load(Ordering::Acquire),
        done: upload.done,
        pending: target.pending.load(Ordering::Acquire),
    }))
}

#[derive(Clone, Copy, Debug)]
struct PressureCompletionSnapshot {
    phase: &'static str,
    route: Route,
    target: usize,
    entered: bool,
    consumed: usize,
    eom: bool,
    action: usize,
    native_cancelled: bool,
    invalid: bool,
    created: [usize; 3],
    dropped: [usize; 3],
    pending: usize,
    completed: usize,
    ledger_present: Option<bool>,
    baseline: usize,
    queue: Option<PressureQueue>,
    sent: usize,
    done: bool,
    blocked: bool,
    wire_baseline: usize,
    wire: Option<PressureWireCounts>,
}

#[derive(Debug, Error)]
#[error(
    "{source}; native-pressure completion-failure {snapshot:?}; {checkpoints:?}; socket-profile={socket_profile:?}"
)]
struct PressureCompletionFailure {
    #[source]
    source: FixtureError,
    snapshot: PressureCompletionSnapshot,
    checkpoints: PressureCheckpoints,
    socket_profile: Option<SocketProfilePair>,
}

#[derive(Clone, Copy, Debug, Default)]
struct PressureCheckpoints {
    admitted: Option<PressureObservation>,
    before_recheck: Option<PressureCompletionSnapshot>,
}

fn pressure_completion_diagnostic(
    phase: &'static str,
    route: Route,
    target: &PressureTarget,
    upload: &PressureUpload,
) -> PressureCompletionSnapshot {
    let ledger = target.ledger.try_lock().ok().map(|ledger| ledger.clone());
    let ledger_present = ledger.as_ref().map(Option::is_some);
    let queue = ledger
        .as_ref()
        .and_then(Option::as_ref)
        .and_then(|ledger| ledger.queue.try_lock().ok().map(|queue| *queue));
    let wire = upload.counts.try_lock().ok().map(|counts| *counts);
    let created: [usize; 3] =
        std::array::from_fn(|index| target.created[index].load(Ordering::Acquire));
    let dropped: [usize; 3] =
        std::array::from_fn(|index| target.dropped[index].load(Ordering::Acquire));
    PressureCompletionSnapshot {
        phase,
        route,
        target: target.id,
        entered: target.entered.load(Ordering::Acquire),
        consumed: target.consumed.load(Ordering::Acquire),
        eom: target.eom.load(Ordering::Acquire),
        action: target.action.load(Ordering::Acquire),
        native_cancelled: target.native_cancelled.load(Ordering::Acquire),
        invalid: target.invalid.load(Ordering::Acquire),
        created,
        dropped,
        pending: target.pending.load(Ordering::Acquire),
        completed: target.completed.load(Ordering::Acquire),
        ledger_present,
        baseline: target.baseline,
        queue,
        sent: upload.offset,
        done: upload.done,
        blocked: upload.blocked,
        wire_baseline: upload.wire_baseline,
        wire,
    }
}

fn poll_held_upload(
    mut upload: Pin<&mut PressureUpload>,
    context: &mut Context<'_>,
    phase: &'static str,
    route: Route,
    target: &PressureTarget,
) -> Poll<TestResult<()>> {
    match upload.as_mut().poll(context) {
        Poll::Ready(Ok(())) => {
            let source = FixtureError::Assertion("upload completed without required pressure");
            let snapshot = pressure_completion_diagnostic(phase, route, target, &upload);
            let checkpoints = upload.checkpoints;
            let socket_profile = upload.socket_profile;
            Poll::Ready(Err(PressureCompletionFailure {
                source,
                snapshot,
                checkpoints,
                socket_profile,
            }
            .into()))
        }
        other => other,
    }
}

async fn pressure_barrier(
    route: Route,
    target: &PressureTarget,
    mut upload: Pin<&mut PressureUpload>,
    phase: &'static str,
) -> TestResult<()> {
    within(
        "actual target queue and wire pressure",
        poll_fn(|context| {
            target.changed.register(context.waker());
            if let Poll::Ready(result) =
                poll_held_upload(upload.as_mut(), context, phase, route, target)
            {
                return Poll::Ready(result);
            }
            match pressure_observation(route, target, &upload) {
                Ok(Some(observed)) if observed.admitted() => {
                    if phase == "initial" {
                        upload.as_mut().get_mut().checkpoints.admitted = Some(observed);
                    }
                    Poll::Ready(Ok(()))
                }
                Ok(_) => Poll::Pending,
                Err(error) => Poll::Ready(Err(error)),
            }
        }),
    )
    .await?
}

async fn pressure_probe(
    endpoint: &Endpoint,
    healthy: &mut Peer,
    target: &PressureTarget,
    mut upload: Pin<&mut PressureUpload>,
    addresses: PeerAddresses,
) -> TestResult<()> {
    let path = format!("/pressure-pending/{}", target.id);
    let mut probe = Box::pin(healthy.exchange(endpoint.route, &path));
    within(
        "healthy probe while independent upload stays driven",
        poll_fn(|context| {
            if let Poll::Ready(result) =
                poll_held_upload(upload.as_mut(), context, "probe", endpoint.route, target)
            {
                return Poll::Ready(result);
            }
            probe.as_mut().poll(context)
        }),
    )
    .await??;
    drop(probe);
    let (target_context, target_stream) = endpoint
        .shared
        .request(&format!("/pressure/{}", target.id), addresses)?;
    let (probe_context, probe_stream) = endpoint.shared.request(&path, healthy.addresses)?;
    require(
        Arc::ptr_eq(&target_context, &target.ledger()?.context),
        "target source and queue recorder share actual identity",
    )?;
    match endpoint.route.protocol {
        Protocol::H1 => require(
            !Arc::ptr_eq(&target_context, &probe_context)
                && addresses.local != healthy.addresses.local,
            "healthy H1 probe uses a distinct accepted connection",
        )?,
        Protocol::H2 => require(
            Arc::ptr_eq(&target_context, &probe_context)
                && target_stream.is_some()
                && probe_stream.is_some()
                && target_stream != probe_stream,
            "healthy H2 probe uses a sibling stream on the pressured connection",
        )?,
    }
    let before_recheck =
        pressure_completion_diagnostic("before-recheck", endpoint.route, target, &upload);
    upload.as_mut().get_mut().checkpoints.before_recheck = Some(before_recheck);
    pressure_barrier(endpoint.route, target, upload, "recheck").await?;
    require(
        target.pending.load(Ordering::Acquire) == 2,
        "pressure rechecked after an actual healthy pending-receipt probe",
    )
}

async fn pressure_retired(
    endpoint: &Endpoint,
    healthy: &mut Peer,
    target: &PressureTarget,
    server: &HttpServer,
) -> TestResult<()> {
    healthy
        .exchange(endpoint.route, &format!("/pressure-retired/{}", target.id))
        .await?;
    let ledger = target.ledger()?;
    within(
        "pressure queue reconciliation after actual receipt",
        poll_fn(|context| {
            target.changed.register(context.waker());
            let queue = ledger.snapshot();
            if queue.invalid {
                Poll::Ready(Err(FixtureError::Assertion(
                    "invalid retired pressure queue",
                )))
            } else if queue.read == 0 {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }),
    )
    .await??;
    target.retired()?;
    if target.ingress_bound.is_some() || target.envelope_bound.is_some() {
        let queue = ledger.snapshot();
        target.check_ingress(queue, "live-worker zero")?;
        let recovery = H1IngressRecovery {
            queue,
            running: server.state() == HttpServerState::Running
                && server.control().state() == HttpServerState::Running,
            starts: endpoint.shared.starts.load(Ordering::Acquire),
            stops: endpoint.shared.stops.load(Ordering::Acquire),
        };
        require(
            target
                .ingress_bound
                .is_none_or(|bound| recovery.admitted(bound, target.id))
                && target
                    .envelope_bound
                    .is_none_or(|bound| recovery.envelope_admitted(bound)),
            "zero target BODY credit after actual receipt, before native worker drain",
        )?;
        let mut slot = target
            .ingress_recovery
            .lock()
            .expect("BODY-credit live recovery");
        require(
            slot.is_none(),
            "one retained live-worker zero checkpoint per target",
        )?;
        *slot = Some(recovery);
    }
    healthy
        .exchange(
            endpoint.route,
            &format!("/context/pressure-after-{}", target.id),
        )
        .await
}

async fn pressure_matrix(
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
    server: &HttpServer,
) -> TestResult<()> {
    let state = endpoint
        .shared
        .pressure
        .as_ref()
        .expect("pressure exercise");
    let h2_wire = Arc::new(Mutex::new(PressureWireCounts::default()));
    let mut healthy = if endpoint.route.protocol == Protocol::H2 {
        let (io, addresses) = endpoint.pressure_connect(Arc::clone(&h2_wire)).await?;
        require(
            drivers.len() < MAX_DRIVERS,
            "bounded pressure h2 driver ownership",
        )?;
        let (sender, connection) =
            within("pressure h2 handshake", h2::client::handshake(io)).await??;
        drivers.spawn(async move {
            connection.await?;
            Ok(())
        });
        Peer {
            wire: PeerWire::H2(sender),
            addresses,
        }
    } else {
        endpoint.peer(drivers).await?
    };
    for _ in 0..WARMUPS {
        let warmup = start_target(endpoint, &mut healthy, Stage::Complete).await?;
        finish_target(endpoint, &mut healthy, warmup).await?;
    }
    let h2_ledger = if endpoint.route.protocol == Protocol::H2 {
        let (context, _) = endpoint
            .shared
            .request(&format!("/retired/{WARMUPS}"), healthy.addresses)?;
        Some(state.ledger(&context)?)
    } else {
        None
    };
    for cancel in [false, true] {
        let baseline = if let Some(ledger) = &h2_ledger {
            let queue = ledger.snapshot();
            require(
                !queue.invalid && queue.read == 0,
                "healthy zero-ingress H2 baseline before each target",
            )?;
            queue.received
        } else {
            0
        };
        let (target, action) = state.install(baseline)?;
        let path = format!("/pressure/{}", target.id);
        let (wire, counts, addresses) = match &mut healthy.wire {
            PeerWire::H1(_) => {
                let counts = Arc::new(Mutex::new(PressureWireCounts::default()));
                let (mut io, addresses) = endpoint.pressure_connect(Arc::clone(&counts)).await?;
                state
                    .bind_ingress_target(&endpoint.shared, &target, addresses)
                    .await?;
                h1_request(&mut io, &path, PRESSURE_BYTES, b"").await?;
                (PressureWire::H1(io), counts, addresses)
            }
            PeerWire::H2(sender) => {
                state
                    .bind_ingress_target(&endpoint.shared, &target, healthy.addresses)
                    .await?;
                let exchange =
                    h2_request(sender, endpoint.route, &path, PRESSURE_BYTES, b"").await?;
                (
                    PressureWire::H2(exchange),
                    Arc::clone(&h2_wire),
                    healthy.addresses,
                )
            }
        };
        let socket_profile = if endpoint.route.protocol == Protocol::H1 {
            Some(state.paired_socket_profile(target.id, addresses).await?)
        } else {
            None
        };
        let mut upload = Box::pin(PressureUpload::new(wire, counts, socket_profile));
        pressure_barrier(endpoint.route, &target, upload.as_mut(), "initial").await?;
        if let Some(pair) = socket_profile {
            require(
                target.ledger()?.socket_profile == Some(pair.receiver),
                "body target binds the frozen socket profile before healthy progress",
            )?;
        }
        pressure_probe(endpoint, &mut healthy, &target, upload.as_mut(), addresses).await?;
        let selected = if !cancel {
            PressureAction::Release
        } else if endpoint.route.protocol == Protocol::H1 {
            PressureAction::NativeCancel
        } else {
            PressureAction::PeerCancel
        };
        target.action.store(selected as usize, Ordering::Release);
        match selected {
            PressureAction::Release => {
                action
                    .send(selected)
                    .map_err(|_| FixtureError::Assertion("native pressure release owner alive"))?;
                within("released independent pressure upload", upload.as_mut()).await??;
                require(
                    upload.done && upload.offset == PRESSURE_BYTES,
                    "released writer resumes through exact upload EOM",
                )?;
                match upload.wire.take().expect("released wire owner") {
                    PressureWire::H1(mut io) => {
                        within("released H1 response head", h1_head(&mut io, HEALTH.len()))
                            .await??;
                        let mut body = [0; HEALTH.len()];
                        within("released H1 response body", io.read_exact(&mut body)).await??;
                        require(body.as_slice() == HEALTH, "exact released H1 response")?;
                        within("released H1 shutdown", io.shutdown()).await??;
                    }
                    PressureWire::H2(mut exchange) => {
                        let response = within(
                            "released H2 response head",
                            exchange.response.take().expect("response owner"),
                        )
                        .await??;
                        h2_head(&response, HEALTH.len())?;
                        within(
                            "released H2 response EOF",
                            h2_body(&mut response.into_body(), HEALTH, true),
                        )
                        .await??;
                    }
                }
            }
            PressureAction::NativeCancel => {
                action.send(selected).map_err(|_| {
                    FixtureError::Assertion("native pressure cancellation owner alive")
                })?;
                within(
                    "worker applied target-native cancellation",
                    poll_fn(|context| {
                        target.changed.register(context.waker());
                        if target.native_cancelled.load(Ordering::Acquire) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }),
                )
                .await?;
                let PressureWire::H1(io) = upload.wire.take().expect("cancelled H1 wire owner")
                else {
                    return Err(FixtureError::Assertion("native cancellation route is H1").into());
                };
                io.reset()?;
            }
            PressureAction::PeerCancel => {
                let PressureWire::H2(mut exchange) =
                    upload.wire.take().expect("cancelled H2 stream owner")
                else {
                    return Err(FixtureError::Assertion("peer cancellation route is H2").into());
                };
                let stream = StreamId::new(u64::from(exchange.stream.stream_id().as_u32()));
                exchange.stream.send_reset(h2::Reason::CANCEL);
                let result = within(
                    "actual peer-cancelled H2 response",
                    exchange.response.take().expect("response owner"),
                )
                .await?;
                let error = result.err().ok_or(FixtureError::Assertion(
                    "peer-cancelled H2 target cannot respond",
                ))?;
                require(
                    error.is_reset() && error.reason() == Some(h2::Reason::CANCEL),
                    "actual H2 target reset result",
                )?;
                pressure_retired(endpoint, &mut healthy, &target, server).await?;
                let resets = endpoint.shared.resets.lock().expect("native resets");
                require(
                    resets
                        .iter()
                        .filter(|(observed, code)| {
                            *observed == stream && *code == HttpErrorCode::CANCEL
                        })
                        .count()
                        == 1,
                    "native session observed exactly one peer CANCEL on the pressured stream",
                )?;
                drop(resets);
                drop(action);
                drop(upload);
                continue;
            }
        }
        drop(upload);
        pressure_retired(endpoint, &mut healthy, &target, server).await?;
    }
    healthy.close().await
}

#[derive(Clone, Copy, Debug)]
struct EgressObservation {
    prefix: usize,
    peer_bytes: usize,
    baseline: usize,
    produced: usize,
    delivered: usize,
    capacity: usize,
    pending: bool,
    producer_done: bool,
    source_created: usize,
    source_drops: usize,
    terminal: bool,
    stopped: bool,
    action: usize,
    invalid: bool,
}

fn egress_pressure_held(observed: EgressObservation) -> bool {
    observed.prefix > 0
        && observed.prefix < EGRESS_BYTES
        && observed.peer_bytes == observed.prefix
        && observed.produced > observed.baseline
        && observed.produced < EGRESS_BYTES
        && observed.delivered > observed.prefix
        && observed.delivered <= observed.produced
        && observed
            .produced
            .checked_sub(observed.delivered)
            .is_some_and(|queued| queued >= EGRESS_SLOTS * CHUNK)
        && observed.capacity == 0
        && observed.pending
        && !observed.producer_done
        && observed.source_created == 1
        && observed.source_drops == 0
        && !observed.terminal
        && !observed.stopped
        && observed.action == 0
        && !observed.invalid
}

impl EgressProducer {
    fn snapshot(&self, baseline: usize) -> EgressObservation {
        let target = &self.evidence;
        EgressObservation {
            prefix: target.prefix.load(Ordering::Acquire),
            peer_bytes: target.peer_bytes.load(Ordering::Acquire),
            baseline,
            produced: target.produced.load(Ordering::Acquire),
            delivered: target.delivered.load(Ordering::Acquire),
            capacity: self.sender.capacity(),
            pending: self.pending,
            producer_done: target
                .producer_result
                .lock()
                .expect("egress result")
                .is_some(),
            source_created: target.source_created.load(Ordering::Acquire),
            source_drops: target.source_drops.load(Ordering::Acquire),
            terminal: target.terminal.load(Ordering::Acquire),
            stopped: !target.stops.lock().expect("egress stops").is_empty(),
            action: target.action.load(Ordering::Acquire),
            invalid: target.invalid.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Error)]
#[error("egress pressure {phase}: {observation:?}: {source}")]
struct EgressPressureFailure {
    phase: &'static str,
    observation: EgressObservation,
    #[source]
    source: BoxError,
}

async fn egress_barrier(
    producer: &mut EgressProducer,
    baseline: usize,
    phase: &'static str,
) -> TestResult<EgressObservation> {
    let result = within(
        "real pending egress admission",
        poll_fn(|context| {
            producer.evidence.changed.register(context.waker());
            producer.poll(context);
            let observed = producer.snapshot(baseline);
            if let Some(error) = producer.unexpected_error() {
                Poll::Ready(Err(error))
            } else if observed.producer_done
                || observed.source_drops != 0
                || observed.terminal
                || observed.stopped
                || observed.invalid
                || observed.action != 0
            {
                Poll::Ready(Err(FixtureError::Assertion(
                    "response completed, stopped or invalid before pressure action",
                )
                .into()))
            } else if egress_pressure_held(observed) {
                Poll::Ready(Ok(observed))
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .and_then(|result| result);
    result.map_err(|source| {
        EgressPressureFailure {
            phase,
            observation: producer.snapshot(baseline),
            source,
        }
        .into()
    })
}

fn check_egress_bytes(bytes: &[u8], offset: usize) -> TestResult<()> {
    require(
        !bytes.is_empty()
            && offset
                .checked_add(bytes.len())
                .is_some_and(|end| end <= EGRESS_BYTES)
            && bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == ((offset + index) % 251) as u8),
        "exact ordered egress response bytes",
    )
}

async fn egress_prefix(wire: &mut TargetWire) -> TestResult<usize> {
    match wire {
        TargetWire::H1(io) => {
            h1_head(io, EGRESS_BYTES).await?;
            let mut bytes = [0; CHUNK];
            io.read_exact(&mut bytes).await?;
            check_egress_bytes(&bytes, 0)?;
            Ok(bytes.len())
        }
        TargetWire::H2(exchange) => {
            let response = exchange
                .response
                .take()
                .expect("egress response future")
                .await?;
            h2_head(&response, EGRESS_BYTES)?;
            let mut body = response.into_body();
            let bytes = body
                .data()
                .await
                .ok_or(FixtureError::Assertion("real positive H2 DATA prefix"))??;
            check_egress_bytes(&bytes, 0)?;
            require(
                bytes.len() <= EGRESS_STREAM_WINDOW as usize,
                "bounded H2 prefix",
            )?;
            body.flow_control().release_capacity(bytes.len())?;
            let length = bytes.len();
            exchange.body = Some(body);
            Ok(length)
        }
    }
}

async fn egress_release(
    wire: TargetWire,
    route: Route,
    addresses: PeerAddresses,
    evidence: &Arc<EgressEvidence>,
) -> TestResult<()> {
    let mut offset = evidence.peer_bytes.load(Ordering::Acquire);
    match wire {
        TargetWire::H1(mut io) => {
            let mut bytes = [0; CHUNK];
            while offset < EGRESS_BYTES {
                let amount = CHUNK.min(EGRESS_BYTES - offset);
                io.read_exact(&mut bytes[..amount]).await?;
                check_egress_bytes(&bytes[..amount], offset)?;
                offset += amount;
                evidence.peer_bytes.store(offset, Ordering::Release);
            }
            let mut completed = Peer {
                wire: PeerWire::H1(io),
                addresses,
            };
            completed
                .exchange(route, &format!("/context/egress-tail-{}", evidence.id))
                .await?;
            completed.close().await?;
        }
        TargetWire::H2(mut exchange) => {
            let body = exchange.body.as_mut().expect("retained egress H2 body");
            while let Some(data) = body.data().await {
                let bytes = data?;
                check_egress_bytes(&bytes, offset)?;
                offset += bytes.len();
                evidence.peer_bytes.store(offset, Ordering::Release);
                body.flow_control().release_capacity(bytes.len())?;
            }
            require(
                body.trailers().await?.is_none(),
                "no undeclared egress H2 trailers",
            )?;
        }
    }
    require(
        offset == EGRESS_BYTES,
        "complete declared response and terminal framing",
    )
}

async fn egress_cancel(
    wire: TargetWire,
    evidence: &Arc<EgressEvidence>,
) -> TestResult<Option<StreamId>> {
    match wire {
        TargetWire::H1(io) => {
            io.reset()?;
            Ok(None)
        }
        TargetWire::H2(mut exchange) => {
            let stream = StreamId::new(u64::from(exchange.stream.stream_id().as_u32()));
            exchange.stream.send_reset(h2::Reason::CANCEL);
            let body = exchange.body.as_mut().expect("cancelled egress body owner");
            let mut offset = evidence.peer_bytes.load(Ordering::Acquire);
            loop {
                match body.data().await {
                    Some(Ok(bytes)) => {
                        check_egress_bytes(&bytes, offset)?;
                        offset += bytes.len();
                        require(
                            offset
                                <= evidence.prefix.load(Ordering::Acquire)
                                    + EGRESS_STREAM_WINDOW as usize,
                            "only bounded already-buffered DATA after CANCEL",
                        )?;
                        evidence.peer_bytes.store(offset, Ordering::Release);
                    }
                    Some(Err(error)) => {
                        if !error.is_reset() || error.reason() != Some(h2::Reason::CANCEL) {
                            return Err(error.into());
                        }
                        *evidence.peer_reset.lock().expect("actual egress reset") = Some(error);
                        break;
                    }
                    None => {
                        return Err(FixtureError::Assertion(
                            "cancelled pressured response cannot finish normally",
                        )
                        .into());
                    }
                }
            }
            Ok(Some(stream))
        }
    }
}

async fn egress_matrix(
    endpoint: &Endpoint,
    drivers: &mut JoinSet<TestResult<()>>,
) -> TestResult<()> {
    let state = endpoint.shared.egress.as_ref().expect("egress exercise");
    let mut healthy = endpoint.peer(drivers).await?;
    healthy
        .exchange(endpoint.route, "/context/egress-warm")
        .await?;
    for cancel in [false, true] {
        let mut producer = state.install()?;
        let evidence = Arc::clone(&producer.evidence);
        let path = format!("/egress/{}", evidence.id);
        let (mut wire, addresses) = match &mut healthy.wire {
            PeerWire::H1(_) => {
                let (mut io, addresses) = endpoint.connect().await?;
                h1_request(&mut io, &path, 0, b"").await?;
                (TargetWire::H1(io), addresses)
            }
            PeerWire::H2(sender) => (
                TargetWire::H2(h2_request(sender, endpoint.route, &path, 0, b"").await?),
                healthy.addresses,
            ),
        };
        let (prefix, baseline) = producer.prefix(egress_prefix(&mut wire)).await?;
        evidence.prefix.store(prefix, Ordering::Release);
        evidence.peer_bytes.store(prefix, Ordering::Release);
        let initial = egress_barrier(&mut producer, baseline, "initial").await?;
        let probe = format!("/egress-pending/{}", evidence.id);
        producer
            .during(healthy.exchange(endpoint.route, &probe))
            .await?;
        let (target_context, target_stream) = endpoint.shared.request(&path, addresses)?;
        let (probe_context, probe_stream) = endpoint.shared.request(&probe, healthy.addresses)?;
        require(
            if endpoint.route.protocol == Protocol::H2 {
                Arc::ptr_eq(&target_context, &probe_context)
                    && target_stream.is_some()
                    && probe_stream.is_some()
                    && target_stream != probe_stream
            } else {
                !Arc::ptr_eq(&target_context, &probe_context)
                    && addresses.local != healthy.addresses.local
            },
            "healthy progress is an independent H1 connection or same-connection H2 sibling",
        )?;
        let recheck = egress_barrier(&mut producer, baseline, "recheck").await?;
        require(
            initial.prefix == recheck.prefix
                && initial.peer_bytes == recheck.peer_bytes
                && evidence.pending_receipts.load(Ordering::Acquire) == 1,
            "reader remains held through healthy progress and pending receipt recheck",
        )?;
        evidence
            .action
            .store(if cancel { 2 } else { 1 }, Ordering::Release);
        let reset = if cancel {
            producer.during(egress_cancel(wire, &evidence)).await?
        } else {
            producer
                .during(egress_release(wire, endpoint.route, addresses, &evidence))
                .await?;
            None
        };
        producer
            .during(healthy.exchange(endpoint.route, &format!("/egress-retired/{}", evidence.id)))
            .await?;
        producer.finish().await?;
        if let Some(stream) = reset {
            require(
                endpoint
                    .shared
                    .resets
                    .lock()
                    .expect("native egress resets")
                    .iter()
                    .filter(|entry| **entry == (stream, HttpErrorCode::CANCEL))
                    .count()
                    == 1,
                "native observed the actual target-stream peer CANCEL exactly once",
            )?;
        }
        evidence.retired(endpoint.route)?;
        require(
            evidence.peer_bytes.load(Ordering::Acquire) == EGRESS_BYTES || cancel,
            "released peer verifies all response bytes",
        )?;
    }
    healthy.close().await
}

async fn join_drivers(drivers: &mut JoinSet<TestResult<()>>, errors: &mut Vec<BoxError>) {
    let result = within("independent connection driver joins", async {
        while let Some(result) = drivers.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.into()),
            }
        }
    })
    .await;
    if let Err(error) = result {
        errors.push(error);
        drivers.abort_all();
        while let Some(result) = drivers.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.into()),
            }
        }
    }
}

fn force_stop_after_failure(server: &HttpServer, errors: &mut Vec<BoxError>) {
    if server.state() == HttpServerState::Stopped {
        return;
    }
    match server.force_stop() {
        Ok(true) => {}
        Ok(false) => errors.push(
            FixtureError::Assertion("failure cleanup performed native physical joins").into(),
        ),
        Err(error) => errors.push(error.into()),
    }
}

#[derive(Clone, Copy, Debug)]
enum Exercise {
    Context,
    Cancellation,
    PrematureRetirementControl,
    Resources,
    Pressure,
    ResponseBacking,
    Egress,
    SelectedRetention,
}

fn run_case(route: Route, exercise: Exercise) -> TestResult<()> {
    run_case_observed(route, exercise).map(|_| ())
}

fn run_case_observed(route: Route, exercise: Exercise) -> TestResult<Option<RetentionHeld>> {
    let retention = matches!(exercise, Exercise::SelectedRetention);
    if !retention {
        eprintln!("native-lifecycle begin {route:?} {exercise:?}");
    }
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let mut config = ServerConfig {
        plaintext_protocol: route.codec(),
        num_io_threads: 1,
        command_capacity: 16,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    let client_tls = if route.tls {
        let (server, client) = tls_fixture(route)?;
        config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(server)));
        Some(client)
    } else {
        None
    };
    let mut shared = Shared::new(route);
    if matches!(exercise, Exercise::Egress) {
        Arc::get_mut(&mut shared)
            .expect("unshared egress fixture setup")
            .egress = Some(Arc::new(EgressState::new()));
    }
    if matches!(exercise, Exercise::Pressure) {
        Arc::get_mut(&mut shared)
            .expect("unshared fixture setup")
            .pressure = Some(Arc::new(PressureState::configured(route, &config)?));
    }
    if matches!(
        exercise,
        Exercise::ResponseBacking | Exercise::SelectedRetention
    ) {
        if matches!(exercise, Exercise::ResponseBacking) {
            require(
                route.protocol == Protocol::H1 && !route.tls,
                "backing control is plaintext H1 only",
            )?;
        }
        Arc::get_mut(&mut shared)
            .expect("unshared backing fixture setup")
            .backing = Some(Arc::new(BackingTotals::default()));
    }
    let backing_totals = shared.backing.as_ref().map(Arc::clone);
    config.connection_observer = Some(Arc::clone(&shared) as Arc<dyn AcceptedConnectionObserver>);
    config.session.stream_read_timeout = NATIVE_TIMEOUT;
    config.session.connection_idle_timeout = NATIVE_TIMEOUT;
    config.session.write_timeout = NATIVE_TIMEOUT;
    if let Some(pressure) = &shared.pressure {
        let http2 = crate::acceptor::http2_session_config(&config.session);
        require(
            config.session.stream_flow_control == 256 * 1024
                && http2.stream_receive_window == 256 * 1024
                && http2.connection_receive_window == 20 * 256 * 1024,
            "pressure oracle binds unchanged native queue and window defaults",
        )?;
        let pressure = Arc::clone(pressure);
        config.session.stats_factory =
            Some(Arc::new(move |connection| pressure.recorder(connection)));
    }
    if matches!(exercise, Exercise::Pressure) && route.protocol == Protocol::H1 {
        config.session.accepted_socket_profile = Some(Arc::new({
            let shared = Arc::clone(&shared);
            move |socket| {
                shared.check_owner();
                let pressure = shared.pressure.as_ref().expect("H1 pressure profile state");
                let result = socket_profile(socket, SocketProfileDirection::Receiver)
                    .and_then(|receipt| pressure.record_socket_profile(receipt));
                result.map_err(|error| {
                    let native = shared.fail(error);
                    pressure.fail_socket_profile();
                    native
                })
            }
        }));
    }
    config.session.lifecycle_observer_factory = Some(Arc::new({
        let shared = Arc::clone(&shared);
        move || Rc::new(SessionObserver(Arc::clone(&shared))) as Rc<dyn LifecycleObserver>
    }));
    if matches!(exercise, Exercise::Egress) && route.protocol == Protocol::H1 {
        config.session.accepted_socket_profile = Some(Arc::new({
            let shared = Arc::clone(&shared);
            move |socket| {
                shared.check_owner();
                socket_profile(socket, SocketProfileDirection::Sender)
                    .and_then(|receipt| {
                        shared
                            .egress
                            .as_ref()
                            .expect("egress profile")
                            .profile(receipt)
                    })
                    .map_err(|error| shared.fail(error))
            }
        }));
    }
    let factory: HandlerFactory = Arc::new({
        let shared = Arc::clone(&shared);
        move |_| {
            Rc::new(NativeHandler {
                shared: Arc::clone(&shared),
                receipt: Rc::new(RefCell::new(None)),
                pressure_receipt: shared
                    .pressure
                    .as_ref()
                    .map(|_| Rc::new(RefCell::new(None))),
                backing_alias: shared.backing.as_ref().map(|_| Rc::new(RefCell::new(None))),
                egress_receipt: shared.egress.as_ref().map(|_| Rc::new(RefCell::new(None))),
            }) as HttpHandlerRef
        }
    });
    let resources = matches!(exercise, Exercise::Resources | Exercise::ResponseBacking);
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut drivers = JoinSet::new();
    let mut errors = Vec::with_capacity(MAX_REQUESTS);
    let mut retention_held = None;
    if resources {
        record_sample(&mut samples, sample(Checkpoint::R0, None, None)?)?;
    }
    if let Some(pressure) = &shared.pressure {
        require(
            pressure.envelope_bound == Some(EnvelopeIngressBound::from_config(route, &config)?),
            "recorder envelope matches the final configuration consumed by HttpServer::start",
        )?;
    }
    let server = HttpServer::start(
        config,
        factory,
        Some(Arc::clone(&shared) as Arc<dyn ServerObserver>),
    )?;
    let control = server.control();
    let endpoint = server.address().map(|address| Endpoint {
        address,
        route,
        tls: client_tls,
        shared: Arc::clone(&shared),
    });
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let endpoint = endpoint
            .as_ref()
            .ok_or(FixtureError::Assertion("one native listener address"))?;
        match exercise {
            Exercise::Context => drive(&runtime, context_matrix(endpoint, &mut drivers)),
            Exercise::Cancellation => drive(&runtime, async {
                let mut healthy = endpoint.peer(&mut drivers).await?;
                for stage in CANCEL_STAGES {
                    let target = start_target(endpoint, &mut healthy, stage).await?;
                    finish_target(endpoint, &mut healthy, target).await?;
                }
                healthy.close().await
            }),
            Exercise::PrematureRetirementControl => drive(
                &runtime,
                premature_retirement_negative_control(endpoint, &mut drivers),
            ),
            Exercise::Resources | Exercise::ResponseBacking => {
                resource_series(&runtime, &server, endpoint, &mut drivers, &mut samples)
            }
            Exercise::SelectedRetention => {
                retention_held = Some(selected_retention_pair(
                    &runtime,
                    &server,
                    endpoint,
                    &mut drivers,
                )?);
                Ok(())
            }
            Exercise::Pressure => runtime.block_on(async {
                timeout(
                    SERIES_BUDGET,
                    pressure_matrix(endpoint, &mut drivers, &server),
                )
                .await
                .map_err(|_| FixtureError::Deadline("pressure route budget"))?
            }),
            Exercise::Egress => runtime.block_on(async {
                timeout(SERIES_BUDGET, egress_matrix(endpoint, &mut drivers))
                    .await
                    .map_err(|_| FixtureError::Deadline("egress route budget"))?
            }),
        }
    }));
    let mut panic = None;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => errors.push(error),
        Err(payload) => panic = Some(payload),
    }
    if !errors.is_empty() || panic.is_some() {
        force_stop_after_failure(&server, &mut errors);
    }
    runtime.block_on(join_drivers(&mut drivers, &mut errors));
    if !errors.is_empty() || panic.is_some() {
        force_stop_after_failure(&server, &mut errors);
    }
    if server.state() != HttpServerState::Stopped {
        if !server.drain() {
            errors.push(FixtureError::Assertion("root graceful drain requested").into());
        }
        match server.wait_for_stop() {
            Ok(true) => {}
            Ok(false) => {
                errors.push(FixtureError::Assertion("root performed native physical joins").into())
            }
            Err(error) => errors.push(error.into()),
        }
    }
    if let Err(error) = require(
        server.state() == HttpServerState::Stopped && control.state() == HttpServerState::Stopped,
        "root and retained control report checked native stop",
    ) {
        errors.push(error);
    }
    let joined = shared.joined();
    let joined = if matches!(exercise, Exercise::PrematureRetirementControl) {
        require_premature_retirement_rejection(joined, 1, Stage::Response)
    } else {
        joined
    };
    if let Err(error) = joined {
        errors.push(error);
    }
    if let Some(pressure) = &shared.pressure
        && let Err(error) =
            pressure.joined(shared.accepted.lock().expect("accepted metadata").len())
    {
        errors.push(error);
    }
    if !retention {
        shared.report_targets();
    }
    if let Some(egress) = &shared.egress {
        if let Err(error) = egress.joined(
            route,
            shared.accepted.lock().expect("accepted metadata").len(),
        ) {
            errors.push(error);
        }
        for target in egress.targets.lock().expect("egress targets").iter() {
            eprintln!(
                "native-egress {route:?} target={} produced={} delivered={} prefix={} action={} source_stops={:?} producer_result={:?} peer_reset={:?} socket_pair={:?}",
                target.id,
                target.produced.load(Ordering::Acquire),
                target.delivered.load(Ordering::Acquire),
                target.prefix.load(Ordering::Acquire),
                target.action.load(Ordering::Acquire),
                target.stops.lock().expect("egress stops"),
                target.producer_result.lock().expect("egress result"),
                target.peer_reset.lock().expect("egress reset"),
                target.socket_pair.lock().expect("egress pair")
            );
        }
    }
    if let Some(pressure) = &shared.pressure {
        pressure.report_socket_profiles(route);
    }
    errors.append(&mut *shared.errors.lock().expect("native fixture failures"));
    let worker = shared
        .owner
        .lock()
        .expect("native worker identity")
        .map(|identity| identity.linux);
    let retained = shared
        .accepted
        .lock()
        .expect("accepted metadata")
        .first()
        .map(|entry| Arc::clone(&entry.context));
    if let Some(context) = retained.as_ref() {
        if let Err(error) = require(
            context.security() == route.security()
                && context.peer_identity().is_none()
                && context.upstream_origin().is_none(),
            "retained immutable metadata remains readable after physical joins",
        ) {
            errors.push(error);
        }
    }
    if resources {
        match sample(Checkpoint::R4, None, worker) {
            Ok(observed) => {
                if let Err(error) = require(
                    !observed.worker_present
                        && observed.fd_count <= samples[0].fd_count
                        && observed.thread_count <= samples[0].thread_count,
                    "post-join FD/thread footprint with caller runtime still alive",
                ) {
                    errors.push(error);
                }
                if let Err(error) = record_sample(&mut samples, observed) {
                    errors.push(error);
                }
            }
            Err(error) => errors.push(error),
        }
    }
    drop(retained);
    if let Some(totals) = &backing_totals {
        if !retention {
            eprintln!("native-backing R4 {:?}", totals.snapshot());
        }
        let expected = if retention {
            2
        } else {
            2 * (WARMUPS + MEASURED)
        };
        if let Err(error) = totals.reconciled(expected) {
            errors.push(error);
        }
    }
    drop(endpoint);
    drop(control);
    drop(server);
    drop(shared);
    if let Some(totals) = &backing_totals {
        if !retention {
            eprintln!("native-backing R5 {:?}", totals.snapshot());
        }
        let expected = if retention {
            2
        } else {
            2 * (WARMUPS + MEASURED)
        };
        if let Err(error) = totals.reconciled(expected) {
            errors.push(error);
        }
    }
    if resources {
        match sample(Checkpoint::R5, None, worker) {
            Ok(observed) => {
                if let Err(error) = require(
                    !observed.worker_present
                        && observed.fd_count <= samples[0].fd_count
                        && observed.thread_count <= samples[0].thread_count,
                    "final same-process FD/thread footprint after closed native handles drop",
                ) {
                    errors.push(error);
                }
                if let Err(error) = record_sample(&mut samples, observed) {
                    errors.push(error);
                }
            }
            Err(error) => errors.push(error),
        }
        if let Err(error) = require(
            samples.len() == SAMPLE_COUNT,
            "complete two-warmup eight-measured R0 through R5 series",
        ) {
            errors.push(error);
        }
        for observed in &samples {
            eprintln!(
                "native-resource {route:?} {:?} cycle={:?} fd={} threads={} rss_bytes={} worker_tid={:?} worker_present={}",
                observed.checkpoint,
                observed.cycle,
                observed.fd_count,
                observed.thread_count,
                observed.rss_bytes,
                observed.worker_tid,
                observed.worker_present
            );
        }
        report_thread_snapshots(&mut samples, &mut errors);
    }
    drop(runtime);
    if let Some(payload) = panic {
        if retention {
            let _ = writeln!(
                io::stderr().lock(),
                "native retention panic cleanup: {errors:?}"
            );
        } else {
            eprintln!("native lifecycle panic cleanup: {errors:?}");
        }
        resume_unwind(payload);
    }
    if errors.is_empty() {
        Ok(retention_held)
    } else {
        Err(FixtureError::Case { route, errors }.into())
    }
}

#[test]
fn native_transport_egress_backpressure_release_cancel_matrix() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for route in ROUTES {
        run_case(route, Exercise::Egress).unwrap_or_else(|error| panic!("{error:?}"));
    }
}

#[test]
fn native_transport_egress_backpressure_predicate_controls() {
    let positive = EgressObservation {
        prefix: CHUNK,
        peer_bytes: CHUNK,
        baseline: 3 * CHUNK,
        produced: (EGRESS_SLOTS + 4) * CHUNK,
        delivered: 4 * CHUNK,
        capacity: 0,
        pending: true,
        producer_done: false,
        source_created: 1,
        source_drops: 0,
        terminal: false,
        stopped: false,
        action: 0,
        invalid: false,
    };
    assert!(egress_pressure_held(positive));
    for negative in [
        EgressObservation {
            prefix: 0,
            peer_bytes: 0,
            ..positive
        },
        EgressObservation {
            prefix: EGRESS_BYTES,
            peer_bytes: EGRESS_BYTES,
            ..positive
        },
        EgressObservation {
            peer_bytes: CHUNK + 1,
            ..positive
        },
        EgressObservation {
            baseline: positive.produced,
            ..positive
        },
        EgressObservation {
            delivered: CHUNK,
            ..positive
        },
        EgressObservation {
            delivered: positive.produced + 1,
            ..positive
        },
        EgressObservation {
            produced: EGRESS_BYTES,
            ..positive
        },
        EgressObservation {
            produced: 0,
            ..positive
        },
        EgressObservation {
            produced: positive.produced - 1,
            ..positive
        },
        EgressObservation {
            capacity: 1,
            ..positive
        },
        EgressObservation {
            capacity: EGRESS_SLOTS,
            ..positive
        },
        EgressObservation {
            pending: false,
            ..positive
        },
        EgressObservation {
            producer_done: true,
            ..positive
        },
        EgressObservation {
            source_created: 0,
            ..positive
        },
        EgressObservation {
            source_created: 2,
            ..positive
        },
        EgressObservation {
            source_drops: 1,
            ..positive
        },
        EgressObservation {
            terminal: true,
            ..positive
        },
        EgressObservation {
            stopped: true,
            ..positive
        },
        EgressObservation {
            action: 1,
            ..positive
        },
        EgressObservation {
            action: 2,
            ..positive
        },
        EgressObservation {
            invalid: true,
            ..positive
        },
    ] {
        assert!(!egress_pressure_held(negative), "reject {negative:?}");
    }
    check_egress_bytes(&[249, 250, 0, 1, 2, 3], 249).expect("literal wraparound bytes");
    assert!(check_egress_bytes(&[249, 250, 0, 1, 3, 2], 249).is_err());
    assert!(check_egress_bytes(&[], 0).is_err());
    assert!(check_egress_bytes(&[0], EGRESS_BYTES).is_err());
    assert!(check_egress_bytes(&[0, 1], usize::MAX).is_err());
    for phase in ["initial", "recheck"] {
        let error = EgressPressureFailure {
            phase,
            observation: positive,
            source: FixtureError::Deadline("literal egress control").into(),
        };
        assert!(matches!(
            error
                .source()
                .expect("original source")
                .downcast_ref::<FixtureError>(),
            Some(FixtureError::Deadline("literal egress control"))
        ));
        let debug = format!("{error:?}");
        for field in [
            "prefix",
            "peer_bytes",
            "baseline",
            "produced",
            "delivered",
            "capacity",
            "pending",
            "producer_done",
            "source_created",
            "source_drops",
            "terminal",
            "stopped",
            "action",
            "invalid",
        ] {
            assert!(debug.contains(field), "captured bounded field {field}");
        }
    }
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("control runtime");
    runtime.block_on(async {
        require_pending_egress_receipt(Box::pin(pending()))
            .await
            .expect("one pending receipt poll is admitted");
        let premature = require_pending_egress_receipt(Box::pin(ready(Ok(()))))
            .await
            .expect_err("completed receipt cannot be admitted as pending");
        assert!(matches!(
            premature.downcast_ref::<FixtureError>(),
            Some(FixtureError::Assertion(
                "pressured response receipt remains pending"
            ))
        ));
        let cleanup_error = require_pending_egress_receipt(Box::pin(ready(Err(
            ExchangeCompletionError::CleanupUnconfirmed,
        ))))
        .await
        .expect_err("actual failed cleanup cannot be admitted as pending");
        assert_eq!(
            cleanup_error.downcast_ref::<ExchangeCompletionError>(),
            Some(&ExchangeCompletionError::CleanupUnconfirmed)
        );
        let shared = Shared::new(ROUTES[0]);
        let native = shared.fail(cleanup_error);
        assert_eq!(native.code(), HttpErrorCode::INTERNAL_ERROR);
        {
            let errors = shared.errors.lock().expect("retained native fixture error");
            assert_eq!(errors.len(), 1);
            assert_eq!(
                errors[0].downcast_ref::<ExchangeCompletionError>(),
                Some(&ExchangeCompletionError::CleanupUnconfirmed),
                "the original cleanup failure survives the native error bridge"
            );
        }
        let state = EgressState::new();
        let mut producer = state.install().expect("bounded producer");
        let mut plan = state
            .plan
            .lock()
            .expect("plan")
            .take()
            .expect("actual receiver");
        producer
            .send_prefix()
            .await
            .expect("first chunk before unrestricted producer control");
        poll_fn(|context| {
            producer.poll(context);
            assert!(producer.pending);
            assert_eq!(producer.sender.capacity(), 0);
            Poll::Ready(())
        })
        .await;
        assert_eq!(
            producer.evidence.produced.load(Ordering::Acquire),
            EGRESS_SLOTS * CHUNK
        );
        assert!(
            !egress_pressure_held(producer.snapshot(0)),
            "real full channel without headers/prefix/native owner grants no wire credit"
        );
        let bytes = plan.receiver.recv().await.expect("one actual queued chunk");
        check_egress_bytes(&bytes, 0).expect("first produced chunk");
        drop(bytes);
        poll_fn(|context| {
            producer.poll(context);
            assert!(producer.pending);
            assert_eq!(producer.sender.capacity(), 0);
            Poll::Ready(())
        })
        .await;
        assert_eq!(
            producer.evidence.produced.load(Ordering::Acquire),
            (EGRESS_SLOTS + 1) * CHUNK
        );
        drop(plan.receiver);
        producer
            .finish()
            .await
            .expect("actual producer return after receiver destruction");
        assert!(matches!(
            &*producer.evidence.producer_result.lock().expect("result"),
            Some(Err(_))
        ));
        let original = producer
            .evidence
            .producer_result
            .lock()
            .expect("result")
            .as_ref()
            .expect("actual result")
            .as_ref()
            .expect_err("receiver closed")
            .clone();
        let error = producer
            .unexpected_error()
            .expect("early admission error is not hidden");
        let retained = error
            .downcast_ref::<Arc<mpsc::error::SendError<()>>>()
            .expect("original typed admission error");
        assert!(Arc::ptr_eq(&original, retained));
        producer.evidence.action.store(2, Ordering::Release);
        assert!(
            producer.unexpected_error().is_none(),
            "only explicit cancellation expects closed admission"
        );
        assert!(
            matches!(
                &*producer.evidence.producer_result.lock().expect("result"),
                Some(Err(_))
            ),
            "expected cancellation retains the original outcome"
        );
        assert!(producer.sender.is_closed());
        assert_eq!(
            producer.evidence.produced.load(Ordering::Acquire),
            (EGRESS_SLOTS + 1) * CHUNK
        );

        let prefix_control = || {
            let state = EgressState::new();
            let producer = state.install().expect("prefix control producer");
            let plan = state
                .plan
                .lock()
                .expect("prefix control plan")
                .take()
                .expect("prefix control receiver");
            (producer, plan.receiver)
        };
        for (delayed_polls, prefix_length) in [(0, CHUNK), (2, CHUNK), (0, 1), (2, 1)] {
            let (mut producer, mut receiver) = prefix_control();
            let evidence = Arc::clone(&producer.evidence);
            let capacity = producer.sender.clone();
            let work_polls = AtomicUsize::new(0);
            let (prefix, baseline) = {
                let work = poll_fn(|_| {
                    assert_eq!(evidence.produced.load(Ordering::Acquire), CHUNK);
                    assert!(
                        evidence
                            .producer_result
                            .lock()
                            .expect("prefix control result")
                            .is_none(),
                        "first chunk is not completed production"
                    );
                    let observed = work_polls.fetch_add(1, Ordering::AcqRel);
                    if observed < delayed_polls {
                        return Poll::Pending;
                    }
                    assert_eq!(observed, delayed_polls);
                    let bytes = receiver.try_recv().expect("actual first chunk is queued");
                    assert_eq!(bytes.len(), CHUNK);
                    check_egress_bytes(&bytes, 0).expect("offset-zero setup bytes");
                    Poll::Ready(Ok(prefix_length))
                });
                let mut setup = Box::pin(producer.prefix(work));
                for expected in 1..=delayed_polls {
                    poll_fn(|context| {
                        let result = setup.as_mut().poll(context);
                        assert!(result.is_pending(), "premature prefix return: {result:?}");
                        let observed = work_polls.load(Ordering::Acquire);
                        if observed == expected {
                            assert_eq!(evidence.produced.load(Ordering::Acquire), CHUNK);
                            assert_eq!(capacity.capacity(), EGRESS_SLOTS - 1);
                            Poll::Ready(())
                        } else {
                            assert!(observed < expected);
                            Poll::Pending
                        }
                    })
                    .await;
                }
                setup.await.expect("controlled actual prefix observation")
            };
            assert_eq!(work_polls.load(Ordering::Acquire), delayed_polls + 1);
            assert_eq!(prefix, prefix_length);
            assert_eq!(baseline, CHUNK);
            assert_eq!(evidence.produced.load(Ordering::Acquire), baseline);
            assert_eq!(capacity.capacity(), EGRESS_SLOTS);
            assert!(!producer.pending, "setup is not admission pressure");
            assert!(
                evidence
                    .producer_result
                    .lock()
                    .expect("prefix control result")
                    .is_none()
            );
            evidence.prefix.store(prefix, Ordering::Release);
            evidence.peer_bytes.store(prefix, Ordering::Release);
            assert!(!egress_pressure_held(producer.snapshot(baseline)));
            let duplicate = producer
                .prefix(ready(Ok(prefix_length)))
                .await
                .expect_err("prefix setup cannot be repeated");
            assert!(matches!(
                duplicate.downcast_ref::<FixtureError>(),
                Some(FixtureError::Assertion("fresh egress prefix setup"))
            ));
            assert_eq!(evidence.produced.load(Ordering::Acquire), baseline);
            producer
                .during(poll_fn(|_| {
                    if capacity.capacity() == 0 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    }
                }))
                .await
                .expect("actual bounded continuation admission");
            assert!(producer.pending);
            assert_eq!(capacity.capacity(), 0);
            assert_eq!(
                evidence.produced.load(Ordering::Acquire),
                (EGRESS_SLOTS + 1) * CHUNK
            );
            assert!(evidence.produced.load(Ordering::Acquire) > baseline);
            assert!(
                !egress_pressure_held(producer.snapshot(baseline)),
                "channel controls do not invent native source or wire evidence"
            );
            let bytes = receiver.try_recv().expect("first continuation chunk");
            check_egress_bytes(&bytes, CHUNK).expect("continuation follows prefix exactly");
            drop(bytes);
            drop(receiver);
            producer
                .finish()
                .await
                .expect("actual inline continuation return");
            assert!(matches!(
                &*evidence
                    .producer_result
                    .lock()
                    .expect("prefix control result"),
                Some(Err(_))
            ));
        }
        for invalid_prefix in [0, CHUNK + 1] {
            let (mut producer, receiver) = prefix_control();
            let error = producer
                .prefix(ready(Ok(invalid_prefix)))
                .await
                .expect_err("invalid prefix cannot admit continuation");
            assert!(matches!(
                error.downcast_ref::<FixtureError>(),
                Some(FixtureError::Assertion(
                    "actual prefix precedes egress continuation"
                ))
            ));
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), CHUNK);
            assert!(!producer.pending);
            assert!(
                producer
                    .evidence
                    .producer_result
                    .lock()
                    .expect("invalid prefix result")
                    .is_none()
            );
            drop(receiver);
            producer.finish().await.expect("invalid prefix cleanup");
        }
        {
            let (mut producer, receiver) = prefix_control();
            let capacity = producer.sender.clone();
            producer
                .during(poll_fn(|_| {
                    if capacity.capacity() == 0 {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    }
                }))
                .await
                .expect("deliberately advanced continuation control");
            let advanced = producer.evidence.produced.load(Ordering::Acquire);
            assert_eq!(advanced, EGRESS_SLOTS * CHUNK);
            assert!(!producer.prefix_started);
            let error = producer
                .prefix(ready(Ok(CHUNK)))
                .await
                .expect_err("late setup cannot reset advanced production");
            assert!(matches!(
                error.downcast_ref::<FixtureError>(),
                Some(FixtureError::Assertion("fresh egress prefix setup"))
            ));
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), advanced);
            assert!(producer.pending);
            drop(receiver);
            producer.finish().await.expect("late setup cleanup");
        }
        {
            let (mut producer, receiver) = prefix_control();
            drop(receiver);
            let mut peer_polled = false;
            let error = producer
                .prefix(async {
                    peer_polled = true;
                    Ok(CHUNK)
                })
                .await
                .expect_err("closed admission fails before prefix work");
            assert!(!peer_polled);
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), 0);
            assert!(!producer.pending);
            let original = producer
                .evidence
                .producer_result
                .lock()
                .expect("prefix admission result")
                .as_ref()
                .expect("actual prefix admission outcome")
                .as_ref()
                .expect_err("receiver closed before prefix")
                .clone();
            let retained = error
                .downcast_ref::<Arc<mpsc::error::SendError<()>>>()
                .expect("original typed prefix admission error");
            assert!(Arc::ptr_eq(&original, retained));
            producer
                .finish()
                .await
                .expect("sticky failed prefix outcome");
            assert!(Arc::ptr_eq(&original, retained));
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), 0);
        }
        {
            let (mut producer, receiver) = prefix_control();
            let original = Arc::new(io::Error::other("literal peer-prefix failure"));
            let error = producer
                .prefix(async { Err(Box::new(Arc::clone(&original)) as BoxError) })
                .await
                .expect_err("peer-prefix failure remains primary");
            let retained = error
                .downcast_ref::<Arc<io::Error>>()
                .expect("original typed peer-prefix error");
            assert!(Arc::ptr_eq(&original, retained));
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), CHUNK);
            assert!(!producer.pending);
            assert!(
                producer
                    .evidence
                    .producer_result
                    .lock()
                    .expect("peer-prefix producer result")
                    .is_none(),
                "peer failure does not poll or complete the continuation"
            );
            drop(receiver);
            producer
                .finish()
                .await
                .expect("peer-prefix failure cleanup");
            let secondary = producer
                .unexpected_error()
                .expect("actual closed continuation remains retained");
            assert!(
                secondary
                    .downcast_ref::<Arc<mpsc::error::SendError<()>>>()
                    .is_some()
            );
            assert!(Arc::ptr_eq(&original, retained));
            assert_eq!(producer.evidence.produced.load(Ordering::Acquire), CHUNK);
        }
    });
}

#[test]
fn native_transport_backpressure_release_cancel_matrix() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for route in ROUTES {
        run_case(route, Exercise::Pressure).unwrap_or_else(|error| panic!("{error:?}"));
    }
}

fn envelope_ingress_controls() -> TestResult<()> {
    for route in ROUTES {
        let mut config = ServerConfig {
            plaintext_protocol: route.codec(),
            num_io_threads: 1,
            ..ServerConfig::default()
        };
        if route.tls {
            let (options, _) = tls_fixture(route)?;
            config.tls_credentials = Some(Arc::new(StaticTlsCredentials::new(options)));
        }
        let bound = EnvelopeIngressBound::from_config(route, &config)?;
        require(
            bound.route == route
                && bound.stream == 262_144
                && bound.connection == 5_242_880
                && match route.protocol {
                    Protocol::H1 => bound.segment == 262_144 && bound.ceiling == 266_143,
                    Protocol::H2 => bound.segment == 16_384 && bound.ceiling == 5_242_880,
                },
            "fixed route-specific BODY credit domains",
        )?;
        for changed in 0..8 {
            let mut candidate = ServerConfig {
                plaintext_protocol: route.codec(),
                num_io_threads: 1,
                tls_credentials: config.tls_credentials.clone(),
                ..ServerConfig::default()
            };
            match changed {
                0 => {
                    candidate.plaintext_protocol = if route.protocol == Protocol::H1 {
                        CodecProtocol::Http2
                    } else {
                        CodecProtocol::Http11
                    }
                }
                1 => candidate.num_io_threads = 2,
                2 => candidate.session.stream_flow_control += 1,
                3 => candidate.session.connection_flow_control -= 1,
                4 => candidate.session.read_allocation_size += 1,
                5 => {
                    candidate.session.byte_stream_limits =
                        ByteStreamLimits::new(262_145, 262_144, 1_024)?
                }
                6 => {
                    candidate.session.byte_stream_limits =
                        ByteStreamLimits::new(262_144, 262_145, 1_024)?
                }
                7 => {
                    if route.tls {
                        candidate.tls_credentials = None;
                    } else {
                        let (options, _) = tls_fixture(Route { tls: true, ..route })?;
                        candidate.tls_credentials =
                            Some(Arc::new(StaticTlsCredentials::new(options)));
                    }
                }
                _ => unreachable!(),
            }
            require(
                EnvelopeIngressBound::from_config(route, &candidate).is_err(),
                "changed constructor inputs cannot retain the approved route envelope",
            )?;
        }
        if route.protocol == Protocol::H2 {
            let settings = config.session.settings.clone();
            for changed in 0..5 {
                config.session.settings = settings.clone();
                match changed {
                    0 => config
                        .session
                        .settings
                        .retain(|setting| setting.identifier != 5),
                    1 => {
                        config
                            .session
                            .settings
                            .iter_mut()
                            .find(|setting| setting.identifier == 5)
                            .expect("default advertised frame limit")
                            .value += 1
                    }
                    2 => config.session.settings.push(roxygen_codec::CodecSetting {
                        identifier: 5,
                        value: 16_384,
                    }),
                    3 => config.session.settings.push(roxygen_codec::CodecSetting {
                        identifier: 4,
                        value: 262_145,
                    }),
                    4 => {
                        for _ in 0..2 {
                            config.session.settings.push(roxygen_codec::CodecSetting {
                                identifier: 4,
                                value: 262_144,
                            });
                        }
                    }
                    _ => unreachable!(),
                }
                require(
                    EnvelopeIngressBound::from_config(route, &config).is_err(),
                    "missing, conflicting or duplicate effective H2 settings reject",
                )?;
            }
            config.session.settings = settings;
            config.session.settings.push(roxygen_codec::CodecSetting {
                identifier: 4,
                value: 262_144,
            });
            require(
                EnvelopeIngressBound::from_config(route, &config)? == bound,
                "one explicit unchanged stream setting binds the same effective constructor",
            )?;
        }
        let mut fresh = PressureQueue::default();
        fresh.bind_envelope(bound)?;
        require(
            fresh.envelope_valid(bound) && !fresh.envelope_pressure(bound),
            "idle is bounded but is not pressure",
        )?;
        let mut rebound = fresh;
        require(
            rebound.bind_envelope(bound).is_err() && !rebound.envelope_valid(bound),
            "no lifetime envelope reset",
        )?;
        for changed in 0..7 {
            let mut dirty = PressureQueue::default();
            match changed {
                0 => dirty.read = 1,
                1 => dirty.write = 1,
                2 => dirty.received = 1,
                3 => dirty.opened = 1,
                4 => dirty.closed = 1,
                5 => dirty.drops = 1,
                6 => dirty.invalid = true,
                _ => unreachable!(),
            }
            require(
                dirty.bind_envelope(bound).is_err(),
                "binding must precede every connection callback",
            )?;
        }
        fresh.opened();
        let mut boundary = fresh;
        for _ in 0..bound.ceiling / bound.segment {
            boundary.delta(true, bound.segment);
        }
        boundary.delta(true, bound.ceiling % bound.segment);
        require(
            boundary.read == bound.ceiling && boundary.envelope_pressure(bound),
            "exact prospective ceiling is inclusive",
        )?;
        let mut exceeded = boundary;
        exceeded.delta(true, 1);
        exceeded.delta(true, -bound.ceiling - 1);
        require(
            exceeded.read == 0
                && !exceeded.envelope_valid(bound)
                && exceeded.envelope.is_some_and(|credit| {
                    credit.peak == bound.ceiling + 1
                        && credit.first_failure == Some("connection BODY-credit ceiling")
                }),
            "one-byte excursion stays rejected after complete credit reconciliation",
        )?;
        exceeded.fail("later secondary failure");
        require(
            exceeded.envelope.is_some_and(|credit| {
                credit.first_failure == Some("connection BODY-credit ceiling")
            }),
            "later callback failures retain the original envelope violation",
        )?;
        let mut oversized = fresh;
        oversized.delta(true, bound.segment + 1);
        oversized.delta(true, -bound.segment - 1);
        require(
            !oversized.envelope_valid(bound)
                && oversized.envelope.is_some_and(|credit| {
                    credit.first_failure == Some("positive callback segment bound")
                }),
            "oversized callback cannot be erased by its balancing negative",
        )?;
        for changed in 0..4 {
            let mut arithmetic = fresh;
            match changed {
                0 => arithmetic.delta(true, -1),
                1 => {
                    arithmetic.read = i64::MAX;
                    arithmetic.delta(true, 1);
                }
                2 => {
                    arithmetic.received = usize::MAX;
                    arithmetic.delta(true, 1);
                }
                3 => {
                    arithmetic.write = i64::MAX;
                    arithmetic.delta(false, 1);
                }
                _ => unreachable!(),
            }
            require(
                !arithmetic.envelope_valid(bound),
                "signed balances and lifetime byte totals are checked",
            )?;
        }
        let owner = current_identity();
        let mut wrong_worker = boundary;
        wrong_worker.check_owner(owner, WorkerIdentity { linux: 0, ..owner });
        wrong_worker.check_owner(owner, owner);
        require(
            !wrong_worker.envelope_valid(bound)
                && wrong_worker
                    .envelope
                    .is_some_and(|credit| credit.first_failure == Some("callback worker identity")),
            "the actual original callback worker cannot be substituted then restored",
        )?;
        let mut write_domain = fresh;
        write_domain.delta(false, bound.ceiling + 1);
        write_domain.delta(false, -bound.ceiling - 1);
        require(
            write_domain.envelope_valid(bound),
            "BODY read ceiling is not a fabricated native-write or allocation ceiling",
        )?;
        boundary.delta(true, -bound.ceiling);
        let recovery = H1IngressRecovery {
            queue: boundary,
            running: true,
            starts: 1,
            stops: 0,
        };
        require(
            recovery.envelope_admitted(bound),
            "balanced credit recovers on the live original worker",
        )?;
        for invalid in [
            H1IngressRecovery {
                running: false,
                ..recovery
            },
            H1IngressRecovery {
                starts: 2,
                ..recovery
            },
            H1IngressRecovery {
                stops: 1,
                ..recovery
            },
            H1IngressRecovery {
                queue: fresh,
                ..recovery
            },
        ] {
            require(
                !invalid.envelope_admitted(bound),
                "stopped, substituted or idle workers cannot provide recovery",
            )?;
        }
        let context = Arc::new(ConnectionContext::new(
            Default::default(),
            Default::default(),
            route.security(),
            None,
        ));
        let connection = crate::acceptor::connection_info(route.codec(), context);
        require(
            bound.matches(&connection),
            "actual connection protocol and security bind the route",
        )?;
        for changed in 0..3 {
            let mut wrong = connection.clone();
            match changed {
                0 => {
                    wrong.protocol = if route.protocol == Protocol::H1 {
                        CodecProtocol::Http2
                    } else {
                        CodecProtocol::Http11
                    }
                }
                1 => wrong.secure = !route.tls,
                2 => {
                    wrong.context = Arc::new(ConnectionContext::new(
                        Default::default(),
                        Default::default(),
                        Route {
                            tls: !route.tls,
                            ..route
                        }
                        .security(),
                        None,
                    ))
                }
                _ => unreachable!(),
            }
            let state = PressureState::configured(route, &config)?;
            let recorder = state.recorder(&wrong);
            recorder.record_transaction_opened();
            recorder.record_buffered_read_bytes(1);
            recorder.record_buffered_read_bytes(-1);
            recorder.record_transaction_closed();
            drop(recorder);
            let queue = state.ledger(&wrong.context)?.snapshot();
            require(
                !bound.matches(&wrong)
                    && !queue.envelope_valid(bound)
                    && queue.envelope.is_some_and(|credit| {
                        credit.first_failure == Some("actual accepted protocol and security")
                    }),
                "later balanced callbacks cannot legitimize an incorrectly attributed accepted connection",
            )?;
        }
        let state = PressureState::configured(route, &config)?;
        let recorder = state.recorder(&connection);
        let ledger = state.ledger(&connection.context)?;
        recorder.record_transaction_opened();
        if route.protocol == Protocol::H2 {
            recorder.record_transaction_opened();
            for _ in 0..bound.stream / bound.segment {
                recorder.record_buffered_read_bytes(bound.segment);
            }
            recorder.record_buffered_read_bytes(1);
            require(
                ledger.snapshot().read == bound.stream + 1
                    && ledger.snapshot().envelope_valid(bound),
                "raw connection callbacks can transiently exceed one stream window before stream rejection",
            )?;
            recorder.record_buffered_read_bytes(-bound.stream - 1);
            recorder.record_transaction_closed();
        }
        recorder.record_transaction_closed();
        recorder.record_transaction_opened();
        recorder.record_buffered_read_bytes(1);
        recorder.record_buffered_read_bytes(-1);
        recorder.record_transaction_closed();
        drop(recorder);
        let retired = ledger.snapshot();
        require(
            retired.envelope_valid(bound)
                && retired.drops == 1
                && retired.read == 0
                && retired.write == 0
                && retired.opened == retired.closed
                && retired.opened == if route.protocol == Protocol::H2 { 3 } else { 2 },
            "connection lifetime accounting admits healthy siblings and reuse then actual recorder destruction",
        )?;
        ledger.delta(true, 1);
        ledger.delta(true, -1);
        require(
            !ledger.snapshot().envelope_valid(bound)
                && ledger.snapshot().envelope.is_some_and(|credit| {
                    credit.first_failure == Some("callback after recorder destruction")
                }),
            "balanced callbacks after destruction cannot resurrect an accepted recorder",
        )?;
    }
    Ok(())
}

#[test]
fn native_transport_backpressure_predicate_controls() {
    retention_owner_controls().unwrap();
    envelope_ingress_controls().unwrap();
    let route = Route {
        protocol: Protocol::H1,
        tls: false,
    };
    let config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http11,
        num_io_threads: 1,
        ..ServerConfig::default()
    };
    let bound = H1IngressBound::from_config(route, &config).unwrap();
    assert_eq!(bound.flow, 262_144);
    assert_eq!(bound.segment, 131_072);
    assert_eq!(bound.ceiling, 266_143);
    for other in ROUTES
        .into_iter()
        .filter(|other| other.tls || other.protocol != Protocol::H1)
    {
        assert!(H1IngressBound::from_config(other, &config).is_err());
    }
    for changed in 0..6 {
        let mut changed_config = ServerConfig {
            plaintext_protocol: CodecProtocol::Http11,
            num_io_threads: 1,
            ..ServerConfig::default()
        };
        match changed {
            0 => changed_config.session.stream_flow_control += 1,
            1 => changed_config.session.read_allocation_size += 1,
            2 => {
                changed_config.session.byte_stream_limits =
                    ByteStreamLimits::new(262_145, 262_144, 1_024).unwrap();
            }
            3 => changed_config.session.connection_flow_control -= 1,
            4 => changed_config.num_io_threads = 2,
            5 => changed_config.plaintext_protocol = CodecProtocol::Http2,
            _ => unreachable!(),
        }
        assert!(H1IngressBound::from_config(route, &changed_config).is_err());
    }
    let mut fresh = PressureQueue::default();
    fresh.bind_ingress(bound, 1).unwrap();
    let mut rebound = fresh;
    assert!(rebound.bind_ingress(bound, 1).is_err());
    assert!(rebound.invalid);
    fresh.opened();
    let mut unbounded = PressureQueue::default();
    unbounded.opened();
    unbounded.delta(true, bound.ceiling + 1);
    assert!(!unbounded.invalid);
    assert!(
        unbounded.ingress.is_none(),
        "other recorders retain their original accounting"
    );
    let mut ceiling_queue = fresh;
    for _ in 0..(bound.flow - 1) / bound.segment {
        ceiling_queue.delta(true, bound.segment);
    }
    ceiling_queue.delta(true, (bound.flow - 1) % bound.segment);
    assert_eq!(ceiling_queue.read, bound.flow - 1);
    assert!(!ceiling_queue.ingress_pressure(bound, 1));
    ceiling_queue.delta(
        true,
        i64::try_from(config.session.read_allocation_size).unwrap(),
    );
    assert_eq!(ceiling_queue.read, 266_143);
    assert_eq!(ceiling_queue.ingress.unwrap().peak, 266_143);
    assert!(ceiling_queue.ingress_pressure(bound, 1));
    assert!(!ceiling_queue.ingress_pressure(bound, 2));
    assert!(!ceiling_queue.ingress_pressure(
        H1IngressBound {
            ceiling: 266_144,
            ..bound
        },
        1,
    ));
    let mut exceeded = ceiling_queue;
    exceeded.delta(true, 1);
    assert_eq!(exceeded.ingress.unwrap().peak, 266_144);
    assert!(exceeded.invalid);
    exceeded.delta(true, -266_144);
    assert_eq!(exceeded.read, 0);
    assert_eq!(exceeded.ingress.unwrap().peak, 266_144);
    assert!(
        !exceeded.ingress_pressure(bound, 1),
        "zero cannot erase an excursion"
    );
    assert!(exceeded.bind_ingress(bound, 1).is_err());
    let mut oversized_callback = fresh;
    oversized_callback.delta(true, bound.segment + 1);
    assert!(
        oversized_callback.invalid,
        "one callback cannot exceed one TCP read buffer"
    );
    let mut underflow = ceiling_queue;
    underflow.delta(true, -bound.ceiling - 1);
    assert!(underflow.invalid);
    underflow.delta(true, -bound.ceiling);
    assert_eq!(underflow.read, 0);
    assert!(!underflow.ingress_pressure(bound, 1));
    let mut overflow = ceiling_queue;
    overflow.read = i64::MAX;
    overflow.delta(true, 1);
    assert!(overflow.invalid);
    let mut received_overflow = fresh;
    received_overflow.received = usize::MAX;
    received_overflow.delta(true, 1);
    assert!(received_overflow.invalid);
    let mut wrong_worker = ceiling_queue;
    let owner = current_identity();
    wrong_worker.check_owner(owner, WorkerIdentity { linux: 0, ..owner });
    wrong_worker.check_owner(owner, owner);
    assert!(
        wrong_worker.invalid,
        "later correct identity cannot clear failure"
    );
    assert!(!wrong_worker.ingress_pressure(bound, 1));
    let mut second_transaction = ceiling_queue;
    second_transaction.closed = 1;
    second_transaction.opened();
    assert!(second_transaction.invalid);
    assert!(!second_transaction.ingress_pressure(bound, 1));
    for invalid in [
        PressureQueue {
            received: 1,
            ..PressureQueue::default()
        },
        PressureQueue {
            opened: 1,
            ..PressureQueue::default()
        },
        PressureQueue {
            invalid: true,
            ..PressureQueue::default()
        },
    ] {
        let mut invalid = invalid;
        assert!(invalid.bind_ingress(bound, 1).is_err());
    }
    for target in [0, 3] {
        assert!(
            PressureQueue::default()
                .bind_ingress(bound, target)
                .is_err()
        );
    }
    let mut recovered_queue = ceiling_queue;
    recovered_queue.delta(true, -bound.ceiling);
    let recovery = H1IngressRecovery {
        queue: recovered_queue,
        running: true,
        starts: 1,
        stops: 0,
    };
    assert_eq!(recovery.queue.read, 0);
    assert_eq!(recovery.queue.ingress.unwrap().peak, 266_143);
    assert!(recovery.admitted(bound, 1));
    for invalid in [
        H1IngressRecovery {
            queue: fresh,
            ..recovery
        },
        H1IngressRecovery {
            queue: ceiling_queue,
            ..recovery
        },
        H1IngressRecovery {
            queue: exceeded,
            ..recovery
        },
        H1IngressRecovery {
            queue: underflow,
            ..recovery
        },
        H1IngressRecovery {
            queue: overflow,
            ..recovery
        },
        H1IngressRecovery {
            queue: received_overflow,
            ..recovery
        },
        H1IngressRecovery {
            queue: wrong_worker,
            ..recovery
        },
        H1IngressRecovery {
            queue: second_transaction,
            ..recovery
        },
        H1IngressRecovery {
            running: false,
            ..recovery
        },
        H1IngressRecovery {
            starts: 0,
            ..recovery
        },
        H1IngressRecovery {
            stops: 1,
            ..recovery
        },
    ] {
        assert!(
            !invalid.admitted(bound, 1),
            "invalid live-worker recovery: {invalid:?}"
        );
    }
    let failure = H1IngressFailure {
        source: FixtureError::Assertion("valid target-bound BODY-credit callbacks"),
        phase: "pressure",
        target: 1,
        queue: exceeded,
    };
    let boxed: BoxError = failure.into();
    let failure = boxed.downcast_ref::<H1IngressFailure>().unwrap();
    assert_eq!(failure.phase, "pressure");
    assert_eq!(failure.target, 1);
    assert_eq!(failure.queue.ingress.unwrap().peak, 266_144);
    assert!(matches!(
        failure.source().unwrap().downcast_ref::<FixtureError>(),
        Some(FixtureError::Assertion(
            "valid target-bound BODY-credit callbacks"
        ))
    ));
    assert!(
        crate::acceptor::ServerSessionConfig::default()
            .accepted_socket_profile
            .is_none()
    );
    let addresses = PeerAddresses {
        local: SocketAddr::from(([127, 0, 0, 1], 12_345)),
        remote: SocketAddr::from(([127, 0, 0, 1], 23_456)),
    };
    let sender = socket_profile_with(
        SocketProfileDirection::Sender,
        addresses,
        |requested| {
            assert_eq!(requested, 64 * 1024);
            Ok(())
        },
        || Ok(64 * 1024),
    )
    .unwrap();
    let receiver = socket_profile_with(
        SocketProfileDirection::Receiver,
        PeerAddresses {
            local: addresses.remote,
            remote: addresses.local,
        },
        |requested| {
            assert_eq!(requested, 64 * 1024);
            Ok(())
        },
        || Ok(256 * 1024),
    )
    .unwrap();
    let pair = paired_socket_profile(Protocol::H1, addresses, Some(sender), Some(receiver))
        .unwrap()
        .unwrap();
    assert_eq!(pair.sender, sender);
    assert_eq!(pair.receiver, receiver);
    assert_eq!(
        paired_socket_profile(Protocol::H2, addresses, None, None).unwrap(),
        None
    );
    for (missing_sender, missing_receiver) in
        [(None, None), (Some(sender), None), (None, Some(receiver))]
    {
        assert!(
            paired_socket_profile(Protocol::H1, addresses, missing_sender, missing_receiver)
                .is_err()
        );
    }
    for (extra_sender, extra_receiver) in [
        (Some(sender), None),
        (None, Some(receiver)),
        (Some(sender), Some(receiver)),
    ] {
        assert!(
            paired_socket_profile(Protocol::H2, addresses, extra_sender, extra_receiver).is_err()
        );
    }
    for invalid in [
        SocketBufferReceipt {
            direction: SocketProfileDirection::Sender,
            ..receiver
        },
        SocketBufferReceipt {
            addresses,
            ..receiver
        },
        SocketBufferReceipt {
            addresses: PeerAddresses {
                local: addresses.local,
                ..receiver.addresses
            },
            ..receiver
        },
        SocketBufferReceipt {
            addresses: PeerAddresses {
                remote: addresses.remote,
                ..receiver.addresses
            },
            ..receiver
        },
    ] {
        assert!(
            paired_socket_profile(Protocol::H1, addresses, Some(sender), Some(invalid)).is_err()
        );
    }
    assert!(
        paired_socket_profile(
            Protocol::H1,
            receiver.addresses,
            Some(sender),
            Some(receiver)
        )
        .is_err()
    );
    for receipt in [sender, receiver] {
        for effective in [
            0,
            PRESSURE_SOCKET_REQUEST - 1,
            PRESSURE_WINDOW + 1,
            usize::MAX,
        ] {
            let invalid = SocketBufferReceipt {
                effective,
                ..receipt
            };
            let error = invalid.validate().unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<SocketProfileValidationError>()
                    .unwrap()
                    .receipt,
                invalid
            );
        }
        assert!(
            SocketBufferReceipt {
                requested: 0,
                ..receipt
            }
            .validate()
            .is_err()
        );
        let calls = std::cell::Cell::new(0);
        let error = socket_profile_with(
            receipt.direction,
            receipt.addresses,
            |requested| {
                assert_eq!(requested, PRESSURE_SOCKET_REQUEST);
                calls.set(1);
                Err(io::Error::from_raw_os_error(libc::EACCES))
            },
            || {
                calls.set(2);
                Ok(PRESSURE_SOCKET_REQUEST)
            },
        )
        .unwrap_err();
        assert_eq!(calls.get(), 1, "failed setter never queries or publishes");
        let error = error.downcast_ref::<SocketProfileIoError>().unwrap();
        assert_eq!(error.direction, receipt.direction);
        assert_eq!(error.operation, "set buffer size");
        let source = error.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert!(std::ptr::eq(source, &error.source));
        assert_eq!(source.raw_os_error(), Some(libc::EACCES));
        assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
        calls.set(0);
        let error = socket_profile_with(
            receipt.direction,
            receipt.addresses,
            |requested| {
                assert_eq!(requested, PRESSURE_SOCKET_REQUEST);
                calls.set(1);
                Ok(())
            },
            || {
                assert_eq!(calls.get(), 1);
                calls.set(2);
                Err(io::Error::other(FixtureError::Assertion(
                    "original socket query failure",
                )))
            },
        )
        .unwrap_err();
        assert_eq!(calls.get(), 2);
        let shared = Shared::new(Route {
            protocol: Protocol::H1,
            tls: false,
        });
        let _native = shared.fail(error);
        let errors = shared.errors.lock().unwrap();
        assert_eq!(
            errors.len(),
            1,
            "acceptor bridge retains its original typed error"
        );
        let error = errors[0].downcast_ref::<SocketProfileIoError>().unwrap();
        assert_eq!(error.direction, receipt.direction);
        assert_eq!(error.operation, "get buffer size");
        assert_eq!(error.source.kind(), io::ErrorKind::Other);
        assert!(matches!(
            error
                .source
                .get_ref()
                .unwrap()
                .downcast_ref::<FixtureError>(),
            Some(FixtureError::Assertion("original socket query failure"))
        ));
    }
    let mut records = SocketProfileRecords::default();
    records.record(sender).unwrap();
    records.record(receiver).unwrap();
    assert!(records.record(sender).is_err());
    assert_eq!(records.receipts.iter().flatten().count(), 2);
    records.record_pair(1, pair).unwrap();
    assert!(records.record_pair(1, pair).is_err());
    assert!(records.record_pair(0, pair).is_err());
    assert!(records.record_pair(3, pair).is_err());
    let mut bounded = SocketProfileRecords::default();
    for index in 0..MAX_CONNECTIONS {
        bounded
            .record(SocketBufferReceipt {
                addresses: PeerAddresses {
                    local: SocketAddr::from((
                        [127, 0, 0, 1],
                        10_000 + u16::try_from(index).unwrap(),
                    )),
                    ..addresses
                },
                ..sender
            })
            .unwrap();
    }
    assert!(bounded.record(sender).is_err());
    assert_eq!(bounded.receipts.iter().flatten().count(), MAX_CONNECTIONS);
    let state = PressureState::new(true, None);
    state.record_socket_profile(sender).unwrap();
    assert!(state.record_socket_profile(sender).is_err());
    assert!(
        state.record_socket_profile(receiver).is_err(),
        "publication failure is sticky"
    );
    assert!(
        PressureState::new(false, None)
            .record_socket_profile(sender)
            .is_err()
    );
    let ready = PressureState::new(true, None);
    ready.record_socket_profile(sender).unwrap();
    assert_eq!(
        ready
            .socket_profiles
            .lock()
            .unwrap()
            .ready_pair(addresses)
            .unwrap(),
        None
    );
    ready.record_socket_profile(receiver).unwrap();
    assert_eq!(
        ready
            .socket_profiles
            .lock()
            .unwrap()
            .ready_pair(addresses)
            .unwrap(),
        Some(pair)
    );
    assert!(ready.ledgers.lock().unwrap().is_empty());
    assert!(ready.targets.lock().unwrap().is_empty());
    ready.fail_socket_profile();
    assert!(
        ready
            .socket_profiles
            .lock()
            .unwrap()
            .ready_pair(addresses)
            .is_err()
    );

    for protocol in [Protocol::H1, Protocol::H2] {
        let positive = PressureObservation {
            protocol,
            target: 1,
            expected: 1,
            consumed: CHUNK,
            received: PRESSURE_WINDOW + CHUNK,
            queued: PRESSURE_WINDOW as i64,
            sent: PRESSURE_WINDOW + CHUNK,
            wire: PRESSURE_WINDOW + CHUNK,
            blocked: true,
            held: true,
            valid: true,
            action: 0,
            done: false,
            pending: 2,
        };
        assert!(
            positive.admitted(),
            "structurally valid {protocol:?} pressure"
        );
        if protocol == Protocol::H1 {
            let bounded = PressureObservation {
                queued: ceiling_queue.read,
                received: bound.ceiling as usize + CHUNK,
                sent: bound.ceiling as usize + CHUNK,
                wire: bound.ceiling as usize + CHUNK,
                valid: ceiling_queue.ingress_pressure(bound, positive.target),
                ..positive
            };
            assert!(
                bounded.admitted(),
                "L plus the original pressure prerequisites"
            );
            assert!(
                !PressureObservation {
                    valid: exceeded.ingress_pressure(bound, positive.target),
                    ..bounded
                }
                .admitted()
            );
            assert!(
                !PressureObservation {
                    queued: 0,
                    received: CHUNK,
                    valid: recovered_queue.ingress_pressure(bound, positive.target),
                    ..positive
                }
                .admitted(),
                "zero recovery alone is not pressure"
            );
        }
        let negatives = [
            PressureObservation {
                target: 2,
                ..positive
            },
            PressureObservation {
                target: 0,
                expected: 0,
                ..positive
            },
            PressureObservation {
                blocked: false,
                ..positive
            },
            PressureObservation {
                wire: 0,
                ..positive
            },
            PressureObservation {
                held: false,
                ..positive
            },
            PressureObservation {
                valid: false,
                ..positive
            },
            PressureObservation {
                action: PressureAction::Release as usize,
                ..positive
            },
            PressureObservation {
                action: PressureAction::NativeCancel as usize,
                ..positive
            },
            PressureObservation {
                action: PressureAction::PeerCancel as usize,
                ..positive
            },
            PressureObservation {
                done: true,
                ..positive
            },
            PressureObservation {
                pending: 0,
                ..positive
            },
            PressureObservation {
                consumed: BODY_SIZE,
                ..positive
            },
            PressureObservation {
                sent: PRESSURE_BYTES,
                ..positive
            },
            PressureObservation {
                sent: CHUNK,
                ..positive
            },
            PressureObservation {
                received: 0,
                queued: 0,
                ..positive
            },
            PressureObservation {
                received: CHUNK,
                queued: 0,
                ..positive
            },
            PressureObservation {
                queued: -1,
                ..positive
            },
            PressureObservation {
                queued: PRESSURE_WINDOW as i64 + 1,
                ..positive
            },
            PressureObservation {
                received: usize::MAX,
                ..positive
            },
            PressureObservation {
                received: BODY_SIZE,
                queued: CHUNK as i64,
                sent: BODY_SIZE,
                wire: BODY_SIZE,
                ..positive
            },
            PressureObservation {
                received: 65_535,
                queued: 65_535 - CHUNK as i64,
                sent: 65_535,
                ..positive
            },
        ];
        for (index, negative) in negatives.into_iter().enumerate() {
            assert!(
                !negative.admitted(),
                "{protocol:?} negative pressure predicate {index}"
            );
            if protocol == Protocol::H1 {
                assert!(ceiling_queue.ingress_pressure(bound, positive.target));
                assert!(
                    !negative.admitted(),
                    "a bounded peak cannot replace the original pressure predicates"
                );
                assert!(
                    paired_socket_profile(protocol, addresses, Some(sender), Some(receiver))
                        .unwrap()
                        .is_some()
                );
                assert!(
                    !negative.admitted(),
                    "valid socket profile cannot qualify {negative:?}"
                );
            }
        }
    }

    let queue = PressureQueue {
        read: 31,
        write: 37,
        received: 41,
        opened: 2,
        closed: 1,
        drops: 0,
        invalid: true,
        ingress: None,
        envelope: None,
    };
    let wire = PressureWireCounts {
        written: 53,
        blocked: true,
        invalid: true,
        syscall_would_block: 59,
        readiness_pending: 61,
    };
    for (phase, ledger_present, queue, wire, before_recheck_sent) in [
        ("initial", Some(true), Some(queue), Some(wire), None),
        ("probe", Some(false), None, Some(wire), None),
        ("recheck", Some(true), None, None, Some(PRESSURE_BYTES)),
        ("recheck", None, None, None, Some(PRESSURE_BYTES - CHUNK)),
    ] {
        let snapshot = PressureCompletionSnapshot {
            phase,
            route: Route {
                protocol: Protocol::H1,
                tls: false,
            },
            target: 7,
            entered: true,
            consumed: CHUNK,
            eom: false,
            action: 0,
            native_cancelled: false,
            invalid: true,
            created: [1, 1, 0],
            dropped: [0, 0, 0],
            pending: 1,
            completed: 0,
            ledger_present,
            baseline: 5,
            queue,
            sent: PRESSURE_BYTES,
            done: true,
            blocked: false,
            wire_baseline: 11,
            wire,
        };
        let checkpoints = PressureCheckpoints {
            admitted: (phase != "initial").then_some(PressureObservation {
                protocol: Protocol::H1,
                target: 7,
                expected: 7,
                consumed: CHUNK,
                received: PRESSURE_WINDOW + CHUNK,
                queued: PRESSURE_WINDOW as i64,
                sent: PRESSURE_WINDOW + 2 * CHUNK,
                wire: PRESSURE_WINDOW + 2 * CHUNK,
                blocked: true,
                held: true,
                valid: true,
                action: 0,
                done: false,
                pending: 1,
            }),
            before_recheck: before_recheck_sent.map(|sent| PressureCompletionSnapshot {
                phase: "before-recheck",
                sent,
                done: false,
                ..snapshot
            }),
        };
        assert_eq!(checkpoints.admitted.is_some(), phase != "initial");
        if let Some(admitted) = checkpoints.admitted {
            assert!(admitted.admitted());
        }
        assert_eq!(
            checkpoints.before_recheck.map(|before| before.sent),
            before_recheck_sent
        );
        let failure = PressureCompletionFailure {
            source: FixtureError::Assertion("upload completed without required pressure"),
            snapshot,
            checkpoints,
            socket_profile: (phase != "initial").then_some(pair),
        };
        assert!(matches!(
            failure
                .source()
                .expect("original pressure assertion remains the typed source")
                .downcast_ref::<FixtureError>(),
            Some(FixtureError::Assertion(
                "upload completed without required pressure"
            ))
        ));
        assert!(failure.to_string().starts_with(
            "native lifecycle assertion: upload completed without required pressure; native-pressure completion-failure "
        ));
        let case = FixtureError::Case {
            route: failure.snapshot.route,
            errors: vec![failure.into()],
        };
        let debug = format!("{case:?}");
        assert!(debug.contains("PressureCompletionFailure"), "{debug}");
        assert!(debug.contains("PressureCompletionSnapshot"), "{debug}");
        assert!(debug.contains(&format!("{checkpoints:?}")), "{debug}");
        assert!(
            debug.contains(&format!(
                "socket_profile: {:?}",
                (phase != "initial").then_some(pair)
            )),
            "{debug}"
        );
        assert!(
            debug.contains("Assertion(\"upload completed without required pressure\")"),
            "{debug}"
        );
        for field in [
            format!("phase: {:?}", snapshot.phase),
            format!("route: {:?}", snapshot.route),
            format!("target: {}", snapshot.target),
            format!("entered: {}", snapshot.entered),
            format!("consumed: {}", snapshot.consumed),
            format!("eom: {}", snapshot.eom),
            format!("action: {}", snapshot.action),
            format!("native_cancelled: {}", snapshot.native_cancelled),
            format!("invalid: {}", snapshot.invalid),
            format!("created: {:?}", snapshot.created),
            format!("dropped: {:?}", snapshot.dropped),
            format!("pending: {}", snapshot.pending),
            format!("completed: {}", snapshot.completed),
            format!("ledger_present: {:?}", snapshot.ledger_present),
            format!("baseline: {}", snapshot.baseline),
            format!("queue: {:?}", snapshot.queue),
            format!("sent: {}", snapshot.sent),
            format!("done: {}", snapshot.done),
            format!("blocked: {}", snapshot.blocked),
            format!("wire_baseline: {}", snapshot.wire_baseline),
            format!("wire: {:?}", snapshot.wire),
        ] {
            assert!(debug.contains(&field), "missing {field}: {debug}");
        }
    }
}

#[test]
fn native_transport_context_matrix() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for route in ROUTES {
        run_case(route, Exercise::Context).unwrap_or_else(|error| panic!("{error:?}"));
    }
}

#[test]
fn native_staged_peer_cancel_matrix() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for route in ROUTES {
        run_case(route, Exercise::Cancellation).unwrap_or_else(|error| panic!("{error:?}"));
    }
}

#[test]
fn native_h1_premature_retirement_negative_control() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let route = Route {
        protocol: Protocol::H1,
        tls: false,
    };
    run_case(route, Exercise::PrematureRetirementControl)
        .unwrap_or_else(|error| panic!("{error:?}"));
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "process-wide FD/thread checkpoints require per-test process isolation (one test per process)"
)]
fn native_response_backing_retirement_control() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for route in ROUTES {
        run_case(route, Exercise::Context)
            .unwrap_or_else(|error| panic!("backing infrastructure warmup {route:?}: {error:?}"));
    }
    run_case(
        Route {
            protocol: Protocol::H1,
            tls: false,
        },
        Exercise::ResponseBacking,
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
}

#[test]
#[cfg_attr(
    not(fbcode_build),
    ignore = "process-wide FD/thread checkpoints require per-test process isolation (one test per process)"
)]
fn native_alive_owner_resource_checkpoints() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    eprintln!("native-resource infrastructure-warmup begin routes=4");
    for route in ROUTES {
        run_case(route, Exercise::Context).unwrap_or_else(|error| {
            panic!("native-resource infrastructure-warmup {route:?}: {error:?}")
        });
    }
    eprintln!("native-resource infrastructure-warmup complete");
    eprintln!("native-resource warmed-envelope measurement begin routes=4");
    for route in ROUTES {
        run_case(route, Exercise::Resources).unwrap_or_else(|error| panic!("{error:?}"));
    }
}
