/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::future::poll_fn;
use std::io;
use std::io::IoSlice;
use std::marker::PhantomPinned;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use axum::Router;
use axum::body::Body as AxumBody;
use axum::extract::State;
use axum::routing::post;
use bytes::Bytes;
use http::HeaderMap;
use http::HeaderValue;
use http::Request;
use http::Response;
use http::StatusCode;
use http::Version;
use http::header::HOST;
use http::header::TE;
use http::header::TRAILER;
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
use roxygen_hyper_adapter::body::RoxygenBody;
use roxygen_hyper_adapter::source::ApplicationBodyError;
use roxygen_runtime::managed::RuntimeConfig;
use roxygen_runtime::managed::RuntimeError;
use roxygen_runtime::managed::RuntimeHandle;
use roxygen_runtime::managed::RuntimeOwner;
use roxygen_runtime::managed::ShutdownReport;
use roxygen_runtime::source_proxy::RemoteSourceError;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_tower_adapter::client::BoxError;
use roxygen_tower_adapter::client::Client;
use roxygen_tower_adapter::client::ClientCompletionError;
use roxygen_tower_adapter::client::ClientError;
use roxygen_tower_adapter::client::ClientExchange;
use roxygen_tower_adapter::client::ClientOptions;
use roxygen_tower_adapter::client::ClientOwner;
use roxygen_tower_adapter::client::ClientShutdownError;
use roxygen_tower_adapter::client::ClientStartError;
use roxygen_tower_adapter::client::ClientStartup;
use roxygen_tower_adapter::client::ResponseFuture;
use roxygen_tower_adapter::client::connector::ConnectRequest;
use roxygen_tower_adapter::client::connector::ConnectedIo;
use roxygen_tower_adapter::client::connector::ConnectorPolicy;
use roxygen_tower_adapter::route::Origin;
use roxygen_tower_adapter::route::ProtocolPolicy;
use roxygen_tower_adapter::route::RouteSpec;
use roxygen_tower_adapter::server::ServerConnection;
use roxygen_tower_adapter::server::ServerError;
use roxygen_tower_adapter::server::ServerExchange;
use roxygen_tower_adapter::server::ServerFailure;
use roxygen_tower_adapter::server::ServerOptions;
use roxygen_tower_adapter::server::ServerStartup;
use roxygen_tower_adapter::server::prepare_connection;
use roxygen_transport::established::EstablishedIo;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tower::Service;
use tower::ServiceExt;
use tower::service_fn;

const OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);
const TRAILER_NAME: &str = "x-public-tail";
const WIRE_SAMPLE_LIMIT: usize = 256;

type ServerOutcome = Result<(), Arc<ServerError>>;
type ClientJoin = Result<Result<(), ClientShutdownError>, JoinError>;
type ServerJoin = Result<ServerOutcome, JoinError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Http1,
    Http2,
}

impl Protocol {
    fn codec(self) -> CodecProtocol {
        match self {
            Self::Http1 => CodecProtocol::Http11,
            Self::Http2 => CodecProtocol::Http2,
        }
    }

    pub fn version(self) -> Version {
        match self {
            Self::Http1 => Version::HTTP_11,
            Self::Http2 => Version::HTTP_2,
        }
    }

    fn policy(self) -> ProtocolPolicy {
        match self {
            Self::Http1 => ProtocolPolicy::Http1Only,
            Self::Http2 => ProtocolPolicy::Http2Only,
        }
    }

    fn wire_prefix(self) -> &'static [u8] {
        match self {
            Self::Http1 => b"POST /stream HTTP/1.1\r\n",
            Self::Http2 => b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    Streaming,
    BodyError,
    StartupFailure,
    Cancellation,
}

impl Scenario {
    fn prefix(self) -> &'static [u8] {
        match self {
            Self::BodyError => b"response-prefix",
            Self::Streaming | Self::Cancellation => b"public-",
            Self::StartupFailure => b"",
        }
    }
}

#[derive(Clone, Copy, Debug)]
/// A fault to inject into an otherwise successful network report, which the
/// verifier must then reject.
pub enum VerifierFault {
    WrongRuntimeStartup,
    ProjectionRun,
    ReadTimeoutBody,
    ProtocolErrorBody,
    ReadTimeoutRun,
    ProtocolErrorRun,
    UploadGateLost,
    PrematurePrefix,
    MissingClientCleanup,
    FailedClientCleanup,
    MissingBodyError,
    FalseBodyEof,
    MissingClientSubmission,
    UnconfirmedServerExchangeCleanup,
    UnconfirmedServerRunCleanup,
    ServerExchangePanic,
    ServerRunPanic,
}

#[derive(Clone, Debug, Default)]
/// The byte count and up to the first 256 bytes of one socket direction.
pub struct WireSample {
    pub bytes: u64,
    pub prefix: Bytes,
}

impl WireSample {
    fn record(&mut self, bytes: &[u8]) {
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        let count = bytes.len().min(WIRE_SAMPLE_LIMIT - self.prefix.len());
        if count != 0 {
            let mut prefix = self.prefix.to_vec();
            prefix.extend_from_slice(&bytes[..count]);
            self.prefix = Bytes::from(prefix);
        }
    }
}

#[derive(Clone, Debug, Default)]
/// Observations of delegated Tokio I/O polls, without buffering or driving HTTP.
pub struct IoProgress {
    pub read: WireSample,
    pub written: WireSample,
    pub read_polls: u64,
    pub read_pending: u64,
    pub write_polls: u64,
    pub write_pending: u64,
    pub flush_polls: u64,
    pub flush_pending: u64,
    pub read_error: bool,
    pub write_error: bool,
    pub read_eof: bool,
}

#[derive(Clone, Debug, Default)]
/// A body producer's polls, gate transitions, and destruction.
pub struct ProducerProgress {
    pub polls: u64,
    pub data_bytes_returned: u64,
    pub pending: u64,
    pub gate_opened: bool,
    pub gate_sender_lost: bool,
    pub retired: bool,
}

#[derive(Clone, Debug, Default)]
/// Fixed-size phase observations, also sampled before a timed operation is dropped.
pub struct Progress {
    pub handler_returned_response: bool,
    pub upload: ProducerProgress,
    pub response: ProducerProgress,
    pub echo: ProducerProgress,
    pub client_response_polls: u64,
    pub client_response_pending: u64,
    pub client_response_ready: bool,
    pub client_response_failed: bool,
    pub client_body_frame_waits: u64,
    pub prefix_bytes_received: usize,
    pub nonempty_prefix_frames: usize,
    pub client_cancel_requested: bool,
    pub server_cancel_requested: bool,
    pub upload_gate_held_through_retirement: bool,
    pub client_io: IoProgress,
    pub server_io: IoProgress,
}

#[derive(Clone, Copy)]
enum BodyRole {
    Upload,
    Response,
}

impl Progress {
    fn producer(&mut self, role: BodyRole) -> &mut ProducerProgress {
        match role {
            BodyRole::Upload => &mut self.upload,
            BodyRole::Response => &mut self.response,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    NativeStartedOutsideTokio,
    ClientRunRegistered,
    ClientStartupPolled,
    ServerRunRegistered,
    ServerStartupPolled,
    AdmissionClosed,
    ClientRunJoined,
    ServerRunJoined,
    NativeQuiesced,
    ReactorChecked,
    NativeJoinedOutsideTokio,
    ApplicationRuntimeDropped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameKind {
    Data,
    Trailers,
    Other,
}

#[derive(Debug, Default)]
pub struct Frames {
    pub data: Vec<Bytes>,
    pub trailers: Vec<HeaderMap>,
    pub order: Vec<FrameKind>,
    pub eof: bool,
}

impl Frames {
    fn record(&mut self, frame: &Frame<Bytes>) {
        if let Some(data) = frame.data_ref() {
            self.data.push(data.clone());
            self.order.push(FrameKind::Data);
        } else if let Some(trailers) = frame.trailers_ref() {
            self.trailers.push(trailers.clone());
            self.order.push(FrameKind::Trailers);
        } else {
            self.order.push(FrameKind::Other);
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.data
            .iter()
            .flat_map(|data| data.iter().copied())
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct Observations {
    pub upload: Frames,
    pub download: Frames,
    pub response_error: Option<BoxError>,
    pub request_error: bool,
    pub request_version: Option<Version>,
    pub facade_request_version: Option<Version>,
    pub response_version: Option<Version>,
    pub request_host: Option<HeaderValue>,
    pub request_uri: Option<http::Uri>,
    pub listener: Option<SocketAddr>,
    pub dial_local: Option<SocketAddr>,
    pub dial_peer: Option<SocketAddr>,
    pub accepted_local: Option<SocketAddr>,
    pub accepted_peer: Option<SocketAddr>,
    pub dial_attempts: usize,
    pub wire_prefix: Vec<u8>,
    pub context_matches_endpoints: bool,
    pub first_response_before_upload_release: bool,
    pub explicit_cancellation: bool,
    pub bodies_created: usize,
    pub bodies_dropped: usize,
    pub body_polls: usize,
    pub wrong_body_thread: bool,
    pub body_after_runtime_drop: bool,
    pub bodies_destroyed_before_runtime_drop: bool,
    pub closed_worker_before_startup: bool,
}

#[derive(Debug, Default)]
pub struct Phases {
    pub operation: Option<Result<(), BoxError>>,
    pub timeout_progress: Option<Progress>,
    pub accept: Option<Result<(), BoxError>>,
    pub request: Option<Result<(), BoxError>>,
    pub client_startup: Option<Result<(), ClientStartError>>,
    pub server_startup: Option<Result<(), ServerError>>,
    pub early_quiescence: Option<Result<(), RuntimeError>>,
    pub client_exchange: Option<Result<(), ClientCompletionError>>,
    pub server_exchange: Option<ServerOutcome>,
    pub client_run: Option<ClientJoin>,
    pub server_run: Option<ServerJoin>,
    pub server_reaped_before_client_shutdown: bool,
    pub client_submission_retained_until_shutdown: bool,
    pub native_quiescence: Option<Result<(), RuntimeError>>,
    pub reactor_check: Option<Result<(), BoxError>>,
    pub native_join: Option<Result<ShutdownReport, RuntimeError>>,
}

struct RetainedBootstrap {
    _native_owner: RuntimeOwner,
    _application_runtime: Runtime,
    _credits: Vec<OwnedSemaphorePermit>,
    _gate_sender: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for RetainedBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedBootstrap(unconfirmed physical join)")
    }
}

#[must_use = "Inspect all outcomes and retain any unconfirmed bootstrap owners"]
#[derive(Debug)]
pub struct Report {
    pub protocol: Protocol,
    pub scenario: Scenario,
    pub phases: Phases,
    pub observations: Observations,
    pub progress: Progress,
    pub events: Vec<Event>,
    pub admission_closed: bool,
    retained_bootstrap: Option<RetainedBootstrap>,
}

impl Report {
    pub fn has_retained_owners(&self) -> bool {
        self.retained_bootstrap.is_some()
    }

    pub fn verify(&self) -> Result<(), BoxError> {
        require(succeeded(&self.phases.operation), "scenario did not finish")?;
        require(self.admission_closed, "outer run admission was not closed")?;
        require(
            !self.has_retained_owners(),
            "native physical join is unconfirmed",
        )?;
        require(
            succeeded(&self.phases.native_quiescence),
            "native quiescence failed",
        )?;
        require(
            succeeded(&self.phases.reactor_check),
            "post-quiescence TCP probe failed",
        )?;
        require(
            matches!(&self.phases.native_join, Some(Ok(report)) if report.workers_joined == 1),
            "the native worker was not physically joined",
        )?;
        for (earlier, later) in [
            (Event::ClientRunRegistered, Event::ClientStartupPolled),
            (Event::ServerRunRegistered, Event::ServerStartupPolled),
            (Event::ClientRunJoined, Event::NativeJoinedOutsideTokio),
            (Event::ServerRunJoined, Event::NativeJoinedOutsideTokio),
            (Event::NativeQuiesced, Event::ReactorChecked),
            (Event::ReactorChecked, Event::NativeJoinedOutsideTokio),
            (
                Event::NativeJoinedOutsideTokio,
                Event::ApplicationRuntimeDropped,
            ),
        ] {
            require(
                self.precedes(earlier, later),
                "execution-owner ordering failed",
            )?;
        }
        require(
            matches!(&self.phases.client_run, Some(Ok(_))),
            "client parent was not reaped",
        )?;
        require(
            matches!(&self.phases.server_run, Some(Ok(_))),
            "server parent was not reaped",
        )?;
        if let Some(Ok(Err(error))) = &self.phases.client_run {
            require(
                !matches!(
                    &error.0,
                    ClientError::CleanupUnconfirmed | ClientError::Panicked
                ),
                "client run reports unconfirmed cleanup or panic",
            )?;
        }
        if let Some(Ok(Err(error))) = &self.phases.server_run {
            require(
                error.cleanup_confirmed(),
                "server run cleanup is unconfirmed",
            )?;
            require(error.first_panic().is_none(), "server run caught a panic")?;
        }
        require(
            !self.observations.wrong_body_thread,
            "body left the application thread",
        )?;
        require(
            !self.observations.body_after_runtime_drop,
            "body outlived the reactor",
        )?;
        require(
            self.observations.bodies_destroyed_before_runtime_drop,
            "runtime destruction was used to destroy application bodies",
        )?;
        require(
            self.observations.bodies_created == self.observations.bodies_dropped,
            "application body destruction is incomplete",
        )?;
        if self.scenario == Scenario::StartupFailure {
            require(
                succeeded(&self.phases.early_quiescence),
                "startup failure setup failed",
            )?;
            require(
                self.observations.closed_worker_before_startup,
                "startup was not activated against the quiesced worker",
            )?;
            require(
                matches!(
                    &self.phases.client_startup,
                    Some(Err(ClientStartError::Client(ClientError::Closed)))
                ),
                "client startup did not report closed admission",
            )?;
            require(
                matches!(&self.phases.server_startup, Some(Err(error)) if matches!(error.cause(), ServerFailure::Admission)),
                "server startup did not report closed admission",
            )?;
            require(
                matches!(
                    &self.phases.client_run,
                    Some(Ok(Err(ClientShutdownError(ClientError::Closed))))
                ),
                "failed-startup client Run did not report closed admission",
            )?;
            require(
                matches!(&self.phases.server_run, Some(Ok(Err(error))) if matches!(ServerError::cause(error.as_ref()), ServerFailure::Admission)),
                "failed-startup server Run did not report closed admission",
            )?;
            return require(
                self.observations.dial_attempts == 0,
                "dormant client dialed",
            );
        }
        require(
            succeeded(&self.phases.client_startup),
            "client startup failed",
        )?;
        require(
            succeeded(&self.phases.server_startup),
            "server startup failed",
        )?;
        require(succeeded(&self.phases.accept), "accept/start failed")?;
        require(succeeded(&self.phases.request), "Tower request failed")?;
        require(
            self.progress.prefix_bytes_received == self.scenario.prefix().len()
                && self.progress.nonempty_prefix_frames == self.scenario.prefix().len(),
            "the complete nonempty prefix was not consumed in one-byte public reads",
        )?;
        require(
            !self.progress.upload.gate_sender_lost && !self.progress.response.gate_sender_lost,
            "sender loss, not the intended scenario, terminated a producer",
        )?;
        require(
            self.observations.dial_attempts == 1,
            "unexpected replay or connection count",
        )?;
        require(
            self.observations.wire_prefix == self.protocol.wire_prefix(),
            "native HTTP wire prefix disagrees with protocol",
        )?;
        require(
            self.observations.context_matches_endpoints,
            "connection context lost transport facts",
        )?;
        require(
            self.observations.dial_local == self.observations.accepted_peer
                && self.observations.dial_peer == self.observations.accepted_local
                && self.observations.dial_peer == self.observations.listener,
            "TCP endpoint observations disagree",
        )?;
        let authority_matches = self.observations.listener.is_some_and(|address| {
            let expected = address.to_string();
            match &self.observations.request_host {
                Some(host) => host.to_str().is_ok_and(|host| host == expected),
                None => {
                    self.protocol == Protocol::Http2
                        && self
                            .observations
                            .request_uri
                            .as_ref()
                            .and_then(http::Uri::authority)
                            .is_some_and(|authority| authority.as_str() == expected)
                }
            }
        });
        require(
            authority_matches,
            "explicit route authority did not reach Axum",
        )?;
        require(
            self.observations.facade_request_version == Some(self.protocol.version()),
            "server facade delivered a head version inconsistent with the explicit wire protocol",
        )?;
        require(
            self.observations.request_version == Some(self.protocol.version())
                && self.observations.response_version == Some(self.protocol.version()),
            "public HTTP head version disagrees with the explicit wire protocol",
        )?;
        require(
            self.observations.bodies_created == 2,
            "the actual upload/response producers did not run",
        )?;
        require(
            self.observations.body_polls > 0,
            "no application body was polled",
        )?;
        require(
            self.phases.client_submission_retained_until_shutdown,
            "client submission handle was lost before deliberate shutdown",
        )?;
        if let Some(Err(error)) = &self.phases.client_exchange {
            require(
                !error.cleanup_unconfirmed(),
                "client exchange cleanup is unconfirmed",
            )?;
        }
        if let Some(Err(error)) = &self.phases.server_exchange {
            require(
                error.cleanup_confirmed(),
                "server exchange cleanup is unconfirmed",
            )?;
            require(
                error.first_panic().is_none(),
                "server exchange caught a panic",
            )?;
        }
        match self.scenario {
            Scenario::Streaming => {
                require(
                    self.phases.server_reaped_before_client_shutdown,
                    "server did not finish draining before client shutdown",
                )?;
                require(
                    self.observations.first_response_before_upload_release,
                    "upload was not streamed bidirectionally",
                )?;
                require(
                    self.observations.upload.bytes() == b"public-stream",
                    "upload data changed",
                )?;
                require(
                    self.observations.download.bytes() == b"public-stream",
                    "echo data changed",
                )?;
                for frames in [&self.observations.upload, &self.observations.download] {
                    require(
                        frames.trailers.len() == 1
                            && frames.trailers[0].get(TRAILER_NAME)
                                == Some(&HeaderValue::from_static("verified")),
                        "streaming trailers were lost or changed",
                    )?;
                    require(
                        frames.order.last() == Some(&FrameKind::Trailers)
                            && !frames.order.contains(&FrameKind::Other),
                        "frame/trailer ordering changed",
                    )?;
                }
                require(
                    self.observations.download.eof && self.observations.response_error.is_none(),
                    "stream did not end cleanly",
                )?;
                require(
                    succeeded(&self.phases.client_exchange),
                    "client exchange did not complete cleanly",
                )?;
                require(
                    succeeded(&self.phases.server_exchange),
                    "server exchange did not complete cleanly",
                )?;
                require(
                    matches!(&self.phases.client_run, Some(Ok(Ok(())))),
                    "client run failed",
                )?;
                require(
                    matches!(&self.phases.server_run, Some(Ok(Ok(())))),
                    "server run failed",
                )
            }
            Scenario::BodyError | Scenario::Cancellation => {
                require(
                    self.observations
                        .response_error
                        .as_deref()
                        .is_some_and(|error| {
                            expected_response_failure(error, self.protocol, self.scenario)
                        }),
                    "response body has an unrelated or missing failure",
                )?;
                require(
                    !self.observations.download.eof,
                    "failed body reported successful EOF",
                )?;
                if self.scenario == Scenario::BodyError {
                    require_successful_client_cleanup(self.phases.client_exchange.as_ref())?;
                } else {
                    require(
                        matches!(&self.phases.client_exchange, Some(Err(error)) if expected_client_exchange_failure(error.cause(), self.protocol, self.scenario)),
                        "client exchange has an unrelated or missing failure",
                    )?;
                }
                require(
                    matches!(&self.phases.server_exchange, Some(Err(error)) if expected_server_failure(ServerError::cause(error.as_ref()), self.protocol, self.scenario)),
                    "server exchange has an unrelated or missing failure",
                )?;
                self.verify_failed_scenario_runs()?;
                if self.scenario == Scenario::BodyError {
                    require(
                        self.observations.download.bytes() == b"response-prefix",
                        "data before the body error was lost",
                    )?;
                    require(
                        self.progress.response.gate_opened && self.progress.response.retired,
                        "injected producer did not cross its acknowledged-prefix gate and retire",
                    )
                } else {
                    require(
                        self.observations.explicit_cancellation,
                        "no explicit exchange cancellation occurred",
                    )?;
                    require(
                        self.progress.client_cancel_requested
                            && self.progress.server_cancel_requested
                            && self.progress.upload_gate_held_through_retirement,
                        "client cancellation did not retire the producer with its gate sender held",
                    )?;
                    require(
                        self.observations.download.bytes() == b"public-",
                        "cancellation did not interrupt a live stream",
                    )
                }
            }
            Scenario::StartupFailure => unreachable!(),
        }
    }

    fn verify_failed_scenario_runs(&self) -> Result<(), BoxError> {
        if self.protocol == Protocol::Http2 {
            require(
                matches!(&self.phases.client_run, Some(Ok(Ok(())))),
                "stream-local H2 failure unexpectedly failed ClientRun",
            )?;
        } else if self.scenario == Scenario::BodyError {
            require(
                matches!(&self.phases.client_run, Some(Ok(Err(ClientShutdownError(ClientError::Pool(PoolError::SessionJoin { source })))))
                    if source.code() == HttpErrorCode::TRANSPORT_EOF),
                "H1 body-error ClientRun did not retain the expected session EOF",
            )?;
        } else {
            require(
                matches!(&self.phases.client_run, Some(Ok(Err(ClientShutdownError(ClientError::Pool(PoolError::SessionJoin { source })))))
                    if peer_termination(source, Protocol::Http1, self.scenario)
                        || (self.scenario == Scenario::Cancellation && local_cancellation(source))),
                "H1 ClientRun did not report the intended session termination",
            )?;
        }
        if self.protocol == Protocol::Http2 && self.scenario == Scenario::Cancellation {
            let (exchange, run) = self.h2_cancellation_server_errors()?;
            require_h2_cancelled_server_errors(exchange, run)?;
            return require(
                matches!(&self.phases.native_join, Some(Ok(report))
                    if report.workers_joined == 1 && report.workers_forced == 0),
                "H2 cancellation requires one physically joined worker and zero forced workers",
            );
        }
        require(
            matches!(&self.phases.server_run, Some(Ok(Err(error)))
                if expected_server_failure(ServerError::cause(error.as_ref()), self.protocol, self.scenario)),
            "ServerRun did not retain its scenario-specific cause",
        )
    }

    fn h2_cancellation_server_errors(
        &self,
    ) -> Result<(ServerFailureObservation<'_>, ServerFailureObservation<'_>), BoxError> {
        require(
            self.protocol == Protocol::Http2 && self.scenario == Scenario::Cancellation,
            "wrong retained H2 cancellation control scenario",
        )?;
        match (&self.phases.server_exchange, &self.phases.server_run) {
            (Some(Err(exchange_error)), Some(Ok(Err(run_error)))) => Ok((
                ServerFailureObservation::observe(exchange_error.as_ref()),
                ServerFailureObservation::observe(run_error.as_ref()),
            )),
            _ => Err(io::Error::other(
                "H2 cancellation requires an actual failed server exchange and reaped failed ServerRun",
            )
            .into()),
        }
    }

    /// Verifies the report, then checks that the verifier rejects it with `fault` injected.
    pub fn verify_rejects(&mut self, fault: VerifierFault) -> Result<(), BoxError> {
        self.verify()?;
        let rejected = match fault {
            VerifierFault::UnconfirmedServerExchangeCleanup => {
                let (mut exchange, run) = self.h2_cancellation_server_errors()?;
                exchange.cleanup_confirmed = false;
                require_h2_cancelled_server_errors(exchange, run).is_err()
            }
            VerifierFault::UnconfirmedServerRunCleanup => {
                let (exchange, mut run) = self.h2_cancellation_server_errors()?;
                run.cleanup_confirmed = false;
                require_h2_cancelled_server_errors(exchange, run).is_err()
            }
            VerifierFault::ServerExchangePanic => {
                let (mut exchange, run) = self.h2_cancellation_server_errors()?;
                exchange.panic_present = true;
                require_h2_cancelled_server_errors(exchange, run).is_err()
            }
            VerifierFault::ServerRunPanic => {
                let (exchange, mut run) = self.h2_cancellation_server_errors()?;
                run.panic_present = true;
                require_h2_cancelled_server_errors(exchange, run).is_err()
            }
            VerifierFault::MissingClientCleanup => {
                require(
                    self.scenario == Scenario::BodyError,
                    "wrong cleanup control scenario",
                )?;
                let original = self.phases.client_exchange.take();
                let rejected = self.verify().is_err();
                self.phases.client_exchange = original;
                rejected
            }
            VerifierFault::FailedClientCleanup => {
                require(
                    self.scenario == Scenario::BodyError,
                    "wrong cleanup control scenario",
                )?;
                [ClientError::Cancelled, ClientError::CleanupUnconfirmed]
                    .into_iter()
                    .all(|error| {
                        require_successful_client_cleanup(Some(&Err::<(), _>(error))).is_err()
                    })
            }
            VerifierFault::MissingBodyError => {
                require(
                    self.scenario == Scenario::BodyError,
                    "wrong body-error control scenario",
                )?;
                let original = self.observations.response_error.take();
                let rejected = self.verify().is_err();
                self.observations.response_error = original;
                rejected
            }
            VerifierFault::FalseBodyEof => {
                require(
                    self.scenario == Scenario::BodyError,
                    "wrong body-error control scenario",
                )?;
                let original = self.observations.download.eof;
                self.observations.download.eof = true;
                let rejected = self.verify().is_err();
                self.observations.download.eof = original;
                rejected
            }
            VerifierFault::MissingClientSubmission => {
                require(
                    self.scenario != Scenario::StartupFailure,
                    "startup has no client submission handle",
                )?;
                let original = self.phases.client_submission_retained_until_shutdown;
                self.phases.client_submission_retained_until_shutdown = false;
                let rejected = self.verify().is_err();
                self.phases.client_submission_retained_until_shutdown = original;
                rejected
            }
            VerifierFault::WrongRuntimeStartup => {
                require(
                    self.scenario == Scenario::StartupFailure,
                    "wrong startup control scenario",
                )?;
                let original = self
                    .phases
                    .client_startup
                    .replace(Err(ClientStartError::Client(ClientError::WrongRuntime)));
                let rejected = self.verify().is_err();
                self.phases.client_startup = original;
                rejected
            }
            VerifierFault::ProjectionRun => {
                let original = self
                    .phases
                    .client_run
                    .replace(Ok(Err(ClientShutdownError(ClientError::Projection))));
                let rejected = self.verify().is_err();
                self.phases.client_run = original;
                rejected
            }
            VerifierFault::ReadTimeoutRun | VerifierFault::ProtocolErrorRun => {
                let code = if matches!(fault, VerifierFault::ReadTimeoutRun) {
                    HttpErrorCode::READ_TIMEOUT
                } else {
                    HttpErrorCode::PROTOCOL_ERROR
                };
                let original = self.phases.client_run.replace(Ok(Err(ClientShutdownError(
                    ClientError::Pool(PoolError::SessionJoin {
                        source: HttpError::new(code),
                    }),
                ))));
                let rejected = self.verify().is_err();
                self.phases.client_run = original;
                rejected
            }
            VerifierFault::UploadGateLost => {
                require(
                    self.scenario == Scenario::Cancellation,
                    "wrong gate-loss control scenario",
                )?;
                let original = self.progress.upload.gate_sender_lost;
                self.progress.upload.gate_sender_lost = true;
                let rejected = self.verify().is_err();
                self.progress.upload.gate_sender_lost = original;
                rejected
            }
            VerifierFault::ReadTimeoutBody | VerifierFault::ProtocolErrorBody => {
                require(
                    matches!(self.scenario, Scenario::BodyError | Scenario::Cancellation),
                    "wrong failed-body control scenario",
                )?;
                let code = if matches!(fault, VerifierFault::ReadTimeoutBody) {
                    HttpErrorCode::READ_TIMEOUT
                } else {
                    HttpErrorCode::PROTOCOL_ERROR
                };
                let original = self
                    .observations
                    .response_error
                    .replace(Box::new(RemoteSourceError::Source(HttpError::new(code))));
                let rejected = self.verify().is_err();
                self.observations.response_error = original;
                rejected
            }
            VerifierFault::PrematurePrefix => {
                require(
                    self.scenario != Scenario::StartupFailure,
                    "startup has no DATA prefix",
                )?;
                let original = self.progress.prefix_bytes_received;
                self.progress.prefix_bytes_received = original.saturating_sub(1);
                let rejected = self.verify().is_err();
                self.progress.prefix_bytes_received = original;
                rejected
            }
        };
        require(
            rejected,
            "verifier accepted a deliberately unrelated outcome",
        )
    }

    fn precedes(&self, earlier: Event, later: Event) -> bool {
        match (
            self.events.iter().position(|event| *event == earlier),
            self.events.iter().position(|event| *event == later),
        ) {
            (Some(first), Some(second)) => first < second,
            _ => false,
        }
    }
}

struct Probe {
    scenario: Scenario,
    application_thread: ThreadId,
    reactor_alive: AtomicBool,
    observed: Mutex<Observations>,
    progress: Mutex<Progress>,
    events: Mutex<Vec<Event>>,
    server_exchange: Mutex<Option<ServerExchange>>,
    response_gate: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Probe {
    fn event(&self, event: Event) {
        self.events.lock().expect("event lock").push(event);
    }

    fn body_created(&self) {
        self.observed
            .lock()
            .expect("observation lock")
            .bodies_created += 1;
    }

    fn body_activity(&self, dropped: bool) {
        let mut observed = self.observed.lock().expect("observation lock");
        observed.wrong_body_thread |= std::thread::current().id() != self.application_thread;
        observed.body_after_runtime_drop |= !self.reactor_alive.load(Ordering::SeqCst);
        if dropped {
            observed.bodies_dropped += 1;
        } else {
            observed.body_polls += 1;
        }
    }
}

struct ScriptState {
    frames: VecDeque<Result<Frame<Bytes>, BoxError>>,
    gate: Option<oneshot::Receiver<()>>,
    first_sent: bool,
    ended: bool,
}

struct ScriptBody {
    state: RefCell<ScriptState>,
    probe: Arc<Probe>,
    role: BodyRole,
    _pinned: PhantomPinned,
}

impl ScriptBody {
    fn new(
        probe: Arc<Probe>,
        role: BodyRole,
        frames: Vec<Result<Frame<Bytes>, BoxError>>,
        gate: Option<oneshot::Receiver<()>>,
    ) -> Self {
        probe.body_created();
        Self {
            state: RefCell::new(ScriptState {
                ended: frames.is_empty(),
                frames: frames.into(),
                gate,
                first_sent: false,
            }),
            probe,
            role,
            _pinned: PhantomPinned,
        }
    }
}

impl Body for ScriptBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let body = self.as_ref().get_ref();
        body.probe.body_activity(false);
        {
            let mut progress = body.probe.progress.lock().expect("progress lock");
            let producer = progress.producer(body.role);
            producer.polls = producer.polls.saturating_add(1);
        }
        let mut state = body.state.borrow_mut();
        if state.ended {
            return Poll::Ready(None);
        }
        if state.first_sent {
            if let Some(gate) = &mut state.gate {
                match Pin::new(gate).poll(context) {
                    Poll::Pending => {
                        let mut progress = body.probe.progress.lock().expect("progress lock");
                        let producer = progress.producer(body.role);
                        producer.pending = producer.pending.saturating_add(1);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(())) => {
                        state.gate = None;
                        body.probe
                            .progress
                            .lock()
                            .expect("progress lock")
                            .producer(body.role)
                            .gate_opened = true;
                    }
                    Poll::Ready(Err(error)) => {
                        body.probe
                            .progress
                            .lock()
                            .expect("progress lock")
                            .producer(body.role)
                            .gate_sender_lost = true;
                        state.gate = None;
                        state.frames.clear();
                        state.ended = true;
                        return Poll::Ready(Some(Err(Box::new(error))));
                    }
                }
            }
        }
        let frame = state.frames.pop_front();
        if let Some(Ok(frame)) = &frame {
            if let Some(data) = frame.data_ref() {
                let mut progress = body.probe.progress.lock().expect("progress lock");
                let producer = progress.producer(body.role);
                producer.data_bytes_returned = producer
                    .data_bytes_returned
                    .saturating_add(data.len() as u64);
            }
        }
        state.first_sent = true;
        state.ended = state.frames.is_empty();
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.state.borrow().ended
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::new()
    }
}

impl Drop for ScriptBody {
    fn drop(&mut self) {
        self.probe
            .progress
            .lock()
            .expect("progress lock")
            .producer(self.role)
            .retired = true;
        self.probe.body_activity(true);
    }
}

struct EchoBody {
    input: RefCell<Pin<Box<AxumBody>>>,
    probe: Arc<Probe>,
    _pinned: PhantomPinned,
}

impl Body for EchoBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let body = self.as_ref().get_ref();
        body.probe.body_activity(false);
        {
            let mut progress = body.probe.progress.lock().expect("progress lock");
            progress.echo.polls = progress.echo.polls.saturating_add(1);
        }
        match body.input.borrow_mut().as_mut().poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                body.probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .upload
                    .record(&frame);
                if let Some(data) = frame.data_ref() {
                    let mut progress = body.probe.progress.lock().expect("progress lock");
                    progress.echo.data_bytes_returned = progress
                        .echo
                        .data_bytes_returned
                        .saturating_add(data.len() as u64);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                body.probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .request_error = true;
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Ready(None) => {
                body.probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .upload
                    .eof = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                let mut progress = body.probe.progress.lock().expect("progress lock");
                progress.echo.pending = progress.echo.pending.saturating_add(1);
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.input.borrow().is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.input.borrow().size_hint()
    }
}

impl Drop for EchoBody {
    fn drop(&mut self) {
        self.probe
            .progress
            .lock()
            .expect("progress lock")
            .echo
            .retired = true;
        self.probe.body_activity(true);
    }
}

#[derive(Debug)]
struct InjectedBodyError;

impl fmt::Display for InjectedBodyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("intentional public qualification response-body failure")
    }
}

impl Error for InjectedBodyError {}

async fn echo(State(probe): State<Arc<Probe>>, request: Request<AxumBody>) -> Response<AxumBody> {
    let version = request.version();
    *probe.server_exchange.lock().expect("exchange lock") =
        request.extensions().get::<ServerExchange>().cloned();
    {
        let mut observed = probe.observed.lock().expect("observation lock");
        observed.request_version = Some(version);
        observed.request_host = request.headers().get(HOST).cloned();
        observed.request_uri = Some(request.uri().clone());
    }
    let body = if probe.scenario == Scenario::BodyError {
        let gate = probe.response_gate.lock().expect("gate lock").take();
        AxumBody::new(ScriptBody::new(
            probe.clone(),
            BodyRole::Response,
            vec![
                Ok(Frame::data(Bytes::from_static(b"response-prefix"))),
                Err(Box::new(InjectedBodyError)),
            ],
            gate,
        ))
    } else {
        probe.body_created();
        AxumBody::new(EchoBody {
            input: RefCell::new(Box::pin(request.into_body())),
            probe: probe.clone(),
            _pinned: PhantomPinned,
        })
    };
    let mut response = Response::new(body);
    *response.version_mut() = version;
    response
        .headers_mut()
        .insert(TRAILER, HeaderValue::from_static(TRAILER_NAME));
    probe
        .progress
        .lock()
        .expect("progress lock")
        .handler_returned_response = true;
    response
}

struct Parents {
    admission: Arc<Semaphore>,
    client_credit: Option<OwnedSemaphorePermit>,
    server_credit: Option<OwnedSemaphorePermit>,
    client_run: Option<JoinHandle<Result<(), ClientShutdownError>>>,
    server_run: Option<JoinHandle<ServerOutcome>>,
    client_owner: Option<ClientOwner>,
    client_submission: Option<Client>,
    server_connection: Option<ServerConnection>,
    client_exchange: Option<ClientExchange>,
}

impl Parents {
    fn new() -> Self {
        Self {
            admission: Arc::new(Semaphore::new(2)),
            client_credit: None,
            server_credit: None,
            client_run: None,
            server_run: None,
            client_owner: None,
            client_submission: None,
            server_connection: None,
            client_exchange: None,
        }
    }

    fn take_credits(&mut self) -> Vec<OwnedSemaphorePermit> {
        self.client_credit
            .take()
            .into_iter()
            .chain(self.server_credit.take())
            .collect()
    }
}

fn endpoint(address: SocketAddr) -> TransportEndpoint {
    TransportEndpoint::Socket(SocketAddress::new(address.ip().to_string(), address.port()))
}

#[derive(Clone, Copy)]
enum IoSide {
    Client,
    Server,
}

struct ObservedIo {
    stream: tokio::net::TcpStream,
    probe: Arc<Probe>,
    side: IoSide,
}

impl ObservedIo {
    fn record(&self, update: impl FnOnce(&mut IoProgress)) {
        let mut progress = self.probe.progress.lock().expect("progress lock");
        update(match self.side {
            IoSide::Client => &mut progress.client_io,
            IoSide::Server => &mut progress.server_io,
        });
    }

    fn record_write(&self, result: &Poll<io::Result<usize>>, buffers: &[IoSlice<'_>]) {
        self.record(|progress| {
            progress.write_polls += 1;
            match result {
                Poll::Pending => progress.write_pending += 1,
                Poll::Ready(Err(_)) => progress.write_error = true,
                Poll::Ready(Ok(written)) => {
                    let mut remaining = *written;
                    for buffer in buffers {
                        let count = remaining.min(buffer.len());
                        progress.written.record(&buffer[..count]);
                        remaining -= count;
                        if remaining == 0 {
                            break;
                        }
                    }
                }
            }
        });
    }
}

impl AsyncRead for ObservedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let capacity = buffer.remaining();
        let result = Pin::new(&mut this.stream).poll_read(context, buffer);
        this.record(|progress| {
            progress.read_polls += 1;
            match &result {
                Poll::Pending => progress.read_pending += 1,
                Poll::Ready(Err(_)) => progress.read_error = true,
                Poll::Ready(Ok(())) => {
                    let bytes = &buffer.filled()[before..];
                    progress.read.record(bytes);
                    progress.read_eof |= capacity != 0 && bytes.is_empty();
                }
            }
        });
        result
    }
}

impl AsyncWrite for ObservedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.stream).poll_write(context, buffer);
        this.record_write(&result, &[IoSlice::new(buffer)]);
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.stream).poll_write_vectored(context, buffers);
        this.record_write(&result, buffers);
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.stream).poll_flush(context);
        this.record(|progress| {
            progress.flush_polls += 1;
            progress.flush_pending += u64::from(result.is_pending());
            progress.write_error |= matches!(&result, Poll::Ready(Err(_)));
        });
        result
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}

fn established(
    stream: tokio::net::TcpStream,
    probe: &Arc<Probe>,
    side: IoSide,
) -> io::Result<EstablishedIo<ObservedIo>> {
    let local = endpoint(stream.local_addr()?);
    let peer = endpoint(stream.peer_addr()?);
    Ok(EstablishedIo::new(
        ObservedIo {
            stream,
            probe: probe.clone(),
            side,
        },
        local,
        peer,
        None,
    ))
}

fn prepare_client(
    worker: &WorkerHandle,
    address: SocketAddr,
    protocol: Protocol,
    probe: &Arc<Probe>,
    parents: &mut Parents,
) -> Result<ClientStartup, BoxError> {
    require(
        parents.client_run.is_none(),
        "client run slot already occupied",
    )?;
    let credit = parents.admission.clone().try_acquire_owned()?;
    let origin: Origin = format!("http://{address}").parse()?;
    let expected = RouteSpec::direct(origin, protocol.policy());
    let connector_probe = probe.clone();
    let connector = service_fn(move |request: ConnectRequest| {
        let expected = expected.clone();
        let probe = connector_probe.clone();
        async move {
            require(
                request.route() == &expected,
                "connector received another route",
            )?;
            require(
                !request.cancellation().is_cancellation_requested(),
                "dial already cancelled",
            )?;
            {
                let mut observed = probe.observed.lock().expect("observation lock");
                observed.dial_attempts += 1;
                require(
                    observed.dial_attempts == 1,
                    "one-connection example refuses replay",
                )?;
            }
            let stream =
                timeout(request.timeout(), tokio::net::TcpStream::connect(address)).await??;
            require(
                !request.cancellation().is_cancellation_requested(),
                "dial cancelled",
            )?;
            {
                let mut observed = probe.observed.lock().expect("observation lock");
                observed.dial_local = Some(stream.local_addr()?);
                observed.dial_peer = Some(stream.peer_addr()?);
            }
            Ok::<_, BoxError>(ConnectedIo::new(
                expected,
                established(stream, &probe, IoSide::Client)?,
                protocol.codec(),
                TransportSecurity::Plaintext,
            ))
        }
    });
    let mut options = ClientOptions {
        maximum_exchanges: 1,
        command_capacity: 4,
        maximum_connector_tasks: 1,
        maximum_physical_connections: 1,
        maximum_routes: 1,
        connector_policy: ConnectorPolicy::Direct,
        ..ClientOptions::default()
    };
    options.response.maximum_read = NonZeroU32::MIN;
    let (startup, run) = Client::prepare(worker.clone(), connector, options)?;
    parents.client_credit = Some(credit);
    parents.client_run = Some(tokio::spawn(run));
    probe.event(Event::ClientRunRegistered);
    Ok(startup)
}

fn prepare_server(
    stream: tokio::net::TcpStream,
    worker: &WorkerHandle,
    protocol: Protocol,
    probe: &Arc<Probe>,
    parents: &mut Parents,
) -> Result<ServerStartup, BoxError> {
    require(
        parents.server_run.is_none(),
        "server run slot already occupied",
    )?;
    let credit = parents.admission.clone().try_acquire_owned()?;
    {
        let mut observed = probe.observed.lock().expect("observation lock");
        observed.accepted_local = Some(stream.local_addr()?);
        observed.accepted_peer = Some(stream.peer_addr()?);
    }
    let service = Router::new()
        .route("/stream", post(echo))
        .with_state(probe.clone())
        .into_service::<RoxygenBody>();
    let ingress_probe = probe.clone();
    let service = service.map_request(move |request: Request<RoxygenBody>| {
        ingress_probe
            .observed
            .lock()
            .expect("observation lock")
            .facade_request_version = Some(request.version());
        request
    });
    let mut options = ServerOptions::new(protocol.codec(), TransportSecurity::Plaintext);
    options.max_exchanges = NonZeroUsize::MIN;
    options.dispatch_capacity = NonZeroUsize::MIN;
    let (startup, run) = prepare_connection::<_, _, AxumBody>(
        worker.clone(),
        established(stream, probe, IoSide::Server)?,
        service,
        options,
    )?;
    parents.server_credit = Some(credit);
    parents.server_run = Some(tokio::spawn(run));
    probe.event(Event::ServerRunRegistered);
    Ok(startup)
}

async fn start_client(
    startup: ClientStartup,
    parents: &mut Parents,
    outcome: &mut Option<Result<(), ClientStartError>>,
    probe: &Probe,
) -> Result<Client, BoxError> {
    probe.event(Event::ClientStartupPolled);
    match startup.start().await {
        Ok((client, owner)) => {
            parents.client_submission = Some(client.clone());
            parents.client_owner = Some(owner);
            *outcome = Some(Ok(()));
            Ok(client)
        }
        Err(error) => {
            *outcome = Some(Err(error));
            Err(io::Error::other("client startup failed; original outcome retained").into())
        }
    }
}

async fn start_server(
    startup: ServerStartup,
    parents: &mut Parents,
    outcome: &mut Option<Result<(), ServerError>>,
    probe: &Probe,
) -> Result<(), BoxError> {
    probe.event(Event::ServerStartupPolled);
    match startup.start().await {
        Ok(connection) => {
            {
                let mut observed = probe.observed.lock().expect("observation lock");
                let context = connection.connection_context();
                observed.context_matches_endpoints = observed
                    .accepted_local
                    .is_some_and(|address| context.local_endpoint() == &endpoint(address))
                    && observed
                        .accepted_peer
                        .is_some_and(|address| context.peer_endpoint() == &endpoint(address))
                    && context.security() == TransportSecurity::Plaintext
                    && context.peer_identity().is_none();
            }
            parents.server_connection = Some(connection);
            *outcome = Some(Ok(()));
            Ok(())
        }
        Err(error) => {
            *outcome = Some(Err(error));
            Err(io::Error::other("server startup failed; original outcome retained").into())
        }
    }
}

async fn receive_response(
    response: ResponseFuture,
    release: &mut Option<oneshot::Sender<()>>,
    exchange: ClientExchange,
    probe: &Arc<Probe>,
) -> Result<(), BoxError> {
    let mut response = Box::pin(response);
    let response = poll_fn(|context| {
        let result = response.as_mut().poll(context);
        let mut progress = probe.progress.lock().expect("progress lock");
        progress.client_response_polls += 1;
        match &result {
            Poll::Pending => progress.client_response_pending += 1,
            Poll::Ready(Ok(_)) => progress.client_response_ready = true,
            Poll::Ready(Err(_)) => progress.client_response_failed = true,
        }
        result
    })
    .await?;
    require(
        response.status() == StatusCode::OK,
        "Axum did not return success",
    )?;
    probe
        .observed
        .lock()
        .expect("observation lock")
        .response_version = Some(response.version());
    let mut body = Box::pin(response.into_body());
    loop {
        probe
            .progress
            .lock()
            .expect("progress lock")
            .client_body_frame_waits += 1;
        match body.frame().await {
            Some(Ok(frame)) => {
                probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .download
                    .record(&frame);
                let prefix_complete =
                    if let Some(bytes) = frame.data_ref().filter(|bytes| !bytes.is_empty()) {
                        require(
                            bytes.len() == 1,
                            "public maximum_read did not fragment response data",
                        )?;
                        let mut progress = probe.progress.lock().expect("progress lock");
                        let prefix = probe.scenario.prefix();
                        if progress.prefix_bytes_received < prefix.len() {
                            let start = progress.prefix_bytes_received;
                            require(
                                prefix.get(start..start + bytes.len()) == Some(bytes.as_ref()),
                                "unexpected data before complete response prefix",
                            )?;
                            progress.prefix_bytes_received += bytes.len();
                            progress.nonempty_prefix_frames += 1;
                            progress.prefix_bytes_received == prefix.len()
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                if prefix_complete {
                    if probe.scenario != Scenario::BodyError {
                        probe
                            .observed
                            .lock()
                            .expect("observation lock")
                            .first_response_before_upload_release = release.is_some();
                    }
                    if probe.scenario == Scenario::Cancellation {
                        exchange.cancel();
                        probe
                            .progress
                            .lock()
                            .expect("progress lock")
                            .client_cancel_requested = true;
                    } else if let Some(sender) = release.take() {
                        sender
                            .send(())
                            .map_err(|()| io::Error::other("producer gate disappeared"))?;
                    }
                }
            }
            Some(Err(error)) => {
                probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .response_error = Some(error);
                break;
            }
            None => {
                probe
                    .observed
                    .lock()
                    .expect("observation lock")
                    .download
                    .eof = true;
                break;
            }
        }
    }
    Ok(())
}

struct Exercise<'a> {
    native: &'a RuntimeHandle,
    protocol: Protocol,
    probe: &'a Arc<Probe>,
    parents: &'a mut Parents,
    phases: &'a mut Phases,
    release: &'a mut Option<oneshot::Sender<()>>,
}

async fn exercise(input: Exercise<'_>) -> Result<(), BoxError> {
    let Exercise {
        native,
        protocol,
        probe,
        parents,
        phases,
        release,
    } = input;
    let worker = native
        .worker(0)
        .ok_or_else(|| io::Error::other("configured worker missing"))?;
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    probe.observed.lock().expect("observation lock").listener = Some(address);
    let client_startup = prepare_client(&worker, address, protocol, probe, parents)?;
    if probe.scenario == Scenario::StartupFailure {
        let (peer, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let peer = peer?;
        let (stream, _) = accepted?;
        let server_startup = prepare_server(stream, &worker, protocol, probe, parents)?;
        phases.early_quiescence = Some(native.quiesce(Duration::ZERO).await);
        probe
            .observed
            .lock()
            .expect("observation lock")
            .closed_worker_before_startup = !worker.is_accepting();
        let client_result =
            start_client(client_startup, parents, &mut phases.client_startup, probe).await;
        let server_result =
            start_server(server_startup, parents, &mut phases.server_startup, probe).await;
        let both_failed = client_result.is_err() && server_result.is_err();
        drop(peer);
        return require(
            both_failed,
            "activation on the quiesced worker unexpectedly succeeded",
        );
    }
    let client = start_client(client_startup, parents, &mut phases.client_startup, probe).await?;
    let origin: Origin = format!("http://{address}").parse()?;
    let route = client.route(RouteSpec::direct(origin, protocol.policy()))?;
    let (sender, gate) = oneshot::channel();
    *release = Some(sender);
    let body = if probe.scenario == Scenario::BodyError {
        *probe.response_gate.lock().expect("gate lock") = Some(gate);
        ScriptBody::new(probe.clone(), BodyRole::Upload, Vec::new(), None)
    } else {
        let mut trailers = HeaderMap::new();
        trailers.insert(TRAILER_NAME, HeaderValue::from_static("verified"));
        ScriptBody::new(
            probe.clone(),
            BodyRole::Upload,
            vec![
                Ok(Frame::data(Bytes::from_static(b"public-"))),
                Ok(Frame::data(Bytes::from_static(b"stream"))),
                Ok(Frame::trailers(trailers)),
            ],
            Some(gate),
        )
    };
    let request = Request::builder()
        .method("POST")
        .uri("/stream")
        .version(protocol.version())
        .header(TE, "trailers")
        .header(TRAILER, TRAILER_NAME)
        .body(body)?;
    let mut service = client.service::<ScriptBody>(&route)?;
    service.ready().await?;
    let response = service.call(request);
    let exchange = response.exchange();
    parents.client_exchange = Some(exchange.clone());
    {
        let Phases {
            accept,
            request,
            server_startup,
            ..
        } = phases;
        tokio::join!(
            async {
                *accept = Some(
                    async {
                        let (stream, _) = listener.accept().await?;
                        check_wire_prefix(&stream, protocol, probe).await?;
                        let startup = prepare_server(stream, &worker, protocol, probe, parents)?;
                        start_server(startup, parents, server_startup, probe).await
                    }
                    .await,
                );
            },
            async {
                *request = Some(receive_response(response, release, exchange.clone(), probe).await);
            },
        );
        require(
            succeeded(accept) && succeeded(request),
            "accept/request phase failed; outcomes retained",
        )?;
    }
    require(
        probe
            .server_exchange
            .lock()
            .expect("exchange lock")
            .is_some(),
        "Axum request lacks ServerExchange",
    )?;
    if probe.scenario == Scenario::Cancellation {
        phases.client_exchange = Some(exchange.wait().await);
        {
            let mut progress = probe.progress.lock().expect("progress lock");
            progress.upload_gate_held_through_retirement = release
                .as_ref()
                .is_some_and(|sender| sender.is_closed())
                && progress.upload.retired
                && !progress.upload.gate_opened
                && !progress.upload.gate_sender_lost
                && matches!(&phases.client_exchange, Some(Err(error)) if !error.cleanup_unconfirmed());
        }
        let server = probe
            .server_exchange
            .lock()
            .expect("exchange lock")
            .clone()
            .ok_or_else(|| io::Error::other("Axum request lacks ServerExchange"))?;
        server.cancel();
        probe
            .progress
            .lock()
            .expect("progress lock")
            .server_cancel_requested = true;
        probe
            .observed
            .lock()
            .expect("observation lock")
            .explicit_cancellation = true;
    }
    observe_exchanges(parents, probe, phases).await;
    Ok(())
}

async fn reap<Outcome>(
    parent: &mut Option<JoinHandle<Outcome>>,
    probe: &Probe,
    event: Event,
) -> Option<Result<Outcome, JoinError>> {
    let result = match parent.as_mut() {
        Some(parent) => Some(parent.await),
        None => None,
    };
    if result.is_some() {
        probe.event(event);
    }
    parent.take();
    result
}

async fn observe_exchanges(parents: &Parents, probe: &Probe, phases: &mut Phases) {
    let server = probe.server_exchange.lock().expect("exchange lock").clone();
    let Phases {
        client_exchange,
        server_exchange,
        ..
    } = phases;
    tokio::join!(
        async {
            if client_exchange.is_none() {
                if let Some(exchange) = &parents.client_exchange {
                    *client_exchange = Some(exchange.wait().await);
                }
            }
        },
        async {
            if server_exchange.is_none() {
                if let Some(exchange) = server {
                    *server_exchange = Some(exchange.wait_completed().await);
                }
            }
        },
    );
}

async fn cleanup(
    native: &RuntimeHandle,
    parents: &mut Parents,
    probe: &Arc<Probe>,
    phases: &mut Phases,
) {
    parents.admission.close();
    probe.event(Event::AdmissionClosed);
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    if let Some(connection) = &parents.server_connection {
        connection.begin_drain();
    }
    if succeeded(&phases.operation) && parents.server_connection.is_some() {
        let mut draining = Box::pin(reap(&mut parents.server_run, probe, Event::ServerRunJoined));
        if let Ok(outcome) = timeout_at(deadline, draining.as_mut()).await {
            phases.server_reaped_before_client_shutdown = outcome.is_some();
            phases.server_run = outcome;
        }
    }
    phases.client_submission_retained_until_shutdown = parents.client_submission.is_some();
    if let Some(owner) = &parents.client_owner {
        owner.shutdown();
    }
    drop(parents.client_submission.take());
    let Parents {
        client_run,
        server_run,
        server_connection,
        client_exchange,
        ..
    } = parents;
    let connection = server_connection.as_ref();
    let client_exchange = client_exchange.as_ref();
    let mut joining = Box::pin(async {
        tokio::join!(
            reap(client_run, probe, Event::ClientRunJoined),
            reap(server_run, probe, Event::ServerRunJoined),
            async {
                let result = native
                    .quiesce(deadline.saturating_duration_since(Instant::now()))
                    .await;
                probe.event(Event::NativeQuiesced);
                result
            },
        )
    });
    let outcomes = match timeout_at(deadline, joining.as_mut()).await {
        Ok(outcomes) => {
            drop(joining);
            outcomes
        }
        Err(_) => {
            if let Some(connection) = connection {
                connection.cancel();
            }
            if let Some(exchange) = client_exchange {
                exchange.cancel();
            }
            if let Some(exchange) = &*probe.server_exchange.lock().expect("exchange lock") {
                exchange.cancel();
            }
            joining.await
        }
    };
    phases.client_run = outcomes.0;
    if outcomes.1.is_some() {
        phases.server_run = outcomes.1;
    }
    phases.native_quiescence = Some(outcomes.2);
    observe_exchanges(parents, probe, phases).await;
    phases.reactor_check = Some(match timeout(OPERATION_TIMEOUT, check_reactor()).await {
        Ok(result) => result,
        Err(error) => Err(Box::new(error)),
    });
    probe.event(Event::ReactorChecked);
}

async fn check_reactor() -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let (client, server) = tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
    let mut client = client?;
    let (mut server, _) = server?;
    let mut received = [0_u8; 1];
    let (written, read) = tokio::join!(client.write_all(b"R"), server.read_exact(&mut received));
    written?;
    read?;
    require(received == *b"R", "reactor probe data changed")
}

async fn check_wire_prefix(
    stream: &tokio::net::TcpStream,
    protocol: Protocol,
    probe: &Probe,
) -> Result<(), BoxError> {
    let expected = protocol.wire_prefix();
    let mut prefix = vec![0; expected.len()];
    loop {
        let count = stream.peek(&mut prefix).await?;
        require(count != 0, "TCP closed before an HTTP wire prefix")?;
        if count == expected.len() {
            require(prefix == expected, "unexpected native HTTP wire prefix")?;
            probe.observed.lock().expect("observation lock").wire_prefix = prefix;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

pub fn run(protocol: Protocol, scenario: Scenario) -> Result<Report, Box<dyn Error>> {
    if Handle::try_current().is_ok() {
        return Err(io::Error::other("call run from a synchronous root outside Tokio").into());
    }
    let application = Builder::new_current_thread().enable_all().build()?;
    let mut owner = RuntimeOwner::start(RuntimeConfig {
        workers: 1,
        command_capacity: 64,
    })?;
    let native = owner.handle();
    let probe = Arc::new(Probe {
        scenario,
        application_thread: std::thread::current().id(),
        reactor_alive: AtomicBool::new(true),
        observed: Mutex::new(Observations::default()),
        progress: Mutex::new(Progress::default()),
        events: Mutex::new(vec![Event::NativeStartedOutsideTokio]),
        server_exchange: Mutex::new(None),
        response_gate: Mutex::new(None),
    });
    let mut parents = Parents::new();
    let mut phases = Phases::default();
    let mut release = None;
    application.block_on(async {
        let (operation, timeout_progress): (Result<(), BoxError>, Option<Progress>) = {
            let mut pending = Box::pin(exercise(Exercise {
                native: &native,
                protocol,
                probe: &probe,
                parents: &mut parents,
                phases: &mut phases,
                release: &mut release,
            }));
            match timeout(OPERATION_TIMEOUT, pending.as_mut()).await {
                Ok(result) => (result, None),
                Err(error) => (
                    Err(Box::new(error)),
                    Some(probe.progress.lock().expect("progress lock").clone()),
                ),
            }
        };
        phases.operation = Some(operation);
        phases.timeout_progress = timeout_progress;
        cleanup(&native, &mut parents, &probe, &mut phases).await;
    });
    let outside_tokio = Handle::try_current().is_err();
    let joined = owner.join();
    let physically_joined = matches!(&joined, Ok(_) | Err(RuntimeError::WorkersFailed { .. }));
    phases.native_join = Some(joined);
    let admission_closed = parents.admission.is_closed();
    let credits = parents.take_credits();
    drop(parents);
    let retained_bootstrap = if physically_joined && outside_tokio {
        probe.event(Event::NativeJoinedOutsideTokio);
        drop(owner);
        drop(credits);
        drop(release);
        {
            let mut observed = probe.observed.lock().expect("observation lock");
            observed.bodies_destroyed_before_runtime_drop =
                observed.bodies_created == observed.bodies_dropped;
        }
        probe.reactor_alive.store(false, Ordering::SeqCst);
        drop(application);
        probe.event(Event::ApplicationRuntimeDropped);
        None
    } else {
        Some(RetainedBootstrap {
            _native_owner: owner,
            _application_runtime: application,
            _credits: credits,
            _gate_sender: release,
        })
    };
    let observations = std::mem::take(&mut *probe.observed.lock().expect("observation lock"));
    let progress = std::mem::take(&mut *probe.progress.lock().expect("progress lock"));
    let events = std::mem::take(&mut *probe.events.lock().expect("event lock"));
    Ok(Report {
        protocol,
        scenario,
        phases,
        observations,
        progress,
        events,
        admission_closed,
        retained_bootstrap,
    })
}

struct ServerFailureObservation<'error> {
    cause: &'error ServerFailure,
    cleanup_confirmed: bool,
    panic_present: bool,
}

impl<'error> ServerFailureObservation<'error> {
    fn observe(error: &'error ServerError) -> Self {
        Self {
            cause: error.cause(),
            cleanup_confirmed: error.cleanup_confirmed(),
            panic_present: error.first_panic().is_some(),
        }
    }
}

fn require_h2_cancelled_server_errors(
    exchange: ServerFailureObservation<'_>,
    run: ServerFailureObservation<'_>,
) -> Result<(), BoxError> {
    require(
        matches!(
            exchange.cause,
            ServerFailure::Body(ApplicationBodyError::Cancelled)
        ) && matches!(
            run.cause,
            ServerFailure::Body(ApplicationBodyError::Cancelled)
        ),
        "H2 cancellation server exchange and ServerRun must both retain Body(Cancelled)",
    )?;
    require(
        exchange.cleanup_confirmed && run.cleanup_confirmed,
        "H2 cancellation server exchange or ServerRun cleanup is unconfirmed",
    )?;
    require(
        !exchange.panic_present && !run.panic_present,
        "H2 cancellation server exchange or ServerRun caught a panic",
    )?;
    require(
        std::ptr::eq(exchange.cause, run.cause),
        "H2 cancellation ServerRun did not retain the actual server exchange cause",
    )
}

fn require_successful_client_cleanup<Failure>(
    receipt: Option<&Result<(), Failure>>,
) -> Result<(), BoxError> {
    require(
        matches!(receipt, Some(Ok(()))),
        "body-error client cleanup receipt is not exactly Ok(())",
    )
}

fn succeeded<Success, Failure>(result: &Option<Result<Success, Failure>>) -> bool {
    matches!(result, Some(Ok(_)))
}

fn require(condition: bool, message: &'static str) -> Result<(), BoxError> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message).into())
    }
}

fn has_injected_error(mut error: &(dyn Error + 'static)) -> bool {
    loop {
        if error.is::<InjectedBodyError>() {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

fn local_cancellation(error: &HttpError) -> bool {
    matches!(
        error.code(),
        HttpErrorCode::CANCEL | HttpErrorCode::CORO_CANCELLED
    )
}

fn peer_termination(error: &HttpError, protocol: Protocol, scenario: Scenario) -> bool {
    match (protocol, scenario) {
        (Protocol::Http1, Scenario::BodyError | Scenario::Cancellation) => {
            matches!(
                error.code(),
                HttpErrorCode::TRANSPORT_EOF | HttpErrorCode::TRANSPORT_READ_ERROR
            )
        }
        (Protocol::Http2, Scenario::BodyError) => error.code() == HttpErrorCode::INTERNAL_ERROR,
        (Protocol::Http2, Scenario::Cancellation) => error.code() == HttpErrorCode::CANCEL,
        _ => false,
    }
}

fn cancelled_application(error: &(dyn Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<ApplicationBodyError>(),
        Some(ApplicationBodyError::Cancelled)
    )
}

fn expected_client_exchange_failure(
    error: &ClientError,
    protocol: Protocol,
    scenario: Scenario,
) -> bool {
    match (scenario, error) {
        (Scenario::BodyError, ClientError::Native(error)) => {
            peer_termination(error, protocol, scenario)
        }
        (Scenario::Cancellation, ClientError::Cancelled) => true,
        (Scenario::Cancellation, ClientError::Native(error)) => local_cancellation(error),
        (Scenario::Cancellation, ClientError::Application(error)) => {
            error.http_error().code() == HttpErrorCode::BODY_PARSE_ERROR
                && error
                    .external_cause()
                    .is_some_and(|cause| cancelled_application(cause.as_ref()))
        }
        _ => false,
    }
}

fn expected_response_failure(
    error: &(dyn Error + 'static),
    protocol: Protocol,
    scenario: Scenario,
) -> bool {
    if let Some(error) = error.downcast_ref::<RemoteSourceError<HttpError>>() {
        return match (scenario, error) {
            (Scenario::BodyError, RemoteSourceError::Source(error)) => {
                peer_termination(error, protocol, scenario)
            }
            (Scenario::Cancellation, RemoteSourceError::Stopped) => true,
            (Scenario::Cancellation, RemoteSourceError::Source(error)) => local_cancellation(error),
            _ => false,
        };
    }
    if let Some(error) = error.downcast_ref::<ClientError>() {
        return expected_client_exchange_failure(error, protocol, scenario);
    }
    match error.downcast_ref::<HttpError>() {
        Some(error) if scenario == Scenario::BodyError => {
            peer_termination(error, protocol, scenario)
        }
        Some(error) if scenario == Scenario::Cancellation => local_cancellation(error),
        _ => false,
    }
}

fn echo_peer_cancellation(mut error: &(dyn Error + 'static), protocol: Protocol) -> bool {
    loop {
        if let Some(RemoteSourceError::Source(error)) =
            error.downcast_ref::<RemoteSourceError<HttpError>>()
        {
            return peer_termination(error, protocol, Scenario::Cancellation);
        }
        if let Some(error) = error.downcast_ref::<HttpError>() {
            return peer_termination(error, protocol, Scenario::Cancellation);
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

fn expected_server_failure(error: &ServerFailure, protocol: Protocol, scenario: Scenario) -> bool {
    match (scenario, error) {
        (Scenario::BodyError, ServerFailure::Body(ApplicationBodyError::Application(error))) => {
            has_injected_error(error.as_ref())
        }
        (
            Scenario::Cancellation,
            ServerFailure::Cancelled | ServerFailure::Body(ApplicationBodyError::Cancelled),
        ) => true,
        (Scenario::Cancellation, ServerFailure::Native(error)) => {
            local_cancellation(error) || peer_termination(error, protocol, scenario)
        }
        (Scenario::Cancellation, ServerFailure::Body(ApplicationBodyError::Application(error))) => {
            echo_peer_cancellation(error.as_ref(), protocol)
        }
        _ => false,
    }
}
