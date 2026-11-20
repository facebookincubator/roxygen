/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Fixed-worker TCP/TLS HTTP server lifecycle.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::num::NonZeroUsize;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use roxygen_core::error::HttpError;
use roxygen_core::filter_factory::FilterFactory;
use roxygen_core::filter_factory::HttpFilterFactoryHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::http::CodecProtocol;
pub use roxygen_runtime::BlockingPoolLimits;
use roxygen_runtime::tokio_host::ManagedWorker;
use roxygen_runtime::tokio_host::ManagedWorkerError;
use roxygen_runtime::tokio_host::ManagedWorkerOptions;
use roxygen_runtime::worker::SubmitError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
pub use roxygen_runtime::worker::WorkerPollStats;
use roxygen_session::uniplex::UniplexRunSummary;
#[cfg(test)]
use roxygen_transport::byte_stream::TransportRetirement;
use roxygen_transport::tls::TlsServerOptions;
use socket2::Domain;
use socket2::Protocol;
use socket2::SockAddr;
use socket2::Socket;
use socket2::Type;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::acceptor::AcceptedConnectionObserver;
use crate::acceptor::AcceptorConfig;
use crate::acceptor::ConnectionId;
use crate::acceptor::ConnectionLifecycle;
use crate::acceptor::ConnectionLifecycleObserver;
use crate::acceptor::NON_ABORTIVE_CLOSE_WITH_TLS;
use crate::acceptor::NewConnectionFilter;
use crate::acceptor::ServerSessionConfig;
use crate::acceptor::SessionControl;
use crate::acceptor::TlsHandshakeObserver;
use crate::acceptor::WorkerApp;
use crate::acceptor::accept_session;
use crate::acceptor::invoke_accept_callback;
use crate::acceptor::run_session;
pub use crate::connection::ServeTransportOptions;
pub use crate::connection::ServerConnection;
pub use crate::connection::serve_transport;
use crate::owned_application::OwnedApplicationFactory;
use crate::owned_application::OwnedConnectionFactory;
use crate::owned_application::StopAbandon;
use crate::owned_application::WorkerView;

const DEFAULT_COMMAND_CAPACITY: usize = 1_024;
const DEFAULT_BACKLOG: i32 = 1_024;
/// Pause before accepting again after a persistent accept error such as `EMFILE`.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_secs(1);

#[cfg(test)]
pub(crate) const ADMISSION_EVENT_LIMIT: usize = 8;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionListenerKey {
    pub(crate) worker_index: usize,
    pub(crate) task_id: tokio::task::Id,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionAttempt {
    Enqueued,
    Full,
    Closed,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionSocket {
    pub(crate) worker_index: usize,
    pub(crate) local: Option<SocketAddr>,
    pub(crate) peer: SocketAddr,
    pub(crate) capacity: usize,
    pub(crate) max_capacity: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionJoinOutcome {
    Returned,
    Cancelled,
    Panicked,
    OtherFailure,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionEvent {
    ListenerRegistered(AdmissionListenerKey),
    DequeueHeld {
        worker_index: usize,
    },
    Socket {
        key: AdmissionListenerKey,
        socket: AdmissionSocket,
        attempt: AdmissionAttempt,
    },
    ForceObserved {
        worker_index: usize,
        queued: usize,
        capacity: usize,
        max_capacity: usize,
        held: bool,
        sessions: usize,
        controls: usize,
        mappings: usize,
    },
    ListenerReaped {
        key: AdmissionListenerKey,
        outcome: AdmissionJoinOutcome,
        remaining: usize,
    },
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct AdmissionProgress {
    stage: usize,
    key: Option<AdmissionListenerKey>,
    first_socket: Option<AdmissionSocket>,
    failed: bool,
}

#[cfg(test)]
impl AdmissionProgress {
    pub(crate) fn observe(&mut self, event: AdmissionEvent) -> bool {
        let valid = match (self.stage, event) {
            (0, AdmissionEvent::ListenerRegistered(key)) => {
                self.key = Some(key);
                key.worker_index == 0
            }
            (1, AdmissionEvent::DequeueHeld { worker_index }) => worker_index == 0,
            (
                2,
                AdmissionEvent::Socket {
                    key,
                    socket,
                    attempt: AdmissionAttempt::Enqueued,
                },
            ) => {
                self.first_socket = Some(socket);
                self.key == Some(key)
                    && socket.worker_index == 0
                    && socket.local.is_some()
                    && socket.capacity == 0
                    && socket.max_capacity == 1
            }
            (
                3,
                AdmissionEvent::Socket {
                    key,
                    socket,
                    attempt: AdmissionAttempt::Full,
                },
            ) => {
                self.key == Some(key)
                    && socket.worker_index == 0
                    && socket.capacity == 0
                    && socket.max_capacity == 1
                    && self.first_socket.is_some_and(|first| {
                        first.local == socket.local && first.peer != socket.peer
                    })
            }
            (
                4,
                AdmissionEvent::ForceObserved {
                    worker_index: 0,
                    queued: 1,
                    capacity: 0,
                    max_capacity: 1,
                    held: true,
                    sessions: 0,
                    controls: 0,
                    mappings: 0,
                },
            ) => true,
            (
                5,
                AdmissionEvent::ListenerReaped {
                    key,
                    outcome,
                    remaining: 0,
                },
            ) => {
                self.key == Some(key)
                    && matches!(
                        outcome,
                        AdmissionJoinOutcome::Returned | AdmissionJoinOutcome::Cancelled
                    )
            }
            _ => false,
        };
        self.failed |= !valid;
        if !self.failed {
            self.stage += 1;
        }
        !self.failed
    }

    pub(crate) fn saturated(&self) -> bool {
        !self.failed && self.stage == 4
    }

    pub(crate) fn retired(&self) -> bool {
        !self.failed && self.stage == 6
    }
}

#[cfg(test)]
pub(crate) struct AdmissionProbe {
    sender: SyncSender<AdmissionEvent>,
    failed: AtomicBool,
    records: AtomicUsize,
    registrations: AtomicUsize,
    progress: Mutex<AdmissionProgress>,
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[cfg(test)]
impl AdmissionProbe {
    pub(crate) fn new() -> (
        Arc<Self>,
        Receiver<AdmissionEvent>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(ADMISSION_EVENT_LIMIT);
        let (release, gate) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                sender,
                failed: AtomicBool::new(false),
                records: AtomicUsize::new(0),
                registrations: AtomicUsize::new(0),
                progress: Mutex::new(AdmissionProgress::default()),
                gate: Mutex::new(Some(gate)),
            }),
            receiver,
            release,
        )
    }

    fn confirm(&self, valid: bool) -> bool {
        if !valid {
            self.failed.store(true, Ordering::Release);
        }
        valid
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub(crate) fn registrations(&self) -> usize {
        self.registrations.load(Ordering::Acquire)
    }

    fn publish(&self, event: AdmissionEvent) {
        if !self.confirm(
            self.records
                .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count
                        .checked_add(1)
                        .filter(|count| *count <= ADMISSION_EVENT_LIMIT)
                })
                .is_ok(),
        ) {
            return;
        }
        let valid = match self.progress.try_lock() {
            Ok(mut progress) => progress.observe(event),
            Err(_) => false,
        };
        self.confirm(valid);
        self.confirm(self.sender.try_send(event).is_ok());
    }

    fn take_gate(&self) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let gate = self.gate.try_lock().ok().and_then(|mut gate| gate.take());
        self.confirm(gate.is_some());
        gate
    }

    fn session_registered(&self) {
        let _ = self
            .registrations
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            });
        self.confirm(false);
    }
}

#[cfg(test)]
async fn observe_accept_reaps(
    tasks: &mut JoinSet<()>,
    probe: &AdmissionProbe,
    worker_index: usize,
) {
    while let Some(result) = tasks.join_next_with_id().await {
        let (task_id, outcome) = match result {
            Ok((task_id, ())) => (task_id, AdmissionJoinOutcome::Returned),
            Err(error) => {
                let task_id = error.id();
                let outcome = if error.is_panic() {
                    AdmissionJoinOutcome::Panicked
                } else if error.is_cancelled() {
                    AdmissionJoinOutcome::Cancelled
                } else {
                    AdmissionJoinOutcome::OtherFailure
                };
                drop(error);
                (task_id, outcome)
            }
        };
        probe.publish(AdmissionEvent::ListenerReaped {
            key: AdmissionListenerKey {
                worker_index,
                task_id,
            },
            outcome,
            remaining: tasks.len(),
        });
    }
}

#[cfg(test)]
pub(crate) const SESSION_RETIREMENT_EVENT_LIMIT: usize = 8;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SessionRetirementKey {
    pub(crate) worker_index: usize,
    pub(crate) session_id: u64,
    pub(crate) task_id: tokio::task::Id,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) enum SessionRunOutcome {
    Returned(Result<UniplexRunSummary, HttpError>),
    JoinFailed { panic: bool, cancelled: bool },
}

#[cfg(test)]
pub(crate) struct SessionReaped {
    pub(crate) outcome: SessionRunOutcome,
    pub(crate) mapping_removed: bool,
    pub(crate) control_removed: bool,
    pub(crate) remaining_tasks: usize,
    pub(crate) remaining_controls: usize,
}

#[cfg(test)]
pub(crate) enum SessionRetirementRecord {
    Registered,
    TransportBound(Option<TransportRetirement>),
    RunReturned,
    Reaped(SessionReaped),
}

#[cfg(test)]
pub(crate) struct SessionRetirementEvent {
    pub(crate) key: SessionRetirementKey,
    pub(crate) record: SessionRetirementRecord,
}

#[cfg(test)]
pub(crate) struct SessionRunGate {
    pub(crate) worker_index: usize,
    pub(crate) session_id: u64,
    pub(crate) release: tokio::sync::oneshot::Receiver<()>,
    pub(crate) timeout: std::time::Duration,
}

#[cfg(test)]
pub(crate) struct SessionRetirementProbe {
    sender: SyncSender<SessionRetirementEvent>,
    failed: AtomicBool,
    records: AtomicUsize,
    gate: Mutex<Option<SessionRunGate>>,
}

#[cfg(test)]
impl SessionRetirementProbe {
    pub(crate) fn new(
        gate: Option<SessionRunGate>,
    ) -> (Arc<Self>, Receiver<SessionRetirementEvent>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(SESSION_RETIREMENT_EVENT_LIMIT);
        (
            Arc::new(Self {
                sender,
                failed: AtomicBool::new(false),
                records: AtomicUsize::new(0),
                gate: Mutex::new(gate),
            }),
            receiver,
        )
    }

    pub(crate) fn confirm(&self, valid: bool) -> bool {
        if !valid {
            self.failed.store(true, Ordering::Release);
        }
        valid
    }

    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn publish(&self, key: SessionRetirementKey, record: SessionRetirementRecord) {
        if !self.confirm(
            self.records
                .try_update(Ordering::AcqRel, Ordering::Acquire, |records| {
                    records
                        .checked_add(1)
                        .filter(|records| *records <= SESSION_RETIREMENT_EVENT_LIMIT)
                })
                .is_ok(),
        ) {
            return;
        }
        self.confirm(
            self.sender
                .try_send(SessionRetirementEvent { key, record })
                .is_ok(),
        );
    }

    fn take_gate(&self, worker_index: usize, session_id: u64) -> Option<SessionRunGate> {
        match self.gate.try_lock() {
            Ok(mut gate) => {
                if gate.as_ref().is_some_and(|gate| {
                    gate.worker_index == worker_index && gate.session_id == session_id
                }) {
                    gate.take()
                } else {
                    None
                }
            }
            Err(_) => {
                self.confirm(false);
                None
            }
        }
    }
}

/// Holds every worker's dequeue of accepted sockets until released, so a test can fill the
/// accepted-socket queues.
#[cfg(test)]
pub(crate) struct DequeueHoldForTest {
    state: watch::Sender<DequeueHoldState>,
}

#[cfg(test)]
#[derive(Default)]
struct DequeueHoldState {
    released: bool,
    held: std::collections::BTreeSet<usize>,
}

#[cfg(test)]
impl DequeueHoldForTest {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: watch::Sender::new(DequeueHoldState::default()),
        })
    }

    /// Returns once worker `worker_index` is parked before a dequeue.
    pub(crate) async fn wait_held(&self, worker_index: usize) {
        let _ = self
            .state
            .subscribe()
            .wait_for(|state| state.held.contains(&worker_index))
            .await;
    }

    /// Lets every worker dequeue from now on. Idempotent.
    pub(crate) fn release(&self) {
        self.state.send_modify(|state| state.released = true);
    }

    /// Parks worker `worker_index` until `release`, recording that it is held.
    async fn hold(&self, worker_index: usize) {
        let mut state = self.state.subscribe();
        if state.borrow().released {
            return;
        }
        self.state
            .send_if_modified(|state| state.held.insert(worker_index));
        let _ = state.wait_for(|state| state.released).await;
    }
}

/// Runs on a worker right after it spawns a session task, before the worker polls anything
/// else, so a test can act while that task has not started.
#[cfg(test)]
pub(crate) type SessionSpawnedHookForTest = dyn Fn() + Send + Sync;

pub type HandlerFactory = Arc<dyn Fn(usize) -> HttpHandlerRef + Send + Sync>;

/// Constructs worker-local application state on the worker serving its requests.
/// The context can register asynchronous cleanup that is joined at worker exit.
pub type WorkerHandlerFactory = Arc<dyn Fn(WorkerContext, usize) -> HttpHandlerRef + Send + Sync>;

/// What each worker calls once, on itself, to build what it serves its connections with.
#[derive(Clone)]
enum WorkerAppFactory {
    Handler(WorkerHandlerFactory),
    Owned(OwnedApplicationFactory),
}

pub trait ServerFilterFactory: Send + Sync + 'static {
    fn make_filter_factory(&self) -> Rc<dyn FilterFactory>;

    fn on_server_start(&self, _worker_index: usize) {}

    fn on_server_stop(&self, _worker_index: usize) {}
}

pub trait ServerObserver: Send + Sync + 'static {
    fn on_thread_start(&self, _worker_index: usize) {}

    fn on_thread_stop(&self, _worker_index: usize) {}
}

pub trait TlsCredentialProvider: Send + Sync + 'static {
    fn load(&self) -> Result<TlsServerOptions, HttpError>;
}

pub struct StaticTlsCredentials {
    options: TlsServerOptions,
}

impl StaticTlsCredentials {
    pub fn new(options: TlsServerOptions) -> Self {
        Self { options }
    }
}

impl TlsCredentialProvider for StaticTlsCredentials {
    fn load(&self) -> Result<TlsServerOptions, HttpError> {
        Ok(self.options.clone())
    }
}

/// Which workers accept connections.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AcceptDistribution {
    /// Worker 0 accepts every connection and hands the sockets to the workers in turn. A socket
    /// whose worker's accepted-socket queue is full is closed and counted in
    /// [`AcceptStats::handoff_dropped`].
    #[default]
    Worker0,
    /// Each worker binds its own `SO_REUSEPORT` listener for every address and serves the
    /// connections it accepts. A full accepted-socket queue pauses that worker's accepts and
    /// leaves new connections in the kernel backlog, so no socket is dropped. Requires
    /// `reuse_port` and no prebound listeners.
    PerWorkerReusePort,
}

pub struct ServerSocketConfig {
    pub bind_addresses: Vec<SocketAddr>,
    pub prebound_listeners: Vec<TcpListener>,
    pub backlog: i32,
    pub reuse_port: bool,
    pub tcp_max_segment: Option<u32>,
    pub use_zero_copy: bool,
    /// Which workers accept connections. Defaults to [`AcceptDistribution::Worker0`].
    pub accept_distribution: AcceptDistribution,
}

impl Default for ServerSocketConfig {
    fn default() -> Self {
        Self {
            bind_addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
            prebound_listeners: Vec::new(),
            backlog: DEFAULT_BACKLOG,
            reuse_port: false,
            tcp_max_segment: None,
            use_zero_copy: false,
            accept_distribution: AcceptDistribution::Worker0,
        }
    }
}

pub struct ServerConfig {
    pub socket: ServerSocketConfig,
    pub plaintext_protocol: CodecProtocol,
    pub session: ServerSessionConfig,
    pub num_io_threads: usize,
    pub command_capacity: usize,
    /// The capacity of each worker's queue of accepted sockets. `None` uses `command_capacity`.
    pub accepted_socket_capacity: Option<NonZeroUsize>,
    pub shutdown_on_signals: Vec<i32>,
    pub new_connection_filter: Option<Arc<dyn NewConnectionFilter>>,
    pub filter_factories: Vec<Arc<dyn ServerFilterFactory>>,
    pub tls_credentials: Option<Arc<dyn TlsCredentialProvider>>,
    pub tls_observer: Option<Arc<dyn TlsHandshakeObserver>>,
    pub connection_observer: Option<Arc<dyn AcceptedConnectionObserver>>,
    /// Observes every accepted connection, from the start of its session task on a worker to
    /// its close, under a `ConnectionId`. Unlike `ServerSessionConfig::stats_factory`, every
    /// session mode accepts it. `None`, the default, observes nothing.
    pub lifecycle_observer: Option<Arc<dyn ConnectionLifecycleObserver>>,
    pub zero_copy_enable_threshold: usize,
    /// Limits on each worker's blocking pool. The default keeps Tokio's,
    /// 512 threads with a 10 s keep-alive per worker.
    pub worker_blocking_pool: BlockingPoolLimits,
    #[cfg(test)]
    pub(crate) session_retirement_probe: Option<Arc<SessionRetirementProbe>>,
    #[cfg(test)]
    pub(crate) admission_probe: Option<Arc<AdmissionProbe>>,
    #[cfg(test)]
    pub(crate) dequeue_hold_for_test: Option<Arc<DequeueHoldForTest>>,
    #[cfg(test)]
    pub(crate) session_spawned_for_test: Option<Arc<SessionSpawnedHookForTest>>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            socket: ServerSocketConfig::default(),
            plaintext_protocol: CodecProtocol::Http11,
            session: ServerSessionConfig::default(),
            num_io_threads: 1,
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            accepted_socket_capacity: None,
            shutdown_on_signals: vec![libc::SIGINT],
            new_connection_filter: None,
            filter_factories: Vec::new(),
            tls_credentials: None,
            tls_observer: None,
            connection_observer: None,
            lifecycle_observer: None,
            zero_copy_enable_threshold: 0,
            worker_blocking_pool: BlockingPoolLimits::default(),
            #[cfg(test)]
            session_retirement_probe: None,
            #[cfg(test)]
            admission_probe: None,
            #[cfg(test)]
            dequeue_hold_for_test: None,
            #[cfg(test)]
            session_spawned_for_test: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum HttpServerState {
    Uninitialized,
    Running,
    Draining,
    ForceStopping,
    Stopped,
}

impl HttpServerState {
    fn from_raw(value: u8) -> Self {
        match value {
            1 => Self::Running,
            2 => Self::Draining,
            3 => Self::ForceStopping,
            4 => Self::Stopped,
            _ => Self::Uninitialized,
        }
    }
}

#[derive(Debug, Error)]
pub enum HttpServerError {
    #[error("server configuration is invalid: {0}")]
    InvalidConfig(&'static str),
    #[error("server socket setup failed: {0}")]
    Socket(#[from] io::Error),
    #[error("server worker setup failed: {0}")]
    Worker(#[from] ManagedWorkerError),
    #[error("server worker rejected its startup command: {0}")]
    Submit(#[from] SubmitError),
    #[error("server worker failed to initialize: {0}")]
    WorkerStartup(String),
    #[error("server TLS credential reload failed: {0}")]
    TlsReload(HttpError),
    #[error("server {stage:?} callback panicked on worker {worker_index}")]
    CallbackPanic {
        worker_index: usize,
        stage: ServerCallbackStage,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerCallbackStage {
    HandlerFactory,
    FilterConstruction,
    TlsHandshakeObserver,
    AcceptedConnectionObserver,
    LifecycleObserverFactory,
    ObserverStart,
    FilterStart,
    SessionDrain,
    SessionForceStop,
    ObserverStop,
    FilterStop,
    ConnectionLifecycleObserver,
}

#[derive(Clone, Copy)]
pub(crate) struct CallbackFailure {
    worker_index: usize,
    stage: ServerCallbackStage,
}

enum WorkerCommand {
    Drain,
    ForceStop,
    StopAccepting(AcceptStopAck),
}

/// Acknowledges a [`StopAccepting`] when dropped. A worker that exits without handling the
/// command still acknowledges it, because its listeners closed with it.
struct AcceptStopAck(Option<oneshot::Sender<()>>);

impl Drop for AcceptStopAck {
    fn drop(&mut self) {
        if let Some(acknowledgement) = self.0.take() {
            let _ = acknowledgement.send(());
        }
    }
}

/// Resolves once every worker has closed its listeners after
/// [`HttpServerControl::stop_accepting`].
///
/// A worker acknowledges once every listener task it runs has exited, so its listeners are closed
/// and none of its accepted sockets waits outside its queue. A worker that has exited counts as
/// acknowledged. Dropping this future does not cancel the stop.
pub struct StopAccepting {
    acknowledgements: Vec<Option<oneshot::Receiver<()>>>,
}

impl StopAccepting {
    /// Indices of the workers whose acknowledgement has not arrived, so a caller that bounds the
    /// wait can name them.
    pub fn unacknowledged(&self) -> Vec<usize> {
        self.acknowledgements
            .iter()
            .enumerate()
            .filter(|(_, acknowledgement)| {
                acknowledgement
                    .as_ref()
                    .is_some_and(oneshot::Receiver::is_empty)
            })
            .map(|(index, _)| index)
            .collect()
    }
}

impl Future for StopAccepting {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let acknowledgements = &mut self.get_mut().acknowledgements;
        for acknowledgement in acknowledgements.iter_mut() {
            if let Some(receiver) = acknowledgement
                && Pin::new(receiver).poll(context).is_ready()
            {
                *acknowledgement = None;
            }
        }
        if acknowledgements.iter().all(Option::is_none) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct WorkerConfig {
    index: usize,
    acceptor: AcceptorConfig,
    app_factory: WorkerAppFactory,
    filter_factories: Vec<Arc<dyn ServerFilterFactory>>,
    observer: Option<Arc<dyn ServerObserver>>,
    draining: Arc<AtomicBool>,
    control: HttpServerControl,
    shutdown_on_signals: Vec<i32>,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    lifecycle_started: Arc<AtomicBool>,
    accept_distribution: AcceptDistribution,
    accept_counters: Arc<[Arc<AcceptCounters>]>,
    lifecycle_observer: Option<Arc<dyn ConnectionLifecycleObserver>>,
    #[cfg(test)]
    session_retirement_probe: Option<Arc<SessionRetirementProbe>>,
    #[cfg(test)]
    admission_probe: Option<Arc<AdmissionProbe>>,
    #[cfg(test)]
    dequeue_hold_for_test: Option<Arc<DequeueHoldForTest>>,
    #[cfg(test)]
    session_spawned_for_test: Option<Arc<SessionSpawnedHookForTest>>,
}

/// A snapshot of one worker's accept counters.
///
/// In [`AcceptDistribution::Worker0`] mode a socket counts toward the worker it is handed to,
/// not toward worker 0. A snapshot reads each value independently.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AcceptStats {
    /// Sockets accepted for this worker.
    pub accepted: u64,
    /// Sockets that worker 0 handed to this worker and that were closed without a session: this
    /// worker's queue was full or closed, or the worker stopped dequeuing for a drain. Only
    /// [`AcceptDistribution::Worker0`] mode drops sockets.
    pub handoff_dropped: u64,
    /// Gauge: sockets accepted but not yet turned into sessions (held by a listener task awaiting
    /// queue capacity, queued in the accepted-socket queue, or held by a session task that
    /// has not started).
    pub pending_handoff: u64,
    /// Sockets closed unsessionized by a forced stop, including one whose
    /// session task the stop cancelled before the task started.
    pub closed_at_force_stop: u64,
}

impl AcceptStats {
    /// All counters zero.
    pub const fn new() -> Self {
        Self {
            accepted: 0,
            handoff_dropped: 0,
            pending_handoff: 0,
            closed_at_force_stop: 0,
        }
    }

    /// Sets `accepted`.
    pub const fn with_accepted(mut self, accepted: u64) -> Self {
        self.accepted = accepted;
        self
    }

    /// Sets `handoff_dropped`.
    pub const fn with_handoff_dropped(mut self, handoff_dropped: u64) -> Self {
        self.handoff_dropped = handoff_dropped;
        self
    }

    /// Sets `pending_handoff`.
    pub const fn with_pending_handoff(mut self, pending_handoff: u64) -> Self {
        self.pending_handoff = pending_handoff;
        self
    }

    /// Sets `closed_at_force_stop`.
    pub const fn with_closed_at_force_stop(mut self, closed_at_force_stop: u64) -> Self {
        self.closed_at_force_stop = closed_at_force_stop;
        self
    }
}

/// One worker's live accept counters.
#[derive(Default)]
struct AcceptCounters {
    accepted: AtomicU64,
    handoff_dropped: AtomicU64,
    pending_handoff: AtomicU64,
    closed_at_force_stop: AtomicU64,
}

impl AcceptCounters {
    fn snapshot(&self) -> AcceptStats {
        AcceptStats::new()
            .with_accepted(self.accepted.load(Ordering::Relaxed))
            .with_handoff_dropped(self.handoff_dropped.load(Ordering::Relaxed))
            .with_pending_handoff(self.pending_handoff.load(Ordering::Relaxed))
            .with_closed_at_force_stop(self.closed_at_force_stop.load(Ordering::Relaxed))
    }
}

/// An accepted socket that is not a session yet. It counts in its worker's `pending_handoff`
/// until its session task starts or it is closed. Dropping it any other way, such as with an
/// aborted listener task, a closed queue or a session task cancelled before it started, counts it
/// in `closed_at_force_stop`.
struct PendingSocket {
    stream: Option<TcpStream>,
    counters: Arc<AcceptCounters>,
}

impl PendingSocket {
    fn accepted(stream: TcpStream, counters: &Arc<AcceptCounters>) -> Self {
        counters.accepted.fetch_add(1, Ordering::Relaxed);
        counters.pending_handoff.fetch_add(1, Ordering::Relaxed);
        Self {
            stream: Some(stream),
            counters: Arc::clone(counters),
        }
    }

    #[cfg(test)]
    fn stream(&self) -> &TcpStream {
        self.stream
            .as_ref()
            .expect("a pending socket holds its stream until it leaves the count")
    }

    fn leave(&mut self) -> TcpStream {
        self.counters
            .pending_handoff
            .fetch_sub(1, Ordering::Relaxed);
        self.stream
            .take()
            .expect("a pending socket leaves the count once")
    }

    /// Hands the socket to the session task that has just started for it.
    fn into_session(mut self) -> TcpStream {
        self.leave()
    }

    /// Closes the socket because its worker could not take it.
    fn drop_handoff(mut self) {
        self.counters
            .handoff_dropped
            .fetch_add(1, Ordering::Relaxed);
        drop(self.leave());
    }
}

impl Drop for PendingSocket {
    fn drop(&mut self) {
        if self.stream.is_some() {
            self.counters
                .closed_at_force_stop
                .fetch_add(1, Ordering::Relaxed);
            drop(self.leave());
        }
    }
}

#[derive(Clone)]
pub struct HttpServerControl {
    state: Arc<AtomicU8>,
    draining: Arc<AtomicBool>,
    commands: Vec<mpsc::UnboundedSender<WorkerCommand>>,
    worker_handles: Arc<[WorkerHandle]>,
    accept_counters: Arc<[Arc<AcceptCounters>]>,
    abandon: Arc<watch::Sender<bool>>,
}

impl HttpServerControl {
    pub fn state(&self) -> HttpServerState {
        HttpServerState::from_raw(self.state.load(Ordering::Acquire))
    }

    /// Each worker's driver poll counters, in worker index order, including
    /// after the workers have stopped.
    pub fn worker_poll_stats(&self) -> Vec<WorkerPollStats> {
        self.worker_handles
            .iter()
            .map(WorkerHandle::poll_stats)
            .collect()
    }

    /// A read-only view of each worker, in worker index order.
    pub fn workers(&self) -> Vec<WorkerView> {
        self.worker_handles
            .iter()
            .enumerate()
            .map(|(index, handle)| WorkerView::new(index, handle.clone()))
            .collect()
    }

    /// Each worker's accept counters, in worker index order, including after the workers have
    /// stopped.
    pub fn accept_stats(&self) -> Vec<AcceptStats> {
        self.accept_counters
            .iter()
            .map(|counters| counters.snapshot())
            .collect()
    }

    /// Closes every listener without draining sessions. The returned future resolves once every
    /// worker has dropped its listeners; sockets already accepted still become sessions.
    /// Idempotent; `drain()` still closes listeners if this was not called. Dropping the future
    /// does not cancel the stop.
    pub fn stop_accepting(&self) -> StopAccepting {
        StopAccepting {
            acknowledgements: self
                .commands
                .iter()
                .map(|commands| {
                    let (acknowledgement, acknowledged) = oneshot::channel();
                    let _ = commands.send(WorkerCommand::StopAccepting(AcceptStopAck(Some(
                        acknowledgement,
                    ))));
                    Some(acknowledged)
                })
                .collect(),
        }
    }

    pub fn drain(&self) -> bool {
        if self
            .state
            .compare_exchange(
                HttpServerState::Running as u8,
                HttpServerState::Draining as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        self.draining.store(true, Ordering::Release);
        for sender in &self.commands {
            let _ = sender.send(WorkerCommand::Drain);
        }
        true
    }

    /// Fires every worker's `StopAbandon`: each worker of an owned application drops its
    /// pending `worker_stopping` future on its own thread and stops. Idempotent; non-blocking.
    pub fn abandon_stopping(&self) {
        self.abandon.send_replace(true);
    }

    /// Starts a forced stop without waiting for it: every worker aborts and joins its sessions.
    /// Returns false if a forced stop has already started or the server has stopped.
    ///
    /// This does not fire `abandon_stopping()`. A worker whose `worker_stopping` future never
    /// resolves stays in it; a caller holding only the control calls `abandon_stopping()` after
    /// its own bound, or wakes that future through its own signal.
    pub fn force_stop(&self) -> bool {
        let previous = self
            .state
            .swap(HttpServerState::ForceStopping as u8, Ordering::AcqRel);
        if matches!(
            HttpServerState::from_raw(previous),
            HttpServerState::ForceStopping | HttpServerState::Stopped
        ) {
            return false;
        }
        self.draining.store(true, Ordering::Release);
        for sender in &self.commands {
            let _ = sender.send(WorkerCommand::ForceStop);
        }
        true
    }
}

pub struct HttpServer {
    state: Arc<AtomicU8>,
    draining: Arc<AtomicBool>,
    addresses: Vec<SocketAddr>,
    tcp_max_segments: Vec<Option<u32>>,
    num_io_threads: usize,
    workers: RefCell<Vec<ManagedWorker>>,
    worker_handles: Arc<[WorkerHandle]>,
    accept_counters: Arc<[Arc<AcceptCounters>]>,
    commands: Vec<mpsc::UnboundedSender<WorkerCommand>>,
    abandon: Arc<watch::Sender<bool>>,
    tls_credentials: Option<Arc<dyn TlsCredentialProvider>>,
    tls: Option<Arc<ArcSwap<TlsServerOptions>>>,
    callback_failure: Arc<Mutex<Option<CallbackFailure>>>,
    exit_receivers: RefCell<Vec<Receiver<()>>>,
}

impl HttpServer {
    pub fn start(
        config: ServerConfig,
        handler_factory: HandlerFactory,
        observer: Option<Arc<dyn ServerObserver>>,
    ) -> Result<Self, HttpServerError> {
        Self::start_with_worker_factory(
            config,
            Arc::new(move |_, index| handler_factory(index)),
            observer,
        )
    }

    /// Starts a server whose handlers are constructed with their owning worker.
    /// Worker cleanup scopes release application resources before the workers are joined.
    pub fn start_with_worker_factory(
        config: ServerConfig,
        handler_factory: WorkerHandlerFactory,
        observer: Option<Arc<dyn ServerObserver>>,
    ) -> Result<Self, HttpServerError> {
        Self::start_workers(config, WorkerAppFactory::Handler(handler_factory), observer)
    }

    /// Starts the workers with the owned application. Returns the owner; keep it on this
    /// thread.
    ///
    /// `config` must enable `owned_http1` and `owned_http2`, and must set no filter factories,
    /// statistics factory or session lifecycle observer factory; the server-level
    /// `lifecycle_observer` is allowed.
    pub fn start_owned(
        config: ServerConfig,
        factory: OwnedApplicationFactory,
        observer: Option<Arc<dyn ServerObserver>>,
    ) -> Result<Self, HttpServerError> {
        check_owned_application(&config).map_err(HttpServerError::InvalidConfig)?;
        Self::start_workers(config, WorkerAppFactory::Owned(factory), observer)
    }

    fn start_workers(
        mut config: ServerConfig,
        app_factory: WorkerAppFactory,
        observer: Option<Arc<dyn ServerObserver>>,
    ) -> Result<Self, HttpServerError> {
        if config.command_capacity == 0 {
            return Err(HttpServerError::InvalidConfig(
                "command capacity must be nonzero",
            ));
        }
        config
            .session
            .check_owned()
            .map_err(HttpServerError::InvalidConfig)?;
        if config.session.non_abortive_close && config.tls_credentials.is_some() {
            return Err(HttpServerError::InvalidConfig(NON_ABORTIVE_CLOSE_WITH_TLS));
        }
        if config.socket.accept_distribution == AcceptDistribution::PerWorkerReusePort
            && (!config.socket.reuse_port || !config.socket.prebound_listeners.is_empty())
        {
            return Err(HttpServerError::InvalidConfig(
                "per-worker accept requires reuse_port and no prebound listeners",
            ));
        }
        let num_io_threads = if config.num_io_threads == 0 {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
        } else {
            config.num_io_threads
        };
        let per_worker_accept =
            config.socket.accept_distribution == AcceptDistribution::PerWorkerReusePort;
        let mut worker_listeners = if per_worker_accept {
            bind_per_worker_listeners(&config.socket, num_io_threads)?
        } else {
            Vec::new()
        };
        let listeners = if per_worker_accept {
            worker_listeners.remove(0)
        } else if config.socket.prebound_listeners.is_empty() {
            bind_listeners(&config.socket)?
        } else {
            let listeners = std::mem::take(&mut config.socket.prebound_listeners);
            if config.socket.use_zero_copy {
                listeners.iter().for_each(enable_zero_copy_best_effort);
            }
            listeners
        };
        if listeners.is_empty() {
            return Err(HttpServerError::InvalidConfig(
                "at least one listening socket is required",
            ));
        }
        #[cfg(test)]
        if config.admission_probe.is_some()
            && (num_io_threads != 1
                || listeners.len() != 1
                || config.command_capacity != 1
                || !config.shutdown_on_signals.is_empty()
                || config.tls_credentials.is_some())
        {
            return Err(HttpServerError::InvalidConfig(
                "admission probe requires one worker/listener, capacity one, no signals or TLS",
            ));
        }
        for listener in listeners.iter().chain(worker_listeners.iter().flatten()) {
            listener.set_nonblocking(true)?;
        }
        let addresses = listeners
            .iter()
            .map(TcpListener::local_addr)
            .collect::<Result<Vec<_>, _>>()?;
        let tcp_max_segments = listeners.iter().map(read_tcp_max_segment).collect();
        let tls = match config.tls_credentials.as_ref() {
            Some(provider) => Some(Arc::new(ArcSwap::from_pointee(
                provider.load().map_err(HttpServerError::TlsReload)?,
            ))),
            None => None,
        };
        let acceptor = AcceptorConfig {
            plaintext_protocol: config.plaintext_protocol,
            session: config.session,
            connection_filter: config.new_connection_filter,
            tls: tls.clone(),
            tls_observer: config.tls_observer,
            connection_observer: config.connection_observer,
            zero_copy_threshold: config.zero_copy_enable_threshold,
        };
        let state = Arc::new(AtomicU8::new(HttpServerState::Uninitialized as u8));
        let draining = Arc::new(AtomicBool::new(false));
        let callback_failure = Arc::new(Mutex::new(None));
        let mut workers = Vec::with_capacity(num_io_threads);
        let mut commands = Vec::with_capacity(num_io_threads);
        let mut receivers = Vec::with_capacity(num_io_threads);
        let mut accepted_senders = Vec::with_capacity(num_io_threads);
        let mut accepted_receivers = Vec::with_capacity(num_io_threads);
        let mut lifecycle_started = Vec::with_capacity(num_io_threads);
        let mut exit_receivers = Vec::with_capacity(num_io_threads);
        for index in 0..num_io_threads {
            let (sender, receiver) = mpsc::unbounded_channel();
            commands.push(sender);
            receivers.push(Some(receiver));
            let (accepted_sender, accepted_receiver) = mpsc::channel(
                config
                    .accepted_socket_capacity
                    .map_or(config.command_capacity, NonZeroUsize::get),
            );
            accepted_senders.push(accepted_sender);
            accepted_receivers.push(Some(accepted_receiver));
            let started = Arc::new(AtomicBool::new(false));
            lifecycle_started.push(Arc::clone(&started));
            let callback_observer = observer.clone();
            let callback_factories = config.filter_factories.clone();
            let callback_failure = Arc::clone(&callback_failure);
            let (exit_sender, exit_receiver) = std::sync::mpsc::sync_channel(1);
            workers.push(ManagedWorker::new_with_options(
                ManagedWorkerOptions::new(config.command_capacity)
                    .with_blocking(config.worker_blocking_pool),
                move || {
                    if started.load(Ordering::Acquire) {
                        notify_worker_stop(
                            index,
                            callback_observer,
                            callback_factories,
                            &callback_failure,
                        );
                    }
                    let _ = exit_sender.send(());
                },
            )?);
            exit_receivers.push(exit_receiver);
        }

        let accept_round_robin = Arc::new(AtomicUsize::new(0));
        let worker_handles: Arc<[WorkerHandle]> =
            workers.iter().map(ManagedWorker::handle).collect();
        let accept_counters: Arc<[Arc<AcceptCounters>]> = (0..num_io_threads)
            .map(|_| Arc::new(AcceptCounters::default()))
            .collect();
        let abandon = Arc::new(watch::Sender::new(false));
        let control = HttpServerControl {
            state: Arc::clone(&state),
            draining: Arc::clone(&draining),
            commands: commands.clone(),
            worker_handles: Arc::clone(&worker_handles),
            accept_counters: Arc::clone(&accept_counters),
            abandon: Arc::clone(&abandon),
        };
        let mut startup_receivers = Vec::with_capacity(num_io_threads);
        let mut owned_listeners = Some(listeners);
        let mut worker_listeners = worker_listeners.into_iter();
        for index in 0..num_io_threads {
            let receiver = receivers[index]
                .take()
                .expect("each server worker owns one command receiver");
            let accepted_receiver = accepted_receivers[index]
                .take()
                .expect("each server worker owns one accepted-socket receiver");
            let worker_config = WorkerConfig {
                index,
                acceptor: acceptor.clone(),
                app_factory: app_factory.clone(),
                filter_factories: config.filter_factories.clone(),
                observer: observer.clone(),
                draining: Arc::clone(&draining),
                control: control.clone(),
                shutdown_on_signals: if index == 0 {
                    config.shutdown_on_signals.clone()
                } else {
                    Vec::new()
                },
                callback_failure: Arc::clone(&callback_failure),
                lifecycle_started: Arc::clone(&lifecycle_started[index]),
                accept_distribution: config.socket.accept_distribution,
                accept_counters: Arc::clone(&accept_counters),
                lifecycle_observer: config.lifecycle_observer.clone(),
                #[cfg(test)]
                session_retirement_probe: config.session_retirement_probe.clone(),
                #[cfg(test)]
                admission_probe: config.admission_probe.clone(),
                #[cfg(test)]
                dequeue_hold_for_test: config.dequeue_hold_for_test.clone(),
                #[cfg(test)]
                session_spawned_for_test: config.session_spawned_for_test.clone(),
            };
            let worker_accepted = if per_worker_accept {
                vec![accepted_senders[index].clone()]
            } else {
                accepted_senders.clone()
            };
            let round_robin = Arc::clone(&accept_round_robin);
            let listeners = if index == 0 {
                owned_listeners.take().unwrap_or_default()
            } else if per_worker_accept {
                worker_listeners.next().unwrap_or_default()
            } else {
                Vec::new()
            };
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
            workers[index].handle().spawn_local(move |context| {
                worker_loop(
                    context,
                    receiver,
                    accepted_receiver,
                    worker_accepted,
                    round_robin,
                    listeners,
                    worker_config,
                    started_tx,
                )
            })?;
            startup_receivers.push(started_rx);
        }
        for receiver in startup_receivers {
            match receiver.recv() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(HttpServerError::WorkerStartup(error)),
                Err(error) => return Err(HttpServerError::WorkerStartup(error.to_string())),
            }
        }
        state.store(HttpServerState::Running as u8, Ordering::Release);
        Ok(Self {
            state,
            draining,
            addresses,
            tcp_max_segments,
            num_io_threads,
            workers: RefCell::new(workers),
            worker_handles,
            accept_counters,
            commands,
            abandon,
            tls_credentials: config.tls_credentials,
            tls,
            callback_failure,
            exit_receivers: RefCell::new(exit_receivers),
        })
    }

    pub fn state(&self) -> HttpServerState {
        HttpServerState::from_raw(self.state.load(Ordering::Acquire))
    }

    pub fn control(&self) -> HttpServerControl {
        HttpServerControl {
            state: Arc::clone(&self.state),
            draining: Arc::clone(&self.draining),
            commands: self.commands.clone(),
            worker_handles: Arc::clone(&self.worker_handles),
            accept_counters: Arc::clone(&self.accept_counters),
            abandon: Arc::clone(&self.abandon),
        }
    }

    /// Each worker's driver poll counters, in worker index order, including
    /// after the workers have stopped.
    pub fn worker_poll_stats(&self) -> Vec<WorkerPollStats> {
        self.worker_handles
            .iter()
            .map(WorkerHandle::poll_stats)
            .collect()
    }

    pub fn address(&self) -> Option<SocketAddr> {
        (self.addresses.len() == 1).then(|| self.addresses[0])
    }

    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    pub fn tcp_max_segment(&self, listener_index: usize) -> Option<u32> {
        self.tcp_max_segments.get(listener_index).copied().flatten()
    }

    pub const fn num_io_threads(&self) -> usize {
        self.num_io_threads
    }

    pub fn drain(&self) -> bool {
        self.control().drain()
    }

    /// Abandons graceful worker stop (as `HttpServerControl::abandon_stopping` does), so no
    /// worker waits on a `worker_stopping` future, then force-stops and joins every worker.
    pub fn force_stop(&self) -> Result<bool, HttpServerError> {
        if self.state() == HttpServerState::Stopped && self.workers.borrow().is_empty() {
            return Ok(false);
        }
        self.abandon.send_replace(true);
        let previous = self.state();
        if !matches!(previous, HttpServerState::ForceStopping) {
            let _ = self.drain();
        }
        self.state
            .store(HttpServerState::ForceStopping as u8, Ordering::Release);
        for sender in &self.commands {
            let _ = sender.send(WorkerCommand::ForceStop);
        }
        self.finish_workers()?;
        Ok(true)
    }

    /// Waits for a previously requested graceful drain and joins every worker. With an owned
    /// application, it also waits for every worker's `worker_stopping` future; a caller that
    /// needs a bound calls `HttpServerControl::abandon_stopping` first.
    pub fn wait_for_stop(&self) -> Result<bool, HttpServerError> {
        match self.state() {
            HttpServerState::Stopped => return Ok(false),
            HttpServerState::Draining => {}
            _ => {
                return Err(HttpServerError::InvalidConfig(
                    "wait_for_stop requires a draining server",
                ));
            }
        }
        self.finish_workers()?;
        Ok(true)
    }

    /// Joins every worker thread. It does not stop the workers: call it after they have closed
    /// (`WorkerView::closed()`), or after `HttpServerControl::abandon_stopping` once their stop
    /// has started. It never detaches a thread.
    pub fn join(self) -> Result<(), HttpServerError> {
        self.finish_workers()
    }

    fn finish_workers(&self) -> Result<(), HttpServerError> {
        std::mem::take(&mut *self.exit_receivers.borrow_mut())
            .into_iter()
            .for_each(|receiver| {
                let _ = receiver.recv();
            });
        let workers = std::mem::take(&mut *self.workers.borrow_mut());
        let mut worker_error = None;
        for worker in workers {
            if let Err(error) = worker.shutdown()
                && worker_error.is_none()
            {
                worker_error = Some(error);
            }
        }
        self.state
            .store(HttpServerState::Stopped as u8, Ordering::Release);
        if let Some(failure) = self
            .callback_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(HttpServerError::CallbackPanic {
                worker_index: failure.worker_index,
                stage: failure.stage,
            });
        }
        if let Some(error) = worker_error {
            return Err(HttpServerError::Worker(error));
        }
        Ok(())
    }

    pub fn update_tls_credentials(&self) -> Result<(), HttpServerError> {
        let Some(provider) = self.tls_credentials.as_ref() else {
            return Err(HttpServerError::InvalidConfig(
                "TLS credentials are not configured",
            ));
        };
        let Some(tls) = self.tls.as_ref() else {
            return Err(HttpServerError::InvalidConfig(
                "TLS runtime state is not configured",
            ));
        };
        let replacement = provider.load().map_err(HttpServerError::TlsReload)?;
        tls.store(Arc::new(replacement));
        Ok(())
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let _ = self.force_stop();
    }
}

async fn worker_loop(
    context: WorkerContext,
    mut receiver: mpsc::UnboundedReceiver<WorkerCommand>,
    mut accepted_receiver: mpsc::Receiver<PendingSocket>,
    accepted_senders: Vec<mpsc::Sender<PendingSocket>>,
    round_robin: Arc<AtomicUsize>,
    listeners: Vec<TcpListener>,
    config: WorkerConfig,
    started: SyncSender<Result<(), String>>,
) {
    let app = match &config.app_factory {
        WorkerAppFactory::Handler(handler_factory) => {
            let user_handler = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler_factory(context.clone(), config.index)
            })) {
                Ok(handler) => handler,
                Err(_) => {
                    record_callback_failure(
                        &config.callback_failure,
                        config.index,
                        ServerCallbackStage::HandlerFactory,
                    );
                    let _ = started.send(Err("handler factory panicked".to_owned()));
                    context.handle().begin_shutdown();
                    return;
                }
            };
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                build_handler(user_handler, &config.filter_factories)
            })) {
                Ok(handler) => WorkerApp::Handler(handler),
                Err(_) => {
                    record_callback_failure(
                        &config.callback_failure,
                        config.index,
                        ServerCallbackStage::FilterConstruction,
                    );
                    let _ = started.send(Err("filter factory panicked".to_owned()));
                    context.handle().begin_shutdown();
                    return;
                }
            }
        }
        WorkerAppFactory::Owned(factory) => {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                factory(context.clone(), config.index)
            })) {
                Ok(Ok(factory)) => WorkerApp::Owned(factory),
                Ok(Err(error)) => {
                    let _ = started.send(Err(error.to_string()));
                    context.handle().begin_shutdown();
                    return;
                }
                Err(payload) => {
                    record_callback_failure(
                        &config.callback_failure,
                        config.index,
                        ServerCallbackStage::HandlerFactory,
                    );
                    discard_panicking_value(payload);
                    let _ = started.send(Err("owned application factory panicked".to_owned()));
                    context.handle().begin_shutdown();
                    return;
                }
            }
        }
    };
    if let Some(observer) = config.observer.as_ref() {
        invoke_callback(&config, ServerCallbackStage::ObserverStart, || {
            observer.on_thread_start(config.index);
        });
    }
    for factory in &config.filter_factories {
        invoke_callback(&config, ServerCallbackStage::FilterStart, || {
            factory.on_server_start(config.index);
        });
    }
    config.lifecycle_started.store(true, Ordering::Release);
    let acceptor = Rc::new(config.acceptor.clone());
    #[cfg(test)]
    let mut admission_gate = config
        .admission_probe
        .as_ref()
        .and_then(|probe| probe.take_gate());
    #[cfg(test)]
    let mut dequeue_held = false;

    let (stop_accepting, accept_stop) = watch::channel(false);
    let mut listener_tasks = JoinSet::new();
    for listener in listeners {
        let listener = match tokio::net::TcpListener::from_std(listener) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = started.send(Err(error.to_string()));
                context.handle().begin_shutdown();
                return;
            }
        };
        let stop = AcceptStop {
            draining: Arc::clone(&config.draining),
            requested: accept_stop.clone(),
        };
        let task = match config.accept_distribution {
            AcceptDistribution::Worker0 => listener_tasks.spawn_local(round_robin_accept_loop(
                listener,
                RoundRobinHandoff {
                    senders: accepted_senders.clone(),
                    next: Arc::clone(&round_robin),
                    counters: Arc::clone(&config.accept_counters),
                },
                stop,
                #[cfg(test)]
                config.admission_probe.clone(),
                #[cfg(test)]
                config.index,
            )),
            AcceptDistribution::PerWorkerReusePort => {
                listener_tasks.spawn_local(per_worker_accept_loop(
                    listener,
                    accepted_senders[0].clone(),
                    Arc::clone(&config.accept_counters[config.index]),
                    stop,
                ))
            }
        };
        #[cfg(test)]
        if let Some(probe) = &config.admission_probe {
            probe.publish(AdmissionEvent::ListenerRegistered(AdmissionListenerKey {
                worker_index: config.index,
                task_id: task.id(),
            }));
        }
        drop(task);
    }
    let mut signal_tasks: JoinSet<()> = JoinSet::new();
    #[cfg(unix)]
    for signal in &config.shutdown_on_signals {
        let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::from_raw(*signal))
        else {
            continue;
        };
        let control = config.control.clone();
        signal_tasks.spawn_local(async move {
            signal.recv().await;
            control.drain();
        });
    }
    let _ = started.send(Ok(()));

    let mut next_session = 1_u64;
    let worker_id = u32::try_from(config.index).unwrap_or(u32::MAX);
    let mut controls: BTreeMap<u64, Rc<RefCell<Option<SessionControl>>>> = BTreeMap::new();
    let mut task_sessions = HashMap::new();
    let mut sessions: JoinSet<(u64, Result<UniplexRunSummary, HttpError>)> = JoinSet::new();
    let mut drain_requested = false;
    let mut stop_acknowledgements = Vec::new();
    let per_worker_accept = config.accept_distribution == AcceptDistribution::PerWorkerReusePort;
    // Until the accepted-socket queue is closed and empty.
    let mut receiving = true;
    loop {
        tokio::select! {
            biased;
            command = receiver.recv() => {
                match command {
                    Some(WorkerCommand::Drain) => {
                        drain_requested = true;
                        if per_worker_accept {
                            // Each listener task drops its listener, queues the socket it holds and
                            // exits; the queue is closed once they all have, and every socket in
                            // it becomes a draining session.
                            stop_accepting.send_replace(true);
                            if listener_tasks.is_empty() {
                                accepted_receiver.close();
                            }
                        } else {
                            listener_tasks.abort_all();
                        }
                        signal_tasks.abort_all();
                        for control in controls.values() {
                            if let Some(control) = control.borrow().as_ref() {
                                invoke_callback(&config, ServerCallbackStage::SessionDrain, || {
                                    control.drain();
                                });
                            }
                        }
                    }
                    Some(WorkerCommand::ForceStop) => {
                        #[cfg(test)]
                        if let Some(probe) = &config.admission_probe {
                            probe.publish(AdmissionEvent::ForceObserved {
                                worker_index: config.index,
                                queued: accepted_receiver.len(),
                                capacity: accepted_receiver.capacity(),
                                max_capacity: accepted_receiver.max_capacity(),
                                held: dequeue_held && admission_gate.is_some(),
                                sessions: sessions.len(),
                                controls: controls.len(),
                                mappings: task_sessions.len(),
                            });
                        }
                        #[cfg(test)]
                        if let Some(probe) = &config.session_retirement_probe {
                            probe.confirm(sessions.is_empty() && controls.is_empty() && task_sessions.is_empty());
                        }
                        listener_tasks.abort_all();
                        signal_tasks.abort_all();
                        for control in controls.values() {
                            if let Some(control) = control.borrow().as_ref() {
                                invoke_callback(
                                    &config,
                                    ServerCallbackStage::SessionForceStop,
                                    || {
                                        control.force_stop();
                                    },
                                );
                            }
                        }
                        sessions.abort_all();
                        while sessions.join_next().await.is_some() {}
                        controls.clear();
                        task_sessions.clear();
                        #[cfg(test)]
                        if let Some(probe) = &config.admission_probe {
                            observe_accept_reaps(&mut listener_tasks, probe, config.index).await;
                        }
                        while listener_tasks.join_next().await.is_some() {}
                        while signal_tasks.join_next().await.is_some() {}
                        // Closes the queued sockets, each counted in `closed_at_force_stop`.
                        accepted_receiver.close();
                        while let Ok(socket) = accepted_receiver.try_recv() {
                            drop(socket);
                        }
                        break;
                    }
                    Some(WorkerCommand::StopAccepting(acknowledgement)) => {
                        stop_accepting.send_replace(true);
                        stop_acknowledgements.push(acknowledgement);
                        if listener_tasks.is_empty() {
                            stop_acknowledgements.clear();
                        }
                    }
                    None => break,
                }
            }
            Some(_) = listener_tasks.join_next(), if !listener_tasks.is_empty() => {
                if listener_tasks.is_empty() {
                    stop_acknowledgements.clear();
                    if per_worker_accept && drain_requested {
                        accepted_receiver.close();
                    }
                }
            }
            accepted = async {
                #[cfg(test)]
                if let Some(hold) = &config.dequeue_hold_for_test {
                    hold.hold(config.index).await;
                }
                #[cfg(test)]
                if let Some(gate) = admission_gate.as_mut() {
                    if !dequeue_held {
                        dequeue_held = true;
                        if let Some(probe) = &config.admission_probe {
                            probe.publish(AdmissionEvent::DequeueHeld { worker_index: config.index });
                        }
                    }
                    let _ = gate.await;
                    if let Some(probe) = &config.admission_probe {
                        probe.confirm(false);
                    }
                    std::future::pending::<()>().await;
                }
                accepted_receiver.recv().await
            }, if receiving && (per_worker_accept || !drain_requested) => {
                if let Some(socket) = accepted {
                    let id = next_session;
                    next_session = next_session.wrapping_add(1);
                    let slot = Rc::new(RefCell::new(None));
                    #[cfg(test)]
                    if let Some(probe) = &config.session_retirement_probe {
                        probe.confirm(id != 0 && !controls.contains_key(&id));
                    }
                    controls.insert(id, Rc::clone(&slot));
                    let lifecycle_observer = config.lifecycle_observer.clone();
                    let context = context.clone();
                    let app = app.clone();
                    let acceptor = Rc::clone(&acceptor);
                    let draining = Arc::clone(&config.draining);
                    let callback_failure = Arc::clone(&config.callback_failure);
                    let worker_index = config.index;
                    #[cfg(test)]
                    let probe = config.session_retirement_probe.clone();
                    #[cfg(test)]
                    let gate = probe.as_ref().and_then(|probe| probe.take_gate(worker_index, id));
                    let task = sessions.spawn_local(async move {
                        let socket = socket.into_session();
                        let connection_id = ConnectionId {
                            worker: worker_id,
                            serial: id,
                        };
                        let lifecycle = lifecycle_observer.map(|observer| {
                            ConnectionLifecycle::accepted(
                                observer,
                                connection_id,
                                worker_index,
                                Arc::clone(&callback_failure),
                            )
                        });
                        #[cfg(test)]
                        let key = SessionRetirementKey {
                            worker_index,
                            session_id: id,
                            task_id: tokio::task::id(),
                        };
                        #[cfg(test)]
                        let mut run_returned = false;
                        let result = match accept_session(
                            context,
                            socket,
                            app,
                            connection_id,
                            Rc::clone(&acceptor),
                            draining,
                            worker_index,
                            Arc::clone(&callback_failure),
                        )
                        .await
                        {
                            Ok(accepted) => {
                                #[cfg(test)]
                                if let Some(probe) = &probe {
                                    probe.publish(
                                        key,
                                        SessionRetirementRecord::TransportBound(accepted.retirement),
                                    );
                                }
                                let mut lifecycle = lifecycle.map(|lifecycle| {
                                    lifecycle.admitted(accepted.protocol, &accepted.control)
                                });
                                slot.replace(Some(accepted.control));
                                let notification = acceptor.connection_observer.as_ref().map_or(Ok(()), |observer| {
                                    invoke_accept_callback(
                                        &callback_failure,
                                        worker_index,
                                        ServerCallbackStage::AcceptedConnectionObserver,
                                        || observer.on_registered(),
                                    )
                                });
                                if notification.is_err() {
                                    let control = slot.borrow().clone();
                                    if let Some(control) = control {
                                        discard_panicking_value(invoke_accept_callback(
                                            &callback_failure,
                                            worker_index,
                                            ServerCallbackStage::SessionForceStop,
                                            || control.force_stop(),
                                        ));
                                    }
                                }
                                drop(acceptor);
                                drop(callback_failure);
                                let result = match lifecycle.as_mut() {
                                    Some(lifecycle) => lifecycle.run(accepted.session).await,
                                    None => run_session(accepted.session).await,
                                };
                                slot.borrow_mut().take();
                                #[cfg(test)]
                                {
                                    run_returned = true;
                                }
                                let result = if let Err(error) = notification {
                                    discard_panicking_value(result);
                                    Err(error)
                                } else {
                                    result
                                };
                                if let Some(lifecycle) = lifecycle {
                                    lifecycle.close(&result);
                                }
                                result
                            }
                            Err(failure) => {
                                if let Some(lifecycle) = lifecycle {
                                    lifecycle.close(&failure.outcome());
                                }
                                Err(failure.into_error())
                            }
                        };
                        #[cfg(test)]
                        {
                            drop(slot);
                            if let Some(probe) = &probe
                                && probe.confirm(run_returned)
                            {
                                probe.publish(key, SessionRetirementRecord::RunReturned);
                                if let Some(gate) = gate {
                                    probe.confirm(matches!(
                                        tokio::time::timeout(gate.timeout, gate.release).await,
                                        Ok(Ok(()))
                                    ));
                                }
                            }
                        }
                        (id, result)
                    });
                    #[cfg(test)]
                    if let Some(probe) = &config.session_retirement_probe {
                        probe.confirm(!task_sessions.contains_key(&task.id()));
                    }
                    task_sessions.insert(task.id(), id);
                    #[cfg(test)]
                    if let Some(probe) = &config.admission_probe {
                        probe.session_registered();
                    }
                    #[cfg(test)]
                    if let Some(probe) = &config.session_retirement_probe {
                        probe.publish(
                            SessionRetirementKey {
                                worker_index,
                                session_id: id,
                                task_id: task.id(),
                            },
                            SessionRetirementRecord::Registered,
                        );
                    }
                    #[cfg(test)]
                    if let Some(hook) = &config.session_spawned_for_test {
                        hook();
                    }
                } else {
                    receiving = false;
                }
            }
            completed = sessions.join_next_with_id(), if !sessions.is_empty() => {
                #[cfg(test)]
                let observed = config.session_retirement_probe.as_ref().and_then(|probe| {
                    let (task_id, id, outcome) = match &completed {
                        Some(Ok((task_id, (id, result)))) => {
                            (*task_id, *id, SessionRunOutcome::Returned(result.clone()))
                        }
                        Some(Err(error)) => {
                            let task_id = error.id();
                            let Some(id) = task_sessions.get(&task_id).copied() else {
                                probe.confirm(false);
                                return None;
                            };
                            (
                                task_id,
                                id,
                                SessionRunOutcome::JoinFailed {
                                    panic: error.is_panic(),
                                    cancelled: error.is_cancelled(),
                                },
                            )
                        }
                        None => return None,
                    };
                    let mapping_removed = task_sessions.get(&task_id) == Some(&id);
                    let control_removed = controls.contains_key(&id);
                    probe.confirm(mapping_removed && control_removed);
                    Some((
                        SessionRetirementKey {
                            worker_index: config.index,
                            session_id: id,
                            task_id,
                        },
                        SessionReaped {
                            outcome,
                            mapping_removed,
                            control_removed,
                            remaining_tasks: 0,
                            remaining_controls: 0,
                        },
                    ))
                });
                match completed {
                    Some(Ok((task_id, (id, _)))) => {
                        task_sessions.remove(&task_id);
                        controls.remove(&id);
                    }
                    Some(Err(error)) => {
                        if let Some(id) = task_sessions.remove(&error.id()) {
                            controls.remove(&id);
                        }
                    }
                    None => {}
                }
                #[cfg(test)]
                if let Some((key, mut reaped)) = observed
                    && let Some(probe) = &config.session_retirement_probe
                {
                    reaped.mapping_removed &= !task_sessions.contains_key(&key.task_id);
                    reaped.control_removed &= !controls.contains_key(&key.session_id);
                    reaped.remaining_tasks = task_sessions.len();
                    reaped.remaining_controls = controls.len();
                    probe.confirm(reaped.mapping_removed && reaped.control_removed);
                    probe.publish(key, SessionRetirementRecord::Reaped(reaped));
                }
            }
        }
        if drain_requested
            && sessions.is_empty()
            && (!per_worker_accept || (listener_tasks.is_empty() && !receiving))
        {
            break;
        }
    }
    // A drained `Worker0`-mode worker stops dequeuing, so the sockets still queued are dropped. A
    // drained per-worker queue is already closed and empty here.
    accepted_receiver.close();
    while let Ok(socket) = accepted_receiver.try_recv() {
        socket.drop_handoff();
    }
    listener_tasks.abort_all();
    #[cfg(test)]
    if let Some(probe) = &config.admission_probe {
        observe_accept_reaps(&mut listener_tasks, probe, config.index).await;
    }
    while listener_tasks.join_next().await.is_some() {}
    stop_acknowledgements.clear();
    signal_tasks.abort_all();
    while signal_tasks.join_next().await.is_some() {}
    #[cfg(test)]
    if let Some(probe) = &config.session_retirement_probe {
        probe.confirm(sessions.is_empty() && controls.is_empty() && task_sessions.is_empty());
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
    if let WorkerApp::Owned(factory) = &app {
        await_worker_stopping(factory.as_ref(), &config).await;
    }
    context.handle().begin_shutdown();
}

/// Runs the owned application's `worker_stopping` and waits for its future, or for the owner to
/// abandon graceful stop: an abandon that has fired before the first poll drops the future
/// unpolled, and the abandon is checked before each poll. The future is dropped on this thread.
/// A panic in the call, a poll or the drop is recorded as the worker's callback failure.
async fn await_worker_stopping(factory: &dyn OwnedConnectionFactory, config: &WorkerConfig) {
    let abandon = StopAbandon::new(config.control.abandon.subscribe());
    let mut stopping = None;
    invoke_callback(config, ServerCallbackStage::HandlerFactory, || {
        stopping = factory.worker_stopping(abandon.clone());
    });
    let Some(mut stopping) = stopping else {
        return;
    };
    if !abandon.is_abandoned() {
        let mut abandoned = std::pin::pin!(abandon.abandoned());
        std::future::poll_fn(|context| {
            if abandoned.as_mut().poll(context).is_ready() {
                return Poll::Ready(());
            }
            let mut poll = Poll::Ready(());
            invoke_callback(config, ServerCallbackStage::HandlerFactory, || {
                poll = stopping.as_mut().poll(context);
            });
            poll
        })
        .await;
    }
    invoke_callback(config, ServerCallbackStage::HandlerFactory, || {
        drop(stopping);
    });
}

/// Every worker's accepted-socket queue and counters, which worker 0 hands sockets to in turn.
struct RoundRobinHandoff {
    senders: Vec<mpsc::Sender<PendingSocket>>,
    next: Arc<AtomicUsize>,
    counters: Arc<[Arc<AcceptCounters>]>,
}

/// Tells an accept loop to stop accepting: a drain started, or a stop was requested.
struct AcceptStop {
    draining: Arc<AtomicBool>,
    requested: watch::Receiver<bool>,
}

impl AcceptStop {
    fn draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Resolves once a stop is requested, or once the worker loop that requests it is gone.
    async fn requested(&mut self) {
        let _ = self.requested.wait_for(|stop| *stop).await;
    }
}

/// Accepts on worker 0's `listener` and hands each socket to the next worker in turn. A socket
/// whose worker's queue is full or closed is dropped, which closes it.
async fn round_robin_accept_loop(
    listener: tokio::net::TcpListener,
    handoff: RoundRobinHandoff,
    mut stop: AcceptStop,
    #[cfg(test)] admission_probe: Option<Arc<AdmissionProbe>>,
    #[cfg(test)] worker_index: usize,
) {
    while !stop.draining() {
        let accepted = tokio::select! {
            biased;
            () = stop.requested() => break,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, peer)) => {
                #[cfg(not(test))]
                let _ = peer;
                let Ok(stream) = stream.into_std() else {
                    #[cfg(test)]
                    if let Some(probe) = &admission_probe {
                        probe.confirm(false);
                    }
                    continue;
                };
                let worker_accepted = &handoff.senders;
                let index = handoff.next.fetch_add(1, Ordering::Relaxed) % worker_accepted.len();
                let socket = PendingSocket::accepted(stream, &handoff.counters[index]);
                #[cfg(test)]
                if let Some(probe) = &admission_probe {
                    let local = socket.stream().local_addr().ok();
                    let attempt = match worker_accepted[index].try_send(socket) {
                        Ok(()) => AdmissionAttempt::Enqueued,
                        Err(mpsc::error::TrySendError::Full(socket)) => {
                            socket.drop_handoff();
                            AdmissionAttempt::Full
                        }
                        Err(mpsc::error::TrySendError::Closed(socket)) => {
                            socket.drop_handoff();
                            AdmissionAttempt::Closed
                        }
                    };
                    probe.publish(AdmissionEvent::Socket {
                        key: AdmissionListenerKey {
                            worker_index,
                            task_id: tokio::task::id(),
                        },
                        socket: AdmissionSocket {
                            worker_index: index,
                            local,
                            peer,
                            capacity: worker_accepted[index].capacity(),
                            max_capacity: worker_accepted[index].max_capacity(),
                        },
                        attempt,
                    });
                } else if let Err(error) = worker_accepted[index].try_send(socket) {
                    error.into_inner().drop_handoff();
                }
                #[cfg(not(test))]
                if let Err(error) = worker_accepted[index].try_send(socket) {
                    error.into_inner().drop_handoff();
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(error) => match accept_retry_delay(&error) {
                Some(delay) => tokio::select! {
                    biased;
                    () = stop.requested() => break,
                    () = tokio::time::sleep(delay) => {}
                },
                None => tokio::task::yield_now().await,
            },
        }
    }
}

/// Accepts on one of this worker's own `listener`s and queues each socket for this worker's
/// session loop. A full queue pauses the accepts, which leaves new connections in the kernel
/// backlog.
async fn per_worker_accept_loop(
    listener: tokio::net::TcpListener,
    queue: mpsc::Sender<PendingSocket>,
    counters: Arc<AcceptCounters>,
    mut stop: AcceptStop,
) {
    while !stop.draining() {
        let accepted = tokio::select! {
            biased;
            () = stop.requested() => break,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((stream, _)) => {
                let Ok(stream) = stream.into_std() else {
                    continue;
                };
                let send = queue.send(PendingSocket::accepted(stream, &counters));
                tokio::pin!(send);
                let sent = tokio::select! {
                    biased;
                    sent = &mut send => sent,
                    () = stop.requested() => {
                        // Closes the listener at once. The session loop keeps dequeuing until
                        // this task exits, so the send still completes.
                        drop(listener);
                        let _ = send.await;
                        return;
                    }
                };
                if sent.is_err() {
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(error) => match accept_retry_delay(&error) {
                Some(delay) => tokio::select! {
                    biased;
                    () = stop.requested() => break,
                    () = tokio::time::sleep(delay) => {}
                },
                None => tokio::task::yield_now().await,
            },
        }
    }
}

/// Returns how long the acceptor waits before accepting again after `error`,
/// or `None` to retry immediately.
///
/// accept(2) reports some errors of a single pending connection (the peer
/// aborted, a firewall refused it, a network error was already pending), and
/// the listener stays healthy. Any other error, such as descriptor exhaustion
/// (`EMFILE`, `ENFILE`) or memory pressure, persists until something else
/// changes, so the acceptor pauses instead of spinning on it.
pub(crate) fn accept_retry_delay(error: &io::Error) -> Option<Duration> {
    let per_connection = match error.raw_os_error() {
        Some(
            libc::ECONNABORTED
            | libc::EINTR
            | libc::EPERM
            | libc::EPROTO
            | libc::ENOPROTOOPT
            | libc::ENETDOWN
            | libc::ENETUNREACH
            | libc::EHOSTDOWN
            | libc::EHOSTUNREACH
            | libc::EOPNOTSUPP,
        ) => true,
        #[cfg(target_os = "linux")]
        Some(libc::ENONET) => true,
        _ => false,
    };
    (!per_connection).then_some(ACCEPT_ERROR_BACKOFF)
}

/// Rejects a configuration that `HttpServer::start_owned` does not support: an owned
/// application serves both protocols on the owned drivers, and the classic per-session hooks
/// apply only to the built-in sessions around a handler.
fn check_owned_application(config: &ServerConfig) -> Result<(), &'static str> {
    if !config.session.owned_http1 {
        return Err("start_owned requires owned_http1");
    }
    if !config.session.owned_http2 {
        return Err("start_owned requires owned_http2");
    }
    if !config.filter_factories.is_empty() {
        return Err("start_owned does not support filter_factories");
    }
    if config.session.stats_factory.is_some() {
        return Err("start_owned does not support stats_factory");
    }
    if config.session.lifecycle_observer_factory.is_some() {
        return Err("start_owned does not support lifecycle_observer_factory");
    }
    Ok(())
}

fn build_handler(
    user_handler: HttpHandlerRef,
    filter_factories: &[Arc<dyn ServerFilterFactory>],
) -> HttpHandlerRef {
    if filter_factories.is_empty() {
        return user_handler;
    }
    let mut filters = HttpFilterFactoryHandler::new();
    for factory in filter_factories {
        filters.add_filter_factory(Some(factory.make_filter_factory()));
    }
    let chain = Rc::new(filters.into_chain_handler());
    chain.set_next_handler(Some(user_handler));
    chain as HttpHandlerRef
}

fn notify_worker_stop(
    worker_index: usize,
    observer: Option<Arc<dyn ServerObserver>>,
    filter_factories: Vec<Arc<dyn ServerFilterFactory>>,
    callback_failure: &Arc<Mutex<Option<CallbackFailure>>>,
) {
    if let Some(observer) = observer {
        invoke_lifecycle_callback(
            callback_failure,
            worker_index,
            ServerCallbackStage::ObserverStop,
            || {
                observer.on_thread_stop(worker_index);
            },
        );
        discard_panicking_value(observer);
    }
    for factory in filter_factories {
        invoke_lifecycle_callback(
            callback_failure,
            worker_index,
            ServerCallbackStage::FilterStop,
            || {
                factory.on_server_stop(worker_index);
            },
        );
        discard_panicking_value(factory);
    }
}

fn invoke_lifecycle_callback(
    callback_failure: &Arc<Mutex<Option<CallbackFailure>>>,
    worker_index: usize,
    stage: ServerCallbackStage,
    callback: impl FnOnce(),
) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        record_callback_failure(callback_failure, worker_index, stage);
        discard_panicking_value(payload);
    }
}

pub(crate) fn discard_panicking_value<T>(value: T) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)));
}

fn invoke_callback(config: &WorkerConfig, stage: ServerCallbackStage, callback: impl FnOnce()) {
    invoke_lifecycle_callback(&config.callback_failure, config.index, stage, callback);
}

pub(crate) fn record_callback_failure(
    callback_failure: &Arc<Mutex<Option<CallbackFailure>>>,
    worker_index: usize,
    stage: ServerCallbackStage,
) {
    let mut failure = callback_failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    failure.get_or_insert(CallbackFailure {
        worker_index,
        stage,
    });
}

fn bind_listeners(config: &ServerSocketConfig) -> Result<Vec<TcpListener>, io::Error> {
    if config.bind_addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "server has no bind addresses",
        ));
    }
    config
        .bind_addresses
        .iter()
        .copied()
        .map(|address| bind_listener(address, config))
        .collect()
}

/// Binds every worker's listeners for per-worker accept. Worker 0 binds first, and the other
/// workers bind the addresses it was assigned, so a port 0 resolves to one shared port.
fn bind_per_worker_listeners(
    config: &ServerSocketConfig,
    workers: usize,
) -> Result<Vec<Vec<TcpListener>>, io::Error> {
    let first = bind_listeners(config)?;
    let assigned = first
        .iter()
        .map(TcpListener::local_addr)
        .collect::<Result<Vec<_>, _>>()?;
    std::iter::once(Ok(first))
        .chain((1..workers).map(|_| {
            assigned
                .iter()
                .map(|&address| bind_listener(address, config))
                .collect()
        }))
        .collect()
}

fn bind_listener(
    address: SocketAddr,
    config: &ServerSocketConfig,
) -> Result<TcpListener, io::Error> {
    let domain = if address.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    if config.reuse_port {
        set_reuse_port(&socket)?;
    }
    if let Some(max_segment) = config.tcp_max_segment {
        set_tcp_max_segment(&socket, max_segment)?;
    }
    if config.use_zero_copy {
        enable_zero_copy_best_effort(&socket);
    }
    socket.bind(&SockAddr::from(address))?;
    socket.listen(config.backlog)?;
    let listener: TcpListener = socket.into();
    Ok(listener)
}

#[cfg(target_os = "linux")]
fn enable_zero_copy_best_effort(socket: &impl AsRawFd) {
    let value: libc::c_int = 1;
    // SAFETY: `socket` owns a valid fd for this call, and `value` is an initialized
    // `c_int` whose pointer and reported length match.
    unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ZEROCOPY,
            (&raw const value).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn enable_zero_copy_best_effort<T>(_socket: &T) {}

#[cfg(unix)]
fn set_socket_option(
    socket: &Socket,
    level: libc::c_int,
    option: libc::c_int,
    value: libc::c_int,
) -> Result<(), io::Error> {
    use std::os::fd::AsRawFd;

    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            level,
            option,
            (&raw const value).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn set_reuse_port(socket: &Socket) -> Result<(), io::Error> {
    set_socket_option(socket, libc::SOL_SOCKET, libc::SO_REUSEPORT, 1)
}

#[cfg(not(unix))]
fn set_reuse_port(_socket: &Socket) -> Result<(), io::Error> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn set_tcp_max_segment(socket: &Socket, maximum: u32) -> Result<(), io::Error> {
    let maximum = libc::c_int::try_from(maximum)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "TCP_MAXSEG exceeds c_int"))?;
    set_socket_option(socket, libc::IPPROTO_TCP, libc::TCP_MAXSEG, maximum)
}

#[cfg(not(unix))]
fn set_tcp_max_segment(_socket: &Socket, _maximum: u32) -> Result<(), io::Error> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "TCP_MAXSEG is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn read_tcp_max_segment(listener: &TcpListener) -> Option<u32> {
    use std::os::fd::AsRawFd;

    let mut value: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            listener.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_MAXSEG,
            (&raw mut value).cast(),
            &raw mut length,
        )
    };
    (result == 0 && value >= 0).then_some(value as u32)
}

#[cfg(not(unix))]
fn read_tcp_max_segment(_listener: &TcpListener) -> Option<u32> {
    None
}
use arc_swap::ArcSwap;
