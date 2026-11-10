//! Worker-local upstream session pooling and destination cache.

mod session_list;

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::ops::ControlFlow;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::task_group::TaskGroupError;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_runtime::worker::WorkerState;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_transport::tls::TlsSessionStore;
pub(crate) use session_list::Slot as SessionSlot;
use thiserror::Error;
use tokio::sync::Notify;
use tokio::sync::watch;

use crate::connector::CertVerifyLogFn;
use crate::connector::ClientSession;
use crate::connector::ConnectionParams;
use crate::connector::CoroDnsResolver;
use crate::connector::DnsResolver;
use crate::connector::HttpConnector;
use crate::connector::LocalClientFuture;
use crate::connector::ResolvedHost;
use crate::connector::SessionConnector;
use crate::connector::SessionParams;
use crate::experimental::owned::OwnedClientSession;
use crate::experimental::owned::OwnedSessionSelection;
use crate::experimental::owned::worker_admits;
use crate::pool::session_list::Class;
use crate::pool::session_list::Linked;
use crate::pool::session_list::SessionList;

const DEFAULT_MAX_POOLS: usize = 10;
const DEFAULT_REAP_SCAN: usize = 5;
static NEXT_MAX_AGE_JITTER: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

#[derive(Clone, Debug, Error)]
pub enum PoolError {
    /// Driver ownership could not be registered before publication.
    #[error("pool cleanup registration failed: {source}")]
    CleanupRegistration { source: CleanupRegistrationError },
    /// Every task was joined, but at least one failed.
    #[error("pool task join failed: {source}")]
    TaskJoin { source: TaskGroupError },
    /// An owned session completed with a failure.
    #[error("pool session join failed: {source}")]
    SessionJoin { source: HttpError },
    /// Cleanup continued after a cancellation or teardown callback panicked.
    #[error("pool cleanup callback panicked")]
    CleanupPanicked,
    /// These parameters belong to native connection creation, not attachment.
    #[error("native connection/session configuration is unavailable for an attached factory")]
    NativeConfigurationOnly,
    /// The factory's immediate-hop security disagrees with the pool contract.
    #[error("attached session security does not match the pool")]
    SecurityMismatch,
    /// The attached pool's connection bounds refuse another session; see
    /// [`HttpSessionPool::full`].
    #[error("attached pool ownership capacity is exhausted")]
    OwnershipCapacity,
    #[error("Connect Failed: {source}")]
    ConnectFailed { source: HttpError },
    /// The selected attached-factory failure retains its original external cause.
    #[error("Connect Failed: {source}")]
    AttachedConnectFailed { source: AttachedConnectFailure },
    #[error("request reservation failed: {source}")]
    ReservationFailed { source: HttpError },
    #[error("Timed out waiting for session")]
    Timeout,
    #[error("Cancelled")]
    Cancelled,
    #[error("Pool is draining")]
    Draining,
    #[error("max waiters exceeded")]
    MaxWaiters,
    #[error("session belongs to a different worker")]
    WrongWorker,
    #[error("session is not an unbound idle session")]
    InvalidSessionState,
    #[error("default cache connection parameters must configure TLS")]
    InvalidConnectionParams,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolParams {
    pub max_connections: u32,
    pub max_connection_attempts: u8,
    pub enable_tls_session_caching: bool,
    pub connect_timeout: Duration,
    pub max_age: Duration,
    pub max_waiters: u16,
    /// Roxygen-only (`DIV-032`): retires a session once it has had no active or reserved
    /// exchange for this long, through a pool task and again at acquisition; zero, the
    /// default, disables it. A session with an active or reserved exchange is never retired,
    /// however long the exchange runs. Native pools only: an attached pool's
    /// [`AttachedPoolParams`] has no idle timeout.
    pub idle_timeout: Duration,
    /// Roxygen-only (`DIV-032`): which idle session an acquisition takes when no busy
    /// session can take another exchange. Native pools only.
    pub reuse_order: SessionReuseOrder,
    /// Roxygen-only (`DIV-032`): when the pool task and acquisitions retire
    /// sessions idle for `idle_timeout`.
    pub idle_sweep: IdleSweep,
}

/// When idle retirement retires sessions (`DIV-032`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IdleSweep {
    /// The pool task retires each session when it has been idle for
    /// `idle_timeout`, and an acquisition retires every such session it finds.
    #[default]
    AtExpiry,
    /// As hyper-util 0.1.20's legacy pool (reqwest 0.12.28): the pool task sweeps
    /// every `max(idle_timeout, 90 ms)`, from when a session first became idle with
    /// no waiter to take it until the pool drains, retiring the sessions idle for
    /// longer than `idle_timeout`, so a session closes up to one period after that.
    /// An acquisition retires expired sessions only in reuse order until it finds
    /// one that is not, and leaves the others to the sweep.
    Interval,
}

/// The idle session that a pool acquisition reuses (`DIV-032`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SessionReuseOrder {
    /// The frozen order: the session given an exchange least recently.
    #[default]
    LeastRecentlyUsed,
    /// The session that most recently lost its last active or reserved exchange, as
    /// hyper-util's legacy pool (reqwest) reuses the connection returned last, so that
    /// sessions beyond what the traffic needs stay idle and reach `idle_timeout`.
    MostRecentlyUsed,
}

impl Default for PoolParams {
    fn default() -> Self {
        Self {
            max_connections: 100,
            max_connection_attempts: 3,
            enable_tls_session_caching: true,
            connect_timeout: Duration::from_secs(2),
            max_age: Duration::from_secs(10 * 60),
            max_waiters: u16::MAX,
            idle_timeout: Duration::ZERO,
            reuse_order: SessionReuseOrder::LeastRecentlyUsed,
            idle_sweep: IdleSweep::AtExpiry,
        }
    }
}

/// Scheduler settings for an address-free pool. Native TLS settings are absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttachedPoolParams {
    /// Bounds pending factories plus established sessions. Retiring sessions,
    /// which a drain, an expiry, or a flush removed from selection and which
    /// have not closed, count too unless `max_retiring` is set.
    /// Zero disables reuse and retains one connection slot.
    pub max_connections: u32,
    /// Maximum factory attempts per connection task; zero normalizes to one.
    pub max_connection_attempts: u8,
    /// Scheduler wait timeout, also supplied to each factory invocation.
    /// Zero disables the additional acquisition and factory-attempt deadlines
    /// for attached pools only; native [`PoolParams`] semantics are unchanged.
    /// A factory's underlying connector may retain its own finite timeout.
    /// Cancellation, waiter limits, and ownership bounds remain enforced.
    pub connect_timeout: Duration,
    /// Native jittered session age limit; zero disables expiry. With
    /// `max_retiring` set, an aged session that is the last selectable one
    /// keeps serving while the dial gate would refuse its replacement.
    pub max_age: Duration,
    /// Native waiter limit, applied when established sessions alone refuse a
    /// dial: they reach `max_connections`, retiring sessions included unless
    /// `max_retiring` is set, or with it set, established and retiring sessions
    /// together reach `max_connections + max_retiring`. Pending connects retain
    /// the native exception to this limit.
    pub max_waiters: u16,
    /// `None` counts retiring sessions against `max_connections`. `Some(n)`
    /// frees a session's `max_connections` slot once it starts retiring, as
    /// `HTTPCoroSessionPool` does, and permits a dial only while all owned
    /// sessions, retiring included, plus pending connects stay below
    /// `max_connections + n` (`DIV-025`).
    pub max_retiring: Option<u32>,
    /// Waiters one pending connect absorbs, counting the acquisition that
    /// started it, before another acquisition starts a connect within the
    /// connection bounds; zero normalizes to one, the native scheduler. Above
    /// one, a connect absorbs no more waiters than the session of the last
    /// completed connect admitted streams. The connect's session wakes every
    /// waiter still credited to it, so those it cannot serve dial or wait
    /// within the bounds again, and a woken waiter that a pending connect
    /// absorbs passes its wake to the first waiter no pending connect covers.
    /// A failed connect fails waiters as `addNewConnection` does; above one,
    /// releasing its slot wakes no waiter (`DIV-025`).
    pub pending_connect_credit: u32,
}

impl Default for AttachedPoolParams {
    fn default() -> Self {
        let native = PoolParams::default();
        Self {
            max_connections: native.max_connections,
            max_connection_attempts: native.max_connection_attempts,
            connect_timeout: native.connect_timeout,
            max_age: native.max_age,
            max_waiters: native.max_waiters,
            max_retiring: None,
            pending_connect_credit: 1,
        }
    }
}

/// Creates one unbound session on the supplied worker without a native address,
/// resolver, or TLS fallback. The factory owns its destination and configuration.
pub trait AttachedSessionFactory: 'static {
    /// Runs locally. Cancellation is pool-owned, not tied to an individual waiter.
    /// The factory enforces a nonzero supplied attempt timeout. Zero requests no
    /// additional attempt deadline, without replacing the underlying connector's
    /// configured finite or disabled timeout. Waiter expiry alone does not
    /// cancel the factory or discard its eventual handoff.
    /// On cancellation the future must finish resource disposal and join any work
    /// it owns before returning, or return the session for the pool to drain/join.
    /// Hidden detached app-runtime work is not permitted. An application runtime
    /// used by the factory must keep progressing through runtime quiescence.
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, HttpError>>;
}

#[derive(Clone)]
struct AttachedConnectCause(Option<Arc<dyn StdError + Send + Sync>>);

impl AttachedConnectCause {
    fn into_inner(mut self) -> Option<Arc<dyn StdError + Send + Sync>> {
        self.0.take()
    }
}

impl Drop for AttachedConnectCause {
    fn drop(&mut self) {
        drop_caught(self.0.take());
    }
}

/// A native connection failure paired with the original application error.
///
/// Clones share the external error, without requiring that error to be cloneable.
/// The source chain exposes the original object; native metadata is available
/// through [`Self::http_error`]. Diagnostics format only the native error and do
/// not invoke application formatting. Owned cause disposal contains destructor
/// panics so it cannot interrupt pool cleanup. Cloning or extracting the raw Arc
/// transfers responsibility for that reference's diagnostics and disposal to its
/// caller. No application task or pool ownership is added by this value.
#[derive(Clone)]
pub struct AttachedConnectFailure {
    http_error: HttpError,
    cause: AttachedConnectCause,
}

impl AttachedConnectFailure {
    pub fn new(http_error: HttpError) -> Self {
        Self {
            http_error,
            cause: AttachedConnectCause(None),
        }
    }

    pub fn with_cause(http_error: HttpError, cause: Arc<dyn StdError + Send + Sync>) -> Self {
        Self {
            http_error,
            cause: AttachedConnectCause(Some(cause)),
        }
    }

    pub const fn http_error(&self) -> &HttpError {
        &self.http_error
    }

    pub fn external_cause(&self) -> Option<&Arc<dyn StdError + Send + Sync>> {
        self.cause.0.as_ref()
    }

    pub fn into_parts(self) -> (HttpError, Option<Arc<dyn StdError + Send + Sync>>) {
        (self.http_error, self.cause.into_inner())
    }
}

impl From<HttpError> for AttachedConnectFailure {
    fn from(http_error: HttpError) -> Self {
        Self::new(http_error)
    }
}

impl fmt::Display for AttachedConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.http_error, formatter)
    }
}

impl fmt::Debug for AttachedConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachedConnectFailure")
            .field("http_error", &self.http_error)
            .field("has_external_cause", &self.cause.0.is_some())
            .finish()
    }
}

impl StdError for AttachedConnectFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.cause.0 {
            Some(cause) => Some(cause.as_ref()),
            None => Some(&self.http_error),
        }
    }
}

/// An attached factory whose failures retain their original external causes.
///
/// Execution, timeout, cancellation, and resource-join obligations are identical
/// to [`AttachedSessionFactory`]. The future remains worker-local. A failure is
/// associated with its actual connection-task result, not with a particular
/// waiter: native retry selection and fanout are unchanged.
pub trait AttachedSessionFactoryWithCause: 'static {
    /// Enforces a nonzero attempt timeout; zero adds no deadline over the
    /// underlying connector's configured timeout. Original connector causes and
    /// the cancellation/join obligations of [`AttachedSessionFactory`] remain.
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>;
}

struct NativeConnectFuture {
    future: LocalClientFuture<Result<ClientSession, HttpError>>,
}

impl Future for NativeConnectFuture {
    type Output = Result<ClientSession, AttachedConnectFailure>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future
            .as_mut()
            .poll(context)
            .map(|result| result.map_err(AttachedConnectFailure::from))
    }
}

struct LegacyAttachedSessionFactory(Rc<dyn AttachedSessionFactory>);

impl AttachedSessionFactoryWithCause for LegacyAttachedSessionFactory {
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>> {
        Box::pin(NativeConnectFuture {
            future: self.0.create(context, timeout, cancellation),
        })
    }
}

enum PoolBackend {
    Native {
        address: SocketAddress,
        connection: ConnectionParams,
        session: SessionParams,
        connector: Rc<dyn SessionConnector>,
    },
    Attached {
        security: TransportSecurity,
        factory: Option<Rc<dyn AttachedSessionFactoryWithCause>>,
        max_retiring: Option<u32>,
        pending_connect_credit: u32,
    },
    /// Native dials of owned client sessions (`DIV-046`).
    Owned {
        address: SocketAddress,
        connection: ConnectionParams,
        session: SessionParams,
    },
}

impl PoolBackend {
    fn is_attached(&self) -> bool {
        matches!(self, Self::Attached { .. })
    }

    fn is_secure(&self) -> bool {
        match self {
            Self::Native { connection, .. } | Self::Owned { connection, .. } => matches!(
                &connection.transport,
                crate::connector::ClientTransportOptions::Tls(_)
            ),
            Self::Attached { security, .. } => *security == TransportSecurity::Tls,
        }
    }
}

pub trait PoolObserver: 'static {
    fn on_pending_connections_changed(&self, _delta: i32) {}
}

pub trait IdleSessionObserver: 'static {
    fn on_idle_sessions_changed(&self, pool: &HttpSessionPool);
}

pub struct GetSessionResult {
    pub reservation: RequestReservation,
    pub session: ClientSession,
}

/// What an acquisition returns: one reservation on the session it selected.
pub(crate) trait PoolReservation: Sized + 'static {
    /// Reserves `session`, which `state`'s pool selected.
    fn reserve(state: &Rc<RefCell<PoolState>>, session: PooledSession) -> Result<Self, PoolError>;
}

impl PoolReservation for GetSessionResult {
    fn reserve(_state: &Rc<RefCell<PoolState>>, session: PooledSession) -> Result<Self, PoolError> {
        let PooledSession::Uniplex(session) = session else {
            return Err(PoolError::InvalidSessionState);
        };
        match session.reserve_request() {
            Ok(reservation) => Ok(Self {
                reservation,
                session,
            }),
            Err(source) => Err(PoolError::ReservationFailed { source }),
        }
    }
}

pub struct DetachedClientSession(ClientSession);

impl DetachedClientSession {
    pub const fn session(&self) -> &ClientSession {
        &self.0
    }
}

pub trait HttpSessionFactory: 'static {
    fn get_session_with_reservation(
        &self,
        host: Box<str>,
        port: u16,
        secure: bool,
        connect_timeout: Duration,
        server_address: Option<Box<str>>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>>;

    fn requires_absolute_urls(&self) -> bool {
        false
    }
}

pub struct SimpleHttpSessionFactory {
    context: WorkerContext,
}

impl SimpleHttpSessionFactory {
    pub fn new(context: WorkerContext) -> Self {
        Self { context }
    }
}

impl HttpSessionFactory for SimpleHttpSessionFactory {
    fn get_session_with_reservation(
        &self,
        host: Box<str>,
        port: u16,
        secure: bool,
        connect_timeout: Duration,
        server_address: Option<Box<str>>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        let context = self.context.clone();
        Box::pin(async move {
            let session = crate::HttpClient::get_session(
                &context,
                host,
                port,
                secure,
                connect_timeout,
                Duration::from_secs(5),
                server_address,
                None,
                None,
                cancellation,
            )
            .await
            .map_err(|source| PoolError::ConnectFailed { source })?;
            let reservation = session
                .reserve_request()
                .map_err(|source| PoolError::ReservationFailed { source })?;
            Ok(GetSessionResult {
                reservation,
                session,
            })
        })
    }
}

/// A pooled session. A pool's backend creates one kind: native and attached
/// pools hold uniplex `ClientSession`s, owned pools owned client sessions
/// (`DIV-046`).
#[derive(Clone)]
pub(crate) enum PooledSession {
    Uniplex(ClientSession),
    Owned(OwnedClientSession),
}

impl PooledSession {
    fn is_finished(&self) -> bool {
        match self {
            Self::Uniplex(session) => session.is_finished(),
            Self::Owned(session) => session.is_finished(),
        }
    }

    fn is_draining(&self) -> bool {
        match self {
            Self::Uniplex(session) => session.is_draining(),
            Self::Owned(session) => session.is_draining(),
        }
    }

    fn is_idle(&self) -> bool {
        match self {
            Self::Uniplex(session) => session.is_idle(),
            Self::Owned(session) => session.is_pooled_idle(),
        }
    }

    /// For an owned session, without its worker's admission, which an owned
    /// pool's acquisition step checks once for all of its sessions.
    fn supports_more_transactions(&self) -> bool {
        match self {
            Self::Uniplex(session) => session.supports_more_transactions(),
            Self::Owned(session) => session.selectable_capacity() != 0,
        }
    }

    /// An owned HTTP/1.1 session whose slot is returning (`DIV-029`).
    fn is_returning(&self) -> bool {
        match self {
            Self::Uniplex(_) => false,
            Self::Owned(session) => session.is_returning(),
        }
    }

    fn available_transactions(&self) -> usize {
        match self {
            Self::Uniplex(session) => session.available_transactions(),
            Self::Owned(session) => {
                usize::try_from(session.available_capacity()).unwrap_or(usize::MAX)
            }
        }
    }

    fn admitted_transactions(&self) -> u32 {
        match self {
            Self::Uniplex(session) => session.admitted_transactions(),
            Self::Owned(session) => session.admitted_transactions(),
        }
    }

    /// An owned session an acquisition may wait for instead of dialing, since
    /// its driver returns a slot at its next turn (`DIV-046`); never a uniplex
    /// session.
    fn claimable(&self) -> bool {
        match self {
            Self::Uniplex(_) => false,
            Self::Owned(session) => session.claimable(),
        }
    }

    fn claim(&self, waiter: u64) {
        if let Self::Owned(session) = self {
            session.claim(waiter);
        }
    }

    /// The waiter whose acquisition waits for the session's returning slot.
    fn claimant(&self) -> Option<u64> {
        match self {
            Self::Uniplex(_) => None,
            Self::Owned(session) => session.claimant(),
        }
    }

    fn release_claim(&self, waiter: u64) {
        if let Self::Owned(session) = self {
            session.release_claim(waiter);
        }
    }

    fn active_transactions(&self) -> u32 {
        match self {
            Self::Uniplex(session) => session.active_transactions(),
            Self::Owned(session) => session.admitted_transactions(),
        }
    }

    /// The exchanges in flight that least-loaded selection compares (`DIV-048`): an owned
    /// session's `in_flight`; a uniplex session's reserved and active transactions.
    fn load(&self) -> u32 {
        match self {
            Self::Uniplex(session) => session.admitted_transactions(),
            Self::Owned(session) => session.in_flight(),
        }
    }

    fn close_admission(&self) {
        match self {
            Self::Uniplex(session) => session.close_admission(),
            Self::Owned(session) => session.close_admission(),
        }
    }

    fn initiate_drain(&self) -> Result<bool, HttpError> {
        match self {
            Self::Uniplex(session) => session.initiate_drain(),
            Self::Owned(session) => session.initiate_drain(),
        }
    }

    fn is_same_session(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Uniplex(session), Self::Uniplex(other)) => session.is_same_session(other),
            (Self::Owned(session), Self::Owned(other)) => session.is_same_session(other),
            _ => false,
        }
    }

    /// The slot of the session's entry in its pool's list, which an owned session records
    /// (`None` while it has no entry); `None` for a uniplex session, which records none, so
    /// that its pool searches for its entry.
    fn recorded_slot(&self) -> Option<Option<SessionSlot>> {
        match self {
            Self::Uniplex(_) => None,
            Self::Owned(session) => Some(session.pool_slot()),
        }
    }

    fn set_pool_slot(&self, slot: Option<SessionSlot>) {
        if let Self::Owned(session) = self {
            session.set_pool_slot(slot);
        }
    }

    #[cfg(test)]
    fn is_client_session(&self, other: &ClientSession) -> bool {
        matches!(self, Self::Uniplex(session) if session.is_same_session(other))
    }

    fn age(&self) -> Duration {
        match self {
            Self::Uniplex(session) => session.age(),
            Self::Owned(session) => session.age(),
        }
    }

    /// Binds the session to `pool`. An owned session also takes the flag that tells the
    /// pool's index it stopped admitting without a release (S6.9).
    fn bind_pool(&self, pool: Weak<RefCell<PoolState>>, unsettled: Option<Rc<Cell<bool>>>) {
        match self {
            Self::Uniplex(session) => session.bind_pool(pool),
            Self::Owned(session) => session.bind_pool(pool, unsettled),
        }
    }

    /// When an owned session started, from which its age counts; `None` for a uniplex
    /// session, whose pool keeps no index.
    fn started(&self) -> Option<Instant> {
        match self {
            Self::Uniplex(_) => None,
            Self::Owned(session) => Some(session.started()),
        }
    }

    fn is_bound_to_pool(&self) -> bool {
        match self {
            Self::Uniplex(session) => session.is_bound_to_pool(),
            Self::Owned(session) => session.is_bound_to_pool(),
        }
    }

    fn worker_id(&self) -> WorkerId {
        match self {
            Self::Uniplex(session) => session.worker_id(),
            Self::Owned(session) => session.worker_id(),
        }
    }

    fn connection_context(&self) -> &Arc<ConnectionContext> {
        match self {
            Self::Uniplex(session) => session.connection_context(),
            Self::Owned(session) => session.connection_context(),
        }
    }

    fn wait_closed(&self) -> LocalClientFuture<Result<UniplexRunSummary, HttpError>> {
        match self {
            Self::Uniplex(session) => session.wait_closed(),
            Self::Owned(session) => session.wait_closed(),
        }
    }

    /// The uniplex session of a native or attached pool's entry.
    fn into_client_session(self) -> ClientSession {
        match self {
            Self::Uniplex(session) => session,
            Self::Owned(_) => unreachable!("native and attached pools hold uniplex sessions"),
        }
    }
}

struct SessionEntry {
    session: PooledSession,
    max_age: Duration,
    ownership: Option<Rc<SessionOwnership>>,
    /// When the session last lost its last active or reserved exchange, while it has none.
    idle_since: Option<Instant>,
    /// An indexed pool's: whether the session counted in `idle_sessions` when the pool last
    /// counted it (S6.9).
    counted_idle: bool,
}

impl Linked for SessionEntry {
    fn set_slot(&self, slot: Option<SessionSlot>) {
        self.session.set_pool_slot(slot);
    }
}

impl SessionEntry {
    fn new(
        session: PooledSession,
        max_age: Duration,
        ownership: Option<Rc<SessionOwnership>>,
    ) -> Self {
        Self {
            session,
            max_age,
            ownership,
            idle_since: None,
            counted_idle: false,
        }
    }

    /// Stamps the session idle once it has no active or reserved exchange, at the pass's
    /// `now`, which the first stamp reads, and clears the stamp while it has one.
    fn stamp_idle(&mut self, now: &mut Option<Instant>) {
        self.idle_since = if self.session.admitted_transactions() == 0 {
            self.idle_since
                .or_else(|| Some(*now.get_or_insert_with(Instant::now)))
        } else {
            None
        };
    }

    /// When an owned session reaches its maximum age; `None` when it never does.
    fn max_age_deadline(&self) -> Option<Instant> {
        if self.max_age.is_zero() {
            return None;
        }
        self.session.started()?.checked_add(self.max_age)
    }

    /// When the session became idle, while it has no active or reserved exchange.
    /// The session's own count decides, as a callback run by a new reservation
    /// can reach the pool before the reservation's `TransactionAttached` does.
    fn idle_since(&self) -> Option<Instant> {
        self.idle_since
            .filter(|_| self.session.admitted_transactions() == 0)
    }

    /// Whether the session has been idle for `timeout`: at least that long at
    /// expiry, longer on an interval, as hyper-util's checkout and sweep compare.
    fn idle_expired(&self, now: Instant, timeout: Duration, sweep: IdleSweep) -> bool {
        !timeout.is_zero()
            && self.idle_since().is_some_and(|since| {
                let idle = now.saturating_duration_since(since);
                match sweep {
                    IdleSweep::AtExpiry => idle >= timeout,
                    IdleSweep::Interval => idle > timeout,
                }
            })
    }
}

/// Waiting acquisitions credited to a pending connect, including the one that
/// started it.
struct ConnectCredit {
    waiters: Cell<u32>,
}

struct Waiter {
    id: u64,
    baton: Rc<CancellableBaton>,
    error: RefCell<Option<PoolError>>,
    credit: Option<Rc<ConnectCredit>>,
}

impl Waiter {
    fn signal(&self) {
        self.baton.signal();
    }

    fn cancel(&self, error: PoolError) {
        self.error.replace(Some(error));
        self.baton.signal();
    }
}

struct ReloadConfig {
    callback: Rc<dyn Fn(&HttpSessionPool)>,
    generation: u64,
    stop: Rc<CancellableBaton>,
}

struct PoolTasks {
    group: RefCell<Option<TaskGroup>>,
    shutdown: Rc<CancellableBaton>,
    changed: Notify,
    failure: RefCell<Option<PoolError>>,
    completion: watch::Sender<Option<Result<(), PoolError>>>,
    /// Tasks `spawn_pool_task` spawned.
    #[cfg(test)]
    spawned: Cell<u64>,
}

impl PoolTasks {
    fn fail(&self, error: PoolError) {
        self.failure.borrow_mut().get_or_insert(error);
    }
}

struct TaskCompletionNotice {
    tasks: Weak<PoolTasks>,
}

impl Drop for TaskCompletionNotice {
    fn drop(&mut self) {
        let _ = catch_callback(|| {
            if let Some(tasks) = self.tasks.upgrade() {
                tasks.changed.notify_one();
            }
        });
    }
}

fn spawn_pool_task<F, Fut>(tasks: &Rc<PoolTasks>, factory: F) -> Result<(), TaskGroupError>
where
    F: FnOnce(CancellationToken) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let notice = TaskCompletionNotice {
        tasks: Rc::downgrade(tasks),
    };
    tasks
        .group
        .borrow_mut()
        .as_mut()
        .ok_or(TaskGroupError::Closed)?
        .spawn_local(move |cancellation| async move {
            let _notice = notice;
            factory(cancellation).await;
        })?;
    #[cfg(test)]
    tasks.spawned.set(tasks.spawned.get() + 1);
    Ok(())
}

fn catch_callback<Output>(callback: impl FnOnce() -> Output) -> Result<Output, ()> {
    match catch_unwind(AssertUnwindSafe(callback)) {
        Ok(output) => Ok(output),
        Err(payload) => {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                roxygen_runtime::panic::discard_panic(secondary);
            }
            Err(())
        }
    }
}

fn drop_caught<T>(value: T) {
    let _ = catch_callback(|| drop(value));
}

struct PoolCleanupRegistration(Option<WorkerCleanupRegistration>);

impl Drop for PoolCleanupRegistration {
    fn drop(&mut self) {
        if let Some(registration) = self.0.take() {
            let _ = registration.disarm();
        }
    }
}

fn request_pool_shutdown(state: &Weak<RefCell<PoolState>>, tasks: &PoolTasks) {
    if let Some(state) = state.upgrade()
        && catch_callback(|| {
            HttpSessionPool {
                state,
                drain_on_drop: false,
            }
            .drain()
        })
        .is_err()
    {
        tasks.fail(PoolError::CleanupPanicked);
    }
    let _ = catch_callback(|| tasks.shutdown.signal());
}

async fn supervise_pool(
    state: Weak<RefCell<PoolState>>,
    tasks: Rc<PoolTasks>,
    context: WorkerContext,
    registration: PoolCleanupRegistration,
) {
    let _registration = registration;
    let shutdown = tasks.shutdown.wait(CancellationToken::never());
    let quiescing = context.quiescing();
    tokio::pin!(shutdown, quiescing);
    loop {
        let changed = tasks.changed.notified();
        if let Some(group) = tasks.group.borrow_mut().as_mut() {
            group.reap_finished();
        }
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            () = &mut quiescing => {
                request_pool_shutdown(&state, &tasks);
                break;
            }
            () = changed => tokio::task::yield_now().await,
        }
    }
    let mut group = tasks
        .group
        .borrow_mut()
        .take()
        .expect("registered supervisor owns the pool group");
    if catch_callback(|| group.cancel()).is_err() {
        tasks.fail(PoolError::CleanupPanicked);
    }
    if let Err(source) = group.join().await {
        tasks.fail(PoolError::TaskJoin { source });
    }
    tasks
        .completion
        .send_replace(Some(tasks.failure.borrow().clone().map_or(Ok(()), Err)));
}

/// The shortest period of `IdleSweep::Interval`, as hyper-util's `MIN_CHECK`.
const MIN_IDLE_SWEEP_PERIOD: Duration = Duration::from_millis(90);

/// Retires sessions that keep no active or reserved exchange for the pool's
/// `idle_timeout` (`DIV-032`): with `IdleSweep::AtExpiry` when the longest-idle
/// session expires, with `IdleSweep::Interval` every `max(idle_timeout, 90 ms)`
/// from when a session first became idle with no waiter to take it, as hyper-util
/// 0.1.20's `IdleTask`.
///
/// At expiry, a session that becomes idle expires after every session already
/// idle, so the task is signalled only while it waits without a deadline because
/// no session was idle; otherwise it wakes only at its deadline. Every pass scans
/// all pooled sessions. A pass retires every session expired by then, or finds that
/// the session it waited for was reused and recomputes from the longest-idle
/// session left; idle periods that end at k distinct times cost up to k retiring
/// passes, each followed by a recompute, even within one `idle_timeout`. Apart from
/// those, steady traffic, which reuses sessions idle only briefly, costs about one
/// or two passes per `idle_timeout` rather than one per request. On an interval,
/// the task is signalled once, to start, and then sweeps once per period until the
/// pool drains, whether or not any session is idle.
async fn retire_idle_sessions(
    state: Weak<RefCell<PoolState>>,
    wake: Rc<CancellableBaton>,
    cancellation: CancellationToken,
) {
    // `IdleSweep::Interval`: when the next sweep runs, once started.
    let mut sweep_at: Option<Option<Instant>> = None;
    loop {
        let (deadline, now, sweep, period) = {
            let Some(pool) = state.upgrade() else {
                return;
            };
            let mut pool = pool.borrow_mut();
            #[cfg(test)]
            pool.idle_retirement_passes
                .set(pool.idle_retirement_passes.get() + 1);
            let now = Instant::now();
            let timeout = pool.pool_params.idle_timeout;
            let sweep = pool.pool_params.idle_sweep;
            let period = timeout.max(MIN_IDLE_SWEEP_PERIOD);
            // A deadline past `Instant`'s range never comes: wait unparked, unsignalled.
            let deadline = match sweep {
                IdleSweep::AtExpiry => {
                    let longest_idle = pool.longest_idle_since();
                    pool.idle_retirement_parked = longest_idle.is_none();
                    longest_idle.and_then(|since| since.checked_add(timeout))
                }
                IdleSweep::Interval => {
                    if sweep_at.is_none() {
                        sweep_at = pool.idle_sweep_from.map(|from| from.checked_add(period));
                    }
                    sweep_at.flatten()
                }
            };
            (deadline, now, sweep, period)
        };
        if let Some(deadline) = deadline
            && deadline <= now
        {
            let Some(pool) = state.upgrade() else {
                return;
            };
            let expired = pool.borrow_mut().take_idle_expired(now);
            #[cfg(test)]
            if !expired.is_empty() {
                let passes = &pool.borrow().idle_retirement_retiring_passes;
                passes.set(passes.get() + 1);
            }
            drop(pool);
            match sweep {
                IdleSweep::AtExpiry if expired.is_empty() => {
                    // Unreachable while expiry and retirement share their predicate.
                    tokio::task::yield_now().await;
                }
                IdleSweep::AtExpiry => {}
                // As hyper-util, from when the sweep ends.
                IdleSweep::Interval => sweep_at = Some(Instant::now().checked_add(period)),
            }
            drop(expired);
            continue;
        }
        // A zero baton timeout waits without a deadline.
        let remaining = deadline.map_or(Duration::ZERO, |deadline| deadline - now);
        let status = wake.timed_wait(remaining, cancellation.clone()).await;
        match status {
            WaitStatus::Signalled | WaitStatus::TimedOut => wake.reset(),
            WaitStatus::Cancelled | WaitStatus::NotReady => return,
        }
    }
}

/// An owned HTTP/1.1 pool's selection index (S6.9, performance only): its sessions that
/// can take an exchange, linked in the pool's order into a busy and an idle class list
/// (`SessionList`'s classes), with lower bounds of when a member's maximum age or any idle
/// timeout can end. While it is settled, an acquisition step selects the first busy session
/// that can take an exchange, else the idle session at the back (most recently used) or
/// front (least recently used) of the idle list, and a release the pool hears for one
/// session updates that session alone, each without a pass over the pool's sessions,
/// with the result of those passes. Any other step or event takes the pool's passes and
/// unsettles the index, and the next release's passes rebuild and settle it.
///
/// The index is exact while every change to a session that the passes test reaches the pool
/// as an event for that session: an owned HTTP/1.1 session becomes able to take an exchange
/// again only when its driver returns its slot (`capacity_available`) or its slot turns
/// returning (`DIV-029`), which its returning hook reports (`record_returning`), becomes idle
/// only in that callback or in the idleness its run reports, stops being idle only by a
/// reservation, and stops admitting by `admission_closed`, by the pool closing it, or by a
/// cancellation, which sets `unsettled`. A session that stops taking exchanges without an
/// event (its worker cancelled) stays in its list until a step meets it and drops it.
struct SessionIndex {
    /// Set while the index may not mirror the pool; `None` when the pool keeps no index.
    /// Shared with the pool's owned sessions, whose cancellation sets it.
    unsettled: Option<Rc<Cell<bool>>>,
    /// No session's idle stamp is earlier.
    idle_floor: Option<Instant>,
    /// No session in a class list reaches its maximum age earlier; `None` while none can. The
    /// passes retire a session for its age only while it can take an exchange, so a session
    /// that ages while it cannot leaves the floor until it joins a list again.
    age_floor: Option<Instant>,
}

impl SessionIndex {
    /// An empty pool's index, which mirrors its empty pool.
    fn new(enabled: bool) -> Self {
        Self {
            unsettled: enabled.then(|| Rc::new(Cell::new(false))),
            idle_floor: None,
            age_floor: None,
        }
    }
}

/// An index list whose member's class changed without an event the pool heard.
struct IndexMismatch;

pub(crate) struct PoolState {
    context: WorkerContext,
    pool_params: PoolParams,
    backend: PoolBackend,
    observer: Option<Rc<dyn PoolObserver>>,
    idle_observer: Option<Rc<dyn IdleSessionObserver>>,
    sessions: SessionList<SessionEntry>,
    /// An owned pool's selection among its established sessions (`DIV-048`).
    selection: OwnedSessionSelection,
    index: SessionIndex,
    idle_sessions: usize,
    waiters: VecDeque<Rc<Waiter>>,
    connects_in_progress: u32,
    connect_credits: Vec<Rc<ConnectCredit>>,
    /// Streams the last credited session admitted when it attached.
    observed_capacity: Option<u32>,
    owned_connections: u64,
    tasks: Rc<PoolTasks>,
    /// Wakes the idle-retirement task when a session becomes idle while it waits
    /// without a deadline (`idle_retirement_parked`), or, on an interval, to start.
    idle_wake: Rc<CancellableBaton>,
    idle_retirement_parked: bool,
    /// `IdleSweep::Interval`: when a session first became idle with no waiter to
    /// take it, as hyper-util's first `put` onto its idle list spawns its sweep.
    idle_sweep_from: Option<Instant>,
    #[cfg(test)]
    idle_retirement_passes: Cell<u64>,
    #[cfg(test)]
    idle_retirement_retiring_passes: Cell<u64>,
    /// `notify_session_event` passes over the pool's sessions.
    #[cfg(test)]
    session_event_passes: Cell<u64>,
    /// Connection attempts the pool started.
    #[cfg(test)]
    connection_attempts: Cell<u64>,
    /// Passes over the pool's sessions on an acquisition, reservation or release path.
    #[cfg(test)]
    pool_scans: Cell<u64>,
    /// Searches for a session that an acquisition may wait for instead of dialing.
    #[cfg(test)]
    claim_scans: Cell<u64>,
    /// Steps and releases take the pool's passes, as without an index.
    #[cfg(test)]
    force_scans: Cell<bool>,
    next_waiter_id: u64,
    pooling_enabled: bool,
    draining: bool,
    reload: Option<ReloadConfig>,
    reload_generation: u64,
}

impl Drop for PoolState {
    fn drop(&mut self) {
        if let Some(reload) = self.reload.take() {
            let _ = catch_callback(|| reload.stop.signal());
        }
        if catch_callback(|| {
            if let Some(group) = self.tasks.group.borrow_mut().as_mut() {
                group.cancel();
            }
        })
        .is_err()
        {
            self.tasks.fail(PoolError::CleanupPanicked);
        }
        let _ = catch_callback(|| self.tasks.shutdown.signal());
    }
}

impl PoolState {
    /// Counts a pass over the pool's sessions on an acquisition, reservation or release path.
    fn count_scan(&self) {
        #[cfg(test)]
        self.pool_scans.set(self.pool_scans.get() + 1);
    }

    /// The slot of `session`'s entry: an owned session's is the slot it recorded, found
    /// without a search; a uniplex session's is found by a pass over the pool's sessions.
    fn slot_of(&self, session: &PooledSession) -> Option<SessionSlot> {
        let Some(recorded) = session.recorded_slot() else {
            self.count_scan();
            return self
                .sessions
                .find(|entry| entry.session.is_same_session(session));
        };
        let slot = recorded?;
        let own =
            self.sessions.holds(slot) && self.sessions.get(slot).session.is_same_session(session);
        debug_assert!(own, "an owned session records its own entry's slot");
        own.then_some(slot)
    }

    /// Whether the index mirrors the pool (S6.9).
    fn index_settled(&self) -> bool {
        self.index
            .unsettled
            .as_ref()
            .is_some_and(|unsettled| !unsettled.get())
            && !self.scans_forced()
    }

    #[cfg(test)]
    fn scans_forced(&self) -> bool {
        self.force_scans.get()
    }

    #[cfg(not(test))]
    fn scans_forced(&self) -> bool {
        false
    }

    /// Leaves the index to the pool's passes until a release's passes settle it again.
    fn unsettle_index(&self) {
        if let Some(unsettled) = &self.index.unsettled {
            unsettled.set(true);
        }
    }

    /// An indexed session's class: busy or idle while it can take an exchange.
    fn class_of(entry: &SessionEntry) -> Option<Class> {
        entry.session.supports_more_transactions().then(|| {
            if entry.session.is_idle() {
                Class::Idle
            } else {
                Class::Busy
            }
        })
    }

    /// Whether an acquisition may take a returning session: only while none waits and the pool
    /// reuses sessions; otherwise its retirement wakes the first waiter, or drains it
    /// (`DIV-046`).
    fn selects_returning(&self) -> bool {
        self.pooling_enabled && self.waiters.is_empty()
    }

    /// A busy session that an acquisition may take: it can take another exchange, and it is not
    /// returning unless `returning` (`selects_returning`).
    fn takes_busy(entry: &SessionEntry, returning: bool) -> bool {
        !entry.session.is_idle()
            && entry.session.supports_more_transactions()
            && (returning || !entry.session.is_returning())
    }

    /// Rebuilds the index after a release's passes, which left every stamp and the idle count
    /// as they compute them (S6.9): each session that can take an exchange joins its class
    /// list in the pool's order, each entry records whether it counted idle, and the floors are
    /// recomputed, the age floor over the class lists' members. The index settles only if what
    /// selection through it relies on holds: no session is finished or draining, a stamped pool
    /// stamps exactly its idle sessions, and, most recently used, the idle list is in stamp
    /// order.
    fn settle_index(&mut self) {
        let Some(unsettled) = self.index.unsettled.clone() else {
            return;
        };
        self.count_scan();
        self.sessions.clear_classes();
        let stamps_idle = self.stamps_idle();
        let recent_first = self.pool_params.reuse_order == SessionReuseOrder::MostRecentlyUsed;
        let mut settled = true;
        let mut idle_floor: Option<Instant> = None;
        let mut age_floor: Option<Instant> = None;
        let mut last_idle: Option<Instant> = None;
        let mut cursor = self.sessions.front();
        while let Some(slot) = cursor {
            cursor = self.sessions.next(slot);
            let entry = self.sessions.get_mut(slot);
            entry.counted_idle = entry.session.is_idle();
            let class = Self::class_of(entry);
            settled &= !entry.session.is_finished() && !entry.session.is_draining();
            settled &= !stamps_idle
                || (entry.session.admitted_transactions() == 0) == entry.idle_since.is_some();
            if let Some(since) = entry.idle_since {
                idle_floor = Some(idle_floor.map_or(since, |floor| floor.min(since)));
            }
            if class.is_some()
                && let Some(deadline) = entry.max_age_deadline()
            {
                age_floor = Some(age_floor.map_or(deadline, |floor| floor.min(deadline)));
            }
            if recent_first && class == Some(Class::Idle) {
                settled &= entry
                    .idle_since
                    .is_some_and(|since| last_idle.is_none_or(|last| last <= since));
                last_idle = entry.idle_since.or(last_idle);
            }
            self.sessions.set_class(slot, class);
        }
        self.index.idle_floor = idle_floor;
        self.index.age_floor = age_floor;
        unsettled.set(!settled);
    }

    /// Whether an acquisition step may select through the index: it is settled, and no
    /// session that can take an exchange can have reached its maximum age, nor, retired at
    /// expiry, any session its idle timeout, so that the step's own search for them would find
    /// none.
    fn index_ready(&self, now: Option<Instant>) -> bool {
        if !self.index_settled() {
            return false;
        }
        if let Some(floor) = self.index.age_floor
            && now.unwrap_or_else(Instant::now) >= floor
        {
            return false;
        }
        if self.pool_params.idle_sweep == IdleSweep::AtExpiry
            && let (Some(now), Some(floor)) = (now, self.index.idle_floor)
            && now.saturating_duration_since(floor) >= self.pool_params.idle_timeout
        {
            return false;
        }
        true
    }

    /// Selects through the index the session that `next_step`'s passes would select (S6.9):
    /// the first busy session that can take an exchange, else the idle session at the back
    /// (most recently used) or front (least recently used) of the idle list, retiring each
    /// expired one it meets on an interval, as `select_idle_session` does; least loaded
    /// (`DIV-048`), as `indexed_least_loaded_busy_session` says. A member that can no longer
    /// take an exchange leaves its list; one whose class changed without an event falls back to
    /// the passes and unsettles the index.
    fn select_indexed(
        &mut self,
        checkout_at: Option<Instant>,
        retired: &mut RetiredSessions,
    ) -> Option<SessionSlot> {
        let least_loaded = self.selection == OwnedSessionSelection::LeastLoaded;
        let busy = if least_loaded {
            self.indexed_least_loaded_busy_session()
        } else {
            match self.indexed_busy_session() {
                Ok(busy) => Ok(busy.map_or(ControlFlow::Continue(None), ControlFlow::Break)),
                Err(IndexMismatch) => {
                    self.unsettle_index();
                    self.count_scan();
                    let returning = self.selects_returning();
                    return self
                        .sessions
                        .find(|entry| Self::takes_busy(entry, returning))
                        .or_else(|| self.select_idle_session(checkout_at, retired));
                }
            }
        };
        let selected = match busy {
            Ok(ControlFlow::Break(slot)) => Ok(Some(slot)),
            Ok(ControlFlow::Continue(fallback)) => self
                .indexed_idle_session(checkout_at, retired)
                .map(|idle| idle.or(fallback)),
            Err(mismatch) => Err(mismatch),
        };
        let Ok(selected) = selected else {
            self.unsettle_index();
            return if least_loaded {
                self.select_least_loaded(checkout_at, retired)
            } else {
                self.select_idle_session(checkout_at, retired)
            };
        };
        #[cfg(debug_assertions)]
        assert_eq!(
            selected,
            self.reference_selection(),
            "the index selects as the pool's passes do"
        );
        selected
    }

    fn indexed_busy_session(&mut self) -> Result<Option<SessionSlot>, IndexMismatch> {
        let returning = self.selects_returning();
        let mut cursor = self.sessions.class_front(Class::Busy);
        while let Some(slot) = cursor {
            cursor = self.sessions.class_next(slot);
            let entry = self.sessions.get(slot);
            match Self::class_of(entry) {
                Some(Class::Busy) if !returning && entry.session.is_returning() => {}
                Some(Class::Busy) => return Ok(Some(slot)),
                Some(Class::Idle) => return Err(IndexMismatch),
                // It takes an exchange again only once its slot returns, which indexes it.
                None => self.sessions.set_class(slot, None),
            }
        }
        Ok(None)
    }

    /// The busy list's part of `select_least_loaded` through the index (`DIV-048`): `Break`
    /// with the first member with no exchange in flight; else `Continue` with the first member
    /// of the fewest exchanges in flight, if any, which `select_indexed` takes only when the
    /// idle list, walked as `indexed_idle_session` walks it, yields no session. Busy members of
    /// an indexed (HTTP/1.1) pool are sessions not yet reserved or whose driver has not settled
    /// their free slot, so the busy list is short.
    fn indexed_least_loaded_busy_session(
        &mut self,
    ) -> Result<ControlFlow<SessionSlot, Option<SessionSlot>>, IndexMismatch> {
        let returning = self.selects_returning();
        let mut least: Option<(u32, SessionSlot)> = None;
        let mut cursor = self.sessions.class_front(Class::Busy);
        while let Some(slot) = cursor {
            cursor = self.sessions.class_next(slot);
            let entry = self.sessions.get(slot);
            match Self::class_of(entry) {
                Some(Class::Busy) if !returning && entry.session.is_returning() => {}
                Some(Class::Busy) => {
                    let load = entry.session.load();
                    if load == 0 {
                        return Ok(ControlFlow::Break(slot));
                    }
                    if least.is_none_or(|(minimum, _)| load < minimum) {
                        least = Some((load, slot));
                    }
                }
                Some(Class::Idle) => return Err(IndexMismatch),
                // It takes an exchange again only once its slot returns, which indexes it.
                None => self.sessions.set_class(slot, None),
            }
        }
        Ok(ControlFlow::Continue(least.map(|(_, slot)| slot)))
    }

    /// `select_indexed` is its one caller, so that it inlines into `next_step`.
    fn indexed_idle_session(
        &mut self,
        checkout_at: Option<Instant>,
        retired: &mut RetiredSessions,
    ) -> Result<Option<SessionSlot>, IndexMismatch> {
        let timeout = self.pool_params.idle_timeout;
        let sweep = self.pool_params.idle_sweep;
        let recent_first = self.pool_params.reuse_order == SessionReuseOrder::MostRecentlyUsed;
        loop {
            let candidate = if recent_first {
                self.sessions.class_back(Class::Idle)
            } else {
                self.sessions.class_front(Class::Idle)
            };
            let Some(slot) = candidate else {
                return Ok(None);
            };
            let entry = self.sessions.get(slot);
            match Self::class_of(entry) {
                None => {
                    self.sessions.set_class(slot, None);
                    continue;
                }
                Some(Class::Busy) => return Err(IndexMismatch),
                Some(Class::Idle) if recent_first && entry.idle_since().is_none() => {
                    return Err(IndexMismatch);
                }
                Some(Class::Idle) => {}
            }
            if !checkout_at.is_some_and(|now| entry.idle_expired(now, timeout, sweep)) {
                return Ok(Some(slot));
            }
            let entry = self.sessions.remove(slot);
            entry.session.close_admission();
            retired.push(entry.session);
            // Its count and stamp leave with it: a release's passes settle the index again.
            self.unsettle_index();
        }
    }

    /// The selection of `next_step`'s passes, without their retirements or counts.
    #[cfg(debug_assertions)]
    fn reference_selection(&self) -> Option<SessionSlot> {
        if self.selection == OwnedSessionSelection::LeastLoaded {
            return self.least_loaded_entry();
        }
        let returning = self.selects_returning();
        self.sessions
            .find(|entry| Self::takes_busy(entry, returning))
            .or_else(|| match self.pool_params.reuse_order {
                SessionReuseOrder::LeastRecentlyUsed => None,
                SessionReuseOrder::MostRecentlyUsed => self.most_recently_idle_entry(),
            })
            .or_else(|| {
                self.sessions.find(|entry| {
                    entry.session.supports_more_transactions() && !entry.session.is_returning()
                })
            })
    }

    /// Indexes the session at `slot` again after an event the pool heard for it alone (S6.9).
    /// `moved`: the event moved it to the back of the pool's order, where it joins its class
    /// list. A class it takes without that move needs a search for its place: the session
    /// leaves the index unsettled instead. Most recently used, an idle session out of stamp
    /// order with its neighbors does too.
    fn index_session(&mut self, slot: SessionSlot, moved: bool) {
        if self.index.unsettled.is_none() {
            return;
        }
        let class = Self::class_of(self.sessions.get(slot));
        let current = self.sessions.class(slot);
        if class.is_none() || (!moved && class != current) {
            self.sessions.set_class(slot, None);
            if class.is_some() {
                self.unsettle_index();
            }
            return;
        }
        if class != current {
            self.sessions.set_class(slot, class);
            self.lower_age_floor(slot);
        }
        if class == Some(Class::Idle)
            && self.pool_params.reuse_order == SessionReuseOrder::MostRecentlyUsed
            && !self.idle_stamp_in_order(slot)
        {
            self.unsettle_index();
        }
    }

    /// Lowers the age floor to the maximum-age deadline of the session at `slot`, which joined
    /// a class list.
    fn lower_age_floor(&mut self, slot: SessionSlot) {
        if let Some(deadline) = self.sessions.get(slot).max_age_deadline() {
            let floor = self.index.age_floor.get_or_insert(deadline);
            *floor = (*floor).min(deadline);
        }
    }

    /// Whether the idle session at `slot` is stamped no earlier than the idle list member
    /// before it and no later than the one after it.
    fn idle_stamp_in_order(&self, slot: SessionSlot) -> bool {
        let stamp = |slot| self.sessions.get(slot).idle_since;
        let Some(own) = stamp(slot) else {
            return false;
        };
        self.sessions
            .class_prev(slot)
            .and_then(stamp)
            .is_none_or(|prev| prev <= own)
            && self
                .sessions
                .class_next(slot)
                .and_then(stamp)
                .is_none_or(|next| own <= next)
    }

    /// Whether the index can hear a release of `session` alone (S6.9): it is settled, so no
    /// other session is finished, draining or changed since the pool last looked at it; the
    /// session itself still admits and runs; and the release drains nothing.
    fn index_hears(&self, session: &PooledSession, release_idle: bool) -> bool {
        self.index_settled()
            && (self.pooling_enabled || !release_idle)
            && !session.is_finished()
            && !session.is_draining()
    }

    /// The waiter of the acquisition that waits for `session`'s returning slot, which takes
    /// the session's wake whatever it reports (`DIV-046`).
    fn take_claimer(&mut self, session: Option<&PooledSession>) -> Option<Rc<Waiter>> {
        session
            .and_then(PooledSession::claimant)
            .and_then(|id| self.waiters.iter().position(|waiter| waiter.id == id))
            .and_then(|index| self.waiters.remove(index))
    }

    /// `notify_session_event`'s pass for a release the index hears, on `session` alone
    /// (S6.9): its move to the back, its idle count, the claimer and waiters it wakes, its
    /// stamp, then its class. Returns the claimer, the waiters, the idle count and whether
    /// idle retirement wakes.
    fn release_indexed(
        &mut self,
        session: &PooledSession,
        enqueue_back: bool,
        wake_count: usize,
    ) -> (
        Option<Rc<Waiter>>,
        Vec<Rc<Waiter>>,
        usize,
        Option<Rc<CancellableBaton>>,
    ) {
        let slot = self.slot_of(session);
        if enqueue_back && let Some(slot) = slot {
            self.sessions.move_to_back(slot);
        }
        if let Some(slot) = slot {
            let entry = self.sessions.get_mut(slot);
            let idle = entry.session.is_idle();
            let counted = std::mem::replace(&mut entry.counted_idle, idle);
            self.idle_sessions =
                (self.idle_sessions + usize::from(idle)).saturating_sub(usize::from(counted));
        }
        let claimer = self.take_claimer(Some(session));
        let waiters = self.take_waiters(wake_count.saturating_sub(usize::from(claimer.is_some())));
        let idle_wake = self
            .track_idle_session(slot, claimer.is_some() || !waiters.is_empty())
            .then(|| Rc::clone(&self.idle_wake));
        if let Some(slot) = slot {
            self.index_session(slot, enqueue_back);
        }
        #[cfg(debug_assertions)]
        self.verify_index();
        (claimer, waiters, self.idle_sessions, idle_wake)
    }

    /// Checks that a settled index mirrors the pool: every session that can take an exchange
    /// is in its class list, the lists hold only pooled sessions in the pool's order, the
    /// counts, stamps and floors are what the passes would leave, and, most recently used, the
    /// idle list is in stamp order.
    #[cfg(debug_assertions)]
    fn verify_index(&self) {
        if !self.index_settled() {
            return;
        }
        let stamps_idle = self.stamps_idle();
        let recent_first = self.pool_params.reuse_order == SessionReuseOrder::MostRecentlyUsed;
        let mut busy = self.sessions.class_front(Class::Busy);
        let mut idle = self.sessions.class_front(Class::Idle);
        let mut counted = 0;
        let mut last_idle: Option<Instant> = None;
        self.sessions.entries().for_each(|(slot, entry)| {
            let session = &entry.session;
            assert!(
                !session.is_finished() && !session.is_draining(),
                "a settled index's pool holds no finished or draining session"
            );
            assert_eq!(
                entry.counted_idle,
                session.is_idle(),
                "the idle count is current"
            );
            counted += usize::from(entry.counted_idle);
            if stamps_idle {
                assert_eq!(
                    session.admitted_transactions() == 0,
                    entry.idle_since.is_some(),
                    "exactly the idle sessions are stamped"
                );
            }
            if let Some(since) = entry.idle_since {
                assert!(
                    self.index.idle_floor.is_some_and(|floor| floor <= since),
                    "the idle floor bounds every stamp"
                );
            }
            if self.sessions.class(slot).is_some()
                && let Some(deadline) = entry.max_age_deadline()
            {
                assert!(
                    self.index.age_floor.is_some_and(|floor| floor <= deadline),
                    "the age floor bounds every class list member's deadline"
                );
            }
            if let Some(class) = Self::class_of(entry) {
                assert_eq!(
                    self.sessions.class(slot),
                    Some(class),
                    "a session that can take an exchange is in its class list"
                );
            }
            match self.sessions.class(slot) {
                Some(Class::Busy) => {
                    assert_eq!(busy, Some(slot), "the busy list is in the pool's order");
                    busy = self.sessions.class_next(slot);
                }
                Some(Class::Idle) => {
                    assert_eq!(idle, Some(slot), "the idle list is in the pool's order");
                    idle = self.sessions.class_next(slot);
                    if recent_first && let Some(since) = entry.idle_since {
                        assert!(
                            last_idle.is_none_or(|last| last <= since),
                            "the idle list is in stamp order"
                        );
                        last_idle = Some(since);
                    }
                }
                None => {}
            }
        });
        assert_eq!(
            (busy, idle),
            (None, None),
            "the class lists hold only pooled sessions"
        );
        assert_eq!(counted, self.idle_sessions, "the idle count is the pass's");
    }

    fn full(&self) -> bool {
        let maximum = u64::from(self.pool_params.max_connections);
        match self.ownership_bound() {
            None => self.capacity_used() >= maximum,
            Some(bound) => {
                self.active_sessions() + u64::from(self.connects_in_progress) >= maximum
                    || self.owned_connections >= bound
            }
        }
    }

    fn overflowed(&self) -> bool {
        let maximum = u64::from(self.pool_params.max_connections);
        match self.ownership_bound() {
            None => self.capacity_used() > maximum,
            Some(bound) => {
                self.active_sessions() + u64::from(self.connects_in_progress) > maximum
                    || self.owned_connections > bound
            }
        }
    }

    /// `max_connections + max_retiring` when an attached pool bounds its
    /// retiring sessions separately.
    fn ownership_bound(&self) -> Option<u64> {
        let PoolBackend::Attached {
            max_retiring: Some(retiring),
            ..
        } = &self.backend
        else {
            return None;
        };
        Some(u64::from(self.pool_params.max_connections) + u64::from(*retiring))
    }

    fn pending_connect_credit(&self) -> u32 {
        match &self.backend {
            PoolBackend::Attached {
                pending_connect_credit,
                ..
            } => *pending_connect_credit,
            PoolBackend::Native { .. } | PoolBackend::Owned { .. } => 1,
        }
    }

    /// Credits a new waiter to a pending connect that can still absorb one,
    /// within the streams the last credited session admitted.
    fn absorb_waiter(&self) -> Option<Rc<ConnectCredit>> {
        let limit = self
            .pending_connect_credit()
            .min(self.observed_capacity.unwrap_or(u32::MAX));
        let credit = self
            .connect_credits
            .iter()
            .find(|credit| credit.waiters.get() < limit)?;
        credit.waiters.set(credit.waiters.get() + 1);
        Some(Rc::clone(credit))
    }

    fn take_credited_waiters(&mut self, credit: &Rc<ConnectCredit>) -> Vec<Rc<Waiter>> {
        let (credited, waiters): (VecDeque<_>, _) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| {
                waiter
                    .credit
                    .as_ref()
                    .is_some_and(|own| Rc::ptr_eq(own, credit))
            });
        self.waiters = waiters;
        credited.into()
    }

    /// Removes the first waiter that no pending connect is expected to serve.
    fn take_uncovered_waiter(&mut self) -> Option<Rc<Waiter>> {
        let index = self.waiters.iter().position(|waiter| {
            waiter.credit.as_ref().is_none_or(|credit| {
                !self
                    .connect_credits
                    .iter()
                    .any(|pending| Rc::ptr_eq(pending, credit))
            })
        })?;
        self.waiters.remove(index)
    }

    fn active_sessions(&self) -> u64 {
        self.sessions
            .iter()
            .filter(|entry| !entry.session.is_finished() && !entry.session.is_draining())
            .count() as u64
    }

    fn capacity_used(&self) -> u64 {
        if self.backend.is_attached() {
            self.owned_connections
        } else {
            self.sessions.len() as u64 + u64::from(self.connects_in_progress)
        }
    }

    fn established_sessions_at_limit(&self) -> bool {
        if let Some(bound) = self.ownership_bound() {
            return self.active_sessions() >= u64::from(self.pool_params.max_connections)
                || self
                    .owned_connections
                    .saturating_sub(u64::from(self.connects_in_progress))
                    >= bound;
        }
        if self.backend.is_attached() {
            self.owned_connections
                .saturating_sub(u64::from(self.connects_in_progress))
                >= u64::from(self.pool_params.max_connections)
        } else {
            self.sessions.len() >= self.pool_params.max_connections as usize
        }
    }

    fn take_waiters(&mut self, count: usize) -> Vec<Rc<Waiter>> {
        let count = count.min(self.waiters.len());
        self.waiters.drain(..count).collect()
    }

    fn take_all_waiters(&mut self) -> Vec<Rc<Waiter>> {
        self.waiters.drain(..).collect()
    }

    /// Stamps when each session lost its last active or reserved exchange, for idle
    /// retirement and the most-recently-used order, and reports whether idle retirement
    /// must wake for a session that became idle: at expiry while it waits without a
    /// deadline; on an interval to start sweeping, the first time no waiter is
    /// `handed` a session as one becomes idle.
    fn track_idle_sessions(&mut self, handed: bool) -> bool {
        if !self.stamps_idle() {
            return false;
        }
        let mut now = None;
        self.count_scan();
        self.sessions
            .for_each_mut(|entry| entry.stamp_idle(&mut now));
        self.wakes_idle_retirement(now, handed)
    }

    /// `track_idle_sessions` for the session at `slot` alone, which a settled index vouches
    /// is that pass: every other session's stamp already is what it would leave (S6.9).
    fn track_idle_session(&mut self, slot: Option<SessionSlot>, handed: bool) -> bool {
        if !self.stamps_idle() {
            return false;
        }
        let mut now = None;
        if let Some(slot) = slot {
            self.sessions.get_mut(slot).stamp_idle(&mut now);
        }
        if let Some(now) = now {
            self.index.idle_floor.get_or_insert(now);
        }
        self.wakes_idle_retirement(now, handed)
    }

    /// Whether sessions are stamped idle: for idle retirement or the most-recently-used order.
    fn stamps_idle(&self) -> bool {
        !self.pool_params.idle_timeout.is_zero()
            || self.pool_params.reuse_order == SessionReuseOrder::MostRecentlyUsed
    }

    /// Whether idle retirement must wake after a pass that first stamped a session idle at
    /// `now`, if any.
    fn wakes_idle_retirement(&mut self, now: Option<Instant>, handed: bool) -> bool {
        let Some(now) = now.filter(|_| !self.pool_params.idle_timeout.is_zero()) else {
            return false;
        };
        match self.pool_params.idle_sweep {
            IdleSweep::AtExpiry => std::mem::take(&mut self.idle_retirement_parked),
            IdleSweep::Interval => {
                let start = !handed && self.idle_sweep_from.is_none();
                if start {
                    self.idle_sweep_from = Some(now);
                }
                start
            }
        }
    }

    /// Whether the pool's sessions may take an acquisition: an owned pool checks
    /// its worker's admission here, once per acquisition step, for all of its
    /// sessions (`DIV-046`), where a `ClientSession` checks its own.
    fn admits_reservations(&self) -> bool {
        !matches!(self.backend, PoolBackend::Owned { .. }) || worker_admits(self.context.handle())
    }

    /// When the longest-idle session became idle; `None` while no session is idle.
    fn longest_idle_since(&self) -> Option<Instant> {
        self.sessions
            .iter()
            .filter_map(SessionEntry::idle_since)
            .min()
    }

    /// Removes the idle-expired sessions and closes their admission before any is
    /// drained: a drain runs callbacks, which must not reserve a session that a
    /// later drain in the same batch would then refuse.
    fn take_idle_expired(&mut self, now: Instant) -> RetiredSessions {
        let timeout = self.pool_params.idle_timeout;
        let sweep = self.pool_params.idle_sweep;
        let mut expired = RetiredSessions::default();
        self.sessions.retain(|entry| {
            let retain = !entry.idle_expired(now, timeout, sweep);
            if !retain {
                entry.session.close_admission();
                expired.push(entry.session.clone());
            }
            retain
        });
        if !expired.is_empty() {
            self.unsettle_index();
        }
        expired
    }

    /// The session an acquisition reuses when no busy session can take another
    /// exchange, in reuse order, never a returning one (`DIV-046`). On an
    /// interval (`checkout_at`), as hyper-util's checkout pops its idle list
    /// from the end: it retires expired candidates into `retired`, closing
    /// their admission, until it finds one that is not, and leaves older
    /// expired sessions to the sweep.
    #[inline(never)]
    fn select_idle_session(
        &mut self,
        checkout_at: Option<Instant>,
        retired: &mut RetiredSessions,
    ) -> Option<SessionSlot> {
        let timeout = self.pool_params.idle_timeout;
        let sweep = self.pool_params.idle_sweep;
        loop {
            let slot = match self.pool_params.reuse_order {
                SessionReuseOrder::LeastRecentlyUsed => None,
                SessionReuseOrder::MostRecentlyUsed => self.most_recently_idle_session(),
            }
            .or_else(|| {
                self.count_scan();
                self.sessions.find(|entry| {
                    entry.session.supports_more_transactions() && !entry.session.is_returning()
                })
            })?;
            if !checkout_at
                .is_some_and(|now| self.sessions.get(slot).idle_expired(now, timeout, sweep))
            {
                return Some(slot);
            }
            let entry = self.sessions.remove(slot);
            entry.session.close_admission();
            retired.push(entry.session);
        }
    }

    /// The selectable session that most recently became idle; of two that became idle at
    /// once, the later in the pool's order.
    fn most_recently_idle_session(&self) -> Option<SessionSlot> {
        self.count_scan();
        self.most_recently_idle_entry()
    }

    /// `most_recently_idle_session` without the count.
    fn most_recently_idle_entry(&self) -> Option<SessionSlot> {
        self.sessions
            .entries()
            .enumerate()
            .filter(|(_, (_, entry))| entry.session.supports_more_transactions())
            .filter_map(|(position, (slot, entry))| {
                entry.idle_since().map(|since| ((since, position), slot))
            })
            .max_by_key(|(key, _)| *key)
            .map(|(_, slot)| slot)
    }

    /// The session a least-loaded acquisition takes (`DIV-048`), as `least_loaded_entry`
    /// finds it. On an interval (`checkout_at`), an idle candidate that has expired is retired
    /// into `retired`, its admission closed, as `select_idle_session` retires one, and the
    /// search runs again over the sessions left.
    #[inline(never)]
    fn select_least_loaded(
        &mut self,
        checkout_at: Option<Instant>,
        retired: &mut RetiredSessions,
    ) -> Option<SessionSlot> {
        let timeout = self.pool_params.idle_timeout;
        let sweep = self.pool_params.idle_sweep;
        loop {
            self.count_scan();
            let slot = self.least_loaded_entry()?;
            let entry = self.sessions.get(slot);
            if !entry.session.is_idle()
                || !checkout_at.is_some_and(|now| entry.idle_expired(now, timeout, sweep))
            {
                return Some(slot);
            }
            let entry = self.sessions.remove(slot);
            entry.session.close_admission();
            retired.push(entry.session);
        }
    }

    /// Of the sessions that can take an exchange, one with the fewest exchanges in flight
    /// (`PooledSession::load`), taken in the passes' order (`DIV-048`): the first busy one in
    /// the pool's order, else an idle one in reuse order. An idle session has none in flight,
    /// so when no busy session is that lightly loaded, every candidate is idle.
    fn least_loaded_entry(&self) -> Option<SessionSlot> {
        let returning = self.selects_returning();
        let (_, busy) = self
            .sessions
            .entries()
            .filter(|(_, entry)| {
                entry.session.supports_more_transactions()
                    && (returning || !entry.session.is_returning())
            })
            .fold(None, |least, (slot, entry)| {
                let load = entry.session.load();
                let busy = (!entry.session.is_idle()).then_some(slot);
                match least {
                    Some((minimum, first)) if minimum < load => Some((minimum, first)),
                    Some((minimum, Some(first))) if minimum == load => Some((minimum, Some(first))),
                    _ => Some((load, busy)),
                }
            })?;
        busy.or_else(|| match self.pool_params.reuse_order {
            SessionReuseOrder::LeastRecentlyUsed => None,
            SessionReuseOrder::MostRecentlyUsed => self.most_recently_idle_entry(),
        })
        .or_else(|| {
            self.sessions
                .find(|entry| entry.session.is_idle() && entry.session.supports_more_transactions())
        })
    }
}

#[cfg(test)]
thread_local! {
    /// This thread's passes over a pool batch: waiters to signal, or retired
    /// sessions to drain.
    static BATCH_PASSES: Cell<u64> = const { Cell::new(0) };
}

/// Takes this thread's count of passes over a pool batch, resetting it.
#[cfg(test)]
pub(crate) fn take_batch_passes_for_test() -> u64 {
    BATCH_PASSES.replace(0)
}

#[cfg(test)]
fn count_batch_pass() {
    BATCH_PASSES.set(BATCH_PASSES.get() + 1);
}

fn signal_waiters(waiters: Vec<Rc<Waiter>>) {
    if waiters.is_empty() {
        return;
    }
    #[cfg(test)]
    count_batch_pass();
    waiters.into_iter().for_each(|waiter| {
        let _ = catch_callback(|| waiter.signal());
    });
}

fn cancel_waiters(waiters: Vec<Rc<Waiter>>, error: PoolError) {
    waiters.into_iter().for_each(|waiter| {
        let error = error.clone();
        let _ = catch_callback(|| waiter.cancel(error));
    });
}

fn drain_sessions(sessions: Vec<PooledSession>) {
    if sessions.is_empty() {
        return;
    }
    #[cfg(test)]
    count_batch_pass();
    sessions.into_iter().for_each(|session| {
        let _ = catch_callback(|| session.initiate_drain());
    });
}

/// Sessions removed from the pool whose drains are still owed. Dropping the
/// batch, also while unwinding from a panicking callback, drains every session
/// left in it: the pool no longer reaches them, so a skipped drain would leave
/// the connection open and the pool's drain waiting for it.
#[derive(Default)]
struct RetiredSessions(Vec<PooledSession>);

impl RetiredSessions {
    fn push(&mut self, session: PooledSession) {
        self.0.push(session);
    }

    fn append(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Drains every session, in order, then resumes the first panic a drain raised.
    fn drain_propagating_panics(mut self) {
        if self.0.is_empty() {
            return;
        }
        #[cfg(test)]
        count_batch_pass();
        let mut first_panic = None;
        std::mem::take(&mut self.0).into_iter().for_each(|session| {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| session.initiate_drain())) {
                if first_panic.is_none() {
                    first_panic = Some(payload);
                } else {
                    drop_caught(payload);
                }
            }
        });
        if let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

impl Drop for RetiredSessions {
    fn drop(&mut self) {
        if self.0.is_empty() {
            return;
        }
        drain_sessions(std::mem::take(&mut self.0));
    }
}

fn pop_next_session_for_teardown(state: &mut PoolState) -> Option<SessionEntry> {
    let slot = state
        .sessions
        .find(|entry| !entry.session.supports_more_transactions())
        .or_else(|| state.sessions.find(|entry| !entry.session.is_idle()))
        .or_else(|| state.sessions.front())?;
    Some(state.sessions.remove(slot))
}

pub(crate) fn notify_session_event(
    pool: &Rc<RefCell<PoolState>>,
    worker: WorkerId,
    changed_session: Option<&PooledSession>,
    enqueue_back: bool,
    wake_count: usize,
    release_idle: bool,
) {
    let (sessions_to_drain, claimer, waiters, idle_changed, idle_wake) = {
        let mut state = pool.borrow_mut();
        if state.context.worker_id() != worker {
            return;
        }
        #[cfg(test)]
        state
            .session_event_passes
            .set(state.session_event_passes.get() + 1);
        let prior_idle = state.idle_sessions;
        if let Some(session) =
            changed_session.filter(|session| state.index_hears(session, release_idle))
        {
            let (claimer, waiters, idle, idle_wake) =
                state.release_indexed(session, enqueue_back, wake_count);
            (
                Vec::new(),
                claimer,
                waiters,
                prior_idle.abs_diff(idle),
                idle_wake,
            )
        } else {
            state.count_scan();
            state
                .sessions
                .retain(|entry| !entry.session.is_finished() && !entry.session.is_draining());
            if enqueue_back
                && let Some(changed_session) = changed_session
                && let Some(slot) = state.slot_of(changed_session)
            {
                state.sessions.move_to_back(slot);
            }
            let sessions_to_drain = if state.pooling_enabled || !release_idle {
                Vec::new()
            } else if let Some(changed_session) = changed_session {
                state
                    .slot_of(changed_session)
                    .map(|slot| vec![state.sessions.remove(slot).session])
                    .unwrap_or_default()
            } else {
                state.count_scan();
                let mut draining = Vec::new();
                let mut cursor = state.sessions.front();
                while let Some(slot) = cursor {
                    cursor = state.sessions.next(slot);
                    if state.sessions.get(slot).session.active_transactions() == 0 {
                        draining.push(state.sessions.remove(slot).session);
                    }
                }
                draining
            };
            state.count_scan();
            let idle = state
                .sessions
                .iter()
                .filter(|entry| entry.session.is_idle())
                .count();
            state.idle_sessions = idle;
            let wake_count = if !state.pooling_enabled && !sessions_to_drain.is_empty() {
                1
            } else {
                wake_count
            };
            let claimer = state.take_claimer(changed_session);
            let waiters =
                state.take_waiters(wake_count.saturating_sub(usize::from(claimer.is_some())));
            let idle_wake = state
                .track_idle_sessions(claimer.is_some() || !waiters.is_empty())
                .then(|| Rc::clone(&state.idle_wake));
            state.settle_index();
            (
                sessions_to_drain,
                claimer,
                waiters,
                prior_idle.abs_diff(idle),
                idle_wake,
            )
        }
    };
    if let Some(idle_wake) = idle_wake {
        let _ = catch_callback(|| idle_wake.signal());
    }
    if let Some(claimer) = claimer {
        let _ = catch_callback(|| claimer.signal());
    }
    signal_waiters(waiters);
    drain_sessions(sessions_to_drain);
    for _ in 0..idle_changed {
        notify_idle_observer(pool);
    }
}

/// Records a reservation an owned pool's acquisition made, as a
/// `ClientSession`'s `TransactionAttached` reaches `notify_session_event`, on
/// the session's entry alone (`DIV-046`): no longer idle, and moved to the back
/// when `enqueue_back`. Every other session's state is unchanged: each idle
/// transition was reported when it happened. A reservation that takes the
/// returning slot an acquisition waits for (a reservation on the session's
/// handle) wakes that acquisition, which steps again: the slot's retirement
/// announces no capacity once it is taken (`DIV-029`).
pub(crate) fn record_reservation(
    pool: &Rc<RefCell<PoolState>>,
    session: &PooledSession,
    was_idle: bool,
    enqueue_back: bool,
) {
    let mut state = pool.borrow_mut();
    let Some(slot) = state.slot_of(session) else {
        return;
    };
    let entry = state.sessions.get_mut(slot);
    entry.idle_since = None;
    entry.counted_idle = entry.session.is_idle();
    if was_idle {
        state.idle_sessions = state.idle_sessions.saturating_sub(1);
    }
    if enqueue_back {
        state.sessions.move_to_back(slot);
    }
    state.index_session(slot, enqueue_back);
    #[cfg(debug_assertions)]
    state.verify_index();
    let claimer = state.take_claimer(Some(session));
    drop(state);
    if let Some(claimer) = claimer {
        let _ = catch_callback(|| claimer.signal());
    }
}

/// Lists an owned HTTP/1.1 session whose slot turned returning (`DIV-029`), at its exchange's
/// consumer's completion or at the release of a reservation taken on that slot, as a busy
/// session that can take an exchange: as a returned slot's release
/// would, the session moves to the back of the pool's order, where it joins the index's busy
/// list, so that selection sees it before its driver retires the exchange. Returns false,
/// changing nothing, when the pool is borrowed further up the stack.
pub(crate) fn record_returning(pool: &Rc<RefCell<PoolState>>, session: &PooledSession) -> bool {
    let Ok(mut state) = pool.try_borrow_mut() else {
        return false;
    };
    if let Some(slot) = state.slot_of(session) {
        state.sessions.move_to_back(slot);
        state.index_session(slot, true);
        #[cfg(debug_assertions)]
        state.verify_index();
    }
    true
}

fn notify_idle_observer(pool: &Rc<RefCell<PoolState>>) {
    let observer = pool.borrow().idle_observer.clone();
    notify_idle_observer_with(pool, observer);
}

fn notify_idle_observer_with(
    pool: &Rc<RefCell<PoolState>>,
    observer: Option<Rc<dyn IdleSessionObserver>>,
) {
    if let Some(observer) = observer {
        let _ = catch_callback(|| {
            observer.on_idle_sessions_changed(&HttpSessionPool {
                state: Rc::clone(pool),
                drain_on_drop: false,
            });
        });
        drop_caught(observer);
    }
}

struct WaiterRegistration {
    state: Weak<RefCell<PoolState>>,
    id: u64,
    credit: Option<Rc<ConnectCredit>>,
    /// The session whose returning slot the waiter waits for (`DIV-046`).
    claim: Option<PooledSession>,
}

impl Drop for WaiterRegistration {
    fn drop(&mut self) {
        if let Some(session) = self.claim.take() {
            session.release_claim(self.id);
        }
        if let Some(credit) = self.credit.take() {
            credit.waiters.set(credit.waiters.get().saturating_sub(1));
        }
        if let Some(state) = self.state.upgrade() {
            state
                .borrow_mut()
                .waiters
                .retain(|waiter| waiter.id != self.id);
        }
    }
}

struct ConnectingGuard {
    state: Option<Weak<RefCell<PoolState>>>,
    observer: Option<Rc<dyn PoolObserver>>,
    credit: Option<Rc<ConnectCredit>>,
}

impl ConnectingGuard {
    fn release(&mut self, wake_waiter: bool) {
        let observer = self.observer.take();
        let Some(state) = self.state.take().and_then(|state| state.upgrade()) else {
            notify_pending_observer(observer, -1);
            return;
        };
        let waiters = {
            let mut state = state.borrow_mut();
            state.connects_in_progress = state.connects_in_progress.saturating_sub(1);
            if let Some(credit) = &self.credit {
                state
                    .connect_credits
                    .retain(|pending| !Rc::ptr_eq(pending, credit));
            }
            if wake_waiter {
                state.take_waiters(1)
            } else {
                Vec::new()
            }
        };
        signal_waiters(waiters);
        notify_pending_observer(observer, -1);
    }
}

fn notify_pending_observer(observer: Option<Rc<dyn PoolObserver>>, delta: i32) {
    if let Some(observer) = observer {
        let _ = catch_callback(|| observer.on_pending_connections_changed(delta));
        drop_caught(observer);
    }
}

impl Drop for ConnectingGuard {
    fn drop(&mut self) {
        self.release(true);
    }
}

struct SessionOwnership {
    pool: Weak<RefCell<PoolState>>,
    counted: bool,
    wake_on_release: Cell<bool>,
    transferred: Rc<CancellableBaton>,
}

impl SessionOwnership {
    fn reserve(pool: &Rc<RefCell<PoolState>>, state: &mut PoolState) -> OwnershipGuard {
        let counted = state.backend.is_attached();
        let ownership = OwnershipGuard(Rc::new(Self {
            pool: Rc::downgrade(pool),
            counted,
            wake_on_release: Cell::new(true),
            transferred: Rc::new(CancellableBaton::new()),
        }));
        if counted {
            state.owned_connections += 1;
        }
        ownership
    }
}

struct OwnershipGuard(Rc<SessionOwnership>);

impl Drop for OwnershipGuard {
    fn drop(&mut self) {
        if !self.0.counted {
            return;
        }
        let _ = catch_callback(|| {
            if let Some(pool) = self.0.pool.upgrade() {
                let waiters = {
                    let mut state = pool.borrow_mut();
                    state.owned_connections -= 1;
                    if self.0.wake_on_release.get() {
                        state.take_waiters(1)
                    } else {
                        Vec::new()
                    }
                };
                signal_waiters(waiters);
            }
        });
    }
}

struct SessionCompletion {
    closed: LocalClientFuture<Result<(), HttpError>>,
    ownership: Rc<SessionOwnership>,
}

impl SessionCompletion {
    fn new(session: &PooledSession, ownership: Rc<SessionOwnership>) -> Self {
        let closed = session.wait_closed();
        Self {
            closed: Box::pin(async move { closed.await.map(|_| ()) }),
            ownership,
        }
    }

    async fn observe(self, tasks: &PoolTasks) {
        tokio::select! {
            biased;
            _ = self.ownership.transferred.wait(CancellationToken::never()) => {}
            result = self.closed => {
                if let Err(source) = result {
                    tasks.fail(PoolError::SessionJoin { source });
                }
            }
        }
    }
}

enum ConnectionFactory {
    Native {
        address: SocketAddress,
        connector: Rc<dyn SessionConnector>,
    },
    Attached {
        security: TransportSecurity,
        factory: Rc<dyn AttachedSessionFactoryWithCause>,
    },
    Owned {
        address: SocketAddress,
    },
}

/// The configuration of one native or owned connection attempt, read from the
/// pool's backend at each attempt.
enum DialConfig {
    Native(ConnectionParams, SessionParams),
    Owned(ConnectionParams, SessionParams),
}

/// One connection attempt's dial: a uniplex session's, or an owned session's.
enum Dial {
    Uniplex(LocalClientFuture<Result<ClientSession, AttachedConnectFailure>>),
    Owned(LocalClientFuture<Result<OwnedClientSession, HttpError>>),
}

impl Future for Dial {
    type Output = Result<PooledSession, AttachedConnectFailure>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Uniplex(future) => future
                .as_mut()
                .poll(context)
                .map(|result| result.map(PooledSession::Uniplex)),
            Self::Owned(future) => future.as_mut().poll(context).map(|result| {
                result
                    .map(PooledSession::Owned)
                    .map_err(AttachedConnectFailure::from)
            }),
        }
    }
}

struct ConnectionAttempt {
    context: WorkerContext,
    factory: ConnectionFactory,
    ownership: Option<OwnershipGuard>,
    guard: ConnectingGuard,
}

enum AcquireStep {
    /// `retired`: idle-expired sessions to drain once `session` is reserved, so
    /// that no callback their drain runs can reserve `session` first.
    Session {
        session: PooledSession,
        retired: RetiredSessions,
    },
    StartAndWait {
        attempt: ConnectionAttempt,
        waiter: Rc<Waiter>,
        registration: WaiterRegistration,
        wait_timeout: Duration,
    },
    Wait {
        waiter: Rc<Waiter>,
        registration: WaiterRegistration,
        wait_timeout: Duration,
    },
}

pub struct HttpSessionPool {
    state: Rc<RefCell<PoolState>>,
    drain_on_drop: bool,
}

impl HttpSessionPool {
    pub fn new(
        context: WorkerContext,
        address: SocketAddress,
        pool_params: PoolParams,
        connection_params: ConnectionParams,
        session_params: SessionParams,
    ) -> Self {
        Self::with_connector(
            context,
            address,
            pool_params,
            connection_params,
            session_params,
            Rc::new(HttpConnector),
        )
    }

    pub fn with_connector(
        context: WorkerContext,
        address: SocketAddress,
        pool_params: PoolParams,
        mut connection_params: ConnectionParams,
        session_params: SessionParams,
        connector: Rc<dyn SessionConnector>,
    ) -> Self {
        configure_connection_params(&mut connection_params, pool_params);
        let pool = Self::build(
            context,
            pool_params,
            PoolBackend::Native {
                address,
                connection: connection_params,
                session: session_params,
                connector,
            },
        );
        pool.start(pool_params);
        pool
    }

    /// An owned pool (`DIV-046`): `with_connector`'s pool, dialing owned client
    /// sessions with `connection` and `session`.
    pub(crate) fn owned(
        context: WorkerContext,
        address: SocketAddress,
        pool_params: PoolParams,
        connection: ConnectionParams,
        session: SessionParams,
    ) -> Self {
        let pool = Self::build(
            context,
            pool_params,
            PoolBackend::Owned {
                address,
                connection,
                session,
            },
        );
        pool.start(pool_params);
        pool
    }

    /// Installs a native or owned pool's supervisor, then its idle retirement.
    fn start(&self, pool_params: PoolParams) {
        if let Err(error) = self.install_supervisor() {
            self.drain();
            let tasks = Rc::clone(&self.state.borrow().tasks);
            tasks.fail(error.clone());
            tasks.completion.send_replace(Some(Err(error)));
        } else if !pool_params.idle_timeout.is_zero() {
            self.start_idle_retirement();
        }
    }

    /// Creates a worker-local address-free pool after registering its supervisor.
    /// The security declaration describes the immediate hop, independently of an
    /// origin scheme; `Unknown` does not claim TLS or authentication.
    ///
    /// Pending factories count against `max_connections`. Sessions retiring
    /// after a drain, an expiry, or a flush stay owned until completion and
    /// count against it too unless [`AttachedPoolParams::max_retiring`] is set;
    /// then pending factories and all owned sessions together count against
    /// `max_connections` plus that bound. New construction rejects quiescence.
    /// Lowering capacity does not evict outstanding ownership; it prevents new
    /// admission until enough existing slots finish or transfer.
    pub fn try_with_attached_factory(
        context: WorkerContext,
        params: AttachedPoolParams,
        security: TransportSecurity,
        factory: Rc<dyn AttachedSessionFactory>,
    ) -> Result<Self, PoolError> {
        Self::try_with_attached_factory_with_cause(
            context,
            params,
            security,
            Rc::new(LegacyAttachedSessionFactory(factory)),
        )
    }

    /// Creates an address-free pool that retains selected external connector causes.
    ///
    /// Admission, ownership bounds, retries, and cleanup match
    /// [`Self::try_with_attached_factory`]. Native-only failures still produce
    /// [`PoolError::ConnectFailed`]; cause-bearing failures produce
    /// [`PoolError::AttachedConnectFailed`]. This does not retry HTTP requests.
    pub fn try_with_attached_factory_with_cause(
        context: WorkerContext,
        params: AttachedPoolParams,
        security: TransportSecurity,
        factory: Rc<dyn AttachedSessionFactoryWithCause>,
    ) -> Result<Self, PoolError> {
        if !context.handle().is_accepting() {
            return Err(PoolError::Draining);
        }
        let pool = Self::build(
            context,
            PoolParams {
                max_connections: params.max_connections,
                max_connection_attempts: params.max_connection_attempts,
                enable_tls_session_caching: false,
                connect_timeout: params.connect_timeout,
                max_age: params.max_age,
                max_waiters: params.max_waiters,
                idle_timeout: Duration::ZERO,
                reuse_order: SessionReuseOrder::LeastRecentlyUsed,
                idle_sweep: IdleSweep::AtExpiry,
            },
            PoolBackend::Attached {
                security,
                factory: Some(factory),
                max_retiring: params.max_retiring,
                pending_connect_credit: params.pending_connect_credit.max(1),
            },
        );
        pool.install_supervisor()?;
        if !pool.state.borrow().context.handle().is_accepting() {
            return Err(PoolError::Draining);
        }
        Ok(pool)
    }

    fn build(context: WorkerContext, mut pool_params: PoolParams, backend: PoolBackend) -> Self {
        let pooling_enabled = pool_params.max_connections != 0;
        let indexed = matches!(
            &backend,
            PoolBackend::Owned { connection, .. }
                if connection.plaintext_protocol == CodecProtocol::Http11
        );
        pool_params.max_connections = pool_params.max_connections.max(1);
        pool_params.max_connection_attempts = pool_params.max_connection_attempts.max(1);
        let (completion, _) = watch::channel(None);
        let tasks = Rc::new(PoolTasks {
            group: RefCell::new(Some(TaskGroup::new())),
            shutdown: Rc::new(CancellableBaton::new()),
            changed: Notify::new(),
            failure: RefCell::new(None),
            completion,
            #[cfg(test)]
            spawned: Cell::default(),
        });
        Self {
            state: Rc::new(RefCell::new(PoolState {
                context,
                pool_params,
                backend,
                observer: None,
                idle_observer: None,
                sessions: SessionList::new(),
                selection: OwnedSessionSelection::FirstAvailable,
                index: SessionIndex::new(indexed),
                idle_sessions: 0,
                waiters: VecDeque::new(),
                connects_in_progress: 0,
                connect_credits: Vec::new(),
                observed_capacity: None,
                owned_connections: 0,
                tasks: Rc::clone(&tasks),
                idle_wake: Rc::new(CancellableBaton::new()),
                idle_retirement_parked: false,
                idle_sweep_from: None,
                #[cfg(test)]
                idle_retirement_passes: Cell::default(),
                #[cfg(test)]
                idle_retirement_retiring_passes: Cell::default(),
                #[cfg(test)]
                session_event_passes: Cell::default(),
                #[cfg(test)]
                connection_attempts: Cell::default(),
                #[cfg(test)]
                pool_scans: Cell::default(),
                #[cfg(test)]
                claim_scans: Cell::default(),
                #[cfg(test)]
                force_scans: Cell::default(),
                next_waiter_id: 1,
                pooling_enabled,
                draining: false,
                reload: None,
                reload_generation: 0,
            })),
            drain_on_drop: true,
        }
    }

    fn start_idle_retirement(&self) {
        let (tasks, wake) = {
            let state = self.state.borrow();
            (Rc::clone(&state.tasks), Rc::clone(&state.idle_wake))
        };
        let state = Rc::downgrade(&self.state);
        // A refused spawn means the pool is already draining, which retires every session.
        let _ = spawn_pool_task(&tasks, move |cancellation| {
            retire_idle_sessions(state, wake, cancellation)
        });
    }

    fn install_supervisor(&self) -> Result<(), PoolError> {
        let context = self.state.borrow().context.clone();
        let tasks = Rc::clone(&self.state.borrow().tasks);
        let cleanup_tasks = Rc::clone(&tasks);
        let state = Rc::downgrade(&self.state);
        let cleanup_state = state.clone();
        let registration = context
            .register_cleanup(move || {
                request_pool_shutdown(&cleanup_state, &cleanup_tasks);
            })
            .map_err(|source| PoolError::CleanupRegistration { source })?;
        let registration = PoolCleanupRegistration(Some(registration));
        let cleanup = context
            .register_scope(supervise_pool(state, tasks, context.clone(), registration))
            .map_err(|source| PoolError::CleanupRegistration { source })?;
        drop(
            cleanup
                .activate()
                .map_err(|source| PoolError::CleanupRegistration { source })?,
        );
        Ok(())
    }

    pub fn get_session_with_reservation(
        &self,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        acquire_session(Rc::clone(&self.state), cancellation)
    }

    pub(crate) fn state(&self) -> &Rc<RefCell<PoolState>> {
        &self.state
    }

    pub fn set_max_connections(&self, maximum: u32) -> Result<(), PoolError> {
        let mut state = self.state.borrow_mut();
        ensure_pool_open(&state)?;
        state.pooling_enabled = maximum != 0;
        state.pool_params.max_connections = maximum.max(1);
        Ok(())
    }

    pub fn set_max_connection_attempts(&self, attempts: u8) -> Result<(), PoolError> {
        let mut state = self.state.borrow_mut();
        ensure_pool_open(&state)?;
        state.pool_params.max_connection_attempts = attempts.max(1);
        Ok(())
    }

    pub fn set_pooling_enabled(&self, enabled: bool) -> Result<(), PoolError> {
        let mut state = self.state.borrow_mut();
        ensure_pool_open(&state)?;
        state.pooling_enabled = enabled;
        Ok(())
    }

    pub fn set_max_age(&self, maximum: Duration) -> Result<(), PoolError> {
        let mut state = self.state.borrow_mut();
        ensure_pool_open(&state)?;
        state.pool_params.max_age = maximum;
        Ok(())
    }

    /// An owned pool's selection among its established sessions (`DIV-048`).
    pub(crate) fn set_session_selection(
        &self,
        selection: OwnedSessionSelection,
    ) -> Result<(), PoolError> {
        let mut state = self.state.borrow_mut();
        ensure_pool_open(&state)?;
        state.selection = selection;
        Ok(())
    }

    pub fn set_session_params(&self, params: SessionParams) -> Result<(), PoolError> {
        let old = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            let PoolBackend::Native { session, .. } = &mut state.backend else {
                return Err(PoolError::NativeConfigurationOnly);
            };
            std::mem::replace(session, params)
        };
        drop_caught(old);
        Ok(())
    }

    pub fn set_connection_params(&self, mut params: ConnectionParams) -> Result<(), PoolError> {
        let old = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            let pool_params = state.pool_params;
            let PoolBackend::Native { connection, .. } = &mut state.backend else {
                return Err(PoolError::NativeConfigurationOnly);
            };
            configure_connection_params(&mut params, pool_params);
            std::mem::replace(connection, params)
        };
        drop_caught(old);
        Ok(())
    }

    pub fn set_observer(&self, observer: Option<Rc<dyn PoolObserver>>) -> Result<(), PoolError> {
        let old = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            std::mem::replace(&mut state.observer, observer)
        };
        drop_caught(old);
        Ok(())
    }

    pub fn set_idle_session_observer(
        &self,
        observer: Option<Rc<dyn IdleSessionObserver>>,
    ) -> Result<(), PoolError> {
        let old = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            std::mem::replace(&mut state.idle_observer, observer)
        };
        drop_caught(old);
        Ok(())
    }

    pub fn set_reload_callback<F>(&self, callback: F, interval: Duration) -> Result<(), PoolError>
    where
        F: Fn(&HttpSessionPool) + 'static,
    {
        if interval == Duration::ZERO {
            return Ok(());
        }
        let (tasks, generation, stop, prior_reload) = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            let prior_reload = state.reload.take();
            state.reload_generation = state.reload_generation.wrapping_add(1);
            let generation = state.reload_generation;
            let stop = Rc::new(CancellableBaton::new());
            state.reload = Some(ReloadConfig {
                callback: Rc::new(callback),
                generation,
                stop: Rc::clone(&stop),
            });
            (Rc::clone(&state.tasks), generation, stop, prior_reload)
        };
        if let Some(prior_reload) = prior_reload {
            let _ = catch_callback(|| prior_reload.stop.signal());
            drop_caught(prior_reload);
        }
        let state = Rc::downgrade(&self.state);
        let task_stop = Rc::clone(&stop);
        let spawn = spawn_pool_task(&tasks, move |cancellation| async move {
            loop {
                match task_stop.timed_wait(interval, cancellation.clone()).await {
                    WaitStatus::TimedOut => task_stop.reset(),
                    WaitStatus::Signalled | WaitStatus::Cancelled | WaitStatus::NotReady => {
                        return;
                    }
                }
                let Some(state) = state.upgrade() else {
                    return;
                };
                let callback = {
                    let state = state.borrow();
                    let Some(reload) = &state.reload else {
                        return;
                    };
                    if reload.generation != generation {
                        return;
                    }
                    Rc::clone(&reload.callback)
                };
                let pool = HttpSessionPool {
                    state: Rc::clone(&state),
                    drain_on_drop: false,
                };
                if catch_callback(|| callback(&pool)).is_err() {
                    continue;
                }
                if state.borrow().draining
                    || !state
                        .borrow()
                        .reload
                        .as_ref()
                        .is_some_and(|reload| reload.generation == generation)
                {
                    return;
                }
            }
        });
        if spawn.is_err() {
            let _ = catch_callback(|| stop.signal());
            return Err(PoolError::Draining);
        }
        Ok(())
    }

    /// Whether the dial gate refuses another connection. Pending connects and
    /// established sessions count against `max_connections`, and so do an
    /// attached pool's retiring sessions unless it sets
    /// [`AttachedPoolParams::max_retiring`]. With it set to `n`, retiring
    /// sessions leave that count and the gate also refuses once all owned
    /// sessions plus pending connects reach `max_connections + n`. While this
    /// is false, [`AttachedPoolParams::pending_connect_credit`] may still
    /// credit an acquisition to a pending connect instead of dialing.
    pub fn full(&self) -> bool {
        self.state.borrow().full()
    }

    /// Whether the pool holds more connections than [`full`](Self::full)
    /// admits, for example after [`set_max_connections`](Self::set_max_connections)
    /// lowered the limit: the connections `full` counts exceed
    /// `max_connections`, or, with [`AttachedPoolParams::max_retiring`] set to
    /// `n`, all owned sessions plus pending connects exceed
    /// `max_connections + n`.
    pub fn overflowed(&self) -> bool {
        self.state.borrow().overflowed()
    }

    pub fn is_secure(&self) -> bool {
        self.state.borrow().backend.is_secure()
    }

    pub fn worker_id(&self) -> WorkerId {
        self.state.borrow().context.worker_id()
    }

    pub fn describe(&self) -> String {
        let state = self.state.borrow();
        match &state.backend {
            PoolBackend::Native { address, .. } => format!(
                "HttpSessionPool(peer={:?}, secure={}, sessions={}, pending={}, waiters={})",
                address,
                state.backend.is_secure(),
                state.sessions.len(),
                state.connects_in_progress,
                state.waiters.len(),
            ),
            PoolBackend::Attached { security, .. } => format!(
                "HttpSessionPool(attached, security={security:?}, sessions={}, pending={}, owned={}, waiters={})",
                state.sessions.len(),
                state.connects_in_progress,
                state.owned_connections,
                state.waiters.len(),
            ),
            PoolBackend::Owned { address, .. } => format!(
                "OwnedHttpSessionPool(peer={:?}, sessions={}, pending={}, waiters={})",
                address,
                state.sessions.len(),
                state.connects_in_progress,
                state.waiters.len(),
            ),
        }
    }

    pub fn has_available_sessions(&self) -> bool {
        self.state
            .borrow()
            .sessions
            .iter()
            .any(|entry| entry.session.supports_more_transactions())
    }

    pub fn has_idle_sessions(&self) -> bool {
        self.state
            .borrow()
            .sessions
            .iter()
            .any(|entry| entry.session.is_idle())
    }

    pub fn empty(&self) -> bool {
        let state = self.state.borrow();
        state.sessions.is_empty() && state.waiters.is_empty() && state.connects_in_progress == 0
    }

    /// Initiates drain immediately, even if the returned observation is never
    /// polled. Completion joins every pool task and owned session, including
    /// late factories and sessions retiring after flush/expiry. Explicitly
    /// detached sessions transfer that responsibility to their new owner.
    ///
    /// This is not transport flush/shutdown or physical worker completion. Keep
    /// required runtimes alive through runtime quiescence, then join the owner.
    /// Dropping this future never abandons cleanup. Repeated observations retain
    /// the same failure, reported only after the remaining joins finish.
    pub fn drain_and_join(&self) -> LocalClientFuture<Result<(), PoolError>> {
        let mut completion = self.state.borrow().tasks.completion.subscribe();
        self.drain();
        Box::pin(async move {
            loop {
                if let Some(result) = completion.borrow().clone() {
                    return result;
                }
                completion
                    .changed()
                    .await
                    .map_err(|_| PoolError::TaskJoin {
                        source: TaskGroupError::TaskCancelled,
                    })?;
            }
        })
    }

    pub fn drain(&self) -> bool {
        let (waiters, tasks, mut payloads) = {
            let mut state = self.state.borrow_mut();
            if state.draining {
                return false;
            }
            state.draining = true;
            state.unsettle_index();
            let waiters = state.take_all_waiters();
            state.idle_sessions = 0;
            state.connects_in_progress = 0;
            let mut payloads = PoolPayloads {
                reload: state.reload.take(),
                observer: state.observer.take(),
                idle_observer: state.idle_observer.take(),
                connection_params: None,
                session_params: None,
                connector: None,
                attached_factory: None,
            };
            match &mut state.backend {
                PoolBackend::Native {
                    connection,
                    session,
                    connector,
                    ..
                } => {
                    payloads.connection_params = Some(std::mem::take(connection));
                    payloads.session_params = Some(std::mem::take(session));
                    payloads.connector = Some(std::mem::replace(connector, Rc::new(HttpConnector)));
                }
                PoolBackend::Attached { factory, .. } => payloads.attached_factory = factory.take(),
                PoolBackend::Owned {
                    connection,
                    session,
                    ..
                } => {
                    payloads.connection_params = Some(std::mem::take(connection));
                    payloads.session_params = Some(std::mem::take(session));
                }
            }
            (waiters, Rc::clone(&state.tasks), payloads)
        };
        if let Some(reload) = payloads.reload.take() {
            let _ = catch_callback(|| reload.stop.signal());
            drop_caught(reload);
        }
        if catch_callback(|| {
            if let Some(group) = tasks.group.borrow_mut().as_mut() {
                group.cancel();
            }
        })
        .is_err()
        {
            tasks.fail(PoolError::CleanupPanicked);
        }
        let _ = catch_callback(|| tasks.shutdown.signal());
        cancel_waiters(waiters, PoolError::Draining);
        loop {
            let session = {
                let mut state = self.state.borrow_mut();
                pop_next_session_for_teardown(&mut state).map(|entry| entry.session)
            };
            let Some(session) = session else {
                break;
            };
            drain_sessions(vec![session]);
        }
        drop_caught(payloads.observer.take());
        drop_caught(payloads.idle_observer.take());
        drop_caught(payloads.connection_params.take());
        drop_caught(payloads.session_params.take());
        drop_caught(payloads.connector.take());
        if catch_callback(|| drop(payloads.attached_factory.take())).is_err() {
            tasks.fail(PoolError::CleanupPanicked);
        }
        true
    }

    pub fn flush(&self) {
        let waiters = {
            let mut state = self.state.borrow_mut();
            state.unsettle_index();
            state.take_all_waiters()
        };
        cancel_waiters(waiters, PoolError::Draining);
        loop {
            let next = {
                let mut state = self.state.borrow_mut();
                pop_next_session_for_teardown(&mut state).map(|entry| {
                    let was_idle = entry.session.is_idle();
                    if was_idle {
                        state.idle_sessions = state.idle_sessions.saturating_sub(1);
                    }
                    (entry.session, was_idle, state.idle_observer.clone())
                })
            };
            let Some((session, was_idle, observer)) = next else {
                break;
            };
            drain_sessions(vec![session]);
            if was_idle {
                notify_idle_observer_with(&self.state, observer);
            }
        }
    }

    /// Transfers responsibility for the detached session's completion to the
    /// recipient. Its former observer releases any attached ownership slot on
    /// its next turn, independently of the detached session's eventual closure.
    pub fn detach_idle_session(&self) -> Option<DetachedClientSession> {
        let mut state = self.state.borrow_mut();
        if !state.waiters.is_empty() {
            return None;
        }
        let slot = state.sessions.find(|entry| entry.session.is_idle())?;
        state.unsettle_index();
        let entry = state.sessions.remove(slot);
        state.idle_sessions = state.idle_sessions.saturating_sub(1);
        drop(state);
        let session = entry.session.into_client_session();
        session.unbind_pool();
        if let Some(ownership) = entry.ownership {
            let _ = catch_callback(|| ownership.transferred.signal());
        }
        notify_idle_observer(&self.state);
        Some(DetachedClientSession(session))
    }

    /// Establishes joined ownership before making a detached session available.
    /// Attached pools require matching security and an available ownership slot;
    /// native pools retain their original insertion and configuration policies.
    pub fn insert_idle_session(&self, session: DetachedClientSession) -> Result<(), PoolError> {
        let client_session = session.0;
        let session = PooledSession::Uniplex(client_session.clone());
        let owned = {
            let mut state = self.state.borrow_mut();
            ensure_pool_open(&state)?;
            if session.worker_id() != state.context.worker_id() {
                return Err(PoolError::WrongWorker);
            }
            if session.is_bound_to_pool() || !session.is_idle() {
                return Err(PoolError::InvalidSessionState);
            }
            if let PoolBackend::Attached { security, .. } = &state.backend {
                if session.connection_context().security() != *security {
                    return Err(PoolError::SecurityMismatch);
                }
                if state.full() {
                    return Err(PoolError::OwnershipCapacity);
                }
            }
            SessionOwnership::reserve(&self.state, &mut state)
        };
        let tasks = Rc::clone(&self.state.borrow().tasks);
        let ownership = Rc::clone(&owned.0);
        let completion = SessionCompletion::new(&session, Rc::clone(&ownership));
        let observing_tasks = Rc::clone(&tasks);
        spawn_pool_task(&tasks, move |_| async move {
            let _owned = owned;
            completion.observe(&observing_tasks).await;
        })
        .map_err(|_| PoolError::Draining)?;
        let (worker, wake_count) = {
            let mut state = self.state.borrow_mut();
            if let Err(error) = ensure_pool_open(&state) {
                drop(state);
                drain_sessions(vec![session]);
                return Err(error);
            }
            client_session.mark_available();
            session.bind_pool(Rc::downgrade(&self.state), None);
            let wake_count = session.available_transactions();
            let worker = state.context.worker_id();
            let max_age = sampled_max_age(state.pool_params.max_age);
            state
                .sessions
                .push_back(SessionEntry::new(session, max_age, Some(ownership)));
            (worker, wake_count)
        };
        notify_session_event(&self.state, worker, None, false, wake_count, true);
        Ok(())
    }

    pub fn session_count(&self) -> usize {
        self.state.borrow().sessions.len()
    }

    #[cfg(test)]
    pub(crate) fn task_count_for_test(&self) -> usize {
        self.state
            .borrow()
            .tasks
            .group
            .borrow()
            .as_ref()
            .map_or(0, TaskGroup::len)
    }

    #[cfg(test)]
    pub(crate) fn waiter_count_for_test(&self) -> usize {
        self.state.borrow().waiters.len()
    }

    #[cfg(test)]
    pub(crate) fn wake_waiters_for_test(&self, count: usize) {
        let waiters = self.state.borrow_mut().take_waiters(count);
        signal_waiters(waiters);
    }

    #[cfg(test)]
    pub(crate) fn expire_sessions_for_test(&self) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        state.sessions.for_each_mut(|entry| {
            let age = entry.session.age();
            assert!(!age.is_zero(), "the fixture established an aged session");
            entry.max_age = age;
        });
    }

    #[cfg(test)]
    pub(crate) fn expire_session_for_test(&self, session: &ClientSession) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        let slot = state
            .sessions
            .find(|entry| entry.session.is_client_session(session))
            .expect("the session is pooled");
        let entry = state.sessions.get_mut(slot);
        let age = entry.session.age();
        assert!(!age.is_zero(), "the fixture established an aged session");
        entry.max_age = age;
    }

    #[cfg(test)]
    pub(crate) fn age_idle_sessions_for_test(&self, by: Duration) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        state.sessions.for_each_mut(|entry| {
            let since = entry.idle_since.expect("the fixture left the session idle");
            entry.idle_since = since.checked_sub(by);
        });
    }

    #[cfg(test)]
    pub(crate) fn age_idle_session_for_test(&self, session: &ClientSession, by: Duration) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        let slot = state
            .sessions
            .find(|entry| entry.session.is_client_session(session))
            .expect("the session is pooled");
        let entry = state.sessions.get_mut(slot);
        let since = entry.idle_since.expect("the fixture left the session idle");
        entry.idle_since = since.checked_sub(by);
    }

    /// Stamps every session idle since `by` ago, active or not, and wakes idle
    /// retirement, as a missed session event would leave them.
    #[cfg(test)]
    pub(crate) fn stamp_sessions_idle_for_test(&self, by: Duration) {
        let wake = {
            let mut state = self.state.borrow_mut();
            let since = Instant::now().checked_sub(by);
            state.unsettle_index();
            state
                .sessions
                .for_each_mut(|entry| entry.idle_since = since);
            Rc::clone(&state.idle_wake)
        };
        wake.signal();
    }

    /// Sets a pooled session's maximum age to its age, as if it had reached it.
    #[cfg(test)]
    pub(crate) fn expire_pooled_session_for_test(&self, session: &PooledSession) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        let slot = state.slot_of(session).expect("the session is pooled");
        let entry = state.sessions.get_mut(slot);
        let age = entry.session.age();
        assert!(!age.is_zero(), "the fixture established an aged session");
        entry.max_age = age;
    }

    /// Moves a pooled session's idle stamp `by` earlier, as if it had been idle that much
    /// longer.
    #[cfg(test)]
    pub(crate) fn age_idle_pooled_session_for_test(&self, session: &PooledSession, by: Duration) {
        let mut state = self.state.borrow_mut();
        state.unsettle_index();
        let slot = state.slot_of(session).expect("the session is pooled");
        let entry = state.sessions.get_mut(slot);
        let since = entry.idle_since.expect("the fixture left the session idle");
        entry.idle_since = since.checked_sub(by);
    }

    /// The sessions in the index's busy and idle lists.
    #[cfg(test)]
    pub(crate) fn indexed_sessions_for_test(&self) -> (usize, usize) {
        let state = self.state.borrow();
        let members = |class| {
            std::iter::successors(state.sessions.class_front(class), |slot| {
                state.sessions.class_next(*slot)
            })
            .count()
        };
        (members(Class::Busy), members(Class::Idle))
    }

    /// Moves every idle stamp, and the index's idle floor, `by` earlier, as if that much time
    /// had passed for the idle sessions alone, leaving the index settled if it was.
    #[cfg(test)]
    pub(crate) fn pass_idle_time_for_test(&self, by: Duration) {
        let mut state = self.state.borrow_mut();
        state.sessions.for_each_mut(|entry| {
            entry.idle_since = entry.idle_since.and_then(|since| since.checked_sub(by));
        });
        state.index.idle_floor = state
            .index
            .idle_floor
            .and_then(|floor| floor.checked_sub(by));
    }

    #[cfg(test)]
    pub(crate) fn index_settled_for_test(&self) -> bool {
        self.state.borrow().index_settled()
    }

    #[cfg(test)]
    pub(crate) fn force_pool_scans_for_test(&self, forced: bool) {
        self.state.borrow().force_scans.set(forced);
    }

    #[cfg(test)]
    pub(crate) fn pending_connects_for_test(&self) -> u32 {
        self.state.borrow().connects_in_progress
    }

    #[cfg(test)]
    pub(crate) fn idle_retirement_passes_for_test(&self) -> u64 {
        self.state.borrow().idle_retirement_passes.get()
    }

    #[cfg(test)]
    pub(crate) fn session_event_passes_for_test(&self) -> u64 {
        self.state.borrow().session_event_passes.get()
    }

    #[cfg(test)]
    pub(crate) fn connection_attempts_for_test(&self) -> u64 {
        self.state.borrow().connection_attempts.get()
    }

    #[cfg(test)]
    pub(crate) fn pool_scans_for_test(&self) -> u64 {
        self.state.borrow().pool_scans.get()
    }

    #[cfg(test)]
    pub(crate) fn claim_scans_for_test(&self) -> u64 {
        self.state.borrow().claim_scans.get()
    }

    #[cfg(test)]
    pub(crate) fn pool_task_spawns_for_test(&self) -> u64 {
        self.state.borrow().tasks.spawned.get()
    }

    /// Idle-retirement passes that retired at least one session.
    #[cfg(test)]
    pub(crate) fn idle_retirement_retiring_passes_for_test(&self) -> u64 {
        self.state.borrow().idle_retirement_retiring_passes.get()
    }

    #[cfg(test)]
    pub(crate) fn acquisition_step_for_test(&self, started: Instant) -> Result<(), PoolError> {
        next_step(&self.state, &mut Some(started), false).map(|step| {
            if let AcquireStep::Session { retired, .. } = step {
                retired.drain_propagating_panics();
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn insert_session_for_teardown_test(&self, session: ClientSession) {
        let session = PooledSession::Uniplex(session);
        session.bind_pool(Rc::downgrade(&self.state), None);
        let mut state = self.state.borrow_mut();
        if session.is_idle() {
            state.idle_sessions = state.idle_sessions.saturating_add(1);
        }
        state
            .sessions
            .push_back(SessionEntry::new(session, Duration::ZERO, None));
    }
}

struct PoolPayloads {
    reload: Option<ReloadConfig>,
    observer: Option<Rc<dyn PoolObserver>>,
    idle_observer: Option<Rc<dyn IdleSessionObserver>>,
    connection_params: Option<ConnectionParams>,
    session_params: Option<SessionParams>,
    connector: Option<Rc<dyn SessionConnector>>,
    attached_factory: Option<Rc<dyn AttachedSessionFactoryWithCause>>,
}

fn ensure_pool_open(state: &PoolState) -> Result<(), PoolError> {
    if state.draining
        || state.context.handle().state() != WorkerState::Open
        || (state.backend.is_attached() && !state.context.handle().is_accepting())
    {
        return Err(PoolError::Draining);
    }
    Ok(())
}

impl Drop for HttpSessionPool {
    fn drop(&mut self) {
        if self.drain_on_drop {
            let _ = self.drain();
        }
    }
}

pub(crate) fn acquire_session<Reservation: PoolReservation>(
    state: Rc<RefCell<PoolState>>,
    cancellation: CancellationToken,
) -> LocalClientFuture<Result<Reservation, PoolError>> {
    Box::pin(async move {
        let mut started = Some(Instant::now());
        if cancellation.is_cancellation_requested() {
            return Err(PoolError::Cancelled);
        }
        match acquisition_step(&state, &mut started, false)? {
            Stepped::Reserved(reservation) => Ok(reservation),
            Stepped::Wait(wait) => {
                let started = started.expect("the acquisition anchored its deadline");
                acquire_after_wait(state, cancellation, started, wait).await
            }
        }
    })
}

/// How an acquisition that did not end at its first step continues.
pub(crate) enum AcquisitionStart<Reservation> {
    Done(Result<Reservation, PoolError>),
    Waiting(LocalClientFuture<Result<Reservation, PoolError>>),
}

/// Starts an acquisition now, for an owned pool's fast path (`DIV-046`): a
/// first step that selects a session, or fails, ends it with no boxed future
/// and no clock read for its deadline; only one that must wait for a session
/// or a dial continues in a boxed future, its deadline anchored when that step
/// found no session.
pub(crate) fn start_acquisition<Reservation: PoolReservation>(
    state: Rc<RefCell<PoolState>>,
    cancellation: CancellationToken,
) -> AcquisitionStart<Reservation> {
    if cancellation.is_cancellation_requested() {
        return AcquisitionStart::Done(Err(PoolError::Cancelled));
    }
    let mut started = None;
    let wait = match acquisition_step(&state, &mut started, false) {
        Ok(Stepped::Reserved(reservation)) => return AcquisitionStart::Done(Ok(reservation)),
        Err(error) => return AcquisitionStart::Done(Err(error)),
        Ok(Stepped::Wait(wait)) => wait,
    };
    let started = started.expect("a step that waits anchored the acquisition's deadline");
    AcquisitionStart::Waiting(Box::pin(acquire_after_wait(
        state,
        cancellation,
        started,
        wait,
    )))
}

/// A wait an acquisition step left, for a session or for the dial it started.
struct PendingWait {
    waiter: Rc<Waiter>,
    registration: WaiterRegistration,
    wait_timeout: Duration,
}

/// What one acquisition step did.
enum Stepped<Reservation> {
    Reserved(Reservation),
    Wait(PendingWait),
}

/// Reserves the session the step selects, draining the idle-expired sessions it
/// retired afterwards, or starts the dial it admits.
fn acquisition_step<Reservation: PoolReservation>(
    state: &Rc<RefCell<PoolState>>,
    started: &mut Option<Instant>,
    woken: bool,
) -> Result<Stepped<Reservation>, PoolError> {
    Ok(match next_step(state, started, woken)? {
        AcquireStep::Session { session, retired } => {
            let reservation = Reservation::reserve(state, session);
            retired.drain_propagating_panics();
            Stepped::Reserved(reservation?)
        }
        AcquireStep::StartAndWait {
            attempt,
            waiter,
            registration,
            wait_timeout,
        } => {
            start_connection(state, attempt);
            Stepped::Wait(PendingWait {
                waiter,
                registration,
                wait_timeout,
            })
        }
        AcquireStep::Wait {
            waiter,
            registration,
            wait_timeout,
        } => Stepped::Wait(PendingWait {
            waiter,
            registration,
            wait_timeout,
        }),
    })
}

/// Waits, then steps again, until a step reserves a session or fails.
async fn acquire_after_wait<Reservation: PoolReservation>(
    state: Rc<RefCell<PoolState>>,
    cancellation: CancellationToken,
    started: Instant,
    mut wait: PendingWait,
) -> Result<Reservation, PoolError> {
    let mut started = Some(started);
    loop {
        let PendingWait {
            waiter,
            registration,
            wait_timeout,
        } = wait;
        wait_for_wakeup(waiter, registration, wait_timeout, cancellation.clone()).await?;
        if cancellation.is_cancellation_requested() {
            return Err(PoolError::Cancelled);
        }
        wait = match acquisition_step(&state, &mut started, true)? {
            Stepped::Reserved(reservation) => return Ok(reservation),
            Stepped::Wait(wait) => wait,
        };
    }
}

#[cfg(test)]
thread_local! {
    /// This thread's `wait_for_wakeup` calls: acquisition steps that waited.
    static WAITS_FOR_WAKEUP: Cell<u64> = const { Cell::new(0) };
}

/// Takes this thread's count of `wait_for_wakeup` calls, resetting it.
#[cfg(test)]
pub(crate) fn take_waits_for_wakeup_for_test() -> u64 {
    WAITS_FOR_WAKEUP.replace(0)
}

async fn wait_for_wakeup(
    waiter: Rc<Waiter>,
    registration: WaiterRegistration,
    wait_timeout: Duration,
    cancellation: CancellationToken,
) -> Result<(), PoolError> {
    #[cfg(test)]
    WAITS_FOR_WAKEUP.set(WAITS_FOR_WAKEUP.get() + 1);
    let status = waiter.baton.timed_wait(wait_timeout, cancellation).await;
    drop(registration);
    if let Some(error) = waiter.error.borrow_mut().take() {
        return Err(error);
    }
    match status {
        WaitStatus::Signalled => Ok(()),
        WaitStatus::TimedOut => Err(PoolError::Timeout),
        WaitStatus::Cancelled | WaitStatus::NotReady => Err(PoolError::Cancelled),
    }
}

fn start_connection(state: &Rc<RefCell<PoolState>>, attempt: ConnectionAttempt) {
    #[cfg(test)]
    {
        let attempts = &state.borrow().connection_attempts;
        attempts.set(attempts.get() + 1);
    }
    notify_pending_observer(attempt.guard.observer.clone(), 1);
    let weak_state = Rc::downgrade(state);
    let tasks = Rc::clone(&state.borrow().tasks);
    let observing_tasks = Rc::clone(&tasks);
    let result = spawn_pool_task(&tasks, move |cancellation| async move {
        let mut attempt = attempt;
        let completion =
            run_connection_attempt(weak_state, &mut attempt, cancellation, &observing_tasks).await;
        let _owned = attempt
            .ownership
            .take()
            .expect("attempt retains its ownership guard until handoff");
        if catch_callback(|| drop(attempt)).is_err() {
            observing_tasks.fail(PoolError::CleanupPanicked);
        }
        if let Some(completion) = completion {
            completion.observe(&observing_tasks).await;
        }
    });
    if result.is_err()
        && let Some(state) = Rc::downgrade(state).upgrade()
    {
        let waiters = state.borrow_mut().take_all_waiters();
        cancel_waiters(waiters, PoolError::Draining);
    }
}

async fn run_connection_attempt(
    state: Weak<RefCell<PoolState>>,
    attempt: &mut ConnectionAttempt,
    cancellation: CancellationToken,
    tasks: &PoolTasks,
) -> Option<SessionCompletion> {
    let ConnectionAttempt {
        context,
        factory,
        ownership,
        guard,
    } = attempt;
    let ownership = &ownership
        .as_ref()
        .expect("admitted attempt owns its reserved slot")
        .0;
    let mut last_error = None;
    let mut attempts = 0;
    loop {
        if cancellation.is_cancellation_requested() {
            return None;
        }
        let pool = state.upgrade()?;
        let Some((timeout, config)) = ({
            let pool = pool.borrow();
            if ensure_pool_open(&pool).is_err()
                || pool.waiters.is_empty()
                || attempts >= pool.pool_params.max_connection_attempts
            {
                None
            } else {
                Some((
                    pool.pool_params.connect_timeout,
                    match &pool.backend {
                        PoolBackend::Native {
                            connection,
                            session,
                            ..
                        } => Some(DialConfig::Native(connection.clone(), session.clone())),
                        PoolBackend::Owned {
                            connection,
                            session,
                            ..
                        } => Some(DialConfig::Owned(connection.clone(), session.clone())),
                        PoolBackend::Attached { .. } => None,
                    },
                ))
            }
        }) else {
            break;
        };
        attempts = attempts.saturating_add(1);
        let mut connecting = match (&*factory, config) {
            (
                ConnectionFactory::Native { address, connector },
                Some(DialConfig::Native(connection, session)),
            ) => Dial::Uniplex(Box::pin(NativeConnectFuture {
                future: connector.connect(
                    context.clone(),
                    address.clone(),
                    timeout,
                    connection,
                    session,
                    cancellation.clone(),
                ),
            })),
            (
                ConnectionFactory::Owned { address },
                Some(DialConfig::Owned(connection, session)),
            ) => Dial::Owned(OwnedClientSession::connect(
                context,
                address.clone(),
                timeout,
                connection,
                session,
                cancellation.clone(),
            )),
            (ConnectionFactory::Attached { factory, .. }, _) => {
                Dial::Uniplex(factory.create(context.clone(), timeout, cancellation.clone()))
            }
            _ => unreachable!("a native or owned backend retains its dial configuration"),
        };
        drop(pool);
        let outcome = (&mut connecting).await;
        if catch_callback(|| drop(connecting)).is_err() {
            tasks.fail(PoolError::CleanupPanicked);
        }
        match outcome {
            Ok(connected) => {
                if let ConnectionFactory::Attached { security, .. } = &*factory {
                    if connected.worker_id() != context.worker_id() {
                        last_error = Some(PoolError::WrongWorker);
                        continue;
                    }
                    if connected.is_bound_to_pool() || connected.active_transactions() != 0 {
                        last_error = Some(PoolError::InvalidSessionState);
                        continue;
                    }
                    let invalid = if connected.is_finished() || connected.is_draining() {
                        Some(PoolError::InvalidSessionState)
                    } else if connected.connection_context().security() != *security {
                        Some(PoolError::SecurityMismatch)
                    } else {
                        None
                    };
                    if let Some(error) = invalid {
                        let completion = SessionCompletion::new(&connected, Rc::clone(ownership));
                        drain_sessions(vec![connected]);
                        completion.observe(tasks).await;
                        last_error = Some(error);
                        continue;
                    }
                }
                let completion = SessionCompletion::new(&connected, Rc::clone(ownership));
                let Some(pool) = state.upgrade() else {
                    drain_sessions(vec![connected]);
                    return Some(completion);
                };
                if cancellation.is_cancellation_requested()
                    || ensure_pool_open(&pool.borrow()).is_err()
                {
                    drain_sessions(vec![connected]);
                    return Some(completion);
                }
                let unsettled = pool.borrow().index.unsettled.clone();
                connected.bind_pool(Rc::downgrade(&pool), unsettled);
                let wake_count = connected.available_transactions();
                let max_age = sampled_max_age(pool.borrow().pool_params.max_age);
                pool.borrow_mut().sessions.push_back(SessionEntry::new(
                    connected,
                    max_age,
                    Some(Rc::clone(ownership)),
                ));
                notify_session_event(&pool, context.worker_id(), None, false, wake_count, false);
                if let Some(credit) = &guard.credit {
                    let credited = {
                        let mut state = pool.borrow_mut();
                        state.observed_capacity =
                            Some(u32::try_from(wake_count).unwrap_or(u32::MAX));
                        // Credited waiters the session cannot serve go back through the gate.
                        state.take_credited_waiters(credit)
                    };
                    signal_waiters(credited);
                }
                guard.release(false);
                return Some(completion);
            }
            Err(source) => {
                last_error = Some(if source.external_cause().is_some() {
                    PoolError::AttachedConnectFailed { source }
                } else {
                    PoolError::ConnectFailed {
                        source: source.into_parts().0,
                    }
                });
            }
        }
    }
    if cancellation.is_cancellation_requested() {
        return None;
    }
    let Some(error) = last_error else {
        guard.release(false);
        return None;
    };
    let Some(pool) = state.upgrade() else {
        guard.release(false);
        return None;
    };
    let waiters = {
        let mut state = pool.borrow_mut();
        if state.sessions.is_empty() && state.connects_in_progress == 1 {
            state.take_all_waiters()
        } else {
            Vec::new()
        }
    };
    cancel_waiters(waiters, error);
    if guard.credit.is_some() {
        // Frozen `addNewConnection` signals no waiter after a failed connect.
        ownership.wake_on_release.set(false);
    }
    guard.release(false);
    None
}

/// `started` anchors the acquisition's deadline; a first step without one
/// anchors it when it finds no session.
fn next_step(
    state: &Rc<RefCell<PoolState>>,
    started: &mut Option<Instant>,
    woken: bool,
) -> Result<AcquireStep, PoolError> {
    let mut expired = RetiredSessions::default();
    let mut idle_expired = RetiredSessions::default();
    let mut relayed = None;
    let step = (|| {
        let mut pool = state.borrow_mut();
        ensure_pool_open(&pool)?;
        let admitting = pool.admits_reservations();
        let idle_timeout = pool.pool_params.idle_timeout;
        let idle_sweep = pool.pool_params.idle_sweep;
        let now = (!idle_timeout.is_zero()).then(Instant::now);
        // At expiry, every expired session is retired here; on an interval, selection
        // below retires them as hyper-util's checkout does.
        let expired_at = now.filter(|_| idle_sweep == IdleSweep::AtExpiry);
        let checkout_at = now.filter(|_| idle_sweep == IdleSweep::Interval);
        // A settled index selects as the passes below would, without them (S6.9).
        let selected = if admitting && pool.index_ready(now) {
            pool.select_indexed(checkout_at, &mut idle_expired)
        } else {
            pool.unsettle_index();
            pool.count_scan();
            pool.sessions
                .retain(|entry| !entry.session.is_finished() && !entry.session.is_draining());
            let bounded = pool.ownership_bound().is_some();
            while let Some(slot) = {
                pool.count_scan();
                pool.sessions.find(|entry| {
                    (entry.max_age != Duration::ZERO
                        && admitting
                        && entry.session.supports_more_transactions()
                        && entry.session.age() >= entry.max_age)
                        || expired_at
                            .is_some_and(|now| entry.idle_expired(now, idle_timeout, idle_sweep))
                })
            } {
                let next = pool.sessions.next(slot);
                let entry = pool.sessions.remove(slot);
                if bounded
                    && pool.full()
                    && !pool
                        .sessions
                        .iter()
                        .any(|entry| entry.session.supports_more_transactions())
                {
                    // The last selectable session serves on while the gate refuses its replacement.
                    pool.sessions.insert_before(next, entry);
                    break;
                }
                if expired_at.is_some_and(|now| entry.idle_expired(now, idle_timeout, idle_sweep)) {
                    // As `take_idle_expired`: no drain may let a callback reserve it.
                    entry.session.close_admission();
                    idle_expired.push(entry.session);
                } else {
                    expired.push(entry.session);
                }
            }
            if !admitting {
                None
            } else if pool.selection == OwnedSessionSelection::LeastLoaded {
                pool.select_least_loaded(checkout_at, &mut idle_expired)
            } else {
                let returning = pool.selects_returning();
                pool.count_scan();
                pool.sessions
                    .find(|entry| PoolState::takes_busy(entry, returning))
                    .or_else(|| pool.select_idle_session(checkout_at, &mut idle_expired))
            }
        };
        Ok(if let Some(slot) = selected {
            let session = pool.sessions.get(slot).session.clone();
            AcquireStep::Session {
                session,
                retired: RetiredSessions::default(),
            }
        } else {
            let wait_timeout = pool.pool_params.connect_timeout;
            let started = *started.get_or_insert_with(Instant::now);
            if (!pool.backend.is_attached() || !wait_timeout.is_zero())
                && started.elapsed() > wait_timeout
            {
                return Err(PoolError::Timeout);
            }
            let at_connection_capacity = pool.full();
            if pool.established_sessions_at_limit()
                && pool.waiters.len() >= usize::from(pool.pool_params.max_waiters)
            {
                return Err(PoolError::MaxWaiters);
            }
            let absorbed = if at_connection_capacity {
                None
            } else {
                pool.absorb_waiter()
            };
            if woken && absorbed.is_some() {
                // The wake left the dial gate open; pass it to an uncovered waiter.
                relayed = pool.take_uncovered_waiter();
            }
            let waits = at_connection_capacity || absorbed.is_some();
            // An owned pool's acquisition that would dial waits instead for a
            // session whose driver returns a slot at its next turn (`DIV-046`).
            let claim = if waits || !admitting || !matches!(pool.backend, PoolBackend::Owned { .. })
            {
                None
            } else {
                #[cfg(test)]
                pool.claim_scans.set(pool.claim_scans.get() + 1);
                pool.sessions
                    .iter()
                    .find(|entry| entry.session.claimable())
                    .map(|entry| entry.session.clone())
            };
            let credit = if !waits && pool.pending_connect_credit() > 1 {
                let credit = Rc::new(ConnectCredit {
                    waiters: Cell::new(1),
                });
                pool.connect_credits.push(Rc::clone(&credit));
                Some(credit)
            } else {
                absorbed
            };
            let waiter = Rc::new(Waiter {
                id: pool.next_waiter_id,
                baton: Rc::new(CancellableBaton::new()),
                error: RefCell::new(None),
                credit: credit.clone(),
            });
            pool.next_waiter_id = pool.next_waiter_id.wrapping_add(1);
            pool.waiters.push_back(Rc::clone(&waiter));
            if let Some(session) = &claim {
                session.claim(waiter.id);
            }
            let claimed = claim.is_some();
            let registration = WaiterRegistration {
                state: Rc::downgrade(state),
                id: waiter.id,
                credit: credit.clone(),
                claim,
            };
            if waits || claimed {
                AcquireStep::Wait {
                    registration,
                    waiter,
                    wait_timeout,
                }
            } else {
                pool.connects_in_progress = pool.connects_in_progress.saturating_add(1);
                let ownership = SessionOwnership::reserve(state, &mut pool);
                let factory = match &pool.backend {
                    PoolBackend::Native {
                        address, connector, ..
                    } => ConnectionFactory::Native {
                        address: address.clone(),
                        connector: Rc::clone(connector),
                    },
                    PoolBackend::Attached {
                        security, factory, ..
                    } => ConnectionFactory::Attached {
                        security: *security,
                        factory: Rc::clone(
                            factory
                                .as_ref()
                                .expect("open attached pool retains its factory"),
                        ),
                    },
                    PoolBackend::Owned { address, .. } => ConnectionFactory::Owned {
                        address: address.clone(),
                    },
                };
                AcquireStep::StartAndWait {
                    attempt: ConnectionAttempt {
                        context: pool.context.clone(),
                        factory,
                        ownership: Some(ownership),
                        guard: ConnectingGuard {
                            state: Some(Rc::downgrade(state)),
                            observer: pool.observer.clone(),
                            credit,
                        },
                    },
                    registration,
                    waiter,
                    wait_timeout,
                }
            }
        })
    })();
    signal_waiters(relayed.into_iter().collect());
    let step = match step {
        Ok(AcquireStep::Session { session, .. }) => Ok(AcquireStep::Session {
            session,
            retired: idle_expired,
        }),
        step => {
            expired.append(&mut idle_expired);
            step
        }
    };
    expired.drain_propagating_panics();
    step
}

fn sampled_max_age(maximum: Duration) -> Duration {
    if maximum == Duration::ZERO {
        return Duration::ZERO;
    }
    let mut value = NEXT_MAX_AGE_JITTER.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    let permille = 700_u128 + u128::from(value % 601);
    let nanos = maximum.as_nanos().saturating_mul(permille) / 1000;
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PoolKey {
    address: Box<str>,
    port: u16,
    secure: bool,
    options_key: Option<usize>,
}

pub struct ClientConnectionCache {
    context: WorkerContext,
    pools: Rc<RefCell<BTreeMap<PoolKey, Rc<HttpSessionPool>>>>,
    lru: Rc<RefCell<VecDeque<PoolKey>>>,
    maximum_pools: usize,
    pool_params: Rc<RefCell<PoolParams>>,
    connection_params: Rc<RefCell<Option<ConnectionParams>>>,
    session_params: Rc<RefCell<SessionParams>>,
    resolver: Rc<RefCell<Option<Rc<dyn DnsResolver>>>>,
    resolver_generation: Rc<RefCell<CancellationSource>>,
    draining: Rc<Cell<bool>>,
    payloads: Rc<CachePayloads>,
    session_store: Rc<RefCell<Option<TlsSessionStore>>>,
    cert_verify_log: Rc<RefCell<Option<CertVerifyLogFn>>>,
}

struct CachePayloads {
    connection_params: Rc<RefCell<Option<ConnectionParams>>>,
    session_params: Rc<RefCell<SessionParams>>,
    resolver: Rc<RefCell<Option<Rc<dyn DnsResolver>>>>,
    resolver_generation: Rc<RefCell<CancellationSource>>,
    session_store: Rc<RefCell<Option<TlsSessionStore>>>,
    cert_verify_log: Rc<RefCell<Option<CertVerifyLogFn>>>,
}

impl ClientConnectionCache {
    pub fn new(context: WorkerContext, maximum_pools: usize) -> Self {
        let resolver_generation = Rc::new(RefCell::new(CancellationSource::new()));
        let payloads = Rc::new(CachePayloads {
            connection_params: Rc::new(RefCell::new(None)),
            session_params: Rc::new(RefCell::new(SessionParams::http_client_defaults(None))),
            resolver: Rc::new(RefCell::new(None)),
            resolver_generation: Rc::clone(&resolver_generation),
            session_store: Rc::new(RefCell::new(Some(TlsSessionStore::in_memory(10_000)))),
            cert_verify_log: Rc::new(RefCell::new(None)),
        });
        let cache = Self {
            context,
            pools: Rc::new(RefCell::new(BTreeMap::new())),
            lru: Rc::new(RefCell::new(VecDeque::new())),
            maximum_pools,
            pool_params: Rc::new(RefCell::new(PoolParams {
                max_connection_attempts: 1,
                connect_timeout: Duration::from_secs(3),
                max_age: Duration::ZERO,
                ..PoolParams::default()
            })),
            connection_params: Rc::clone(&payloads.connection_params),
            session_params: Rc::clone(&payloads.session_params),
            resolver: Rc::clone(&payloads.resolver),
            resolver_generation,
            draining: Rc::new(Cell::new(false)),
            session_store: Rc::clone(&payloads.session_store),
            cert_verify_log: Rc::clone(&payloads.cert_verify_log),
            payloads,
        };
        let pools = Rc::clone(&cache.pools);
        let lru = Rc::clone(&cache.lru);
        let draining = Rc::clone(&cache.draining);
        let payloads = Rc::clone(&cache.payloads);
        let _cleanup = cache.context.register_cleanup(move || {
            drain_cache_state(&pools, &lru, &draining, &payloads);
        });
        cache
    }

    pub fn with_default_limit(context: WorkerContext) -> Self {
        Self::new(context, DEFAULT_MAX_POOLS)
    }

    pub fn maximum_pools(&self) -> usize {
        self.maximum_pools
    }

    pub fn pool_count(&self) -> usize {
        self.pools.borrow().len()
    }

    pub fn set_pool_params(&self, params: PoolParams) -> Result<(), PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        self.pool_params.replace(params);
        Ok(())
    }

    pub fn set_connection_params(&self, params: ConnectionParams) -> Result<(), PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        if matches!(
            &params.transport,
            crate::connector::ClientTransportOptions::Plaintext
        ) {
            return Err(PoolError::InvalidConnectionParams);
        }
        let old = self.connection_params.replace(Some(params));
        drop_caught(old);
        Ok(())
    }

    pub fn set_session_params(&self, params: SessionParams) -> Result<(), PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        let old = self.session_params.replace(params);
        drop_caught(old);
        Ok(())
    }

    pub fn set_dns_resolver(&self, resolver: Option<Rc<dyn DnsResolver>>) -> Result<(), PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        let generation = self.resolver_generation.replace(CancellationSource::new());
        let old = self.resolver.replace(resolver);
        let _ = catch_unwind(AssertUnwindSafe(|| generation.request_cancellation()));
        drop_caught(old);
        Ok(())
    }

    pub fn set_cert_verify_log_fn(&self, log: Option<CertVerifyLogFn>) -> Result<(), PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        let old = self.cert_verify_log.replace(log);
        drop_caught(old);
        Ok(())
    }

    pub fn get_session_with_reservation(
        &self,
        host: impl Into<Box<str>>,
        port: u16,
        secure: bool,
        timeout: Duration,
        server_address: Option<Box<str>>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        self.get_session_with_connection_params(
            host,
            port,
            secure,
            timeout,
            server_address,
            None,
            cancellation,
        )
    }

    pub fn get_session_with_connection_params(
        &self,
        host: impl Into<Box<str>>,
        port: u16,
        secure: bool,
        timeout: Duration,
        server_address: Option<Box<str>>,
        connection: Option<ConnectionParams>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        if let Err(error) = ensure_cache_open(&self.context, &self.draining) {
            return Box::pin(std::future::ready(Err(error)));
        }
        let host = host.into();
        let context = self.context.clone();
        let resolver = self.resolver.borrow().clone();
        let resolver_generation = self.resolver_generation.borrow().token();
        let pools = self.pools.clone();
        let lru = self.lru.clone();
        let configured_pool_params = Rc::clone(&self.pool_params);
        let default_connection = Rc::clone(&self.connection_params);
        let configured_session_params = Rc::clone(&self.session_params);
        let session_store = Rc::clone(&self.session_store);
        let cert_verify_log = Rc::clone(&self.cert_verify_log);
        let maximum_pools = self.maximum_pools;
        let draining = self.draining.clone();
        Box::pin(async move {
            if cancellation.is_cancellation_requested() {
                return Err(PoolError::Cancelled);
            }
            let address = match server_address {
                Some(address) => SocketAddress::new(address.as_bytes(), port),
                None => {
                    let resolution = CoroDnsResolver::resolve_host(
                        &context,
                        host.clone(),
                        timeout,
                        cancellation.clone(),
                        resolver,
                    );
                    let resolved = await_cache_resolution(resolution, resolver_generation)
                        .await
                        .map_err(|source| PoolError::ConnectFailed { source })?;
                    SocketAddress::new(resolved.primary.host(), port)
                }
            };
            if cancellation.is_cancellation_requested() {
                return Err(PoolError::Cancelled);
            }
            if draining.get() || context.handle().state() != WorkerState::Open {
                return Err(PoolError::Draining);
            }
            let pool_params = *configured_pool_params.borrow();
            let options_key = connection.as_ref().map(ConnectionParams::cache_key);
            let default_connection = default_connection.borrow().clone();
            let use_cache_session_store = connection.is_none() && default_connection.is_none();
            let configured_connection = connection.or(default_connection);
            let session_params = configured_session_params.borrow().clone();
            let shared_store = use_cache_session_store
                .then(|| session_store.borrow().clone())
                .flatten();
            let connection = connection_for_destination(
                &host,
                secure,
                configured_connection,
                shared_store.as_ref(),
                cert_verify_log.borrow().clone(),
            );
            let key = PoolKey {
                address: String::from_utf8_lossy(address.host())
                    .into_owned()
                    .into_boxed_str(),
                port,
                secure,
                options_key,
            };
            reap_empty(&pools, &lru, DEFAULT_REAP_SCAN);
            let pool = get_or_insert_pool(
                &context,
                &pools,
                &lru,
                maximum_pools,
                key,
                address,
                pool_params,
                connection,
                session_params,
            );
            pool.get_session_with_reservation(cancellation).await
        })
    }

    pub fn get_session_with_url(
        &self,
        url: impl AsRef<[u8]>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        let url =
            roxygen_core::url::Url::parse(url, false, roxygen_core::url::UrlMode::StrictCompat);
        if !url.is_valid() || !url.has_host() {
            return Box::pin(std::future::ready(Err(PoolError::ConnectFailed {
                source: HttpError::with_message(
                    roxygen_core::error::HttpErrorCode::INVALID_STATE_TRANSITION,
                    b"invalid URL",
                ),
            })));
        }
        self.get_session_with_reservation(
            String::from_utf8_lossy(url.host()).into_owned(),
            url.port(),
            url.is_secure(),
            timeout,
            None,
            cancellation,
        )
    }

    pub fn pool_for_address(
        &self,
        host: impl AsRef<str>,
        address: impl AsRef<str>,
        port: u16,
        secure: bool,
        connection: Option<ConnectionParams>,
    ) -> Result<Rc<HttpSessionPool>, PoolError> {
        ensure_cache_open(&self.context, &self.draining)?;
        let connection_key = connection.as_ref().map(ConnectionParams::cache_key);
        let default_connection = self.connection_params.borrow().clone();
        let use_cache_session_store = connection.is_none() && default_connection.is_none();
        let key = PoolKey {
            address: address.as_ref().into(),
            port,
            secure,
            options_key: connection_key,
        };
        let connection = connection_for_destination(
            host.as_ref(),
            secure,
            connection.or(default_connection),
            use_cache_session_store
                .then(|| self.session_store.borrow().clone())
                .flatten()
                .as_ref(),
            self.cert_verify_log.borrow().clone(),
        );
        Ok(get_or_insert_pool(
            &self.context,
            &self.pools,
            &self.lru,
            self.maximum_pools,
            key,
            SocketAddress::new(address.as_ref(), port),
            *self.pool_params.borrow(),
            connection,
            self.session_params.borrow().clone(),
        ))
    }

    pub fn pool_exists(
        &self,
        address: impl AsRef<str>,
        port: u16,
        secure: bool,
        connection: Option<&ConnectionParams>,
    ) -> bool {
        self.pools.borrow().contains_key(&PoolKey {
            address: address.as_ref().into(),
            port,
            secure,
            options_key: connection.map(ConnectionParams::cache_key),
        })
    }

    pub fn reap_empty_pools(&self, maximum_to_scan: usize) {
        reap_empty(&self.pools, &self.lru, maximum_to_scan);
    }

    pub fn reap_all_empty_pools(&self) {
        self.reap_empty_pools(self.pool_count());
    }

    pub fn drain(&self) -> bool {
        drain_cache_state(&self.pools, &self.lru, &self.draining, &self.payloads)
    }
}

fn drain_cache_state(
    pools: &RefCell<BTreeMap<PoolKey, Rc<HttpSessionPool>>>,
    lru: &RefCell<VecDeque<PoolKey>>,
    draining: &Cell<bool>,
    payloads: &CachePayloads,
) -> bool {
    if draining.replace(true) {
        return false;
    }
    let pools = { std::mem::take(&mut *pools.borrow_mut()) };
    lru.borrow_mut().clear();
    let connection_params = payloads.connection_params.borrow_mut().take();
    let session_params = payloads.session_params.replace(SessionParams::default());
    let resolver = payloads.resolver.borrow_mut().take();
    let resolver_generation = payloads
        .resolver_generation
        .replace(CancellationSource::new());
    let session_store = payloads.session_store.borrow_mut().take();
    let cert_verify_log = payloads.cert_verify_log.borrow_mut().take();
    pools.into_values().for_each(|pool| {
        let _ = catch_unwind(AssertUnwindSafe(|| pool.drain()));
    });
    drop_caught(connection_params);
    drop_caught(session_params);
    drop_caught(resolver);
    let _ = catch_unwind(AssertUnwindSafe(|| {
        resolver_generation.request_cancellation()
    }));
    drop_caught(session_store);
    drop_caught(cert_verify_log);
    true
}

fn ensure_cache_open(context: &WorkerContext, draining: &Cell<bool>) -> Result<(), PoolError> {
    if draining.get() || context.handle().state() != WorkerState::Open {
        return Err(PoolError::Draining);
    }
    Ok(())
}

async fn await_cache_resolution(
    resolution: LocalClientFuture<Result<ResolvedHost, HttpError>>,
    generation: CancellationToken,
) -> Result<ResolvedHost, HttpError> {
    let generation_baton = CancellableBaton::new();
    let generation_wait = generation_baton.wait(generation.clone());
    tokio::pin!(resolution);
    tokio::pin!(generation_wait);
    let result = tokio::select! {
        result = &mut resolution => result,
        status = &mut generation_wait => {
            debug_assert_eq!(status, WaitStatus::Cancelled);
            return Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"cache DNS resolver was replaced",
            ));
        }
    };
    if generation.is_cancellation_requested() {
        return Err(HttpError::with_message(
            HttpErrorCode::DROPPED,
            b"cache DNS resolver was replaced",
        ));
    }
    result
}

impl HttpSessionFactory for ClientConnectionCache {
    fn get_session_with_reservation(
        &self,
        host: Box<str>,
        port: u16,
        secure: bool,
        connect_timeout: Duration,
        server_address: Option<Box<str>>,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<GetSessionResult, PoolError>> {
        ClientConnectionCache::get_session_with_reservation(
            self,
            host,
            port,
            secure,
            connect_timeout,
            server_address,
            cancellation,
        )
    }
}

impl Drop for ClientConnectionCache {
    fn drop(&mut self) {
        let _ = self.drain();
    }
}

fn get_or_insert_pool(
    context: &WorkerContext,
    pools: &RefCell<BTreeMap<PoolKey, Rc<HttpSessionPool>>>,
    lru: &RefCell<VecDeque<PoolKey>>,
    maximum_pools: usize,
    key: PoolKey,
    address: SocketAddress,
    pool_params: PoolParams,
    connection: ConnectionParams,
    session: SessionParams,
) -> Rc<HttpSessionPool> {
    if let Some(pool) = pools.borrow().get(&key).cloned() {
        promote_key(lru, &key);
        return pool;
    }
    while pools.borrow().len() >= maximum_pools.max(1) {
        let Some(evicted) = lru.borrow_mut().pop_back() else {
            break;
        };
        let removed = { pools.borrow_mut().remove(&evicted) };
        if let Some(pool) = removed {
            let _ = pool.drain();
        }
    }
    let pool = Rc::new(HttpSessionPool::new(
        context.clone(),
        address,
        pool_params,
        connection,
        session,
    ));
    pools.borrow_mut().insert(key.clone(), Rc::clone(&pool));
    lru.borrow_mut().push_front(key);
    pool
}

fn connection_for_destination(
    host: &str,
    secure: bool,
    configured: Option<ConnectionParams>,
    session_store: Option<&TlsSessionStore>,
    cert_verify_log: Option<CertVerifyLogFn>,
) -> ConnectionParams {
    let mut connection = configured.unwrap_or_default();
    if connection.cert_verify_log.is_none() {
        connection.cert_verify_log = cert_verify_log;
    }
    let mut connection = connection.for_destination(host, secure);
    if let (true, Some(session_store), crate::connector::ClientTransportOptions::Tls(options)) =
        (secure, session_store, &mut connection.transport)
    {
        options.set_shared_session_store(session_store);
    }
    connection
}

fn configure_connection_params(params: &mut ConnectionParams, pool: PoolParams) {
    params.configure_tls_options();
    if let crate::connector::ClientTransportOptions::Tls(options) = &mut params.transport {
        if !options.has_explicit_session_store() {
            options.set_session_caching(pool.enable_tls_session_caching);
        }
    }
}

fn promote_key(lru: &RefCell<VecDeque<PoolKey>>, key: &PoolKey) {
    let mut lru = lru.borrow_mut();
    lru.retain(|candidate| candidate != key);
    lru.push_front(key.clone());
}

fn reap_empty(
    pools: &RefCell<BTreeMap<PoolKey, Rc<HttpSessionPool>>>,
    lru: &RefCell<VecDeque<PoolKey>>,
    maximum_to_scan: usize,
) {
    let candidates: Vec<_> = lru
        .borrow()
        .iter()
        .rev()
        .take(maximum_to_scan)
        .cloned()
        .collect();
    candidates.into_iter().for_each(|key| {
        let empty = pools.borrow().get(&key).is_some_and(|pool| pool.empty());
        if empty {
            let removed = { pools.borrow_mut().remove(&key) };
            lru.borrow_mut().retain(|candidate| candidate != &key);
            drop_caught(removed);
        }
    });
}
