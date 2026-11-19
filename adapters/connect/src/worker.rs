/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Protocol-worker state: routes, attached pools, the negative cache, and one task per tunnel.

use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::future::pending;
use std::future::poll_fn;
use std::hash::Hash;
use std::io;
use std::io::IoSlice;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::task::ready;
use std::time::Duration;
use std::time::Instant;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use roxygen_client::connector::ClientAttachOptions;
use roxygen_client::connector::ClientSession;
use roxygen_client::connector::LocalClientFuture;
use roxygen_client::pool::AttachedConnectFailure;
use roxygen_client::pool::AttachedSessionFactoryWithCause;
use roxygen_client::pool::GetSessionResult;
use roxygen_client::pool::HttpSessionPool;
use roxygen_client::pool::PoolError;
use roxygen_client::pool::PoolObserver;
use roxygen_connect::async_transport::HttpConnectAsyncTransport;
use roxygen_connect::experimental::ConnectAbortExt;
use roxygen_connect::experimental::ConnectResponseHeadExt;
use roxygen_connect::experimental::connect_rejection;
use roxygen_connect::experimental::connect_with_reset_on_cancel;
use roxygen_connect::session::ConnectSession;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ids::StreamId;
use roxygen_core::message::SocketAddress;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_http2::hpack::DefaultIndexingStrategy;
use roxygen_http2::hpack::HeaderIndexingStrategy;
use roxygen_http2::hpack::HeaderName;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::task_group::TaskCompletion;
use roxygen_runtime::task_group::TaskGroup;
use roxygen_runtime::task_group::TaskGroupError;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;
use roxygen_session::http2::stats::Http2SessionStats;
use roxygen_session::session::LifecycleEvent;
use roxygen_session::session::LifecycleObserver;
use roxygen_session::session::RequestReservation;
use roxygen_session::shutdown::DrainTrigger;
use roxygen_session::uniplex::UniplexResponse;
use roxygen_transport::byte_stream::ByteStreamLimits;
use roxygen_transport::established::EstablishedIo;
use roxygen_transport::established::adopt_local;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::TransportSecurity;
use crate::error::ConnectError;
use crate::error::ConnectErrorKind;
use crate::error::ConnectPhase;
use crate::error::DialCancelled;
use crate::error::DialTimedOut;
use crate::error::SessionState;
use crate::error::from_native;
use crate::error::from_pool;
use crate::error::response_head;
use crate::options::ConnectOptions;
use crate::options::NATIVE_CONNECT_TIMEOUT;
use crate::relay::Publisher;
use crate::relay::Relay;
use crate::relay::RelayClose;
use crate::relay::Relayed;
use crate::runtime::Connector;
use crate::runtime::DialError;
use crate::runtime::NativeRequest;
use crate::runtime::ProxyIo;
use crate::runtime::credential_header;
use crate::stats::GaugeHold;
use crate::stats::Recorder;
use crate::tunnel::Bridge;
use crate::tunnel::Choice;
use crate::tunnel::Ending;
use crate::tunnel::Established;
use crate::tunnel::PendingTunnel;
use crate::tunnel::Progress;
use crate::tunnel::StreamHold;
use crate::tunnel::TunnelInfo;
use crate::tunnel::Unchosen;

const TRANSPORT_IO_LIMIT: usize = 64 * 1024;

/// An admitted connect on its way to the worker.
pub(crate) struct Command<K> {
    pub(crate) key: K,
    pub(crate) request: NativeRequest,
    pub(crate) connector: Arc<dyn Connector>,
    pub(crate) cancellation: Arc<CancellationSource>,
    pub(crate) admitted: Instant,
    pub(crate) pending: Pending,
}

/// The reply channel, admission permit, and waiter gauge of an admitted connect that has not
/// been replied to yet.
///
/// Fields drop in order, so a dropped connect releases its permit and waiter unit before its
/// closed reply wakes the caller.
pub(crate) struct Pending {
    permit: OwnedSemaphorePermit,
    waiting: Option<GaugeHold>,
    reply: oneshot::Sender<Result<PendingTunnel, ConnectError>>,
    stats: Arc<Recorder>,
}

impl Pending {
    pub(crate) fn new(
        reply: oneshot::Sender<Result<PendingTunnel, ConnectError>>,
        permit: OwnedSemaphorePermit,
        waiting: GaugeHold,
        stats: Arc<Recorder>,
    ) -> Self {
        Self {
            permit,
            waiting: Some(waiting),
            reply,
            stats,
        }
    }

    fn waiting(&mut self, waiting: bool) {
        if waiting {
            self.waiting.get_or_insert_with(|| {
                GaugeHold::new(Arc::clone(&self.stats), |stats| &stats.waiters)
            });
        } else {
            self.waiting = None;
        }
    }

    /// Releases the route use and the admission permit, then delivers the error, so a caller that
    /// retries immediately finds both free.
    fn fail(self, error: ConnectError, route: Option<RouteUse>) {
        let Self {
            permit,
            waiting,
            reply,
            stats,
        } = self;
        match error.kind() {
            ConnectErrorKind::InvalidRequest => stats.invalid_request.add(),
            ConnectErrorKind::Overloaded => stats.overloaded.add(),
            ConnectErrorKind::RouteCapacity => stats.route_capacity.add(),
            ConnectErrorKind::AcquireTimeout => stats.acquire_timeouts.add(),
            ConnectErrorKind::ResponseTimeout => stats.response_timeouts.add(),
            ConnectErrorKind::Rejected => stats.rejected.add(),
            _ => {}
        }
        drop(route);
        drop(waiting);
        drop(permit);
        let _ = reply.send(Err(error));
    }
}

/// A relay's route use, `active_tunnels` count, and admission permit, released in that order and
/// exactly once before its outcome is published, even when its job is dropped.
pub(crate) struct Settlement {
    route: Option<RouteUse>,
    active: GaugeHold,
    progress: Arc<Progress>,
    publisher: Publisher,
}

impl Settlement {
    pub(crate) fn new(
        route: Option<RouteUse>,
        active: GaugeHold,
        progress: Arc<Progress>,
        publisher: Publisher,
    ) -> Self {
        Self {
            route,
            active,
            progress,
            publisher,
        }
    }

    /// Counts how the relay ended.
    pub(crate) fn record(&self, ending: Ending, stats: &Recorder) {
        if self.progress.end(ending) {
            ending(stats).add();
        }
    }

    /// Records what the relay moved, for the outcome to report.
    pub(crate) fn relayed(&mut self, relayed: Relayed) {
        self.publisher.relayed(relayed);
    }

    /// Records how the relay ended. The outcome keeps it even if the settlement is dropped.
    pub(crate) fn ended(
        &mut self,
        close: RelayClose,
        stream_reset_by_peer: bool,
        error: Option<io::Error>,
    ) {
        self.publisher.ended(close, stream_reset_by_peer, error);
    }

    pub(crate) fn settle(self) {
        let Self {
            route,
            active,
            progress,
            publisher,
        } = self;
        drop(route);
        drop(active);
        drop(progress);
        drop(publisher);
    }
}

/// Literal-without-indexing for credentials; the default strategy for every other header.
#[derive(Clone, Copy)]
pub(crate) struct CredentialIndexingStrategy;

impl HeaderIndexingStrategy for CredentialIndexingStrategy {
    fn index_header(&self, name: &HeaderName, value: &[u8], name_exists: bool) -> bool {
        !credential_header(name.as_bytes())
            && DefaultIndexingStrategy.index_header(name, value, name_exists)
    }

    fn clone_box(&self) -> Box<dyn HeaderIndexingStrategy> {
        Box::new(*self)
    }
}

/// Wraps a connector's I/O so that its shutdown fails after `limit`, for example when a TLS
/// `close_notify` goes to a peer that stopped reading. The native transport's close drain keeps
/// the I/O until its shutdown completes or the worker stops, so an unbounded stall would keep
/// every retired connection.
struct BoundedShutdown {
    io: Box<dyn ProxyIo>,
    limit: Duration,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl AsyncRead for BoundedShutdown {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(context, buffer)
    }
}

impl AsyncWrite for BoundedShutdown {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Poll::Ready(result) = Pin::new(&mut this.io).poll_shutdown(context) {
            return Poll::Ready(result);
        }
        let limit = this.limit;
        let deadline = this
            .deadline
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
        ready!(deadline.as_mut().poll(context));
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "proxy connection shutdown timed out",
        )))
    }
}

/// Route keys whose proxy completed TLS without h2, each until its expiry.
pub(crate) struct NegativeCache<K> {
    entries: HashMap<K, Option<Instant>>,
    capacity: usize,
    ttl: Duration,
}

impl<K> NegativeCache<K>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            ttl,
        }
    }

    /// Records `key`; a full map first evicts the entry that expires soonest.
    pub(crate) fn insert(&mut self, key: K, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= self.capacity {
            let soonest = self
                .entries
                .iter()
                .min_by_key(|(_, expiry)| (expiry.is_none(), **expiry))
                .map(|(key, _)| key.clone());
            if let Some(soonest) = soonest {
                self.entries.remove(&soonest);
            }
        }
        self.entries.insert(key, now.checked_add(self.ttl));
    }

    /// Whether `key` is cached and unexpired; an expired entry is removed.
    pub(crate) fn contains(&mut self, key: &K, now: Instant) -> bool {
        match self.entries.get(key) {
            None => false,
            Some(expiry) if expiry.is_none_or(|expiry| expiry > now) => true,
            Some(_) => {
                self.entries.remove(key);
                false
            }
        }
    }

    pub(crate) fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|_, expiry| expiry.is_none_or(|expiry| expiry > now));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Resolves once `token` is cancelled.
pub(crate) async fn cancelled(token: CancellationToken) {
    let mut registration = token.wake_registration();
    poll_fn(move |context| {
        if registration.update_waker(context.waker()) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending().await,
    }
}

/// The tick that evicts idle routes and prunes the negative cache: a quarter of the smaller
/// nonzero of `idle_timeout` and `h2_unsupported_ttl`, within 1 ms and 1 s.
fn maintenance_period(options: &ConnectOptions) -> Duration {
    [options.idle_timeout, options.h2_unsupported_ttl]
        .into_iter()
        .filter(|timeout| !timeout.is_zero())
        .min()
        .map_or(Duration::from_secs(1), |timeout| {
            (timeout / 4).clamp(Duration::from_millis(1), Duration::from_secs(1))
        })
}

/// When a route last held a tunnel, connect, or dial.
struct RouteActivity {
    uses: Cell<usize>,
    pending_dials: Cell<usize>,
    last_used: Cell<Instant>,
}

impl RouteActivity {
    fn idle_for(&self, timeout: Duration) -> bool {
        self.uses.get() == 0
            && self.pending_dials.get() == 0
            && self.last_used.get().elapsed() >= timeout
    }
}

struct PendingDials(Rc<RouteActivity>);

impl PoolObserver for PendingDials {
    fn on_pending_connections_changed(&self, delta: i32) {
        let activity = &self.0;
        let pending = activity
            .pending_dials
            .get()
            .saturating_add_signed(delta as isize);
        activity.pending_dials.set(pending);
        if pending == 0 {
            activity.last_used.set(Instant::now());
        }
    }
}

/// What its session's lifecycle events showed about one committed CONNECT stream.
#[derive(Default)]
pub(crate) struct StreamWatch {
    /// A response head, informational or malformed, arrived for the stream.
    responded: Cell<bool>,
    /// The peer reset the stream with a code other than `REFUSED_STREAM` before the session's
    /// transport failed.
    peer_reset: Cell<bool>,
    /// The peer refused the stream: an RST_STREAM(`REFUSED_STREAM`) on it, or a GOAWAY whose
    /// last-stream-id is below it.
    refused: Cell<bool>,
    /// The stream is closed on the wire, so an abort sends no frame. It returned its outgoing
    /// capacity, the peer reset it, or its session ended.
    closed: Cell<bool>,
    /// The session wrote the upload's END_STREAM (`RequestEnd`), so flow control does not hold
    /// any of the upload.
    upload_ended: Cell<bool>,
    closed_waker: Cell<Option<Waker>>,
}

impl StreamWatch {
    pub(crate) fn closed(&self) -> bool {
        self.closed.get()
    }

    pub(crate) fn upload_ended(&self) -> bool {
        self.upload_ended.get()
    }

    pub(crate) fn peer_reset(&self) -> bool {
        self.peer_reset.get()
    }

    pub(crate) fn poll_closed(&self, context: &mut Context<'_>) -> Poll<()> {
        if self.closed.get() {
            return Poll::Ready(());
        }
        self.closed_waker.replace(Some(context.waker().clone()));
        Poll::Pending
    }

    pub(crate) async fn wait_closed(&self) {
        poll_fn(|context| self.poll_closed(context)).await;
    }

    fn close(&self) {
        self.closed.set(true);
        if let Some(waker) = self.closed_waker.take() {
            waker.wake();
        }
    }
}

/// The adapter's view of one attached session.
struct SessionMonitor {
    connection: OnceCell<Arc<ConnectionContext>>,
    streams: Cell<u64>,
    /// Committed CONNECT streams, each held weakly for its attempt and then its tunnel.
    watched: RefCell<HashMap<StreamId, Weak<StreamWatch>>>,
    transport_failed: Cell<bool>,
    /// A connection-level ingress error arrived: the peer broke HTTP/2 or sent a GOAWAY with an
    /// error code.
    protocol_failed: Cell<bool>,
    drain_counted: Cell<bool>,
    open: Cell<bool>,
    stats: Arc<Recorder>,
    shutting_down: Rc<Cell<bool>>,
}

impl SessionMonitor {
    /// Starts a CONNECT stream; returns whether the session carried an earlier one.
    fn begin_stream(&self) -> bool {
        let earlier = self.streams.get();
        self.streams.set(earlier + 1);
        earlier != 0
    }

    fn watch(&self, stream: StreamId, watch: &Rc<StreamWatch>) {
        let mut watched = self.watched.borrow_mut();
        watched.retain(|_, watch| watch.strong_count() != 0);
        watched.insert(stream, Rc::downgrade(watch));
    }

    fn watched(&self, stream: StreamId) -> Option<Rc<StreamWatch>> {
        self.watched.borrow().get(&stream).and_then(Weak::upgrade)
    }

    fn refuse_above(&self, last_good_stream: StreamId) {
        self.watched
            .borrow()
            .iter()
            .filter(|(stream, _)| **stream > last_good_stream)
            .filter_map(|(_, watch)| watch.upgrade())
            .for_each(|watch| watch.refused.set(true));
    }

    fn close_watched(&self) {
        let watched: Vec<_> = self
            .watched
            .borrow()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        watched.iter().for_each(|watch| watch.close());
    }

    fn close(&self) {
        if self.open.replace(false) {
            self.stats.open_sessions.decrement();
        }
    }
}

impl Drop for SessionMonitor {
    fn drop(&mut self) {
        self.close();
    }
}

struct SessionObserver(Rc<SessionMonitor>);

impl LifecycleObserver for SessionObserver {
    fn on_event(&self, event: LifecycleEvent) {
        let monitor = &self.0;
        match event {
            // Transport EOF and connection-level failures. GOAWAY alone does not close the
            // transport.
            LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError)
            | LifecycleEvent::OutgoingAdmissionClosed
            | LifecycleEvent::Http2IngressEof => monitor.transport_failed.set(true),
            LifecycleEvent::PeerReset { stream, code } => {
                if let Some(watch) = monitor.watched(stream) {
                    if code == HttpErrorCode::REFUSED_STREAM {
                        watch.refused.set(true);
                    } else if !monitor.transport_failed.get() {
                        watch.peer_reset.set(true);
                    }
                    watch.close();
                }
            }
            LifecycleEvent::Http2Goaway {
                last_good_stream, ..
            } => monitor.refuse_above(last_good_stream),
            LifecycleEvent::DrainStarted(
                DrainTrigger::PeerGoaway | DrainTrigger::Local | DrainTrigger::CloseWhenIdle,
            ) if !monitor.shutting_down.get() && !monitor.drain_counted.replace(true) => {
                monitor.stats.session_drains.add();
            }
            LifecycleEvent::Http2IngressMessage { stream } => {
                if let Some(watch) = monitor.watched(stream) {
                    watch.responded.set(true);
                }
            }
            // Only peer bytes, a GOAWAY's error code included, raise ingress errors; the session
            // closes admission before it reports a connection-level one.
            LifecycleEvent::Http2IngressError { stream, .. } => {
                if let Some(watch) = monitor.watched(stream) {
                    watch.responded.set(true);
                } else if stream.is_unspecified() {
                    monitor.protocol_failed.set(true);
                }
            }
            LifecycleEvent::RequestEnd { stream } => {
                if let Some(watch) = monitor.watched(stream) {
                    watch.upload_ended.set(true);
                }
            }
            LifecycleEvent::OutgoingCapacityReleased { stream } => {
                if let Some(watch) = monitor.watched(stream) {
                    watch.close();
                }
            }
            LifecycleEvent::Destroy => {
                monitor.transport_failed.set(true);
                monitor.close();
                monitor.close_watched();
            }
            _ => {}
        }
    }
}

struct SessionStats(Arc<Recorder>);

impl Http2SessionStats for SessionStats {
    fn record_ping_timeout(&self) {
        self.0.ping_timeouts.add();
    }
}

/// A route's session that records which stream each CONNECT commits on. It returns the
/// committed response, abort included, unchanged and without suspending, as
/// `connect_with_reset_on_cancel` requires of a custom session.
struct WatchedSession {
    session: ClientSession,
    monitor: Option<Rc<SessionMonitor>>,
    watch: Rc<StreamWatch>,
}

impl ConnectSession for WatchedSession {
    fn worker_id(&self) -> WorkerId {
        ConnectSession::worker_id(&self.session)
    }

    fn local_address(&self) -> SocketAddress {
        ConnectSession::local_address(&self.session)
    }

    fn peer_address(&self) -> SocketAddress {
        ConnectSession::peer_address(&self.session)
    }

    fn local_endpoint(&self) -> TransportEndpoint {
        ConnectSession::local_endpoint(&self.session)
    }

    fn peer_endpoint(&self) -> TransportEndpoint {
        ConnectSession::peer_endpoint(&self.session)
    }

    fn send_connect_request(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<UniplexResponse, HttpError>> {
        let send =
            ConnectSession::send_connect_request(&self.session, request, reservation, cancellation);
        let monitor = self.monitor.clone();
        let watch = Rc::clone(&self.watch);
        Box::pin(async move {
            let response = send.await?;
            if let Some(monitor) = monitor {
                monitor.watch(response.stream, &watch);
            }
            Ok(response)
        })
    }

    fn initiate_drain(&self) -> Result<bool, HttpError> {
        ConnectSession::initiate_drain(&self.session)
    }
}

/// The monitors of a route's sessions, found by their connection context.
#[derive(Default)]
struct SessionRegistry(RefCell<Vec<Rc<SessionMonitor>>>);

impl SessionRegistry {
    fn register(&self, monitor: Rc<SessionMonitor>) {
        let mut monitors = self.0.borrow_mut();
        monitors.retain(|monitor| monitor.open.get());
        monitors.push(monitor);
    }

    fn find(&self, session: &ClientSession) -> Option<Rc<SessionMonitor>> {
        self.0
            .borrow()
            .iter()
            .find(|monitor| {
                monitor
                    .connection
                    .get()
                    .is_some_and(|connection| Arc::ptr_eq(connection, session.connection_context()))
            })
            .cloned()
    }
}

/// Dials and attaches one route's sessions.
struct Factory<K> {
    key: K,
    connector: Arc<dyn Connector>,
    security: TransportSecurity,
    options: Rc<ConnectOptions>,
    stats: Arc<Recorder>,
    negative: Rc<RefCell<NegativeCache<K>>>,
    sessions: Rc<SessionRegistry>,
    shutting_down: Rc<Cell<bool>>,
    session_cancellation: CancellationToken,
}

impl<K> AttachedSessionFactoryWithCause for Factory<K>
where
    K: Eq + Hash + Clone + 'static,
{
    fn create(
        &self,
        context: WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<ClientSession, AttachedConnectFailure>> {
        self.stats.dial_attempts.add();
        let dial = self.connector.connect();
        let key = self.key.clone();
        let security = self.security;
        let options = Rc::clone(&self.options);
        let stats = Arc::clone(&self.stats);
        let negative = Rc::clone(&self.negative);
        let sessions = Rc::clone(&self.sessions);
        let shutting_down = Rc::clone(&self.shutting_down);
        let session_cancellation = self.session_cancellation.clone();
        Box::pin(async move {
            let dialed = tokio::select! {
                biased;
                () = cancelled(cancellation) => {
                    return Err(AttachedConnectFailure::with_cause(
                        HttpError::with_message(HttpErrorCode::CANCEL, b"proxy dial cancelled"),
                        Arc::new(DialCancelled),
                    ));
                }
                dialed = tokio::time::timeout(timeout, dial) => dialed,
            };
            let failed = |error: AttachedConnectFailure| {
                stats.dial_failures.add();
                Err(error)
            };
            let connected = match dialed {
                Err(_) => {
                    return failed(AttachedConnectFailure::with_cause(
                        HttpError::with_message(
                            HttpErrorCode::READ_TIMEOUT,
                            b"proxy dial timed out",
                        ),
                        Arc::new(DialTimedOut),
                    ));
                }
                Ok(Err(DialError::H2NotNegotiated)) => {
                    stats.h2_not_negotiated.add();
                    negative.borrow_mut().insert(key, Instant::now());
                    return failed(AttachedConnectFailure::with_cause(
                        HttpError::with_message(
                            HttpErrorCode::CONNECT_ERROR,
                            b"proxy did not negotiate HTTP/2",
                        ),
                        Arc::new(DialError::H2NotNegotiated),
                    ));
                }
                Ok(Err(error)) => {
                    return failed(AttachedConnectFailure::with_cause(
                        HttpError::with_message(
                            HttpErrorCode::CONNECT_ERROR,
                            b"proxy connector failed",
                        ),
                        Arc::new(error),
                    ));
                }
                Ok(Ok(connected)) => connected,
            };
            if connected.security != security {
                return failed(
                    HttpError::with_message(
                        HttpErrorCode::CONNECT_ERROR,
                        b"connector security differs from its route",
                    )
                    .into(),
                );
            }
            let attached = ByteStreamLimits::new(TRANSPORT_IO_LIMIT, TRANSPORT_IO_LIMIT, 1)
                .and_then(|limits| {
                    adopt_local(
                        &context,
                        EstablishedIo::new(
                            BoundedShutdown {
                                io: connected.io,
                                limit: options.shutdown_timeout,
                                deadline: None,
                            },
                            connected.local_endpoint,
                            connected.peer_endpoint,
                            None,
                        ),
                        limits,
                    )
                })
                .and_then(|transport| {
                    let monitor = Rc::new(SessionMonitor {
                        connection: OnceCell::new(),
                        streams: Cell::new(0),
                        watched: RefCell::new(HashMap::new()),
                        transport_failed: Cell::new(false),
                        protocol_failed: Cell::new(false),
                        drain_counted: Cell::new(false),
                        open: Cell::new(false),
                        stats: Arc::clone(&stats),
                        shutting_down,
                    });
                    let mut attach = ClientAttachOptions::new(CodecProtocol::Http2, security);
                    attach.session = options.session_params(
                        Rc::new(SessionObserver(Rc::clone(&monitor))),
                        Rc::new(SessionStats(Arc::clone(&stats))),
                    );
                    attach.cancellation = session_cancellation;
                    let session = ClientSession::attach_transport(&context, transport, attach)?;
                    let _ = monitor
                        .connection
                        .set(Arc::clone(session.connection_context()));
                    monitor.open.set(true);
                    stats.open_sessions.increment();
                    stats.sessions_opened.add();
                    sessions.register(monitor);
                    Ok(session)
                });
            attached.or_else(|error| failed(error.into()))
        })
    }
}

/// One route key's pool and the bookkeeping its retirement depends on.
struct Route {
    pool: HttpSessionPool,
    /// The security of the connector that created the route, which its sessions report.
    security: TransportSecurity,
    activity: Rc<RouteActivity>,
    sessions: Rc<SessionRegistry>,
    _live: GaugeHold,
}

/// A connect or tunnel that keeps its route from retiring.
pub(crate) struct RouteUse(Rc<Route>);

impl RouteUse {
    fn new(route: Rc<Route>) -> Self {
        let activity = &route.activity;
        activity.uses.set(activity.uses.get() + 1);
        Self(route)
    }
}

impl Drop for RouteUse {
    fn drop(&mut self) {
        let activity = &self.0.activity;
        activity.uses.set(activity.uses.get() - 1);
        activity.last_used.set(Instant::now());
    }
}

/// Per-tunnel tasks; dropping the group, as when the worker panics, drops every task on the
/// owner thread so their drop guards fail their tunnels.
struct TunnelTasks(TaskGroup);

impl TunnelTasks {
    fn completions(&mut self) -> impl Iterator<Item = TaskCompletion> + '_ {
        std::iter::from_fn(|| self.0.try_join_next())
    }
}

impl Drop for TunnelTasks {
    fn drop(&mut self) {
        self.0.abort_all();
        self.completions().for_each(TaskCompletion::discard);
    }
}

enum Event<K> {
    Stop,
    Command(Command<K>),
    Joined(TaskCompletion),
    Retired(Result<(), PoolError>),
    Tick,
}

struct Worker<K> {
    /// Declared first, so a worker lost to a panic closes admission before its tunnels fail.
    requests: mpsc::Receiver<Command<K>>,
    context: WorkerContext,
    options: Rc<ConnectOptions>,
    stats: Arc<Recorder>,
    routes: HashMap<K, Rc<Route>>,
    retiring: FuturesUnordered<LocalClientFuture<Result<(), PoolError>>>,
    negative: Rc<RefCell<NegativeCache<K>>>,
    tunnels: TunnelTasks,
    shutting_down: Rc<Cell<bool>>,
    sessions: CancellationSource,
    failure: Option<ConnectError>,
}

pub(crate) async fn run<K>(
    context: WorkerContext,
    requests: mpsc::Receiver<Command<K>>,
    options: ConnectOptions,
    stop: CancellationToken,
    stats: Arc<Recorder>,
) -> Result<(), ConnectError>
where
    K: Eq + Hash + Clone + 'static,
{
    let mut maintenance = tokio::time::interval(maintenance_period(&options));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut worker = Worker {
        requests,
        negative: Rc::new(RefCell::new(NegativeCache::new(
            options.max_routes,
            options.h2_unsupported_ttl,
        ))),
        context,
        options: Rc::new(options),
        stats,
        routes: HashMap::new(),
        retiring: FuturesUnordered::new(),
        tunnels: TunnelTasks(TaskGroup::new()),
        shutting_down: Rc::new(Cell::new(false)),
        sessions: CancellationSource::new(),
        failure: None,
    };
    let mut requests_open = true;
    loop {
        let event = tokio::select! {
            biased;
            () = cancelled(stop.clone()) => Event::Stop,
            () = worker.context.quiescing() => Event::Stop,
            // Ahead of the work, so a worker that always has work still evicts and prunes.
            _ = maintenance.tick() => Event::Tick,
            Some(completion) = poll_fn(|context| worker.tunnels.0.poll_join_next(context)),
                if !worker.tunnels.0.is_empty() => Event::Joined(completion),
            Some(retired) = worker.retiring.next(), if !worker.retiring.is_empty() => {
                Event::Retired(retired)
            }
            command = worker.requests.recv(), if requests_open => match command {
                Some(command) => Event::Command(command),
                // Established tunnels outlive the last client handle; only the owner stops the
                // worker.
                None => {
                    requests_open = false;
                    continue;
                }
            },
        };
        match event {
            Event::Stop => break,
            Event::Command(command) => worker.admit(command),
            Event::Joined(completion) => worker.joined(completion),
            Event::Retired(result) => worker.retired(result),
            Event::Tick => worker.maintain(),
        }
    }
    worker.shutdown().await
}

impl<K> Worker<K>
where
    K: Eq + Hash + Clone + 'static,
{
    fn admit(&mut self, command: Command<K>) {
        let Command {
            key,
            request,
            connector,
            cancellation,
            admitted,
            pending,
        } = command;
        if cancellation.is_cancellation_requested() {
            return;
        }
        if self.negative.borrow_mut().contains(&key, Instant::now()) {
            self.stats.negative_cache_hits.add();
            pending.fail(ConnectError::h2_not_negotiated(), None);
            return;
        }
        let route = match self.routes.get(&key) {
            // The route's sessions would carry this request's credentials.
            Some(route) if connector.security() != route.security => {
                pending.fail(
                    ConnectError::invalid_request("connector security differs from its route"),
                    None,
                );
                return;
            }
            Some(route) => Rc::clone(route),
            None => {
                if self.routes.len() + self.retiring.len() >= self.options.max_routes {
                    pending.fail(ConnectError::route_capacity(), None);
                    return;
                }
                match self.create_route(key.clone(), connector) {
                    Ok(route) => {
                        self.routes.insert(key, Rc::clone(&route));
                        route
                    }
                    Err(error) => {
                        pending.fail(from_pool(error).error, None);
                        return;
                    }
                }
            }
        };
        let setup = Setup {
            context: self.context.clone(),
            route: RouteUse::new(route),
            request,
            cancellation,
            admitted,
            pending,
            options: Rc::clone(&self.options),
            stats: Arc::clone(&self.stats),
        };
        // The group closes only at shutdown, after admission stopped. A rejected spawn drops the
        // reply, which reports the connect as closed.
        let _ = self
            .tunnels
            .0
            .spawn_local_tracked(move |_| run_tunnel(setup));
    }

    fn create_route(&self, key: K, connector: Arc<dyn Connector>) -> Result<Rc<Route>, PoolError> {
        let security = connector.security();
        let activity = Rc::new(RouteActivity {
            uses: Cell::new(0),
            pending_dials: Cell::new(0),
            last_used: Cell::new(Instant::now()),
        });
        let sessions = Rc::new(SessionRegistry::default());
        let pool = HttpSessionPool::try_with_attached_factory_with_cause(
            self.context.clone(),
            self.options.pool_params(),
            security,
            Rc::new(Factory {
                key,
                connector,
                security,
                options: Rc::clone(&self.options),
                stats: Arc::clone(&self.stats),
                negative: Rc::clone(&self.negative),
                sessions: Rc::clone(&sessions),
                shutting_down: Rc::clone(&self.shutting_down),
                session_cancellation: self.sessions.token(),
            }),
        )?;
        pool.set_observer(Some(Rc::new(PendingDials(Rc::clone(&activity)))))?;
        Ok(Rc::new(Route {
            pool,
            security,
            activity,
            sessions,
            _live: GaugeHold::new(Arc::clone(&self.stats), |stats| &stats.routes),
        }))
    }

    fn joined(&mut self, completion: TaskCompletion) {
        if completion.result == Err(TaskGroupError::TaskPanicked) {
            self.failure.get_or_insert_with(|| {
                ConnectError::closed(true).with_source("CONNECT tunnel task panicked")
            });
        }
        completion.discard();
    }

    fn retired(&mut self, result: Result<(), PoolError>) {
        match result {
            Ok(()) => {}
            Err(PoolError::SessionJoin { .. }) => self.stats.shutdown_errors.add(),
            Err(error) => {
                self.failure
                    .get_or_insert_with(|| ConnectError::closed(true).with_source(error));
            }
        }
    }

    fn maintain(&mut self) {
        self.negative.borrow_mut().prune(Instant::now());
        if self.options.idle_timeout.is_zero() {
            return;
        }
        let idle: Vec<K> = self
            .routes
            .iter()
            .filter(|(_, route)| route.activity.idle_for(self.options.idle_timeout))
            .map(|(key, _)| key.clone())
            .collect();
        for key in idle {
            if let Some(route) = self.routes.remove(&key) {
                self.retire(route);
            }
        }
    }

    fn retire(&mut self, route: Rc<Route>) {
        let joined = route.pool.drain_and_join();
        self.retiring.push(Box::pin(async move {
            let result = joined.await;
            drop(route);
            result
        }));
    }

    async fn shutdown(mut self) -> Result<(), ConnectError> {
        self.shutting_down.set(true);
        fail_queued(&mut self.requests).await;
        let routes: Vec<Rc<Route>> = self.routes.drain().map(|(_, route)| route).collect();
        routes.into_iter().for_each(|route| self.retire(route));
        let drained = tokio::time::timeout(self.options.shutdown_timeout, self.join_all()).await;
        if drained.is_err() {
            self.tunnels.0.abort_all();
            let completions: Vec<TaskCompletion> = self.tunnels.completions().collect();
            completions
                .into_iter()
                .for_each(|completion| self.joined(completion));
            self.sessions.request_cancellation();
            self.join_all().await;
        }
        self.tunnels.0.close();
        self.failure.take().map_or(Ok(()), Err)
    }

    async fn join_all(&mut self) {
        loop {
            let event = tokio::select! {
                Some(completion) = poll_fn(|context| self.tunnels.0.poll_join_next(context)),
                    if !self.tunnels.0.is_empty() => Event::<K>::Joined(completion),
                Some(retired) = self.retiring.next(), if !self.retiring.is_empty() => {
                    Event::Retired(retired)
                }
                else => return,
            };
            match event {
                Event::Joined(completion) => self.joined(completion),
                Event::Retired(result) => self.retired(result),
                Event::Stop | Event::Command(_) | Event::Tick => {}
            }
        }
    }
}

/// Closes the channel and fails every admitted connect in it, including one whose send raced the
/// close: `recv` ends only once every send permit is released.
async fn fail_queued<K>(requests: &mut mpsc::Receiver<Command<K>>) {
    requests.close();
    while let Some(command) = requests.recv().await {
        command.pending.fail(ConnectError::closed(false), None);
    }
}

/// Everything one connect's task owns.
struct Setup {
    context: WorkerContext,
    route: RouteUse,
    request: NativeRequest,
    cancellation: Arc<CancellationSource>,
    admitted: Instant,
    pending: Pending,
    options: Rc<ConnectOptions>,
    stats: Arc<Recorder>,
}

enum Attempted {
    Established(Established),
    Failed(ConnectError),
    /// The caller dropped its connect, so the reply is not awaited.
    Abandoned,
}

async fn run_tunnel(mut setup: Setup) {
    let attempted = establish(&mut setup).await;
    let Setup {
        context,
        route,
        request,
        cancellation,
        admitted: _,
        pending,
        options,
        stats,
    } = setup;
    // The request's credentials are not kept for the tunnel's lifetime.
    drop((context, request));
    let established = match attempted {
        Attempted::Established(established) => established,
        Attempted::Failed(error) => {
            pending.fail(error, Some(route));
            return;
        }
        Attempted::Abandoned => return,
    };
    let Pending { reply, permit, .. } = pending;
    let (pending, mut unchosen) = Unchosen::new(
        established,
        options.bridge_capacity,
        permit,
        route,
        Arc::clone(&cancellation),
        Arc::clone(&stats),
    );
    if reply.send(Ok(pending)).is_err() {
        return;
    }
    let dropped = cancellation.token();
    match unchosen.choice(dropped.clone()).await {
        None => unchosen.abandon(),
        Some(Choice::Bridge(worker)) => {
            let (hold, route) = unchosen.choose();
            let mut bridge = Bridge::new(hold, worker, options.shutdown_timeout);
            bridge.run(dropped).await;
            drop(bridge);
            drop(route);
        }
        Some(Choice::Relay(request)) => {
            stats.tunnels_relayed.add();
            let (io, outcome) = request.into_parts();
            let (
                StreamHold {
                    transport,
                    watch,
                    progress,
                    stats,
                    active,
                },
                route,
            ) = unchosen.choose();
            let settlement =
                Settlement::new(Some(route), active, progress, Publisher::new(outcome));
            Relay::new(
                transport,
                watch,
                stats,
                settlement,
                options.shutdown_timeout,
            )
            .run(io, dropped)
            .await;
        }
    }
}

/// Acquires a session and establishes the CONNECT, retrying provably unprocessed attempts
/// within the connect's acquire window.
async fn establish(setup: &mut Setup) -> Attempted {
    let Setup {
        context,
        route,
        request,
        cancellation,
        admitted,
        pending,
        options,
        stats,
    } = setup;
    let caller = cancellation.token();
    let deadline = tokio::time::Instant::from_std(*admitted).checked_add(options.acquire_timeout);
    let window_open = || deadline.is_none_or(|deadline| tokio::time::Instant::now() < deadline);
    let mut retries = 0;
    loop {
        pending.waiting(true);
        let acquired = tokio::select! {
            biased;
            () = cancelled(caller.clone()) => return Attempted::Abandoned,
            () = sleep_until(deadline) => return Attempted::Failed(ConnectError::acquire_timeout()),
            acquired = route.0.pool.get_session_with_reservation(caller.clone()) => acquired,
        };
        let classified = match acquired {
            Ok(GetSessionResult {
                reservation,
                session,
            }) => {
                pending.waiting(false);
                let acquire = admitted.elapsed();
                stats.acquire_latency.record(acquire);
                let monitor = route.0.sessions.find(&session);
                let reused_session = monitor
                    .as_ref()
                    .is_some_and(|monitor| monitor.begin_stream());
                let watch = Rc::new(StreamWatch::default());
                let watched = WatchedSession {
                    session: session.clone(),
                    monitor: monitor.clone(),
                    watch: Rc::clone(&watch),
                };
                let committed = Instant::now();
                // The reservation goes to the native connect in the poll that acquired it.
                let mut connecting = connect_with_reset_on_cancel(
                    context,
                    &watched,
                    reservation,
                    &request.authority,
                    NATIVE_CONNECT_TIMEOUT,
                    request.headers.clone(),
                    u32::try_from(options.bridge_capacity).unwrap_or(u32::MAX),
                    caller.clone(),
                );
                let connected = match futures::poll!(connecting.as_mut()) {
                    Poll::Ready(connected) => {
                        connected.map_err(|error| (error, ConnectPhase::BeforeCommit))
                    }
                    Poll::Pending => tokio::select! {
                        biased;
                        () = cancelled(caller.clone()) => return Attempted::Abandoned,
                        () = tokio::time::sleep(options.response_timeout) => {
                            return Attempted::Failed(ConnectError::response_timeout());
                        }
                        connected = &mut connecting => {
                            connected.map_err(|error| (error, ConnectPhase::AwaitingHead))
                        }
                    },
                };
                drop(connecting);
                let (error, phase) = match connected {
                    Ok(mut stream) => {
                        if caller.is_cancellation_requested() {
                            stream.abort();
                            return Attempted::Abandoned;
                        }
                        let response_head_latency = committed.elapsed();
                        stats.response_latency.record(response_head_latency);
                        let converted = stream
                            .take_connect_response()
                            .ok_or(None)
                            .and_then(|head| response_head(&head).map_err(Some));
                        let response = match converted {
                            Ok(response) => response,
                            Err(conversion) => {
                                stream.abort();
                                let error = ConnectError::failed(
                                    "proxy CONNECT response head cannot be represented",
                                );
                                return Attempted::Failed(match conversion {
                                    Some(conversion) => error.with_source(conversion),
                                    None => error,
                                });
                            }
                        };
                        return Attempted::Established(Established {
                            transport: HttpConnectAsyncTransport::new(context, stream),
                            watch,
                            response,
                            info: TunnelInfo {
                                reused_session,
                                retries,
                                acquire,
                                response_head: response_head_latency,
                            },
                        });
                    }
                    Err(failure) => failure,
                };
                if caller.is_cancellation_requested() {
                    return Attempted::Abandoned;
                }
                if connect_rejection(&error).is_some() {
                    stats.response_latency.record(committed.elapsed());
                }
                let transport_failed = session.is_finished()
                    || monitor
                        .as_ref()
                        .is_some_and(|monitor| monitor.transport_failed.get());
                from_native(
                    error,
                    phase,
                    SessionState {
                        transport_failed,
                        reused: reused_session,
                        peer_reset: watch.peer_reset.get(),
                        response_started: watch.responded.get(),
                        protocol_error: monitor
                            .as_ref()
                            .is_some_and(|monitor| monitor.protocol_failed.get()),
                        refused: watch.refused.get(),
                    },
                )
            }
            Err(error) => {
                if caller.is_cancellation_requested() {
                    return Attempted::Abandoned;
                }
                from_pool(error)
            }
        };
        if !classified.retryable || retries >= options.max_unprocessed_retries || !window_open() {
            return Attempted::Failed(classified.error);
        }
        retries += 1;
        stats.retries.add();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;
    use std::time::Instant;

    use futures::future::pending;
    use roxygen_connect::stream::RequestHeaderMap;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::ids::StreamId;
    use roxygen_http2::hpack::HpackEncoder;
    use roxygen_http2::hpack::HpackHeader;
    use roxygen_runtime::cancellation::CancellationSource;
    use roxygen_session::session::LifecycleEvent;
    use roxygen_session::session::LifecycleObserver;
    use tokio::sync::Semaphore;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use super::Command;
    use super::CredentialIndexingStrategy;
    use super::NegativeCache;
    use super::Pending;
    use super::SessionMonitor;
    use super::SessionObserver;
    use super::Settlement;
    use super::StreamWatch;
    use super::fail_queued;
    use super::maintenance_period;
    use crate::error::ConnectError;
    use crate::error::ConnectErrorKind;
    use crate::options::ConnectOptions;
    use crate::relay;
    use crate::relay::Publisher;
    use crate::relay::RelayClose;
    use crate::relay::RelayIo;
    use crate::relay::Relayed;
    use crate::runtime::ConnectedIo;
    use crate::runtime::Connector;
    use crate::runtime::DialError;
    use crate::runtime::NativeRequest;
    use crate::runtime::credential_header;
    use crate::stats::GaugeHold;
    use crate::stats::Recorder;
    use crate::tunnel::Choice;
    use crate::tunnel::Progress;

    struct NeverDials;

    impl Connector for NeverDials {
        fn connect(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<ConnectedIo, DialError>> + Send + 'static>>
        {
            Box::pin(pending())
        }
    }

    #[test]
    fn shutdown_fails_a_connect_whose_send_raced_the_close() {
        let stats = Arc::new(Recorder::default());
        let (commands, mut requests) = mpsc::channel(1);
        // A caller between `try_send`'s permit and its push when the worker closes the channel.
        let in_flight = commands.try_reserve().expect("a free slot");
        let (reply, mut caller) = oneshot::channel();
        let command = Command {
            key: (),
            request: NativeRequest {
                authority: "target.example:443".into(),
                headers: RequestHeaderMap::new(),
            },
            connector: Arc::new(NeverDials),
            cancellation: Arc::new(CancellationSource::new()),
            admitted: Instant::now(),
            pending: Pending::new(
                reply,
                Arc::new(Semaphore::new(1))
                    .try_acquire_owned()
                    .expect("permit"),
                GaugeHold::new(Arc::clone(&stats), |stats| &stats.waiters),
                Arc::clone(&stats),
            ),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let mut queued = Box::pin(fail_queued(&mut requests));
            assert!(
                futures::poll!(queued.as_mut()).is_pending(),
                "the drain waits for the outstanding send",
            );
            in_flight.send(command);
            queued.await;
        });
        let error = caller
            .try_recv()
            .expect("the raced connect is answered during the drain")
            .expect_err("the runtime is closed");
        assert_eq!(
            (error.kind(), error.committed()),
            (ConnectErrorKind::Closed, false),
            "a connect the worker never saw sent nothing",
        );
    }

    /// Records the admission state at the moment the reply wakes the caller.
    struct WakeObserver {
        permits: Arc<Semaphore>,
        stats: Arc<Recorder>,
        seen: Mutex<Option<(usize, u64)>>,
    }

    impl Wake for WakeObserver {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *self.seen.lock().expect("observer lock") = Some((
                self.permits.available_permits(),
                self.stats.snapshot().waiters,
            ));
        }
    }

    /// The admission state the caller observes when `end` replies to its pending connect.
    fn admission_when_the_caller_wakes(end: impl FnOnce(Pending)) -> Option<(usize, u64)> {
        let permits = Arc::new(Semaphore::new(1));
        let stats = Arc::new(Recorder::default());
        let observer = Arc::new(WakeObserver {
            permits: Arc::clone(&permits),
            stats: Arc::clone(&stats),
            seen: Mutex::new(None),
        });
        let (reply, mut receiver) = oneshot::channel();
        let waker = Waker::from(Arc::clone(&observer));
        assert!(
            Pin::new(&mut receiver)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        end(Pending::new(
            reply,
            Arc::clone(&permits).try_acquire_owned().expect("permit"),
            GaugeHold::new(Arc::clone(&stats), |stats| &stats.waiters),
            Arc::clone(&stats),
        ));
        *observer.seen.lock().expect("observer lock")
    }

    #[test]
    fn failed_connect_releases_admission_before_its_reply_wakes_the_caller() {
        assert_eq!(
            admission_when_the_caller_wakes(|pending| {
                pending.fail(ConnectError::route_capacity(), None)
            }),
            Some((1, 0)),
            "the caller wakes with its permit and waiter slot already released",
        );
        assert_eq!(
            admission_when_the_caller_wakes(drop),
            Some((1, 0)),
            "a connect dropped unanswered, as by a task panic, releases them first too",
        );
    }

    /// Records the admission permits and active tunnels at the moment a relay's outcome wakes its
    /// caller.
    struct OutcomeObserver {
        permits: Arc<Semaphore>,
        stats: Arc<Recorder>,
        seen: Mutex<Option<(usize, u64)>>,
    }

    impl Wake for OutcomeObserver {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *self.seen.lock().expect("observer lock") = Some((
                self.permits.available_permits(),
                self.stats.snapshot().active_tunnels,
            ));
        }
    }

    #[test]
    fn relay_releases_admission_before_its_outcome_wakes_the_caller() {
        let settled: [(&str, fn(Settlement)); 2] = [
            ("a published outcome", |mut settlement| {
                settlement.relayed(Relayed {
                    uploaded: 5,
                    downloaded: 7,
                    download_ended: true,
                    peer_ended_download: true,
                });
                settlement.ended(RelayClose::Finished, false, None);
                settlement.settle();
            }),
            ("a relay job dropped without an outcome", drop),
        ];
        for (case, end) in settled {
            let permits = Arc::new(Semaphore::new(1));
            let stats = Arc::new(Recorder::default());
            let observer = Arc::new(OutcomeObserver {
                permits: Arc::clone(&permits),
                stats: Arc::clone(&stats),
                seen: Mutex::new(None),
            });
            let (choice, mut chosen) = oneshot::channel();
            let (application, _peer) = tokio::io::duplex(64);
            let mut relay = Box::pin(relay::start(
                Some(choice),
                RelayIo::Boxed(Box::new(application)),
                Arc::new(CancellationSource::new()),
            ));
            let waker = Waker::from(Arc::clone(&observer));
            assert!(
                relay
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let Ok(Choice::Relay(request)) = chosen.try_recv() else {
                panic!("{case}: relay() delivers its request");
            };
            let (_io, outcome) = request.into_parts();
            let progress = Arc::new(Progress::new(
                Arc::clone(&permits).try_acquire_owned().expect("permit"),
            ));
            end(Settlement::new(
                None,
                GaugeHold::new(Arc::clone(&stats), |stats| &stats.active_tunnels),
                progress,
                Publisher::new(outcome),
            ));
            assert_eq!(
                *observer.seen.lock().expect("observer lock"),
                Some((1, 0)),
                "{case}: the caller wakes with its permit and active count already released",
            );
        }
    }

    fn monitor() -> Rc<SessionMonitor> {
        Rc::new(SessionMonitor {
            connection: Default::default(),
            streams: Default::default(),
            watched: Default::default(),
            transport_failed: Default::default(),
            protocol_failed: Default::default(),
            drain_counted: Default::default(),
            open: Default::default(),
            stats: Arc::new(Recorder::default()),
            shutting_down: Default::default(),
        })
    }

    #[test]
    fn only_a_refusal_of_the_attempts_own_stream_or_above_a_goaway_counts() {
        let monitor = monitor();
        let observer = SessionObserver(Rc::clone(&monitor));
        let watch = |stream| {
            let watch = Rc::new(StreamWatch::default());
            monitor.watch(StreamId::new(stream), &watch);
            watch
        };
        let (sibling, own) = (watch(1), watch(3));
        observer.on_event(LifecycleEvent::PeerReset {
            stream: StreamId::new(1),
            code: HttpErrorCode::REFUSED_STREAM,
        });
        assert!(sibling.refused.get());
        assert!(!own.refused.get(), "a sibling stream's refusal");
        observer.on_event(LifecycleEvent::PeerReset {
            stream: StreamId::new(3),
            code: HttpErrorCode::REFUSED_STREAM,
        });
        assert!(own.refused.get(), "the attempt's own refusal");

        let monitor = self::monitor();
        let observer = SessionObserver(Rc::clone(&monitor));
        let watch = |stream| {
            let watch = Rc::new(StreamWatch::default());
            monitor.watch(StreamId::new(stream), &watch);
            watch
        };
        let (below, at, above) = (watch(1), watch(3), watch(5));
        observer.on_event(LifecycleEvent::Http2Goaway {
            last_good_stream: StreamId::new(3),
            code: HttpErrorCode::ENHANCE_YOUR_CALM,
        });
        assert!(
            !below.refused.get() && !at.refused.get(),
            "a GOAWAY covering a stream does not refuse it"
        );
        assert!(
            above.refused.get(),
            "a stream above the GOAWAY's last-stream-id, whatever its error code"
        );
    }

    #[test]
    fn only_a_peer_reset_of_the_attempts_own_stream_makes_its_failure_final() {
        let monitor = monitor();
        let observer = SessionObserver(Rc::clone(&monitor));
        let reset = |stream, code| LifecycleEvent::PeerReset {
            stream: StreamId::new(stream),
            code,
        };
        let own = Rc::new(StreamWatch::default());
        monitor.watch(StreamId::new(3), &own);
        observer.on_event(reset(1, HttpErrorCode::CANCEL));
        assert!(!own.peer_reset.get(), "a sibling stream's reset");
        observer.on_event(reset(3, HttpErrorCode::REFUSED_STREAM));
        assert!(
            !own.peer_reset.get(),
            "a refusal leaves the stream unprocessed"
        );
        observer.on_event(reset(3, HttpErrorCode::CANCEL));
        assert!(own.peer_reset.get(), "the attempt's own stream reset");

        let late = Rc::new(StreamWatch::default());
        monitor.watch(StreamId::new(5), &late);
        observer.on_event(LifecycleEvent::Http2IngressEof);
        observer.on_event(reset(5, HttpErrorCode::CANCEL));
        assert!(
            !late.peer_reset.get(),
            "a reset after the transport failed is part of the session's failure"
        );
    }

    #[test]
    fn ingress_protocol_errors_end_the_retry_of_their_stream_or_session() {
        let monitor = monitor();
        let observer = SessionObserver(Rc::clone(&monitor));
        let error = |stream| LifecycleEvent::Http2IngressError {
            stream: StreamId::new(stream),
            code: HttpErrorCode::PROTOCOL_ERROR,
        };
        let own = Rc::new(StreamWatch::default());
        monitor.watch(StreamId::new(3), &own);
        observer.on_event(error(1));
        assert!(
            !own.responded.get() && !monitor.protocol_failed.get(),
            "a sibling stream's malformed response"
        );
        observer.on_event(error(3));
        assert!(own.responded.get(), "the attempt's own malformed response");
        assert!(!monitor.protocol_failed.get());
        // A connection error closes admission before the session reports its ingress error.
        observer.on_event(LifecycleEvent::OutgoingAdmissionClosed);
        observer.on_event(error(0));
        assert!(monitor.protocol_failed.get(), "a connection-level error");
    }

    #[test]
    fn maintenance_period_is_a_quarter_of_the_smaller_timeout() {
        let millis = Duration::from_millis;
        for (idle, ttl, expected, case) in [
            (200, 0, 50, "idle timeout only"),
            (0, 400, 100, "negative-cache TTL only"),
            (200, 400, 50, "the smaller idle timeout"),
            (400, 200, 50, "the smaller TTL"),
            (0, 0, 1000, "both disabled"),
            (120_000, 300_000, 1000, "the defaults, at most 1 s"),
            (2, 0, 1, "at least 1 ms"),
        ] {
            let options = ConnectOptions {
                idle_timeout: millis(idle),
                h2_unsupported_ttl: millis(ttl),
                ..Default::default()
            };
            assert_eq!(maintenance_period(&options), millis(expected), "{case}");
        }
    }

    #[test]
    fn credentials_never_enter_the_session_dynamic_table() {
        let mut encoder = HpackEncoder::new(false);
        encoder.set_header_indexing_strategy(Rc::new(CredentialIndexingStrategy));
        let headers = [
            HpackHeader::new(":method", "CONNECT"),
            HpackHeader::new(":authority", "target.example:443"),
            HpackHeader::new("authorization", "origin-credential"),
            HpackHeader::new("proxy-authorization", "proxy-credential"),
            HpackHeader::new("cookie", "request-cookie"),
            HpackHeader::new("set-cookie", "response-cookie"),
            HpackHeader::new("user-agent", "CONNECT-client"),
        ];
        for _ in 0..3 {
            encoder = encoder.clone();
            let encoded = encoder.encode(&headers);
            for header in headers
                .iter()
                .filter(|header| credential_header(header.name.as_bytes()))
            {
                assert!(
                    encoded
                        .windows(header.value.len())
                        .any(|bytes| bytes == &*header.value),
                    "each credential remains a literal on repeated blocks",
                );
            }
            assert!(
                encoder
                    .context()
                    .table()
                    .entries()
                    .all(|header| !credential_header(header.name.as_bytes())),
                "the session must not retain credentials in its HPACK dynamic table",
            );
            assert!(
                encoder
                    .context()
                    .table()
                    .entries()
                    .any(|header| header.name.as_bytes() == b"user-agent"),
                "ordinary headers still use dynamic compression",
            );
        }
    }

    #[test]
    fn negative_cache_map_is_bounded_and_pruned() {
        let start = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut cache = NegativeCache::new(2, ttl);
        cache.insert("a", start);
        cache.insert("b", start + Duration::from_secs(1));
        assert!(cache.contains(&"a", start + Duration::from_secs(9)));
        assert!(
            !cache.contains(&"a", start + ttl),
            "an entry expires at its TTL"
        );
        assert_eq!(cache.len(), 1, "an expired lookup removes the entry");

        cache.insert("a", start + Duration::from_secs(2));
        cache.insert("c", start + Duration::from_secs(3));
        assert_eq!(cache.len(), 2, "the map holds at most its capacity");
        assert!(
            !cache.contains(&"b", start + Duration::from_secs(3)),
            "the entry that expires soonest is replaced",
        );
        assert!(cache.contains(&"a", start + Duration::from_secs(3)));
        assert!(cache.contains(&"c", start + Duration::from_secs(3)));

        cache.insert("a", start + Duration::from_secs(5));
        assert_eq!(cache.len(), 2, "refreshing a key evicts nothing");
        cache.prune(start + Duration::from_secs(13));
        assert_eq!(
            cache.len(),
            1,
            "pruning drops the entry that expired at 13 s"
        );
        assert!(cache.contains(&"a", start + Duration::from_secs(13)));
        cache.prune(start + Duration::from_secs(15));
        assert_eq!(cache.len(), 0);

        let mut disabled = NegativeCache::new(4, Duration::ZERO);
        disabled.insert("a", start);
        assert!(
            !disabled.contains(&"a", start),
            "a zero TTL disables the cache"
        );
    }
}
