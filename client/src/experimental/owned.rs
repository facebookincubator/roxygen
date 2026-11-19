/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental owned client sessions (`DIV-046`).
//!
//! [`OwnedClientSession`] runs one client connection on an owned upstream driver,
//! `OwnedH1UpstreamSession` for HTTP/1.1 or `OwnedH2UpstreamSession` for HTTP/2, which polls
//! the codec, the request uploads and the response sources from one future per connection,
//! with no task per request. It takes `ClientSession`'s inputs ([`ConnectionParams`],
//! [`SessionParams`], [`ClientAttachOptions`]) and keeps its lifetime contract: the driver's
//! run is rooted in a worker scope rather than in the handle, the attachment's cancellation
//! cancels the session, worker quiescence closes its admission, and
//! [`OwnedClientSession::wait_closed`] observes the run's outcome. Either closes admission at
//! once, before the run observes it; the attachment's cancellation wins over quiescence and is
//! observed until the run's cleanup completes, and a panic in the quiescence drain cancels the
//! session and fails it `INTERNAL_ERROR`, as for a `ClientSession`, and is counted in the worker's
//! `WorkerPollStats` (`DIV-056`). A panic the driver contains, also counted there, fails only its
//! request (on HTTP/1.1, which cannot drop half a message, it also ends the session), drains the
//! session, or ends the session with `Err(INTERNAL_ERROR)`, which `wait_closed` reports; the
//! worker keeps serving. A reservation's submission
//! resolves at the driver's commitment, or at the first response head, with the response's
//! fresh [`HttpResponseSource`]; submissions and requests report `OwnedRequestProgressExt`
//! progress (`DIV-035`).
//!
//! Refused before any dial, with `INVALID_STATE_TRANSITION` and an error that names the
//! field, instead of ignored:
//!
//! - on HTTP/1.1, session timers: `stream_read_timeout`, `connection_read_timeout` and
//!   `write_timeout` must each be `Some(Duration::ZERO)`, since `None` selects
//!   `UniplexConfig`'s nonzero default;
//! - `lifecycle_observer`, `session_stats`, `header_codec_stats`, and `ping_probes` with a
//!   nonzero interval;
//! - on HTTP/2, an `ENABLE_PUSH` setting other than 0;
//! - TLS: `connect` dials plaintext TCP only, and `attach` refuses `non_abortive_close` on a
//!   TLS transport, as `ClientSession::attach_transport` does.
//!
//! Honored as `ClientSession` honors them: on HTTP/2 `settings`,
//! `max_concurrent_outgoing_streams`, `connection_flow_control`, `header_indexing_strategy`
//! and all three session timers, except that `ENABLE_PUSH=0` joins settings that carry none,
//! where `ClientSession` advertises 1; on HTTP/1.1 `unframed_bodiless_http1_requests`
//! (`DIV-036`); on both, `deliver_ingress_before_abort` (`DIV-033`), and `non_abortive_close`
//! (`DIV-038`) on the plaintext transport. HTTP/1.1 sessions read before committing a
//! request, fail an unstarted request when the peer closes, and batch ready output, as
//! uniplex sessions do (`DIV-035`, `DIV-034`). HTTP/1.1 sessions ignore the HTTP/2-only fields.
//!
//! Dropping every handle leaves the connection open until `close_when_idle`, `cancel`, its
//! peer or its worker's quiescence closes it, where dropping the last `ClientSession`
//! drains it (`DIV-020`): a drain would refuse a submission the driver has not committed yet.
//!
//! An HTTP/1.1 session's driver is rooted with `OwnedH1UpstreamSession::run_pull_through`
//! (`DIV-047`) in its run scope, whose stop check, quiescence drain and idle report are
//! unchanged. Each exchange selects how its response is read:
//! [`OwnedClientReservation::submit`] as on any session, or
//! [`OwnedClientReservation::submit_pull_through`], which resolves to an
//! [`OwnedClientPullThroughBody`] whose reads drive the connection once the request is
//! written. On HTTP/2 a pull-through submission is refused with `INVALID_STATE_TRANSITION`
//! before it reaches the driver, and is not reported unprocessed.
//!
//! [`OwnedHttpSessionPool`] pools owned sessions to one address with `HttpSessionPool`'s
//! scheduler and `PoolParams`, which it shares: the drivers' `capacity_available` and
//! `admission_closed` callbacks, and the idleness a driver publishes at the end of a turn,
//! which the session's run scope reports, feed the pool as a `ClientSession`'s lifecycle
//! events do. Its acquisitions take a selected session on their first poll with no
//! allocation and no clock read for their deadline ([`OwnedSessionAcquisition`]), an
//! HTTP/1.1 session whose slot is returning included while no acquisition waits (`DIV-046`),
//! and one that would dial waits instead for an HTTP/1.1 session whose driver returns its
//! slot at its next turn. An acquisition that dials is handed its dial's session, reserved for
//! it when the dial completes, if it still waits then, as hyper-util sends a request on the
//! connection it dialed for it, which [`OwnedClientReservation::dialed_for_acquisition`]
//! reports. In an HTTP/1.1 pool a dial that fails after its attempts fails it with the dial's
//! error, as hyper-util fails a request whose connect fails; an HTTP/2 pool's fails waiters
//! as `HttpSessionPool`'s does.
//! [`OwnedHttpSessionPool::set_session_selection`] opts into taking the least loaded of the
//! sessions that can take an exchange (`DIV-048`). A reservation
//! taken on a pooled session's handle reaches its pool as one the pool's acquisition made,
//! and the admission the pool closes on a session it retires or drains refuses the handle's
//! reservations, as a `ClientSession`'s draining flag does.

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use roxygen_codec::CodecSetting;
use roxygen_core::body_event_queue::HttpBodySource;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::SocketAddress;
use roxygen_core::stream_source::HttpResponseSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::ContainedPanicScope;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerId;
use roxygen_runtime::worker::WorkerQuiescing;
use roxygen_session::experimental::OwnedRequestProgressExt;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::owned::OwnedUpstreamObserver;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1ExclusiveSubmission;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h1::OwnedH1PullThroughBody;
use roxygen_session::owned_h1::OwnedH1PullThroughSubmission;
use roxygen_session::owned_h1::OwnedH1UpstreamHandle;
use roxygen_session::owned_h1::OwnedH1UpstreamRequest;
use roxygen_session::owned_h1::OwnedH1UpstreamReservation;
use roxygen_session::owned_h1::OwnedH1UpstreamSession;
use roxygen_session::owned_h2::OwnedH2ExclusiveSubmission;
use roxygen_session::owned_h2::OwnedH2Options;
use roxygen_session::owned_h2::OwnedH2UpstreamHandle;
use roxygen_session::owned_h2::OwnedH2UpstreamRequest;
use roxygen_session::owned_h2::OwnedH2UpstreamReservation;
use roxygen_session::owned_h2::OwnedH2UpstreamSession;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::byte_stream::OwnedByteStream;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::LocalClientFuture;
use crate::connector::SessionCompletion;
use crate::connector::SessionCompletionPublisher;
use crate::connector::SessionParams;
use crate::connector::check_attachment_admission;
use crate::connector::client_byte_stream_limits;
use crate::connector::client_error;
use crate::connector::connection_context;
use crate::connector::dial_plaintext;
use crate::connector::http2_session_config;
use crate::connector::non_abortive_close_with_tls;
use crate::connector::uniplex_config_for_session_params;
use crate::connector::wait_session_completion;
use crate::pool::PoolState;
use crate::pool::PooledSession;
use crate::pool::SessionSlot;
use crate::pool::count_pool_stats;
use crate::pool::notify_session_event;
use crate::pool::record_reservation;
use crate::pool::record_returning;

mod pool;

pub use pool::OwnedGetSessionResult;
pub use pool::OwnedHttpSessionPool;
pub use pool::OwnedPoolStats;
pub use pool::OwnedSessionAcquisition;
pub use pool::OwnedSessionSelection;

/// HTTP/2 `SETTINGS_ENABLE_PUSH`.
const ENABLE_PUSH: u16 = 2;

/// A cloneable owner-local handle for one client connection on an owned
/// upstream driver.
#[derive(Clone)]
pub struct OwnedClientSession {
    inner: Rc<SessionInner>,
}

struct SessionInner {
    upstream: Upstream,
    protocol: CodecProtocol,
    connection: Arc<ConnectionContext>,
    completion: SessionCompletion,
    attachment: CancellationToken,
    worker: WorkerHandle,
    worker_id: WorkerId,
    started: Instant,
    pool: PoolBinding,
}

/// A pooled session's link to its [`OwnedHttpSessionPool`] (`DIV-046`).
struct PoolBinding {
    pool: RefCell<Option<Weak<RefCell<PoolState>>>>,
    /// Admission closed: the pool closed it, or the driver reported it.
    draining: Cell<bool>,
    /// No capacity was left when the session was last reserved or reported.
    full: Cell<bool>,
    /// The pool last saw the session idle, or the session is not pooled.
    reported_idle: Cell<bool>,
    /// The session has taken a reservation since it was pooled. Until then the
    /// pool does not count it idle, as a `ClientSession` is not idle before its
    /// first `DeactivateConnection`.
    reserved: Cell<bool>,
    /// The waiter of the acquisition that waits for the slot the driver is
    /// about to return, instead of dialing.
    claimant: Cell<Option<u64>>,
    /// The slot of the session's entry in its pool's list, so that the pool
    /// finds the entry without a search.
    slot: Cell<Option<SessionSlot>>,
    /// Set when the session stops admitting without an event its pool hears,
    /// so that the pool's selection index looks at every session again (S6.9).
    unsettled: RefCell<Option<Rc<Cell<bool>>>>,
}

impl Default for PoolBinding {
    fn default() -> Self {
        Self {
            pool: RefCell::new(None),
            draining: Cell::new(false),
            full: Cell::new(false),
            reported_idle: Cell::new(true),
            reserved: Cell::new(false),
            claimant: Cell::new(None),
            slot: Cell::new(None),
            unsettled: RefCell::new(None),
        }
    }
}

impl SessionInner {
    /// Tells the session's pool, if any, that the session stops admitting without
    /// an event the pool hears, as a cancellation does (S6.9).
    fn unsettle_pool(&self) {
        if let Some(unsettled) = self.pool.unsettled.borrow().as_ref() {
            unsettled.set(true);
        }
    }
}

/// What a pooled session reports to its pool, as a `ClientSession`'s lifecycle
/// events report to an `HttpSessionPool`.
#[derive(Clone, Copy)]
enum PoolEvent {
    /// A released slot left capacity available (`TransactionDetached`,
    /// `OutgoingCapacityReleased`).
    CapacityAvailable,
    /// The driver closed admission (`OutgoingAdmissionClosed`).
    AdmissionClosed,
    /// The driver published the connection idle at the end of a turn
    /// (`DeactivateConnection`).
    Idle,
}

/// The driver's callbacks, which feed the pool's notifications and counters.
struct PoolObserver {
    session: Weak<SessionInner>,
    /// Apart from `session`: once the pool drops a session whose admission closed and its
    /// handles are gone, the driver still reports its streams' resets.
    pool: Weak<RefCell<PoolState>>,
}

impl PoolObserver {
    /// Counts in the pool's stats (`DIV-032`).
    fn count(&self, count: impl FnOnce(&mut OwnedPoolStats)) {
        if let Some(pool) = self.pool.upgrade() {
            count_pool_stats(&pool, count);
        }
    }
}

impl OwnedUpstreamObserver for PoolObserver {
    fn capacity_available(&self) {
        if let Some(inner) = self.session.upgrade() {
            notify_pool(&inner, PoolEvent::CapacityAvailable);
        }
    }

    fn admission_closed(&self) {
        if let Some(inner) = self.session.upgrade() {
            let retired = inner.pool.draining.replace(true);
            let peer_goaway = match &inner.upstream {
                Upstream::Http2(handle) if !retired => handle.peer_goaway(),
                _ => None,
            };
            if retired || peer_goaway.is_some() {
                self.count(|stats| match peer_goaway {
                    None => stats.local_drains += 1,
                    Some(code) => {
                        stats.peer_goaways += 1;
                        if code != HttpErrorCode::NO_ERROR {
                            stats.peer_goaway_errors += 1;
                        }
                    }
                });
            }
            notify_pool(&inner, PoolEvent::AdmissionClosed);
        }
    }

    fn peer_reset(&self, code: HttpErrorCode) {
        self.count(|stats| {
            if code == HttpErrorCode::REFUSED_STREAM {
                stats.refused_streams += 1;
            } else {
                stats.reset_streams += 1;
            }
        });
    }
}

/// Reports `event` to the session's pool, if it is pooled: a session with
/// capacity again wakes as many waiters as it can serve, and one whose
/// admission closed wakes one, which dials its replacement. As a
/// `ClientSession`'s wake count, the capacity omits the worker's admission,
/// which the woken acquisitions check.
fn notify_pool(inner: &Rc<SessionInner>, event: PoolEvent) {
    let Some(pool) = inner.pool.pool.borrow().as_ref().and_then(Weak::upgrade) else {
        return;
    };
    let session = OwnedClientSession {
        inner: Rc::clone(inner),
    };
    let (enqueue_back, wake_count, release_idle) = match event {
        PoolEvent::CapacityAvailable => {
            let available = session.selectable_capacity();
            let was_full = inner.pool.full.replace(available == 0);
            (
                was_full && available != 0,
                usize::try_from(available).unwrap_or(usize::MAX),
                true,
            )
        }
        PoolEvent::AdmissionClosed => (false, 1, false),
        PoolEvent::Idle => {
            let available = session.selectable_capacity();
            inner.pool.full.set(available == 0);
            (
                available != 0,
                usize::try_from(available).unwrap_or(usize::MAX),
                true,
            )
        }
    };
    inner.pool.reported_idle.set(session.is_idle());
    notify_session_event(
        &pool,
        inner.worker_id,
        Some(&PooledSession::Owned(session)),
        enqueue_back,
        wake_count,
        release_idle,
    );
}

/// Lists a pooled HTTP/1.1 session in its pool's selection index once its slot turned
/// returning (`DIV-029`), which no pool event reports. A pool borrowed
/// further up the stack has its index unsettled instead, so that its passes see the session.
fn report_returning(inner: &Rc<SessionInner>) {
    let Some(pool) = inner.pool.pool.borrow().as_ref().and_then(Weak::upgrade) else {
        return;
    };
    let session = OwnedClientSession {
        inner: Rc::clone(inner),
    };
    if session.is_draining() || !session.is_returning() {
        return;
    }
    if !record_returning(&pool, &PooledSession::Owned(session)) {
        inner.unsettle_pool();
    }
}

#[derive(Clone)]
enum Upstream {
    Http1(OwnedH1UpstreamHandle),
    Http2(OwnedH2UpstreamHandle),
}

impl Upstream {
    fn available_capacity(&self) -> u32 {
        match self {
            Self::Http1(handle) => handle.available_capacity(),
            Self::Http2(handle) => handle.available_capacity(),
        }
    }

    fn is_idle(&self) -> bool {
        match self {
            Self::Http1(handle) => handle.is_idle(),
            Self::Http2(handle) => handle.is_idle(),
        }
    }

    /// HTTP/1.1 only (`DIV-029`); an HTTP/2 stream's capacity returns at once.
    fn returning_capacity(&self) -> u32 {
        match self {
            Self::Http1(handle) => handle.returning_capacity(),
            Self::Http2(_) => 0,
        }
    }

    fn close_when_idle(&self) -> Result<bool, HttpError> {
        match self {
            Self::Http1(handle) => handle.close_when_idle(),
            Self::Http2(handle) => handle.close_when_idle(),
        }
    }

    fn cancel(&self) {
        match self {
            Self::Http1(handle) => handle.cancel(),
            Self::Http2(handle) => handle.cancel(),
        }
    }
}

impl OwnedClientSession {
    /// Dials `address` over plaintext TCP with `connection`'s TCP options and starts the
    /// owned driver for `connection.plaintext_protocol`, as `HttpConnector::connect` starts
    /// a `ClientSession`. Parameters the driver cannot honor, and TLS, are refused before
    /// the dial.
    pub fn connect(
        context: &WorkerContext,
        address: SocketAddress,
        timeout: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Self, HttpError>> {
        let context = context.clone();
        Box::pin(async move {
            check_attachment_admission(&context, &cancellation)?;
            refuse_tls(&connection)?;
            let config = DriverConfig::new(connection.plaintext_protocol, &session)?;
            let post_connect_cancellation = cancellation.clone();
            let (transport, metadata) = dial_plaintext(
                &context,
                address,
                client_byte_stream_limits(),
                timeout,
                cancellation,
                connection.tcp_options,
                session.non_abortive_close,
            )
            .await?;
            check_attachment_admission(&context, &post_connect_cancellation)?;
            start(
                &context,
                Rc::new(transport),
                config,
                metadata,
                CancellationToken::never(),
            )
        })
    }

    /// Consumes an established transport on its owner worker, as
    /// `ClientSession::attach_transport` does, and starts the owned driver for
    /// `options.protocol`. Cancelling `options.cancellation` cancels the session.
    pub fn attach(
        worker: &WorkerContext,
        transport: OwnedByteStream,
        options: ClientAttachOptions,
    ) -> Result<Self, HttpError> {
        check_attachment_admission(worker, &options.cancellation)?;
        let mut config = DriverConfig::new(options.protocol, &options.session)?;
        if options.session.non_abortive_close {
            if options.security == TransportSecurity::Tls {
                return Err(non_abortive_close_with_tls());
            }
            transport.enable_non_abortive_close(worker)?;
        }
        let limits = transport.limits(worker)?;
        let metadata = connection_context(
            worker,
            &transport,
            options.security,
            options.upstream_origin,
        )?;
        let io = config.io_mut();
        io.maximum_read_size = io.maximum_read_size.min(limits.maximum_read_size());
        io.read_allocation_size = io.read_allocation_size.min(limits.maximum_read_size());
        io.maximum_write_size = io.maximum_write_size.min(limits.maximum_write_size());
        start(
            worker,
            Rc::new(transport),
            config,
            metadata,
            options.cancellation,
        )
    }

    /// The wire protocol of the session's driver.
    pub fn protocol(&self) -> CodecProtocol {
        self.inner.protocol
    }

    /// Immutable connection metadata that can cross a thread boundary.
    pub fn connection_context(&self) -> &Arc<ConnectionContext> {
        &self.inner.connection
    }

    /// Requests the connection can admit now: 0 once the attachment is
    /// cancelled, the worker stops accepting work or the session's pool closes
    /// its admission; otherwise on HTTP/1.1 1 while its one slot is free or
    /// returning (`DIV-029`) and admission is open, and on HTTP/2 the lower of
    /// its own and its peer's
    /// `MAX_CONCURRENT_STREAMS`, less the reserved and active streams.
    pub fn available_capacity(&self) -> u32 {
        if self.admission_closed() {
            return 0;
        }
        self.inner.upstream.available_capacity()
    }

    /// No reservation, exchange, upload or write is outstanding.
    pub fn is_idle(&self) -> bool {
        self.inner.upstream.is_idle()
    }

    /// The driver's run has completed.
    pub fn is_finished(&self) -> bool {
        match &self.inner.upstream {
            Upstream::Http1(handle) => handle.is_finished(),
            Upstream::Http2(handle) => handle.is_finished(),
        }
    }

    /// Takes one admission slot. Fails with `DROPPED` once the attachment is
    /// cancelled, the worker stops accepting work or the session's pool closes
    /// its admission, as `ClientSession::reserve_request` does, before the run
    /// observes any of them, and otherwise with `REFUSED_STREAM` when no slot
    /// is available. A pooled session's pool records the reservation as one
    /// its acquisition made.
    pub fn reserve_request(&self) -> Result<OwnedClientReservation, HttpError> {
        if self.admission_closed() {
            return Err(admission_closed_error());
        }
        let pool = self
            .inner
            .pool
            .pool
            .borrow()
            .as_ref()
            .and_then(Weak::upgrade);
        match pool {
            Some(pool) => self.reserve_for_pool(&pool),
            None => self.reserve_upstream(),
        }
    }

    /// As `reserve_request` on a session of `pool`, without the worker
    /// admission check, which the pool's acquisition step made.
    pub(crate) fn reserve_pooled(
        &self,
        pool: &Rc<RefCell<PoolState>>,
    ) -> Result<OwnedClientReservation, HttpError> {
        if self.session_admission_closed() {
            return Err(admission_closed_error());
        }
        self.reserve_for_pool(pool)
    }

    /// Reserves a slot and records it with `pool`, as a `ClientSession`'s
    /// `TransactionAttached` reaches its pool: the session is no longer idle,
    /// and moves to the back when it was idle or became full.
    fn reserve_for_pool(
        &self,
        pool: &Rc<RefCell<PoolState>>,
    ) -> Result<OwnedClientReservation, HttpError> {
        let was_idle = self.is_pooled_idle();
        let reservation = self.reserve_upstream()?;
        let full = self.selectable_capacity() == 0;
        self.inner.pool.full.set(full);
        self.inner.pool.reported_idle.set(false);
        self.inner.pool.reserved.set(true);
        record_reservation(
            pool,
            &PooledSession::Owned(self.clone()),
            was_idle,
            was_idle || full,
        );
        Ok(reservation)
    }

    fn reserve_upstream(&self) -> Result<OwnedClientReservation, HttpError> {
        Ok(OwnedClientReservation {
            upstream: match &self.inner.upstream {
                Upstream::Http1(handle) => Reservation::Http1(handle.reserve_request()?),
                Upstream::Http2(handle) => Reservation::Http2(handle.reserve_request()?),
            },
            dialed_for_acquisition: false,
        })
    }

    /// Closes admission and closes the connection once its exchanges retire.
    /// Returns whether this call changed the session's state.
    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        self.inner.upstream.close_when_idle()
    }

    /// Fails the connection with `CORO_CANCELLED`: admission closes, live
    /// exchanges fail and the transport resets.
    pub fn cancel(&self) {
        self.inner.unsettle_pool();
        self.inner.upstream.cancel();
    }

    /// Observes the driver's outcome once its run, I/O cleanup included, has
    /// completed. Dropping this observation changes nothing.
    pub fn wait_closed(&self) -> LocalClientFuture<Result<UniplexRunSummary, HttpError>> {
        wait_session_completion(&self.inner.completion)
    }

    /// As `ClientSession`'s admission check, ahead of the run's next poll.
    fn admission_closed(&self) -> bool {
        self.session_admission_closed() || !worker_admits(&self.inner.worker)
    }

    /// `admission_closed` without the worker admission check: the attachment
    /// is cancelled, or the pool closed the session's admission (as a
    /// `ClientSession`'s draining flag) or saw the driver close it.
    fn session_admission_closed(&self) -> bool {
        self.is_draining() || self.inner.attachment.is_cancellation_requested()
    }

    /// Admission closed for the pool: it closed it, or the driver reported it.
    pub(crate) fn is_draining(&self) -> bool {
        self.inner.pool.draining.get()
    }

    /// Idle and admitting after its first reservation, as a pool counts a
    /// `ClientSession` idle: an acquisition selects a session the pool dialed
    /// as one that can take an exchange, so the dialing acquisition takes it
    /// before the idle checkout can retire it.
    pub(crate) fn is_pooled_idle(&self) -> bool {
        !self.is_draining() && self.inner.pool.reserved.get() && self.is_idle()
    }

    /// An HTTP/1.1 session whose slot is returning: its exchange's consumer
    /// completed it, and the driver's next turn retires it
    /// (`OwnedH1UpstreamHandle::returning_capacity`, `DIV-029`).
    pub(crate) fn is_returning(&self) -> bool {
        self.inner.upstream.returning_capacity() != 0
    }

    /// `available_capacity` without the worker admission check, which the
    /// pool's acquisition step makes once for all of its sessions.
    pub(crate) fn selectable_capacity(&self) -> u32 {
        if self.session_admission_closed() {
            0
        } else {
            self.inner.upstream.available_capacity()
        }
    }

    /// Whether an acquisition may wait for this session instead of dialing: no
    /// other acquisition waits for it, its admission is open, and its driver
    /// returns a slot at its next turn, which the read response's completion
    /// woke (`OwnedH1UpstreamHandle::returning_capacity`).
    pub(crate) fn claimable(&self) -> bool {
        self.inner.pool.claimant.get().is_none()
            && !self.session_admission_closed()
            && self.inner.upstream.returning_capacity() != 0
    }

    /// Records that `waiter`'s acquisition waits for the returning slot.
    pub(crate) fn claim(&self, waiter: u64) {
        self.inner.pool.claimant.set(Some(waiter));
    }

    /// The waiter whose acquisition waits for the returning slot.
    pub(crate) fn claimant(&self) -> Option<u64> {
        self.inner.pool.claimant.get()
    }

    /// Ends `waiter`'s claim, if it still holds it.
    pub(crate) fn release_claim(&self, waiter: u64) {
        if self.inner.pool.claimant.get() == Some(waiter) {
            self.inner.pool.claimant.set(None);
        }
    }

    /// 0 while the session is idle, otherwise 1: an exchange, a reservation, an
    /// upload or a write is outstanding.
    #[inline]
    pub(crate) fn admitted_transactions(&self) -> u32 {
        u32::from(!self.is_idle())
    }

    /// The exchanges in flight that a least-loaded pool compares (`DIV-048`): on HTTP/2 the
    /// streams its driver admitted, reserved or open; on HTTP/1.1, whose one slot is free
    /// whenever the session can take an exchange, `admitted_transactions`, so that an idle
    /// session comes before one whose driver has not settled its free slot, except that a
    /// returning session (`DIV-029`), whose exchange its consumer completed, has none, and so
    /// comes first, as the busy one of equally loaded sessions (`DIV-046`).
    pub(crate) fn in_flight(&self) -> u32 {
        match &self.inner.upstream {
            Upstream::Http1(_) => u32::from(!self.is_idle() && !self.is_returning()),
            Upstream::Http2(handle) => handle.admitted_streams(),
        }
    }

    /// Refuses further pool reservations ahead of `initiate_drain`.
    pub(crate) fn close_admission(&self) {
        self.inner.pool.draining.set(true);
    }

    /// Closes admission and closes the connection once its exchanges retire.
    pub(crate) fn initiate_drain(&self) -> Result<bool, HttpError> {
        self.close_admission();
        self.close_when_idle()
    }

    pub(crate) fn is_same_session(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn age(&self) -> Duration {
        self.inner.started.elapsed()
    }

    /// When the session started, from which its age counts.
    pub(crate) fn started(&self) -> Instant {
        self.inner.started
    }

    pub(crate) fn worker_id(&self) -> WorkerId {
        self.inner.worker_id
    }

    /// Binds the session to its pool, whose notifications the driver's
    /// capacity and admission callbacks then feed.
    pub(crate) fn bind_pool(
        &self,
        pool: Weak<RefCell<PoolState>>,
        unsettled: Option<Rc<Cell<bool>>>,
    ) {
        self.inner.pool.pool.replace(Some(Weak::clone(&pool)));
        self.inner.pool.unsettled.replace(unsettled);
        self.inner.pool.reported_idle.set(self.is_idle());
        let observer = Rc::new(PoolObserver {
            session: Rc::downgrade(&self.inner),
            pool,
        });
        // A finished session refuses the observer; the pool drops it at its next turn.
        let _ = match &self.inner.upstream {
            Upstream::Http1(handle) => handle.set_observer(observer),
            Upstream::Http2(handle) => handle.set_observer(observer),
        };
        if let Upstream::Http1(handle) = &self.inner.upstream {
            let session = Rc::downgrade(&self.inner);
            handle.set_returning_hook(Rc::new(move || {
                if let Some(inner) = session.upgrade() {
                    report_returning(&inner);
                }
            }));
        }
    }

    pub(crate) fn is_bound_to_pool(&self) -> bool {
        self.inner.pool.pool.borrow().is_some()
    }

    /// The slot of the session's entry in its pool's list, while it has one.
    pub(crate) fn pool_slot(&self) -> Option<SessionSlot> {
        self.inner.pool.slot.get()
    }

    pub(crate) fn set_pool_slot(&self, slot: Option<SessionSlot>) {
        self.inner.pool.slot.set(slot);
    }
}

#[cfg(test)]
thread_local! {
    static WORKER_ADMISSION_CHECKS: Cell<u64> = const { Cell::new(0) };
}

/// The worker admission checks owned sessions and pools made on this thread.
#[cfg(test)]
pub(crate) fn worker_admission_checks_for_test() -> u64 {
    WORKER_ADMISSION_CHECKS.with(Cell::get)
}

/// Whether `worker` still accepts new work: an uncontended lock of its
/// command queue.
pub(crate) fn worker_admits(worker: &WorkerHandle) -> bool {
    #[cfg(test)]
    WORKER_ADMISSION_CHECKS.with(|checks| checks.set(checks.get() + 1));
    worker.is_accepting()
}

pub(crate) fn admission_closed_error() -> HttpError {
    client_error(HttpErrorCode::DROPPED, b"client session admission closed")
}

/// Refuses TLS: owned sessions dial plaintext TCP only.
fn refuse_tls(connection: &ConnectionParams) -> Result<(), HttpError> {
    if matches!(connection.transport, ClientTransportOptions::Tls(_)) {
        return Err(refusal(
            "owned client sessions do not support TLS: they dial plaintext TCP",
        ));
    }
    Ok(())
}

/// The driver configuration a session's parameters select.
enum DriverConfig {
    Http1(UniplexConfig),
    Http2(UniplexConfig, Http2SessionConfig),
}

impl DriverConfig {
    /// Refuses, naming the field, what the owned drivers cannot honor.
    fn new(protocol: CodecProtocol, session: &SessionParams) -> Result<Self, HttpError> {
        let (http2, name) = match protocol {
            CodecProtocol::Http11 => (false, "HTTP/1.1"),
            CodecProtocol::Http2 => (true, "HTTP/2"),
            _ => {
                return Err(client_error(
                    HttpErrorCode::CONNECT_ERROR,
                    b"unsupported attachment protocol",
                ));
            }
        };
        let timers = [
            ("stream_read_timeout", session.stream_read_timeout),
            ("connection_read_timeout", session.connection_read_timeout),
            ("write_timeout", session.write_timeout),
        ];
        if !http2
            && let Some((field, _)) = timers
                .iter()
                .find(|(_, timeout)| *timeout != Some(Duration::ZERO))
        {
            return Err(refusal(format!(
                "owned HTTP/1.1 client sessions are untimed: {field} must be Some(Duration::ZERO)"
            )));
        }
        let unsupported = [
            ("lifecycle_observer", session.lifecycle_observer.is_some()),
            ("session_stats", session.session_stats.is_some()),
            ("header_codec_stats", session.header_codec_stats.is_some()),
            (
                "ping_probes",
                session
                    .ping_probes
                    .is_some_and(|probes| !probes.interval.is_zero()),
            ),
        ];
        if let Some((field, _)) = unsupported.iter().find(|(_, refused)| *refused) {
            return Err(refusal(format!(
                "owned {name} client sessions do not support {field}"
            )));
        }
        let io = uniplex_config_for_session_params(session);
        if !http2 {
            return Ok(Self::Http1(io));
        }
        let mut http2 = http2_session_config(session);
        match http2
            .settings
            .iter()
            .find(|setting| setting.identifier == ENABLE_PUSH)
        {
            None => http2.settings.push(CodecSetting {
                identifier: ENABLE_PUSH,
                value: 0,
            }),
            Some(setting) if setting.value == 0 => {}
            Some(_) => {
                return Err(refusal(
                    "owned HTTP/2 client sessions accept no server push: ENABLE_PUSH must be 0",
                ));
            }
        }
        Ok(Self::Http2(io, http2))
    }

    fn io_mut(&mut self) -> &mut UniplexConfig {
        match self {
            Self::Http1(io) | Self::Http2(io, _) => io,
        }
    }
}

fn refusal(message: impl AsRef<[u8]>) -> HttpError {
    client_error(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

/// Starts the driver on its own worker scope, which publishes its outcome.
/// Cancelling `attachment` cancels the session.
fn start(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    config: DriverConfig,
    connection: Arc<ConnectionContext>,
    attachment: CancellationToken,
) -> Result<OwnedClientSession, HttpError> {
    let completion = SessionCompletion::default();
    let scope = RunScope {
        worker: context.clone(),
        attachment: attachment.wake_registration(),
        quiescing: context.quiescing(),
        completed: SessionCompletionPublisher::new(&completion),
        session: Weak::new(),
        aborting: false,
        draining: false,
        failure: None,
    };
    let inner = |upstream, protocol| {
        Rc::new(SessionInner {
            upstream,
            protocol,
            connection,
            completion,
            attachment,
            worker: context.handle().clone(),
            worker_id: context.worker_id(),
            started: Instant::now(),
            pool: PoolBinding::default(),
        })
    };
    let (inner, registration) = match config {
        DriverConfig::Http1(io) => {
            let (session, handle) = OwnedH1UpstreamSession::new(OwnedH1Options {
                context: context.clone(),
                transport,
                io,
                http1: Http1DriverConfig::default(),
            })?;
            let inner = inner(Upstream::Http1(handle), CodecProtocol::Http11);
            let scope = RunScope {
                session: Rc::downgrade(&inner),
                ..scope
            };
            let registration = context
                .register_scope(scope.run(inner.upstream.clone(), session.run_pull_through()));
            (inner, registration)
        }
        DriverConfig::Http2(io, http2) => {
            let (session, handle) = OwnedH2UpstreamSession::new(OwnedH2Options {
                context: context.clone(),
                transport,
                io,
                http2,
            })?;
            let inner = inner(Upstream::Http2(handle), CodecProtocol::Http2);
            let scope = RunScope {
                session: Rc::downgrade(&inner),
                ..scope
            };
            let registration =
                context.register_scope(scope.run(inner.upstream.clone(), session.run()));
            (inner, registration)
        }
    };
    registration
        .map_err(|error| client_error(HttpErrorCode::INVALID_STATE_TRANSITION, error.to_string()))?
        .activate()
        .map_err(|error| client_error(HttpErrorCode::DROPPED, error.to_string()))?
        .detach();
    Ok(OwnedClientSession { inner })
}

/// The session's worker scope, as `ClientSession`'s run owner: the
/// attachment's cancellation cancels the session, and worker quiescence
/// closes its admission, so it closes once its exchanges retire.
struct RunScope {
    worker: WorkerContext,
    attachment: CancellationWakeRegistration,
    quiescing: WorkerQuiescing,
    completed: SessionCompletionPublisher,
    session: Weak<SessionInner>,
    aborting: bool,
    draining: bool,
    failure: Option<HttpError>,
}

impl RunScope {
    /// Runs the driver, then publishes its outcome, which an attachment
    /// cancellation, or a failure to close admission, replaces. As uniplex
    /// polls a `ClientSession`'s run owner, the stop requests are polled
    /// before each poll of the run and once after the run completes, so a
    /// cancellation that the run's final poll requests, the driver's
    /// destruction included, is observed.
    async fn run(
        mut self,
        upstream: Upstream,
        run: impl Future<Output = Result<UniplexRunSummary, HttpError>>,
    ) {
        let mut run = pin!(run);
        let outcome = poll_fn(|context| {
            self.poll_stop(&upstream, context);
            let Poll::Ready(outcome) = run.as_mut().poll(context) else {
                self.report_idle();
                return Poll::Pending;
            };
            self.poll_stop(&upstream, context);
            Poll::Ready(outcome)
        })
        .await;
        self.completed.publish(self.failure.map_or(outcome, Err));
    }

    /// Reports a pooled session that became idle to its pool, as a
    /// `ClientSession`'s `DeactivateConnection` does: the driver publishes
    /// idleness at the end of a quiescent turn, after the callback that released
    /// the last slot, so the pool can retire it and reuse it in order.
    fn report_idle(&self) {
        if let Some(inner) = self.session.upgrade()
            && !inner.pool.reported_idle.get()
            && inner.upstream.is_idle()
        {
            notify_pool(&inner, PoolEvent::Idle);
        }
    }

    /// As `ClientSessionOwner::poll_stop`, the attachment's cancellation wins
    /// over quiescence, which drains no cancelled session.
    fn poll_stop(&mut self, upstream: &Upstream, context: &mut Context<'_>) {
        if !self.aborting && self.attachment.update_waker(context.waker()) {
            self.aborting = true;
            self.draining = true;
            self.failure = Some(client_error(
                HttpErrorCode::CORO_CANCELLED,
                b"attached session cancelled",
            ));
            self.unsettle_pool();
            upstream.cancel();
        }
        if !self.draining && Pin::new(&mut self.quiescing).poll(context).is_ready() {
            self.draining = true;
            if let Some(failure) = drain(&self.worker, upstream) {
                self.fail_drain(failure);
            }
        }
    }

    /// Records the failure of a quiescence drain, which cancelled the session.
    #[cold]
    #[inline(never)]
    fn fail_drain(&mut self, failure: HttpError) {
        self.failure = Some(failure);
        self.unsettle_pool();
    }

    fn unsettle_pool(&self) {
        if let Some(inner) = self.session.upgrade() {
            inner.unsettle_pool();
        }
    }
}

/// Closes admission for worker quiescence, as `ClientSessionOwner::stop` does:
/// a failure, or a panic of a callback the close runs (a refused request's
/// source cleanup), cancels the session, whose run then completes its I/O
/// cleanup, and is returned. The panic is counted at connection scope on
/// `worker` (`DIV-056`).
fn drain(worker: &WorkerContext, upstream: &Upstream) -> Option<HttpError> {
    let failure = match catch_unwind(AssertUnwindSafe(|| upstream.close_when_idle())) {
        Ok(Ok(_)) => return None,
        Ok(Err(error)) => error,
        Err(payload) => {
            worker.record_contained_panic(
                ContainedPanicScope::Connection,
                "session_drain",
                None,
                &*payload,
            );
            discard_panic(payload);
            client_error(
                HttpErrorCode::INTERNAL_ERROR,
                b"session drain callback panicked",
            )
        }
    };
    upstream.cancel();
    Some(failure)
}

/// One admission slot of an [`OwnedClientSession`], returned exactly once if
/// it is not submitted.
pub struct OwnedClientReservation {
    upstream: Reservation,
    dialed_for_acquisition: bool,
}

enum Reservation {
    Http1(OwnedH1UpstreamReservation),
    Http2(OwnedH2UpstreamReservation),
}

impl OwnedClientReservation {
    /// Whether an [`OwnedHttpSessionPool`] dialed this reservation's connection for the
    /// acquisition that returned the reservation, and reserved it when the dial completed
    /// (`DIV-046`). hyper-util's legacy pool calls such a connection new and every connection
    /// its checkout takes reused, one dialed for another request included, and resends a
    /// request refused before it was written only on a reused one. A reservation an
    /// acquisition took on a session it selected, idle, returning or dialed for another
    /// acquisition, or one taken with [`OwnedClientSession::reserve_request`], was not dialed
    /// for its acquisition.
    pub fn dialed_for_acquisition(&self) -> bool {
        self.dialed_for_acquisition
    }

    pub(crate) fn mark_dialed_for_acquisition(&mut self) {
        self.dialed_for_acquisition = true;
    }

    /// Submits an already-read request head and its body source. The
    /// submission resolves at the driver's commitment, with the response's
    /// fresh source.
    pub fn submit(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedClientSubmission {
        OwnedClientSubmission(match self.upstream {
            Reservation::Http1(reservation) => {
                Submission::Http1(reservation.submit_exclusive(headers, source, cancellation))
            }
            Reservation::Http2(reservation) => {
                Submission::Http2(reservation.submit_exclusive(headers, source, cancellation))
            }
        })
    }

    /// As [`submit`](Self::submit), but the submission resolves at the first
    /// response head, or at a response that ends or fails before one, as the
    /// owned upstreams' `submit_with_response_interest` does, which documents
    /// `producer_completion`.
    pub fn submit_with_response_interest(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        producer_completion: Option<IngressCompletion>,
    ) -> OwnedClientSubmission {
        OwnedClientSubmission(match self.upstream {
            Reservation::Http1(reservation) => {
                Submission::Http1(reservation.submit_with_response_interest(
                    headers,
                    source,
                    cancellation,
                    producer_completion,
                ))
            }
            Reservation::Http2(reservation) => {
                Submission::Http2(reservation.submit_with_response_interest(
                    headers,
                    source,
                    cancellation,
                    producer_completion,
                ))
            }
        })
    }

    /// Roxygen-only (`DIV-047`): as [`submit`](Self::submit), but the response's
    /// [`OwnedClientPullThroughBody`] drives the HTTP/1.1 connection from inside its reads
    /// (`OwnedH1UpstreamReservation::submit_pull_through`). On HTTP/2 the request is refused
    /// with `INVALID_STATE_TRANSITION` at once, and the refusal does not report
    /// `request_unprocessed`.
    pub fn submit_pull_through(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedClientPullThroughSubmission {
        OwnedClientPullThroughSubmission(match self.upstream {
            Reservation::Http1(reservation) => PullThroughSubmission::Http1(
                reservation.submit_pull_through(headers, source, cancellation),
            ),
            Reservation::Http2(_) => PullThroughSubmission::Refused(Some(refusal(
                "owned HTTP/2 client sessions have no pull-through exchanges",
            ))),
        })
    }
}

/// A submitted request: a future of its response. Dropping it before it
/// resolves cancels the request, as the owned upstreams' submissions do.
pub struct OwnedClientSubmission(Submission);

enum Submission {
    Http1(OwnedH1ExclusiveSubmission),
    Http2(OwnedH2ExclusiveSubmission),
}

impl OwnedClientSubmission {
    /// Selects `error` for the request; selected before the response is
    /// claimed, it fails the submission.
    pub fn cancel(&self, error: HttpError) {
        match &self.0 {
            Submission::Http1(submission) => submission.cancel(error),
            Submission::Http2(submission) => submission.cancel(error),
        }
    }
}

impl OwnedRequestProgressExt for OwnedClientSubmission {
    fn request_unprocessed(&self) -> bool {
        match &self.0 {
            Submission::Http1(submission) => submission.request_unprocessed(),
            Submission::Http2(submission) => submission.request_unprocessed(),
        }
    }

    fn sequence_number(&self) -> Option<u64> {
        match &self.0 {
            Submission::Http1(submission) => submission.sequence_number(),
            Submission::Http2(submission) => submission.sequence_number(),
        }
    }
}

impl Future for OwnedClientSubmission {
    type Output = Result<OwnedClientResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            Submission::Http1(submission) => Pin::new(submission).poll(context).map(|response| {
                response.map(|response| {
                    OwnedClientResponse::new(
                        response.source,
                        OwnedClientRequest(Request::Http1(response.control)),
                    )
                })
            }),
            Submission::Http2(submission) => Pin::new(submission).poll(context).map(|response| {
                response.map(|response| {
                    OwnedClientResponse::new(
                        response.source,
                        OwnedClientRequest(Request::Http2(response.control)),
                    )
                })
            }),
        }
    }
}

/// A claimed response: its fresh source, its request's control, and how many
/// requests its connection committed before it.
pub struct OwnedClientResponse {
    pub source: HttpResponseSource,
    pub request: OwnedClientRequest,
    pub sequence: u64,
}

impl OwnedClientResponse {
    fn new(source: HttpResponseSource, request: OwnedClientRequest) -> Self {
        let sequence = request
            .sequence_number()
            .expect("should be fixed when the driver commits the request, before its response");
        Self {
            source,
            request,
            sequence,
        }
    }
}

/// A submitted pull-through request (`DIV-047`): a future of its response, which resolves at
/// the driver's commitment. Dropping it before it resolves cancels the request, as dropping an
/// [`OwnedClientSubmission`] does.
pub struct OwnedClientPullThroughSubmission(PullThroughSubmission);

enum PullThroughSubmission {
    Http1(OwnedH1PullThroughSubmission),
    /// Refused before it reached a driver: HTTP/2 has no pull-through exchanges.
    Refused(Option<HttpError>),
}

impl OwnedClientPullThroughSubmission {
    /// Selects `error` for the request; selected before the response is
    /// claimed, it fails the submission.
    pub fn cancel(&self, error: HttpError) {
        if let PullThroughSubmission::Http1(submission) = &self.0 {
            submission.cancel(error);
        }
    }
}

impl OwnedRequestProgressExt for OwnedClientPullThroughSubmission {
    fn request_unprocessed(&self) -> bool {
        match &self.0 {
            PullThroughSubmission::Http1(submission) => submission.request_unprocessed(),
            PullThroughSubmission::Refused(_) => false,
        }
    }

    fn sequence_number(&self) -> Option<u64> {
        match &self.0 {
            PullThroughSubmission::Http1(submission) => submission.sequence_number(),
            PullThroughSubmission::Refused(_) => None,
        }
    }
}

impl Future for OwnedClientPullThroughSubmission {
    type Output = Result<OwnedClientPullThroughResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            PullThroughSubmission::Http1(submission) => {
                Pin::new(submission).poll(context).map(|response| {
                    response.map(|response| {
                        let request = OwnedClientRequest(Request::Http1(response.control));
                        let sequence = request.sequence_number().expect(
                            "should be fixed when the driver commits the request, before its response",
                        );
                        OwnedClientPullThroughResponse {
                            body: OwnedClientPullThroughBody(response.body),
                            request,
                            sequence,
                        }
                    })
                })
            }
            PullThroughSubmission::Refused(error) => Poll::Ready(Err(error
                .take()
                .expect("an owned pull-through submission was polled after it resolved"))),
        }
    }
}

/// A claimed pull-through response (`DIV-047`): its body, its request's control, and how many
/// requests its connection committed before it.
pub struct OwnedClientPullThroughResponse {
    pub body: OwnedClientPullThroughBody,
    pub request: OwnedClientRequest,
    pub sequence: u64,
}

/// A pull-through exchange's response body (`DIV-047`), `OwnedH1PullThroughBody`'s client
/// handle.
pub struct OwnedClientPullThroughBody(OwnedH1PullThroughBody);

impl OwnedClientPullThroughBody {
    /// Reads the next response head, 1xx heads included, as the response's reader does.
    pub fn poll_header(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        self.0.poll_header(context)
    }

    /// Reads the next body event, of at most `maximum` body bytes. It never returns
    /// `HttpBodyEvent::suspend`.
    #[inline]
    pub fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        self.0.poll_body(context, maximum)
    }

    /// Stops the response, as dropping the body does: its read retires before its source is
    /// cleared with `error`, and its connection's drive returns to the session's run.
    pub fn stop(&mut self, error: Option<HttpErrorCode>) {
        self.0.stop(error);
    }
}

/// The control and progress of a committed request.
#[derive(Clone)]
pub struct OwnedClientRequest(Request);

#[derive(Clone)]
enum Request {
    Http1(OwnedH1UpstreamRequest),
    Http2(OwnedH2UpstreamRequest),
}

impl OwnedClientRequest {
    /// The request's admission slot has been released.
    pub fn capacity_released(&self) -> bool {
        match &self.0 {
            Request::Http1(request) => request.capacity_released(),
            Request::Http2(request) => request.capacity_released(),
        }
    }

    /// Selects `error` for the request: its response fails with it. Before
    /// both messages end, HTTP/1.1 closes the connection and HTTP/2 resets
    /// the stream.
    pub fn cancel(&self, error: HttpError) {
        match &self.0 {
            Request::Http1(request) => request.cancel(error),
            Request::Http2(request) => request.cancel(error),
        }
    }
}

impl OwnedRequestProgressExt for OwnedClientRequest {
    fn request_unprocessed(&self) -> bool {
        match &self.0 {
            Request::Http1(request) => request.request_unprocessed(),
            Request::Http2(request) => request.request_unprocessed(),
        }
    }

    fn sequence_number(&self) -> Option<u64> {
        match &self.0 {
            Request::Http1(request) => request.sequence_number(),
            Request::Http2(request) => request.sequence_number(),
        }
    }
}
