//! The experimental owned session pool (`DIV-046`).

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use roxygen_core::error::HttpError;
use roxygen_core::message::SocketAddress;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerId;

use super::DriverConfig;
use super::OwnedClientReservation;
use super::OwnedClientSession;
use super::refuse_tls;
use crate::connector::ConnectionParams;
use crate::connector::LocalClientFuture;
use crate::connector::SessionParams;
use crate::pool::AcquisitionStart;
use crate::pool::HttpSessionPool;
use crate::pool::PoolError;
use crate::pool::PoolParams;
use crate::pool::PoolReservation;
use crate::pool::PoolState;
use crate::pool::PooledSession;
use crate::pool::start_acquisition;

/// A worker-local pool of [`OwnedClientSession`]s to one address, with
/// `HttpSessionPool`'s scheduler and [`PoolParams`]: the same connection,
/// waiter, connect-timeout, acquisition-timeout, maximum-age, idle-timeout,
/// reuse-order and idle-sweep semantics (`DIV-032`), shared with it. An HTTP/1.1 pool keeps its
/// sessions that can take an exchange in a selection index, so that an
/// acquisition that finds one, and a release, make no pass over the pool's
/// sessions however many it holds; the selection is the scheduler's, or, opted into with
/// [`OwnedHttpSessionPool::set_session_selection`], the least loaded session's (`DIV-048`).
pub struct OwnedHttpSessionPool(HttpSessionPool);

/// Which session an [`OwnedHttpSessionPool`]'s acquisition takes among its established
/// sessions that can take an exchange (`DIV-048`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OwnedSessionSelection {
    /// `HttpSessionPool`'s order: the first session in the pool's order that has an exchange
    /// and can take another, else an idle session in `PoolParams::reuse_order` (`DIV-032`), so
    /// that exchanges pack onto the first session with room.
    #[default]
    FirstAvailable,
    /// The session with the fewest exchanges in flight: on HTTP/2 the fewest streams reserved
    /// or open, so idle sessions first; on HTTP/1.1, whose sessions take one exchange, an idle
    /// session before one whose free slot its driver has not settled yet, and a session whose
    /// slot is returning, which has none in flight, before an idle one (`DIV-046`). Equally
    /// loaded sessions are taken in `FirstAvailable`'s order: a busy one first, in the pool's
    /// order, then an idle one in `PoolParams::reuse_order`.
    LeastLoaded,
}

/// A snapshot of an [`OwnedHttpSessionPool`]'s counters (`DIV-032`), which the pool keeps
/// on its worker and updates only on its slow paths: dials, waits, admission closes,
/// session completions and peer stream resets. Every field but `waiters` only grows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct OwnedPoolStats {
    /// Sessions the pool's dials established and added to the pool.
    pub sessions_opened: u64,
    /// Of those, the sessions whose run has completed, cleanly or not.
    pub sessions_closed: u64,
    /// Dial attempts, retries included.
    pub connect_attempts: u64,
    /// Dial attempts that failed.
    pub connect_failures: u64,
    /// Sessions whose admission a peer's GOAWAY closed, counted when it closed, before the
    /// session's streams finish: a `NO_ERROR` GOAWAY, which drains the connection, as well
    /// as one with an error code. A GOAWAY that arrives after admission closed for another
    /// reason is not counted.
    pub peer_goaways: u64,
    /// Of `peer_goaways`, those whose GOAWAY carried a code other than `NO_ERROR`.
    pub peer_goaway_errors: u64,
    /// Sessions whose admission the pool closed to retire them: at `max_age` or
    /// `idle_timeout`, at their release when `max_connections` is zero, or in the pool's
    /// drain.
    pub local_drains: u64,
    /// Streams the peer refused, unprocessed: with an RST_STREAM carrying
    /// `REFUSED_STREAM`, or with a GOAWAY whose last stream precedes them. Counted at the
    /// refusal, also on a session the pool no longer holds, as after a GOAWAY.
    pub refused_streams: u64,
    /// Streams the peer reset with an RST_STREAM carrying another code, counted as
    /// `refused_streams` are; not a `NO_ERROR` reset after the stream's whole response,
    /// which stops only its upload (`DIV-050`).
    pub reset_streams: u64,
    /// Acquisitions waiting now for a session, a stream slot or a dial.
    pub waiters: usize,
    /// The most acquisitions that waited at once.
    pub peak_waiters: usize,
    /// Acquisitions that failed with [`PoolError::Timeout`].
    pub acquisition_timeouts: u64,
    /// Acquisitions refused with [`PoolError::MaxWaiters`].
    pub max_waiters_rejections: u64,
}

/// One reservation on the session an acquisition selected.
pub struct OwnedGetSessionResult {
    pub reservation: OwnedClientReservation,
    pub session: OwnedClientSession,
}

impl PoolReservation for OwnedGetSessionResult {
    fn reserve(state: &Rc<RefCell<PoolState>>, session: PooledSession) -> Result<Self, PoolError> {
        let PooledSession::Owned(session) = session else {
            return Err(PoolError::InvalidSessionState);
        };
        let reservation = session
            .reserve_pooled(state)
            .map_err(|source| PoolError::ReservationFailed { source })?;
        Ok(Self {
            reservation,
            session,
        })
    }
}

/// A pending [`OwnedHttpSessionPool`] acquisition. Its first poll runs the
/// pool's first acquisition step in place: a selected session is reserved and
/// returned with no allocation, no clock read for the acquisition's deadline
/// and one worker admission check, plus another after a drain the step runs
/// (`DIV-046`), and the reservation is recorded on its entry alone. A nonzero
/// `idle_timeout` or `max_age` still reads the clock for the sessions' expiry,
/// as `HttpSessionPool`'s step does. Only an acquisition that waits for a
/// session or a dial continues in a boxed future.
/// While no acquisition waits, an HTTP/1.1 session whose slot is returning is
/// taken at once (`DIV-046`). One that finds no session to take, below
/// `max_connections`, waits for such a session, if no other acquisition waits
/// for it, instead of starting a connection attempt; that session's next
/// notification wakes it ahead of earlier waiters (`DIV-046`).
/// Dropping it before it resolves abandons the acquisition, as dropping
/// `HttpSessionPool`'s does, and leaves a returning session it waited for to
/// the next acquisition.
pub struct OwnedSessionAcquisition(Acquisition);

enum Acquisition {
    Start {
        state: Rc<RefCell<PoolState>>,
        cancellation: CancellationToken,
    },
    Waiting(LocalClientFuture<Result<OwnedGetSessionResult, PoolError>>),
    Done,
}

impl Future for OwnedSessionAcquisition {
    type Output = Result<OwnedGetSessionResult, PoolError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let acquisition = &mut self.get_mut().0;
        loop {
            match std::mem::replace(acquisition, Acquisition::Done) {
                Acquisition::Start {
                    state,
                    cancellation,
                } => match start_acquisition(state, cancellation) {
                    AcquisitionStart::Done(result) => return Poll::Ready(result),
                    AcquisitionStart::Waiting(waiting) => {
                        *acquisition = Acquisition::Waiting(waiting);
                    }
                },
                Acquisition::Waiting(mut waiting) => {
                    let Poll::Ready(result) = waiting.as_mut().poll(context) else {
                        *acquisition = Acquisition::Waiting(waiting);
                        return Poll::Pending;
                    };
                    return Poll::Ready(result);
                }
                Acquisition::Done => {
                    panic!("an owned pool acquisition was polled after it resolved")
                }
            }
        }
    }
}

impl OwnedHttpSessionPool {
    /// A pool dialing `address` over plaintext TCP, as
    /// [`OwnedClientSession::connect`] does, with `connection` and `session`.
    /// Parameters an owned session cannot honor, and TLS, are
    /// refused here, as `connect` refuses them, before any dial.
    pub fn new(
        context: WorkerContext,
        address: SocketAddress,
        pool_params: PoolParams,
        connection: ConnectionParams,
        session: SessionParams,
    ) -> Result<Self, HttpError> {
        refuse_tls(&connection)?;
        DriverConfig::new(connection.plaintext_protocol, &session)?;
        Ok(Self(HttpSessionPool::owned(
            context,
            address,
            pool_params,
            connection,
            session,
        )))
    }

    /// Acquires a reservation, as `HttpSessionPool::get_session_with_reservation`,
    /// on the fast acquisition path: see [`OwnedSessionAcquisition`].
    pub fn get_session_with_reservation(
        &self,
        cancellation: CancellationToken,
    ) -> OwnedSessionAcquisition {
        OwnedSessionAcquisition(Acquisition::Start {
            state: Rc::clone(self.0.state()),
            cancellation,
        })
    }

    /// Selects among the pool's established sessions as `selection` says (`DIV-048`), from
    /// the next acquisition step on; [`OwnedSessionSelection::FirstAvailable`] until set.
    /// Refused with [`PoolError::Draining`] once the pool drains, as `HttpSessionPool`'s
    /// setters are.
    pub fn set_session_selection(&self, selection: OwnedSessionSelection) -> Result<(), PoolError> {
        self.0.set_session_selection(selection)
    }

    pub fn session_count(&self) -> usize {
        self.0.session_count()
    }

    /// The pool's counters now (`DIV-032`).
    pub fn stats(&self) -> OwnedPoolStats {
        self.0.owned_stats()
    }

    /// No session, waiter or pending connect.
    pub fn empty(&self) -> bool {
        self.0.empty()
    }

    /// As `HttpSessionPool::drain`.
    pub fn drain(&self) -> bool {
        self.0.drain()
    }

    /// As `HttpSessionPool::drain_and_join`.
    pub fn drain_and_join(&self) -> LocalClientFuture<Result<(), PoolError>> {
        self.0.drain_and_join()
    }

    pub fn worker_id(&self) -> WorkerId {
        self.0.worker_id()
    }

    #[cfg(test)]
    pub(crate) fn expire_sessions_for_test(&self) {
        self.0.expire_sessions_for_test();
    }

    #[cfg(test)]
    pub(crate) fn session_event_passes_for_test(&self) -> u64 {
        self.0.session_event_passes_for_test()
    }

    #[cfg(test)]
    pub(crate) fn connection_attempts_for_test(&self) -> u64 {
        self.0.connection_attempts_for_test()
    }

    #[cfg(test)]
    pub(crate) fn acquisition_step_for_test(&self, started: Instant) -> Result<(), PoolError> {
        self.0.acquisition_step_for_test(started)
    }

    #[cfg(test)]
    pub(crate) fn age_idle_session_for_test(&self, session: &OwnedClientSession, by: Duration) {
        self.0
            .age_idle_pooled_session_for_test(&PooledSession::Owned(session.clone()), by);
    }

    #[cfg(test)]
    pub(crate) fn expire_session_for_test(&self, session: &OwnedClientSession) {
        self.0
            .expire_pooled_session_for_test(&PooledSession::Owned(session.clone()));
    }

    #[cfg(test)]
    pub(crate) fn indexed_sessions_for_test(&self) -> (usize, usize) {
        self.0.indexed_sessions_for_test()
    }

    #[cfg(test)]
    pub(crate) fn pass_idle_time_for_test(&self, by: Duration) {
        self.0.pass_idle_time_for_test(by);
    }

    #[cfg(test)]
    pub(crate) fn index_settled_for_test(&self) -> bool {
        self.0.index_settled_for_test()
    }

    #[cfg(test)]
    pub(crate) fn force_pool_scans_for_test(&self, forced: bool) {
        self.0.force_pool_scans_for_test(forced);
    }

    #[cfg(test)]
    pub(crate) fn pending_connects_for_test(&self) -> u32 {
        self.0.pending_connects_for_test()
    }

    #[cfg(test)]
    pub(crate) fn pool_scans_for_test(&self) -> u64 {
        self.0.pool_scans_for_test()
    }

    #[cfg(test)]
    pub(crate) fn claim_scans_for_test(&self) -> u64 {
        self.0.claim_scans_for_test()
    }

    #[cfg(test)]
    pub(crate) fn pool_task_spawns_for_test(&self) -> u64 {
        self.0.pool_task_spawns_for_test()
    }

    #[cfg(test)]
    pub(crate) fn idle_retirement_retiring_passes_for_test(&self) -> u64 {
        self.0.idle_retirement_retiring_passes_for_test()
    }
}
