/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! The experimental owned session pool.

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

/// A worker-local pool of [`OwnedClientSession`]s to one address.
///
/// It shares `HttpSessionPool`'s scheduler and [`PoolParams`], with the same connection,
/// waiter, connect-timeout, acquisition-timeout, maximum-age, idle-timeout, reuse-order and
/// idle-sweep behavior. An HTTP/1.1 pool keeps the sessions that can take an exchange in an
/// index, so that an acquisition that takes one, and a release, do not scan the pool's
/// sessions. The scheduler selects the session unless
/// [`OwnedHttpSessionPool::set_session_selection`] switches to the least loaded one.
pub struct OwnedHttpSessionPool(HttpSessionPool);

/// Which session an [`OwnedHttpSessionPool`]'s acquisition takes among its established
/// sessions that can take an exchange.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OwnedSessionSelection {
    /// `HttpSessionPool`'s order: the first session in the pool's order that has an exchange
    /// and can take another, else an idle session in `PoolParams::reuse_order`. Exchanges
    /// pack onto the first session with room.
    #[default]
    FirstAvailable,
    /// The session with the fewest exchanges in flight.
    ///
    /// On HTTP/2 that is the fewest streams reserved or open, so idle sessions come first.
    /// On HTTP/1.1, where a session takes one exchange, a session whose slot is returning
    /// comes first, then an idle session, then one whose driver is not idle yet. Equally
    /// loaded sessions are taken in `FirstAvailable`'s order: a busy one first, in the
    /// pool's order, then an idle one in `PoolParams::reuse_order`.
    LeastLoaded,
}

/// A snapshot of an [`OwnedHttpSessionPool`]'s counters.
///
/// The pool keeps them on its worker and updates them only on slow paths: dials, waits,
/// admission closes, session completions and peer stream resets. Every field but
/// `waiters` only grows.
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
    /// Sessions whose admission was closed by a peer GOAWAY, with `NO_ERROR` or an error
    /// code. Counted when admission closes, before the session's streams finish. A GOAWAY
    /// that arrives after admission closed for another reason is not counted.
    pub peer_goaways: u64,
    /// Of `peer_goaways`, those whose GOAWAY carried a code other than `NO_ERROR`.
    pub peer_goaway_errors: u64,
    /// Sessions the pool closed to new exchanges to retire them: at `max_age` or
    /// `idle_timeout`, on release when `max_connections` is zero, or when the pool drains.
    pub local_drains: u64,
    /// Streams the peer refused unprocessed, with an RST_STREAM carrying `REFUSED_STREAM`
    /// or a GOAWAY whose last stream ID is lower. Counted at the refusal, also on a session
    /// the pool has already removed, as after a GOAWAY.
    pub refused_streams: u64,
    /// Streams the peer reset with an RST_STREAM carrying any other code, counted like
    /// `refused_streams`. A `NO_ERROR` reset after the whole response arrived only stops
    /// the upload, and is not counted.
    pub reset_streams: u64,
    /// Acquisitions waiting now for a session, a stream slot or a dial.
    pub waiters: usize,
    /// The highest number of acquisitions waiting at the same time.
    pub peak_waiters: usize,
    /// Acquisitions that failed with [`PoolError::Timeout`].
    pub acquisition_timeouts: u64,
    /// Acquisitions rejected with [`PoolError::MaxWaiters`].
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

    fn handed_over(reservation: OwnedGetSessionResult) -> Result<Self, PoolError> {
        Ok(reservation)
    }
}

/// A pending [`OwnedHttpSessionPool`] acquisition.
///
/// Its first poll runs the pool's first acquisition step in place. A selected session is
/// reserved and returned without allocating or reading the clock for the acquisition's
/// deadline. A nonzero `idle_timeout` or `max_age` still reads the clock for session
/// expiry, as in `HttpSessionPool`. Only an acquisition that waits for a session or a dial
/// continues in a boxed future.
///
/// While the waiter queue is empty, an HTTP/1.1 session whose slot is returning is taken
/// immediately. Below `max_connections`, an acquisition that cannot take a session waits
/// for such a returning session instead of dialing, unless another acquisition already
/// waits for it. That session's next notification wakes it ahead of earlier waiters.
///
/// An acquisition that starts a dial and is still waiting when the dial ends takes its
/// outcome, as hyper-util sends a request on the connection it dialed for it. A new
/// session is reserved for it while the worker accepts reservations, so that a peer close
/// read before the acquisition runs fails its request unprocessed instead of causing
/// another dial. [`OwnedClientReservation::dialed_for_acquisition`] reports such a
/// reservation. In an HTTP/1.1 pool a failed dial fails it with
/// [`PoolError::ConnectFailed`], whatever other sessions or dials the pool has, as
/// hyper-util fails a request whose connect fails. In an HTTP/2 pool, as in
/// `HttpSessionPool`, a failed dial fails waiters only when it was the last pending dial of
/// a pool without sessions, and then it fails all of them.
///
/// Dropping it before it resolves abandons the acquisition and leaves a returning session
/// it waited for to the next acquisition.
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
    /// Creates a pool that dials `address` over plaintext TCP, as
    /// [`OwnedClientSession::connect`] does, with `connection` and `session`.
    /// TLS and parameters an owned session cannot support are rejected here,
    /// as `connect` rejects them, before any dial.
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

    /// Acquires a reservation, like `HttpSessionPool::get_session_with_reservation`
    /// but on the fast path described at [`OwnedSessionAcquisition`].
    pub fn get_session_with_reservation(
        &self,
        cancellation: CancellationToken,
    ) -> OwnedSessionAcquisition {
        OwnedSessionAcquisition(Acquisition::Start {
            state: Rc::clone(self.0.state()),
            cancellation,
        })
    }

    /// Sets how acquisitions select among the pool's established sessions, from the next
    /// acquisition step on. The default is [`OwnedSessionSelection::FirstAvailable`].
    /// Fails with [`PoolError::Draining`] once the pool drains, like `HttpSessionPool`'s
    /// setters.
    pub fn set_session_selection(&self, selection: OwnedSessionSelection) -> Result<(), PoolError> {
        self.0.set_session_selection(selection)
    }

    pub fn session_count(&self) -> usize {
        self.0.session_count()
    }

    /// Returns a snapshot of the pool's counters.
    pub fn stats(&self) -> OwnedPoolStats {
        self.0.owned_stats()
    }

    /// Whether the pool has no session, waiter or pending connect.
    pub fn empty(&self) -> bool {
        self.0.empty()
    }

    /// Drains the pool, like `HttpSessionPool::drain`.
    pub fn drain(&self) -> bool {
        self.0.drain()
    }

    /// Drains the pool and returns a future that resolves once its tasks and sessions are
    /// joined, like `HttpSessionPool::drain_and_join`.
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
    pub(crate) fn claim_visits_for_test(&self) -> u64 {
        self.0.claim_visits_for_test()
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
