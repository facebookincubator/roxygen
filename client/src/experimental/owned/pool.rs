//! The experimental owned session pool (`DIV-046`).

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::task::Poll;
#[cfg(test)]
use std::time::Duration;

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
/// waiter, connect-timeout, maximum-age, idle-timeout, reuse-order and
/// idle-sweep semantics (`DIV-032`), shared with it. An HTTP/1.1 pool keeps its
/// sessions that can take an exchange in a selection index, so that an
/// acquisition that finds one, and a release, make no pass over the pool's
/// sessions however many it holds; the selection is the scheduler's.
pub struct OwnedHttpSessionPool(HttpSessionPool);

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
/// and one worker admission check, and the reservation is recorded on its
/// entry alone. A nonzero `idle_timeout` or `max_age` still reads the clock
/// for the sessions' expiry, as `HttpSessionPool`'s step does. Only an
/// acquisition that waits for a session or a dial continues in a boxed future.
/// One that finds no session to take, below `max_connections`, waits for an
/// HTTP/1.1 session whose driver returns its slot at its next turn, if no other
/// acquisition waits for it, instead of starting a connection attempt; that
/// session's next notification wakes it ahead of earlier waiters (`DIV-046`).
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

    pub fn session_count(&self) -> usize {
        self.0.session_count()
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
    pub(crate) fn session_event_passes_for_test(&self) -> u64 {
        self.0.session_event_passes_for_test()
    }

    #[cfg(test)]
    pub(crate) fn connection_attempts_for_test(&self) -> u64 {
        self.0.connection_attempts_for_test()
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
}
