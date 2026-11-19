/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::keep_alive::KeepAliveError;
use roxygen_runtime::keep_alive::KeepAliveOwner;
use roxygen_runtime::keep_alive::KeepAlivePtr;

use crate::connection::ConnectionContext;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::exchange::ExchangeCompletion;
use crate::source_holder::HttpSourceHolder;

/// Cumulative accepted shared-response counts: completed, then status classes 1–5.
///
/// Updates wrap and snapshots load each counter independently. This bank owns no
/// callbacks or request/session state and may be shared with an exporter.
#[derive(Default)]
pub struct ResponseCounters {
    values: [AtomicU64; 6],
}

impl ResponseCounters {
    /// Loads completed followed by the five status-class totals independently.
    pub fn snapshot(&self) -> [u64; 6] {
        std::array::from_fn(|index| self.values[index].load(Ordering::Relaxed))
    }

    fn record(&self, status: u16) {
        self.values[0].fetch_add(1, Ordering::Relaxed);
        let class = usize::from(status / 100);
        if (1..=5).contains(&class) {
            self.values[class].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// One shared response awaiting its session's live-generation acceptance gates.
/// Its status describes the shared response, not a later wire-status override.
pub struct HandlerResponseProposal {
    counters: Arc<ResponseCounters>,
    status: u16,
}

impl HandlerResponseProposal {
    /// Records this proposal once, after the session has accepted its generation
    /// and before fallible response setup. Dropping it records nothing.
    pub fn accept(self) {
        self.counters.record(self.status);
    }
}

enum ProposalState {
    Open(Option<HandlerResponseProposal>),
    Sealed,
}

/// The existing owner-local cancellation owner for one handler invocation.
///
/// Session task/control owners retain this payload. Contexts expose only Weak
/// proposal views, so retaining one cannot prolong the cancellation source.
pub struct HandlerInvocation {
    cancellation: CancellationSource,
    proposal: RefCell<ProposalState>,
}

impl Default for HandlerInvocation {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerInvocation {
    /// Creates one cancellation owner with an open, empty proposal slot.
    pub fn new() -> Self {
        Self {
            cancellation: CancellationSource::new(),
            proposal: RefCell::new(ProposalState::Open(None)),
        }
    }

    /// Returns the existing invocation cancellation token.
    pub fn token(&self) -> CancellationToken {
        self.cancellation.token()
    }

    /// Requests cancellation with the cancellation source's existing wake semantics.
    pub fn request_cancellation(&self) -> bool {
        self.cancellation.request_cancellation()
    }

    /// Returns a Weak proposal view of this exact invocation allocation.
    pub fn response_proposer(self: &Rc<Self>) -> HandlerResponseProposer {
        HandlerResponseProposer(Rc::downgrade(self))
    }

    /// Permanently closes proposal submission, transferring at most one proposal.
    /// The task must seal on normal completion and on unpolled/aborted destruction;
    /// ineligible completion discards the returned proposal without accepting it.
    pub fn seal_response_proposal(&self) -> Option<HandlerResponseProposal> {
        let state = std::mem::replace(&mut *self.proposal.borrow_mut(), ProposalState::Sealed);
        match state {
            ProposalState::Open(proposal) => proposal,
            ProposalState::Sealed => None,
        }
    }
}

/// A non-owning view of one invocation's one-shot shared-response proposal slot.
#[derive(Clone)]
pub struct HandlerResponseProposer(Weak<HandlerInvocation>);

impl HandlerResponseProposer {
    /// Submits the first response only. Expired, sealed, and duplicate submission
    /// return false without replacing a proposal or changing handler behavior.
    pub fn propose(&self, counters: Arc<ResponseCounters>, status: u16) -> bool {
        let Some(invocation) = self.0.upgrade() else {
            return false;
        };
        let mut state = invocation.proposal.borrow_mut();
        if let ProposalState::Open(slot @ None) = &mut *state {
            *slot = Some(HandlerResponseProposal { counters, status });
            true
        } else {
            false
        }
    }
}

/// A nonempty owner-local session-liveness capability for one handler call.
#[derive(Clone)]
pub struct HandlerSessionContext {
    _keep_alive: KeepAlivePtr<()>,
    peer_identity: Option<Rc<[u8]>>,
    connection: Option<Arc<ConnectionContext>>,
    exchange: Option<ExchangeCompletion>,
    response_proposer: Option<HandlerResponseProposer>,
    _not_send: PhantomData<Rc<()>>,
}

impl HandlerSessionContext {
    /// Acquires a handler context without authenticated peer identity.
    pub fn acquire(owner: &KeepAliveOwner<()>) -> Result<Self, KeepAliveError> {
        Self::acquire_with_peer_identity(owner, None)
    }

    /// Acquires a handler context carrying transport-owned peer identity bytes.
    pub fn acquire_with_peer_identity(
        owner: &KeepAliveOwner<()>,
        peer_identity: Option<Rc<[u8]>>,
    ) -> Result<Self, KeepAliveError> {
        Ok(Self {
            _keep_alive: owner.acquire_keep_alive()?,
            peer_identity,
            connection: None,
            exchange: None,
            response_proposer: None,
            _not_send: PhantomData,
        })
    }

    /// Returns the transport-supplied identity without changing its encoding or
    /// verification policy.
    pub fn peer_identity(&self) -> Option<&[u8]> {
        self.peer_identity.as_deref()
    }

    /// Adds trusted connection metadata without replacing the session's identity
    /// or its owner-local liveness capability. Conflicting enrichment fails closed.
    pub fn with_connection_context(
        mut self,
        connection: Arc<ConnectionContext>,
    ) -> Result<Self, HttpError> {
        if self.peer_identity() != connection.peer_identity()
            || self
                .connection
                .as_ref()
                .is_some_and(|existing| existing.as_ref() != connection.as_ref())
        {
            return Err(HttpError::with_message(
                HttpErrorCode::INADEQUATE_SECURITY,
                b"connection metadata conflicts with the handler's trusted context",
            ));
        }
        self.connection = Some(connection);
        Ok(self)
    }

    /// Returns transferable metadata, never a transferable session capability.
    pub fn connection_context(&self) -> Option<&Arc<ConnectionContext>> {
        self.connection.as_ref()
    }

    /// Attaches sticky cleanup observation for this exchange. Reattaching a
    /// different exchange fails closed; this receipt does not own cleanup.
    pub fn with_exchange_completion(
        mut self,
        completion: ExchangeCompletion,
    ) -> Result<Self, HttpError> {
        if self
            .exchange
            .as_ref()
            .is_some_and(|existing| !existing.same_exchange(&completion))
        {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"handler context already belongs to another exchange",
            ));
        }
        self.exchange = Some(completion);
        Ok(self)
    }

    /// Binds the invocation's non-owning proposal view. A different invocation,
    /// including an expired one, cannot replace an existing binding.
    pub fn with_response_proposer(
        mut self,
        proposer: HandlerResponseProposer,
    ) -> Result<Self, HttpError> {
        if self
            .response_proposer
            .as_ref()
            .is_some_and(|existing| !Weak::ptr_eq(&existing.0, &proposer.0))
        {
            return Err(HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"handler context already belongs to another invocation",
            ));
        }
        self.response_proposer = Some(proposer);
        Ok(self)
    }

    /// Extracts the proposal view without retaining session or cancellation ownership.
    pub fn response_proposer(&self) -> Option<HandlerResponseProposer> {
        self.response_proposer.clone()
    }

    /// Returns this exchange's cleanup observation, if attached.
    ///
    /// A handler must not await its own receipt: its retirement contributes to
    /// the completion being observed.
    pub fn exchange_completion(&self) -> Option<&ExchangeCompletion> {
        self.exchange.as_ref()
    }
}

pub type LocalHandlerFuture =
    Pin<Box<dyn Future<Output = Result<HttpSourceHolder, HttpError>> + 'static>>;

pub type HttpHandlerRef = Rc<dyn HttpHandler>;

/// An owner-local handler invocation that retains its session context.
pub struct HandlerFuture {
    inner: Option<LocalHandlerFuture>,
    context: Option<HandlerSessionContext>,
}

impl HandlerFuture {
    pub(crate) fn new(context: HandlerSessionContext, future: LocalHandlerFuture) -> Self {
        Self {
            inner: Some(future),
            context: Some(context),
        }
    }

    fn finish(&mut self) {
        let inner = self.inner.take();
        drop(inner);
        let context = self.context.take();
        drop(context);
    }
}

impl Future for HandlerFuture {
    type Output = Result<HttpSourceHolder, HttpError>;

    fn poll(mut self: Pin<&mut Self>, poll_context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = {
            let inner = self
                .inner
                .as_mut()
                .expect("a completed handler future cannot be polled again");
            inner.as_mut().poll(poll_context)
        };
        let Poll::Ready(result) = result else {
            return Poll::Pending;
        };
        self.finish();
        Poll::Ready(result)
    }
}

impl Drop for HandlerFuture {
    fn drop(&mut self) {
        self.finish();
    }
}

/// An owner-local HTTP request handler.
///
/// The request source is transferred into the invocation and the response
/// source is transferred back to the caller. A handler error retains its
/// identity so the session can wrap it in an error source before filtering.
pub trait HttpHandler: 'static {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture;
}
