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

use crate::buf_queue::BufQueue;
use crate::connection::ConnectionContext;
use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::exchange::ExchangeCompletion;
use crate::fixed_source::DECIMAL_DIGITS;
use crate::fixed_source::format_decimal;
use crate::headers::HttpHeaderCode;
use crate::message::HttpMessage;
use crate::source_holder::HttpSourceHolder;

/// Cumulative counts of accepted shared responses, in total and by status class.
///
/// Index 0 counts every response and indexes 1 to 5 count the 1xx to 5xx classes.
/// Counters wrap on overflow, and a snapshot loads each one independently. The
/// counters do not hold request or session state, so they can be shared with an
/// exporter.
#[derive(Default)]
pub struct ResponseCounters {
    values: [AtomicU64; 6],
}

impl ResponseCounters {
    /// Returns the total followed by the five status-class counts, each loaded
    /// independently.
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

/// A shared response waiting to be counted once its session accepts it.
/// The status is the shared response's, not a later wire-status override.
pub struct HandlerResponseProposal {
    counters: Arc<ResponseCounters>,
    status: u16,
}

impl HandlerResponseProposal {
    /// Records this proposal once, after the session has accepted its generation
    /// and before fallible response setup. Dropping it does not record it.
    pub fn accept(self) {
        self.counters.record(self.status);
    }
}

enum ProposalState {
    Open(Option<HandlerResponseProposal>),
    Sealed,
}

/// The owner-local cancellation owner for one handler invocation.
///
/// The session's task and control owners hold this value. Handler contexts hold
/// only weak proposal handles, so retaining a context does not keep the
/// cancellation source alive.
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

    /// Returns the invocation's cancellation token.
    pub fn token(&self) -> CancellationToken {
        self.cancellation.token()
    }

    /// Requests cancellation through the invocation's [`CancellationSource`].
    pub fn request_cancellation(&self) -> bool {
        self.cancellation.request_cancellation()
    }

    /// Returns a weak proposal handle for this invocation.
    pub fn response_proposer(self: &Rc<Self>) -> HandlerResponseProposer {
        HandlerResponseProposer(Rc::downgrade(self))
    }

    /// Closes proposal submission for good and returns the proposal, if any.
    ///
    /// The task must call this when it completes and when it is dropped unpolled or
    /// aborted. A completion that should not be counted drops the returned proposal
    /// without accepting it.
    pub fn seal_response_proposal(&self) -> Option<HandlerResponseProposal> {
        let state = std::mem::replace(&mut *self.proposal.borrow_mut(), ProposalState::Sealed);
        match state {
            ProposalState::Open(proposal) => proposal,
            ProposalState::Sealed => None,
        }
    }
}

/// A non-owning handle to an invocation's one-shot shared-response proposal slot.
#[derive(Clone)]
pub struct HandlerResponseProposer(Weak<HandlerInvocation>);

impl HandlerResponseProposer {
    /// Submits a response for counting. Returns `false` without replacing the
    /// proposal if the invocation has expired, is sealed, or already has one.
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

/// An owner-local handle that keeps the session alive for one handler call.
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

    /// Adds trusted connection metadata. Fails with `INADEQUATE_SECURITY` if its peer
    /// identity differs from this context's or if different metadata is already attached.
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

    /// Returns the connection metadata. Holding it does not keep the session alive.
    pub fn connection_context(&self) -> Option<&Arc<ConnectionContext>> {
        self.connection.as_ref()
    }

    /// Attaches this exchange's cleanup completion. Fails with
    /// `INVALID_STATE_TRANSITION` if a different exchange is already attached.
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

    /// Binds the invocation's proposal handle. Fails with `INVALID_STATE_TRANSITION`
    /// if a different invocation, even an expired one, is already bound.
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

    /// Returns the proposal handle, which keeps neither the session nor the invocation
    /// alive.
    pub fn response_proposer(&self) -> Option<HandlerResponseProposer> {
        self.response_proposer.clone()
    }

    /// Returns this exchange's cleanup observation, if attached.
    ///
    /// A handler must not await its own exchange's completion, because the
    /// handler's retirement is part of that completion.
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

/// A whole response in memory, which [`HttpHandler::respond_now`] returns.
pub struct FixedResponse {
    head: HttpMessage,
    body: BufQueue,
}

impl FixedResponse {
    /// Sets `Content-Length` in `head` to the length of `body`, as
    /// `HttpFixedSource::new` does with a body.
    pub fn new(mut head: HttpMessage, body: BufQueue) -> Self {
        let mut digits = [0; DECIMAL_DIGITS];
        head.headers_mut().set(
            HttpHeaderCode::ContentLength,
            format_decimal(body.chain_length(), &mut digits),
        );
        Self { head, body }
    }

    /// Sends `head` as it is, so it carries the framing headers that the
    /// response needs. The header entries are left untouched, so a head whose
    /// entries are shared with its clones stays shared.
    ///
    /// Debug builds panic when a `Content-Length` in `head` differs from the
    /// length of a body that is not empty.
    pub fn from_parts(head: HttpMessage, body: BufQueue) -> Self {
        debug_assert!(
            body.is_empty() || content_length_matches(&head, body.chain_length()),
            "a fixed response's Content-Length differs from the length of its body"
        );
        Self { head, body }
    }

    pub fn head(&self) -> &HttpMessage {
        &self.head
    }

    pub fn body(&self) -> &BufQueue {
        &self.body
    }

    pub fn into_parts(self) -> (HttpMessage, BufQueue) {
        (self.head, self.body)
    }
}

/// Whether every `Content-Length` in `head` is `length`.
fn content_length_matches(head: &HttpMessage, length: usize) -> bool {
    head.headers()
        .iter()
        .filter(|&(code, _, _)| code == HttpHeaderCode::ContentLength)
        .all(|(_, _, value)| {
            std::str::from_utf8(value.trim_ascii())
                .is_ok_and(|value| value.parse::<usize>() == Ok(length))
        })
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

    /// Answers a request synchronously with a whole response, or returns `None`
    /// to leave the request to [`Self::handle_request`].
    ///
    /// Only the experimental owned HTTP/1.1 and HTTP/2 drivers and the server's
    /// HTTP/3 listener call this, with the head of each request without a body,
    /// and they send the response it returns without a request source, a
    /// handler future or a response source. An error or a panic is answered as
    /// the same failure of `handle_request` is. Other sessions call only
    /// `handle_request`, so it must still answer every request, and answer it as
    /// this method would. [`FixedResponse::new`] sets `Content-Length` as
    /// `HttpFixedSource::new` does, and [`FixedResponse::from_parts`] sends the
    /// head as it is.
    fn respond_now(&self, _request: &HttpMessage) -> Option<Result<FixedResponse, HttpError>> {
        None
    }
}
