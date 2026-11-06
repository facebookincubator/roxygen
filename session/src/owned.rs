//! Protocol-neutral contracts for the owned (poll-driven) session drivers.

use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use roxygen_core::error::HttpError;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSourceOwner;

/// An application exchange remains owned until ingress consumption, application
/// body retirement, and physical write completion have all been observed, except where a
/// driver documents an early-retirement exception. The owned HTTP/2 driver's exception
/// (`DIV-028`): a downstream exchange that is not reset, whose request fully arrived with no
/// trailing RST owed, and that registered no byte event skips physical write completion when
/// the write queue reports it is not saturated as its END_STREAM is serialized.
pub trait OwnedExchange: 'static {
    /// Supplies a validated owned request head and its existing body source.
    /// `eom` implies the terminal empty-body read was consumed before handoff.
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool);
    /// Supplies a fresh ingress body without exposing an independent reader.
    /// The default preserves the general source API for existing applications.
    fn request_body(&mut self, head: Box<HttpMessage>, body: HttpIngressBody, eom: bool) {
        self.request(head, body.into_source_owner(), eom);
    }

    /// Produces the response head without packaging it into a source.
    /// After returning `Pending`, wake the supplied context when polling can progress.
    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>>;
    /// Produces at most `maximum` data bytes, the driver's allowance for one data event
    /// (HTTP/2: flow-control credit and frame size; HTTP/1.1: the driver's write allowance).
    /// Preserve source terminal handling, byte-event registrations, padding, and suspension
    /// events.
    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>>;
    /// Cancels the owned policy/body operation with its selected cause. `peer_reset` is true
    /// when the peer reset the stream (HTTP/2 RST_STREAM) or closed the connection before the
    /// exchange finished (HTTP/1.1). All synchronous callback/destructor work runs outside
    /// connection-state borrows.
    fn cancel(&mut self, error: &HttpError, peer_reset: bool);
    /// Records codec-accepted response completion, independently of native write completion.
    fn response_complete(&mut self);
    /// Retires the exchange after the driver's full fence, or earlier under the driver's
    /// documented exception (HTTP/2: END_STREAM serialization under `DIV-028`); the
    /// application determines success.
    fn complete(self);
}

/// The caller supplies one concrete exchange owner per admitted stream.
pub trait OwnedApplication: 'static {
    type Exchange: OwnedExchange;
    /// Begins accounting before decoded head validation can report a stream error.
    fn new_exchange(&self, stream: StreamId, ingress: IngressCompletion) -> Self::Exchange;
}

/// Admission notifications run on the session owner, outside state borrows.
/// Capacity release precedes the notification that another reservation can start.
pub trait OwnedUpstreamObserver: 'static {
    fn capacity_released(&self, _stream: StreamId) {}
    /// Precedes `capacity_released` when the submitter has not yet taken the response.
    /// `released_submission_retired` follows exactly once, when the submitter takes the
    /// response or drops the submission.
    fn capacity_released_before_delivery(&self) {}
    fn released_submission_retired(&self) {}
    fn capacity_available(&self) {}
    fn admission_closed(&self) {}
}

/// An owner-local deadline observed independently of response-body consumption.
/// The sticky producer result takes precedence over polling the deadline.
/// Dropping an observer disarms it without selecting a response outcome.
pub trait OwnedResponseObserver: 'static {
    /// A pending deadline must arrange to wake the supplied stream waker.
    fn poll_deadline(&mut self, context: &mut Context<'_>) -> Poll<HttpError>;

    /// Consumes the observer before applying producer completion or deadline policy.
    fn response_complete(self: Box<Self>, completed: Result<Instant, HttpError>);
}
