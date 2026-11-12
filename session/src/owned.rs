//! Protocol-neutral contracts for the owned (poll-driven) session drivers.

use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;
use std::time::Instant;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::LocalTransportFuture;

use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexReader;
use crate::uniplex::UniplexTransport;

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
    /// Whether the exchange's handler is still running, so it has not yet returned its response.
    /// When the request body's read times out before the final response head, as in
    /// `UniplexSession`, the driver waits for a running handler and sends a final 408 it answers
    /// (any other final response becomes a bare 408), and otherwise sends a bare 408 at once.
    fn handler_active(&self) -> bool {
        false
    }
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
    /// The peer reset a live stream, on HTTP/2: with an RST_STREAM carrying `code`, or with a
    /// GOAWAY that refused the stream, as `REFUSED_STREAM`. Once per stream, at its first
    /// peer reset; not for a `NO_ERROR` reset after the stream's whole response, which stops
    /// only its upload (`DIV-050`).
    fn peer_reset(&self, _code: HttpErrorCode) {}
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

/// An owned driver's read: through the transport's [`UniplexReader`] when it offers one, else
/// through one `UniplexTransport::read` future per read. The reader picks the path at the call
/// site, and each path runs out of line, so neither carries the other's code.
pub(crate) struct ReadSlot {
    reader: Option<Box<dyn UniplexReader>>,
    timeout: Duration,
    cancellation: CancellationToken,
    read: Option<SlotRead>,
    /// The read last returned drained the transport (`UniplexReader::is_drained`): a read started
    /// before the waker of the poll that returned it fires would pend. Always false without a
    /// reader.
    drained: bool,
}

/// A started read.
enum SlotRead {
    Future(LocalTransportFuture<Result<UniplexRead, HttpError>>),
    /// The reader's started read.
    Reader,
}

impl ReadSlot {
    /// Acquires `transport`'s reader for reads with `timeout` and `cancellation`.
    pub(crate) fn new(
        transport: &dyn UniplexTransport,
        context: &WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self, HttpError> {
        Ok(Self {
            reader: transport.try_reader(context, timeout, cancellation.clone())?,
            timeout,
            cancellation,
            read: None,
            drained: false,
        })
    }

    /// Whether a started read has not yet been returned by `poll`.
    pub(crate) fn is_reading(&self) -> bool {
        self.read.is_some()
    }

    /// Whether reads go through the transport's [`UniplexReader`] rather than one read future
    /// each.
    pub(crate) fn uses_reader(&self) -> bool {
        self.reader.is_some()
    }

    /// Whether the read last returned drained the transport. A driver then starts its next read
    /// only once its read waker fires.
    pub(crate) fn is_drained(&self) -> bool {
        self.drained
    }

    /// Starts a read of at most `maximum` bytes that first offers `allocation_size` bytes, and
    /// polls it at once; `Ready` returns it.
    #[inline(always)]
    pub(crate) fn poll_start(
        &mut self,
        transport: &dyn UniplexTransport,
        context: &WorkerContext,
        waker: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        debug_assert!(
            self.read.is_none(),
            "a driver starts a read only when none is started"
        );
        // The read counts as started before its first poll, so a poll that panics leaves it
        // started, as a read started and then polled does.
        if self.reader.is_some() {
            self.poll_start_reader(waker, maximum, allocation_size)
        } else {
            self.poll_start_future(transport, context, waker, maximum, allocation_size)
        }
    }

    /// Polls the started read; `Ready` returns it.
    #[inline(always)]
    pub(crate) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        if self.reader.is_some() {
            self.poll_reader(context)
        } else {
            self.poll_future(context)
        }
    }

    /// `poll_start` without a reader.
    #[inline(never)]
    fn poll_start_future(
        &mut self,
        transport: &dyn UniplexTransport,
        context: &WorkerContext,
        waker: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        self.read = Some(SlotRead::Future(transport.read(
            context,
            maximum,
            allocation_size,
            self.timeout,
            self.cancellation.clone(),
        )));
        self.poll_started_future(waker)
    }

    /// `poll` without a reader.
    #[inline(never)]
    fn poll_future(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRead, HttpError>> {
        self.poll_started_future(context)
    }

    #[inline(always)]
    fn poll_started_future(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        let Some(SlotRead::Future(future)) = &mut self.read else {
            panic!("a polled read was started");
        };
        let polled = future.as_mut().poll(context);
        if polled.is_pending() {
            return Poll::Pending;
        }
        self.read = None;
        polled
    }

    /// `poll_start` through the reader.
    #[inline(never)]
    fn poll_start_reader(
        &mut self,
        waker: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        let reader = self
            .reader
            .as_mut()
            .expect("a reader read keeps its reader");
        self.read = Some(SlotRead::Reader);
        let result = ready!(reader.poll_start_read(waker, maximum, allocation_size));
        self.read = None;
        self.note_drained(Poll::Ready(result))
    }

    /// `poll` through the reader.
    #[inline(never)]
    fn poll_reader(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRead, HttpError>> {
        assert!(self.read.is_some(), "a polled read was started");
        let result = ready!(
            self.reader
                .as_mut()
                .expect("a reader read keeps its reader")
                .poll_read(context)
        );
        self.read = None;
        self.note_drained(Poll::Ready(result))
    }

    fn note_drained(
        &mut self,
        polled: Poll<Result<UniplexRead, HttpError>>,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        self.drained = match &polled {
            Poll::Ready(Ok(_)) => self
                .reader
                .as_ref()
                .is_some_and(|reader| reader.is_drained()),
            Poll::Ready(Err(_)) | Poll::Pending => false,
        };
        polled
    }
}

#[cfg(test)]
#[path = "../tests/read_slot.rs"]
mod read_slot_tests;
