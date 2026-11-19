/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Protocol-neutral contracts for the owned (poll-driven) session drivers.

use std::any::Any;
#[cfg(test)]
use std::cell::RefCell;
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
use roxygen_runtime::worker::ContainedPanicScope;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_stream::LocalTransportFuture;

use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexReader;
use crate::uniplex::UniplexTransport;

/// One application exchange served by an owned driver.
///
/// The exchange stays owned until its request ingress is consumed, its response body is
/// retired, and its writes have completed. The owned drivers make one exception. A downstream
/// exchange that has received its whole request and registered no byte event does not wait for
/// its writes if the write queue is not saturated when its end of message is serialized. On
/// HTTP/2 the stream must also not be reset and must have no trailing RST_STREAM pending.
pub trait OwnedExchange: 'static {
    /// Supplies a validated owned request head and its existing body source.
    /// `eom` implies the terminal empty-body read was consumed before handoff.
    fn request(&mut self, head: Box<HttpMessage>, body: HttpStreamSourceOwner, eom: bool);
    /// Supplies a fresh ingress body without exposing an independent reader.
    /// The default converts the body into a source and calls `request`.
    fn request_body(&mut self, head: Box<HttpMessage>, body: HttpIngressBody, eom: bool) {
        self.request(head, body.into_source_owner(), eom);
    }

    /// Produces the response head without packaging it into a source.
    /// After returning `Pending`, wake the supplied context when polling can progress.
    fn poll_headers(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>>;
    /// Produces at most `maximum` data bytes, the driver's allowance for one data event. On
    /// HTTP/2 the allowance follows flow-control credit and frame size, on HTTP/1.1 the driver's
    /// write allowance. Preserve source terminal handling, byte-event registrations, padding,
    /// and suspension events.
    fn poll_body(
        &mut self,
        context: &mut Context<'_>,
        maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>>;
    /// Whether the exchange's handler is still running and has not returned its response.
    ///
    /// When a request body read times out before the final response head, the driver waits for
    /// a running handler and sends its final response if that is a 408. Any other final response
    /// becomes a bare 408. Without a running handler the driver sends a bare 408 immediately.
    /// `UniplexSession` behaves the same way.
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
    /// Retires the exchange once the conditions in the trait docs hold, or without waiting for
    /// its writes under the early-retirement exception described there. The application
    /// determines success.
    fn complete(self);
}

/// Creates the application's exchange for each admitted stream.
pub trait OwnedApplication: 'static {
    type Exchange: OwnedExchange;
    /// Creates the exchange for `stream`. It is called before the decoded head is validated, so
    /// accounting begins before a stream error can be reported.
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
    /// The HTTP/2 peer reset a live stream, with an RST_STREAM carrying `code` or with a GOAWAY
    /// whose last-stream-id excludes the stream, reported as `REFUSED_STREAM`. Called once per
    /// stream, at its first peer reset. Not called for a `NO_ERROR` reset after the stream's
    /// whole response, which only stops its upload.
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

/// Where an owned driver contained a panic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PanicSite {
    /// A driver turn, outside any narrower boundary.
    DriverTurn,
    /// A lifecycle observer.
    LifecycleObserver,
    /// An upstream session's admission callbacks: its `OwnedUpstreamObserver` and the
    /// refusals of queued submissions.
    UpstreamObserver,
    /// A retiring or cancelled exchange: its `complete`, `cancel` or destructor, its source's
    /// `stop_reading` or destructor, or a value it owned.
    ExchangeRetirement,
    /// A byte-event callback.
    ByteEvent,
    /// A request head's encoded-headers callback, or the head event's destructor.
    HeadersEncoded,
    /// A client request's upload: its source, resume or ingress-body callback.
    UploadBody,
    /// A client request's `OwnedResponseObserver`.
    ResponseObserver,
    /// The refusal of a client submission the driver has not committed: its source's
    /// `stop_reading` or destructor, its submitter's waker, or a value it owned.
    Submission,
    /// The transport's close or a cancellation waker.
    Transport,
    /// A client request's commitment, which runs no application code.
    Commit,
    /// A step of the teardown that follows a Driver-scope panic, or a field dropped as the
    /// driver is released.
    Teardown,
}

impl PanicSite {
    const fn scope(self) -> ContainedPanicScope {
        match self {
            Self::ExchangeRetirement
            | Self::ByteEvent
            | Self::HeadersEncoded
            | Self::UploadBody
            | Self::ResponseObserver
            | Self::Submission => ContainedPanicScope::Exchange,
            Self::LifecycleObserver | Self::UpstreamObserver => ContainedPanicScope::Connection,
            Self::DriverTurn | Self::Transport | Self::Commit | Self::Teardown => {
                ContainedPanicScope::Driver
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::DriverTurn => "driver_turn",
            Self::LifecycleObserver => "lifecycle_observer",
            Self::UpstreamObserver => "upstream_observer",
            Self::ExchangeRetirement => "exchange_retirement",
            Self::ByteEvent => "byte_event",
            Self::HeadersEncoded => "headers_encoded",
            Self::UploadBody => "upload_body",
            Self::ResponseObserver => "response_observer",
            Self::Submission => "submission",
            Self::Transport => "transport",
            Self::Commit => "commit",
            Self::Teardown => "teardown",
        }
    }
}

/// Reports a contained panic to the worker: its `WorkerPollStats` count and, under the runtime's
/// `observability` feature, an error event. The caller still disposes of `payload`.
#[cold]
#[inline(never)]
pub(crate) fn report_contained_panic(
    context: &WorkerContext,
    site: PanicSite,
    stream: Option<StreamId>,
    payload: &(dyn Any + Send),
) {
    #[cfg(test)]
    CONTAINED_PANICS.with_borrow_mut(|reports| {
        let message = payload
            .downcast_ref::<&'static str>()
            .map(|text| (*text).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned());
        reports.push((site, stream, message));
    });
    context.record_contained_panic(
        site.scope(),
        site.name(),
        stream.map(StreamId::get),
        payload,
    );
}

#[cfg(test)]
thread_local! {
    static CONTAINED_PANICS: RefCell<Vec<(PanicSite, Option<StreamId>, Option<String>)>> =
        const { RefCell::new(Vec::new()) };
}

/// Takes the panics the owned drivers on this thread reported, in order, with each one's site,
/// stream and message.
#[cfg(test)]
pub(crate) fn take_contained_panics() -> Vec<(PanicSite, Option<StreamId>, Option<String>)> {
    CONTAINED_PANICS.take()
}

/// An owned driver's read: through the transport's [`UniplexReader`] when it offers one, else
/// through one `UniplexTransport::read` future per read. The path is chosen at each call site
/// and each path is out of line, so neither inlines the other's code.
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
    /// A `UniplexTransport::read` future.
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

    /// Whether the reader reports the peer's FIN or RST in the transport
    /// (`UniplexReader::peer_closed`). Always false without a reader.
    pub(crate) fn peer_closed(&self) -> bool {
        self.reader
            .as_ref()
            .is_some_and(|reader| reader.peer_closed())
    }

    /// Starts a read of at most `maximum` bytes that first offers `allocation_size` bytes, and
    /// polls it immediately. A `Ready` result ends the started read.
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

    /// Polls the started read. A `Ready` result ends it.
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

    /// Drops the started read unfinished after its polls panicked: its future, or the reader that
    /// holds it, so no later poll reaches it.
    pub(crate) fn abandon(&mut self) {
        self.read = None;
        drop(self.reader.take());
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
