/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Unstable APIs.
//!
//! Every item in this module may change or be removed in any release.
//!
//! - [`UniplexStreamAbort`], taken through [`UniplexResponseAbortExt`], resets
//!   one committed stream with `CANCEL`, whether or not its request EOM was
//!   sent.
//! - [`PingProbeConfig`] configures HTTP/2 PING probes that detect a dead
//!   peer, and [`is_ping_probe_timeout`] recognizes the error they drop a
//!   connection with.
//! - [`UniplexResponseProgressExt`] reports whether a committed request's head
//!   was serialized, so that a caller can resend a request that failed before
//!   any of it was written, as hyper-util's client does on reused connections.
//!   [`OwnedRequestProgressExt`] is its counterpart for the owned upstream
//!   drivers: whether a request failed unprocessed, and how many requests its
//!   connection carried before it.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;

use crate::uniplex::StreamAbort;
use crate::uniplex::UniplexResponse;

/// A one-use, owner-local reset of one committed stream.
///
/// It holds its session weakly and is not `Send`. Dropping it does not reset
/// the stream.
pub struct UniplexStreamAbort(pub(crate) StreamAbort);

impl UniplexStreamAbort {
    /// Resets the stream with `CANCEL`.
    ///
    /// On HTTP/2 this sends one RST_STREAM, even after the request EOM. On
    /// HTTP/1.1, which cannot reset one stream, the connection drains its
    /// queued writes and then closes with a TCP reset. The response reader's
    /// pending or next read fails with `CANCEL`, queued request egress fails,
    /// and the stream's unread response bytes are discarded and their
    /// connection credit returned.
    ///
    /// A stream that has received its response END_STREAM and sent its request
    /// EOM gets no frame. A stream that either side already reset, a retired
    /// stream, and a session that failed or finished are left unchanged. An
    /// HTTP/1.1 tunnel that keeps reading after its request EOM closed the
    /// connection's writes still resets the connection.
    ///
    /// Call it from owner-local code. A session callout, such as a request
    /// source's callback that the session runs while it processes a stream,
    /// cannot reach the stream, so an abort made there has no effect and still
    /// uses up the capability. Lifecycle observers run outside callouts and
    /// may abort. If a lifecycle observer, a waker, or the request source's
    /// `stop_reading` panics during the abort, the first such panic resumes
    /// after the stream is reset and its parked reader is woken. Later panics
    /// are discarded.
    pub fn abort(self) {
        self.0.abort();
    }
}

/// Access to the abort that a [`UniplexResponse`] carries.
pub trait UniplexResponseAbortExt {
    /// Takes the stream's abort; later calls return `None`.
    fn take_stream_abort(&mut self) -> Option<UniplexStreamAbort>;
}

impl UniplexResponseAbortExt for UniplexResponse {
    fn take_stream_abort(&mut self) -> Option<UniplexStreamAbort> {
        self.abort.take().map(UniplexStreamAbort)
    }
}

/// Request progress of a [`UniplexResponse`].
pub trait UniplexResponseProgressExt {
    /// Whether the session began serializing the request's head for writing.
    ///
    /// It stays false if the stream or session failed before that, for example
    /// because the session processed its peer's EOF first. The request then has
    /// not reached the transport, so resending it elsewhere cannot repeat it.
    /// hyper-util's `retry_canceled_requests` calls this an unstarted request.
    /// An HTTP/2 head is serialized when the request is committed.
    fn request_started(&self) -> bool;
}

impl UniplexResponseProgressExt for UniplexResponse {
    fn request_started(&self) -> bool {
        self.exchange.egress_started()
    }
}

/// Request progress of an owned upstream request.
///
/// Implemented for the owned HTTP/1.1 and HTTP/2 upstream submissions and
/// request controls, such as `OwnedH1UpstreamSubmission` and
/// `OwnedH1UpstreamRequest`. A submission and its response's request control
/// read the same state, which stays readable after a failure.
pub trait OwnedRequestProgressExt {
    /// Whether the request failed unprocessed, meaning that resending it on
    /// another connection cannot repeat it.
    ///
    /// A request is unprocessed when its connection rejected it before the
    /// driver committed it, so it was never serialized. That happens once
    /// the connection stops admitting requests: on the peer's EOF or GOAWAY, a
    /// drain, a response that left the connection unusable, or the
    /// connection's failure or cancel. On HTTP/2 a request is also unprocessed
    /// when, before any response head arrived, the peer rejected its stream
    /// with RST_STREAM(`REFUSED_STREAM`) or with a GOAWAY whose last-stream-id
    /// is below the stream's ID, and that refusal was the stream's first
    /// reset. RFC 9113 defines such a request as not processed. hyper-util's
    /// `retry_canceled_requests` similarly resends a request that its
    /// dispatcher never took.
    ///
    /// It stays false while the request is pending, once it succeeds, and
    /// after any other failure of a committed request. It also stays false for
    /// a failure that the request or its consumer caused (a request the driver
    /// rejects, a cancel or a deadline), even when a refusal follows, because
    /// resending would repeat or override it. An unprocessed request fails
    /// with its refusal's error. A cancel, deadline or cancellation that
    /// follows the refusal does not replace it.
    fn request_unprocessed(&self) -> bool;

    /// How many requests the connection committed before this one.
    ///
    /// The value is fixed when the driver commits the request or the
    /// connection rejects it (see `request_unprocessed`). It is `None` before
    /// either and after any other failure before commit. A committed request
    /// keeps its value whatever follows. A nonzero value means the connection
    /// was reused: hyper-util resends an unprocessed request on a newly
    /// acquired connection only when its connection was reused.
    fn sequence_number(&self) -> Option<u64>;
}

#[cfg(test)]
thread_local! {
    static AFTER_REFUSAL_CAUSE_SAMPLE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `hook` once, right after the next refusal on this thread (by a
/// connection or an HTTP/2 peer) samples its request's selected cause. Tests
/// use it to land another thread's cancellation in that window.
#[cfg(test)]
pub(crate) fn after_next_refusal_cause_sample(hook: impl FnOnce() + 'static) {
    AFTER_REFUSAL_CAUSE_SAMPLE.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
pub(crate) fn refusal_cause_sampled() {
    let hook = AFTER_REFUSAL_CAUSE_SAMPLE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

/// HTTP/2 PING probes that detect a dead peer on one session.
///
/// When the interval expires with no probe outstanding, the session sends a
/// PING. If that probe is still unanswered `timeout` later, the session drops
/// the connection with an error that [`is_ping_probe_timeout`] recognizes.
/// The session's first stream starts the interval. Detaching its last stream
/// cancels the interval and any outstanding probe. A matching reply clears
/// the probe and restarts the interval. Reads (with
/// `extend_interval_on_ingress`) and replies arm the interval whether or not
/// streams are open, so an idle session keeps probing while its peer replies,
/// until it closes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PingProbeConfig {
    /// Time from arming the interval to sending a probe. Zero disables probing.
    pub interval: Duration,
    /// How long a probe may go unanswered before the connection is dropped.
    /// Must be nonzero when `interval` is nonzero.
    pub timeout: Duration,
    /// Arms or restarts the interval on every read, whatever the stream count,
    /// unless a probe is outstanding.
    pub extend_interval_on_ingress: bool,
    /// Sends the first probe when the session starts.
    pub immediate: bool,
}

impl Default for PingProbeConfig {
    fn default() -> Self {
        Self {
            interval: Duration::ZERO,
            timeout: Duration::ZERO,
            extend_interval_on_ingress: true,
            immediate: false,
        }
    }
}

/// Marks the error of a probe drop. Stream failures rewrite the error's
/// message but keep its cause.
#[derive(Debug, thiserror::Error)]
#[error("Ping probe timed out")]
struct PingProbeTimedOut;

/// The error with which an unanswered probe drops the connection.
pub(crate) fn ping_probe_timeout_error() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"Ping probe timed out")
        .with_cause(Arc::new(PingProbeTimedOut))
}

/// Whether `error` means the connection was dropped because a PING probe
/// went unanswered.
///
/// Such an error has code `DROPPED` and a private marker as its direct cause.
/// The session's run result and each failed stream's response carry it. The
/// message is not part of the contract (stream errors rewrite it), so an error
/// rebuilt from its code and message is not recognized. Only `error` itself is
/// checked: unwrap an error that wraps it, such as `roxygen_connect`'s
/// `TRANSPORT_READ_ERROR`, through [`Error::source`] first. A failed stream's
/// request-body completion carries only `DROPPED` and is not recognized.
pub fn is_ping_probe_timeout(error: &HttpError) -> bool {
    error.code() == HttpErrorCode::DROPPED
        && error
            .source()
            .is_some_and(|cause| cause.is::<PingProbeTimedOut>())
}
