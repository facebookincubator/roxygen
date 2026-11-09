//! Roxygen-only APIs that prototype proposed Proxygen APIs.
//!
//! **Experimental.** Each API may change or be removed when Proxygen decides
//! on the API it prototypes; `DIVERGENCES.md` records each divergence and its
//! retirement.
//!
//! - `DIV-023`: [`UniplexStreamAbort`], the abort of one committed stream. It
//!   prototypes a stream-scoped reset on `HTTPSessionContext` that calls
//!   `HTTPCoroSession::egressResetStream` with `CANCEL` whether or not the
//!   stream's egress EOM was sent, which frozen Proxygen does not have. It
//!   backs the proposed `HTTPConnectAsyncTransport::closeWithReset` and the
//!   opt-in reset-on-cancel parameter of `HTTPConnectStream::connect` and
//!   `connectUnique`.
//! - `DIV-024`: [`PingProbeConfig`] and [`is_ping_probe_timeout`], HTTP/2 PING
//!   probes with the semantics of non-coro `HTTPSession::PingProber`. They
//!   prototype probe interval, timeout, `extendIntervalOnIngress`, and
//!   `immediate` fields on `HTTPCoroConnector::SessionParams` and
//!   `HTTPUniplexTransportSession::enablePingProbes`, and retire once
//!   Proxygen ships equivalent coro probes and the reference revision is
//!   re-frozen past them.
//! - `DIV-035`: [`UniplexResponseProgressExt`], whether a committed request's
//!   head was serialized. It prototypes a request-progress query on the coro
//!   client's response that would let a caller resend a request that failed
//!   before any of it was written, as Hyper's legacy client does for reused
//!   connections, which frozen Proxygen does not have.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;

use crate::uniplex::StreamAbort;
use crate::uniplex::UniplexResponse;

/// A one-use, owner-local reset of one committed stream.
///
/// It holds its session weakly and is not `Send`. Dropping it changes nothing.
pub struct UniplexStreamAbort(pub(crate) StreamAbort);

impl UniplexStreamAbort {
    /// Resets the stream with `CANCEL`, as `egressResetStream` does: one
    /// RST_STREAM on HTTP/2, also after the request EOM, and the connection's
    /// existing drain on HTTP/1.1. The response reader's pending or next read
    /// fails with `CANCEL`, queued request egress fails, and the stream's
    /// unread response bytes are discarded with their connection credit.
    ///
    /// A stream whose response END_STREAM arrived and whose request EOM was
    /// sent gets no frame. A stream that either side already reset, a retired
    /// stream, and a session that failed or finished are left unchanged. An
    /// HTTP/1.1 tunnel that keeps reading after its request EOM closed the
    /// connection's writes still resets the connection.
    ///
    /// Call it from owner-local code. A session callout, such as a request
    /// source's callback that the session runs while it processes a stream,
    /// cannot reach the stream, so an abort made there changes nothing and
    /// the capability is used up. Lifecycle observers run outside callouts and
    /// may abort. A panic of a lifecycle observer, a waker, or the request
    /// source's `stop_reading` that the abort runs resumes after the stream
    /// is reset and its parked reader is woken; later panics are discarded.
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

/// Request progress of a [`UniplexResponse`] (`DIV-035`).
pub trait UniplexResponseProgressExt {
    /// Whether the session began serializing the request's head for writing.
    /// It stays false if the stream or session failed before that (for
    /// example, the session saw its peer's EOF first), so none of the request
    /// reached the transport and resending it elsewhere cannot repeat it: what
    /// hyper-util's `retry_canceled_requests` calls an unstarted request. An
    /// HTTP/2 head is serialized when the request is committed.
    fn request_started(&self) -> bool;
}

impl UniplexResponseProgressExt for UniplexResponse {
    fn request_started(&self) -> bool {
        self.exchange.egress_started()
    }
}

/// Opt-in HTTP/2 PING probes for one session, the arguments of frozen
/// `HTTPSession::enablePingProbes` (`DIV-024`).
///
/// An expiry of the interval with no probe outstanding sends a PING; an expiry
/// with that probe still unanswered `timeout` later drops the connection with
/// an error that [`is_ping_probe_timeout`] recognizes. The session's first
/// transaction starts the interval, the detach of its last cancels the
/// interval and any outstanding probe, and a matching reply clears the probe
/// and restarts the interval. Reads (with `extend_interval_on_ingress`) and
/// replies arm the interval whether or not streams are open, so a session
/// with none keeps probing while its peer acknowledges, until it closes. A
/// zero `interval` disables probing; a nonzero interval requires a nonzero
/// `timeout`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PingProbeConfig {
    pub interval: Duration,
    pub timeout: Duration,
    /// Arms or restarts the interval on every read while no probe is
    /// outstanding, whatever the stream count.
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

/// Whether `error` is the drop of a connection whose PING probe went
/// unanswered: code `DROPPED` with the drop's private marker as its own
/// cause. The session's run result and each failed stream's response carry
/// it. The message is not part of the contract (stream errors rewrite it), so
/// an error rebuilt from its code and message is not recognized. Only `error`
/// itself is checked: a caller holding an error that wraps it, such as
/// `roxygen_connect`'s `TRANSPORT_READ_ERROR`, unwraps it through
/// [`Error::source`] first. A failed stream's request-body completion carries
/// only `DROPPED`, as frozen `stopReading` does, and is not recognized.
pub fn is_ping_probe_timeout(error: &HttpError) -> bool {
    error.code() == HttpErrorCode::DROPPED
        && error
            .source()
            .is_some_and(|cause| cause.is::<PingProbeTimedOut>())
}
