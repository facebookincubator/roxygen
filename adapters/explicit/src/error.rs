/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Typed exchange errors: phase, kind, HTTP error code, and wire reachability.

use std::error::Error as _;
use std::io;

use roxygen_connect::experimental::connect_rejection;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_runtime::worker::CleanupRegistrationError;

use crate::exchange::Timings;
use crate::tunnel::ConnectHead;

/// The exchange position a failure was observed in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    /// Before the dial: input checks, slot admission, and queueing.
    Submit,
    /// TCP connect, and adoption or attachment of the established stream. A worker loss or
    /// panic between the exchange's start and its hand-off to the session is also reported in
    /// this phase, even during the TLS handshake. A tunnel target never uses this phase.
    Connect,
    /// A tunnel's proxy leg: the dial to the proxy, and its adoption, attachment, and request
    /// reservation.
    ProxyConnect,
    /// A tunnel's CONNECT, from its commit to its final head, and the inner stream's adoption
    /// and attachment. A worker loss or panic between a tunnel exchange's start and its
    /// hand-off to the inner session is also reported in this phase, even during the proxy
    /// dial or a handshake.
    TunnelConnect,
    /// The TLS handshake.
    Tls,
    /// A request write after the stream was attached.
    Write,
    /// After attachment and before the final response head.
    Read,
    /// After the final response head.
    Body,
}

/// Why an exchange failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// The deadline expired, or a dial, read or write timed out.
    Timeout,
    /// The TCP connection was refused.
    Refused,
    /// The connection was reset or aborted.
    Reset,
    /// The peer closed the connection.
    RemoteClosed,
    /// The peer's certificate failed verification.
    TlsVerification,
    /// The TLS handshake failed for another reason.
    TlsHandshake,
    /// The stream was refused (`REFUSED_STREAM`).
    StreamRefused,
    /// A local resource limit was reached (`ENHANCE_YOUR_CALM`), such as a full worker command
    /// queue.
    ExcessiveLoad,
    /// A protocol violation, or input that this API rejects. A message that the HTTP/1.1 codec
    /// cannot parse is `Other` instead, with code `HEADER_PARSE_ERROR` or `BODY_PARSE_ERROR`.
    Protocol,
    /// The exchange's own cancellation token was cancelled.
    Cancelled,
    /// The slot rejected the send without dialing.
    Overloaded,
    /// The slot's worker closed, or the runtime shut down.
    WorkerLost,
    /// The exchange panicked on its worker.
    Panicked,
    /// The proxy responded to the CONNECT with a final non-2xx head, held in `connect_head`.
    TunnelRejected,
    /// Any other failure.
    Other,
}

/// A typed exchange failure.
#[derive(Debug, thiserror::Error)]
#[error("{phase:?}/{kind:?}: {detail}")]
pub struct ExplicitError {
    /// Where in the exchange the failure was observed.
    pub phase: Phase,
    /// Why the exchange failed.
    pub kind: ErrorKind,
    /// `HttpErrorCode::as_u16` when an `HttpError` produced the failure.
    pub code: Option<u16>,
    /// The kind of the underlying I/O error, if there is one.
    pub io_kind: Option<io::ErrorKind>,
    /// A human-readable description.
    pub detail: Box<str>,
    /// False only when the request provably never reached the session, so replaying it on a
    /// fresh connection cannot duplicate it.
    pub reached_wire: bool,
    /// The stages measured before the failure. For a `BodyOutcome::Truncated` cause these are
    /// the response's timings. After a caught panic only the dial stages (`queued`,
    /// `tcp_connect`, `tls_handshake`) remain. When the worker dropped the send's command,
    /// `queued` is zero and every other stage is `None`.
    pub timings: Box<Timings>,
    /// A tunnel's CONNECT response head. For `ErrorKind::TunnelRejected` it is the rejection,
    /// and for a later failure that the exchange returns as `Err` it is the 2xx head. A
    /// `BodyOutcome::Truncated` cause does not carry it, because `Response::connect_head` does.
    /// It is `None` after a caught panic or when the worker dropped the send's command.
    pub connect_head: Option<Box<ConnectHead>>,
}

/// Where in the exchange an `HttpError` was observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Position {
    /// Adopting or attaching the established stream.
    Attach,
    /// Adopting or attaching a tunnel's proxy stream, or reserving its CONNECT.
    ProxyAttach,
    /// Adopting or attaching a tunnel's inner stream.
    TunnelAttach,
    /// A tunnel's CONNECT, from its commit to its final head.
    Connect,
    /// After attachment and before the final response head.
    BeforeHead,
    /// After the final response head.
    AfterHead,
}

/// Exchange state that changes how an error classifies.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Observed {
    pub(crate) own_cancelled: bool,
    pub(crate) worker_open: bool,
    pub(crate) deadline_passed: bool,
}

impl ExplicitError {
    pub(crate) fn new(phase: Phase, kind: ErrorKind, detail: impl Into<Box<str>>) -> Self {
        Self {
            phase,
            kind,
            code: None,
            io_kind: None,
            detail: detail.into(),
            reached_wire: false,
            timings: Box::default(),
            connect_head: None,
        }
    }

    /// Moves the error to `phase`, for example a proxy dial failure to `Phase::ProxyConnect`.
    pub(crate) fn in_phase(mut self, phase: Phase) -> Self {
        self.phase = phase;
        self
    }

    /// The error for a worker that rejected a cleanup reservation for `phase`'s stream.
    pub(crate) fn from_cleanup_registration(
        error: &CleanupRegistrationError,
        phase: Phase,
    ) -> Self {
        let kind = match error {
            CleanupRegistrationError::Closed => ErrorKind::WorkerLost,
            _ => ErrorKind::Other,
        };
        Self::new(phase, kind, error.to_string())
    }

    pub(crate) fn reached(mut self, reached_wire: bool) -> Self {
        self.reached_wire = reached_wire;
        self
    }

    /// The deadline expired in `phase`.
    pub(crate) fn expired(phase: Phase) -> Self {
        Self::new(phase, ErrorKind::Timeout, "request deadline expired")
    }

    /// Whether this is a deadline expiry, which alone carries neither a code nor an I/O kind.
    pub(crate) fn is_expiry(&self) -> bool {
        self.kind == ErrorKind::Timeout && self.code.is_none() && self.io_kind.is_none()
    }

    pub(crate) fn from_connect_io(error: &io::Error) -> Self {
        let kind = match error.kind() {
            io::ErrorKind::ConnectionRefused => ErrorKind::Refused,
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted => ErrorKind::Reset,
            io::ErrorKind::TimedOut => ErrorKind::Timeout,
            _ => ErrorKind::Other,
        };
        Self {
            io_kind: Some(error.kind()),
            ..Self::new(Phase::Connect, kind, error.to_string())
        }
    }

    /// Classifies a TLS handshake failure, which tokio-rustls reports as an `io::Error` that may
    /// carry a `rustls::Error`.
    pub(crate) fn from_tls_io(error: &io::Error) -> Self {
        if let Some(tls) = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        {
            return Self::from_rustls(tls);
        }
        let kind = match error.kind() {
            io::ErrorKind::TimedOut => ErrorKind::Timeout,
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted => ErrorKind::Reset,
            io::ErrorKind::UnexpectedEof => ErrorKind::RemoteClosed,
            _ => ErrorKind::Other,
        };
        Self {
            io_kind: Some(error.kind()),
            ..Self::new(Phase::Tls, kind, error.to_string())
        }
    }

    /// Classifies a failure of the TLS handshake inside a tunnel. The tunnel reports a proxy
    /// session failure as an `io::Error` that wraps its `HttpError`, which classifies as
    /// `from_http` would, in `Phase::Tls`.
    pub(crate) fn from_tunnel_tls_io(error: &io::Error, observed: Observed) -> Self {
        match wrapped_http(error) {
            Some(http) => Self::classify(http, Phase::Tls, false, false, observed),
            None => Self::from_tls_io(error).observed(observed),
        }
    }

    pub(crate) fn from_rustls(error: &rustls::Error) -> Self {
        let kind = match error {
            rustls::Error::InvalidCertificate(_) => ErrorKind::TlsVerification,
            _ => ErrorKind::TlsHandshake,
        };
        Self::new(Phase::Tls, kind, error.to_string())
    }

    pub(crate) fn from_http(error: &HttpError, position: Position, observed: Observed) -> Self {
        let (phase, reached_wire) = match position {
            Position::Attach => (Phase::Connect, false),
            Position::ProxyAttach => (Phase::ProxyConnect, false),
            Position::TunnelAttach | Position::Connect => (Phase::TunnelConnect, false),
            Position::BeforeHead => (Phase::Read, true),
            Position::AfterHead => (Phase::Body, true),
        };
        Self::classify(error, phase, reached_wire, reached_wire, observed)
    }

    /// Classifies a failure of a tunnel's CONNECT. A final non-2xx head is `TunnelRejected`.
    /// The head comes from the typed `ConnectRejected` cause, or from a head-wait
    /// `CONTENT_LENGTH_MISMATCH` that carries a final non-2xx head.
    pub(crate) fn from_connect(error: &HttpError, observed: Observed) -> Self {
        let head = match connect_rejection(error) {
            Some(rejected) => Some(ConnectHead::from_message(
                rejected.status_code(),
                rejected.response(),
            )),
            None if error.code() == HttpErrorCode::CONTENT_LENGTH_MISMATCH => {
                error.http_message().and_then(|head| {
                    let status = head.response_status()?;
                    let is_final = status == 101 || status / 100 != 1;
                    (is_final && status / 100 != 2).then(|| ConnectHead::from_message(status, head))
                })
            }
            None => None,
        };
        let Some(head) = head else {
            return Self::from_http(error, Position::Connect, observed);
        };
        Self {
            code: Some(error.code().as_u16()),
            connect_head: Some(Box::new(head)),
            ..Self::new(
                Phase::TunnelConnect,
                ErrorKind::TunnelRejected,
                String::from_utf8_lossy(&error.describe()).into_owned(),
            )
        }
        .observed(observed)
    }

    /// Applies `from_http`'s rules in `phase`. With `writes_anchor`, a write failure moves to
    /// `Phase::Write`. When the I/O cause wraps an `HttpError`, as a tunnel's proxy session
    /// failure does, the kind and I/O kind come from the wrapped error and its own cause.
    fn classify(
        error: &HttpError,
        phase: Phase,
        reached_wire: bool,
        writes_anchor: bool,
        observed: Observed,
    ) -> Self {
        let code = error.code();
        let cause = io_cause(error);
        let (rule_code, io_kind) = match cause.and_then(wrapped_http) {
            Some(wrapped) => (wrapped.code(), io_cause(wrapped).map(io::Error::kind)),
            None => (code, cause.map(io::Error::kind)),
        };
        let timeout =
            rule_code == HttpErrorCode::READ_TIMEOUT || rule_code == HttpErrorCode::WRITE_TIMEOUT;
        if timeout && observed.deadline_passed {
            return Self::expired(phase)
                .reached(reached_wire)
                .observed(observed);
        }
        let phase = if writes_anchor
            && (code == HttpErrorCode::WRITE_TIMEOUT
                || code == HttpErrorCode::TRANSPORT_WRITE_ERROR)
        {
            Phase::Write
        } else {
            phase
        };
        let reset = matches!(
            io_kind,
            Some(io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted)
        );
        let kind = match rule_code {
            _ if timeout => ErrorKind::Timeout,
            HttpErrorCode::TRANSPORT_READ_ERROR | HttpErrorCode::TRANSPORT_WRITE_ERROR if reset => {
                ErrorKind::Reset
            }
            HttpErrorCode::TRANSPORT_EOF => ErrorKind::RemoteClosed,
            HttpErrorCode::REFUSED_STREAM => ErrorKind::StreamRefused,
            HttpErrorCode::ENHANCE_YOUR_CALM => ErrorKind::ExcessiveLoad,
            HttpErrorCode::PROTOCOL_ERROR => ErrorKind::Protocol,
            HttpErrorCode::CANCEL | HttpErrorCode::CORO_CANCELLED if observed.own_cancelled => {
                ErrorKind::Cancelled
            }
            _ => ErrorKind::Other,
        };
        Self {
            code: Some(code.as_u16()),
            io_kind,
            reached_wire,
            ..Self::new(
                phase,
                kind,
                String::from_utf8_lossy(&error.describe()).into_owned(),
            )
        }
        .observed(observed)
    }

    /// An error seen after the worker left `Open` is `WorkerLost`, unless the exchange's own
    /// token was cancelled.
    pub(crate) fn observed(mut self, observed: Observed) -> Self {
        if !observed.worker_open && !observed.own_cancelled {
            self.kind = ErrorKind::WorkerLost;
        }
        self
    }
}

fn io_cause(error: &HttpError) -> Option<&io::Error> {
    error.source()?.downcast_ref::<io::Error>()
}

/// The `HttpError` that an `io::Error` wraps, as `HttpConnectAsyncTransport` reports a proxy
/// session read failure.
fn wrapped_http(error: &io::Error) -> Option<&HttpError> {
    error.get_ref()?.downcast_ref::<HttpError>()
}
