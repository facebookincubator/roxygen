//! Typed exchange errors: phase, kind, Roxygen error code, and wire reachability.

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
    /// panic after the exchange started and before the hand-off to the session also reports
    /// it, even during the TLS handshake. A tunnel target never reports it.
    Connect,
    /// A tunnel's proxy leg: the dial to the proxy, and its adoption, attachment, and request
    /// reservation.
    ProxyConnect,
    /// A tunnel's CONNECT, from its commit to its final head, and the inner stream's adoption
    /// and attachment. A worker loss or panic after a tunnel exchange started and before the
    /// hand-off to the inner session also reports it, even during the proxy dial or a
    /// handshake.
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
    Timeout,
    Refused,
    Reset,
    RemoteClosed,
    TlsVerification,
    TlsHandshake,
    StreamRefused,
    ExcessiveLoad,
    Protocol,
    /// The exchange's own cancellation token was cancelled.
    Cancelled,
    /// The slot refused the send without dialing.
    Overloaded,
    /// The slot's worker closed, or the runtime shut down.
    WorkerLost,
    /// The exchange panicked on its worker.
    Panicked,
    /// The proxy answered the CONNECT with a final non-2xx head, which `connect_head` holds.
    TunnelRejected,
    Other,
}

/// A typed exchange failure.
#[derive(Debug, thiserror::Error)]
#[error("{phase:?}/{kind:?}: {detail}")]
pub struct ExplicitError {
    pub phase: Phase,
    pub kind: ErrorKind,
    /// `HttpErrorCode::as_u16` when a Roxygen error produced the failure.
    pub code: Option<u16>,
    pub io_kind: Option<io::ErrorKind>,
    pub detail: Box<str>,
    /// False only when the request provably never reached the session, so replaying it on a
    /// fresh connection cannot duplicate it.
    pub reached_wire: bool,
    /// The stages measured before the failure; a `BodyOutcome::Truncated` cause carries its
    /// response's timings. A caught panic keeps only the dial's stages (`queued`,
    /// `tcp_connect`, `tls_handshake`), and a send whose worker dropped its command, taking
    /// the measurements with it, carries none.
    pub timings: Box<Timings>,
    /// A tunnel's CONNECT head: the rejection's for `ErrorKind::TunnelRejected`, and the 2xx
    /// head for every failure after it that the exchange returns. A caught panic and a send
    /// whose worker dropped its command carry none.
    pub connect_head: Option<Box<ConnectHead>>,
}

/// Where in the exchange a Roxygen error was observed.
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
    BeforeHead,
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

    /// Moves the error to `phase`, as a tunnel's proxy dial reports a dial failure.
    pub(crate) fn in_phase(mut self, phase: Phase) -> Self {
        self.phase = phase;
        self
    }

    /// A worker that refused a cleanup reservation for `phase`'s stream.
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

    /// Classifies a failure of a tunnel's CONNECT. A final non-2xx head is `TunnelRejected`:
    /// the typed `ConnectRejected` cause, or a head-wait `CONTENT_LENGTH_MISMATCH`, which the
    /// HTTP/1.1 limit of `DIV-011` reports for a rejection with a body, carrying it.
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

    /// `from_http`'s rules in `phase`. A write failure re-anchors to `Phase::Write` when
    /// `writes_anchor`. An I/O cause that wraps an `HttpError`, as a tunnel reports a proxy
    /// session failure, gives the kind and I/O kind of the wrapped error and its own cause.
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
