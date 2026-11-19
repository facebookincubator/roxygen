/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Stable CONNECT failure kinds, commitment, and tunnel I/O error mapping.

use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;

use http::Response;
use roxygen_client::pool::PoolError;
use roxygen_connect::experimental::connect_rejection;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::message::HttpMessage;
use roxygen_http_adapter::NetworkHeadContext;
use roxygen_http_adapter::response_head_to_http;
use roxygen_session::experimental::is_ping_probe_timeout;

use crate::runtime::DialError;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// The stable classification of a [`ConnectError`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectErrorKind {
    /// `ConnectOptions` failed validation; no worker started.
    InvalidOptions,
    /// The request failed validation before admission, or its connector reports other security
    /// than the live route of its key.
    InvalidRequest,
    /// Every admission permit is held, or the route rejected another waiter.
    Overloaded,
    /// A new route key would exceed `max_routes`, retiring routes included.
    RouteCapacity,
    /// The runtime shut down or its worker was lost, before or after commit.
    Closed,
    /// The proxy completed TLS without negotiating h2, on this attempt or within the last
    /// `h2_unsupported_ttl`.
    H2NotNegotiated,
    /// The connector failed, for example with a refused or reset TCP connection or a TLS or
    /// certificate error.
    Connector,
    /// The acquire window closed, or the session factory timed out the dial the connect waited on.
    AcquireTimeout,
    /// No response head arrived within `response_timeout` after the request committed.
    ResponseTimeout,
    /// The proxy returned a final non-2xx head, available through [`ConnectError::response`].
    Rejected,
    /// The proxy provably did not process the CONNECT and no retry remained.
    Refused,
    /// Any other failure after the request committed, including a reused session's transport
    /// failure before the head once no retry remained.
    Failed,
}

/// A failed `ConnectRuntime::new`, `ConnectClient::connect`, or shutdown.
///
/// Internal Roxygen errors are reachable only through [`Error::source`]. `Debug` redacts the
/// header values of a rejected response.
#[derive(thiserror::Error)]
#[error("{message}")]
pub struct ConnectError {
    kind: ConnectErrorKind,
    committed: bool,
    message: String,
    response: Option<Box<Response<()>>>,
    #[source]
    source: Option<BoxError>,
}

impl ConnectError {
    pub(crate) fn new(kind: ConnectErrorKind, committed: bool, message: impl Into<String>) -> Self {
        Self {
            kind,
            committed,
            message: message.into(),
            response: None,
            source: None,
        }
    }

    pub(crate) fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub(crate) fn invalid_options(reason: &'static str) -> Self {
        Self::new(
            ConnectErrorKind::InvalidOptions,
            false,
            format!("invalid CONNECT options: {reason}"),
        )
    }

    pub(crate) fn invalid_request(reason: &'static str) -> Self {
        Self::new(
            ConnectErrorKind::InvalidRequest,
            false,
            format!("invalid CONNECT request: {reason}"),
        )
    }

    pub(crate) fn overloaded() -> Self {
        Self::new(
            ConnectErrorKind::Overloaded,
            false,
            "CONNECT admission capacity is exhausted",
        )
    }

    pub(crate) fn route_capacity() -> Self {
        Self::new(
            ConnectErrorKind::RouteCapacity,
            false,
            "CONNECT route capacity is exhausted",
        )
    }

    pub(crate) fn closed(committed: bool) -> Self {
        Self::new(
            ConnectErrorKind::Closed,
            committed,
            "CONNECT runtime is closed",
        )
    }

    pub(crate) fn h2_not_negotiated() -> Self {
        Self::new(
            ConnectErrorKind::H2NotNegotiated,
            false,
            "proxy did not negotiate HTTP/2",
        )
    }

    pub(crate) fn acquire_timeout() -> Self {
        Self::new(
            ConnectErrorKind::AcquireTimeout,
            false,
            "CONNECT acquisition timed out",
        )
    }

    pub(crate) fn response_timeout() -> Self {
        Self::new(
            ConnectErrorKind::ResponseTimeout,
            true,
            "CONNECT response head timed out",
        )
    }

    pub(crate) fn rejected(response: Response<()>) -> Self {
        let mut error = Self::new(
            ConnectErrorKind::Rejected,
            true,
            format!("proxy rejected CONNECT with status {}", response.status()),
        );
        error.response = Some(Box::new(response));
        error
    }

    pub(crate) fn failed(message: &'static str) -> Self {
        Self::new(ConnectErrorKind::Failed, true, message)
    }

    /// The stable failure kind.
    pub fn kind(&self) -> ConnectErrorKind {
        self.kind
    }

    /// Whether the proxy may have acted on the CONNECT.
    ///
    /// False for [`ConnectErrorKind::Refused`] and for every failure before the session accepted
    /// the request. True for `ResponseTimeout`, `Rejected`, and `Failed`. For `Closed` it depends
    /// on the phase, and a phase the caller cannot observe counts as committed.
    pub fn committed(&self) -> bool {
        self.committed
    }

    /// The final non-2xx head of a [`ConnectErrorKind::Rejected`] CONNECT, without its body.
    pub fn response(&self) -> Option<&Response<()>> {
        self.response.as_deref()
    }
}

impl fmt::Debug for ConnectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redacted<'a>(&'a Response<()>);

        impl fmt::Debug for Redacted<'_> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct("Response")
                    .field("status", &self.0.status())
                    .field("version", &self.0.version())
                    .field(
                        "headers",
                        &self
                            .0
                            .headers()
                            .keys()
                            .map(|name| name.as_str())
                            .collect::<Vec<_>>(),
                    )
                    .finish_non_exhaustive()
            }
        }

        formatter
            .debug_struct("ConnectError")
            .field("kind", &self.kind)
            .field("committed", &self.committed)
            .field("message", &self.message)
            .field("response", &self.response.as_deref().map(Redacted))
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

/// An attempt's failure and whether the CONNECT may be retried.
pub(crate) struct Classified {
    pub(crate) error: ConnectError,
    pub(crate) retryable: bool,
}

impl Classified {
    fn final_error(error: ConnectError) -> Self {
        Self {
            error,
            retryable: false,
        }
    }
}

/// The session factory ended a dial that outlived its attempt timeout.
#[derive(Debug, thiserror::Error)]
#[error("proxy dial and TLS timed out")]
pub(crate) struct DialTimedOut;

/// The pool cancelled a dial while it drained.
#[derive(Debug, thiserror::Error)]
#[error("proxy dial cancelled")]
pub(crate) struct DialCancelled;

/// Classifies a failed pool acquisition.
pub(crate) fn from_pool(error: PoolError) -> Classified {
    let classified = match &error {
        PoolError::Timeout => ConnectError::acquire_timeout(),
        PoolError::MaxWaiters | PoolError::OwnershipCapacity => ConnectError::overloaded(),
        PoolError::ReservationFailed { .. } => {
            return Classified {
                error: refused("proxy session refused the stream reservation").with_source(error),
                retryable: true,
            };
        }
        PoolError::AttachedConnectFailed { source } => {
            let cause = source.external_cause().cloned();
            return Classified::final_error(match cause {
                Some(cause) => from_dial_cause(cause),
                None => connector_failed().with_source(error),
            });
        }
        PoolError::ConnectFailed { .. }
        | PoolError::SecurityMismatch
        | PoolError::WrongWorker
        | PoolError::InvalidSessionState => connector_failed(),
        PoolError::Draining
        | PoolError::Cancelled
        | PoolError::CleanupRegistration { .. }
        | PoolError::TaskJoin { .. }
        | PoolError::SessionJoin { .. }
        | PoolError::CleanupPanicked
        | PoolError::NativeConfigurationOnly
        | PoolError::InvalidConnectionParams => ConnectError::closed(false),
    };
    Classified::final_error(classified.with_source(error))
}

fn from_dial_cause(cause: Arc<dyn Error + Send + Sync>) -> ConnectError {
    if let Some(dial) = cause.downcast_ref::<DialError>() {
        return match dial {
            DialError::H2NotNegotiated => ConnectError::h2_not_negotiated(),
            DialError::Other(source) => connector_failed().with_source(Arc::clone(source)),
        };
    }
    if cause.is::<DialTimedOut>() {
        return ConnectError::acquire_timeout().with_source(cause);
    }
    if cause.is::<DialCancelled>() {
        return ConnectError::closed(false).with_source(cause);
    }
    connector_failed().with_source(cause)
}

fn connector_failed() -> ConnectError {
    ConnectError::new(ConnectErrorKind::Connector, false, "proxy connector failed")
}

fn refused(message: &'static str) -> ConnectError {
    ConnectError::new(ConnectErrorKind::Refused, false, message)
}

/// Where a native CONNECT failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectPhase {
    /// The connect future failed before it ever suspended, so its request never committed.
    BeforeCommit,
    /// The request committed and the connect was waiting for its final response head.
    AwaitingHead,
}

/// The attempt's session as seen when its CONNECT failed.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SessionState {
    /// The session's transport closed, reset, or failed.
    pub(crate) transport_failed: bool,
    /// The session carried an earlier CONNECT stream.
    pub(crate) reused: bool,
    /// Before the transport failed, the peer reset the attempt's stream with a code other than
    /// `REFUSED_STREAM`, so the failure is that stream reset, not the session's.
    pub(crate) peer_reset: bool,
    /// A response head, informational included, arrived for the attempt's stream.
    pub(crate) response_started: bool,
    /// The session failed on a connection error from the peer, either a protocol violation it
    /// detected or a GOAWAY that carried an error code. This is not a transport failure.
    pub(crate) protocol_error: bool,
    /// The peer refused the attempt's stream: an RST_STREAM(`REFUSED_STREAM`) on it, or a GOAWAY
    /// whose last-stream-id is below it.
    pub(crate) refused: bool,
}

/// Classifies a failed native CONNECT that the caller did not cancel.
pub(crate) fn from_native(
    error: HttpError,
    phase: ConnectPhase,
    session: SessionState,
) -> Classified {
    if let Some(rejection) = connect_rejection(&error) {
        return Classified::final_error(match response_head(rejection.response()) {
            Ok(response) => ConnectError::rejected(response).with_source(error),
            Err(conversion) => ConnectError::failed("proxy rejection head cannot be represented")
                .with_source(conversion),
        });
    }
    if phase == ConnectPhase::BeforeCommit {
        return Classified {
            error: refused("proxy session refused the CONNECT before sending it")
                .with_source(error),
            retryable: true,
        };
    }
    if session.response_started {
        return Classified::final_error(
            ConnectError::failed("CONNECT stream failed after its response started")
                .with_source(error),
        );
    }
    // A stream above a GOAWAY's last-stream-id was not processed whatever the GOAWAY's error code
    // (RFC 9113 section 6.8). That code reaches the streams the GOAWAY covers, so the code alone
    // does not prove a refusal.
    if session.refused {
        return Classified {
            error: refused("proxy session refused the CONNECT").with_source(error),
            retryable: true,
        };
    }
    if session.protocol_error {
        return Classified::final_error(
            ConnectError::failed(
                "proxy ended the session with an error before the CONNECT response",
            )
            .with_source(error),
        );
    }
    if error.code() == HttpErrorCode::READ_TIMEOUT {
        return Classified::final_error(ConnectError::response_timeout().with_source(error));
    }
    let session_failed = session.transport_failed && !session.peer_reset;
    let retryable = session_failed && session.reused;
    let message = if session_failed {
        "proxy session failed before the CONNECT response"
    } else {
        "CONNECT stream failed before its response"
    };
    Classified {
        error: ConnectError::failed(message).with_source(error),
        retryable,
    }
}

/// Converts a final CONNECT response head to `http` types.
pub(crate) fn response_head(
    head: &HttpMessage,
) -> Result<Response<()>, roxygen_http_adapter::HttpAdapterError> {
    Ok(Response::from_parts(
        response_head_to_http(head, NetworkHeadContext::default())?,
        (),
    ))
}

/// Whether `error`, from a native transport read, is a PING probe drop.
///
/// The transport wraps the stream's error in `io::Error::other`, whose `source()` skips it, so
/// the walk starts at the inner error and follows `HttpError` causes, which reach the ingress
/// error inside a `TRANSPORT_READ_ERROR`.
pub(crate) fn is_probe_drop(error: &io::Error) -> bool {
    let mut cause = error.get_ref().map(|inner| inner as &(dyn Error + 'static));
    while let Some(current) = cause {
        let Some(http) = current.downcast_ref::<HttpError>() else {
            return false;
        };
        if is_ping_probe_timeout(http) {
            return true;
        }
        cause = http.source();
    }
    false
}

/// Why an established tunnel ended without completing both directions.
#[derive(Debug)]
pub(crate) enum TunnelFailure {
    /// The native transport failed: a peer reset, GOAWAY, session failure, or probe drop.
    Native(io::Error),
    /// The adapter reset the stream: tunnel drop, job drop, or runtime shutdown.
    Aborted(&'static str),
}

impl TunnelFailure {
    pub(crate) fn kind(&self) -> io::ErrorKind {
        match self {
            Self::Native(error) => {
                let timed_out = error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<HttpError>())
                    .is_some_and(|inner| {
                        matches!(
                            inner.code(),
                            HttpErrorCode::READ_TIMEOUT | HttpErrorCode::WRITE_TIMEOUT
                        )
                    });
                if timed_out || error.kind() == io::ErrorKind::TimedOut || is_probe_drop(error) {
                    io::ErrorKind::TimedOut
                } else {
                    io::ErrorKind::ConnectionReset
                }
            }
            Self::Aborted(_) => io::ErrorKind::ConnectionAborted,
        }
    }

    /// The error every later application read and write of the tunnel reports.
    pub(crate) fn into_io_error(self) -> io::Error {
        let kind = self.kind();
        match self {
            Self::Native(error) => io::Error::new(kind, error),
            Self::Aborted(reason) => io::Error::new(kind, reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::sync::Arc;

    use http::Response;
    use http::StatusCode;
    use roxygen_client::pool::AttachedConnectFailure;
    use roxygen_client::pool::PoolError;
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;

    use super::ConnectError;
    use super::ConnectErrorKind;
    use super::ConnectPhase;
    use super::DialCancelled;
    use super::DialTimedOut;
    use super::SessionState;
    use super::TunnelFailure;
    use super::from_native;
    use super::from_pool;
    use super::is_probe_drop;
    use crate::runtime::DialError;

    fn http(code: HttpErrorCode) -> HttpError {
        HttpError::with_message(code, b"test")
    }

    fn attached(cause: Arc<dyn Error + Send + Sync>) -> PoolError {
        PoolError::AttachedConnectFailed {
            source: AttachedConnectFailure::with_cause(http(HttpErrorCode::CONNECT_ERROR), cause),
        }
    }

    #[test]
    fn pool_errors_map_to_stable_kinds_and_commitment() {
        use ConnectErrorKind::*;
        let table: Vec<(&str, PoolError, ConnectErrorKind)> = vec![
            ("pool wait timeout", PoolError::Timeout, AcquireTimeout),
            ("pool draining at shutdown", PoolError::Draining, Closed),
            ("pool cancelled", PoolError::Cancelled, Closed),
            ("waiter limit", PoolError::MaxWaiters, Overloaded),
            ("ownership bound", PoolError::OwnershipCapacity, Overloaded),
            (
                "reservation",
                PoolError::ReservationFailed {
                    source: http(HttpErrorCode::REFUSED_STREAM),
                },
                Refused,
            ),
            (
                "h2 not negotiated",
                attached(Arc::new(DialError::H2NotNegotiated)),
                H2NotNegotiated,
            ),
            (
                "connector failure",
                attached(Arc::new(DialError::Other(Arc::new(io::Error::from(
                    io::ErrorKind::ConnectionRefused,
                ))))),
                Connector,
            ),
            (
                "factory dial timeout",
                attached(Arc::new(DialTimedOut)),
                AcquireTimeout,
            ),
            (
                "dial cancelled by drain",
                attached(Arc::new(DialCancelled)),
                Closed,
            ),
            (
                "unknown dial cause",
                attached(Arc::new(io::Error::from(io::ErrorKind::Other))),
                Connector,
            ),
            (
                "attachment failure",
                PoolError::ConnectFailed {
                    source: http(HttpErrorCode::CONNECT_ERROR),
                },
                Connector,
            ),
            ("security mismatch", PoolError::SecurityMismatch, Connector),
            ("cleanup panic", PoolError::CleanupPanicked, Closed),
        ];
        for (name, error, kind) in table {
            let classified = from_pool(error);
            assert_eq!(classified.error.kind(), kind, "{name}");
            assert!(
                !classified.error.committed(),
                "{name}: pool failures send nothing"
            );
            assert_eq!(classified.retryable, kind == Refused, "{name}");
        }
        let connector = from_pool(attached(Arc::new(DialError::Other(Arc::new(
            io::Error::new(io::ErrorKind::ConnectionRefused, "refused by test"),
        )))));
        let source = connector.error.source().expect("connector cause");
        assert_eq!(
            source.to_string(),
            "refused by test",
            "the connector's own error is the source"
        );
    }

    #[test]
    fn native_errors_map_to_stable_kinds_and_commitment() {
        use ConnectErrorKind::*;
        let fresh = SessionState::default();
        let reused = SessionState {
            reused: true,
            ..fresh
        };
        let refused_fresh = SessionState {
            refused: true,
            ..fresh
        };
        let refused_reused = SessionState {
            refused: true,
            ..reused
        };
        let failed_fresh = SessionState {
            transport_failed: true,
            ..fresh
        };
        let failed_reused = SessionState {
            transport_failed: true,
            ..reused
        };
        let reset_then_failed_reused = SessionState {
            peer_reset: true,
            ..failed_reused
        };
        let responded_reused = SessionState {
            response_started: true,
            ..reused
        };
        let responded_then_failed_reused = SessionState {
            response_started: true,
            ..failed_reused
        };
        let protocol_error_then_failed_reused = SessionState {
            protocol_error: true,
            ..failed_reused
        };
        let refused_then_protocol_error = SessionState {
            refused: true,
            ..protocol_error_then_failed_reused
        };
        let head = ConnectPhase::AwaitingHead;
        let before = ConnectPhase::BeforeCommit;
        // (name, code, phase, session, kind, committed, retryable)
        let table = [
            (
                "peer REFUSED_STREAM",
                HttpErrorCode::REFUSED_STREAM,
                head,
                refused_reused,
                Refused,
                false,
                true,
            ),
            (
                "GOAWAY above the stream",
                HttpErrorCode::REFUSED_STREAM,
                head,
                refused_fresh,
                Refused,
                false,
                true,
            ),
            (
                "REFUSED_STREAM code of a GOAWAY covering the stream",
                HttpErrorCode::REFUSED_STREAM,
                head,
                protocol_error_then_failed_reused,
                Failed,
                true,
                false,
            ),
            (
                "REFUSED_STREAM code without a refusal of the stream",
                HttpErrorCode::REFUSED_STREAM,
                head,
                reused,
                Failed,
                true,
                false,
            ),
            (
                "stream refused, then a connection error, as above an error GOAWAY",
                HttpErrorCode::REFUSED_STREAM,
                head,
                refused_then_protocol_error,
                Refused,
                false,
                true,
            ),
            (
                "draining session at send",
                HttpErrorCode::REFUSED_STREAM,
                before,
                fresh,
                Refused,
                false,
                true,
            ),
            (
                "admission closed at send",
                HttpErrorCode::DROPPED,
                before,
                reused,
                Refused,
                false,
                true,
            ),
            (
                "cancelled session at send",
                HttpErrorCode::CORO_CANCELLED,
                before,
                fresh,
                Refused,
                false,
                true,
            ),
            (
                "reused transport EOF",
                HttpErrorCode::TRANSPORT_EOF,
                head,
                failed_reused,
                Failed,
                true,
                true,
            ),
            (
                "reused transport reset",
                HttpErrorCode::TRANSPORT_READ_ERROR,
                head,
                failed_reused,
                Failed,
                true,
                true,
            ),
            (
                "reused session DROPPED",
                HttpErrorCode::DROPPED,
                head,
                failed_reused,
                Failed,
                true,
                true,
            ),
            (
                "fresh transport EOF",
                HttpErrorCode::TRANSPORT_EOF,
                head,
                failed_fresh,
                Failed,
                true,
                false,
            ),
            (
                "fresh session DROPPED",
                HttpErrorCode::DROPPED,
                head,
                failed_fresh,
                Failed,
                true,
                false,
            ),
            (
                "peer CANCEL on a live session",
                HttpErrorCode::CANCEL,
                head,
                reused,
                Failed,
                true,
                false,
            ),
            (
                "own stream's peer CANCEL, then transport closed, on a reused session",
                HttpErrorCode::CANCEL,
                head,
                reset_then_failed_reused,
                Failed,
                true,
                false,
            ),
            (
                "103, then reused transport EOF",
                HttpErrorCode::TRANSPORT_EOF,
                head,
                responded_then_failed_reused,
                Failed,
                true,
                false,
            ),
            (
                "103, then peer REFUSED_STREAM",
                HttpErrorCode::REFUSED_STREAM,
                head,
                SessionState {
                    refused: true,
                    ..responded_reused
                },
                Failed,
                true,
                false,
            ),
            (
                "connection protocol error, then reused session failed",
                HttpErrorCode::PROTOCOL_ERROR,
                head,
                protocol_error_then_failed_reused,
                Failed,
                true,
                false,
            ),
            (
                "peer INTERNAL_ERROR",
                HttpErrorCode::INTERNAL_ERROR,
                head,
                fresh,
                Failed,
                true,
                false,
            ),
            (
                "native read timeout",
                HttpErrorCode::READ_TIMEOUT,
                head,
                reused,
                ResponseTimeout,
                true,
                false,
            ),
        ];
        for (name, code, phase, session, kind, committed, retryable) in table {
            let classified = from_native(http(code), phase, session);
            assert_eq!(classified.error.kind(), kind, "{name}");
            assert_eq!(classified.error.committed(), committed, "{name}");
            assert_eq!(classified.retryable, retryable, "{name}");
        }
    }

    #[test]
    fn tunnel_failures_map_to_io_error_kinds() {
        let native = |code| TunnelFailure::Native(io::Error::other(http(code)));
        let table = [
            (
                "peer RST_STREAM",
                native(HttpErrorCode::CANCEL),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "GOAWAY refusal",
                native(HttpErrorCode::REFUSED_STREAM),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "peer GOAWAY error",
                native(HttpErrorCode::PROTOCOL_ERROR),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "transport EOF",
                native(HttpErrorCode::TRANSPORT_EOF),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "session DROPPED",
                native(HttpErrorCode::DROPPED),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "read timeout",
                native(HttpErrorCode::READ_TIMEOUT),
                io::ErrorKind::TimedOut,
            ),
            (
                "write timeout",
                native(HttpErrorCode::WRITE_TIMEOUT),
                io::ErrorKind::TimedOut,
            ),
            (
                "closed egress",
                TunnelFailure::Native(io::Error::from(io::ErrorKind::NotConnected)),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "failed deferred write",
                TunnelFailure::Native(io::Error::from(io::ErrorKind::ConnectionAborted)),
                io::ErrorKind::ConnectionReset,
            ),
            (
                "tunnel dropped",
                TunnelFailure::Aborted("dropped"),
                io::ErrorKind::ConnectionAborted,
            ),
            (
                "job dropped",
                TunnelFailure::Aborted("job"),
                io::ErrorKind::ConnectionAborted,
            ),
        ];
        for (name, failure, kind) in table {
            assert_eq!(failure.kind(), kind, "{name}");
            assert_eq!(failure.into_io_error().kind(), kind, "{name}");
        }
    }

    #[test]
    fn only_marked_probe_drops_map_to_timed_out() {
        let rebuilt = || HttpError::with_message(HttpErrorCode::DROPPED, b"Ping probe timed out");
        let wrapped = |inner: HttpError| {
            HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, inner.describe())
                .with_cause(Arc::new(inner))
        };
        let table = [
            (
                "unmarked DROPPED",
                io::Error::other(http(HttpErrorCode::DROPPED)),
            ),
            (
                "rebuilt from the probe's code and message",
                io::Error::other(rebuilt()),
            ),
            (
                "transport read error around an unmarked DROPPED",
                io::Error::other(wrapped(rebuilt())),
            ),
            (
                "probe message outside an HttpError",
                io::Error::other("Ping probe timed out"),
            ),
            (
                "probe message as an io::Error cause",
                io::Error::new(io::ErrorKind::ConnectionAborted, rebuilt()),
            ),
            ("peer CANCEL", io::Error::other(http(HttpErrorCode::CANCEL))),
            (
                "failed deferred write",
                io::Error::new(io::ErrorKind::ConnectionAborted, "Socket error"),
            ),
        ];
        for (name, error) in table {
            assert!(!is_probe_drop(&error), "{name}");
            assert_eq!(
                TunnelFailure::Native(error).kind(),
                io::ErrorKind::ConnectionReset,
                "{name}: only a real session's marked drop is TimedOut",
            );
        }
    }

    #[test]
    fn rejected_debug_redacts_header_values() {
        let response = Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("x-proxy-reason", "secret-diagnostic")
            .header("set-cookie", "session=private")
            .body(())
            .expect("response");
        let error = ConnectError::rejected(response);
        assert_eq!(error.kind(), ConnectErrorKind::Rejected);
        assert!(error.committed());
        assert_eq!(
            error.response().expect("rejected head").headers()["x-proxy-reason"],
            "secret-diagnostic",
            "the head stays available to the caller",
        );
        let debug = format!("{error:?}");
        assert!(debug.contains("403"), "{debug}");
        assert!(debug.contains("x-proxy-reason"), "{debug}");
        assert!(!debug.contains("secret-diagnostic"), "{debug}");
        assert!(!debug.contains("session=private"), "{debug}");
        assert!(!error.to_string().contains("secret-diagnostic"));
    }

    #[test]
    fn only_provably_unprocessed_failures_are_retryable() {
        let reused = SessionState {
            reused: true,
            ..SessionState::default()
        };
        let refused_reused = SessionState {
            refused: true,
            ..reused
        };
        let failed_reused = SessionState {
            transport_failed: true,
            ..reused
        };
        let failed_fresh = SessionState {
            transport_failed: true,
            ..SessionState::default()
        };
        let reset_then_failed_reused = SessionState {
            peer_reset: true,
            ..failed_reused
        };
        let responded_then_failed_reused = SessionState {
            response_started: true,
            ..failed_reused
        };
        let protocol_error_then_failed_reused = SessionState {
            protocol_error: true,
            ..failed_reused
        };
        let refused_then_protocol_error = SessionState {
            refused: true,
            ..protocol_error_then_failed_reused
        };
        let head = ConnectPhase::AwaitingHead;
        let retryable = [
            (
                "a refusal of the stream",
                from_native(http(HttpErrorCode::REFUSED_STREAM), head, refused_reused),
            ),
            (
                "a stream above the last-stream-id of a GOAWAY with an error code",
                from_native(
                    http(HttpErrorCode::REFUSED_STREAM),
                    head,
                    refused_then_protocol_error,
                ),
            ),
            (
                "draining at send",
                from_native(
                    http(HttpErrorCode::DROPPED),
                    ConnectPhase::BeforeCommit,
                    reused,
                ),
            ),
            (
                "reused transport failure",
                from_native(http(HttpErrorCode::TRANSPORT_EOF), head, failed_reused),
            ),
            (
                "reservation failure",
                from_pool(PoolError::ReservationFailed {
                    source: http(HttpErrorCode::DROPPED),
                }),
            ),
        ];
        for (name, classified) in retryable {
            assert!(classified.retryable, "{name} is provably unprocessed");
        }
        let rejection = HttpError::with_message(HttpErrorCode::CONNECT_ERROR, b"CONNECT failed");
        let final_failures = [
            (
                "fresh transport failure",
                from_native(http(HttpErrorCode::TRANSPORT_EOF), head, failed_fresh),
            ),
            (
                "stream reset after commit",
                from_native(http(HttpErrorCode::CANCEL), head, reused),
            ),
            (
                "own stream reset, then reused transport failure",
                from_native(http(HttpErrorCode::CANCEL), head, reset_then_failed_reused),
            ),
            (
                "connection protocol error, then reused transport failure",
                from_native(
                    http(HttpErrorCode::TRANSPORT_EOF),
                    head,
                    protocol_error_then_failed_reused,
                ),
            ),
            (
                "REFUSED_STREAM code of a GOAWAY covering the stream",
                from_native(
                    http(HttpErrorCode::REFUSED_STREAM),
                    head,
                    protocol_error_then_failed_reused,
                ),
            ),
            (
                "informational head, then reused transport failure",
                from_native(
                    http(HttpErrorCode::TRANSPORT_EOF),
                    head,
                    responded_then_failed_reused,
                ),
            ),
            (
                "non-rejection CONNECT_ERROR",
                from_native(rejection, head, reused),
            ),
            ("pool timeout", from_pool(PoolError::Timeout)),
            (
                "dial failure",
                from_pool(attached(Arc::new(DialError::H2NotNegotiated))),
            ),
            ("dial timeout", from_pool(attached(Arc::new(DialTimedOut)))),
            ("overload", from_pool(PoolError::MaxWaiters)),
            ("shutdown", from_pool(PoolError::Draining)),
        ];
        for (name, classified) in final_failures {
            assert!(!classified.retryable, "{name} is never retried");
        }
    }
}
