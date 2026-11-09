//! Experimental owned client sessions (`DIV-046`).
//!
//! [`OwnedClientSession`] runs one client connection on an owned upstream driver,
//! `OwnedH1UpstreamSession` for HTTP/1.1 or `OwnedH2UpstreamSession` for HTTP/2, which polls
//! the codec, the request uploads and the response sources from one future per connection,
//! with no task per request. It takes `ClientSession`'s inputs ([`ConnectionParams`],
//! [`SessionParams`], [`ClientAttachOptions`]) and keeps its lifetime contract: the driver's
//! run is rooted in a worker scope rather than in the handle, the attachment's cancellation
//! cancels the session, worker quiescence closes its admission, and
//! [`OwnedClientSession::wait_closed`] observes the run's outcome. Either closes admission at
//! once, before the run observes it; the attachment's cancellation wins over quiescence and is
//! observed until the run's cleanup completes, and a panic in the quiescence drain cancels the
//! session and fails it `INTERNAL_ERROR`, as for a `ClientSession`. A reservation's submission
//! resolves at the driver's commitment, or at the first response head, with the response's
//! fresh [`HttpResponseSource`]; submissions and requests report `OwnedRequestProgressExt`
//! progress (`DIV-035`).
//!
//! Refused before any dial, with `INVALID_STATE_TRANSITION` and an error that names the
//! field, instead of ignored:
//!
//! - on HTTP/1.1, session timers: `stream_read_timeout`, `connection_read_timeout` and
//!   `write_timeout` must each be `Some(Duration::ZERO)`, since `None` selects
//!   `UniplexConfig`'s nonzero default;
//! - `lifecycle_observer`, `session_stats`, `header_codec_stats`, and `ping_probes` with a
//!   nonzero interval;
//! - on HTTP/2, `deliver_ingress_before_abort` (`DIV-033`) and an `ENABLE_PUSH` setting
//!   other than 0;
//! - TLS: `connect` dials plaintext TCP only, and `attach` refuses `non_abortive_close` on a
//!   TLS transport, as `ClientSession::attach_transport` does.
//!
//! Honored as `ClientSession` honors them: on HTTP/2 `settings`,
//! `max_concurrent_outgoing_streams`, `connection_flow_control`, `header_indexing_strategy`
//! and all three session timers, except that `ENABLE_PUSH=0` joins settings that carry none,
//! where `ClientSession` advertises 1; on HTTP/1.1 `deliver_ingress_before_abort` and
//! `unframed_bodiless_http1_requests` (`DIV-036`); `non_abortive_close` (`DIV-038`) on the
//! plaintext transport. HTTP/1.1 sessions read before committing a request, fail an
//! unstarted request when the peer closes, and batch ready output, as uniplex sessions
//! do (`DIV-035`, `DIV-034`). HTTP/1.1 sessions ignore the HTTP/2-only fields.
//!
//! Dropping every handle leaves the connection open until `close_when_idle`, `cancel`, its
//! peer or its worker's quiescence closes it, where dropping the last `ClientSession`
//! drains it (`DIV-020`): a drain would refuse a submission the driver has not committed yet.

use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use roxygen_codec::CodecSetting;
use roxygen_core::body_event_queue::HttpBodySource;
use roxygen_core::connection::ConnectionContext;
use roxygen_core::connection::TransportSecurity;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::http::CodecProtocol;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::SocketAddress;
use roxygen_core::stream_source::HttpResponseSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerQuiescing;
use roxygen_session::experimental::OwnedRequestProgressExt;
use roxygen_session::http2::Http2SessionConfig;
use roxygen_session::owned_h1::Http1DriverConfig;
use roxygen_session::owned_h1::OwnedH1ExclusiveSubmission;
use roxygen_session::owned_h1::OwnedH1Options;
use roxygen_session::owned_h1::OwnedH1UpstreamHandle;
use roxygen_session::owned_h1::OwnedH1UpstreamRequest;
use roxygen_session::owned_h1::OwnedH1UpstreamReservation;
use roxygen_session::owned_h1::OwnedH1UpstreamSession;
use roxygen_session::owned_h2::OwnedH2ExclusiveSubmission;
use roxygen_session::owned_h2::OwnedH2Options;
use roxygen_session::owned_h2::OwnedH2UpstreamHandle;
use roxygen_session::owned_h2::OwnedH2UpstreamRequest;
use roxygen_session::owned_h2::OwnedH2UpstreamReservation;
use roxygen_session::owned_h2::OwnedH2UpstreamSession;
use roxygen_session::uniplex::UniplexConfig;
use roxygen_session::uniplex::UniplexRunSummary;
use roxygen_session::uniplex::UniplexTransport;
use roxygen_transport::byte_stream::OwnedByteStream;

use crate::connector::ClientAttachOptions;
use crate::connector::ClientTransportOptions;
use crate::connector::ConnectionParams;
use crate::connector::LocalClientFuture;
use crate::connector::SessionCompletion;
use crate::connector::SessionCompletionPublisher;
use crate::connector::SessionParams;
use crate::connector::check_attachment_admission;
use crate::connector::client_byte_stream_limits;
use crate::connector::client_error;
use crate::connector::connection_context;
use crate::connector::dial_plaintext;
use crate::connector::http2_session_config;
use crate::connector::non_abortive_close_with_tls;
use crate::connector::uniplex_config_for_session_params;
use crate::connector::wait_session_completion;

/// HTTP/2 `SETTINGS_ENABLE_PUSH`.
const ENABLE_PUSH: u16 = 2;

/// A cloneable owner-local handle for one client connection on an owned
/// upstream driver.
#[derive(Clone)]
pub struct OwnedClientSession {
    inner: Rc<SessionInner>,
}

struct SessionInner {
    upstream: Upstream,
    protocol: CodecProtocol,
    connection: Arc<ConnectionContext>,
    completion: SessionCompletion,
    attachment: CancellationToken,
    worker: WorkerHandle,
}

#[derive(Clone)]
enum Upstream {
    Http1(OwnedH1UpstreamHandle),
    Http2(OwnedH2UpstreamHandle),
}

impl Upstream {
    fn close_when_idle(&self) -> Result<bool, HttpError> {
        match self {
            Self::Http1(handle) => handle.close_when_idle(),
            Self::Http2(handle) => handle.close_when_idle(),
        }
    }

    fn cancel(&self) {
        match self {
            Self::Http1(handle) => handle.cancel(),
            Self::Http2(handle) => handle.cancel(),
        }
    }
}

impl OwnedClientSession {
    /// Dials `address` over plaintext TCP with `connection`'s TCP options and starts the
    /// owned driver for `connection.plaintext_protocol`, as `HttpConnector::connect` starts
    /// a `ClientSession`. Parameters the driver cannot honor, and TLS, are refused before
    /// the dial.
    pub fn connect(
        context: &WorkerContext,
        address: SocketAddress,
        timeout: Duration,
        connection: ConnectionParams,
        session: SessionParams,
        cancellation: CancellationToken,
    ) -> LocalClientFuture<Result<Self, HttpError>> {
        let context = context.clone();
        Box::pin(async move {
            check_attachment_admission(&context, &cancellation)?;
            if matches!(connection.transport, ClientTransportOptions::Tls(_)) {
                return Err(refusal(
                    "owned client sessions do not support TLS: they dial plaintext TCP",
                ));
            }
            let config = DriverConfig::new(connection.plaintext_protocol, &session)?;
            let post_connect_cancellation = cancellation.clone();
            let (transport, metadata) = dial_plaintext(
                &context,
                address,
                client_byte_stream_limits(),
                timeout,
                cancellation,
                connection.tcp_options,
                session.non_abortive_close,
            )
            .await?;
            check_attachment_admission(&context, &post_connect_cancellation)?;
            start(
                &context,
                Rc::new(transport),
                config,
                metadata,
                CancellationToken::never(),
            )
        })
    }

    /// Consumes an established transport on its owner worker, as
    /// `ClientSession::attach_transport` does, and starts the owned driver for
    /// `options.protocol`. Cancelling `options.cancellation` cancels the session.
    pub fn attach(
        worker: &WorkerContext,
        transport: OwnedByteStream,
        options: ClientAttachOptions,
    ) -> Result<Self, HttpError> {
        check_attachment_admission(worker, &options.cancellation)?;
        let mut config = DriverConfig::new(options.protocol, &options.session)?;
        if options.session.non_abortive_close {
            if options.security == TransportSecurity::Tls {
                return Err(non_abortive_close_with_tls());
            }
            transport.enable_non_abortive_close(worker)?;
        }
        let limits = transport.limits(worker)?;
        let metadata = connection_context(
            worker,
            &transport,
            options.security,
            options.upstream_origin,
        )?;
        let io = config.io_mut();
        io.maximum_read_size = io.maximum_read_size.min(limits.maximum_read_size());
        io.read_allocation_size = io.read_allocation_size.min(limits.maximum_read_size());
        io.maximum_write_size = io.maximum_write_size.min(limits.maximum_write_size());
        start(
            worker,
            Rc::new(transport),
            config,
            metadata,
            options.cancellation,
        )
    }

    /// The wire protocol of the session's driver.
    pub fn protocol(&self) -> CodecProtocol {
        self.inner.protocol
    }

    /// Immutable connection metadata that can cross a thread boundary.
    pub fn connection_context(&self) -> &Arc<ConnectionContext> {
        &self.inner.connection
    }

    /// Requests the connection can admit now: 0 once the attachment is
    /// cancelled or the worker stops accepting work; otherwise on HTTP/1.1 1
    /// while its one slot is free and admission is open, and on HTTP/2 the
    /// lower of its own and its peer's `MAX_CONCURRENT_STREAMS`, less the
    /// reserved and active streams.
    pub fn available_capacity(&self) -> u32 {
        if self.admission_closed() {
            return 0;
        }
        match &self.inner.upstream {
            Upstream::Http1(handle) => handle.available_capacity(),
            Upstream::Http2(handle) => handle.available_capacity(),
        }
    }

    /// No reservation, exchange, upload or write is outstanding.
    pub fn is_idle(&self) -> bool {
        match &self.inner.upstream {
            Upstream::Http1(handle) => handle.is_idle(),
            Upstream::Http2(handle) => handle.is_idle(),
        }
    }

    /// The driver's run has completed.
    pub fn is_finished(&self) -> bool {
        match &self.inner.upstream {
            Upstream::Http1(handle) => handle.is_finished(),
            Upstream::Http2(handle) => handle.is_finished(),
        }
    }

    /// Takes one admission slot. Fails with `DROPPED` once the attachment is
    /// cancelled or the worker stops accepting work, as
    /// `ClientSession::reserve_request` does, before the run observes either,
    /// and otherwise with `REFUSED_STREAM` when no slot is available.
    pub fn reserve_request(&self) -> Result<OwnedClientReservation, HttpError> {
        if self.admission_closed() {
            return Err(client_error(
                HttpErrorCode::DROPPED,
                b"client session admission closed",
            ));
        }
        Ok(OwnedClientReservation(match &self.inner.upstream {
            Upstream::Http1(handle) => Reservation::Http1(handle.reserve_request()?),
            Upstream::Http2(handle) => Reservation::Http2(handle.reserve_request()?),
        }))
    }

    /// Closes admission and closes the connection once its exchanges retire.
    /// Returns whether this call changed the session's state.
    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        self.inner.upstream.close_when_idle()
    }

    /// Fails the connection with `CORO_CANCELLED`: admission closes, live
    /// exchanges fail and the transport resets.
    pub fn cancel(&self) {
        self.inner.upstream.cancel();
    }

    /// Observes the driver's outcome once its run, I/O cleanup included, has
    /// completed. Dropping this observation changes nothing.
    pub fn wait_closed(&self) -> LocalClientFuture<Result<UniplexRunSummary, HttpError>> {
        wait_session_completion(&self.inner.completion)
    }

    /// As `ClientSession`'s admission check, ahead of the run's next poll.
    fn admission_closed(&self) -> bool {
        self.inner.attachment.is_cancellation_requested() || !self.inner.worker.is_accepting()
    }
}

/// The driver configuration a session's parameters select.
enum DriverConfig {
    Http1(UniplexConfig),
    Http2(UniplexConfig, Http2SessionConfig),
}

impl DriverConfig {
    /// Refuses, naming the field, what the owned drivers cannot honor.
    fn new(protocol: CodecProtocol, session: &SessionParams) -> Result<Self, HttpError> {
        let (http2, name) = match protocol {
            CodecProtocol::Http11 => (false, "HTTP/1.1"),
            CodecProtocol::Http2 => (true, "HTTP/2"),
            _ => {
                return Err(client_error(
                    HttpErrorCode::CONNECT_ERROR,
                    b"unsupported attachment protocol",
                ));
            }
        };
        let timers = [
            ("stream_read_timeout", session.stream_read_timeout),
            ("connection_read_timeout", session.connection_read_timeout),
            ("write_timeout", session.write_timeout),
        ];
        if !http2
            && let Some((field, _)) = timers
                .iter()
                .find(|(_, timeout)| *timeout != Some(Duration::ZERO))
        {
            return Err(refusal(format!(
                "owned HTTP/1.1 client sessions are untimed: {field} must be Some(Duration::ZERO)"
            )));
        }
        let unsupported = [
            ("lifecycle_observer", session.lifecycle_observer.is_some()),
            ("session_stats", session.session_stats.is_some()),
            ("header_codec_stats", session.header_codec_stats.is_some()),
            (
                "ping_probes",
                session
                    .ping_probes
                    .is_some_and(|probes| !probes.interval.is_zero()),
            ),
            (
                "deliver_ingress_before_abort",
                http2 && session.deliver_ingress_before_abort,
            ),
        ];
        if let Some((field, _)) = unsupported.iter().find(|(_, refused)| *refused) {
            return Err(refusal(format!(
                "owned {name} client sessions do not support {field}"
            )));
        }
        let io = uniplex_config_for_session_params(session);
        if !http2 {
            return Ok(Self::Http1(io));
        }
        let mut http2 = http2_session_config(session);
        match http2
            .settings
            .iter()
            .find(|setting| setting.identifier == ENABLE_PUSH)
        {
            None => http2.settings.push(CodecSetting {
                identifier: ENABLE_PUSH,
                value: 0,
            }),
            Some(setting) if setting.value == 0 => {}
            Some(_) => {
                return Err(refusal(
                    "owned HTTP/2 client sessions accept no server push: ENABLE_PUSH must be 0",
                ));
            }
        }
        Ok(Self::Http2(io, http2))
    }

    fn io_mut(&mut self) -> &mut UniplexConfig {
        match self {
            Self::Http1(io) | Self::Http2(io, _) => io,
        }
    }
}

fn refusal(message: impl AsRef<[u8]>) -> HttpError {
    client_error(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

/// Starts the driver on its own worker scope, which publishes its outcome.
/// Cancelling `attachment` cancels the session.
fn start(
    context: &WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    config: DriverConfig,
    connection: Arc<ConnectionContext>,
    attachment: CancellationToken,
) -> Result<OwnedClientSession, HttpError> {
    let completion = SessionCompletion::default();
    let completed = SessionCompletionPublisher::new(&completion);
    let scope = RunScope {
        attachment: attachment.wake_registration(),
        quiescing: context.quiescing(),
        completed,
        aborting: false,
        draining: false,
        failure: None,
    };
    let (upstream, protocol, registration) = match config {
        DriverConfig::Http1(io) => {
            let (session, handle) = OwnedH1UpstreamSession::new(OwnedH1Options {
                context: context.clone(),
                transport,
                io,
                http1: Http1DriverConfig::default(),
            })?;
            let upstream = Upstream::Http1(handle);
            (
                upstream.clone(),
                CodecProtocol::Http11,
                context.register_scope(scope.run(upstream, session.run())),
            )
        }
        DriverConfig::Http2(io, http2) => {
            let (session, handle) = OwnedH2UpstreamSession::new(OwnedH2Options {
                context: context.clone(),
                transport,
                io,
                http2,
            })?;
            let upstream = Upstream::Http2(handle);
            (
                upstream.clone(),
                CodecProtocol::Http2,
                context.register_scope(scope.run(upstream, session.run())),
            )
        }
    };
    registration
        .map_err(|error| client_error(HttpErrorCode::INVALID_STATE_TRANSITION, error.to_string()))?
        .activate()
        .map_err(|error| client_error(HttpErrorCode::DROPPED, error.to_string()))?
        .detach();
    Ok(OwnedClientSession {
        inner: Rc::new(SessionInner {
            upstream,
            protocol,
            connection,
            completion,
            attachment,
            worker: context.handle().clone(),
        }),
    })
}

/// The session's worker scope, as `ClientSession`'s run owner: the
/// attachment's cancellation cancels the session, and worker quiescence
/// closes its admission, so it closes once its exchanges retire.
struct RunScope {
    attachment: CancellationWakeRegistration,
    quiescing: WorkerQuiescing,
    completed: SessionCompletionPublisher,
    aborting: bool,
    draining: bool,
    failure: Option<HttpError>,
}

impl RunScope {
    /// Runs the driver, then publishes its outcome, which an attachment
    /// cancellation, or a failure to close admission, replaces. As uniplex
    /// polls a `ClientSession`'s run owner, the stop requests are polled
    /// before each poll of the run and once after the run completes, so a
    /// cancellation that the run's final poll requests, the driver's
    /// destruction included, is observed.
    async fn run(
        mut self,
        upstream: Upstream,
        run: impl Future<Output = Result<UniplexRunSummary, HttpError>>,
    ) {
        let mut run = pin!(run);
        let outcome = poll_fn(|context| {
            self.poll_stop(&upstream, context);
            let outcome = ready!(run.as_mut().poll(context));
            self.poll_stop(&upstream, context);
            Poll::Ready(outcome)
        })
        .await;
        self.completed.publish(self.failure.map_or(outcome, Err));
    }

    /// As `ClientSessionOwner::poll_stop`, the attachment's cancellation wins
    /// over quiescence, which drains no cancelled session.
    fn poll_stop(&mut self, upstream: &Upstream, context: &mut Context<'_>) {
        if !self.aborting && self.attachment.update_waker(context.waker()) {
            self.aborting = true;
            self.draining = true;
            self.failure = Some(client_error(
                HttpErrorCode::CORO_CANCELLED,
                b"attached session cancelled",
            ));
            upstream.cancel();
        }
        if !self.draining && Pin::new(&mut self.quiescing).poll(context).is_ready() {
            self.draining = true;
            self.failure = drain(upstream);
        }
    }
}

/// Closes admission for worker quiescence, as `ClientSessionOwner::stop` does:
/// a failure, or a panic of a callback the close runs (a refused request's
/// source cleanup), cancels the session, whose run then completes its I/O
/// cleanup, and is returned.
fn drain(upstream: &Upstream) -> Option<HttpError> {
    let failure = match catch_unwind(AssertUnwindSafe(|| upstream.close_when_idle())) {
        Ok(Ok(_)) => return None,
        Ok(Err(error)) => error,
        Err(payload) => {
            discard_panic(payload);
            client_error(
                HttpErrorCode::INTERNAL_ERROR,
                b"session drain callback panicked",
            )
        }
    };
    upstream.cancel();
    Some(failure)
}

/// One admission slot of an [`OwnedClientSession`], returned exactly once if
/// it is not submitted.
pub struct OwnedClientReservation(Reservation);

enum Reservation {
    Http1(OwnedH1UpstreamReservation),
    Http2(OwnedH2UpstreamReservation),
}

impl OwnedClientReservation {
    /// Submits an already-read request head and its body source. The
    /// submission resolves at the driver's commitment, with the response's
    /// fresh source.
    pub fn submit(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
    ) -> OwnedClientSubmission {
        OwnedClientSubmission(match self.0 {
            Reservation::Http1(reservation) => {
                Submission::Http1(reservation.submit_exclusive(headers, source, cancellation))
            }
            Reservation::Http2(reservation) => {
                Submission::Http2(reservation.submit_exclusive(headers, source, cancellation))
            }
        })
    }

    /// As [`submit`](Self::submit), but the submission resolves at the first
    /// response head, or at a response that ends or fails before one, as the
    /// owned upstreams' `submit_with_response_interest` does, which documents
    /// `producer_completion`.
    pub fn submit_with_response_interest(
        self,
        headers: HttpHeaderEvent,
        source: impl Into<HttpBodySource>,
        cancellation: CancellationToken,
        producer_completion: Option<IngressCompletion>,
    ) -> OwnedClientSubmission {
        OwnedClientSubmission(match self.0 {
            Reservation::Http1(reservation) => {
                Submission::Http1(reservation.submit_with_response_interest(
                    headers,
                    source,
                    cancellation,
                    producer_completion,
                ))
            }
            Reservation::Http2(reservation) => {
                Submission::Http2(reservation.submit_with_response_interest(
                    headers,
                    source,
                    cancellation,
                    producer_completion,
                ))
            }
        })
    }
}

/// A submitted request: a future of its response. Dropping it before it
/// resolves cancels the request, as the owned upstreams' submissions do.
pub struct OwnedClientSubmission(Submission);

enum Submission {
    Http1(OwnedH1ExclusiveSubmission),
    Http2(OwnedH2ExclusiveSubmission),
}

impl OwnedClientSubmission {
    /// Selects `error` for the request; selected before the response is
    /// claimed, it fails the submission.
    pub fn cancel(&self, error: HttpError) {
        match &self.0 {
            Submission::Http1(submission) => submission.cancel(error),
            Submission::Http2(submission) => submission.cancel(error),
        }
    }
}

impl OwnedRequestProgressExt for OwnedClientSubmission {
    fn request_unprocessed(&self) -> bool {
        match &self.0 {
            Submission::Http1(submission) => submission.request_unprocessed(),
            Submission::Http2(submission) => submission.request_unprocessed(),
        }
    }

    fn sequence_number(&self) -> Option<u64> {
        match &self.0 {
            Submission::Http1(submission) => submission.sequence_number(),
            Submission::Http2(submission) => submission.sequence_number(),
        }
    }
}

impl Future for OwnedClientSubmission {
    type Output = Result<OwnedClientResponse, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            Submission::Http1(submission) => Pin::new(submission).poll(context).map(|response| {
                response.map(|response| {
                    OwnedClientResponse::new(
                        response.source,
                        OwnedClientRequest(Request::Http1(response.control)),
                    )
                })
            }),
            Submission::Http2(submission) => Pin::new(submission).poll(context).map(|response| {
                response.map(|response| {
                    OwnedClientResponse::new(
                        response.source,
                        OwnedClientRequest(Request::Http2(response.control)),
                    )
                })
            }),
        }
    }
}

/// A claimed response: its fresh source, its request's control, and how many
/// requests its connection committed before it.
pub struct OwnedClientResponse {
    pub source: HttpResponseSource,
    pub request: OwnedClientRequest,
    pub sequence: u64,
}

impl OwnedClientResponse {
    fn new(source: HttpResponseSource, request: OwnedClientRequest) -> Self {
        let sequence = request
            .sequence_number()
            .expect("should be fixed when the driver commits the request, before its response");
        Self {
            source,
            request,
            sequence,
        }
    }
}

/// The control and progress of a committed request.
#[derive(Clone)]
pub struct OwnedClientRequest(Request);

#[derive(Clone)]
enum Request {
    Http1(OwnedH1UpstreamRequest),
    Http2(OwnedH2UpstreamRequest),
}

impl OwnedClientRequest {
    /// The request's admission slot has been released.
    pub fn capacity_released(&self) -> bool {
        match &self.0 {
            Request::Http1(request) => request.capacity_released(),
            Request::Http2(request) => request.capacity_released(),
        }
    }

    /// Selects `error` for the request: its response fails with it. Before
    /// both messages end, HTTP/1.1 closes the connection and HTTP/2 resets
    /// the stream.
    pub fn cancel(&self, error: HttpError) {
        match &self.0 {
            Request::Http1(request) => request.cancel(error),
            Request::Http2(request) => request.cancel(error),
        }
    }
}

impl OwnedRequestProgressExt for OwnedClientRequest {
    fn request_unprocessed(&self) -> bool {
        match &self.0 {
            Request::Http1(request) => request.request_unprocessed(),
            Request::Http2(request) => request.request_unprocessed(),
        }
    }

    fn sequence_number(&self) -> Option<u64> {
        match &self.0 {
            Request::Http1(request) => request.sequence_number(),
            Request::Http2(request) => request.sequence_number(),
        }
    }
}
