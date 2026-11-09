//! Experimental owned HTTP/1.1 downstream driver, and its upstream sibling,
//! [`OwnedH1UpstreamSession`].
//!
//! The poll-driven sibling of [`crate::owned_h2`]: one driver future per connection runs
//! `Http1xCodec` and the application's [`OwnedExchange`]s, with no task per request. Streams
//! are serial: the parser pauses once a request is complete and resumes when its exchange
//! retires. An exchange retires once its response's last byte is written, or, as DIV-028 allows
//! for HTTP/2, when its end of message is serialized: if its request is complete, it
//! registered no byte event, and the transport's write queue reports that it is not
//! saturated.
//!
//! [`OwnedH1Session::new`] honors these [`UniplexConfig`] fields: `buffer_limit` (the request
//! body window and the bound on unparsed input and on unwritten output),
//! `ingress_buffer_limit` (parsing pauses while the unread request body is at it),
//! `maximum_read_size`, `read_allocation_size` and `maximum_write_size` (each write, and
//! each response body poll together with the output already queued and its chunk framing).
//! It fails with `INVALID_STATE_TRANSITION` for anything else: a nonzero `read_timeout`,
//! `stream_read_timeout` or `write_timeout` (the driver is untimed; the caller keeps timeout
//! responsibility), `drain_request_body_on_drop` false, `wait_for_http1_response_on_drain`
//! or `abort_exchanges_on_eof` or `deliver_ingress_before_abort` true, `maximum_write_size`
//! above `buffer_limit` or too small to carry a chunk's framing beside one body byte, a
//! transport with byte-event capability, or a context that is not the cancellation-free
//! owner thread.
//!
//! Framing, close and upgrade follow `UniplexSession`'s port of the Proxygen HTTP/1.1 session
//! ([`Http1UpgradePolicy::Proxygen`]): trailers are carried in both directions, the
//! application sends any `100 Continue`, a request the codec rejects gets its error response
//! before the connection is reset, and an accepted upgrade or CONNECT carries the tunnel as
//! the exchange's request and response bodies, with no request after it. Response byte-event
//! registrations are cancelled, never fired. DIV-029 lists where this driver differs from
//! `UniplexSession`. A request whose reader stops it without error (`NO_ERROR`) before it
//! ends still gets its whole response; nothing more of its upload is parsed, and after the
//! response the connection closes in stages: its write side first, then its read side at peer
//! EOF or once `buffer_limit` bytes are read after the write side shut down (DIV-054). Under
//! the Roxygen-only [`Http1UpgradePolicy::AsOrdinaryRequest`] (DIV-030), an upgrade or
//! CONNECT request is served as an ordinary request instead, and the connection stays
//! HTTP/1.1.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use parking_lot::Mutex;
use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::error_adapter::http_exception_to_http_error_code;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeFuture;
use roxygen_core::fixed_source::get_error_response;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::message::HttpMessage;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSourceDrain;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_core::stream_source::HttpStreamSourceSharedCallback;
use roxygen_http1::Http1xCodec;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::panic::discard_panic;
use roxygen_runtime::task_group::in_forced_teardown;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;

use crate::owned::OwnedApplication;
use crate::owned::OwnedExchange;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex::UniplexTransport;
use crate::uniplex::UniplexWriteQueue;

mod upstream;

pub use upstream::OwnedH1ExclusiveResponse;
pub use upstream::OwnedH1ExclusiveSubmission;
pub use upstream::OwnedH1UpstreamHandle;
pub use upstream::OwnedH1UpstreamRequest;
pub use upstream::OwnedH1UpstreamReservation;
pub use upstream::OwnedH1UpstreamResponse;
pub use upstream::OwnedH1UpstreamSession;
pub use upstream::OwnedH1UpstreamSubmission;

const TURN_BUDGET: usize = 64;
/// The most chunked framing one response body event adds: a hex length of at most 8 digits
/// and its CRLF, then the chunk's CRLF and the last chunk.
const CHUNK_OVERHEAD: usize = 8 + b"\r\n".len() + b"\r\n0\r\n\r\n".len();

/// Inputs selected once, before starting an experimental HTTP/1.1 connection.
pub struct OwnedH1Options {
    pub context: WorkerContext,
    pub transport: Rc<dyn UniplexTransport>,
    pub io: UniplexConfig,
    pub http1: Http1DriverConfig,
}

/// HTTP/1.1 codec choices for an owned connection.
#[derive(Clone, Debug, Default)]
pub struct Http1DriverConfig {
    /// `Http1xCodec::set_strict_validation`; off by default, as the codec is.
    pub strict_validation: bool,
    /// How an upgrade or CONNECT request is handled.
    pub upgrade_policy: Http1UpgradePolicy,
}

/// How an upgrade or CONNECT request is handled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Http1UpgradePolicy {
    /// Downstream, as `UniplexSession`: parsing waits for the final response head, which
    /// accepts the upgrade (the bytes that follow are the request body, through EOF, and the
    /// response body is sent unframed) or refuses it (the request ends and the connection
    /// closes after the response). Upstream, the owned driver carries no tunnel and refuses
    /// the request before any byte (`DIV-029`).
    #[default]
    Proxygen,
    /// Roxygen-only: the request is an ordinary request, with its method, target and headers
    /// unchanged, and no tunnel is carried. Downstream (`DIV-030`), it is handed over and
    /// parsing continues behind it as HTTP/1.1
    /// (`Http1xCodec::set_downstream_upgrade_as_ordinary`), and a response that switches
    /// protocols is refused. Upstream (`DIV-031`), it is sent and its response read by their
    /// own framing (`Http1xCodec::set_upstream_upgrade_as_ordinary`); a head that does not
    /// frame its body as the codec writes it and the egress websocket flag are refused before
    /// any byte, and a `101` fails the connection.
    AsOrdinaryRequest,
}

/// The session owner's waker, shared by every wake source of one connection.
#[derive(Default)]
struct TaskWaker(Mutex<Option<Waker>>);

impl TaskWaker {
    fn register(&self, waker: &Waker) {
        let replaced = {
            let mut slot = self.0.lock();
            if slot
                .as_ref()
                .is_some_and(|current| current.will_wake(waker))
            {
                return;
            }
            slot.replace(waker.clone())
        };
        drop(replaced);
    }

    fn wake(&self) {
        let waker = self.0.lock().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// One wake source: a ready flag the driver consumes before polling that source.
struct Signal {
    ready: AtomicBool,
    task: Arc<TaskWaker>,
}

impl Signal {
    fn new(task: &Arc<TaskWaker>) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(false),
            task: Arc::clone(task),
        })
    }

    fn mark(&self) {
        self.ready.store(true, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    fn take_ready(&self) -> bool {
        self.is_ready() && self.ready.swap(false, Ordering::AcqRel)
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark();
        self.task.wake();
    }
}

struct Control {
    drain: Cell<bool>,
    cancel: Cell<bool>,
    complete: Cell<bool>,
    task: Arc<TaskWaker>,
}

/// Owner-local control of one owned HTTP/1.1 connection; the driver acts on each request in
/// its next poll.
#[derive(Clone)]
pub struct OwnedH1Handle(Rc<Control>);

impl OwnedH1Handle {
    /// Makes the next response carry `Connection: close` and closes the connection once it is
    /// idle. Returns whether this call changed the session's state.
    pub fn initiate_drain(&self) -> Result<bool, HttpError> {
        if self.0.complete.get() {
            return Ok(false);
        }
        let changed = !self.0.drain.replace(true);
        self.0.task.wake();
        Ok(changed)
    }

    /// Fails the connection with `CORO_CANCELLED`: the live exchange is cancelled and dropped
    /// and the transport is reset, on the worker, before `run` completes.
    pub fn cancel(&self) {
        self.0.cancel.set(true);
        self.0.task.wake();
    }
}

/// Request-source notifications; they only record and wake the owner.
struct IngressSignals {
    source_complete: Cell<bool>,
    completed: Cell<Option<Result<(), HttpError>>>,
    stream: Arc<Signal>,
}

impl IngressSignals {
    /// Whether the source completed with `NO_ERROR`, its reader having stopped without error,
    /// and the driver has yet to take that completion.
    fn stopped(&self) -> bool {
        if !self.source_complete.get() {
            return false;
        }
        let completed = self.completed.take();
        let stopped =
            matches!(&completed, Some(Err(error)) if error.code() == HttpErrorCode::NO_ERROR);
        self.completed.set(completed);
        stopped
    }
}

impl HttpStreamSourceSharedCallback for IngressSignals {
    fn bytes_processed(&self, _id: StreamId, _amount: usize, _acknowledge: usize) {
        self.stream.wake_by_ref();
    }

    fn source_complete(&self, _id: StreamId, error: Option<HttpError>) {
        self.source_complete.set(true);
        self.completed.set(Some(error.map_or(Ok(()), Err)));
        self.stream.wake_by_ref();
    }
}

/// This driver tracks no byte offsets: each response byte-event registration is cancelled, with
/// `INVALID_STATE_TRANSITION`, as soon as its event is polled. Its callbacks may cancel the
/// session.
fn refuse_byte_events(
    registrations: Vec<HttpByteEventRegistration>,
    control: &Control,
    context: &WorkerContext,
) -> Result<(), HttpError> {
    if registrations.is_empty() {
        return Ok(());
    }
    cancel_byte_events(registrations);
    check_cancel(control, context)
}

/// Cancels each registration with `INVALID_STATE_TRANSITION`: this driver delivers no byte
/// events.
fn cancel_byte_events(registrations: Vec<HttpByteEventRegistration>) {
    let error = failure(
        HttpErrorCode::INVALID_STATE_TRANSITION,
        b"owned H1 delivers no byte events",
    );
    for mut registration in registrations {
        registration.cancel(&error, None);
    }
}

/// The one live exchange and its request source.
struct Exchange<E: OwnedExchange> {
    application: Option<E>,
    signals: Rc<IngressSignals>,
    source: Option<HttpIngressBody>,
    producer: HttpStreamSourceProducer,
    drain: Option<HttpStreamSourceDrain>,
    head: Option<Box<HttpMessage>>,
    started: bool,
    body_events: bool,
    /// An upgrade or CONNECT request, which parsing hands over at its final response head.
    upgrade: bool,
    ingress_complete: bool,
    consumed: bool,
    response_headers: bool,
    response_complete: bool,
    /// A suspended response body read's resume, awaited before the body is read again.
    resume: Option<ResumeFuture>,
    write_boundary: u64,
    /// The response registered a byte event (each is refused); it keeps the write fence.
    registered_byte_events: bool,
    /// Skips the write fence (`DIV-028`'s rule, decided as the response ends).
    retires_at_serialization: bool,
}

impl<E: OwnedExchange> Drop for Exchange<E> {
    fn drop(&mut self) {
        let mut panic = None;
        capture(&mut panic, || drop(self.application.take()));
        capture(&mut panic, || drop(self.source.take()));
        capture(&mut panic, || drop(self.drain.take()));
        capture(&mut panic, || drop(self.resume.take()));
        capture(&mut panic, || drop(self.head.take()));
        if let Some(panic) = panic {
            if std::thread::panicking() {
                discard_panic(panic);
            } else {
                resume_unwind(panic);
            }
        }
    }
}

type PanicPayload = Box<dyn Any + Send>;

fn capture<T>(first: &mut Option<PanicPayload>, action: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(result) => Some(result),
        Err(panic) => {
            if first.is_none() {
                *first = Some(panic);
            } else {
                discard_panic(panic);
            }
            None
        }
    }
}

#[cold]
#[inline(never)]
fn failure(code: HttpErrorCode, message: &'static [u8]) -> HttpError {
    HttpError::with_message(code, message)
}

/// What polling the outstanding read found.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPoll {
    Pending,
    /// Ready with no bytes and no EOF.
    Empty,
    /// Ready with bytes, EOF or a failure.
    Data,
}

fn cancel_requested(control: &Control, context: &WorkerContext) -> bool {
    control.cancel.get() || context.is_cancelled()
}

/// Run after each application callback: one that cancelled the session gets nothing more
/// called, serialized or written after it.
fn check_cancel(control: &Control, context: &WorkerContext) -> Result<(), HttpError> {
    if cancel_requested(control, context) {
        return Err(cancelled());
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn cancelled() -> HttpError {
    failure(
        HttpErrorCode::CORO_CANCELLED,
        b"owned H1 connection cancelled",
    )
}

struct NoopCallback;
impl HttpCodecCallback for NoopCallback {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

/// As `UniplexSession`'s HTTP/1.1 drain: the codec stops keep-alive, so the next response
/// says `Connection: close`, and the idle connection closes.
#[cold]
fn drain_codec(codec: &mut Http1xCodec, egress: &mut BufQueue) -> Result<(), HttpError> {
    let last_stream = codec.last_incoming_stream_id();
    codec
        .generate(
            egress,
            EgressFrame::Goaway {
                last_stream,
                code: H2ErrorCode::NO_ERROR,
                debug_data: None,
            },
            &mut NoopCallback,
        )
        .map(drop)
}

/// `DIV-054`: the request's reader stopped it without error before it ended. HTTP/1.1 cannot
/// reset one request, so nothing more of the upload is parsed, and the codec stops keep-alive:
/// a response head not yet serialized says `Connection: close`, and the connection closes once
/// the response is written.
#[cold]
fn stop_request_ingress(codec: &mut Http1xCodec, ingress_stopped: &mut bool) {
    *ingress_stopped = true;
    codec.generate_immediate_goaway();
}

/// A poll-driven HTTP/1.1 downstream connection with one exchange at a time.
pub struct OwnedH1Session<A: OwnedApplication>(Driver<A>);

impl<A: OwnedApplication> OwnedH1Session<A> {
    pub fn new(
        application: A,
        options: OwnedH1Options,
    ) -> Result<(Self, OwnedH1Handle), HttpError> {
        let (driver, handle) = Driver::new(application, options)?;
        Ok((Self(driver), handle))
    }

    /// Runs inside the caller's registered worker cleanup scope, as `OwnedH2Session::run`.
    pub fn run(self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        DriverRun(Some(self.0))
    }
}

/// Drives a [`Driver`] to completion, isolating callback panics in every poll.
struct DriverRun<A: OwnedApplication>(Option<Driver<A>>);

// The driver is only ever accessed through `&mut`; nothing pins it.
impl<A: OwnedApplication> Unpin for DriverRun<A> {}

impl<A: OwnedApplication> Future for DriverRun<A> {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let driver = this
            .0
            .as_mut()
            .expect("owned H1 runner polled after completion");
        driver.started = true;
        match catch_unwind(AssertUnwindSafe(|| driver.poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                let mut driver = this.0.take().expect("owned H1 runner completes once");
                driver.control.complete.set(true);
                if let Some(panic) = driver.panic.take() {
                    resume_unwind(panic);
                }
                Poll::Ready(result)
            }
            Err(panic) => {
                driver.retain_panic(panic);
                driver.abort(failure(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"owned H1 callback panicked",
                ));
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct Driver<A: OwnedApplication> {
    context: WorkerContext,
    transport: Rc<dyn UniplexTransport>,
    application: A,
    control: Rc<Control>,
    config: UniplexConfig,
    codec: Http1xCodec,
    exchange: Option<Box<Exchange<A::Exchange>>>,
    ingress: BufQueue,
    egress: BufQueue,
    read: Option<LocalTransportFuture<Result<UniplexRead, HttpError>>>,
    write: Option<LocalTransportFuture<Result<WriteInfo, TransportWriteError>>>,
    write_queue: Option<Box<dyn UniplexWriteQueue>>,
    task: Arc<TaskWaker>,
    read_wake: Arc<Signal>,
    read_waker: Waker,
    write_wake: Arc<Signal>,
    write_waker: Waker,
    stream_wake: Arc<Signal>,
    stream_waker: Waker,
    cancellation: CancellationSource,
    write_cancellation: CancellationSource,
    draining: bool,
    eof: bool,
    eof_processed: bool,
    /// A failed read, applied once what was read before it is handled (`holds_read_error`).
    read_error: Option<HttpError>,
    exchange_pause: bool,
    flow_pause: bool,
    /// Parsing waits past an upgrade or CONNECT request head until its final response head
    /// resolves the upgrade in the codec, as `UniplexSession`'s upgrade pause.
    upgrade_pending: bool,
    /// The request's reader stopped it without error before it ended: nothing more is parsed
    /// (`DIV-054`).
    ingress_stopped: bool,
    /// The write side's shutdown, once a staged close after a stopped upload took it.
    write_shutdown: Option<Result<(), HttpError>>,
    /// The response body was not polled because the next write must make room for it.
    body_awaits_egress: bool,
    fatal: Option<HttpError>,
    /// The connection failed with an error response queued: it resets once that is written.
    closing_error: Option<HttpError>,
    /// A failed read whose reset waits for the serialized bytes to be written: it stays the
    /// run's error if another failure resets first, as `UniplexSession` keeps the first error.
    failed_read: Option<HttpError>,
    panic: Option<PanicPayload>,
    transport_reset: bool,
    write_end: u64,
    summary: UniplexRunSummary,
    budget_yield: Option<LocalFuture>,
    started: bool,
}

/// The codec callback: borrows what one `on_ingress` call may change.
struct CodecInput<'a, A: OwnedApplication> {
    application: &'a A,
    context: &'a WorkerContext,
    control: &'a Control,
    config: &'a UniplexConfig,
    exchange: &'a mut Option<Box<Exchange<A::Exchange>>>,
    stream_wake: &'a Arc<Signal>,
    exchange_pause: &'a mut bool,
    flow_pause: &'a mut bool,
    upgrade_pending: &'a mut bool,
    /// The codec entered an accepted upgrade; it consumes no input in that call.
    upgrade_completed: bool,
    /// The first failure, and whether it is the peer's (the connection ended mid-request).
    failure: Option<(HttpError, bool)>,
    /// The error response status the codec selected for that failure.
    error_status: Option<u16>,
}

impl<A: OwnedApplication> CodecInput<'_, A> {
    fn fail(&mut self, error: HttpError) {
        self.failure.get_or_insert((error, false));
    }

    fn new_exchange(&mut self, id: StreamId) -> Result<(), HttpError> {
        if self.exchange.is_some() {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 request began while another exchange is live",
            ));
        }
        let signals = Rc::new(IngressSignals {
            source_complete: Cell::new(false),
            completed: Cell::new(None),
            stream: Arc::clone(self.stream_wake),
        });
        let source = HttpIngressBody::new_shared(
            id,
            signals.clone(),
            self.config.buffer_limit as u32,
            self.context.host().clone(),
        )
        .map_err(|_| {
            failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"invalid owned H1 request window",
            )
        })?;
        let producer = source.producer();
        let drain = source.drain_on_drop();
        let ingress = source
            .ingress_completion()
            .expect("a fresh request source has an ingress receipt");
        let application = self.application.new_exchange(id, ingress);
        *self.exchange = Some(Box::new(Exchange {
            application: Some(application),
            signals,
            source: Some(source),
            producer,
            drain: Some(drain),
            head: None,
            started: false,
            body_events: false,
            upgrade: false,
            ingress_complete: false,
            consumed: false,
            response_headers: false,
            response_complete: false,
            resume: None,
            write_boundary: 0,
            registered_byte_events: false,
            retires_at_serialization: false,
        }));
        check_cancel(self.control, self.context)
    }
}

impl<A: OwnedApplication> HttpCodecCallback for CodecInput<'_, A> {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let mut actions = Vec::new();
        if self.failure.is_some() {
            actions.push(CodecCallbackAction::SetParserPaused(true));
            return actions.into();
        }
        match event {
            CodecEvent::MessageBegin { stream, .. } => {
                if let Err(error) = self.new_exchange(stream) {
                    self.fail(error);
                }
            }
            CodecEvent::HeadersComplete { mut message, .. } => {
                let Some(exchange) = self.exchange.as_mut() else {
                    return actions.into();
                };
                let source = exchange
                    .source
                    .as_ref()
                    .expect("an unstarted request retains its source");
                if !source.initialize_body_only(&mut message, false) {
                    self.fail(failure(
                        HttpErrorCode::HEADER_PARSE_ERROR,
                        b"ingress source rejected request head",
                    ));
                    return actions.into();
                }
                // The codec marks a downstream CONNECT upgraded and a WebSocket upgrade request
                // as one, unless it parses them as ordinary requests (`DIV-030`).
                let upgrade = message.is_ingress_websocket_upgrade() || message.is_upgraded();
                exchange.upgrade = upgrade;
                exchange.head = Some(message);
                self.stream_wake.mark();
                if upgrade {
                    *self.upgrade_pending = true;
                    actions.push(CodecCallbackAction::SetParserPaused(true));
                }
            }
            CodecEvent::Body { body, padding, .. } => {
                let Some(exchange) = self.exchange.as_mut() else {
                    return actions.into();
                };
                exchange.body_events = true;
                match exchange.producer.body(body, padding, false) {
                    FlowControlState::Error => self.fail(failure(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"ingress source rejected body bytes",
                    )),
                    // As `UniplexSession`: parsing stops while the unread body is at the limit.
                    state
                        if state == FlowControlState::Closed
                            || exchange.producer.body_bytes_buffered().is_some_and(
                                |buffered| buffered >= self.config.ingress_buffer_limit,
                            ) =>
                    {
                        *self.flow_pause = true;
                        actions.push(CodecCallbackAction::SetParserPaused(true));
                    }
                    _ => {}
                }
            }
            // An accepted upgrade: the bytes that follow are the request's body, through EOF.
            CodecEvent::MessageComplete { upgraded: true, .. } => {
                self.upgrade_completed = true;
            }
            CodecEvent::MessageComplete { .. } => {
                self.complete_request(&mut actions);
            }
            CodecEvent::TrailersComplete { trailers, .. } => {
                let Some(exchange) = self.exchange.as_mut() else {
                    return actions.into();
                };
                // As `UniplexSession`: the trailers are an event the exchange reads from its
                // body source; the message completion that follows ends the body.
                exchange.body_events = true;
                if !exchange.producer.trailers(*trailers) {
                    self.fail(failure(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        b"ingress source rejected trailers",
                    ));
                }
            }
            CodecEvent::Error { error, .. } => {
                let code = http_exception_to_http_error_code(error);
                if self.failure.is_none() {
                    self.failure = Some((
                        HttpError::with_message(code, error.message()),
                        code == HttpErrorCode::TRANSPORT_EOF,
                    ));
                    self.error_status = error
                        .http_status_code()
                        .and_then(|status| u16::try_from(status).ok());
                }
            }
            _ => {}
        }
        actions.into()
    }
}

impl<A: OwnedApplication> CodecInput<'_, A> {
    fn complete_request(&mut self, actions: &mut Vec<CodecCallbackAction>) {
        let Some(exchange) = self.exchange.as_mut() else {
            return;
        };
        if !exchange.ingress_complete {
            exchange.ingress_complete = true;
            exchange.producer.eom();
        }
        self.stream_wake.mark();
        *self.exchange_pause = true;
        actions.push(CodecCallbackAction::SetParserPaused(true));
    }
}

impl<A: OwnedApplication> Driver<A> {
    fn new(application: A, options: OwnedH1Options) -> Result<(Self, OwnedH1Handle), HttpError> {
        let OwnedH1Options {
            context,
            transport,
            io,
            http1,
        } = options;
        let io = io.validate()?;
        if !context.is_owner_thread()
            || context.is_cancelled()
            || io.maximum_write_size > io.buffer_limit
            || io.maximum_write_size <= CHUNK_OVERHEAD
            || !io.read_timeout.is_zero()
            || !io.stream_read_timeout.is_zero()
            || !io.write_timeout.is_zero()
            || !io.drain_request_body_on_drop
            || io.wait_for_http1_response_on_drain
            || io.abort_exchanges_on_eof
            || io.deliver_ingress_before_abort
            || transport.byte_event_capability(&context)? != ByteEventCapability::Unsupported
        {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"owned H1 configuration is not supported",
            ));
        }
        let task = Arc::new(TaskWaker::default());
        let control = Rc::new(Control {
            drain: Cell::new(false),
            cancel: Cell::new(false),
            complete: Cell::new(false),
            task: Arc::clone(&task),
        });
        let read_wake = Signal::new(&task);
        let write_wake = Signal::new(&task);
        let stream_wake = Signal::new(&task);
        let cancellation = CancellationSource::new();
        let write_cancellation = CancellationSource::new();
        let write_queue = transport.try_write_queue(
            &context,
            io.maximum_write_size,
            io.write_timeout,
            write_cancellation.token(),
        )?;
        let mut codec = Http1xCodec::with_options(
            TransportDirection::Downstream,
            false,
            http1.strict_validation,
        );
        codec.set_downstream_upgrade_as_ordinary(
            http1.upgrade_policy == Http1UpgradePolicy::AsOrdinaryRequest,
        );
        Ok((
            Self {
                context,
                transport,
                application,
                control: Rc::clone(&control),
                config: io,
                codec,
                exchange: None,
                ingress: BufQueue::new(io.buffer_limit),
                egress: BufQueue::new(io.buffer_limit),
                read: None,
                write: None,
                write_queue,
                task,
                read_waker: Waker::from(Arc::clone(&read_wake)),
                read_wake,
                write_waker: Waker::from(Arc::clone(&write_wake)),
                write_wake,
                stream_waker: Waker::from(Arc::clone(&stream_wake)),
                stream_wake,
                cancellation,
                write_cancellation,
                draining: false,
                eof: false,
                eof_processed: false,
                read_error: None,
                exchange_pause: false,
                flow_pause: false,
                upgrade_pending: false,
                ingress_stopped: false,
                write_shutdown: None,
                body_awaits_egress: false,
                fatal: None,
                closing_error: None,
                failed_read: None,
                panic: None,
                transport_reset: false,
                write_end: 0,
                summary: UniplexRunSummary::default(),
                budget_yield: None,
                started: false,
            },
            OwnedH1Handle(control),
        ))
    }

    fn retain_panic(&mut self, panic: PanicPayload) {
        if self.panic.is_none() {
            self.panic = Some(panic);
        } else {
            discard_panic(panic);
        }
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRunSummary, HttpError>> {
        self.task.register(context.waker());
        if self.budget_yield.is_some() && self.poll_budget_yield(context).is_pending() {
            return Poll::Pending;
        }
        let mut turn = 0;
        while turn < TURN_BUDGET {
            self.observe_cancel();
            self.observe_drain();
            let mut progress = false;
            if let Some(wrote) = self.poll_write() {
                progress |= wrote;
                self.observe_cancel();
            }
            if let Some(read) = self.poll_read(&mut turn) {
                progress |= read;
                self.observe_cancel();
                // A drain requested from the read admits nothing it carried.
                self.observe_drain();
            }
            self.observe_stop();
            // A busy codec may hold a completion it deferred while the parser was paused. As
            // `UniplexSession`, an idle draining connection admits no further request.
            if self.fatal.is_none()
                && self.closing_error.is_none()
                && !self.ingress_paused()
                && !(self.draining && self.exchange.is_none())
                && (!self.ingress.is_empty()
                    || self.codec.is_busy()
                    || (self.eof && !self.eof_processed))
            {
                match self.parse_input() {
                    Ok(parsed) => progress |= parsed,
                    Err(error) => self.abort(error),
                }
            }
            if !self.holds_read_error()
                && let Some(error) = self.read_error.take()
            {
                self.fail_read(error);
                progress = true;
            }
            if self.fatal.is_none() && self.stream_wake.take_ready() {
                progress = true;
                if let Err(error) = self.poll_exchange() {
                    self.abort(error);
                }
            }
            if self.fatal.is_none() && self.retire_exchange() {
                progress = true;
            }
            if !self.transport_reset
                && !self.egress.is_empty()
                && self.can_start_write()
                && (!progress
                    || self.egress.chain_length() >= self.config.maximum_write_size
                    || turn + 1 >= TURN_BUDGET
                    || self.fatal.is_some())
            {
                // A write queue's capacity check may cancel; the abort discards the egress.
                self.observe_cancel();
                if !self.transport_reset
                    && let Err(error) = self.start_write()
                {
                    self.abort(error);
                }
                progress = true;
            }
            if self.fatal.is_none()
                && self.closing_error.is_some()
                && self.egress.is_empty()
                && !self.writes_pending()
            {
                let error = self.closing_error.take().expect("checked above");
                self.abort(error);
            }
            if self.fatal.is_some() && !self.transport_reset {
                self.reset_transport();
                progress = true;
            }
            if self.transport_reset
                && !self.writes_pending()
                && self.read.is_none()
                && self.exchange.is_none()
            {
                return Poll::Ready(Err(self
                    .fatal
                    .take()
                    .expect("a reset connection retains its cause")));
            }
            if self.fatal.is_none()
                && self.exchange.is_none()
                && self.egress.is_empty()
                && !self.writes_pending()
                && (self.eof_processed || !self.codec.is_reusable())
            {
                if let Some(closed) = self.close_idle() {
                    return Poll::Ready(closed);
                }
                // Once polled after the cancellation request, only the outstanding read can
                // progress: its completion wakes the task.
                if !progress && !self.read_wake.is_ready() {
                    return Poll::Pending;
                }
                turn += 1;
                continue;
            }
            if !progress {
                return Poll::Pending;
            }
            turn += 1;
        }
        self.budget_yield = Some(self.context.host().yield_to_io());
        // A yield that completes on its first poll arranged no wake.
        if self.poll_budget_yield(context).is_ready() {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// Run at each phase boundary: the transport's callbacks, like the application's, may
    /// cancel the session, and the phases after them must not act for it.
    fn observe_cancel(&mut self) {
        if cancel_requested(&self.control, &self.context) && !self.transport_reset {
            self.abort(cancelled());
        }
    }

    fn observe_drain(&mut self) {
        if self.control.drain.get() && !self.draining && self.fatal.is_none() {
            self.start_drain();
        }
    }

    /// A stop the request's reader reported since the last poll takes effect before anything
    /// read with it is parsed (`DIV-054`); as in `poll_exchange`, not once the response ended.
    fn observe_stop(&mut self) {
        if let Some(exchange) = &self.exchange
            && !exchange.ingress_complete
            && !exchange.response_complete
            && exchange.signals.stopped()
        {
            stop_request_ingress(&mut self.codec, &mut self.ingress_stopped);
        }
    }

    fn poll_budget_yield(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let Some(mut wait) = self.budget_yield.take() else {
            return Poll::Ready(());
        };
        let poll = wait.as_mut().poll(context);
        if poll.is_pending() {
            self.budget_yield = Some(wait);
        }
        poll
    }

    #[cold]
    fn start_drain(&mut self) {
        self.draining = true;
        if let Err(error) = drain_codec(&mut self.codec, &mut self.egress) {
            self.abort(error);
        }
    }

    fn parse_input(&mut self) -> Result<bool, HttpError> {
        let mut callback = CodecInput::<A> {
            application: &self.application,
            context: &self.context,
            control: &self.control,
            config: &self.config,
            exchange: &mut self.exchange,
            stream_wake: &self.stream_wake,
            exchange_pause: &mut self.exchange_pause,
            flow_pause: &mut self.flow_pause,
            upgrade_pending: &mut self.upgrade_pending,
            upgrade_completed: false,
            failure: None,
            error_status: None,
        };
        let length = self.ingress.chain_length();
        let (consumed, mut egress_results) = if length == 0 && !self.codec.is_busy() {
            (0, Vec::new())
        } else {
            let result = self
                .codec
                .on_ingress(&self.ingress, &mut callback, &mut self.egress);
            (result.consumed, result.egress_results)
        };
        // As `UniplexSession`, entering the upgrade is progress: the tunnel's bytes are parsed
        // before EOF.
        let upgrade_completed = callback.upgrade_completed;
        // The codec ignores EOF while its parser is paused, so EOF waits behind every pause:
        // an upgrade's until the final response head resolves it.
        let finishing_eof = self.eof
            && !self.eof_processed
            && !upgrade_completed
            && !*callback.upgrade_pending
            && !*callback.exchange_pause
            && !*callback.flow_pause
            && (consumed == length || consumed == 0);
        if finishing_eof {
            self.eof_processed = true;
            let result = self.codec.on_ingress_eof(&mut callback, &mut self.egress);
            egress_results.extend(result.egress_results);
        }
        let failed = callback.failure.take();
        let error_status = callback.error_status;
        if consumed > length {
            return Err(failure(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                b"codec consumed beyond the supplied prefix",
            ));
        }
        self.ingress
            .trim_start(consumed)
            .expect("codec consumption was range checked");
        for result in egress_results {
            result?;
        }
        if let Some((error, peer)) = failed {
            match error_status {
                // As `UniplexSession`: a request that can still be answered gets the codec's
                // error response before the connection is reset.
                Some(status)
                    if !self
                        .exchange
                        .as_ref()
                        .is_some_and(|exchange| exchange.response_headers) =>
                {
                    self.fail_with_response(error, peer, status)?;
                }
                _ => self.fail(error, peer),
            }
            return Ok(true);
        }
        Ok(consumed != 0 || finishing_eof || upgrade_completed)
    }

    /// Whether this driver holds the parser: a request awaits its exchange's retirement, its
    /// unread body is at `ingress_buffer_limit`, or its reader stopped it.
    fn ingress_paused(&self) -> bool {
        self.exchange_pause || self.flow_pause || self.upgrade_pending || self.ingress_stopped
    }

    /// Whether a failed read waits for what was read before it: `UniplexSession` reads only once
    /// it has parsed everything read so far, so it sees the failure after that. The wait covers
    /// a pending upgrade, bytes a pause keeps unparsed (an unfinished request's body behind flow
    /// control, or a pipelined request behind the exchange), and a whole request the exchange
    /// has not received: Uniplex's handler still reads a request that ended before the failure,
    /// but not an unfinished one.
    fn holds_read_error(&self) -> bool {
        self.upgrade_pending
            || (self.flow_pause && self.codec.is_busy())
            || (self.exchange_pause && !self.ingress.is_empty())
            || self.exchange.as_ref().is_some_and(|exchange| {
                !exchange.started && exchange.head.is_some() && exchange.ingress_complete
            })
    }

    /// Hands the request to the exchange and serializes what its response produces.
    fn poll_exchange(&mut self) -> Result<(), HttpError> {
        let Some(exchange) = self.exchange.as_mut() else {
            return Ok(());
        };
        if self.flow_pause
            && exchange
                .producer
                .body_bytes_buffered()
                .is_none_or(|buffered| buffered < self.config.ingress_buffer_limit)
        {
            self.flow_pause = false;
            self.codec.set_parser_paused(self.exchange_pause);
        }
        if !exchange.consumed && exchange.signals.source_complete.get() {
            exchange.consumed = true;
            if let Some(Err(error)) = exchange.signals.completed.take()
                && !exchange.response_complete
            {
                // A reader that stopped an ordinary request without error leaves its response
                // to finish (`DIV-054`).
                if error.code() != HttpErrorCode::NO_ERROR || exchange.upgrade {
                    return Err(error);
                }
                if !exchange.ingress_complete {
                    stop_request_ingress(&mut self.codec, &mut self.ingress_stopped);
                }
            }
        }
        let mut context = Context::from_waker(&self.stream_waker);
        if !exchange.started
            && let Some(head) = exchange.head.take()
        {
            let mut source = exchange
                .source
                .take()
                .expect("an unstarted request retains its source");
            let eom = exchange.ingress_complete && !exchange.body_events;
            if eom {
                source = source.retire_empty_body(&mut context)?;
            }
            exchange.started = true;
            exchange
                .application
                .as_mut()
                .expect("the application remains owned")
                .request_body(head, source, eom);
            check_cancel(&self.control, &self.context)?;
            // As `UniplexSession`, whose read loop reaches a failed read it already holds before
            // the handler it spawned for the request runs: the failure ends the exchange first.
            if self.read_error.is_some() && !self.holds_read_error() {
                return Ok(());
            }
        }
        let exchange = self
            .exchange
            .as_mut()
            .expect("nothing above removes the exchange");
        if !exchange.started || exchange.response_complete {
            return Ok(());
        }
        let stream = self.codec.last_incoming_stream_id();
        let application = exchange
            .application
            .as_mut()
            .expect("the application remains owned");
        let eom = if !exchange.response_headers {
            let Poll::Ready(headers) = application.poll_headers(&mut context) else {
                return Ok(());
            };
            check_cancel(&self.control, &self.context)?;
            let mut headers: HttpHeaderEvent = headers?;
            let registrations = headers.take_byte_event_registrations();
            exchange.registered_byte_events |= !registrations.is_empty();
            refuse_byte_events(registrations, &self.control, &self.context)?;
            let final_headers = headers.is_final().map_err(|error| {
                HttpError::with_message(HttpErrorCode::MESSAGE_ERROR, error.to_string().as_bytes())
            })?;
            let eom = headers.eom();
            // A drain requested from an exchange callback applies to this response.
            if self.control.drain.get() && !self.draining {
                self.draining = true;
                drain_codec(&mut self.codec, &mut self.egress)?;
            }
            // So does a stop the exchange's callbacks reported (`DIV-054`).
            if !exchange.ingress_complete && exchange.signals.stopped() {
                stop_request_ingress(&mut self.codec, &mut self.ingress_stopped);
            }
            let generated = self.codec.generate(
                &mut self.egress,
                EgressFrame::Headers {
                    stream,
                    message: headers.headers(),
                    end_stream: eom,
                    extra_headers: None,
                },
                &mut NoopCallback,
            )?;
            exchange.response_headers = final_headers;
            if final_headers && self.upgrade_pending {
                // As `UniplexSession`: the final head accepts or refuses the upgrade, and parsing
                // resumes, into the tunnel or on to the request's end.
                self.upgrade_pending = false;
                self.codec.resolve_downstream_upgrade(headers.headers());
                self.codec
                    .set_parser_paused(self.exchange_pause || self.flow_pause);
            }
            if let Some(size) = generated.header_size {
                headers.on_headers_encoded(size);
                check_cancel(&self.control, &self.context)?;
            }
            eom
        } else {
            // As `UniplexSession`'s egress read, a suspended read is read again once it resumes,
            // whatever its status. The resume future is caller code.
            if let Some(resume) = &mut exchange.resume {
                if resume.as_mut().poll(&mut context).is_pending() {
                    return Ok(());
                }
                exchange.resume = None;
                check_cancel(&self.control, &self.context)?;
            }
            // As OwnedH2's serialization budget: what is already queued and the framing count
            // against one write.
            let room = self
                .config
                .maximum_write_size
                .saturating_sub(self.egress.chain_length())
                .min(self.egress.remaining_capacity())
                .saturating_sub(CHUNK_OVERHEAD);
            if room == 0 {
                self.body_awaits_egress = true;
                return Ok(());
            }
            let Poll::Ready(event) = application.poll_body(&mut context, room as u32) else {
                return Ok(());
            };
            check_cancel(&self.control, &self.context)?;
            let mut event = event?;
            let registrations = event.take_byte_event_registrations();
            exchange.registered_byte_events |= !registrations.is_empty();
            refuse_byte_events(registrations, &self.control, &self.context)?;
            // A stop reported with this event, which may be the response's last, or from its
            // byte events' cancellation (`DIV-054`).
            if !exchange.ingress_complete && exchange.signals.stopped() {
                stop_request_ingress(&mut self.codec, &mut self.ingress_stopped);
            }
            let eom = event.eom();
            match event.into_kind() {
                HttpBodyEventKind::Body(body) => {
                    self.codec.generate(
                        &mut self.egress,
                        EgressFrame::Body {
                            stream,
                            body,
                            padding: None,
                            end_stream: eom,
                        },
                        &mut NoopCallback,
                    )?;
                }
                HttpBodyEventKind::Trailers(trailers) => {
                    self.codec.generate(
                        &mut self.egress,
                        EgressFrame::Trailers {
                            stream,
                            trailers: &trailers,
                        },
                        &mut NoopCallback,
                    )?;
                }
                HttpBodyEventKind::Suspend(resume) => exchange.resume = Some(resume),
                _ => {
                    return Err(failure(
                        HttpErrorCode::PROTOCOL_ERROR,
                        b"owned H1 response event is not supported",
                    ));
                }
            }
            eom
        };
        self.stream_wake.mark();
        if eom {
            exchange.response_complete = true;
            // `DIV-028`'s rule, as OwnedH2 applies it downstream: the terms run in this order, so
            // only an otherwise eligible exchange queries the queue.
            exchange.retires_at_serialization = exchange.ingress_complete
                && !exchange.registered_byte_events
                && self
                    .write_queue
                    .as_ref()
                    .is_some_and(|queue| !queue.is_saturated());
            exchange.write_boundary = self
                .summary
                .bytes_scheduled
                .saturating_add(self.egress.chain_length() as u64);
            application.response_complete();
            check_cancel(&self.control, &self.context)?;
        }
        Ok(())
    }

    /// Retires the exchange after ingress consumption, response completion and its write.
    fn retire_exchange(&mut self) -> bool {
        let Some(exchange) = self.exchange.as_mut() else {
            return false;
        };
        if !exchange.consumed && exchange.signals.source_complete.get() {
            self.stream_wake.mark();
            return false;
        }
        if !(exchange.consumed
            && exchange.response_complete
            && (exchange.retires_at_serialization
                || exchange.write_boundary <= self.summary.bytes_written))
        {
            return false;
        }
        let mut exchange = self.exchange.take().expect("the exchange was present");
        if let Some(application) = exchange.application.take() {
            capture(&mut self.panic, || application.complete());
        }
        capture(&mut self.panic, || drop(exchange));
        // As OwnedH2: a retirement panic fails the connection, and `run` resumes it.
        if self.panic.is_some() {
            self.abort(failure(
                HttpErrorCode::INTERNAL_ERROR,
                b"owned H1 retirement panicked",
            ));
            return true;
        }
        // A closing connection admits no read-ahead request; the idle check closes it.
        if self.codec.is_reusable() {
            self.exchange_pause = false;
            self.flow_pause = false;
            self.codec.set_parser_paused(false);
        }
        true
    }

    /// Consumes ready reads, so peer EOF already read is seen before the next parse, as
    /// `UniplexSession`'s read loop sees it. Each read after the first uses up one of the turn's
    /// remaining turns, and an empty read ends the batch. `None` when nothing was polled.
    fn poll_read(&mut self, turn: &mut usize) -> Option<bool> {
        let mut read = self.poll_read_once()?;
        let progress = read != ReadPoll::Pending;
        while read == ReadPoll::Data && *turn + 1 < TURN_BUDGET {
            read = match self.poll_read_once() {
                Some(ReadPoll::Pending) | None => break,
                Some(next) => next,
            };
            *turn += 1;
        }
        Some(progress)
    }

    /// `None` when no read was outstanding to poll.
    fn poll_read_once(&mut self) -> Option<ReadPoll> {
        if self.read.is_none()
            && !self.eof
            && self.read_error.is_none()
            && self.fatal.is_none()
            && self.closing_error.is_none()
            && !self.cancellation.is_cancellation_requested()
        {
            let maximum = self
                .config
                .maximum_read_size
                .min(self.ingress.remaining_capacity());
            if maximum != 0 {
                self.read = Some(self.transport.read(
                    &self.context,
                    maximum,
                    self.config.read_allocation_size.min(maximum),
                    self.config.read_timeout,
                    self.cancellation.token(),
                ));
                self.read_wake.mark();
            }
        }
        let read = self.read.as_mut()?;
        if !self.read_wake.take_ready() {
            return None;
        }
        let mut context = Context::from_waker(&self.read_waker);
        let Poll::Ready(result) = read.as_mut().poll(&mut context) else {
            return Some(ReadPoll::Pending);
        };
        self.read.take();
        match result {
            Ok(read) if read.bytes_read() == 0 && !read.is_eof() => return Some(ReadPoll::Empty),
            Ok(read) => {
                self.summary.bytes_read = self
                    .summary
                    .bytes_read
                    .saturating_add(read.bytes_read() as u64);
                if read.is_eof() {
                    // As `UniplexSession`: a response generated after peer EOF closes.
                    self.eof = true;
                    self.codec.generate_immediate_goaway();
                }
                let mut data = read.into_data();
                if self.ingress.append(&mut data).is_err() {
                    self.abort(failure(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"transport read exceeded requested ingress capacity",
                    ));
                }
            }
            Err(error) => {
                if self.fatal.is_none() && !self.cancellation.is_cancellation_requested() {
                    self.read_error = Some(error);
                }
            }
        }
        Some(ReadPoll::Data)
    }

    fn writes_pending(&self) -> bool {
        self.write.is_some()
            || self
                .write_queue
                .as_ref()
                .is_some_and(|queue| !queue.is_idle())
    }

    fn can_start_write(&self) -> bool {
        match &self.write_queue {
            Some(queue) => queue.can_write(
                self.egress
                    .chain_length()
                    .min(self.config.maximum_write_size),
            ),
            None => self.write.is_none(),
        }
    }

    fn start_write(&mut self) -> Result<(), HttpError> {
        let data = self.egress.split_at_most(self.config.maximum_write_size);
        self.summary.bytes_scheduled = self
            .summary
            .bytes_scheduled
            .saturating_add(data.chain_length() as u64);
        self.write_end = self.summary.bytes_scheduled;
        if std::mem::take(&mut self.body_awaits_egress) {
            self.stream_wake.mark();
        }
        if let Some(queue) = &mut self.write_queue {
            queue.start_write(data)?;
        } else {
            self.write = Some(self.transport.write(
                &self.context,
                data,
                ByteEventWriteFlags::default(),
                self.config.write_timeout,
                self.write_cancellation.token(),
            ));
        }
        self.write_wake.mark();
        Ok(())
    }

    /// `None` when no write was ready to poll.
    fn poll_write(&mut self) -> Option<bool> {
        if !self.write_wake.take_ready() {
            return None;
        }
        let mut context = Context::from_waker(&self.write_waker);
        let mut progress = false;
        let mut failed = None;
        if let Some(queue) = &mut self.write_queue {
            while !queue.is_idle() {
                let Poll::Ready(completion) = queue.poll_complete(&mut context) else {
                    break;
                };
                progress = true;
                let bytes = match &completion.result {
                    Ok(info) => info.bytes_written,
                    Err(error) => error.bytes_written(),
                };
                self.summary.bytes_written =
                    self.summary.bytes_written.saturating_add(bytes as u64);
                match completion.result {
                    Ok(_) if bytes == completion.requested_bytes => {}
                    Ok(_) => {
                        failed.get_or_insert(failure(
                            HttpErrorCode::TRANSPORT_WRITE_ERROR,
                            b"transport reported a short successful write",
                        ));
                    }
                    Err(error) => {
                        failed.get_or_insert_with(|| error.error().clone());
                    }
                }
            }
        } else if let Some(write) = &mut self.write {
            let Poll::Ready(result) = write.as_mut().poll(&mut context) else {
                return Some(false);
            };
            self.write.take();
            progress = true;
            let bytes = match &result {
                Ok(info) => info.bytes_written,
                Err(error) => error.bytes_written(),
            };
            self.summary.bytes_written = self.summary.bytes_written.saturating_add(bytes as u64);
            match result {
                Err(error) => failed = Some(error.error().clone()),
                Ok(_) if self.summary.bytes_written != self.write_end => {
                    failed = Some(failure(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"transport reported a short successful write",
                    ));
                }
                Ok(_) => {}
            }
        }
        if let Some(error) = failed {
            self.abort(error);
        }
        Some(progress)
    }

    /// `None` while the outstanding read has yet to observe cancellation, or while a staged
    /// close still reads.
    fn close_idle(&mut self) -> Option<Result<UniplexRunSummary, HttpError>> {
        if self.ingress_stopped && self.stages_close() {
            return None;
        }
        if self.cancellation.request_cancellation() {
            self.read_wake.mark();
        }
        if self.read.is_some() {
            return None;
        }
        let shutdown = self
            .write_shutdown
            .take()
            .unwrap_or_else(|| self.transport.shutdown_write(&self.context));
        Some(shutdown.map(|()| self.summary))
    }

    /// RFC 9112 §9.6's staged close after a stopped upload (`DIV-054`): the write side shuts
    /// down first, and the input read but never to be parsed is discarded, so a full input
    /// buffer still leaves room to read; then the rest of the upload is read, unparsed, until
    /// peer EOF or until `buffer_limit` more bytes are read. A failed shutdown ends it at once,
    /// so the run returns that error. Whether it is still reading.
    #[cold]
    fn stages_close(&mut self) -> bool {
        if self.write_shutdown.is_none() {
            self.write_shutdown = Some(self.transport.shutdown_write(&self.context));
            self.ingress.trim_start_at_most(usize::MAX);
        }
        matches!(self.write_shutdown, Some(Ok(())))
            && !self.eof
            && self.ingress.remaining_capacity() != 0
    }

    /// Cancels the live exchange, queues the error response with `status`, and resets the
    /// connection once that response is written.
    #[cold]
    fn fail_with_response(
        &mut self,
        error: HttpError,
        peer: bool,
        status: u16,
    ) -> Result<(), HttpError> {
        self.cancel_exchange(&error, peer);
        let mut response = get_error_response(status, b"").map_err(|source| {
            HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, source.to_string().as_bytes())
        })?;
        let message = response
            .message_mut()
            .expect("a new error response retains its message");
        let stream = self.codec.last_incoming_stream_id();
        self.codec.generate(
            &mut self.egress,
            EgressFrame::Headers {
                stream,
                message,
                end_stream: true,
                extra_headers: None,
            },
            &mut NoopCallback,
        )?;
        self.closing_error = Some(error);
        Ok(())
    }

    /// Fails the connection for a failed read. As `UniplexSession`, the exchange is cancelled
    /// at once, as the peer's failure, and the transport resets once the bytes already
    /// serialized are written.
    #[cold]
    fn fail_read(&mut self, error: HttpError) {
        if self.egress.is_empty() && !self.writes_pending() {
            self.fail(error, true);
            return;
        }
        self.cancel_exchange(&error, true);
        if self.closing_error.is_none() {
            self.failed_read = Some(error.clone());
            self.closing_error = Some(error);
        }
    }

    #[cold]
    fn reset_transport(&mut self) {
        self.read_wake.mark();
        self.write_wake.mark();
        if !self.transport_reset {
            self.transport_reset = true;
            self.summary.reset = true;
            capture(&mut self.panic, || self.cancellation.request_cancellation());
            capture(&mut self.panic, || {
                self.write_cancellation.request_cancellation()
            });
            if let Some(Err(error)) = capture(&mut self.panic, || {
                self.transport.close_with_reset(&self.context)
            }) {
                self.fatal.get_or_insert(error);
            }
        }
    }

    /// Fails the connection: the live exchange is cancelled and dropped, pending egress is
    /// discarded, and the transport is reset once outstanding operations are cancelled.
    #[cold]
    fn abort(&mut self, error: HttpError) {
        self.fail(error, false);
    }

    /// As `abort`; `peer` is the exchange's `cancel` cause: the peer ended the connection.
    #[cold]
    fn fail(&mut self, error: HttpError, peer: bool) {
        let first = self.failed_read.take();
        self.fatal
            .get_or_insert_with(|| first.unwrap_or_else(|| error.clone()));
        self.cancel_exchange(&error, peer);
        self.egress = BufQueue::new(self.config.buffer_limit);
        self.reset_transport();
    }

    /// Cancels and drops the live exchange with `error`; `peer` is its `cancel` cause.
    #[cold]
    fn cancel_exchange(&mut self, error: &HttpError, peer: bool) {
        if let Some(mut exchange) = self.exchange.take() {
            capture(&mut self.panic, || {
                exchange
                    .producer
                    .abort_with_error(error.clone(), b"owned H1 exchange cancelled")
            });
            if let Some(application) = &mut exchange.application {
                capture(&mut self.panic, || application.cancel(error, peer));
            }
            capture(&mut self.panic, || drop(exchange));
        }
    }
}

impl<A: OwnedApplication> Drop for Driver<A> {
    fn drop(&mut self) {
        if !self.control.complete.get() {
            self.abort(failure(
                HttpErrorCode::CORO_CANCELLED,
                b"owned H1 runner abandoned before cleanup completed",
            ));
            if let Some(panic) = self.panic.take() {
                if std::thread::panicking() {
                    discard_panic(panic);
                } else {
                    resume_unwind(panic);
                }
            }
            assert!(
                !self.started || std::thread::panicking() || in_forced_teardown(),
                "started owned H1 runner dropped before application and native I/O cleanup joined"
            );
        }
    }
}
