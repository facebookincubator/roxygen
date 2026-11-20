/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Experimental owned HTTP/1.1 downstream driver, and its upstream counterpart,
//! [`OwnedH1UpstreamSession`], which the [`upstream`] module documents.
//!
//! This is the poll-driven HTTP/1.1 counterpart of [`crate::owned_h2`]: one driver future per
//! connection runs `Http1xCodec` and the application's [`OwnedExchange`]s, with no task per
//! request. Streams are serial. The parser pauses once a request is complete and resumes when
//! its exchange retires. An exchange retires once its response's last byte is written. It
//! retires as soon as its end of message is serialized if its request is complete, it
//! registered no byte event, and the transport's write queue is not saturated. The owned
//! HTTP/2 driver applies the same rule.
//!
//! [`OwnedH1Session::new`] supports these [`UniplexConfig`] fields:
//!
//! - `buffer_limit`: the request body window, and the bound on unparsed input and on unwritten
//!   output.
//! - `ingress_buffer_limit`: parsing pauses while the unread request body is at this limit.
//! - `maximum_read_size` and `read_allocation_size`.
//! - `maximum_write_size`: the bound on each write, and on each response body poll together
//!   with the output already queued and its chunk framing.
//! - `abort_exchanges_on_eof`: a peer EOF fails the connection while the live exchange is open,
//!   as it does under `UniplexSession`. The EOF is judged at the point where `UniplexSession`
//!   would read it.
//! - `add_date_header_to_response`.
//!
//! HTTP/1 parsing uses llhttp's method table, so `exact_method_tokens` affects only HTTP/2.
//! `new` fails with `INVALID_STATE_TRANSITION` for anything else:
//!
//! - a nonzero `read_timeout`, `stream_read_timeout` or `write_timeout`. The driver has no
//!   timers, so timeouts are up to the caller.
//! - `drain_request_body_on_drop` false, or `wait_for_http1_response_on_drain` or
//!   `deliver_ingress_before_abort` true.
//! - a `maximum_write_size` above `buffer_limit`, or too small to carry a chunk's framing
//!   beside one body byte.
//! - a transport with byte-event capability.
//! - a context that is not on the owner thread or is already cancelled.
//!
//! Under the default [`Http1UpgradePolicy`], framing, close and upgrade follow
//! `UniplexSession`. Trailers are carried in both directions, and the application sends any
//! `100 Continue`. A request the codec rejects gets its error response before the connection
//! is reset. An accepted upgrade or CONNECT carries the tunnel as the exchange's request and
//! response bodies, and the connection does not carry another request after it. Under
//! [`Http1UpgradePolicy::AsOrdinaryRequest`], an upgrade or CONNECT request is served as an
//! ordinary request, and the connection stays HTTP/1.1. Response byte-event registrations are
//! cancelled, never fired.
//!
//! If a request's reader stops it with `NO_ERROR` before it ends, the request still gets its
//! whole response, but the rest of its upload is not parsed. After the response the connection
//! closes in stages: the write side first, then the read side at peer EOF or once
//! `buffer_limit` bytes are read after the write side shuts down.

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::ops::Deref;
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
use crate::owned::PanicSite;
use crate::owned::ReadSlot;
use crate::owned::report_contained_panic;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexRunSummary;
use crate::uniplex::UniplexTransport;
use crate::uniplex::UniplexWriteQueue;

pub mod upstream;

pub use upstream::OwnedH1ExclusiveResponse;
pub use upstream::OwnedH1ExclusiveSubmission;
pub use upstream::OwnedH1PullThroughBody;
pub use upstream::OwnedH1PullThroughResponse;
pub use upstream::OwnedH1PullThroughSubmission;
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

#[cfg(test)]
thread_local! {
    static DRIVER_TURNS: Cell<usize> = const { Cell::new(0) };
    static CODEC_PARSES: Cell<usize> = const { Cell::new(0) };
    static PHASE_CHECKS: Cell<usize> = const { Cell::new(0) };
    static CANCEL_SELECTIONS: Cell<usize> = const { Cell::new(0) };
    static RESPONSE_EVENTS: Cell<usize> = const { Cell::new(0) };
    static RESPONSE_DELIVERIES: Cell<usize> = const { Cell::new(0) };
    static DYN_RESPONSE_DELIVERIES: Cell<usize> = const { Cell::new(0) };
    static BYTE_EVENT_REFUSALS: Cell<usize> = const { Cell::new(0) };
    static READ_START_CHECKS: Cell<usize> = const { Cell::new(0) };
    static READ_POLLS: Cell<usize> = const { Cell::new(0) };
    static WRITE_POLLS: Cell<usize> = const { Cell::new(0) };
    static SIGNAL_TAKES: Cell<usize> = const { Cell::new(0) };
    static AFTER_BODY_EVENT_SERIALIZED: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    static UPSTREAM_RUN_FAULT: Cell<bool> = const { Cell::new(false) };
    static UPSTREAM_COMMIT_FAULT: Cell<bool> = const { Cell::new(false) };
}

/// Makes the next pending poll of an owned HTTP/1.1 upstream run on this thread panic outside the
/// driver's boundaries, where only a fault in the run's own code could.
#[cfg(test)]
pub(crate) fn fault_next_upstream_run_poll() {
    UPSTREAM_RUN_FAULT.set(true);
}

/// Makes the next request commitment of an owned HTTP/1.1 upstream on this thread panic midway,
/// where only a fault in the driver's commit code could.
#[cfg(test)]
pub(crate) fn fault_next_upstream_commit() {
    UPSTREAM_COMMIT_FAULT.set(true);
}

/// How many turns the owned HTTP/1.1 drivers on this thread have run.
#[cfg(test)]
pub(crate) fn driver_turns() -> usize {
    DRIVER_TURNS.with(Cell::get)
}

/// How many times the owned HTTP/1.1 drivers on this thread have handed their input to the
/// codec (`on_ingress`, or `on_ingress_with_callback` upstream).
#[cfg(test)]
pub(crate) fn codec_parses() -> usize {
    CODEC_PARSES.with(Cell::get)
}

/// How many times the owned HTTP/1.1 drivers on this thread have checked whether a phase before
/// the write would act (`phases_due_before_write`).
#[cfg(test)]
pub(crate) fn phase_checks() -> usize {
    PHASE_CHECKS.with(Cell::get)
}

/// How many times the owned HTTP/1.1 upstream drivers on this thread have selected a live
/// exchange's cancel cause (`Exchange::cancel_error`).
#[cfg(test)]
pub(crate) fn cancel_selections() -> usize {
    CANCEL_SELECTIONS.with(Cell::get)
}

/// How many response codec events the owned HTTP/1.1 upstream drivers on this thread have
/// handled: checked for a failure, a missing exchange and a cancel, then acted on.
#[cfg(test)]
pub(crate) fn response_events() -> usize {
    RESPONSE_EVENTS.with(Cell::get)
}

/// How many response codec callbacks the owned HTTP/1.1 upstream drivers on this thread have
/// received (`on_ingress_event`), and how many of them came through `dyn HttpCodecCallback`.
#[cfg(test)]
pub(crate) fn response_deliveries() -> (usize, usize) {
    (
        RESPONSE_DELIVERIES.with(Cell::get),
        DYN_RESPONSE_DELIVERIES.with(Cell::get),
    )
}

/// How many byte-event refusal errors the owned HTTP/1.1 drivers on this thread have built
/// (`cancel_byte_events`).
#[cfg(test)]
pub(crate) fn byte_event_refusals() -> usize {
    BYTE_EVENT_REFUSALS.with(Cell::get)
}

/// How many times the owned HTTP/1.1 drivers on this thread, with no read started, have evaluated
/// whether to start one (`poll_read_once`).
#[cfg(test)]
pub(crate) fn read_start_checks() -> usize {
    READ_START_CHECKS.with(Cell::get)
}

/// How many times the owned HTTP/1.1 drivers on this thread have entered their read phase
/// (`poll_read_once`) and their write-completion phase (`poll_write`), and taken a wake signal
/// (`Signal::take_ready`).
#[cfg(test)]
pub(crate) fn read_write_polls_and_signal_takes() -> (usize, usize, usize) {
    (
        READ_POLLS.with(Cell::get),
        WRITE_POLLS.with(Cell::get),
        SIGNAL_TAKES.with(Cell::get),
    )
}

/// Runs `hook` once, right after this thread's owned HTTP/1.1 downstream drivers serialize the
/// next response body event and before the driver's next step. Tests use it to land input from
/// another thread, the transport or a request body read in that window.
#[cfg(test)]
pub(crate) fn after_next_body_event_serialized(hook: impl FnOnce() + 'static) {
    AFTER_BODY_EVENT_SERIALIZED.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
fn body_event_serialized() {
    let hook = AFTER_BODY_EVENT_SERIALIZED.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

/// One of a driver's wake signals, for a test that marks it from another thread, as its source's
/// wake does first (`Signal::wake_by_ref`), while the driver consumes the wake to poll that
/// source immediately (`Signal::clear`).
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SignalProbe(Arc<Signal>);

#[cfg(test)]
impl SignalProbe {
    pub(crate) fn new() -> Self {
        Self(Signal::new(&Arc::new(TaskWaker::default())))
    }

    pub(crate) fn mark(&self) {
        self.0.mark();
    }

    pub(crate) fn clear(&self) {
        self.0.clear();
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.0.is_ready()
    }
}

#[cfg(test)]
fn count(counter: &'static std::thread::LocalKey<Cell<usize>>) {
    counter.with(|count| count.set(count.get() + 1));
}

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
    /// Handle the upgrade downstream as `UniplexSession` does, and reject it upstream.
    /// Downstream, parsing waits for the final response head. If that head accepts the
    /// upgrade, the bytes that follow are the request body through EOF, and the response body
    /// is sent unframed. If it rejects the upgrade, the request ends and the connection closes
    /// after the response. Upstream, the owned driver does not carry tunnels and rejects the
    /// request before writing any of it.
    #[default]
    Proxygen,
    /// Treat the request as an ordinary request, with its method, target and headers
    /// unchanged, and carry no tunnel. Downstream, the request is handed over and parsing
    /// continues after it as HTTP/1.1 (`Http1xCodec::set_downstream_upgrade_as_ordinary`). A
    /// response that switches protocols is rejected. Upstream, the request is sent and its
    /// response read with their own framing (`Http1xCodec::set_upstream_upgrade_as_ordinary`).
    /// A head that does not frame its body the way the codec writes it, and the egress
    /// websocket flag, are rejected before any byte is written. A `101` fails the connection.
    AsOrdinaryRequest,
}

/// The session owner's waker, shared by every wake source of one connection, and its wake
/// record. Each wake sets the record, and the owned HTTP/1.1 upstream driver clears it as each
/// of its polls begins, so that a pull-through body can check whether one of the driver's wake
/// sources fired since the driver's last poll.
#[derive(Default)]
struct TaskWaker {
    waker: Mutex<Option<Waker>>,
    woken: AtomicBool,
}

impl TaskWaker {
    fn register(&self, waker: &Waker) {
        let replaced = {
            let mut slot = self.waker.lock();
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
        self.woken.store(true, Ordering::Release);
        let waker = self.waker.lock().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Clears the wake record as a poll begins. The swap acquires the store of a wake it
    /// overwrites, so the poll observes the signal that wake marked first.
    #[inline]
    fn clear_wake_record(&self) {
        self.woken.swap(false, Ordering::AcqRel);
    }

    /// Whether a wake happened since the last poll began.
    #[inline]
    fn woken(&self) -> bool {
        self.woken.load(Ordering::Acquire)
    }

    /// Records a wake without waking, for a drive revocation.
    fn note_wake(&self) {
        self.woken.store(true, Ordering::Release);
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

    #[inline]
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Consumes a wake in place of `take_ready`, for a driver that polls this signal's source
    /// immediately. As in `take_ready`, the swap acquires the mark of a wake it overwrites, so
    /// the poll observes what that wake's source published first. A later wake marks the signal
    /// again. A plain store could overwrite another thread's mark, and the poll would then miss
    /// what that thread published.
    #[inline]
    fn clear(&self) {
        if self.is_ready() {
            self.ready.swap(false, Ordering::AcqRel);
        }
    }

    fn take_ready(&self) -> bool {
        #[cfg(test)]
        count(&SIGNAL_TAKES);
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
    /// Set by `complete_handshake`.
    handshake_complete: Cell<bool>,
    task: Arc<TaskWaker>,
}

impl Control {
    /// Marks the connection handshake complete. The driver calls it as it delivers a validated
    /// request head to its exchange, and only there.
    fn complete_handshake(&self) {
        self.handshake_complete.set(true);
    }
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

    /// Whether the connection handshake has completed: the driver has validated the first
    /// request head and delivered it to its exchange. It stays true once set.
    pub fn handshake_complete(&self) -> bool {
        self.0.handshake_complete.get()
    }

    /// Whether the session has finished: true once its run has completed, with or without an
    /// error. A session that was never run, or whose run was dropped before completing, has not
    /// finished. A run that resumes a callback panic has finished when the panic resumes.
    pub fn is_finished(&self) -> bool {
        self.0.complete.get()
    }
}

/// Request-source notifications; they only record and wake the owner.
struct IngressSignals {
    source_complete: Cell<bool>,
    completed: Cell<Option<Result<(), HttpError>>>,
    stream: Arc<Signal>,
    /// The connection of a pull-through exchange. The source's completion returns the drive to
    /// the connection's run before the wake that announces it.
    drive: Option<upstream::DriveLink>,
    /// An upstream connection's returning hook, armed while the exchange
    /// is published as retiring, which the source's completion runs.
    returning: Cell<Option<Rc<dyn Fn()>>>,
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

#[cfg(test)]
thread_local! {
    /// `bytes_processed` callbacks that this thread's owned HTTP/1.1 drivers received.
    static INGRESS_CREDIT_CALLBACKS: Cell<usize> = const { Cell::new(0) };
}

/// Takes this thread's count of owned HTTP/1.1 `bytes_processed` callbacks, resetting it.
#[cfg(test)]
pub(crate) fn take_ingress_credit_callbacks_for_test() -> usize {
    INGRESS_CREDIT_CALLBACKS.replace(0)
}

impl HttpStreamSourceSharedCallback for IngressSignals {
    fn bytes_processed(&self, _id: StreamId, _amount: usize, _acknowledge: usize) {
        #[cfg(test)]
        INGRESS_CREDIT_CALLBACKS.set(INGRESS_CREDIT_CALLBACKS.get() + 1);
        self.stream.wake_by_ref();
    }

    fn source_complete(&self, id: StreamId, error: Option<HttpError>) {
        self.source_complete.set(true);
        self.completed.set(Some(error.map_or(Ok(()), Err)));
        if let Some(drive) = &self.drive {
            drive.hand_back(id);
        }
        if let Some(returning) = self.returning.take() {
            returning();
        }
        self.stream.wake_by_ref();
    }
}

/// Cancels a polled event's byte-event registrations with `INVALID_STATE_TRANSITION`, because
/// this driver does not track byte offsets. Their callbacks may cancel the session, so a cancel
/// is checked afterwards.
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

/// Cancels each registration with `INVALID_STATE_TRANSITION`: this driver does not deliver byte
/// events.
fn cancel_byte_events(registrations: Vec<HttpByteEventRegistration>) {
    if registrations.is_empty() {
        return;
    }
    #[cfg(test)]
    count(&BYTE_EVENT_REFUSALS);
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
    /// The request closes the connection, because it does not keep the connection alive or
    /// because its upgrade was rejected.
    closing_request: bool,
    /// The upgrade completed: the exchange carries an established tunnel.
    tunnel: bool,
    ingress_complete: bool,
    consumed: bool,
    response_headers: bool,
    response_complete: bool,
    /// A suspended response body read's resume, awaited before the body is read again.
    resume: Option<ResumeFuture>,
    write_boundary: u64,
    /// The response registered a byte event, which was cancelled. The exchange keeps the write
    /// fence.
    registered_byte_events: bool,
    /// Skips the write fence under the early-retirement rule. Set when the response ends.
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

/// The result of polling the outstanding read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPoll {
    Pending,
    /// Ready with no bytes and no EOF.
    Empty,
    /// Ready with bytes, EOF or a failure.
    Data,
}

#[inline]
fn cancel_requested(control: &Control, context: &WorkerContext) -> bool {
    control.cancel.get() || context.is_cancelled()
}

/// Run after each application callback. Once a callback cancels the session, the driver does
/// not call, serialize or write anything more.
fn check_cancel(control: &Control, context: &WorkerContext) -> Result<(), HttpError> {
    if cancel_requested(control, context) {
        return Err(cancelled());
    }
    Ok(())
}

/// Whether parsing paused behind the unread request body may resume: the body is below
/// `ingress_buffer_limit`, or the source has stopped buffering it.
#[inline]
fn resumes_parsing(producer: &HttpStreamSourceProducer, ingress_buffer_limit: u32) -> bool {
    producer
        .body_bytes_buffered()
        .is_none_or(|buffered| buffered < ingress_buffer_limit)
}

/// `HttpCodecCallback::ingress_body_limit` for the body `producer` receives: how far its unread
/// body may grow before it reaches `config`'s HTTP/1.1 ceiling.
#[inline]
fn ingress_body_limit(
    producer: &HttpStreamSourceProducer,
    config: &UniplexConfig,
) -> Option<usize> {
    let buffered = producer.body_bytes_buffered()? as usize;
    Some(config.http1_ingress_body_ceiling().saturating_sub(buffered))
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
/// carries `Connection: close`, and the idle connection closes.
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

/// Handles a request whose reader stopped it without error before it ended. HTTP/1.1 cannot
/// reset one request, so the rest of the upload is not parsed and the codec stops keep-alive.
/// A response head not yet serialized carries `Connection: close`, and the connection closes
/// once the response is written.
#[cold]
fn stop_request_ingress(codec: &mut Http1xCodec, ingress_stopped: &mut bool) {
    *ingress_stopped = true;
    codec.generate_immediate_goaway();
}

/// A connection's codec, and whether a parse could act on its input. The flag is set by new
/// input and by every change to the codec outside a parse, and cleared by a parse that consumes
/// all of its input. Until it is set again, parsing the same input with the same codec would
/// have no effect: the codec defers a message's completion only while its parser is paused, and
/// only these changes resume it.
struct IngressCodec {
    codec: Http1xCodec,
    reparse: bool,
}

impl IngressCodec {
    fn new(codec: Http1xCodec) -> Self {
        Self {
            codec,
            reparse: true,
        }
    }

    /// The codec, for a change outside a parse, after which a parse may act again.
    fn get_mut(&mut self) -> &mut Http1xCodec {
        self.reparse = true;
        &mut self.codec
    }
}

impl Deref for IngressCodec {
    type Target = Http1xCodec;

    fn deref(&self) -> &Http1xCodec {
        &self.codec
    }
}

/// A poll-driven HTTP/1.1 downstream connection with one exchange at a time.
pub struct OwnedH1Session<A: OwnedApplication>(Driver<A>);

impl<A: OwnedApplication> OwnedH1Session<A> {
    pub fn new(
        application: A,
        options: OwnedH1Options,
    ) -> Result<(Self, OwnedH1Handle), HttpError> {
        Self::new_with(options, |_| application)
    }

    /// Creates the session as `new` does, with the application that `make` builds from the
    /// session's handle, as `OwnedH2Session::new_with` does. `make` runs once, after every check
    /// that can fail creation, so it does not run for a session that returns an error.
    pub fn new_with(
        options: OwnedH1Options,
        make: impl FnOnce(&OwnedH1Handle) -> A,
    ) -> Result<(Self, OwnedH1Handle), HttpError> {
        let (driver, handle) = Driver::new(options, make)?;
        Ok((Self(driver), handle))
    }

    /// Runs the session inside the caller's registered worker cleanup scope (see
    /// `WorkerContext::register_scope`), as `OwnedH2Session::run` does.
    ///
    /// Dropping the run after its first poll and before it completes panics, outside a forced
    /// worker teardown, because its native I/O has not joined. To stop early, call the handle's
    /// `cancel` and poll the run to completion. Dropping the session or its run before the first
    /// poll does not panic.
    pub fn run(self) -> impl Future<Output = Result<UniplexRunSummary, HttpError>> {
        DriverRun(Some(self.0))
    }
}

/// Drives a [`Driver`] to completion, isolating callback panics in every poll.
struct DriverRun<A: OwnedApplication>(Option<Driver<A>>);

// The driver is only accessed through `&mut` and is never pinned.
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
                report_contained_panic(&driver.context, PanicSite::DriverTurn, None, &*panic);
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
    codec: IngressCodec,
    exchange: Option<Box<Exchange<A::Exchange>>>,
    ingress: BufQueue,
    egress: BufQueue,
    read: ReadSlot,
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
    /// The peer EOF was judged under `abort_exchanges_on_eof` and did not abort, so the codec
    /// may take it.
    eof_judged: bool,
    /// Under `abort_exchanges_on_eof`, reading continues after a complete request that closes
    /// the connection, so that the peer's EOF is seen. Those reads are discarded unparsed.
    eof_watch: bool,
    /// A failed read, applied once what was read before it is handled (`holds_read_error`).
    read_error: Option<HttpError>,
    exchange_pause: bool,
    flow_pause: bool,
    /// Parsing waits past an upgrade or CONNECT request head until its final response head
    /// resolves the upgrade in the codec, as `UniplexSession`'s upgrade pause.
    upgrade_pending: bool,
    /// The request's reader stopped it without error before it ended, so the rest of the input
    /// is not parsed.
    ingress_stopped: bool,
    /// The result of shutting down the write side, once a staged close after a stopped upload
    /// has done so.
    write_shutdown: Option<Result<(), HttpError>>,
    /// The response body was not polled because the next write must make room for it.
    body_awaits_egress: bool,
    fatal: Option<HttpError>,
    /// The connection failed with an error response queued: it resets once that is written.
    closing_error: Option<HttpError>,
    /// A failed read that resets the connection once the serialized bytes are written. As in
    /// `UniplexSession`, it stays the run's error if another failure resets first.
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
            drive: None,
            returning: Cell::new(None),
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
        // Parsing resumes once the unread body is below the limit it paused at.
        producer.set_resume_threshold(Some(self.config.ingress_buffer_limit));
        let drain = source.drain_on_drop();
        let application = self.application.new_exchange_with(id, || {
            source
                .ingress_completion()
                .expect("a fresh request source has an ingress receipt")
        });
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
            closing_request: false,
            tunnel: false,
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

    fn ingress_body_limit(&self, _stream: StreamId) -> Option<usize> {
        ingress_body_limit(&self.exchange.as_ref()?.producer, self.config)
    }

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
                // as one, unless it parses them as ordinary requests.
                let upgrade = message.is_ingress_websocket_upgrade() || message.is_upgraded();
                exchange.upgrade = upgrade;
                exchange.closing_request = !message.wants_keepalive();
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
                    // Not a window overrun: the body limit keeps body events within the window.
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
                if let Some(exchange) = self.exchange.as_mut() {
                    exchange.tunnel = true;
                }
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
    fn new(
        options: OwnedH1Options,
        make: impl FnOnce(&OwnedH1Handle) -> A,
    ) -> Result<(Self, OwnedH1Handle), HttpError> {
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
            handshake_complete: Cell::new(false),
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
        let read = ReadSlot::new(
            transport.as_ref(),
            &context,
            io.read_timeout,
            cancellation.token(),
        )?;
        let mut codec = Http1xCodec::with_options(
            TransportDirection::Downstream,
            false,
            http1.strict_validation,
        );
        codec.set_downstream_upgrade_as_ordinary(
            http1.upgrade_policy == Http1UpgradePolicy::AsOrdinaryRequest,
        );
        codec.set_add_date_header_to_response(io.add_date_header_to_response);
        let handle = OwnedH1Handle(control);
        let application = make(&handle);
        Ok((
            Self {
                context,
                transport,
                application,
                control: Rc::clone(&handle.0),
                config: io,
                codec: IngressCodec::new(codec),
                exchange: None,
                ingress: BufQueue::new(io.buffer_limit),
                egress: BufQueue::new(io.buffer_limit),
                read,
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
                eof_judged: false,
                eof_watch: false,
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
            handle,
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
        // The progress of this turn's opening phases, when the turn before ran them.
        let mut opened = None;
        while turn < TURN_BUDGET {
            #[cfg(test)]
            count(&DRIVER_TURNS);
            let mut progress = match opened.take() {
                Some(progress) => progress,
                None => self.open_turn(),
            };
            if let Some(read) = self.poll_read(&mut turn) {
                progress |= read;
                self.observe_cancel();
                // A drain requested during the read takes effect before its input is parsed, so
                // a request that the read carried is not admitted.
                self.observe_drain();
            }
            self.observe_stop();
            if self.parse_due() {
                match self.parse_input() {
                    Ok(parsed) => progress |= parsed,
                    Err(error) => self.abort(error),
                }
            }
            // Input the parse step left paused, as behind a retiring exchange or an upgrade, is
            // parsed before a peer EOF is judged.
            let paused = self.ingress_paused();
            if !self.holds_read_error()
                && let Some(error) = self.read_error.take()
            {
                self.fail_read(error);
                progress = true;
            }
            if self.fatal.is_none() && self.stream_wake.take_ready() {
                progress = true;
                match self.poll_exchange(&mut turn) {
                    Ok(false) => {}
                    // The exchange ran this turn's write decision, which held the write back, and
                    // the rest of the turn would have no effect.
                    Ok(true) => {
                        turn += 1;
                        continue;
                    }
                    Err(error) => self.abort(error),
                }
            }
            if self.fatal.is_none() && self.retire_exchange() {
                progress = true;
            }
            if self.eof_abort_due() && !(paused && !self.ingress_paused()) {
                self.judge_eof();
                progress = true;
            }
            // The write waits while a phase before it can still add to it, since a phase that
            // acted this turn may act again. Otherwise the write starts in place of the next turn.
            // That turn still counts against the budget, which sets when a write is forced and
            // when the poll yields.
            let write_waits = progress
                && self.egress.chain_length() < self.config.maximum_write_size
                && turn + 1 < TURN_BUDGET
                && self.fatal.is_none();
            let write_ready = self.write_ready();
            let write_started = write_ready && (!write_waits || !self.phases_due_before_write());
            if write_started {
                if write_waits {
                    turn += 1;
                }
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
                && !self.read.is_reading()
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
                && !self.eof_abort_watched()
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
            if turn >= TURN_BUDGET {
                break;
            }
            // After a turn that started a write, the next is due: the write marked its signal, or
            // a failure reset the connection. That turn opens by taking the write's completion,
            // so it is opened here, and the rest of it runs only if it would act. If not, that
            // turn would have ended the poll, with the yield if it was the budget's last.
            if write_started {
                let progress = self.open_turn();
                if turn + 1 < TURN_BUDGET && !self.turn_due() {
                    return Poll::Pending;
                }
                opened = Some(progress);
                continue;
            }
            // Return here if the next turn would not act. After a turn that held its write back,
            // the next turn is due, because the phase that held the write back still is.
            if !write_ready && !self.turn_due() {
                return Poll::Pending;
            }
        }
        self.budget_yield = Some(self.context.host().yield_to_io());
        // A yield that completes on its first poll arranged no wake.
        if self.poll_budget_yield(context).is_ready() {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// Whether a next turn would act. The terms cover a phase before the write, the write, a
    /// closing error, a failure or a reset, and the idle close. Each is the condition under which
    /// that step acts, or a wider one, and evaluating them does not run transport or application
    /// code.
    fn turn_due(&self) -> bool {
        self.transport_reset
            || self.fatal.is_some()
            || self.closing_error.is_some()
            || self.phases_due_before_write()
            || if self.egress.is_empty() {
                self.exchange.is_none() && (self.eof_processed || !self.codec.is_reusable())
            } else {
                // A queue's capacity check is transport code.
                self.write_queue.is_some() || self.write.is_none()
            }
    }

    /// Whether a phase before the write would act in a next turn. The phases are the write
    /// completion, the read, the exchange, the cancel and drain checks, a read start, the parse,
    /// a held read failure, the exchange's retirement and the peer EOF judgment. Each term is the
    /// condition under which that phase acts, or a wider one. The terms do not run transport or
    /// application code or change state, so their order affects only cost. The signals, which
    /// most often make a turn due, come first.
    #[inline(always)]
    fn phases_due_before_write(&self) -> bool {
        #[cfg(test)]
        count(&PHASE_CHECKS);
        self.write_wake.is_ready()
            || self.read_wake.is_ready()
            || (self.fatal.is_none() && self.stream_wake.is_ready())
            || self.unsignalled_phases_due()
    }

    /// The terms of `phases_due_before_write` that follow its signals.
    #[inline(always)]
    fn unsignalled_phases_due(&self) -> bool {
        (!self.transport_reset && cancel_requested(&self.control, &self.context))
            || (self.control.drain.get() && !self.draining && self.fatal.is_none())
            || (!self.read.is_reading() && !self.read.is_drained() && self.read_capacity() != 0)
            || self.parse_due()
            || (self.read_error.is_some() && !self.holds_read_error())
            || (self.fatal.is_none() && self.retire_due())
            || self.eof_abort_due()
    }

    /// Whether this turn, right after the exchange serialized a body event that does not end the
    /// response, has done everything before its write decision, and that decision holds the
    /// write back behind the exchange's signal, which the event marks (`write_waits`). That is
    /// the case when the peer EOF judgment is not due and the write is below the write size
    /// within the budget. Retirement cannot act before the response ends, and the steps after
    /// the decision do not act while the exchange is live.
    #[inline(always)]
    fn write_held_after_body_event(&self, turn: usize) -> bool {
        turn + 1 < TURN_BUDGET
            && self.egress.chain_length() < self.config.maximum_write_size
            && !self.eof_abort_due()
    }

    /// Whether the next turn would only poll the body again, when this turn held its write back
    /// behind a body event (`write_held_after_body_event`). This is `phases_due_before_write`
    /// without the exchange's signal, plus the flow-control lift before `poll_exchange`'s body
    /// poll. `retire_due` and `eof_abort_due` cover the other steps before that poll. Each term
    /// is exact or wider, so polling the body now runs that turn.
    #[inline(always)]
    fn body_polls_again(&self) -> bool {
        !self.write_wake.is_ready()
            && !self.read_wake.is_ready()
            && !self.unsignalled_phases_due()
            && !(self.flow_pause
                && self.exchange.as_ref().is_some_and(|exchange| {
                    resumes_parsing(&exchange.producer, self.config.ingress_buffer_limit)
                }))
    }

    /// Whether the parse phase acts: the input or the codec changed since a parse consumed all
    /// of its input, and there is something to parse. A busy codec may hold a completion it
    /// deferred while the parser was paused. As in `UniplexSession`, an idle draining connection
    /// does not admit another request.
    fn parse_due(&self) -> bool {
        self.codec.reparse
            && self.fatal.is_none()
            && self.closing_error.is_none()
            && !self.ingress_paused()
            && !(self.draining && self.exchange.is_none())
            && (!self.ingress.is_empty()
                || self.codec.is_busy()
                // Under `abort_exchanges_on_eof`, the codec takes the EOF only once it is judged.
                || (self.eof
                    && !self.eof_processed
                    && (self.eof_judged || !self.config.abort_exchanges_on_eof)))
    }

    /// Whether `retire_exchange` acts: it notes the request source's completion, or retires
    /// the exchange.
    fn retire_due(&self) -> bool {
        self.exchange.as_ref().is_some_and(|exchange| {
            if exchange.consumed {
                exchange.response_complete
                    && (exchange.retires_at_serialization
                        || exchange.write_boundary <= self.summary.bytes_written)
            } else {
                exchange.signals.source_complete.get()
            }
        })
    }

    /// A turn's opening phases: the cancel and drain checks, then the write's completion. Returns
    /// whether the write progressed.
    #[inline(always)]
    fn open_turn(&mut self) -> bool {
        self.observe_cancel();
        self.observe_drain();
        let Some(wrote) = self.poll_write() else {
            return false;
        };
        self.observe_cancel();
        wrote
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

    /// A stop that the request's reader reported since the last poll takes effect before any
    /// input read with it is parsed. As in `poll_exchange`, it has no effect once the response
    /// has ended.
    fn observe_stop(&mut self) {
        if let Some(exchange) = &self.exchange
            && !exchange.ingress_complete
            && !exchange.response_complete
            && exchange.signals.stopped()
        {
            stop_request_ingress(self.codec.get_mut(), &mut self.ingress_stopped);
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
        if let Err(error) = drain_codec(self.codec.get_mut(), &mut self.egress) {
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
            #[cfg(test)]
            count(&CODEC_PARSES);
            let result =
                self.codec
                    .codec
                    .on_ingress(&self.ingress, &mut callback, &mut self.egress);
            (result.consumed, result.egress_results)
        };
        // As `UniplexSession`, entering the upgrade is progress: the tunnel's bytes are parsed
        // before EOF.
        let upgrade_completed = callback.upgrade_completed;
        // The codec ignores EOF while its parser is paused, so EOF waits behind every pause:
        // an upgrade's until the final response head resolves it.
        // Under `abort_exchanges_on_eof`, the codec takes the EOF only once it is judged, so a
        // mid-request EOF cannot fail the request first.
        let finishing_eof = self.eof
            && !self.eof_processed
            && (self.eof_judged || !self.config.abort_exchanges_on_eof)
            && !upgrade_completed
            && !*callback.upgrade_pending
            && !*callback.exchange_pause
            && !*callback.flow_pause
            && (consumed == length || consumed == 0);
        if finishing_eof {
            self.eof_processed = true;
            let result = self
                .codec
                .codec
                .on_ingress_eof(&mut callback, &mut self.egress);
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
        if self.config.abort_exchanges_on_eof
            && !self.eof_watch
            && self
                .exchange
                .as_ref()
                .is_some_and(|exchange| exchange.ingress_complete && exchange.closing_request)
        {
            self.start_eof_watch();
        }
        // The codec parses until its input runs out or its parser pauses or fails, and the
        // EOF it can take is taken: a parse of what remains acts only after a change. The
        // tunnel an upgrade entered is parsed next.
        self.codec.reparse = !self.ingress.is_empty() || upgrade_completed;
        for result in egress_results {
            result?;
        }
        if let Some((error, peer)) = failed {
            match error_status {
                // As `UniplexSession`: a request whose final response head is not yet serialized
                // gets the codec's error response before the connection is reset.
                Some(status)
                    if !self
                        .exchange
                        .as_ref()
                        .is_some_and(|exchange| exchange.response_headers) =>
                {
                    self.fail_with_response(error, peer, status)?;
                }
                // As `UniplexSession`'s `reset_h1_ingress_stream`: a peer EOF inside a request
                // whose response has started resets once the serialized bytes are written.
                _ if peer => self.fail_read(error),
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

    /// Whether a failed read waits for what was read before it. `UniplexSession` reads only once
    /// it has parsed everything read so far, so it handles the failure after that. The wait
    /// covers a pending upgrade, bytes a pause keeps unparsed (an unfinished request's body
    /// behind flow control, or a pipelined request behind the exchange), and a whole request the
    /// exchange has not received. `UniplexSession`'s handler still reads a request that ended
    /// before the failure, but not an unfinished one.
    fn holds_read_error(&self) -> bool {
        self.upgrade_pending
            || (self.flow_pause && self.codec.is_busy())
            || (self.exchange_pause && !self.ingress.is_empty())
            || self.exchange.as_ref().is_some_and(|exchange| {
                !exchange.started && exchange.head.is_some() && exchange.ingress_complete
            })
    }

    /// Hands the request to the exchange and serializes what its response produces.
    ///
    /// After a response body event that does not end the response, if the rest of the turn
    /// would only hold the write back (`write_held_after_body_event`), it runs that write
    /// decision's capacity check, which may reenter. It then polls the body again in place of
    /// the next turn while that turn would only do the same (`body_polls_again`), and counts
    /// each such poll as a turn. Otherwise it ends the turn there and returns `true`, meaning
    /// that this turn's write decision has run.
    fn poll_exchange(&mut self, turn: &mut usize) -> Result<bool, HttpError> {
        let Some(exchange) = self.exchange.as_mut() else {
            return Ok(false);
        };
        if self.flow_pause && resumes_parsing(&exchange.producer, self.config.ingress_buffer_limit)
        {
            self.flow_pause = false;
            self.codec.get_mut().set_parser_paused(self.exchange_pause);
        }
        if !exchange.consumed && exchange.signals.source_complete.get() {
            exchange.consumed = true;
            if let Some(Err(error)) = exchange.signals.completed.take()
                && !exchange.response_complete
            {
                // If the reader stopped an ordinary request without error, its response still
                // finishes.
                if error.code() != HttpErrorCode::NO_ERROR || exchange.upgrade {
                    return Err(error);
                }
                if !exchange.ingress_complete {
                    stop_request_ingress(self.codec.get_mut(), &mut self.ingress_stopped);
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
            self.control.complete_handshake();
            exchange
                .application
                .as_mut()
                .expect("the application remains owned")
                .request_body(head, source, eom);
            check_cancel(&self.control, &self.context)?;
            // `UniplexSession`'s read loop handles a failed read it already holds before the
            // request's handler runs, so here too the failure ends the exchange first.
            if self.read_error.is_some() && !self.holds_read_error() {
                return Ok(false);
            }
        }
        // Under `abort_exchanges_on_eof`, an EOF already read is judged before another response
        // event is serialized, as `UniplexSession`'s `handle_eof` runs in its read loop before
        // its egress task. The application is still polled, but what it returns is not
        // serialized, because the exchange is open and the judgment aborts it.
        let eof_due = self.eof_abort_due() && self.exchange_open();
        let mut exchange = self
            .exchange
            .as_mut()
            .expect("nothing above removes the exchange");
        if !exchange.started || exchange.response_complete {
            return Ok(false);
        }
        let stream = self.codec.last_incoming_stream_id();
        let mut application = exchange
            .application
            .as_mut()
            .expect("the application remains owned");
        let eom = if !exchange.response_headers {
            let Poll::Ready(headers) = application.poll_headers(&mut context) else {
                return Ok(false);
            };
            check_cancel(&self.control, &self.context)?;
            if eof_due {
                drop(headers);
                self.judge_eof();
                return Ok(false);
            }
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
                drain_codec(self.codec.get_mut(), &mut self.egress)?;
            }
            // So does a stop that the exchange's callbacks reported.
            if !exchange.ingress_complete && exchange.signals.stopped() {
                stop_request_ingress(self.codec.get_mut(), &mut self.ingress_stopped);
            }
            let generated = self.codec.get_mut().generate(
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
                // As `UniplexSession`: the final head accepts or rejects the upgrade, and parsing
                // resumes, into the tunnel or on to the request's end.
                self.upgrade_pending = false;
                let codec = self.codec.get_mut();
                if codec.resolve_downstream_upgrade(headers.headers()) == Some(false) {
                    // As `UniplexSession`, a rejected upgrade closes the connection after its
                    // request.
                    exchange.closing_request = true;
                }
                codec.set_parser_paused(self.exchange_pause || self.flow_pause);
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
                    return Ok(false);
                }
                exchange.resume = None;
                check_cancel(&self.control, &self.context)?;
            }
            loop {
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
                    return Ok(false);
                }
                let Poll::Ready(event) = application.poll_body(&mut context, room as u32) else {
                    return Ok(false);
                };
                check_cancel(&self.control, &self.context)?;
                if eof_due {
                    drop(event);
                    self.judge_eof();
                    return Ok(false);
                }
                let mut event = event?;
                let registrations = event.take_byte_event_registrations();
                exchange.registered_byte_events |= !registrations.is_empty();
                refuse_byte_events(registrations, &self.control, &self.context)?;
                // A stop reported with this event, which may be the response's last, or from its
                // byte events' cancellation.
                if !exchange.ingress_complete && exchange.signals.stopped() {
                    stop_request_ingress(self.codec.get_mut(), &mut self.ingress_stopped);
                }
                let eom = event.eom();
                match event.into_kind() {
                    HttpBodyEventKind::Body(body) => {
                        self.codec.get_mut().generate(
                            &mut self.egress,
                            EgressFrame::Body {
                                stream,
                                body,
                                padding: None,
                                end_stream: eom,
                            },
                            &mut NoopCallback,
                        )?;
                        #[cfg(test)]
                        body_event_serialized();
                        if !eom && self.write_held_after_body_event(*turn) {
                            // The decision's result does not matter: the write waits.
                            let _ = self.write_ready();
                            if !self.body_polls_again() {
                                self.stream_wake.mark();
                                return Ok(true);
                            }
                            *turn += 1;
                            self.stream_wake.clear();
                            exchange = self
                                .exchange
                                .as_mut()
                                .expect("nothing above removes the exchange");
                            application = exchange
                                .application
                                .as_mut()
                                .expect("the application remains owned");
                            continue;
                        }
                    }
                    HttpBodyEventKind::Trailers(trailers) => {
                        self.codec.get_mut().generate(
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
                break eom;
            }
        };
        self.stream_wake.mark();
        if eom {
            let exchange = self
                .exchange
                .as_mut()
                .expect("nothing above removes the exchange");
            exchange.response_complete = true;
            // The early-retirement rule, as OwnedH2 applies it downstream. The terms run in this
            // order so that only an otherwise eligible exchange queries the queue.
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
            exchange
                .application
                .as_mut()
                .expect("the application remains owned")
                .response_complete();
            check_cancel(&self.control, &self.context)?;
        }
        Ok(false)
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
        // A closing connection does not admit a read-ahead request, and the idle check closes it.
        // Under `abort_exchanges_on_eof`, a request read before a peer EOF that is not yet judged
        // is parsed, as `UniplexSession` parses what it read before it reads the EOF.
        if self.codec.is_reusable() || self.eof_abort_watched() {
            self.exchange_pause = false;
            self.flow_pause = false;
            self.codec.get_mut().set_parser_paused(false);
        }
        true
    }

    /// Consumes ready reads, so that a peer EOF already read is handled before the next parse,
    /// as in `UniplexSession`'s read loop. Each read after the first uses up one of the poll's
    /// remaining turns, and an empty read ends the batch. `None` when no read was polled.
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
        #[cfg(test)]
        count(&READ_POLLS);
        // Until a drained reader's waker fires, a call checks only that waker, as for an
        // outstanding read.
        if self.read.is_drained() && !self.read_wake.is_ready() {
            return None;
        }
        let polled = if self.read.is_reading() {
            if !self.read_wake.take_ready() {
                return None;
            }
            self.read.poll(&mut Context::from_waker(&self.read_waker))
        } else {
            #[cfg(test)]
            count(&READ_START_CHECKS);
            let maximum = self.read_capacity();
            // A drained reader's next read waits for its waker (`ReadSlot::is_drained`).
            if maximum == 0 || (!self.read_wake.take_ready() && self.read.is_drained()) {
                return None;
            }
            self.read.poll_start(
                self.transport.as_ref(),
                &self.context,
                &mut Context::from_waker(&self.read_waker),
                maximum,
                self.config.read_allocation_size.min(maximum),
            )
        };
        let Poll::Ready(result) = polled else {
            return Some(ReadPoll::Pending);
        };
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
                    self.codec.get_mut().generate_immediate_goaway();
                }
                self.codec.reparse = true;
                let mut data = read.into_data();
                if self.eof_watch {
                    drop(data);
                } else if self.ingress.append(&mut data).is_err() {
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

    /// How much a new read may take: 0 while a read cannot start, because the connection read
    /// EOF or a failure or is ending, or while the ingress buffer is full.
    fn read_capacity(&self) -> usize {
        if self.eof
            || self.read_error.is_some()
            || self.fatal.is_some()
            || self.closing_error.is_some()
            || self.cancellation.is_cancellation_requested()
        {
            return 0;
        }
        self.config
            .maximum_read_size
            .min(self.ingress.remaining_capacity())
    }

    fn writes_pending(&self) -> bool {
        self.write.is_some()
            || self
                .write_queue
                .as_ref()
                .is_some_and(|queue| !queue.is_idle())
    }

    /// Whether a write may start: something is serialized and the transport takes it. A write
    /// queue's capacity check is transport code, which may reenter.
    #[inline(always)]
    fn write_ready(&self) -> bool {
        !self.transport_reset && !self.egress.is_empty() && self.can_start_write()
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
        #[cfg(test)]
        count(&WRITE_POLLS);
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

    /// Whether a peer EOF was read under `abort_exchanges_on_eof` and is not yet judged.
    #[inline]
    fn eof_abort_watched(&self) -> bool {
        self.eof
            && self.config.abort_exchanges_on_eof
            && !self.eof_judged
            && self.fatal.is_none()
            && self.closing_error.is_none()
    }

    /// Whether the peer EOF is judged now. Like a failed read, it waits for what was read before
    /// it (`holds_read_error`), the point where `UniplexSession` would read it.
    #[inline]
    fn eof_abort_due(&self) -> bool {
        self.eof_abort_watched() && !self.holds_read_error()
    }

    /// Whether the live exchange is open in `UniplexSession`'s sense: its request
    /// head arrived, it carries no established tunnel, and its response's end is not yet
    /// serialized (`UniplexSession` detaches a stream once that end is serialized).
    #[inline]
    fn exchange_open(&self) -> bool {
        self.exchange.as_ref().is_some_and(|exchange| {
            (exchange.started || exchange.head.is_some())
                && !exchange.tunnel
                && !exchange.response_complete
        })
    }

    /// Fails the connection for the peer's EOF while the live exchange is open, as
    /// `UniplexSession`'s `fail_connection` does. The exchange is cancelled immediately with the
    /// peer as the cause, and the transport resets once the bytes already serialized are written
    /// (`fail_read`). Otherwise the EOF takes the default path.
    #[cold]
    fn judge_eof(&mut self) {
        if self.exchange_open() {
            self.fail_read(failure(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"));
        } else {
            self.eof_judged = true;
            // The codec may now take the EOF.
            self.codec.reparse = true;
        }
    }

    /// Starts the EOF watch, as `UniplexSession` does: the input read after a complete request
    /// that closes the connection is discarded, and so are later reads.
    #[cold]
    fn start_eof_watch(&mut self) {
        self.eof_watch = true;
        self.ingress.trim_start_at_most(usize::MAX);
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
        if self.read.is_reading() {
            return None;
        }
        let shutdown = self
            .write_shutdown
            .take()
            .unwrap_or_else(|| self.transport.shutdown_write(&self.context));
        Some(shutdown.map(|()| self.summary))
    }

    /// Runs RFC 9112's staged close after a stopped upload, and returns whether it is still
    /// reading. The write side shuts down first, so that the client can receive the response
    /// before any TCP reset. Input that will never be parsed is discarded, so a full input buffer
    /// still leaves room to read. The rest of the upload is then read, unparsed, until peer EOF
    /// or until `buffer_limit` more bytes are read. A failed shutdown ends the close
    /// immediately, so the run returns that error.
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
        self.codec.get_mut().generate(
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

    /// Fails the connection for a failed read. As in `UniplexSession`, the exchange is cancelled
    /// immediately with the peer as the cause, and the transport resets once the bytes already
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

    /// Like `abort`. `peer` is true when the peer ended the connection, and is passed to the
    /// exchange's `cancel`.
    #[cold]
    fn fail(&mut self, error: HttpError, peer: bool) {
        let first = self.failed_read.take();
        self.fatal
            .get_or_insert_with(|| first.unwrap_or_else(|| error.clone()));
        self.cancel_exchange(&error, peer);
        self.egress = BufQueue::new(self.config.buffer_limit);
        self.reset_transport();
    }

    /// Cancels and drops the live exchange with `error`, passing `peer` to its `cancel`.
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
