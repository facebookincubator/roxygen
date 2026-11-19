/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Worker-local HTTP/1.1 and HTTP/2 session driver over a byte stream.
//!
//! HTTP/1.1 writes batch ready events within the configured buffer and write limits.
//! Upstream HTTP/1.1 polls reads before writes. A request whose head is still unserialized
//! when the peer's EOF arrives fails, and retrying it is up to the caller.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::pin::pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

static NEXT_SESSION_PING_TOKEN: AtomicU64 = AtomicU64::new(1);

use futures::future::MaybeDone;
use roxygen_codec::CodecCallbackAction;
use roxygen_codec::CodecCallbackActions;
use roxygen_codec::CodecCallbackResult;
use roxygen_codec::CodecCapabilities;
use roxygen_codec::CodecEvent;
use roxygen_codec::CodecIngressResult;
use roxygen_codec::CodecSetting;
use roxygen_codec::EgressFrame;
use roxygen_codec::GeneratedFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_codec::HttpExceptionDirection;
use roxygen_codec::OwnedEgressFrame;
use roxygen_codec::error_adapter::h2_error_code_to_http_error_code;
use roxygen_codec::error_adapter::http_error_code_to_h2_error_code;
use roxygen_codec::error_adapter::http_exception_to_http_error_code;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::FieldSectionInfo;
use roxygen_core::byte_event::FieldSectionType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::exchange::ExchangeCompletion;
use roxygen_core::fixed_source::HttpErrorSource;
use roxygen_core::fixed_source::get_error_response;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::CodecProtocol;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::priority::HttpPriorityHeaderAccess;
use roxygen_core::rfc2616::BodyAllowed;
use roxygen_core::rfc2616::body_implied;
use roxygen_core::rfc2616::is_request_body_allowed;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::DeferredReadTimeout;
use roxygen_core::stream_source::FlowControlState;
use roxygen_http1::Http1xCodec;
use roxygen_http2::Http2Codec;
use roxygen_http2::flow_control::ConnectionFlowControlCallback;
use roxygen_http2::flow_control::FlowControlledCodec;
use roxygen_http2::frame::FrameType;
use roxygen_http2::settings::SettingsId;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::cancellation::CancellationWakeRegistration;
use roxygen_runtime::host::Host;
use roxygen_runtime::host::LocalFuture;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::CleanupRegistrationError;
use roxygen_runtime::worker::WorkerCleanupTaskRegistration;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerId;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_event::AsyncSocketByteEventObserver;
use roxygen_transport::byte_event::ByteEventOffsets;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_event::SocketByteEventType;
use roxygen_transport::byte_event::TxAckEvent;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::ByteStreamTransport;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::OwnedReader;
use roxygen_transport::byte_stream::OwnedWriteQueue;
use roxygen_transport::byte_stream::PeerIdentity;
use roxygen_transport::byte_stream::TransportRead;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use tokio::sync::futures::Notified;

use crate::exchange::ExchangeTracker;
use crate::experimental::PingProbeConfig;
use crate::experimental::ping_probe_timeout_error;
use crate::http2::DEFAULT_STREAM_WINDOW;
use crate::http2::Http2DriverState;
use crate::http2::Http2SessionConfig;
use crate::http2::Http2StatsEvent;
use crate::http2::StreamReceiveWindowMode;
use crate::http2::protocol::CodecFailureContext;
use crate::http2::protocol::CodecFailureDisposition;
use crate::http2::protocol::GoawayPhase;
use crate::http2::protocol::PeerSetting;
use crate::http2::protocol::SentGoaway;
use crate::http2::protocol::peer_goaway_refuses;
use crate::http2::protocol::plan_goaway;
use crate::http2::rate_limit::RateLimitKind;
use crate::http2::stats::Http2SessionStats;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::session::LifecycleObserverId;
use crate::session::LifecycleObserverRegistration;
use crate::session::LifecycleObservers;
use crate::session::LifecycleRegistry;
use crate::session::LocalSessionFuture;
use crate::session::OutgoingTransactionActivity;
use crate::session::PreparedOutgoingRequest;
use crate::session::RequestHeadGate;
use crate::session::RequestReservation;
#[cfg(test)]
use crate::session::ResetOrigin;
use crate::session::SessionCore;
use crate::session::StreamAction;
use crate::session::StreamUpdate;
use crate::session::small_map::Entry;
use crate::session::small_map::SmallMap;
use crate::shutdown::DrainTrigger;
use crate::stream_state::EgressEvent;
use crate::stream_state::EgressTransfer;
use crate::stream_state::SourceWakeCounter;

const DEFAULT_BUFFER_LIMIT: usize = 1 << 20;
const DEFAULT_INGRESS_BUFFER_LIMIT: u32 = 65_535;
const DEFAULT_READ_SIZE: usize = 64 << 10;
const DEFAULT_READ_ALLOCATION_SIZE: usize = 4_000;
const DEFAULT_WRITE_SIZE: usize = 64 << 10;
const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PARSE_PASSES_PER_TICK: usize = 16;
const MAX_TRANSPORT_EVENT_BRIDGES: usize = 96;

type PanicPayload = Box<dyn std::any::Any + Send>;

fn capture_panic(first: &mut Option<PanicPayload>, callback: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
        if first.is_none() {
            *first = Some(payload);
        } else {
            roxygen_runtime::panic::discard_panic(payload);
        }
    }
}

fn retain_first_panic(first: &mut Option<PanicPayload>, next: Option<PanicPayload>) {
    if first.is_none() {
        *first = next;
    } else if let Some(payload) = next {
        roxygen_runtime::panic::discard_panic(payload);
    }
}

fn resume_first_panic(first: Option<PanicPayload>) {
    if let Some(payload) = first {
        std::panic::resume_unwind(payload);
    }
}

/// One read returned by a transport consumed by [`UniplexSession`].
pub struct UniplexRead {
    data: BufQueue,
    eof: bool,
}

impl UniplexRead {
    pub fn new(data: BufQueue, eof: bool) -> Self {
        Self { data, eof }
    }

    pub fn bytes_read(&self) -> usize {
        self.data.chain_length()
    }

    pub const fn is_eof(&self) -> bool {
        self.eof
    }

    pub fn into_data(self) -> BufQueue {
        self.data
    }
}

/// One retired submission from a connection-owned write queue.
pub struct UniplexWriteCompletion {
    pub requested_bytes: usize,
    pub result: Result<WriteInfo, TransportWriteError>,
}

/// An exclusive, bounded byte writer that reports write completions in order.
///
/// Operational failures, including zero-byte failures, also produce completions. A
/// full queue frees capacity only as the caller consumes completions.
pub trait UniplexWriteQueue: 'static {
    /// Returns whether a write of `bytes` can start now.
    ///
    /// This reports capacity only. A terminal transport state does not make it return
    /// false, so the write is accepted and its error is delivered by `poll_complete`.
    ///
    /// A queue whose writes progress only while `poll_complete` is polled must withhold
    /// capacity while a write is outstanding. Owned HTTP/2 arms its writer for a submission
    /// only while it waits for the completion, and a `false` here counts as such a wait
    /// (`write_completion_needed`). An unarmed write on such a queue would stall until an
    /// unrelated wake polled the session.
    fn can_write(&self, bytes: usize) -> bool;
    /// Consumes one capacity-admitted submission. An error means interface misuse.
    /// Transport rejection and cancellation are delivered by `poll_complete`.
    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError>;
    /// Registers the owner waker and consumes the next completion, in submission order.
    fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<UniplexWriteCompletion>;
    /// Whether every submission's completion has been consumed. A finished write whose
    /// completion is still unconsumed keeps the queue busy.
    fn is_idle(&self) -> bool;
    /// Whether bytes submitted now would wait behind bytes the transport has not yet accepted:
    /// a transport write is outstanding, a whole transport write is already queued, or the
    /// transport cannot write anymore. Owned HTTP/2 keeps a downstream stream's write fence
    /// while this holds. The default returns `true`, which keeps that fence for every queue that
    /// does not implement this.
    fn is_saturated(&self) -> bool {
        true
    }
}

/// The native TCP byte stream's queue (`ByteStreamTransport::write_queue`), which implements
/// `is_saturated`.
impl UniplexWriteQueue for OwnedWriteQueue {
    fn can_write(&self, bytes: usize) -> bool {
        OwnedWriteQueue::can_write(self, bytes)
    }

    fn start_write(&mut self, data: BufQueue) -> Result<(), HttpError> {
        OwnedWriteQueue::start_write(self, data)
    }

    fn poll_complete(&mut self, context: &mut Context<'_>) -> Poll<UniplexWriteCompletion> {
        OwnedWriteQueue::poll_complete(self, context).map(|completion| UniplexWriteCompletion {
            requested_bytes: completion.requested_bytes,
            result: completion.result,
        })
    }

    fn is_idle(&self) -> bool {
        OwnedWriteQueue::is_idle(self)
    }

    fn is_saturated(&self) -> bool {
        OwnedWriteQueue::is_saturated(self)
    }
}

/// An exclusive, poll-based reader that lets a connection's owner read without a future per
/// read. One read runs at a time and returns `UniplexTransport::read`'s results.
pub trait UniplexReader: 'static {
    /// Starts a read of at most `maximum` bytes that first offers `allocation_size` bytes. Errors
    /// indicate interface misuse, such as a started read that has not been returned yet. Failures
    /// of the read itself are delivered by `poll_read`.
    fn start_read(&mut self, maximum: usize, allocation_size: usize) -> Result<(), HttpError>;
    /// Registers the owner waker and returns the started read's result once it is ready.
    fn poll_read(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRead, HttpError>>;

    /// Starts a read as `start_read` does and polls it immediately, as `poll_read` does.
    fn poll_start_read(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        if let Err(error) = self.start_read(maximum, allocation_size) {
            return Poll::Ready(Err(error));
        }
        self.poll_read(context)
    }

    /// Whether the read last returned drained the transport: a read started now would pend, and
    /// the waker of the poll that returned it is woken once the transport is readable, closed,
    /// reset or dropped, or the reader's cancellation is requested. The owner then starts its
    /// next read only once that waker fires. The default returns `false`.
    fn is_drained(&self) -> bool {
        false
    }

    /// Whether the peer's FIN or RST has reached the transport, even behind bytes that a read
    /// has not returned yet. It does not consume data. The default returns `false`.
    fn peer_closed(&self) -> bool {
        false
    }
}

/// The native byte stream's poll-based reader (`ByteStreamTransport::reader`).
impl UniplexReader for OwnedReader {
    fn start_read(&mut self, maximum: usize, allocation_size: usize) -> Result<(), HttpError> {
        OwnedReader::start_read(self, maximum, allocation_size)
    }

    fn poll_read(&mut self, context: &mut Context<'_>) -> Poll<Result<UniplexRead, HttpError>> {
        OwnedReader::poll_read(self, context).map(uniplex_read)
    }

    fn poll_start_read(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        allocation_size: usize,
    ) -> Poll<Result<UniplexRead, HttpError>> {
        OwnedReader::poll_start_read(self, context, maximum, allocation_size).map(uniplex_read)
    }

    fn is_drained(&self) -> bool {
        OwnedReader::is_drained(self)
    }

    fn peer_closed(&self) -> bool {
        OwnedReader::peer_closed(self)
    }
}

fn uniplex_read(result: Result<TransportRead, HttpError>) -> Result<UniplexRead, HttpError> {
    result.map(|read| {
        let eof = read.is_eof();
        UniplexRead::new(read.into_data(), eof)
    })
}

/// Byte-stream surface used by the uniplex loops.
///
/// Every `ByteStreamTransport`, including `OwnedByteStream`, implements this
/// surface. The separate trait permits deterministic session tests without
/// exposing the transport crate's private I/O adapter.
pub trait UniplexTransport: 'static {
    /// Acquires an optional exclusive writer. Unsupported configurations retain
    /// the ordinary single-write API, including its timeout behavior.
    ///
    /// A queue may observe `cancellation` only when polled, as the native TCP
    /// queue does. A cancellation requested from another task then wakes the
    /// task that last polled the outstanding write.
    fn try_write_queue(
        &self,
        _context: &WorkerContext,
        _maximum_write_size: usize,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexWriteQueue>>, HttpError> {
        Ok(None)
    }

    /// Acquires an optional poll-based reader, which reads with `timeout` and `cancellation`.
    /// Unsupported configurations retain `read`, including its timeout behavior.
    fn try_reader(
        &self,
        _context: &WorkerContext,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexReader>>, HttpError> {
        Ok(None)
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>>;

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>>;

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError>;
    fn close(&self, context: &WorkerContext) -> Result<(), HttpError>;
    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError>;

    fn peer_identity(&self, _context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        Ok(None)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError>;

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError>;
}

impl<T: ByteStreamTransport> UniplexTransport for T {
    fn try_write_queue(
        &self,
        context: &WorkerContext,
        maximum_write_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexWriteQueue>>, HttpError> {
        Ok(ByteStreamTransport::write_queue(
            self,
            context,
            maximum_write_size,
            timeout,
            cancellation,
        )?
        .map(|queue| Box::new(queue) as Box<dyn UniplexWriteQueue>))
    }

    fn try_reader(
        &self,
        context: &WorkerContext,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Option<Box<dyn UniplexReader>>, HttpError> {
        Ok(
            ByteStreamTransport::reader(self, context, timeout, cancellation)?
                .map(|reader| Box::new(reader) as Box<dyn UniplexReader>),
        )
    }

    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let read = ByteStreamTransport::read(
            self,
            context,
            maximum,
            allocation_size,
            timeout,
            cancellation,
        );
        Box::pin(async move {
            let mut read = read;
            let mut result = Some(read.as_mut().await);
            let mut first_panic = None;
            capture_panic(&mut first_panic, || drop(read));
            if first_panic.is_some() {
                capture_panic(&mut first_panic, || drop(result.take()));
                resume_first_panic(first_panic);
            }
            result.expect("read completed without a panic").map(|read| {
                let eof = read.is_eof();
                UniplexRead::new(read.into_data(), eof)
            })
        })
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        ByteStreamTransport::write_with_byte_events(
            self,
            context,
            data,
            flags,
            timeout,
            cancellation,
        )
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::shutdown_write(self, context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::close(self, context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        ByteStreamTransport::close_with_reset(self, context)
    }

    fn peer_identity(&self, context: &WorkerContext) -> Result<Option<PeerIdentity>, HttpError> {
        ByteStreamTransport::peer_identity(self, context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        ByteStreamTransport::byte_event_capability(self, context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        ByteStreamTransport::register_byte_event(self, context, registration)
    }
}

/// Byte-stream and per-stream options for `UniplexSession` and the owned drivers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UniplexConfig {
    /// Size limit of the ingress and egress buffers, and of an HTTP/1.1 stream's unread body.
    pub buffer_limit: usize,
    /// The buffered ingress bytes of an HTTP/1.1 stream at which parsing pauses.
    pub ingress_buffer_limit: u32,
    /// The most bytes one transport read returns.
    pub maximum_read_size: usize,
    /// The buffer size first offered to each transport read.
    pub read_allocation_size: usize,
    /// The most bytes in one transport write.
    pub maximum_write_size: usize,
    /// Idle timeout for each transport read. Zero disables it.
    pub read_timeout: Duration,
    /// Bounds each wait for a stream's body data. Zero disables it.
    pub stream_read_timeout: Duration,
    /// Bounds a stalled transport write and a wait for flow-control credit. Zero disables it.
    pub write_timeout: Duration,
    /// Drain unread downstream request bodies through EOM after a graceful consumer
    /// drop, including when an early HTTP/1 response selects connection closure.
    pub drain_request_body_on_drop: bool,
    /// Keep downstream HTTP/1 admission open during a local drain until the next
    /// response can carry `Connection: close`.
    pub wait_for_http1_response_on_drain: bool,
    /// Fail a downstream HTTP/1.1 connection, as a reset does, when the peer's EOF
    /// arrives while a transaction other than an established tunnel is open. Reads continue
    /// past a request that closes the connection so that the EOF is seen. The owned HTTP/1.1
    /// downstream driver applies it by the same rules, the owned upstream drivers reject it,
    /// and owned HTTP/2 ignores it. HTTP/2 sessions use
    /// `Http2SessionConfig::abort_streams_on_eof`.
    pub abort_exchanges_on_eof: bool,
    /// Deliver queued ingress before an abort. Off by default. When a stream's ingress is
    /// aborted (a peer reset, a transport EOF or read error, a connection error), the head
    /// and body events its source already queued reach the reader, in order, before the
    /// error, instead of being discarded. hyper's body channel and h2 also deliver received
    /// data before a later error. Applies in both directions. The owned HTTP/1.1 upstream
    /// driver and the owned HTTP/2 driver's upstream role support it, and the other owned
    /// drivers reject it.
    pub deliver_ingress_before_abort: bool,
    /// Send bodiless upstream HTTP/1.1 requests without body framing. Off by default. A
    /// request whose head ends the message and carries neither `Content-Length` nor a
    /// chunked `Transfer-Encoding` goes out unframed, as hyper sends a request with an
    /// empty body, instead of `Transfer-Encoding: chunked` and an empty last chunk for a
    /// `POST` (`Http1xCodec::set_unframed_bodiless_requests`). Downstream and HTTP/2
    /// sessions ignore it, and the owned HTTP/1.1 upstream driver supports it.
    pub unframed_bodiless_http1_requests: bool,
    /// Abandon unwritten HTTP/1.1 output after a body-source error. Off by default.
    ///
    /// Set this when the transport has `OwnedByteStream::enable_non_abortive_close` enabled.
    /// After the head is serialized, a body-source error drops the output that is not yet
    /// written and the session closes immediately, as hyper drops its unflushed buffer and
    /// the I/O. Without it, that output drains to a peer that may have stopped reading.
    /// HTTP/2 sessions and the owned drivers ignore it.
    pub non_abortive_close: bool,
    /// Keep HTTP/2 request methods as exact, case-sensitive RFC tokens. Off by default.
    /// HTTP/1 always uses llhttp's recognized method table, independently of this option.
    pub exact_method_tokens: bool,
    /// Keep HTTP/2 header values exactly as decoded, as h2 and hyper do. Off by default.
    ///
    /// The codec stores every decoded field value with its surrounding spaces and tabs
    /// instead of trimming it (`Http2Codec::set_verbatim_field_values`), and checks values
    /// as h2 does, so it never accepts a value that h2 rejects. A value with DEL or a control
    /// byte other than HTAB is rejected. `content-length` must be decimal, and `te` may only be
    /// `trailers`. Sent heads are encoded with their values as stored. An empty
    /// `content-length` is kept, and its stream's body is checked against zero, as h2
    /// treats it. HTTP/1.1 sessions ignore this option and trim optional whitespace, as
    /// `httparse` does. The owned downstream HTTP/2 driver (`OwnedH2Session`) applies it as
    /// a downstream session does, and the owned upstream HTTP/2 driver rejects it.
    pub verbatim_http2_header_values: bool,
    /// Add a `Date` header to each response that lacks one
    /// (`Http1xCodec::set_add_date_header_to_response`,
    /// `Http2Codec::set_add_date_header_to_response`). On by default. Upstream sessions ignore
    /// it, and the owned downstream drivers apply it as a downstream session does.
    pub add_date_header_to_response: bool,
}

impl Default for UniplexConfig {
    fn default() -> Self {
        Self {
            buffer_limit: DEFAULT_BUFFER_LIMIT,
            ingress_buffer_limit: DEFAULT_INGRESS_BUFFER_LIMIT,
            maximum_read_size: DEFAULT_READ_SIZE,
            read_allocation_size: DEFAULT_READ_ALLOCATION_SIZE,
            maximum_write_size: DEFAULT_WRITE_SIZE,
            read_timeout: DEFAULT_IO_TIMEOUT,
            stream_read_timeout: DEFAULT_IO_TIMEOUT,
            write_timeout: DEFAULT_IO_TIMEOUT,
            drain_request_body_on_drop: false,
            wait_for_http1_response_on_drain: false,
            abort_exchanges_on_eof: false,
            deliver_ingress_before_abort: false,
            unframed_bodiless_http1_requests: false,
            non_abortive_close: false,
            exact_method_tokens: false,
            verbatim_http2_header_values: false,
            add_date_header_to_response: true,
        }
    }
}

impl UniplexConfig {
    pub(crate) fn validate(self) -> Result<Self, HttpError> {
        if self.buffer_limit == 0
            || self.buffer_limit > i32::MAX as usize
            || self.ingress_buffer_limit == 0
            || self.ingress_buffer_limit as usize > self.buffer_limit
            || self.maximum_read_size == 0
            || self.maximum_read_size > self.buffer_limit
            || self.read_allocation_size == 0
            || self.maximum_write_size == 0
            || self.read_allocation_size > self.maximum_read_size
        {
            return Err(invalid_transition(b"invalid uniplex I/O limits"));
        }
        Ok(self)
    }

    /// The most unread body an HTTP/1.1 codec may buffer for a stream before it calls
    /// `HttpCodecCallback::ingress_body_limit` again. It is the smallest of the body source's
    /// strict window, `buffer_limit`, and `read_allocation_size - 1` past
    /// `ingress_buffer_limit` (where parsing pauses), however large the read's buffers are.
    pub(crate) fn http1_ingress_body_ceiling(&self) -> usize {
        self.buffer_limit
            .min(self.ingress_buffer_limit as usize + self.read_allocation_size - 1)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UniplexRunSummary {
    pub bytes_read: u64,
    pub bytes_scheduled: u64,
    pub bytes_written: u64,
    pub reset: bool,
}

/// The wire protocol of a session built by [`UniplexSession::new_owned`].
pub enum UniplexProtocolConfig {
    /// HTTP/1.x, with optional protocol-neutral statistics callbacks.
    Http1 {
        stats: Option<Rc<dyn Http2SessionStats>>,
    },
    /// HTTP/2 with its session options.
    Http2(Http2SessionConfig),
}

/// The owner of a session started by [`UniplexSession::start`].
///
/// The session's run task polls its owner, so the owner does not need a task of
/// its own that waits on the run.
pub trait UniplexRunOwner {
    /// Acts on the owner's stop requests. The run task calls this before its
    /// first poll of the run, before each poll that follows a wake of the
    /// waker this registered, and once after the run completes and releases
    /// its state, but never while it polls the run. It registers `context`'s
    /// waker for any request not yet seen.
    fn poll_stop(&mut self, context: &mut Context<'_>);

    /// Takes the run's outcome after that last `poll_stop`.
    fn finish(self: Box<Self>, outcome: Result<UniplexRunSummary, HttpError>);
}

/// Response ingress returned after an upstream request is committed.
pub struct UniplexResponse {
    pub stream: StreamId,
    pub sequence_number: u64,
    pub source: HttpSourceHolder,
    pub completion: ExchangeCompletion,
    /// Taken through `experimental::UniplexResponseAbortExt`.
    pub(crate) abort: Option<StreamAbort>,
    /// Read through `experimental::UniplexResponseProgressExt`.
    pub(crate) exchange: ExchangeTracker,
}

static READY_DISPATCH: LazyLock<bool> =
    LazyLock::new(|| std::env::var_os("ROXYGEN_READY_DISPATCH").is_none_or(|value| value == "1"));

struct IngressMeta {
    complete: bool,
    discard: bool,
    /// Read only by HTTP/2's `INVALID_STATE_TRANSITION` failure, so HTTP/1.1
    /// leaves it `None`.
    message: Option<roxygen_core::message::HttpMessage>,
    expected_content_length: Option<u64>,
    received_body_length: u64,
    request_get: bool,
    get_body_recorded: bool,
}

pub(crate) fn message_content_length(message: &roxygen_core::message::HttpMessage) -> Option<u64> {
    std::str::from_utf8(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::ContentLength),
    )
    .ok()?
    .trim()
    .parse()
    .ok()
}

/// The length a downstream request's body is checked against: its
/// `content-length`, or zero for a present, empty one with `empty_is_zero`.
pub(crate) fn checked_content_length(
    message: &roxygen_core::message::HttpMessage,
    empty_is_zero: bool,
) -> Option<u64> {
    let headers = message.headers();
    if empty_is_zero
        && headers.number_of_values(HttpHeaderCode::ContentLength) == 1
        && headers
            .get_single_or_empty(HttpHeaderCode::ContentLength)
            .is_empty()
    {
        return Some(0);
    }
    message_content_length(message)
}

pub(crate) fn request_has_forbidden_body(message: &mut roxygen_core::message::HttpMessage) -> bool {
    message.is_request()
        && is_request_body_allowed(message.method().ok().flatten()) == BodyAllowed::NotAllowed
        && body_implied(message.headers())
}

enum EgressWork {
    Event {
        stream: StreamId,
        event: EgressEvent,
    },
    Transfer {
        stream: StreamId,
        transfer: EgressTransfer,
        cancellation: Option<CancellationToken>,
        readiness: Option<EgressSourceReadiness>,
    },
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct EgressTransferKey {
    stream: StreamId,
    generation: u64,
}

pub(crate) struct QueuedEgressTransfer {
    transfer: EgressTransfer,
    cancellation: Option<CancellationToken>,
    readiness: Option<EgressSourceReadiness>,
}

impl QueuedEgressTransfer {
    fn publish_body_source(&self) {
        if body_source_is_ready(self)
            && let Some(readiness) = &self.readiness
        {
            readiness.wake.publish();
        }
    }

    fn into_work(self, stream: StreamId) -> EgressWork {
        EgressWork::Transfer {
            stream,
            transfer: self.transfer,
            cancellation: self.cancellation,
            readiness: self.readiness,
        }
    }
}

enum ReadyEgressWork {
    Http1,
    Http2(EgressTransferKey),
}

impl Http2DriverState {
    fn egress_queue(&self, stream: StreamId) -> Option<&VecDeque<QueuedEgressTransfer>> {
        self.streams
            .get(&stream)
            .map(|state| &state.egress_work)
            .or_else(|| self.orphaned_egress.get(&stream))
    }

    fn push_transfer(&mut self, stream: StreamId, work: QueuedEgressTransfer, front: bool) {
        let key = EgressTransferKey {
            stream,
            generation: work.transfer.generation(),
        };
        // A source callback may detach its stream while its transfer is checked out.
        // Returning that owner must not recreate protocol or scheduler membership.
        let queue = match self.streams.get_mut(&stream) {
            Some(state) => &mut state.egress_work,
            None => self.orphaned_egress.entry(stream).or_default(),
        };
        if queue.capacity() == 0 {
            queue.reserve_exact(1);
        }
        if front {
            queue.push_front(work);
            self.egress_order.push_front(key);
            queue
                .front()
                .expect("inserted transfer")
                .publish_body_source();
        } else {
            queue.push_back(work);
            self.egress_order.push_back(key);
            queue
                .back()
                .expect("inserted transfer")
                .publish_body_source();
        }
    }

    fn take_transfer(&mut self, key: EgressTransferKey) -> EgressWork {
        let queue = match self.streams.get_mut(&key.stream) {
            Some(state) => &mut state.egress_work,
            None => self
                .orphaned_egress
                .get_mut(&key.stream)
                .expect("ordered egress key has a stream or orphan owner"),
        };
        let position = queue
            .iter()
            .position(|work| work.transfer.generation() == key.generation)
            .expect("ordered egress key has a queued generation");
        let work = queue.remove(position).expect("position was found");
        if queue.is_empty() {
            self.orphaned_egress.remove(&key.stream);
        }
        work.into_work(key.stream)
    }

    fn queued_transfers(&self) -> impl Iterator<Item = (StreamId, &QueuedEgressTransfer)> {
        // Global order also determines lazy buffer timer initialization and retirement.
        self.egress_order.iter().map(|key| {
            let work = self
                .egress_queue(key.stream)
                .and_then(|queue| {
                    queue
                        .iter()
                        .find(|work| work.transfer.generation() == key.generation)
                })
                .expect("ordered egress key has a queued owner");
            (key.stream, work)
        })
    }
}

type BodySourceCandidates = BTreeMap<EgressTransferKey, std::sync::Weak<EgressSourceWake>>;

/// Ready body sources published by their wakers, and the writer wake those
/// wakers notify.
#[derive(Default)]
pub(crate) struct BodySourcePublications {
    candidates: Mutex<BodySourceCandidates>,
    source_ready: Arc<tokio::sync::Notify>,
}

impl BodySourcePublications {
    fn candidates(&self) -> std::sync::MutexGuard<'_, BodySourceCandidates> {
        self.candidates
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn take(&self) -> BodySourceCandidates {
        std::mem::take(&mut *self.candidates())
    }
}

struct BodySourcePublication {
    key: EgressTransferKey,
    owner: std::sync::Weak<BodySourcePublications>,
}

struct EgressSourceWake {
    ready: AtomicBool,
    notification: Arc<tokio::sync::Notify>,
    publication: Option<BodySourcePublication>,
    active: AtomicBool,
}

impl EgressSourceWake {
    fn publish(self: &Arc<Self>) {
        let Some(publication) = &self.publication else {
            return;
        };
        let Some(owner) = publication.owner.upgrade() else {
            return;
        };
        let mut candidates = owner.candidates();
        if self.active.load(Ordering::Relaxed) {
            candidates
                .entry(publication.key)
                .or_insert_with(|| Arc::downgrade(self));
        }
    }

    fn retire(&self) {
        if let Some(publication) = &self.publication
            && let Some(owner) = publication.owner.upgrade()
        {
            let mut candidates = owner.candidates();
            self.active.store(false, Ordering::Relaxed);
            if candidates
                .get(&publication.key)
                .is_some_and(|wake| std::ptr::eq(wake.as_ptr(), self))
            {
                candidates.remove(&publication.key);
            }
        } else {
            self.active.store(false, Ordering::Relaxed);
        }
    }
}

impl Wake for EgressSourceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        self.publish();
        self.notification.notify_one();
    }
}

struct EgressSourceReadiness {
    wake: Arc<EgressSourceWake>,
    cancellation: Option<LocalSessionFuture<WaitStatus>>,
    cancelled: bool,
    intrinsic_cancellation: Option<LocalSessionFuture<WaitStatus>>,
}

impl EgressSourceReadiness {
    fn new(
        notification: Arc<tokio::sync::Notify>,
        cancellation: Option<CancellationToken>,
        publication: Option<BodySourcePublication>,
        intrinsic_cancellation: Option<CancellationToken>,
    ) -> Self {
        let mut readiness = Self {
            wake: Arc::new(EgressSourceWake {
                ready: AtomicBool::new(true),
                notification,
                publication,
                active: AtomicBool::new(true),
            }),
            cancellation: cancellation.map(|token| {
                Box::pin(async move { CancellableBaton::new().wait(token).await })
                    as LocalSessionFuture<WaitStatus>
            }),
            cancelled: false,
            intrinsic_cancellation: intrinsic_cancellation.map(|token| {
                Box::pin(async move { CancellableBaton::new().wait(token).await })
                    as LocalSessionFuture<WaitStatus>
            }),
        };
        let waker = Waker::from(Arc::clone(&readiness.wake));
        let mut context = Context::from_waker(&waker);
        readiness.poll_cancellation(&mut context);
        // Capacity-full and production-complete owners may have no child cancellation wait.
        if let Some(wait) = &mut readiness.intrinsic_cancellation
            && wait.as_mut().poll(&mut context).is_ready()
        {
            readiness.intrinsic_cancellation = None;
        }
        readiness
    }

    fn is_ready(&self) -> bool {
        self.wake.ready.load(Ordering::Acquire)
    }

    fn poll_cancellation(&mut self, context: &mut Context<'_>) -> bool {
        if let Some(cancellation) = &mut self.cancellation
            && let Poll::Ready(status) = cancellation.as_mut().poll(context)
        {
            self.cancelled = status == WaitStatus::Cancelled;
            self.cancellation = None;
        }
        self.cancelled
    }

    fn poll(
        &mut self,
        source: Pin<&mut impl Future<Output = Result<Option<EgressEvent>, HttpError>>>,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        self.poll_with(|context| source.poll(context))
    }

    fn poll_with(
        &mut self,
        poll_source: impl FnOnce(&mut Context<'_>) -> Poll<Result<Option<EgressEvent>, HttpError>>,
    ) -> Poll<Result<Option<EgressEvent>, HttpError>> {
        self.wake.ready.store(false, Ordering::Release);
        let waker = Waker::from(Arc::clone(&self.wake));
        let mut context = Context::from_waker(&waker);
        if self.poll_cancellation(&mut context) {
            return Poll::Ready(Err(HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"request body production cancelled",
            )));
        }
        let result = poll_source(&mut context);
        if result.is_ready() {
            self.wake.ready.store(true, Ordering::Release);
        }
        result
    }
}

impl Drop for EgressSourceReadiness {
    fn drop(&mut self) {
        // Transfer destruction may publish cancellation before this field is dropped.
        self.wake.retire();
    }
}

struct TransportObserverBridge {
    observer: Rc<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    fallback_offset: u64,
    pending: Cell<u8>,
}

impl HttpByteEventCallback for TransportObserverBridge {
    fn on_byte_event(&self, event: HttpByteEvent) {
        let event_type = match event.event_type {
            ByteEventType::NicTx => SocketByteEventType::Tx,
            ByteEventType::CumulativeAck => SocketByteEventType::Ack,
            ByteEventType::TransportWrite | ByteEventType::KernelWrite => return,
        };
        let offset = if event.transport_offset == 0 {
            self.fallback_offset
        } else {
            event.transport_offset
        };
        self.pending
            .set(self.pending.get() & !event.event_type.bit());
        let _ = self
            .observer
            .socket_byte_event(&self.context, event_type, offset);
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        let event_type = match event.event_type {
            ByteEventType::NicTx => SocketByteEventType::Tx,
            ByteEventType::CumulativeAck => SocketByteEventType::Ack,
            ByteEventType::TransportWrite | ByteEventType::KernelWrite => return,
        };
        self.pending
            .set(self.pending.get() & !event.event_type.bit());
        let _ = self.observer.cancel_socket_byte_event(
            &self.context,
            event_type,
            self.fallback_offset,
            &error,
        );
    }
}

pub(crate) struct UniplexLifecycleDispatcher {
    next_id: Cell<u64>,
    observers: LifecycleObservers,
    pending: RefCell<VecDeque<LifecycleEvent>>,
    dispatching: Cell<bool>,
    ping_prober: Option<Rc<PingProber>>,
}

impl UniplexLifecycleDispatcher {
    pub(crate) fn new(ping_prober: Option<Rc<PingProber>>) -> Self {
        Self {
            next_id: Cell::new(1),
            observers: LifecycleObservers::default(),
            pending: RefCell::new(VecDeque::new()),
            dispatching: Cell::new(false),
            ping_prober,
        }
    }

    pub(crate) fn add_observer(
        self: &Rc<Self>,
        observer: Rc<dyn LifecycleObserver>,
    ) -> LifecycleObserverId {
        let id = LifecycleObserverId::from_raw(self.next_id.get());
        self.next_id.set(self.next_id.get().wrapping_add(1));
        self.observers.insert(id, observer.clone());
        let registration =
            LifecycleObserverRegistration::new(id, LifecycleRegistry::Uniplex(Rc::downgrade(self)));
        let attached = catch_unwind(AssertUnwindSafe(|| {
            observer.on_attached_with_registration(registration)
        }));
        if attached.is_err() {
            self.observers.remove(id);
        }
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(observer)));
        if let Err(payload) = attached {
            std::panic::resume_unwind(payload);
        }
        if let Err(payload) = dropped {
            std::panic::resume_unwind(payload);
        }
        id
    }

    pub(crate) fn remove_observer(&self, id: LifecycleObserverId) -> bool {
        self.observers.remove(id)
    }

    pub(crate) fn flush(&self) {
        if self.dispatching.replace(true) {
            return;
        }
        let mut first_panic = None;
        loop {
            let Some(event) = self.pending.borrow_mut().pop_front() else {
                break;
            };
            let observers = self.observers.snapshot();
            for observer in observers {
                for result in [
                    catch_unwind(AssertUnwindSafe(|| observer.on_event(event))),
                    catch_unwind(AssertUnwindSafe(|| drop(observer))),
                ] {
                    if let Err(payload) = result {
                        if first_panic.is_none() {
                            first_panic = Some(payload);
                        }
                    }
                }
            }
        }
        self.dispatching.set(false);
        if let Some(payload) = first_panic {
            std::panic::resume_unwind(payload);
        }
    }

    pub(crate) fn clear(&self) {
        self.pending.borrow_mut().clear();
        let observers = self.observers.take();
        drop_items_individually(observers.into_iter().map(|(_, observer)| observer));
    }
}

impl LifecycleObserver for UniplexLifecycleDispatcher {
    fn on_event(&self, event: LifecycleEvent) {
        // Probes start when the connection activates and stop when it deactivates.
        if let Some(prober) = &self.ping_prober {
            match event {
                LifecycleEvent::ActivateConnection => prober.start_probes(),
                LifecycleEvent::DeactivateConnection => prober.cancel_probes(),
                _ => {}
            }
        }
        self.pending.borrow_mut().push_back(event);
    }
}

/// The PING probe timer: the interval before the next probe, or the
/// outstanding probe's timeout. `run_ping_probes` sleeps until its deadline.
pub(crate) struct PingProber {
    host: Host,
    interval: Duration,
    timeout: Duration,
    extend_interval_on_ingress: bool,
    immediate: bool,
    deadline: Cell<Option<Instant>>,
    probe: Cell<Option<u64>>,
    rearmed: tokio::sync::Notify,
}

impl PingProber {
    fn new(config: PingProbeConfig, host: Host) -> Option<Rc<Self>> {
        (!config.interval.is_zero()).then(|| {
            Rc::new(Self {
                host,
                interval: config.interval,
                timeout: config.timeout,
                extend_interval_on_ingress: config.extend_interval_on_ingress,
                immediate: config.immediate,
                deadline: Cell::new(None),
                probe: Cell::new(None),
                rearmed: tokio::sync::Notify::new(),
            })
        })
    }

    fn start_probes(&self) {
        self.refresh_timeout(false);
    }

    fn cancel_probes(&self) {
        self.probe.set(None);
        self.deadline.set(None);
    }

    fn refresh_timeout(&self, on_ingress: bool) {
        if self.probe.get().is_none() && (!on_ingress || self.extend_interval_on_ingress) {
            let deadline = self.host.deadline_after(self.interval);
            // A later deadline is picked up when the timer wakes for the earlier one.
            if self
                .deadline
                .replace(Some(deadline))
                .is_none_or(|previous| deadline < previous)
            {
                self.rearmed.notify_one();
            }
        }
    }

    fn on_ping_reply(&self, data: u64) {
        if self.probe.get() == Some(data) {
            self.probe.set(None);
            self.refresh_timeout(false);
        }
    }
}

enum DriverCodec {
    Http1(Http1xCodec),
    Http2(FlowControlledCodec),
}

impl DriverCodec {
    fn generate_immediate_goaway(&mut self) {
        if let Self::Http1(codec) = self {
            codec.generate_immediate_goaway();
        }
    }

    fn resolve_downstream_upgrade(
        &mut self,
        response: &roxygen_core::message::HttpMessage,
    ) -> Option<bool> {
        match self {
            Self::Http1(codec) => codec.resolve_downstream_upgrade(response),
            Self::Http2(_) => None,
        }
    }

    fn connection_send_credit(&self) -> u32 {
        match self {
            Self::Http1(_) => u32::MAX,
            Self::Http2(codec) => codec.available_send(),
        }
    }

    fn ingress_bytes_processed(
        &mut self,
        output: &mut BufQueue,
        amount: u32,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<(), HttpError> {
        match self {
            Self::Http1(_) => Ok(()),
            Self::Http2(codec) => codec
                .ingress_bytes_processed(output, amount, callback)
                .map(|_| ()),
        }
    }
}

impl HttpCodec for DriverCodec {
    fn protocol(&self) -> CodecProtocol {
        match self {
            Self::Http1(codec) => codec.protocol(),
            Self::Http2(codec) => codec.protocol(),
        }
    }

    fn transport_direction(&self) -> TransportDirection {
        match self {
            Self::Http1(codec) => codec.transport_direction(),
            Self::Http2(codec) => codec.transport_direction(),
        }
    }

    fn capabilities(&self) -> CodecCapabilities {
        match self {
            Self::Http1(codec) => codec.capabilities(),
            Self::Http2(codec) => codec.capabilities(),
        }
    }

    fn create_stream(&mut self) -> StreamId {
        match self {
            Self::Http1(codec) => codec.create_stream(),
            Self::Http2(codec) => codec.create_stream(),
        }
    }

    fn is_busy(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.is_busy(),
            Self::Http2(codec) => codec.is_busy(),
        }
    }

    fn has_partial_request_head(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.has_partial_request_head(),
            Self::Http2(codec) => codec.has_partial_request_head(),
        }
    }

    fn set_parser_paused(&mut self, paused: bool) {
        match self {
            Self::Http1(codec) => codec.set_parser_paused(paused),
            Self::Http2(codec) => codec.set_parser_paused(paused),
        }
    }

    fn is_parser_paused(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.is_parser_paused(),
            Self::Http2(codec) => codec.is_parser_paused(),
        }
    }

    fn on_ingress(
        &mut self,
        input: &BufQueue,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecIngressResult {
        match self {
            Self::Http1(codec) => codec.on_ingress(input, callback, callback_egress),
            Self::Http2(codec) => codec.on_ingress(input, callback, callback_egress),
        }
    }

    fn on_ingress_eof(
        &mut self,
        callback: &mut dyn HttpCodecCallback,
        callback_egress: &mut BufQueue,
    ) -> CodecCallbackResult {
        match self {
            Self::Http1(codec) => codec.on_ingress_eof(callback, callback_egress),
            Self::Http2(codec) => codec.on_ingress_eof(callback, callback_egress),
        }
    }

    fn is_reusable(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.is_reusable(),
            Self::Http2(codec) => codec.is_reusable(),
        }
    }

    fn is_waiting_to_drain(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.is_waiting_to_drain(),
            Self::Http2(codec) => codec.is_waiting_to_drain(),
        }
    }

    fn close_on_egress_complete(&self) -> bool {
        match self {
            Self::Http1(codec) => codec.close_on_egress_complete(),
            Self::Http2(codec) => codec.close_on_egress_complete(),
        }
    }

    fn last_incoming_stream_id(&self) -> StreamId {
        match self {
            Self::Http1(codec) => codec.last_incoming_stream_id(),
            Self::Http2(codec) => codec.last_incoming_stream_id(),
        }
    }

    fn generate(
        &mut self,
        output: &mut BufQueue,
        frame: EgressFrame<'_>,
        callback: &mut dyn HttpCodecCallback,
    ) -> Result<GeneratedFrame, HttpError> {
        match self {
            Self::Http1(codec) => codec.generate(output, frame, callback),
            Self::Http2(codec) => codec.generate(output, frame, callback),
        }
    }
}

struct ConnectionWindowWake {
    shared: Rc<UniplexShared>,
    transition: Rc<Cell<Option<bool>>>,
}

impl ConnectionFlowControlCallback for ConnectionWindowWake {
    fn on_connection_send_window_open(&self) {
        self.transition.set(Some(true));
        self.shared.write_wake.notify_one();
    }

    fn on_connection_send_window_closed(&self) {
        self.transition.set(Some(false));
        self.shared.write_wake.notify_one();
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct H2EgressOwnersSnapshot {
    pub(crate) egress_complete: Option<bool>,
    pub(crate) queued: usize,
    pub(crate) complete: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct H2TimeoutArmSnapshot {
    pub(crate) generation: u64,
    pub(crate) flow_deadline: Instant,
    pub(crate) buffer_deadline: Instant,
    pub(crate) connection_credit: u32,
    pub(crate) stream_credit: u32,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct H2TimeoutPhaseCopies {
    pub(crate) blocked: Option<H2TimeoutArmSnapshot>,
    pub(crate) rearmed: Option<H2TimeoutArmSnapshot>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct H2SourceFlowSnapshot {
    pub(crate) sequence: u64,
    pub(crate) flow_deadline: Option<Instant>,
    pub(crate) buffer_timeout: Option<(StreamId, Instant)>,
    pub(crate) blocked: Option<(Vec<StreamId>, bool)>,
    pub(crate) connection_credit: u32,
    pub(crate) stream_credit: u32,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct H2SourceTimerTestControl {
    pub(crate) snapshot: Rc<RefCell<Option<H2SourceFlowSnapshot>>>,
    pub(crate) capture_next_arm: Rc<Cell<bool>>,
    pub(crate) first_arm: Rc<RefCell<Option<H2SourceFlowSnapshot>>>,
    pub(crate) changed: Rc<tokio::sync::Notify>,
    pub(crate) pause_next_source_yield: Rc<Cell<bool>>,
    pub(crate) pause_next_source_service: Rc<Cell<bool>>,
    pub(crate) source_yield_paused: Rc<Cell<bool>>,
    pub(crate) resume_source_yield: Rc<tokio::sync::Notify>,
    pub(crate) pause_next_writer_wait: Rc<Cell<bool>>,
    pub(crate) writer_wait_paused: Rc<Cell<bool>>,
    pub(crate) resume_writer_wait: Rc<tokio::sync::Notify>,
}

/// One-shot test callbacks run by the writer immediately before it builds an
/// idle wait and immediately after that wait returns.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct WriterWaitHooks {
    pub(crate) before: Option<Box<dyn FnOnce()>>,
    pub(crate) after: Option<Box<dyn FnOnce()>>,
}

#[cfg(test)]
fn run_writer_wait_hook(
    state: &Rc<RefCell<DriverState>>,
    select: impl FnOnce(&mut WriterWaitHooks) -> Option<Box<dyn FnOnce()>>,
) {
    let hook = select(&mut state.borrow_mut().writer_wait_hooks);
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
struct H2SourceTimerProbe {
    stream: StreamId,
    control: H2SourceTimerTestControl,
}

#[cfg(test)]
struct H2TimeoutArmProbe {
    stream: StreamId,
    generation: Cell<u64>,
    buffer_deadline: Cell<Option<Instant>>,
    phases: Rc<Cell<H2TimeoutPhaseCopies>>,
}

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BufferedEgressStatsSnapshot {
    pub buffered_bytes: i64,
    pub delta_capacity: Option<usize>,
    pub tracked_streams: usize,
    pub content_length_mismatch: Option<(u64, u64)>,
}

#[derive(Default)]
struct EgressMeta {
    stream_offset: u64,
    body_offset: u64,
    response_timeout_pending: bool,
}

struct DriverState {
    core: Option<Box<SessionCore>>,
    lifecycle: Rc<UniplexLifecycleDispatcher>,
    codec: DriverCodec,
    http2: Option<Box<Http2DriverState>>,
    http1_stats: Option<Http1StatsState>,
    handler: Option<HttpHandlerRef>,
    ingress_meta: SmallMap<StreamId, Box<IngressMeta>>,
    pending_pushes: BTreeMap<StreamId, PendingPush>,
    connection_window_transition: Option<Rc<Cell<Option<bool>>>>,
    egress_work: VecDeque<EgressWork>,
    ready_dispatch: bool,
    flow_timeout_activity: bool,
    deferred_drops: Vec<EgressWork>,
    deferred_push_drops: Vec<HttpSourceHolder>,
    ingress: BufQueue,
    egress: BufQueue,
    egress_meta: SmallMap<StreamId, EgressMeta>,
    app_buffered_egress: BTreeMap<StreamId, i64>,
    app_content_length_mismatches: BTreeSet<StreamId>,
    pending_tx_ack: Option<TxAckEvent>,
    byte_event_bridges: Vec<Rc<TransportObserverBridge>>,
    stream_read_timeout: Duration,
    ingress_buffer_limit: u32,
    http1_ingress_body_ceiling: usize,
    wait_for_http1_response_on_drain: bool,
    abort_exchanges_on_eof: bool,
    flow_control_paused: bool,
    anti_pipeline_paused: bool,
    upgrade_paused: bool,
    close_after_ingress: bool,
    /// Reads continue past a close-delimited request only to detect the peer's EOF
    /// (`abort_exchanges_on_eof`); what they return is discarded.
    eof_watch: bool,
    ingress_close_stream: Option<StreamId>,
    close_egress_boundary: Option<StreamId>,
    upgrade_stream_awaiting_detach: Option<StreamId>,
    upstream_ingress_upgraded: bool,
    zero_byte_write_pending: bool,
    /// The writer task's wake for batched HTTP/1.1 attempts that do not wait,
    /// created on the first such attempt and kept for the connection.
    batched_source_wake: Option<Arc<SourceWakeCounter>>,
    bytes_read: u64,
    bytes_scheduled: u64,
    bytes_written: u64,
    reads_closed: bool,
    writes_finished: bool,
    reset_after_draining_writes: bool,
    transport_reset_issued: bool,
    reset_after_error_response: Option<StreamId>,
    deferred_drain: bool,
    deferred_connection_error: Option<HttpError>,
    deferred_connection_work: VecDeque<EgressWork>,
    deferred_stream_work: Vec<(EgressWork, HttpError)>,
    byte_event_cancel_error: Option<HttpError>,
    stream_byte_event_cancellations: Vec<(StreamId, HttpError)>,
    fatal_error: Option<HttpError>,
    #[cfg(test)]
    h2_timeout_probe: Option<H2TimeoutArmProbe>,
    #[cfg(test)]
    h2_source_timer_probe: Option<H2SourceTimerProbe>,
    #[cfg(test)]
    writer_wait_hooks: WriterWaitHooks,
    #[cfg(any(test, feature = "test-hooks"))]
    writer_hold: Option<Rc<tokio::sync::Notify>>,
}

struct Http1StatsState {
    stats: Rc<dyn Http2SessionStats>,
    pending: Vec<Http2StatsEvent>,
    transactions_opened: u64,
    finalized: bool,
}

impl Http1StatsState {
    fn record_transaction_opened(&mut self) {
        if self.transactions_opened != 0 {
            self.pending.push(Http2StatsEvent::SessionReused);
        }
        self.transactions_opened = self.transactions_opened.saturating_add(1);
        self.pending.push(Http2StatsEvent::TransactionOpened);
    }

    fn record_transaction_closed(&mut self) {
        self.pending.push(Http2StatsEvent::TransactionClosed);
    }

    fn finalize(&mut self) {
        if !self.finalized {
            self.finalized = true;
            self.pending.push(Http2StatsEvent::TransactionsServed(
                self.transactions_opened,
            ));
        }
    }
}

struct PendingPush {
    associated_stream: StreamId,
    source: HttpSourceHolder,
}

impl DriverState {
    // Only the writer consumes this history, immediately before setting up its
    // flow deadline. Callback servicing must not erase coalesced ordinary work.
    fn note_flow_timeout_activity(&mut self) {
        self.flow_timeout_activity |= self.http2.is_some();
    }

    #[cfg(test)]
    fn record_h2_source_timers(
        &self,
        flow_deadline: Option<Instant>,
        buffer_timeout: Option<(StreamId, Instant)>,
        blocked: Option<&(Vec<StreamId>, bool)>,
    ) -> Option<Rc<tokio::sync::Notify>> {
        let probe = self.h2_source_timer_probe.as_ref()?;
        let sequence = probe
            .control
            .snapshot
            .borrow()
            .as_ref()
            .map_or(1, |snapshot| {
                snapshot.sequence.checked_add(1).expect("test sequence")
            });
        let snapshot = H2SourceFlowSnapshot {
            sequence,
            flow_deadline,
            buffer_timeout,
            blocked: blocked.cloned(),
            connection_credit: self.codec.connection_send_credit(),
            stream_credit: self.http2.as_ref()?.stream_send_credit(probe.stream),
        };
        if probe.control.capture_next_arm.replace(false) {
            *probe.control.first_arm.borrow_mut() = Some(snapshot.clone());
        }
        *probe.control.snapshot.borrow_mut() = Some(snapshot);
        Some(Rc::clone(&probe.control.changed))
    }

    fn core(&self) -> &SessionCore {
        self.core
            .as_deref()
            .expect("a running uniplex owns its core")
    }

    fn core_mut(&mut self) -> &mut SessionCore {
        self.core
            .as_deref_mut()
            .expect("a running uniplex owns its core")
    }

    fn take_response_timeout(&mut self, stream: StreamId) -> bool {
        self.egress_meta
            .get_mut(&stream)
            .is_some_and(|meta| std::mem::take(&mut meta.response_timeout_pending))
    }

    fn has_session_stats(&self) -> bool {
        match &self.http2 {
            Some(http2) => http2.config.stats.is_some(),
            None => self.http1_stats.is_some(),
        }
    }

    fn record_error(&mut self, error: HttpError) {
        if self.fatal_error.is_none() {
            self.fatal_error = Some(error);
        }
    }

    /// Reads a stream's `IngressMeta` message, or returns `None` when the stream
    /// has no `IngressMeta`.
    #[cfg(test)]
    pub(crate) fn with_ingress_message_for_test<R>(
        &self,
        stream: StreamId,
        read: impl FnOnce(Option<&roxygen_core::message::HttpMessage>) -> R,
    ) -> Option<R> {
        self.ingress_meta
            .get(&stream)
            .map(|meta| read(meta.message.as_ref()))
    }

    fn h1_close_rejects_egress(&self, stream: StreamId) -> bool {
        self.codec.protocol() == CodecProtocol::Http11
            && self.core().direction().is_downstream()
            && self
                .close_egress_boundary
                .is_some_and(|boundary| stream > boundary)
    }

    fn h1_selected_request_pending(&self) -> bool {
        self.codec.protocol() == CodecProtocol::Http11
            && self.core().direction().is_downstream()
            && self.core().drains_request_body_on_drop()
            && !self.reset_after_draining_writes
            && self.close_egress_boundary.is_some_and(|boundary| {
                self.core().stream(boundary).is_ok_and(|stream| {
                    !stream.stream_source_complete()
                        && stream.terminal_error().is_none()
                        && !stream.ingress_cancel_requested()
                })
            })
    }

    fn h1_selected_close_bytes_drained(&self) -> bool {
        self.codec.protocol() == CodecProtocol::Http11
            && self.core().direction().is_downstream()
            && self.close_egress_boundary.is_some()
            && self.codec.close_on_egress_complete()
            && self.egress.is_empty()
            && !self.zero_byte_write_pending
            && !self.h1_selected_request_pending()
    }

    fn enqueue_transfer(
        &mut self,
        stream: StreamId,
        transfer: EgressTransfer,
        cancellation: Option<CancellationToken>,
    ) {
        if let Some(http2) = &mut self.http2 {
            let readiness = EgressSourceReadiness::new(
                Arc::clone(&http2.body_source_publications.source_ready),
                cancellation.clone(),
                Some(BodySourcePublication {
                    key: EgressTransferKey {
                        stream,
                        generation: transfer.generation(),
                    },
                    owner: Arc::downgrade(&http2.body_source_publications),
                }),
                Some(transfer.cancellation_token()),
            );
            http2.push_transfer(
                stream,
                QueuedEgressTransfer {
                    transfer,
                    readiness: Some(readiness),
                    cancellation,
                },
                false,
            );
            http2.scheduler.enqueue(stream);
            return;
        }
        let position = self.egress_work.iter().position(|work| match work {
            EgressWork::Event { stream: queued, .. }
            | EgressWork::Transfer { stream: queued, .. } => *queued > stream,
        });
        let work = EgressWork::Transfer {
            stream,
            transfer,
            cancellation,
            readiness: None,
        };
        if self.h1_close_rejects_egress(stream) {
            self.egress_work.push_back(work);
            return;
        }
        if let Some(position) = position {
            self.egress_work.insert(position, work);
        } else {
            self.egress_work.push_back(work);
        }
    }

    fn ready_egress_work(&self, skipped: &BTreeSet<(StreamId, u64)>) -> Option<ReadyEgressWork> {
        if let Some(http2) = &self.http2 {
            if http2.egress_order.is_empty() {
                return None;
            }
            let connection_credit = self.codec.connection_send_credit();
            let eligible = |work: &QueuedEgressTransfer, stream| {
                work.transfer.cancellation_requested()
                    || work
                        .cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancellation_requested)
                    || (!skipped.contains(&(stream, work.transfer.generation()))
                        && (work.transfer.is_complete()
                            || (work.transfer.can_read_source()
                                && work
                                    .readiness
                                    .as_ref()
                                    .is_none_or(EgressSourceReadiness::is_ready))
                            || (connection_credit != 0
                                && work
                                    .transfer
                                    .can_dequeue_body(http2.stream_send_credit(stream)))))
            };
            let selected = http2.scheduler.next_matching(|stream| {
                #[cfg(test)]
                H2_EGRESS_SCAN_EVALUATIONS.set(H2_EGRESS_SCAN_EVALUATIONS.get() + 1);
                http2.streams.get(&stream).is_some_and(|state| {
                    state.egress_work.iter().any(|work| eligible(work, stream))
                })
            })?;
            let work = http2
                .streams
                .get(&selected)?
                .egress_work
                .iter()
                .find(|work| eligible(work, selected))?;
            return Some(ReadyEgressWork::Http2(EgressTransferKey {
                stream: selected,
                generation: work.transfer.generation(),
            }));
        }
        let front_stream = self.egress_work.front().map(|work| match work {
            EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => *stream,
        });
        if front_stream.is_some_and(|stream| self.h1_close_rejects_egress(stream)) {
            return None;
        }
        if self.core().direction().is_downstream()
            && !egress_stream_is_ready(front_stream, self.ingress_meta.keys().next().copied())
        {
            return None;
        }
        front_stream.map(|_| ReadyEgressWork::Http1)
    }

    fn pop_ready_egress_work(&mut self, skipped: &BTreeSet<(StreamId, u64)>) -> Option<EgressWork> {
        match self.ready_egress_work(skipped)? {
            ReadyEgressWork::Http1 => self.egress_work.pop_front(),
            ReadyEgressWork::Http2(key) => {
                let http2 = self.http2.as_mut().expect("HTTP/2 work has HTTP/2 state");
                let position = http2
                    .egress_order
                    .iter()
                    .position(|queued| *queued == key)
                    .expect("selected transfer is in global order");
                http2.egress_order.remove(position);
                Some(http2.take_transfer(key))
            }
        }
    }

    fn return_egress_transfer(&mut self, stream: StreamId, work: QueuedEgressTransfer) {
        match &mut self.http2 {
            Some(http2) => http2.push_transfer(stream, work, true),
            None => self.egress_work.push_front(work.into_work(stream)),
        }
    }

    fn take_egress_work(&mut self) -> VecDeque<EgressWork> {
        match &mut self.http2 {
            Some(http2) => std::mem::take(&mut http2.egress_order)
                .into_iter()
                .map(|key| http2.take_transfer(key))
                .collect(),
            None => std::mem::take(&mut self.egress_work),
        }
    }

    fn extract_egress_work(&mut self, matches: impl Fn(StreamId) -> bool) -> VecDeque<EgressWork> {
        let mut removed = VecDeque::new();
        if let Some(http2) = &mut self.http2 {
            let mut position = 0;
            while let Some(key) = http2.egress_order.get(position).copied() {
                if matches(key.stream) {
                    http2.egress_order.remove(position);
                    removed.push_back(http2.take_transfer(key));
                } else {
                    position += 1;
                }
            }
        } else {
            let queued = std::mem::take(&mut self.egress_work);
            for work in queued {
                let stream = match &work {
                    EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => {
                        *stream
                    }
                };
                if matches(stream) {
                    removed.push_back(work);
                } else {
                    self.egress_work.push_back(work);
                }
            }
        }
        removed
    }

    fn should_continue_reading(&self) -> bool {
        let draining_ingress_grace = self.http2.as_ref().is_some_and(|http2| {
            self.core().direction().is_downstream()
                && http2.sent_initial_goaway
                && !http2.sent_final_goaway
        });
        !self.reads_closed
            && !self.writes_finished
            && !self.reset_after_draining_writes
            && (self.codec.protocol() == CodecProtocol::Http2
                || self.reset_after_error_response.is_none())
            && (draining_ingress_grace
                || self.codec.is_reusable()
                || self.codec.is_busy()
                || self.core().num_transactions() != 0)
    }

    fn has_pending_egress(&self) -> bool {
        !self.egress.is_empty()
            || !self.egress_work.is_empty()
            || self
                .http2
                .as_ref()
                .is_some_and(|http2| !http2.egress_order.is_empty())
            || self.core().has_pending_events()
            || self.core().pending_handler_task_count() != 0
            || self.h1_selected_request_pending()
    }

    fn should_continue_writing(&self) -> bool {
        let close_with_open_streams =
            self.codec.close_on_egress_complete() || self.reset_after_draining_writes;
        let draining_ingress_grace = self.http2.as_ref().is_some_and(|http2| {
            self.core().direction().is_downstream()
                && http2.sent_initial_goaway
                && !http2.sent_final_goaway
        });
        self.has_pending_egress()
            || (!close_with_open_streams && self.core().num_transactions() != 0)
            || (!self.reads_closed && (self.codec.is_reusable() || draining_ingress_grace))
    }

    fn should_wait_for_ingress_resume(&self) -> bool {
        self.flow_control_paused || self.anti_pipeline_paused || self.upgrade_paused
    }

    fn has_pending_callback_work(&self) -> bool {
        self.core().has_pending_driver_work()
            || self.should_wait_for_ingress_resume()
            || self.deferred_drain
            || self.deferred_connection_error.is_some()
            || !self.deferred_connection_work.is_empty()
            || !self.deferred_drops.is_empty()
            || !self.deferred_stream_work.is_empty()
            || !self.deferred_push_drops.is_empty()
            || self
                .http2
                .as_ref()
                .is_some_and(|http2| !http2.pending_stats.is_empty())
            || self
                .http1_stats
                .as_ref()
                .is_some_and(|http1| !http1.pending.is_empty())
            || !self.lifecycle.pending.borrow().is_empty()
    }

    fn h2_blocked_streams(&self) -> Option<(Vec<StreamId>, bool)> {
        #[cfg(test)]
        writer_probe::note(|counts| counts.http2_setup += 1);
        let http2 = self.http2.as_ref()?;
        let connection_blocked = self.codec.connection_send_credit() == 0;
        let streams = if connection_blocked {
            http2
                .queued_transfers()
                .filter_map(|(stream, work)| {
                    (work.transfer.is_reading_body() && work.transfer.has_buffered_events())
                        .then_some(stream)
                })
                .collect::<Vec<_>>()
        } else {
            self.core()
                .stream_ids()
                .into_iter()
                .filter(|stream| {
                    http2.stream_send_credit(*stream) == 0
                        && self
                            .core()
                            .stream(*stream)
                            .is_ok_and(|state| !state.egress_complete())
                })
                .collect::<Vec<_>>()
        };
        (!streams.is_empty()).then_some((streams, connection_blocked))
    }

    fn h2_buffer_timeout(&self, write_timeout: Duration) -> Option<(EgressTransferKey, Instant)> {
        #[cfg(test)]
        writer_probe::note(|counts| counts.http2_setup += 1);
        #[cfg(test)]
        if let Some(probe) = &self.h2_timeout_probe {
            probe.buffer_deadline.set(None);
        }
        if write_timeout.is_zero() {
            return None;
        }
        self.http2
            .as_ref()?
            .queued_transfers()
            .filter_map(|(stream, work)| {
                let transfer = &work.transfer;
                if !transfer.is_reading_body()
                    || transfer.cancellation_requested()
                    || !transfer.generation_is_current()
                    || work
                        .cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancellation_requested)
                    || !self
                        .core()
                        .stream(stream)
                        .is_ok_and(|state| !state.egress_complete())
                {
                    return None;
                }
                let deadline = transfer.buffer_timeout_deadline();
                #[cfg(test)]
                if let Some(probe) = &self.h2_timeout_probe
                    && probe.stream == stream
                {
                    probe.buffer_deadline.set(deadline);
                }
                deadline.map(|deadline| {
                    (
                        EgressTransferKey {
                            stream,
                            generation: transfer.generation(),
                        },
                        deadline,
                    )
                })
            })
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn revalidate_h2_buffer_timeout(
        &self,
        candidate: Option<(EgressTransferKey, Instant)>,
    ) -> Option<(EgressTransferKey, Instant)> {
        let (key, armed_deadline) = candidate?;
        let http2 = self.http2.as_ref()?;
        if !http2.streams.contains_key(&key.stream)
            || !self
                .core()
                .stream(key.stream)
                .is_ok_and(|state| !state.egress_complete())
        {
            return None;
        }
        http2.egress_queue(key.stream)?.iter().find_map(|work| {
            let transfer = &work.transfer;
            if transfer.generation() == key.generation
                && transfer.generation_is_current()
                && transfer.is_reading_body()
                && !transfer.is_complete()
                && !transfer.cancellation_requested()
                && !work
                    .cancellation
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancellation_requested)
            {
                transfer
                    .buffer_timeout_deadline()
                    .filter(|deadline| *deadline == armed_deadline)
                    .map(|deadline| (key, deadline))
            } else {
                None
            }
        })
    }

    #[cfg(test)]
    fn record_h2_timeout_arm(&self, flow_deadline: Option<Instant>) {
        let (Some(probe), Some(http2), Some(flow_deadline)) =
            (&self.h2_timeout_probe, &self.http2, flow_deadline)
        else {
            return;
        };
        let generation = probe.generation.get().saturating_add(1);
        probe.generation.set(generation);
        let Some(buffer_deadline) = probe.buffer_deadline.get() else {
            return;
        };
        let stream_credit = http2.stream_send_credit(probe.stream);
        if stream_credit != 0 {
            return;
        }
        let connection_credit = self.codec.connection_send_credit();
        let snapshot = H2TimeoutArmSnapshot {
            generation,
            flow_deadline,
            buffer_deadline,
            connection_credit,
            stream_credit,
        };
        let mut phases = probe.phases.get();
        if connection_credit == 0 && phases.blocked.is_none() {
            phases.blocked = Some(snapshot);
        } else if connection_credit != 0 && phases.blocked.is_some() && phases.rearmed.is_none() {
            phases.rearmed = Some(snapshot);
        }
        probe.phases.set(phases);
    }
}

fn egress_stream_is_ready(front: Option<StreamId>, earliest_ingress: Option<StreamId>) -> bool {
    !matches!((front, earliest_ingress), (Some(front), Some(earliest)) if front > earliest)
}

fn flush_lifecycle_events(state: &Rc<RefCell<DriverState>>) {
    let lifecycle = Rc::clone(&state.borrow().lifecycle);
    lifecycle.flush();
}

fn signed_byte_count(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

fn arm_response_timeout(state: &mut DriverState, stream: StreamId) {
    if !state.core().direction().is_upstream() {
        return;
    }
    if state.take_response_timeout(stream) {
        let _ = state.core().arm_stream_read_timeout(stream);
    }
}

fn sync_buffered_egress_stats(state: &Rc<RefCell<DriverState>>) {
    let current = {
        let state = state.borrow();
        if !state.has_session_stats() {
            return;
        }
        state
            .core()
            .stream_ids()
            .into_iter()
            .filter_map(|stream| {
                state.core().stream(stream).ok().map(|state| {
                    (
                        stream,
                        state.buffered_egress_bytes(),
                        state.take_buffered_egress_deltas(),
                        state.egress_content_length_mismatch().is_some(),
                    )
                })
            })
            .collect::<Vec<_>>()
    };
    let mut state = state.borrow_mut();
    let current_bytes = current
        .iter()
        .map(|(stream, bytes, _, _)| (*stream, *bytes))
        .collect::<BTreeMap<_, _>>();
    let previous = std::mem::replace(&mut state.app_buffered_egress, current_bytes.clone());
    let new_mismatches = current
        .iter()
        .filter_map(|(stream, _, _, mismatch)| {
            (*mismatch && state.app_content_length_mismatches.insert(*stream)).then_some(*stream)
        })
        .count();
    let Some(pending) = pending_session_stats(&mut state) else {
        return;
    };
    current.into_iter().for_each(|(_, _, deltas, _)| {
        pending.extend(deltas.into_iter().map(Http2StatsEvent::BufferedWrite));
    });
    for (stream, before) in previous {
        if !current_bytes.contains_key(&stream) && before != 0 {
            pending.push(Http2StatsEvent::BufferedWrite(-before));
        }
    }
    for _ in 0..new_mismatches {
        pending.push(Http2StatsEvent::ContentLengthMismatch);
    }
}

fn pending_session_stats(state: &mut DriverState) -> Option<&mut Vec<Http2StatsEvent>> {
    match (&mut state.http2, &mut state.http1_stats) {
        (Some(http2), _) if http2.config.stats.is_some() => Some(&mut http2.pending_stats),
        (Some(_), _) => None,
        (None, Some(http1)) => Some(&mut http1.pending),
        (None, None) => None,
    }
}

fn flush_session_stats(state: &Rc<RefCell<DriverState>>) {
    let (stats, events) = {
        let mut state = state.borrow_mut();
        let state = &mut *state;
        match (&mut state.http2, &mut state.http1_stats) {
            (Some(http2), _) => (
                http2.config.stats.clone(),
                std::mem::take(&mut http2.pending_stats),
            ),
            (None, Some(http1)) => (
                Some(Rc::clone(&http1.stats)),
                std::mem::take(&mut http1.pending),
            ),
            (None, None) => return,
        }
    };
    let Some(stats) = stats else {
        return;
    };
    let mut first_panic = None;
    for event in events {
        capture_panic(&mut first_panic, || match event {
            Http2StatsEvent::TransactionOpened => stats.record_transaction_opened(),
            Http2StatsEvent::TransactionClosed => stats.record_transaction_closed(),
            Http2StatsEvent::SessionReused => stats.record_session_reused(),
            Http2StatsEvent::TransactionsServed(transactions) => {
                stats.record_transactions_served(transactions)
            }
            Http2StatsEvent::TransactionStalled => stats.record_transaction_stalled(),
            Http2StatsEvent::SessionStalled => stats.record_session_stalled(),
            Http2StatsEvent::BufferedRead(bytes) => stats.record_buffered_read_bytes(bytes),
            Http2StatsEvent::BufferedWrite(bytes) => stats.record_buffered_write_bytes(bytes),
            Http2StatsEvent::ContentLengthMismatch => stats.record_content_length_mismatch(),
            Http2StatsEvent::IngressGetBody => stats.record_ingress_get_body(),
            Http2StatsEvent::RateLimit(sample) => stats.record_rate_limit(sample),
            Http2StatsEvent::ReadLoopLimit => stats.record_read_loop_limit(),
            Http2StatsEvent::PingTimeout => stats.record_ping_timeout(),
        });
    }
    resume_first_panic(first_panic);
}

fn flush_http2_flow_transition(state: &Rc<RefCell<DriverState>>) {
    let transition = state
        .borrow()
        .connection_window_transition
        .as_ref()
        .and_then(|transition| transition.take());
    let Some(open) = transition else {
        return;
    };
    {
        let mut state = state.borrow_mut();
        state.note_flow_timeout_activity();
        state.core().observe_lifecycle(if open {
            LifecycleEvent::Http2FlowWindowOpen
        } else {
            LifecycleEvent::Http2FlowWindowClosed
        });
        if !open
            && let Some(http2) = state.http2.as_mut()
            && http2.config.stats.is_some()
        {
            http2.pending_stats.push(Http2StatsEvent::SessionStalled);
        }
    }
    flush_session_stats(state);
    flush_lifecycle_events(state);
}

#[cfg(test)]
pub(crate) fn egress_stream_is_ready_for_test(front: StreamId, earliest: StreamId) -> bool {
    egress_stream_is_ready(Some(front), Some(earliest))
}

#[cfg(test)]
thread_local! {
    /// `bytes_processed` callbacks that this thread's sessions drained from their sources.
    static INGRESS_CREDIT_CALLBACKS: Cell<usize> = const { Cell::new(0) };
    /// Streams that this thread's HTTP/2 egress scans checked for eligible work.
    static H2_EGRESS_SCAN_EVALUATIONS: Cell<usize> = const { Cell::new(0) };
}

/// Takes this thread's count of drained `bytes_processed` callbacks, resetting it.
#[cfg(test)]
pub(crate) fn take_ingress_credit_callbacks_for_test() -> usize {
    INGRESS_CREDIT_CALLBACKS.replace(0)
}

/// Takes this thread's count of HTTP/2 egress scan stream checks, resetting it.
#[cfg(test)]
pub(crate) fn take_h2_egress_scan_evaluations_for_test() -> usize {
    H2_EGRESS_SCAN_EVALUATIONS.replace(0)
}

struct DetachedSessionCore<'a> {
    state: &'a Rc<RefCell<DriverState>>,
    core: Option<Box<SessionCore>>,
}

impl<'a> DetachedSessionCore<'a> {
    fn take(state: &'a Rc<RefCell<DriverState>>) -> Self {
        let core = state
            .borrow_mut()
            .core
            .take()
            .expect("a running uniplex owns its core");
        Self {
            state,
            core: Some(core),
        }
    }

    fn core_mut(&mut self) -> &mut SessionCore {
        self.core
            .as_deref_mut()
            .expect("a detached core is restored exactly once")
    }

    fn restore(mut self) {
        let (deferred, deferred_work) = {
            let mut state = self.state.borrow_mut();
            (
                state.deferred_connection_error.take(),
                std::mem::take(&mut state.deferred_connection_work),
            )
        };
        let failure = deferred.as_ref().map(|error| {
            catch_unwind(AssertUnwindSafe(|| {
                self.core_mut().fail_connection(error.clone());
            }))
        });
        self.state.borrow_mut().core = self.core.take();
        let cancellation = catch_unwind(AssertUnwindSafe(|| match deferred.as_ref() {
            Some(error) => cancel_egress_work_items(deferred_work, error),
            None => drop_items_individually(deferred_work),
        }));
        if let Some(Err(payload)) = failure {
            std::panic::resume_unwind(payload);
        }
        if let Err(payload) = cancellation {
            std::panic::resume_unwind(payload);
        }
    }
}

impl Drop for DetachedSessionCore<'_> {
    fn drop(&mut self) {
        if let Some(mut core) = self.core.take() {
            let (deferred, deferred_work) = {
                let mut state = self.state.borrow_mut();
                (
                    state.deferred_connection_error.take(),
                    std::mem::take(&mut state.deferred_connection_work),
                )
            };
            if let Some(error) = deferred.as_ref() {
                let _ = catch_unwind(AssertUnwindSafe(|| core.fail_connection(error.clone())));
            }
            self.state.borrow_mut().core = Some(core);
            let _ = catch_unwind(AssertUnwindSafe(|| match deferred.as_ref() {
                Some(error) => cancel_egress_work_items(deferred_work, error),
                None => drop_items_individually(deferred_work),
            }));
        }
    }
}

/// Same-worker command surface retained while [`UniplexSession::run`] owns the
/// transport loops.
#[derive(Clone)]
pub struct UniplexHandle {
    state: Weak<RefCell<DriverState>>,
    shared: Rc<UniplexShared>,
    observer: Weak<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    cleanup: Weak<RunCleanupControl>,
    outgoing_activity: OutgoingTransactionActivity,
}

mod shared {
    use std::cell::Cell;

    use roxygen_runtime::cancellation::CancellationSource;

    /// Wakes, cancellation sources, and lifecycle flags shared by one session,
    /// its run future, its transport loops, and its handles. It does not own the
    /// driver state, transport, observer, or cleanup task.
    pub struct UniplexShared {
        pub(super) read_wake: tokio::sync::Notify,
        pub(super) write_wake: tokio::sync::Notify,
        pub(super) read_cancellation: CancellationSource,
        pub(super) write_cancellation: CancellationSource,
        pub(super) closing: Cell<bool>,
        pub(super) completion: Cell<bool>,
    }
}

use shared::UniplexShared;

/// The reset of one committed stream behind `experimental::UniplexStreamAbort`.
/// It holds the driver state weakly.
pub(crate) struct StreamAbort {
    state: Weak<RefCell<DriverState>>,
    shared: Rc<UniplexShared>,
    stream: StreamId,
}

impl StreamAbort {
    /// Resets the stream with `CANCEL` if it is still open in either direction.
    /// A stream that has received its response EOM and sent its request EOM is
    /// closed in both directions, so only its unread response is discarded.
    /// A session that failed or finished writing is left unchanged, but a
    /// closing session still resets a live stream, such as an HTTP/1.1 tunnel
    /// that keeps reading after its upload EOM closed the connection's writes.
    /// From a session callout, which runs while the session borrows its state
    /// or holds its core detached, the abort cannot reach the stream and has
    /// no effect.
    ///
    /// The reset's state change and its RST_STREAM (on HTTP/1.1, the TCP reset
    /// after the drained writes) are recorded before any foreign callback
    /// runs. The callbacks (completion wakers, the request source's
    /// `stop_reading`, lifecycle observers) then run one by one, and the first
    /// panic resumes after all of them.
    pub(crate) fn abort(self) {
        if self.shared.completion.get() {
            return;
        }
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let error = HttpError::with_message(HttpErrorCode::CANCEL, b"stream aborted");
        let (reset, ingress, ingress_eom_seen) = {
            let Ok(mut driver) = state.try_borrow_mut() else {
                return;
            };
            if driver.fatal_error.is_some() || driver.writes_finished {
                return;
            }
            let Some(stream) = driver
                .core
                .as_deref()
                .and_then(|core| core.stream_if_present(self.stream))
            else {
                return;
            };
            if stream.egress_complete() && stream.terminal_error().is_some() {
                return;
            }
            let ingress = stream.ingress_producer();
            let ingress_eom_seen = stream.ingress_eom_seen();
            let reset = if !ingress_eom_seen || !stream.egress_complete() {
                let Ok(retirement) = driver
                    .core_mut()
                    .prepare_abort_stream_without_wire(self.stream, error.clone())
                else {
                    return;
                };
                let mut update = retirement.update();
                update.action = StreamAction::Reset {
                    code: HttpErrorCode::CANCEL,
                    origin: crate::session::ResetOrigin::Ingress,
                };
                let drain = apply_stream_update_locked(&mut driver, update);
                Some((retirement, drain))
            } else {
                None
            };
            (reset, ingress, ingress_eom_seen)
        };
        let mut first_panic = None;
        if let Some((retirement, drain)) = reset {
            let core = DetachedSessionCore::take(&state);
            capture_panic(&mut first_panic, || {
                ingress.abort_discarding_queued(error.clone(), error.message());
            });
            capture_panic(&mut first_panic, || {
                if let Err(payload) = retirement.retire() {
                    std::panic::resume_unwind(payload);
                }
            });
            capture_panic(&mut first_panic, || core.restore());
            if let Some(trigger) = drain {
                retain_first_panic(
                    &mut first_panic,
                    initiate_drain_after_state_borrow(&state, trigger).panic,
                );
            }
            capture_panic(&mut first_panic, || drop_deferred_work(&state));
            capture_panic(&mut first_panic, || flush_session_stats(&state));
            capture_panic(&mut first_panic, || flush_lifecycle_events(&state));
            capture_panic(&mut first_panic, || self.shared.read_wake.notify_one());
            capture_panic(&mut first_panic, || self.shared.write_wake.notify_one());
        }
        // An abort cannot fail a source that has seen EOM, so stop it instead.
        if ingress_eom_seen {
            capture_panic(&mut first_panic, || {
                ingress.stop_reading(Some(HttpErrorCode::CANCEL));
            });
        }
        resume_first_panic(first_panic);
    }
}

#[cfg(test)]
impl std::ops::Deref for UniplexHandle {
    type Target = UniplexShared;

    fn deref(&self) -> &UniplexShared {
        &self.shared
    }
}

impl UniplexHandle {
    fn live_state(&self) -> Result<Rc<RefCell<DriverState>>, HttpError> {
        if self.shared.closing.get() || self.shared.completion.get() {
            return Err(session_finished());
        }
        self.state.upgrade().ok_or_else(session_finished)
    }

    fn admission_state(&self) -> Result<Rc<RefCell<DriverState>>, HttpError> {
        if self.context.handle().state() != WorkerState::Open || self.shared.closing.get() {
            return Err(session_finished());
        }
        let state = self
            .state
            .upgrade()
            .filter(|_| !self.shared.completion.get())
            .ok_or_else(session_finished)?;
        if state.borrow().core.is_none() {
            return Err(session_finished());
        }
        Ok(state)
    }

    #[cfg(test)]
    pub(crate) fn h2_egress_owners_for_test(
        &self,
        stream: StreamId,
    ) -> Result<H2EgressOwnersSnapshot, HttpError> {
        let state = self.live_state()?;
        let driver = state.borrow();
        if driver.http2.is_none() {
            return Err(invalid_transition(b"egress owner snapshot requires HTTP/2"));
        }
        let mut snapshot = H2EgressOwnersSnapshot {
            egress_complete: driver
                .core()
                .stream(stream)
                .ok()
                .map(|state| state.egress_complete()),
            queued: 0,
            complete: 0,
        };
        if let Some(queue) = driver
            .http2
            .as_ref()
            .and_then(|http2| http2.egress_queue(stream))
        {
            snapshot.queued = queue.len();
            snapshot.complete = queue
                .iter()
                .filter(|work| work.transfer.is_complete())
                .count();
        }
        Ok(snapshot)
    }

    pub fn worker_id(&self) -> WorkerId {
        self.context.worker_id()
    }

    pub fn outgoing_transaction_activity(&self) -> OutgoingTransactionActivity {
        self.outgoing_activity.clone()
    }

    /// Reports whether this stream has returned its outgoing protocol capacity.
    ///
    /// Pair with `LifecycleEvent::OutgoingCapacityReleased` when observation
    /// begins after committing a request. Source ownership and exchange cleanup
    /// remain independent of protocol capacity.
    pub fn outgoing_capacity_released(&self, stream: StreamId) -> Result<bool, HttpError> {
        let state = self.live_state()?;
        Ok(state
            .borrow()
            .core()
            .stream(stream)?
            .outgoing_capacity_released())
    }

    pub fn protocol(&self) -> Result<CodecProtocol, HttpError> {
        Ok(self.live_state()?.borrow().codec.protocol())
    }

    pub fn update_http2_priority(
        &self,
        stream: StreamId,
        priority: roxygen_core::priority::HttpPriority,
    ) -> Result<bool, HttpError> {
        let state = self.live_state()?;
        let mut state = state.borrow_mut();
        let http2 = state.http2.as_mut().ok_or_else(|| {
            invalid_transition(b"HTTP/2 priority is unavailable on an HTTP/1.1 session")
        })?;
        if !http2.scheduler.contains(stream) {
            return Ok(false);
        }
        http2.update_priority(stream, priority);
        state.note_flow_timeout_activity();
        drop(state);
        self.shared.write_wake.notify_one();
        Ok(true)
    }

    pub fn http2_stream_send_credit(&self, stream: StreamId) -> Result<u32, HttpError> {
        let state = self.live_state()?;
        let state = state.borrow();
        state
            .http2
            .as_ref()
            .map(|http2| http2.stream_send_credit(stream))
            .ok_or_else(|| {
                invalid_transition(b"HTTP/2 flow control is unavailable on an HTTP/1.1 session")
            })
    }

    pub fn is_finished(&self) -> bool {
        self.shared.completion.get()
    }

    /// Reports whether a running session has no transactions or queued/in-flight egress.
    ///
    /// This owner-local snapshot does not retire admission or close the transport.
    /// Call before requesting drain, which can itself enqueue control frames.
    pub fn is_idle(&self) -> bool {
        let Ok(state) = self.live_state() else {
            return false;
        };
        let Ok(state) = state.try_borrow() else {
            return false;
        };
        let Some(core) = state.core.as_ref() else {
            return false;
        };
        core.num_transactions() == 0
            && core.pending_exchange_count() == 0
            && !state.has_pending_egress()
            && !state.zero_byte_write_pending
            && state.bytes_scheduled != u64::MAX
            && state.bytes_scheduled == state.bytes_written
            && state.fatal_error.is_none()
            && !state.reset_after_draining_writes
    }

    #[cfg(test)]
    pub(crate) fn closing_state_for_test(&self) -> (bool, bool) {
        (self.shared.closing.get(), self.shared.completion.get())
    }

    #[cfg(test)]
    pub(crate) fn set_writer_wait_hooks_for_test(&self, hooks: WriterWaitHooks) {
        if let Some(state) = self.state.upgrade() {
            state.borrow_mut().writer_wait_hooks = hooks;
        }
    }

    /// Holds the writer, the next time it would stage egress, until `release`
    /// is notified, so that the reader can process what the peer sent first.
    /// Test-only: the `test-hooks` feature exposes it to other crates' tests.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn hold_writer_for_test(&self, release: Rc<tokio::sync::Notify>) {
        if let Some(state) = self.state.upgrade() {
            state.borrow_mut().writer_hold = Some(release);
        }
    }

    #[cfg(test)]
    pub(crate) fn callback_queue_for_test(&self) -> Rc<crate::stream_state::StreamCallbacks> {
        self.state
            .upgrade()
            .expect("live session state")
            .borrow()
            .core()
            .callback_queue()
    }

    #[cfg(test)]
    pub(crate) fn request_write_cancellation_for_test(&self) -> bool {
        self.shared.write_cancellation.request_cancellation()
    }

    #[cfg(test)]
    pub(crate) fn wake_writer_for_test(&self) {
        self.shared.write_wake.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn cleanup_reachable_for_test(&self) -> bool {
        self.cleanup.upgrade().is_some()
    }

    #[cfg(test)]
    pub(crate) fn drain_reader_handler_callback_for_test(&self) -> Result<bool, HttpError> {
        let state = self.live_state()?;
        if !state.borrow().core().has_pending_handler_completion() {
            return Ok(false);
        }
        drive_read_callbacks(&state, &self.shared.read_wake, &self.shared.write_wake)?;
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn has_response_timeout_for_test(&self, stream: StreamId) -> bool {
        let Some(state) = self.state.upgrade() else {
            return false;
        };
        let driver = state.borrow();
        driver
            .egress_meta
            .get(&stream)
            .is_some_and(|meta| meta.response_timeout_pending)
    }

    #[cfg(test)]
    pub(crate) fn observe_h2_timeout_arms_for_test(
        &self,
        stream: StreamId,
    ) -> Result<Rc<Cell<H2TimeoutPhaseCopies>>, HttpError> {
        let phases = Rc::new(Cell::new(H2TimeoutPhaseCopies::default()));
        self.live_state()?.borrow_mut().h2_timeout_probe = Some(H2TimeoutArmProbe {
            stream,
            generation: Cell::new(0),
            buffer_deadline: Cell::new(None),
            phases: Rc::clone(&phases),
        });
        Ok(phases)
    }

    #[cfg(test)]
    pub fn buffered_ingress_for_test(&self) -> Result<Vec<u8>, HttpError> {
        let state = self.live_state()?;
        let state = state.borrow();
        state
            .ingress
            .coalesce(state.ingress.chain_length())
            .map(|bytes| bytes.to_vec())
            .map_err(|error| {
                HttpError::with_message(HttpErrorCode::INTERNAL_ERROR, error.to_string().as_bytes())
            })
    }

    #[cfg(test)]
    pub(crate) fn h1_close_snapshot_for_test(
        &self,
        stream: StreamId,
    ) -> Result<(bool, bool, bool, usize), HttpError> {
        let state = self
            .state
            .upgrade()
            .filter(|_| !self.shared.completion.get())
            .ok_or_else(session_finished)?;
        let driver = state.borrow();
        if driver.core.is_none() {
            return Err(session_finished());
        }
        let queued = driver
            .egress_work
            .iter()
            .filter(|work| match work {
                EgressWork::Event { stream: queued, .. }
                | EgressWork::Transfer { stream: queued, .. } => *queued == stream,
            })
            .count();
        Ok((
            driver.ingress_meta.contains_key(&stream),
            driver.core().stream(stream).is_ok(),
            driver.h1_close_rejects_egress(stream),
            queued,
        ))
    }

    #[cfg(test)]
    pub(crate) fn h1_close_sealed_for_test(&self, stream: StreamId) -> Result<bool, HttpError> {
        let state = self
            .state
            .upgrade()
            .filter(|_| !self.shared.completion.get())
            .ok_or_else(session_finished)?;
        let driver = state.borrow();
        if driver.core.is_none() {
            return Err(session_finished());
        }
        Ok(driver.h1_close_rejects_egress(stream)
            && driver.core().stream(stream).map_or(true, |state| {
                state.egress_complete() && state.ingress_cancel_requested()
            }))
    }

    #[cfg(test)]
    pub(crate) fn h1_handler_cancellation_for_test(
        &self,
        stream: StreamId,
    ) -> Result<Option<CancellationToken>, HttpError> {
        let state = self.live_state()?;
        let cancellation = state.borrow().core().handler_cancellation_for_test(stream);
        Ok(cancellation)
    }

    #[cfg(test)]
    pub(crate) fn record_h1_ingress_error_for_test(
        &self,
        error: HttpError,
    ) -> Result<(), HttpError> {
        let state = self.live_state()?;
        let mut driver = state.borrow_mut();
        driver.record_error(error.clone());
        driver.byte_event_cancel_error.get_or_insert(error);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn h1_ingress_reset_for_test(&self, stream: StreamId) -> Result<bool, HttpError> {
        let state = self.state.upgrade().ok_or_else(session_finished)?;
        let driver = state.borrow();
        Ok(driver.reset_after_draining_writes
            && driver.reads_closed
            && driver.core.as_ref().is_some_and(|core| {
                core.is_draining()
                    && core.stream(stream).is_ok_and(|stream| {
                        stream.egress_complete() && stream.ingress_cancel_requested()
                    })
            }))
    }

    #[cfg(test)]
    pub(crate) async fn promote_h1_close_event_for_test(
        &self,
        stream: StreamId,
    ) -> Result<bool, HttpError> {
        let state = self.live_state()?;
        let work = {
            let mut driver = state.borrow_mut();
            let Some(position) = driver.egress_work.iter().position(|work| matches!(work, EgressWork::Transfer { stream: queued, .. } if *queued == stream)) else {
                return Ok(false);
            };
            driver
                .egress_work
                .remove(position)
                .expect("located transfer")
        };
        let EgressWork::Transfer {
            stream,
            mut transfer,
            cancellation,
            readiness,
        } = work
        else {
            unreachable!("only a transfer was selected");
        };
        let event = transfer.next_event_with_limit(64 << 10).await;
        let mut driver = state.borrow_mut();
        driver.egress_work.push_back(EgressWork::Transfer {
            stream,
            transfer,
            cancellation,
            readiness,
        });
        match event? {
            Some(event) => {
                driver
                    .egress_work
                    .push_front(EgressWork::Event { stream, event });
                Ok(true)
            }
            None => Ok(false),
        }
    }

    #[cfg(test)]
    pub fn transport_event_bridge_count_for_test(&self) -> Result<usize, HttpError> {
        if self.shared.completion.get() {
            return Err(session_finished());
        }
        Ok(self
            .state
            .upgrade()
            .ok_or_else(session_finished)?
            .borrow()
            .byte_event_bridges
            .len())
    }

    #[cfg(test)]
    pub(crate) fn buffered_egress_stats_for_test(
        &self,
        stream: StreamId,
    ) -> Result<BufferedEgressStatsSnapshot, HttpError> {
        let state = self.live_state()?;
        let state = state.borrow();
        let stream = state.core().stream(stream)?;
        Ok(BufferedEgressStatsSnapshot {
            buffered_bytes: stream.buffered_egress_bytes(),
            delta_capacity: stream.buffered_egress_stats_storage_for_test(),
            tracked_streams: state.app_buffered_egress.len(),
            content_length_mismatch: stream.egress_content_length_mismatch(),
        })
    }

    #[cfg(test)]
    pub fn fail_connection_while_core_detached_for_test(
        &self,
        error: HttpError,
    ) -> Result<(), HttpError> {
        let state = self.live_state()?;
        let core = DetachedSessionCore::take(&state);
        fail_connection(&state, &self.shared.write_wake, error, true);
        core.restore();
        flush_lifecycle_events(&state);
        Ok(())
    }

    pub fn reserve_request(&self) -> Result<RequestReservation, HttpError> {
        let state = self.admission_state()?;
        let mut reservation = state.borrow().core().reserve_request();
        if let Ok(reservation) = &mut reservation {
            let release_state = Rc::downgrade(&state);
            reservation.set_release_hook(Rc::new(move || {
                let Some(state) = release_state.upgrade() else {
                    return;
                };
                let core_available = state.try_borrow().is_ok_and(|driver| driver.core.is_some());
                if core_available {
                    flush_lifecycle_events(&state);
                }
            }));
        }
        flush_lifecycle_events(&state);
        reservation
    }

    pub fn set_max_concurrent_outgoing_streams(&self, maximum: u32) -> Result<bool, HttpError> {
        if maximum == 0 {
            return Ok(false);
        }
        let state = self.admission_state()?;
        let mut driver = state.borrow_mut();
        let was_available = driver.core().outgoing_capacity_available();
        let Some(http2) = driver.http2.as_mut() else {
            return Ok(false);
        };
        http2.config.max_concurrent_outgoing_streams = maximum;
        let effective = maximum.min(http2.peer_max_concurrent_streams);
        let updated = driver
            .core_mut()
            .set_max_concurrent_outgoing_streams(effective);
        debug_assert!(updated, "HTTP/2 core accepts a nonzero stream limit");
        let is_available = driver.core().outgoing_capacity_available();
        if was_available != is_available {
            driver.core().observe_lifecycle(if is_available {
                LifecycleEvent::Http2StreamsNotFull
            } else {
                LifecycleEvent::Http2StreamsFull
            });
        }
        drop(driver);
        flush_lifecycle_events(&state);
        Ok(true)
    }

    pub fn prepare_request(
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalSessionFuture<Result<PreparedOutgoingRequest, HttpError>> {
        SessionCore::prepare_request(request_source, reservation, cancellation)
    }

    /// Starts a request-header read through `gate`, as [`RequestHeadGate`]
    /// describes, without boxing the returned future.
    pub fn prepare_request_gated<G: RequestHeadGate>(
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
        gate: G,
    ) -> impl Future<Output = Result<PreparedOutgoingRequest, HttpError>> + 'static {
        SessionCore::prepare_request_gated(request_source, reservation, cancellation, gate)
    }

    pub fn prepare_request_with_headers(
        headers: HttpHeaderEvent,
        request_source: HttpSourceHolder,
        reservation: RequestReservation,
    ) -> PreparedOutgoingRequest {
        SessionCore::prepare_request_with_headers(headers, request_source, reservation)
    }

    pub fn send_prepared_request(
        &self,
        request: PreparedOutgoingRequest,
    ) -> Result<UniplexResponse, HttpError> {
        self.send_prepared_request_inner(request, None)
    }

    /// Commits a prepared request with cancellation retained through its upload.
    pub fn send_prepared_request_with_cancellation(
        &self,
        request: PreparedOutgoingRequest,
        cancellation: CancellationToken,
    ) -> Result<UniplexResponse, HttpError> {
        self.send_prepared_request_inner(request, Some(cancellation))
    }

    fn send_prepared_request_inner(
        &self,
        mut request: PreparedOutgoingRequest,
        cancellation: Option<CancellationToken>,
    ) -> Result<UniplexResponse, HttpError> {
        let state = self.admission_state()?;
        if state.borrow().codec.protocol() == CodecProtocol::Http2
            && request_has_forbidden_body(request.message_mut())
        {
            return Err(HttpError::with_message(
                HttpErrorCode::HEADER_PARSE_ERROR,
                b"RFC2616 request body is not allowed for this method",
            ));
        }
        let validation = state.borrow().core().validate_prepared_request(&request);
        validation?;
        let header_observer = if state.borrow().codec.protocol() == CodecProtocol::Http2 {
            Some(self.observer.upgrade().ok_or_else(session_finished)?)
        } else {
            None
        };
        let stream = state.borrow_mut().codec.create_stream();
        let mut core = DetachedSessionCore::take(&state);
        let outgoing = core.core_mut().send_prepared_request(request, stream);
        core.restore();
        let outgoing = match outgoing {
            Ok(outgoing) => outgoing,
            Err(error) => {
                flush_lifecycle_events(&state);
                return Err(error);
            }
        };
        let mut driver = state.borrow_mut();
        driver.note_flow_timeout_activity();
        let sequence_number = outgoing.sequence_number;
        if let Some(http2) = &mut driver.http2 {
            let priority = outgoing
                .headers
                .headers()
                .get_http_priority()
                .unwrap_or_default();
            http2.insert_stream(stream, priority);
            http2.record_transaction_opened();
        } else if let Some(http1) = &mut driver.http1_stats {
            http1.record_transaction_opened();
        }
        let timeout = match driver.codec.protocol() {
            CodecProtocol::Http2 => DeferredReadTimeout::DeferUpdates(driver.stream_read_timeout),
            CodecProtocol::Http11 if outgoing.headers.eom() => {
                DeferredReadTimeout::CancelOnUpdate(driver.stream_read_timeout)
            }
            _ => DeferredReadTimeout::ReplaceUpdates(driver.stream_read_timeout),
        };
        let producer = driver
            .core()
            .stream(stream)
            .expect("committed stream")
            .ingress_producer();
        producer.defer_read_timeout(timeout);
        if driver.http2.is_none() && driver.http1_stats.is_none() {
            // Only the read that resumes parsing reports credit, unless stats need every read.
            producer.set_resume_threshold(Some(driver.ingress_buffer_limit));
        }
        driver
            .egress_meta
            .entry(stream)
            .or_default()
            .response_timeout_pending = true;
        let source = outgoing.response_source;
        let exchange = driver
            .core()
            .stream(stream)
            .expect("committed stream")
            .exchange_tracker();
        let committed_headers = if let Some(observer) = header_observer {
            Some((outgoing.headers, observer))
        } else {
            driver.egress_work.push_back(EgressWork::Event {
                stream,
                event: EgressEvent::Headers(outgoing.headers),
            });
            None
        };
        if let Some(transfer) = outgoing.body {
            driver.enqueue_transfer(stream, transfer, cancellation);
        }
        drop(driver);
        if let Some((headers, observer)) = committed_headers {
            let mut first_panic = None;
            let mut serialization_error = None;
            capture_panic(&mut first_panic, || {
                serialization_error = serialize_event(
                    &state,
                    &observer,
                    &self.context,
                    SerializationOrigin::Committed(self),
                    stream,
                    EgressEvent::Headers(headers),
                )
                .err();
            });
            capture_panic(&mut first_panic, || flush_session_stats(&state));
            capture_panic(&mut first_panic, || flush_lifecycle_events(&state));
            capture_panic(&mut first_panic, || self.shared.write_wake.notify_one());
            if serialization_error.is_some() || first_panic.is_some() {
                let error = state
                    .borrow()
                    .fatal_error
                    .clone()
                    .or(serialization_error)
                    .unwrap_or_else(|| {
                        HttpError::with_message(
                            HttpErrorCode::INTERNAL_ERROR,
                            b"uniplex transport loop panicked",
                        )
                    });
                self.retain_committed_header_error(&state, &error);
                capture_panic(&mut first_panic, || {
                    fail_connection(&state, &self.shared.write_wake, error, true)
                });
                capture_panic(&mut first_panic, || {
                    self.shared.read_cancellation.request_cancellation();
                });
                capture_panic(&mut first_panic, || self.shared.write_wake.notify_one());
                if let Some(cleanup) = self.cleanup.upgrade() {
                    capture_panic(&mut first_panic, || wake_cleanup(&cleanup));
                }
            }
            resume_first_panic(first_panic);
        } else {
            self.shared.write_wake.notify_one();
            flush_session_stats(&state);
            flush_lifecycle_events(&state);
        }
        Ok(UniplexResponse {
            stream,
            sequence_number,
            source,
            completion: outgoing.completion,
            abort: Some(StreamAbort {
                state: Rc::downgrade(&state),
                shared: Rc::clone(&self.shared),
                stream,
            }),
            exchange,
        })
    }

    fn retain_committed_header_error(&self, state: &Rc<RefCell<DriverState>>, error: &HttpError) {
        state.borrow_mut().record_error(error.clone());
        self.shared.closing.set(true);
        if let Some(cleanup) = self.cleanup.upgrade() {
            cleanup.trigger.set(CleanupTrigger::Abort);
        }
    }

    /// Reserves capacity, then reads the request head and commits the request
    /// in one future.
    pub fn send_request(
        &self,
        request_source: HttpSourceHolder,
        cancellation: CancellationToken,
    ) -> LocalSessionFuture<Result<UniplexResponse, HttpError>> {
        let reservation = match self.reserve_request() {
            Ok(reservation) => reservation,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let body_cancellation = cancellation.clone();
        let preparation =
            SessionCore::prepare_request_unboxed(request_source, reservation, cancellation);
        let handle = self.clone();
        Box::pin(async move {
            let prepared = preparation.await?;
            handle.send_prepared_request_with_cancellation(prepared, body_cancellation)
        })
    }

    /// [`Self::send_request`] with the request head read through `gate`.
    pub fn send_request_gated<G: RequestHeadGate>(
        &self,
        request_source: HttpSourceHolder,
        cancellation: CancellationToken,
        gate: G,
    ) -> LocalSessionFuture<Result<UniplexResponse, HttpError>> {
        let reservation = match self.reserve_request() {
            Ok(reservation) => reservation,
            Err(error) => return Box::pin(std::future::ready(Err(error))),
        };
        let body_cancellation = cancellation.clone();
        let preparation =
            SessionCore::prepare_request_gated(request_source, reservation, cancellation, gate);
        let handle = self.clone();
        Box::pin(async move {
            let prepared = preparation.await?;
            handle.send_prepared_request_with_cancellation(prepared, body_cancellation)
        })
    }

    /// Queues an HTTP/2 PING request on this session.
    pub fn send_ping(&self) -> Result<u64, HttpError> {
        let data = NEXT_SESSION_PING_TOKEN.fetch_add(1, Ordering::Relaxed);
        let state = self.live_state()?;
        {
            let mut state = state.borrow_mut();
            if state.codec.protocol() != CodecProtocol::Http2 {
                return Err(invalid_transition(
                    b"PING requests require an HTTP/2 session",
                ));
            }
            let mut callback = NoopCodecCallback;
            let DriverState { codec, egress, .. } = &mut *state;
            codec.generate(
                egress,
                EgressFrame::PingRequest { data: Some(data) },
                &mut callback,
            )?;
            state.note_flow_timeout_activity();
        }
        self.shared.write_wake.notify_one();
        Ok(data)
    }

    #[cfg(test)]
    pub(crate) fn observe_h2_source_timers_for_test(
        &self,
        stream: StreamId,
    ) -> Result<H2SourceTimerTestControl, HttpError> {
        let state = self.live_state()?;
        let mut state = state.borrow_mut();
        if state.http2.is_none() || state.h2_source_timer_probe.is_some() {
            return Err(invalid_transition(
                b"source timer observation requires an unobserved H2 session",
            ));
        }
        let control = H2SourceTimerTestControl {
            snapshot: Rc::new(RefCell::new(None)),
            capture_next_arm: Rc::new(Cell::new(false)),
            first_arm: Rc::new(RefCell::new(None)),
            changed: Rc::new(tokio::sync::Notify::new()),
            pause_next_source_yield: Rc::new(Cell::new(false)),
            pause_next_source_service: Rc::new(Cell::new(false)),
            source_yield_paused: Rc::new(Cell::new(false)),
            resume_source_yield: Rc::new(tokio::sync::Notify::new()),
            pause_next_writer_wait: Rc::new(Cell::new(false)),
            writer_wait_paused: Rc::new(Cell::new(false)),
            resume_writer_wait: Rc::new(tokio::sync::Notify::new()),
        };
        state.h2_source_timer_probe = Some(H2SourceTimerProbe {
            stream,
            control: control.clone(),
        });
        Ok(control)
    }

    pub fn initiate_drain(&self) -> Result<bool, HttpError> {
        if self.shared.closing.get() || self.shared.completion.get() {
            return Ok(false);
        }
        self.initiate_drain_with(DrainTrigger::Local)
    }

    /// Stops accepting work and closes as soon as the current transactions detach.
    pub fn close_when_idle(&self) -> Result<bool, HttpError> {
        self.initiate_drain_with(DrainTrigger::CloseWhenIdle)
    }

    fn initiate_drain_with(&self, trigger: DrainTrigger) -> Result<bool, HttpError> {
        let state = self.live_state()?;
        {
            let mut driver = state.borrow_mut();
            if driver.core.is_none() {
                let close_http1 = trigger == DrainTrigger::CloseWhenIdle
                    && driver.codec.protocol() == CodecProtocol::Http11
                    && driver.codec.is_reusable();
                let changed = !driver.deferred_drain || close_http1;
                if close_http1 {
                    driver.codec.generate_immediate_goaway();
                }
                driver.deferred_drain = true;
                driver.note_flow_timeout_activity();
                self.shared.write_wake.notify_one();
                self.shared.read_wake.notify_one();
                return Ok(changed);
            }
        }
        let DrainTransition {
            changed,
            error,
            panic,
        } = initiate_drain_after_state_borrow(&state, trigger);
        let mut first_panic = panic;
        if changed {
            let cancel_read = {
                let state = state.borrow();
                let waiting_for_final_goaway = state.http2.as_ref().is_some_and(|http2| {
                    state.core().direction().is_downstream()
                        && http2.sent_initial_goaway
                        && !http2.sent_final_goaway
                });
                let waiting_for_http1_response = state.codec.protocol() == CodecProtocol::Http11
                    && state.codec.is_waiting_to_drain();
                state.core().num_transactions() == 0
                    && !waiting_for_final_goaway
                    && !waiting_for_http1_response
            };
            if cancel_read {
                capture_panic(&mut first_panic, || {
                    self.shared.read_cancellation.request_cancellation();
                });
            }
            capture_panic(&mut first_panic, || self.shared.write_wake.notify_one());
            capture_panic(&mut first_panic, || self.shared.read_wake.notify_one());
        }
        resume_first_panic(first_panic);
        if let Some(error) = error {
            return Err(error);
        }
        Ok(changed)
    }

    pub fn deliver_socket_byte_event(
        &self,
        event_type: SocketByteEventType,
        offset: u64,
    ) -> Result<usize, HttpError> {
        if self.shared.completion.get() || self.state.upgrade().is_none() {
            return Err(session_finished());
        }
        self.observer
            .upgrade()
            .ok_or_else(session_finished)?
            .socket_byte_event(&self.context, event_type, offset)
    }
}

/// A worker-local HTTP/1.1 or HTTP/2 session over one byte stream.
///
/// It owns the codec, the session core and the transport until both transport
/// loops and all stream work have completed.
pub struct UniplexSession {
    state: Rc<RefCell<DriverState>>,
    transport: Weak<dyn UniplexTransport>,
    observer: Weak<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    shared: Rc<UniplexShared>,
    cleanup: Rc<RunCleanupControl>,
    cleanup_registration: Option<WorkerCleanupTaskRegistration>,
    started: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CleanupTrigger {
    Pending,
    Running,
    Abort,
}

struct RunCleanupControl {
    trigger: Cell<CleanupTrigger>,
    abort_started: Cell<bool>,
    waker: RefCell<Option<std::task::Waker>>,
    result: RefCell<(Option<HttpError>, Option<HttpError>)>,
    summary: Cell<Option<UniplexRunSummary>>,
    panic: RefCell<Option<Box<dyn std::any::Any + Send>>>,
    /// The owner that `start` hands to the run task, until the task takes it.
    owner: RefCell<Option<Box<dyn UniplexRunOwner>>>,
}

impl RunCleanupControl {
    /// The outcome of a completed run, taken once.
    fn take_outcome(&self) -> Result<UniplexRunSummary, HttpError> {
        let summary = self.summary.get().unwrap_or_default();
        let (pending_error, finalize_error) = std::mem::take(&mut *self.result.borrow_mut());
        match pending_error.or(finalize_error) {
            Some(error) => Err(error),
            None => Ok(summary),
        }
    }
}

struct CleanupStartWait {
    cleanup: Rc<RunCleanupControl>,
    worker: WorkerHandle,
}

impl Future for CleanupStartWait {
    type Output = (CleanupTrigger, bool);

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let trigger = self.cleanup.trigger.get();
        if trigger != CleanupTrigger::Pending || self.worker.is_cancelled() {
            return Poll::Ready((trigger, self.worker.is_cancelled()));
        }
        let mut waker = self.cleanup.waker.borrow_mut();
        if waker
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(context.waker()))
        {
            *waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

struct RunCompletion {
    cleanup: Rc<RunCleanupControl>,
    state: Weak<RefCell<DriverState>>,
    transport: Weak<dyn UniplexTransport>,
    observer: Weak<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    shared: Rc<UniplexShared>,
    registration: Option<WorkerCleanupTaskRegistration>,
    started: bool,
    armed: bool,
}

impl Future for RunCompletion {
    type Output = Result<UniplexRunSummary, HttpError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.shared.completion.get() {
            self.armed = false;
            return Poll::Ready(self.cleanup.take_outcome());
        }
        if !self.started {
            if self.cleanup.trigger.get() == CleanupTrigger::Pending {
                self.cleanup.trigger.set(CleanupTrigger::Running);
            }
            if let Some(registration) = self.registration.take() {
                if let Ok(task) = registration.activate() {
                    task.detach();
                }
            }
            self.started = true;
        }
        let mut waker = self.cleanup.waker.borrow_mut();
        if waker
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(context.waker()))
        {
            *waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for RunCompletion {
    fn drop(&mut self) {
        self.abort_unfinished();
    }
}

impl RunCompletion {
    /// Starts abort cleanup, as a dropped run future does, unless the run
    /// completed or this was never armed.
    fn abort_unfinished(&mut self) {
        if !self.armed || self.shared.completion.get() {
            return;
        }
        self.armed = false;
        self.shared.closing.set(true);
        self.cleanup.trigger.set(CleanupTrigger::Abort);
        if let (Some(state), Some(transport), Some(observer)) = (
            self.state.upgrade(),
            self.transport.upgrade(),
            self.observer.upgrade(),
        ) {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                request_abort_cleanup(
                    &self.cleanup,
                    &state,
                    &transport,
                    &observer,
                    &self.context,
                    &self.shared.read_cancellation,
                    &self.shared.write_cancellation,
                    &self.shared.write_wake,
                );
            })) {
                record_cleanup_panic(&self.cleanup, payload);
            }
        }
    }
}

fn wake_cleanup(cleanup: &RunCleanupControl) {
    if let Some(waker) = cleanup.waker.borrow_mut().take() {
        waker.wake();
    }
}

#[cold]
#[inline(never)]
fn record_cleanup_panic(cleanup: &RunCleanupControl, payload: Box<dyn std::any::Any + Send>) {
    let mut first = cleanup.panic.borrow_mut();
    if first.is_none() {
        *first = Some(payload);
    }
}

/// Polls the future in `slot` to completion and then drops it in place, each
/// under its own unwind boundary. The caller owns the slot's storage.
async fn catch_future_unwind<F: Future>(
    mut slot: Pin<&mut Option<F>>,
) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let result = poll_fn(|context| {
        catch_unwind(AssertUnwindSafe(|| {
            slot.as_mut()
                .as_pin_mut()
                .expect("polled after completion")
                .poll(context)
        }))
        .map_or_else(|payload| Poll::Ready(Err(payload)), |poll| poll.map(Ok))
    })
    .await;
    let dropped = catch_unwind(AssertUnwindSafe(|| slot.set(None)));
    match (result, dropped) {
        (Err(payload), _) => Err(payload),
        (Ok(output), Ok(())) => Ok(output),
        (Ok(_), Err(payload)) => Err(payload),
    }
}

/// The waker that a started session's run task gives its owner. A wake marks
/// a stop request pending and wakes the run task, which then polls the owner.
/// The run's own wakes, such as a completed read, do not poll the owner.
struct OwnerWake {
    pending: AtomicBool,
    task: Mutex<Option<Waker>>,
}

impl Wake for OwnerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.pending.store(true, Ordering::Release);
        let task = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(task) = task {
            task.wake();
        }
    }
}

impl OwnerWake {
    fn set_task(&self, task: Option<Waker>) {
        let previous = std::mem::replace(
            &mut *self
                .task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            task,
        );
        drop(previous);
    }
}

/// The owner of a started session, as its run task holds it.
enum RunOwnerSlot {
    /// `start` has not handed over an owner, or the session never started.
    Unclaimed,
    Serving(Box<dyn UniplexRunOwner>),
    Released,
}

/// The run task of a session built by [`UniplexSession::new_owned`]: the
/// session's cleanup task plus the owner that [`UniplexSession::start`] hands
/// it, in place of a second task that awaits the run for the owner.
///
/// `release` arms when the owner arrives. Dropping this future before the run
/// completes then starts abort cleanup, as dropping a `run` future does.
async fn owned_run(cleanup: impl Future<Output = ()>, release: RunCompletion) {
    let mut owner = RunOwnerSlot::Unclaimed;
    let mut cleanup = pin!(Some(cleanup));
    let mut release = release;
    let mut first_panic = None;
    let owner_wake = Arc::new(OwnerWake {
        pending: AtomicBool::new(true),
        task: Mutex::new(None),
    });
    let owner_waker = Waker::from(Arc::clone(&owner_wake));
    let mut task_waker: Option<Waker> = None;
    poll_fn(|context| {
        if matches!(owner, RunOwnerSlot::Unclaimed)
            && let Some(claimed) = release.cleanup.owner.borrow_mut().take()
        {
            owner = RunOwnerSlot::Serving(claimed);
            release.armed = true;
        }
        if let RunOwnerSlot::Serving(serving) = &mut owner {
            if !task_waker
                .as_ref()
                .is_some_and(|registered| registered.will_wake(context.waker()))
            {
                let waker = context.waker().clone();
                owner_wake.set_task(Some(waker.clone()));
                task_waker = Some(waker);
            }
            if owner_wake.pending.load(Ordering::Relaxed)
                && owner_wake.pending.swap(false, Ordering::Acquire)
                && let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    serving.poll_stop(&mut Context::from_waker(&owner_waker))
                }))
            {
                release_panicked_owner(&mut owner, &mut release, &mut first_panic, payload);
            }
        }
        let polled = catch_unwind(AssertUnwindSafe(|| {
            cleanup
                .as_mut()
                .as_pin_mut()
                .expect("polled after completion")
                .poll(context)
        }));
        match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(())) => {}
            Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
        }
        capture_panic(&mut first_panic, || cleanup.set(None));
        if release.shared.completion.get()
            && let RunOwnerSlot::Serving(serving) = &mut owner
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                serving.poll_stop(&mut Context::from_waker(&owner_waker))
            }))
        {
            release_panicked_owner(&mut owner, &mut release, &mut first_panic, payload);
        }
        Poll::Ready(())
    })
    .await;
    owner_wake.set_task(None);
    if let RunOwnerSlot::Serving(serving) = std::mem::replace(&mut owner, RunOwnerSlot::Released) {
        if release.shared.completion.get() {
            let outcome = release.cleanup.take_outcome();
            capture_panic(&mut first_panic, || serving.finish(outcome));
        } else {
            capture_panic(&mut first_panic, || drop(serving));
        }
    }
    resume_first_panic(first_panic);
}

/// Contains a panic of a started session's owner the way a panic of a task
/// awaiting `run` is contained: worker shutdown begins, the run aborts, the
/// owner drops without an outcome, and the run task resumes the panic last.
#[cold]
#[inline(never)]
fn release_panicked_owner(
    owner: &mut RunOwnerSlot,
    release: &mut RunCompletion,
    first_panic: &mut Option<PanicPayload>,
    payload: PanicPayload,
) {
    retain_first_panic(first_panic, Some(payload));
    capture_panic(first_panic, || {
        release.context.handle().begin_shutdown();
    });
    capture_panic(first_panic, || release.abort_unfinished());
    let released = std::mem::replace(owner, RunOwnerSlot::Released);
    capture_panic(first_panic, || drop(released));
}

fn reset_transport_once(
    state: &Rc<RefCell<DriverState>>,
    transport: &Rc<dyn UniplexTransport>,
    context: &WorkerContext,
) -> Result<(), HttpError> {
    if std::mem::replace(&mut state.borrow_mut().transport_reset_issued, true) {
        return Ok(());
    }
    transport.close_with_reset(context)
}

fn request_abort_cleanup(
    cleanup: &RunCleanupControl,
    state: &Rc<RefCell<DriverState>>,
    transport: &Rc<dyn UniplexTransport>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    read_cancellation: &CancellationSource,
    write_cancellation: &CancellationSource,
    write_wake: &tokio::sync::Notify,
) {
    cleanup.trigger.set(CleanupTrigger::Abort);
    if cleanup.abort_started.replace(true) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| wake_cleanup(cleanup))) {
            record_cleanup_panic(cleanup, payload);
        }
        return;
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        read_cancellation.request_cancellation()
    })) {
        record_cleanup_panic(cleanup, payload);
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        write_cancellation.request_cancellation()
    })) {
        record_cleanup_panic(cleanup, payload);
    }
    let error = HttpError::with_message(
        HttpErrorCode::CORO_CANCELLED,
        b"uniplex run future was cancelled",
    );
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        fail_connection(state, write_wake, error, true)
    })) {
        record_cleanup_panic(cleanup, payload);
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
        cancel_pending_byte_events(state, observer, context)
    })) {
        record_cleanup_panic(cleanup, payload);
    }
    match catch_unwind(AssertUnwindSafe(|| {
        reset_transport_once(state, transport, context)
    })) {
        Ok(Err(close_error)) => state.borrow_mut().record_error(close_error),
        Ok(Ok(())) => {}
        Err(payload) => record_cleanup_panic(cleanup, payload),
    }
    let (discarded_work, pending_tx_ack) = {
        let mut state = state.borrow_mut();
        (state.take_egress_work(), state.pending_tx_ack.take())
    };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop_items_individually(discarded_work)))
    {
        record_cleanup_panic(cleanup, payload);
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(pending_tx_ack))) {
        record_cleanup_panic(cleanup, payload);
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| wake_cleanup(cleanup))) {
        record_cleanup_panic(cleanup, payload);
    }
}

impl Drop for UniplexSession {
    fn drop(&mut self) {
        if self.started || self.shared.completion.get() || self.state.borrow().core.is_none() {
            return;
        }
        self.shared.closing.set(true);
        self.cleanup.trigger.set(CleanupTrigger::Abort);
        if let (Some(transport), Some(observer)) =
            (self.transport.upgrade(), self.observer.upgrade())
        {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                request_abort_cleanup(
                    &self.cleanup,
                    &self.state,
                    &transport,
                    &observer,
                    &self.context,
                    &self.shared.read_cancellation,
                    &self.shared.write_cancellation,
                    &self.shared.write_wake,
                );
            })) {
                record_cleanup_panic(&self.cleanup, payload);
            }
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| wake_cleanup(&self.cleanup))) {
            record_cleanup_panic(&self.cleanup, payload);
        }
    }
}

impl UniplexSession {
    pub fn set_byte_event_timeout(&self, timeout: Duration) -> Result<(), HttpError> {
        self.observer
            .upgrade()
            .ok_or_else(session_finished)?
            .set_byte_event_timeout(&self.context, timeout)
    }

    pub fn add_lifecycle_observer(
        &self,
        observer: Rc<dyn LifecycleObserver>,
    ) -> Result<LifecycleObserverId, HttpError> {
        let lifecycle = Rc::clone(&self.state.borrow().lifecycle);
        Ok(lifecycle.add_observer(observer))
    }

    #[cfg(test)]
    pub(crate) fn trigger_batched_stream_error_drain_for_test(
        &self,
        handle: &UniplexHandle,
        stream: StreamId,
        detached_stream: StreamId,
    ) -> bool {
        let drain_trigger = {
            let mut state = self.state.borrow_mut();
            state.ingress_meta.insert(
                stream,
                Box::new(IngressMeta {
                    complete: false,
                    discard: false,
                    message: None,
                    expected_content_length: None,
                    received_body_length: 0,
                    request_get: false,
                    get_body_recorded: false,
                }),
            );
            state.ingress_meta.insert(
                detached_stream,
                Box::new(IngressMeta {
                    complete: true,
                    discard: false,
                    message: None,
                    expected_content_length: None,
                    received_body_length: 0,
                    request_get: false,
                    get_body_recorded: false,
                }),
            );
            apply_stream_updates_locked(
                &mut state,
                [
                    StreamUpdate {
                        stream,
                        action: StreamAction::Reset {
                            code: HttpErrorCode::INTERNAL_ERROR,
                            origin: ResetOrigin::Source,
                        },
                        detached: false,
                        cancel_egress: false,
                        cancel_egress_error: None,
                        replacement: None,
                    },
                    StreamUpdate {
                        stream: detached_stream,
                        action: StreamAction::None,
                        detached: true,
                        cancel_egress: false,
                        cancel_egress_error: None,
                        replacement: None,
                    },
                ],
            )
        };
        let mut first_panic = None;
        if let Some(trigger) = drain_trigger {
            retain_first_panic(
                &mut first_panic,
                initiate_drain_after_state_borrow(&self.state, trigger).panic,
            );
        }
        capture_panic(&mut first_panic, || drop_deferred_work(&self.state));
        capture_panic(&mut first_panic, || flush_lifecycle_events(&self.state));
        handle.shared.read_wake.notify_one();
        let detached = !self
            .state
            .borrow()
            .ingress_meta
            .contains_key(&detached_stream);
        resume_first_panic(first_panic);
        detached
    }

    #[cfg(test)]
    pub(crate) fn trigger_deferred_drain_for_test(
        &self,
        handle: &UniplexHandle,
    ) -> Result<(), HttpError> {
        self.state.borrow_mut().deferred_drain = true;
        drive_core_callbacks(&self.state, &handle.shared.read_wake)
    }

    #[cfg(test)]
    pub(crate) fn has_ingress_meta_for_test(&self, stream: StreamId) -> bool {
        self.state.borrow().ingress_meta.contains_key(&stream)
    }

    #[cfg(test)]
    pub(crate) fn reset_after_draining_writes_for_test(&self) -> bool {
        self.state.borrow().reset_after_draining_writes
    }

    #[cfg(test)]
    pub(crate) fn trigger_ingress_and_egress_lifecycle_for_test(
        &self,
        handle: &UniplexHandle,
        stream: StreamId,
    ) -> Result<(), HttpError> {
        let request = self
            .state
            .borrow_mut()
            .core_mut()
            .begin_incoming_stream(stream)?;
        flush_lifecycle_events(&self.state);
        let (_, update) = self
            .state
            .borrow_mut()
            .core_mut()
            .response_headers(stream, true, true)?;
        apply_stream_update(&self.state, update, &handle.shared.read_wake);
        drop(request);
        Ok(())
    }

    /// Constructs an HTTP/1 session on the current worker.
    pub fn new(
        context: WorkerContext,
        direction: TransportDirection,
        transport: Rc<dyn UniplexTransport>,
        handler: Option<HttpHandlerRef>,
        config: UniplexConfig,
    ) -> Result<(Self, UniplexHandle), HttpError> {
        Self::new_for_protocol(
            context, direction, transport, handler, config, None, None, false,
        )
    }

    /// Constructs an HTTP/1 session with protocol-neutral statistics callbacks.
    pub fn new_with_stats(
        context: WorkerContext,
        direction: TransportDirection,
        transport: Rc<dyn UniplexTransport>,
        handler: Option<HttpHandlerRef>,
        config: UniplexConfig,
        stats: Rc<dyn Http2SessionStats>,
    ) -> Result<(Self, UniplexHandle), HttpError> {
        Self::new_for_protocol(
            context,
            direction,
            transport,
            handler,
            config,
            None,
            Some(stats),
            false,
        )
    }

    /// Constructs a multiplexed HTTP/2 session on the current worker.
    pub fn new_http2(
        context: WorkerContext,
        direction: TransportDirection,
        transport: Rc<dyn UniplexTransport>,
        handler: Option<HttpHandlerRef>,
        config: UniplexConfig,
        http2: Http2SessionConfig,
    ) -> Result<(Self, UniplexHandle), HttpError> {
        Self::new_for_protocol(
            context,
            direction,
            transport,
            handler,
            config,
            Some(http2),
            None,
            false,
        )
    }

    /// Constructs a session that [`start`](Self::start) runs for an owner.
    ///
    /// Its run task is an admitted scope, so graceful worker shutdown waits for
    /// it, as it does for the scope that awaits [`run`](Self::run).
    pub fn new_owned(
        context: WorkerContext,
        direction: TransportDirection,
        transport: Rc<dyn UniplexTransport>,
        handler: Option<HttpHandlerRef>,
        config: UniplexConfig,
        protocol: UniplexProtocolConfig,
    ) -> Result<(Self, UniplexHandle), HttpError> {
        let (http2, http1_stats) = match protocol {
            UniplexProtocolConfig::Http1 { stats } => (None, stats),
            UniplexProtocolConfig::Http2(http2) => (Some(http2), None),
        };
        Self::new_for_protocol(
            context,
            direction,
            transport,
            handler,
            config,
            http2,
            http1_stats,
            true,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the public constructors differ only in protocol and run hosting"
    )]
    fn new_for_protocol(
        context: WorkerContext,
        direction: TransportDirection,
        transport: Rc<dyn UniplexTransport>,
        handler: Option<HttpHandlerRef>,
        config: UniplexConfig,
        mut http2_config: Option<Http2SessionConfig>,
        http1_stats: Option<Rc<dyn Http2SessionStats>>,
        owned: bool,
    ) -> Result<(Self, UniplexHandle), HttpError> {
        let config = config.validate()?;
        if let Some(http2) = http2_config.as_mut() {
            if http2.max_concurrent_outgoing_streams == 0 {
                return Err(invalid_transition(
                    b"configured HTTP/2 outgoing stream limit must be nonzero",
                ));
            }
            if http2.stream_receive_window == 0 || http2.stream_receive_window > i32::MAX as u32 {
                return Err(invalid_transition(
                    b"configured HTTP/2 receive window is not representable",
                ));
            }
            if http2
                .ping_probes
                .is_some_and(|probes| !probes.interval.is_zero() && probes.timeout.is_zero())
            {
                return Err(invalid_transition(
                    b"HTTP/2 PING probes require a nonzero timeout",
                ));
            }
            let advertised = http2
                .settings
                .iter()
                .rev()
                .find(|setting| setting.identifier == SettingsId::InitialWindowSize.as_u16())
                .map(|setting| setting.value);
            match (http2.stream_receive_window_mode, advertised) {
                (StreamReceiveWindowMode::MirrorAdvertisedSettings, Some(advertised)) => {
                    let advertised = u32::try_from(advertised).map_err(|_| {
                        invalid_transition(b"HTTP/2 initial stream window exceeds u32")
                    })?;
                    if advertised == 0 || advertised > i32::MAX as u32 {
                        return Err(invalid_transition(
                            b"HTTP/2 initial stream window is not representable",
                        ));
                    }
                    if http2.stream_receive_window != DEFAULT_STREAM_WINDOW
                        && http2.stream_receive_window != advertised
                    {
                        return Err(invalid_transition(
                            b"HTTP/2 stream receive window conflicts with advertised SETTINGS",
                        ));
                    }
                    http2.stream_receive_window = advertised;
                }
                (_, None) if http2.stream_receive_window != DEFAULT_STREAM_WINDOW => {
                    http2.settings.push(CodecSetting {
                        identifier: SettingsId::InitialWindowSize.as_u16(),
                        value: u64::from(http2.stream_receive_window),
                    });
                }
                _ => {}
            }
        }
        if direction.is_downstream() != handler.is_some() {
            return Err(invalid_transition(
                b"downstream uniplex sessions require exactly one handler",
            ));
        }
        let observer = Rc::new(AsyncSocketByteEventObserver::new(&context)?);
        let peer_identity = transport
            .peer_identity(&context)?
            .map(|identity| Rc::<[u8]>::from(identity.as_bytes()));
        let tx_ack_enabled =
            transport.byte_event_capability(&context)? == ByteEventCapability::TransportEvents;
        observer.set_tx_ack_events_enabled(&context, tx_ack_enabled)?;
        let ping_prober = http2_config
            .as_ref()
            .and_then(|http2| http2.ping_probes)
            .and_then(|probes| PingProber::new(probes, context.host().clone()));
        let lifecycle = Rc::new(UniplexLifecycleDispatcher::new(ping_prober.clone()));
        let shared = Rc::new(UniplexShared {
            read_wake: tokio::sync::Notify::new(),
            write_wake: tokio::sync::Notify::new(),
            read_cancellation: CancellationSource::new(),
            write_cancellation: CancellationSource::new(),
            closing: Cell::new(false),
            completion: Cell::new(false),
        });
        let mut egress = BufQueue::new(config.buffer_limit);
        let (codec, connection_window_transition) = if let Some(http2) = &http2_config {
            let mut codec = Http2Codec::new(direction);
            if direction.is_upstream() {
                codec.egress_settings_mut().set(SettingsId::EnablePush, 1);
            }
            for setting in &http2.settings {
                if let Some(id) = SettingsId::from_u16(setting.identifier) {
                    codec.egress_settings_mut().set(id, setting.value);
                }
            }
            if http2.double_goaway_drain {
                codec.enable_double_goaway_drain();
            } else {
                codec.disable_double_goaway_drain();
            }
            codec.set_header_codec_stats(http2.header_codec_stats.clone());
            if let Some(strategy) = &http2.header_indexing_strategy {
                codec.set_header_indexing_strategy(Rc::clone(strategy));
            }
            codec.set_exact_methods(config.exact_method_tokens && direction.is_downstream());
            codec.set_verbatim_field_values(config.verbatim_http2_header_values);
            codec.set_add_date_header_to_response(config.add_date_header_to_response);
            let mut callback = NoopCodecCallback;
            codec.generate(&mut egress, EgressFrame::ConnectionPreface, &mut callback)?;
            codec.generate_configured_settings(&mut egress, &mut callback)?;
            let connection_window_transition = Rc::new(Cell::new(None));
            let notification = Rc::new(ConnectionWindowWake {
                shared: Rc::clone(&shared),
                transition: Rc::clone(&connection_window_transition),
            });
            let codec = DriverCodec::Http2(FlowControlledCodec::new(
                Box::new(codec),
                notification,
                &mut egress,
                &mut callback,
                http2.connection_receive_window,
            )?);
            (codec, Some(connection_window_transition))
        } else {
            let mut codec = Http1xCodec::new(direction);
            codec.set_unframed_bodiless_requests(config.unframed_bodiless_http1_requests);
            codec.set_add_date_header_to_response(config.add_date_header_to_response);
            (DriverCodec::Http1(codec), None)
        };
        let http2_state = http2_config.clone().map(|config| {
            let mut http2 = Box::new(Http2DriverState::new(config, context.host().clone()));
            http2.ping_prober = ping_prober;
            http2
        });
        let state = Rc::new(RefCell::new(DriverState {
            core: None,
            lifecycle: Rc::clone(&lifecycle),
            codec,
            http2: http2_state,
            http1_stats: http1_stats.map(|stats| Http1StatsState {
                stats,
                pending: Vec::new(),
                transactions_opened: 0,
                finalized: false,
            }),
            handler,
            ingress_meta: SmallMap::new(),
            pending_pushes: BTreeMap::new(),
            connection_window_transition,
            egress_work: VecDeque::new(),
            ready_dispatch: *READY_DISPATCH,
            flow_timeout_activity: false,
            deferred_drops: Vec::new(),
            deferred_push_drops: Vec::new(),
            ingress: BufQueue::new(config.buffer_limit),
            egress,
            egress_meta: SmallMap::new(),
            app_buffered_egress: BTreeMap::new(),
            app_content_length_mismatches: BTreeSet::new(),
            pending_tx_ack: None,
            byte_event_bridges: Vec::new(),
            stream_read_timeout: config.stream_read_timeout,
            ingress_buffer_limit: config.ingress_buffer_limit,
            http1_ingress_body_ceiling: config.http1_ingress_body_ceiling(),
            wait_for_http1_response_on_drain: config.wait_for_http1_response_on_drain,
            abort_exchanges_on_eof: config.abort_exchanges_on_eof,
            flow_control_paused: false,
            anti_pipeline_paused: false,
            upgrade_paused: false,
            close_after_ingress: false,
            eof_watch: false,
            ingress_close_stream: None,
            close_egress_boundary: None,
            upgrade_stream_awaiting_detach: None,
            upstream_ingress_upgraded: false,
            zero_byte_write_pending: false,
            batched_source_wake: None,
            bytes_read: 0,
            bytes_scheduled: 0,
            bytes_written: 0,
            reads_closed: false,
            writes_finished: false,
            reset_after_draining_writes: false,
            transport_reset_issued: false,
            reset_after_error_response: None,
            deferred_drain: false,
            deferred_connection_error: None,
            deferred_connection_work: VecDeque::new(),
            deferred_stream_work: Vec::new(),
            byte_event_cancel_error: None,
            stream_byte_event_cancellations: Vec::new(),
            fatal_error: None,
            #[cfg(test)]
            h2_timeout_probe: None,
            #[cfg(test)]
            h2_source_timer_probe: None,
            #[cfg(test)]
            writer_wait_hooks: WriterWaitHooks::default(),
            #[cfg(any(test, feature = "test-hooks"))]
            writer_hold: None,
        }));
        let cleanup = Rc::new(RunCleanupControl {
            trigger: Cell::new(CleanupTrigger::Pending),
            abort_started: Cell::new(false),
            waker: RefCell::new(None),
            result: RefCell::new((None, None)),
            summary: Cell::new(None),
            panic: RefCell::new(None),
            owner: RefCell::new(None),
        });
        let cleanup_state = Rc::clone(&state);
        let cleanup_transport = Rc::clone(&transport);
        let cleanup_observer = Rc::clone(&observer);
        let cleanup_context = context.clone();
        let cleanup_shared = Rc::clone(&shared);
        let cleanup_control = Rc::clone(&cleanup);
        let cleanup_worker = context.handle().clone();
        let cleanup_task = async move {
            let (trigger, worker_cancelled) = CleanupStartWait {
                cleanup: Rc::clone(&cleanup_control),
                worker: cleanup_worker.clone(),
            }
            .await;
            let mut run_result = None;
            if trigger == CleanupTrigger::Running && !worker_cancelled {
                let mut loops = Box::pin(run_transport_loops(
                    Rc::clone(&cleanup_state),
                    Rc::clone(&cleanup_transport),
                    Rc::clone(&cleanup_observer),
                    cleanup_context.clone(),
                    config,
                    Rc::clone(&cleanup_shared),
                ));
                run_result = poll_fn(|task_context| {
                    if cleanup_worker.is_cancelled()
                        || cleanup_control.trigger.get() == CleanupTrigger::Abort
                    {
                        return Poll::Ready(None);
                    }
                    match catch_unwind(AssertUnwindSafe(|| loops.as_mut().poll(task_context))) {
                        Ok(poll) => poll.map(Some),
                        Err(payload) => {
                            record_cleanup_panic(&cleanup_control, payload);
                            Poll::Ready(None)
                        }
                    }
                })
                .await;
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(loops))) {
                    record_cleanup_panic(&cleanup_control, payload);
                }
            }
            let abort = trigger == CleanupTrigger::Abort
                || (trigger == CleanupTrigger::Pending && worker_cancelled)
                || cleanup_worker.is_cancelled()
                || cleanup_control.trigger.get() == CleanupTrigger::Abort
                || run_result.is_none();
            if abort {
                cleanup_shared.closing.set(true);
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    request_abort_cleanup(
                        &cleanup_control,
                        &cleanup_state,
                        &cleanup_transport,
                        &cleanup_observer,
                        &cleanup_context,
                        &cleanup_shared.read_cancellation,
                        &cleanup_shared.write_cancellation,
                        &cleanup_shared.write_wake,
                    );
                })) {
                    record_cleanup_panic(&cleanup_control, payload);
                }
            }
            let summary = {
                let state = cleanup_state.borrow();
                UniplexRunSummary {
                    bytes_read: state.bytes_read,
                    bytes_scheduled: state.bytes_scheduled,
                    bytes_written: state.bytes_written,
                    reset: state.reset_after_draining_writes,
                }
            };
            cleanup_control.summary.set(Some(summary));
            let mut run_error = match run_result {
                Some(Err(error)) => Some(error),
                Some(Ok(())) | None => cleanup_state.borrow_mut().fatal_error.take(),
            };
            if run_error.is_none() && cleanup_control.panic.borrow().is_some() {
                run_error = Some(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"uniplex transport loop panicked",
                ));
            }
            cleanup_shared.closing.set(true);
            let pending_push_error =
                cleanup_state
                    .borrow()
                    .fatal_error
                    .clone()
                    .unwrap_or_else(|| {
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"session cleanup cancelled an incomplete push promise",
                        )
                    });
            match catch_unwind(AssertUnwindSafe(|| {
                cancel_pending_pushes(
                    &cleanup_state,
                    &cleanup_shared.read_wake,
                    &pending_push_error,
                )
            })) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => cleanup_state.borrow_mut().record_error(error),
                Err(payload) => record_cleanup_panic(&cleanup_control, payload),
            }
            if let Err(payload) = catch_future_unwind(pin!(Some(wait_for_all_streams(
                &cleanup_state,
                &cleanup_shared.read_wake,
                &cleanup_observer,
                &cleanup_context,
            ))))
            .await
            {
                record_cleanup_panic(&cleanup_control, payload);
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                cancel_pending_byte_events(&cleanup_state, &cleanup_observer, &cleanup_context)
            })) {
                record_cleanup_panic(&cleanup_control, payload);
            }
            match catch_unwind(AssertUnwindSafe(|| {
                cleanup_observer.zero_refs(&cleanup_context)
            })) {
                Ok(Ok(wait)) => {
                    if let Err(payload) = catch_future_unwind(pin!(Some(wait))).await {
                        record_cleanup_panic(&cleanup_control, payload);
                    }
                }
                Ok(Err(error)) => cleanup_state.borrow_mut().record_error(error),
                Err(payload) => record_cleanup_panic(&cleanup_control, payload),
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                sync_buffered_egress_stats(&cleanup_state)
            })) {
                record_cleanup_panic(&cleanup_control, payload);
            }
            {
                let mut state = cleanup_state.borrow_mut();
                state.egress = BufQueue::new(0);
                if let Some(http2) = state.http2.as_mut() {
                    http2.finalize_stats();
                } else if let Some(http1) = state.http1_stats.as_mut() {
                    http1.finalize();
                }
            }
            if let Err(payload) =
                catch_unwind(AssertUnwindSafe(|| flush_session_stats(&cleanup_state)))
            {
                record_cleanup_panic(&cleanup_control, payload);
            }
            let (
                discarded_work,
                deferred_work,
                pending_pushes,
                deferred_push_drops,
                pending_tx_ack,
                handler,
                bridges,
            ) = {
                let mut state = cleanup_state.borrow_mut();
                (
                    state.take_egress_work(),
                    std::mem::take(&mut state.deferred_drops),
                    std::mem::take(&mut state.pending_pushes),
                    std::mem::take(&mut state.deferred_push_drops),
                    state.pending_tx_ack.take(),
                    state.handler.take(),
                    std::mem::take(&mut state.byte_event_bridges),
                )
            };
            for dropped in [
                catch_unwind(AssertUnwindSafe(|| drop_items_individually(discarded_work))),
                catch_unwind(AssertUnwindSafe(|| drop_items_individually(deferred_work))),
                catch_unwind(AssertUnwindSafe(|| {
                    drop_items_individually(
                        pending_pushes.into_values().map(|pending| pending.source),
                    )
                })),
                catch_unwind(AssertUnwindSafe(|| {
                    drop_items_individually(deferred_push_drops)
                })),
                catch_unwind(AssertUnwindSafe(|| drop(pending_tx_ack))),
                catch_unwind(AssertUnwindSafe(|| drop(handler))),
                catch_unwind(AssertUnwindSafe(|| drop(bridges))),
            ] {
                if let Err(payload) = dropped {
                    record_cleanup_panic(&cleanup_control, payload);
                }
            }
            let exchange_error = match catch_future_unwind(pin!(Some(finish_exchange_cleanup(
                &cleanup_state,
                &cleanup_observer,
                &cleanup_context,
                &cleanup_shared.read_wake,
            ))))
            .await
            {
                Ok(result) => result.err(),
                Err(payload) => {
                    record_cleanup_panic(&cleanup_control, payload);
                    Some(HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        b"exchange cleanup panicked",
                    ))
                }
            };
            let core = cleanup_state.borrow_mut().core.take();
            let (pending_error, finalize_error) = if let Some(mut core) = core {
                let pending_error =
                    match catch_unwind(AssertUnwindSafe(|| core.process_pending_events())) {
                        Ok(result) => result.err(),
                        Err(payload) => {
                            record_cleanup_panic(&cleanup_control, payload);
                            Some(HttpError::with_message(
                                HttpErrorCode::INTERNAL_ERROR,
                                b"session callback cleanup panicked",
                            ))
                        }
                    };
                let finalize_error = match catch_future_unwind(pin!(Some((*core).finalize()))).await
                {
                    Ok(Ok(())) => None,
                    Ok(Err(task_error)) => Some(HttpError::with_message(
                        HttpErrorCode::INTERNAL_ERROR,
                        task_error.to_string().as_bytes(),
                    )),
                    Err(payload) => {
                        record_cleanup_panic(&cleanup_control, payload);
                        Some(HttpError::with_message(
                            HttpErrorCode::INTERNAL_ERROR,
                            b"session finalization panicked",
                        ))
                    }
                };
                (exchange_error.or(pending_error), finalize_error)
            } else {
                (None, None)
            };
            if let Err(payload) =
                catch_unwind(AssertUnwindSafe(|| flush_lifecycle_events(&cleanup_state)))
            {
                record_cleanup_panic(&cleanup_control, payload);
            }
            let lifecycle = Rc::clone(&cleanup_state.borrow().lifecycle);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| lifecycle.clear())) {
                record_cleanup_panic(&cleanup_control, payload);
            }
            *cleanup_control.result.borrow_mut() = (run_error.or(pending_error), finalize_error);
            cleanup_shared.completion.set(true);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| wake_cleanup(&cleanup_control)))
            {
                record_cleanup_panic(&cleanup_control, payload);
            }
            if let Some(payload) = cleanup_control.panic.borrow_mut().take() {
                std::panic::resume_unwind(payload);
            }
        };
        #[cfg(test)]
        let cleanup_task = count_cleanup_polls(cleanup_task);
        let cleanup_registration = if owned {
            let release = RunCompletion {
                cleanup: Rc::clone(&cleanup),
                state: Rc::downgrade(&state),
                transport: Rc::downgrade(&transport),
                observer: Rc::downgrade(&observer),
                context: context.clone(),
                shared: Rc::clone(&shared),
                registration: None,
                started: true,
                armed: false,
            };
            context.register_scope(owned_run(cleanup_task, release))
        } else {
            context.register_cleanup_task(cleanup_task)
        }
        .map_err(|error| {
            HttpError::with_message(
                HttpErrorCode::INVALID_STATE_TRANSITION,
                error.to_string().as_bytes(),
            )
        })?;
        let protocol = if http2_config.is_some() {
            CodecProtocol::Http2
        } else {
            CodecProtocol::Http11
        };
        let mut core = match SessionCore::with_host(
            direction,
            protocol,
            context.handle().clone(),
            context.host().clone(),
        ) {
            Ok(core) => core,
            Err(error) => {
                let _ = cleanup_registration.disarm();
                return Err(error);
            }
        };
        core.require_driver_cleanup();
        core.set_buffered_egress_stats_enabled(state.borrow().has_session_stats());
        core.add_lifecycle_observer(lifecycle);
        if let Some(http2) = &http2_config {
            let _ = core.set_max_concurrent_outgoing_streams(http2.max_concurrent_outgoing_streams);
            let _ = core.set_ingress_receive_window(http2.stream_receive_window);
        } else {
            let _ = core.set_ingress_receive_window(config.buffer_limit as u32);
        }
        let _ = core.set_egress_write_timeout(config.write_timeout);
        let _ = core.set_drain_request_body_on_drop(config.drain_request_body_on_drop);
        let _ = core.set_deliver_ingress_before_abort(config.deliver_ingress_before_abort);
        let _ = core.set_empty_content_length_is_zero(
            http2_config.is_some() && config.verbatim_http2_header_values,
        );
        let _ = core.set_peer_identity(peer_identity);
        let outgoing_activity = core.outgoing_transaction_activity();
        state.borrow_mut().core = Some(Box::new(core));
        let handle = UniplexHandle {
            state: Rc::downgrade(&state),
            shared: Rc::clone(&shared),
            observer: Rc::downgrade(&observer),
            context: context.clone(),
            cleanup: Rc::downgrade(&cleanup),
            outgoing_activity,
        };
        Ok((
            Self {
                state,
                transport: Rc::downgrade(&transport),
                observer: Rc::downgrade(&observer),
                context,
                shared,
                cleanup,
                cleanup_registration: Some(cleanup_registration),
                started: false,
            },
            handle,
        ))
    }

    /// Starts the session's run on its own task, which serves `owner` until
    /// the run completes.
    ///
    /// The task polls the transport loops and the session's cleanup as
    /// [`run`](Self::run)'s task does, and calls `owner` between those polls.
    /// It drops the run's own state before `owner` takes the outcome. If the
    /// task stops before the run completes, as at forced worker shutdown, it
    /// starts abort cleanup as a dropped `run` future does and drops `owner`
    /// without an outcome. A panic of `owner` is contained like a panic of that
    /// future's task: it begins worker shutdown and aborts the run, and the
    /// task resumes it after the run's cleanup.
    ///
    /// On error the run does not start; dropping the session aborts it.
    pub fn start(
        mut self,
        owner: Box<dyn UniplexRunOwner>,
    ) -> Result<(), CleanupRegistrationError> {
        let registration = self
            .cleanup_registration
            .take()
            .expect("an unstarted session retains its cleanup registration");
        *self.cleanup.owner.borrow_mut() = Some(owner);
        let pending = self.cleanup.trigger.get() == CleanupTrigger::Pending;
        if pending {
            self.cleanup.trigger.set(CleanupTrigger::Running);
        }
        match registration.activate() {
            Ok(task) => {
                task.detach();
                self.started = true;
                Ok(())
            }
            Err(error) => {
                if pending {
                    self.cleanup.trigger.set(CleanupTrigger::Pending);
                }
                let owner = self.cleanup.owner.borrow_mut().take();
                drop(owner);
                Err(error)
            }
        }
    }

    pub fn run(mut self) -> LocalSessionFuture<Result<UniplexRunSummary, HttpError>> {
        let completion = RunCompletion {
            cleanup: Rc::clone(&self.cleanup),
            state: Rc::downgrade(&self.state),
            transport: self.transport.clone(),
            observer: self.observer.clone(),
            context: self.context.clone(),
            shared: Rc::clone(&self.shared),
            registration: self.cleanup_registration.take(),
            started: false,
            armed: true,
        };
        self.started = true;
        drop(self);
        Box::pin(completion)
    }
}

async fn run_transport_loops(
    state: Rc<RefCell<DriverState>>,
    transport: Rc<dyn UniplexTransport>,
    observer: Rc<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    config: UniplexConfig,
    shared: Rc<UniplexShared>,
) -> Result<(), HttpError> {
    let read = read_loop(
        Rc::clone(&state),
        Rc::clone(&transport),
        Rc::clone(&observer),
        context.clone(),
        config,
        Rc::clone(&shared),
    );
    let write = write_loop(
        Rc::clone(&state),
        Rc::clone(&transport),
        Rc::clone(&observer),
        context.clone(),
        config,
        Rc::clone(&shared),
    );
    let ping_prober = state
        .borrow()
        .http2
        .as_ref()
        .and_then(|http2| http2.ping_prober.clone());
    let read_first_always = {
        let state = state.borrow();
        state.http2.is_none() && state.core().direction().is_upstream()
    };
    {
        // Pinned write first so that the read loop drops first.
        let mut write = pin!(MaybeDone::Future(write));
        let mut read = pin!(MaybeDone::Future(read));
        let loops = join_transport_loops(read.as_mut(), write.as_mut(), read_first_always);
        match ping_prober {
            // The probe timer ends with the transport loops. It is polled after
            // them, so a reply read in the same poll clears the probe first,
            // and outside the cooperative budget, which ready reads can spend
            // on every poll.
            Some(prober) => tokio::select! {
                biased;
                () = loops => {}
                never = tokio::task::coop::unconstrained(run_ping_probes(
                    prober, &state, &transport, &observer, &context, &shared,
                )) => {
                    match never {}
                }
            },
            None => loops.await,
        }
    }

    if state.borrow().reset_after_draining_writes {
        let cancellation_error = state.borrow().fatal_error.clone().unwrap_or_else(|| {
            HttpError::with_message(
                HttpErrorCode::CORO_CANCELLED,
                b"uniplex byte events cancelled during reset",
            )
        });
        observer.cancel_events(&context, &cancellation_error)?;
    }
    let _ = observer.zero_refs(&context)?.await;
    let (discarded_work, pending_tx_ack) = {
        let mut state = state.borrow_mut();
        (state.take_egress_work(), state.pending_tx_ack.take())
    };
    drop_items_individually(discarded_work);
    drop(pending_tx_ack);
    match state.borrow_mut().fatal_error.take() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Polls the read and write loops, alternating which goes first on each poll
/// (with `read_first_always`, the read loop goes first on every poll) and
/// polling every pending loop with the task's own context. When the loop
/// polled second completes while the first is still pending, the first is
/// polled once more in that pass so that it observes the completion without
/// another task poll.
async fn join_transport_loops<R, W>(
    mut read: Pin<&mut MaybeDone<R>>,
    mut write: Pin<&mut MaybeDone<W>>,
    read_first_always: bool,
) where
    R: Future<Output = ()>,
    W: Future<Output = ()>,
{
    let mut read_first = true;
    poll_fn(|task_context| {
        #[cfg(test)]
        let (read_was_done, write_was_done) =
            (maybe_done_is_done(&read), maybe_done_is_done(&write));
        let pass_read_first = read_first || read_first_always;
        read_first = !read_first;
        let poll = if pass_read_first {
            poll_join_pass(read.as_mut(), write.as_mut(), task_context)
        } else {
            poll_join_pass(write.as_mut(), read.as_mut(), task_context)
        };
        #[cfg(test)]
        writer_probe::note_join_pass(
            !read_was_done && maybe_done_is_done(&read),
            !write_was_done && maybe_done_is_done(&write),
        );
        poll
    })
    .await;
}

fn poll_join_pass<A, B>(
    mut first: Pin<&mut MaybeDone<A>>,
    mut second: Pin<&mut MaybeDone<B>>,
    task_context: &mut Context<'_>,
) -> Poll<()>
where
    A: Future<Output = ()>,
    B: Future<Output = ()>,
{
    let _ = first.as_mut().poll(task_context);
    let second_was_pending = !maybe_done_is_done(&second);
    let _ = second.as_mut().poll(task_context);
    if second_was_pending && maybe_done_is_done(&second) && !maybe_done_is_done(&first) {
        let _ = first.as_mut().poll(task_context);
    }
    if maybe_done_is_done(&first) && maybe_done_is_done(&second) {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

fn maybe_done_is_done<F: Future>(future: &MaybeDone<F>) -> bool {
    matches!(future, MaybeDone::Done(_))
}

/// Runs the session's PING probes until the transport loops end and drop it.
async fn run_ping_probes(
    prober: Rc<PingProber>,
    state: &Rc<RefCell<DriverState>>,
    transport: &Rc<dyn UniplexTransport>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    shared: &UniplexShared,
) -> Infallible {
    if prober.immediate {
        expire_ping_probe(&prober, state, transport, observer, context, shared);
    }
    loop {
        let rearmed = prober.rearmed.notified();
        let Some(deadline) = prober.deadline.get() else {
            rearmed.await;
            continue;
        };
        tokio::select! {
            biased;
            () = rearmed => {}
            () = context.host().sleep_until(deadline) => {
                if prober.deadline.get() == Some(deadline) {
                    expire_ping_probe(&prober, state, transport, observer, context, shared);
                }
            }
        }
    }
}

/// Handles the probe timer: sends a probe, or drops the connection when the
/// outstanding probe went unanswered.
fn expire_ping_probe(
    prober: &PingProber,
    state: &Rc<RefCell<DriverState>>,
    transport: &Rc<dyn UniplexTransport>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    shared: &UniplexShared,
) {
    if prober.probe.get().is_some() {
        prober.cancel_probes();
        drop_connection_for_ping_probe(state, transport, observer, context, shared);
        return;
    }
    let data = NEXT_SESSION_PING_TOKEN.fetch_add(1, Ordering::Relaxed);
    prober.probe.set(Some(data));
    prober
        .deadline
        .set(Some(prober.host.deadline_after(prober.timeout)));
    {
        let mut driver = state.borrow_mut();
        let DriverState { codec, egress, .. } = &mut *driver;
        // The result is ignored, so a probe that could not be queued still times
        // out.
        let _ = codec.generate(
            egress,
            EgressFrame::PingRequest { data: Some(data) },
            &mut NoopCodecCallback,
        );
        driver.note_flow_timeout_activity();
    }
    shared.write_wake.notify_one();
}

/// Drops the connection after a probe timeout: records the timeout, fails every
/// stream, discards the unsent egress, and resets the transport without waiting
/// for the writer to drain it.
fn drop_connection_for_ping_probe(
    state: &Rc<RefCell<DriverState>>,
    transport: &Rc<dyn UniplexTransport>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    shared: &UniplexShared,
) {
    let mut first_panic = None;
    if let Some(pending) = pending_session_stats(&mut state.borrow_mut()) {
        pending.push(Http2StatsEvent::PingTimeout);
    }
    capture_panic(&mut first_panic, || flush_session_stats(state));
    if let Some(core) = state.borrow().core.as_deref() {
        core.observe_lifecycle(LifecycleEvent::Http2PingProbeTimeout);
    }
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    capture_panic(&mut first_panic, || {
        shared.read_cancellation.request_cancellation();
    });
    capture_panic(&mut first_panic, || {
        shared.write_cancellation.request_cancellation();
    });
    capture_panic(&mut first_panic, || {
        fail_connection(state, &shared.write_wake, ping_probe_timeout_error(), true);
    });
    capture_panic(&mut first_panic, || {
        cancel_pending_byte_events(state, observer, context);
    });
    match catch_unwind(AssertUnwindSafe(|| {
        reset_transport_once(state, transport, context)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => state.borrow_mut().record_error(error),
        Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
    }
    let (discarded_work, discarded_egress) = {
        let mut driver = state.borrow_mut();
        (driver.take_egress_work(), driver.egress.take())
    };
    capture_panic(&mut first_panic, || drop_items_individually(discarded_work));
    drop(discarded_egress);
    capture_panic(&mut first_panic, || shared.write_wake.notify_one());
    resume_first_panic(first_panic);
}

async fn read_loop(
    state: Rc<RefCell<DriverState>>,
    transport: Rc<dyn UniplexTransport>,
    observer: Rc<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    config: UniplexConfig,
    shared: Rc<UniplexShared>,
) {
    let cancellation = &shared.read_cancellation;
    let read_wake = &shared.read_wake;
    let write_wake = &shared.write_wake;
    let mut pending_eof = false;
    loop {
        if let Err(error) = drive_read_callbacks(&state, read_wake, write_wake) {
            fail_connection(&state, write_wake, error, true);
        }
        state
            .borrow_mut()
            .byte_event_bridges
            .retain(|bridge| bridge.pending.get() != 0);
        cancel_pending_byte_events(&state, &observer, &context);
        let (should_continue, paused, started_with_ingress, parse_ready) = {
            let state = state.borrow();
            let started_with_ingress = !state.ingress.is_empty();
            (
                state.should_continue_reading(),
                state.should_wait_for_ingress_resume(),
                started_with_ingress,
                started_with_ingress || state.codec.is_busy(),
            )
        };
        if !should_continue {
            break;
        }
        if paused {
            let timed_out = if config.read_timeout.is_zero() {
                let _ = wait_for_notify_or_cancellation(read_wake, cancellation.token()).await;
                false
            } else {
                let deadline = context.host().deadline_after(config.read_timeout);
                tokio::select! {
                    biased;
                    _ = wait_for_notify_or_cancellation(read_wake, cancellation.token()) => false,
                    _ = context.host().sleep_until(deadline) => true,
                }
            };
            if timed_out {
                fail_connection(
                    &state,
                    write_wake,
                    HttpError::with_message(
                        HttpErrorCode::READ_TIMEOUT,
                        b"ingress backpressure timeout",
                    ),
                    true,
                );
                cancel_pending_byte_events(&state, &observer, &context);
                break;
            }
            if cancellation.token().is_cancellation_requested() {
                break;
            }
            continue;
        }
        if parse_ready {
            let parse_passes = if started_with_ingress {
                MAX_PARSE_PASSES_PER_TICK
            } else {
                1
            };
            let mut parse_budget_exhausted = started_with_ingress;
            let mut parse_blocked = false;
            for pass in 0..parse_passes {
                match parse_one_prefix(&state) {
                    Ok(ParseOutcome::Consumed) => {
                        cancel_pending_byte_events(&state, &observer, &context);
                    }
                    Ok(ParseOutcome::Blocked) => {
                        cancel_pending_byte_events(&state, &observer, &context);
                        parse_budget_exhausted = false;
                        parse_blocked = true;
                        break;
                    }
                    Err(error) => {
                        handle_codec_failure(&state, write_wake, error);
                        cancel_pending_byte_events(&state, &observer, &context);
                        parse_budget_exhausted = false;
                        break;
                    }
                }
                let http2 = {
                    let mut state = state.borrow_mut();
                    state.note_flow_timeout_activity();
                    state.http2.is_some()
                };
                // Only HTTP/2 wakes the writer after every parse. The HTTP/1.1
                // callbacks that need the writer, such as a detach or an error,
                // wake it themselves.
                if http2 {
                    write_wake.notify_one();
                }
                if pass + 1 == MAX_PARSE_PASSES_PER_TICK {
                    context.host().yield_to_io().await;
                }
            }
            if parse_budget_exhausted {
                if let Some(pending) = pending_session_stats(&mut state.borrow_mut()) {
                    pending.push(Http2StatsEvent::ReadLoopLimit);
                }
            }
            flush_session_stats(&state);
            let state = state.borrow();
            let empty_drive_needs_more_input = !started_with_ingress
                && parse_blocked
                && state.ingress.is_empty()
                && state.codec.is_busy();
            if parse_budget_exhausted
                || state.should_wait_for_ingress_resume()
                || (state.ingress.is_empty() && !empty_drive_needs_more_input)
                || !state.should_continue_reading()
            {
                continue;
            }
        }

        if pending_eof {
            handle_eof(&state, read_wake, write_wake);
            break;
        }

        let mut pending_read = Some(transport.read(
            &context,
            config.maximum_read_size,
            config.read_allocation_size,
            config.read_timeout,
            cancellation.token(),
        ));
        let read = loop {
            let rate_deadline = {
                let state = state.borrow();
                if state.core().direction().is_downstream() {
                    state
                        .http2
                        .as_deref()
                        .and_then(Http2DriverState::next_rate_limit_deadline)
                } else {
                    None
                }
            };
            let read_future = pending_read.as_mut().expect("admitted transport read");
            if let Some(deadline) = rate_deadline {
                tokio::select! {
                    read = read_future.as_mut() => break read,
                    _ = context.host().sleep_until(deadline) => {
                        if let Some(http2) = &mut state.borrow_mut().http2 {
                            http2.expire_rate_limits(context.host().now());
                        }
                        flush_session_stats(&state);
                    }
                }
            } else {
                break read_future.as_mut().await;
            }
            if let Err(error) = drive_read_callbacks(&state, read_wake, write_wake) {
                fail_connection(&state, write_wake, error, true);
            }
            state
                .borrow_mut()
                .byte_event_bridges
                .retain(|bridge| bridge.pending.get() != 0);
            cancel_pending_byte_events(&state, &observer, &context);
            if !state.borrow().should_continue_reading() {
                // Sampling can close admission while this read is still in flight.
                break retire_transport_read(
                    pending_read.take().expect("admitted transport read"),
                    cancellation,
                )
                .await;
            }
        };
        drop(pending_read);
        let read = match read {
            Ok(read) => read,
            Err(error) if error.code() == HttpErrorCode::READ_TIMEOUT => {
                handle_read_timeout(&state, write_wake);
                continue;
            }
            Err(error) if error.code() == HttpErrorCode::CORO_CANCELLED => break,
            Err(error) => {
                fail_connection(&state, write_wake, error, true);
                cancel_pending_byte_events(&state, &observer, &context);
                break;
            }
        };

        let eof = read.is_eof();
        let bytes_read = read.bytes_read();
        if eof && state.borrow().core().direction().is_upstream() {
            state.borrow().core().close_outgoing_admission();
            flush_lifecycle_events(&state);
        }
        if state.borrow().codec.protocol() == CodecProtocol::Http2 {
            state
                .borrow()
                .core()
                .observe_lifecycle(LifecycleEvent::Http2Read {
                    bytes: bytes_read,
                    stream: None,
                });
            flush_lifecycle_events(&state);
        }
        if bytes_read == 0 {
            if eof {
                handle_eof(&state, read_wake, write_wake);
                break;
            }
            context.host().yield_to_io().await;
            continue;
        }
        pending_eof = eof;
        if let Some(prober) = state
            .borrow()
            .http2
            .as_ref()
            .and_then(|http2| http2.ping_prober.as_deref())
        {
            prober.refresh_timeout(true);
        }
        let append_error = {
            let mut state = state.borrow_mut();
            state.bytes_read = state.bytes_read.saturating_add(bytes_read as u64);
            let mut data = read.into_data();
            if state.eof_watch {
                None
            } else {
                state.ingress.append(&mut data).err()
            }
        };
        if let Some(error) = append_error {
            fail_connection(
                &state,
                write_wake,
                HttpError::with_message(
                    HttpErrorCode::FLOW_CONTROL_ERROR,
                    error.to_string().as_bytes(),
                ),
                true,
            );
            cancel_pending_byte_events(&state, &observer, &context);
            break;
        }
        if let Err(error) = drive_read_callbacks(&state, read_wake, write_wake) {
            fail_connection(&state, write_wake, error, true);
            cancel_pending_byte_events(&state, &observer, &context);
            break;
        }
    }
    {
        let mut state = state.borrow_mut();
        state.reads_closed = true;
        state.note_flow_timeout_activity();
    }
    write_wake.notify_one();
}

async fn retire_transport_read(
    mut read: LocalTransportFuture<Result<UniplexRead, HttpError>>,
    cancellation: &CancellationSource,
) -> Result<UniplexRead, HttpError> {
    let mut first_panic = None;
    capture_panic(&mut first_panic, || {
        cancellation.request_cancellation();
    });
    let mut result = match catch_future_unwind(pin!(Some(read.as_mut()))).await {
        Ok(result) => Some(result),
        Err(payload) => {
            retain_first_panic(&mut first_panic, Some(payload));
            None
        }
    };
    capture_panic(&mut first_panic, || drop(read));
    if first_panic.is_some() {
        capture_panic(&mut first_panic, || drop(result.take()));
        resume_first_panic(first_panic);
    }
    result.expect("read completed without a panic")
}

#[cfg(test)]
mod read_retirement_tests {
    use std::sync::Mutex;

    use roxygen_core::message::SocketAddress;
    use roxygen_transport::byte_stream::TransportRead;

    use super::*;

    fn record_read_event(events: &Mutex<Vec<&'static str>>, event: &'static str) {
        events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("blanket read cause")]
    struct BlanketReadCause {
        events: Arc<Mutex<Vec<&'static str>>>,
        panic_drop: bool,
    }

    impl Drop for BlanketReadCause {
        fn drop(&mut self) {
            record_read_event(&self.events, "cause_drop");
            if self.panic_drop {
                std::panic::panic_any(SecondaryReadPanic(Arc::clone(&self.events)));
            }
        }
    }

    struct SecondaryReadPanic(Arc<Mutex<Vec<&'static str>>>);

    impl Drop for SecondaryReadPanic {
        fn drop(&mut self) {
            record_read_event(&self.0, "secondary_payload_drop");
        }
    }

    struct BlanketReadFuture {
        events: Arc<Mutex<Vec<&'static str>>>,
        pending_first: bool,
        panic_poll: bool,
        panic_drop: bool,
        panic_cause: bool,
    }

    impl Future for BlanketReadFuture {
        type Output = Result<TransportRead, HttpError>;

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            record_read_event(&self.events, "poll");
            if std::mem::take(&mut self.pending_first) {
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            if self.panic_poll {
                panic!("blanket read poll");
            }
            record_read_event(&self.events, "ready_error");
            Poll::Ready(Err(HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR)
                .with_cause(Arc::new(BlanketReadCause {
                    events: Arc::clone(&self.events),
                    panic_drop: self.panic_cause,
                }))))
        }
    }

    impl Drop for BlanketReadFuture {
        fn drop(&mut self) {
            record_read_event(&self.events, "child_drop");
            if self.panic_drop {
                panic!("blanket read child drop");
            }
        }
    }

    struct BlanketReadTransport {
        events: Arc<Mutex<Vec<&'static str>>>,
        pending_first: bool,
        panic_poll: bool,
        panic_drop: bool,
        panic_cause: bool,
    }

    impl ByteStreamTransport for BlanketReadTransport {
        fn read(
            &self,
            _context: &WorkerContext,
            _maximum: usize,
            _allocation_size: usize,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<TransportRead, HttpError>> {
            record_read_event(&self.events, "admitted");
            Box::pin(BlanketReadFuture {
                events: Arc::clone(&self.events),
                pending_first: self.pending_first,
                panic_poll: self.panic_poll,
                panic_drop: self.panic_drop,
                panic_cause: self.panic_cause,
            })
        }

        fn write(
            &self,
            _context: &WorkerContext,
            _data: BufQueue,
            _timeout: Duration,
            _cancellation: CancellationToken,
        ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
            unreachable!("read-only fixture")
        }

        fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            unreachable!("read-only fixture")
        }

        fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            unreachable!("read-only fixture")
        }

        fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
            unreachable!("read-only fixture")
        }

        fn local_address(&self, _context: &WorkerContext) -> Result<SocketAddress, HttpError> {
            unreachable!("read-only fixture")
        }

        fn peer_address(&self, _context: &WorkerContext) -> Result<SocketAddress, HttpError> {
            unreachable!("read-only fixture")
        }

        fn peer_identity(
            &self,
            _context: &WorkerContext,
        ) -> Result<Option<PeerIdentity>, HttpError> {
            unreachable!("read-only fixture")
        }

        fn byte_event_capability(
            &self,
            _context: &WorkerContext,
        ) -> Result<ByteEventCapability, HttpError> {
            unreachable!("read-only fixture")
        }

        fn register_byte_event(
            &self,
            _context: &WorkerContext,
            _registration: HttpByteEventRegistration,
        ) -> Result<(), ByteEventRegistrationError> {
            unreachable!("read-only fixture")
        }
    }

    #[test]
    fn blanket_transport_read_destroys_child_and_ready_error() {
        let observations = crate::uniplex_tests::run_on_worker(|context| {
            Box::pin(async move {
                let mut observations = Vec::new();
                for pending_first in [false, true] {
                    for (panic_drop, panic_cause) in [(false, false), (true, false), (true, true)] {
                        let events = Arc::new(Mutex::new(Vec::with_capacity(9)));
                        let transport = BlanketReadTransport {
                            events: Arc::clone(&events),
                            pending_first,
                            panic_poll: false,
                            panic_drop,
                            panic_cause,
                        };
                        let cancellation = CancellationSource::new();
                        let mut read = UniplexTransport::read(
                            &transport,
                            &context,
                            1,
                            1,
                            Duration::ZERO,
                            cancellation.token(),
                        );
                        let (panic_message, returned_code, output_drop_panicked) =
                            match catch_future_unwind(pin!(Some(read.as_mut()))).await {
                                Ok(result) => {
                                    record_read_event(&events, "output_returned");
                                    let code = result.as_ref().err().map(HttpError::code);
                                    let panicked =
                                        catch_unwind(AssertUnwindSafe(|| drop(result))).is_err();
                                    (None, code, panicked)
                                }
                                Err(payload) => {
                                    record_read_event(&events, "caught_panic");
                                    let matches = payload.downcast_ref::<&str>()
                                        == Some(&"blanket read child drop");
                                    drop(payload);
                                    (Some(matches), None, false)
                                }
                            };
                        let wrapper_drop_panicked =
                            catch_unwind(AssertUnwindSafe(|| drop(read))).is_err();
                        record_read_event(&events, "wrapper_dropped");
                        let events = events
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .clone();
                        observations.push((
                            pending_first,
                            panic_drop,
                            panic_cause,
                            panic_message,
                            returned_code,
                            output_drop_panicked,
                            wrapper_drop_panicked,
                            events,
                        ));
                    }
                }
                observations
            })
        });
        for (
            pending_first,
            panic_drop,
            panic_cause,
            panic_message,
            code,
            output_panicked,
            wrapper_panicked,
            events,
        ) in &observations
        {
            assert_eq!(
                *panic_message,
                panic_drop.then_some(true),
                "{observations:?}"
            );
            assert_eq!(
                *code,
                (!panic_drop).then_some(HttpErrorCode::TRANSPORT_READ_ERROR),
                "{observations:?}"
            );
            assert!(!output_panicked, "{observations:?}");
            assert!(!wrapper_panicked, "{observations:?}");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| **event == "cause_drop")
                    .count(),
                1,
                "ready error must be destroyed: {observations:?}"
            );
            let mut expected = vec!["admitted", "poll"];
            if *pending_first {
                expected.push("poll");
            }
            expected.extend(["ready_error", "child_drop"]);
            if *panic_drop {
                expected.push("cause_drop");
                if *panic_cause {
                    expected.push("secondary_payload_drop");
                }
                expected.push("caught_panic");
            } else {
                expected.extend(["output_returned", "cause_drop"]);
            }
            expected.push("wrapper_dropped");
            assert_eq!(*events, expected, "{observations:?}");
        }
    }

    #[test]
    fn blanket_transport_read_poll_panic_and_abandonment_drop_child_once() {
        let observations = crate::uniplex_tests::run_on_worker(|context| {
            Box::pin(async move {
                let mut observations = Vec::new();
                for (polls, pending_first, panic_poll) in [
                    (0, false, false),
                    (1, true, false),
                    (1, false, true),
                    (2, true, true),
                ] {
                    let events = Arc::new(Mutex::new(Vec::with_capacity(7)));
                    let transport = BlanketReadTransport {
                        events: Arc::clone(&events),
                        pending_first,
                        panic_poll,
                        panic_drop: false,
                        panic_cause: false,
                    };
                    let cancellation = CancellationSource::new();
                    let mut read = UniplexTransport::read(
                        &transport,
                        &context,
                        1,
                        1,
                        Duration::ZERO,
                        cancellation.token(),
                    );
                    let waker = Waker::from(Arc::new(RetirementWake(false)));
                    let mut task_context = Context::from_waker(&waker);
                    let mut pending_polls = 0;
                    let mut panic_message = None;
                    for _ in 0..polls {
                        match catch_unwind(AssertUnwindSafe(|| {
                            read.as_mut().poll(&mut task_context)
                        })) {
                            Ok(Poll::Pending) => pending_polls += 1,
                            Ok(Poll::Ready(result)) => {
                                record_read_event(&events, "unexpected_ready");
                                drop(result);
                                break;
                            }
                            Err(payload) => {
                                record_read_event(&events, "poll_panic_caught");
                                panic_message = Some(
                                    payload.downcast_ref::<&str>() == Some(&"blanket read poll"),
                                );
                                drop(payload);
                                break;
                            }
                        }
                    }
                    record_read_event(&events, "before_wrapper_drop");
                    let wrapper_panicked = catch_unwind(AssertUnwindSafe(|| drop(read))).is_err();
                    record_read_event(&events, "wrapper_dropped");
                    let events = events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone();
                    observations.push((
                        polls,
                        panic_poll,
                        pending_polls,
                        panic_message,
                        wrapper_panicked,
                        events,
                    ));
                }
                observations
            })
        });
        for (polls, panic_poll, pending_polls, panic_message, wrapper_panicked, events) in
            &observations
        {
            assert_eq!(
                *panic_message,
                panic_poll.then_some(true),
                "{observations:?}"
            );
            assert_eq!(
                *pending_polls,
                polls - usize::from(*panic_poll),
                "{observations:?}"
            );
            assert!(!wrapper_panicked, "{observations:?}");
            let mut expected = vec!["admitted"];
            expected.extend(std::iter::repeat_n("poll", *polls));
            if *panic_poll {
                expected.extend(["child_drop", "poll_panic_caught", "before_wrapper_drop"]);
            } else {
                expected.extend(["before_wrapper_drop", "child_drop"]);
            }
            expected.push("wrapper_dropped");
            assert_eq!(*events, expected, "{observations:?}");
        }
    }

    struct RetirementWake(bool);

    impl Wake for RetirementWake {
        fn wake(self: Arc<Self>) {
            if self.0 {
                panic!("cancellation wake");
            }
        }
    }

    struct ReadBytes(Arc<std::sync::atomic::AtomicUsize>);

    impl AsRef<[u8]> for ReadBytes {
        fn as_ref(&self) -> &[u8] {
            b"x"
        }
    }

    impl Drop for ReadBytes {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("read output drop");
        }
    }

    struct RetirementRead {
        wait: LocalTransportFuture<WaitStatus>,
        polls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        output_drops: Arc<std::sync::atomic::AtomicUsize>,
        panic_poll: bool,
    }

    impl Future for RetirementRead {
        type Output = Result<UniplexRead, HttpError>;

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.set(self.polls.get() + 1);
            let Poll::Ready(status) = self.wait.as_mut().poll(context) else {
                return Poll::Pending;
            };
            assert_eq!(status, WaitStatus::Cancelled);
            if self.panic_poll {
                panic!("read poll");
            }
            let bytes = bytes::Bytes::from_owner(ReadBytes(Arc::clone(&self.output_drops)));
            Poll::Ready(Ok(UniplexRead::new(
                BufQueue::from_bytes(bytes, 1).unwrap(),
                false,
            )))
        }
    }

    impl Drop for RetirementRead {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            panic!("read drop");
        }
    }

    #[test]
    fn pending_read_retirement_preserves_first_panic_and_retires_owners() {
        for (panic_wake, panic_poll) in [(true, true), (true, false), (false, true), (false, false)]
        {
            let cancellation = CancellationSource::new();
            let token = cancellation.token();
            let polls = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let output_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut read = Box::pin(RetirementRead {
                wait: Box::pin(async move { CancellableBaton::new().wait(token).await }),
                polls: Rc::clone(&polls),
                drops: Rc::clone(&drops),
                output_drops: Arc::clone(&output_drops),
                panic_poll,
            });
            let waker = Waker::from(Arc::new(RetirementWake(panic_wake)));
            assert!(
                read.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(catch_future_unwind(pin!(Some(retire_transport_read(
                    read,
                    &cancellation,
                )))));
            let payload = result.err().expect("retirement preserves the first panic");
            let expected = if panic_wake {
                "cancellation wake"
            } else if panic_poll {
                "read poll"
            } else {
                "read drop"
            };
            assert_eq!(payload.downcast_ref::<&str>(), Some(&expected));
            assert_eq!(polls.get(), 2);
            assert_eq!(drops.get(), 1);
            assert_eq!(
                output_drops.load(Ordering::SeqCst),
                usize::from(!panic_poll)
            );
        }
    }
}

enum ParseOutcome {
    Consumed,
    Blocked,
}

struct CodecFailure {
    stream: StreamId,
    error: HttpError,
    http_status: Option<u16>,
    wire_code: Option<H2ErrorCode>,
    direction: HttpExceptionDirection,
    new_stream: bool,
}

impl CodecFailure {
    fn plain(stream: StreamId, error: HttpError) -> Self {
        Self {
            stream,
            error,
            http_status: None,
            wire_code: None,
            direction: HttpExceptionDirection::IngressAndEgress,
            new_stream: false,
        }
    }
}

fn flush_parse_failure_lifecycle(state: &Rc<RefCell<DriverState>>, failure: &CodecFailure) {
    {
        let driver = state.borrow();
        if driver.core().direction().is_upstream()
            && (driver.codec.protocol() == CodecProtocol::Http11 || failure.stream.is_unspecified())
        {
            driver.core().close_outgoing_admission();
        }
    }
    flush_lifecycle_events(state);
}

fn parse_one_prefix(state: &Rc<RefCell<DriverState>>) -> Result<ParseOutcome, CodecFailure> {
    let state_owner = Rc::clone(state);
    let mut state = state.borrow_mut();
    if state.flow_control_paused || state.anti_pipeline_paused || state.upgrade_paused {
        return Ok(ParseOutcome::Blocked);
    }
    if state.eof_watch {
        drop(state.ingress.take());
        return Ok(ParseOutcome::Blocked);
    }
    let input = state.ingress.clone_shared();
    let input_len = input.chain_length();
    let (
        result,
        failures,
        stream_updates,
        upstream_message_complete,
        upgrade_completed,
        rejected_body,
        drain_trigger,
        peer_goaway_generated,
        codec_reusable,
    ) = {
        let DriverState {
            core,
            codec,
            http2,
            http1_stats,
            handler,
            ingress_meta,
            pending_pushes,
            egress,
            stream_read_timeout,
            ingress_buffer_limit,
            http1_ingress_body_ceiling,
            flow_control_paused,
            anti_pipeline_paused,
            upgrade_paused,
            close_after_ingress,
            abort_exchanges_on_eof,
            eof_watch,
            ingress_close_stream,
            reads_closed,
            stream_byte_event_cancellations,
            ..
        } = &mut *state;
        let mut callback = SessionCodecCallback {
            core: core
                .as_deref_mut()
                .expect("a running uniplex owns its core"),
            http2,
            http1_stats,
            handler: handler.clone(),
            ingress_meta,
            pending_pushes,
            stream_read_timeout: *stream_read_timeout,
            ingress_buffer_limit: *ingress_buffer_limit,
            http1_ingress_body_ceiling: *http1_ingress_body_ceiling,
            flow_control_paused,
            anti_pipeline_paused,
            upgrade_paused,
            close_after_ingress,
            abort_exchanges_on_eof: *abort_exchanges_on_eof,
            eof_watch,
            ingress_close_stream,
            reads_closed,
            stream_byte_event_cancellations,
            failures: Vec::new(),
            stream_updates: Vec::new(),
            upstream_message_complete: false,
            upgrade_completed: false,
            rejected_body: false,
            drain_trigger: None,
            peer_goaway_generation: CallbackGoawayGeneration::NotRequested,
        };
        let result = codec.on_ingress(&input, &mut callback, egress);
        (
            result,
            callback.failures,
            callback.stream_updates,
            callback.upstream_message_complete,
            callback.upgrade_completed,
            callback.rejected_body,
            callback.drain_trigger,
            matches!(
                callback.peer_goaway_generation,
                CallbackGoawayGeneration::Generated
            ),
            codec.is_reusable(),
        )
    };
    if upgrade_completed && state.core().direction().is_upstream() {
        state.upstream_ingress_upgraded = true;
    }
    if peer_goaway_generated {
        let waiting_to_drain = state.codec.is_waiting_to_drain();
        if let Some(http2) = state.http2.as_mut() {
            let phase = if waiting_to_drain {
                GoawayPhase::Initial
            } else {
                GoawayPhase::Final
            };
            if phase.commit(
                SentGoaway {
                    sent_initial: &mut http2.sent_initial_goaway,
                    sent_final: &mut http2.sent_final_goaway,
                    deadline: &mut http2.goaway_deadline,
                },
                || http2.host.deadline_after(http2.config.goaway_timeout),
            ) {
                state.core_mut().set_draining_ingress_grace(false);
            }
        }
    }
    drop(input);
    if result.consumed > input_len {
        drop(state);
        let failure = CodecFailure::plain(
            StreamId::UNSPECIFIED,
            invalid_transition(b"codec consumed beyond the supplied prefix"),
        );
        flush_parse_failure_lifecycle(&state_owner, &failure);
        return Err(failure);
    }
    state
        .ingress
        .trim_start(result.consumed)
        .expect("the codec consumption was range checked");
    let failure = failures.into_iter().next();
    if state.codec.protocol() == CodecProtocol::Http11
        && let Some(failure) = failure
    {
        drop(state);
        flush_parse_failure_lifecycle(&state_owner, &failure);
        return Err(failure);
    }
    let rejected_credit = state.http2.as_mut().map_or(0, |http2| {
        std::mem::take(&mut http2.connection_credit_to_return)
    });
    // A rejected padding-only DATA frame returns no body bytes, but its padding is
    // pending connection credit that must still be returned.
    if rejected_credit != 0 || rejected_body {
        let mut callback = NoopCodecCallback;
        let DriverState { codec, egress, .. } = &mut *state;
        if let Err(error) = codec.ingress_bytes_processed(egress, rejected_credit, &mut callback) {
            drop(state);
            let failure = CodecFailure::plain(StreamId::UNSPECIFIED, error);
            flush_parse_failure_lifecycle(&state_owner, &failure);
            return Err(failure);
        }
    }
    let drain_trigger = apply_stream_updates_locked(&mut state, stream_updates)
        .or(drain_trigger)
        .or_else(|| {
            (upstream_message_complete && !codec_reusable).then_some(DrainTrigger::CodecNotReusable)
        });
    if let Some(error) = result.egress_results.into_iter().find_map(Result::err) {
        drop(state);
        let failure = CodecFailure::plain(StreamId::UNSPECIFIED, error);
        flush_parse_failure_lifecycle(&state_owner, &failure);
        return Err(failure);
    }
    if let Some(failure) = failure {
        drop(state);
        flush_parse_failure_lifecycle(&state_owner, &failure);
        return Err(failure);
    }
    let outcome = if result.consumed == 0 && !upgrade_completed {
        ParseOutcome::Blocked
    } else {
        ParseOutcome::Consumed
    };
    drop(state);
    if let Some(trigger) = drain_trigger {
        let transition = initiate_drain_after_state_borrow(&state_owner, trigger);
        resume_first_panic(transition.panic);
    } else {
        flush_lifecycle_events(&state_owner);
    }
    Ok(outcome)
}

enum CallbackGoawayGeneration {
    NotRequested,
    Pending,
    Generated,
}

struct SessionCodecCallback<'a> {
    core: &'a mut SessionCore,
    http2: &'a mut Option<Box<Http2DriverState>>,
    http1_stats: &'a mut Option<Http1StatsState>,
    handler: Option<HttpHandlerRef>,
    ingress_meta: &'a mut SmallMap<StreamId, Box<IngressMeta>>,
    pending_pushes: &'a mut BTreeMap<StreamId, PendingPush>,
    stream_read_timeout: Duration,
    ingress_buffer_limit: u32,
    http1_ingress_body_ceiling: usize,
    flow_control_paused: &'a mut bool,
    anti_pipeline_paused: &'a mut bool,
    upgrade_paused: &'a mut bool,
    close_after_ingress: &'a mut bool,
    abort_exchanges_on_eof: bool,
    eof_watch: &'a mut bool,
    ingress_close_stream: &'a mut Option<StreamId>,
    reads_closed: &'a mut bool,
    stream_byte_event_cancellations: &'a mut Vec<(StreamId, HttpError)>,
    failures: Vec<CodecFailure>,
    stream_updates: Vec<StreamUpdate>,
    upstream_message_complete: bool,
    upgrade_completed: bool,
    rejected_body: bool,
    drain_trigger: Option<DrainTrigger>,
    peer_goaway_generation: CallbackGoawayGeneration,
}

impl SessionCodecCallback<'_> {
    fn pending_stats(&mut self) -> Option<&mut Vec<Http2StatsEvent>> {
        match (&mut self.http2, &mut self.http1_stats) {
            (Some(http2), _) if http2.config.stats.is_some() => Some(&mut http2.pending_stats),
            (Some(_), _) => None,
            (None, Some(http1)) => Some(&mut http1.pending),
            (None, None) => None,
        }
    }

    fn configured_incoming_stream_limit(&self) -> u64 {
        self.http2
            .as_ref()
            .and_then(|http2| {
                http2.config.settings.iter().rev().find_map(|setting| {
                    (setting.identifier == SettingsId::MaxConcurrentStreams.as_u16())
                        .then_some(setting.value)
                })
            })
            .unwrap_or(u64::from(u32::MAX))
    }

    fn fail(&mut self, stream: StreamId, error: HttpError) {
        if stream.is_unspecified()
            && !self
                .failures
                .iter()
                .any(|failure| failure.stream.is_unspecified())
        {
            self.failures.clear();
            self.failures.push(CodecFailure::plain(stream, error));
        } else if self.failures.is_empty() {
            self.failures.push(CodecFailure::plain(stream, error));
        }
    }

    /// Whether an HTTP/2 stream has left both the core and the driver. Such a
    /// stream has no ingress record, and its later frames are discarded apart from
    /// their connection credit.
    fn stream_erased(&self, stream: StreamId) -> bool {
        self.http2
            .as_ref()
            .is_some_and(|http2| !http2.streams.contains_key(&stream))
            && self.core.stream_if_present(stream).is_none()
    }

    /// Whether an upgraded stream's ingress source has completed while the stream
    /// waits for its egress. Source completion leaves such a stream in place, and
    /// DATA that its source rejects, including DATA after the peer's END_STREAM,
    /// returns only connection credit.
    fn upgraded_source_complete(&self, stream: StreamId) -> bool {
        self.core
            .stream_if_present(stream)
            .is_some_and(|state| state.is_upgraded() && state.stream_source_complete())
    }

    /// Whether a live stream is upgraded. DATA or trailers that its source
    /// rejects fail only that source, with INVALID_STATE_TRANSITION, and source
    /// completion does not reset an upgraded stream. The body and trailer
    /// callbacks do not send a reset themselves.
    fn stream_is_upgraded(&self, stream: StreamId) -> bool {
        self.core
            .stream_if_present(stream)
            .is_some_and(|state| state.is_upgraded())
    }

    fn stream_is_discarded(&mut self, stream: StreamId) -> bool {
        let upgraded_source_complete = self.upgraded_source_complete(stream);
        let Some(meta) = self.ingress_meta.get_mut(&stream) else {
            return upgraded_source_complete || self.stream_erased(stream);
        };
        if !meta.discard
            && (upgraded_source_complete
                || (self.core.direction().is_downstream()
                    && self.core.stream_if_present(stream).is_none()))
        {
            meta.discard = true;
        }
        meta.discard
    }

    fn rate_limit_breached(&mut self, kind: RateLimitKind) -> bool {
        self.core.direction().is_downstream()
            && self
                .http2
                .as_mut()
                .is_some_and(|http2| http2.record_rate(kind))
    }

    fn apply_header_rate_limit(
        &mut self,
        stream: StreamId,
        actions: &mut Vec<CodecCallbackAction>,
    ) -> bool {
        if !self.rate_limit_breached(RateLimitKind::HeaderFrames) {
            return false;
        }
        actions.push(CodecCallbackAction::SetParserPaused(true));
        match self.core.abort_stream_without_wire(
            stream,
            HttpError::with_message(HttpErrorCode::CANCEL, b"HTTP/2 header rate limit exceeded"),
        ) {
            Ok(update) => self.stream_updates.push(update),
            Err(error) => self.fail(stream, error),
        }
        self.drain_trigger = Some(DrainTrigger::HeaderRateLimit);
        true
    }

    fn deliver_push(
        &mut self,
        stream: StreamId,
        associated_stream: StreamId,
        mut source: HttpSourceHolder,
        message: roxygen_core::message::HttpMessage,
    ) {
        let source = match source.release() {
            Ok(Some(source)) => source,
            Ok(None) => {
                self.fail(stream, invalid_transition(b"pushed stream source is empty"));
                return;
            }
            Err(error) => {
                self.fail(
                    stream,
                    HttpError::with_message(
                        HttpErrorCode::INVALID_STATE_TRANSITION,
                        error.to_string().as_bytes(),
                    ),
                );
                return;
            }
        };
        match self.core.stream(associated_stream) {
            // A rejected promise stops the pushed source, whose completion resets it.
            Ok(parent) => {
                let _ = parent
                    .ingress_producer()
                    .push_promise(message, source, false);
            }
            Err(error) => self.fail(stream, error),
        }
    }

    fn begin_push(
        &mut self,
        stream: StreamId,
        associated_stream: StreamId,
        _message: Option<roxygen_core::message::HttpMessage>,
    ) {
        if let Err(error) = self.core.observe_request_begin(stream) {
            self.fail(stream, error);
            return;
        }
        if self.core.stream_if_present(associated_stream).is_none() {
            return;
        }
        self.core.mark_ingress_reconciliation_pending();
        self.ingress_meta.entry(stream).or_insert_with(|| {
            Box::new(IngressMeta {
                complete: false,
                discard: false,
                message: None,
                expected_content_length: None,
                received_body_length: 0,
                request_get: false,
                get_body_recorded: false,
            })
        });
        let push_limit_reached = self.http2.as_ref().is_some_and(|http2| {
            let active_pushes = http2
                .streams
                .values()
                .filter(|stream| stream.promised)
                .count() as u64;
            active_pushes >= self.configured_incoming_stream_limit()
        });
        if push_limit_reached {
            self.pending_pushes.insert(
                stream,
                PendingPush {
                    associated_stream,
                    source: HttpSourceHolder::new(Box::new(HttpErrorSource::new(
                        HttpError::with_message(
                            HttpErrorCode::REFUSED_STREAM,
                            b"peer push exceeds the configured stream limit",
                        ),
                    ))),
                },
            );
            return;
        }
        let request = match self.core.begin_pushed_stream(stream) {
            Ok(request) => request,
            Err(error) => {
                self.fail(stream, error);
                return;
            }
        };
        let mut source = request.into_source();
        source.set_read_timeout(self.stream_read_timeout);
        if let Some(http2) = self.http2.as_mut() {
            http2.insert_stream(stream, Default::default());
            http2.record_transaction_opened();
            if let Some(pushed) = http2.streams.get_mut(&stream) {
                pushed.parent = Some(associated_stream);
                pushed.promised = true;
            }
        }
        self.pending_pushes.insert(
            stream,
            PendingPush {
                associated_stream,
                source,
            },
        );
    }
}

impl HttpCodecCallback for SessionCodecCallback<'_> {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        if matches!(
            self.peer_goaway_generation,
            CallbackGoawayGeneration::Pending
        ) && let CodecEvent::GeneratedFrameHeader { frame_type, .. } = event
            && frame_type == FrameType::GOAWAY.as_u8()
        {
            self.peer_goaway_generation = CallbackGoawayGeneration::Generated;
        }
    }

    fn on_ingress_event(&mut self, event: CodecEvent<'_>) -> CodecCallbackActions {
        let mut actions = Vec::new();
        if self.core.protocol() == CodecProtocol::Http2
            && self
                .failures
                .iter()
                .any(|failure| failure.stream.is_unspecified())
        {
            return actions.into();
        }
        match event {
            CodecEvent::MessageBegin { stream, .. } => {
                self.core.mark_ingress_reconciliation_pending();
                self.ingress_meta.entry(stream).or_insert_with(|| {
                    Box::new(IngressMeta {
                        complete: false,
                        discard: false,
                        message: None,
                        expected_content_length: None,
                        received_body_length: 0,
                        request_get: false,
                        get_body_recorded: false,
                    })
                });
                if self.core.direction().is_downstream() {
                    if u64::from(self.core.num_incoming_streams())
                        >= self.configured_incoming_stream_limit()
                    {
                        if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                            meta.discard = true;
                        }
                        actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::Reset {
                            stream,
                            code: H2ErrorCode::REFUSED_STREAM,
                        }));
                        return actions.into();
                    }
                    if let Some(http2) = self.http2.as_mut()
                        && !http2.streams.contains_key(&stream)
                    {
                        http2.insert_stream(stream, Default::default());
                        http2.record_transaction_opened();
                    }
                    match self.core.begin_incoming_stream(stream) {
                        Ok(mut request) => {
                            if self.http2.is_none()
                                && let Some(http1) = self.http1_stats.as_mut()
                            {
                                http1.record_transaction_opened();
                            }
                            if self.http2.is_none()
                                && self.http1_stats.is_none()
                                && let Some(state) = self.core.stream_if_present(stream)
                            {
                                // Only the read that resumes parsing reports credit, unless
                                // stats need every read.
                                state
                                    .ingress_producer()
                                    .set_resume_threshold(Some(self.ingress_buffer_limit));
                            }
                            request
                                .source_mut()
                                .set_read_timeout(self.stream_read_timeout);
                            match self.handler.clone() {
                                Some(handler) => {
                                    if let Err(error) =
                                        self.core.start_request_handler(request, handler)
                                    {
                                        self.fail(stream, error);
                                    }
                                }
                                None => self.fail(
                                    stream,
                                    invalid_transition(b"request stream has no session handler"),
                                ),
                            }
                        }
                        Err(error) => self.fail(stream, error),
                    }
                    if self.core.protocol() == CodecProtocol::Http11
                        && self.core.num_incoming_streams() > 1
                    {
                        *self.anti_pipeline_paused = true;
                        actions.push(roxygen_codec::CodecCallbackAction::SetParserPaused(true));
                    }
                } else if self.stream_erased(stream) {
                    self.ingress_meta.remove(&stream);
                } else if self.core.protocol() == CodecProtocol::Http2
                    && self.core.stream_if_present(stream).is_none()
                    && let Some(meta) = self.ingress_meta.get_mut(&stream)
                {
                    meta.discard = true;
                }
            }
            CodecEvent::HeadersComplete {
                stream,
                mut message,
            } => {
                let keep_copy = self.core.protocol() == CodecProtocol::Http2;
                if request_has_forbidden_body(&mut message) {
                    if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                        meta.discard = true;
                        if keep_copy {
                            message.headers_mut().share_entries();
                            meta.message = Some((*message).clone());
                        }
                    }
                    let mut error = HttpError::with_message(
                        HttpErrorCode::HEADER_PARSE_ERROR,
                        b"RFC2616 request body is not allowed for this method",
                    );
                    error.set_http_message(Some(*message));
                    self.failures.push(CodecFailure {
                        stream,
                        error,
                        http_status: Some(400),
                        wire_code: None,
                        direction: HttpExceptionDirection::Ingress,
                        new_stream: true,
                    });
                    return actions.into();
                }
                if self.apply_header_rate_limit(stream, &mut actions) {
                    return actions.into();
                }
                if self.core.protocol() == CodecProtocol::Http2 {
                    self.core
                        .observe_lifecycle(LifecycleEvent::Http2IngressMessage { stream });
                }
                if let Some(pending) = self.pending_pushes.remove(&stream) {
                    self.deliver_push(stream, pending.associated_stream, pending.source, *message);
                    return actions.into();
                }
                if self.core.direction().is_downstream()
                    && message.method().ok().flatten() == Some(roxygen_core::http::HttpMethod::Head)
                {
                    if let Some(http2) = self.http2.as_mut() {
                        http2.head_requests.insert(stream);
                    }
                }
                if self.core.direction().is_downstream()
                    && let Some(priority) = message.get_http_priority()
                    && let Some(http2) = self.http2.as_mut()
                {
                    http2.update_priority(stream, priority);
                }
                if self.core.protocol() == CodecProtocol::Http11
                    && self.core.direction().is_downstream()
                    && !message.wants_keepalive()
                {
                    *self.close_after_ingress = true;
                    *self.ingress_close_stream = Some(stream);
                }
                let pending_upgrade = self.core.protocol() == CodecProtocol::Http11
                    && self.core.direction().is_downstream()
                    && (message.is_ingress_websocket_upgrade()
                        || message.method().ok().flatten()
                            == Some(roxygen_core::http::HttpMethod::Connect));
                if !self.stream_is_discarded(stream) {
                    if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                        meta.expected_content_length = if self.core.direction().is_downstream() {
                            checked_content_length(
                                &message,
                                self.core.empty_content_length_is_zero(),
                            )
                        } else {
                            None
                        };
                        meta.request_get = self.core.direction().is_downstream()
                            && message.method().ok().flatten()
                                == Some(roxygen_core::http::HttpMethod::Get);
                        if keep_copy {
                            message.headers_mut().share_entries();
                            meta.message = Some((*message).clone());
                        }
                    }
                    self.core.check_for_upgrade(stream, &message, true);
                    if let Err(error) = self.core.ingress_headers(stream, *message, false) {
                        self.fail(stream, error);
                    }
                }
                if pending_upgrade {
                    *self.upgrade_paused = true;
                    actions.push(roxygen_codec::CodecCallbackAction::SetParserPaused(true));
                }
            }
            CodecEvent::Body {
                stream,
                body,
                padding,
            } => {
                let body_length = body.chain_length();
                if body_length != 0
                    && let Some(pending) = self.pending_stats()
                {
                    pending.push(Http2StatsEvent::BufferedRead(signed_byte_count(
                        body_length,
                    )));
                }
                if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                    meta.received_body_length = meta
                        .received_body_length
                        .saturating_add(u64::try_from(body_length).unwrap_or(u64::MAX));
                }
                if self.stream_is_discarded(stream) {
                    if let Some(http2) = self.http2.as_mut() {
                        if body_length != 0 && http2.config.stats.is_some() {
                            http2.pending_stats.push(Http2StatsEvent::BufferedRead(
                                -signed_byte_count(body_length),
                            ));
                        }
                        self.rejected_body = true;
                        if !http2.return_rejected_connection_credit(body_length) {
                            self.fail(
                                StreamId::UNSPECIFIED,
                                HttpError::with_message(
                                    HttpErrorCode::FLOW_CONTROL_ERROR,
                                    b"rejected DATA connection credit overflowed",
                                ),
                            );
                        }
                    } else if body_length != 0
                        && let Some(pending) = self.pending_stats()
                    {
                        pending.push(Http2StatsEvent::BufferedRead(-signed_byte_count(
                            body_length,
                        )));
                    }
                } else {
                    match self.core.ingress_body(stream, body, padding, false) {
                        Ok(FlowControlState::Open | FlowControlState::Closed) => {
                            if body_length != 0 {
                                let record_get_body =
                                    self.ingress_meta.get_mut(&stream).is_some_and(|meta| {
                                        let record = meta.request_get && !meta.get_body_recorded;
                                        meta.get_body_recorded |= record;
                                        record
                                    });
                                if record_get_body && let Some(pending) = self.pending_stats() {
                                    pending.push(Http2StatsEvent::IngressGetBody);
                                }
                            }
                            if self.core.protocol() == CodecProtocol::Http11
                                && self.core.stream_if_present(stream).is_some_and(|state| {
                                    state.buffered_ingress_bytes() >= self.ingress_buffer_limit
                                })
                            {
                                *self.flow_control_paused = true;
                                actions.push(roxygen_codec::CodecCallbackAction::SetParserPaused(
                                    true,
                                ));
                            }
                        }
                        Ok(FlowControlState::Error) => {
                            if let Some(http2) = self.http2.as_mut() {
                                if body_length != 0 && http2.config.stats.is_some() {
                                    http2.pending_stats.push(Http2StatsEvent::BufferedRead(
                                        -signed_byte_count(body_length),
                                    ));
                                }
                                self.rejected_body = true;
                                let _ = http2.return_rejected_connection_credit(body_length);
                            } else if body_length != 0
                                && let Some(pending) = self.pending_stats()
                            {
                                pending.push(Http2StatsEvent::BufferedRead(-signed_byte_count(
                                    body_length,
                                )));
                            }
                            let invalid_state = self.core.protocol() == CodecProtocol::Http2
                                && self
                                    .ingress_meta
                                    .get(&stream)
                                    .is_some_and(|meta| meta.complete);
                            if !invalid_state {
                                let error = HttpError::with_message(
                                    HttpErrorCode::FLOW_CONTROL_ERROR,
                                    b"ingress source rejected body bytes",
                                );
                                // Not the window for HTTP/1.1: its body limit keeps body events
                                // within it.
                                if self.core.protocol() == CodecProtocol::Http11
                                    && self.core.direction().is_downstream()
                                    && !stream.is_unspecified()
                                {
                                    if self.failures.is_empty() {
                                        self.failures.push(CodecFailure {
                                            http_status: Some(400),
                                            ..CodecFailure::plain(stream, error)
                                        });
                                    }
                                } else {
                                    self.fail(stream, error);
                                }
                            } else if self.failures.is_empty() && !self.stream_is_upgraded(stream) {
                                let mut error = HttpError::with_message(
                                    HttpErrorCode::INVALID_STATE_TRANSITION,
                                    b"ingress source rejected body bytes",
                                );
                                error.set_http_message(
                                    self.ingress_meta
                                        .get(&stream)
                                        .and_then(|meta| meta.message.clone()),
                                );
                                self.failures.push(CodecFailure {
                                    stream,
                                    error,
                                    http_status: None,
                                    wire_code: Some(H2ErrorCode::PROTOCOL_ERROR),
                                    direction: HttpExceptionDirection::Ingress,
                                    new_stream: false,
                                });
                            }
                        }
                        Err(error) => {
                            if let Some(http2) = self.http2.as_mut() {
                                if body_length != 0 && http2.config.stats.is_some() {
                                    http2.pending_stats.push(Http2StatsEvent::BufferedRead(
                                        -signed_byte_count(body_length),
                                    ));
                                }
                                self.rejected_body = true;
                                let _ = http2.return_rejected_connection_credit(body_length);
                            } else if body_length != 0
                                && let Some(pending) = self.pending_stats()
                            {
                                pending.push(Http2StatsEvent::BufferedRead(-signed_byte_count(
                                    body_length,
                                )));
                            }
                            self.fail(stream, error);
                        }
                    }
                }
            }
            CodecEvent::TrailersComplete { stream, trailers } => {
                if !self.stream_is_discarded(stream) {
                    match self.core.stream(stream) {
                        Ok(stream_state) => {
                            if !stream_state.ingress_producer().trailers(*trailers)
                                && !self.stream_is_upgraded(stream)
                            {
                                self.fail(
                                    stream,
                                    invalid_transition(b"ingress source rejected trailers"),
                                );
                            }
                            if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                                meta.complete = true;
                            }
                        }
                        Err(error) => self.fail(stream, error),
                    }
                }
            }
            CodecEvent::MessageComplete { stream, upgraded } => {
                if self.core.direction().is_upstream() {
                    self.upstream_message_complete = true;
                }
                if upgraded {
                    self.upgrade_completed = true;
                    return actions.into();
                }
                if self.core.direction().is_downstream() && *self.close_after_ingress {
                    *self.close_after_ingress = false;
                    if self.abort_exchanges_on_eof {
                        *self.eof_watch = true;
                    } else {
                        *self.reads_closed = true;
                    }
                    actions.push(roxygen_codec::CodecCallbackAction::SetParserPaused(true));
                }
                let discarded = self.stream_is_discarded(stream);
                let already_complete = self
                    .ingress_meta
                    .get_mut(&stream)
                    .is_some_and(|meta| std::mem::replace(&mut meta.complete, true));
                if discarded {
                    self.ingress_meta.remove(&stream);
                } else if !already_complete
                    || self.core.stream(stream).is_ok_and(|state| {
                        !state.ingress_eom_seen() && !state.ingress_cancel_requested()
                    })
                {
                    match self.core.ingress_eom(stream) {
                        Err(error) => self.fail(stream, error),
                        Ok(_) => {
                            let mismatch = self.ingress_meta.get(&stream).is_some_and(|meta| {
                                meta.expected_content_length
                                    .is_some_and(|expected| expected != meta.received_body_length)
                            });
                            if mismatch && self.failures.is_empty() {
                                if let Some(pending) = self.pending_stats() {
                                    pending.push(Http2StatsEvent::ContentLengthMismatch);
                                }
                                // The source's `eom` fails only the source, and source
                                // completion does not reset an upgraded stream.
                                if self.stream_is_upgraded(stream) {
                                    return actions.into();
                                }
                                self.failures.push(CodecFailure {
                                    stream,
                                    error: HttpError::with_message(
                                        HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                                        b"ingress Content-Length did not match body bytes",
                                    ),
                                    http_status: None,
                                    wire_code: Some(H2ErrorCode::INTERNAL_ERROR),
                                    direction: HttpExceptionDirection::Ingress,
                                    new_stream: false,
                                });
                            }
                        }
                    }
                }
            }
            CodecEvent::Error {
                stream,
                error,
                new_stream,
            } => {
                if !stream.is_unspecified()
                    && error.codec_status_code().is_none()
                    && self.rate_limit_breached(RateLimitKind::DirectErrors)
                {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"HTTP/2 direct-error rate limit exceeded",
                        ),
                    );
                } else {
                    let mut partial_message = error.partial_message().cloned();
                    let code = if partial_message
                        .as_mut()
                        .is_some_and(request_has_forbidden_body)
                    {
                        HttpErrorCode::HEADER_PARSE_ERROR
                    } else {
                        http_exception_to_http_error_code(error)
                    };
                    let mut adapted = HttpError::with_message(code, error.message());
                    adapted.set_http_message(partial_message);
                    if self.failures.is_empty() {
                        self.failures.push(CodecFailure {
                            stream,
                            error: adapted,
                            http_status: error
                                .http_status_code()
                                .and_then(|status| u16::try_from(status).ok()),
                            wire_code: error.codec_status_code(),
                            direction: error.direction(),
                            new_stream,
                        });
                    }
                }
            }
            CodecEvent::Abort { stream, code } => {
                if self.rate_limit_breached(RateLimitKind::Resets) {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"HTTP/2 reset rate limit exceeded",
                        ),
                    );
                    return actions.into();
                }
                if let Some(meta) = self.ingress_meta.get_mut(&stream) {
                    meta.complete = true;
                }
                let local_code = h2_error_code_to_http_error_code(code);
                let local_code = if local_code == HttpErrorCode::NO_ERROR {
                    HttpErrorCode::CANCEL
                } else {
                    local_code
                };
                self.stream_byte_event_cancellations.push((
                    stream,
                    HttpError::with_message(local_code, b"peer reset cancelled local byte events"),
                ));
                match self.core.peer_reset(stream, local_code) {
                    Ok(Some(update)) => self.stream_updates.push(update),
                    Ok(None) => {}
                    Err(error) => self.fail(stream, error),
                }
            }
            CodecEvent::Goaway {
                last_good_stream,
                code,
                ..
            } => {
                if let Some(http2) = self.http2.as_mut() {
                    http2.received_goaway = true;
                    http2.last_good_stream = last_good_stream;
                }
                self.core.observe_lifecycle(LifecycleEvent::Http2Goaway {
                    last_good_stream,
                    code: h2_error_code_to_http_error_code(code),
                });
                let direction = self.core.direction();
                for stream in self
                    .core
                    .stream_ids()
                    .into_iter()
                    .filter(|stream| peer_goaway_refuses(*stream, last_good_stream, direction))
                {
                    match self.core.peer_reset(stream, HttpErrorCode::REFUSED_STREAM) {
                        Ok(Some(update)) => self.stream_updates.push(update),
                        Ok(None) => {}
                        Err(error) => self.fail(stream, error),
                    }
                }
                self.drain_trigger = Some(DrainTrigger::PeerGoaway);
                if code != H2ErrorCode::NO_ERROR {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            h2_error_code_to_http_error_code(code),
                            b"peer GOAWAY carried a connection error",
                        ),
                    );
                } else if self.core.initiate_drain_with(DrainTrigger::PeerGoaway) {
                    self.core.set_draining_ingress_grace(false);
                    self.drain_trigger = None;
                    self.peer_goaway_generation = CallbackGoawayGeneration::Pending;
                    actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::Goaway {
                        last_stream: StreamId::MAX,
                        code: H2ErrorCode::NO_ERROR,
                        debug_data: None,
                    }));
                }
            }
            CodecEvent::PushMessageBegin {
                stream,
                associated_stream,
                message,
            } => {
                self.begin_push(stream, associated_stream, message.cloned());
            }
            CodecEvent::PingRequest { data } => {
                if self.rate_limit_breached(RateLimitKind::ControlFrames) {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"HTTP/2 control-frame rate limit exceeded",
                        ),
                    );
                    return actions.into();
                }
                self.core
                    .observe_lifecycle(LifecycleEvent::Http2PingReplySent {
                        latency: Duration::ZERO,
                    });
                actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::PingReply {
                    data,
                }));
            }
            CodecEvent::WindowUpdate { stream, amount } => {
                if !stream.is_unspecified()
                    && self
                        .http2
                        .as_ref()
                        .is_some_and(|http2| !http2.streams.contains_key(&stream))
                {
                    return actions.into();
                }
                if !stream.is_unspecified()
                    && !self
                        .http2
                        .as_mut()
                        .is_some_and(|http2| http2.free_stream_send(stream, amount))
                {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                            b"stream send-window update overflowed",
                        ),
                    );
                }
                if let Some(http2) = self.http2.as_mut() {
                    http2.scheduler.enqueue(stream);
                }
            }
            CodecEvent::Settings { settings } => {
                if self.rate_limit_breached(RateLimitKind::ControlFrames) {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"HTTP/2 control-frame rate limit exceeded",
                        ),
                    );
                    return actions.into();
                }
                self.core.observe_lifecycle(LifecycleEvent::Http2Settings {
                    count: settings.len(),
                });
                for setting in settings.iter() {
                    self.core
                        .observe_lifecycle(LifecycleEvent::Http2Setting { setting: *setting });
                    match PeerSetting::from(*setting) {
                        PeerSetting::MaxConcurrentStreams(maximum) => {
                            let was_available = self.core.outgoing_capacity_available();
                            let effective = if let Some(http2) = self.http2.as_mut() {
                                http2.peer_max_concurrent_streams = maximum;
                                maximum.min(http2.config.max_concurrent_outgoing_streams)
                            } else {
                                maximum
                            };
                            let _ = self.core.set_max_concurrent_outgoing_streams(effective);
                            let is_available = self.core.outgoing_capacity_available();
                            if was_available != is_available {
                                self.core.observe_lifecycle(if is_available {
                                    LifecycleEvent::Http2StreamsNotFull
                                } else {
                                    LifecycleEvent::Http2StreamsFull
                                });
                            }
                        }
                        PeerSetting::EnablePush(enabled) => {
                            if let Some(http2) = self.http2.as_mut() {
                                http2.peer_push_enabled = enabled;
                            }
                        }
                        PeerSetting::InitialWindowSize(capacity) => {
                            if !self
                                .http2
                                .as_mut()
                                .is_some_and(|http2| http2.update_initial_send_window(capacity))
                            {
                                self.fail(
                                    StreamId::UNSPECIFIED,
                                    HttpError::with_message(
                                        HttpErrorCode::FLOW_CONTROL_ERROR,
                                        b"peer initial stream window is not representable",
                                    ),
                                );
                            }
                        }
                        PeerSetting::Other => {}
                    }
                }
                actions.push(CodecCallbackAction::Generate(OwnedEgressFrame::SettingsAck));
            }
            CodecEvent::FrameHeader { .. } => {}
            CodecEvent::PingReply { data } => {
                if let Some(prober) = self
                    .http2
                    .as_ref()
                    .and_then(|http2| http2.ping_prober.as_deref())
                {
                    prober.on_ping_reply(data);
                }
                self.core
                    .observe_lifecycle(LifecycleEvent::Http2PingReplyReceived);
            }
            CodecEvent::SettingsAck => {
                self.core
                    .observe_lifecycle(LifecycleEvent::Http2SettingsAck);
            }
            CodecEvent::Priority { .. } => {
                if self.rate_limit_breached(RateLimitKind::ControlFrames) {
                    self.fail(
                        StreamId::UNSPECIFIED,
                        HttpError::with_message(
                            HttpErrorCode::CANCEL,
                            b"HTTP/2 control-frame rate limit exceeded",
                        ),
                    );
                }
            }
            CodecEvent::ChunkHeader { .. }
            | CodecEvent::ChunkComplete { .. }
            | CodecEvent::PushPriority { .. }
            | CodecEvent::GeneratedFrameHeader { .. } => {}
        }
        actions.into()
    }

    fn num_outgoing_streams(&self) -> u32 {
        self.core.num_outgoing_streams()
    }

    fn num_incoming_streams(&self) -> u32 {
        self.core.num_incoming_streams()
    }

    fn ingress_body_limit(&self, stream: StreamId) -> Option<usize> {
        let state = self.core.stream_if_present(stream)?;
        let buffered = state.buffered_ingress_bytes() as usize;
        Some(self.http1_ingress_body_ceiling.saturating_sub(buffered))
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) enum WriteWakeReason {
    Work,
    SourceReady,
    ByteEventError(HttpError),
    FlowTimeout,
    BufferTimeout(StreamId),
    GoawayTimeout,
}

fn h2_flow_timeout(
    host: &Host,
    blocked: bool,
    write_timeout: Duration,
) -> Option<(Instant, LocalFuture)> {
    (blocked && !write_timeout.is_zero()).then(|| {
        let deadline = host.deadline_after(write_timeout);
        (deadline, host.sleep_until(deadline))
    })
}

struct SourceFlowWait {
    blocked: (Vec<StreamId>, bool),
    deadline: Instant,
}

fn source_flow_timeout(
    host: &Host,
    blocked: Option<&(Vec<StreamId>, bool)>,
    write_timeout: Duration,
    retained: &mut Option<SourceFlowWait>,
) -> Option<LocalFuture> {
    #[cfg(test)]
    writer_probe::note(|counts| counts.http2_setup += 1);
    let Some(blocked) = blocked else {
        *retained = None;
        return None;
    };
    if retained
        .as_ref()
        .is_some_and(|wait| wait.blocked != *blocked)
    {
        *retained = None;
    }
    if let Some(wait) = retained {
        return Some(host.sleep_until(wait.deadline));
    }
    let (deadline, timeout) = h2_flow_timeout(host, true, write_timeout)?;
    *retained = Some(SourceFlowWait {
        blocked: blocked.clone(),
        deadline,
    });
    Some(timeout)
}

#[cfg(test)]
mod hosted_flow_timeout_tests {
    use super::*;
    use crate::external_host_tests::ManualHost;
    use crate::external_host_tests::poll_once;

    #[test]
    fn source_flow_timer_retains_deadline_on_external_host() {
        let (_, clock, host) = ManualHost::create();
        let blocked = (vec![StreamId::new(1)], false);
        let mut retained = None;
        let first_deadline = host.now() + Duration::from_secs(2);
        let mut timer =
            source_flow_timeout(&host, Some(&blocked), Duration::from_secs(2), &mut retained)
                .unwrap();
        assert!(poll_once(timer.as_mut()).is_pending());
        clock.advance(Duration::from_secs(1));
        drop(timer);
        let mut timer =
            source_flow_timeout(&host, Some(&blocked), Duration::from_secs(2), &mut retained)
                .unwrap();
        assert_eq!(retained.as_ref().unwrap().deadline, first_deadline);
        assert!(poll_once(timer.as_mut()).is_pending());
        clock.advance(Duration::from_secs(1));
        assert!(poll_once(timer.as_mut()).is_ready());
        drop(timer);
        let changed = (vec![StreamId::new(3)], false);
        let mut timer =
            source_flow_timeout(&host, Some(&changed), Duration::from_secs(2), &mut retained)
                .unwrap();
        assert_eq!(
            retained.as_ref().unwrap().deadline,
            host.now() + Duration::from_secs(2)
        );
        assert!(poll_once(timer.as_mut()).is_pending());
        drop(timer);
        assert!(source_flow_timeout(&host, None, Duration::from_secs(2), &mut retained).is_none());
        assert!(retained.is_none());
        assert!(
            source_flow_timeout(&host, Some(&blocked), Duration::ZERO, &mut retained).is_none()
        );
    }
}

#[cfg(test)]
pub(crate) fn h2_flow_timeout_for_test(
    blocked: bool,
    write_timeout: Duration,
) -> Option<(Instant, LocalFuture)> {
    h2_flow_timeout(&Host::tokio(), blocked, write_timeout)
}

fn arbitrate_h2_timeout(
    selected: WriteWakeReason,
    flow_deadline: Option<Instant>,
    buffer_timeout: Option<(StreamId, Instant)>,
    now: Instant,
) -> WriteWakeReason {
    if matches!(selected, WriteWakeReason::FlowTimeout)
        && let (Some(flow_deadline), Some((stream, buffer_deadline))) =
            (flow_deadline, buffer_timeout)
        && buffer_deadline < flow_deadline
        && buffer_deadline <= now
    {
        return WriteWakeReason::BufferTimeout(stream);
    }
    selected
}

fn arbitrate_h2_timeout_at_current_time(
    host: &Host,
    selected: WriteWakeReason,
    flow_deadline: Option<Instant>,
    buffer_timeout: Option<(StreamId, Instant)>,
) -> WriteWakeReason {
    arbitrate_h2_timeout(selected, flow_deadline, buffer_timeout, host.now())
}

#[cfg(test)]
pub(crate) fn arbitrate_h2_timeout_at_current_time_for_test(
    selected: WriteWakeReason,
    flow_deadline: Option<Instant>,
    buffer_timeout: Option<(StreamId, Instant)>,
) -> WriteWakeReason {
    arbitrate_h2_timeout_at_current_time(&Host::tokio(), selected, flow_deadline, buffer_timeout)
}

#[cfg(test)]
pub(crate) fn arbitrate_h2_timeout_for_test(
    selected: WriteWakeReason,
    flow_deadline: Option<Instant>,
    buffer_timeout: Option<(StreamId, Instant)>,
    now: Instant,
) -> WriteWakeReason {
    arbitrate_h2_timeout(selected, flow_deadline, buffer_timeout, now)
}

async fn wait_for_optional_deadline(host: &Host, deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => host.sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn yield_source_turn(
    host: &Host,
    state: &Rc<RefCell<DriverState>>,
    cancellation: CancellationToken,
) {
    host.yield_to_io().await;
    #[cfg(test)]
    pause_source_turn_for_test(state, cancellation, true).await;
    #[cfg(not(test))]
    let _ = (state, cancellation);
}

#[cfg(test)]
async fn pause_source_turn_for_test(
    state: &Rc<RefCell<DriverState>>,
    cancellation: CancellationToken,
    yielded: bool,
) {
    let control = state
        .borrow()
        .h2_source_timer_probe
        .as_ref()
        .map(|probe| probe.control.clone());
    let Some(control) = control else {
        return;
    };
    let pause = if yielded {
        control.pause_next_source_yield.replace(false)
    } else {
        control.pause_next_source_service.replace(false)
    };
    if pause {
        control.source_yield_paused.set(true);
        control.changed.notify_one();
        let cancelled = CancellableBaton::new();
        tokio::select! {
            _ = control.resume_source_yield.notified() => {},
            _ = cancelled.wait(cancellation) => {},
        }
        control.source_yield_paused.set(false);
        control.changed.notify_one();
    }
}

/// HTTP/1.1 idle wait for the writer: one poll of each wake source in a fixed
/// order, stopping at the first that is ready, with `select!`'s cooperative
/// budget check. The HTTP/2-only timers of the multiplexed wait are never
/// armed on HTTP/1.1.
///
/// With `discard_stored_wake`, the writer reached this wait from its checks
/// without suspending and left no callback work, so a `write_wake` permit
/// stored before those checks has been handled. The first poll consumes such
/// a permit and waits for the next wake. Only the writer waits on
/// `write_wake`, and every notification runs on the session's worker.
fn h1_idle_wait<'a, C: Future<Output = WaitStatus>>(
    write_wake: &'a tokio::sync::Notify,
    mut notified: Pin<&mut Notified<'a>>,
    mut discard_stored_wake: bool,
    mut callback_wait: Pin<&mut C>,
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    read_wake: &tokio::sync::Notify,
) -> impl Future<Output = WriteWakeReason> {
    poll_fn(move |task_context| {
        let discard_stored_wake = std::mem::take(&mut discard_stored_wake);
        // Dropping the guard restores the budget, so, like `select!`'s gate,
        // this does not consume budget and defers the wake when the budget is spent.
        let Poll::Ready(unused_budget) = tokio::task::coop::poll_proceed(task_context) else {
            return Poll::Pending;
        };
        drop(unused_budget);
        let mut notified_ready = notified.as_mut().poll(task_context).is_ready();
        if notified_ready && discard_stored_wake {
            notified.set(write_wake.notified());
            notified_ready = notified.as_mut().poll(task_context).is_ready();
        }
        if notified_ready
            || callback_wait.as_mut().poll(task_context).is_ready()
            || poll_exchange_cleanup(state, observer, context, read_wake, task_context).is_ready()
        {
            return Poll::Ready(WriteWakeReason::Work);
        }
        match observer.poll_timeouts(context, task_context) {
            Ok(0) => Poll::Pending,
            Ok(_) => {
                state.borrow_mut().note_flow_timeout_activity();
                Poll::Ready(WriteWakeReason::Work)
            }
            Err(error) => Poll::Ready(WriteWakeReason::ByteEventError(error)),
        }
    })
}

/// Returns whether the writer should restart its loop instead of waiting.
#[cfg(test)]
async fn pause_writer_wait_for_test<F: Future<Output = WriteWakeReason>>(
    state: &Rc<RefCell<DriverState>>,
    mut native_wake: Pin<&mut F>,
    source_is_ready: bool,
) -> bool {
    let control = state
        .borrow()
        .h2_source_timer_probe
        .as_ref()
        .map(|probe| probe.control.clone());
    if let Some(control) = control
        && control.pause_next_writer_wait.get()
        && !source_is_ready
    {
        let polled = poll_fn(|context| Poll::Ready(native_wake.as_mut().poll(context))).await;
        match polled {
            Poll::Ready(WriteWakeReason::Work) => return true,
            Poll::Ready(_) => panic!("writer wait probe requires an idle session"),
            Poll::Pending => {}
        }
        control.pause_next_writer_wait.set(false);
        control.writer_wait_paused.set(true);
        control.changed.notify_one();
        control.resume_writer_wait.notified().await;
        control.writer_wait_paused.set(false);
    }
    false
}

/// Test-only counts of writer waits and the work they build.
#[cfg(test)]
pub(crate) mod writer_probe {
    use std::cell::Cell;
    use std::future::Future;
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::Waker;

    use roxygen_runtime::cancellation::CancellationToken;

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct WriterCounts {
        /// Idle waits the writer entered.
        pub(crate) idle_waits: usize,
        /// Idle waits that returned a wake reason.
        pub(crate) idle_wakes: usize,
        /// Writer cancellation-waker updates that reach the token's registry:
        /// a listener install, or a replacement of a cached waker that would
        /// not wake the new one.
        pub(crate) cancellation_registry_updates: usize,
        /// Boxed `pending_event_wait` futures built while the writer builds or
        /// polls an idle wait.
        pub(crate) idle_pending_event_waits: usize,
        /// Runs of HTTP/2-only flow, buffer, and stall timer setup.
        pub(crate) http2_setup: usize,
        /// Write batches that built their callback wait and wake `select!`.
        pub(crate) batch_selects: usize,
        /// Write batches whose transport write completed on its first poll.
        pub(crate) first_poll_writes: usize,
        /// Passes through the top of the writer loop.
        pub(crate) loop_passes: usize,
        /// Polls of the transport-loop join.
        pub(crate) join_passes: usize,
        /// The join pass in which the read loop completed.
        pub(crate) read_done_pass: Option<usize>,
        /// The join pass in which the write loop completed.
        pub(crate) write_done_pass: Option<usize>,
        /// Polls of the session's cleanup task.
        pub(crate) cleanup_polls: usize,
        /// The cleanup poll in which the write loop completed.
        pub(crate) write_done_cleanup_poll: Option<usize>,
        /// The cleanup poll in which the cleanup task completed.
        pub(crate) cleanup_done_poll: Option<usize>,
    }

    thread_local! {
        static COUNTS: Cell<WriterCounts> = Cell::new(WriterCounts::default());
        static IN_IDLE_WAIT: Cell<bool> = const { Cell::new(false) };
    }

    /// Marks the writer's idle wait as active on this thread until dropped.
    pub(crate) struct IdleWaitScope {
        previous: bool,
    }

    impl IdleWaitScope {
        pub(crate) fn enter() -> Self {
            Self {
                previous: IN_IDLE_WAIT.replace(true),
            }
        }
    }

    impl Drop for IdleWaitScope {
        fn drop(&mut self) {
            IN_IDLE_WAIT.set(self.previous);
        }
    }

    /// Polls `wait` with each poll inside an [`IdleWaitScope`].
    pub(crate) async fn idle_scoped<F: Future>(wait: F) -> F::Output {
        let mut wait = pin!(wait);
        poll_fn(|context| {
            let _scope = IdleWaitScope::enter();
            wait.as_mut().poll(context)
        })
        .await
    }

    pub(crate) fn note_pending_event_wait() {
        if IN_IDLE_WAIT.get() {
            note(|counts| counts.idle_pending_event_waits += 1);
        }
    }

    /// Mirrors a `CancellationWakeRegistration`'s cached waker to count the
    /// updates that reach its token's registry.
    pub(crate) struct RegistryMirror {
        token: CancellationToken,
        cached: Option<Waker>,
    }

    impl RegistryMirror {
        pub(crate) fn new(token: CancellationToken) -> Self {
            Self {
                token,
                cached: None,
            }
        }

        pub(crate) fn note_update(&mut self, waker: &Waker) {
            if !self.token.can_be_cancelled() || self.token.is_cancellation_requested() {
                self.cached = None;
                return;
            }
            if self
                .cached
                .as_ref()
                .is_some_and(|cached| cached.will_wake(waker))
            {
                return;
            }
            self.cached = Some(waker.clone());
            note(|counts| counts.cancellation_registry_updates += 1);
        }
    }

    pub(crate) fn note(update: impl FnOnce(&mut WriterCounts)) {
        COUNTS.with(|counts| {
            let mut current = counts.get();
            update(&mut current);
            counts.set(current);
        });
    }

    pub(crate) fn snapshot() -> WriterCounts {
        COUNTS.with(Cell::get)
    }

    pub(crate) fn reset() {
        COUNTS.with(|counts| counts.set(WriterCounts::default()));
    }

    pub(crate) fn note_join_pass(read_completed: bool, write_completed: bool) {
        note(|counts| {
            counts.join_passes += 1;
            if read_completed {
                counts.read_done_pass = Some(counts.join_passes);
            }
            if write_completed {
                counts.write_done_pass = Some(counts.join_passes);
                counts.write_done_cleanup_poll = Some(counts.cleanup_polls);
            }
        });
    }
}

/// Counts polls of a session cleanup task for [`writer_probe`].
#[cfg(test)]
async fn count_cleanup_polls(cleanup: impl Future<Output = ()>) {
    let mut cleanup = pin!(cleanup);
    poll_fn(|context| {
        writer_probe::note(|counts| counts.cleanup_polls += 1);
        let poll = cleanup.as_mut().poll(context);
        if poll.is_ready() {
            writer_probe::note(|counts| counts.cleanup_done_poll = Some(counts.cleanup_polls));
        }
        poll
    })
    .await;
}

/// The writer's write-cancellation listener, kept across its idle waits.
struct WriterCancellationWake {
    registration: CancellationWakeRegistration,
    #[cfg(test)]
    mirror: writer_probe::RegistryMirror,
}

impl WriterCancellationWake {
    fn new(token: &CancellationToken) -> Self {
        Self {
            registration: token.wake_registration(),
            #[cfg(test)]
            mirror: writer_probe::RegistryMirror::new(token.clone()),
        }
    }

    fn update_waker(&mut self, waker: &Waker) -> bool {
        #[cfg(test)]
        self.mirror.note_update(waker);
        self.registration.update_waker(waker)
    }
}

#[cold]
#[inline(never)]
fn cancel_writer_for_worker_shutdown(
    state: &Rc<RefCell<DriverState>>,
    write_wake: &tokio::sync::Notify,
    read_cancellation: &CancellationSource,
) {
    fail_connection(
        state,
        write_wake,
        HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"worker shutdown cancelled the session writer",
        ),
        true,
    );
    read_cancellation.request_cancellation();
}

async fn write_loop(
    state: Rc<RefCell<DriverState>>,
    transport: Rc<dyn UniplexTransport>,
    observer: Rc<AsyncSocketByteEventObserver>,
    context: WorkerContext,
    config: UniplexConfig,
    shared: Rc<UniplexShared>,
) {
    let UniplexShared {
        read_wake,
        write_wake,
        read_cancellation,
        write_cancellation,
        closing,
        ..
    } = &*shared;
    let source_ready = state
        .borrow()
        .http2
        .as_ref()
        .map(|http2| Arc::clone(&http2.body_source_publications.source_ready));
    let is_http2 = source_ready.is_some();
    let callbacks = state.borrow().core().callback_queue();
    let mut write_cancellation_wake = WriterCancellationWake::new(&write_cancellation.token());
    let mut skipped_sources = BTreeSet::new();
    let mut body_source_cursor = None;
    let mut source_flow_wait = None;
    let mut retire_selected_close = false;
    loop {
        #[cfg(test)]
        writer_probe::note(|counts| counts.loop_passes += 1);
        let mut pass_suspended = false;
        if context.is_cancelled() {
            cancel_writer_for_worker_shutdown(&state, write_wake, read_cancellation);
            break;
        }
        flush_http2_flow_transition(&state);
        if let Err(error) = drive_core_callbacks(&state, read_wake) {
            fail_connection(&state, write_wake, error, true);
        }
        sync_buffered_egress_stats(&state);
        flush_session_stats(&state);
        state
            .borrow_mut()
            .byte_event_bridges
            .retain(|bridge| bridge.pending.get() != 0);
        cancel_pending_byte_events(&state, &observer, &context);

        poll_fn(|task_context| {
            let _ = poll_exchange_cleanup(&state, &observer, &context, read_wake, task_context);
            Poll::Ready(())
        })
        .await;

        let selected_close_drained = state.borrow().h1_selected_close_bytes_drained();
        if selected_close_drained {
            retire_selected_close = true;
            break;
        }

        if state.borrow().reset_after_draining_writes && state.borrow().egress.is_empty() {
            let cancelled_work = {
                let mut state = state.borrow_mut();
                state.take_egress_work()
            };
            drop_items_individually(cancelled_work);
            break;
        }

        if is_http2 {
            let serviced = poll_fn(|task_context| {
                Poll::Ready(poll_body_sources(
                    &state,
                    &mut body_source_cursor,
                    task_context,
                ))
            })
            .await;
            if serviced && let Err(error) = drive_core_callbacks(&state, read_wake) {
                fail_connection(&state, write_wake, error, true);
            }
            if serviced {
                if has_ready_body_source(&state.borrow()) {
                    yield_source_turn(context.host(), &state, write_cancellation.token()).await;
                } else {
                    #[cfg(test)]
                    pause_source_turn_for_test(&state, write_cancellation.token(), false).await;
                }
            }
        }

        #[cfg(any(test, feature = "test-hooks"))]
        {
            let hold = state.borrow_mut().writer_hold.take();
            if let Some(release) = hold {
                release.notified().await;
            }
        }

        let can_stage_egress = {
            let state = state.borrow();
            !state.zero_byte_write_pending
                && state.egress.chain_length() < config.maximum_write_size
        };
        if can_stage_egress {
            let (wait_for_source, http1_queued) = {
                let state = state.borrow();
                let http1 = state.http2.is_none();
                (
                    http1 && state.egress.is_empty(),
                    (http1 && !state.egress.is_empty()).then(|| state.egress.remaining_capacity()),
                )
            };
            let work = state.borrow_mut().pop_ready_egress_work(&skipped_sources);
            if let Some(work) = work {
                let source_identity = match &work {
                    EgressWork::Transfer {
                        stream,
                        transfer,
                        readiness: Some(_),
                        ..
                    } => Some((*stream, transfer.generation())),
                    _ => None,
                };
                let work_stream = match &work {
                    EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => {
                        *stream
                    }
                };
                pass_suspended = true;
                let flush_pending_egress = match next_egress_event(
                    &state,
                    work,
                    &observer,
                    &context,
                    read_wake,
                    write_wake,
                    write_cancellation.token(),
                    wait_for_source,
                )
                .await
                {
                    Ok(NextEgressEvent::Ready(stream, event))
                        if http1_queued
                            .is_some_and(|remaining| !http1_event_fits(&event, remaining)) =>
                    {
                        // Batched HTTP/1.1: the event follows the queued bytes once
                        // they are written, as it would unbatched.
                        source_flow_wait = None;
                        state
                            .borrow_mut()
                            .egress_work
                            .push_front(EgressWork::Event { stream, event });
                        true
                    }
                    Ok(NextEgressEvent::Ready(stream, event)) => {
                        source_flow_wait = None;
                        if let Err(error) = serialize_event(
                            &state,
                            &observer,
                            &context,
                            SerializationOrigin::Writer(read_wake),
                            stream,
                            event,
                        ) {
                            fail_connection(&state, write_wake, error, true);
                        }
                        false
                    }
                    Ok(NextEgressEvent::Finished) => {
                        source_flow_wait = None;
                        false
                    }
                    Ok(outcome @ (NextEgressEvent::Pending | NextEgressEvent::Interrupted)) => {
                        if matches!(outcome, NextEgressEvent::Interrupted) {
                            source_flow_wait = None;
                        }
                        if let Some(identity) = source_identity {
                            skipped_sources.insert(identity);
                            state.borrow().ready_egress_work(&skipped_sources).is_none()
                        } else {
                            true
                        }
                    }
                    Err(error) => {
                        source_flow_wait = None;
                        if state.borrow().codec.protocol() == CodecProtocol::Http2
                            && !work_stream.is_unspecified()
                        {
                            if let Err(callback_error) = drive_core_callbacks(&state, read_wake) {
                                fail_connection(&state, write_wake, callback_error, true);
                            }
                        } else if state.borrow().codec.protocol() == CodecProtocol::Http11
                            && !work_stream.is_unspecified()
                        {
                            // With `non_abortive_close`, a body error abandons the unwritten
                            // output, as hyper drops its unflushed buffer with the connection.
                            let abandon_output = config.non_abortive_close
                                && state
                                    .borrow()
                                    .core()
                                    .exchange_tracker(work_stream)
                                    .is_some_and(|exchange| exchange.egress_started());
                            {
                                let mut driver = state.borrow_mut();
                                driver.record_error(error.clone());
                                if driver.byte_event_cancel_error.is_none() {
                                    driver.byte_event_cancel_error = Some(error.clone());
                                }
                            }
                            let mut first_panic = None;
                            let mut callback_error = None;
                            capture_panic(&mut first_panic, || {
                                callback_error = drive_core_callbacks(&state, read_wake).err();
                            });
                            capture_panic(&mut first_panic, || {
                                fail_connection(&state, write_wake, error, true);
                            });
                            if let Some(callback_error) = callback_error {
                                capture_panic(&mut first_panic, || {
                                    fail_connection(&state, write_wake, callback_error, true);
                                });
                            }
                            if abandon_output {
                                let unwritten = state.borrow_mut().egress.take();
                                drop(unwritten);
                            }
                            resume_first_panic(first_panic);
                        } else {
                            fail_connection(&state, write_wake, error, true);
                        }
                        false
                    }
                };
                if !flush_pending_egress {
                    continue;
                }
            }
        }

        if !state.borrow().egress.is_empty() || state.borrow().zero_byte_write_pending {
            if let Err(error) = write_one_batch(
                WriteBatchContext {
                    state: &state,
                    observer: &observer,
                    context: &context,
                    read_wake,
                    write_wake,
                    body_source_cursor: &mut body_source_cursor,
                },
                &transport,
                config,
                write_cancellation.token(),
            )
            .await
            {
                let byte_event_error = error.clone();
                fail_connection(&state, write_wake, error, true);
                let _ = observer.cancel_events(&context, &byte_event_error);
                state.borrow_mut().byte_event_cancel_error.take();
                read_cancellation.request_cancellation();
                break;
            }
            skipped_sources.clear();
            source_flow_wait = None;
            continue;
        }

        if !state.borrow().should_continue_writing() {
            let generated_final_goaway = {
                let mut state = state.borrow_mut();
                let should_generate = state.core().direction().is_downstream()
                    && state
                        .http2
                        .as_ref()
                        .is_some_and(|http2| http2.sent_initial_goaway && !http2.sent_final_goaway);
                should_generate && generate_goaway(&mut state, H2ErrorCode::NO_ERROR).is_ok()
            };
            if generated_final_goaway {
                continue;
            }
            break;
        }

        if !skipped_sources.is_empty() {
            pass_suspended = true;
            yield_source_turn(context.host(), &state, write_cancellation.token()).await;
            skipped_sources.clear();
        }
        #[cfg(test)]
        run_writer_wait_hook(&state, |hooks| hooks.before.take());
        #[cfg(test)]
        let construction_scope = writer_probe::IdleWaitScope::enter();
        let callback_wait =
            callbacks.wait_registered(|waker| write_cancellation_wake.update_waker(waker));
        let notified = write_wake.notified();
        #[cfg(test)]
        drop(construction_scope);
        #[cfg(test)]
        writer_probe::note(|counts| counts.idle_waits += 1);
        let blocked;
        let buffer_timeout;
        let flow_deadline;
        let wake_reason = if let Some(source_ready) = &source_ready {
            let timeout_poll = poll_byte_event_timeouts(Rc::clone(&observer), context.clone());
            blocked = state.borrow().h2_blocked_streams();
            buffer_timeout = state.borrow().h2_buffer_timeout(config.write_timeout);
            if let Some((streams, false)) = &blocked {
                if let Some(http2) = state.borrow_mut().http2.as_mut() {
                    for stream in streams {
                        http2.mark_stream_stalled(*stream);
                    }
                }
                flush_session_stats(&state);
            }
            let goaway_deadline = state
                .borrow()
                .http2
                .as_ref()
                .and_then(|http2| http2.goaway_deadline);
            if std::mem::take(&mut state.borrow_mut().flow_timeout_activity) {
                source_flow_wait = None;
            }
            let flow_timeout = source_flow_timeout(
                context.host(),
                blocked.as_ref(),
                config.write_timeout,
                &mut source_flow_wait,
            );
            flow_deadline = source_flow_wait.as_ref().map(|wait| wait.deadline);
            #[cfg(test)]
            state.borrow().record_h2_timeout_arm(flow_deadline);
            #[cfg(test)]
            {
                let changed = state.borrow().record_h2_source_timers(
                    flow_deadline,
                    buffer_timeout.map(|(key, deadline)| (key.stream, deadline)),
                    blocked.as_ref(),
                );
                if let Some(changed) = changed {
                    changed.notify_one();
                }
            }
            let source_is_ready = {
                let state = state.borrow();
                state.http2.is_some() && state.ready_egress_work(&skipped_sources).is_some()
            };
            let native_wake = async {
                if let Some(flow_timeout) = flow_timeout {
                    let mut flow_timeout = std::pin::pin!(flow_timeout);
                    tokio::select! {
                        _ = callback_wait => WriteWakeReason::Work,
                        _ = notified => WriteWakeReason::Work,
                        _ = poll_fn(|task_context| poll_exchange_cleanup(&state, &observer, &context, read_wake, task_context)) => WriteWakeReason::Work,
                        result = timeout_poll => match result {
                            Ok(()) => {
                                state.borrow_mut().note_flow_timeout_activity();
                                WriteWakeReason::Work
                            },
                            Err(error) => WriteWakeReason::ByteEventError(error),
                        },
                        _ = flow_timeout.as_mut() => WriteWakeReason::FlowTimeout,
                        _ = wait_for_optional_deadline(context.host(), buffer_timeout.map(|(_, deadline)| deadline)) => {
                            WriteWakeReason::BufferTimeout(
                                buffer_timeout.expect("an armed buffer timeout has a stream").0.stream,
                            )
                        },
                        _ = wait_for_optional_deadline(context.host(), goaway_deadline) => WriteWakeReason::GoawayTimeout,
                    }
                } else {
                    tokio::select! {
                        _ = callback_wait => WriteWakeReason::Work,
                        _ = notified => WriteWakeReason::Work,
                        _ = poll_fn(|task_context| poll_exchange_cleanup(&state, &observer, &context, read_wake, task_context)) => WriteWakeReason::Work,
                        result = timeout_poll => match result {
                            Ok(()) => {
                                state.borrow_mut().note_flow_timeout_activity();
                                WriteWakeReason::Work
                            },
                            Err(error) => WriteWakeReason::ByteEventError(error),
                        },
                        _ = wait_for_optional_deadline(context.host(), buffer_timeout.map(|(_, deadline)| deadline)) => {
                            WriteWakeReason::BufferTimeout(
                                buffer_timeout.expect("an armed buffer timeout has a stream").0.stream,
                            )
                        },
                        _ = wait_for_optional_deadline(context.host(), goaway_deadline) => WriteWakeReason::GoawayTimeout,
                    }
                }
            };
            #[cfg(test)]
            let mut native_wake = Box::pin(writer_probe::idle_scoped(native_wake));
            #[cfg(test)]
            if pause_writer_wait_for_test(&state, native_wake.as_mut(), source_is_ready).await {
                continue;
            }
            tokio::select! {
                biased;
                result = native_wake => result,
                () = std::future::ready(()), if source_is_ready => WriteWakeReason::SourceReady,
                _ = source_ready.notified() => WriteWakeReason::SourceReady,
            }
        } else {
            blocked = None;
            buffer_timeout = None;
            flow_deadline = None;
            let discard_stored_wake =
                !pass_suspended && !state.borrow().has_pending_callback_work();
            let mut notified = pin!(notified);
            let mut callback_wait = pin!(callback_wait);
            let native_wake = h1_idle_wait(
                write_wake,
                notified.as_mut(),
                discard_stored_wake,
                callback_wait.as_mut(),
                &state,
                &observer,
                &context,
                read_wake,
            );
            #[cfg(test)]
            let mut native_wake = Box::pin(writer_probe::idle_scoped(native_wake));
            #[cfg(test)]
            if pause_writer_wait_for_test(&state, native_wake.as_mut(), false).await {
                continue;
            }
            native_wake.await
        };
        #[cfg(test)]
        writer_probe::note(|counts| counts.idle_wakes += 1);
        #[cfg(test)]
        run_writer_wait_hook(&state, |hooks| hooks.after.take());
        if !is_http2
            || !matches!(
                wake_reason,
                WriteWakeReason::SourceReady | WriteWakeReason::Work
            )
        {
            source_flow_wait = None;
        }
        let wake_reason = if matches!(wake_reason, WriteWakeReason::FlowTimeout) {
            let current_buffer_timeout =
                state.borrow().revalidate_h2_buffer_timeout(buffer_timeout);
            arbitrate_h2_timeout_at_current_time(
                context.host(),
                wake_reason,
                flow_deadline,
                current_buffer_timeout.map(|(key, deadline)| (key.stream, deadline)),
            )
        } else {
            wake_reason
        };
        if let WriteWakeReason::ByteEventError(error) = wake_reason {
            fail_connection(&state, write_wake, error, true);
            read_cancellation.request_cancellation();
            break;
        }
        if matches!(wake_reason, WriteWakeReason::GoawayTimeout) {
            let (generated, cancel_read) = {
                let mut state = state.borrow_mut();
                let generated = generate_goaway(&mut state, H2ErrorCode::NO_ERROR);
                let cancel_read = generated.is_ok() && !state.should_continue_reading();
                (generated, cancel_read)
            };
            match generated {
                Ok(()) => {
                    if cancel_read {
                        read_cancellation.request_cancellation();
                    }
                    read_wake.notify_one();
                }
                Err(error) => fail_connection(&state, write_wake, error, true),
            }
            continue;
        }
        if let WriteWakeReason::BufferTimeout(stream) = wake_reason {
            if expire_body_source(
                &state,
                buffer_timeout.filter(|(key, _)| key.stream == stream),
            ) && let Err(error) = drive_core_callbacks(&state, read_wake)
            {
                fail_connection(&state, write_wake, error, true);
            }
            continue;
        }
        if matches!(wake_reason, WriteWakeReason::FlowTimeout) {
            let (streams, connection_blocked) =
                blocked.expect("a flow timeout is armed only for blocked work");
            if connection_blocked {
                // Blocked work that changed since the snapshot, such as an
                // aborted stream's, re-arms the timer instead.
                if state.borrow().h2_blocked_streams() != Some((streams, true)) {
                    continue;
                }
                fail_connection(
                    &state,
                    write_wake,
                    HttpError::with_message(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"HTTP/2 connection flow-control timeout",
                    ),
                    true,
                );
            } else {
                for stream in streams {
                    // A stream reset since the blocked snapshot does not get a second reset.
                    let already_reset = state
                        .borrow()
                        .core()
                        .stream_if_present(stream)
                        .is_some_and(|stream| {
                            stream.egress_complete() && stream.terminal_error().is_some()
                        });
                    if already_reset {
                        continue;
                    }
                    let mut core = DetachedSessionCore::take(&state);
                    let update = core.core_mut().reset_stream_state(
                        stream,
                        HttpError::with_message(
                            HttpErrorCode::FLOW_CONTROL_ERROR,
                            b"HTTP/2 stream flow-control timeout",
                        ),
                        crate::session::ResetOrigin::Ingress,
                    );
                    core.restore();
                    if let Ok(update) = update {
                        apply_stream_update(&state, update, read_wake);
                    }
                }
            }
            flush_session_stats(&state);
            continue;
        }
        if write_cancellation.token().is_cancellation_requested() {
            break;
        }
    }
    drop(write_cancellation_wake);
    drop(callbacks);

    closing.set(true);
    let mut first_panic =
        initiate_drain_after_state_borrow(&state, DrainTrigger::CloseWhenIdle).panic;
    capture_panic(&mut first_panic, || {
        cancel_pending_byte_events(&state, &observer, &context)
    });
    let reset_before_close = state.borrow().reset_after_draining_writes;
    match catch_unwind(AssertUnwindSafe(|| {
        if reset_before_close {
            reset_transport_once(&state, &transport, &context)
        } else {
            transport.shutdown_write(&context)
        }
    })) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => state.borrow_mut().record_error(error),
        Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
    }
    if retire_selected_close {
        collect_close_wait_panic(
            &mut first_panic,
            pin!(Some(wait_for_byte_event_refs(
                &state, &observer, &context, write_wake
            ))),
        )
        .await;
        match catch_unwind(AssertUnwindSafe(|| {
            retire_h1_selected_close(&state, read_cancellation, read_wake)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => retain_h1_close_panic(
                &mut first_panic,
                catch_unwind(AssertUnwindSafe(|| {
                    fail_connection(&state, write_wake, error, true)
                }))
                .err(),
            ),
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    let pending_push_error = state.borrow().fatal_error.clone().unwrap_or_else(|| {
        HttpError::with_message(
            HttpErrorCode::CANCEL,
            b"session close cancelled an incomplete push promise",
        )
    });
    match catch_unwind(AssertUnwindSafe(|| {
        cancel_pending_pushes(&state, read_wake, &pending_push_error)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => state.borrow_mut().record_error(error),
        Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
    }
    collect_close_wait_panic(
        &mut first_panic,
        pin!(Some(wait_for_all_streams(
            &state, read_wake, &observer, &context
        ))),
    )
    .await;
    capture_panic(&mut first_panic, || {
        cancel_pending_byte_events(&state, &observer, &context)
    });
    let reset_before_byte_events = state.borrow().reset_after_draining_writes;
    if !reset_before_close && reset_before_byte_events {
        match catch_unwind(AssertUnwindSafe(|| {
            reset_transport_once(&state, &transport, &context)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => state.borrow_mut().record_error(error),
            Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
        }
    }
    if !reset_before_byte_events {
        collect_close_wait_panic(
            &mut first_panic,
            pin!(Some(wait_for_byte_event_refs(
                &state, &observer, &context, write_wake
            ))),
        )
        .await;
    }
    capture_panic(&mut first_panic, || {
        cancel_pending_byte_events(&state, &observer, &context)
    });
    if !reset_before_byte_events && state.borrow().reset_after_draining_writes {
        match catch_unwind(AssertUnwindSafe(|| {
            reset_transport_once(&state, &transport, &context)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => state.borrow_mut().record_error(error),
            Err(payload) => retain_first_panic(&mut first_panic, Some(payload)),
        }
    }
    state.borrow_mut().writes_finished = true;
    capture_panic(&mut first_panic, || {
        read_cancellation.request_cancellation();
    });
    capture_panic(&mut first_panic, || read_wake.notify_one());
    resume_first_panic(first_panic);
}

enum NextEgressEvent {
    Ready(StreamId, EgressEvent),
    Finished,
    Pending,
    Interrupted,
}

/// Like [`catch_future_unwind`], retaining the first panic of the wait or of
/// its in-place drop.
async fn collect_close_wait_panic<F: Future<Output = ()>>(
    first: &mut Option<PanicPayload>,
    mut slot: Pin<&mut Option<F>>,
) {
    let panic = poll_fn(|context| {
        match catch_unwind(AssertUnwindSafe(|| {
            slot.as_mut()
                .as_pin_mut()
                .expect("polled after completion")
                .poll(context)
        })) {
            Ok(poll) => poll.map(|()| None),
            Err(payload) => Poll::Ready(Some(payload)),
        }
    })
    .await;
    retain_h1_close_panic(first, panic);
    retain_h1_close_panic(
        first,
        catch_unwind(AssertUnwindSafe(|| slot.set(None))).err(),
    );
}

fn retain_h1_close_panic(first: &mut Option<PanicPayload>, next: Option<PanicPayload>) {
    if first.is_none() {
        *first = next;
    } else if let Some(payload) = next {
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            roxygen_runtime::panic::discard_panic(secondary);
        }
    }
}

fn retire_h1_close_work(work: EgressWork, error: &HttpError, first: &mut Option<PanicPayload>) {
    let mut work = work;
    let registrations = match &mut work {
        EgressWork::Event { event, .. } => match event {
            EgressEvent::Headers(headers) => headers.take_byte_event_registrations(),
            EgressEvent::Body(body) => body.take_byte_event_registrations(),
        },
        EgressWork::Transfer { .. } => Vec::new(),
    };
    for mut registration in registrations {
        retain_h1_close_panic(
            first,
            catch_unwind(AssertUnwindSafe(|| registration.cancel(error, None))).err(),
        );
        retain_h1_close_panic(
            first,
            catch_unwind(AssertUnwindSafe(|| drop(registration))).err(),
        );
    }
    retain_h1_close_panic(first, catch_unwind(AssertUnwindSafe(|| drop(work))).err());
}

fn retire_h1_selected_close(
    state: &Rc<RefCell<DriverState>>,
    read_cancellation: &CancellationSource,
    read_wake: &tokio::sync::Notify,
) -> Result<(), HttpError> {
    let (streams, queued, exchanges, close_reader) = {
        let mut driver = state.borrow_mut();
        if !driver.h1_selected_close_bytes_drained() {
            return Ok(());
        }
        let streams = driver
            .core()
            .stream_ids()
            .into_iter()
            .filter(|stream| Some(*stream) != driver.upgrade_stream_awaiting_detach)
            .collect::<Vec<_>>();
        let protected = driver.upgrade_stream_awaiting_detach;
        if streams.is_empty()
            && driver.egress_work.is_empty()
            && (driver.reads_closed || protected.is_some())
        {
            return Ok(());
        }
        let close_reader = protected.is_none();
        if close_reader {
            driver.reads_closed = true;
        }
        for stream in &streams {
            if let Some(meta) = driver.ingress_meta.get_mut(stream) {
                meta.discard = true;
            }
        }
        let work = std::mem::take(&mut driver.egress_work);
        let mut queued = VecDeque::with_capacity(work.len());
        for item in work {
            let stream = match &item {
                EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => *stream,
            };
            let completed_transfer =
                matches!(&item, EgressWork::Transfer { transfer, .. } if transfer.is_complete());
            if Some(stream) == protected && !completed_transfer {
                driver.egress_work.push_back(item);
            } else {
                queued.push_back(item);
            }
        }
        let mut affected = streams.iter().copied().collect::<BTreeSet<_>>();
        affected.extend(queued.iter().map(|work| match work {
            EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => *stream,
        }));
        let exchanges = affected
            .into_iter()
            .filter_map(|stream| driver.core().exchange_tracker(stream))
            .collect::<Vec<_>>();
        (streams, queued, exchanges, close_reader)
    };
    let error = HttpError::with_message(
        HttpErrorCode::CORO_CANCELLED,
        b"connection closed after an earlier response",
    );
    let mut first_panic = None;
    let mut first_error = None;
    let mut updates = Vec::with_capacity(streams.len());
    let mut retirements = Vec::with_capacity(streams.len());
    let guards = exchanges
        .iter()
        .map(|exchange| exchange.guard())
        .collect::<Vec<_>>();
    for stream in streams {
        match catch_unwind(AssertUnwindSafe(|| {
            let mut driver = state.borrow_mut();
            driver
                .core_mut()
                .prepare_abort_stream_without_wire(stream, error.clone())
        })) {
            Ok(Ok(retirement)) => {
                updates.push(retirement.update());
                retirements.push(retirement);
            }
            Ok(Err(error)) => {
                state.borrow_mut().record_error(error.clone());
                first_error.get_or_insert(error);
            }
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    for retirement in &mut retirements {
        match catch_unwind(AssertUnwindSafe(|| retirement.prepare_queues())) {
            Ok(panic) => retain_h1_close_panic(&mut first_panic, panic),
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    for retirement in retirements {
        match catch_unwind(AssertUnwindSafe(|| retirement.retire())) {
            Ok(result) => retain_h1_close_panic(&mut first_panic, result.err()),
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    for update in updates {
        retain_h1_close_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| {
                apply_stream_update(state, update, read_wake)
            }))
            .err(),
        );
    }
    for work in queued {
        retire_h1_close_work(work, &error, &mut first_panic);
    }
    if close_reader {
        retain_h1_close_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| {
                read_cancellation.request_cancellation()
            }))
            .err(),
        );
    }
    retain_h1_close_panic(
        &mut first_panic,
        catch_unwind(AssertUnwindSafe(|| read_wake.notify_one())).err(),
    );
    retain_h1_close_panic(
        &mut first_panic,
        catch_unwind(AssertUnwindSafe(|| flush_lifecycle_events(state))).err(),
    );
    if first_panic.is_some() || first_error.is_some() {
        for exchange in &exchanges {
            exchange.fail();
        }
    }
    for guard in guards {
        retain_h1_close_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| drop(guard))).err(),
        );
        if first_panic.is_some() {
            for exchange in &exchanges {
                exchange.fail();
            }
        }
    }
    resume_first_panic(first_panic);
    first_error.map_or(Ok(()), Err)
}

const MAX_BODY_SOURCES_PER_TURN: usize = 64;

fn body_source_is_ready(work: &QueuedEgressTransfer) -> bool {
    work.transfer.is_reading_body()
        && !work.transfer.is_complete()
        && (work.transfer.cancellation_requested()
            || !work.transfer.generation_is_current()
            || work
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancellation_requested)
            || (work.transfer.can_read_source()
                && work
                    .readiness
                    .as_ref()
                    .is_none_or(EgressSourceReadiness::is_ready)))
}

fn ready_body_source_candidates(http2: &Http2DriverState) -> BodySourceCandidates {
    // A concurrent wake publishes into the new map, never the snapshot being rejected.
    let mut candidates = http2.body_source_publications.take();
    candidates.retain(|key, published| {
        http2.egress_queue(key.stream).is_some_and(|queue| {
            queue.iter().any(|work| {
                work.transfer.generation() == key.generation
                    && work.readiness.as_ref().is_some_and(|readiness| {
                        std::ptr::eq(published.as_ptr(), Arc::as_ptr(&readiness.wake))
                            && readiness.wake.active.load(Ordering::Relaxed)
                    })
                    && body_source_is_ready(work)
            })
        })
    });
    if !candidates.is_empty() {
        let mut current = http2.body_source_publications.candidates();
        // The immutable driver borrow keeps each validated readiness owner alive through union.
        for (key, wake) in &candidates {
            current.entry(*key).or_insert_with(|| wake.clone());
        }
    }
    candidates
}

fn has_ready_body_source(state: &DriverState) -> bool {
    state
        .http2
        .as_ref()
        .is_some_and(|http2| !ready_body_source_candidates(http2).is_empty())
}

fn ready_body_source_keys(
    state: &DriverState,
    after: Option<EgressTransferKey>,
) -> Vec<EgressTransferKey> {
    let Some(http2) = &state.http2 else {
        return Vec::new();
    };
    let candidates = ready_body_source_candidates(http2);
    if candidates.is_empty() {
        return Vec::new();
    }
    let start = after
        .and_then(|key| {
            http2
                .egress_order
                .iter()
                .position(|candidate| *candidate == key)
        })
        .map_or(0, |position| position + 1);
    http2
        .egress_order
        .iter()
        .cycle()
        .skip(start)
        .take(http2.egress_order.len())
        .filter(|key| candidates.contains_key(*key))
        .take(MAX_BODY_SOURCES_PER_TURN)
        .copied()
        .collect()
}

fn poll_body_sources(
    state: &Rc<RefCell<DriverState>>,
    cursor: &mut Option<EgressTransferKey>,
    task_context: &mut Context<'_>,
) -> bool {
    let keys = ready_body_source_keys(&state.borrow(), *cursor);
    let serviced = !keys.is_empty();
    if !serviced {
        return false;
    }
    // Preserve the existing relative serialization/retirement order even when a
    // source callback removes adjacent generations while this owner is checked out.
    let order = state
        .borrow()
        .http2
        .as_ref()
        .expect("ready H2 sources")
        .egress_order
        .clone();
    for key in keys {
        *cursor = Some(key);
        let checked_out = {
            let mut driver = state.borrow_mut();
            driver.http2.as_mut().and_then(|http2| {
                let position = http2
                    .egress_order
                    .iter()
                    .position(|candidate| *candidate == key)?;
                http2.egress_order.remove(position);
                Some(http2.take_transfer(key))
            })
        };
        let Some(EgressWork::Transfer {
            stream,
            mut transfer,
            cancellation,
            mut readiness,
        }) = checked_out
        else {
            continue;
        };
        let live = state
            .borrow()
            .core()
            .stream(stream)
            .is_ok_and(|stream| !stream.egress_complete());
        if !live || !transfer.generation_is_current() || transfer.cancellation_requested() {
            continue;
        }
        let result = match &mut readiness {
            Some(readiness) => readiness.poll_with(|context| transfer.poll_body_source(context)),
            None => transfer.poll_body_source(task_context),
        };
        let live = state
            .borrow()
            .core()
            .stream(stream)
            .is_ok_and(|stream| !stream.egress_complete());
        if matches!(result, Poll::Ready(Err(_)))
            || transfer.is_complete()
            || !live
            || !transfer.generation_is_current()
            || transfer.cancellation_requested()
        {
            continue;
        }
        let work = QueuedEgressTransfer {
            transfer,
            cancellation,
            readiness,
        };
        let mut driver = state.borrow_mut();
        let http2 = driver
            .http2
            .as_mut()
            .expect("checked-out H2 source retains its driver");
        let queue = match http2.streams.get_mut(&stream) {
            Some(state) => &mut state.egress_work,
            None => http2.orphaned_egress.entry(stream).or_default(),
        };
        let successors = || {
            order
                .iter()
                .skip_while(move |candidate| **candidate != key)
                .skip(1)
        };
        let queue_position = successors()
            .filter(|next| next.stream == stream)
            .find_map(|next| {
                queue
                    .iter()
                    .position(|work| work.transfer.generation() == next.generation)
            })
            .unwrap_or(queue.len());
        queue.insert(queue_position, work);
        let position = successors()
            .find_map(|next| {
                http2
                    .egress_order
                    .iter()
                    .position(|candidate| candidate == next)
            })
            .unwrap_or(http2.egress_order.len());
        http2.egress_order.insert(position, key);
        queue[queue_position].publish_body_source();
    }
    serviced
}

fn expire_body_source(
    state: &Rc<RefCell<DriverState>>,
    candidate: Option<(EgressTransferKey, Instant)>,
) -> bool {
    let work = {
        let mut driver = state.borrow_mut();
        let Some((key, deadline)) = driver.revalidate_h2_buffer_timeout(candidate) else {
            return false;
        };
        if deadline
            > driver
                .http2
                .as_ref()
                .expect("validated H2 timeout")
                .host
                .now()
        {
            return false;
        }
        let http2 = driver.http2.as_mut().expect("validated H2 timeout");
        let position = http2
            .egress_order
            .iter()
            .position(|candidate| *candidate == key)
            .expect("validated queued generation");
        http2.egress_order.remove(position);
        http2.take_transfer(key)
    };
    if let EgressWork::Transfer { mut transfer, .. } = work {
        transfer.timeout_buffer_space();
    }
    true
}

struct ReadyEgressDispatch<'a> {
    state: &'a Rc<RefCell<DriverState>>,
    observer: &'a AsyncSocketByteEventObserver,
    context: &'a WorkerContext,
    read_wake: &'a tokio::sync::Notify,
    write_wake: &'a tokio::sync::Notify,
}

impl ReadyEgressDispatch<'_> {
    fn poll(
        &mut self,
        task_context: &mut Context<'_>,
        transfer: &mut EgressTransfer,
        readiness: Option<&mut EgressSourceReadiness>,
        maximum_body: Option<u32>,
    ) -> Option<Option<Result<Option<EgressEvent>, HttpError>>> {
        if poll_exchange_cleanup(
            self.state,
            self.observer,
            self.context,
            self.read_wake,
            task_context,
        )
        .is_ready()
        {
            return Some(None);
        }
        match self.observer.poll_timeouts(self.context, task_context) {
            Ok(0) => {}
            result => {
                if let Err(error) = result {
                    fail_connection(self.state, self.write_wake, error, true);
                }
                return Some(None);
            }
        }
        let result = match readiness {
            Some(readiness) => readiness
                .poll_with(|context| transfer.poll_next_event_with_credit(context, maximum_body)),
            // HTTP/1.1 batching's attempt that does not wait.
            None => {
                let wake = {
                    let mut state = self.state.borrow_mut();
                    match &state.batched_source_wake {
                        Some(wake) if wake.wakes_task_of(task_context) => Arc::clone(wake),
                        _ => Arc::clone(state.batched_source_wake.insert(Arc::new(
                            SourceWakeCounter::new(task_context.waker().clone()),
                        ))),
                    }
                };
                transfer.poll_next_event_until_woken(&wake, task_context, maximum_body)
            }
        };
        match result {
            Poll::Ready(result) => Some(Some(result)),
            Poll::Pending => {
                let notified = std::pin::pin!(self.write_wake.notified());
                if notified.poll(task_context).is_ready() {
                    Some(None)
                } else {
                    None
                }
            }
        }
    }
}

/// Whether an HTTP/1.1 codec surely serializes `event` within `remaining`
/// bytes: body bytes or padding (none on HTTP/1.1) with the longest chunk
/// framing and message end they can take. The size of a head or
/// trailers is known only once they are serialized, so they never join queued
/// bytes.
fn http1_event_fits(event: &EgressEvent, remaining: usize) -> bool {
    let EgressEvent::Body(body) = event else {
        return false;
    };
    let length = match body.kind() {
        HttpBodyEventKind::Body(data) => data.chain_length(),
        HttpBodyEventKind::Padding(_) => 0,
        _ => return false,
    };
    // `<hex length>\r\n` opens a chunk.
    let chunk_prefix = if length == 0 {
        0
    } else {
        (usize::BITS - length.leading_zeros()).div_ceil(4) as usize + 2
    };
    // The chunk's CRLF, then the last chunk at the end of the message.
    let suffix = if body.eom() { 7 } else { 2 };
    length
        .checked_add(chunk_prefix + suffix)
        .is_some_and(|framed| framed <= remaining)
}

async fn next_egress_event(
    state: &Rc<RefCell<DriverState>>,
    work: EgressWork,
    observer: &Rc<AsyncSocketByteEventObserver>,
    context: &WorkerContext,
    read_wake: &tokio::sync::Notify,
    write_wake: &tokio::sync::Notify,
    cancellation: CancellationToken,
    wait_for_source: bool,
) -> Result<NextEgressEvent, HttpError> {
    let stream = match &work {
        EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => *stream,
    };
    if state.borrow().h1_close_rejects_egress(stream) {
        state.borrow_mut().egress_work.push_back(work);
        return Ok(NextEgressEvent::Finished);
    }
    match work {
        EgressWork::Event { stream, event } => Ok(NextEgressEvent::Ready(stream, event)),
        EgressWork::Transfer {
            stream,
            mut transfer,
            cancellation: transfer_cancellation,
            mut readiness,
        } => {
            let maximum_body = {
                let state = state.borrow();
                state.http2.as_ref().map_or(Some(64 << 10), |http2| {
                    let connection_credit = state.codec.connection_send_credit();
                    (connection_credit != 0).then(|| {
                        connection_credit
                            .min(http2.stream_send_credit(stream))
                            .min(64 << 10)
                    })
                })
            };
            // A nonwaiting attempt stays on the owner through its first poll.
            // Ready callback/cancellation notifications retain their consuming wait.
            // On HTTP/1.1 a nonwaiting attempt is batching's, made before every
            // flush of a streamed response, so it does not build a wait.
            let use_ready_dispatch = {
                let state = state.borrow();
                !wait_for_source
                    && tokio::task::coop::has_budget_remaining()
                    && !cancellation.is_cancellation_requested()
                    && !state.core().has_pending_events()
                    && if state.http2.is_some() {
                        state.ready_dispatch && readiness.is_some()
                    } else {
                        transfer_cancellation
                            .as_ref()
                            .is_none_or(|token| !token.is_cancellation_requested())
                    }
            };
            let event = if use_ready_dispatch {
                let mut dispatch = ReadyEgressDispatch {
                    state,
                    observer,
                    context,
                    read_wake,
                    write_wake,
                };
                poll_fn(|task_context| {
                    Poll::Ready(dispatch.poll(
                        task_context,
                        &mut transfer,
                        readiness.as_mut(),
                        maximum_body,
                    ))
                })
                .await
            } else {
                let callback_wait = {
                    let state = state.borrow();
                    state.core().pending_event_wait(cancellation.clone())
                };
                let cancellation_baton = CancellableBaton::new();
                let transfer_cancel = transfer_cancellation
                    .clone()
                    .unwrap_or_else(|| cancellation.clone());
                let separately_wait_for_cancellation =
                    transfer_cancellation.is_some() && readiness.is_none();
                let mut source_read = Box::pin(transfer.next_event_with_credit(maximum_body));
                let mut selected = Box::pin(async {
                    tokio::select! {
                        biased;
                        status = cancellation_baton.wait(transfer_cancel), if separately_wait_for_cancellation => {
                            if status == WaitStatus::Cancelled {
                                Some(Err(HttpError::with_message(
                                    HttpErrorCode::CORO_CANCELLED,
                                    b"request body production cancelled",
                                )))
                            } else {
                                None
                            }
                        },
                        _ = callback_wait => None,
                        _ = poll_fn(|task_context| poll_exchange_cleanup(state, observer, context, read_wake, task_context)) => None,
                        result = poll_byte_event_timeouts(Rc::clone(observer), context.clone()) => {
                            if let Err(error) = result {
                                fail_connection(state, write_wake, error, true);
                            }
                            None
                        },
                        result = poll_fn(|task_context| {
                            match &mut readiness {
                                Some(readiness) => readiness.poll(source_read.as_mut()),
                                None => source_read.as_mut().poll(task_context),
                            }
                        }) => Some(result),
                        _ = write_wake.notified() => None,
                    }
                });
                let event = if wait_for_source {
                    Some(selected.as_mut().await)
                } else {
                    poll_fn(|context| {
                        Poll::Ready(match selected.as_mut().poll(context) {
                            Poll::Ready(event) => Some(event),
                            Poll::Pending => None,
                        })
                    })
                    .await
                };
                drop(selected);
                drop(source_read);
                event
            };
            let Some(event) = event else {
                let stream_accepts_egress = state
                    .borrow()
                    .core()
                    .stream(stream)
                    .is_ok_and(|stream| !stream.egress_complete());
                if stream_accepts_egress && !transfer.cancellation_requested() {
                    state.borrow_mut().return_egress_transfer(
                        stream,
                        QueuedEgressTransfer {
                            transfer,
                            cancellation: transfer_cancellation,
                            readiness,
                        },
                    );
                }
                return Ok(NextEgressEvent::Pending);
            };
            match event {
                Some(Ok(Some(event))) => {
                    state.borrow_mut().return_egress_transfer(
                        stream,
                        QueuedEgressTransfer {
                            transfer,
                            cancellation: transfer_cancellation,
                            readiness,
                        },
                    );
                    Ok(NextEgressEvent::Ready(stream, event))
                }
                Some(Ok(None)) => {
                    let stream_accepts_egress = state
                        .borrow()
                        .core()
                        .stream(stream)
                        .is_ok_and(|stream| !stream.egress_complete());
                    if !transfer.is_complete()
                        && stream_accepts_egress
                        && !transfer.cancellation_requested()
                    {
                        state.borrow_mut().return_egress_transfer(
                            stream,
                            QueuedEgressTransfer {
                                transfer,
                                cancellation: transfer_cancellation,
                                readiness,
                            },
                        );
                        Ok(NextEgressEvent::Pending)
                    } else {
                        Ok(NextEgressEvent::Finished)
                    }
                }
                Some(Err(_)) if transfer.cancellation_requested() => Ok(NextEgressEvent::Finished),
                Some(Err(error)) => Err(error),
                None => {
                    let stream_accepts_egress = state
                        .borrow()
                        .core()
                        .stream(stream)
                        .is_ok_and(|stream| !stream.egress_complete());
                    if stream_accepts_egress && !transfer.cancellation_requested() {
                        state.borrow_mut().return_egress_transfer(
                            stream,
                            QueuedEgressTransfer {
                                transfer,
                                cancellation: transfer_cancellation,
                                readiness,
                            },
                        );
                    }
                    Ok(if wait_for_source {
                        NextEgressEvent::Finished
                    } else {
                        NextEgressEvent::Interrupted
                    })
                }
            }
        }
    }
}

enum SerializationOrigin<'a> {
    Writer(&'a tokio::sync::Notify),
    Committed(&'a UniplexHandle),
}

impl SerializationOrigin<'_> {
    fn retain_error(&self, state: &Rc<RefCell<DriverState>>, error: &HttpError) {
        if let Self::Committed(handle) = self {
            handle.retain_committed_header_error(state, error);
        }
    }
}

fn serialize_event(
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    origin: SerializationOrigin<'_>,
    stream: StreamId,
    event: EgressEvent,
) -> Result<(), HttpError> {
    let committed = matches!(origin, SerializationOrigin::Committed(_));
    if state.borrow().h1_close_rejects_egress(stream) {
        state
            .borrow_mut()
            .egress_work
            .push_back(EgressWork::Event { stream, event });
        return Ok(());
    }
    if matches!(event, EgressEvent::Headers(_))
        && let Ok(stream) = state.borrow().core().stream(stream)
    {
        stream.exchange_tracker().mark_egress_started();
    }
    enum PreparedEvent {
        Headers {
            event: roxygen_core::events::HttpHeaderEvent,
            final_headers: bool,
            eom: bool,
        },
        Body {
            kind: HttpBodyEventKind,
            eom: bool,
        },
    }

    let (mut prepared, mut registrations) = match event {
        EgressEvent::Headers(mut headers) => {
            let mut registrations = headers.take_byte_event_registrations();
            let final_headers = match headers.is_final() {
                Ok(final_headers) => final_headers,
                Err(source_error) => {
                    let error = HttpError::with_message(
                        HttpErrorCode::MESSAGE_ERROR,
                        source_error.to_string().as_bytes(),
                    );
                    origin.retain_error(state, &error);
                    cancel_registrations(&mut registrations, &error);
                    return Err(error);
                }
            };
            let eom = headers.eom();
            (
                PreparedEvent::Headers {
                    event: headers,
                    final_headers,
                    eom,
                },
                registrations,
            )
        }
        EgressEvent::Body(mut body_event) => {
            let eom = body_event.eom();
            let mut registrations = body_event.take_byte_event_registrations();
            let kind = body_event.into_kind();
            if matches!(&kind, HttpBodyEventKind::Suspend(_)) {
                let error = invalid_transition(b"a suspended event escaped the egress queue");
                origin.retain_error(state, &error);
                cancel_registrations(&mut registrations, &error);
                return Err(error);
            }
            (PreparedEvent::Body { kind, eom }, registrations)
        }
    };
    sync_buffered_egress_stats(state);
    let dequeued_body_length = match &prepared {
        PreparedEvent::Body {
            kind: HttpBodyEventKind::Body(body),
            ..
        } => body.chain_length(),
        _ => 0,
    };
    if dequeued_body_length != 0 {
        let mut state = state.borrow_mut();
        if let Ok(stream_state) = state.core().stream(stream) {
            stream_state.record_egress_dequeued(dequeued_body_length);
        }
        if let Some(buffered) = state.app_buffered_egress.get_mut(&stream) {
            *buffered = buffered.saturating_sub(signed_byte_count(dequeued_body_length));
        }
        if let Some(pending) = pending_session_stats(&mut state) {
            pending.push(Http2StatsEvent::BufferedWrite(-signed_byte_count(
                dequeued_body_length,
            )));
        }
    }
    if !committed {
        flush_session_stats(state);
    }
    let schedule_body_bytes = matches!(prepared, PreparedEvent::Body { .. });
    let mut callback = NoopCodecCallback;
    let mut field_section_info = None;
    let mut encoded_header_callback = None;
    let mut resolved_downstream_upgrade = false;
    let mut accepted_downstream_upgrade = false;
    let mut drain_trigger = None;
    let serialized = (|| -> Result<(bool, ByteEventOffsets), HttpError> {
        let mut state = state.borrow_mut();
        let (generated, eom, body_length) = match &mut prepared {
            PreparedEvent::Headers {
                event: headers,
                final_headers,
                eom,
            } => {
                if let Some(priority) = headers.headers().get_http_priority() {
                    if let Some(http2) = &mut state.http2 {
                        http2.update_priority(stream, priority);
                        http2.scheduler.enqueue(stream);
                    }
                }
                resolved_downstream_upgrade = state.core().direction().is_downstream()
                    && state.upgrade_paused
                    && *final_headers;
                let closes_after_response = state.core().direction().is_downstream()
                    && *final_headers
                    && (!headers.headers().wants_keepalive()
                        || state.ingress_close_stream == Some(stream)
                        || resolved_downstream_upgrade);
                let downstream_final = state.core().direction().is_downstream() && *final_headers;
                state
                    .core_mut()
                    .check_for_upgrade(stream, headers.headers(), false);
                let DriverState { codec, egress, .. } = &mut *state;
                let generated = codec.generate(
                    egress,
                    EgressFrame::Headers {
                        stream,
                        message: headers.headers(),
                        end_stream: *eom,
                        extra_headers: None,
                    },
                    &mut callback,
                )?;
                if resolved_downstream_upgrade {
                    accepted_downstream_upgrade = codec
                        .resolve_downstream_upgrade(headers.headers())
                        .unwrap_or(false);
                }
                if codec.protocol() == CodecProtocol::Http11 {
                    if downstream_final && !codec.is_reusable() {
                        state.close_egress_boundary.get_or_insert(stream);
                    }
                } else if closes_after_response {
                    state.close_egress_boundary = Some(stream);
                }
                if let Some(size) = generated.header_size {
                    field_section_info = Some(FieldSectionInfo {
                        section_type: FieldSectionType::Headers,
                        final_headers: *final_headers,
                        size,
                    });
                    encoded_header_callback = Some(size);
                }
                if *eom {
                    arm_response_timeout(&mut state, stream);
                }
                let response_headers =
                    state
                        .core_mut()
                        .response_headers(stream, *final_headers, *eom);
                let (_, update) = response_headers?;
                drain_trigger = apply_stream_update_locked(&mut state, update);
                (generated, *eom, 0)
            }
            PreparedEvent::Body { kind, eom } => {
                let (generated, body_length) = match kind {
                    HttpBodyEventKind::Body(body) => {
                        if state
                            .http2
                            .as_ref()
                            .is_some_and(|http2| http2.head_requests.contains(&stream))
                        {
                            let generated = if *eom {
                                let DriverState { codec, egress, .. } = &mut *state;
                                codec.generate(
                                    egress,
                                    EgressFrame::EndOfMessage { stream },
                                    &mut callback,
                                )?
                            } else {
                                GeneratedFrame::default()
                            };
                            *body = BufQueue::new(0);
                            (generated, 0)
                        } else {
                            let body_length = body.chain_length() as u64;
                            if let Some(http2) = &mut state.http2 {
                                let amount = u32::try_from(body_length).map_err(|_| {
                                    HttpError::with_message(
                                        HttpErrorCode::FLOW_CONTROL_ERROR,
                                        b"HTTP/2 body chunk exceeds window accounting",
                                    )
                                })?;
                                if !http2.reserve_stream_send(stream, amount) {
                                    return Err(HttpError::with_message(
                                        HttpErrorCode::FLOW_CONTROL_ERROR,
                                        b"HTTP/2 stream send window is exhausted",
                                    ));
                                }
                            }
                            let body = std::mem::replace(body, BufQueue::new(0));
                            let DriverState { codec, egress, .. } = &mut *state;
                            (
                                codec.generate(
                                    egress,
                                    EgressFrame::Body {
                                        stream,
                                        body,
                                        padding: None,
                                        end_stream: *eom,
                                    },
                                    &mut callback,
                                )?,
                                body_length,
                            )
                        }
                    }
                    HttpBodyEventKind::Trailers(trailers) => {
                        let DriverState { codec, egress, .. } = &mut *state;
                        let generated = codec.generate(
                            egress,
                            EgressFrame::Trailers { stream, trailers },
                            &mut callback,
                        )?;
                        if let Some(size) = generated.header_size {
                            field_section_info = Some(FieldSectionInfo {
                                section_type: FieldSectionType::Trailers,
                                final_headers: true,
                                size,
                            });
                        }
                        (generated, 0)
                    }
                    HttpBodyEventKind::Padding(length) => {
                        let DriverState { codec, egress, .. } = &mut *state;
                        (
                            codec.generate(
                                egress,
                                EgressFrame::Padding {
                                    stream,
                                    length: *length,
                                },
                                &mut callback,
                            )?,
                            0,
                        )
                    }
                    HttpBodyEventKind::PushPromise(push) => {
                        let push_permitted = state.http2.as_ref().is_some_and(|http2| {
                            http2.peer_push_enabled
                                && state.core().num_outgoing_streams()
                                    < http2
                                        .peer_max_concurrent_streams
                                        .min(http2.config.max_concurrent_outgoing_streams)
                        });
                        if !push_permitted {
                            field_section_info = Some(FieldSectionInfo {
                                section_type: FieldSectionType::PushPromise,
                                final_headers: true,
                                size: Default::default(),
                            });
                            (GeneratedFrame::default(), 0)
                        } else {
                            let pushed_stream = state.codec.create_stream();
                            let source = push.move_push_source().ok_or_else(|| {
                                invalid_transition(b"HTTP/2 push promise has no source")
                            })?;
                            let promise = push.promise().clone();
                            let transfer = state.core_mut().start_outgoing_push(
                                pushed_stream,
                                promise,
                                source,
                            )?;
                            let priority = push.promise().get_http_priority().unwrap_or_default();
                            let http2 = state
                                .http2
                                .as_mut()
                                .expect("HTTP/2 state remains installed");
                            http2.insert_stream(pushed_stream, priority);
                            http2.record_transaction_opened();
                            if let Some(pushed) = http2.streams.get_mut(&pushed_stream) {
                                pushed.parent = Some(stream);
                                pushed.promised = true;
                            }
                            let DriverState { codec, egress, .. } = &mut *state;
                            let generated = codec.generate(
                                egress,
                                EgressFrame::PushPromise {
                                    stream: pushed_stream,
                                    associated_stream: stream,
                                    message: push.promise(),
                                    end_stream: false,
                                },
                                &mut callback,
                            )?;
                            if let Some(size) = generated.header_size {
                                field_section_info = Some(FieldSectionInfo {
                                    section_type: FieldSectionType::PushPromise,
                                    final_headers: true,
                                    size,
                                });
                            }
                            state.enqueue_transfer(pushed_stream, transfer, None);
                            (generated, 0)
                        }
                    }
                    HttpBodyEventKind::Upgrade => (GeneratedFrame::default(), 0),
                    HttpBodyEventKind::Suspend(_) => {
                        unreachable!("suspended egress was rejected before serialization")
                    }
                };
                if *eom {
                    arm_response_timeout(&mut state, stream);
                    let update = state.core_mut().egress_finished(stream)?;
                    drain_trigger = apply_stream_update_locked(&mut state, update);
                }
                (generated, *eom, body_length)
            }
        };

        if eom && state.reset_after_error_response == Some(stream) {
            state.reset_after_error_response = None;
            if state.codec.protocol() == CodecProtocol::Http11 {
                state.reset_after_draining_writes = true;
            } else {
                let DriverState { codec, egress, .. } = &mut *state;
                codec.generate(
                    egress,
                    EgressFrame::Reset {
                        stream,
                        code: H2ErrorCode::CANCEL,
                    },
                    &mut callback,
                )?;
            }
        }

        let offsets = state.egress_meta.entry(stream).or_default();
        offsets.stream_offset = offsets
            .stream_offset
            .saturating_add(generated.bytes_written as u64);
        let stream_offset = offsets.stream_offset;
        offsets.body_offset = offsets.body_offset.saturating_add(body_length);
        let body_offset = offsets.body_offset;
        let session_byte_offset = state
            .bytes_scheduled
            .saturating_add(state.egress.chain_length() as u64);
        if generated.bytes_written == 0 && !registrations.is_empty() {
            state.zero_byte_write_pending = true;
        }
        if schedule_body_bytes && let Some(http2) = &mut state.http2 {
            http2.scheduler.consume(stream, generated.bytes_written);
        }
        Ok((
            eom,
            ByteEventOffsets {
                stream_id: stream,
                session_byte_offset,
                session_bytes_scheduled: state.bytes_scheduled,
                stream_offset,
                body_offset,
            },
        ))
    })();
    let (eom, offsets) = match serialized {
        Ok(serialized) => serialized,
        Err(error) => {
            origin.retain_error(state, &error);
            cancel_registrations(&mut registrations, &error);
            flush_lifecycle_events(state);
            return Err(error);
        }
    };
    let mut first_panic = None;
    let mut registration_error = None;
    if committed {
        capture_panic(&mut first_panic, || {
            registration_error = observer
                .register_byte_events(
                    context,
                    offsets,
                    field_section_info,
                    std::mem::take(&mut registrations),
                    eom,
                )
                .err();
        });
        if let Some(error) = &registration_error {
            origin.retain_error(state, error);
        }
    }
    if let Some(trigger) = drain_trigger {
        retain_first_panic(
            &mut first_panic,
            initiate_drain_after_state_borrow(state, trigger).panic,
        );
    }
    capture_panic(&mut first_panic, || drop_deferred_work(state));
    if resolved_downstream_upgrade {
        let mut state = state.borrow_mut();
        state.upgrade_stream_awaiting_detach = Some(stream);
        if !accepted_downstream_upgrade {
            state.close_after_ingress = true;
        }
        state.upgrade_paused = false;
        state.codec.set_parser_paused(false);
        match &origin {
            SerializationOrigin::Writer(read_wake) => read_wake.notify_one(),
            SerializationOrigin::Committed(handle) => handle.shared.read_wake.notify_one(),
        }
    }
    capture_panic(&mut first_panic, || sync_buffered_egress_stats(state));
    capture_panic(&mut first_panic, || flush_session_stats(state));
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    if !committed {
        registration_error = observer
            .register_byte_events(context, offsets, field_section_info, registrations, eom)
            .err();
    }
    if let PreparedEvent::Headers { event, .. } = &mut prepared {
        if let Some(size) = encoded_header_callback {
            capture_panic(&mut first_panic, || {
                event.on_headers_encoded(size);
            });
        }
    }
    if let PreparedEvent::Body {
        kind: HttpBodyEventKind::PushPromise(push),
        ..
    } = &mut prepared
    {
        capture_panic(&mut first_panic, || drop(push.move_push_source()));
    }
    resume_first_panic(first_panic);
    if let Some(error) = registration_error {
        return Err(error);
    }
    Ok(())
}

struct WriteBatchContext<'a> {
    state: &'a Rc<RefCell<DriverState>>,
    observer: &'a Rc<AsyncSocketByteEventObserver>,
    context: &'a WorkerContext,
    read_wake: &'a tokio::sync::Notify,
    write_wake: &'a tokio::sync::Notify,
    body_source_cursor: &'a mut Option<EgressTransferKey>,
}

async fn write_one_batch(
    batch: WriteBatchContext<'_>,
    transport: &Rc<dyn UniplexTransport>,
    config: UniplexConfig,
    cancellation: CancellationToken,
) -> Result<(), HttpError> {
    let WriteBatchContext {
        state,
        observer,
        context,
        read_wake,
        write_wake,
        body_source_cursor,
    } = batch;
    let (data, scheduled_end, mut boundary, flags) = {
        let mut state = state.borrow_mut();
        state.zero_byte_write_pending = false;
        if state.pending_tx_ack.is_none() {
            state.pending_tx_ack = observer.next_tx_ack_event(context)?;
        }
        let boundary_offset = state
            .pending_tx_ack
            .as_ref()
            .map(TxAckEvent::session_byte_offset);
        if boundary_offset.is_some_and(|offset| offset < state.bytes_scheduled) {
            return Err(invalid_transition(
                b"byte-event boundary was already passed",
            ));
        }
        let until_boundary = boundary_offset
            .map(|offset| offset.saturating_sub(state.bytes_scheduled) as usize)
            .unwrap_or(usize::MAX);
        let amount = state
            .egress
            .chain_length()
            .min(config.maximum_write_size)
            .min(until_boundary);
        let data = state.egress.split_at_most(amount);
        let scheduled_end = state.bytes_scheduled.saturating_add(amount as u64);
        let boundary = if boundary_offset == Some(scheduled_end) {
            state.pending_tx_ack.take()
        } else {
            None
        };
        let flags = boundary
            .as_ref()
            .map_or_else(ByteEventWriteFlags::default, TxAckEvent::write_flags);
        state.bytes_scheduled = scheduled_end;
        (data, scheduled_end, boundary, flags)
    };

    if flags.timestamp_tx || flags.timestamp_ack {
        state
            .borrow_mut()
            .byte_event_bridges
            .retain(|bridge| bridge.pending.get() != 0);
        if state.borrow().byte_event_bridges.len() >= MAX_TRANSPORT_EVENT_BRIDGES {
            let error = HttpError::with_message(
                HttpErrorCode::ENHANCE_YOUR_CALM,
                b"transport byte-event bridge capacity exhausted",
            );
            if let Some(boundary) = boundary.take() {
                boundary.cancel(&error);
            }
            return Err(error);
        }
        let mut pending = 0;
        if flags.timestamp_tx {
            pending |= ByteEventType::NicTx.bit();
        }
        if flags.timestamp_ack {
            pending |= ByteEventType::CumulativeAck.bit();
        }
        let bridge = Rc::new(TransportObserverBridge {
            observer: Rc::clone(observer),
            context: context.clone(),
            fallback_offset: scheduled_end,
            pending: Cell::new(pending),
        });
        if let Err(registration_error) = transport.register_byte_event(
            context,
            HttpByteEventRegistration::new(None, pending, &bridge),
        ) {
            let error = HttpError::with_message(
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                registration_error.to_string().as_bytes(),
            );
            if let Some(boundary) = boundary.take() {
                boundary.cancel(&error);
            }
            return Err(error);
        }
        state.borrow_mut().byte_event_bridges.push(bridge);
    }
    if let Err(error) = observer.transport_write(context, scheduled_end) {
        if let Some(boundary) = boundary.take() {
            boundary.cancel(&error);
        }
        return Err(error);
    }
    let mut write = transport.write(context, data, flags, config.write_timeout, cancellation);
    let first_write = if state.borrow().http2.is_none() && tokio::task::coop::has_budget_remaining()
    {
        poll_fn(|task_context| Poll::Ready(write.as_mut().poll(task_context))).await
    } else {
        Poll::Pending
    };
    let result = if let Poll::Ready(result) = first_write {
        #[cfg(test)]
        writer_probe::note(|counts| counts.first_poll_writes += 1);
        result
    } else {
        let mut poll_observer_timeouts = true;
        let source_ready = state
            .borrow()
            .http2
            .as_ref()
            .map(|http2| Arc::clone(&http2.body_source_publications.source_ready));
        loop {
            // A write still pending when its transport was reset is abandoned,
            // whether or not the transport acts on the cancellation.
            if state.borrow().transport_reset_issued {
                let error = state.borrow().fatal_error.clone().unwrap_or_else(|| {
                    HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"uniplex write abandoned after a transport reset",
                    )
                });
                break Err(TransportWriteError::new(error, 0));
            }
            let buffer_timeout = state.borrow().h2_buffer_timeout(config.write_timeout);
            let callback_wait = state
                .borrow()
                .core()
                .pending_event_wait(CancellationToken::never());
            #[cfg(test)]
            writer_probe::note(|counts| counts.batch_selects += 1);
            let native_wait = async {
                tokio::select! {
                    result = &mut write => Some(result),
                    _ = write_wake.notified() => None,
                    _ = callback_wait => {
                        if let Err(error) = drive_core_callbacks(state, read_wake) {
                            fail_connection(state, write_wake, error, true);
                        }
                        None
                    },
                    _ = poll_fn(|task_context| poll_exchange_cleanup(state, observer, context, read_wake, task_context)) => None,
                    result = poll_byte_event_timeouts(Rc::clone(observer), context.clone()), if poll_observer_timeouts => {
                        if let Err(error) = result {
                            poll_observer_timeouts = false;
                            fail_connection(state, write_wake, error, true);
                        }
                        None
                    },
                    _ = wait_for_optional_deadline(context.host(), buffer_timeout.map(|(_, deadline)| deadline)) => {
                        if expire_body_source(state, buffer_timeout)
                            && let Err(error) = drive_core_callbacks(state, read_wake)
                        {
                            fail_connection(state, write_wake, error, true);
                        }
                        None
                    },
                }
            };
            let mut native_wait = std::pin::pin!(native_wait);
            match poll_fn(|task_context| Poll::Ready(native_wait.as_mut().poll(task_context))).await
            {
                Poll::Ready(Some(result)) => break result,
                Poll::Ready(None) => continue,
                Poll::Pending => {}
            }
            // Only an actual Pending result from native arbitration permits a source turn.
            let serviced = poll_fn(|task_context| {
                Poll::Ready(poll_body_sources(state, body_source_cursor, task_context))
            })
            .await;
            if serviced {
                if let Err(error) = drive_core_callbacks(state, read_wake) {
                    fail_connection(state, write_wake, error, true);
                }
                if has_ready_body_source(&state.borrow()) {
                    context.host().yield_to_io().await;
                }
                continue;
            }
            let completed = match &source_ready {
                Some(source_ready) => tokio::select! {
                    biased;
                    result = native_wait.as_mut() => result,
                    _ = source_ready.notified() => None,
                },
                None => native_wait.as_mut().await,
            };
            if let Some(result) = completed {
                break result;
            }
        }
    };
    drop(write);
    let written = match &result {
        Ok(info) => info.bytes_written,
        Err(error) => error.bytes_written(),
    };
    let written_offset = {
        let mut state = state.borrow_mut();
        state.bytes_written = state.bytes_written.saturating_add(written as u64);
        state.bytes_written
    };
    flush_session_stats(state);
    if let Err(error) =
        observer.socket_byte_event(context, SocketByteEventType::Write, written_offset)
    {
        if let Some(boundary) = boundary.take() {
            boundary.cancel(&error);
        }
        return Err(error);
    }
    if let Err(error) = result {
        let error = error.error().clone();
        if let Some(boundary) = boundary.take() {
            boundary.cancel(&error);
        }
        return Err(error);
    }
    let completed_boundary = if written_offset == scheduled_end {
        boundary.take()
    } else {
        None
    };
    if let Err(error) =
        observer.transport_write_complete(context, written_offset, completed_boundary)
    {
        if let Some(boundary) = boundary.take() {
            boundary.cancel(&error);
        }
        return Err(error);
    }
    if written_offset != scheduled_end {
        let error = HttpError::with_message(
            HttpErrorCode::TRANSPORT_WRITE_ERROR,
            b"transport reported a short successful write",
        );
        if let Some(boundary) = boundary.take() {
            boundary.cancel(&error);
        }
        return Err(error);
    }
    {
        let state = state.borrow();
        if state.codec.protocol() == CodecProtocol::Http2 {
            state
                .core()
                .observe_lifecycle(LifecycleEvent::Http2Write { bytes: written });
        }
    }
    flush_lifecycle_events(state);
    Ok(())
}

fn drive_read_callbacks(
    state: &Rc<RefCell<DriverState>>,
    read_wake: &tokio::sync::Notify,
    write_wake: &tokio::sync::Notify,
) -> Result<(), HttpError> {
    let had_callbacks = state.borrow().core().has_pending_events();
    drive_core_callbacks(state, read_wake)?;
    if had_callbacks {
        // The writer's callback wait can discard a wake after the reader empties
        // the queue, even when those callbacks produced egress or retired streams.
        write_wake.notify_one();
    }
    Ok(())
}

fn drive_core_callbacks(
    state: &Rc<RefCell<DriverState>>,
    read_wake: &tokio::sync::Notify,
) -> Result<(), HttpError> {
    if !state.borrow().has_pending_callback_work() {
        return Ok(());
    }
    let mut core = DetachedSessionCore::take(state);
    let processed = core.core_mut().process_pending_events_for_driver();
    let flow_updates = core.core_mut().take_flow_control_updates();
    #[cfg(test)]
    INGRESS_CREDIT_CALLBACKS.set(INGRESS_CREDIT_CALLBACKS.get() + flow_updates.len());
    core.restore();
    let processed = match processed {
        Ok(processed) => processed,
        Err(error) => {
            flush_lifecycle_events(state);
            return Err(error);
        }
    };
    let state_owner = Rc::clone(state);
    let mut state = state.borrow_mut();
    if processed.flow_timeout_activity {
        state.note_flow_timeout_activity();
    }
    let mut drain_trigger = apply_stream_updates_locked(&mut state, processed.updates);
    if state.http2.is_none()
        && let Some(http1) = state.http1_stats.as_mut()
    {
        http1.pending.extend(
            flow_updates
                .iter()
                .filter(|update| update.amount != 0)
                .map(|update| Http2StatsEvent::BufferedRead(-signed_byte_count(update.amount))),
        );
    }
    if state.http2.is_some() {
        let mut callback = NoopCodecCallback;
        for update in &flow_updates {
            if update.amount != 0
                && let Some(pending) = pending_session_stats(&mut state)
            {
                pending.push(Http2StatsEvent::BufferedRead(-signed_byte_count(
                    update.amount,
                )));
            }
            if update.to_acknowledge != 0 {
                state.note_flow_timeout_activity();
                let amount = u32::try_from(update.to_acknowledge).map_err(|_| {
                    HttpError::with_message(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"stream receive-window acknowledgement overflowed",
                    )
                })?;
                let DriverState { codec, egress, .. } = &mut *state;
                codec.generate(
                    egress,
                    EgressFrame::WindowUpdate {
                        stream: update.stream,
                        amount,
                    },
                    &mut callback,
                )?;
            }
            if update.amount != 0 {
                let amount = u32::try_from(update.amount).map_err(|_| {
                    HttpError::with_message(
                        HttpErrorCode::FLOW_CONTROL_ERROR,
                        b"connection receive-window acknowledgement overflowed",
                    )
                })?;
                let generated = {
                    let DriverState { codec, egress, .. } = &mut *state;
                    let before = egress.chain_length();
                    codec.ingress_bytes_processed(egress, amount, &mut callback)?;
                    egress.chain_length() != before
                };
                if generated {
                    state.note_flow_timeout_activity();
                }
            }
        }
    }
    let ingress_buffer_limit = state.ingress_buffer_limit;
    let flow_resumed = state.flow_control_paused
        && flow_updates.iter().any(|update| {
            update.window_opened
                || state
                    .core()
                    .stream_if_present(update.stream)
                    .is_none_or(|stream| stream.buffered_ingress_bytes() < ingress_buffer_limit)
        });
    if flow_resumed {
        state.flow_control_paused = false;
    }
    let anti_pipeline_resumed =
        state.anti_pipeline_paused && state.core().num_incoming_streams() <= 1;
    if anti_pipeline_resumed {
        state.anti_pipeline_paused = false;
    }
    if (flow_resumed || anti_pipeline_resumed)
        && state.codec.is_parser_paused()
        && !state.flow_control_paused
        && !state.anti_pipeline_paused
        && !state.upgrade_paused
    {
        state.codec.set_parser_paused(false);
        read_wake.notify_one();
    }
    if state.core().ingress_reconciliation_pending() {
        let streams: Vec<_> = state.ingress_meta.keys().copied().collect();
        for stream in streams {
            let discard = state
                .core()
                .stream_if_present(stream)
                .is_none_or(|stream| stream.stream_source_complete() && !stream.ingress_eom_seen());
            if discard {
                if let Some(meta) = state.ingress_meta.get_mut(&stream) {
                    meta.discard = true;
                }
            }
        }
        // Deferred callouts below can invalidate this scan.
        state.core().acknowledge_ingress_reconciliation();
    }
    if std::mem::take(&mut state.deferred_drain) {
        drain_trigger.get_or_insert(DrainTrigger::Local);
    }
    drop(state);
    let mut first_panic = None;
    if let Some(trigger) = drain_trigger {
        retain_first_panic(
            &mut first_panic,
            initiate_drain_after_state_borrow(&state_owner, trigger).panic,
        );
    }
    capture_panic(&mut first_panic, || drop_deferred_work(&state_owner));
    capture_panic(&mut first_panic, || flush_session_stats(&state_owner));
    capture_panic(&mut first_panic, || flush_lifecycle_events(&state_owner));
    resume_first_panic(first_panic);
    Ok(())
}

fn apply_stream_update(
    state: &Rc<RefCell<DriverState>>,
    update: StreamUpdate,
    read_wake: &tokio::sync::Notify,
) {
    let drain_trigger = { apply_stream_update_locked(&mut state.borrow_mut(), update) };
    let mut first_panic = None;
    if let Some(trigger) = drain_trigger {
        retain_first_panic(
            &mut first_panic,
            initiate_drain_after_state_borrow(state, trigger).panic,
        );
    }
    capture_panic(&mut first_panic, || drop_deferred_work(state));
    capture_panic(&mut first_panic, || flush_session_stats(state));
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    read_wake.notify_one();
    resume_first_panic(first_panic);
}

fn apply_stream_update_locked(
    state: &mut DriverState,
    update: StreamUpdate,
) -> Option<DrainTrigger> {
    let mut drain_trigger = None;
    if update.cancel_egress && state.codec.protocol() == CodecProtocol::Http2 {
        state.note_flow_timeout_activity();
        let cancellation_error = update.cancel_egress_error.clone().unwrap_or_else(|| {
            HttpError::with_message(HttpErrorCode::CANCEL, b"stream egress was cancelled")
        });
        if let Some(http2) = &mut state.http2 {
            http2.scheduler.remove(update.stream);
        }
        let removed = state.extract_egress_work(|stream| stream == update.stream);
        state.deferred_stream_work.extend(
            removed
                .into_iter()
                .map(|work| (work, cancellation_error.clone())),
        );
    }
    if let Some(transfer) = update.replacement {
        state.note_flow_timeout_activity();
        let removed = state.extract_egress_work(|stream| stream == update.stream);
        state.deferred_drops.extend(removed);
        state.enqueue_transfer(update.stream, transfer, None);
    }
    if !matches!(update.action, StreamAction::None) {
        state.note_flow_timeout_activity();
    }
    match update.action {
        StreamAction::None => {}
        StreamAction::SendErrorResponse(_) => {
            if state.codec.protocol() == CodecProtocol::Http11
                && state
                    .ingress_meta
                    .get(&update.stream)
                    .is_some_and(|meta| !meta.complete)
            {
                drain_trigger = Some(DrainTrigger::StreamReset);
            }
        }
        StreamAction::Reset { code, origin } => {
            state.take_response_timeout(update.stream);
            if state.codec.protocol() == CodecProtocol::Http11 {
                state.byte_event_cancel_error.get_or_insert_with(|| {
                    HttpError::with_message(code, b"stream reset cancelled byte events")
                });
                state.reset_after_draining_writes = true;
                drain_trigger = Some(DrainTrigger::StreamReset);
            } else {
                if let Some(meta) = state.ingress_meta.get_mut(&update.stream) {
                    meta.discard = true;
                }
                let mut callback = NoopCodecCallback;
                let wire_code = http_error_code_to_h2_error_code(code, origin.from_source());
                let children = state
                    .http2
                    .as_ref()
                    .map_or_else(Vec::new, |http2| http2.pushed_children(update.stream));
                let reset_streams = std::iter::once(update.stream)
                    .chain(children.iter().copied())
                    .collect::<Vec<_>>();
                state
                    .stream_byte_event_cancellations
                    .extend(reset_streams.iter().copied().map(|stream| {
                        (
                            stream,
                            HttpError::with_message(code, b"stream reset cancelled byte events"),
                        )
                    }));
                for stream in reset_streams.iter().copied() {
                    let DriverState { codec, egress, .. } = state;
                    if let Err(error) = codec.generate(
                        egress,
                        EgressFrame::Reset {
                            stream,
                            code: wire_code,
                        },
                        &mut callback,
                    ) {
                        state.record_error(error);
                    }
                }
                for child in children {
                    if let Err(error) = state.core_mut().peer_reset(child, code) {
                        state.record_error(error);
                    }
                }
                if let Some(http2) = &mut state.http2 {
                    for stream in &reset_streams {
                        http2.scheduler.remove(*stream);
                    }
                }
                let removed = state.extract_egress_work(|stream| reset_streams.contains(&stream));
                state.deferred_drops.extend(removed);
            }
        }
    }
    if update.detached
        && state
            .core()
            .exchange_tracker(update.stream)
            .is_none_or(|tracker| tracker.claim_driver_update())
    {
        state.note_flow_timeout_activity();
        state.take_response_timeout(update.stream);
        if let Some(pending) = state.pending_pushes.remove(&update.stream) {
            state.deferred_push_drops.push(pending.source);
        }
        let pending_children = state
            .pending_pushes
            .iter()
            .filter_map(|(stream, pending)| {
                (pending.associated_stream == update.stream).then_some(*stream)
            })
            .collect::<Vec<_>>();
        for child in pending_children {
            if let Some(pending) = state.pending_pushes.remove(&child) {
                state.deferred_push_drops.push(pending.source);
            }
        }
        let removed = state.extract_egress_work(|stream| stream == update.stream);
        state.deferred_drops.extend(removed);
        if let Some(http2) = &mut state.http2 {
            http2.remove_stream(update.stream);
            if http2.config.stats.is_some() {
                http2.pending_stats.push(Http2StatsEvent::TransactionClosed);
            }
        } else if let Some(http1) = &mut state.http1_stats {
            http1.record_transaction_closed();
        }
        if state.upgrade_stream_awaiting_detach == Some(update.stream) {
            state.upgrade_stream_awaiting_detach = None;
        }
        let erased = state.http2.is_some();
        if let Entry::Occupied(mut entry) = state.ingress_meta.entry(update.stream) {
            if entry.get().complete || erased {
                entry.remove();
            } else {
                entry.get_mut().discard = true;
            }
        }
        state.egress_meta.remove(&update.stream);
    }
    drain_trigger
}

fn poll_exchange_cleanup(
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    read_wake: &tokio::sync::Notify,
    task_context: &mut Context<'_>,
) -> Poll<usize> {
    let exchanges = {
        let state = state.borrow();
        if state.lifecycle.dispatching.get() {
            return Poll::Pending;
        }
        let Some(core) = &state.core else {
            return Poll::Pending;
        };
        core.detached_exchanges()
    };
    if exchanges.is_empty() {
        return Poll::Pending;
    }
    let mut first_panic = None;
    for (stream, tracker) in &exchanges {
        if !tracker.driver_applied() {
            capture_panic(&mut first_panic, || {
                apply_stream_update(
                    state,
                    StreamUpdate::new(*stream, StreamAction::None, true),
                    read_wake,
                );
            });
        }
    }
    capture_panic(&mut first_panic, || drop_deferred_work(state));
    capture_panic(&mut first_panic, || sync_buffered_egress_stats(state));
    capture_panic(&mut first_panic, || flush_session_stats(state));
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    capture_panic(&mut first_panic, || {
        cancel_pending_byte_events(state, observer, context)
    });
    for (stream, tracker) in &exchanges {
        if first_panic.is_some() {
            tracker.fail();
        }
        capture_panic(&mut first_panic, || {
            match observer.poll_stream_events_drained(context, *stream, task_context) {
                Poll::Ready(Ok(())) => tracker.finish_driver(),
                Poll::Ready(Err(_)) => {
                    tracker.fail();
                    tracker.finish_driver();
                }
                Poll::Pending => {}
            }
        });
    }
    if first_panic.is_some() {
        for (_, tracker) in &exchanges {
            tracker.fail();
        }
    }
    let ready = state.borrow_mut().core_mut().take_ready_exchanges();
    let count = ready.len();
    if count != 0 {
        state.borrow_mut().note_flow_timeout_activity();
    }
    for exchange in ready {
        capture_panic(&mut first_panic, || exchange.publish());
    }
    resume_first_panic(first_panic);
    if count == 0 {
        Poll::Pending
    } else {
        Poll::Ready(count)
    }
}

async fn finish_exchange_cleanup(
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    read_wake: &tokio::sync::Notify,
) -> Result<(), HttpError> {
    loop {
        if state.borrow().core.is_none() {
            return Ok(());
        }
        drive_core_callbacks(state, read_wake)?;
        if state.borrow().core().pending_exchange_count() == 0 {
            return Ok(());
        }
        let callbacks = state
            .borrow()
            .core()
            .pending_event_wait(CancellationToken::never());
        tokio::select! {
            _ = callbacks => {},
            _ = poll_fn(|task_context| poll_exchange_cleanup(state, observer, context, read_wake, task_context)) => {},
        }
    }
}

fn apply_stream_updates_locked(
    state: &mut DriverState,
    updates: impl IntoIterator<Item = StreamUpdate>,
) -> Option<DrainTrigger> {
    let mut drain_trigger = None;
    for update in updates {
        let update_trigger = apply_stream_update_locked(state, update);
        drain_trigger = drain_trigger.or(update_trigger);
    }
    drain_trigger
}

struct DrainTransition {
    changed: bool,
    error: Option<HttpError>,
    panic: Option<PanicPayload>,
}

fn initiate_drain_after_state_borrow(
    state: &Rc<RefCell<DriverState>>,
    trigger: DrainTrigger,
) -> DrainTransition {
    let mut first_panic = None;
    let mut core = DetachedSessionCore::take(state);
    let (http1_grace, ingress_grace, close_http1) = {
        let state = state.borrow();
        let downstream = core.core_mut().direction().is_downstream();
        let http1_grace = trigger == DrainTrigger::Local
            && downstream
            && state.codec.protocol() == CodecProtocol::Http11
            && state.wait_for_http1_response_on_drain
            && state.codec.is_reusable();
        let http2_grace = trigger == DrainTrigger::Local
            && state.codec.protocol() == CodecProtocol::Http2
            && downstream
            && state
                .http2
                .as_ref()
                .is_some_and(|http2| http2.config.double_goaway_drain);
        let close_http1 = trigger != DrainTrigger::Local
            && state.codec.protocol() == CodecProtocol::Http11
            && state.codec.is_reusable();
        (http1_grace, http1_grace || http2_grace, close_http1)
    };
    core.core_mut().set_draining_ingress_grace(ingress_grace);
    let changed = match catch_unwind(AssertUnwindSafe(|| {
        core.core_mut().initiate_drain_with(trigger)
    })) {
        Ok(changed) => changed,
        Err(payload) => {
            first_panic = Some(payload);
            true
        }
    } || close_http1;
    capture_panic(&mut first_panic, || core.restore());
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    let mut error = None;
    capture_panic(&mut first_panic, || {
        let mut driver = state.borrow_mut();
        let deferred = std::mem::take(&mut driver.deferred_drain);
        if changed || deferred {
            driver.note_flow_timeout_activity();
            let http1_grace = http1_grace && driver.codec.is_reusable();
            if driver.codec.protocol() == CodecProtocol::Http11 {
                driver.core_mut().set_draining_ingress_grace(http1_grace);
            }
            if trigger == DrainTrigger::CloseWhenIdle
                && let Some(http2) = driver.http2.as_mut()
            {
                http2.sent_initial_goaway = true;
            }
            error = if trigger == DrainTrigger::HeaderRateLimit {
                generate_header_rate_limit_goaway(&mut driver)
            } else if http1_grace {
                let DriverState { codec, egress, .. } = &mut *driver;
                codec
                    .generate(
                        egress,
                        EgressFrame::Goaway {
                            last_stream: StreamId::MAX,
                            code: H2ErrorCode::NO_ERROR,
                            debug_data: None,
                        },
                        &mut NoopCodecCallback,
                    )
                    .map(|_| ())
            } else {
                generate_goaway(&mut driver, H2ErrorCode::NO_ERROR)
            }
            .err();
            if error.is_some() && driver.codec.protocol() == CodecProtocol::Http2 {
                driver.deferred_drain = true;
            }
        }
    });
    DrainTransition {
        changed,
        error,
        panic: first_panic,
    }
}

fn drop_items_individually<T>(items: impl IntoIterator<Item = T>) {
    let mut first_panic = None;
    for item in items {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(item))) {
            if first_panic.is_none() {
                first_panic = Some(payload);
            }
        }
    }
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}

fn cancel_registrations(registrations: &mut Vec<HttpByteEventRegistration>, error: &HttpError) {
    let mut first_panic = None;
    for registration in registrations {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            registration.cancel(error, None);
        })) {
            if first_panic.is_none() {
                first_panic = Some(payload);
            }
        }
    }
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}

fn cancel_egress_work_items(items: VecDeque<EgressWork>, error: &HttpError) {
    let mut first_panic = None;
    for mut item in items {
        let cancellation = catch_unwind(AssertUnwindSafe(|| {
            if let EgressWork::Event { event, .. } = &mut item {
                match event {
                    EgressEvent::Headers(headers) => {
                        cancel_registrations(headers.byte_event_registrations_mut(), error);
                    }
                    EgressEvent::Body(body) => {
                        cancel_registrations(body.byte_event_registrations_mut(), error);
                    }
                }
            }
        }));
        if let Err(payload) = cancellation {
            if first_panic.is_none() {
                first_panic = Some(payload);
            }
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(item))) {
            if first_panic.is_none() {
                first_panic = Some(payload);
            }
        }
    }
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}

#[cfg(test)]
pub(crate) fn drop_egress_transfers_for_test(transfers: Vec<EgressTransfer>) {
    drop_items_individually(transfers);
}

fn drop_deferred_work(state: &Rc<RefCell<DriverState>>) {
    let (deferred, stream_work, pushes) = {
        let mut state = state.borrow_mut();
        if !state.deferred_drops.is_empty()
            || !state.deferred_stream_work.is_empty()
            || !state.deferred_push_drops.is_empty()
        {
            state.note_flow_timeout_activity();
        }
        (
            std::mem::take(&mut state.deferred_drops),
            std::mem::take(&mut state.deferred_stream_work),
            std::mem::take(&mut state.deferred_push_drops),
        )
    };
    let mut first_panic = None;
    capture_panic(&mut first_panic, || drop_items_individually(deferred));
    for (work, error) in stream_work {
        capture_panic(&mut first_panic, || {
            cancel_egress_work_items(VecDeque::from([work]), &error)
        });
    }
    capture_panic(&mut first_panic, || drop_items_individually(pushes));
    resume_first_panic(first_panic);
}

fn cancel_pending_pushes(
    state: &Rc<RefCell<DriverState>>,
    read_wake: &tokio::sync::Notify,
    error: &HttpError,
) -> Result<(), HttpError> {
    let pending = std::mem::take(&mut state.borrow_mut().pending_pushes);
    let mut first_panic = None;
    for mut push in pending.into_values() {
        capture_panic(&mut first_panic, || {
            push.source.stop_reading(Some(error.code()));
        });
        capture_panic(&mut first_panic, || drop(push));
    }
    let result = drive_core_callbacks(state, read_wake);
    resume_first_panic(first_panic);
    result
}

fn handle_read_timeout(state: &Rc<RefCell<DriverState>>, write_wake: &tokio::sync::Notify) {
    state.borrow_mut().note_flow_timeout_activity();
    let transition = initiate_drain_after_state_borrow(state, DrainTrigger::IdleTimeout);
    let mut first_panic = transition.panic;
    capture_panic(&mut first_panic, || write_wake.notify_one());
    resume_first_panic(first_panic);
}

fn handle_eof(
    state: &Rc<RefCell<DriverState>>,
    read_wake: &tokio::sync::Notify,
    write_wake: &tokio::sync::Notify,
) {
    // A source's `stop_reading` completes it synchronously, so a source
    // that completed in the pass that read the EOF must count before streams are
    // judged incomplete.
    if let Err(error) = drive_read_callbacks(state, read_wake, write_wake) {
        fail_connection(state, write_wake, error, true);
    }
    let (failures, upstream_response_pending, upstream) = {
        let mut state = state.borrow_mut();
        state.note_flow_timeout_activity();
        let DriverState {
            core,
            codec,
            http2,
            http1_stats,
            handler,
            ingress_meta,
            pending_pushes,
            egress,
            stream_read_timeout,
            ingress_buffer_limit,
            http1_ingress_body_ceiling,
            flow_control_paused,
            anti_pipeline_paused,
            upgrade_paused,
            close_after_ingress,
            abort_exchanges_on_eof,
            eof_watch,
            ingress_close_stream,
            reads_closed,
            stream_byte_event_cancellations,
            upstream_ingress_upgraded,
            ..
        } = &mut *state;
        let core = core
            .as_deref_mut()
            .expect("a running uniplex owns its core");
        let upstream = core.direction().is_upstream();
        if core.protocol() == CodecProtocol::Http2 {
            core.observe_lifecycle(LifecycleEvent::Http2IngressEof);
        }
        codec.generate_immediate_goaway();
        let mut callback = SessionCodecCallback {
            core,
            http2,
            http1_stats,
            handler: handler.clone(),
            ingress_meta,
            pending_pushes,
            stream_read_timeout: *stream_read_timeout,
            ingress_buffer_limit: *ingress_buffer_limit,
            http1_ingress_body_ceiling: *http1_ingress_body_ceiling,
            flow_control_paused,
            anti_pipeline_paused,
            upgrade_paused,
            close_after_ingress,
            abort_exchanges_on_eof: *abort_exchanges_on_eof,
            eof_watch,
            ingress_close_stream,
            reads_closed,
            stream_byte_event_cancellations,
            failures: Vec::new(),
            stream_updates: Vec::new(),
            upstream_message_complete: false,
            upgrade_completed: false,
            rejected_body: false,
            drain_trigger: None,
            peer_goaway_generation: CallbackGoawayGeneration::NotRequested,
        };
        let result = codec.on_ingress_eof(&mut callback, egress);
        for result in result.egress_results {
            if let Err(error) = result {
                callback
                    .failures
                    .push(CodecFailure::plain(StreamId::UNSPECIFIED, error));
            }
        }
        // At EOF the HTTP/1.1 codec completes any 2xx CONNECT or 101 ingress instead of
        // failing it, so the tunnel's egress may still finish.
        let response_pending = upstream
            && codec.is_busy()
            && !*upstream_ingress_upgraded
            && !callback.upgrade_completed;
        (callback.failures, response_pending, upstream)
    };
    let mut first_panic = None;
    if upstream {
        retain_first_panic(
            &mut first_panic,
            initiate_drain_after_state_borrow(state, DrainTrigger::CodecNotReusable).panic,
        );
    }
    if let Some(failure) = failures.into_iter().next() {
        capture_panic(&mut first_panic, || {
            handle_codec_failure(state, write_wake, failure)
        });
    }

    let downstream = state.borrow().core().direction().is_downstream();
    let incomplete: Vec<_> = state
        .borrow()
        .ingress_meta
        .iter()
        .filter_map(|(stream, meta)| (!meta.complete && !meta.discard).then_some(*stream))
        .collect();
    let abort_exchanges = downstream && {
        let state = state.borrow();
        let core = state.core();
        match state.codec.protocol() {
            // An upgraded tunnel's EOF is a half-close: its egress may still finish.
            CodecProtocol::Http11 => {
                state.abort_exchanges_on_eof
                    && core.stream_ids().into_iter().any(|stream| {
                        core.stream_if_present(stream)
                            .is_some_and(|stream| !stream.is_upgraded())
                    })
            }
            // With `abort_streams_on_eof`: a request head still arriving, or a stream
            // whose response END_STREAM is unserialized, tunnels included.
            CodecProtocol::Http2 => {
                core.connection_error().is_none()
                    && state
                        .http2
                        .as_ref()
                        .is_some_and(|http2| http2.config.abort_streams_on_eof)
                    && (state.codec.has_partial_request_head()
                        || core.stream_ids().into_iter().any(|stream| {
                            core.stream_if_present(stream)
                                .is_some_and(|stream| !stream.egress_complete())
                        }))
            }
            _ => false,
        }
    };
    // An unserialized HTTP/1.1 request cannot get a response after peer EOF.
    let upstream_request_unstarted = upstream && !upstream_response_pending && {
        let state = state.borrow();
        let streams = state.core().stream_ids();
        state.codec.protocol() == CodecProtocol::Http11
            && !streams.is_empty()
            && streams.iter().all(|stream| {
                state
                    .core()
                    .exchange_tracker(*stream)
                    .is_some_and(|exchange| !exchange.egress_started())
            })
    };
    if abort_exchanges {
        capture_panic(&mut first_panic, || {
            fail_connection(
                state,
                write_wake,
                HttpError::with_message(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"),
                true,
            );
        });
    } else if downstream {
        for stream in incomplete {
            capture_panic(&mut first_panic, || {
                let mut core = DetachedSessionCore::take(state);
                let result = core.core_mut().ingress_transport_eof(stream);
                core.restore();
                if let Err(error) = result {
                    fail_connection(state, write_wake, error, true);
                }
            });
        }
    } else if upstream_response_pending || upstream_request_unstarted || !incomplete.is_empty() {
        capture_panic(&mut first_panic, || {
            fail_connection(
                state,
                write_wake,
                HttpError::with_message(HttpErrorCode::TRANSPORT_EOF, b"transport EOF"),
                true,
            );
        });
    }
    state.borrow_mut().reads_closed = true;
    capture_panic(&mut first_panic, || write_wake.notify_one());
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    resume_first_panic(first_panic);
}

fn handle_codec_failure(
    state: &Rc<RefCell<DriverState>>,
    write_wake: &tokio::sync::Notify,
    mut failure: CodecFailure,
) {
    state.borrow_mut().note_flow_timeout_activity();
    let protocol = state.borrow().codec.protocol();
    let downstream = state.borrow().core().direction().is_downstream();
    if protocol == CodecProtocol::Http2 {
        state
            .borrow()
            .core()
            .observe_lifecycle(LifecycleEvent::Http2IngressError {
                stream: failure.stream,
                code: failure.error.code(),
            });
    }
    if protocol == CodecProtocol::Http11 {
        if downstream && !failure.stream.is_unspecified() {
            let status = {
                let mut driver = state.borrow_mut();
                driver.record_error(failure.error.clone());
                driver
                    .byte_event_cancel_error
                    .get_or_insert_with(|| failure.error.clone());
                failure.http_status.filter(|_| {
                    driver
                        .core()
                        .stream(failure.stream)
                        .is_ok_and(|stream| stream.can_send_headers())
                })
            };
            let mut first_panic = None;
            let mut scheduled = false;
            if let Some(status) = status {
                state.borrow_mut().reset_after_error_response = Some(failure.stream);
                match catch_unwind(AssertUnwindSafe(|| {
                    schedule_error_response(
                        state,
                        write_wake,
                        failure.stream,
                        status,
                        failure.error.clone(),
                    )
                })) {
                    Ok(result) => scheduled = result,
                    Err(payload) => first_panic = Some(payload),
                }
            }
            if !scheduled {
                retain_h1_close_panic(
                    &mut first_panic,
                    catch_unwind(AssertUnwindSafe(|| {
                        reset_h1_ingress_stream(state, write_wake, failure.stream, failure.error)
                    }))
                    .err(),
                );
            }
            capture_panic(&mut first_panic, || write_wake.notify_one());
            resume_first_panic(first_panic);
        } else {
            fail_connection(state, write_wake, failure.error, true);
            write_wake.notify_one();
        }
        return;
    }
    let disposition = {
        let state = state.borrow();
        CodecFailureContext {
            direction: state.core().direction(),
            stream: failure.stream,
            exception_direction: failure.direction,
            new_stream: failure.new_stream,
            http_status: failure.http_status,
            wire_code: failure.wire_code,
            live_stream: state
                .core()
                .stream_if_present(failure.stream)
                .map(|stream| stream.can_send_headers()),
            // Source completion leaves an abandoned upgraded stream unreset while
            // its egress is open, so this failure still resets it.
            discarded: state
                .ingress_meta
                .get(&failure.stream)
                .is_some_and(|meta| meta.discard)
                && !state
                    .core()
                    .stream_if_present(failure.stream)
                    .is_some_and(|stream| stream.is_upgraded() && !stream.egress_complete()),
            protocol_stream_present: state
                .http2
                .as_ref()
                .is_some_and(|http2| http2.streams.contains_key(&failure.stream)),
        }
        .disposition()
    };
    match disposition {
        CodecFailureDisposition::Response(status) => {
            if let Some(message) = failure.error.take_http_message() {
                let mut core = DetachedSessionCore::take(state);
                let result = core
                    .core_mut()
                    .ingress_headers(failure.stream, message, false);
                core.restore();
                if let Err(error) = result {
                    fail_connection(state, write_wake, error, true);
                    return;
                }
            }
            schedule_error_response(state, write_wake, failure.stream, status, failure.error);
        }
        CodecFailureDisposition::Ignore => {}
        CodecFailureDisposition::Reset {
            explicit_wire_code,
            fallback_wire_code,
        } => {
            let update = if state.borrow().core().stream(failure.stream).is_ok() {
                let mut core = DetachedSessionCore::take(state);
                let mut update = core.core_mut().reset_stream_state(
                    failure.stream,
                    failure.error.clone(),
                    crate::session::ResetOrigin::Ingress,
                );
                if let (Ok(update), Some(wire_code)) = (&mut update, explicit_wire_code) {
                    update.action = StreamAction::Reset {
                        code: h2_error_code_to_http_error_code(wire_code),
                        origin: crate::session::ResetOrigin::Ingress,
                    };
                }
                core.restore();
                update.ok()
            } else {
                None
            };
            if let Some(update) = update {
                apply_stream_update(state, update, write_wake);
            } else {
                let mut state = state.borrow_mut();
                let mut callback = NoopCodecCallback;
                let DriverState { codec, egress, .. } = &mut *state;
                if let Err(error) = codec.generate(
                    egress,
                    EgressFrame::Reset {
                        stream: failure.stream,
                        code: fallback_wire_code,
                    },
                    &mut callback,
                ) {
                    state.record_error(error);
                }
            }
        }
        CodecFailureDisposition::Connection => {
            fail_connection(state, write_wake, failure.error, true);
        }
    }
    write_wake.notify_one();
}

fn reset_h1_ingress_stream(
    state: &Rc<RefCell<DriverState>>,
    write_wake: &tokio::sync::Notify,
    stream: StreamId,
    error: HttpError,
) {
    let exchange = state.borrow().core().exchange_tracker(stream);
    let guard = exchange.as_ref().map(|exchange| exchange.guard());
    let (mut retirement, drain, queued_work) = {
        let mut driver = state.borrow_mut();
        driver.reads_closed = true;
        driver.reset_after_error_response = None;
        if let Some(meta) = driver.ingress_meta.get_mut(&stream) {
            meta.complete = true;
            meta.discard = true;
        }
        let retirement = if driver.core().stream(stream).is_ok() {
            Some(
                driver
                    .core_mut()
                    .prepare_abort_stream_without_wire(stream, error.clone()),
            )
        } else {
            None
        };
        let retirement = match retirement {
            Some(Ok(retirement)) => Some(retirement),
            Some(Err(error)) => {
                driver.record_error(error);
                None
            }
            None => None,
        };
        let mut update = retirement.as_ref().map_or_else(
            || StreamUpdate::new(stream, StreamAction::None, false),
            |retirement| retirement.update(),
        );
        update.action = StreamAction::Reset {
            code: error.code(),
            origin: crate::session::ResetOrigin::Ingress,
        };
        let drain = apply_stream_update_locked(&mut driver, update);
        let queued = std::mem::take(&mut driver.egress_work);
        let mut queued_work = Vec::new();
        for work in queued {
            let queued_stream = match &work {
                EgressWork::Event { stream, .. } | EgressWork::Transfer { stream, .. } => *stream,
            };
            if queued_stream == stream {
                queued_work.push(work);
            } else {
                driver.egress_work.push_back(work);
            }
        }
        (retirement, drain, queued_work)
    };
    let mut first_panic = None;
    if let Some(retirement) = &mut retirement {
        match catch_unwind(AssertUnwindSafe(|| retirement.prepare_queues())) {
            Ok(panic) => retain_h1_close_panic(&mut first_panic, panic),
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    if let Some(trigger) = drain {
        retain_h1_close_panic(
            &mut first_panic,
            initiate_drain_after_state_borrow(state, trigger).panic,
        );
    }
    if let Some(retirement) = retirement {
        match catch_unwind(AssertUnwindSafe(|| retirement.retire())) {
            Ok(result) => retain_h1_close_panic(&mut first_panic, result.err()),
            Err(payload) => retain_h1_close_panic(&mut first_panic, Some(payload)),
        }
    }
    for work in queued_work {
        retire_h1_close_work(work, &error, &mut first_panic);
    }
    let (deferred, stream_work, pushes) = {
        let mut driver = state.borrow_mut();
        (
            std::mem::take(&mut driver.deferred_drops),
            std::mem::take(&mut driver.deferred_stream_work),
            std::mem::take(&mut driver.deferred_push_drops),
        )
    };
    for work in deferred {
        retire_h1_close_work(work, &error, &mut first_panic);
    }
    for (work, error) in stream_work {
        retire_h1_close_work(work, &error, &mut first_panic);
    }
    for source in pushes {
        retain_h1_close_panic(
            &mut first_panic,
            catch_unwind(AssertUnwindSafe(|| drop(source))).err(),
        );
    }
    capture_panic(&mut first_panic, || flush_session_stats(state));
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    capture_panic(&mut first_panic, || write_wake.notify_one());
    if first_panic.is_some()
        && let Some(exchange) = &exchange
    {
        exchange.fail();
    }
    retain_h1_close_panic(
        &mut first_panic,
        catch_unwind(AssertUnwindSafe(|| drop(guard))).err(),
    );
    if first_panic.is_some()
        && let Some(exchange) = &exchange
    {
        exchange.fail();
    }
    resume_first_panic(first_panic);
}

fn schedule_error_response(
    state: &Rc<RefCell<DriverState>>,
    write_wake: &tokio::sync::Notify,
    stream: StreamId,
    status: u16,
    error: HttpError,
) -> bool {
    {
        let mut driver = state.borrow_mut();
        let Ok(current) = driver.core().stream(stream) else {
            return false;
        };
        if driver.codec.protocol() == CodecProtocol::Http11 && !current.can_send_headers() {
            return false;
        }
        if let Some(meta) = driver.ingress_meta.get_mut(&stream) {
            meta.complete = true;
            meta.discard = true;
        }
    }
    let mut core = DetachedSessionCore::take(state);
    let producer = core
        .core_mut()
        .stream(stream)
        .ok()
        .map(|stream| stream.ingress_producer());
    if let Some(producer) = producer {
        producer.abort(error.code(), error.message());
    }
    let scheduled = core
        .core_mut()
        .set_error_response_status(stream, status)
        .is_ok();
    let mut replacement = None;
    let mut pending_response = None;
    let mut install_failure = None;
    let mut errors = Vec::new();
    if scheduled {
        match core
            .core_mut()
            .install_error_response_after_source_complete(stream)
        {
            Ok(update) => pending_response = update,
            Err(install_error) => install_failure = Some(install_error),
        }
    } else {
        if core.core_mut().protocol() == CodecProtocol::Http11
            && !core
                .core_mut()
                .stream(stream)
                .is_ok_and(|stream| stream.can_send_headers())
        {
            core.restore();
            return false;
        }
        match get_error_response(status, b"") {
            Ok(source) => {
                match core.core_mut().install_egress_source(
                    stream,
                    HttpSourceHolder::new(Box::new(source)),
                    true,
                ) {
                    Ok(transfer) => {
                        replacement = Some(transfer);
                        if let Err(status_error) =
                            core.core_mut().set_error_response_status(stream, status)
                        {
                            errors.push(status_error);
                        }
                    }
                    Err(install_error) => errors.push(install_error),
                }
            }
            Err(source_error) => errors.push(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                source_error.to_string().as_bytes(),
            )),
        }
    }
    let mut first_panic = None;
    let http1 = core.core_mut().protocol() == CodecProtocol::Http11;
    if http1 {
        capture_panic(&mut first_panic, || {
            core.core_mut().initiate_drain();
        });
    }
    capture_panic(&mut first_panic, || core.restore());
    let mut driver = state.borrow_mut();
    if http1 {
        driver.deferred_drain = false;
    }
    if let Some(transfer) = replacement {
        driver.enqueue_transfer(stream, transfer, None);
    }
    if let Some(update) = pending_response {
        let drain_trigger = apply_stream_update_locked(&mut driver, update);
        debug_assert!(
            drain_trigger.is_none(),
            "the stream's ingress is already marked complete"
        );
    }
    for error in errors {
        driver.record_error(error);
    }
    if http1 {
        let _ = generate_goaway(&mut driver, H2ErrorCode::NO_ERROR);
    }
    drop(driver);
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    if let Some(install_error) = install_failure {
        capture_panic(&mut first_panic, || {
            fail_connection(state, write_wake, install_error, true);
        });
    }
    resume_first_panic(first_panic);
    true
}

fn fail_connection(
    state: &Rc<RefCell<DriverState>>,
    write_wake: &tokio::sync::Notify,
    error: HttpError,
    reset: bool,
) {
    let (queued_work, core_available) = {
        let mut state = state.borrow_mut();
        state.note_flow_timeout_activity();
        state.record_error(error.clone());
        if state.byte_event_cancel_error.is_none() {
            state.byte_event_cancel_error = Some(error.clone());
        }
        let mut queued_work = state.take_egress_work();
        state.reset_after_draining_writes |= reset;
        state.reads_closed = true;
        let wire_code = http_error_code_to_h2_error_code(error.code(), false);
        let _ = generate_goaway(&mut state, wire_code);
        let core_available = state.core.is_some();
        if !core_available && state.deferred_connection_error.is_none() {
            state.deferred_connection_error = Some(error.clone());
        }
        if !core_available {
            state.deferred_connection_work.append(&mut queued_work);
        }
        (queued_work, core_available)
    };
    let mut first_panic = None;
    if core_available {
        let mut core = DetachedSessionCore::take(state);
        capture_panic(&mut first_panic, || {
            core.core_mut().fail_connection(error.clone());
        });
        capture_panic(&mut first_panic, || core.restore());
    }
    let mut driver = state.borrow_mut();
    if std::mem::take(&mut driver.deferred_drain) {
        let _ = generate_goaway(&mut driver, H2ErrorCode::NO_ERROR);
    }
    drop(driver);
    capture_panic(&mut first_panic, || flush_lifecycle_events(state));
    capture_panic(&mut first_panic, || {
        cancel_egress_work_items(queued_work, &error)
    });
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop_deferred_work(state))) {
        if first_panic.is_none() {
            first_panic = Some(payload);
        }
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| write_wake.notify_one())) {
        if first_panic.is_none() {
            first_panic = Some(payload);
        }
    }
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}

fn generate_goaway(state: &mut DriverState, code: H2ErrorCode) -> Result<(), HttpError> {
    let plan = plan_goaway(
        code,
        state.codec.last_incoming_stream_id(),
        state
            .http2
            .as_ref()
            .is_some_and(|http2| http2.config.double_goaway_drain),
        state
            .http2
            .as_ref()
            .is_some_and(|http2| http2.sent_initial_goaway),
    );
    let mut callback = NoopCodecCallback;
    let DriverState { codec, egress, .. } = state;
    codec.generate(
        egress,
        EgressFrame::Goaway {
            last_stream: plan.last_stream,
            code,
            debug_data: None,
        },
        &mut callback,
    )?;
    if let Some(http2) = &mut state.http2
        && plan.phase.commit(
            SentGoaway {
                sent_initial: &mut http2.sent_initial_goaway,
                sent_final: &mut http2.sent_final_goaway,
                deadline: &mut http2.goaway_deadline,
            },
            || http2.host.deadline_after(http2.config.goaway_timeout),
        )
    {
        state.core_mut().set_draining_ingress_grace(false);
    }
    Ok(())
}

fn generate_header_rate_limit_goaway(state: &mut DriverState) -> Result<(), HttpError> {
    let mut callback = NoopCodecCallback;
    let DriverState { codec, egress, .. } = state;
    codec.generate(
        egress,
        EgressFrame::Goaway {
            last_stream: StreamId::MAX,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        },
        &mut callback,
    )?;
    if let Some(http2) = &mut state.http2 {
        http2.sent_initial_goaway = true;
        http2.sent_final_goaway = true;
        http2.goaway_deadline = None;
    }
    state.core_mut().set_draining_ingress_grace(false);
    Ok(())
}

async fn wait_for_notify_or_cancellation(
    notify: &tokio::sync::Notify,
    cancellation: CancellationToken,
) {
    let baton = CancellableBaton::new();
    tokio::select! {
        _ = notify.notified() => {},
        _ = baton.wait(cancellation) => {},
    }
}

async fn poll_byte_event_timeouts(
    observer: Rc<AsyncSocketByteEventObserver>,
    context: WorkerContext,
) -> Result<(), HttpError> {
    poll_fn(
        |task_context| match observer.poll_timeouts(&context, task_context) {
            Ok(0) => Poll::Pending,
            Ok(_) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(error)),
        },
    )
    .await
}

async fn wait_for_byte_event_refs(
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
    write_wake: &tokio::sync::Notify,
) {
    loop {
        cancel_pending_byte_events(state, observer, context);
        if state.borrow().reset_after_draining_writes {
            return;
        }
        if observer.pending_timeout_refs(context).unwrap_or_default() == 0 {
            return;
        }
        let zero_refs = match observer.zero_refs(context) {
            Ok(wait) => wait,
            Err(_) => return,
        };
        tokio::select! {
            _ = zero_refs => return,
            _ = write_wake.notified() => {},
            _ = poll_fn(|task_context| match observer.poll_timeouts(context, task_context) {
                Ok(0) => Poll::Pending,
                Ok(_) | Err(_) => Poll::Ready(()),
            }) => {},
        }
    }
}

fn cancel_pending_byte_events(
    state: &Rc<RefCell<DriverState>>,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
) {
    let (error, stream_errors) = {
        let mut state = state.borrow_mut();
        if state.byte_event_cancel_error.is_some()
            || !state.stream_byte_event_cancellations.is_empty()
        {
            state.note_flow_timeout_activity();
        }
        (
            state.byte_event_cancel_error.take(),
            std::mem::take(&mut state.stream_byte_event_cancellations),
        )
    };
    let mut first_panic = None;
    for (stream, error) in stream_errors {
        capture_panic(&mut first_panic, || {
            if let Err(cancel_error) = observer.cancel_stream_events(context, stream, &error) {
                state.borrow_mut().record_error(cancel_error);
            }
        });
    }
    if let Some(error) = error {
        capture_panic(&mut first_panic, || {
            if let Err(cancel_error) = observer.cancel_events(context, &error) {
                state.borrow_mut().record_error(cancel_error);
            }
        });
    }
    resume_first_panic(first_panic);
}

async fn wait_for_all_streams(
    state: &Rc<RefCell<DriverState>>,
    read_wake: &tokio::sync::Notify,
    observer: &AsyncSocketByteEventObserver,
    context: &WorkerContext,
) {
    loop {
        if drive_core_callbacks(state, read_wake).is_err() {
            return;
        }
        poll_fn(|task_context| {
            let _ = poll_exchange_cleanup(state, observer, context, read_wake, task_context);
            Poll::Ready(())
        })
        .await;
        if state.borrow().core().num_transactions() == 0 {
            return;
        }
        let wait = {
            let state = state.borrow();
            state.core().pending_event_wait(CancellationToken::never())
        };
        let transaction_wait = {
            let state = state.borrow();
            state.core().transaction_detach_wait()
        };
        tokio::select! {
            _ = wait => {},
            _ = transaction_wait => {},
            _ = poll_fn(|task_context| poll_exchange_cleanup(state, observer, context, read_wake, task_context)) => {},
        }
    }
}

struct NoopCodecCallback;

impl HttpCodecCallback for NoopCodecCallback {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

fn invalid_transition(message: impl AsRef<[u8]>) -> HttpError {
    HttpError::with_message(HttpErrorCode::INVALID_STATE_TRANSITION, message)
}

fn session_finished() -> HttpError {
    HttpError::with_message(HttpErrorCode::DROPPED, b"uniplex session has finished")
}

#[cfg(test)]
#[path = "../tests/shutdown_progress.rs"]
mod shutdown_progress_tests;

#[cfg(test)]
#[path = "../tests/ready_dispatch.rs"]
mod ready_dispatch_tests;

#[cfg(test)]
#[path = "../tests/ownership_replay.rs"]
mod ownership_replay;

#[cfg(test)]
#[path = "../tests/message_copies.rs"]
mod message_copies;
