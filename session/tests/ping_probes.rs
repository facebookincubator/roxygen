//! HTTP/2 PING probe oracles (`DIV-024`): the frozen `HTTP2DownstreamSessionTest` probe cases
//! and the Roxygen-only timer, failure, and configuration cases.
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
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
use std::time::Duration;

use bytes::Bytes;
use futures::FutureExt;
use roxygen_codec::CodecEvent;
use roxygen_codec::EgressFrame;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpCodec;
use roxygen_codec::HttpCodecCallback;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_core::ingress_completion::IngressCompletion;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_core::stream_source::FlowControlState;
use roxygen_core::stream_source::HttpIngressBody;
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
use roxygen_core::stream_source::HttpStreamSourceOwner;
use roxygen_core::stream_source::HttpStreamSourceProducer;
use roxygen_http2::Http2Codec;
use roxygen_http2::settings::SettingsId;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::worker::WorkerContext;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::task::LocalSet;
use tokio::time::Instant;

use crate::experimental::PingProbeConfig;
use crate::experimental::is_ping_probe_timeout;
use crate::http2::Http2SessionConfig;
use crate::http2::rate_limit::Http2RateLimitConfig;
use crate::http2::rate_limit::RateLimitConfig;
use crate::http2::stats::Http2SessionStats;
use crate::owned_h2::OwnedH2Application;
use crate::owned_h2::OwnedH2Exchange;
use crate::owned_h2::OwnedH2Options;
use crate::owned_h2::OwnedH2Session;
use crate::owned_h2::OwnedH2UpstreamSession;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::shutdown::DrainTrigger;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexResponse;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;

/// Virtual time: every test runs on a paused clock.
const TEST_DEADLINE: Duration = Duration::from_secs(120);
const INTERVAL: Duration = Duration::from_secs(1);
const TIMEOUT: Duration = Duration::from_secs(1);
/// Timers fire at their deadline rounded up to the clock's millisecond tick.
const TICK: Duration = Duration::from_millis(2);
/// Virtual time that each read of a flood (`Wire::flood`) takes.
const FLOOD_READ: Duration = Duration::from_micros(100);
/// A task poll makes at most 128 budgeted reads (12.8 ms of flood); the probe
/// timer fires within two polls of its deadline.
const FLOOD_SLACK: Duration = Duration::from_millis(50);
const STREAM_1: StreamId = StreamId::new(1);
const STREAM_3: StreamId = StreamId::new(3);
const STREAM_5: StreamId = StreamId::new(5);

fn run_paused(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static,
) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("current-thread runtime");
    let result = runtime.block_on(LocalSet::new().run_until(async move {
        let (worker, driver) = WorkerDriver::new(16).expect("worker driver");
        let call = worker
            .call_local(move |context| {
                let worker = context.handle().clone();
                async move {
                    let result =
                        AssertUnwindSafe(tokio::time::timeout(TEST_DEADLINE, test(context)))
                            .catch_unwind()
                            .await;
                    worker.begin_shutdown();
                    result
                }
            })
            .expect("worker call");
        let (_, result) = tokio::join!(driver, call);
        result.expect("worker call completed")
    }));
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("the test exceeded its deadline"),
        Err(payload) => resume_unwind(payload),
    }
}

fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).expect("queue capacity")
}

fn bytes_of(queue: &BufQueue) -> Vec<u8> {
    queue
        .coalesce(queue.chain_length())
        .expect("bounded queue")
        .to_vec()
}

/// Waits on the paused clock, one millisecond per check.
async fn until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn assert_at(actual: Instant, expected: Instant, what: &str) {
    assert!(
        actual >= expected && actual <= expected + TICK,
        "{what} at {actual:?}, expected {expected:?}"
    );
}

fn assert_probe_timeout(error: &HttpError) {
    assert_eq!(error.code(), HttpErrorCode::DROPPED, "{error:?}");
    assert!(is_ping_probe_timeout(error), "{error:?}");
}

fn probes(immediate: bool) -> PingProbeConfig {
    PingProbeConfig {
        interval: INTERVAL,
        timeout: TIMEOUT,
        extend_interval_on_ingress: true,
        immediate,
    }
}

/// No connection read, stream read, or write timeout.
fn untimed() -> UniplexConfig {
    UniplexConfig {
        read_timeout: Duration::ZERO,
        stream_read_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        ..UniplexConfig::default()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Close {
    Open,
    ShutdownWrite,
    Closed,
    Reset,
}

struct WireState {
    reads: VecDeque<Option<Vec<u8>>>,
    writes: Vec<(Instant, Vec<u8>)>,
    stall_writes: bool,
    stalled_writes: usize,
    abandoned_writes: usize,
    writes_after_reset: usize,
    resets: usize,
    reset_at: Option<Instant>,
    close: Close,
    flood: Option<(Vec<u8>, Instant)>,
    last_flood_read: Option<Instant>,
}

/// The peer's view of one byte-stream connection. `None` in `reads` is EOF.
#[derive(Clone)]
struct Wire {
    state: Rc<RefCell<WireState>>,
    readable: Rc<Notify>,
}

impl Wire {
    fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(WireState {
                reads: VecDeque::new(),
                writes: Vec::new(),
                stall_writes: false,
                stalled_writes: 0,
                abandoned_writes: 0,
                writes_after_reset: 0,
                resets: 0,
                reset_at: None,
                close: Close::Open,
                flood: None,
                last_flood_read: None,
            })),
            readable: Rc::new(Notify::new()),
        }
    }

    fn transport(&self) -> Rc<dyn UniplexTransport> {
        Rc::new(WireTransport { wire: self.clone() })
    }

    fn deliver(&self, bytes: Vec<u8>) {
        self.state.borrow_mut().reads.push_back(Some(bytes));
        self.readable.notify_one();
    }

    fn end(&self) {
        self.state.borrow_mut().reads.push_back(None);
        self.readable.notify_one();
    }

    /// Makes every read return `bytes` at once until `until`, as a socket the
    /// peer keeps readable: each read spends one unit of the task's
    /// cooperative budget, as a ready Tokio socket read does, and `FLOOD_READ`
    /// of virtual time.
    fn flood(&self, bytes: Vec<u8>, until: Instant) {
        self.state.borrow_mut().flood = Some((bytes, until));
        self.readable.notify_one();
    }

    /// Makes every write stay pending, whatever its cancellation, as on a
    /// peer that accepts no bytes.
    fn stall_writes(&self) {
        self.state.borrow_mut().stall_writes = true;
    }

    fn reads_drained(&self) -> bool {
        self.state.borrow().reads.is_empty()
    }

    fn resets(&self) -> usize {
        self.state.borrow().resets
    }

    fn reset_at(&self) -> Option<Instant> {
        self.state.borrow().reset_at
    }
}

struct WireTransport {
    wire: Wire,
}

/// Counts a stalled write whose future the session dropped.
struct StalledWrite(Wire);

impl Drop for StalledWrite {
    fn drop(&mut self) {
        self.0.state.borrow_mut().abandoned_writes += 1;
    }
}

impl UniplexTransport for WireTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        maximum: usize,
        _allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let wire = self.wire.clone();
        let deadline = (!timeout.is_zero()).then(|| Instant::now() + timeout);
        Box::pin(async move {
            let baton = CancellableBaton::new();
            let mut cancelled = std::pin::pin!(baton.wait(cancellation));
            loop {
                if wire.state.borrow().close == Close::Reset {
                    return Err(HttpError::with_message(
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                        b"connection reset locally",
                    ));
                }
                let flood = wire.state.borrow().flood.clone();
                if let Some((bytes, _)) = flood.filter(|(_, until)| Instant::now() < *until) {
                    std::future::poll_fn(tokio::task::coop::poll_proceed)
                        .await
                        .made_progress();
                    // `advance` moves the clock before it yields.
                    let _ = tokio::time::advance(FLOOD_READ).now_or_never();
                    wire.state.borrow_mut().last_flood_read = Some(Instant::now());
                    return Ok(UniplexRead::new(queue(&bytes), false));
                }
                let next = wire.state.borrow_mut().reads.pop_front();
                match next {
                    Some(Some(mut bytes)) => {
                        if bytes.len() > maximum {
                            let rest = bytes.split_off(maximum);
                            wire.state.borrow_mut().reads.push_front(Some(rest));
                        }
                        return Ok(UniplexRead::new(queue(&bytes), false));
                    }
                    Some(None) => return Ok(UniplexRead::new(BufQueue::new(0), true)),
                    None => {}
                }
                let expired = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    () = wire.readable.notified() => {}
                    _ = &mut cancelled => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"peer read cancelled",
                        ));
                    }
                    () = expired => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::READ_TIMEOUT,
                            b"peer read timed out",
                        ));
                    }
                }
            }
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        _flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let mut state = self.wire.state.borrow_mut();
        if state.close == Close::Reset {
            state.writes_after_reset += 1;
            return Box::pin(std::future::ready(Err(TransportWriteError::new(
                HttpError::with_message(
                    HttpErrorCode::TRANSPORT_WRITE_ERROR,
                    b"connection reset locally",
                ),
                0,
            ))));
        }
        if state.stall_writes {
            state.stalled_writes += 1;
            let stalled = StalledWrite(self.wire.clone());
            return Box::pin(async move {
                let _stalled = stalled;
                std::future::pending().await
            });
        }
        let length = data.chain_length();
        state.writes.push((Instant::now(), bytes_of(&data)));
        Box::pin(std::future::ready(Ok(WriteInfo {
            bytes_written: length,
        })))
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.wire.state.borrow_mut();
        if state.close == Close::Open {
            state.close = Close::ShutdownWrite;
        }
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.wire.state.borrow_mut();
        if state.close != Close::Reset {
            state.close = Close::Closed;
        }
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.wire.state.borrow_mut();
        state.resets += 1;
        state.reset_at.get_or_insert_with(Instant::now);
        state.close = Close::Reset;
        drop(state);
        self.wire.readable.notify_one();
        Ok(())
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        Ok(ByteEventCapability::Unsupported)
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        _registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        Err(ByteEventRegistrationError::Unsupported)
    }
}

struct Silent;

impl HttpCodecCallback for Silent {
    fn on_event(&mut self, _event: CodecEvent<'_>) {}
}

/// One frame the session wrote, as the peer decodes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Frame {
    PingRequest(u64),
    PingReply(u64),
    Goaway(H2ErrorCode),
    Reset(StreamId, H2ErrorCode),
    Headers(StreamId),
    Data(StreamId),
    Error,
}

#[derive(Default)]
struct FrameLog(Vec<Frame>);

impl HttpCodecCallback for FrameLog {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        let frame = match event {
            CodecEvent::PingRequest { data } => Frame::PingRequest(data),
            CodecEvent::PingReply { data } => Frame::PingReply(data),
            CodecEvent::Goaway { code, .. } => Frame::Goaway(code),
            CodecEvent::Abort { stream, code } => Frame::Reset(stream, code),
            CodecEvent::HeadersComplete { stream, .. } => Frame::Headers(stream),
            CodecEvent::Body { stream, .. } => Frame::Data(stream),
            CodecEvent::Error { .. } => Frame::Error,
            _ => return,
        };
        self.0.push(frame);
    }
}

/// The session's HTTP/2 peer: one codec frames the peer's bytes and decodes
/// the session's, each decoded frame stamped with the time of its write.
struct Peer {
    codec: Http2Codec,
    observed: usize,
    remainder: Vec<u8>,
    frames: Vec<(Instant, Frame)>,
}

impl Peer {
    fn new(codec: Http2Codec) -> Self {
        Self {
            codec,
            observed: 0,
            remainder: Vec::new(),
            frames: Vec::new(),
        }
    }

    /// A client for a downstream session, and its preface and SETTINGS.
    fn client() -> (Self, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Upstream);
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate(&mut output, EgressFrame::ConnectionPreface, &mut Silent)
            .expect("client preface");
        codec
            .generate_configured_settings(&mut output, &mut Silent)
            .expect("client SETTINGS");
        (Self::new(codec), bytes_of(&output))
    }

    /// A server for an upstream session, and its SETTINGS.
    fn server(settings: &[(SettingsId, u64)]) -> (Self, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        for (id, value) in settings {
            codec.egress_settings_mut().set(*id, *value);
        }
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut Silent)
            .expect("server SETTINGS");
        (Self::new(codec), bytes_of(&output))
    }

    fn frame(&mut self, frame: EgressFrame<'_>) -> Vec<u8> {
        let mut output = BufQueue::new(1 << 20);
        self.codec
            .generate(&mut output, frame, &mut Silent)
            .expect("peer frame");
        bytes_of(&output)
    }

    fn ping(&mut self, data: u64) -> Vec<u8> {
        self.frame(EgressFrame::PingRequest { data: Some(data) })
    }

    fn ping_reply(&mut self, data: u64) -> Vec<u8> {
        self.frame(EgressFrame::PingReply { data })
    }

    fn settings(&mut self) -> Vec<u8> {
        self.frame(EgressFrame::Settings { settings: &[] })
    }

    fn window_update(&mut self, stream: StreamId, amount: u32) -> Vec<u8> {
        self.frame(EgressFrame::WindowUpdate { stream, amount })
    }

    fn data(&mut self, stream: StreamId, bytes: &[u8], end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Body {
            stream,
            body: queue(bytes),
            padding: None,
            end_stream,
        })
    }

    fn goaway(&mut self, last_stream: StreamId) -> Vec<u8> {
        self.frame(EgressFrame::Goaway {
            last_stream,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        })
    }

    fn headers(&mut self, stream: StreamId, message: &HttpMessage, end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Headers {
            stream,
            message,
            end_stream,
            extra_headers: None,
        })
    }

    /// A client's GET on its next stream.
    fn request(&mut self, end_stream: bool) -> (StreamId, Vec<u8>) {
        let stream = self.codec.create_stream();
        let mut request = HttpMessage::new();
        request.set_method(HttpMethod::Get).expect("GET");
        request.set_url(b"/", false).expect("path");
        request.set_http_version(1, 1);
        request
            .headers_mut()
            .set(HttpHeaderCode::Host, b"example.com");
        (stream, self.headers(stream, &request, end_stream))
    }

    /// Decodes the session's writes since the last call.
    fn observe(&mut self, wire: &Wire) {
        let writes = wire.state.borrow().writes[self.observed..].to_vec();
        self.observed += writes.len();
        for (at, bytes) in writes {
            self.remainder.extend_from_slice(&bytes);
            if self.remainder.is_empty() {
                continue;
            }
            let input = BufQueue::copy_from_slice(&self.remainder, self.remainder.len())
                .expect("decoded bytes fit");
            let mut log = FrameLog::default();
            let mut output = BufQueue::new(1 << 20);
            let result = self.codec.on_ingress(&input, &mut log, &mut output);
            self.remainder.drain(..result.consumed);
            for frame in log.0 {
                assert_ne!(frame, Frame::Error, "the peer decodes no invalid frame");
                self.frames.push((at, frame));
            }
        }
    }

    fn probes(&mut self, wire: &Wire) -> Vec<(Instant, u64)> {
        self.observe(wire);
        self.frames
            .iter()
            .filter_map(|(at, frame)| match frame {
                Frame::PingRequest(data) => Some((*at, *data)),
                _ => None,
            })
            .collect()
    }

    fn goaways(&mut self, wire: &Wire) -> usize {
        self.observe(wire);
        self.frames
            .iter()
            .filter(|(_, frame)| matches!(frame, Frame::Goaway(_)))
            .count()
    }

    /// Waits for the session's `ordinal`th PING request (zero-based).
    async fn probe(&mut self, wire: &Wire, ordinal: usize) -> (Instant, u64) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(probe) = self.probes(wire).get(ordinal) {
                return *probe;
            }
            assert!(Instant::now() < deadline, "no PING probe {ordinal}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// Probe-timeout statistics and lifecycle events, in delivery order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Record {
    PingTimeout,
    Event(LifecycleEvent),
}

#[derive(Default)]
struct Recorder(RefCell<Vec<Record>>);

impl Recorder {
    fn count(&self, record: Record) -> usize {
        self.0
            .borrow()
            .iter()
            .filter(|candidate| **candidate == record)
            .count()
    }

    fn ping_timeouts(&self) -> usize {
        self.count(Record::PingTimeout)
    }

    fn records(&self) -> Vec<Record> {
        self.0.borrow().clone()
    }

    fn detached(&self, stream: StreamId) -> bool {
        self.0
            .borrow()
            .contains(&Record::Event(LifecycleEvent::TransactionDetached {
                stream: Some(stream),
            }))
    }
}

impl Http2SessionStats for Recorder {
    fn record_ping_timeout(&self) {
        self.0.borrow_mut().push(Record::PingTimeout);
    }
}

impl LifecycleObserver for Recorder {
    fn on_event(&self, event: LifecycleEvent) {
        self.0.borrow_mut().push(Record::Event(event));
    }
}

/// One HTTP/2 session under test and its peer.
struct Fixture {
    session: UniplexSession,
    handle: UniplexHandle,
    wire: Wire,
    peer: Peer,
    /// The peer's opening bytes: a client's preface and SETTINGS, or a
    /// server's SETTINGS.
    hello: Vec<u8>,
    recorder: Rc<Recorder>,
}

impl Fixture {
    fn build(
        context: WorkerContext,
        handler: Option<Rc<dyn HttpHandler>>,
        uniplex: UniplexConfig,
        ping_probes: Option<PingProbeConfig>,
        settings: &[(SettingsId, u64)],
    ) -> Self {
        let wire = Wire::new();
        let recorder = Rc::new(Recorder::default());
        let downstream = handler.is_some();
        let (session, handle) = UniplexSession::new_http2(
            context,
            if downstream {
                TransportDirection::Downstream
            } else {
                TransportDirection::Upstream
            },
            wire.transport(),
            handler,
            uniplex,
            Http2SessionConfig {
                stats: Some(Rc::clone(&recorder) as Rc<dyn Http2SessionStats>),
                ping_probes,
                ..Http2SessionConfig::default()
            },
        )
        .expect("HTTP/2 session");
        session
            .add_lifecycle_observer(Rc::clone(&recorder) as Rc<dyn LifecycleObserver>)
            .expect("lifecycle observer");
        let (peer, hello) = if downstream {
            Peer::client()
        } else {
            Peer::server(settings)
        };
        Self {
            session,
            handle,
            wire,
            peer,
            hello,
            recorder,
        }
    }

    /// A downstream session whose requests `handler` serves.
    fn downstream(
        context: WorkerContext,
        handler: impl HttpHandler,
        uniplex: UniplexConfig,
        ping_probes: Option<PingProbeConfig>,
    ) -> Self {
        Self::build(context, Some(Rc::new(handler)), uniplex, ping_probes, &[])
    }

    fn upstream(
        context: WorkerContext,
        uniplex: UniplexConfig,
        ping_probes: Option<PingProbeConfig>,
    ) -> Self {
        Self::build(context, None, uniplex, ping_probes, &[])
    }

    fn split(self) -> (UniplexSession, Script) {
        (
            self.session,
            Script {
                handle: self.handle,
                wire: self.wire,
                peer: self.peer,
                hello: self.hello,
                recorder: self.recorder,
            },
        )
    }
}

/// The test's side of a running session.
struct Script {
    handle: UniplexHandle,
    wire: Wire,
    peer: Peer,
    hello: Vec<u8>,
    recorder: Rc<Recorder>,
}

impl Script {
    fn hello(&self) {
        self.wire.deliver(self.hello.clone());
    }

    async fn consumed(&self) {
        until("the session to consume the peer's bytes", || {
            self.wire.reads_drained()
                && !matches!(self.handle.buffered_ingress_for_test(), Ok(bytes) if !bytes.is_empty())
        })
        .await;
    }

    async fn probe(&mut self, ordinal: usize) -> (Instant, u64) {
        self.peer.probe(&self.wire, ordinal).await
    }

    fn probes(&mut self) -> Vec<(Instant, u64)> {
        self.peer.probes(&self.wire)
    }

    fn reply(&mut self, data: u64) {
        let reply = self.peer.ping_reply(data);
        self.wire.deliver(reply);
    }
}

/// Serves each request with a fixed response once its body ends.
struct Respond {
    status: u16,
    body: usize,
}

impl HttpHandler for Respond {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let status = self.status;
        let body = self.body;
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if !head.eom() {
                while !request.read_body_event(u32::MAX).await?.eom() {}
            }
            let body = (body != 0).then(|| queue(&vec![b'x'; body]));
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(status, body).expect("response"),
            )))
        })
    }
}

/// How the session retired a response that never produces.
#[derive(Default)]
struct HeldResponse {
    requests: Cell<usize>,
    stopped: Cell<Option<Option<HttpErrorCode>>>,
}

/// Reads each request to its end, then answers with a response that never
/// produces, so its stream stays open.
struct Hold(Rc<HeldResponse>);

impl HttpHandler for Hold {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let held = Rc::clone(&self.0);
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if !head.eom() {
                while !request.read_body_event(u32::MAX).await?.eom() {}
            }
            held.requests.set(held.requests.get() + 1);
            Ok(HttpSourceHolder::new(Box::new(Unanswered(held))))
        })
    }
}

struct Unanswered(Rc<HeldResponse>);

impl HttpSource for Unanswered {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.0.stopped.set(Some(error));
    }
}

fn connect_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Connect).expect("CONNECT");
    message.set_url(b"example:443", false).expect("authority");
    message.set_http_version(1, 1);
    message
        .headers_mut()
        .set(HttpHeaderCode::Host, b"example:443");
    message
}

fn status(code: u16) -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_status_code(code).expect("status");
    message.set_http_version(1, 1);
    message
}

/// Records how a request body source completed.
#[derive(Clone, Default)]
struct UploadCompletion(Rc<RefCell<Option<Option<HttpError>>>>);

impl UploadCompletion {
    fn error(&self) -> Option<HttpError> {
        self.0.borrow().clone().flatten()
    }
}

impl HttpStreamSourceCallback for UploadCompletion {
    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        *self.0.borrow_mut() = Some(error);
    }
}

/// One CONNECT stream, open in both directions.
struct Tunnel {
    response: UniplexResponse,
    upload: HttpStreamSourceProducer,
    completion: UploadCompletion,
}

impl Tunnel {
    /// Commits a CONNECT on `stream` without answering it.
    async fn commit(script: &mut Script, stream: StreamId) -> Self {
        let source = HttpStreamSource::new_async(None, None, 1 << 20).expect("upload source");
        let completion = UploadCompletion::default();
        source.set_callback(Some(Box::new(completion.clone())));
        let upload = source.producer();
        assert!(upload.headers(connect_request(), false));
        let response = script
            .handle
            .send_request(
                HttpSourceHolder::new(Box::new(source)),
                CancellationToken::never(),
            )
            .await
            .expect("the CONNECT is committed");
        assert_eq!(response.stream, stream);
        Self {
            response,
            upload,
            completion,
        }
    }

    async fn open(script: &mut Script, stream: StreamId) -> Self {
        let mut tunnel = Self::commit(script, stream).await;
        let head = script.peer.headers(stream, &status(200), false);
        script.wire.deliver(head);
        let mut head = tunnel
            .response
            .source
            .read_header_event()
            .await
            .expect("the CONNECT response");
        assert_eq!(head.headers_mut().status_code().expect("status"), 200);
        tunnel
    }

    fn send(&self, bytes: &[u8]) {
        assert!(self.upload.body(queue(bytes), 0, false) != FlowControlState::Error);
    }

    fn read(&mut self) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.response.source.read_body_event(u32::MAX)
    }

    /// The reader's next read once it waits, after any ready events without
    /// bytes (the CONNECT's upgrade).
    fn pending_read(&mut self) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        loop {
            let mut read = self.read();
            match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Pending => return read,
                Poll::Ready(Ok(event)) if event.as_body().is_none() && !event.eom() => {}
                Poll::Ready(result) => panic!("the read resolved: ok={}", result.is_ok()),
            }
        }
    }

    /// The reader's next failure, skipping events without bytes.
    async fn read_error(&mut self) -> HttpError {
        loop {
            match self.read().await {
                Ok(event) if event.as_body().is_none() && !event.eom() => {}
                Ok(_) => panic!("the read returned tunnel bytes instead of failing"),
                Err(error) => return error,
            }
        }
    }

    /// Ends both directions and reads the response to its end.
    async fn finish(mut self, script: &mut Script) {
        let stream = self.response.stream;
        self.upload.eom();
        let end = script.peer.data(stream, b"", true);
        script.wire.deliver(end);
        while !self.read().await.expect("tunnel body").eom() {}
        until("the stream to detach", || script.recorder.detached(stream)).await;
    }
}

/// A read polled with its own waker, which records whether the read was
/// woken.
struct ParkedRead {
    what: &'static str,
    read: Pin<Box<dyn Future<Output = Result<(), HttpError>>>>,
    woken: Arc<WakeFlag>,
}

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl ParkedRead {
    fn park<T: 'static>(what: &'static str, read: LocalSourceFuture<Result<T, HttpError>>) -> Self {
        let mut parked = Self {
            what,
            read: Box::pin(read.map(|result| result.map(drop))),
            woken: Arc::default(),
        };
        assert!(parked.poll().is_pending(), "{what} waits");
        parked
    }

    fn poll(&mut self) -> Poll<Result<(), HttpError>> {
        let waker = Waker::from(Arc::clone(&self.woken));
        self.read.as_mut().poll(&mut Context::from_waker(&waker))
    }

    fn woken(&self) -> bool {
        self.woken.0.load(Ordering::SeqCst)
    }
}

/// An owned HTTP/2 application that admits no stream.
struct NoExchanges;

struct NoExchange;

impl OwnedH2Exchange for NoExchange {
    fn request(&mut self, _head: Box<HttpMessage>, _body: HttpStreamSourceOwner, _eom: bool) {}

    fn request_body(&mut self, _head: Box<HttpMessage>, _body: HttpIngressBody, _eom: bool) {}

    fn poll_headers(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<Result<HttpHeaderEvent, HttpError>> {
        Poll::Pending
    }

    fn poll_body(
        &mut self,
        _context: &mut Context<'_>,
        _maximum: u32,
    ) -> Poll<Result<HttpBodyEvent, HttpError>> {
        Poll::Pending
    }

    fn cancel(&mut self, _error: &HttpError, _peer_reset: bool) {}

    fn response_complete(&mut self) {}

    fn complete(self) {}
}

impl OwnedH2Application for NoExchanges {
    type Exchange = NoExchange;

    fn new_exchange(&self, _stream: StreamId, _ingress: IngressCompletion) -> NoExchange {
        NoExchange
    }
}

/// The owned driver's supported configuration, as its own oracles build it.
fn owned_options(
    context: WorkerContext,
    upstream: bool,
    ping_probes: Option<PingProbeConfig>,
) -> OwnedH2Options {
    let unlimited = RateLimitConfig::new(u64::MAX, Duration::from_millis(100), 0);
    OwnedH2Options {
        context,
        transport: Wire::new().transport(),
        io: UniplexConfig {
            drain_request_body_on_drop: !upstream,
            ingress_buffer_limit: 1 << 20,
            ..untimed()
        },
        http2: Http2SessionConfig {
            settings: if upstream {
                vec![roxygen_codec::CodecSetting {
                    identifier: SettingsId::EnablePush.as_u16(),
                    value: 0,
                }]
            } else {
                Vec::new()
            },
            double_goaway_drain: !upstream,
            goaway_timeout: Duration::ZERO,
            rate_limits: Http2RateLimitConfig {
                control_frames: unlimited,
                header_frames: unlimited,
                resets: unlimited,
                direct_errors: unlimited,
            },
            ping_probes,
            ..Http2SessionConfig::default()
        },
    }
}

#[test]
fn default_and_zero_interval_send_no_probe() {
    let default = PingProbeConfig::default();
    assert_eq!(default.interval, Duration::ZERO);
    assert_eq!(default.timeout, Duration::ZERO);
    assert!(default.extend_interval_on_ingress);
    assert!(!default.immediate);
    assert!(Http2SessionConfig::default().ping_probes.is_none());
    let disabled = [
        None,
        Some(default),
        Some(PingProbeConfig {
            timeout: TIMEOUT,
            extend_interval_on_ingress: false,
            immediate: true,
            ..default
        }),
    ];
    for ping_probes in disabled {
        run_paused(move |context| {
            Box::pin(async move {
                let (session, mut script) =
                    Fixture::upstream(context, untimed(), ping_probes).split();
                let exchange = async {
                    script.hello();
                    let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                    tokio::time::sleep(INTERVAL * 3).await;
                    tunnel.finish(&mut script).await;
                    tokio::time::sleep(INTERVAL * 3).await;
                    assert_eq!(script.probes(), []);
                    script.handle.close_when_idle().expect("close");
                };
                let (result, ()) = tokio::join!(session.run(), exchange);
                result.expect("the session closes cleanly");
                assert_eq!(script.recorder.ping_timeouts(), 0);
                assert_eq!(script.probes(), []);
            })
        });
    }
}

#[test]
fn nonzero_interval_with_zero_timeout_is_rejected() {
    run_paused(|context| {
        Box::pin(async move {
            for downstream in [false, true] {
                let handler = downstream.then(|| {
                    Rc::new(Respond {
                        status: 200,
                        body: 0,
                    }) as Rc<dyn HttpHandler>
                });
                let built = UniplexSession::new_http2(
                    context.clone(),
                    if downstream {
                        TransportDirection::Downstream
                    } else {
                        TransportDirection::Upstream
                    },
                    Wire::new().transport(),
                    handler,
                    UniplexConfig::default(),
                    Http2SessionConfig {
                        ping_probes: Some(PingProbeConfig {
                            interval: INTERVAL,
                            ..PingProbeConfig::default()
                        }),
                        ..Http2SessionConfig::default()
                    },
                );
                let Err(error) = built else {
                    panic!("a nonzero interval with a zero timeout builds a session");
                };
                assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
            }

            // Sub-second durations are used as given: the probe goes out one
            // interval after the read that armed it, the drop one timeout later.
            let interval = Duration::from_millis(250);
            let timeout = Duration::from_millis(100);
            let (session, mut script) = Fixture::upstream(
                context,
                untimed(),
                Some(PingProbeConfig {
                    interval,
                    timeout,
                    ..PingProbeConfig::default()
                }),
            )
            .split();
            let armed = Instant::now();
            script.hello();
            let exchange = async {
                let (sent, _) = script.probe(0).await;
                assert_at(sent, armed + interval, "the probe");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_at(Instant::now(), armed + interval + timeout, "the drop");
        })
    });
}

#[test]
fn owned_h2_driver_rejects_a_nonzero_probe_interval() {
    run_paused(|context| {
        Box::pin(async move {
            let cases = [
                (None, true),
                (Some(PingProbeConfig::default()), true),
                (
                    Some(PingProbeConfig {
                        timeout: TIMEOUT,
                        immediate: true,
                        ..PingProbeConfig::default()
                    }),
                    true,
                ),
                (Some(probes(false)), false),
            ];
            for (ping_probes, builds) in cases {
                let downstream = OwnedH2Session::new(
                    NoExchanges,
                    owned_options(context.clone(), false, ping_probes),
                );
                let upstream =
                    OwnedH2UpstreamSession::new(owned_options(context.clone(), true, ping_probes));
                for result in [downstream.map(drop), upstream.map(drop)] {
                    match result {
                        Ok(()) => assert!(builds, "{ping_probes:?} built an owned driver"),
                        Err(error) => {
                            assert!(!builds, "{ping_probes:?} was rejected: {error:?}");
                            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
                        }
                    }
                }
            }
        })
    });
}

/// Sends DATA on `stream` every three tenths of an interval until `done`.
async fn pump(script: &mut Script, stream: StreamId, done: impl Fn(&mut Script) -> bool) {
    while !done(script) {
        tokio::time::sleep(INTERVAL * 3 / 10).await;
        let data = script.peer.data(stream, b"down", false);
        script.wire.deliver(data);
    }
}

#[test]
fn immediate_probe_reply_keeps_the_session() {
    run_paused(|context| {
        Box::pin(async move {
            let respond = Respond {
                status: 200,
                body: 0,
            };
            let (session, mut script) = Fixture::downstream(
                context,
                respond,
                UniplexConfig::default(),
                Some(probes(true)),
            )
            .split();
            let started = Instant::now();
            script.hello();
            let exchange = async {
                let (sent, data) = script.probe(0).await;
                assert_at(sent, started, "the immediate probe");
                script.reply(data);
                let replied = Instant::now();
                // The reply cleared the probe and restarted the interval.
                let (next, data) = script.probe(1).await;
                assert_at(next, replied + INTERVAL, "the next probe");
                script.reply(data);
                script.consumed().await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("acknowledged probes keep the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}

#[test]
fn unanswered_immediate_probe_drops_the_connection() {
    run_paused(|context| {
        Box::pin(async move {
            let respond = Respond {
                status: 200,
                body: 0,
            };
            let (session, mut script) = Fixture::downstream(
                context,
                respond,
                UniplexConfig::default(),
                Some(probes(true)),
            )
            .split();
            script.hello();
            let (result, (sent, _)) = tokio::join!(session.run(), script.probe(0));
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the connection"));
            assert_at(Instant::now(), sent + TIMEOUT, "the drop");
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn first_stream_starts_the_interval_and_an_unanswered_probe_drops_it() {
    run_paused(|context| {
        Box::pin(async move {
            let held = Rc::new(HeldResponse::default());
            let (session, mut script) = Fixture::downstream(
                context,
                Hold(Rc::clone(&held)),
                UniplexConfig::default(),
                Some(probes(false)),
            )
            .split();
            let started = Instant::now();
            let exchange = async {
                tokio::time::sleep(INTERVAL / 4).await;
                let (stream, request) = script.peer.request(true);
                assert_eq!(stream, STREAM_1);
                let mut bytes = script.hello.clone();
                bytes.extend(request);
                script.wire.deliver(bytes);
                let (sent, _) = script.probe(0).await;
                assert_at(sent, started + INTERVAL / 4 + INTERVAL, "the probe");
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the connection"));
            assert_at(Instant::now(), sent + TIMEOUT, "the drop");
            assert_eq!(held.requests.get(), 1);
            assert_eq!(held.stopped.get(), Some(Some(HttpErrorCode::DROPPED)));
            assert!(
                script.recorder.detached(STREAM_1),
                "the open stream detached"
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn unmatched_ping_reply_is_ignored() {
    for close_when_idle in [true, false] {
        run_paused(move |context| {
            Box::pin(async move {
                let respond = Respond {
                    status: 200,
                    body: 0,
                };
                let (session, mut script) = Fixture::downstream(
                    context,
                    respond,
                    UniplexConfig::default(),
                    Some(probes(true)),
                )
                .split();
                script.hello();
                let exchange = async {
                    let (sent, data) = script.probe(0).await;
                    script.reply(data.wrapping_add(1));
                    script.consumed().await;
                    assert_eq!(script.recorder.ping_timeouts(), 0);
                    if close_when_idle {
                        script.handle.close_when_idle().expect("close");
                    }
                    sent
                };
                let (result, sent) = tokio::join!(session.run(), exchange);
                if close_when_idle {
                    result.expect("the session closes when idle");
                    assert!(Instant::now() < sent + TIMEOUT);
                    assert_eq!(script.recorder.ping_timeouts(), 0);
                } else {
                    // The probe stayed outstanding.
                    assert_probe_timeout(&result.expect_err("the probe still times out"));
                    assert_at(Instant::now(), sent + TIMEOUT, "the drop");
                    assert_eq!(script.recorder.ping_timeouts(), 1);
                }
            })
        });
    }
}

#[test]
fn last_stream_detach_cancels_the_outstanding_probe() {
    run_paused(|context| {
        Box::pin(async move {
            let respond = Respond {
                status: 200,
                body: 100,
            };
            // The connection's idle timeout closes the session after the stream.
            let (session, mut script) = Fixture::downstream(
                context,
                respond,
                UniplexConfig {
                    read_timeout: INTERVAL * 2,
                    ..UniplexConfig::default()
                },
                Some(probes(true)),
            )
            .split();
            let exchange = async {
                let (stream, request) = script.peer.request(true);
                let mut bytes = script.hello.clone();
                bytes.extend(request);
                script.wire.deliver(bytes);
                let (sent, _) = script.probe(0).await;
                until("the stream to detach", || script.recorder.detached(stream)).await;
                assert!(Instant::now() < sent + TIMEOUT);
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            result.expect("the cancelled probe never drops the connection");
            assert!(
                Instant::now() >= sent + INTERVAL * 2,
                "the idle timeout closed it"
            );
            assert_eq!(script.recorder.ping_timeouts(), 0);
            script.peer.observe(&script.wire);
            assert!(
                script
                    .peer
                    .frames
                    .iter()
                    .any(|(_, frame)| *frame == Frame::Headers(STREAM_1))
            );
            assert_eq!(script.probes().len(), 1);
        })
    });
}

#[test]
fn ingress_rearms_the_interval_while_no_probe_is_outstanding() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let mut last = Instant::now();
                for kind in 0..8_u64 {
                    tokio::time::sleep(INTERVAL * 3 / 5).await;
                    let frame = match kind % 4 {
                        0 => script.peer.data(STREAM_1, b"down", false),
                        1 => script.peer.ping(0xfeed + kind),
                        2 => script.peer.settings(),
                        _ => script.peer.window_update(STREAM_1, 1024),
                    };
                    script.wire.deliver(frame);
                    last = Instant::now();
                }
                script.consumed().await;
                assert_eq!(script.probes(), [], "frequent ingress sends no probe");
                let (sent, data) = script.probe(0).await;
                assert_at(
                    sent,
                    last + INTERVAL,
                    "the probe after the peer fell silent",
                );
                script.reply(data);
                tunnel.finish(&mut script).await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("an acknowledged probe keeps the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}

#[test]
fn ingress_does_not_extend_an_outstanding_probe() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let wire = script.wire.clone();
            let exchange = async {
                let _tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let (sent, _) = script.probe(0).await;
                pump(&mut script, STREAM_1, |script| {
                    script.wire.reset_at().is_some()
                })
                .await;
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the probe times out despite DATA"));
            assert_at(wire.reset_at().expect("reset"), sent + TIMEOUT, "the drop");
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn sustained_ingress_does_not_delay_the_probe_timeout() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(true))).split();
            script.hello();
            let wire = script.wire.clone();
            let exchange = async {
                let (sent, data) = script.probe(0).await;
                // Reads stay ready, so the read loop spends every poll's whole
                // budget before the probe timer is polled.
                let unmatched = script.peer.ping_reply(!data);
                script.wire.flood(unmatched, sent + TIMEOUT * 2);
                until("the drop", || script.wire.reset_at().is_some()).await;
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the connection"));
            let reset = wire.reset_at().expect("reset");
            assert!(
                reset >= sent + TIMEOUT && reset <= sent + TIMEOUT + FLOOD_SLACK,
                "the drop at {:?} after the probe, expected {TIMEOUT:?}",
                reset - sent
            );
            assert!(
                wire.state
                    .borrow()
                    .last_flood_read
                    .is_some_and(|read| read >= sent + TIMEOUT),
                "reads stayed ready through the probe's deadline"
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn interval_ignores_ingress_without_extend_interval_on_ingress() {
    run_paused(|context| {
        Box::pin(async move {
            let config = PingProbeConfig {
                extend_interval_on_ingress: false,
                ..probes(false)
            };
            let (session, mut script) = Fixture::upstream(context, untimed(), Some(config)).split();
            script.hello();
            let exchange = async {
                let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let opened = Instant::now();
                pump(&mut script, STREAM_1, |script| !script.probes().is_empty()).await;
                let (sent, data) = script.probe(0).await;
                assert_at(sent, opened + INTERVAL, "the first probe despite ingress");
                script.reply(data);
                let replied = Instant::now();
                pump(&mut script, STREAM_1, |script| script.probes().len() == 2).await;
                let (sent, data) = script.probe(1).await;
                assert_at(sent, replied + INTERVAL, "the probe after the reply");
                script.reply(data);
                tunnel.finish(&mut script).await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("acknowledged probes keep the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}

#[test]
fn matching_reply_clears_the_probe_and_rearms_the_interval() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let mut armed = Instant::now();
                for ordinal in 0..3 {
                    let (sent, data) = script.probe(ordinal).await;
                    assert_at(sent, armed + INTERVAL, "the probe");
                    tokio::time::sleep(TIMEOUT / 2).await;
                    script.reply(data);
                    armed = Instant::now();
                }
                tunnel.finish(&mut script).await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("replies within the timeout keep the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}

#[test]
fn upstream_first_transaction_starts_and_last_detach_cancels_probing() {
    run_paused(|context| {
        Box::pin(async move {
            // Reads arm nothing, so only transactions start the interval.
            let config = PingProbeConfig {
                extend_interval_on_ingress: false,
                ..probes(false)
            };
            let (session, mut script) = Fixture::upstream(context, untimed(), Some(config)).split();
            script.hello();
            let exchange = async {
                tokio::time::sleep(INTERVAL * 2).await;
                assert_eq!(script.probes(), [], "no transaction, no probe");

                let reservation = script.handle.reserve_request().expect("reservation");
                let reserved = Instant::now();
                let (sent, data) = script.probe(0).await;
                assert_at(sent, reserved + INTERVAL, "the reservation's probe");
                script.reply(data);
                script.consumed().await;
                // The reply restarted the interval; the last detach cancels it.
                drop(reservation);
                tokio::time::sleep(INTERVAL * 3).await;
                assert_eq!(script.probes().len(), 1);

                let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let opened = Instant::now();
                let (sent, cancelled) = script.probe(1).await;
                assert_at(sent, opened + INTERVAL, "the request's probe");
                // The last detach also cancels the outstanding probe.
                tunnel.finish(&mut script).await;
                assert!(Instant::now() < sent + TIMEOUT);
                tokio::time::sleep(TIMEOUT * 3).await;
                assert_eq!(script.probes().len(), 2);
                script.reply(cancelled);
                script.consumed().await;
                tokio::time::sleep(INTERVAL * 3).await;
                assert_eq!(
                    script.probes().len(),
                    2,
                    "a late reply to the cancelled probe arms nothing"
                );

                let tunnel = Tunnel::open(&mut script, STREAM_3).await;
                let opened = Instant::now();
                let (sent, data) = script.probe(2).await;
                assert_at(sent, opened + INTERVAL, "the next transaction's probe");
                script.reply(data);
                tunnel.finish(&mut script).await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("cancelled probes never drop the connection");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });

    // The first transaction restarts an interval that a read armed.
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                script.consumed().await;
                tokio::time::sleep(INTERVAL / 2).await;
                let reservation = script.handle.reserve_request().expect("reservation");
                let reserved = Instant::now();
                let (sent, data) = script.probe(0).await;
                assert_at(sent, reserved + INTERVAL, "the reservation's probe");
                script.reply(data);
                script.consumed().await;
                drop(reservation);
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("an acknowledged probe keeps the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });

    // A second transaction neither restarts the interval nor, by detaching
    // first, cancels the probe.
    run_paused(|context| {
        Box::pin(async move {
            let config = PingProbeConfig {
                timeout: INTERVAL / 2,
                extend_interval_on_ingress: false,
                ..probes(false)
            };
            let (session, mut script) = Fixture::upstream(context, untimed(), Some(config)).split();
            script.hello();
            let exchange = async {
                let first = Tunnel::open(&mut script, STREAM_1).await;
                let opened = Instant::now();
                tokio::time::sleep(INTERVAL / 2).await;
                let mut second = Tunnel::open(&mut script, STREAM_3).await;
                let (sent, _) = script.probe(0).await;
                assert_at(sent, opened + INTERVAL, "the first transaction's probe");
                first.finish(&mut script).await;
                assert!(Instant::now() < sent + config.timeout);
                let error = tokio::time::timeout(INTERVAL * 3, second.read_error())
                    .await
                    .expect("the open transaction keeps the probe outstanding");
                assert_probe_timeout(&error);
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_at(
                script.wire.reset_at().expect("reset"),
                sent + config.timeout,
                "the drop",
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });

    // The first transaction leaves a probe that a read armed outstanding.
    run_paused(|context| {
        Box::pin(async move {
            let config = PingProbeConfig {
                timeout: INTERVAL / 2,
                ..probes(false)
            };
            let (session, mut script) = Fixture::upstream(context, untimed(), Some(config)).split();
            script.hello();
            let exchange = async {
                let (sent, _) = script.probe(0).await;
                tokio::time::sleep(config.timeout / 2).await;
                let reservation = script.handle.reserve_request().expect("reservation");
                until("the drop", || script.wire.reset_at().is_some()).await;
                drop(reservation);
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the outstanding probe drops the session"));
            assert_at(
                script.wire.reset_at().expect("reset"),
                sent + config.timeout,
                "the drop",
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn a_read_after_the_last_detach_arms_the_interval() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                tunnel.finish(&mut script).await;
                tokio::time::sleep(INTERVAL * 3).await;
                assert_eq!(script.probes(), [], "the detach cancelled the interval");
                let update = script.peer.window_update(StreamId::UNSPECIFIED, 1024);
                script.wire.deliver(update);
                let read = Instant::now();
                let (sent, data) = script.probe(0).await;
                assert_at(sent, read + INTERVAL, "the idle probe");
                script.reply(data);
                script.consumed().await;
                script.handle.close_when_idle().expect("close");
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.expect("an acknowledged idle probe keeps the session");
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}

#[test]
fn manual_send_ping_reply_does_not_clear_a_probe() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(true))).split();
            script.hello();
            let wire = script.wire.clone();
            let exchange = async {
                let (sent, probe) = script.probe(0).await;
                let manual = script.handle.send_ping().expect("manual PING");
                assert_ne!(manual, probe);
                let (_, written) = script.probe(1).await;
                assert_eq!(written, manual);
                script.reply(manual);
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the probe stays outstanding"));
            assert_at(wire.reset_at().expect("reset"), sent + TIMEOUT, "the drop");
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn idle_session_probes_every_interval_until_a_probe_goes_unanswered() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            let started = Instant::now();
            script.hello();
            let exchange = async {
                let mut armed = started;
                for ordinal in 0..2 {
                    let (sent, data) = script.probe(ordinal).await;
                    assert_at(sent, armed + INTERVAL, "the idle probe");
                    tokio::time::sleep(TIMEOUT / 10).await;
                    script.reply(data);
                    armed = Instant::now();
                }
                let (sent, _) = script.probe(2).await;
                assert_at(sent, armed + INTERVAL, "the unanswered probe");
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_at(Instant::now(), sent + TIMEOUT, "the drop");
            assert!(
                !script
                    .recorder
                    .records()
                    .contains(&Record::Event(LifecycleEvent::ActivateConnection)),
                "no stream was ever open"
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn probe_timeout_fails_the_connection_with_dropped_and_records_once() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                script.probe(0).await;
                tunnel.read_error().await
            };
            let (result, stream) = tokio::join!(session.run(), exchange);
            let error = result.expect_err("the unanswered probe fails the connection");
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            assert_eq!(error.message(), b"Ping probe timed out");
            assert_probe_timeout(&stream);
            assert_eq!(script.recorder.ping_timeouts(), 1);
            assert_eq!(script.wire.resets(), 1);
            assert_eq!(
                script.peer.goaways(&script.wire),
                0,
                "the drop sends no GOAWAY"
            );
            assert_eq!(
                script.wire.state.borrow().writes_after_reset,
                0,
                "nothing was written after the reset"
            );

            // Nothing happens once `run` returned.
            let writes = script.wire.state.borrow().writes.len();
            tokio::time::sleep((INTERVAL + TIMEOUT) * 3).await;
            assert_eq!(script.recorder.ping_timeouts(), 1);
            assert_eq!(script.wire.state.borrow().writes.len(), writes);
        })
    });
}

#[test]
fn probe_timeout_delivers_its_lifecycle_event_before_the_failure() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                script.probe(0).await;
                tunnel.read_error().await
            };
            let (result, stream) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe fails the connection"));
            assert_probe_timeout(&stream);
            let records = script.recorder.records();
            let position = |record: Record| {
                let positions: Vec<_> = records
                    .iter()
                    .enumerate()
                    .filter_map(|(index, candidate)| (*candidate == record).then_some(index))
                    .collect();
                assert_eq!(positions.len(), 1, "{record:?} once in {records:?}");
                positions[0]
            };
            let stats = position(Record::PingTimeout);
            let event = position(Record::Event(LifecycleEvent::Http2PingProbeTimeout));
            let drain = position(Record::Event(LifecycleEvent::DrainStarted(
                DrainTrigger::ConnectionError,
            )));
            let detach = position(Record::Event(LifecycleEvent::TransactionDetached {
                stream: Some(STREAM_1),
            }));
            assert!(
                stats < event && event < drain && drain < detach,
                "{records:?}"
            );
        })
    });
}

#[test]
fn probe_timeout_fails_every_open_stream() {
    run_paused(|context| {
        Box::pin(async move {
            // A four-byte stream window holds the second stream's upload.
            let (session, mut script) = Fixture::build(
                context,
                None,
                untimed(),
                Some(probes(false)),
                &[(SettingsId::InitialWindowSize, 4)],
            )
            .split();
            script.hello();
            let exchange = async {
                let mut parked = Tunnel::open(&mut script, STREAM_1).await;
                let mut blocked = Tunnel::open(&mut script, STREAM_3).await;
                blocked.send(b"blocked upload");
                let mut unanswered = Tunnel::commit(&mut script, STREAM_5).await;
                let (sent, _) = script.probe(0).await;
                let mut reads = [
                    ParkedRead::park("the parked read", parked.pending_read()),
                    ParkedRead::park("the blocked stream read", blocked.pending_read()),
                    ParkedRead::park(
                        "the unanswered head",
                        unanswered.response.source.read_header_event(),
                    ),
                ];
                tokio::time::sleep_until(sent + TIMEOUT - TICK).await;
                assert!(
                    reads.iter().all(|read| !read.woken()),
                    "no reader is woken before the drop"
                );
                for read in &reads {
                    until(&format!("a wake of {}", read.what), || read.woken()).await;
                }
                let errors = reads.each_mut().map(|read| match read.poll() {
                    Poll::Ready(Err(error)) => error,
                    other => panic!("{} after its wake: {other:?}", read.what),
                });
                (errors, blocked)
            };
            let (result, (errors, blocked)) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe fails the connection"));
            for error in &errors {
                assert_probe_timeout(error);
            }
            // `HttpSource::stop_reading` carries only the code to the request
            // body, as frozen `stopReading` does.
            let upload = blocked
                .completion
                .error()
                .expect("the blocked upload fails");
            assert_eq!(upload.code(), HttpErrorCode::DROPPED, "{upload:?}");
            assert!(!is_ping_probe_timeout(&upload), "{upload:?}");
            assert_eq!(
                blocked.upload.body(queue(b"more"), 0, false),
                FlowControlState::Error
            );
            assert!(script.recorder.detached(STREAM_1));
            assert!(script.recorder.detached(STREAM_3));
            assert!(script.recorder.detached(STREAM_5));
            assert!(
                script.handle.reserve_request().is_err(),
                "no further stream"
            );
        })
    });
}

/// The transport was reset once, every write left pending was dropped, and
/// nothing was written after the reset.
fn assert_pending_writes_abandoned(wire: &Wire) {
    let state = wire.state.borrow();
    assert_eq!(state.resets, 1, "close_with_reset once");
    assert!(state.stalled_writes > 0);
    assert_eq!(
        state.abandoned_writes, state.stalled_writes,
        "pending writes abandoned"
    );
    assert_eq!(state.writes_after_reset, 0, "no write after the reset");
}

#[test]
fn probe_timeout_resets_a_transport_whose_writes_stay_pending() {
    run_paused(|context| {
        Box::pin(async move {
            let (session, script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.wire.stall_writes();
            let armed = Instant::now();
            script.hello();
            let result = session.run().await;
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_at(Instant::now(), armed + INTERVAL + TIMEOUT, "the drop");
            assert_eq!(script.wire.reset_at(), Some(Instant::now()));
            assert_pending_writes_abandoned(&script.wire);
            assert!(
                script.wire.state.borrow().writes.is_empty(),
                "the peer saw no bytes, no GOAWAY"
            );
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });

    // With the probe's own write pending, an open stream fails with the probe
    // error and the connection fails once.
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let wire = script.wire.clone();
            let exchange = async {
                let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let opened = Instant::now();
                script.consumed().await;
                script.wire.stall_writes();
                (opened, tunnel.read_error().await)
            };
            let (result, (opened, stream)) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_probe_timeout(&stream);
            assert_at(
                wire.reset_at().expect("reset"),
                opened + INTERVAL + TIMEOUT,
                "the drop",
            );
            assert_pending_writes_abandoned(&wire);
            for record in [
                Record::PingTimeout,
                Record::Event(LifecycleEvent::Http2PingProbeTimeout),
                Record::Event(LifecycleEvent::DrainStarted(DrainTrigger::ConnectionError)),
                Record::Event(LifecycleEvent::TransactionDetached {
                    stream: Some(STREAM_1),
                }),
            ] {
                assert_eq!(script.recorder.count(record), 1, "{record:?}");
            }
        })
    });

    // A connection error whose GOAWAY stays pending still ends with the
    // probe's reset, and the run keeps that first error.
    run_paused(|context| {
        Box::pin(async move {
            let (session, script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.wire.stall_writes();
            // A PING on a stream is a connection error (RFC 9113 section 6.7).
            let mut bytes = script.hello.clone();
            bytes.extend([0, 0, 8, 6, 0, 0, 0, 0, 1]);
            bytes.extend([0; 8]);
            let armed = Instant::now();
            script.wire.deliver(bytes);
            let error = session
                .run()
                .await
                .expect_err("the connection error fails the session");
            assert!(!is_ping_probe_timeout(&error), "{error:?}");
            assert_at(Instant::now(), armed + INTERVAL + TIMEOUT, "the reset");
            assert_pending_writes_abandoned(&script.wire);
            assert_eq!(script.recorder.ping_timeouts(), 1);
        })
    });
}

#[test]
fn is_ping_probe_timeout_recognizes_the_probe_drop() {
    for error in [
        HttpError::new(HttpErrorCode::DROPPED),
        HttpError::with_message(HttpErrorCode::DROPPED, b"Ping probe timed out"),
        HttpError::with_message(HttpErrorCode::DROPPED, b"connection dropped"),
        HttpError::with_message(HttpErrorCode::READ_TIMEOUT, b"Ping probe timed out"),
    ] {
        assert!(!is_ping_probe_timeout(&error), "{error:?}");
    }
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                script.probe(0).await;
                tunnel.read_error().await
            };
            let (result, stream) = tokio::join!(session.run(), exchange);
            let run = result.expect_err("the unanswered probe fails the connection");
            for error in [&run, &stream, &run.clone()] {
                assert!(is_ping_probe_timeout(error), "{error:?}");
            }
            let mut recoded = run.clone();
            recoded.set_code(HttpErrorCode::CANCEL);
            assert!(!is_ping_probe_timeout(&recoded));
        })
    });
}

#[test]
fn probing_does_not_hold_a_draining_or_closing_session() {
    // A drain keeps probing while its stream finishes, then completes at once.
    for peer_goaway in [true, false] {
        run_paused(move |context| {
            Box::pin(async move {
                let (session, mut script) =
                    Fixture::upstream(context, untimed(), Some(probes(false))).split();
                script.hello();
                let exchange = async {
                    let tunnel = Tunnel::open(&mut script, STREAM_1).await;
                    if peer_goaway {
                        let goaway = script.peer.goaway(STREAM_1);
                        script.wire.deliver(goaway);
                        script.consumed().await;
                    } else {
                        assert!(script.handle.initiate_drain().expect("drain"));
                    }
                    let draining = Instant::now();
                    let (sent, data) = script.probe(0).await;
                    assert!(sent >= draining, "the draining session probes");
                    script.reply(data);
                    tunnel.finish(&mut script).await;
                    Instant::now()
                };
                let (result, finished) = tokio::join!(session.run(), exchange);
                result.expect("the drained session closes cleanly");
                assert_at(Instant::now(), finished, "the drained session's end");
                assert_eq!(script.recorder.ping_timeouts(), 0);
            })
        });
    }

    // An unanswered probe still drops a draining session and fails its stream.
    for peer_goaway in [true, false] {
        run_paused(move |context| {
            Box::pin(async move {
                let (session, mut script) =
                    Fixture::upstream(context, untimed(), Some(probes(false))).split();
                script.hello();
                let exchange = async {
                    let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                    if peer_goaway {
                        let goaway = script.peer.goaway(STREAM_1);
                        script.wire.deliver(goaway);
                        script.consumed().await;
                    } else {
                        assert!(script.handle.initiate_drain().expect("drain"));
                    }
                    let (sent, _) = script.probe(0).await;
                    (sent, tunnel.read_error().await)
                };
                let (result, (sent, stream)) = tokio::join!(session.run(), exchange);
                assert_probe_timeout(&result.expect_err("the unanswered probe drops it"));
                assert_probe_timeout(&stream);
                assert_at(Instant::now(), sent + TIMEOUT, "the drop");
                assert_eq!(script.recorder.ping_timeouts(), 1);
            })
        });
    }

    // Closing when idle and a peer EOF end the session without the timer.
    for eof in [false, true] {
        run_paused(move |context| {
            Box::pin(async move {
                let (session, mut script) =
                    Fixture::upstream(context, untimed(), Some(probes(true))).split();
                script.hello();
                let exchange = async {
                    script.probe(0).await;
                    if eof {
                        script.wire.end();
                    } else {
                        script.handle.close_when_idle().expect("close");
                    }
                    Instant::now()
                };
                let (result, closed) = tokio::join!(session.run(), exchange);
                if let Err(error) = result {
                    assert!(!is_ping_probe_timeout(&error), "{error:?}");
                }
                assert_at(Instant::now(), closed, "the session's end");
                assert_eq!(script.recorder.ping_timeouts(), 0);
            })
        });
    }

    // Cancelling the run ends the session without the timer.
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(true))).split();
            script.hello();
            let mut run = session.run();
            let sent = tokio::select! {
                _ = &mut run => panic!("the session ended before its probe"),
                (sent, _) = script.probe(0) => sent,
            };
            drop(run);
            until("the cancelled session to finish", || {
                script.handle.is_finished()
            })
            .await;
            assert!(Instant::now() < sent + TIMEOUT);
            tokio::time::sleep(TIMEOUT * 3).await;
            assert_eq!(script.recorder.ping_timeouts(), 0);
            assert_eq!(script.probes().len(), 1);
        })
    });
}

#[test]
fn probe_timer_ends_with_the_session() {
    for immediate in [false, true] {
        run_paused(move |context| {
            Box::pin(async move {
                let (session, mut script) =
                    Fixture::upstream(context, untimed(), Some(probes(immediate))).split();
                script.hello();
                let exchange = async {
                    tokio::time::sleep(INTERVAL / 2).await;
                    script.handle.close_when_idle().expect("close");
                };
                let (result, ()) = tokio::join!(session.run(), exchange);
                result.expect("the session closes cleanly");
                let probes = script.probes().len();
                assert_eq!(probes, usize::from(immediate));
                let writes = script.wire.state.borrow().writes.len();
                tokio::time::sleep((INTERVAL + TIMEOUT) * 3).await;
                assert_eq!(script.wire.state.borrow().writes.len(), writes);
                assert_eq!(script.probes().len(), probes);
                assert_eq!(script.recorder.ping_timeouts(), 0);
            })
        });
    }
}

#[test]
fn probes_run_with_the_connection_read_timeout_disabled() {
    // No read timeout: a quiet session with an open stream ends only by its probe.
    run_paused(|context| {
        Box::pin(async move {
            let (session, mut script) =
                Fixture::upstream(context, untimed(), Some(probes(false))).split();
            script.hello();
            let exchange = async {
                let mut tunnel = Tunnel::open(&mut script, STREAM_1).await;
                let opened = Instant::now();
                let (sent, data) = script.probe(0).await;
                assert_at(sent, opened + INTERVAL, "the probe");
                script.reply(data);
                let (sent, _) = script.probe(1).await;
                assert_probe_timeout(&tunnel.read_error().await);
                sent
            };
            let (result, sent) = tokio::join!(session.run(), exchange);
            assert_probe_timeout(&result.expect_err("the unanswered probe drops the session"));
            assert_at(Instant::now(), sent + TIMEOUT, "the drop");
        })
    });

    // A read timeout shorter than the interval still drains the idle session.
    run_paused(|context| {
        Box::pin(async move {
            let read_timeout = INTERVAL / 2;
            let (session, mut script) = Fixture::upstream(
                context,
                UniplexConfig {
                    read_timeout,
                    ..untimed()
                },
                Some(probes(false)),
            )
            .split();
            let read = Instant::now();
            script.hello();
            session.run().await.expect("the idle session drains");
            assert_at(Instant::now(), read + read_timeout, "the idle drain");
            assert_eq!(script.probes(), []);
            assert_eq!(script.recorder.ping_timeouts(), 0);
        })
    });
}
