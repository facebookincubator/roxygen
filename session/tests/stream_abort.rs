//! `DIV-023` oracles for the `roxygen_session::experimental` stream abort.
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
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
use roxygen_core::stream_source::HttpStreamSource;
use roxygen_core::stream_source::HttpStreamSourceCallback;
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
use tokio::task::LocalSet;

use crate::experimental::UniplexResponseAbortExt;
use crate::experimental::UniplexStreamAbort;
use crate::http2::Http2SessionConfig;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::shutdown::DrainTrigger;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexResponse;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;
use crate::uniplex::WriterWaitHooks;

const TEST_DEADLINE: Duration = Duration::from_secs(20);
const WAIT_DEADLINE: Duration = Duration::from_secs(5);
const WINDOW: usize = 65_535;
const STREAM_1: StreamId = StreamId::new(1);
const STREAM_3: StreamId = StreamId::new(3);
const STREAM_5: StreamId = StreamId::new(5);

fn run_on_worker(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + 'static,
) {
    let runtime = Builder::new_current_thread()
        .enable_all()
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

async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let reached = tokio::time::timeout(WAIT_DEADLINE, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(reached.is_ok(), "timed out waiting for {what}");
}

/// Returns once no task on the worker can make progress.
async fn settle() {
    tokio::time::pause();
    tokio::time::sleep(Duration::from_millis(1)).await;
    tokio::time::resume();
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
    written: Vec<u8>,
    close: Close,
    hold_writes: bool,
    write_parked: bool,
}

/// The peer's view of one byte-stream connection. `None` in `reads` is EOF.
#[derive(Clone)]
struct Wire {
    state: Rc<RefCell<WireState>>,
    readable: Rc<tokio::sync::Notify>,
    writable: Rc<tokio::sync::Notify>,
}

impl Wire {
    fn new() -> Self {
        Self {
            state: Rc::new(RefCell::new(WireState {
                reads: VecDeque::new(),
                written: Vec::new(),
                close: Close::Open,
                hold_writes: false,
                write_parked: false,
            })),
            readable: Rc::new(tokio::sync::Notify::new()),
            writable: Rc::new(tokio::sync::Notify::new()),
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

    fn written(&self) -> Vec<u8> {
        self.state.borrow().written.clone()
    }

    fn close(&self) -> Close {
        self.state.borrow().close
    }

    fn reads_drained(&self) -> bool {
        self.state.borrow().reads.is_empty()
    }

    /// Parks the session's next transport write until `release_writes`.
    fn hold_writes(&self) {
        self.state.borrow_mut().hold_writes = true;
    }

    fn release_writes(&self) {
        self.state.borrow_mut().hold_writes = false;
        self.writable.notify_one();
    }

    fn write_parked(&self) -> bool {
        self.state.borrow().write_parked
    }
}

struct WireTransport {
    wire: Wire,
}

impl UniplexTransport for WireTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let wire = self.wire.clone();
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
                tokio::select! {
                    () = wire.readable.notified() => {}
                    _ = &mut cancelled => {
                        return Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"peer read cancelled",
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
        let wire = self.wire.clone();
        Box::pin(async move {
            while wire.state.borrow().hold_writes {
                wire.state.borrow_mut().write_parked = true;
                wire.writable.notified().await;
            }
            let mut state = wire.state.borrow_mut();
            state.write_parked = false;
            if state.close == Close::Reset {
                return Err(TransportWriteError::new(
                    HttpError::with_message(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"connection reset locally",
                    ),
                    0,
                ));
            }
            let length = data.chain_length();
            state.written.extend_from_slice(&bytes_of(&data));
            Ok(WriteInfo {
                bytes_written: length,
            })
        })
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
        self.wire.state.borrow_mut().close = Close::Reset;
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

/// The proxy, an HTTP/2 server that frames its own bytes.
struct H2Peer {
    codec: Http2Codec,
}

impl H2Peer {
    fn server(settings: &[(SettingsId, u64)]) -> (Self, Vec<u8>) {
        let mut codec = Http2Codec::new(TransportDirection::Downstream);
        for (id, value) in settings {
            codec.egress_settings_mut().set(*id, *value);
        }
        let mut output = BufQueue::new(1 << 20);
        codec
            .generate_configured_settings(&mut output, &mut Silent)
            .expect("server SETTINGS");
        (Self { codec }, bytes_of(&output))
    }

    fn frame(&mut self, frame: EgressFrame<'_>) -> Vec<u8> {
        let mut output = BufQueue::new(1 << 20);
        self.codec
            .generate(&mut output, frame, &mut Silent)
            .expect("peer frame");
        bytes_of(&output)
    }

    fn headers(&mut self, stream: StreamId, message: &HttpMessage, end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Headers {
            stream,
            message,
            end_stream,
            extra_headers: None,
        })
    }

    fn data(&mut self, stream: StreamId, bytes: &[u8], end_stream: bool) -> Vec<u8> {
        self.frame(EgressFrame::Body {
            stream,
            body: queue(bytes),
            padding: None,
            end_stream,
        })
    }

    fn reset(&mut self, stream: StreamId, code: H2ErrorCode) -> Vec<u8> {
        self.frame(EgressFrame::Reset { stream, code })
    }

    fn window_update(&mut self, stream: StreamId, amount: u32) -> Vec<u8> {
        self.frame(EgressFrame::WindowUpdate { stream, amount })
    }

    fn goaway(&mut self, last_stream: StreamId) -> Vec<u8> {
        self.frame(EgressFrame::Goaway {
            last_stream,
            code: H2ErrorCode::NO_ERROR,
            debug_data: None,
        })
    }
}

/// HTTP/2 frames the session wrote, as the proxy decodes them.
#[derive(Default)]
struct Frames {
    resets: Vec<(StreamId, H2ErrorCode)>,
    window_updates: Vec<(StreamId, u32)>,
    data: Vec<(StreamId, Vec<u8>)>,
    ended: Vec<StreamId>,
    errors: Vec<StreamId>,
}

impl Frames {
    fn data_on(&self, stream: StreamId) -> Vec<u8> {
        self.data
            .iter()
            .filter(|(id, _)| *id == stream)
            .flat_map(|(_, bytes)| bytes.iter().copied())
            .collect()
    }

    fn credit_for(&self, stream: StreamId) -> usize {
        self.window_updates
            .iter()
            .filter(|(id, _)| *id == stream)
            .map(|(_, amount)| *amount as usize)
            .sum()
    }
}

impl HttpCodecCallback for Frames {
    fn on_event(&mut self, event: CodecEvent<'_>) {
        match event {
            CodecEvent::Abort { stream, code } => self.resets.push((stream, code)),
            CodecEvent::WindowUpdate { stream, amount } => {
                self.window_updates.push((stream, amount));
            }
            CodecEvent::Body { stream, body, .. } => self.data.push((stream, bytes_of(&body))),
            CodecEvent::MessageComplete { stream, .. } => self.ended.push(stream),
            CodecEvent::Error { stream, .. } => self.errors.push(stream),
            _ => {}
        }
    }
}

fn frames(wire: &Wire) -> Frames {
    let mut codec = Http2Codec::new(TransportDirection::Downstream);
    let mut frames = Frames::default();
    let mut output = BufQueue::new(1 << 20);
    let written = wire.written();
    let input = BufQueue::copy_from_slice(&written, written.len()).expect("decoded wire fits");
    let result = codec.on_ingress(&input, &mut frames, &mut output);
    assert_eq!(
        result.consumed,
        written.len(),
        "the proxy decodes every byte"
    );
    assert_eq!(frames.errors, [], "the proxy decodes no invalid frame");
    frames
}

fn resets(wire: &Wire) -> Vec<(StreamId, H2ErrorCode)> {
    frames(wire).resets
}

/// Stream lifecycle transitions observed on the session owner.
#[derive(Default)]
struct Lifecycle {
    detached: RefCell<Vec<StreamId>>,
    drains: RefCell<Vec<DrainTrigger>>,
}

impl Lifecycle {
    fn attach(session: &UniplexSession) -> Rc<Self> {
        let lifecycle = Rc::new(Self::default());
        session
            .add_lifecycle_observer(Rc::clone(&lifecycle) as Rc<dyn LifecycleObserver>)
            .expect("lifecycle observer");
        lifecycle
    }

    fn detached(&self, stream: StreamId) -> bool {
        self.detached.borrow().contains(&stream)
    }

    fn drains(&self) -> Vec<DrainTrigger> {
        self.drains.borrow().clone()
    }
}

impl LifecycleObserver for Lifecycle {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::TransactionDetached {
                stream: Some(stream),
            } => self.detached.borrow_mut().push(stream),
            LifecycleEvent::DrainStarted(trigger) => self.drains.borrow_mut().push(trigger),
            _ => {}
        }
    }
}

/// Counts wakes.
#[derive(Default)]
struct WakeCounter(AtomicUsize);

impl WakeCounter {
    fn waker(self: &Arc<Self>) -> Waker {
        Waker::from(Arc::clone(self))
    }

    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A waker that panics when woken.
struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("waker panic");
    }
}

/// Records how a request source completed.
#[derive(Clone, Default)]
struct UploadCompletion(Rc<RefCell<Option<Option<HttpError>>>>);

impl UploadCompletion {
    fn error_code(&self) -> Option<HttpErrorCode> {
        self.0
            .borrow()
            .as_ref()
            .and_then(|error| error.as_ref().map(HttpError::code))
    }
}

impl HttpStreamSourceCallback for UploadCompletion {
    fn source_complete(&mut self, _id: StreamId, error: Option<HttpError>) {
        *self.0.borrow_mut() = Some(error);
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

/// Aborts another stream from the upload source's completion callback, which
/// the session runs while it processes that upload's stream.
struct AbortOnUploadComplete(Rc<RefCell<Option<UniplexStreamAbort>>>);

impl HttpStreamSourceCallback for AbortOnUploadComplete {
    fn source_complete(&mut self, _id: StreamId, _error: Option<HttpError>) {
        let abort = self.0.borrow_mut().take();
        if let Some(abort) = abort {
            abort.abort();
        }
    }
}

/// Aborts a stream from a lifecycle observer when the peer's GOAWAY starts the drain.
#[derive(Default)]
struct AbortOnPeerGoaway(RefCell<Option<UniplexStreamAbort>>);

impl LifecycleObserver for AbortOnPeerGoaway {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(
            event,
            LifecycleEvent::DrainStarted(DrainTrigger::PeerGoaway)
        ) {
            let abort = self.0.borrow_mut().take();
            if let Some(abort) = abort {
                abort.abort();
            }
        }
    }
}

/// Aborts at the first writer wake at or after `deadline` that finds the abort
/// in `slot`, which on an idle session is the stream flow-control timeout
/// armed after `deadline - timeout`.
fn abort_at_flow_timeout(
    handle: UniplexHandle,
    slot: Rc<RefCell<Option<UniplexStreamAbort>>>,
    deadline: tokio::time::Instant,
    fired: Rc<Cell<bool>>,
) -> Box<dyn FnOnce()> {
    Box::new(move || {
        let abort = if tokio::time::Instant::now() < deadline {
            None
        } else {
            slot.borrow_mut().take()
        };
        if let Some(abort) = abort {
            abort.abort();
            fired.set(true);
            return;
        }
        let rearmed = abort_at_flow_timeout(handle.clone(), slot, deadline, fired);
        handle.set_writer_wait_hooks_for_test(WriterWaitHooks {
            before: None,
            after: Some(rearmed),
        });
    })
}

/// Panics on the drain that an HTTP/1.1 stream reset starts.
struct PanicOnStreamReset;

impl LifecycleObserver for PanicOnStreamReset {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(
            event,
            LifecycleEvent::DrainStarted(DrainTrigger::StreamReset)
        ) {
            panic!("lifecycle observer panic");
        }
    }
}

/// A request whose CONNECT head is queued now and whose upload the test writes.
fn upload_source(
    callback: Option<Box<dyn HttpStreamSourceCallback>>,
) -> (HttpSourceHolder, HttpStreamSourceProducer) {
    let source = HttpStreamSource::new_async(None, None, 1 << 20).expect("upload source");
    if let Some(callback) = callback {
        source.set_callback(Some(callback));
    }
    let producer = source.producer();
    assert!(producer.headers(connect_request(), false));
    (HttpSourceHolder::new(Box::new(source)), producer)
}

/// A request source whose `stop_reading` stops the upload, then panics.
struct PanicOnStop {
    source: HttpStreamSource,
    stopped: Rc<Cell<bool>>,
}

impl HttpSource for PanicOnStop {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.source.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.source.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.source.stop_reading(error);
        self.stopped.set(true);
        panic!("request source stop_reading panic");
    }

    fn get_stream_id(&self) -> Option<StreamId> {
        self.source.get_stream_id()
    }

    fn ingress_completion(&self) -> Option<IngressCompletion> {
        self.source.ingress_completion()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.source.set_read_timeout(timeout);
    }
}

fn send(upload: &HttpStreamSourceProducer, bytes: &[u8]) {
    assert!(upload.body(queue(bytes), 0, false) != FlowControlState::Error);
}

/// One CONNECT tunnel whose 200 the reader has read.
struct Tunnel {
    response: UniplexResponse,
    upload: HttpStreamSourceProducer,
}

impl Tunnel {
    fn take_abort(&mut self) -> UniplexStreamAbort {
        self.response
            .take_stream_abort()
            .expect("a committed request carries its abort")
    }

    fn read(&mut self) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.response.source.read_body_event(u32::MAX)
    }

    async fn read_body(&mut self, expected: usize) -> Vec<u8> {
        let mut received = Vec::new();
        while received.len() < expected {
            let event = self.read().await.expect("tunnel body");
            if let Some(body) = event.as_body() {
                received.extend_from_slice(&bytes_of(body));
            }
            assert!(!event.eom(), "the tunnel ended after {}", received.len());
        }
        received
    }

    async fn read_to_eom(&mut self) -> Vec<u8> {
        let mut received = Vec::new();
        loop {
            let event = self.read().await.expect("tunnel body");
            if let Some(body) = event.as_body() {
                received.extend_from_slice(&bytes_of(body));
            }
            if event.eom() {
                return received;
            }
        }
    }

    /// Starts a read that parks, after the suspend that precedes a first wait.
    fn park_read(&mut self, waker: &Waker) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        loop {
            let mut read = self.read();
            match read.as_mut().poll(&mut Context::from_waker(waker)) {
                Poll::Pending => return read,
                Poll::Ready(Ok(event)) if event.as_body().is_none() && !event.eom() => {}
                Poll::Ready(Ok(_)) => panic!("the read returned tunnel bytes instead of parking"),
                Poll::Ready(Err(error)) => panic!("the read failed instead of parking: {error:?}"),
            }
        }
    }

    /// The reader's next read, which must fail.
    async fn read_error(&mut self) -> HttpError {
        self.read()
            .await
            .err()
            .expect("the read fails instead of returning tunnel bytes")
    }
}

async fn open_tunnel(
    handle: &UniplexHandle,
    wire: &Wire,
    peer: &mut H2Peer,
    stream: StreamId,
    callback: Option<Box<dyn HttpStreamSourceCallback>>,
) -> Tunnel {
    let (request, upload) = upload_source(callback);
    commit_tunnel(handle, wire, peer, stream, request, upload).await
}

async fn commit_tunnel(
    handle: &UniplexHandle,
    wire: &Wire,
    peer: &mut H2Peer,
    stream: StreamId,
    request: HttpSourceHolder,
    upload: HttpStreamSourceProducer,
) -> Tunnel {
    let mut response = handle
        .send_request(request, CancellationToken::never())
        .await
        .expect("the CONNECT is committed");
    assert_eq!(response.stream, stream);
    wire.deliver(peer.headers(stream, &status(200), false));
    let mut head = response
        .source
        .read_header_event()
        .await
        .expect("the CONNECT response");
    assert_eq!(head.headers_mut().status_code().expect("status"), 200);
    Tunnel { response, upload }
}

fn h2_session(
    context: WorkerContext,
    config: Http2SessionConfig,
    settings: &[(SettingsId, u64)],
) -> (UniplexSession, UniplexHandle, Wire, H2Peer, Rc<Lifecycle>) {
    h2_session_with(context, UniplexConfig::default(), config, settings)
}

fn h2_session_with(
    context: WorkerContext,
    uniplex: UniplexConfig,
    config: Http2SessionConfig,
    settings: &[(SettingsId, u64)],
) -> (UniplexSession, UniplexHandle, Wire, H2Peer, Rc<Lifecycle>) {
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new_http2(
        context,
        TransportDirection::Upstream,
        wire.transport(),
        None,
        uniplex,
        config,
    )
    .expect("upstream HTTP/2 session");
    let lifecycle = Lifecycle::attach(&session);
    let (peer, server_settings) = H2Peer::server(settings);
    wire.deliver(server_settings);
    (session, handle, wire, peer, lifecycle)
}

/// Waits until the session has read and parsed every byte the peer sent.
async fn ingress_consumed(wire: &Wire, handle: &UniplexHandle) {
    eventually("the session to consume the peer's bytes", || {
        wire.reads_drained()
            && !matches!(handle.buffered_ingress_for_test(), Ok(bytes) if !bytes.is_empty())
    })
    .await;
}

async fn reset_sent(wire: &Wire, stream: StreamId) {
    eventually("a RST_STREAM", || {
        resets(wire).iter().any(|(id, _)| *id == stream)
    })
    .await;
}

/// Ends the tunnel's upload, then its response, and reads that response.
async fn finish_tunnel(tunnel: &mut Tunnel, wire: &Wire, peer: &mut H2Peer, lifecycle: &Lifecycle) {
    let stream = tunnel.response.stream;
    tunnel.upload.eom();
    wire.deliver(peer.data(stream, b"", true));
    tunnel.read_to_eom().await;
    eventually("the stream to detach", || lifecycle.detached(stream)).await;
}

fn assert_cancel(error: &HttpError) {
    assert_eq!(error.code(), HttpErrorCode::CANCEL, "{error:?}");
}

/// Sends connection-flow-controlled DATA within the peer's view of the window.
async fn send_within_window(
    wire: &Wire,
    peer: &mut H2Peer,
    stream: StreamId,
    total: usize,
    window: usize,
    sent: &mut usize,
) {
    let mut remaining = total;
    while remaining != 0 {
        let available = || (window + frames(wire).credit_for(StreamId::UNSPECIFIED)) - *sent;
        eventually("connection credit", || available() != 0).await;
        let chunk = available().min(remaining).min(16_384);
        wire.deliver(peer.data(stream, &vec![b'x'; chunk], false));
        remaining -= chunk;
        *sent += chunk;
    }
}

#[test]
fn h2_abort_resets_only_its_stream_with_cancel() {
    run_on_worker(|context| {
        Box::pin(async move {
            // A four-byte stream window holds the sibling's upload in the session.
            let (session, handle, wire, mut peer, lifecycle) = h2_session(
                context,
                Http2SessionConfig::default(),
                &[(SettingsId::InitialWindowSize, 4)],
            );
            let observed = wire.clone();
            let exchange = async move {
                let mut aborted = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                send(&aborted.upload, b"up");
                send(&sibling.upload, b"sibling");
                observed.deliver(peer.data(STREAM_3, b"down", false));
                eventually("the uploads within the window", || {
                    let written = frames(&observed);
                    written.data_on(STREAM_1) == b"up" && written.data_on(STREAM_3) == b"sibl"
                })
                .await;
                ingress_consumed(&observed, &handle).await;
                aborted.take_abort().abort();
                reset_sent(&observed, STREAM_1).await;
                assert_cancel(&aborted.read_error().await);
                observed.deliver(peer.window_update(STREAM_3, 1 << 16));
                eventually("the sibling's queued upload", || {
                    frames(&observed).data_on(STREAM_3) == b"sibling"
                })
                .await;
                assert_eq!(sibling.read_body(4).await, b"down");
                send(&sibling.upload, b"more");
                observed.deliver(peer.data(STREAM_3, b"later", false));
                assert_eq!(sibling.read_body(5).await, b"later");
                eventually("the sibling's later upload", || {
                    frames(&observed).data_on(STREAM_3) == b"siblingmore"
                })
                .await;
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });
}

#[test]
fn h2_abort_after_request_eom_resets_streaming_response() {
    run_on_worker(|context| {
        Box::pin(async move {
            // No stream read timeout can wake the parked reader below.
            let (session, handle, wire, mut peer, lifecycle) = h2_session_with(
                context,
                UniplexConfig {
                    stream_read_timeout: Duration::ZERO,
                    ..UniplexConfig::default()
                },
                Http2SessionConfig::default(),
                &[],
            );
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                send(&tunnel.upload, b"last");
                tunnel.upload.eom();
                eventually("the upload END_STREAM", || {
                    frames(&observed).ended.contains(&STREAM_1)
                })
                .await;
                observed.deliver(peer.data(STREAM_1, b"streaming", false));
                assert_eq!(tunnel.read_body(9).await, b"streaming");
                tunnel.take_abort().abort();
                reset_sent(&observed, STREAM_1).await;
                assert_cancel(&tunnel.read_error().await);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;

                // An ingress-completion waker that panics while the reset fails
                // the response: the reset still finishes and the already-parked
                // reader still wakes, then the panic resumes.
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                tunnel.upload.eom();
                eventually("the upload END_STREAM", || {
                    frames(&observed).ended.contains(&STREAM_3)
                })
                .await;
                observed.deliver(peer.data(STREAM_3, b"streaming", false));
                assert_eq!(tunnel.read_body(9).await, b"streaming");
                let completion = tunnel
                    .response
                    .source
                    .ingress_completion()
                    .expect("the response observes its completion");
                let mut completion_wait = completion.wait();
                let panicking = Waker::from(Arc::new(PanicOnWake));
                assert!(
                    completion_wait
                        .as_mut()
                        .poll(&mut Context::from_waker(&panicking))
                        .is_pending()
                );
                let reader = Arc::new(WakeCounter::default());
                let mut read = tunnel.park_read(&reader.waker());
                let abort = tunnel.take_abort();
                assert!(
                    catch_unwind(AssertUnwindSafe(|| abort.abort())).is_err(),
                    "the waker's panic reaches the caller"
                );
                assert!(reader.wakes() > 0, "the abort wakes the parked reader");
                let Poll::Ready(Err(error)) = read
                    .as_mut()
                    .poll(&mut Context::from_waker(&reader.waker()))
                else {
                    panic!("the parked read fails");
                };
                assert_cancel(&error);
                drop(read);
                drop(completion_wait);
                reset_sent(&observed, STREAM_3).await;
                let Some(Err(error)) = completion.snapshot() else {
                    panic!("the response completed with an error");
                };
                assert_cancel(&error);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_3)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                resets(&wire),
                [
                    (STREAM_1, H2ErrorCode::CANCEL),
                    (STREAM_3, H2ErrorCode::CANCEL)
                ]
            );
        })
    });
}

#[test]
fn h2_abort_during_peer_goaway_drain_resets_the_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let on_goaway = Rc::new(AbortOnPeerGoaway::default());
            session
                .add_lifecycle_observer(Rc::clone(&on_goaway) as Rc<dyn LifecycleObserver>)
                .expect("lifecycle observer");
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let mut observer_aborted =
                    open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                *on_goaway.0.borrow_mut() = Some(observer_aborted.take_abort());
                observed.deliver(peer.goaway(STREAM_3));
                eventually("the peer GOAWAY drain", || {
                    lifecycle.drains().contains(&DrainTrigger::PeerGoaway)
                })
                .await;
                assert!(on_goaway.0.borrow().is_none(), "the observer aborted");
                reset_sent(&observed, STREAM_3).await;
                assert_cancel(&observer_aborted.read_error().await);
                assert_eq!(
                    handle.closing_state_for_test(),
                    (false, false),
                    "a draining session is open"
                );
                tunnel.take_abort().abort();
                reset_sent(&observed, STREAM_1).await;
                assert_cancel(&tunnel.read_error().await);
                eventually("the aborted streams to detach", || {
                    lifecycle.detached(STREAM_1) && lifecycle.detached(STREAM_3)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                resets(&wire),
                [
                    (STREAM_3, H2ErrorCode::CANCEL),
                    (STREAM_1, H2ErrorCode::CANCEL)
                ]
            );
        })
    });
}

#[test]
fn h2_abort_during_local_drain_resets_the_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                assert!(handle.initiate_drain().expect("drain"));
                assert!(lifecycle.drains().contains(&DrainTrigger::Local));
                assert_eq!(
                    handle.closing_state_for_test(),
                    (false, false),
                    "a draining session is open"
                );
                tunnel.take_abort().abort();
                reset_sent(&observed, STREAM_1).await;
                assert_cancel(&tunnel.read_error().await);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });
}

#[test]
fn h2_abort_fails_reader_and_pending_egress() {
    run_on_worker(|context| {
        Box::pin(async move {
            // A four-byte stream window parks the rest of the upload in the session.
            let (session, handle, wire, mut peer, lifecycle) = h2_session(
                context,
                Http2SessionConfig::default(),
                &[(SettingsId::InitialWindowSize, 4)],
            );
            let observed = wire.clone();
            let exchange = async move {
                let completion = UploadCompletion::default();
                let mut tunnel = open_tunnel(
                    &handle,
                    &observed,
                    &mut peer,
                    STREAM_1,
                    Some(Box::new(completion.clone())),
                )
                .await;
                send(&tunnel.upload, b"0123456789");
                eventually("the window's bytes", || {
                    frames(&observed).data_on(STREAM_1) == b"0123"
                })
                .await;
                let reader = Arc::new(WakeCounter::default());
                let mut read = tunnel.park_read(&reader.waker());
                tunnel.take_abort().abort();
                assert!(reader.wakes() > 0, "the abort wakes the parked reader");
                let Poll::Ready(Err(error)) = read
                    .as_mut()
                    .poll(&mut Context::from_waker(&reader.waker()))
                else {
                    panic!("the parked read fails");
                };
                assert_cancel(&error);
                drop(read);
                reset_sent(&observed, STREAM_1).await;
                eventually("the upload to fail", || completion.error_code().is_some()).await;
                assert_eq!(
                    tunnel.upload.body(queue(b"later"), 0, false),
                    FlowControlState::Error,
                    "the upload accepts no more bytes"
                );
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                // Window the peer reopens after the RST_STREAM carries no queued byte.
                observed.deliver(peer.window_update(STREAM_1, 1 << 16));
                ingress_consumed(&observed, &handle).await;
                settle().await;
                assert_eq!(frames(&observed).data_on(STREAM_1), b"0123");
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });
}

#[test]
fn h2_abort_returns_connection_credit_for_unread_data() {
    abort_returns_connection_credit_for_unread_data(UniplexConfig::default());
}

/// A local abort discards what its reader has not read, even where an ingress
/// abort would leave it to be read first (`DIV-033`).
#[test]
fn h2_abort_returns_connection_credit_for_unread_data_with_ingress_before_abort() {
    abort_returns_connection_credit_for_unread_data(UniplexConfig {
        deliver_ingress_before_abort: true,
        ..UniplexConfig::default()
    });
}

fn abort_returns_connection_credit_for_unread_data(uniplex: UniplexConfig) {
    run_on_worker(move |context| {
        Box::pin(async move {
            // The windows admit more unread DATA than half the stream window, the
            // threshold at which stream credit would be acknowledged.
            let config = Http2SessionConfig {
                connection_receive_window: 4 * WINDOW as u32,
                stream_receive_window: 4 * WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let unread = 3 * WINDOW;
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session_with(context, uniplex, config, &[]);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let mut sent = 0;
                send_within_window(&observed, &mut peer, STREAM_1, unread, WINDOW, &mut sent).await;
                ingress_consumed(&observed, &handle).await;
                let connection_credit = || frames(&observed).credit_for(StreamId::UNSPECIFIED);
                let before = connection_credit();
                tunnel.take_abort().abort();
                eventually("connection credit for the unread DATA", || {
                    connection_credit() >= before + unread
                })
                .await;
                let written = frames(&observed);
                assert_eq!(written.credit_for(StreamId::UNSPECIFIED), before + unread);
                assert_eq!(
                    written.credit_for(STREAM_1),
                    0,
                    "the aborted stream acknowledged its unread DATA"
                );
                assert_cancel(&tunnel.read_error().await);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;

                // DATA the peer had in flight when the RST_STREAM went out. The
                // session acknowledges connection credit once more than half the
                // window is pending, so eight full frames return in one update.
                let late = 8 * 16_384;
                let before = connection_credit();
                send_within_window(&observed, &mut peer, STREAM_1, late, WINDOW, &mut sent).await;
                ingress_consumed(&observed, &handle).await;
                eventually("connection credit for the late DATA", || {
                    connection_credit() >= before + late
                })
                .await;
                assert_eq!(connection_credit(), before + late);

                // The response END_STREAM arrived unread while the upload is open.
                let mut half_closed =
                    open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                send_within_window(&observed, &mut peer, STREAM_3, unread, WINDOW, &mut sent).await;
                observed.deliver(peer.data(STREAM_3, b"", true));
                ingress_consumed(&observed, &handle).await;
                let before = connection_credit();
                half_closed.take_abort().abort();
                reset_sent(&observed, STREAM_3).await;
                eventually("connection credit for the unread DATA", || {
                    connection_credit() >= before + unread
                })
                .await;
                assert_eq!(connection_credit(), before + unread);
                assert_eq!(frames(&observed).credit_for(STREAM_3), 0);
                assert_cancel(&half_closed.read_error().await);
                eventually("the half-closed stream to detach", || {
                    lifecycle.detached(STREAM_3)
                })
                .await;

                // Every discarded byte returned its credit: a sibling receives
                // the whole connection window without reading.
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_5, None).await;
                send_within_window(
                    &observed,
                    &mut peer,
                    STREAM_5,
                    4 * WINDOW,
                    WINDOW,
                    &mut sent,
                )
                .await;
                ingress_consumed(&observed, &handle).await;
                assert_eq!(sibling.read_body(4 * WINDOW).await.len(), 4 * WINDOW);
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(
                resets(&wire),
                [
                    (STREAM_1, H2ErrorCode::CANCEL),
                    (STREAM_3, H2ErrorCode::CANCEL)
                ]
            );
            assert_eq!(frames(&wire).credit_for(STREAM_1), 0);
            assert_eq!(frames(&wire).credit_for(STREAM_3), 0);
        })
    });
}

/// What the reader of a stream the peer reset does with the DATA that the reset
/// left to be read first (`DIV-033`).
#[derive(Clone, Copy, Debug)]
enum KeptData {
    Read,
    Dropped,
}

#[test]
fn h2_peer_reset_returns_connection_credit_for_kept_data_as_it_is_read() {
    peer_reset_returns_connection_credit_for_kept_data(KeptData::Read);
}

#[test]
fn h2_peer_reset_returns_connection_credit_for_kept_data_dropped_unread() {
    peer_reset_returns_connection_credit_for_kept_data(KeptData::Dropped);
}

/// With `deliver_ingress_before_abort`, a peer's RST_STREAM leaves the DATA the
/// reader has not read to be read before the reset, and the stream holds that
/// DATA's connection credit until its reader takes it or drops the response.
/// Either returns all of it, so a sibling then receives the whole connection
/// window without reading.
fn peer_reset_returns_connection_credit_for_kept_data(kept: KeptData) {
    run_on_worker(move |context| {
        Box::pin(async move {
            // More unread DATA than half the connection window, the threshold at
            // which connection credit is acknowledged.
            let config = Http2SessionConfig {
                connection_receive_window: 4 * WINDOW as u32,
                stream_receive_window: 4 * WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let unread = 3 * WINDOW;
            let uniplex = UniplexConfig {
                deliver_ingress_before_abort: true,
                ..UniplexConfig::default()
            };
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session_with(context, uniplex, config, &[]);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let connection_credit = || frames(&observed).credit_for(StreamId::UNSPECIFIED);
                let before = connection_credit();
                let mut sent = 0;
                send_within_window(&observed, &mut peer, STREAM_1, unread, WINDOW, &mut sent).await;
                observed.deliver(peer.reset(STREAM_1, H2ErrorCode::CANCEL));
                ingress_consumed(&observed, &handle).await;
                settle().await;
                assert_eq!(
                    connection_credit(),
                    before,
                    "{kept:?}: the kept DATA holds its credit until it is read"
                );
                match kept {
                    KeptData::Read => {
                        assert_eq!(tunnel.read_body(unread).await.len(), unread);
                        assert_cancel(&tunnel.read_error().await);
                    }
                    KeptData::Dropped => drop(tunnel),
                }
                eventually("connection credit for the kept DATA", || {
                    connection_credit() >= before + unread
                })
                .await;
                assert_eq!(connection_credit(), before + unread, "{kept:?}");
                assert_eq!(
                    frames(&observed).credit_for(STREAM_1),
                    0,
                    "{kept:?}: the reset stream acknowledged nothing"
                );
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;

                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                send_within_window(
                    &observed,
                    &mut peer,
                    STREAM_3,
                    4 * WINDOW,
                    WINDOW,
                    &mut sent,
                )
                .await;
                ingress_consumed(&observed, &handle).await;
                assert_eq!(sibling.read_body(4 * WINDOW).await.len(), 4 * WINDOW);
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), []);
        })
    });
}

#[test]
fn h2_abort_keeps_sibling_streams_and_session_usable() {
    run_on_worker(|context| {
        Box::pin(async move {
            // Two concurrent streams: the third request needs the aborted stream's slot.
            let (session, handle, wire, mut peer, lifecycle) = h2_session(
                context,
                Http2SessionConfig::default(),
                &[(SettingsId::MaxConcurrentStreams, 2)],
            );
            let observed = wire.clone();
            let exchange = async move {
                // The aborted stream's upload callback aborts the sibling while
                // the session resets the aborted stream; the callout cannot act.
                let paired = Rc::new(RefCell::new(None));
                let mut aborted = open_tunnel(
                    &handle,
                    &observed,
                    &mut peer,
                    STREAM_1,
                    Some(Box::new(AbortOnUploadComplete(Rc::clone(&paired)))),
                )
                .await;
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                *paired.borrow_mut() = Some(sibling.take_abort());
                assert_eq!(handle.outgoing_transaction_activity().available(), 0);
                aborted.take_abort().abort();
                assert!(paired.borrow().is_none(), "the upload callback ran");
                drop(aborted);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                assert_eq!(handle.outgoing_transaction_activity().available(), 1);
                send(&sibling.upload, b"ping");
                eventually("the sibling upload", || {
                    frames(&observed).data_on(STREAM_3) == b"ping"
                })
                .await;
                observed.deliver(peer.data(STREAM_3, b"pong", false));
                assert_eq!(sibling.read_body(4).await, b"pong");
                let mut third = open_tunnel(&handle, &observed, &mut peer, STREAM_5, None).await;
                send(&third.upload, b"third");
                eventually("the third upload", || {
                    frames(&observed).data_on(STREAM_5) == b"third"
                })
                .await;
                finish_tunnel(&mut third, &observed, &mut peer, &lifecycle).await;
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });

    // The aborted stream's upload alone exhausted the connection window. The
    // abort lands after the connection flow-control timeout fired and before
    // the writer handled it, so the stale timeout fails neither the sibling
    // nor the session. The proxy stays silent until then, so no ingress
    // re-arms the timeout or wakes the writer.
    run_on_worker(|context| {
        Box::pin(async move {
            let write_timeout = Duration::from_millis(100);
            // Stream windows wider than the connection window.
            let (session, handle, wire, mut peer, lifecycle) = h2_session_with(
                context,
                UniplexConfig {
                    write_timeout,
                    ..UniplexConfig::default()
                },
                Http2SessionConfig::default(),
                &[(SettingsId::InitialWindowSize, 1 << 20)],
            );
            let observed = wire.clone();
            let exchange = async move {
                let mut aborted = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                ingress_consumed(&observed, &handle).await;
                let slot = Rc::new(RefCell::new(Some(aborted.take_abort())));
                let fired = Rc::new(Cell::new(false));
                // Set before the upload blocks, so the timeout fires at or after it.
                let deadline = tokio::time::Instant::now() + write_timeout;
                handle.set_writer_wait_hooks_for_test(WriterWaitHooks {
                    before: None,
                    after: Some(abort_at_flow_timeout(
                        handle.clone(),
                        Rc::clone(&slot),
                        deadline,
                        Rc::clone(&fired),
                    )),
                });
                send(&aborted.upload, &vec![b'x'; WINDOW + 100]);
                eventually("the abort at the flow-control timeout", || fired.get()).await;
                reset_sent(&observed, STREAM_1).await;
                assert_cancel(&aborted.read_error().await);
                assert_eq!(frames(&observed).data_on(STREAM_1).len(), WINDOW);
                observed.deliver(peer.window_update(StreamId::UNSPECIFIED, WINDOW as u32));
                send(&sibling.upload, b"ping");
                eventually("the sibling upload", || {
                    frames(&observed).data_on(STREAM_3) == b"ping"
                })
                .await;
                observed.deliver(peer.data(STREAM_3, b"pong", false));
                assert_eq!(sibling.read_body(4).await, b"pong");
                let mut third = open_tunnel(&handle, &observed, &mut peer, STREAM_5, None).await;
                send(&third.upload, b"third");
                eventually("the third upload", || {
                    frames(&observed).data_on(STREAM_5) == b"third"
                })
                .await;
                finish_tunnel(&mut third, &observed, &mut peer, &lifecycle).await;
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });

    // Two callbacks that the abort runs both panic: an ingress-completion
    // waker and the request source's `stop_reading`. The stream still gets
    // exactly one RST_STREAM(CANCEL), the parked reader wakes, and the stream
    // retires; the first panic resumes last and the sibling is unaffected.
    run_on_worker(|context| {
        Box::pin(async move {
            // No stream read timeout can wake the parked reader below.
            let (session, handle, wire, mut peer, lifecycle) = h2_session_with(
                context,
                UniplexConfig {
                    stream_read_timeout: Duration::ZERO,
                    ..UniplexConfig::default()
                },
                Http2SessionConfig::default(),
                &[],
            );
            let observed = wire.clone();
            let exchange = async move {
                let source =
                    HttpStreamSource::new_async(None, None, 1 << 20).expect("upload source");
                let upload = source.producer();
                assert!(upload.headers(connect_request(), false));
                let stopped = Rc::new(Cell::new(false));
                let request = HttpSourceHolder::new(Box::new(PanicOnStop {
                    source,
                    stopped: Rc::clone(&stopped),
                }));
                let mut aborted =
                    commit_tunnel(&handle, &observed, &mut peer, STREAM_1, request, upload).await;
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                send(&aborted.upload, b"up");
                eventually("the aborted stream's upload", || {
                    frames(&observed).data_on(STREAM_1) == b"up"
                })
                .await;
                observed.deliver(peer.data(STREAM_1, b"down", false));
                assert_eq!(aborted.read_body(4).await, b"down");
                let completion = aborted
                    .response
                    .source
                    .ingress_completion()
                    .expect("the response observes its completion");
                let mut completion_wait = completion.wait();
                let panicking = Waker::from(Arc::new(PanicOnWake));
                assert!(
                    completion_wait
                        .as_mut()
                        .poll(&mut Context::from_waker(&panicking))
                        .is_pending()
                );
                let reader = Arc::new(WakeCounter::default());
                let mut read = aborted.park_read(&reader.waker());
                let abort = aborted.take_abort();
                let panic = catch_unwind(AssertUnwindSafe(|| abort.abort()))
                    .expect_err("a callback's panic reaches the caller");
                assert_eq!(
                    panic.downcast_ref::<&str>(),
                    Some(&"waker panic"),
                    "the first panic resumes"
                );
                assert!(
                    stopped.get(),
                    "the request source's stop_reading panicked too"
                );
                assert!(reader.wakes() > 0, "the abort wakes the parked reader");
                let Poll::Ready(Err(error)) = read
                    .as_mut()
                    .poll(&mut Context::from_waker(&reader.waker()))
                else {
                    panic!("the parked read fails");
                };
                assert_cancel(&error);
                drop(read);
                drop(completion_wait);
                let Some(Err(error)) = completion.snapshot() else {
                    panic!("the response completed with an error");
                };
                assert_cancel(&error);
                reset_sent(&observed, STREAM_1).await;
                assert_eq!(
                    aborted.upload.body(queue(b"later"), 0, false),
                    FlowControlState::Error,
                    "the upload accepts no more bytes"
                );
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                send(&sibling.upload, b"ping");
                eventually("the sibling upload", || {
                    frames(&observed).data_on(STREAM_3) == b"ping"
                })
                .await;
                observed.deliver(peer.data(STREAM_3, b"pong", false));
                assert_eq!(sibling.read_body(4).await, b"pong");
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });
}

#[test]
fn abort_capability_is_taken_once_and_drop_is_inert() {
    // Fails to compile if the capability becomes `Send`: both impls then apply.
    trait AmbiguousIfSend<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}
    <UniplexStreamAbort as AmbiguousIfSend<_>>::check();

    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let abort = tunnel.take_abort();
                assert!(tunnel.response.take_stream_abort().is_none());
                drop(abort);
                send(&tunnel.upload, b"still open");
                observed.deliver(peer.data(STREAM_1, b"reply", false));
                assert_eq!(tunnel.read_body(5).await, b"reply");
                eventually("the upload", || {
                    frames(&observed).data_on(STREAM_1) == b"still open"
                })
                .await;
                finish_tunnel(&mut tunnel, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), []);
        })
    });
}

#[test]
fn abort_after_wire_completion_sends_no_frame() {
    run_on_worker(|context| {
        Box::pin(async move {
            let config = Http2SessionConfig {
                connection_receive_window: 4 * WINDOW as u32,
                stream_receive_window: 4 * WINDOW as u32,
                ..Http2SessionConfig::default()
            };
            let unread = 3 * WINDOW;
            let (session, handle, wire, mut peer, lifecycle) = h2_session(context, config, &[]);
            let observed = wire.clone();
            let exchange = async move {
                // Unread DATA and END_STREAM after the upload's END_STREAM.
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                tunnel.upload.eom();
                eventually("the upload END_STREAM", || {
                    frames(&observed).ended.contains(&STREAM_1)
                })
                .await;
                let mut sent = 0;
                send_within_window(&observed, &mut peer, STREAM_1, unread, WINDOW, &mut sent).await;
                observed.deliver(peer.data(STREAM_1, b"", true));
                ingress_consumed(&observed, &handle).await;
                let connection_credit = || frames(&observed).credit_for(StreamId::UNSPECIFIED);
                let before = connection_credit();
                tunnel.take_abort().abort();
                eventually("connection credit for the unread DATA", || {
                    connection_credit() >= before + unread
                })
                .await;
                assert_eq!(connection_credit(), before + unread);
                assert_eq!(frames(&observed).credit_for(STREAM_1), 0);
                assert_cancel(&tunnel.read_error().await);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;

                // A read parked before the END_STREAM arrived fails at its next poll.
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                tunnel.upload.eom();
                eventually("the upload END_STREAM", || {
                    frames(&observed).ended.contains(&STREAM_3)
                })
                .await;
                let reader = Arc::new(WakeCounter::default());
                let mut read = tunnel.park_read(&reader.waker());
                observed.deliver(peer.data(STREAM_3, b"unread", true));
                ingress_consumed(&observed, &handle).await;
                tunnel.take_abort().abort();
                let Poll::Ready(Err(error)) = read
                    .as_mut()
                    .poll(&mut Context::from_waker(&reader.waker()))
                else {
                    panic!("the parked read fails");
                };
                assert_cancel(&error);
                drop(read);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_3)
                })
                .await;
                settle().await;
                assert_eq!(resets(&observed), []);
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), []);
        })
    });
}

#[test]
fn abort_after_peer_reset_is_a_no_op() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let exchange = async move {
                // The reset stream's upload callback aborts its sibling while
                // the session processes the RST_STREAM; the callout cannot act.
                let paired = Rc::new(RefCell::new(None));
                let mut tunnel = open_tunnel(
                    &handle,
                    &observed,
                    &mut peer,
                    STREAM_1,
                    Some(Box::new(AbortOnUploadComplete(Rc::clone(&paired)))),
                )
                .await;
                let mut sibling = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                *paired.borrow_mut() = Some(sibling.take_abort());
                observed.deliver(peer.reset(STREAM_1, H2ErrorCode::REFUSED_STREAM));
                ingress_consumed(&observed, &handle).await;
                settle().await;
                assert!(paired.borrow().is_none(), "the upload callback ran");
                let written = observed.written();
                tunnel.take_abort().abort();
                settle().await;
                assert_eq!(observed.written(), written, "the abort wrote nothing");
                let error = tunnel.read_error().await;
                assert_eq!(error.code(), HttpErrorCode::REFUSED_STREAM, "{error:?}");
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                send(&sibling.upload, b"up");
                observed.deliver(peer.data(STREAM_3, b"down", false));
                assert_eq!(sibling.read_body(4).await, b"down");
                eventually("the sibling upload", || {
                    frames(&observed).data_on(STREAM_3) == b"up"
                })
                .await;
                finish_tunnel(&mut sibling, &observed, &mut peer, &lifecycle).await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), []);
        })
    });
}

#[test]
fn abort_after_local_reset_sends_no_second_rst_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let exchange = async move {
                // An upload source error resets the stream from the egress side.
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                assert!(
                    tunnel
                        .upload
                        .abort(HttpErrorCode::INTERNAL_ERROR, b"upload failed")
                );
                reset_sent(&observed, STREAM_1).await;
                let first = resets(&observed);
                assert_eq!(first.len(), 1);
                tunnel.take_abort().abort();
                settle().await;
                assert_eq!(resets(&observed), first, "no second RST_STREAM");
                let error = tunnel.read_error().await;
                assert_ne!(error.code(), HttpErrorCode::CANCEL, "{error:?}");
                eventually("the reset stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire).len(), 1);
        })
    });

    // The abort lands after the stream flow-control timeout fired and before
    // the writer handled it: the timeout adds no second RST_STREAM. The proxy
    // stays silent, so no ingress re-arms the timeout or wakes the writer.
    run_on_worker(|context| {
        Box::pin(async move {
            let write_timeout = Duration::from_millis(100);
            let (session, handle, wire, _peer, lifecycle) = h2_session_with(
                context,
                UniplexConfig {
                    write_timeout,
                    ..UniplexConfig::default()
                },
                Http2SessionConfig::default(),
                &[(SettingsId::InitialWindowSize, 0)],
            );
            let observed = wire.clone();
            let exchange = async move {
                let deadline = tokio::time::Instant::now() + write_timeout;
                let slot = Rc::new(RefCell::new(None));
                let fired = Rc::new(Cell::new(false));
                handle.set_writer_wait_hooks_for_test(WriterWaitHooks {
                    before: None,
                    after: Some(abort_at_flow_timeout(
                        handle.clone(),
                        Rc::clone(&slot),
                        deadline,
                        Rc::clone(&fired),
                    )),
                });
                let (request, _upload) = upload_source(None);
                let mut response = handle
                    .send_request(request, CancellationToken::never())
                    .await
                    .expect("the CONNECT is committed");
                *slot.borrow_mut() = response.take_stream_abort();
                eventually("the abort at the flow-control timeout", || fired.get()).await;
                reset_sent(&observed, STREAM_1).await;
                let error = response
                    .source
                    .read_header_event()
                    .await
                    .err()
                    .expect("the aborted CONNECT has no response head");
                assert_cancel(&error);
                eventually("the aborted stream to detach", || {
                    lifecycle.detached(STREAM_1)
                })
                .await;
                settle().await;
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), [(STREAM_1, H2ErrorCode::CANCEL)]);
        })
    });
}

#[test]
fn abort_after_retirement_is_a_no_op() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let abort = tunnel.take_abort();
                finish_tunnel(&mut tunnel, &observed, &mut peer, &lifecycle).await;
                settle().await;
                let written = observed.written();
                abort.abort();
                settle().await;
                assert_eq!(observed.written(), written, "the abort wrote nothing");
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(resets(&wire), []);
        })
    });
}

#[test]
fn abort_on_closing_session_is_a_no_op() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, mut peer, _lifecycle) =
                h2_session(context, Http2SessionConfig::default(), &[]);
            let observed = wire.clone();
            let finished_handle = handle.clone();
            let exchange = async move {
                let mut closing = open_tunnel(&handle, &observed, &mut peer, STREAM_1, None).await;
                let mut finished = open_tunnel(&handle, &observed, &mut peer, STREAM_3, None).await;
                let finished_abort = finished.take_abort();
                // The peer's EOF fails the transport; the session closes once
                // both readers release their sources.
                observed.end();
                eventually("the session to close", || handle.closing_state_for_test().0).await;
                assert_eq!(handle.closing_state_for_test(), (true, false));
                settle().await;
                let written = observed.written();
                closing.take_abort().abort();
                settle().await;
                assert_eq!(observed.written(), written, "the abort wrote nothing");
                let error = closing.read_error().await;
                assert_ne!(error.code(), HttpErrorCode::CANCEL, "{error:?}");
                drop(closing);
                drop(finished);
                finished_abort
            };
            let (_, finished_abort) = tokio::join!(session.run(), exchange);
            assert!(finished_handle.is_finished());
            let written = wire.written();
            finished_abort.abort();
            assert_eq!(wire.written(), written, "the abort wrote nothing");
        })
    });
}

fn h1_session(context: WorkerContext) -> (UniplexSession, UniplexHandle, Wire, Rc<Lifecycle>) {
    let wire = Wire::new();
    let (session, handle) = UniplexSession::new(
        context,
        TransportDirection::Upstream,
        wire.transport(),
        None,
        UniplexConfig::default(),
    )
    .expect("HTTP/1.1 session");
    let lifecycle = Lifecycle::attach(&session);
    (session, handle, wire, lifecycle)
}

fn get_request() -> HttpMessage {
    let mut message = HttpMessage::new();
    message.set_method(HttpMethod::Get).expect("GET");
    message.set_url(b"/stream", false).expect("path");
    message.set_http_version(1, 1);
    message.headers_mut().set(HttpHeaderCode::Host, b"example");
    message
}

fn post_request() -> HttpMessage {
    let mut message = get_request();
    message.set_method(HttpMethod::Post).expect("POST");
    message
        .headers_mut()
        .set(HttpHeaderCode::ContentLength, b"10");
    message
}

/// Commits `request` on an HTTP/1.1 session and reads the head of `response`.
async fn h1_exchange(
    handle: &UniplexHandle,
    wire: &Wire,
    request: HttpMessage,
    eom: bool,
    response: &[u8],
) -> Tunnel {
    let before = wire.written().len();
    let source = HttpStreamSource::new_async(None, None, 1 << 20).expect("request source");
    let upload = source.producer();
    assert!(upload.headers(request, eom));
    let mut committed = handle
        .send_request(
            HttpSourceHolder::new(Box::new(source)),
            CancellationToken::never(),
        )
        .await
        .expect("the request is committed");
    eventually("the request head", || {
        let written = wire.written();
        written.len() > before && written.ends_with(b"\r\n\r\n")
    })
    .await;
    wire.deliver(response.to_vec());
    let head = committed
        .source
        .read_header_event()
        .await
        .expect("the response head");
    assert!(!head.eom());
    Tunnel {
        response: committed,
        upload,
    }
}

#[test]
fn h1_abort_starts_the_connection_drain() {
    // A streaming response on a reusable connection: the abort starts its drain.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) = h1_session(context);
            let observed = wire.clone();
            let exchange = async move {
                let mut exchange = h1_exchange(
                    &handle,
                    &observed,
                    get_request(),
                    true,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial",
                )
                .await;
                assert_eq!(exchange.read_body(7).await, b"partial");
                assert_eq!(lifecycle.drains(), []);
                exchange.take_abort().abort();
                assert_eq!(lifecycle.drains(), [DrainTrigger::StreamReset]);
                assert_cancel(&exchange.read_error().await);
                eventually("a TCP reset", || observed.close() == Close::Reset).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });

    // An upgraded CONNECT tunnel, whose connection already drains, ends with a
    // TCP reset instead of a FIN once the queued upload is written.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) = h1_session(context);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = h1_exchange(
                    &handle,
                    &observed,
                    connect_request(),
                    false,
                    b"HTTP/1.1 200 OK\r\n\r\n",
                )
                .await;
                observed.hold_writes();
                send(&tunnel.upload, b"queued");
                eventually("the upload's write", || observed.write_parked()).await;
                tunnel.take_abort().abort();
                assert_cancel(&tunnel.read_error().await);
                settle().await;
                assert_eq!(
                    observed.close(),
                    Close::Open,
                    "the reset waits for the write"
                );
                observed.release_writes();
                eventually("a TCP reset", || observed.close() == Close::Reset).await;
                assert!(observed.written().ends_with(b"queued"));
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });

    // After the upload's EOM the connection sent its FIN and keeps reading the
    // tunnel until the proxy closes; the abort ends it with a TCP reset.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) = h1_session(context);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = h1_exchange(
                    &handle,
                    &observed,
                    connect_request(),
                    false,
                    b"HTTP/1.1 200 OK\r\n\r\n",
                )
                .await;
                tunnel.upload.eom();
                eventually("the upload's FIN", || {
                    observed.close() == Close::ShutdownWrite
                })
                .await;
                observed.deliver(b"streaming".to_vec());
                assert_eq!(tunnel.read_body(9).await, b"streaming");
                tunnel.take_abort().abort();
                assert_cancel(&tunnel.read_error().await);
                eventually("a TCP reset", || observed.close() == Close::Reset).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });

    // The proxy's bytes and FIN arrive while the upload is open: the reader
    // consumes the bytes, then the abort fails its next read and the upload,
    // and resets the connection.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) = h1_session(context);
            let observed = wire.clone();
            let exchange = async move {
                let mut tunnel = h1_exchange(
                    &handle,
                    &observed,
                    connect_request(),
                    false,
                    b"HTTP/1.1 200 OK\r\n\r\n",
                )
                .await;
                observed.deliver(b"streaming".to_vec());
                assert_eq!(tunnel.read_body(9).await, b"streaming");
                observed.end();
                ingress_consumed(&observed, &handle).await;
                assert_eq!(observed.close(), Close::Open, "the upload is still open");
                tunnel.take_abort().abort();
                assert_cancel(&tunnel.read_error().await);
                eventually("a TCP reset", || observed.close() == Close::Reset).await;
                assert_eq!(
                    tunnel.upload.body(queue(b"late"), 0, false),
                    FlowControlState::Error,
                    "the upload accepts no more bytes"
                );
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });

    // A lifecycle observer that panics inside the abort reaches the caller
    // after the abort finished: the reader of a complete response whose
    // upload is open fails, and the connection resets.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, _lifecycle) = h1_session(context);
            session
                .add_lifecycle_observer(Rc::new(PanicOnStreamReset) as Rc<dyn LifecycleObserver>)
                .expect("lifecycle observer");
            let observed = wire.clone();
            let exchange = async move {
                let mut exchange = h1_exchange(
                    &handle,
                    &observed,
                    post_request(),
                    false,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nunread",
                )
                .await;
                ingress_consumed(&observed, &handle).await;
                let abort = exchange.take_abort();
                let aborted = std::panic::catch_unwind(AssertUnwindSafe(|| abort.abort()));
                assert!(aborted.is_err(), "the observer's panic reaches the caller");
                assert_cancel(&exchange.read_error().await);
                eventually("a TCP reset", || observed.close() == Close::Reset).await;
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_eq!(wire.close(), Close::Reset);
        })
    });

    // The no-op rules hold on a reusable connection: a wire-complete exchange
    // and a retired exchange's capability start no drain.
    run_on_worker(|context| {
        Box::pin(async move {
            let (session, handle, wire, lifecycle) = h1_session(context);
            let observed = wire.clone();
            let exchange = async move {
                let mut complete = h1_exchange(
                    &handle,
                    &observed,
                    get_request(),
                    true,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nunread",
                )
                .await;
                ingress_consumed(&observed, &handle).await;
                complete.take_abort().abort();
                assert_cancel(&complete.read_error().await);
                let stream = complete.response.stream;
                eventually("the aborted exchange to detach", || {
                    lifecycle.detached(stream)
                })
                .await;
                drop(complete);
                drop(
                    handle
                        .reserve_request()
                        .expect("the connection stays reusable"),
                );

                let mut retired = h1_exchange(
                    &handle,
                    &observed,
                    get_request(),
                    true,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone",
                )
                .await;
                let stale = retired.take_abort();
                assert_eq!(retired.read_to_eom().await, b"done");
                let stream = retired.response.stream;
                eventually("the retired exchange to detach", || {
                    lifecycle.detached(stream)
                })
                .await;
                drop(retired);
                let mut next = h1_exchange(
                    &handle,
                    &observed,
                    get_request(),
                    true,
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nhalf",
                )
                .await;
                assert_eq!(next.read_body(4).await, b"half");
                stale.abort();
                observed.deliver(b"rest".to_vec());
                assert_eq!(next.read_to_eom().await, b"rest");
                let stream = next.response.stream;
                eventually("the next exchange to detach", || lifecycle.detached(stream)).await;
                drop(next);
                assert_eq!(lifecycle.drains(), []);
                drop(
                    handle
                        .reserve_request()
                        .expect("the connection stays reusable"),
                );
                assert_eq!(observed.close(), Close::Open);
                observed.end();
            };
            let (_, ()) = tokio::join!(session.run(), exchange);
            assert_ne!(wire.close(), Close::Reset);
        })
    });
}

/// Core `set_owned_error`, which every ingress error runs (peer reset,
/// connection failure, abort), signals an already-parked body reader even when
/// an ingress-completion waker panics, then resumes that panic.
#[test]
fn ingress_error_wakes_parked_reader_after_completion_waker_panic() {
    run_on_worker(|_context| {
        Box::pin(async move {
            let mut source =
                HttpStreamSource::new_async(Some(STREAM_1), None, 1 << 20).expect("source");
            source.set_read_timeout_value(Duration::ZERO);
            let producer = source.producer();
            assert!(producer.headers(status(200), false));
            source.read_header_event().await.expect("the response head");
            let completion = source
                .ingress_completion()
                .expect("the source observes its completion");
            let mut completion_wait = completion.wait();
            let panicking = Waker::from(Arc::new(PanicOnWake));
            assert!(
                completion_wait
                    .as_mut()
                    .poll(&mut Context::from_waker(&panicking))
                    .is_pending()
            );
            let reader = Arc::new(WakeCounter::default());
            let mut read = loop {
                let mut read = source.read_body_event(u32::MAX);
                match read
                    .as_mut()
                    .poll(&mut Context::from_waker(&reader.waker()))
                {
                    Poll::Pending => break read,
                    Poll::Ready(Ok(event)) if event.as_body().is_none() && !event.eom() => {}
                    Poll::Ready(Ok(_)) => panic!("the read returned bytes instead of parking"),
                    Poll::Ready(Err(error)) => {
                        panic!("the read failed instead of parking: {error:?}")
                    }
                }
            };
            let panic = catch_unwind(AssertUnwindSafe(|| {
                producer.abort(HttpErrorCode::CANCEL, b"");
            }))
            .expect_err("the waker's panic reaches the producer");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"waker panic"));
            assert_eq!(reader.wakes(), 1, "the error signals the parked reader");
            let Poll::Ready(Err(error)) = read
                .as_mut()
                .poll(&mut Context::from_waker(&reader.waker()))
            else {
                panic!("the parked read fails");
            };
            assert_cancel(&error);
            drop(read);
            drop(completion_wait);
            let Some(Err(error)) = completion.snapshot() else {
                panic!("the source completed with an error");
            };
            assert_cancel(&error);
        })
    });
}
