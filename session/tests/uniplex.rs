use std::cell::Cell;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::thread::ThreadId;
use std::time::Duration;

use bytes::Bytes;
use roxygen_core::buf_queue::BufQueue;
use roxygen_core::byte_event::ByteEventType;
use roxygen_core::byte_event::HttpByteEvent;
use roxygen_core::byte_event::HttpByteEventCallback;
use roxygen_core::byte_event::HttpByteEventRegistration;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::events::ResumeStatus;
use roxygen_core::fixed_source::HttpErrorSource;
use roxygen_core::fixed_source::HttpFixedSource;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::headers::HttpHeaderCode;
use roxygen_core::headers::HttpHeaders;
use roxygen_core::http::HttpMethod;
use roxygen_core::http::TransportDirection;
use roxygen_core::source::HttpSource;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationSource;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::driver::WorkerDriver;
use roxygen_runtime::tokio_host::HostedWorker;
use roxygen_runtime::tokio_host::HostedWorkerOptions;
use roxygen_runtime::wait::CancellableBaton;
use roxygen_runtime::wait::WaitStatus;
use roxygen_runtime::worker::WorkerContext;
use roxygen_runtime::worker::WorkerHandle;
use roxygen_runtime::worker::WorkerState;
use roxygen_transport::byte_event::ByteEventWriteFlags;
use roxygen_transport::byte_stream::ByteEventCapability;
use roxygen_transport::byte_stream::ByteEventRegistrationError;
use roxygen_transport::byte_stream::LocalTransportFuture;
use roxygen_transport::byte_stream::TransportWriteError;
use roxygen_transport::byte_stream::WriteInfo;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

use crate::http2::Http2SessionConfig;
use crate::session::LifecycleEvent;
use crate::session::LifecycleObserver;
use crate::uniplex::UniplexConfig;
use crate::uniplex::UniplexHandle;
use crate::uniplex::UniplexRead;
use crate::uniplex::UniplexSession;
use crate::uniplex::UniplexTransport;
use crate::uniplex::egress_stream_is_ready_for_test;

thread_local! {
    static ESCAPED_RUN: RefCell<Option<crate::session::LocalSessionFuture<Result<crate::uniplex::UniplexRunSummary, HttpError>>>> = RefCell::new(None);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseKind {
    Open,
    ShutdownWrite,
    Closed,
    Reset,
}

enum ReadAction {
    Data(Vec<u8>),
    DataEof(Vec<u8>),
    SegmentedDataEof(BufQueue),
    YieldData(Vec<u8>),
    WaitData(Arc<tokio::sync::Notify>, Vec<u8>),
    WaitDataOrCancellation(Arc<tokio::sync::Notify>, Vec<u8>),
    WaitEof(Arc<tokio::sync::Notify>),
    WaitError(Arc<tokio::sync::Notify>, HttpErrorCode),
    Eof,
    Error(HttpErrorCode),
    ShutdownWorker(WorkerHandle),
}

struct DropTrackedBytes {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for DropTrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for DropTrackedBytes {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct TransportState {
    reads: VecDeque<ReadAction>,
    writes: Vec<Vec<u8>>,
    write_error: Option<HttpErrorCode>,
    write_gate: Option<(Vec<u8>, Rc<tokio::sync::Notify>)>,
    write_flags: Vec<ByteEventWriteFlags>,
    capability: ByteEventCapability,
    capability_error: Option<HttpErrorCode>,
    registrations: Vec<HttpByteEventRegistration>,
    expected_drops_before_reset: Option<(Arc<AtomicUsize>, usize)>,
    close: CloseKind,
}

struct ScriptedTransport {
    state: Rc<RefCell<TransportState>>,
}

struct DropProbeTransport {
    inner: Rc<ScriptedTransport>,
    drops: Arc<AtomicUsize>,
    reset_calls: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl Drop for DropProbeTransport {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl ScriptedTransport {
    fn new(reads: impl IntoIterator<Item = ReadAction>) -> (Rc<Self>, Rc<RefCell<TransportState>>) {
        Self::with_capability(reads, ByteEventCapability::Unsupported)
    }

    fn with_capability(
        reads: impl IntoIterator<Item = ReadAction>,
        capability: ByteEventCapability,
    ) -> (Rc<Self>, Rc<RefCell<TransportState>>) {
        let state = Rc::new(RefCell::new(TransportState {
            reads: reads.into_iter().collect(),
            writes: Vec::new(),
            write_error: None,
            write_gate: None,
            write_flags: Vec::new(),
            capability,
            capability_error: None,
            registrations: Vec::new(),
            expected_drops_before_reset: None,
            close: CloseKind::Open,
        }));
        (
            Rc::new(Self {
                state: Rc::clone(&state),
            }),
            state,
        )
    }
}

impl UniplexTransport for ScriptedTransport {
    fn read(
        &self,
        _context: &WorkerContext,
        _maximum: usize,
        _allocation_size: usize,
        _timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        let action = self.state.borrow_mut().reads.pop_front();
        Box::pin(async move {
            match action {
                Some(ReadAction::Data(bytes)) => Ok(UniplexRead::new(queue(&bytes), false)),
                Some(ReadAction::DataEof(bytes)) => Ok(UniplexRead::new(queue(&bytes), true)),
                Some(ReadAction::SegmentedDataEof(bytes)) => Ok(UniplexRead::new(bytes, true)),
                Some(ReadAction::YieldData(bytes)) => {
                    tokio::task::yield_now().await;
                    Ok(UniplexRead::new(queue(&bytes), false))
                }
                Some(ReadAction::WaitData(ready, bytes)) => {
                    ready.notified().await;
                    tokio::task::yield_now().await;
                    Ok(UniplexRead::new(queue(&bytes), false))
                }
                Some(ReadAction::WaitDataOrCancellation(ready, bytes)) => {
                    let cancelled = CancellableBaton::new();
                    tokio::select! {
                        () = ready.notified() => Ok(UniplexRead::new(queue(&bytes), false)),
                        _ = cancelled.wait(cancellation) => Err(HttpError::with_message(
                            HttpErrorCode::CORO_CANCELLED,
                            b"H1 peer read cancelled",
                        )),
                    }
                }
                Some(ReadAction::WaitEof(ready)) => {
                    ready.notified().await;
                    Ok(UniplexRead::new(BufQueue::new(0), true))
                }
                Some(ReadAction::WaitError(ready, code)) => {
                    ready.notified().await;
                    Err(HttpError::with_message(
                        code,
                        b"scripted delayed read failure",
                    ))
                }
                Some(ReadAction::Eof) => Ok(UniplexRead::new(BufQueue::new(0), true)),
                Some(ReadAction::Error(code)) => {
                    Err(HttpError::with_message(code, b"scripted read failure"))
                }
                Some(ReadAction::ShutdownWorker(worker)) => {
                    assert!(worker.begin_shutdown());
                    let baton = CancellableBaton::new();
                    let _ = baton.wait(cancellation).await;
                    Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"scripted read cancelled after worker shutdown",
                    ))
                }
                None => {
                    let baton = CancellableBaton::new();
                    let _ = baton.wait(cancellation).await;
                    Err(HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"scripted read cancelled",
                    ))
                }
            }
        })
    }

    fn write(
        &self,
        _context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        _timeout: Duration,
        _cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        let len = data.chain_length();
        let bytes = data.coalesce(len).expect("bounded scripted write").to_vec();
        let error = self.state.borrow().write_error;
        let gate = self
            .state
            .borrow()
            .write_gate
            .as_ref()
            .filter(|(needle, _)| {
                bytes
                    .windows(needle.len())
                    .any(|part| part == needle.as_slice())
            })
            .map(|(_, gate)| Rc::clone(gate));
        if error.is_none() {
            let mut state = self.state.borrow_mut();
            state.writes.push(bytes);
            state.write_flags.push(flags);
        }
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            match error {
                Some(code) => Err(TransportWriteError::new(
                    HttpError::with_message(code, b"scripted write failure"),
                    0,
                )),
                None => Ok(WriteInfo { bytes_written: len }),
            }
        })
    }

    fn shutdown_write(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow_mut().close = CloseKind::ShutdownWrite;
        Ok(())
    }

    fn close(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        self.state.borrow_mut().close = CloseKind::Closed;
        Ok(())
    }

    fn close_with_reset(&self, _context: &WorkerContext) -> Result<(), HttpError> {
        let mut state = self.state.borrow_mut();
        if let Some((drops, expected)) = &state.expected_drops_before_reset {
            assert_eq!(drops.load(Ordering::SeqCst), *expected);
        }
        state.close = CloseKind::Reset;
        Ok(())
    }

    fn byte_event_capability(
        &self,
        _context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        let state = self.state.borrow();
        match state.capability_error {
            Some(code) => Err(HttpError::with_message(
                code,
                b"scripted capability failure",
            )),
            None => Ok(state.capability),
        }
    }

    fn register_byte_event(
        &self,
        _context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        if self.state.borrow().capability == ByteEventCapability::Unsupported {
            return Err(ByteEventRegistrationError::Unsupported);
        }
        self.state
            .borrow_mut()
            .registrations
            .extend(registration.into_event_registrations());
        Ok(())
    }
}

impl UniplexTransport for DropProbeTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.inner
            .write(context, data, flags, timeout, cancellation)
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.reset_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

#[derive(Clone, Copy)]
enum HandlerMode {
    Consume(u16, usize),
    Early(u16),
    Error,
    Panic,
    Empty,
    ErrorSource,
    BodyErrorSource,
    Head,
}

struct BodyErrorResponse {
    header: Option<HttpHeaderEvent>,
}

impl BodyErrorResponse {
    fn new() -> Self {
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_status_code(200).unwrap();
        message.set_http_version(1, 1);
        Self {
            header: Some(HttpHeaderEvent::new(message, false).unwrap()),
        }
    }
}

impl HttpSource for BodyErrorResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::ENHANCE_YOUR_CALM,
            b"scripted response body error",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct TestHandler {
    mode: HandlerMode,
    bodies: Rc<Cell<usize>>,
}

struct BackingCaptureHandler {
    body_pointers: Rc<RefCell<Vec<usize>>>,
}

impl HttpHandler for BackingCaptureHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let body_pointers = Rc::clone(&self.body_pointers);
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(!headers.eom());
            loop {
                let event = request_source.read_body_event(u32::MAX).await?;
                let eom = event.eom();
                if let roxygen_core::events::HttpBodyEventKind::Body(body) = event.into_kind() {
                    body_pointers
                        .borrow_mut()
                        .extend(body.segments().map(|segment| segment.as_ptr() as usize));
                }
                if eom {
                    break;
                }
            }
            Ok(fixed_response(200, 0))
        })
    }
}

struct DropProbeHandler {
    drops: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl Drop for DropProbeHandler {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl HttpHandler for DropProbeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        _request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(std::future::pending())
    }
}

struct EventSource {
    header: Option<HttpHeaderEvent>,
    body: VecDeque<HttpBodyEvent>,
}

impl HttpSource for EventSource {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(self.body.pop_front().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"response body exhausted")
        })))
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        let error = HttpError::new(error.unwrap_or(HttpErrorCode::CANCEL));
        let mut first_panic = None;
        if let Some(header) = self.header.as_mut() {
            for registration in header.byte_event_registrations_mut() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    registration.cancel(&error, None);
                })) {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    }
                }
            }
        }
        for event in &mut self.body {
            for registration in event.byte_event_registrations_mut() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    registration.cancel(&error, None);
                })) {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    }
                }
            }
        }
        if let Some(payload) = first_panic {
            std::panic::resume_unwind(payload);
        }
    }
}

struct EventHandler {
    events: RefCell<Option<VecDeque<HttpBodyEvent>>>,
}

struct TimeoutResponseHandler {
    status: u16,
    body: &'static [u8],
}

struct PendingHandler {
    started: Rc<tokio::sync::Notify>,
}

struct EofProbeHandler {
    started: Rc<tokio::sync::Notify>,
    release: Rc<tokio::sync::Notify>,
    drops: Rc<Cell<usize>>,
}

struct DropCount(Rc<Cell<usize>>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

struct ContinueThenTimeoutHandler;

struct ContinueThenTimeoutResponse {
    request: Rc<RefCell<HttpSourceHolder>>,
    informational_pending: bool,
}

impl HttpSource for ContinueThenTimeoutResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        if self.informational_pending {
            self.informational_pending = false;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(100).unwrap();
            message.set_http_version(1, 1);
            let headers = HttpHeaderEvent::new(message, false).unwrap();
            return Box::pin(std::future::ready(Ok(headers)));
        }
        let request = Rc::clone(&self.request);
        Box::pin(async move {
            let error = loop {
                let read = { request.borrow_mut().read_body_event(u32::MAX) };
                match read.await {
                    Ok(event) => match event.into_kind() {
                        roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                            let _ = resume.await;
                        }
                        _ => panic!("request body timer produced data before timeout"),
                    },
                    Err(error) => break error,
                }
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            std::future::pending().await
        })
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.request.borrow_mut().stop_reading(error);
    }
}

impl HttpHandler for ContinueThenTimeoutHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(!headers.eom());
            Ok(HttpSourceHolder::new(Box::new(
                ContinueThenTimeoutResponse {
                    request: Rc::new(RefCell::new(request_source)),
                    informational_pending: true,
                },
            )))
        })
    }
}

impl HttpHandler for PendingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = Rc::clone(&self.started);
        Box::pin(async move {
            request_source.read_header_event().await?;
            started.notify_one();
            std::future::pending().await
        })
    }
}

impl HttpHandler for EofProbeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = Rc::clone(&self.started);
        let release = Rc::clone(&self.release);
        let drops = DropCount(Rc::clone(&self.drops));
        Box::pin(async move {
            let _drops = drops;
            request_source.read_header_event().await?;
            started.notify_one();
            release.notified().await;
            Ok(fixed_response(200, 0))
        })
    }
}

impl HttpHandler for TimeoutResponseHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let status = self.status;
        let body = self.body;
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(!headers.eom());
            let error = loop {
                match request_source.read_body_event(u32::MAX).await {
                    Ok(event) => match event.into_kind() {
                        roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                            let _ = resume.await;
                        }
                        _ => panic!("incomplete request body produced data before timeout"),
                    },
                    Err(error) => break error,
                }
            };
            assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
            Ok(HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_fixed_response(status, Some(queue(body))).unwrap(),
            )))
        })
    }
}

struct BlockedBodyHandler {
    release: Rc<CancellableBaton>,
    bodies: Rc<Cell<usize>>,
}

struct AntiPipelineHandler {
    invocations: Rc<Cell<usize>>,
    first_release: Rc<CancellableBaton>,
}

struct CloseFirstHandler {
    invocations: Rc<Cell<usize>>,
}

impl HttpHandler for CloseFirstHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.invocations.get();
        self.invocations.set(ordinal + 1);
        Box::pin(async move {
            if ordinal == 0 {
                request_source.read_header_event().await?;
            }
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            if ordinal == 0 {
                message.set_wants_keepalive(false);
            }
            Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                message, None,
            ))))
        })
    }
}

impl HttpHandler for AntiPipelineHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.invocations.get();
        self.invocations.set(ordinal + 1);
        let first_release = Rc::clone(&self.first_release);
        Box::pin(async move {
            request_source.read_header_event().await?;
            if ordinal == 0 {
                let _ = first_release.wait(CancellationToken::never()).await;
            }
            Ok(fixed_response(200, 0))
        })
    }
}

struct WebSocketHandler {
    bodies: Rc<Cell<usize>>,
}

struct ConnectHandler {
    bodies: Rc<Cell<usize>>,
}

struct RejectUpgradeHandler {
    invocations: Rc<Cell<usize>>,
    eom_seen: Rc<Cell<bool>>,
}

struct PromptUpgradeHandler {
    bodies: Rc<Cell<usize>>,
}

struct PushDropSource {
    drops: Rc<Cell<usize>>,
}

impl HttpSource for PushDropSource {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl Drop for PushDropSource {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

struct WebSocketResponse {
    request: Rc<RefCell<HttpSourceHolder>>,
    header: Option<HttpHeaderEvent>,
    bodies: Rc<Cell<usize>>,
    body_pending: bool,
}

impl HttpSource for WebSocketResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"upgrade response header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        if !self.body_pending {
            return Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"upgrade response body exhausted",
            ))));
        }
        self.body_pending = false;
        let request = Rc::clone(&self.request);
        let bodies = Rc::clone(&self.bodies);
        Box::pin(async move {
            loop {
                let read = { request.borrow_mut().read_body_event(u32::MAX) };
                let event = read.await?;
                if let Some(body) = event.as_body() {
                    bodies.set(bodies.get().saturating_add(body.chain_length()));
                }
                if event.eom() {
                    break;
                }
            }
            Ok(HttpBodyEvent::body(queue(&[b'y'; 100]), true))
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.request.borrow_mut().stop_reading(error);
    }
}

impl HttpHandler for WebSocketHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let bodies = Rc::clone(&self.bodies);
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(headers.headers().is_ingress_websocket_upgrade());
            assert!(!headers.eom());
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            message.set_egress_websocket_upgrade();
            Ok(HttpSourceHolder::new(Box::new(WebSocketResponse {
                request: Rc::new(RefCell::new(request_source)),
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                bodies,
                body_pending: true,
            })))
        })
    }
}

impl HttpHandler for ConnectHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let bodies = Rc::clone(&self.bodies);
        Box::pin(async move {
            let mut headers = request_source.read_header_event().await?;
            assert_eq!(
                headers.headers_mut().method().unwrap(),
                Some(HttpMethod::Connect),
            );
            assert!(!headers.eom());
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            Ok(HttpSourceHolder::new(Box::new(WebSocketResponse {
                request: Rc::new(RefCell::new(request_source)),
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                bodies,
                body_pending: true,
            })))
        })
    }
}

impl HttpHandler for RejectUpgradeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.invocations.get();
        self.invocations.set(ordinal + 1);
        let eom_seen = Rc::clone(&self.eom_seen);
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            if ordinal == 0 {
                assert!(headers.headers().is_ingress_websocket_upgrade());
                tokio::task::spawn_local(async move {
                    loop {
                        match request_source.read_body_event(u32::MAX).await {
                            Ok(event) if event.eom() => {
                                eom_seen.set(true);
                                break;
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                });
                let mut message = roxygen_core::message::HttpMessage::new();
                message.set_status_code(400).unwrap();
                message.set_http_version(1, 1);
                Ok(HttpSourceHolder::new(Box::new(EventSource {
                    header: Some(HttpHeaderEvent::new(message, true).unwrap()),
                    body: VecDeque::new(),
                })))
            } else {
                Ok(fixed_response(201, 0))
            }
        })
    }
}

impl HttpHandler for PromptUpgradeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let bodies = Rc::clone(&self.bodies);
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(headers.headers().is_ingress_websocket_upgrade());
            tokio::task::spawn_local(async move {
                while let Ok(event) = request_source.read_body_event(u32::MAX).await {
                    if let Some(body) = event.as_body() {
                        bodies.set(bodies.get().saturating_add(body.chain_length()));
                    }
                    if event.eom() {
                        break;
                    }
                }
            });
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(101).unwrap();
            message.set_http_version(1, 1);
            message.set_egress_websocket_upgrade();
            Ok(HttpSourceHolder::new(Box::new(HttpFixedSource::new(
                message, None,
            ))))
        })
    }
}

impl HttpHandler for BlockedBodyHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let release = Rc::clone(&self.release);
        let bodies = Rc::clone(&self.bodies);
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            assert!(!headers.eom());
            let _ = release.wait(CancellationToken::never()).await;
            loop {
                let event = request_source.read_body_event(u32::MAX).await?;
                if let Some(body) = event.as_body() {
                    bodies.set(bodies.get().saturating_add(body.chain_length()));
                }
                if event.eom() {
                    break;
                }
            }
            Ok(fixed_response(200, 100))
        })
    }
}

impl HttpHandler for EventHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let events = self.events.borrow_mut().take().unwrap();
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                body: events,
            })))
        })
    }
}

#[derive(Default)]
struct ByteEventLog {
    fired: RefCell<Vec<HttpByteEvent>>,
    cancelled: Cell<usize>,
    cancellation_codes: RefCell<Vec<HttpErrorCode>>,
}

impl HttpByteEventCallback for ByteEventLog {
    fn on_byte_event(&self, event: HttpByteEvent) {
        self.fired.borrow_mut().push(event);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.cancelled.set(self.cancelled.get() + 1);
        self.cancellation_codes.borrow_mut().push(error.code());
    }
}

struct PanickingByteEventCallback;

impl HttpByteEventCallback for PanickingByteEventCallback {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        panic!("scripted byte-event callback panic");
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {}
}

#[derive(Default)]
struct HeaderOrderingLog {
    cancelled: Cell<bool>,
    encoded_after_registration: Cell<bool>,
}

impl HttpByteEventCallback for HeaderOrderingLog {
    fn on_byte_event(&self, _event: HttpByteEvent) {}

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, _error: HttpError) {
        self.cancelled.set(true);
    }
}

struct HeaderOrderingHandler {
    log: Rc<HeaderOrderingLog>,
}

struct PanickingHeaderDropProbeHandler {
    drops: Arc<AtomicUsize>,
    owner: ThreadId,
}

impl Drop for PanickingHeaderDropProbeHandler {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl HttpHandler for PanickingHeaderDropProbeHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(204).unwrap();
            message.set_http_version(1, 1);
            let mut header = HttpHeaderEvent::new(message, true).unwrap();
            header.set_egress_headers_callback(|_| {
                panic!("scripted encoded-header callback panic");
            });
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(header),
                body: VecDeque::new(),
            })))
        })
    }
}

impl HttpHandler for HeaderOrderingHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let log = Rc::clone(&self.log);
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(204).unwrap();
            message.set_http_version(1, 1);
            let mut header = HttpHeaderEvent::new(message, true).unwrap();
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::NicTx.bit(),
                    &log,
                ));
            let callback_log = Rc::clone(&log);
            header.set_egress_headers_callback(move |_| {
                callback_log
                    .encoded_after_registration
                    .set(callback_log.cancelled.get());
            });
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(header),
                body: VecDeque::new(),
            })))
        })
    }
}

struct StagedTxResponse {
    header: Option<HttpHeaderEvent>,
    stage: u8,
    release_second: Rc<CancellableBaton>,
    first: Rc<PanickingByteEventCallback>,
    second: Rc<ByteEventLog>,
}

impl HttpSource for StagedTxResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"staged header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        match self.stage {
            0 => {
                self.stage = 1;
                let callback = Rc::clone(&self.first);
                Box::pin(async move {
                    let mut body = HttpBodyEvent::body(queue(b"a"), false);
                    body.byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(
                            None,
                            ByteEventType::NicTx.bit(),
                            &callback,
                        ));
                    Ok(body)
                })
            }
            1 => {
                self.stage = 2;
                let release = Rc::clone(&self.release_second);
                let callback = Rc::clone(&self.second);
                Box::pin(async move {
                    let _ = release.wait(CancellationToken::never()).await;
                    let mut body = HttpBodyEvent::body(queue(b"b"), true);
                    body.byte_event_registrations_mut()
                        .push(HttpByteEventRegistration::new(
                            None,
                            ByteEventType::NicTx.bit(),
                            &callback,
                        ));
                    Ok(body)
                })
            }
            _ => Box::pin(std::future::ready(Err(HttpError::with_message(
                HttpErrorCode::DROPPED,
                b"staged body exhausted",
            )))),
        }
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct StagedTxHandler {
    release_second: Rc<CancellableBaton>,
    first: Rc<PanickingByteEventCallback>,
    second: Rc<ByteEventLog>,
}

impl HttpHandler for StagedTxHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let release_second = Rc::clone(&self.release_second);
        let first = Rc::clone(&self.first);
        let second = Rc::clone(&self.second);
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            Ok(HttpSourceHolder::new(Box::new(StagedTxResponse {
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                stage: 0,
                release_second,
                first,
                second,
            })))
        })
    }
}

struct RegisteredHandler {
    callback: Rc<ByteEventLog>,
}

struct ReentrantCancelLog {
    handle: RefCell<Option<crate::uniplex::UniplexHandle>>,
    reentered: Cell<bool>,
    drain_result: Cell<Option<bool>>,
    cancellation_code: Cell<Option<HttpErrorCode>>,
}

impl HttpByteEventCallback for ReentrantCancelLog {
    fn on_byte_event(&self, _event: HttpByteEvent) {}

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.reentered.set(true);
        self.cancellation_code.set(Some(error.code()));
        if let Some(handle) = self.handle.borrow().as_ref() {
            self.drain_result.set(handle.initiate_drain().ok());
        }
    }
}

struct ReentrantDropState {
    handle: RefCell<Option<crate::uniplex::UniplexHandle>>,
    prepared: RefCell<Option<crate::session::PreparedOutgoingRequest>>,
    dropped: Cell<bool>,
    drain_result: Cell<Option<bool>>,
    reserve_error: Cell<Option<HttpErrorCode>>,
    send_error: Cell<Option<HttpErrorCode>>,
}

struct BlockingReentrantResponse {
    started: Arc<tokio::sync::Notify>,
    request_source: Option<HttpSourceHolder>,
    reentry: Rc<ReentrantDropState>,
}

impl Drop for BlockingReentrantResponse {
    fn drop(&mut self) {
        self.reentry.dropped.set(true);
        if let Some(handle) = self.reentry.handle.borrow().as_ref() {
            self.reentry
                .reserve_error
                .set(handle.reserve_request().err().map(|error| error.code()));
            if let Some(prepared) = self.reentry.prepared.borrow_mut().take() {
                self.reentry.send_error.set(
                    handle
                        .send_prepared_request(prepared)
                        .err()
                        .map(|error| error.code()),
                );
            }
            self.reentry.drain_result.set(handle.initiate_drain().ok());
        }
    }
}

impl HttpSource for BlockingReentrantResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let started = Arc::clone(&self.started);
        Box::pin(async move {
            started.notify_one();
            std::future::pending().await
        })
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        if let Some(source) = self.request_source.as_mut() {
            source.stop_reading(error);
        }
    }
}

struct BlockingReentrantHandler {
    started: Arc<tokio::sync::Notify>,
    reentry: Rc<ReentrantDropState>,
}

struct ReentrantPreparedSource {
    handle: crate::uniplex::UniplexHandle,
    drain_result: Rc<Cell<Option<bool>>>,
}

impl Drop for ReentrantPreparedSource {
    fn drop(&mut self) {
        self.drain_result.set(self.handle.initiate_drain().ok());
    }
}

impl HttpSource for ReentrantPreparedSource {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl HttpHandler for BlockingReentrantHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let started = Arc::clone(&self.started);
        let reentry = Rc::clone(&self.reentry);
        Box::pin(async move {
            request_source.read_header_event().await?;
            Ok(HttpSourceHolder::new(Box::new(BlockingReentrantResponse {
                started,
                request_source: Some(request_source),
                reentry,
            })))
        })
    }
}

struct ReentrantBodyRegistrationHandler {
    callback: Rc<ReentrantCancelLog>,
}

impl HttpHandler for ReentrantBodyRegistrationHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let callback = Rc::clone(&self.callback);
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            let mut body = HttpBodyEvent::body(queue(b"queued"), true);
            body.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit(),
                    &callback,
                ));
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                body: VecDeque::from([body]),
            })))
        })
    }
}

struct InvalidHeaderHandler {
    callback: Rc<ReentrantCancelLog>,
}

impl HttpHandler for InvalidHeaderHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let callback = Rc::clone(&self.callback);
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_url(b"/invalid-response", false).unwrap();
            request.set_method(HttpMethod::Get).unwrap();
            request.set_http_version(1, 1);
            let mut header = HttpHeaderEvent::new(request, true).unwrap();
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit(),
                    &callback,
                ));
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(header),
                body: VecDeque::new(),
            })))
        })
    }
}

impl HttpHandler for RegisteredHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let callback = Rc::clone(&self.callback);
        Box::pin(async move {
            request_source.read_header_event().await?;
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_status_code(200).unwrap();
            message.set_http_version(1, 1);
            let mut header = HttpHeaderEvent::new(message, false).unwrap();
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit()
                        | ByteEventType::KernelWrite.bit()
                        | ByteEventType::NicTx.bit()
                        | ByteEventType::CumulativeAck.bit(),
                    &callback,
                ));
            let mut body = HttpBodyEvent::body(queue(b"abc"), true);
            body.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit()
                        | ByteEventType::KernelWrite.bit()
                        | ByteEventType::NicTx.bit()
                        | ByteEventType::CumulativeAck.bit(),
                    &callback,
                ));
            let mut padding = HttpBodyEvent::padding(0, false);
            padding
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                    &callback,
                ));
            Ok(HttpSourceHolder::new(Box::new(EventSource {
                header: Some(header),
                body: VecDeque::from([padding, body]),
            })))
        })
    }
}

impl HttpHandler for TestHandler {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request_source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let mode = self.mode;
        let bodies = Rc::clone(&self.bodies);
        if matches!(mode, HandlerMode::Panic) {
            panic!("scripted handler panic");
        }
        Box::pin(async move {
            let headers = request_source.read_header_event().await?;
            if matches!(mode, HandlerMode::Consume(..)) && !headers.eom() {
                loop {
                    let event = request_source.read_body_event(u32::MAX).await?;
                    let eom = event.eom();
                    match event.into_kind() {
                        roxygen_core::events::HttpBodyEventKind::Suspend(resume) => {
                            let _ = resume.await;
                            continue;
                        }
                        roxygen_core::events::HttpBodyEventKind::Body(body) => {
                            bodies.set(bodies.get().saturating_add(body.chain_length()));
                        }
                        _ => {}
                    }
                    if eom {
                        break;
                    }
                }
            }
            match mode {
                HandlerMode::Consume(status, body_size) => Ok(fixed_response(status, body_size)),
                HandlerMode::Early(status) => Ok(fixed_response(status, 0)),
                HandlerMode::Error => Err(HttpError::with_message(
                    HttpErrorCode::REFUSED_STREAM,
                    b"scripted handler error",
                )),
                HandlerMode::Panic => unreachable!(),
                HandlerMode::Empty => Ok(HttpSourceHolder::empty()),
                HandlerMode::ErrorSource => Ok(HttpSourceHolder::new(Box::new(
                    HttpErrorSource::new(HttpError::with_message(
                        HttpErrorCode::ENHANCE_YOUR_CALM,
                        b"scripted response error",
                    )),
                ))),
                HandlerMode::BodyErrorSource => {
                    Ok(HttpSourceHolder::new(Box::new(BodyErrorResponse::new())))
                }
                HandlerMode::Head => {
                    let mut source = HttpFixedSource::make_fixed_response(200, None).unwrap();
                    source
                        .message_mut()
                        .unwrap()
                        .headers_mut()
                        .set(HttpHeaderCode::ContentLength, b"100".as_slice());
                    Ok(HttpSourceHolder::new(Box::new(source)))
                }
            }
        })
    }
}

struct Outcome {
    output: Vec<u8>,
    close: CloseKind,
    bodies: usize,
    error: Option<HttpErrorCode>,
}

fn queue(bytes: &[u8]) -> BufQueue {
    BufQueue::from_bytes(Bytes::copy_from_slice(bytes), bytes.len()).unwrap()
}

fn fixed_response(status: u16, body_size: usize) -> HttpSourceHolder {
    let body = (body_size != 0).then(|| queue(&vec![b'x'; body_size]));
    HttpSourceHolder::new(Box::new(
        HttpFixedSource::make_fixed_response(status, body).unwrap(),
    ))
}

fn run_local<T: 'static>(test: impl Future<Output = T> + 'static) -> T {
    Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
        .block_on(LocalSet::new().run_until(test))
}

thread_local! {
    /// Whether `run_on_worker` runs its tests on a `HostedWorker`.
    static ON_HOSTED_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Runs `suite` with every `run_on_worker` test in it on a `HostedWorker`,
/// whose owner thread drives a wheel timer: the session's timed batons and
/// host sleeps take slots on it.
pub(crate) fn on_hosted_workers(suite: impl FnOnce()) {
    struct Restore(bool);

    impl Drop for Restore {
        fn drop(&mut self) {
            ON_HOSTED_WORKER.set(self.0);
        }
    }

    let _restore = Restore(ON_HOSTED_WORKER.replace(true));
    suite();
}

/// Whether `run_on_worker` tests run on a `HostedWorker` (`on_hosted_workers`).
pub(crate) fn on_hosted_worker() -> bool {
    ON_HOSTED_WORKER.get()
}

/// Runs `test` as a command on a fresh `HostedWorker` and joins the worker.
pub(crate) fn run_on_hosted_worker<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    let worker = HostedWorker::spawn(HostedWorkerOptions {
        command_capacity: NonZeroUsize::new(16).expect("nonzero capacity"),
        thread_name: "uniplex-timers".to_owned(),
    })
    .expect("hosted worker starts");
    let call = worker
        .handle()
        .call_local(move |context| {
            assert!(
                context.host().wheel_timer().is_some(),
                "a hosted worker drives a wheel timer"
            );
            test(context)
        })
        .expect("worker call");
    let result = futures::executor::block_on(call).expect("worker call completed");
    drop(worker);
    result
}

pub(crate) fn run_on_worker<T: Send + 'static>(
    test: impl FnOnce(WorkerContext) -> Pin<Box<dyn Future<Output = T>>> + Send + 'static,
) -> T {
    if on_hosted_worker() {
        return run_on_hosted_worker(test);
    }
    run_local(async move {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        let call = worker
            .call_local(move |context| {
                let worker = context.handle().clone();
                async move {
                    let result = test(context).await;
                    worker.begin_shutdown();
                    result
                }
            })
            .unwrap();
        let (_, result) = tokio::join!(driver, call);
        result.expect("worker call completed")
    })
}

fn downstream(input: Vec<ReadAction>, mode: HandlerMode) -> Outcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(input);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode,
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

const KEEPALIVE_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: example\r\n\r\n";
const CLOSE_GET: &[u8] = b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n";

/// When the peer's EOF follows a request whose handler parks after the request head.
#[derive(Clone, Copy)]
enum PeerEof {
    WhileParked,
    AfterResponse,
    Never,
}

/// Sends `request`, delivers the peer's EOF at `eof`, and releases the parked handler.
/// Returns the outcome and how many times the handler future was dropped before release.
fn peer_eof(
    request: &'static [u8],
    eof: PeerEof,
    abort_exchanges_on_eof: bool,
) -> (Outcome, usize) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let eof_ready = Arc::new(tokio::sync::Notify::new());
            let mut reads = vec![ReadAction::Data(request.to_vec())];
            if !matches!(eof, PeerEof::Never) {
                reads.push(ReadAction::WaitEof(Arc::clone(&eof_ready)));
            }
            let (transport, state) = ScriptedTransport::new(reads);
            let started = Rc::new(tokio::sync::Notify::new());
            let release = Rc::new(tokio::sync::Notify::new());
            let drops = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(EofProbeHandler {
                started: Rc::clone(&started),
                release: Rc::clone(&release),
                drops: Rc::clone(&drops),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig {
                    abort_exchanges_on_eof,
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            let run = tokio::task::spawn_local(session.run());
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("handler parked after the request head");
            // The current-thread runtime runs the session's pending work before each timer
            // fires: the EOF read, or the response write.
            if matches!(eof, PeerEof::WhileParked) {
                eof_ready.notify_one();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let drops_before_release = drops.get();
            release.notify_one();
            if matches!(eof, PeerEof::AfterResponse) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                eof_ready.notify_one();
            }
            let result = tokio::time::timeout(Duration::from_secs(1), run)
                .await
                .expect("session finished")
                .expect("session task completed");
            let state = state.borrow();
            let outcome = Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            };
            (outcome, drops_before_release)
        })
    })
}

fn simple_get(mode: HandlerMode) -> Outcome {
    downstream(
        vec![
            ReadAction::Data(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        mode,
    )
}

#[test]
fn downstream_h1_runs_without_tokio_runtime() {
    crate::external_host_tests::run_on_host(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::DataEof(
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 3\r\n\r\nabc".to_vec(),
            )]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 4),
                bodies: Rc::clone(&bodies),
            });
            let (session, _) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            session.run().await.unwrap();
            assert_eq!(bodies.get(), 3);
            assert!(state.borrow().writes.concat().ends_with(b"xxxx"));
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

fn simple_post(body: &[u8], mode: HandlerMode) -> Outcome {
    let mut request = format!(
        "POST / HTTP/1.1\r\nHost: example\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(body);
    downstream(vec![ReadAction::Data(request), ReadAction::Eof], mode)
}

fn output_contains(outcome: &Outcome, needle: &[u8]) -> bool {
    outcome
        .output
        .windows(needle.len())
        .any(|window| window == needle)
}

fn assert_ok_response(outcome: Outcome, status: u16) {
    assert_eq!(outcome.error, None);
    assert!(output_contains(
        &outcome,
        format!("HTTP/1.1 {status}").as_bytes()
    ));
    assert_eq!(outcome.close, CloseKind::ShutdownWrite);
}

#[test]
fn upstream_h1_reuses_connection_with_unread_completed_response() {
    run_on_worker(|context| {
        Box::pin(async move {
            let first_ready = Arc::new(tokio::sync::Notify::new());
            let second_ready = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc".to_vec();
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(Arc::clone(&first_ready), response.clone()),
                ReadAction::WaitData(Arc::clone(&second_ready), response),
                ReadAction::WaitEof(Arc::clone(&finish)),
            ]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let first = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/first", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let exercise = async {
                first_ready.notify_one();
                first
                    .source
                    .ingress_completion()
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                assert!(handle.outgoing_capacity_released(first.stream).unwrap());
                assert_eq!(handle.outgoing_transaction_activity().available(), 1);
                let second = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(
                            HttpFixedSource::make_get_request(b"/second", None).unwrap(),
                        )),
                        CancellationToken::never(),
                    )
                    .await
                    .expect("producer-complete response does not hold H1 capacity");
                while !state
                    .borrow()
                    .writes
                    .concat()
                    .windows(12)
                    .any(|bytes| bytes == b"GET /second ")
                {
                    tokio::task::yield_now().await;
                }
                assert!(
                    first.source.readable(),
                    "first response is still unread when request2 reaches the wire"
                );
                second_ready.notify_one();
                second
                    .source
                    .ingress_completion()
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                assert_eq!(handle.outgoing_transaction_activity().available(), 1);
                drop((first, second));
                finish.notify_one();
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(session.run(), exercise)
            })
            .await
            .expect("H1 connection reuses protocol capacity while old response is retained");
            result.unwrap();
        })
    });
}

struct EofAdmissionObserver {
    handle: UniplexHandle,
    closed: Rc<Cell<bool>>,
    releases: Rc<Cell<usize>>,
}

impl LifecycleObserver for EofAdmissionObserver {
    fn on_event(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::OutgoingAdmissionClosed => self.closed.set(true),
            LifecycleEvent::OutgoingCapacityReleased { .. } => {
                assert!(
                    self.closed.get(),
                    "known EOF closes admission before completion callbacks"
                );
                assert_eq!(self.handle.outgoing_transaction_activity().available(), 0);
                assert!(self.handle.reserve_request().is_err());
                self.releases.set(self.releases.get() + 1);
            }
            _ => {}
        }
    }
}

#[test]
fn upstream_h1_final_bytes_do_not_reopen_eof_admission() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = ScriptedTransport::new([
                ReadAction::YieldData(Vec::new()),
                ReadAction::DataEof(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc".to_vec()),
            ]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let releases = Rc::new(Cell::new(0));
            session
                .add_lifecycle_observer(Rc::new(EofAdmissionObserver {
                    handle: handle.clone(),
                    closed: Rc::new(Cell::new(false)),
                    releases: Rc::clone(&releases),
                }))
                .unwrap();
            let response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let consume = async {
                response
                    .source
                    .ingress_completion()
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
                drop(response);
            };
            let (result, ()) = tokio::join!(session.run(), consume);
            result.unwrap();
            assert_eq!(releases.get(), 1);
        })
    });
}

#[test]
fn upstream_h1_parse_failure_closes_admission_before_completed_response_callbacks() {
    struct CapacityObserver {
        handle: UniplexHandle,
        rejected: Rc<Cell<usize>>,
    }
    impl LifecycleObserver for CapacityObserver {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::OutgoingCapacityReleased { .. }) {
                assert_eq!(self.handle.outgoing_transaction_activity().available(), 0);
                assert!(
                    self.handle.reserve_request().is_err(),
                    "an already failed parse must reject reentrant admission"
                );
                self.rejected.set(self.rejected.get() + 1);
            }
        }
    }
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = ScriptedTransport::new([ReadAction::YieldData(
                b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabcnot a response\r\n\r\n".to_vec(),
            )]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let rejected = Rc::new(Cell::new(0));
            session
                .add_lifecycle_observer(Rc::new(CapacityObserver {
                    handle: handle.clone(),
                    rejected: Rc::clone(&rejected),
                }))
                .unwrap();
            let response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let release = async {
                let _ = response.source.ingress_completion().unwrap().wait().await;
                drop(response);
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(session.run(), release)
            })
            .await
            .expect("malformed trailing response terminates the connection");
            assert!(result.is_err());
            assert_eq!(
                rejected.get(),
                1,
                "completed response callback ran inside the failed parse turn"
            );
        })
    });
}

fn upstream(body_size: usize, response_body_size: usize) -> Outcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {response_body_size}\r\n\r\n{}",
                "y".repeat(response_body_size)
            )
            .into_bytes();
            let (transport, state) =
                ScriptedTransport::new([ReadAction::YieldData(response), ReadAction::Eof]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let body = (body_size != 0).then(|| queue(&vec![b'x'; body_size]));
            let source = if body_size == 0 {
                HttpFixedSource::make_get_request(b"/", None).unwrap()
            } else {
                HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, body).unwrap()
            };
            let request = HttpSourceHolder::new(Box::new(source));
            let mut response = handle
                .send_request(request, CancellationSource::new().token())
                .await
                .unwrap()
                .source;
            let consume = async move {
                let headers = response.read_header_event().await.unwrap();
                let mut bytes = 0;
                if !headers.eom() {
                    loop {
                        let event = response.read_body_event(u32::MAX).await.unwrap();
                        bytes += event.as_body().map_or(0, BufQueue::chain_length);
                        if event.eom() {
                            break;
                        }
                    }
                }
                bytes
            };
            let (result, response_bytes) = tokio::join!(session.run(), consume);
            assert_eq!(response_bytes, response_body_size);
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: response_bytes,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn upstream_with_available_headers() -> (Outcome, usize, HttpErrorCode) {
    run_on_worker(|context| {
        Box::pin(async move {
            let header_reads = Rc::new(Cell::new(0));
            let (transport, state) = ScriptedTransport::new([
                ReadAction::YieldData(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/available", false).unwrap();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"1500".as_slice());
            let headers = HttpHeaderEvent::new(message, false).unwrap();
            let source = HttpSourceHolder::new(Box::new(HeadersAlreadyAvailableSource {
                header_reads: Rc::clone(&header_reads),
                body: Some(HttpBodyEvent::body(queue(&vec![b'x'; 1_500]), true)),
            }));
            let prepared = crate::uniplex::UniplexHandle::prepare_request_with_headers(
                headers,
                source,
                handle.reserve_request().unwrap(),
            );
            let mut response = handle.send_prepared_request(prepared).unwrap().source;
            let consume = async move {
                let headers = response.read_header_event().await.unwrap();
                assert!(headers.eom());
            };
            let (result, ()) = tokio::join!(session.run(), consume);
            result.unwrap();
            let outcome = {
                let state = state.borrow();
                Outcome {
                    output: state.writes.concat(),
                    close: state.close,
                    bodies: 0,
                    error: None,
                }
            };

            let (draining_transport, _draining_state) = ScriptedTransport::new([ReadAction::Eof]);
            let draining_transport: Rc<dyn UniplexTransport> = draining_transport;
            let (draining_session, draining_handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                draining_transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/rejected", false).unwrap();
            message.set_method(HttpMethod::Get).unwrap();
            message.set_http_version(1, 1);
            let headers = HttpHeaderEvent::new(message, true).unwrap();
            let source = HttpSourceHolder::new(Box::new(HeadersAlreadyAvailableSource {
                header_reads: Rc::clone(&header_reads),
                body: None,
            }));
            let prepared = crate::uniplex::UniplexHandle::prepare_request_with_headers(
                headers,
                source,
                draining_handle.reserve_request().unwrap(),
            );
            assert!(draining_handle.initiate_drain().unwrap());
            let rejected = match draining_handle.send_prepared_request(prepared) {
                Ok(_) => panic!("draining session accepted caller-provided headers"),
                Err(error) => error.code(),
            };
            draining_session.run().await.unwrap();
            (outcome, header_reads.get(), rejected)
        })
    })
}

fn upstream_response_without_request() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("unsolicited response terminated the upstream session");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn upstream_eof_rejects_prepared_request() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Eof]);
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let prepared = crate::uniplex::UniplexHandle::prepare_request(
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/after-eof", None).unwrap(),
                )),
                handle.reserve_request().unwrap(),
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().close == CloseKind::Open {
                    tokio::select! {
                        result = run.as_mut() => {
                            panic!("session completed with a live reservation: {result:?}")
                        }
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("upstream EOF closed admission before finalization");
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            let error = match handle.send_prepared_request(prepared) {
                Ok(_) => panic!("upstream EOF accepted a prepared request"),
                Err(error) => error,
            };
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
            run.as_mut().await.unwrap();
            assert!(state.borrow().writes.is_empty());
        })
    });
}

fn upstream_eof_fails_committed_request_without_response() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) =
                ScriptedTransport::new([ReadAction::WaitEof(Arc::clone(&release_eof))]);
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/missing-response", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap()
                .source;
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().writes.is_empty() {
                    tokio::select! {
                        result = run.as_mut() => {
                            panic!("session completed before request serialization: {result:?}")
                        }
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("upstream request was serialized before EOF");
            release_eof.notify_one();
            let response_error = async move {
                match response.read_header_event().await {
                    Ok(_) => panic!("EOF before response headers produced headers"),
                    Err(error) => error.code(),
                }
            };
            let (run_result, source_error) = tokio::join!(run.as_mut(), response_error);
            assert_eq!(
                run_result
                    .expect_err("EOF with an active response fails the session")
                    .code(),
                HttpErrorCode::TRANSPORT_EOF,
            );
            assert_eq!(source_error, HttpErrorCode::TRANSPORT_EOF);
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

fn upstream_connection_close_rejects_later_request() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::YieldData(
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            )]);
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/first", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap()
                .source;
            let reject = async move {
                let headers = response.read_header_event().await.unwrap();
                assert!(headers.eom());
                handle
                    .reserve_request()
                    .err()
                    .expect("non-reusable upstream response drains the session")
                    .code()
            };
            let (result, rejection) = tokio::join!(session.run(), reject);
            result.unwrap();
            assert!(matches!(
                rejection,
                HttpErrorCode::REFUSED_STREAM | HttpErrorCode::DROPPED
            ));
            let output = state.borrow().writes.concat();
            assert_eq!(
                output
                    .windows(b"GET /first HTTP/1.1".len())
                    .filter(|wire| *wire == b"GET /first HTTP/1.1")
                    .count(),
                1,
            );
        })
    });
}

fn idle_upstream() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Eof]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn upstream_response_split_around_h1_padding() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n".to_vec()),
                ReadAction::Data(vec![b'y'; 90]),
                ReadAction::YieldData(vec![b'y'; 10]),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let request = HttpFixedSource::make_fixed_request(
                b"/",
                HttpMethod::Post,
                Some(queue(&[b'x'; 100])),
            )
            .unwrap();
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(request)),
                    CancellationToken::never(),
                )
                .await
                .unwrap()
                .source;
            let consume = async move {
                let headers = response.read_header_event().await.unwrap();
                assert!(!headers.eom());
                let mut total = 0;
                loop {
                    let event = response.read_body_event(u32::MAX).await.unwrap();
                    total += event.as_body().map_or(0, BufQueue::chain_length);
                    if event.eom() {
                        break;
                    }
                }
                total
            };
            let (result, total) = tokio::join!(session.run(), consume);
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: total,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1TimeoutPolicy {
    Automatic,
    ExplicitBeforeRead,
    ExplicitWhilePending,
    AutomaticReplacesExplicit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1PublicationCase {
    Complete,
    HeaderTimeout(H1TimeoutPolicy),
    BodyTimeout(H1TimeoutPolicy),
    Cancel,
    UploadError,
}

#[derive(Default)]
struct H1ResponseProbe {
    suspends: Cell<usize>,
    resumed: RefCell<Vec<ResumeStatus>>,
}

fn h1_read_response_body(
    source: &Rc<RefCell<HttpSourceHolder>>,
    probe: &Rc<H1ResponseProbe>,
) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
    let source = Rc::clone(source);
    let probe = Rc::clone(probe);
    Box::pin(async move {
        loop {
            let read = source.borrow_mut().read_body_event(64);
            let event = read.await?;
            if !matches!(event.kind(), HttpBodyEventKind::Suspend(_)) {
                return Ok(event);
            }
            let HttpBodyEventKind::Suspend(resume) = event.into_kind() else {
                unreachable!("the event was checked before consuming it")
            };
            probe.suspends.set(probe.suspends.get() + 1);
            let status = resume.await;
            probe.resumed.borrow_mut().push(status);
        }
    })
}

struct H1UploadProbe {
    owner: ThreadId,
    drops: Cell<usize>,
    pending_reads: Cell<usize>,
    wrong_thread: Cell<bool>,
    stops: RefCell<Vec<Option<HttpErrorCode>>>,
}

struct H1UploadRead(Rc<H1UploadProbe>);

impl Drop for H1UploadRead {
    fn drop(&mut self) {
        self.0.pending_reads.set(self.0.pending_reads.get() - 1);
        if std::thread::current().id() != self.0.owner {
            self.0.wrong_thread.set(true);
        }
    }
}

struct H1PublicationUpload {
    header: Option<HttpHeaderEvent>,
    stage: usize,
    release: Rc<CancellableBaton>,
    probe: Rc<H1UploadProbe>,
    failure: Option<HttpError>,
}

impl HttpSource for H1PublicationUpload {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"upload headers exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let stage = self.stage;
        self.stage += 1;
        let probe = Rc::clone(&self.probe);
        let release = Rc::clone(&self.release);
        let failure = self.failure.clone();
        Box::pin(async move {
            if std::thread::current().id() != probe.owner {
                probe.wrong_thread.set(true);
            }
            match stage {
                0 => Ok(HttpBodyEvent::body(queue(b"upload-"), false)),
                1 => {
                    probe.pending_reads.set(probe.pending_reads.get() + 1);
                    let _read = H1UploadRead(probe);
                    let _ = release.wait(CancellationToken::never()).await;
                    match failure {
                        Some(error) => Err(error),
                        None => Ok(HttpBodyEvent::body(queue(b"done"), false)),
                    }
                }
                2 => {
                    let mut trailers = HttpHeaders::new();
                    trailers.add(b"x-upload", b"sent");
                    Ok(HttpBodyEvent::trailers(trailers))
                }
                _ => Err(HttpError::with_message(
                    HttpErrorCode::DROPPED,
                    b"upload body exhausted",
                )),
            }
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.stops.borrow_mut().push(error);
        if std::thread::current().id() != self.probe.owner {
            self.probe.wrong_thread.set(true);
        }
    }
}

impl Drop for H1PublicationUpload {
    fn drop(&mut self) {
        self.probe.drops.set(self.probe.drops.get() + 1);
        if std::thread::current().id() != self.probe.owner {
            self.probe.wrong_thread.set(true);
        }
    }
}

#[derive(Debug)]
struct H1PublicationOutcome {
    case: H1PublicationCase,
    operation: Result<Option<HttpError>, String>,
    run: Result<crate::uniplex::UniplexRunSummary, HttpError>,
    receipt: Result<(), roxygen_core::exchange::ExchangeCompletionError>,
    source_drops: usize,
    pending_reads: usize,
    stops: Vec<Option<HttpErrorCode>>,
    wrong_thread: bool,
    close: CloseKind,
    suspends: usize,
    resumed: Vec<ResumeStatus>,
}

fn h1_require(condition: bool, reason: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(reason.to_owned())
    }
}

fn h1_publication_case(case: H1PublicationCase) -> H1PublicationOutcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let timeout_policy = match case {
                H1PublicationCase::HeaderTimeout(policy)
                | H1PublicationCase::BodyTimeout(policy) => Some(policy),
                _ => None,
            };
            let start_response = Arc::new(tokio::sync::Notify::new());
            let finish_response = Arc::new(tokio::sync::Notify::new());
            let peer_eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitDataOrCancellation(
                    Arc::clone(&start_response),
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-response\r\n\r\n6\r\nearly-\r\n".to_vec(),
                ),
                ReadAction::WaitDataOrCancellation(
                    Arc::clone(&finish_response),
                    b"4\r\nlate\r\n0\r\nx-response: verified\r\n\r\n".to_vec(),
                ),
                ReadAction::WaitEof(Arc::clone(&peer_eof)),
            ]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig {
                    stream_read_timeout: if timeout_policy.is_some() {
                        Duration::from_millis(10)
                    } else {
                        Duration::from_secs(1)
                    },
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            let release_upload = Rc::new(CancellableBaton::new());
            let probe = Rc::new(H1UploadProbe {
                owner: std::thread::current().id(),
                drops: Cell::new(0),
                pending_reads: Cell::new(0),
                wrong_thread: Cell::new(false),
                stops: RefCell::new(Vec::new()),
            });
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/duplex", false).unwrap();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_http_version(1, 1);
            message.set_chunked(true);
            message.headers_mut().set(b"trailer", b"x-upload");
            let cancellation = CancellationSource::new();
            let reply = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(H1PublicationUpload {
                        header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                        stage: 0,
                        release: Rc::clone(&release_upload),
                        probe: Rc::clone(&probe),
                        failure: (case == H1PublicationCase::UploadError).then(|| {
                            HttpError::with_message(
                                HttpErrorCode::INTERNAL_ERROR,
                                b"first staged upload failure",
                            )
                        }),
                    })),
                    cancellation.token(),
                )
                .await
                .unwrap();
            let response = Rc::new(RefCell::new(reply.source));
            let response_probe = Rc::new(H1ResponseProbe::default());
            let completion = reply.completion;
            let operation = async {
                let result = tokio::time::timeout(Duration::from_secs(2), async {
                    while !state.borrow().writes.concat().windows(7).any(|part| part == b"upload-")
                        || probe.pending_reads.get() != 1
                    {
                        tokio::task::yield_now().await;
                    }
                    if !matches!(case, H1PublicationCase::HeaderTimeout(_)) {
                        start_response.notify_one();
                        let read = response.borrow_mut().read_header_event();
                        let head = read.await.map_err(|error| error.to_string())?;
                        let mut head = head.into_headers();
                        h1_require(head.status_code().ok() == Some(200), "early response status")?;
                        let first = h1_read_response_body(&response, &response_probe).await.map_err(|error| error.to_string())?;
                        h1_require(!first.eom(), "early body became EOM")?;
                        h1_require(
                            first.as_body().is_some_and(|body| body.coalesce(64).is_ok_and(|bytes| bytes.as_ref() == b"early-")),
                            "first response bytes were unavailable before upload EOM",
                        )?;
                    }
                    h1_require(probe.drops.get() == 0, "early response retired pending upload")?;
                    let mut receipt = completion.wait();
                    h1_require(
                        poll_fn(|context| Poll::Ready(receipt.as_mut().poll(context).is_pending())).await,
                        "early response certified an incomplete upload",
                    )?;
                    drop(receipt);
                    if let Some(policy) = timeout_policy {
                        if policy == H1TimeoutPolicy::ExplicitBeforeRead {
                            response.borrow_mut().set_read_timeout(Duration::from_millis(15));
                        }
                        let suspends_before = response_probe.suspends.get();
                        let resumed_before = response_probe.resumed.borrow().len();
                        let mut pending: roxygen_core::source::LocalSourceFuture<Result<(), HttpError>> =
                            if matches!(case, H1PublicationCase::HeaderTimeout(_)) {
                                let read = response.borrow_mut().read_header_event();
                                Box::pin(async move { read.await.map(|_| ()) })
                            } else {
                                let read = h1_read_response_body(&response, &response_probe);
                                Box::pin(async move { read.await.map(|_| ()) })
                            };
                        h1_require(
                            poll_fn(|context| Poll::Ready(pending.as_mut().poll(context).is_pending())).await,
                            "the actual response read/resume must be pending before the timeout boundary",
                        )?;
                        if matches!(case, H1PublicationCase::BodyTimeout(_)) {
                            h1_require(
                                response_probe.suspends.get() == suspends_before + 1
                                    && response_probe.resumed.borrow().len() == resumed_before,
                                "the body timeout must retain the actual pending resume future",
                            )?;
                        }
                        if policy == H1TimeoutPolicy::ExplicitWhilePending {
                            response.borrow_mut().set_read_timeout(Duration::from_millis(15));
                        }
                        let automatic = matches!(policy, H1TimeoutPolicy::Automatic | H1TimeoutPolicy::AutomaticReplacesExplicit);
                        if automatic {
                            h1_require(
                                tokio::time::timeout(Duration::from_millis(45), pending.as_mut()).await.is_err(),
                                "automatic response timer ran while request EOM was withheld",
                            )?;
                            h1_require(
                                response_probe.resumed.borrow().len() == resumed_before,
                                "the retained resume completed during the disabled interval",
                            )?;
                            if policy == H1TimeoutPolicy::AutomaticReplacesExplicit {
                                response.borrow_mut().set_read_timeout(Duration::ZERO);
                            }
                            release_upload.signal();
                        }
                        if !automatic {
                            h1_require(probe.pending_reads.get() == 1 && probe.drops.get() == 0,
                                "explicit timeout must observe the unreleased upload still owned")?;
                        }
                        let result = pending.await;
                        if matches!(case, H1PublicationCase::BodyTimeout(_)) {
                            h1_require(
                                response_probe.resumed.borrow().get(resumed_before) == Some(&ResumeStatus::TimedOut),
                                "the retained response-body resume did not time out",
                            )?;
                        }
                        let upload_eom = state.borrow().writes.concat().windows(18).any(|part| part == b"x-upload: sent\r\n\r\n");
                        h1_require(upload_eom == automatic, "timeout completed on the wrong side of request EOM")?;
                        return Ok(result.err());
                    }
                    if matches!(case, H1PublicationCase::Cancel | H1PublicationCase::UploadError) {
                        let suspends_before = response_probe.suspends.get();
                        let mut pending = h1_read_response_body(&response, &response_probe);
                        h1_require(
                            poll_fn(|context| Poll::Ready(pending.as_mut().poll(context).is_pending())).await,
                            "response body was not pending at failure boundary",
                        )?;
                        h1_require(response_probe.suspends.get() == suspends_before + 1,
                            "failure boundary must retain the actual body resume")?;
                        if case == H1PublicationCase::Cancel {
                            cancellation.request_cancellation();
                        } else {
                            release_upload.signal();
                        }
                        return Ok(pending.await.err());
                    }
                    release_upload.signal();
                    while !state.borrow().writes.concat().windows(18).any(|part| part == b"x-upload: sent\r\n\r\n") {
                        tokio::task::yield_now().await;
                    }
                    finish_response.notify_one();
                    let body = h1_read_response_body(&response, &response_probe).await.map_err(|error| error.to_string())?;
                    h1_require(
                        body.as_body().is_some_and(|body| body.coalesce(64).is_ok_and(|bytes| bytes.as_ref() == b"late")) && !body.eom(),
                        "remaining response body/order changed",
                    )?;
                    let trailers = h1_read_response_body(&response, &response_probe).await.map_err(|error| error.to_string())?;
                    h1_require(trailers.eom(), "trailers did not terminate the response")?;
                    h1_require(
                        matches!(trailers.kind(), roxygen_core::events::HttpBodyEventKind::Trailers(headers) if headers.get_single_or_empty(b"x-response") == b"verified"),
                        "response trailers were lost",
                    )?;
                    Ok(None)
                })
                .await
                .map_err(|_| format!("{case:?}: bounded H1 progress deadline"))
                .and_then(|result| result.map_err(|reason: String| format!("{case:?}: {reason}")));
                if result.is_err() {
                    cancellation.request_cancellation();
                }
                if response.borrow().readable() {
                    response
                        .borrow_mut()
                        .stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
                }
                peer_eof.notify_one();
                let receipt = completion.wait().await;
                (result, receipt)
            };
            let (run, (operation, receipt)) = tokio::join!(session.run(), operation);
            H1PublicationOutcome {
                case,
                operation,
                run,
                receipt,
                source_drops: probe.drops.get(),
                pending_reads: probe.pending_reads.get(),
                stops: probe.stops.borrow().clone(),
                wrong_thread: probe.wrong_thread.get(),
                close: state.borrow().close,
                suspends: response_probe.suspends.get(),
                resumed: response_probe.resumed.borrow().clone(),
            }
        })
    })
}

fn assert_h1_publication_retired(outcome: &H1PublicationOutcome) {
    assert_eq!(outcome.suspends, outcome.resumed.len(), "{outcome:?}");
    assert_eq!(outcome.receipt, Ok(()), "{outcome:?}");
    assert_eq!(outcome.source_drops, 1, "{outcome:?}");
    assert_eq!(outcome.pending_reads, 0, "{outcome:?}");
    assert!(!outcome.wrong_thread, "{outcome:?}");
    assert!(
        outcome.stops.len() <= 1,
        "source stopped twice: {outcome:?}"
    );
}

struct IdleAtDetach {
    handle: UniplexHandle,
    transport: Rc<RefCell<TransportState>>,
    observed: Rc<Cell<Option<(bool, bool)>>>,
    detached: Rc<tokio::sync::Notify>,
}

impl LifecycleObserver for IdleAtDetach {
    fn on_event(&self, event: LifecycleEvent) {
        if matches!(
            event,
            LifecycleEvent::TransactionDetached { stream: Some(_) }
        ) {
            let final_bytes_written = self
                .transport
                .borrow()
                .writes
                .concat()
                .windows(18)
                .any(|bytes| bytes == b"x-upload: sent\r\n\r\n");
            self.observed
                .set(Some((self.handle.is_idle(), final_bytes_written)));
            self.detached.notify_one();
        }
    }
}

#[test]
fn detached_upload_is_not_idle_until_staged_egress_is_written() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let finish = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::WaitData(
                    Arc::clone(&respond),
                    b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
                ),
                ReadAction::WaitEof(Arc::clone(&finish)),
            ]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let observed = Rc::new(Cell::new(None));
            let detached = Rc::new(tokio::sync::Notify::new());
            session
                .add_lifecycle_observer(Rc::new(IdleAtDetach {
                    handle: handle.clone(),
                    transport: Rc::clone(&state),
                    observed: Rc::clone(&observed),
                    detached: Rc::clone(&detached),
                }))
                .unwrap();
            let release_upload = Rc::new(CancellableBaton::new());
            let probe = Rc::new(H1UploadProbe {
                owner: std::thread::current().id(),
                drops: Cell::new(0),
                pending_reads: Cell::new(0),
                wrong_thread: Cell::new(false),
                stops: RefCell::new(Vec::new()),
            });
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_url(b"/early-response", false).unwrap();
            message.set_http_version(1, 1);
            message.set_chunked(true);
            let reply = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(H1PublicationUpload {
                        header: Some(HttpHeaderEvent::new(message, false).unwrap()),
                        stage: 0,
                        release: Rc::clone(&release_upload),
                        probe: Rc::clone(&probe),
                        failure: None,
                    })),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let mut response = reply.source;
            let completion = reply.completion;
            let release_write = Rc::new(tokio::sync::Notify::new());
            state.borrow_mut().write_gate = Some((
                b"x-upload: sent\r\n\r\n".to_vec(),
                Rc::clone(&release_write),
            ));
            let exercise = async {
                respond.notify_one();
                assert!(response.read_header_event().await.unwrap().eom());
                assert!(!handle.is_idle(), "unfinished upload remains live");
                release_upload.signal();
                detached.notified().await;
                assert_eq!(
                    observed.get(),
                    Some((false, false)),
                    "detaching the stream does not make its staged final bytes idle"
                );
                completion
                    .wait()
                    .await
                    .expect("exchange ownership retires during the held write");
                assert!(
                    state
                        .borrow()
                        .writes
                        .concat()
                        .windows(18)
                        .any(|bytes| bytes == b"x-upload: sent\r\n\r\n"),
                    "the final native write has been submitted"
                );
                assert!(
                    !handle.is_idle(),
                    "physical egress remains pending after exchange retirement"
                );
                release_write.notify_one();
                while !handle.is_idle() {
                    tokio::task::yield_now().await;
                }
                assert!(
                    state
                        .borrow()
                        .writes
                        .concat()
                        .windows(18)
                        .any(|bytes| bytes == b"x-upload: sent\r\n\r\n")
                );
                finish.notify_one();
            };
            let (run, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(session.run(), exercise)
            })
            .await
            .expect("staged final upload drains before idleness");
            run.unwrap();
            assert_ne!(state.borrow().close, CloseKind::Reset);
        })
    });
}

#[test]
fn h1_response_head_and_body_progress_before_request_eom() {
    let outcome = h1_publication_case(H1PublicationCase::Complete);
    assert_h1_publication_retired(&outcome);
    assert!(matches!(outcome.operation, Ok(None)), "{outcome:?}");
    assert!(outcome.run.is_ok(), "{outcome:?}");
    assert_ne!(outcome.close, CloseKind::Reset, "{outcome:?}");
}

#[test]
fn h1_response_read_timeout_arms_only_after_request_eom() {
    for case in [
        H1TimeoutPolicy::Automatic,
        H1TimeoutPolicy::ExplicitBeforeRead,
        H1TimeoutPolicy::ExplicitWhilePending,
        H1TimeoutPolicy::AutomaticReplacesExplicit,
    ]
    .into_iter()
    .flat_map(|policy| {
        [
            H1PublicationCase::HeaderTimeout(policy),
            H1PublicationCase::BodyTimeout(policy),
        ]
    }) {
        let outcome = h1_publication_case(case);
        assert_h1_publication_retired(&outcome);
        let error = outcome
            .operation
            .as_ref()
            .expect("timeout operation completed")
            .as_ref()
            .expect("read timed out");
        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT, "{outcome:?}");
        let expected_timeout = match outcome.case {
            H1PublicationCase::HeaderTimeout(
                H1TimeoutPolicy::ExplicitBeforeRead | H1TimeoutPolicy::ExplicitWhilePending,
            )
            | H1PublicationCase::BodyTimeout(
                H1TimeoutPolicy::ExplicitBeforeRead | H1TimeoutPolicy::ExplicitWhilePending,
            ) => b"Read timeout after 15".as_slice(),
            _ => b"Read timeout after 10".as_slice(),
        };
        assert_eq!(error.message(), expected_timeout, "{outcome:?}");
        assert!(
            outcome.run.is_ok(),
            "stream timeout is not a transport failure: {outcome:?}"
        );
        assert_eq!(outcome.close, CloseKind::Reset, "{outcome:?}");
    }
}

#[test]
fn h1_early_response_cancellation_and_upload_errors_preserve_causes() {
    for case in [H1PublicationCase::Cancel, H1PublicationCase::UploadError] {
        let outcome = h1_publication_case(case);
        assert_h1_publication_retired(&outcome);
        let error = outcome
            .operation
            .as_ref()
            .expect("failure operation completed")
            .as_ref()
            .expect("response must fail");
        assert_eq!(error.code(), HttpErrorCode::CORO_CANCELLED, "{outcome:?}");
        let run_error = outcome
            .run
            .as_ref()
            .expect_err("H1 request failure terminates the connection");
        let expected = if case == H1PublicationCase::Cancel {
            HttpErrorCode::CORO_CANCELLED
        } else {
            HttpErrorCode::INTERNAL_ERROR
        };
        assert_eq!(run_error.code(), expected, "{outcome:?}");
        if case == H1PublicationCase::UploadError {
            assert_eq!(
                run_error.message(),
                b"first staged upload failure",
                "{outcome:?}"
            );
        }
        assert_eq!(outcome.close, CloseKind::Reset, "{outcome:?}");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1HeaderEomCase {
    ExplicitBeforeRead,
    ExplicitWhilePending,
    ZeroBeforeRead,
    FiniteToZero,
    CancelDefault,
    CancelExplicit,
}

#[derive(Debug)]
struct H1HeaderEomOutcome {
    operation: Result<HttpError, String>,
    run: Result<crate::uniplex::UniplexRunSummary, HttpError>,
    receipt: Result<(), roxygen_core::exchange::ExchangeCompletionError>,
    automatic_remaining: bool,
    finished: bool,
    reads: usize,
    source_drops: usize,
    stops: Vec<Option<HttpErrorCode>>,
    wrong_thread: bool,
    transport_drops: usize,
    resets: usize,
    close: CloseKind,
}

struct H1HeaderEomProbe {
    owner: ThreadId,
    reads: Cell<usize>,
    drops: Cell<usize>,
    stops: RefCell<Vec<Option<HttpErrorCode>>>,
    wrong_thread: Cell<bool>,
}

struct H1HeaderEomResponse {
    inner: HttpSourceHolder,
    probe: Rc<H1HeaderEomProbe>,
}

impl HttpSource for H1HeaderEomResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.probe.reads.set(self.probe.reads.get() + 1);
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.inner.read_body_event(maximum)
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.stops.borrow_mut().push(error);
        if std::thread::current().id() != self.probe.owner {
            self.probe.wrong_thread.set(true);
        }
        self.inner.stop_reading(error);
    }

    fn get_stream_id(&self) -> Option<roxygen_core::ids::StreamId> {
        self.inner.get_stream_id()
    }

    fn set_read_timeout(&mut self, timeout: Duration) {
        self.inner.set_read_timeout(timeout);
    }
}

impl Drop for H1HeaderEomResponse {
    fn drop(&mut self) {
        self.probe.drops.set(self.probe.drops.get() + 1);
        if std::thread::current().id() != self.probe.owner {
            self.probe.wrong_thread.set(true);
        }
    }
}

async fn poll_h1_header_phase<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

async fn h1_retained_header_pending(
    read: &mut roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
    phase: &str,
) -> Result<(), String> {
    match poll_h1_header_phase(read.as_mut()).await {
        Poll::Pending => Ok(()),
        Poll::Ready(Err(error)) => Err(format!(
            "{phase}: retained header read completed with error: {error:?}"
        )),
        Poll::Ready(Ok(headers)) => Err(format!(
            "{phase}: retained header read completed with headers (eom={})",
            headers.eom(),
        )),
    }
}

async fn h1_retained_header_error(
    read: &mut roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>>,
) -> Result<HttpError, String> {
    let deadline = tokio::time::Instant::now();
    for _ in 0..32 {
        h1_require(
            tokio::time::Instant::now() == deadline,
            "terminal poll advanced the paused clock",
        )?;
        match poll_h1_header_phase(read.as_mut()).await {
            Poll::Ready(Err(error)) => return Ok(error),
            Poll::Ready(Ok(headers)) => {
                return Err(format!(
                    "unexpected response headers (eom={})",
                    headers.eom(),
                ));
            }
            Poll::Pending => tokio::task::yield_now().await,
        }
    }
    Err("retained header read missed its original deadline or cancellation".to_owned())
}

fn h1_header_eom_phase_case(case: H1HeaderEomCase) {
    let outcome = run_on_worker(move |context| {
        Box::pin(async move {
            tokio::time::pause();
            let drops = Arc::new(AtomicUsize::new(0));
            let resets = Arc::new(AtomicUsize::new(0));
            let (transport, state) = ScriptedTransport::new([]);
            let transport: Rc<dyn UniplexTransport> = Rc::new(DropProbeTransport {
                inner: transport,
                drops: Arc::clone(&drops),
                reset_calls: Arc::clone(&resets),
                owner: std::thread::current().id(),
            });
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig {
                    stream_read_timeout: Duration::from_millis(10),
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            let method = if matches!(
                case,
                H1HeaderEomCase::ExplicitWhilePending
                    | H1HeaderEomCase::FiniteToZero
                    | H1HeaderEomCase::CancelExplicit
            ) {
                HttpMethod::Post
            } else {
                HttpMethod::Get
            };
            let mut request = roxygen_core::message::HttpMessage::new();
            request.set_method(method).unwrap();
            request.set_url(b"/header-eom", false).unwrap();
            request.set_http_version(1, 1);
            let reply = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(HttpFixedSource::new(request, None))),
                    CancellationToken::never(),
                )
                .await
                .unwrap();
            let probe = Rc::new(H1HeaderEomProbe {
                owner: std::thread::current().id(),
                reads: Cell::new(0),
                drops: Cell::new(0),
                stops: RefCell::new(Vec::new()),
                wrong_thread: Cell::new(false),
            });
            let mut response = HttpSourceHolder::new(Box::new(H1HeaderEomResponse {
                inner: reply.source,
                probe: Rc::clone(&probe),
            }));
            let mut run = Box::pin(session.run());
            let mut run_result = None;
            let explicit = Duration::from_millis(20);
            match case {
                H1HeaderEomCase::ExplicitBeforeRead
                | H1HeaderEomCase::FiniteToZero
                | H1HeaderEomCase::CancelExplicit => response.set_read_timeout(explicit),
                H1HeaderEomCase::ZeroBeforeRead => response.set_read_timeout(Duration::ZERO),
                H1HeaderEomCase::ExplicitWhilePending | H1HeaderEomCase::CancelDefault => {}
            }
            let mut read = response.read_header_event();
            let cancel = matches!(
                case,
                H1HeaderEomCase::CancelDefault | H1HeaderEomCase::CancelExplicit
            );
            let operation: Result<HttpError, String> = async {
                h1_require(
                    response.get_stream_id() == Some(reply.stream),
                    "stream identity changed",
                )?;
                h1_require(
                    handle.has_response_timeout_for_test(reply.stream),
                    "automatic entry missing before serialization",
                )?;
                h1_retained_header_pending(&mut read, "before explicit setting").await?;
                if case == H1HeaderEomCase::ExplicitWhilePending {
                    response.set_read_timeout(explicit);
                    h1_retained_header_pending(&mut read, "explicit setting during the same read")
                        .await?;
                }
                let started = tokio::time::Instant::now();
                tokio::time::advance(Duration::from_millis(5)).await;
                h1_retained_header_pending(&mut read, "before queued header serialization").await?;
                h1_require(
                    state.borrow().writes.is_empty(),
                    "writer progressed before its first poll",
                )?;
                if cancel {
                    response.stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
                    tokio::time::advance(Duration::from_millis(40)).await;
                    return h1_retained_header_error(&mut read).await;
                }
                if case == H1HeaderEomCase::FiniteToZero {
                    response.set_read_timeout(Duration::ZERO);
                    h1_retained_header_pending(&mut read, "finite-to-zero on the same read")
                        .await?;
                }
                for _ in 0..32 {
                    if let Poll::Ready(result) = poll_h1_header_phase(run.as_mut()).await {
                        let reason = format!("run finished before response timeout: {result:?}");
                        run_result = Some(result);
                        return Err(reason);
                    }
                    if !state.borrow().writes.is_empty() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                let request_line = if method == HttpMethod::Get {
                    b"GET /header-eom HTTP/1.1\r\n".as_slice()
                } else {
                    b"POST /header-eom HTTP/1.1\r\n".as_slice()
                };
                h1_require(
                    state.borrow().writes.concat().starts_with(request_line),
                    "actual header-EOM request was not serialized",
                )?;
                h1_require(
                    !handle.has_response_timeout_for_test(reply.stream),
                    "serialized automatic entry was not consumed",
                )?;
                h1_require(
                    tokio::time::Instant::now() == started + Duration::from_millis(5),
                    "serialization advanced the paused clock",
                )?;
                h1_retained_header_pending(&mut read, "after queued header serialization").await?;
                let deadline = if matches!(
                    case,
                    H1HeaderEomCase::ZeroBeforeRead | H1HeaderEomCase::FiniteToZero
                ) {
                    tokio::time::advance(Duration::from_millis(40)).await;
                    h1_retained_header_pending(
                        &mut read,
                        "zero survives both old finite and automatic deadlines",
                    )
                    .await?;
                    let reset_at = tokio::time::Instant::now();
                    response.set_read_timeout(explicit);
                    h1_retained_header_pending(&mut read, "post-serialization explicit setting")
                        .await?;
                    tokio::time::advance(Duration::from_millis(19)).await;
                    reset_at + explicit
                } else {
                    tokio::time::advance(Duration::from_millis(14)).await;
                    started + explicit
                };
                h1_retained_header_pending(
                    &mut read,
                    "one millisecond before the original explicit deadline",
                )
                .await?;
                tokio::time::advance(Duration::from_millis(1)).await;
                h1_require(
                    tokio::time::Instant::now() == deadline,
                    "explicit deadline was rebased",
                )?;
                let timer_tick = Duration::from_millis(1);
                tokio::time::advance(timer_tick).await;
                h1_require(
                    tokio::time::Instant::now() == deadline + timer_tick,
                    "timer settlement exceeded one backend tick",
                )?;
                h1_retained_header_error(&mut read).await
            }
            .await;
            drop(read);
            if response.readable() {
                response.stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
            }
            drop(response);
            let run_result = match run_result {
                Some(result) => result,
                None => run.as_mut().await,
            };
            drop(run);
            let receipt = reply.completion.wait().await;
            let automatic_remaining = handle.has_response_timeout_for_test(reply.stream);
            let finished = handle.is_finished();
            drop(handle);
            H1HeaderEomOutcome {
                operation,
                run: run_result,
                receipt,
                automatic_remaining,
                finished,
                reads: probe.reads.get(),
                source_drops: probe.drops.get(),
                stops: probe.stops.borrow().clone(),
                wrong_thread: probe.wrong_thread.get(),
                transport_drops: drops.load(Ordering::SeqCst),
                resets: resets.load(Ordering::SeqCst),
                close: state.borrow().close,
            }
        })
    });
    assert_eq!(outcome.receipt, Ok(()), "{case:?}: {outcome:?}");
    assert!(
        outcome.finished,
        "{case:?}: actual session run did not complete: {outcome:?}"
    );
    assert!(
        !outcome.automatic_remaining,
        "{case:?}: timeout entry survived cleanup: {outcome:?}"
    );
    assert_eq!(
        outcome.reads, 1,
        "{case:?}: a replacement read supplied the oracle: {outcome:?}"
    );
    assert_eq!(
        outcome.source_drops, 1,
        "{case:?}: returned source retirement: {outcome:?}"
    );
    assert!(
        !outcome.wrong_thread,
        "{case:?}: source left its owner: {outcome:?}"
    );
    assert_eq!(
        outcome.transport_drops, 1,
        "{case:?}: transport retirement: {outcome:?}"
    );
    assert_eq!(
        outcome.resets, 1,
        "{case:?}: one physical reset: {outcome:?}"
    );
    assert_eq!(outcome.close, CloseKind::Reset, "{case:?}: {outcome:?}");
    assert!(
        outcome.run.is_ok(),
        "{case:?}: source failure is not transport failure: {outcome:?}"
    );
    let error = outcome
        .operation
        .as_ref()
        .unwrap_or_else(|reason| panic!("{case:?}: {reason}; {outcome:?}"));
    if matches!(
        case,
        H1HeaderEomCase::CancelDefault | H1HeaderEomCase::CancelExplicit
    ) {
        assert_eq!(
            error.code(),
            HttpErrorCode::CORO_CANCELLED,
            "{case:?}: {outcome:?}"
        );
        assert_eq!(
            error.message(),
            b"Abandoned reading",
            "{case:?}: {outcome:?}"
        );
        assert_eq!(
            outcome.stops,
            vec![Some(HttpErrorCode::CORO_CANCELLED)],
            "{case:?}: {outcome:?}"
        );
    } else {
        assert_eq!(
            error.code(),
            HttpErrorCode::READ_TIMEOUT,
            "{case:?}: {outcome:?}"
        );
        assert_eq!(
            error.message(),
            b"Read timeout after 20",
            "{case:?}: {outcome:?}"
        );
        assert!(
            outcome.stops.is_empty(),
            "{case:?}: terminal source was stopped again: {outcome:?}"
        );
    }
}

#[test]
fn h1_header_eom_explicit_timeout_survives_queued_serialization() {
    for case in [
        H1HeaderEomCase::ExplicitBeforeRead,
        H1HeaderEomCase::ExplicitWhilePending,
    ] {
        h1_header_eom_phase_case(case);
    }
}

#[test]
fn h1_header_eom_explicit_zero_survives_queued_serialization() {
    for case in [
        H1HeaderEomCase::ZeroBeforeRead,
        H1HeaderEomCase::FiniteToZero,
    ] {
        h1_header_eom_phase_case(case);
    }
}

#[test]
fn h1_header_eom_cancel_before_serialization_preserves_cause() {
    for case in [
        H1HeaderEomCase::CancelDefault,
        H1HeaderEomCase::CancelExplicit,
    ] {
        h1_header_eom_phase_case(case);
    }
}

fn header_only_response_waits_for_serialized_eom() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::YieldData(b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(5),
                ..UniplexConfig::default()
            };
            let (session, handle) =
                UniplexSession::new(context, TransportDirection::Upstream, erased, None, config)
                    .unwrap();
            let request = HttpSourceHolder::new(Box::new(
                HttpFixedSource::make_get_request(b"/", None).unwrap(),
            ));
            let mut response = handle
                .send_request(request, CancellationSource::new().token())
                .await
                .unwrap()
                .source;

            assert!(
                tokio::time::timeout(Duration::from_millis(20), response.read_header_event(),)
                    .await
                    .is_err(),
                "response timeout started before request EOM serialization",
            );

            let consume = async move {
                let headers = response.read_header_event().await.unwrap();
                assert!(headers.eom());
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(session.run(), consume)
            })
            .await
            .expect("serialized request opened its response gate");
            result.unwrap();
            assert!(
                state
                    .borrow()
                    .writes
                    .concat()
                    .starts_with(b"GET / HTTP/1.1")
            );
        })
    });
}

fn rejected_cross_session_request_does_not_advance_h1_stream() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport_a, _state_a) = ScriptedTransport::new([ReadAction::Eof]);
            let (transport_b, _state_b) = ScriptedTransport::new([
                ReadAction::YieldData(b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let (session_a, handle_a) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                transport_a,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let (session_b, handle_b) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport_b,
                None,
                UniplexConfig::default(),
            )
            .unwrap();

            let foreign = crate::uniplex::UniplexHandle::prepare_request(
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/foreign", None).unwrap(),
                )),
                handle_a.reserve_request().unwrap(),
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let error = match handle_b.send_prepared_request(foreign) {
                Ok(_) => panic!("foreign reservation advanced the target session"),
                Err(error) => error,
            };
            assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);

            let valid = crate::uniplex::UniplexHandle::prepare_request(
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/valid", None).unwrap(),
                )),
                handle_b.reserve_request().unwrap(),
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let response = handle_b.send_prepared_request(valid).unwrap();
            assert_eq!(response.stream.get(), 1);
            let mut source = response.source;
            let consume = async move {
                assert!(source.read_header_event().await.unwrap().eom());
            };
            let (result_a, result_b, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(session_a.run(), session_b.run(), consume)
            })
            .await
            .expect("both sessions completed after the rejected cross-session request");
            result_a.unwrap();
            result_b.unwrap();
        })
    });
}

fn draining_upstream_waits_for_active_response() {
    run_on_worker(|context| {
        Box::pin(async move {
            let response = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata".to_vec();
            let (transport, state) = ScriptedTransport::new([
                ReadAction::YieldData(Vec::new()),
                ReadAction::YieldData(Vec::new()),
                ReadAction::YieldData(response),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(
                        HttpFixedSource::make_get_request(b"/", None).unwrap(),
                    )),
                    CancellationToken::never(),
                )
                .await
                .unwrap()
                .source;
            assert!(handle.initiate_drain().unwrap());
            let consume = async move {
                let headers = response.read_header_event().await.unwrap();
                assert!(!headers.eom());
                let body = response.read_body_event(u32::MAX).await.unwrap();
                assert_eq!(body.as_body().unwrap().chain_length(), 4);
                assert!(body.eom());
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(session.run(), consume)
            })
            .await
            .expect("draining session waited for the active response to detach");
            result.unwrap();
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

fn upstream_response_timeout(
    body_size: usize,
    session_timeout: Duration,
    response_timeout: Option<Duration>,
) -> (HttpErrorCode, CloseKind) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: session_timeout,
                ..UniplexConfig::default()
            };
            let (session, handle) =
                UniplexSession::new(context, TransportDirection::Upstream, erased, None, config)
                    .unwrap();
            let body = (body_size != 0).then(|| queue(&vec![b'x'; body_size]));
            let source = if body_size == 0 {
                HttpFixedSource::make_get_request(b"/", None).unwrap()
            } else {
                HttpFixedSource::make_fixed_request(b"/", HttpMethod::Post, body).unwrap()
            };
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(source)),
                    CancellationSource::new().token(),
                )
                .await
                .unwrap()
                .source;
            if let Some(timeout) = response_timeout {
                response.set_read_timeout(timeout);
            }
            let consume = async move {
                match response.read_header_event().await {
                    Ok(_) => panic!("missing response did not reach the stream timeout"),
                    Err(error) => error.code(),
                }
            };
            let (result, error) = tokio::join!(session.run(), consume);
            let _ = result;
            (error, state.borrow().close)
        })
    })
}

fn pending_request_body_keeps_response_timer_stopped() -> (HttpErrorCode, CloseKind) {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(10),
                ..UniplexConfig::default()
            };
            let (session, handle) =
                UniplexSession::new(context, TransportDirection::Upstream, erased, None, config)
                    .unwrap();
            let cancellation = Rc::new(CancellationSource::new());
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(PendingBodyRequest::new())),
                    cancellation.token(),
                )
                .await
                .unwrap()
                .source;
            let run = session.run();
            tokio::pin!(run);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), async {
                    tokio::select! {
                        result = run.as_mut() => {
                            panic!("session completed while request body production was pending: {result:?}")
                        }
                        _ = response.read_header_event() => {
                            panic!("response timer ran before request EOM")
                        }
                    }
                })
                    .await
                    .is_err(),
                "response source timer ran while request body production was pending",
            );
            response.stop_reading(Some(HttpErrorCode::CORO_CANCELLED));
            let source_error = match response.read_header_event().await {
                Ok(_) => panic!("cancelled response read produced headers"),
                Err(error) => error.code(),
            };
            assert_eq!(source_error, HttpErrorCode::DROPPED);
            cancellation.request_cancellation();
            let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("request cancellation terminates the pending session");
            let error = result
                .expect_err("request cancellation fails the active upstream session")
                .code();
            (error, state.borrow().close)
        })
    })
}

struct PendingHeaderSource;

struct HeadersAlreadyAvailableSource {
    header_reads: Rc<Cell<usize>>,
    body: Option<HttpBodyEvent>,
}

impl HttpSource for HeadersAlreadyAvailableSource {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.header_reads.set(self.header_reads.get() + 1);
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"caller-provided headers must not be read from the body source",
        ))))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(self.body.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request body exhausted")
        })))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl HttpSource for PendingHeaderSource {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<
        Result<roxygen_core::events::HttpHeaderEvent, HttpError>,
    > {
        Box::pin(std::future::pending())
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<
        Result<roxygen_core::events::HttpBodyEvent, HttpError>,
    > {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

struct ErrorBodyRequest {
    header: Option<HttpHeaderEvent>,
}

struct PendingBodyRequest {
    header: Option<HttpHeaderEvent>,
}

impl PendingBodyRequest {
    fn new() -> Self {
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_url(b"/", false).unwrap();
        message.set_method(HttpMethod::Post).unwrap();
        message.set_http_version(1, 1);
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"10".as_slice());
        Self {
            header: Some(HttpHeaderEvent::new(message, false).unwrap()),
        }
    }
}

impl HttpSource for PendingBodyRequest {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::pending())
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

impl ErrorBodyRequest {
    fn new() -> Self {
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_url(b"/", false).unwrap();
        message.set_method(HttpMethod::Post).unwrap();
        message.set_http_version(1, 1);
        message
            .headers_mut()
            .set(HttpHeaderCode::ContentLength, b"1".as_slice());
        Self {
            header: Some(HttpHeaderEvent::new(message, false).unwrap()),
        }
    }
}

impl HttpSource for ErrorBodyRequest {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.header.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        Box::pin(std::future::ready(Err(HttpError::with_message(
            HttpErrorCode::INTERNAL_ERROR,
            b"scripted body failure",
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

fn upstream_body_error() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(ErrorBodyRequest::new())),
                    CancellationSource::new().token(),
                )
                .await
                .unwrap()
                .source;
            let consume = async move {
                let _ = response.read_header_event().await;
            };
            let (result, _) = tokio::join!(session.run(), consume);
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn cancel_during_post_body() -> (HttpErrorCode, CloseKind) {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let cancellation = Rc::new(CancellationSource::new());
            let mut response = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(PendingBodyRequest::new())),
                    cancellation.token(),
                )
                .await
                .unwrap()
                .source;
            let trigger = {
                let cancellation = Rc::clone(&cancellation);
                let state = Rc::clone(&state);
                async move {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while state.borrow().writes.is_empty() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("request headers were written before cancellation");
                    cancellation.request_cancellation();
                }
            };
            let consume = async move {
                match response.read_header_event().await {
                    Ok(_) => panic!("cancelled request unexpectedly received response headers"),
                    Err(error) => error.code(),
                }
            };
            let (run_result, (), source_error) = tokio::join!(session.run(), trigger, consume);
            assert!(run_result.is_err());
            (source_error, state.borrow().close)
        })
    })
}

fn cancelled_before_headers() -> HttpErrorCode {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _state) = ScriptedTransport::new([ReadAction::Eof]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let cancellation = CancellationSource::new();
            cancellation.request_cancellation();
            let result = handle
                .send_request(
                    HttpSourceHolder::new(Box::new(PendingHeaderSource)),
                    cancellation.token(),
                )
                .await;
            drop(handle);
            session.run().await.unwrap();
            match result {
                Ok(_) => panic!("cancelled preparation unexpectedly succeeded"),
                Err(error) => error.code(),
            }
        })
    })
}

fn pipelined_early_responses() -> Outcome {
    downstream(
        vec![
            ReadAction::Data(b"POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n".to_vec()),
            ReadAction::YieldData(b"dataGET / HTTP/1.1\r\n\r\n".to_vec()),
            ReadAction::Eof,
        ],
        HandlerMode::Early(401),
    )
}

#[test]
fn downstream_h1_selected_close_waits_for_consumption_or_terminal_ingress() {
    struct HoldRequest(Rc<RefCell<Option<HttpSourceHolder>>>);

    impl HttpHandler for HoldRequest {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            let held = Rc::clone(&self.0);
            Box::pin(async move {
                assert!(!source.read_header_event().await?.eom());
                *held.borrow_mut() = Some(source);
                let mut response = HttpFixedSource::make_fixed_response(403, None).unwrap();
                response.message_mut().unwrap().set_wants_keepalive(false);
                Ok(HttpSourceHolder::new(Box::new(response)))
            })
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            for case in ["complete", "premature_eof", "malformed_chunk"] {
                let release = Arc::new(tokio::sync::Notify::new());
                let reads = match case {
                    "complete" => vec![ReadAction::DataEof(
                        b"POST / HTTP/1.1\r\nHost: example\r\nConnection: close\r\nContent-Length: 4\r\n\r\ndata".to_vec(),
                    )],
                    "premature_eof" => vec![
                        ReadAction::Data(b"POST / HTTP/1.1\r\nHost: example\r\nConnection: close\r\nContent-Length: 4\r\n\r\n".to_vec()),
                        ReadAction::WaitData(Arc::clone(&release), b"da".to_vec()),
                        ReadAction::Eof,
                    ],
                    _ => vec![
                        ReadAction::Data(b"POST / HTTP/1.1\r\nHost: example\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec()),
                        ReadAction::WaitData(Arc::clone(&release), b"z\r\n".to_vec()),
                        ReadAction::Eof,
                    ],
                };
                let (transport, state) = ScriptedTransport::new(reads);
                let held = Rc::new(RefCell::new(None));
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(Rc::new(HoldRequest(Rc::clone(&held)))),
                    UniplexConfig {
                        drain_request_body_on_drop: true,
                        stream_read_timeout: Duration::ZERO,
                        ..UniplexConfig::default()
                    },
                )
                .unwrap();
                let run = session.run();
                tokio::pin!(run);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().writes.is_empty() {
                        tokio::select! {
                            _ = run.as_mut() => panic!("session retired before its early response: {case}"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                }).await.expect("early response written");
                let output = String::from_utf8(state.borrow().writes.concat())
                    .unwrap()
                    .to_ascii_lowercase();
                assert!(output.starts_with("http/1.1 403"), "{case}: {output}");
                assert!(output.contains("connection: close"), "{case}: {output}");
                let ingress = held
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .ingress_completion()
                    .unwrap();
                if case == "complete" {
                    assert!(
                        ingress.snapshot().unwrap().is_ok(),
                        "wire EOM is already queued"
                    );
                    assert!(
                        tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                            .await
                            .is_err()
                    );
                    assert_eq!(state.borrow().close, CloseKind::Open);
                    assert!(
                        handle
                            .h1_close_snapshot_for_test(roxygen_core::ids::StreamId::new(1))
                            .unwrap()
                            .1
                    );
                    let mut source = held.borrow_mut().take().unwrap();
                    let mut body = Vec::new();
                    loop {
                        let event = source.read_body_event(u32::MAX).await.unwrap();
                        if let Some(bytes) = event.as_body() {
                            bytes
                                .segments()
                                .for_each(|segment| body.extend_from_slice(segment));
                        }
                        if event.eom() {
                            break;
                        }
                    }
                    assert_eq!(body, b"data");
                    drop(source);
                } else {
                    assert!(ingress.snapshot().is_none());
                    release.notify_one();
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while state.borrow().close == CloseKind::Open {
                            tokio::select! {
                                _ = run.as_mut() => panic!("session retired while the retained source is unread: {case}"),
                                _ = tokio::task::yield_now() => {},
                            }
                        }
                    }).await.expect("terminal ingress overrides selected-source wait");
                    assert_eq!(state.borrow().close, CloseKind::Reset, "{case}");
                    assert!(ingress.snapshot().unwrap().is_err());
                    let mut source = held.borrow_mut().take().unwrap();
                    assert!(source.read_body_event(u32::MAX).await.is_err(), "{case}");
                    drop(source);
                }
                let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("source retirement joins the connection");
                if case == "complete" {
                    result.unwrap();
                    assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
                } else {
                    assert!(result.is_err(), "{case}");
                }
            }
        })
    });
}

#[test]
fn downstream_h1_graceful_drain_waits_for_next_response_only_when_enabled() {
    run_on_worker(|context| {
        Box::pin(async move {
            for enabled in [false, true] {
                let release = Arc::new(tokio::sync::Notify::new());
                let (transport, state) = ScriptedTransport::new([
                    ReadAction::Data(b"GET /before HTTP/1.1\r\nHost: example\r\n\r\n".to_vec()),
                    ReadAction::WaitDataOrCancellation(
                        Arc::clone(&release),
                        b"GET /after HTTP/1.1\r\nHost: example\r\n\r\n".to_vec(),
                    ),
                ]);
                let handler: HttpHandlerRef = Rc::new(TestHandler {
                    mode: HandlerMode::Consume(200, 0),
                    bodies: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        wait_for_http1_response_on_drain: enabled,
                        ..UniplexConfig::default()
                    },
                )
                .unwrap();
                let run = session.run();
                tokio::pin!(run);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while state.borrow().writes.is_empty()
                        || !state.borrow().reads.is_empty()
                        || handle
                            .h1_close_snapshot_for_test(roxygen_core::ids::StreamId::new(1))
                            .unwrap()
                            .1
                    {
                        tokio::select! {
                            _ = run.as_mut() => panic!("session closed before the idle drain"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("first response retired and second read is pending");
                let before = state.borrow().writes.concat();
                assert!(
                    !String::from_utf8_lossy(&before)
                        .to_ascii_lowercase()
                        .contains("connection: close")
                );
                assert!(handle.initiate_drain().unwrap());
                assert!(!handle.initiate_drain().unwrap());
                if enabled {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                            .await
                            .is_err()
                    );
                    assert_eq!(state.borrow().close, CloseKind::Open);
                    release.notify_one();
                }
                tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("draining H1 session retires")
                    .unwrap();
                let output = state.borrow().writes.concat();
                let output = String::from_utf8(output).unwrap().to_ascii_lowercase();
                assert_eq!(
                    output.matches("http/1.1 200").count(),
                    if enabled { 2 } else { 1 }
                );
                assert_eq!(
                    output.matches("connection: close").count(),
                    usize::from(enabled)
                );
                assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            }
        })
    });
}

#[test]
fn downstream_h1_graceful_drain_allows_close_error_and_timeout_escalation() {
    struct CloseOnDrain(UniplexHandle, Rc<Cell<bool>>);

    impl LifecycleObserver for CloseOnDrain {
        fn on_event(&self, event: LifecycleEvent) {
            if matches!(event, LifecycleEvent::DrainStarted(_)) {
                assert!(self.0.close_when_idle().unwrap());
                self.1.set(true);
            }
        }
    }

    run_on_worker(|context| {
        Box::pin(async move {
            for escalation in ["close", "callback", "timeout", "error"] {
                let release = Arc::new(tokio::sync::Notify::new());
                let action = match escalation {
                    "timeout" => {
                        ReadAction::WaitError(Arc::clone(&release), HttpErrorCode::READ_TIMEOUT)
                    }
                    "error" => ReadAction::WaitError(
                        Arc::clone(&release),
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                    ),
                    _ => ReadAction::WaitDataOrCancellation(
                        Arc::clone(&release),
                        b"GET /late HTTP/1.1\r\nHost: example\r\n\r\n".to_vec(),
                    ),
                };
                let (transport, state) = ScriptedTransport::new([action]);
                let handler: HttpHandlerRef = Rc::new(TestHandler {
                    mode: HandlerMode::Consume(200, 0),
                    bodies: Rc::new(Cell::new(0)),
                });
                let (session, handle) = UniplexSession::new(
                    context.clone(),
                    TransportDirection::Downstream,
                    transport,
                    Some(handler),
                    UniplexConfig {
                        wait_for_http1_response_on_drain: true,
                        ..UniplexConfig::default()
                    },
                )
                .unwrap();
                let reentered = Rc::new(Cell::new(false));
                if escalation == "callback" {
                    session
                        .add_lifecycle_observer(Rc::new(CloseOnDrain(
                            handle.clone(),
                            Rc::clone(&reentered),
                        )))
                        .unwrap();
                }
                let run = session.run();
                tokio::pin!(run);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !state.borrow().reads.is_empty() {
                        tokio::select! {
                            _ = run.as_mut() => panic!("session closed before idle read admission"),
                            _ = tokio::task::yield_now() => {},
                        }
                    }
                })
                .await
                .expect("idle read admitted");
                assert!(handle.initiate_drain().unwrap());
                match escalation {
                    "close" => {
                        assert!(handle.close_when_idle().unwrap());
                    }
                    "callback" => {
                        assert!(reentered.get());
                    }
                    _ => release.notify_one(),
                }
                let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                    .await
                    .expect("strong close overrides graceful drain without another request");
                if escalation == "error" {
                    assert_eq!(
                        result.unwrap_err().code(),
                        HttpErrorCode::TRANSPORT_READ_ERROR
                    );
                    assert_eq!(state.borrow().close, CloseKind::Reset);
                } else {
                    result.unwrap();
                    assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
                }
                assert!(state.borrow().writes.is_empty());
            }
        })
    });
}

fn anti_pipeline_retains_third_request_until_detach() {
    run_on_worker(|context| {
        Box::pin(async move {
            let third = b"GET /three HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n";
            let mut requests = b"GET /one HTTP/1.1\r\nHost: example\r\n\r\nGET /two HTTP/1.1\r\nHost: example\r\n\r\n"
                .to_vec();
            requests.extend_from_slice(third);
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(requests), ReadAction::Eof]);
            let invocations = Rc::new(Cell::new(0));
            let first_release = Rc::new(CancellableBaton::new());
            let handler: HttpHandlerRef = Rc::new(AntiPipelineHandler {
                invocations: Rc::clone(&invocations),
                first_release: Rc::clone(&first_release),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while invocations.get() < 2 {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before lookahead admission"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("one pipelined lookahead request was admitted");
            assert_eq!(invocations.get(), 2);
            let retained = handle.buffered_ingress_for_test().unwrap();
            assert!(retained.ends_with(third));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                    .await
                    .is_err(),
                "the first transaction remained attached",
            );
            assert_eq!(invocations.get(), 2);
            first_release.signal();
            tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("pipeline resumed after the first detach")
                .unwrap();
            assert_eq!(invocations.get(), 3);
            let state = state.borrow();
            assert_eq!(
                state
                    .writes
                    .concat()
                    .windows(b"HTTP/1.1 200".len())
                    .filter(|window| *window == b"HTTP/1.1 200".as_slice())
                    .count(),
                3,
            );
        })
    });
}

fn connection_close_discards_admitted_lookahead() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET /one HTTP/1.1\r\nHost: example\r\n\r\nGET /two HTTP/1.1\r\nHost: example\r\n\r\n"
                    .to_vec(),
            )]);
            let invocations = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(CloseFirstHandler {
                invocations: Rc::clone(&invocations),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("connection-close response detached the lookahead stream")
                .unwrap();
            assert_eq!(invocations.get(), 2);
            let state = state.borrow();
            let output = state.writes.concat();
            assert_eq!(
                output
                    .windows(b"HTTP/1.1 200".len())
                    .filter(|window| *window == b"HTTP/1.1 200".as_slice())
                    .count(),
                1,
            );
            assert_eq!(state.close, CloseKind::ShutdownWrite);
        })
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectedCloseCase {
    ReadyFallback,
    HeaderClose,
    OtherUpgrade,
    ReadyEvent,
    LateHandler,
    BodyAndBytes,
    WriteFailure,
    CallbackFailure,
    SourceFailure,
    HandlerCancellation,
    OrdinaryError,
}

#[derive(Debug)]
enum SelectedCloseStageError {
    Timeout(&'static str),
    EarlyRun(&'static str),
    Native(HttpError),
    MissingReadyEvent,
}

impl std::fmt::Display for SelectedCloseStageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(stage) => write!(formatter, "timed out during {stage}"),
            Self::EarlyRun(stage) => {
                write!(formatter, "premature native completion during {stage}")
            }
            Self::Native(error) => write!(formatter, "native fixture operation failed: {error}"),
            Self::MissingReadyEvent => {
                formatter.write_str("later native response did not yield its ready event")
            }
        }
    }
}

#[derive(Debug)]
struct SelectedCloseMarker;

struct SelectedCloseNestedPayload(Arc<AtomicUsize>);

impl Drop for SelectedCloseNestedPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("nested selected-close payload destructor");
    }
}

struct SelectedCloseSecondaryPayload {
    drops: Arc<AtomicUsize>,
    nested_drops: Arc<AtomicUsize>,
}

impl Drop for SelectedCloseSecondaryPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(SelectedCloseNestedPayload(Arc::clone(&self.nested_drops)));
    }
}

struct SelectedCloseState {
    case: SelectedCloseCase,
    owner: ThreadId,
    calls: Cell<usize>,
    ingress_drops: RefCell<Vec<usize>>,
    handler_drops: RefCell<Vec<usize>>,
    source_drops: RefCell<Vec<usize>>,
    source_stops: RefCell<Vec<(usize, Option<HttpErrorCode>)>>,
    body_reads: Cell<usize>,
    write_entered: Cell<usize>,
    writes_completed: Cell<usize>,
    wrong_owner: Cell<bool>,
    body_gate: Rc<CancellableBaton>,
    write_gate: Rc<CancellableBaton>,
    handler_gate: Rc<CancellableBaton>,
    handle: RefCell<Option<crate::uniplex::UniplexHandle>>,
    receipts: RefCell<Vec<roxygen_core::exchange::ExchangeCompletion>>,
    stage: RefCell<Option<Result<(), SelectedCloseStageError>>>,
    run: RefCell<Option<Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
    transport: RefCell<Option<Rc<RefCell<TransportState>>>>,
    selected_bytes: Rc<ByteEventLog>,
    late_cancelled: RefCell<Vec<(usize, HttpErrorCode)>>,
    late_callback_drops: RefCell<Vec<usize>>,
    late_encoded: Cell<usize>,
    reentered: Cell<usize>,
    ownership_reentry: RefCell<Vec<(&'static str, bool)>>,
    handler_wait_drops: Cell<usize>,
    handler_wakes: RefCell<Vec<(usize, bool)>>,
    handler_wait_outcomes: RefCell<Vec<(usize, Option<WaitStatus>)>>,
    first: Arc<SelectedCloseMarker>,
    secondary_drops: Arc<AtomicUsize>,
    nested_drops: Arc<AtomicUsize>,
}

impl SelectedCloseState {
    fn new(case: SelectedCloseCase) -> Self {
        Self {
            case,
            owner: std::thread::current().id(),
            calls: Cell::new(0),
            ingress_drops: RefCell::new(Vec::new()),
            handler_drops: RefCell::new(Vec::new()),
            source_drops: RefCell::new(Vec::new()),
            source_stops: RefCell::new(Vec::new()),
            body_reads: Cell::new(0),
            write_entered: Cell::new(0),
            writes_completed: Cell::new(0),
            wrong_owner: Cell::new(false),
            body_gate: Rc::new(CancellableBaton::new()),
            write_gate: Rc::new(CancellableBaton::new()),
            handler_gate: Rc::new(CancellableBaton::new()),
            handle: RefCell::new(None),
            receipts: RefCell::new(Vec::new()),
            stage: RefCell::new(None),
            run: RefCell::new(None),
            transport: RefCell::new(None),
            selected_bytes: Rc::new(ByteEventLog::default()),
            late_cancelled: RefCell::new(Vec::new()),
            late_callback_drops: RefCell::new(Vec::new()),
            late_encoded: Cell::new(0),
            reentered: Cell::new(0),
            ownership_reentry: RefCell::new(Vec::new()),
            handler_wait_drops: Cell::new(0),
            handler_wakes: RefCell::new(Vec::new()),
            handler_wait_outcomes: RefCell::new(Vec::new()),
            first: Arc::new(SelectedCloseMarker),
            secondary_drops: Arc::new(AtomicUsize::new(0)),
            nested_drops: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn owner_action(&self) {
        if std::thread::current().id() != self.owner {
            self.wrong_owner.set(true);
        }
    }

    fn release(&self) {
        self.body_gate.signal();
        self.write_gate.signal();
        self.handler_gate.signal();
    }

    fn reenter(&self, phase: &'static str) {
        self.owner_action();
        let observed = catch_unwind(AssertUnwindSafe(|| {
            let handle = self.handle.borrow().as_ref().unwrap().clone();
            handle
                .h1_close_sealed_for_test(roxygen_core::ids::StreamId::new(2))
                .is_ok_and(|sealed| sealed)
                && handle.initiate_drain().is_ok()
        }));
        let restored = match observed {
            Ok(restored) => restored,
            Err(payload) => {
                if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    roxygen_runtime::panic::discard_panic(secondary);
                }
                false
            }
        };
        self.ownership_reentry.borrow_mut().push((phase, restored));
    }
}

thread_local! {
    static SELECTED_CLOSE_STATE: RefCell<Option<Rc<SelectedCloseState>>> = const { RefCell::new(None) };
}

struct SelectedCloseRelease(Rc<SelectedCloseState>);

struct SelectedCloseHandlerWake {
    ordinal: usize,
    token: CancellationToken,
}

impl std::task::Wake for SelectedCloseHandlerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let state = SELECTED_CLOSE_STATE.with_borrow(|state| Rc::clone(state.as_ref().unwrap()));
        let phase = self.token.is_cancellation_requested()
            && matches!(state.stage.borrow().as_ref(), Some(Ok(())))
            && state
                .transport
                .borrow()
                .as_ref()
                .is_some_and(|transport| transport.borrow().close == CloseKind::ShutdownWrite)
            && state.writes_completed.get() != 0
            && state.writes_completed.get() == state.write_entered.get();
        state.handler_wakes.borrow_mut().push((self.ordinal, phase));
        state.reenter("handler cancellation");
        match self.ordinal {
            0 => std::panic::panic_any(Arc::clone(&state.first)),
            1 => std::panic::panic_any(SelectedCloseSecondaryPayload {
                drops: Arc::clone(&state.secondary_drops),
                nested_drops: Arc::clone(&state.nested_drops),
            }),
            _ => {}
        }
    }
}

struct SelectedCloseCancellationWait {
    wait: Pin<Box<roxygen_runtime::wait::CancellableWait>>,
    _baton: CancellableBaton,
    state: Rc<SelectedCloseState>,
    ordinal: usize,
    outcome: Option<WaitStatus>,
}

impl Drop for SelectedCloseCancellationWait {
    fn drop(&mut self) {
        self.state.owner_action();
        if self.outcome.is_none() {
            match catch_unwind(AssertUnwindSafe(|| {
                self.wait
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            })) {
                Ok(Poll::Ready(outcome)) => self.outcome = Some(outcome),
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                        roxygen_runtime::panic::discard_panic(secondary);
                    }
                }
            }
        }
        self.state
            .handler_wait_outcomes
            .borrow_mut()
            .push((self.ordinal, self.outcome));
        self.state
            .handler_wait_drops
            .set(self.state.handler_wait_drops.get() + 1);
    }
}

impl Drop for SelectedCloseRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct SelectedCloseIngress {
    source: HttpSourceHolder,
    state: Rc<SelectedCloseState>,
    ordinal: usize,
}

impl Drop for SelectedCloseIngress {
    fn drop(&mut self) {
        self.state.owner_action();
        self.state.ingress_drops.borrow_mut().push(self.ordinal);
    }
}

struct SelectedCloseHandlerCapture(Rc<SelectedCloseState>, usize);

impl Drop for SelectedCloseHandlerCapture {
    fn drop(&mut self) {
        self.0.owner_action();
        self.0.handler_drops.borrow_mut().push(self.1);
    }
}

struct SelectedCloseCallback(Rc<SelectedCloseState>, usize);

impl HttpByteEventCallback for SelectedCloseCallback {
    fn on_byte_event(&self, _event: HttpByteEvent) {
        self.0.late_encoded.set(self.0.late_encoded.get() + 1);
    }

    fn on_byte_event_cancelled(&self, _event: HttpByteEvent, error: HttpError) {
        self.0.owner_action();
        self.0
            .late_cancelled
            .borrow_mut()
            .push((self.1, error.code()));
        let handle = self.0.handle.borrow().as_ref().unwrap().clone();
        let restored = handle
            .h1_close_snapshot_for_test(roxygen_core::ids::StreamId::new(2))
            .is_ok();
        if restored && handle.initiate_drain().is_ok() {
            self.0.reentered.set(self.0.reentered.get() + 1);
        }
        if self.1 == 0 {
            std::panic::panic_any(Arc::clone(&self.0.first));
        }
        std::panic::panic_any(SelectedCloseSecondaryPayload {
            drops: Arc::clone(&self.0.secondary_drops),
            nested_drops: Arc::clone(&self.0.nested_drops),
        });
    }
}

impl Drop for SelectedCloseCallback {
    fn drop(&mut self) {
        self.0.owner_action();
        self.0.late_callback_drops.borrow_mut().push(self.1);
    }
}

struct SelectedCloseResponse {
    inner: EventSource,
    state: Rc<SelectedCloseState>,
    ordinal: usize,
}

impl SelectedCloseResponse {
    fn new(state: Rc<SelectedCloseState>, ordinal: usize) -> Self {
        let streamed = ordinal == 0 && state.case == SelectedCloseCase::BodyAndBytes;
        let mut message = roxygen_core::message::HttpMessage::new();
        message.set_http_version(1, 1);
        message
            .set_status_code(if ordinal == 0 { 403 } else { 500 })
            .unwrap();
        if ordinal == 0
            && matches!(
                state.case,
                SelectedCloseCase::HeaderClose | SelectedCloseCase::OtherUpgrade
            )
        {
            if state.case == SelectedCloseCase::HeaderClose {
                message
                    .headers_mut()
                    .set(HttpHeaderCode::Connection, b"close");
            }
        } else {
            message.set_wants_keepalive(false);
        }
        message.headers_mut().set(
            HttpHeaderCode::ContentLength,
            if streamed {
                b"3".as_slice()
            } else if ordinal == 0 {
                b"0".as_slice()
            } else {
                b"4".as_slice()
            },
        );
        let mut header = HttpHeaderEvent::new(message, ordinal == 0 && !streamed).unwrap();
        if ordinal == 0 {
            header
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                    &state.selected_bytes,
                ));
        } else if state.case == SelectedCloseCase::CallbackFailure {
            let callbacks = [
                Rc::new(SelectedCloseCallback(Rc::clone(&state), 0)),
                Rc::new(SelectedCloseCallback(Rc::clone(&state), 1)),
            ];
            for callback in &callbacks {
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::KernelWrite.bit(),
                        callback,
                    ));
            }
            let encoded = Rc::clone(&state);
            header.set_egress_headers_callback(move |_| {
                encoded.late_encoded.set(encoded.late_encoded.get() + 1);
                drop(callbacks);
            });
        }
        let mut body = VecDeque::new();
        if streamed {
            let mut padding = HttpBodyEvent::padding(0, false);
            padding
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit() | ByteEventType::KernelWrite.bit(),
                    &state.selected_bytes,
                ));
            body.push_back(padding);
            let mut data = HttpBodyEvent::body(queue(b"abc"), true);
            data.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit()
                        | ByteEventType::KernelWrite.bit()
                        | ByteEventType::NicTx.bit()
                        | ByteEventType::CumulativeAck.bit(),
                    &state.selected_bytes,
                ));
            body.push_back(data);
        } else if ordinal != 0 {
            body.push_back(HttpBodyEvent::body(queue(b"late"), true));
        }
        Self {
            inner: EventSource {
                header: Some(header),
                body,
            },
            state,
            ordinal,
        }
    }
}

impl HttpSource for SelectedCloseResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        self.state.owner_action();
        self.inner.read_header_event()
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        self.state.owner_action();
        self.state.body_reads.set(self.state.body_reads.get() + 1);
        let gate = Rc::clone(&self.state.body_gate);
        let future = self.inner.read_body_event(maximum);
        Box::pin(async move {
            let _ = gate.wait(CancellationToken::never()).await;
            future.await
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.state.owner_action();
        if self.ordinal == 1 && self.state.case == SelectedCloseCase::SourceFailure {
            self.state.reenter("source stop");
        }
        self.state
            .source_stops
            .borrow_mut()
            .push((self.ordinal, error));
        self.inner.stop_reading(error);
    }
}

impl Drop for SelectedCloseResponse {
    fn drop(&mut self) {
        self.state.owner_action();
        self.state.source_drops.borrow_mut().push(self.ordinal);
        if self.ordinal == 1 && self.state.case == SelectedCloseCase::SourceFailure {
            self.state.reenter("source drop");
            std::panic::panic_any(Arc::clone(&self.state.first));
        }
    }
}

struct SelectedCloseHandler(Rc<SelectedCloseState>);

impl HttpHandler for SelectedCloseHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let ordinal = self.0.calls.get();
        self.0.calls.set(ordinal + 1);
        self.0
            .receipts
            .borrow_mut()
            .push(context.exchange_completion().unwrap().clone());
        let mut ingress = SelectedCloseIngress {
            source,
            state: Rc::clone(&self.0),
            ordinal,
        };
        let capture = SelectedCloseHandlerCapture(Rc::clone(&self.0), ordinal);
        let state = Rc::clone(&self.0);
        Box::pin(async move {
            let _capture = capture;
            if state.case == SelectedCloseCase::OrdinaryError {
                ingress.source.read_header_event().await?;
                return Err(HttpError::with_message(
                    HttpErrorCode::INTERNAL_ERROR,
                    b"ordinary selected-close control",
                ));
            }
            if ordinal == 0 {
                ingress.source.read_header_event().await?;
                return Ok(HttpSourceHolder::new(Box::new(SelectedCloseResponse::new(
                    state, ordinal,
                ))));
            }
            if matches!(
                state.case,
                SelectedCloseCase::LateHandler
                    | SelectedCloseCase::BodyAndBytes
                    | SelectedCloseCase::HandlerCancellation
            ) {
                let response = SelectedCloseResponse::new(Rc::clone(&state), ordinal);
                drop(ingress);
                let _ = state.handler_gate.wait(CancellationToken::never()).await;
                return Ok(HttpSourceHolder::new(Box::new(response)));
            }
            if matches!(
                state.case,
                SelectedCloseCase::CallbackFailure | SelectedCloseCase::SourceFailure
            ) {
                return Ok(HttpSourceHolder::new(Box::new(SelectedCloseResponse::new(
                    state, ordinal,
                ))));
            }
            Err(HttpError::with_message(
                HttpErrorCode::INTERNAL_ERROR,
                b"application server exchange cancelled",
            ))
        })
    }
}

struct SelectedCloseTransport {
    inner: Rc<ScriptedTransport>,
    state: Rc<SelectedCloseState>,
}

impl UniplexTransport for SelectedCloseTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation_size: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.inner
            .read(context, maximum, allocation_size, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.state.owner_action();
        self.state
            .write_entered
            .set(self.state.write_entered.get() + 1);
        let state = Rc::clone(&self.state);
        let inner = Rc::clone(&self.inner);
        let context = context.clone();
        Box::pin(async move {
            let status = state.write_gate.wait(cancellation.clone()).await;
            if status == roxygen_runtime::wait::WaitStatus::Cancelled {
                return Err(TransportWriteError::new(
                    HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"selected-close fixture write cancelled",
                    ),
                    0,
                ));
            }
            if state.case == SelectedCloseCase::WriteFailure {
                return Err(TransportWriteError::new(
                    HttpError::with_message(
                        HttpErrorCode::TRANSPORT_WRITE_ERROR,
                        b"selected-close write failure",
                    ),
                    0,
                ));
            }
            let written = inner
                .write(&context, data, flags, timeout, cancellation)
                .await;
            if written.is_ok() {
                state.writes_completed.set(state.writes_completed.get() + 1);
            }
            written
        })
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }

    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }

    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }

    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }

    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

async fn selected_close_wait<
    F: Future<Output = Result<crate::uniplex::UniplexRunSummary, HttpError>>,
>(
    run: &mut Pin<Box<F>>,
    state: &SelectedCloseState,
    label: &'static str,
    ready: impl Fn() -> bool,
) -> Result<(), SelectedCloseStageError> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready() {
            tokio::select! {
                result = run.as_mut() => {
                    *state.run.borrow_mut() = Some(result);
                    return Err(SelectedCloseStageError::EarlyRun(label));
                }
                _ = tokio::task::yield_now() => {},
            }
        }
        Ok(())
    })
    .await
    .unwrap_or(Err(SelectedCloseStageError::Timeout(label)))
}

async fn selected_close_operation(context: WorkerContext, state: Rc<SelectedCloseState>) {
    let _release = SelectedCloseRelease(Rc::clone(&state));
    let request = if state.case == SelectedCloseCase::OtherUpgrade {
        b"GET /one HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: future-protocol\r\n\r\nGET /two HTTP/1.1\r\nHost: example\r\n\r\n".as_slice()
    } else if state.case == SelectedCloseCase::OrdinaryError {
        b"GET /one HTTP/1.1\r\nHost: example\r\n\r\n".as_slice()
    } else {
        b"GET /one HTTP/1.1\r\nHost: example\r\n\r\nGET /two HTTP/1.1\r\nHost: example\r\n\r\n"
            .as_slice()
    };
    let (inner, transport_state) = ScriptedTransport::with_capability(
        [ReadAction::Data(request.to_vec())],
        ByteEventCapability::TransportEvents,
    );
    *state.transport.borrow_mut() = Some(Rc::clone(&transport_state));
    let transport: Rc<dyn UniplexTransport> = Rc::new(SelectedCloseTransport {
        inner,
        state: Rc::clone(&state),
    });
    let handler: HttpHandlerRef = Rc::new(SelectedCloseHandler(Rc::clone(&state)));
    let (session, handle) = UniplexSession::new(
        context.clone(),
        TransportDirection::Downstream,
        transport,
        Some(handler),
        UniplexConfig::default(),
    )
    .unwrap();
    *state.handle.borrow_mut() = Some(handle.clone());
    let mut run = Box::pin(session.run());
    let mut handler_waits = Vec::new();
    let stage = async {
        if state.case == SelectedCloseCase::OrdinaryError {
            state.release();
            return Ok(());
        }
        selected_close_wait(&mut run, &state, "selected write entered", || {
            state.write_entered.get() == 1
        })
        .await?;
        if state.case == SelectedCloseCase::BodyAndBytes {
            state.write_gate.signal();
            selected_close_wait(&mut run, &state, "selected body read held", || {
                state.body_reads.get() == 1
            })
            .await?;
            if transport_state.borrow().close != CloseKind::Open
                || state.source_drops.borrow().contains(&0)
            {
                return Err(SelectedCloseStageError::EarlyRun(
                    "selected body must remain owned",
                ));
            }
            state.body_gate.signal();
            selected_close_wait(&mut run, &state, "selected body bytes drained", || {
                state.body_reads.get() == 2
                    && state.writes_completed.get() == state.write_entered.get()
                    && transport_state.borrow().writes.concat().ends_with(b"abc")
                    && transport_state.borrow().close == CloseKind::ShutdownWrite
            })
            .await?;
            if transport_state.borrow().close != CloseKind::ShutdownWrite
                || state.handler_drops.borrow().contains(&1)
                || state.source_drops.borrow().contains(&1)
            {
                return Err(SelectedCloseStageError::EarlyRun(
                    "later ownership must outlive selected byte events",
                ));
            }
            let later = state.receipts.borrow().get(1).cloned().ok_or(
                SelectedCloseStageError::EarlyRun("actual later exchange receipt"),
            )?;
            if later
                .wait()
                .as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_ready()
            {
                return Err(SelectedCloseStageError::EarlyRun(
                    "later exchange must remain pending before TX/ACK",
                ));
            }
            let offset = transport_state
                .borrow()
                .writes
                .iter()
                .map(Vec::len)
                .sum::<usize>() as u64;
            handle
                .deliver_socket_byte_event(
                    roxygen_transport::byte_event::SocketByteEventType::Tx,
                    offset,
                )
                .map_err(SelectedCloseStageError::Native)?;
            handle
                .deliver_socket_byte_event(
                    roxygen_transport::byte_event::SocketByteEventType::Ack,
                    offset,
                )
                .map_err(SelectedCloseStageError::Native)?;
            return Ok(());
        }
        let later = roxygen_core::ids::StreamId::new(2);
        selected_close_wait(
            &mut run,
            &state,
            "lookahead ingress metadata retired",
            || {
                state.calls.get() == 2
                    && handle.h1_close_snapshot_for_test(later).is_ok_and(
                        |(metadata, live, blocked, queued)| {
                            !metadata
                                && live
                                && blocked
                                && (matches!(
                                    state.case,
                                    SelectedCloseCase::LateHandler
                                        | SelectedCloseCase::HandlerCancellation
                                ) || queued != 0)
                        },
                    )
            },
        )
        .await?;
        if state.case == SelectedCloseCase::HandlerCancellation {
            let token = handle
                .h1_handler_cancellation_for_test(later)
                .map_err(SelectedCloseStageError::Native)?
                .ok_or(SelectedCloseStageError::MissingReadyEvent)?;
            for ordinal in 0..3 {
                let baton = CancellableBaton::new();
                let mut probe = SelectedCloseCancellationWait {
                    wait: Box::pin(baton.wait(token.clone())),
                    _baton: baton,
                    state: Rc::clone(&state),
                    ordinal,
                    outcome: None,
                };
                let waker = std::task::Waker::from(Arc::new(SelectedCloseHandlerWake {
                    ordinal,
                    token: token.clone(),
                }));
                if let Poll::Ready(outcome) = probe
                    .wait
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(&waker))
                {
                    probe.outcome = Some(outcome);
                    return Err(SelectedCloseStageError::EarlyRun(
                        "actual handler cancellation registration",
                    ));
                }
                handler_waits.push(probe);
            }
            if !state.handler_wakes.borrow().is_empty() {
                return Err(SelectedCloseStageError::EarlyRun(
                    "handler cancellation preceded selected retirement",
                ));
            }
        }
        if matches!(
            state.case,
            SelectedCloseCase::ReadyEvent | SelectedCloseCase::CallbackFailure
        ) {
            if !handle
                .promote_h1_close_event_for_test(later)
                .await
                .map_err(SelectedCloseStageError::Native)?
            {
                return Err(SelectedCloseStageError::MissingReadyEvent);
            }
        }
        state.write_gate.signal();
        state.body_gate.signal();
        Ok(())
    }
    .await;
    let failed = stage.is_err();
    *state.stage.borrow_mut() = Some(stage);
    if failed {
        state.release();
        let _ = handle.fail_connection_while_core_detached_for_test(HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"selected-close fixture cleanup",
        ));
    }
    if state.run.borrow().is_none() {
        match tokio::time::timeout(Duration::from_secs(3), run.as_mut()).await {
            Ok(result) => *state.run.borrow_mut() = Some(result),
            Err(_) => {
                *state.stage.borrow_mut() = Some(Err(SelectedCloseStageError::Timeout(
                    "actual native run retirement",
                )));
            }
        }
    }
    drop(run);
    drop(handler_waits);
    state.release();
    context.handle().begin_shutdown();
}

async fn selected_close_catch<F: Future>(
    future: F,
) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut future = Box::pin(future);
    let outcome =
        poll_fn(
            |context| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                Ok(poll) => poll.map(Ok),
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await;
    let dropped = catch_unwind(AssertUnwindSafe(|| drop(future)));
    match outcome {
        Err(first) => {
            if let Err(secondary) = dropped {
                if let Err(nested) = catch_unwind(AssertUnwindSafe(|| drop(secondary))) {
                    roxygen_runtime::panic::discard_panic(nested);
                }
            }
            Err(first)
        }
        Ok(output) => dropped.map(|()| output),
    }
}

fn selected_close_case(case: SelectedCloseCase) {
    run_local(async move {
        let state = Rc::new(SelectedCloseState::new(case));
        let _release = SelectedCloseRelease(Rc::clone(&state));
        SELECTED_CLOSE_STATE.set(Some(Rc::clone(&state)));
        let (worker, driver) = WorkerDriver::new(8).unwrap();
        let call = worker
            .call_local(|context| {
                let state =
                    SELECTED_CLOSE_STATE.with_borrow(|state| Rc::clone(state.as_ref().unwrap()));
                selected_close_operation(context, state)
            })
            .unwrap();
        let (driver_result, call_result) = tokio::join!(selected_close_catch(driver), call);
        SELECTED_CLOSE_STATE.take();
        let driver_panic = match driver_result {
            Ok(()) => Ok(None),
            Err(payload) => match payload.downcast::<Arc<SelectedCloseMarker>>() {
                Ok(original) => Ok(Some(*original)),
                Err(payload) => {
                    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                        roxygen_runtime::panic::discard_panic(secondary);
                    }
                    Err(())
                }
            },
        };
        assert_eq!(worker.state(), WorkerState::Closed);
        assert!(!state.wrong_owner.get());
        assert!(
            matches!(state.stage.borrow().as_ref(), Some(Ok(()))),
            "phase outcome: {:?}",
            state.stage.borrow()
        );
        let panic_case = matches!(
            case,
            SelectedCloseCase::CallbackFailure
                | SelectedCloseCase::SourceFailure
                | SelectedCloseCase::HandlerCancellation
        );
        if panic_case {
            let original = driver_panic
                .expect("native driver preserved the selected-close payload type")
                .expect("actual native driver joins before returning its first panic");
            assert!(Arc::ptr_eq(&original, &state.first));
            assert_eq!(
                call_result,
                Err(roxygen_runtime::worker::WorkerCallError::Cancelled)
            );
        } else {
            assert!(
                matches!(driver_panic, Ok(None)),
                "healthy native owner must not panic"
            );
            call_result.unwrap();
            let run = state.run.borrow();
            let result = run.as_ref().expect("actual native run result");
            if case == SelectedCloseCase::WriteFailure {
                let error = result.as_ref().unwrap_err();
                assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
                assert_eq!(error.message(), b"selected-close write failure");
            } else {
                result.as_ref().unwrap();
            }
        }
        let requests = if case == SelectedCloseCase::OrdinaryError {
            1
        } else {
            2
        };
        assert_eq!(state.calls.get(), requests);
        let mut ingress = state.ingress_drops.borrow().clone();
        ingress.sort_unstable();
        assert_eq!(ingress, (0..requests).collect::<Vec<_>>());
        let mut handlers = state.handler_drops.borrow().clone();
        handlers.sort_unstable();
        assert_eq!(handlers, (0..requests).collect::<Vec<_>>());
        assert_eq!(state.receipts.borrow().len(), requests);
        for (ordinal, receipt) in state.receipts.borrow().iter().enumerate() {
            let expected = if panic_case && ordinal == 1 {
                Err(roxygen_core::exchange::ExchangeCompletionError::CleanupUnconfirmed)
            } else {
                Ok(())
            };
            assert_eq!(
                receipt
                    .wait()
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop())),
                Poll::Ready(expected)
            );
        }
        let transport = state.transport.borrow().as_ref().unwrap().clone();
        let output = transport.borrow().writes.concat();
        if case == SelectedCloseCase::WriteFailure {
            assert!(output.is_empty());
            assert_eq!(transport.borrow().close, CloseKind::Reset);
            assert_eq!(
                state.selected_bytes.cancellation_codes.borrow().as_slice(),
                &[HttpErrorCode::TRANSPORT_WRITE_ERROR]
            );
        } else {
            let end = output
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .unwrap()
                + 4;
            let expected_status = if case == SelectedCloseCase::OrdinaryError {
                b"HTTP/1.1 500".as_slice()
            } else {
                b"HTTP/1.1 403".as_slice()
            };
            assert!(
                output.starts_with(expected_status),
                "wire: {:?}",
                String::from_utf8_lossy(&output)
            );
            let expected_body = match case {
                SelectedCloseCase::OrdinaryError => b"ordinary selected-close control".as_slice(),
                SelectedCloseCase::BodyAndBytes => b"abc".as_slice(),
                _ => b"".as_slice(),
            };
            assert_eq!(&output[end..], expected_body);
            assert_eq!(
                transport.borrow().close,
                if panic_case {
                    CloseKind::Reset
                } else {
                    CloseKind::ShutdownWrite
                }
            );
            assert_eq!(state.selected_bytes.cancelled.get(), 0);
        }
        if case != SelectedCloseCase::OrdinaryError {
            let mut drops = state.source_drops.borrow().clone();
            drops.sort_unstable();
            let expected = if matches!(
                case,
                SelectedCloseCase::LateHandler
                    | SelectedCloseCase::BodyAndBytes
                    | SelectedCloseCase::CallbackFailure
                    | SelectedCloseCase::SourceFailure
                    | SelectedCloseCase::HandlerCancellation
            ) {
                vec![0, 1]
            } else {
                vec![0]
            };
            assert_eq!(drops, expected);
        }
        if case == SelectedCloseCase::BodyAndBytes {
            assert_eq!(state.body_reads.get(), 2);
            assert_eq!(state.write_entered.get(), state.writes_completed.get());
            let events = state.selected_bytes.fired.borrow();
            assert_eq!(events.len(), 8);
            let end = output.len() as u64;
            let tail = &events[events.len() - 4..];
            assert_eq!(
                tail.iter()
                    .map(|event| event.event_type)
                    .collect::<Vec<_>>(),
                vec![
                    ByteEventType::TransportWrite,
                    ByteEventType::KernelWrite,
                    ByteEventType::NicTx,
                    ByteEventType::CumulativeAck
                ]
            );
            assert!(tail.iter().all(|event| event.transport_offset == end));
        }
        if case == SelectedCloseCase::CallbackFailure {
            assert_eq!(
                state.late_cancelled.borrow().as_slice(),
                &[
                    (0, HttpErrorCode::CORO_CANCELLED),
                    (1, HttpErrorCode::CORO_CANCELLED)
                ]
            );
            assert_eq!(state.late_callback_drops.borrow().as_slice(), &[0, 1]);
            assert_eq!(state.reentered.get(), 2);
            assert_eq!(state.late_encoded.get(), 0);
            assert_eq!(state.secondary_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.nested_drops.load(Ordering::SeqCst), 1);
        }
        if case == SelectedCloseCase::SourceFailure {
            assert_eq!(
                state.ownership_reentry.borrow().as_slice(),
                &[("source stop", true), ("source drop", true)]
            );
        }
        if case == SelectedCloseCase::HandlerCancellation {
            assert_eq!(
                state.ownership_reentry.borrow().as_slice(),
                &[("handler cancellation", true); 3]
            );
            assert_eq!(
                state.handler_wakes.borrow().as_slice(),
                &[(0, true), (1, true), (2, true)]
            );
            assert_eq!(state.handler_wait_drops.get(), 3);
            let mut outcomes = state.handler_wait_outcomes.borrow().clone();
            outcomes.sort_by_key(|(ordinal, _)| *ordinal);
            assert_eq!(
                outcomes,
                (0..3)
                    .map(|ordinal| (ordinal, Some(WaitStatus::Cancelled)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(state.secondary_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.nested_drops.load(Ordering::SeqCst), 1);
        }
        assert!(state.handle.borrow().as_ref().unwrap().is_finished());
    });
}

fn request_connection_close_stops_buffered_lookahead() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET /one HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\nGET /two HTTP/1.1\r\nHost: example\r\n\r\n"
                    .to_vec(),
            )]);
            let invocations = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(CloseFirstHandler {
                invocations: Rc::clone(&invocations),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("request-side connection close stopped buffered lookahead")
                .unwrap();
            assert_eq!(invocations.get(), 1);
            let state = state.borrow();
            assert_eq!(
                state
                    .writes
                    .concat()
                    .windows(b"HTTP/1.1 200".len())
                    .filter(|wire| *wire == b"HTTP/1.1 200")
                    .count(),
                1,
            );
            assert_eq!(state.close, CloseKind::ShutdownWrite);
        })
    });
}

fn early_post() -> Outcome {
    downstream(
        vec![
            ReadAction::Data(b"POST / HTTP/1.1\r\nContent-Length: 2000\r\n\r\n".to_vec()),
            ReadAction::YieldData(vec![b'x'; 2_000]),
            ReadAction::Eof,
        ],
        HandlerMode::Early(401),
    )
}

fn incomplete_request(action: ReadAction) -> Outcome {
    downstream(
        vec![
            ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 10\r\n\r\nabc".to_vec(),
            ),
            action,
        ],
        HandlerMode::Consume(200, 0),
    )
}

fn malformed_content_length() -> Outcome {
    downstream(
        vec![
            ReadAction::Data(
                b"POST / HTTP/1.1\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n"
                    .to_vec(),
            ),
            ReadAction::Eof,
        ],
        HandlerMode::Consume(200, 0),
    )
}

fn malformed_chunk_response() -> (Outcome, usize) {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec()),
                ReadAction::Data(b"z\r\n".to_vec()),
                ReadAction::Data(b"GET /must-not-be-read HTTP/1.1\r\n\r\n".to_vec()),
            ]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            let state = state.borrow();
            (
                Outcome {
                    output: state.writes.concat(),
                    close: state.close,
                    bodies: bodies.get(),
                    error: result.err().map(|error| error.code()),
                },
                state.reads.len(),
            )
        })
    })
}

fn backpressured_post(limit: u32, chunks: [usize; 3]) -> Outcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let total = chunks.iter().sum::<usize>();
            let headers =
                format!("POST / HTTP/1.1\r\nHost: example\r\nContent-Length: {total}\r\n\r\n")
                    .into_bytes();
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(headers),
                ReadAction::Data(vec![b'x'; chunks[0]]),
                ReadAction::Data(vec![b'x'; chunks[1]]),
                ReadAction::Data(vec![b'x'; chunks[2]]),
                ReadAction::Eof,
            ]);
            let bodies = Rc::new(Cell::new(0));
            let release = Rc::new(CancellableBaton::new());
            let handler: HttpHandlerRef = Rc::new(BlockedBodyHandler {
                release: Rc::clone(&release),
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                ingress_buffer_limit: limit,
                read_timeout: Duration::ZERO,
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().reads.len() > 2 {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before ingress paused"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("two chunks crossed the ingress pause threshold");
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(
                state.borrow().reads.len(),
                2,
                "paused transport must not consume the third body chunk",
            );
            release.signal();
            let result = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("ingress resumed after the handler consumed buffered data");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn backpressure_timeout() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let headers =
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 72000\r\n\r\n".to_vec();
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(headers),
                ReadAction::Data(vec![b'x'; 35_000]),
                ReadAction::Data(vec![b'x'; 35_000]),
                ReadAction::Data(vec![b'x'; 2_000]),
                ReadAction::Eof,
            ]);
            let release = Rc::new(CancellableBaton::new());
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(BlockedBodyHandler {
                release,
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                read_timeout: Duration::from_millis(20),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("paused ingress timeout terminates the session");
            let state = state.borrow();
            assert_eq!(state.reads.len(), 2);
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn ingress_hard_limit_is_enforced() -> (Outcome, Vec<u8>) {
    run_on_worker(|context| {
        Box::pin(async move {
            let headers =
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 800\r\n\r\n".to_vec();
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(headers),
                ReadAction::Data(vec![b'x'; 400]),
                ReadAction::Data(vec![b'x'; 400]),
            ]);
            let handler: HttpHandlerRef = Rc::new(BlockedBodyHandler {
                release: Rc::new(CancellableBaton::new()),
                bodies: Rc::new(Cell::new(0)),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                buffer_limit: 700,
                ingress_buffer_limit: 500,
                maximum_read_size: 700,
                read_allocation_size: 700,
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("hard ingress limit terminates the overflowing stream");
            let error = result.expect_err("the crossing chunk fails native Run");
            let state = state.borrow();
            (
                Outcome {
                    output: state.writes.concat(),
                    close: state.close,
                    bodies: 0,
                    error: Some(error.code()),
                },
                error.message().to_vec(),
            )
        })
    })
}

fn custom_timeout_response(status: u16) -> Outcome {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec(),
            )]);
            let handler: HttpHandlerRef = Rc::new(TimeoutResponseHandler {
                status,
                body: b"custom response generated",
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(10),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("timed-out request produced its validated response");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn partial_headers_stream_timeout() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: example".to_vec(),
            )]);
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::new(Cell::new(0)),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(10),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("partial request headers timed out at the stream source");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn incomplete_body_stream_timeout() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 10\r\n\r\n".to_vec(),
            )]);
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::new(Cell::new(0)),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(10),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("incomplete request body timed out at the stream source");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn connection_timeout_does_not_abort_active_stream() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 3\r\n\r\n".to_vec(),
                ),
                ReadAction::Error(HttpErrorCode::READ_TIMEOUT),
                ReadAction::YieldData(b"abc".to_vec()),
                ReadAction::Eof,
            ]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 100),
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_secs(1),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("active request survived the connection read timeout");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn continue_then_body_stream_timeout() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n"
                    .to_vec(),
            )]);
            let handler: HttpHandlerRef = Rc::new(ContinueThenTimeoutHandler);
            let erased: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                stream_read_timeout: Duration::from_millis(10),
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("body source timeout followed informational response");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: 0,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn websocket_round_trip() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut request = b"GET / HTTP/1.1\r\nHost: foo.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
                .to_vec();
            request.extend_from_slice(&[b'x'; 100]);
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(request), ReadAction::Eof]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(WebSocketHandler {
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("websocket tunnel reached bidirectional EOM");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn prompt_upgrade_eom_preserves_later_raw_ingress() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_raw = Arc::new(tokio::sync::Notify::new());
            let request = b"GET / HTTP/1.1\r\nHost: foo.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(request.to_vec()),
                ReadAction::WaitData(Arc::clone(&release_raw), vec![b'x'; 100]),
                ReadAction::Eof,
            ]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(PromptUpgradeHandler {
                bodies: Rc::clone(&bodies),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let mut run = session.run();
            tokio::select! {
                biased;
                _ = &mut run => panic!("accepted upgrade completed before delayed raw ingress"),
                _ = async {
                    while state.borrow().close != CloseKind::ShutdownWrite {
                        tokio::task::yield_now().await;
                    }
                } => {},
            }
            release_raw.notify_one();
            tokio::time::timeout(Duration::from_secs(1), run)
                .await
                .expect("accepted upgrade consumed raw ingress after response EOM")
                .unwrap();
            assert_eq!(bodies.get(), 100);
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
        })
    });
}

fn downstream_connect_round_trip() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut request = b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n".to_vec();
            request.extend_from_slice(&[b'x'; 100]);
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(request), ReadAction::Eof]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(ConnectHandler {
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("CONNECT tunnel reached bidirectional EOM");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

/// A CONNECT tunnel, with `abort_exchanges_on_eof`, whose peer sends EOF only after the
/// `200` head is written.
fn downstream_connect_eof_after_response() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut request = b"CONNECT example:443 HTTP/1.1\r\nHost: example:443\r\n\r\n".to_vec();
            request.extend_from_slice(&[b'x'; 100]);
            let eof = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(request),
                ReadAction::WaitEof(Arc::clone(&eof)),
            ]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(ConnectHandler {
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig {
                    abort_exchanges_on_eof: true,
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            let mut run = session.run();
            tokio::select! {
                biased;
                _ = &mut run => panic!("CONNECT tunnel completed before the peer's EOF"),
                _ = async {
                    while !state.borrow().writes.concat().starts_with(b"HTTP/1.1 200") {
                        tokio::task::yield_now().await;
                    }
                } => {},
            }
            eof.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(1), run)
                .await
                .expect("CONNECT tunnel reached bidirectional EOM");
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn rejected_upgrade_unpauses_before_close() -> (Outcome, usize, bool) {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET /upgrade HTTP/1.1\r\nHost: example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\nGET /later HTTP/1.1\r\nHost: example\r\n\r\n"
                    .to_vec(),
            )]);
            let invocations = Rc::new(Cell::new(0));
            let eom_seen = Rc::new(Cell::new(false));
            let handler: HttpHandlerRef = Rc::new(RejectUpgradeHandler {
                invocations: Rc::clone(&invocations),
                eom_seen: Rc::clone(&eom_seen),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("rejected upgrade closed without parsing its buffered lookahead");
            let state = state.borrow();
            (
                Outcome {
                    output: state.writes.concat(),
                    close: state.close,
                    bodies: 0,
                    error: result.err().map(|error| error.code()),
                },
                invocations.get(),
                eom_seen.get(),
            )
        })
    })
}

fn upstream_upgrade_round_trip() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut response = b"HTTP/1.1 200 Connection Established\r\n\r\n".to_vec();
            response.extend_from_slice(&[b'y'; 100]);
            let (transport, state) =
                ScriptedTransport::new([ReadAction::YieldData(response), ReadAction::Eof]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"example:443", false).unwrap();
            message.set_method(HttpMethod::Connect).unwrap();
            message.set_http_version(1, 1);
            let request = HttpSourceHolder::new(Box::new(HttpFixedSource::new(message, None)));
            let mut source = handle
                .send_request(request, CancellationToken::never())
                .await
                .unwrap()
                .source;
            let consume = async move {
                let headers = source.read_header_event().await.unwrap();
                assert!(headers.headers().is_upgraded());
                assert!(!headers.eom());
                let mut total = 0;
                loop {
                    let event = source.read_body_event(u32::MAX).await.unwrap();
                    total += event.as_body().map_or(0, BufQueue::chain_length);
                    if event.eom() {
                        break;
                    }
                }
                total
            };
            let (result, bodies) = tokio::join!(session.run(), consume);
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies,
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn write_timeout() -> Outcome {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            state.borrow_mut().write_error = Some(HttpErrorCode::WRITE_TIMEOUT);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 100),
                bodies: Rc::clone(&bodies),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            let state = state.borrow();
            Outcome {
                output: state.writes.concat(),
                close: state.close,
                bodies: bodies.get(),
                error: result.err().map(|error| error.code()),
            }
        })
    })
}

fn response_header_error() -> Outcome {
    simple_get(HandlerMode::ErrorSource)
}

fn assert_reset(outcome: Outcome) {
    assert_eq!(outcome.close, CloseKind::Reset);
    assert!(outcome.error.is_some());
}

fn branch_response() -> (Outcome, usize, Vec<HttpByteEvent>) {
    run_on_worker(|context| {
        Box::pin(async move {
            let mut trailers = HttpHeaders::new();
            trailers.set(b"x-trailer".as_slice(), b"done".as_slice());
            let push_drops = Rc::new(Cell::new(0));
            let mut promise = roxygen_core::message::HttpMessage::new();
            promise.set_method(HttpMethod::Get).unwrap();
            promise.set_url(b"/push", false).unwrap();
            promise.set_http_version(1, 1);
            let callback = Rc::new(ByteEventLog::default());
            let mut push = HttpBodyEvent::push_promise(
                promise,
                Some(Box::new(PushDropSource {
                    drops: Rc::clone(&push_drops),
                })),
                false,
            );
            push.byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit(),
                    &callback,
                ));
            let events = VecDeque::from([
                HttpBodyEvent::padding(7, false),
                HttpBodyEvent::upgrade(false),
                push,
                HttpBodyEvent::body(queue(b"abc"), false),
                HttpBodyEvent::trailers(trailers),
            ]);
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let handler: HttpHandlerRef = Rc::new(EventHandler {
                events: RefCell::new(Some(events)),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let result = session.run().await;
            let state = state.borrow();
            (
                Outcome {
                    output: state.writes.concat(),
                    close: state.close,
                    bodies: 0,
                    error: result.err().map(|error| error.code()),
                },
                push_drops.get(),
                std::mem::take(&mut *callback.fired.borrow_mut()),
            )
        })
    })
}

fn registered_response() -> (Vec<HttpByteEvent>, Vec<ByteEventWriteFlags>) {
    registered_response_with(UniplexConfig::default(), 2).0
}

/// `RegisteredHandler`'s response once the session has made `writes` writes:
/// the byte events fired, the flags of each write, and the writes.
#[expect(clippy::type_complexity, reason = "a test result tuple")]
fn registered_response_with(
    config: UniplexConfig,
    writes: usize,
) -> ((Vec<HttpByteEvent>, Vec<ByteEventWriteFlags>), Vec<Vec<u8>>) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::with_capability(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                    ReadAction::Eof,
                ],
                ByteEventCapability::TransportEvents,
            );
            let callback = Rc::new(ByteEventLog::default());
            let handler: HttpHandlerRef = Rc::new(RegisteredHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                config,
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().writes.len() < writes {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before registered writes"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("registered response writes completed");
            assert!(
                tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                    .await
                    .is_err(),
                "normal session shutdown did not wait for TX/ACK references",
            );
            assert_eq!(state.borrow().close, CloseKind::ShutdownWrite);
            let offset = state.borrow().writes.iter().map(Vec::len).sum::<usize>() as u64;
            handle
                .deliver_socket_byte_event(
                    roxygen_transport::byte_event::SocketByteEventType::Tx,
                    offset,
                )
                .unwrap();
            handle
                .deliver_socket_byte_event(
                    roxygen_transport::byte_event::SocketByteEventType::Ack,
                    offset,
                )
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("session completed after TX/ACK delivery")
                .unwrap();
            assert_eq!(callback.cancelled.get(), 0);
            let events = std::mem::take(&mut *callback.fired.borrow_mut());
            let flags = state.borrow().write_flags.clone();
            let writes = state.borrow().writes.clone();
            ((events, flags), writes)
        })
    })
}

fn native_byte_event_cancellation_is_kind_and_boundary_scoped() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::with_capability(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                    ReadAction::Eof,
                ],
                ByteEventCapability::TransportEvents,
            );
            let callback = Rc::new(ByteEventLog::default());
            let handler: HttpHandlerRef = Rc::new(RegisteredHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().registrations.len() < 4 {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before native registrations"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("two TX/ACK boundaries reached the capable transport");
            let registrations = std::mem::take(&mut state.borrow_mut().registrations);
            assert_eq!(registrations.len(), 4);
            let mut registrations = registrations.into_iter();
            let mut first_tx = registrations.next().unwrap();
            let mut first_ack = registrations.next().unwrap();
            let mut second_tx = registrations.next().unwrap();
            let mut second_ack = registrations.next().unwrap();
            let error = HttpError::with_message(
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                b"first native TX timestamp failed",
            );
            first_tx.cancel(&error, Some(&HttpByteEvent::new(ByteEventType::NicTx)));
            assert!(first_ack.fire(HttpByteEvent::new(ByteEventType::CumulativeAck)));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                    .await
                    .is_err(),
                "a cancelled first TX did not resolve later boundaries",
            );
            assert!(second_tx.fire(HttpByteEvent::new(ByteEventType::NicTx)));
            assert!(second_ack.fire(HttpByteEvent::new(ByteEventType::CumulativeAck)));
            tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("sibling and later native events completed independently")
                .unwrap();
            assert_eq!(
                callback.cancellation_codes.borrow().as_slice(),
                &[error.code()]
            );
            let fired = callback.fired.borrow();
            assert!(fired.iter().any(|event| {
                event.event_type == ByteEventType::CumulativeAck && event.transport_offset != 0
            }));
            assert!(fired.iter().any(|event| {
                event.event_type == ByteEventType::NicTx && event.transport_offset != 0
            }));
        })
    });
}

fn panicking_native_callback_releases_bridge_before_reentry() {
    run_on_worker(|context| {
        Box::pin(async move {
            let release_second = Rc::new(CancellableBaton::new());
            let first = Rc::new(PanickingByteEventCallback);
            let second = Rc::new(ByteEventLog::default());
            let (transport, state) = ScriptedTransport::with_capability(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                    ReadAction::Eof,
                ],
                ByteEventCapability::TransportEvents,
            );
            let handler: HttpHandlerRef = Rc::new(StagedTxHandler {
                release_second: Rc::clone(&release_second),
                first,
                second: Rc::clone(&second),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().registrations.is_empty() {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before first TX bridge"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("first TX bridge reached the transport");
            let mut first_registration = state.borrow_mut().registrations.remove(0);
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    first_registration.fire(HttpByteEvent::new(ByteEventType::NicTx))
                }))
                .is_err()
            );
            release_second.signal();
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().registrations.is_empty()
                    || handle.transport_event_bridge_count_for_test().unwrap() != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the terminal first bridge was reaped before the second was admitted");
            let mut second_registration = state.borrow_mut().registrations.remove(0);
            assert!(second_registration.fire(HttpByteEvent::new(ByteEventType::NicTx)));
            tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("second bridge completed after the first callback panic")
                .unwrap();
            assert_eq!(second.fired.borrow().len(), 1);
        })
    });
}

fn header_byte_events_register_before_encoded_callback() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let log = Rc::new(HeaderOrderingLog::default());
            let handler: HttpHandlerRef = Rc::new(HeaderOrderingHandler {
                log: Rc::clone(&log),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            session.run().await.unwrap();
            assert!(log.cancelled.get());
            assert!(log.encoded_after_registration.get());
        })
    });
}

fn panicking_header_callback_finishes_driver_cleanup() {
    let transport_drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = Arc::new(AtomicUsize::new(0));
    let (worker, driver) = WorkerDriver::new(16).unwrap();
    let state_handle = worker.clone();
    let transport_probe = Arc::clone(&transport_drops);
    let handler_probe = Arc::clone(&handler_drops);
    let owner = std::thread::current().id();
    let result = catch_unwind(AssertUnwindSafe(move || {
        run_local(async move {
            let call = worker
                .call_local(move |context| {
                    let transport_probe = Arc::clone(&transport_probe);
                    let handler_probe = Arc::clone(&handler_probe);
                    async move {
                        let (inner, _) = ScriptedTransport::new([ReadAction::Data(
                            b"GET / HTTP/1.1\r\n\r\n".to_vec(),
                        )]);
                        let transport: Rc<dyn UniplexTransport> = Rc::new(DropProbeTransport {
                            inner,
                            drops: transport_probe,
                            reset_calls: Arc::new(AtomicUsize::new(0)),
                            owner,
                        });
                        let handler: HttpHandlerRef = Rc::new(PanickingHeaderDropProbeHandler {
                            drops: handler_probe,
                            owner,
                        });
                        let (session, _) = UniplexSession::new(
                            context,
                            TransportDirection::Downstream,
                            transport,
                            Some(handler),
                            UniplexConfig::default(),
                        )
                        .unwrap();
                        let _ = session.run().await;
                    }
                })
                .unwrap();
            let _ = tokio::join!(driver, call);
        });
    }));
    assert!(result.is_err());
    assert_eq!(state_handle.state(), WorkerState::Closed);
    assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
    assert_eq!(handler_drops.load(Ordering::SeqCst), 1);
}

fn read_failure_interrupts_final_byte_event_wait() {
    run_on_worker(|context| {
        Box::pin(async move {
            let fail_read = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::with_capability(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                    ReadAction::WaitError(
                        Arc::clone(&fail_read),
                        HttpErrorCode::TRANSPORT_READ_ERROR,
                    ),
                ],
                ByteEventCapability::TransportEvents,
            );
            let callback = Rc::new(ByteEventLog::default());
            let handler: HttpHandlerRef = Rc::new(RegisteredHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let run = session.run();
            tokio::pin!(run);
            tokio::time::timeout(Duration::from_secs(1), async {
                while state.borrow().writes.len() < 2 {
                    tokio::select! {
                        _ = run.as_mut() => panic!("session completed before byte-event wait"),
                        _ = tokio::task::yield_now() => {},
                    }
                }
            })
            .await
            .expect("writer reached its final TX/ACK wait");
            assert!(
                tokio::time::timeout(Duration::from_millis(20), run.as_mut())
                    .await
                    .is_err(),
                "pending TX/ACK references kept the normal close suspended",
            );
            assert_eq!(state.borrow().close, CloseKind::Open);
            fail_read.notify_one();
            let error = tokio::time::timeout(Duration::from_secs(1), run.as_mut())
                .await
                .expect("read failure interrupted the byte-event wait")
                .expect_err("read failure terminates the session");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(callback.cancelled.get() > 0);
            assert!(
                callback
                    .cancellation_codes
                    .borrow()
                    .iter()
                    .all(|code| *code == HttpErrorCode::TRANSPORT_READ_ERROR)
            );
        })
    });
}

fn write_failure_cancels_tx_ack_boundary_with_triggering_error() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::with_capability(
                [
                    ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                    ReadAction::Eof,
                ],
                ByteEventCapability::TransportEvents,
            );
            state.borrow_mut().write_error = Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
            let callback = Rc::new(ByteEventLog::default());
            let handler: HttpHandlerRef = Rc::new(RegisteredHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let error = session.run().await.expect_err("scripted write failed");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_WRITE_ERROR);
            let codes = callback.cancellation_codes.borrow();
            assert!(!codes.is_empty());
            assert!(
                codes
                    .iter()
                    .all(|code| *code == HttpErrorCode::TRANSPORT_WRITE_ERROR)
            );
        })
    });
}

fn read_failure_cancels_queued_prepared_header_with_triggering_error() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let callback = Rc::new(ByteEventLog::default());
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/queued", false).unwrap();
            message.set_method(HttpMethod::Get).unwrap();
            message.set_http_version(1, 1);
            let mut headers = HttpHeaderEvent::new(message, true).unwrap();
            headers
                .byte_event_registrations_mut()
                .push(HttpByteEventRegistration::new(
                    None,
                    ByteEventType::TransportWrite.bit(),
                    &callback,
                ));
            let prepared = crate::uniplex::UniplexHandle::prepare_request_with_headers(
                headers,
                HttpSourceHolder::new(Box::new(HeadersAlreadyAvailableSource {
                    header_reads: Rc::new(Cell::new(0)),
                    body: None,
                })),
                handle.reserve_request().unwrap(),
            );
            let response = handle.send_prepared_request(prepared).unwrap();
            handle
                .fail_connection_while_core_detached_for_test(HttpError::with_message(
                    HttpErrorCode::TRANSPORT_READ_ERROR,
                    b"scripted detached-core read failure",
                ))
                .unwrap();
            drop(response);
            let error = session
                .run()
                .await
                .expect_err("read failure rejects an unsent prepared request");
            assert_eq!(error.code(), HttpErrorCode::TRANSPORT_READ_ERROR);
            assert!(state.borrow().writes.is_empty());
            assert_eq!(
                callback.cancellation_codes.borrow().as_slice(),
                &[HttpErrorCode::TRANSPORT_READ_ERROR],
            );
        })
    });
}

fn cancelled_run_future_finishes_owner_cleanup() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) =
                ScriptedTransport::new([ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec())]);
            let started = Rc::new(tokio::sync::Notify::new());
            let handler: HttpHandlerRef = Rc::new(PendingHandler {
                started: Rc::clone(&started),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            let task = tokio::task::spawn_local(session.run());
            tokio::time::timeout(Duration::from_secs(1), started.notified())
                .await
                .expect("active handler reached its pending response state");
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(
                handle
                    .reserve_request()
                    .err()
                    .expect("finalizing session rejects request reservation")
                    .code(),
                HttpErrorCode::DROPPED,
            );
            assert!(!handle.initiate_drain().unwrap());
            assert_eq!(
                handle.close_when_idle().unwrap_err().code(),
                HttpErrorCode::DROPPED,
            );
            assert_eq!(
                handle
                    .deliver_socket_byte_event(
                        roxygen_transport::byte_event::SocketByteEventType::Tx,
                        0,
                    )
                    .unwrap(),
                0,
            );
            assert_eq!(state.borrow().close, CloseKind::Reset);
            tokio::time::timeout(Duration::from_secs(1), async {
                while !handle.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("run cancellation completed owner-local session finalization");
            assert!(!handle.initiate_drain().unwrap());
            assert_eq!(
                handle
                    .deliver_socket_byte_event(
                        roxygen_transport::byte_event::SocketByteEventType::Tx,
                        0,
                    )
                    .unwrap_err()
                    .code(),
                HttpErrorCode::DROPPED,
            );
        })
    });
}

fn never_polled_run_cleanup_is_worker_owned() {
    let close = run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let run = session.run();
            drop(run);
            state.borrow().close
        })
    });
    assert_eq!(close, CloseKind::Reset);
}

fn escaped_unpolled_run_releases_owner_resources_before_closed() {
    let transport_drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = Arc::new(AtomicUsize::new(0));
    let owner = std::thread::current().id();
    let transport_probe = Arc::clone(&transport_drops);
    let handler_probe = Arc::clone(&handler_drops);
    run_local(async move {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        let state_handle = worker.clone();
        let call = worker
            .call_local(move |context| {
                let transport_probe = Arc::clone(&transport_probe);
                let handler_probe = Arc::clone(&handler_probe);
                async move {
                    let (inner, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
                    let transport: Rc<dyn UniplexTransport> = Rc::new(DropProbeTransport {
                        inner,
                        drops: transport_probe,
                        reset_calls: Arc::new(AtomicUsize::new(0)),
                        owner,
                    });
                    let handler: HttpHandlerRef = Rc::new(DropProbeHandler {
                        drops: handler_probe,
                        owner,
                    });
                    let (session, _) = UniplexSession::new(
                        context.clone(),
                        TransportDirection::Downstream,
                        transport,
                        Some(handler),
                        UniplexConfig::default(),
                    )
                    .unwrap();
                    ESCAPED_RUN.with(|slot| *slot.borrow_mut() = Some(session.run()));
                    assert!(context.handle().begin_shutdown());
                }
            })
            .unwrap();
        let (_, call_result) = tokio::join!(driver, call);
        call_result.expect("worker command constructed the escaped run future");
        assert_eq!(state_handle.state(), WorkerState::Closed);
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handler_drops.load(Ordering::SeqCst), 1);
        ESCAPED_RUN.with(|slot| drop(slot.borrow_mut().take()));
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handler_drops.load(Ordering::SeqCst), 1);
    });
}

fn escaped_running_run_releases_owner_resources_before_closed() {
    let transport_drops = Arc::new(AtomicUsize::new(0));
    let handler_drops = Arc::new(AtomicUsize::new(0));
    let reset_calls = Arc::new(AtomicUsize::new(0));
    let owner = std::thread::current().id();
    let transport_probe = Arc::clone(&transport_drops);
    let handler_probe = Arc::clone(&handler_drops);
    let reset_probe = Arc::clone(&reset_calls);
    run_local(async move {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        let state_handle = worker.clone();
        let call = worker
            .call_local(move |context| {
                let transport_probe = Arc::clone(&transport_probe);
                let handler_probe = Arc::clone(&handler_probe);
                let reset_probe = Arc::clone(&reset_probe);
                async move {
                    let (inner, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
                    let transport: Rc<dyn UniplexTransport> = Rc::new(DropProbeTransport {
                        inner,
                        drops: transport_probe,
                        reset_calls: reset_probe,
                        owner,
                    });
                    let handler: HttpHandlerRef = Rc::new(DropProbeHandler {
                        drops: handler_probe,
                        owner,
                    });
                    let (session, _) = UniplexSession::new(
                        context.clone(),
                        TransportDirection::Downstream,
                        transport,
                        Some(handler),
                        UniplexConfig::default(),
                    )
                    .unwrap();
                    let mut run = session.run();
                    tokio::select! {
                        biased;
                        _ = &mut run => panic!("pending transport loops completed early"),
                        _ = tokio::task::yield_now() => {},
                    }
                    ESCAPED_RUN.with(|slot| *slot.borrow_mut() = Some(run));
                    assert!(context.handle().begin_shutdown());
                }
            })
            .unwrap();
        let (_, call_result) = tokio::join!(driver, call);
        call_result.expect("worker command started the escaped run future");
        assert_eq!(state_handle.state(), WorkerState::Closed);
        assert_eq!(reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handler_drops.load(Ordering::SeqCst), 1);
        ESCAPED_RUN.with(|slot| drop(slot.borrow_mut().take()));
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handler_drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn worker_shutdown_inside_read_poll_finishes_cleanup() {
    shutdown_inside_read_poll_finishes_cleanup(false);
}

#[test]
fn worker_shutdown_inside_http2_read_poll_finishes_cleanup() {
    shutdown_inside_read_poll_finishes_cleanup(true);
}

fn shutdown_inside_read_poll_finishes_cleanup(http2: bool) {
    let transport_drops = Arc::new(AtomicUsize::new(0));
    let reset_calls = Arc::new(AtomicUsize::new(0));
    let transport_probe = Arc::clone(&transport_drops);
    let reset_probe = Arc::clone(&reset_calls);
    let owner = std::thread::current().id();
    run_local(async move {
        let (worker, driver) = WorkerDriver::new(16).unwrap();
        let state_handle = worker.clone();
        let call = worker
            .call_local(move |context| async move {
                let (inner, _) =
                    ScriptedTransport::new([ReadAction::ShutdownWorker(context.handle().clone())]);
                let transport: Rc<dyn UniplexTransport> = Rc::new(DropProbeTransport {
                    inner,
                    drops: transport_probe,
                    reset_calls: reset_probe,
                    owner,
                });
                let (session, _) = if http2 {
                    UniplexSession::new_http2(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        UniplexConfig::default(),
                        Http2SessionConfig::default(),
                    )
                } else {
                    UniplexSession::new(
                        context,
                        TransportDirection::Upstream,
                        transport,
                        None,
                        UniplexConfig::default(),
                    )
                }
                .unwrap();
                let mut run = session.run();
                poll_fn(|task_context| {
                    assert!(run.as_mut().poll(task_context).is_pending());
                    Poll::Ready(())
                })
                .await;
                ESCAPED_RUN.with(|slot| *slot.borrow_mut() = Some(run));
            })
            .unwrap();
        let (_, result) = tokio::join!(driver, call);
        result.expect("session initialization completed before the transport read");
        assert_eq!(state_handle.state(), WorkerState::Closed);
        assert_eq!(reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
        ESCAPED_RUN.with(|slot| drop(slot.borrow_mut().take()));
        assert_eq!(transport_drops.load(Ordering::SeqCst), 1);
    });
}

fn constructor_errors_precede_session_core_ownership() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new(Vec::<ReadAction>::new());
            state.borrow_mut().capability_error = Some(HttpErrorCode::INTERNAL_ERROR);
            let transport: Rc<dyn UniplexTransport> = transport;
            let error = match UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            ) {
                Ok(_) => panic!("capability failure constructed a session"),
                Err(error) => error,
            };
            assert_eq!(error.code(), HttpErrorCode::INTERNAL_ERROR);

            assert!(context.handle().begin_shutdown());
            let (transport, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let transport: Rc<dyn UniplexTransport> = transport;
            let error = match UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            ) {
                Ok(_) => panic!("closing worker constructed a session"),
                Err(error) => error,
            };
            assert_eq!(error.code(), HttpErrorCode::INVALID_STATE_TRANSITION);
        })
    });
}

fn worker_shutdown_rejects_new_request_admission() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let reservation = handle.reserve_request().unwrap();
            let prepared = crate::uniplex::UniplexHandle::prepare_request(
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/", None).unwrap(),
                )),
                reservation,
                CancellationToken::never(),
            )
            .await
            .unwrap();
            assert!(context.handle().begin_shutdown());
            assert_eq!(
                handle
                    .reserve_request()
                    .err()
                    .expect("closing worker rejects a new reservation")
                    .code(),
                HttpErrorCode::DROPPED,
            );
            assert_eq!(
                handle
                    .send_prepared_request(prepared)
                    .err()
                    .expect("closing worker rejects a prepared commit")
                    .code(),
                HttpErrorCode::DROPPED,
            );
            drop(session);
        })
    });
}

fn rejected_prepared_request_drops_source_without_state_borrow() {
    run_on_worker(|context| {
        Box::pin(async move {
            let make_session = |context: WorkerContext| {
                let (transport, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
                let transport: Rc<dyn UniplexTransport> = transport;
                UniplexSession::new(
                    context,
                    TransportDirection::Upstream,
                    transport,
                    None,
                    UniplexConfig::default(),
                )
                .unwrap()
            };
            let (owner_session, owner) = make_session(context.clone());
            let (foreign_session, foreign) = make_session(context.clone());
            let (draining_session, draining) = make_session(context);

            let foreign_drop = Rc::new(Cell::new(None));
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/", false).unwrap();
            message.set_method(HttpMethod::Get).unwrap();
            message.set_http_version(1, 1);
            let prepared = crate::uniplex::UniplexHandle::prepare_request_with_headers(
                HttpHeaderEvent::new(message, true).unwrap(),
                HttpSourceHolder::new(Box::new(ReentrantPreparedSource {
                    handle: foreign.clone(),
                    drain_result: Rc::clone(&foreign_drop),
                })),
                owner.reserve_request().unwrap(),
            );
            assert!(foreign.send_prepared_request(prepared).is_err());
            assert_eq!(foreign_drop.get(), Some(true));

            let draining_drop = Rc::new(Cell::new(None));
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_url(b"/", false).unwrap();
            message.set_method(HttpMethod::Get).unwrap();
            message.set_http_version(1, 1);
            let prepared = crate::uniplex::UniplexHandle::prepare_request_with_headers(
                HttpHeaderEvent::new(message, true).unwrap(),
                HttpSourceHolder::new(Box::new(ReentrantPreparedSource {
                    handle: draining.clone(),
                    drain_result: Rc::clone(&draining_drop),
                })),
                draining.reserve_request().unwrap(),
            );
            assert!(draining.initiate_drain().unwrap());
            assert!(draining.send_prepared_request(prepared).is_err());
            assert_eq!(draining_drop.get(), Some(false));

            drop(owner_session);
            drop(foreign_session);
            drop(draining_session);
        })
    });
}

fn serialization_error_drops_registrations_after_state_borrow() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let callback = Rc::new(ReentrantCancelLog {
                handle: RefCell::new(None),
                reentered: Cell::new(false),
                drain_result: Cell::new(None),
                cancellation_code: Cell::new(None),
            });
            let handler: HttpHandlerRef = Rc::new(InvalidHeaderHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            *callback.handle.borrow_mut() = Some(handle);
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("invalid response serialization terminated the session");
            let error = result.expect_err("invalid response headers fail serialization");
            assert!(callback.reentered.get());
            assert_eq!(callback.drain_result.get(), Some(true));
            assert_eq!(callback.cancellation_code.get(), Some(error.code()));
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

fn connection_failure_cancels_queued_registration_reentrantly() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(
                b"GET / HTTP/1.1\r\nHost: example\r\n\r\n".to_vec(),
            )]);
            state.borrow_mut().write_error = Some(HttpErrorCode::TRANSPORT_WRITE_ERROR);
            let callback = Rc::new(ReentrantCancelLog {
                handle: RefCell::new(None),
                reentered: Cell::new(false),
                drain_result: Cell::new(None),
                cancellation_code: Cell::new(None),
            });
            let handler: HttpHandlerRef = Rc::new(ReentrantBodyRegistrationHandler {
                callback: Rc::clone(&callback),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig {
                    maximum_write_size: 1,
                    ..UniplexConfig::default()
                },
            )
            .unwrap();
            *callback.handle.borrow_mut() = Some(handle);
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("write failure cancelled queued response registrations");
            let error = result.expect_err("scripted transport write fails the session");
            assert!(callback.reentered.get());
            assert_eq!(callback.drain_result.get(), Some(true));
            assert_eq!(callback.cancellation_code.get(), Some(error.code()));
            assert_eq!(state.borrow().close, CloseKind::Reset);
        })
    });
}

fn forced_error_evicts_pending_handler_response_reentrantly() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (foreign_transport, _) = ScriptedTransport::new(Vec::<ReadAction>::new());
            let foreign_transport: Rc<dyn UniplexTransport> = foreign_transport;
            let (foreign_session, foreign_handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                foreign_transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let foreign_reservation = foreign_handle.reserve_request().unwrap();
            let foreign_prepared = crate::uniplex::UniplexHandle::prepare_request(
                HttpSourceHolder::new(Box::new(
                    HttpFixedSource::make_get_request(b"/", None).unwrap(),
                )),
                foreign_reservation,
                CancellationToken::never(),
            )
            .await
            .unwrap();
            let response_started = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(
                    b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n"
                        .to_vec(),
                ),
                ReadAction::WaitData(Arc::clone(&response_started), b"z\r\n".to_vec()),
            ]);
            let reentry = Rc::new(ReentrantDropState {
                handle: RefCell::new(None),
                prepared: RefCell::new(Some(foreign_prepared)),
                dropped: Cell::new(false),
                drain_result: Cell::new(None),
                reserve_error: Cell::new(None),
                send_error: Cell::new(None),
            });
            let handler: HttpHandlerRef = Rc::new(BlockingReentrantHandler {
                started: response_started,
                reentry: Rc::clone(&reentry),
            });
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(handler),
                UniplexConfig::default(),
            )
            .unwrap();
            *reentry.handle.borrow_mut() = Some(handle);
            let result = tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("forced response replaced a pending handler transfer");
            assert!(result.is_err());
            assert!(reentry.dropped.get());
            assert!(reentry.drain_result.get().is_some());
            assert_eq!(reentry.reserve_error.get(), Some(HttpErrorCode::DROPPED));
            assert_eq!(reentry.send_error.get(), Some(HttpErrorCode::DROPPED));
            let state = state.borrow();
            let output = state.writes.concat();
            assert!(
                output
                    .windows(b"HTTP/1.1 400".len())
                    .any(|wire| wire == b"HTTP/1.1 400")
            );
            assert!(
                !output
                    .windows(b"HTTP/1.1 200".len())
                    .any(|wire| wire == b"HTTP/1.1 200")
            );
            assert_eq!(state.close, CloseKind::Reset);
            drop(foreign_session);
        })
    });
}

fn escaped_handle_does_not_retain_observer() {
    run_on_worker(|context| {
        Box::pin(async move {
            let baseline_guards = context.active_guards();
            let (transport, _) = ScriptedTransport::new([ReadAction::Eof]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, handle) = UniplexSession::new(
                context.clone(),
                TransportDirection::Upstream,
                erased,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            session.run().await.unwrap();
            assert!(handle.is_finished());
            assert_eq!(context.active_guards(), baseline_guards);
            let error = handle
                .deliver_socket_byte_event(
                    roxygen_transport::byte_event::SocketByteEventType::Tx,
                    0,
                )
                .unwrap_err();
            assert_eq!(error.code(), HttpErrorCode::DROPPED);
        })
    });
}

#[test]
fn segmented_ingress_avoids_mandatory_coalescing() {
    run_on_worker(|context| {
        Box::pin(async move {
            let head = Bytes::from_static(
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 4\r\n\r\n",
            );
            let first_body = Bytes::from_static(b"ab");
            let second_body = Bytes::from_static(b"cd");
            let expected_pointers = [first_body.as_ptr() as usize, second_body.as_ptr() as usize];
            let input_length = head.len() + first_body.len() + second_body.len();
            let mut input = BufQueue::new(input_length);
            input.push_bytes(head).expect("header segment fits");
            input
                .push_bytes(first_body)
                .expect("first body segment fits");
            input
                .push_bytes(second_body)
                .expect("second body segment fits");
            let (transport, state) = ScriptedTransport::new([ReadAction::SegmentedDataEof(input)]);
            let body_pointers = Rc::new(RefCell::new(Vec::new()));
            let handler: HttpHandlerRef = Rc::new(BackingCaptureHandler {
                body_pointers: Rc::clone(&body_pointers),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .expect("session construction succeeds");

            session.run().await.expect("segmented request succeeds");

            assert_eq!(*body_pointers.borrow(), expected_pointers);
            assert!(state.borrow().writes.concat().starts_with(b"HTTP/1.1 200"));
        })
    });
}

#[test]
fn exact_body_resume_completes_without_eof() {
    run_on_worker(|context| {
        Box::pin(async move {
            let request = b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndata"
                .to_vec();
            let (transport, state) = ScriptedTransport::new([ReadAction::Data(request)]);
            let bodies = Rc::new(Cell::new(0));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::clone(&bodies),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let config = UniplexConfig {
                ingress_buffer_limit: 4,
                ..UniplexConfig::default()
            };
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                config,
            )
            .expect("session construction succeeds");

            tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("exact-body flow-control resume must not await EOF")
                .expect("exact-body request succeeds");

            assert_eq!(bodies.get(), 4);
            assert!(state.borrow().writes.concat().starts_with(b"HTTP/1.1 200"));
        })
    });
}

#[test]
fn fragmented_headers_continue_reading_after_empty_codec_drive() {
    run_on_worker(|context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\nHost: ex".to_vec()),
                ReadAction::DataEof(b"ample\r\nConnection: close\r\n\r\n".to_vec()),
            ]);
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::new(Cell::new(0)),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .expect("session construction succeeds");

            tokio::time::timeout(Duration::from_secs(1), session.run())
                .await
                .expect("fragmented headers must continue to the next transport read")
                .expect("fragmented request succeeds");

            assert!(state.borrow().writes.concat().starts_with(b"HTTP/1.1 200"));
        })
    });
}

#[test]
fn callback_error_trims_consumed_prefix_with_buffered_suffix() {
    run_on_worker(|context| {
        Box::pin(async move {
            let prefix_drops = Arc::new(AtomicUsize::new(0));
            let prefix = Bytes::from_owner(DropTrackedBytes {
                bytes: b"GET / HTTP/1.1\r\nMalformed\r\n\r\n".to_vec(),
                drops: Arc::clone(&prefix_drops),
            });
            let suffix = Bytes::from_static(b"GET /later HTTP/1.1\r\n\r\n");
            let mut input = BufQueue::new(prefix.len() + suffix.len());
            input.push_bytes(prefix).expect("malformed prefix fits");
            input.push_bytes(suffix).expect("buffered suffix fits");
            let (transport, state) = ScriptedTransport::new([ReadAction::SegmentedDataEof(input)]);
            state.borrow_mut().expected_drops_before_reset = Some((Arc::clone(&prefix_drops), 1));
            let handler: HttpHandlerRef = Rc::new(TestHandler {
                mode: HandlerMode::Consume(200, 0),
                bodies: Rc::new(Cell::new(0)),
            });
            let transport: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                transport,
                Some(handler),
                UniplexConfig::default(),
            )
            .expect("session construction succeeds");

            let _ = session.run().await;

            assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.borrow().close, CloseKind::Reset);
            assert!(state.borrow().writes.concat().starts_with(b"HTTP/1.1 400"));
        })
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1IngressFailureCase {
    EofHeaderEom,
    EofBody,
    ParseHeaderEom,
    ParseBody,
    ParseAfterPriorError,
    HardLimitBody,
    BeforeHeadersEof,
    BeforeHeadersParse,
    NoStatus,
    AlreadyComplete,
}

impl H1IngressFailureCase {
    fn before_headers(self) -> bool {
        matches!(self, Self::BeforeHeadersEof | Self::BeforeHeadersParse)
    }

    fn eof(self) -> bool {
        matches!(
            self,
            Self::EofHeaderEom | Self::EofBody | Self::BeforeHeadersEof
        )
    }

    fn header_eom(self) -> bool {
        matches!(
            self,
            Self::EofHeaderEom | Self::ParseHeaderEom | Self::AlreadyComplete
        )
    }

    fn expected_error(self) -> Option<(HttpErrorCode, &'static [u8])> {
        if self == Self::AlreadyComplete {
            None
        } else if self == Self::ParseAfterPriorError {
            Some((
                HttpErrorCode::TRANSPORT_READ_ERROR,
                b"prior native ingress failure",
            ))
        } else if self == Self::HardLimitBody {
            Some((
                HttpErrorCode::FLOW_CONTROL_ERROR,
                b"ingress source rejected body bytes",
            ))
        } else if self.eof() {
            Some((HttpErrorCode::TRANSPORT_EOF, b"unexpected EOF"))
        } else {
            Some((
                HttpErrorCode::BODY_PARSE_ERROR,
                b"invalid HTTP chunk length",
            ))
        }
    }
}

struct H1IngressFailureProbe {
    owner: ThreadId,
    wrong_owner: std::sync::atomic::AtomicBool,
    handler_drops: AtomicUsize,
    ingress_drops: AtomicUsize,
    response_drops: AtomicUsize,
    response_stops: AtomicUsize,
    io_drops: AtomicUsize,
    live_io_drops: AtomicUsize,
}

impl H1IngressFailureProbe {
    fn new() -> Self {
        Self {
            owner: std::thread::current().id(),
            wrong_owner: std::sync::atomic::AtomicBool::new(false),
            handler_drops: AtomicUsize::new(0),
            ingress_drops: AtomicUsize::new(0),
            response_drops: AtomicUsize::new(0),
            response_stops: AtomicUsize::new(0),
            io_drops: AtomicUsize::new(0),
            live_io_drops: AtomicUsize::new(0),
        }
    }

    fn observe(&self, counter: &AtomicUsize) {
        if self.owner != std::thread::current().id() {
            self.wrong_owner.store(true, Ordering::SeqCst);
        }
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

struct H1IngressFailureState {
    case: H1IngressFailureCase,
    probe: Arc<H1IngressFailureProbe>,
    ingress: RefCell<Option<(HttpSourceHolder, bool)>>,
    ready: tokio::sync::Notify,
    handler_ready: tokio::sync::Notify,
    completion: RefCell<Option<roxygen_core::exchange::ExchangeCompletion>>,
    handle: RefCell<Option<crate::uniplex::UniplexHandle>>,
    retirement: Option<Rc<H1IngressRetirementControl>>,
}

struct H1IngressFailureHandler(Rc<H1IngressFailureState>);

struct H1IngressHandlerCapture(Arc<H1IngressFailureProbe>);

impl Drop for H1IngressHandlerCapture {
    fn drop(&mut self) {
        self.0.observe(&self.0.handler_drops);
    }
}

impl HttpHandler for H1IngressFailureHandler {
    fn handle_request(
        &self,
        context: HandlerSessionContext,
        mut source: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        let state = Rc::clone(&self.0);
        *state.completion.borrow_mut() = context.exchange_completion().cloned();
        let capture = H1IngressHandlerCapture(Arc::clone(&state.probe));
        Box::pin(async move {
            let _capture = capture;
            let header = source.read_header_event().await?;
            *state.ingress.borrow_mut() = Some((source, header.eom()));
            state.ready.notify_one();
            state.handler_ready.notify_one();
            if state.case.before_headers() {
                return std::future::pending().await;
            }
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_http_version(1, 1);
            message
                .set_status_code(if state.case == H1IngressFailureCase::NoStatus {
                    103
                } else {
                    200
                })
                .expect("fixture status");
            if state.case != H1IngressFailureCase::NoStatus {
                message.headers_mut().set(
                    HttpHeaderCode::ContentLength,
                    if state.case.header_eom() {
                        b"0".as_slice()
                    } else {
                        b"3".as_slice()
                    },
                );
            }
            let mut header =
                HttpHeaderEvent::new(message, state.case.header_eom()).expect("fixture header");
            if let Some(control) = &state.retirement {
                header
                    .byte_event_registrations_mut()
                    .push(HttpByteEventRegistration::new(
                        None,
                        ByteEventType::TransportWrite.bit()
                            | ByteEventType::KernelWrite.bit()
                            | ByteEventType::NicTx.bit()
                            | ByteEventType::CumulativeAck.bit(),
                        control,
                    ));
            }
            if state.case == H1IngressFailureCase::ParseAfterPriorError {
                let handle = state
                    .handle
                    .borrow()
                    .as_ref()
                    .expect("installed handle")
                    .clone();
                header.set_egress_headers_callback(move |_| {
                    handle
                        .record_h1_ingress_error_for_test(HttpError::with_message(
                            HttpErrorCode::TRANSPORT_READ_ERROR,
                            b"prior native ingress failure",
                        ))
                        .expect("first native cause before peer sends malformed body");
                });
            }
            Ok(HttpSourceHolder::new(Box::new(H1IngressFailureResponse {
                header: Some(header),
                probe: Arc::clone(&state.probe),
                retirement: state.retirement.clone(),
            })))
        })
    }
}

struct H1IngressFailureResponse {
    header: Option<HttpHeaderEvent>,
    probe: Arc<H1IngressFailureProbe>,
    retirement: Option<Rc<H1IngressRetirementControl>>,
}

impl HttpSource for H1IngressFailureResponse {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        match self.header.take() {
            Some(header) => Box::pin(std::future::ready(Ok(header))),
            None => Box::pin(std::future::pending()),
        }
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let capture = self.retirement.as_ref().map(|control| {
            control.body_reads.set(control.body_reads.get() + 1);
            H1IngressBodyRead(Rc::clone(control))
        });
        Box::pin(async move {
            let _capture = capture;
            std::future::pending().await
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.probe.observe(&self.probe.response_stops);
        if let Some(control) = &self.retirement {
            control.stops.borrow_mut().push(error);
            control.observe_restored_core();
            if control.armed.get() && control.fault == H1IngressRetirementFault::Stop {
                std::panic::panic_any(Arc::clone(&control.first));
            }
        }
    }
}

impl Drop for H1IngressFailureResponse {
    fn drop(&mut self) {
        self.probe.observe(&self.probe.response_drops);
        if let Some(control) = &self.retirement
            && control.armed.get()
            && control.fault == H1IngressRetirementFault::Stop
        {
            std::panic::panic_any(SelectedCloseSecondaryPayload {
                drops: Arc::clone(&control.secondary_drops),
                nested_drops: Arc::clone(&control.nested_drops),
            });
        }
    }
}

struct H1IngressFailureIo {
    socket: tokio::net::TcpStream,
    probe: Arc<H1IngressFailureProbe>,
}

impl tokio::io::AsyncRead for H1IngressFailureIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(Pin::new(&mut self.socket), context, buffer)
    }
}

impl tokio::io::AsyncWrite for H1IngressFailureIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.socket), context, bytes)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.socket), context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut self.socket), context)
    }
}

impl Drop for H1IngressFailureIo {
    fn drop(&mut self) {
        self.probe.observe(&self.probe.io_drops);
        if tokio::runtime::Handle::try_current().is_ok() {
            self.probe.live_io_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct H1IngressFailureReport {
    peer: Option<std::io::Result<Vec<u8>>>,
    run: Option<Result<crate::uniplex::UniplexRunSummary, HttpError>>,
    ingress: Option<Result<(), HttpError>>,
    exchange: Option<Result<(), roxygen_core::exchange::ExchangeCompletionError>>,
    io: Option<Result<(), roxygen_transport::established::AdoptedIoCompletionError>>,
    panic: Option<Box<dyn std::any::Any + Send>>,
    timed_out: bool,
    finished: bool,
    probe: Arc<H1IngressFailureProbe>,
}

struct H1IngressRequestOwner {
    source: HttpSourceHolder,
    probe: Arc<H1IngressFailureProbe>,
}

impl Drop for H1IngressRequestOwner {
    fn drop(&mut self) {
        self.probe.observe(&self.probe.ingress_drops);
    }
}

async fn h1_ingress_failure_read(state: &H1IngressFailureState) -> Result<(), HttpError> {
    state.ready.notified().await;
    let (source, complete) = state
        .ingress
        .borrow_mut()
        .take()
        .expect("actual ingress owner");
    let mut owner = H1IngressRequestOwner {
        source,
        probe: Arc::clone(&state.probe),
    };
    if !complete {
        loop {
            let event = owner.source.read_body_event(u32::MAX).await?;
            let eom = event.eom();
            if let HttpBodyEventKind::Suspend(resume) = event.into_kind() {
                let _ = resume.await;
            }
            if eom {
                break;
            }
        }
    }
    Ok(())
}

async fn h1_ingress_failure_peer(
    mut peer: tokio::net::TcpStream,
    state: Rc<H1IngressFailureState>,
) -> std::io::Result<Vec<u8>> {
    let case = state.case;
    let request = if case == H1IngressFailureCase::AlreadyComplete {
        b"GET /complete HTTP/1.1\r\nHost: loopback\r\n\r\n".as_slice()
    } else if case == H1IngressFailureCase::EofBody {
        b"POST /eof HTTP/1.1\r\nHost: loopback\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n"
            .as_slice()
    } else if case.eof() {
        b"POST /eof HTTP/1.1\r\nHost: loopback\r\nContent-Length: 5\r\n\r\nx".as_slice()
    } else {
        b"POST /parse HTTP/1.1\r\nHost: loopback\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice()
    };
    tokio::io::AsyncWriteExt::write_all(&mut peer, request).await?;
    if case.before_headers() {
        state.handler_ready.notified().await;
    }
    let mut wire = Vec::new();
    if !case.before_headers() {
        while !wire.ends_with(b"\r\n\r\n") {
            let byte = tokio::io::AsyncReadExt::read_u8(&mut peer).await?;
            wire.push(byte);
            if wire.len() > 4096 {
                return Err(std::io::Error::other("unbounded response header"));
            }
        }
    }
    if case.eof() || case == H1IngressFailureCase::AlreadyComplete {
        tokio::io::AsyncWriteExt::shutdown(&mut peer).await?;
    } else {
        tokio::io::AsyncWriteExt::write_all(&mut peer, b"z\r\n").await?;
    }
    let mut tail = tokio::io::AsyncReadExt::take(&mut peer, 4097);
    tokio::io::AsyncReadExt::read_to_end(&mut tail, &mut wire).await?;
    if wire.len() > 4096 {
        return Err(std::io::Error::other(
            "unbounded response after ingress failure",
        ));
    }
    Ok(wire)
}

async fn h1_ingress_failure_operation(
    context: WorkerContext,
    case: H1IngressFailureCase,
) -> H1IngressFailureReport {
    let probe = Arc::new(H1IngressFailureProbe::new());
    let mut report = H1IngressFailureReport {
        peer: None,
        run: None,
        ingress: None,
        exchange: None,
        io: None,
        panic: None,
        timed_out: false,
        finished: false,
        probe: Arc::clone(&probe),
    };
    let outcome = selected_close_catch(tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("loopback listener");
        let (accepted, peer) = tokio::join!(
            listener.accept(),
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        );
        let socket = accepted.expect("accepted loopback I/O").0;
        let peer = peer.expect("connected loopback peer");
        drop(listener);
        let established = roxygen_transport::established::EstablishedIo::new(
            H1IngressFailureIo {
                socket,
                probe: Arc::clone(&probe),
            },
            roxygen_transport::byte_stream::TransportEndpoint::Unknown,
            roxygen_transport::byte_stream::TransportEndpoint::Unknown,
            None,
        );
        let (transport, io) = roxygen_transport::established::adopt_local_with_completion(
            &context,
            established,
            roxygen_transport::byte_stream::ByteStreamLimits::new(4096, 4096, 16).unwrap(),
        )
        .expect("adopted I/O with actual retirement receipt");
        let state = Rc::new(H1IngressFailureState {
            case,
            probe: Arc::clone(&probe),
            ingress: RefCell::new(None),
            ready: tokio::sync::Notify::new(),
            handler_ready: tokio::sync::Notify::new(),
            completion: RefCell::new(None),
            handle: RefCell::new(None),
            retirement: None,
        });
        let transport: Rc<dyn UniplexTransport> = Rc::new(transport);
        let handler: HttpHandlerRef = Rc::new(H1IngressFailureHandler(Rc::clone(&state)));
        let (session, handle) = UniplexSession::new(
            context.clone(),
            TransportDirection::Downstream,
            transport,
            Some(handler),
            UniplexConfig {
                read_timeout: Duration::ZERO,
                ..UniplexConfig::default()
            },
        )
        .expect("native H1 session");
        *state.handle.borrow_mut() = Some(handle.clone());
        let server = async {
            let (run, ingress) = tokio::join!(session.run(), h1_ingress_failure_read(&state));
            report.run = Some(run);
            report.ingress = Some(ingress);
            report.finished = handle.is_finished();
            let completion = state
                .completion
                .borrow()
                .as_ref()
                .expect("exchange receipt")
                .clone();
            report.exchange = Some(completion.wait().await);
            io.cancel();
            report.io = Some(io.completion().wait_retired().await);
        };
        let peer = async {
            report.peer = Some(h1_ingress_failure_peer(peer, Rc::clone(&state)).await);
        };
        tokio::join!(server, peer);
    }))
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(_)) => report.timed_out = true,
        Err(payload) => report.panic = Some(payload),
    }
    report
}

fn h1_ingress_failure_network(case: H1IngressFailureCase) {
    let report = std::thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("I/O fixture runtime")
            .block_on(LocalSet::new().run_until(async move {
                let (worker, driver) = WorkerDriver::new(8).unwrap();
                let call = worker
                    .call_local(move |context| h1_ingress_failure_operation(context, case))
                    .expect("bounded worker submission");
                let call = async {
                    let result = call.await;
                    worker.begin_shutdown();
                    result
                };
                let (driver, result) = tokio::join!(selected_close_catch(driver), call);
                if let Err(payload) = driver {
                    std::panic::resume_unwind(payload);
                }
                assert_eq!(worker.state(), WorkerState::Closed);
                result.expect("actual native application parent joined")
            }))
    })
    .join();
    let mut report = match report {
        Ok(report) => report,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    if let Some(payload) = report.panic.take() {
        std::panic::resume_unwind(payload);
    }
    assert!(
        !report.timed_out,
        "{case:?}: owned network exchange timed out"
    );
    let wire = report
        .peer
        .expect("peer completed")
        .expect("adopted TCP closes with clean EOF");
    let expected_status = if case.before_headers() {
        b"400".as_slice()
    } else if case == H1IngressFailureCase::NoStatus {
        b"103".as_slice()
    } else {
        b"200".as_slice()
    };
    assert!(wire.starts_with(b"HTTP/1.1 "), "{case:?}: {wire:?}");
    assert_eq!(&wire[9..12], expected_status, "{case:?}: {wire:?}");
    let boundary = wire
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap()
        + 4;
    assert_eq!(
        wire.len(),
        boundary,
        "{case:?}: no second response or ungenerated body: {wire:?}"
    );
    if case.before_headers() {
        assert!(
            wire.windows(17)
                .any(|part| part.eq_ignore_ascii_case(b"content-length: 0")),
            "{wire:?}"
        );
    }
    match case.expected_error() {
        Some((code, message)) => {
            let error = report
                .run
                .expect("Run completed")
                .expect_err("ingress failure is native Run failure");
            assert_eq!(error.code(), code, "{case:?}: {error:?}");
            assert_eq!(error.message(), message, "{case:?}");
            let ingress = report
                .ingress
                .expect("actual ingress reader reaped")
                .expect_err("incomplete ingress failed");
            assert_eq!(
                ingress.code(),
                if case.eof() {
                    HttpErrorCode::TRANSPORT_EOF
                } else {
                    HttpErrorCode::BODY_PARSE_ERROR
                }
            );
        }
        None => {
            assert!(report.run.expect("Run completed").is_ok());
            assert!(report.ingress.expect("ingress completed").is_ok());
        }
    }
    assert!(report.finished);
    assert_eq!(report.exchange, Some(Ok(())));
    assert_eq!(report.io, Some(Ok(())));
    assert!(!report.probe.wrong_owner.load(Ordering::SeqCst));
    assert_eq!(report.probe.handler_drops.load(Ordering::SeqCst), 1);
    assert_eq!(report.probe.ingress_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        report.probe.response_drops.load(Ordering::SeqCst),
        usize::from(!case.before_headers())
    );
    assert_eq!(report.probe.io_drops.load(Ordering::SeqCst), 1);
    assert_eq!(report.probe.live_io_drops.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum H1IngressRetirementFault {
    None,
    Stop,
    ByteCancellation,
}

struct H1IngressRetirementControl {
    fault: H1IngressRetirementFault,
    armed: Cell<bool>,
    error_ready: Arc<tokio::sync::Notify>,
    write_gate: CancellableBaton,
    writes_started: Cell<usize>,
    writes_done: Cell<usize>,
    body_reads: Cell<usize>,
    body_read_drops: Cell<usize>,
    handle: RefCell<Option<crate::uniplex::UniplexHandle>>,
    stops: RefCell<Vec<Option<HttpErrorCode>>>,
    restored: RefCell<Vec<bool>>,
    fired: RefCell<Vec<HttpByteEvent>>,
    cancelled: RefCell<Vec<(HttpByteEvent, HttpError)>>,
    first: Arc<SelectedCloseMarker>,
    secondary_drops: Arc<AtomicUsize>,
    nested_drops: Arc<AtomicUsize>,
    stage: RefCell<Option<Result<(), SelectedCloseStageError>>>,
    run: RefCell<Option<Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
    ingress: RefCell<Option<Result<(), HttpError>>>,
    cleanup_error: RefCell<Option<HttpError>>,
    transport: RefCell<Option<Rc<RefCell<TransportState>>>>,
    transport_owner: RefCell<Option<std::rc::Weak<ScriptedTransport>>>,
}

impl H1IngressRetirementControl {
    fn new(fault: H1IngressRetirementFault) -> Self {
        Self {
            fault,
            armed: Cell::new(false),
            error_ready: Arc::new(tokio::sync::Notify::new()),
            write_gate: CancellableBaton::new(),
            writes_started: Cell::new(0),
            writes_done: Cell::new(0),
            body_reads: Cell::new(0),
            body_read_drops: Cell::new(0),
            handle: RefCell::new(None),
            stops: RefCell::new(Vec::new()),
            restored: RefCell::new(Vec::new()),
            fired: RefCell::new(Vec::new()),
            cancelled: RefCell::new(Vec::new()),
            first: Arc::new(SelectedCloseMarker),
            secondary_drops: Arc::new(AtomicUsize::new(0)),
            nested_drops: Arc::new(AtomicUsize::new(0)),
            stage: RefCell::new(None),
            run: RefCell::new(None),
            ingress: RefCell::new(None),
            cleanup_error: RefCell::new(None),
            transport: RefCell::new(None),
            transport_owner: RefCell::new(None),
        }
    }

    fn release(&self) {
        self.write_gate.signal();
        self.error_ready.notify_one();
    }

    fn observe_restored_core(&self) {
        let handle = self
            .handle
            .borrow()
            .as_ref()
            .expect("installed native handle")
            .clone();
        let restored = handle
            .h1_ingress_reset_for_test(roxygen_core::ids::StreamId::new(1))
            .is_ok_and(|reset| reset);
        self.restored.borrow_mut().push(restored);
    }

    async fn wait(
        &self,
        run: &mut Pin<
            Box<impl Future<Output = Result<crate::uniplex::UniplexRunSummary, HttpError>>>,
        >,
        label: &'static str,
        ready: impl Fn() -> bool,
    ) -> Result<(), SelectedCloseStageError> {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !ready() {
                tokio::select! {
                    result = run.as_mut() => {
                        *self.run.borrow_mut() = Some(result);
                        return Err(SelectedCloseStageError::EarlyRun(label));
                    }
                    () = tokio::task::yield_now() => {}
                }
            }
            Ok(())
        })
        .await
        .unwrap_or(Err(SelectedCloseStageError::Timeout(label)))
    }
}

impl HttpByteEventCallback for H1IngressRetirementControl {
    fn on_byte_event(&self, event: HttpByteEvent) {
        self.fired.borrow_mut().push(event);
    }

    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError) {
        self.cancelled.borrow_mut().push((event, error));
        if self.armed.get()
            && self.fault == H1IngressRetirementFault::ByteCancellation
            && self.cancelled.borrow().len() == 1
        {
            self.observe_restored_core();
            std::panic::panic_any(Arc::clone(&self.first));
        }
    }
}

struct H1IngressBodyRead(Rc<H1IngressRetirementControl>);

impl Drop for H1IngressBodyRead {
    fn drop(&mut self) {
        self.0.body_read_drops.set(self.0.body_read_drops.get() + 1);
    }
}

struct H1IngressRetirementRelease(Rc<H1IngressRetirementControl>);

impl Drop for H1IngressRetirementRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct H1IngressGatedTransport {
    inner: Rc<ScriptedTransport>,
    control: Rc<H1IngressRetirementControl>,
    probe: Arc<H1IngressFailureProbe>,
}

impl Drop for H1IngressGatedTransport {
    fn drop(&mut self) {
        self.probe.observe(&self.probe.io_drops);
        if tokio::runtime::Handle::try_current().is_ok() {
            self.probe.live_io_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl UniplexTransport for H1IngressGatedTransport {
    fn read(
        &self,
        context: &WorkerContext,
        maximum: usize,
        allocation: usize,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<UniplexRead, HttpError>> {
        self.inner
            .read(context, maximum, allocation, timeout, cancellation)
    }

    fn write(
        &self,
        context: &WorkerContext,
        data: BufQueue,
        flags: ByteEventWriteFlags,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> LocalTransportFuture<Result<WriteInfo, TransportWriteError>> {
        self.control
            .writes_started
            .set(self.control.writes_started.get() + 1);
        let control = Rc::clone(&self.control);
        let inner = Rc::clone(&self.inner);
        let context = context.clone();
        Box::pin(async move {
            if control.write_gate.wait(cancellation.clone()).await == WaitStatus::Cancelled {
                return Err(TransportWriteError::new(
                    HttpError::with_message(
                        HttpErrorCode::CORO_CANCELLED,
                        b"fixture write cancelled",
                    ),
                    0,
                ));
            }
            let result = inner
                .write(&context, data, flags, timeout, cancellation)
                .await;
            if result.is_ok() {
                control.writes_done.set(control.writes_done.get() + 1);
            }
            result
        })
    }

    fn shutdown_write(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.shutdown_write(context)
    }
    fn close(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close(context)
    }
    fn close_with_reset(&self, context: &WorkerContext) -> Result<(), HttpError> {
        self.inner.close_with_reset(context)
    }
    fn byte_event_capability(
        &self,
        context: &WorkerContext,
    ) -> Result<ByteEventCapability, HttpError> {
        self.inner.byte_event_capability(context)
    }
    fn register_byte_event(
        &self,
        context: &WorkerContext,
        registration: HttpByteEventRegistration,
    ) -> Result<(), ByteEventRegistrationError> {
        self.inner.register_byte_event(context, registration)
    }
}

thread_local! {
    static H1_INGRESS_RETIREMENT_STATE: RefCell<Option<Rc<H1IngressFailureState>>> = const { RefCell::new(None) };
}

async fn h1_ingress_retirement_operation(context: WorkerContext, state: Rc<H1IngressFailureState>) {
    let control = state.retirement.as_ref().unwrap();
    let _release = H1IngressRetirementRelease(Rc::clone(control));
    let hard_limit = state.case == H1IngressFailureCase::HardLimitBody;
    let input = if hard_limit {
        vec![
            ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nContent-Length: 800\r\n\r\n".to_vec(),
            ),
            ReadAction::WaitDataOrCancellation(Arc::clone(&control.error_ready), vec![b'x'; 400]),
            ReadAction::Data(vec![b'x'; 400]),
        ]
    } else {
        vec![
            ReadAction::Data(
                b"POST / HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec(),
            ),
            ReadAction::WaitDataOrCancellation(Arc::clone(&control.error_ready), b"z\r\n".to_vec()),
        ]
    };
    let (inner, transport_state) =
        ScriptedTransport::with_capability(input, ByteEventCapability::TransportEvents);
    *control.transport.borrow_mut() = Some(Rc::clone(&transport_state));
    *control.transport_owner.borrow_mut() = Some(Rc::downgrade(&inner));
    let transport: Rc<dyn UniplexTransport> = Rc::new(H1IngressGatedTransport {
        inner,
        control: Rc::clone(control),
        probe: Arc::clone(&state.probe),
    });
    let handler: HttpHandlerRef = Rc::new(H1IngressFailureHandler(Rc::clone(&state)));
    let config = if hard_limit {
        UniplexConfig {
            buffer_limit: 700,
            ingress_buffer_limit: 500,
            maximum_read_size: 700,
            read_allocation_size: 700,
            read_timeout: Duration::ZERO,
            ..UniplexConfig::default()
        }
    } else {
        UniplexConfig {
            read_timeout: Duration::ZERO,
            ..UniplexConfig::default()
        }
    };
    let (session, handle) = UniplexSession::new(
        context.clone(),
        TransportDirection::Downstream,
        transport,
        Some(handler),
        config,
    )
    .expect("gated native session");
    *state.handle.borrow_mut() = Some(handle.clone());
    *control.handle.borrow_mut() = Some(handle.clone());
    if control.fault != H1IngressRetirementFault::None {
        control.write_gate.signal();
    }
    let mut run = Box::pin(session.run());
    let mut ingress = Box::pin(h1_ingress_failure_read(&state));
    let stage = async {
        control
            .wait(&mut run, "actual final header generation", || {
                control.writes_started.get() == 1
            })
            .await?;
        if control.fault != H1IngressRetirementFault::None {
            control
                .wait(&mut run, "pending actual response body read", || {
                    control.body_reads.get() == 1 && control.writes_done.get() == 1
                })
                .await?;
        }
        if ingress
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_ready()
        {
            return Err(SelectedCloseStageError::EarlyRun(
                "request ingress must remain incomplete",
            ));
        }
        *control.stage.borrow_mut() = Some(Ok(()));
        control.armed.set(true);
        control.error_ready.notify_one();
        if control.fault != H1IngressRetirementFault::None {
            let (result, read) = tokio::join!(run.as_mut(), ingress.as_mut());
            *control.run.borrow_mut() = Some(result);
            *control.ingress.borrow_mut() = Some(read);
            return Err(SelectedCloseStageError::EarlyRun(
                "retirement panic must reach the joined native owner",
            ));
        }
        control
            .wait(
                &mut run,
                "prepared source retirement during held write",
                || !control.stops.borrow().is_empty(),
            )
            .await?;
        if transport_state.borrow().close != CloseKind::Open || control.writes_done.get() != 0 {
            return Err(SelectedCloseStageError::EarlyRun(
                "encoded bytes must drain before reset",
            ));
        }
        let completion = state.completion.borrow().as_ref().unwrap().clone();
        if completion
            .wait()
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_ready()
        {
            return Err(SelectedCloseStageError::EarlyRun(
                "held write certified cleanup",
            ));
        }
        if !handle
            .h1_ingress_reset_for_test(roxygen_core::ids::StreamId::new(1))
            .map_err(SelectedCloseStageError::Native)?
        {
            return Err(SelectedCloseStageError::EarlyRun(
                "reset state must be committed before callbacks",
            ));
        }
        control.write_gate.signal();
        let read = ingress.as_mut().await;
        let (expected_code, _) = state.case.expected_error().expect("ingress failure case");
        let check = match &read {
            Err(error) if error.code() == expected_code => Ok(()),
            Err(error) => Err(SelectedCloseStageError::Native(error.clone())),
            Ok(()) => Err(SelectedCloseStageError::EarlyRun(
                "rejected ingress completed successfully",
            )),
        };
        *control.ingress.borrow_mut() = Some(read);
        check?;
        let result = tokio::time::timeout(Duration::from_secs(2), run.as_mut())
            .await
            .map_err(|_| SelectedCloseStageError::Timeout("actual native Run join"))?;
        *control.run.borrow_mut() = Some(result);
        Ok(())
    };
    let stage = tokio::time::timeout(Duration::from_secs(5), stage)
        .await
        .unwrap_or(Err(SelectedCloseStageError::Timeout(
            "gated retirement phase",
        )));
    let failed = stage.is_err();
    *control.stage.borrow_mut() = Some(stage);
    if failed {
        control.release();
        let cleanup = handle.fail_connection_while_core_detached_for_test(HttpError::with_message(
            HttpErrorCode::CORO_CANCELLED,
            b"post-final ingress fixture cleanup",
        ));
        if let Err(error) = cleanup {
            *control.cleanup_error.borrow_mut() = Some(error);
        }
    }
    drop(ingress);
    drop(run);
    control.release();
}

fn h1_ingress_retirement_case(case: H1IngressFailureCase, fault: H1IngressRetirementFault) {
    let joined = std::thread::spawn(move || {
        run_local(async move {
            let control = Rc::new(H1IngressRetirementControl::new(fault));
            let state = Rc::new(H1IngressFailureState {
                case,
                probe: Arc::new(H1IngressFailureProbe::new()),
                ingress: RefCell::new(None),
                ready: tokio::sync::Notify::new(),
                handler_ready: tokio::sync::Notify::new(),
                completion: RefCell::new(None),
                handle: RefCell::new(None),
                retirement: Some(Rc::clone(&control)),
            });
            H1_INGRESS_RETIREMENT_STATE.set(Some(Rc::clone(&state)));
            let (worker, driver) = WorkerDriver::new(8).unwrap();
            let call = worker
                .call_local(|context| {
                    let state = H1_INGRESS_RETIREMENT_STATE
                        .with_borrow(|state| Rc::clone(state.as_ref().unwrap()));
                    h1_ingress_retirement_operation(context, state)
                })
                .unwrap();
            let call = async {
                let result = call.await;
                worker.begin_shutdown();
                result
            };
            let (driver, call) = tokio::join!(selected_close_catch(driver), call);
            control.release();
            H1_INGRESS_RETIREMENT_STATE.take();
            let panic = match driver {
                Ok(()) => None,
                Err(payload) => match payload.downcast::<Arc<SelectedCloseMarker>>() {
                    Ok(first) => Some(*first),
                    Err(payload) => std::panic::resume_unwind(payload),
                },
            };
            let completion = state
                .completion
                .borrow()
                .as_ref()
                .expect("actual exchange receipt")
                .clone();
            let receipt = tokio::time::timeout(Duration::from_secs(2), completion.wait()).await;
            assert_eq!(worker.state(), WorkerState::Closed);
            assert!(
                control.stage.borrow().as_ref().is_some_and(Result::is_ok),
                "{fault:?}: {:?}",
                control.stage.borrow()
            );
            assert!(
                control.cleanup_error.borrow().is_none(),
                "{:?}",
                control.cleanup_error.borrow()
            );
            assert_eq!(state.probe.handler_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.probe.ingress_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.probe.response_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.probe.io_drops.load(Ordering::SeqCst), 1);
            assert_eq!(state.probe.live_io_drops.load(Ordering::SeqCst), 1);
            assert!(
                control
                    .transport_owner
                    .borrow()
                    .as_ref()
                    .expect("actual transport owner")
                    .upgrade()
                    .is_none()
            );
            assert!(!state.probe.wrong_owner.load(Ordering::SeqCst));
            let (expected_code, expected_message) = case.expected_error().unwrap();
            assert_eq!(control.stops.borrow().as_slice(), &[Some(expected_code)]);
            assert!(!control.restored.borrow().is_empty());
            assert!(control.restored.borrow().iter().all(|restored| *restored));
            assert_eq!(control.body_reads.get(), control.body_read_drops.get());
            let fired = control
                .fired
                .borrow()
                .iter()
                .fold(0, |mask, event| mask | event.event_type.bit());
            let cancelled = control
                .cancelled
                .borrow()
                .iter()
                .fold(0, |mask, (event, _)| mask | event.event_type.bit());
            assert_eq!(
                fired | cancelled,
                ByteEventType::TransportWrite.bit()
                    | ByteEventType::KernelWrite.bit()
                    | ByteEventType::NicTx.bit()
                    | ByteEventType::CumulativeAck.bit()
            );
            assert_eq!(fired & cancelled, 0);
            assert_eq!(
                control.fired.borrow().len() + control.cancelled.borrow().len(),
                4
            );
            assert!(!control.cancelled.borrow().is_empty());
            for (_, error) in control.cancelled.borrow().iter() {
                assert_eq!(error.code(), expected_code);
                assert_eq!(error.message(), expected_message);
            }
            if fault == H1IngressRetirementFault::None {
                assert!(panic.is_none());
                call.expect("healthy parent joined");
                assert_eq!(receipt.unwrap(), Ok(()));
                let run = control.run.borrow();
                let error = run.as_ref().expect("Run joined").as_ref().unwrap_err();
                assert_eq!(error.code(), expected_code);
                assert_eq!(error.message(), expected_message);
                let ingress = control.ingress.borrow();
                let error = ingress
                    .as_ref()
                    .expect("actual ingress reader joined")
                    .as_ref()
                    .unwrap_err();
                assert_eq!(error.code(), expected_code);
                if case == H1IngressFailureCase::HardLimitBody {
                    assert_eq!(error.message(), b"Sender exceeded flow control");
                }
                let transport = control.transport.borrow();
                let transport = transport.as_ref().unwrap().borrow();
                assert_eq!(transport.close, CloseKind::Reset);
                assert_eq!(control.writes_started.get(), 1);
                assert_eq!(control.writes_done.get(), 1);
                assert!(transport.reads.is_empty());
                let wire = transport.writes.concat();
                assert!(wire.starts_with(b"HTTP/1.1 200"));
                assert_eq!(
                    wire.windows(4)
                        .position(|part| part == b"\r\n\r\n")
                        .unwrap()
                        + 4,
                    wire.len()
                );
            } else {
                let first = panic.expect("joined driver preserved retirement panic");
                assert!(Arc::ptr_eq(&first, &control.first));
                assert_eq!(
                    call,
                    Err(roxygen_runtime::worker::WorkerCallError::Cancelled)
                );
                assert_eq!(
                    receipt.unwrap(),
                    Err(roxygen_core::exchange::ExchangeCompletionError::CleanupUnconfirmed)
                );
            }
            assert_eq!(
                control.secondary_drops.load(Ordering::SeqCst),
                usize::from(fault == H1IngressRetirementFault::Stop)
            );
            assert_eq!(
                control.nested_drops.load(Ordering::SeqCst),
                usize::from(fault == H1IngressRetirementFault::Stop)
            );
        });
    })
    .join();
    if let Err(payload) = joined {
        std::panic::resume_unwind(payload);
    }
}

mod owned_h1 {
    include!("owned_h1.rs");
}

mod owned_upstream_h1 {
    include!("owned_upstream_h1.rs");
}

mod h1 {
    use super::*;

    #[test]
    fn post_final_ingress_eof_resets_without_replacement_response() {
        for case in [
            H1IngressFailureCase::EofHeaderEom,
            H1IngressFailureCase::EofBody,
        ] {
            h1_ingress_failure_network(case);
        }
    }

    #[test]
    fn post_final_body_parse_failure_preserves_original_cause() {
        for case in [
            H1IngressFailureCase::ParseHeaderEom,
            H1IngressFailureCase::ParseBody,
            H1IngressFailureCase::ParseAfterPriorError,
        ] {
            h1_ingress_failure_network(case);
        }
    }

    #[test]
    fn pre_final_ingress_failure_preserves_complete_400() {
        for case in [
            H1IngressFailureCase::BeforeHeadersEof,
            H1IngressFailureCase::BeforeHeadersParse,
            H1IngressFailureCase::NoStatus,
            H1IngressFailureCase::AlreadyComplete,
        ] {
            h1_ingress_failure_network(case);
        }
    }

    #[test]
    fn post_final_ingress_failure_drains_bytes_and_joins_owners() {
        for fault in [
            H1IngressRetirementFault::None,
            H1IngressRetirementFault::Stop,
            H1IngressRetirementFault::ByteCancellation,
        ] {
            h1_ingress_retirement_case(H1IngressFailureCase::ParseBody, fault);
        }
        h1_ingress_retirement_case(
            H1IngressFailureCase::HardLimitBody,
            H1IngressRetirementFault::None,
        );
    }

    #[test]
    fn selected_close_retires_ready_and_late_fallbacks() {
        for case in [
            SelectedCloseCase::ReadyFallback,
            SelectedCloseCase::HeaderClose,
            SelectedCloseCase::OtherUpgrade,
            SelectedCloseCase::ReadyEvent,
            SelectedCloseCase::LateHandler,
        ] {
            selected_close_case(case);
        }
    }

    #[test]
    fn selected_close_drains_selected_body_and_byte_events() {
        selected_close_case(SelectedCloseCase::BodyAndBytes);
    }

    #[test]
    fn selected_close_retirement_preserves_first_failure() {
        for case in [
            SelectedCloseCase::WriteFailure,
            SelectedCloseCase::CallbackFailure,
            SelectedCloseCase::SourceFailure,
            SelectedCloseCase::HandlerCancellation,
        ] {
            selected_close_case(case);
        }
    }

    #[test]
    fn selected_close_preserves_ordinary_handler_error_response() {
        selected_close_case(SelectedCloseCase::OrdinaryError);
    }

    #[test]
    fn httpupstream_session_test_simple() {
        let outcome = upstream(0, 100);
        assert_eq!(outcome.error, None);
        assert!(output_contains(&outcome, b"GET / HTTP/1.1"));
        assert_eq!(outcome.bodies, 100);
    }

    #[test]
    fn httpupstream_session_test_simple_post() {
        let outcome = upstream(100, 100);
        assert_eq!(outcome.error, None);
        assert!(output_contains(&outcome, b"Content-Length: 100"));
        assert!(outcome.output.ends_with(&[b'x'; 100]));
    }

    #[test]
    fn httpupstream_session_test_large_post() {
        let outcome = upstream(70_000, 40_000);
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 40_000);
        assert!(outcome.output.len() > 70_000);
    }

    #[test]
    fn httpupstream_session_test_padding() {
        let outcome = upstream_response_split_around_h1_padding();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 100);
        assert!(output_contains(&outcome, b"Content-Length: 100"));
    }

    #[test]
    fn httpupstream_session_test_body_error() {
        assert_reset(upstream_body_error());
    }

    #[test]
    fn h12_upstream_session_test_write_timeout() {
        let outcome = write_timeout();
        assert_reset(outcome);
    }

    #[test]
    fn httpupstream_session_test_post_body_read_timeout() {
        let (error, close) = pending_request_body_keeps_response_timer_stopped();
        assert_eq!(error, HttpErrorCode::CORO_CANCELLED);
        assert_eq!(close, CloseKind::Reset);
    }

    #[test]
    fn httpupstream_session_test_post_read_timeout() {
        let (error, _) = upstream_response_timeout(100, Duration::from_millis(10), None);
        assert_eq!(error, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn httpupstream_session_test_post_read_custom_timeout() {
        let (error, _) =
            upstream_response_timeout(100, Duration::from_secs(1), Some(Duration::from_millis(10)));
        assert_eq!(error, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn httpupstream_session_test_get_read_timeout() {
        let (error, _) = upstream_response_timeout(0, Duration::from_millis(10), None);
        assert_eq!(error, HttpErrorCode::READ_TIMEOUT);
    }

    #[test]
    fn header_only_response_timeout_begins_after_request_eom() {
        header_only_response_waits_for_serialized_eom();
    }

    #[test]
    fn rejected_foreign_prepared_request_preserves_next_h1_stream_id() {
        rejected_cross_session_request_does_not_advance_h1_stream();
    }

    #[test]
    fn drain_half_close_waits_for_active_response_detach() {
        draining_upstream_waits_for_active_response();
    }

    #[test]
    fn httpupstream_session_test_stream_ingress_complete_no_rst() {
        let outcome = upstream(0, 0);
        assert_ne!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn httpupstream_session_test_cancel_before_sending_headers() {
        assert_eq!(cancelled_before_headers(), HttpErrorCode::CORO_CANCELLED);
    }

    #[test]
    fn httpupstream_session_test_cancel_during_post_body() {
        let (error, close) = cancel_during_post_body();
        assert!(error == HttpErrorCode::CORO_CANCELLED || error == HttpErrorCode::CANCEL);
        assert_eq!(close, CloseKind::Reset);
    }

    #[test]
    fn httpupstream_session_test_send_request_headers_available() {
        let (outcome, header_reads, rejected) = upstream_with_available_headers();
        assert_eq!(outcome.error, None);
        assert!(output_contains(&outcome, b"Content-Length: 1500"));
        assert_eq!(header_reads, 0);
        assert_eq!(rejected, HttpErrorCode::REFUSED_STREAM);
    }

    #[test]
    fn h12_upstream_session_test_close_on_response_without_request() {
        let outcome = upstream_response_without_request();
        assert!(outcome.output.is_empty());
        assert!(outcome.error.is_some());
        assert_eq!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn upstream_eof_rejects_prepared_request_before_finalization() {
        upstream_eof_rejects_prepared_request();
    }

    #[test]
    fn upstream_eof_fails_committed_request_without_response_headers() {
        upstream_eof_fails_committed_request_without_response();
    }

    #[test]
    fn upstream_connection_close_drains_before_later_admission() {
        upstream_connection_close_rejects_later_request();
    }

    #[test]
    fn httpupstream_session_test_early_data_handler_receives_settings() {
        let outcome = idle_upstream();
        assert_eq!(outcome.error, None);
        assert!(outcome.output.is_empty());
    }

    #[test]
    fn httpdownstream_session_test_test_eof() {
        let outcome = downstream(vec![ReadAction::Eof], HandlerMode::Consume(200, 0));
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.output, Vec::<u8>::new());
    }

    #[test]
    fn final_transport_read_processes_data_before_eof() {
        let outcome = downstream(
            vec![ReadAction::DataEof(
                b"GET / HTTP/1.1\r\nHost: example\r\n\r\n".to_vec(),
            )],
            HandlerMode::Consume(200, 0),
        );
        assert_ok_response(outcome, 200);
    }

    fn assert_aborted_at_eof((outcome, drops_before_release): (Outcome, usize)) {
        assert_eq!(drops_before_release, 1, "EOF cancels the parked handler");
        assert_eq!(outcome.output, Vec::<u8>::new());
        assert_eq!(outcome.close, CloseKind::Reset);
        assert_eq!(outcome.error, Some(HttpErrorCode::TRANSPORT_EOF));
    }

    fn assert_served((outcome, drops_before_release): (Outcome, usize)) {
        assert_eq!(drops_before_release, 0, "the handler runs until released");
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn downstream_eof_keeps_a_completed_exchange_by_default() {
        assert_served(peer_eof(KEEPALIVE_GET, PeerEof::WhileParked, false));
    }

    #[test]
    fn downstream_eof_aborts_a_completed_exchange_when_enabled() {
        assert_aborted_at_eof(peer_eof(KEEPALIVE_GET, PeerEof::WhileParked, true));
    }

    #[test]
    fn downstream_eof_keeps_a_connection_close_exchange_by_default() {
        assert_served(peer_eof(CLOSE_GET, PeerEof::WhileParked, false));
    }

    #[test]
    fn downstream_eof_aborts_a_connection_close_exchange_when_enabled() {
        assert_aborted_at_eof(peer_eof(CLOSE_GET, PeerEof::WhileParked, true));
    }

    #[test]
    fn downstream_connection_close_exchange_ends_without_eof_when_enabled() {
        let request = b"GET / HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\nGET /after HTTP/1.1\r\nHost: example\r\n\r\n";
        let (outcome, drops_before_release) = peer_eof(request, PeerEof::Never, true);
        assert_eq!(drops_before_release, 0);
        assert_eq!(
            outcome
                .output
                .windows(b"HTTP/1.1".len())
                .filter(|wire| *wire == b"HTTP/1.1")
                .count(),
            1,
            "the request after `Connection: close` is discarded unparsed"
        );
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn downstream_eof_after_the_response_closes_gracefully_when_enabled() {
        assert_served(peer_eof(KEEPALIVE_GET, PeerEof::AfterResponse, true));
    }

    #[test]
    fn downstream_eof_keeps_an_established_tunnel_when_enabled() {
        let outcome = downstream_connect_eof_after_response();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 100);
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
        assert!(outcome.output.ends_with(&[b'y'; 100]));
    }

    #[test]
    fn httpdownstream_session_test_simple() {
        assert_ok_response(simple_get(HandlerMode::Consume(200, 100)), 200);
    }

    #[test]
    fn httpdownstream_session_test_padded_request() {
        let outcome = simple_post(b"hellohello", HandlerMode::Consume(200, 100));
        assert_eq!(outcome.bodies, 10);
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn httpdownstream_session_test_head_response() {
        let outcome = downstream(
            vec![
                ReadAction::Data(b"HEAD / HTTP/1.1\r\nHost: example\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ],
            HandlerMode::Head,
        );
        assert!(output_contains(&outcome, b"Content-Length: 100"));
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn h1_downstream_session_test_read_data_after_cancel() {
        let outcome = pipelined_early_responses();
        assert_eq!(
            outcome
                .output
                .windows(b"HTTP/1.1 401".len())
                .filter(|window| *window == b"HTTP/1.1 401".as_slice())
                .count(),
            2
        );
        assert_ne!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn h1_pipeline_admits_only_one_lookahead_request() {
        anti_pipeline_retains_third_request_until_detach();
    }

    #[test]
    fn earlier_response_remains_ready_after_its_ingress_metadata_detaches() {
        assert!(egress_stream_is_ready_for_test(
            roxygen_core::ids::StreamId::new(1),
            roxygen_core::ids::StreamId::new(2),
        ));
    }

    #[test]
    fn h1_connection_close_discards_paused_lookahead() {
        connection_close_discards_admitted_lookahead();
    }

    #[test]
    fn h1_request_connection_close_stops_buffered_lookahead() {
        request_connection_close_stops_buffered_lookahead();
    }

    #[test]
    fn h1_downstream_session_test_ingress_backpressure_limit_exceeded() {
        let outcome = backpressured_post(65_535, [35_000, 35_000, 2_000]);
        assert_eq!(outcome.bodies, 72_000);
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn h1_downstream_session_test_ingress_backpressure_set_read_buffer_limit() {
        let outcome = backpressured_post(500, [300, 300, 250]);
        assert_eq!(outcome.bodies, 850);
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn h1_downstream_session_test_ingress_backpressure_limit_exceeded_timeout() {
        let outcome = backpressure_timeout();
        assert_eq!(outcome.error, Some(HttpErrorCode::READ_TIMEOUT));
        assert_eq!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn h1_ingress_hard_limit_rejects_the_crossing_chunk() {
        let (outcome, message) = ingress_hard_limit_is_enforced();
        assert!(output_contains(&outcome, b"HTTP/1.1 400"));
        assert_eq!(outcome.close, CloseKind::Reset);
        assert_eq!(outcome.error, Some(HttpErrorCode::FLOW_CONTROL_ERROR));
        assert_eq!(message, b"ingress source rejected body bytes");
        assert!(outcome.output.starts_with(b"HTTP/1.1 400"));
        let boundary = outcome
            .output
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .expect("complete error response head")
            + 4;
        assert_eq!(outcome.output.len(), boundary);
        assert!(
            outcome
                .output
                .windows(17)
                .any(|part| part.eq_ignore_ascii_case(b"content-length: 0"))
        );
        assert!(!output_contains(&outcome, b"HTTP/1.1 500"));
    }

    #[test]
    fn h12_downstream_session_test_body_error() {
        let outcome = simple_get(HandlerMode::BodyErrorSource);
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
        assert_reset(outcome);
    }

    #[test]
    fn httpdownstream_session_test_content_length_mismatch() {
        let outcome = malformed_content_length();
        assert_eq!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn downstream_codec_failure_flushes_400_before_reset() {
        let (outcome, unread) = malformed_chunk_response();
        assert!(output_contains(&outcome, b"HTTP/1.1 400"));
        assert_eq!(outcome.close, CloseKind::Reset);
        assert_eq!(unread, 1);
    }

    #[test]
    fn handler_runtime_error_returns_500() {
        let outcome = simple_get(HandlerMode::Panic);
        assert!(output_contains(&outcome, b"HTTP/1.1 500"));
    }

    #[test]
    fn httpdownstream_session_test_simple_post() {
        let outcome = simple_post(&[b'x'; 100], HandlerMode::Consume(200, 100));
        assert_eq!(outcome.bodies, 100);
        assert_ok_response(outcome, 200);
    }

    #[test]
    fn httpdownstream_session_test_handler_gracefully_terminate_ingress_fixed_response() {
        let outcome = early_post();
        assert!(output_contains(&outcome, b"HTTP/1.1 401"));
        assert_ne!(outcome.close, CloseKind::Reset);
    }

    #[test]
    fn httpdownstream_session_test_handler_gracefully_terminate_ingress_yield_error() {
        let outcome = simple_post(&vec![b'x'; 2_000], HandlerMode::Error);
        assert!(output_contains(&outcome, b"HTTP/1.1 500"));
        assert_ne!(outcome.close, CloseKind::Open);
    }

    #[test]
    fn h1_downstream_session_test_egress_error_rst() {
        let outcome = simple_get(HandlerMode::BodyErrorSource);
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
        assert_reset(outcome);
    }

    #[test]
    fn h1_downstream_session_test_egress_error_rst_pipeline() {
        let outcome = downstream(
            vec![
                ReadAction::Data(b"GET / HTTP/1.1\r\n\r\nGET / HTTP/1.1\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ],
            HandlerMode::BodyErrorSource,
        );
        assert_eq!(
            outcome
                .output
                .windows(b"HTTP/1.1 200".len())
                .filter(|window| *window == b"HTTP/1.1 200".as_slice())
                .count(),
            1,
        );
        assert_reset(outcome);
    }

    #[test]
    fn h1_downstream_session_test_send_connection_close() {
        let outcome = simple_get(HandlerMode::Consume(200, 100));
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
        assert!(output_contains(&outcome, b"Connection: close"));
        assert_eq!(outcome.close, CloseKind::ShutdownWrite);
    }

    #[test]
    fn httpdownstream_session_test_eofwith_stream() {
        let outcome = incomplete_request(ReadAction::Eof);
        assert!(output_contains(&outcome, b"HTTP/1.1 400"));
    }

    #[test]
    fn h12_downstream_session_test_web_socket_upgrade() {
        let outcome = websocket_round_trip();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 100);
        assert!(output_contains(&outcome, b"HTTP/1.1 101"));
        assert!(output_contains(&outcome, b"Sec-WebSocket-Accept:"));
        assert!(outcome.output.ends_with(&[b'y'; 100]));
    }

    #[test]
    fn accepted_upgrade_response_eom_keeps_raw_ingress_alive() {
        prompt_upgrade_eom_preserves_later_raw_ingress();
    }

    #[test]
    fn downstream_connect_upgrade_delivers_raw_ingress() {
        let outcome = downstream_connect_round_trip();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 100);
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
        assert!(outcome.output.ends_with(&[b'y'; 100]));
    }

    #[test]
    fn rejected_upgrade_resolves_parser_pause_before_close() {
        let (outcome, invocations, eom_seen) = rejected_upgrade_unpauses_before_close();
        assert_eq!(invocations, 1);
        assert!(eom_seen);
        assert!(output_contains(&outcome, b"HTTP/1.1 400"));
        assert_eq!(
            outcome
                .output
                .windows(b"HTTP/1.1".len())
                .filter(|wire| *wire == b"HTTP/1.1")
                .count(),
            1,
        );
        assert_eq!(outcome.close, CloseKind::ShutdownWrite);
    }

    #[test]
    fn upstream_connect_upgrade_delivers_raw_ingress() {
        let outcome = upstream_upgrade_round_trip();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 100);
    }

    #[test]
    fn h12_downstream_session_test_write_timeout() {
        assert_reset(write_timeout());
    }

    #[test]
    fn httpdownstream_session_test_read_timeout() {
        let outcome = incomplete_body_stream_timeout();
        assert!(output_contains(&outcome, b"HTTP/1.1 408"));
    }

    #[test]
    fn connection_read_timeout_only_drains_active_request() {
        let outcome = connection_timeout_does_not_abort_active_stream();
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.bodies, 3);
        assert!(output_contains(&outcome, b"HTTP/1.1 200"));
    }

    #[test]
    fn h1_downstream_session_test_headers_read_timeout() {
        let outcome = partial_headers_stream_timeout();
        assert!(output_contains(&outcome, b"HTTP/1.1 408"));
    }

    #[test]
    fn h1_downstream_session_test_body_read_timeout() {
        let outcome = continue_then_body_stream_timeout();
        assert!(output_contains(&outcome, b"HTTP/1.1 100"));
        assert!(output_contains(&outcome, b"HTTP/1.1 408"));
    }

    #[test]
    fn httpdownstream_session_test_handler_no_response() {
        let outcome = simple_get(HandlerMode::Empty);
        assert!(output_contains(&outcome, b"HTTP/1.1 500"));
    }

    #[test]
    fn httpdownstream_session_test_response_headers_error() {
        assert_reset(response_header_error());
    }

    #[test]
    fn httpdownstream_session_test_ingress_error_custom_response() {
        let outcome = custom_timeout_response(408);
        assert!(output_contains(&outcome, b"HTTP/1.1 408"));
        assert!(output_contains(&outcome, b"custom response generated"));
    }

    #[test]
    fn httpdownstream_session_test_ingress_error_custom_response_mismatch() {
        let outcome = custom_timeout_response(500);
        assert!(!output_contains(&outcome, b"HTTP/1.1 500"));
        assert!(output_contains(&outcome, b"HTTP/1.1 408"));
        assert!(!output_contains(&outcome, b"custom response generated"));
    }

    /// The HTTP/1.1 timer tests on a `HostedWorker`, whose wheel timer backs the
    /// session's timed batons and host sleeps: write timeouts, upstream response and
    /// downstream stream read timeouts (408), the connection read timeout's drain,
    /// ingress backpressure, and custom timeout responses.
    #[test]
    fn timers_on_a_hosted_worker() {
        on_hosted_workers(|| {
            h12_upstream_session_test_write_timeout();
            httpupstream_session_test_post_body_read_timeout();
            httpupstream_session_test_post_read_timeout();
            httpupstream_session_test_post_read_custom_timeout();
            httpupstream_session_test_get_read_timeout();
            header_only_response_timeout_begins_after_request_eom();
            h1_downstream_session_test_ingress_backpressure_limit_exceeded_timeout();
            h12_downstream_session_test_write_timeout();
            httpdownstream_session_test_read_timeout();
            connection_read_timeout_only_drains_active_request();
            h1_downstream_session_test_headers_read_timeout();
            h1_downstream_session_test_body_read_timeout();
            httpdownstream_session_test_ingress_error_custom_response();
            httpdownstream_session_test_ingress_error_custom_response_mismatch();
        });
    }

    #[test]
    fn egress_trailers_padding_and_upgrade_keep_wire_order() {
        let (outcome, push_drops, events) = branch_response();
        assert_eq!(outcome.error, None);
        assert_eq!(push_drops, 1);
        let push = events
            .iter()
            .find(|event| {
                event.field_section_info.is_some_and(|info| {
                    info.section_type == roxygen_core::byte_event::FieldSectionType::PushPromise
                })
            })
            .expect("ignored H1 push retained byte-event field metadata");
        let info = push.field_section_info.unwrap();
        assert!(info.final_headers);
        assert_eq!(info.size, Default::default());
        assert!(output_contains(&outcome, b"3\r\nabc\r\n"));
        assert!(output_contains(&outcome, b"x-trailer: done"));
    }

    #[test]
    fn byte_event_boundaries_use_post_generation_offsets() {
        let (events, flags) = registered_response();
        assert_eq!(events.len(), 10);
        for event in &events {
            assert_eq!(event.stream_offset, event.transport_offset);
            assert_eq!(
                event.transport_offset,
                if event.body_offset == 0 { 101 } else { 114 }
            );
        }
        let writes = events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.event_type == ByteEventType::TransportWrite);
        for (write_index, write) in writes {
            let completion_index = events
                .iter()
                .position(|event| {
                    event.event_type == ByteEventType::KernelWrite
                        && event.transport_offset == write.transport_offset
                        && event.field_section_info == write.field_section_info
                })
                .expect("each write registration completes");
            assert!(write_index < completion_index);
        }
        assert_eq!(flags.len(), 2);
        assert!(
            flags
                .iter()
                .all(|flags| flags.timestamp_write && flags.timestamp_tx && flags.timestamp_ack)
        );
    }

    #[test]
    fn read_failure_interrupts_byte_event_wait_and_resets_transport() {
        read_failure_interrupts_final_byte_event_wait();
    }

    #[test]
    fn native_byte_event_cancellation_preserves_sibling_and_later_events() {
        native_byte_event_cancellation_is_kind_and_boundary_scoped();
    }

    #[test]
    fn panicking_native_callback_releases_bridge_before_next_boundary() {
        panicking_native_callback_releases_bridge_before_reentry();
    }

    #[test]
    fn header_byte_event_registration_precedes_encoded_callback() {
        header_byte_events_register_before_encoded_callback();
    }

    #[test]
    fn panicking_header_callback_still_finishes_driver_cleanup() {
        panicking_header_callback_finishes_driver_cleanup();
    }

    #[test]
    fn write_failure_cancels_tx_ack_with_exact_error() {
        write_failure_cancels_tx_ack_boundary_with_triggering_error();
    }

    #[test]
    fn read_failure_cancels_queued_prepared_header_with_exact_error() {
        read_failure_cancels_queued_prepared_header_with_triggering_error();
    }

    #[test]
    fn cancelling_run_finishes_active_stream_cleanup() {
        cancelled_run_future_finishes_owner_cleanup();
    }

    #[test]
    fn dropping_never_polled_run_is_drained_by_worker() {
        never_polled_run_cleanup_is_worker_owned();
    }

    #[test]
    fn escaped_unpolled_run_is_inert_after_worker_closed() {
        escaped_unpolled_run_releases_owner_resources_before_closed();
    }

    #[test]
    fn escaped_running_run_is_inert_after_worker_closed() {
        escaped_running_run_releases_owner_resources_before_closed();
    }

    #[test]
    fn constructor_failures_do_not_drop_unfinalized_core() {
        constructor_errors_precede_session_core_ownership();
    }

    #[test]
    fn worker_shutdown_rejects_reservation_and_prepared_commit() {
        worker_shutdown_rejects_new_request_admission();
    }

    #[test]
    fn rejected_prepared_request_allows_source_drop_reentrancy() {
        rejected_prepared_request_drops_source_without_state_borrow();
    }

    #[test]
    fn serialization_error_allows_registration_cancel_reentrancy() {
        serialization_error_drops_registrations_after_state_borrow();
    }

    #[test]
    fn connection_failure_allows_queued_registration_cancel_reentrancy() {
        connection_failure_cancels_queued_registration_reentrantly();
    }

    #[test]
    fn forced_error_replaces_stale_response_outside_state_borrow() {
        forced_error_evicts_pending_handler_response_reentrantly();
    }

    #[test]
    fn escaped_handle_rejects_events_without_retaining_observer_guard() {
        escaped_handle_does_not_retain_observer();
    }
}

pub(crate) fn ready_dispatch_transport() -> Rc<dyn UniplexTransport> {
    ScriptedTransport::new([ReadAction::Eof]).0
}

/// The writes an upstream HTTP/1.1 session makes for a POST of `hello`
/// answered by the peer once it has the request.
fn upstream_post_writes(config: UniplexConfig) -> Vec<Vec<u8>> {
    run_on_worker(move |context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&respond),
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_vec(),
            )]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                config,
            )
            .unwrap();
            let exchange = async {
                let request = HttpFixedSource::make_fixed_request(
                    b"/",
                    HttpMethod::Post,
                    Some(queue(b"hello")),
                )
                .unwrap();
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                while !state.borrow().writes.concat().ends_with(b"hello") {
                    tokio::task::yield_now().await;
                }
                respond.notify_one();
                let head = response.source.read_header_event().await.unwrap();
                assert_eq!(head.headers().response_status(), Some(200));
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.unwrap();
            state.borrow().writes.clone()
        })
    })
}

#[test]
fn upstream_h1_request_head_and_ready_body_share_a_write() {
    let writes = upstream_post_writes(UniplexConfig::default());
    assert_eq!(writes.len(), 1, "head and body together");
    assert!(writes[0].starts_with(b"POST / HTTP/1.1\r\n"));
    assert!(writes[0].ends_with(b"\r\n\r\nhello"));
}

/// A request whose body becomes readable only when `release` is notified.
struct GatedBody {
    head: Option<HttpHeaderEvent>,
    release: Arc<tokio::sync::Notify>,
    sent: bool,
}

impl HttpSource for GatedBody {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.head.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        _maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        assert!(!std::mem::replace(&mut self.sent, true), "one body event");
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            release.notified().await;
            Ok(HttpBodyEvent::body(queue(b"hello"), true))
        })
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

#[test]
fn batched_h1_egress_writes_the_head_without_waiting_for_the_body() {
    run_on_worker(|context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitData(
                Arc::clone(&respond),
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
            )]);
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                UniplexConfig::default(),
            )
            .unwrap();
            let release = Arc::new(tokio::sync::Notify::new());
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_url(b"/", false).unwrap();
            message.set_http_version(1, 1);
            message
                .headers_mut()
                .set(HttpHeaderCode::ContentLength, b"5");
            let request = GatedBody {
                head: Some(HttpHeaderEvent::new(message, false).unwrap()),
                release: Arc::clone(&release),
                sent: false,
            };
            let exchange = async {
                let response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while state.borrow().writes.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the head is written while the body is pending");
                {
                    let writes = &state.borrow().writes;
                    assert_eq!(writes.len(), 1);
                    assert!(writes[0].starts_with(b"POST / HTTP/1.1\r\n"));
                    assert!(writes[0].ends_with(b"\r\n\r\n"));
                }
                release.notify_one();
                while state.borrow().writes.len() < 2 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(state.borrow().writes[1], b"hello");
                respond.notify_one();
                drop(response);
                handle.initiate_drain().unwrap();
            };
            let (result, ()) = tokio::join!(session.run(), exchange);
            result.unwrap();
        })
    });
}

/// The writes a downstream HTTP/1.1 session makes answering one GET with a
/// fixed three-byte body.
fn downstream_response_writes(config: UniplexConfig) -> Vec<Vec<u8>> {
    struct Fixed;
    impl HttpHandler for Fixed {
        fn handle_request(
            &self,
            _context: HandlerSessionContext,
            mut request_source: HttpSourceHolder,
        ) -> LocalHandlerFuture {
            Box::pin(async move {
                request_source.read_header_event().await?;
                Ok(fixed_response(200, 3))
            })
        }
    }
    run_on_worker(move |context| {
        Box::pin(async move {
            let (transport, state) = ScriptedTransport::new([
                ReadAction::Data(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()),
                ReadAction::Eof,
            ]);
            let erased: Rc<dyn UniplexTransport> = transport;
            let (session, _handle) = UniplexSession::new(
                context,
                TransportDirection::Downstream,
                erased,
                Some(Rc::new(Fixed)),
                config,
            )
            .unwrap();
            session.run().await.unwrap();
            state.borrow().writes.clone()
        })
    })
}

#[test]
fn downstream_h1_response_head_and_ready_body_share_a_write() {
    let writes = downstream_response_writes(UniplexConfig::default());
    assert_eq!(writes.len(), 1, "head and body together: {writes:?}");
    assert!(writes[0].starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(writes[0].ends_with(b"\r\n\r\nxxx"));
}

/// Zero-byte padding shares the head's write, but a TX/ACK boundary still ends
/// a write. Every registration reports its serialized byte offset.
#[test]
fn batched_h1_egress_keeps_byte_event_offsets_and_tx_ack_boundaries() {
    let summary = |events: &[HttpByteEvent]| {
        let mut events = events
            .iter()
            .map(|event| {
                (
                    format!("{:?}", event.event_type),
                    event.transport_offset,
                    event.stream_offset,
                    event.body_offset,
                )
            })
            .collect::<Vec<_>>();
        events.sort();
        events
    };
    let ((events, flags), writes) = registered_response_with(UniplexConfig::default(), 2);
    assert_eq!(
        writes.iter().map(Vec::len).collect::<Vec<_>>(),
        [101, 13],
        "the head's TX/ACK boundary still ends a write"
    );
    assert_eq!(writes[1], b"3\r\nabc\r\n0\r\n\r\n");
    assert!(
        flags
            .iter()
            .all(|flags| flags.timestamp_tx && flags.timestamp_ack),
        "each write ends at a TX/ACK boundary: {flags:?}"
    );
    let mut expected = Vec::new();
    for event_type in [
        ByteEventType::TransportWrite,
        ByteEventType::KernelWrite,
        ByteEventType::NicTx,
        ByteEventType::CumulativeAck,
    ] {
        expected.push((format!("{event_type:?}"), 101, 101, 0));
        expected.push((format!("{event_type:?}"), 114, 114, 3));
    }
    for event_type in [ByteEventType::TransportWrite, ByteEventType::KernelWrite] {
        expected.push((format!("{event_type:?}"), 101, 101, 0));
    }
    expected.sort();
    assert_eq!(summary(&events), expected);
}

/// The egress buffer and write size of the buffer-boundary tests: a queued head
/// leaves too little room for a body that alone fills the buffer.
const BOUNDARY_BUFFER: usize = 64 << 10;
/// Body bytes of the buffer-boundary tests; never a hex digit, so chunk sizes
/// cannot be mistaken for body bytes.
const BOUNDARY_BYTE: u8 = b'z';

/// A request whose body events, `(length, eom)` each, are all ready at once.
struct ReadyRequest {
    head: Option<HttpHeaderEvent>,
    events: VecDeque<(usize, bool)>,
}

impl HttpSource for ReadyRequest {
    fn read_header_event(
        &mut self,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        Box::pin(std::future::ready(self.head.take().ok_or_else(|| {
            HttpError::with_message(HttpErrorCode::DROPPED, b"request header exhausted")
        })))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> roxygen_core::source::LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let (length, eom) = self.events.pop_front().expect("a body event is left");
        assert!(length <= maximum as usize, "each body event fits one read");
        Box::pin(std::future::ready(Ok(HttpBodyEvent::body(
            queue(&vec![BOUNDARY_BYTE; length]),
            eom,
        ))))
    }

    fn stop_reading(&mut self, _error: Option<HttpErrorCode>) {}
}

/// The writes of an upstream HTTP/1.1 POST whose body events are all ready, on
/// a session whose egress buffer and write size are both `BOUNDARY_BUFFER`,
/// with a `Content-Length` of `content_length` or, without one, chunked; and the
/// response status, or the error that failed the exchange instead.
fn boundary_post(
    content_length: Option<usize>,
    events: &'static [(usize, bool)],
) -> (Vec<Vec<u8>>, Result<u16, HttpErrorCode>) {
    run_on_worker(move |context| {
        Box::pin(async move {
            let respond = Arc::new(tokio::sync::Notify::new());
            // A failed session cancels the read that waits for the request.
            let (transport, state) = ScriptedTransport::new([ReadAction::WaitDataOrCancellation(
                Arc::clone(&respond),
                b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_vec(),
            )]);
            let config = UniplexConfig {
                buffer_limit: BOUNDARY_BUFFER,
                maximum_write_size: BOUNDARY_BUFFER,
                ..UniplexConfig::default()
            };
            let (session, handle) = UniplexSession::new(
                context,
                TransportDirection::Upstream,
                transport,
                None,
                config,
            )
            .unwrap();
            let mut message = roxygen_core::message::HttpMessage::new();
            message.set_method(HttpMethod::Post).unwrap();
            message.set_url(b"/", false).unwrap();
            message.set_http_version(1, 1);
            if let Some(length) = content_length {
                message
                    .headers_mut()
                    .set(HttpHeaderCode::ContentLength, length.to_string().as_bytes());
            }
            let request = ReadyRequest {
                head: Some(HttpHeaderEvent::new(message, false).unwrap()),
                events: events.iter().copied().collect(),
            };
            let body_length: usize = events.iter().map(|(length, _)| length).sum();
            let request_sent = || {
                let wire = state.borrow().writes.concat();
                wire.iter().filter(|byte| **byte == BOUNDARY_BYTE).count() == body_length
                    && (content_length.is_some() || wire.ends_with(b"0\r\n\r\n"))
            };
            let exchange = async {
                let mut response = handle
                    .send_request(
                        HttpSourceHolder::new(Box::new(request)),
                        CancellationToken::never(),
                    )
                    .await
                    .map_err(|error| error.code())?;
                let respond_once_sent = async {
                    while !request_sent() {
                        tokio::task::yield_now().await;
                    }
                    respond.notify_one();
                    std::future::pending::<()>().await
                };
                let head = tokio::select! {
                    head = response.source.read_header_event() => head,
                    () = respond_once_sent => unreachable!("the response ends the select"),
                };
                let status = head.map_err(|error| error.code())?;
                drop(response);
                handle.initiate_drain().unwrap();
                Ok(status.headers().response_status().unwrap_or(0))
            };
            let (_, status) = tokio::join!(session.run(), exchange);
            (state.borrow().writes.clone(), status)
        })
    })
}

/// Every write stays within the buffer, and concatenating them preserves the
/// request's framing and complete body.
fn assert_boundary_post(
    content_length: Option<usize>,
    events: &'static [(usize, bool)],
    expected_writes: usize,
) -> Vec<Vec<u8>> {
    let (writes, status) = boundary_post(content_length, events);
    assert_eq!(status, Ok(200));
    assert_eq!(writes.len(), expected_writes);
    assert!(
        writes.iter().all(|write| write.len() <= BOUNDARY_BUFFER),
        "every write fits the buffer"
    );
    let wire = writes.concat();
    let head_end = wire
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4;
    let head = &wire[..head_end];
    assert!(head.starts_with(b"POST / HTTP/1.1\r\n"));
    let mut expected_body = Vec::new();
    if let Some(length) = content_length {
        assert!(String::from_utf8_lossy(head).contains(&format!("Content-Length: {length}\r\n")));
        expected_body.resize(length, BOUNDARY_BYTE);
    } else {
        assert!(String::from_utf8_lossy(head).contains("Transfer-Encoding: chunked\r\n"));
        for &(length, eom) in events {
            if length != 0 {
                expected_body.extend_from_slice(format!("{length:x}\r\n").as_bytes());
                expected_body.extend(std::iter::repeat_n(BOUNDARY_BYTE, length));
                expected_body.extend_from_slice(b"\r\n");
            }
            if eom {
                expected_body.extend_from_slice(b"0\r\n\r\n");
            }
        }
    }
    assert_eq!(&wire[head_end..], expected_body, "complete framed body");
    writes
}

/// A queued head leaves too little room for a body that fills the buffer.
#[test]
fn batched_h1_egress_writes_a_body_event_that_fills_the_buffer_after_the_head() {
    const BODY: &[(usize, bool)] = &[(BOUNDARY_BUFFER - 1, false), (1, true)];
    let writes = assert_boundary_post(Some(BOUNDARY_BUFFER), BODY, 3);
    assert_eq!(writes[1], vec![BOUNDARY_BYTE; BOUNDARY_BUFFER - 1]);
    assert_eq!(writes[2], [BOUNDARY_BYTE]);
}

#[test]
fn batched_h1_egress_writes_a_content_length_body_event_that_no_longer_fits_next() {
    assert_boundary_post(
        Some(BOUNDARY_BUFFER),
        &[(30_000, false), (BOUNDARY_BUFFER - 30_000, true)],
        2,
    );
}

/// The chunk framing and body together fill exactly one buffer.
#[test]
fn batched_h1_egress_writes_a_chunked_body_that_fills_the_buffer_after_the_head() {
    const BODY: &[(usize, bool)] = &[(0xfff3, true)];
    let writes = assert_boundary_post(None, BODY, 2);
    assert_eq!(writes[1].len(), BOUNDARY_BUFFER);
}

#[test]
fn batched_h1_egress_writes_a_chunk_that_no_longer_fits_next() {
    assert_boundary_post(None, &[(40_000, false), (40_000, true)], 2);
}
